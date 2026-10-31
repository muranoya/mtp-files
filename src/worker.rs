//! MTP 操作を担うワーカースレッド。
//!
//! MTP は 1 セッションで 1 トランザクションずつしか処理できない。そこで操作をここへ
//! 集約して直列に実行し、UI スレッドは Command を送って Event を受け取るだけにする。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures::StreamExt;
use futures::executor::block_on;
use mtp_rs::mtp::{HotplugEvent, MtpDeviceInfo, Storage, watch_devices};
use mtp_rs::{Error, MtpDevice, ObjectHandle};

use crate::names::{
    format_datetime, format_size, is_safe_local_name, summarize_names, validate_new_name,
};
use crate::transfer::{self, Meter, OpError, Outcome, Policy};

pub type Emit = Arc<dyn Fn(Event) + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Entry {
    pub handle: ObjectHandle,
    pub name: String,
    pub is_folder: bool,
    pub size: u64,
    pub modified: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceSummary {
    pub location_id: u64,
    pub title: String,
    pub subtitle: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageSummary {
    pub title: String,
    pub subtitle: String,
}

#[derive(Clone, Debug)]
pub enum Transfer {
    Upload { paths: Vec<PathBuf> },
    Download { entries: Vec<Entry>, dest: PathBuf },
}

#[derive(Debug)]
pub enum Command {
    RefreshDevices,
    /// USB の抜き差しの通知。UI の操作ではないので完了時に Event::Idle を出さない。
    DevicesChanged {
        left: Option<u64>,
    },
    Connect(u64),
    SelectStorage(usize),
    Enter {
        handle: ObjectHandle,
        name: String,
    },
    GoToDepth(usize),
    Refresh,
    /// `policy` が None なら同名を確認し、あれば Event::Conflict で問い合わせる。
    Transfer {
        transfer: Transfer,
        policy: Option<Policy>,
    },
    Rename {
        entry: Entry,
        new_name: String,
    },
    CreateFolder(String),
    Delete(Vec<Entry>),
    Shutdown,
}

#[derive(Debug)]
pub enum Event {
    Devices(Vec<DeviceSummary>),
    Connected {
        location_id: u64,
        storages: Vec<StorageSummary>,
    },
    Disconnected,
    StorageSelected(usize),
    Storages(Vec<StorageSummary>),
    Listing {
        crumbs: Vec<String>,
        entries: Vec<Entry>,
        unreadable: usize,
    },
    Busy(String),
    Idle,
    /// `fraction` が None なら全体量が不明な処理中。
    Progress {
        label: String,
        fraction: Option<f32>,
    },
    Conflict {
        transfer: Transfer,
        message: String,
    },
    Done(String),
    Error(String),
}

pub struct WorkerHandle {
    commands: Sender<Command>,
    cancel: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerHandle {
    pub fn send(&self, command: Command) {
        // ワーカーが終了済みなら送れないが、それはアプリの終了時だけで害はない
        let _ = self.commands.send(command);
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    /// 進行中の転送を止めて終了を待つ。応答しないデバイスで終了が固まらないよう上限を設ける。
    pub fn shutdown(&self, timeout: Duration) {
        self.cancel();
        self.send(Command::Shutdown);
        let Some(thread) = self.thread.lock().ok().and_then(|mut t| t.take()) else {
            return;
        };
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = thread.join();
            let _ = done_tx.send(());
        });
        let _ = done_rx.recv_timeout(timeout);
    }
}

pub fn spawn(emit: Emit) -> WorkerHandle {
    let (commands, receiver) = mpsc::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    let thread = thread::spawn({
        let cancel = cancel.clone();
        move || {
            Worker {
                emit,
                cancel,
                session: None,
            }
            .run(receiver)
        }
    });
    WorkerHandle {
        commands,
        cancel,
        thread: Mutex::new(Some(thread)),
    }
}

/// USB の抜き差しを監視し、ワーカーに一覧の更新を依頼する。
///
/// 監視を始められない環境でも、更新ボタンを使えば動くので致命的にはしない。
pub fn spawn_hotplug(worker: &WorkerHandle) {
    let commands = worker.commands.clone();
    thread::spawn(move || {
        let Ok(mut watch) = watch_devices() else {
            return;
        };
        while let Some(event) = block_on(watch.next()) {
            let left = match &event {
                HotplugEvent::Left(info) => Some(info.location_id),
                HotplugEvent::Arrived(_) => None,
            };
            if commands.send(Command::DevicesChanged { left }).is_err() {
                return;
            }
        }
    });
}

/// 指定フォルダの中身を「フォルダ優先・名前順」で返す。2 つ目は読めなかった件数。
pub fn list_entries(
    storage: &Storage,
    parent: Option<ObjectHandle>,
) -> Result<(Vec<Entry>, usize), Error> {
    let collection = block_on(storage.collect_objects(parent))?;
    let mut entries: Vec<Entry> = collection
        .objects
        .into_iter()
        .map(|object| Entry {
            is_folder: object.is_folder(),
            handle: object.handle,
            size: object.size,
            modified: format_datetime(object.modified),
            name: object.filename,
        })
        .collect();
    entries.sort_by_cached_key(|e| (!e.is_folder, e.name.to_lowercase()));
    Ok((entries, collection.skipped.len()))
}

struct Session {
    location_id: u64,
    device: MtpDevice,
    storages: Vec<Storage>,
    current: usize,
    /// 現在のフォルダまでの経路。空ならストレージのルート。
    path: Vec<(ObjectHandle, String)>,
}

impl Session {
    fn storage(&self) -> &Storage {
        &self.storages[self.current]
    }

    fn parent(&self) -> Option<ObjectHandle> {
        self.path.last().map(|(handle, _)| *handle)
    }

    fn storage_summaries(&self) -> Vec<StorageSummary> {
        self.storages
            .iter()
            .enumerate()
            .map(|(i, storage)| summarize_storage(i, storage))
            .collect()
    }
}

fn summarize_storage(index: usize, storage: &Storage) -> StorageSummary {
    let info = storage.info();
    let title = if info.description.trim().is_empty() {
        format!("ストレージ {}", index + 1)
    } else {
        info.description.clone()
    };
    StorageSummary {
        title,
        subtitle: format!(
            "空き {} / {}",
            format_size(info.free_space),
            format_size(info.total_capacity)
        ),
    }
}

fn summarize_device(info: &MtpDeviceInfo) -> DeviceSummary {
    let title = match (info.manufacturer.as_deref(), info.product.as_deref()) {
        (Some(maker), Some(product)) if !product.contains(maker) => format!("{maker} {product}"),
        (_, Some(product)) => product.to_owned(),
        (Some(maker), None) => maker.to_owned(),
        (None, None) => "不明なデバイス".to_owned(),
    };
    DeviceSummary {
        location_id: info.location_id,
        title,
        subtitle: format!("USB {:04x}:{:04x}", info.vendor_id, info.product_id),
    }
}

pub fn describe_device_error(error: &Error) -> String {
    match error {
        Error::Busy => "デバイスが処理中です。少し待ってからもう一度お試しください。".into(),
        Error::ExclusiveAccess => "他のプログラムがデバイスを使用中です。ファイルマネージャーでデバイスを取り出す（アンマウントする）か、mtp-connect などを終了してから、もう一度お試しください。".into(),
        Error::PermissionDenied => "デバイスへのアクセス権がありません。udev ルール（README を参照）を設定し、デバイスを接続し直してください。".into(),
        Error::Unsupported => "このデバイスはこの操作に対応していません。".into(),
        Error::AccessDenied => "デバイスに拒否されました（読み取り専用の可能性があります）。".into(),
        Error::StorageFull => "デバイスの空き容量が足りません。".into(),
        Error::Disconnected | Error::NoDevice => "デバイスとの接続が切れました。".into(),
        Error::DeviceReset => "デバイスの接続をリセットしました。デバイスをもう一度選択してください。".into(),
        Error::Timeout => "デバイスの応答がタイムアウトしました。ケーブルを確認し、接続し直してください。".into(),
        Error::StaleHandle | Error::NotFound => "デバイス上のファイルが変更されています。一覧を更新してやり直してください。".into(),
        Error::NameTooLong { .. } => "名前が長すぎます（MTP の上限は約 254 文字です）。".into(),
        Error::Cancelled => "キャンセルしました。".into(),
        other => format!("デバイスでエラーが発生しました: {other}"),
    }
}

fn describe(error: &OpError) -> String {
    match error {
        OpError::Device(e) => describe_device_error(e),
        OpError::Local { path, source } => {
            format!(
                "PC 側のファイル操作に失敗しました: {}\n{source}",
                path.display()
            )
        }
        OpError::Conflict(message) => message.clone(),
    }
}

fn outcome_message(action: &str, outcome: &Outcome) -> String {
    if outcome.skipped > 0 {
        format!(
            "{} 件{action}しました（スキップ {} 件）。",
            outcome.done, outcome.skipped
        )
    } else {
        format!("{} 件{action}しました。", outcome.done)
    }
}

struct Worker {
    emit: Emit,
    cancel: Arc<AtomicBool>,
    session: Option<Session>,
}

impl Worker {
    fn run(mut self, commands: Receiver<Command>) {
        for command in commands {
            if matches!(command, Command::Shutdown) {
                break;
            }
            if let Command::DevicesChanged { left } = command {
                self.devices_changed(left);
                continue;
            }
            self.cancel.store(false, Ordering::Relaxed);
            self.handle(command);
            (self.emit)(Event::Idle);
        }
        if let Some(session) = self.session.take() {
            let _ = block_on(session.device.close());
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::RefreshDevices => self.refresh_devices(),
            Command::Connect(location_id) => self.connect(location_id),
            Command::SelectStorage(index) => {
                let result = self.with_session(|session, _| {
                    if index < session.storages.len() {
                        session.current = index;
                        session.path.clear();
                    }
                    Ok(())
                });
                if result.is_some() {
                    (self.emit)(Event::StorageSelected(index));
                    self.relist();
                }
            }
            Command::Enter { handle, name } => {
                let entered = self.with_session(|session, _| {
                    session.path.push((handle, name));
                    Ok(())
                });
                if entered.is_some()
                    && !self.relist()
                    && let Some(session) = &mut self.session
                {
                    session.path.pop();
                }
            }
            Command::GoToDepth(depth) => {
                if let Some(session) = &mut self.session {
                    session.path.truncate(depth);
                    self.relist();
                }
            }
            Command::Refresh => {
                self.relist();
            }
            Command::Transfer { transfer, policy } => self.transfer(transfer, policy),
            Command::Rename { entry, new_name } => self.rename(entry, &new_name),
            Command::CreateFolder(name) => self.create_folder(&name),
            Command::Delete(entries) => self.delete(entries),
            Command::DevicesChanged { .. } | Command::Shutdown => {}
        }
    }

    fn devices_changed(&mut self, left: Option<u64>) {
        let current = self.session.as_ref().map(|s| s.location_id);
        if left.is_some() && left == current {
            self.drop_session();
        }
        self.refresh_devices();
    }

    fn refresh_devices(&self) {
        match MtpDevice::list_devices() {
            Ok(devices) => (self.emit)(Event::Devices(
                devices.iter().map(summarize_device).collect(),
            )),
            Err(e) => (self.emit)(Event::Error(describe_device_error(&e))),
        }
    }

    fn drop_session(&mut self) {
        if self.session.take().is_some() {
            (self.emit)(Event::Disconnected);
        }
    }

    fn connect(&mut self, location_id: u64) {
        if let Some(session) = self.session.take() {
            let _ = block_on(session.device.close());
            (self.emit)(Event::Disconnected);
        }
        (self.emit)(Event::Busy("デバイスに接続しています…".into()));
        let opened = block_on(async {
            let device = MtpDevice::builder().open_by_location(location_id).await?;
            let storages = device.storages().await?;
            Ok::<_, Error>((device, storages))
        });
        match opened {
            Ok((_, storages)) if storages.is_empty() => {
                (self.emit)(Event::Error(
                    "ストレージが見つかりません。デバイスのロックを解除し、ファイル転送を許可してから、もう一度選択してください。".into(),
                ));
            }
            Ok((device, storages)) => {
                let session = Session {
                    location_id,
                    device,
                    storages,
                    current: 0,
                    path: Vec::new(),
                };
                (self.emit)(Event::Connected {
                    location_id,
                    storages: session.storage_summaries(),
                });
                self.session = Some(session);
                self.relist();
            }
            Err(e) => (self.emit)(Event::Error(describe_device_error(&e))),
        }
    }

    /// 未接続なら None を返す。
    fn with_session<T>(
        &mut self,
        action: impl FnOnce(&mut Session, &Emit) -> Result<T, OpError>,
    ) -> Option<T> {
        let session = self.session.as_mut()?;
        match action(session, &self.emit) {
            Ok(value) => Some(value),
            Err(e) => {
                self.report(e);
                None
            }
        }
    }

    /// 接続が失われるエラーならセッションも片付ける。
    fn report(&mut self, error: OpError) {
        let lost = matches!(
            &error,
            OpError::Device(e) if e.is_disconnected() || matches!(e, Error::DeviceReset | Error::NoDevice)
        );
        if matches!(&error, OpError::Device(Error::Cancelled)) {
            (self.emit)(Event::Done(describe(&error)));
        } else {
            (self.emit)(Event::Error(describe(&error)));
        }
        if lost {
            self.drop_session();
            self.refresh_devices();
        }
    }

    /// 現在のフォルダを読み直して通知する。成功したら true。
    fn relist(&mut self) -> bool {
        if self.session.is_none() {
            return false;
        }
        (self.emit)(Event::Busy("一覧を読み込んでいます…".into()));
        self.with_session(|session, emit| {
            let (entries, unreadable) = list_entries(session.storage(), session.parent())?;
            emit(Event::Listing {
                crumbs: session.path.iter().map(|(_, n)| n.clone()).collect(),
                entries,
                unreadable,
            });
            Ok(())
        })
        .is_some()
    }

    /// 内容を変更した後に、一覧と空き容量を最新にする。
    fn after_change(&mut self) {
        if let Some(session) = &mut self.session {
            let storage = &mut session.storages[session.current];
            if block_on(storage.refresh()).is_ok() {
                (self.emit)(Event::Storages(session.storage_summaries()));
            }
        }
        self.relist();
    }

    fn meter(&self, verb: &'static str) -> Meter {
        Meter::new(self.emit.clone(), self.cancel.clone(), verb)
    }

    fn transfer(&mut self, transfer: Transfer, policy: Option<Policy>) {
        if policy.is_none() {
            match self.find_conflicts(&transfer) {
                Some(names) if names.is_empty() => {}
                Some(names) => {
                    (self.emit)(Event::Conflict {
                        transfer,
                        message: format!(
                            "転送先に同じ名前のものが {} 件あります。\n{}",
                            names.len(),
                            summarize_names(&names)
                        ),
                    });
                    return;
                }
                None => return,
            }
        }
        // 同名がなければ下の階層でも衝突しないので、方針はどちらでも同じ
        let policy = policy.unwrap_or(Policy::Skip);
        match transfer {
            Transfer::Upload { paths } => self.upload(&paths, policy),
            Transfer::Download { entries, dest } => self.download(&entries, &dest, policy),
        }
    }

    /// 転送先の直下で名前が重複するものを返す。確認できなければ None。
    fn find_conflicts(&mut self, transfer: &Transfer) -> Option<Vec<String>> {
        match transfer {
            Transfer::Upload { paths } => {
                let names: Vec<&str> = paths
                    .iter()
                    .filter_map(|p| p.file_name()?.to_str())
                    .collect();
                self.with_session(|session, _| {
                    let (existing, _) = list_entries(session.storage(), session.parent())?;
                    Ok(existing
                        .into_iter()
                        .filter(|e| names.contains(&e.name.as_str()))
                        .map(|e| e.name)
                        .collect())
                })
            }
            Transfer::Download { entries, dest } => Some(
                entries
                    .iter()
                    .filter(|e| is_safe_local_name(&e.name))
                    .filter(|e| dest.join(&e.name).symlink_metadata().is_ok())
                    .map(|e| e.name.clone())
                    .collect(),
            ),
        }
    }

    fn emit_preparing(&self) {
        (self.emit)(Event::Progress {
            label: "転送の準備をしています…".into(),
            fraction: None,
        });
    }

    fn upload(&mut self, paths: &[PathBuf], policy: Policy) {
        self.emit_preparing();
        let mut meter = self.meter("PC → デバイス");
        let mut outcome = Outcome::default();
        let finished = self.with_session(|session, _| {
            let nodes = transfer::scan_local(paths, &mut outcome.skipped)?;
            transfer::upload(
                session.storage(),
                session.parent(),
                &nodes,
                policy,
                &mut meter,
                &mut outcome,
            )
        });
        if finished.is_some() {
            (self.emit)(Event::Done(outcome_message("デバイスへコピー", &outcome)));
        }
        // 失敗やキャンセルでも途中までは反映されているので読み直す
        self.after_change();
    }

    fn download(&mut self, entries: &[Entry], dest: &std::path::Path, policy: Policy) {
        self.emit_preparing();
        let mut meter = self.meter("デバイス → PC");
        let mut outcome = Outcome::default();
        let finished = self.with_session(|session, _| {
            let use_partial_read = session.device.capabilities().supports_partial_download;
            let nodes =
                transfer::scan_remote(session.storage(), entries, &meter, &mut outcome.skipped)?;
            transfer::download(
                session.storage(),
                &nodes,
                dest,
                policy,
                use_partial_read,
                &mut meter,
                &mut outcome,
            )
        });
        if finished.is_some() {
            (self.emit)(Event::Done(outcome_message(" PC へ保存", &outcome)));
        }
    }

    /// 同じ階層に同名のものがないか確かめる。MTP は重複した名前を許すデバイスがある。
    fn name_in_use(session: &Session, name: &str) -> Result<bool, OpError> {
        let (existing, _) = list_entries(session.storage(), session.parent())?;
        Ok(existing.iter().any(|e| e.name == name))
    }

    fn rename(&mut self, entry: Entry, new_name: &str) {
        let name = match validate_new_name(new_name) {
            Ok(name) => name,
            Err(message) => return (self.emit)(Event::Error(message)),
        };
        if name == entry.name {
            return;
        }
        let renamed = self.with_session(|session, _| {
            if Self::name_in_use(session, name)? {
                return Err(OpError::Conflict(format!(
                    "「{name}」は既に存在します。別の名前を指定してください。"
                )));
            }
            block_on(session.storage().rename(entry.handle, name))?;
            Ok(())
        });
        if renamed.is_some() {
            (self.emit)(Event::Done(format!("「{name}」に名前を変更しました。")));
            self.relist();
        }
    }

    fn create_folder(&mut self, new_name: &str) {
        let name = match validate_new_name(new_name) {
            Ok(name) => name,
            Err(message) => return (self.emit)(Event::Error(message)),
        };
        let created = self.with_session(|session, _| {
            if Self::name_in_use(session, name)? {
                return Err(OpError::Conflict(format!("「{name}」は既に存在します。")));
            }
            block_on(session.storage().create_folder(session.parent(), name))?;
            Ok(())
        });
        if created.is_some() {
            (self.emit)(Event::Done(format!("フォルダ「{name}」を作成しました。")));
            self.relist();
        }
    }

    fn delete(&mut self, entries: Vec<Entry>) {
        let mut meter = self.meter("削除中");
        let mut outcome = Outcome::default();
        let finished = self.with_session(|session, _| {
            transfer::delete_tree(session.storage(), &entries, &mut meter, &mut outcome)
        });
        if finished.is_some() {
            (self.emit)(Event::Done(format!("{} 件削除しました。", outcome.done)));
        }
        self.after_change();
    }
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
