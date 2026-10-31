//! Slint の画面とワーカーをつなぐ。状態はすべて UI スレッド上で持つ。

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use futures::channel::mpsc::{UnboundedSender, unbounded};
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};

use crate::names::{format_size, stem_end, summarize_names};
use crate::selection::Selection;
use crate::transfer::Policy;
use crate::worker::{
    self, Command, DeviceSummary, Entry, Event, StorageSummary, Transfer, WorkerHandle,
};

slint::include_modules!();

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

enum Message {
    Worker(Event),
    Picked(Picked),
}

/// ファイル選択ダイアログの結果。None はキャンセル。
enum Picked {
    Upload(Option<Vec<PathBuf>>),
    Download(Option<PathBuf>, Vec<Entry>),
}

#[derive(Default)]
struct State {
    devices: Vec<DeviceSummary>,
    connected_location: Option<u64>,
    storages: Vec<StorageSummary>,
    current_storage: usize,
    entries: Vec<Entry>,
    selection: Selection,
    worker_busy: bool,
    picker_open: bool,
    status: String,
    /// 完了通知。次の操作まで状態表示に残す。
    notice: String,
    /// 確認ダイアログの回答待ちの操作
    pending_rename: Option<Entry>,
    pending_delete: Vec<Entry>,
    pending_transfer: Option<Transfer>,
}

impl State {
    fn busy(&self) -> bool {
        self.worker_busy || self.picker_open
    }

    fn selected_entries(&self) -> Vec<Entry> {
        self.selection
            .indices()
            .into_iter()
            .filter_map(|i| self.entries.get(i).cloned())
            .collect()
    }
}

struct Ui {
    window: AppWindow,
    worker: WorkerHandle,
    messages: UnboundedSender<Message>,
    state: RefCell<State>,
    devices: Rc<VecModel<SidebarRow>>,
    storages: Rc<VecModel<SidebarRow>>,
    files: Rc<VecModel<FileRow>>,
    crumbs: Rc<VecModel<SharedString>>,
}

impl Ui {
    fn sync_devices(&self) {
        let state = self.state.borrow();
        let rows: Vec<SidebarRow> = state
            .devices
            .iter()
            .map(|d| SidebarRow {
                title: d.title.as_str().into(),
                subtitle: d.subtitle.as_str().into(),
                active: state.connected_location == Some(d.location_id),
            })
            .collect();
        self.devices.set_vec(rows);
    }

    fn sync_storages(&self) {
        let state = self.state.borrow();
        let rows: Vec<SidebarRow> = state
            .storages
            .iter()
            .enumerate()
            .map(|(i, s)| SidebarRow {
                title: s.title.as_str().into(),
                subtitle: s.subtitle.as_str().into(),
                active: i == state.current_storage,
            })
            .collect();
        self.storages.set_vec(rows);
    }

    fn sync_files(&self) {
        let state = self.state.borrow();
        let rows: Vec<FileRow> = state
            .entries
            .iter()
            .enumerate()
            .map(|(i, e)| FileRow {
                name: e.name.as_str().into(),
                size: if e.is_folder {
                    SharedString::new()
                } else {
                    format_size(e.size).into()
                },
                modified: e.modified.as_str().into(),
                is_folder: e.is_folder,
                selected: state.selection.is_selected(i),
            })
            .collect();
        self.files.set_vec(rows);
        drop(state);
        self.sync_selection();
    }

    /// 行全体を作り直さず、選択状態だけを反映する。
    fn sync_selection(&self) {
        let state = self.state.borrow();
        for i in 0..self.files.row_count() {
            let selected = state.selection.is_selected(i);
            if let Some(mut row) = self.files.row_data(i)
                && row.selected != selected
            {
                row.selected = selected;
                self.files.set_row_data(i, row);
            }
        }
        let count = state.selection.indices().len();
        self.window.set_selection_count(count as i32);
        self.window.set_single_selected(count == 1);
        drop(state);
        self.sync_status();
    }

    fn sync_status(&self) {
        let state = self.state.borrow();
        let text = if !state.status.is_empty() {
            state.status.clone()
        } else if !state.notice.is_empty() {
            state.notice.clone()
        } else if state.connected_location.is_none() {
            String::new()
        } else {
            let selected = state.selection.indices().len();
            if selected > 0 {
                format!("{} 項目（{selected} 件選択中）", state.entries.len())
            } else {
                format!("{} 項目", state.entries.len())
            }
        };
        self.window.set_status_text(text.into());
        self.window.set_busy(state.busy());

        let empty = if state.connected_location.is_none() {
            if state.devices.is_empty() {
                "MTP デバイスを USB で接続し、デバイス側で転送を許可してください。"
            } else {
                "左の一覧からデバイスを選択してください。"
            }
        } else if state.worker_busy {
            ""
        } else {
            "このフォルダは空です。"
        };
        self.window.set_empty_text(empty.into());
    }

    fn send(&self, command: Command) {
        // 応答を待つ間の連続操作を防ぐため、送った時点で操作不可にする
        {
            let mut state = self.state.borrow_mut();
            state.worker_busy = true;
            state.notice.clear();
        }
        self.window.set_error_text(SharedString::new());
        self.sync_status();
        self.worker.send(command);
    }

    fn handle_worker_event(&self, event: Event) {
        match event {
            Event::Devices(devices) => {
                self.state.borrow_mut().devices = devices;
                self.sync_devices();
                self.sync_status();
            }
            Event::Connected {
                location_id,
                storages,
            } => {
                {
                    let mut state = self.state.borrow_mut();
                    state.connected_location = Some(location_id);
                    state.storages = storages;
                    state.current_storage = 0;
                }
                self.window.set_connected(true);
                self.sync_devices();
                self.sync_storages();
            }
            Event::Disconnected => {
                {
                    let mut state = self.state.borrow_mut();
                    state.connected_location = None;
                    state.storages.clear();
                    state.entries.clear();
                    state.selection.reset(0);
                }
                self.window.set_connected(false);
                self.crumbs.set_vec(Vec::new());
                self.close_dialog();
                self.sync_devices();
                self.sync_storages();
                self.sync_files();
            }
            Event::StorageSelected(index) => {
                self.state.borrow_mut().current_storage = index;
                self.sync_storages();
            }
            Event::Storages(storages) => {
                self.state.borrow_mut().storages = storages;
                self.sync_storages();
            }
            Event::Listing {
                crumbs,
                entries,
                unreadable,
            } => {
                {
                    let mut state = self.state.borrow_mut();
                    state.selection.reset(entries.len());
                    state.entries = entries;
                }
                self.crumbs.set_vec(
                    crumbs
                        .into_iter()
                        .map(SharedString::from)
                        .collect::<Vec<_>>(),
                );
                self.sync_files();
                if unreadable > 0 {
                    self.window.set_error_text(
                        format!(
                            "{unreadable} 件の情報を読み取れなかったため、一覧に表示していません。"
                        )
                        .into(),
                    );
                }
            }
            Event::Busy(label) => {
                {
                    let mut state = self.state.borrow_mut();
                    state.worker_busy = true;
                    state.status = label;
                }
                self.sync_status();
            }
            Event::Idle => {
                {
                    let mut state = self.state.borrow_mut();
                    state.worker_busy = false;
                    state.status.clear();
                }
                self.window.set_transferring(false);
                self.sync_status();
            }
            Event::Progress { label, fraction } => {
                {
                    let mut state = self.state.borrow_mut();
                    state.worker_busy = true;
                    state.status = label;
                }
                self.window.set_transferring(true);
                self.window.set_progress_known(fraction.is_some());
                self.window.set_progress(fraction.unwrap_or(0.0));
                self.sync_status();
            }
            Event::Conflict { transfer, message } => {
                self.state.borrow_mut().pending_transfer = Some(transfer);
                self.window.set_dialog_title("同じ名前があります".into());
                self.window.set_dialog_message(message.into());
                self.window.set_dialog(DialogKind::Conflict);
            }
            Event::Done(message) => {
                self.state.borrow_mut().notice = message;
                self.sync_status();
            }
            Event::Error(message) => self.window.set_error_text(message.into()),
        }
    }

    fn handle_picked(&self, picked: Picked) {
        self.state.borrow_mut().picker_open = false;
        self.sync_status();
        match picked {
            Picked::Upload(Some(paths)) if !paths.is_empty() => self.send(Command::Transfer {
                transfer: Transfer::Upload { paths },
                policy: None,
            }),
            Picked::Download(Some(dest), entries) if !entries.is_empty() => {
                self.send(Command::Transfer {
                    transfer: Transfer::Download { entries, dest },
                    policy: None,
                });
            }
            _ => {}
        }
    }

    /// OS のファイル選択ダイアログは完了までブロックするので別スレッドで開く。
    fn open_picker(&self, pick: impl FnOnce() -> Picked + Send + 'static) {
        {
            let mut state = self.state.borrow_mut();
            if state.busy() {
                return;
            }
            state.picker_open = true;
        }
        self.sync_status();
        let messages = self.messages.clone();
        thread::spawn(move || {
            let _ = messages.unbounded_send(Message::Picked(pick()));
        });
    }

    fn close_dialog(&self) {
        {
            let mut state = self.state.borrow_mut();
            state.pending_rename = None;
            state.pending_delete.clear();
            state.pending_transfer = None;
        }
        self.window.set_dialog(DialogKind::None);
        self.window.invoke_focus_list();
    }

    fn activate(&self, index: usize) {
        let entry = {
            let state = self.state.borrow();
            if state.busy() {
                return;
            }
            state.entries.get(index).cloned()
        };
        if let Some(entry) = entry.filter(|e| e.is_folder) {
            self.send(Command::Enter {
                handle: entry.handle,
                name: entry.name,
            });
        }
    }

    fn open_rename(&self) {
        let entry = {
            let state = self.state.borrow();
            let selected = state.selected_entries();
            if state.busy() || selected.len() != 1 {
                return;
            }
            selected.into_iter().next()
        };
        let Some(entry) = entry else { return };
        self.window.set_dialog_title("名前の変更".into());
        self.window.set_dialog_input(entry.name.as_str().into());
        self.window
            .set_dialog_select_end(stem_end(&entry.name, entry.is_folder) as i32);
        self.state.borrow_mut().pending_rename = Some(entry);
        self.window.set_dialog(DialogKind::Rename);
    }

    fn open_new_folder(&self) {
        {
            let state = self.state.borrow();
            if state.busy() || state.connected_location.is_none() {
                return;
            }
        }
        let default_name = "新しいフォルダ";
        self.window.set_dialog_title("新規フォルダ".into());
        self.window.set_dialog_input(default_name.into());
        self.window.set_dialog_select_end(default_name.len() as i32);
        self.window.set_dialog(DialogKind::NewFolder);
    }

    fn open_delete(&self) {
        let selected = {
            let state = self.state.borrow();
            if state.busy() {
                return;
            }
            state.selected_entries()
        };
        if selected.is_empty() {
            return;
        }
        let names: Vec<String> = selected.iter().map(|e| e.name.clone()).collect();
        let folder_note = if selected.iter().any(|e| e.is_folder) {
            "\nフォルダは中身ごと削除されます。"
        } else {
            ""
        };
        self.window
            .set_dialog_title(format!("{} 件を削除しますか？", selected.len()).into());
        self.window.set_dialog_message(
            format!(
                "{}\n\nこの操作は元に戻せません。{folder_note}",
                summarize_names(&names)
            )
            .into(),
        );
        self.state.borrow_mut().pending_delete = selected;
        self.window.set_dialog(DialogKind::ConfirmDelete);
    }

    fn dialog_submit(&self, text: String) {
        let kind = self.window.get_dialog();
        let rename = self.state.borrow_mut().pending_rename.take();
        self.close_dialog();
        match (kind, rename) {
            (DialogKind::Rename, Some(entry)) => self.send(Command::Rename {
                entry,
                new_name: text,
            }),
            (DialogKind::NewFolder, _) => self.send(Command::CreateFolder(text)),
            _ => {}
        }
    }
}

#[cfg(feature = "virtual-device")]
fn register_virtual_device_from_env() {
    use mtp_rs::{VirtualDeviceConfig, VirtualStorageConfig, register_virtual_device};
    let Some(dir) = std::env::var_os("MTP_FILES_VIRTUAL_DIR") else {
        return;
    };
    let _ = register_virtual_device(&VirtualDeviceConfig {
        manufacturer: "Virtual".into(),
        model: "仮想ウォッチ".into(),
        serial: "virtual-001".into(),
        storages: vec![VirtualStorageConfig {
            description: "内部ストレージ".into(),
            capacity: 8 * 1024 * 1024 * 1024,
            backing_dir: PathBuf::from(dir),
            read_only: false,
        }],
        ..Default::default()
    });
}

pub fn run() -> Result<(), slint::PlatformError> {
    #[cfg(feature = "virtual-device")]
    register_virtual_device_from_env();

    let window = AppWindow::new()?;
    let (messages, mut incoming) = unbounded();

    let emit_messages = messages.clone();
    let worker = worker::spawn(Arc::new(move |event| {
        let _ = emit_messages.unbounded_send(Message::Worker(event));
    }));
    worker::spawn_hotplug(&worker);

    let devices = Rc::new(VecModel::default());
    let storages = Rc::new(VecModel::default());
    let files = Rc::new(VecModel::default());
    let crumbs = Rc::new(VecModel::default());
    window.set_devices(ModelRc::from(devices.clone()));
    window.set_storages(ModelRc::from(storages.clone()));
    window.set_files(ModelRc::from(files.clone()));
    window.set_crumbs(ModelRc::from(crumbs.clone()));

    let ui = Rc::new(Ui {
        window: window.clone_strong(),
        worker,
        messages,
        state: RefCell::new(State::default()),
        devices,
        storages,
        files,
        crumbs,
    });
    connect_callbacks(&ui);
    ui.sync_status();
    ui.window.invoke_focus_list();
    ui.send(Command::RefreshDevices);

    let receiver = ui.clone();
    slint::spawn_local(async move {
        while let Some(message) = incoming.next().await {
            match message {
                Message::Worker(event) => receiver.handle_worker_event(event),
                Message::Picked(picked) => receiver.handle_picked(picked),
            }
        }
    })
    .expect("イベントループの開始前に呼んでいるので失敗しない");

    window.run()?;

    // 転送中に閉じられても、一時ファイルの片付けとセッションの終了を済ませる
    ui.worker.shutdown(SHUTDOWN_TIMEOUT);
    Ok(())
}

fn connect_callbacks(ui: &Rc<Ui>) {
    let window = &ui.window;

    macro_rules! on {
        ($setter:ident, |$ui:ident $(, $arg:ident)*| $body:expr) => {{
            let weak = Rc::downgrade(ui);
            window.$setter(move |$($arg),*| {
                if let Some($ui) = weak.upgrade() {
                    $body
                }
            });
        }};
    }

    on!(on_refresh_devices, |ui| ui.send(Command::RefreshDevices));
    on!(on_device_clicked, |ui, index| {
        let target = {
            let state = ui.state.borrow();
            state
                .devices
                .get(index as usize)
                .map(|d| d.location_id)
                .filter(|id| !state.busy() && state.connected_location != Some(*id))
        };
        if let Some(location_id) = target {
            ui.send(Command::Connect(location_id));
        }
    });
    on!(on_storage_clicked, |ui, index| {
        if ui.state.borrow().current_storage != index as usize {
            ui.send(Command::SelectStorage(index as usize));
        }
    });
    on!(on_row_pressed, |ui, index, ctrl, shift| {
        {
            let mut state = ui.state.borrow_mut();
            state.selection.click(index as usize, ctrl, shift);
            state.notice.clear();
        }
        ui.sync_selection();
    });
    on!(on_row_activated, |ui, index| ui.activate(index as usize));
    on!(on_activate_selection, |ui| {
        let only = {
            let indices = ui.state.borrow().selection.indices();
            (indices.len() == 1).then(|| indices[0])
        };
        if let Some(index) = only {
            ui.activate(index);
        }
    });
    on!(on_crumb_clicked, |ui, depth| {
        ui.send(Command::GoToDepth(depth as usize));
    });
    on!(on_go_up, |ui| {
        let depth = ui.crumbs.row_count();
        if depth > 0 && !ui.state.borrow().busy() {
            ui.send(Command::GoToDepth(depth - 1));
        }
    });
    on!(on_refresh, |ui| ui.send(Command::Refresh));
    on!(on_upload_files, |ui| {
        ui.open_picker(|| Picked::Upload(rfd::FileDialog::new().pick_files()));
    });
    on!(on_upload_folder, |ui| {
        ui.open_picker(|| {
            Picked::Upload(rfd::FileDialog::new().pick_folder().map(|dir| vec![dir]))
        });
    });
    on!(on_download, |ui| {
        let entries = ui.state.borrow().selected_entries();
        if !entries.is_empty() {
            ui.open_picker(move || Picked::Download(rfd::FileDialog::new().pick_folder(), entries));
        }
    });
    on!(on_open_rename, |ui| ui.open_rename());
    on!(on_open_new_folder, |ui| ui.open_new_folder());
    on!(on_open_delete, |ui| ui.open_delete());
    on!(on_select_all, |ui| {
        ui.state.borrow_mut().selection.select_all();
        ui.sync_selection();
    });
    on!(on_clear_selection, |ui| {
        ui.state.borrow_mut().selection.clear();
        ui.sync_selection();
    });
    on!(on_dialog_submit, |ui, text| ui.dialog_submit(text.into()));
    on!(on_delete_confirmed, |ui| {
        let entries = std::mem::take(&mut ui.state.borrow_mut().pending_delete);
        ui.close_dialog();
        ui.send(Command::Delete(entries));
    });
    on!(on_conflict_resolved, |ui, overwrite| {
        let transfer = ui.state.borrow_mut().pending_transfer.take();
        ui.close_dialog();
        if let Some(transfer) = transfer {
            ui.send(Command::Transfer {
                transfer,
                policy: Some(if overwrite {
                    Policy::Overwrite
                } else {
                    Policy::Skip
                }),
            });
        }
    });
    on!(on_dialog_closed, |ui| ui.close_dialog());
    on!(on_cancel_operation, |ui| ui.worker.cancel());
    on!(on_dismiss_error, |ui| ui
        .window
        .set_error_text(SharedString::new()));
}
