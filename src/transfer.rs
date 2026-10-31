//! ファイル転送と再帰削除。専用のワーカースレッドから同期的に呼び出す。

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::executor::block_on;
use mtp_rs::mtp::{DEFAULT_CANCEL_TIMEOUT, Storage};
use mtp_rs::{ByteRange, Error, NewObjectInfo, ObjectHandle};

use crate::names::is_safe_local_name;
use crate::worker::{Emit, Entry, Event, list_entries};

const UPLOAD_CHUNK: usize = 1024 * 1024;
/// 進捗表示とキャンセルの反応を保つため、1 回の読み出しは小さめにする。
const DOWNLOAD_WINDOW: u32 = 1024 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// 同名のファイルが転送先にあるときの扱い。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Overwrite,
    Skip,
}

#[derive(Debug)]
pub enum OpError {
    Device(Error),
    Local { path: PathBuf, source: io::Error },
    Conflict(String),
}

impl From<Error> for OpError {
    fn from(e: Error) -> Self {
        OpError::Device(e)
    }
}

fn local(path: &Path) -> impl FnOnce(io::Error) -> OpError + use<> {
    let path = path.to_path_buf();
    move |source| OpError::Local { path, source }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    pub done: usize,
    pub skipped: usize,
}

/// (ファイル数, バイト数) を合計する。
fn sum_totals(totals: impl Iterator<Item = (usize, u64)>) -> (usize, u64) {
    totals.fold((0, 0), |acc, t| (acc.0 + t.0, acc.1 + t.1))
}

/// バッチ全体の進捗を数え、UI へ間引いて通知する。
pub struct Meter {
    emit: Emit,
    cancel: Arc<AtomicBool>,
    verb: &'static str,
    bytes_total: u64,
    bytes_done: u64,
    files_total: usize,
    files_done: usize,
    last_emit: Option<Instant>,
}

impl Meter {
    pub fn new(emit: Emit, cancel: Arc<AtomicBool>, verb: &'static str) -> Self {
        Self {
            emit,
            cancel,
            verb,
            bytes_total: 0,
            bytes_done: 0,
            files_total: 0,
            files_done: 0,
            last_emit: None,
        }
    }

    fn plan(&mut self, files: usize, bytes: u64) {
        self.files_total = files;
        self.bytes_total = bytes;
    }

    fn check_cancel(&self) -> Result<(), Error> {
        if self.cancel.load(Ordering::Relaxed) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    fn show(&mut self, name: &str, current: u64) {
        let throttled = self
            .last_emit
            .is_some_and(|t| t.elapsed() < PROGRESS_INTERVAL);
        if throttled {
            return;
        }
        self.last_emit = Some(Instant::now());
        let fraction = if self.bytes_total > 0 {
            Some((self.bytes_done + current) as f32 / self.bytes_total as f32)
        } else if self.files_total > 0 {
            Some(self.files_done as f32 / self.files_total as f32)
        } else {
            None
        };
        let label = if self.files_total > 0 {
            let position = (self.files_done + 1).min(self.files_total);
            format!("{} {position}/{}: {name}", self.verb, self.files_total)
        } else {
            format!("{}: {name}", self.verb)
        };
        (self.emit)(Event::Progress {
            label,
            fraction: fraction.map(|f| f.clamp(0.0, 1.0)),
        });
    }

    fn finish_file(&mut self, size: u64) {
        self.bytes_done += size;
        self.files_done += 1;
    }
}

// ---------------------------------------------------------------- PC → デバイス

#[derive(Debug)]
pub enum LocalNode {
    File {
        path: PathBuf,
        name: String,
        size: u64,
    },
    Dir {
        name: String,
        children: Vec<LocalNode>,
    },
}

impl LocalNode {
    fn name(&self) -> &str {
        match self {
            LocalNode::File { name, .. } | LocalNode::Dir { name, .. } => name,
        }
    }

    fn totals(&self) -> (usize, u64) {
        match self {
            LocalNode::File { size, .. } => (1, *size),
            LocalNode::Dir { children, .. } => sum_totals(children.iter().map(|c| c.totals())),
        }
    }
}

/// シンボリックリンク（循環の恐れがある）と UTF-8 でない名前は対象外とし、件数を数える。
pub fn scan_local(paths: &[PathBuf], skipped: &mut usize) -> Result<Vec<LocalNode>, OpError> {
    let mut nodes = Vec::new();
    for path in paths {
        let meta = fs::symlink_metadata(path).map_err(local(path))?;
        let name = path.file_name().and_then(|n| n.to_str());
        let Some(name) = name.filter(|_| !meta.file_type().is_symlink()) else {
            *skipped += 1;
            continue;
        };
        if meta.is_dir() {
            let mut child_paths = fs::read_dir(path)
                .map_err(local(path))?
                .map(|e| e.map(|e| e.path()))
                .collect::<io::Result<Vec<_>>>()
                .map_err(local(path))?;
            child_paths.sort();
            nodes.push(LocalNode::Dir {
                name: name.to_owned(),
                children: scan_local(&child_paths, skipped)?,
            });
        } else if meta.is_file() {
            nodes.push(LocalNode::File {
                path: path.clone(),
                name: name.to_owned(),
                size: meta.len(),
            });
        } else {
            *skipped += 1;
        }
    }
    Ok(nodes)
}

pub fn upload(
    storage: &Storage,
    parent: Option<ObjectHandle>,
    nodes: &[LocalNode],
    policy: Policy,
    meter: &mut Meter,
    outcome: &mut Outcome,
) -> Result<(), OpError> {
    let (files, bytes) = sum_totals(nodes.iter().map(|n| n.totals()));
    meter.plan(files, bytes);
    let existing = list_entries(storage, parent)?.0;
    upload_level(storage, parent, nodes, &existing, policy, meter, outcome)
}

fn upload_level(
    storage: &Storage,
    parent: Option<ObjectHandle>,
    nodes: &[LocalNode],
    existing: &[Entry],
    policy: Policy,
    meter: &mut Meter,
    outcome: &mut Outcome,
) -> Result<(), OpError> {
    for node in nodes {
        meter.check_cancel()?;
        let found = existing.iter().find(|e| e.name == node.name());
        match node {
            LocalNode::Dir { name, children } => {
                let (folder, listing) = match found {
                    Some(e) if e.is_folder => (e.handle, list_entries(storage, Some(e.handle))?.0),
                    Some(_) if policy == Policy::Skip => {
                        let (files, bytes) = node.totals();
                        outcome.skipped += files.max(1);
                        meter.bytes_done += bytes;
                        meter.files_done += files;
                        continue;
                    }
                    Some(_) => {
                        return Err(OpError::Conflict(format!(
                            "「{name}」は同名のファイルがあるためフォルダを作れません。"
                        )));
                    }
                    None => (block_on(storage.create_folder(parent, name))?, Vec::new()),
                };
                upload_level(
                    storage,
                    Some(folder),
                    children,
                    &listing,
                    policy,
                    meter,
                    outcome,
                )?;
            }
            LocalNode::File { path, name, size } => {
                match found {
                    Some(_) if policy == Policy::Skip => {
                        outcome.skipped += 1;
                        meter.finish_file(*size);
                        continue;
                    }
                    Some(e) if e.is_folder => {
                        return Err(OpError::Conflict(format!(
                            "「{name}」は同名のフォルダがあるため上書きできません。"
                        )));
                    }
                    // MTP に上書きの操作はないので、削除してから作り直す
                    Some(e) => block_on(storage.delete(e.handle))?,
                    None => {}
                }
                upload_file(storage, parent, path, name, *size, meter)?;
                meter.finish_file(*size);
                outcome.done += 1;
            }
        }
    }
    Ok(())
}

struct ChunkReader {
    file: File,
}

impl Iterator for ChunkReader {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0; UPLOAD_CHUNK];
        match self.file.read(&mut buffer) {
            Ok(0) => None,
            Ok(n) => {
                buffer.truncate(n);
                Some(Ok(Bytes::from(buffer)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

fn upload_file(
    storage: &Storage,
    parent: Option<ObjectHandle>,
    path: &Path,
    name: &str,
    size: u64,
    meter: &mut Meter,
) -> Result<(), OpError> {
    let file = File::open(path).map_err(local(path))?;
    meter.show(name, 0);
    let data = futures::stream::iter(ChunkReader { file });
    let result = block_on(storage.upload_with_progress(
        parent,
        NewObjectInfo::file(name, size),
        data,
        |progress| {
            if meter.check_cancel().is_err() {
                return ControlFlow::Break(());
            }
            meter.show(name, progress.bytes_transferred);
            ControlFlow::Continue(())
        },
    ));
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            // 中途半端なファイルが時計に残らないようにする
            if let Some(partial) = e.partial {
                let _ = block_on(storage.delete(partial));
            }
            Err(e.source.into())
        }
    }
}

// ---------------------------------------------------------------- デバイス → PC

pub enum RemoteNode {
    File(Entry),
    Dir {
        entry: Entry,
        children: Vec<RemoteNode>,
    },
}

impl RemoteNode {
    fn totals(&self) -> (usize, u64) {
        match self {
            RemoteNode::File(e) => (1, e.size),
            RemoteNode::Dir { children, .. } => sum_totals(children.iter().map(|c| c.totals())),
        }
    }
}

/// 名前が危険なものと読み取れなかったものは計画に入れず、件数だけ数える。
pub fn scan_remote(
    storage: &Storage,
    entries: &[Entry],
    meter: &Meter,
    skipped: &mut usize,
) -> Result<Vec<RemoteNode>, OpError> {
    let mut nodes = Vec::new();
    for entry in entries {
        meter.check_cancel()?;
        if !is_safe_local_name(&entry.name) {
            *skipped += 1;
            continue;
        }
        if entry.is_folder {
            let (listing, unreadable) = list_entries(storage, Some(entry.handle))?;
            *skipped += unreadable;
            nodes.push(RemoteNode::Dir {
                entry: entry.clone(),
                children: scan_remote(storage, &listing, meter, skipped)?,
            });
        } else {
            nodes.push(RemoteNode::File(entry.clone()));
        }
    }
    Ok(nodes)
}

pub fn download(
    storage: &Storage,
    nodes: &[RemoteNode],
    dest: &Path,
    policy: Policy,
    use_partial_read: bool,
    meter: &mut Meter,
    outcome: &mut Outcome,
) -> Result<(), OpError> {
    let (files, bytes) = sum_totals(nodes.iter().map(|n| n.totals()));
    meter.plan(files, bytes);
    download_level(
        storage,
        nodes,
        dest,
        policy,
        use_partial_read,
        meter,
        outcome,
    )
}

fn download_level(
    storage: &Storage,
    nodes: &[RemoteNode],
    dest: &Path,
    policy: Policy,
    use_partial_read: bool,
    meter: &mut Meter,
    outcome: &mut Outcome,
) -> Result<(), OpError> {
    for node in nodes {
        meter.check_cancel()?;
        match node {
            RemoteNode::Dir { entry, children } => {
                let target = dest.join(&entry.name);
                match fs::create_dir(&target) {
                    Err(e) if e.kind() != io::ErrorKind::AlreadyExists => {
                        return Err(local(&target)(e));
                    }
                    _ => {}
                }
                download_level(
                    storage,
                    children,
                    &target,
                    policy,
                    use_partial_read,
                    meter,
                    outcome,
                )?;
            }
            RemoteNode::File(entry) => {
                let target = dest.join(&entry.name);
                if policy == Policy::Skip && target.symlink_metadata().is_ok() {
                    outcome.skipped += 1;
                    meter.finish_file(entry.size);
                    continue;
                }
                download_file(storage, entry, &target, use_partial_read, meter)?;
                meter.finish_file(entry.size);
                outcome.done += 1;
            }
        }
    }
    Ok(())
}

/// 一時ファイルへ書き切ってから rename する。失敗やキャンセルで既存ファイルを壊さない。
fn download_file(
    storage: &Storage,
    entry: &Entry,
    target: &Path,
    use_partial_read: bool,
    meter: &mut Meter,
) -> Result<(), OpError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp = target.with_file_name(format!(".mtp-files-{}-{unique}.part", std::process::id()));
    let mut file = File::create_new(&temp).map_err(local(&temp))?;
    meter.show(&entry.name, 0);
    let written = if use_partial_read {
        read_windowed(storage, entry, &mut file, &temp, meter)
    } else {
        read_stream(storage, entry, &mut file, &temp, meter)
    };
    let committed = written
        .and_then(|()| file.sync_all().map_err(local(&temp)))
        .and_then(|()| fs::rename(&temp, target).map_err(local(target)));
    if committed.is_err() {
        let _ = fs::remove_file(&temp);
    }
    committed
}

/// 読み出しの合間にセッションが空くので、キャンセルしてもデバイスが固まりにくい。
fn read_windowed(
    storage: &Storage,
    entry: &Entry,
    file: &mut File,
    temp: &Path,
    meter: &mut Meter,
) -> Result<(), OpError> {
    let mut download =
        block_on(storage.download_windowed(entry.handle, ByteRange::Full, DOWNLOAD_WINDOW))?;
    let mut received = 0;
    while let Some(window) = block_on(download.next_window()) {
        let window = window?;
        file.write_all(&window).map_err(local(temp))?;
        received += window.len() as u64;
        meter.show(&entry.name, received);
        meter.check_cancel()?;
    }
    Ok(())
}

/// 部分読み出しに対応しないデバイス向け。途中でやめるときは cancel が必須。
fn read_stream(
    storage: &Storage,
    entry: &Entry,
    file: &mut File,
    temp: &Path,
    meter: &mut Meter,
) -> Result<(), OpError> {
    let mut download = block_on(storage.download(entry.handle, ByteRange::Full))?;
    while let Some(chunk) = block_on(download.next_chunk()) {
        let chunk = chunk?;
        let stop = match file.write_all(&chunk) {
            Err(e) => Some(local(temp)(e)),
            Ok(()) => {
                meter.show(&entry.name, download.bytes_received());
                meter.check_cancel().err().map(OpError::from)
            }
        };
        if let Some(reason) = stop {
            let _ = block_on(download.cancel(DEFAULT_CANCEL_TIMEOUT));
            return Err(reason);
        }
    }
    if download.bytes_received() != download.size() {
        return Err(OpError::Device(Error::invalid_data(format!(
            "「{}」の受信サイズが一致しません（{} / {} バイト）",
            entry.name,
            download.bytes_received(),
            download.size()
        ))));
    }
    Ok(())
}

// ---------------------------------------------------------------- 削除

/// デバイスによってはフォルダの削除で中身を消さないので、子から順に消す。
pub fn delete_tree(
    storage: &Storage,
    entries: &[Entry],
    meter: &mut Meter,
    outcome: &mut Outcome,
) -> Result<(), OpError> {
    for entry in entries {
        meter.check_cancel()?;
        if entry.is_folder {
            let children = list_entries(storage, Some(entry.handle))?.0;
            delete_tree(storage, &children, meter, outcome)?;
        }
        meter.show(&entry.name, 0);
        block_on(storage.delete(entry.handle))?;
        outcome.done += 1;
    }
    Ok(())
}
