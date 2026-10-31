//! 実機なしで、仮想 MTP デバイスに対してワーカーの動作を一通り確認する。

use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use mtp_rs::{
    VirtualDeviceConfig, VirtualStorageConfig, register_virtual_device, unregister_virtual_device,
};
use tempfile::TempDir;

use super::*;

const WAIT: Duration = Duration::from_secs(20);

struct Harness {
    worker: Option<WorkerHandle>,
    events: Mutex<Receiver<Event>>,
    device_dir: TempDir,
    pc_dir: TempDir,
    location_id: u64,
    hold_on_progress: Arc<AtomicBool>,
}

impl Harness {
    fn new() -> Self {
        let device_dir = TempDir::new().unwrap();
        let pc_dir = TempDir::new().unwrap();
        let config = VirtualDeviceConfig {
            manufacturer: "Test".into(),
            model: "仮想ウォッチ".into(),
            serial: format!("test-{}", device_dir.path().display()),
            storages: vec![VirtualStorageConfig {
                description: "内部ストレージ".into(),
                capacity: 64 * 1024 * 1024,
                backing_dir: device_dir.path().to_path_buf(),
                read_only: false,
            }],
            // テストが直接置いたファイルの検知と競合して、ハンドルが変わるのを防ぐ
            watch_backing_dirs: false,
            ..Default::default()
        };
        let location_id = register_virtual_device(&config).location_id;
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let hold_on_progress = Arc::new(AtomicBool::new(false));
        let cancel_flag = Arc::new(OnceLock::<Arc<AtomicBool>>::new());
        let worker = spawn(Arc::new({
            let hold = hold_on_progress.clone();
            let cancel_flag = cancel_flag.clone();
            move |event| {
                let transferring = matches!(
                    event,
                    Event::Progress {
                        fraction: Some(_),
                        ..
                    }
                );
                let _ = tx.lock().unwrap().send(event);
                if transferring && hold.load(Ordering::Relaxed) {
                    wait_until_set(cancel_flag.get().expect("起動時に設定済み"));
                }
            }
        }));
        let _ = cancel_flag.set(worker.cancel.clone());
        Self {
            worker: Some(worker),
            events: Mutex::new(rx),
            device_dir,
            pc_dir,
            location_id,
            hold_on_progress,
        }
    }

    /// 転送が始まったらキャンセルされるまでワーカーを待たせ、完了との競合をなくす。
    fn hold_transfers_until_cancelled(&self) {
        self.hold_on_progress.store(true, Ordering::Relaxed);
    }

    fn send(&self, command: Command) {
        self.worker.as_ref().unwrap().send(command);
    }

    /// コマンド 1 つを実行し、Idle までのイベントを集める。
    fn run(&self, command: Command) -> Vec<Event> {
        self.send(command);
        let events = self.events.lock().unwrap();
        let deadline = Instant::now() + WAIT;
        let mut collected = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match events.recv_timeout(left).expect("ワーカーが応答しない") {
                Event::Idle => return collected,
                event => collected.push(event),
            }
        }
    }

    fn connect(&self) -> Vec<Entry> {
        let events = self.run(Command::Connect(self.location_id));
        assert_no_error(&events);
        listing(&events)
    }

    fn device_path(&self, relative: &str) -> std::path::PathBuf {
        self.device_dir.path().join(relative)
    }

    fn pc_path(&self, relative: &str) -> std::path::PathBuf {
        self.pc_dir.path().join(relative)
    }

    fn list(&self) -> Vec<Entry> {
        let events = self.run(Command::Refresh);
        assert_no_error(&events);
        listing(&events)
    }

    fn upload(&self, paths: Vec<std::path::PathBuf>, policy: Option<Policy>) -> Vec<Event> {
        self.run(Command::Transfer {
            transfer: Transfer::Upload { paths },
            policy,
        })
    }

    fn download(&self, entries: Vec<Entry>, policy: Option<Policy>) -> Vec<Event> {
        self.run(Command::Transfer {
            transfer: Transfer::Download {
                entries,
                dest: self.pc_dir.path().to_path_buf(),
            },
            policy,
        })
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown(Duration::from_secs(5));
        }
        unregister_virtual_device(self.location_id);
    }
}

fn wait_until_set(flag: &AtomicBool) {
    let deadline = Instant::now() + WAIT;
    while !flag.load(Ordering::Relaxed) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn assert_no_error(events: &[Event]) {
    for event in events {
        if let Event::Error(message) = event {
            panic!("予期しないエラー: {message}");
        }
    }
}

fn listing(events: &[Event]) -> Vec<Entry> {
    events
        .iter()
        .rev()
        .find_map(|e| match e {
            Event::Listing { entries, .. } => Some(entries.clone()),
            _ => None,
        })
        .expect("一覧のイベントがない")
}

fn names(entries: &[Entry]) -> Vec<&str> {
    entries.iter().map(|e| e.name.as_str()).collect()
}

fn find<'a>(entries: &'a [Entry], name: &str) -> &'a Entry {
    entries
        .iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("{name} がない: {:?}", names(entries)))
}

fn has_conflict(events: &[Event]) -> bool {
    events.iter().any(|e| matches!(e, Event::Conflict { .. }))
}

fn write(path: &Path, content: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

#[test]
fn lists_japanese_names_with_folders_first() {
    let h = Harness::new();
    write(&h.device_path("走行記録.fit"), b"x");
    write(&h.device_path("音楽/曲.mp3"), b"y");
    assert_eq!(names(&h.connect()), ["音楽", "走行記録.fit"]);
}

#[test]
fn navigates_into_folders_and_back() {
    let h = Harness::new();
    write(&h.device_path("音楽/アルバム/曲.mp3"), b"y");
    let root = h.connect();

    let inside = h.run(Command::Enter {
        handle: find(&root, "音楽").handle,
        name: "音楽".into(),
    });
    assert_eq!(names(&listing(&inside)), ["アルバム"]);
    assert!(inside.iter().any(|e| matches!(
        e,
        Event::Listing { crumbs, .. } if crumbs == &["音楽".to_string()]
    )));

    let back = h.run(Command::GoToDepth(0));
    assert_eq!(names(&listing(&back)), ["音楽"]);
}

#[test]
fn uploads_files_and_nested_folders_with_japanese_names() {
    let h = Harness::new();
    h.connect();
    let content = vec![7u8; 3 * 1024 * 1024 + 11];
    write(&h.pc_path("コース.fit"), &content);
    write(
        &h.pc_path("フォルダ/下の階層/メモ.txt"),
        "こんにちは".as_bytes(),
    );

    let events = h.upload(vec![h.pc_path("コース.fit"), h.pc_path("フォルダ")], None);

    assert_no_error(&events);
    assert!(!has_conflict(&events));
    assert_eq!(fs::read(h.device_path("コース.fit")).unwrap(), content);
    assert_eq!(
        fs::read_to_string(h.device_path("フォルダ/下の階層/メモ.txt")).unwrap(),
        "こんにちは"
    );
}

#[test]
fn upload_conflict_asks_then_overwrites_or_skips() {
    let h = Harness::new();
    write(&h.device_path("a.txt"), b"old");
    h.connect();
    write(&h.pc_path("a.txt"), b"new");
    let paths = vec![h.pc_path("a.txt")];

    let asked = h.upload(paths.clone(), None);
    assert!(has_conflict(&asked));
    assert_eq!(fs::read(h.device_path("a.txt")).unwrap(), b"old");

    assert_no_error(&h.upload(paths.clone(), Some(Policy::Skip)));
    assert_eq!(fs::read(h.device_path("a.txt")).unwrap(), b"old");

    assert_no_error(&h.upload(paths, Some(Policy::Overwrite)));
    assert_eq!(fs::read(h.device_path("a.txt")).unwrap(), b"new");
    assert_eq!(names(&h.list()), ["a.txt"]);
}

#[test]
fn upload_merges_into_existing_folder() {
    let h = Harness::new();
    write(&h.device_path("d/keep.txt"), b"keep");
    write(&h.device_path("d/same.txt"), b"old");
    h.connect();
    write(&h.pc_path("d/same.txt"), b"new");
    write(&h.pc_path("d/added.txt"), b"added");

    assert_no_error(&h.upload(vec![h.pc_path("d")], Some(Policy::Skip)));

    assert_eq!(fs::read(h.device_path("d/keep.txt")).unwrap(), b"keep");
    assert_eq!(fs::read(h.device_path("d/same.txt")).unwrap(), b"old");
    assert_eq!(fs::read(h.device_path("d/added.txt")).unwrap(), b"added");
}

#[cfg(unix)]
#[test]
fn upload_skips_symlinks_so_cycles_cannot_loop() {
    let h = Harness::new();
    h.connect();
    write(&h.pc_path("d/real.txt"), b"x");
    std::os::unix::fs::symlink(h.pc_path("d"), h.pc_path("d/loop")).unwrap();

    let events = h.upload(vec![h.pc_path("d")], None);

    assert_no_error(&events);
    assert!(h.device_path("d/real.txt").exists());
    assert!(!h.device_path("d/loop").exists());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Done(m) if m.contains("スキップ 1 件")))
    );
}

#[test]
fn downloads_recursively_using_partial_reads() {
    let h = Harness::new();
    let content = vec![9u8; 2 * 1024 * 1024 + 5];
    write(&h.device_path("記録/走行.fit"), &content);
    write(&h.device_path("単体.txt"), b"solo");
    write(&h.device_path("空.txt"), b"");
    let root = h.connect();

    let events = h.download(root, None);

    assert_no_error(&events);
    assert_eq!(fs::read(h.pc_path("記録/走行.fit")).unwrap(), content);
    assert_eq!(fs::read(h.pc_path("単体.txt")).unwrap(), b"solo");
    assert_eq!(fs::read(h.pc_path("空.txt")).unwrap(), b"");
    assert_eq!(
        temp_files_in(h.pc_dir.path()),
        0,
        "一時ファイルが残っている"
    );
}

/// 部分読み出しのないデバイス向けの経路は、仮想デバイスでは機能フラグから選ばれないので直接呼ぶ。
fn stream_download(cancel_on_first_progress: bool) -> (TempDir, TempDir, Result<Outcome, OpError>) {
    let device_dir = TempDir::new().unwrap();
    let pc_dir = TempDir::new().unwrap();
    write(
        &device_dir.path().join("記録/走行.fit"),
        &vec![9u8; 2 * 1024 * 1024 + 5],
    );
    write(&pc_dir.path().join("記録/走行.fit"), b"original");
    let device = block_on(MtpDevice::builder().open_virtual(VirtualDeviceConfig {
        storages: vec![VirtualStorageConfig {
            description: "内部".into(),
            capacity: 64 * 1024 * 1024,
            backing_dir: device_dir.path().to_path_buf(),
            read_only: false,
        }],
        ..Default::default()
    }))
    .unwrap();
    let storages = block_on(device.storages()).unwrap();
    let storage = &storages[0];

    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    let emit: Emit = Arc::new(move |_| {
        if cancel_on_first_progress {
            flag.store(true, Ordering::Relaxed);
        }
    });
    let mut meter = Meter::new(emit, cancel, "テスト");
    let mut outcome = Outcome::default();
    let mut skipped = 0;
    let root = list_entries(storage, None).unwrap().0;
    let nodes = transfer::scan_remote(storage, &root, &meter, &mut skipped).unwrap();
    let result = transfer::download(
        storage,
        &nodes,
        pc_dir.path(),
        Policy::Overwrite,
        false,
        &mut meter,
        &mut outcome,
    );
    (device_dir, pc_dir, result.map(|()| outcome))
}

fn temp_files_in(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
        .count()
}

#[test]
fn stream_download_writes_whole_file_and_replaces_existing() {
    let (_device, pc, result) = stream_download(false);
    assert_eq!(result.unwrap().done, 1);
    let saved = fs::read(pc.path().join("記録/走行.fit")).unwrap();
    assert_eq!(saved, vec![9u8; 2 * 1024 * 1024 + 5]);
    assert_eq!(temp_files_in(&pc.path().join("記録")), 0);
}

#[test]
fn cancelled_stream_download_keeps_existing_file() {
    let (_device, pc, result) = stream_download(true);
    assert!(matches!(result, Err(OpError::Device(Error::Cancelled))));
    assert_eq!(
        fs::read(pc.path().join("記録/走行.fit")).unwrap(),
        b"original"
    );
    assert_eq!(temp_files_in(&pc.path().join("記録")), 0);
}

#[test]
fn download_conflict_asks_then_overwrites_or_skips() {
    let h = Harness::new();
    write(&h.device_path("a.txt"), b"device");
    let root = h.connect();
    write(&h.pc_path("a.txt"), b"pc");

    assert!(has_conflict(&h.download(root.clone(), None)));
    assert_no_error(&h.download(root.clone(), Some(Policy::Skip)));
    assert_eq!(fs::read(h.pc_path("a.txt")).unwrap(), b"pc");
    assert_no_error(&h.download(root, Some(Policy::Overwrite)));
    assert_eq!(fs::read(h.pc_path("a.txt")).unwrap(), b"device");
}

#[test]
fn download_refuses_names_that_escape_the_destination() {
    let h = Harness::new();
    write(&h.device_path("safe.txt"), b"ok");
    let mut root = h.connect();
    let mut evil = root[0].clone();
    evil.name = "../evil.txt".into();
    root.push(evil);

    let events = h.download(root, None);

    assert_no_error(&events);
    assert!(h.pc_path("safe.txt").exists());
    assert!(!h.pc_dir.path().parent().unwrap().join("evil.txt").exists());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Done(m) if m.contains("スキップ 1 件")))
    );
}

#[test]
fn renames_to_japanese_and_rejects_duplicates() {
    let h = Harness::new();
    write(&h.device_path("a.txt"), b"a");
    write(&h.device_path("b.txt"), b"b");
    let root = h.connect();
    let a = find(&root, "a.txt").clone();

    let renamed = h.run(Command::Rename {
        entry: a.clone(),
        new_name: "日本語の名前.txt".into(),
    });
    assert_no_error(&renamed);
    assert_eq!(fs::read(h.device_path("日本語の名前.txt")).unwrap(), b"a");
    assert!(!h.device_path("a.txt").exists());

    let duplicate = h.run(Command::Rename {
        entry: find(&h.list(), "日本語の名前.txt").clone(),
        new_name: "b.txt".into(),
    });
    assert!(duplicate.iter().any(|e| matches!(e, Event::Error(_))));
    assert_eq!(fs::read(h.device_path("b.txt")).unwrap(), b"b");

    let invalid = h.run(Command::Rename {
        entry: find(&h.list(), "b.txt").clone(),
        new_name: "x/y".into(),
    });
    assert!(invalid.iter().any(|e| matches!(e, Event::Error(_))));
}

#[test]
fn creates_folder_inside_current_folder() {
    let h = Harness::new();
    write(&h.device_path("親/x.txt"), b"x");
    let root = h.connect();
    h.run(Command::Enter {
        handle: find(&root, "親").handle,
        name: "親".into(),
    });

    assert_no_error(&h.run(Command::CreateFolder("新しいフォルダ".into())));
    assert!(h.device_path("親/新しいフォルダ").is_dir());

    let again = h.run(Command::CreateFolder("新しいフォルダ".into()));
    assert!(again.iter().any(|e| matches!(e, Event::Error(_))));
}

#[test]
fn deletes_folders_recursively() {
    let h = Harness::new();
    write(&h.device_path("消す/下/x.txt"), b"x");
    write(&h.device_path("残す.txt"), b"y");
    let root = h.connect();

    let events = h.run(Command::Delete(vec![find(&root, "消す").clone()]));

    assert_no_error(&events);
    assert!(!h.device_path("消す").exists());
    assert_eq!(names(&listing(&events)), ["残す.txt"]);
}

#[test]
fn cancelled_download_keeps_existing_file_and_no_temp_files() {
    let h = Harness::new();
    write(&h.device_path("big.bin"), &vec![1u8; 8 * 1024 * 1024]);
    let root = h.connect();
    write(&h.pc_path("big.bin"), b"original");
    h.hold_transfers_until_cancelled();

    let worker = h.worker.as_ref().unwrap();
    worker.send(Command::Transfer {
        transfer: Transfer::Download {
            entries: root,
            dest: h.pc_dir.path().to_path_buf(),
        },
        policy: Some(Policy::Overwrite),
    });
    let events = h.events.lock().unwrap();
    let mut cancelled_message = false;
    let deadline = Instant::now() + WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match events.recv_timeout(left).expect("ワーカーが応答しない") {
            Event::Progress { .. } => worker.cancel(),
            Event::Done(message) => cancelled_message = message.contains("キャンセル"),
            Event::Idle => break,
            _ => {}
        }
    }
    drop(events);

    assert!(cancelled_message, "キャンセルとして完了していない");
    assert_eq!(fs::read(h.pc_path("big.bin")).unwrap(), b"original");
    assert_eq!(fs::read_dir(h.pc_dir.path()).unwrap().count(), 1);
}

#[test]
fn cancelled_upload_leaves_no_partial_file_on_device() {
    let h = Harness::new();
    h.connect();
    write(&h.pc_path("big.bin"), &vec![1u8; 8 * 1024 * 1024]);
    h.hold_transfers_until_cancelled();

    let worker = h.worker.as_ref().unwrap();
    worker.send(Command::Transfer {
        transfer: Transfer::Upload {
            paths: vec![h.pc_path("big.bin")],
        },
        policy: None,
    });
    let events = h.events.lock().unwrap();
    let deadline = Instant::now() + WAIT;
    let mut cancelled_message = false;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match events.recv_timeout(left).expect("ワーカーが応答しない") {
            Event::Progress {
                fraction: Some(_), ..
            } => worker.cancel(),
            Event::Done(message) => cancelled_message = message.contains("キャンセル"),
            Event::Idle => break,
            _ => {}
        }
    }
    drop(events);

    assert!(cancelled_message, "キャンセルとして完了していない");
    assert!(
        !h.device_path("big.bin").exists(),
        "途中のファイルが残っている"
    );
}

#[test]
fn shutdown_during_download_removes_temp_file_and_waits_for_worker() {
    let mut h = Harness::new();
    write(&h.device_path("big.bin"), &vec![1u8; 8 * 1024 * 1024]);
    let root = h.connect();
    h.hold_transfers_until_cancelled();
    let worker = h.worker.take().unwrap();
    worker.send(Command::Transfer {
        transfer: Transfer::Download {
            entries: root,
            dest: h.pc_dir.path().to_path_buf(),
        },
        policy: None,
    });
    {
        let events = h.events.lock().unwrap();
        while !matches!(
            events.recv_timeout(WAIT).expect("ワーカーが応答しない"),
            Event::Progress {
                fraction: Some(_),
                ..
            }
        ) {}
    }

    worker.shutdown(WAIT);

    assert_eq!(fs::read_dir(h.pc_dir.path()).unwrap().count(), 0);
}

#[test]
fn device_removal_ends_the_session_without_an_idle_event() {
    let h = Harness::new();
    h.connect();

    h.send(Command::DevicesChanged {
        left: Some(h.location_id),
    });
    let events = h.events.lock().unwrap();
    let first = events.recv_timeout(WAIT).expect("通知がない");
    assert!(matches!(first, Event::Disconnected));
    let second = events.recv_timeout(WAIT).expect("通知がない");
    assert!(matches!(second, Event::Devices(_)));
    assert!(events.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn operations_without_a_session_do_nothing() {
    let h = Harness::new();
    let events = h.run(Command::Refresh);
    assert!(events.is_empty());
}
