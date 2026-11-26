mod support;

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    process::Command,
    sync::Arc,
};

use lockmere::{
    session::{Response, Session},
    wal::{CHECKPOINT_FILE, JOURNAL_FILE},
    Engine, Error, FaultInjector, FaultPoint,
    Isolation::{Serializable, Snapshot},
    Operation, OperationResult, Options, RequestId, RowId,
};

use support::{badge, branch, engine, get, seed, Scratch};

#[test]
fn read_v1_fixture_then_append_checkpoint_and_replay_receipt() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join(JOURNAL_FILE),
        include_bytes!("../fixtures/v1/journal.v1"),
    )
    .unwrap();
    {
        let db = Arc::new(Engine::open(&path, Options::default()).unwrap());
        assert_eq!(db.sequence().unwrap(), 4);
        let mut client = Session::new(db.clone(), "writer").unwrap();
        let Response::Operation(OperationResult::Committed(receipt)) =
            client.execute("write-commit", "COMMIT").unwrap()
        else {
            panic!("expected durable receipt");
        };
        assert!(receipt.replayed);
        assert_eq!(receipt.sequence, 3);
        let reader = db.begin(Snapshot).unwrap().id;
        assert_eq!(
            get(&db, reader, RowId::new("branch", 3)).unwrap().values["code"],
            "south".into()
        );
        assert_eq!(
            get(&db, reader, RowId::new("badge", "alpha"))
                .unwrap()
                .values["branch_id"],
            1.into()
        );
        assert!(get(&db, reader, RowId::new("badge", "stray")).is_none());
        db.rollback(reader).unwrap();
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(writer, branch(8, "fixture_append")).unwrap();
        db.execute(writer, Operation::Commit(None)).unwrap();
        assert_eq!(db.checkpoint().unwrap().sequence, 5);
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.history(0, 10).unwrap().len(), 5);
    assert!(
        db.receipt(&RequestId::new("writer", "write-commit").unwrap())
            .unwrap()
            .unwrap()
            .replayed
    );
}

#[test]
fn v1_checkpoint_fixture_is_bound_to_the_native_journal() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join(JOURNAL_FILE),
        include_bytes!("../fixtures/v1/journal.v1"),
    )
    .unwrap();
    fs::write(
        path.join(CHECKPOINT_FILE),
        include_bytes!("../fixtures/v1/checkpoint.v1"),
    )
    .unwrap();
    let db = Engine::open(&path, Options::default()).unwrap();
    let reader = db.begin(Snapshot).unwrap().id;
    assert_eq!(db.sequence().unwrap(), 4);
    assert!(get(&db, reader, RowId::new("badge", "alpha")).is_some());
    assert!(get(&db, reader, RowId::new("badge", "stray")).is_none());
}

#[test]
fn checkpoint_anchor_leaves_uncommitted_rows_out_and_replays_tail() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    seed(&db);
    let staged = db.begin(Snapshot).unwrap().id;
    db.execute(staged, branch(5, "staged")).unwrap();
    db.checkpoint().unwrap();
    let committed = db.begin(Serializable).unwrap().id;
    db.execute(committed, branch(6, "committed")).unwrap();
    db.execute(committed, Operation::Commit(None)).unwrap();
    drop(db);
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
    let reader = db.begin(Snapshot).unwrap().id;
    assert!(get(&db, reader, RowId::new("branch", 5)).is_none());
    assert!(get(&db, reader, RowId::new("branch", 6)).is_some());
}

#[test]
fn torn_tail_is_truncated_but_complete_malformed_frame_is_rejected() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let file = path.join(JOURNAL_FILE);
    {
        let db = engine(&path);
        seed(&db);
    }
    let len = fs::metadata(&file).unwrap().len();
    append(&file, b"C\t2\t9\t");
    {
        let db = Engine::open(&path, Options::default()).unwrap();
        assert_eq!(db.sequence().unwrap(), 1);
        assert_eq!(fs::metadata(&file).unwrap().len(), len);
    }
    append(&file, b"C\t2\t9\t-\t-\t0\tE2\t#100\n");
    assert!(
        matches!(Engine::open(&path, Options::default()), Err(Error::Corrupt { file: JOURNAL_FILE, offset, .. })
        if offset == len)
    );
}

#[test]
fn alternate_checkpoint_with_matching_schema_and_commit_shape_is_rejected() {
    let scratch = Scratch::new();
    let first = scratch.path().join("first");
    let second = scratch.path().join("second");
    for path in [&first, &second] {
        let db = engine(path);
        seed(&db);
        db.checkpoint().unwrap();
    }
    fs::copy(first.join(CHECKPOINT_FILE), second.join(CHECKPOINT_FILE)).unwrap();
    assert!(matches!(
        Engine::open(second, Options::default()),
        Err(Error::Corrupt {
            file: CHECKPOINT_FILE,
            ..
        })
    ));
}

#[test]
fn orphan_checkpoint_directory_is_refused_without_touching_its_bytes() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    let checkpoint = path.join(CHECKPOINT_FILE);
    let snapshot = include_bytes!("../fixtures/v1/checkpoint.v1");
    fs::write(&checkpoint, snapshot).unwrap();

    let schema = support::schema();
    let error = match Engine::create(&path, schema, Options::default()) {
        Err(error) => error,
        Ok(_) => panic!("orphan checkpoint directory must be refused"),
    };
    assert_eq!(error, Error::OrphanCheckpoint);
    assert_eq!(error.code(), "ORPHAN_CHECKPOINT");

    // The rejected initialization leaves every byte in place and creates
    // neither a journal, a lease, nor any temporary files.
    assert_eq!(fs::read(&checkpoint).unwrap(), snapshot);
    assert!(!path.join(JOURNAL_FILE).exists());
    assert!(!path.join(".lockmere.lock").exists());
    assert!(!path.join(".journal.v1.initial").exists());
    assert!(!path.join(".checkpoint.v1.temporary").exists());
    assert_eq!(fs::read_dir(&path).unwrap().count(), 1);

    // Once the matching journal is restored, the pair is a valid store again.
    fs::write(
        path.join(JOURNAL_FILE),
        include_bytes!("../fixtures/v1/journal.v1"),
    )
    .unwrap();
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 4);
}

#[test]
fn empty_and_nonexistent_directories_still_initialize() {
    let scratch = Scratch::new();
    let empty = scratch.path().join("empty");
    fs::create_dir(&empty).unwrap();
    let db = Engine::create(&empty, support::schema(), Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 0);
    drop(db);
    let reopened = Engine::open(&empty, Options::default()).unwrap();
    assert_eq!(reopened.sequence().unwrap(), 0);

    let missing = scratch.path().join("missing");
    Engine::create(&missing, support::schema(), Options::default()).unwrap();
    assert!(missing.join(JOURNAL_FILE).is_file());
}

#[test]
fn journal_checkpoint_pair_reopens_and_accepts_appended_commits() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join(JOURNAL_FILE),
        include_bytes!("../fixtures/v1/journal.v1"),
    )
    .unwrap();
    fs::write(
        path.join(CHECKPOINT_FILE),
        include_bytes!("../fixtures/v1/checkpoint.v1"),
    )
    .unwrap();
    {
        let db = Engine::open(&path, Options::default()).unwrap();
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(writer, branch(9, "after_pair_open")).unwrap();
        db.execute(writer, Operation::Commit(None)).unwrap();
        assert_eq!(db.sequence().unwrap(), 5);
        db.checkpoint().unwrap();
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 5);
    assert_eq!(db.history(0, 10).unwrap().len(), 5);
}

#[test]
fn database_lease_blocks_second_open_and_releases_on_drop() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    assert!(matches!(
        Engine::open(&path, Options::default()),
        Err(Error::StoreInUse)
    ));
    drop(db);
    Engine::open(&path, Options::default()).unwrap();
}

#[test]
fn before_append_preserves_prior_commit_and_does_not_record_failed_request() {
    let scratch = Scratch::new();
    let path = scratch.db();
    {
        let db = engine(&path);
        seed(&db);
    }
    let options = Options {
        fault: Arc::new(FailAt(FaultPoint::BeforeAppend)),
        ..Options::default()
    };
    let db = Engine::open(&path, options).unwrap();
    let id = RequestId::new("tester", "retry").unwrap();
    let tx = db.begin(Snapshot).unwrap().id;
    db.execute(tx, branch(10, "no_write")).unwrap();
    assert!(matches!(
        db.execute(tx, Operation::Commit(Some(id.clone()))),
        Err(Error::Injected("before_append"))
    ));
    assert!(db.receipt(&id).unwrap().is_none());
    assert_eq!(db.sequence().unwrap(), 1);
    drop(db);
    assert_eq!(
        Engine::open(&path, Options::default())
            .unwrap()
            .sequence()
            .unwrap(),
        1
    );
}

#[test]
fn after_sync_failure_poisoned_engine_reopens_with_commit() {
    let scratch = Scratch::new();
    let path = scratch.db();
    engine(&path);
    let options = Options {
        fault: Arc::new(FailAt(FaultPoint::AfterSyncBeforePublish)),
        ..Options::default()
    };
    let db = Engine::open(&path, options).unwrap();
    let tx = db.begin(Snapshot).unwrap().id;
    let id = RequestId::new("tester", "uncertain").unwrap();
    db.execute(tx, branch(10, "durable")).unwrap();
    assert!(matches!(
        db.execute(tx, Operation::Commit(Some(id.clone()))),
        Err(Error::Injected("after_sync_before_publish"))
    ));
    assert!(matches!(db.begin(Snapshot), Err(Error::StorageUnavailable)));
    assert!(matches!(db.receipt(&id), Err(Error::StorageUnavailable)));
    drop(db);
    let db = Engine::open(&path, Options::default()).unwrap();
    assert!(db.receipt(&id).unwrap().unwrap().replayed);
}

#[test]
fn failed_checkpoint_publish_retains_previous_anchor_and_tail() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let previous;
    {
        let db = engine(&path);
        seed(&db);
        db.checkpoint().unwrap();
        previous = fs::read(path.join(CHECKPOINT_FILE)).unwrap();
    }
    let options = Options {
        fault: Arc::new(FailAt(FaultPoint::BeforeCheckpointPublish)),
        ..Options::default()
    };
    let db = Engine::open(&path, options).unwrap();
    let tx = db.begin(Snapshot).unwrap().id;
    db.execute(tx, branch(10, "newer")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
    assert!(matches!(
        db.checkpoint(),
        Err(Error::Injected("before_checkpoint_publish"))
    ));
    assert_eq!(fs::read(path.join(CHECKPOINT_FILE)).unwrap(), previous);
    drop(db);
    assert_eq!(
        Engine::open(&path, Options::default())
            .unwrap()
            .sequence()
            .unwrap(),
        2
    );
}

#[test]
fn process_exit_between_wal_sync_and_publish_recovers_once() {
    let scratch = Scratch::new();
    let path = scratch.db();
    {
        let db = engine(&path);
        seed(&db);
        db.checkpoint().unwrap();
    }
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child_entry", "--nocapture"])
        .env("LOCKMERE_TEST_CRASH_DIR", &path)
        .output()
        .unwrap();
    assert_eq!(
        child.status.code(),
        Some(86),
        "child stderr: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let db = Arc::new(Engine::open(&path, Options::default()).unwrap());
    assert_eq!(db.sequence().unwrap(), 2);
    let mut session = Session::new(db.clone(), "crash-child").unwrap();
    let result = session.execute("durable-request", "COMMIT").unwrap();
    assert!(
        matches!(result, Response::Operation(OperationResult::Committed(item)) if item.replayed && item.sequence == 2)
    );
    let reader = db.begin(Snapshot).unwrap().id;
    assert!(get(&db, reader, RowId::new("badge", "after_crash")).is_some());
    assert_eq!(db.history(0, 10).unwrap().len(), 2);
}

#[test]
fn crash_child_entry() {
    let Some(directory) = std::env::var_os("LOCKMERE_TEST_CRASH_DIR") else {
        return;
    };
    struct ExitAtDurable;
    impl FaultInjector for ExitAtDurable {
        fn hit(&self, point: FaultPoint) -> lockmere::Result<()> {
            if point == FaultPoint::AfterSyncBeforePublish {
                std::process::exit(86);
            }
            Ok(())
        }
    }

    let options = Options {
        fault: Arc::new(ExitAtDurable),
        ..Options::default()
    };
    let db = Engine::open(Path::new(&directory), options).unwrap();
    let tx = db.begin(Snapshot).unwrap().id;
    db.execute(tx, badge("after_crash", 2)).unwrap();
    let id = RequestId::new("crash-child", "durable-request").unwrap();
    db.execute(tx, Operation::Commit(Some(id))).unwrap();
    panic!("fault point did not end child");
}

struct FailAt(FaultPoint);

impl FaultInjector for FailAt {
    fn hit(&self, point: FaultPoint) -> lockmere::Result<()> {
        if self.0 == point {
            Err(Error::Injected(point.name()))
        } else {
            Ok(())
        }
    }
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}
