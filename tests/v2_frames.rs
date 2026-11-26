mod support;

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use lockmere::{
    wal::JOURNAL_FILE, Engine, Error,
    Isolation::Snapshot,
    Operation, OperationResult, Options, Record, RequestId, Row, RowId, Value,
};

use support::{badge, branch, engine, get, seed, Scratch};

#[test]
fn v1_journal_keeps_original_bytes_when_v2_commits_are_appended() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    let original = include_bytes!("../fixtures/v1/journal.v1").to_vec();
    fs::write(path.join(JOURNAL_FILE), &original).unwrap();
    {
        let db = Engine::open(&path, Options::default()).unwrap();
        assert_eq!(db.sequence().unwrap(), 4);
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(writer, branch(8, "v2_appended")).unwrap();
        db.execute(writer, badge("v2-badge", 8)).unwrap();
        let id = RequestId::new("writer", "v2-commit").unwrap();
        let committed = commit(&db, writer, Some(id));
        assert_eq!(committed.sequence, 5);
        assert_eq!(committed.writes, 2);
        let bytes = fs::read(path.join(JOURNAL_FILE)).unwrap();
        // The real v1 frames keep their original bytes; the new commit is a
        // single compact v2 frame appended after them.
        assert!(bytes.starts_with(&original));
        let appended = &bytes[original.len()..];
        assert_eq!(&appended[..2], b"L2");
        let updater = db.begin(Snapshot).unwrap().id;
        db.execute(
            updater,
            Operation::Update {
                id: RowId::new("branch", 8),
                patch: Row::from([("code".to_owned(), "v2_updated".into())]),
            },
        )
        .unwrap();
        db.execute(updater, Operation::Delete(RowId::new("badge", "v2-badge")))
            .unwrap();
        assert_eq!(commit(&db, updater, None).sequence, 6);
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 6);
    let reader = db.begin(Snapshot).unwrap().id;
    assert_eq!(
        get(&db, reader, RowId::new("branch", 3)).unwrap().values["code"],
        "south".into()
    );
    assert_eq!(
        get(&db, reader, RowId::new("branch", 8)).unwrap().values["code"],
        "v2_updated".into()
    );
    assert!(get(&db, reader, RowId::new("badge", "v2-badge")).is_none());
    assert_eq!(
        get(&db, reader, RowId::new("badge", "alpha"))
            .unwrap()
            .values["branch_id"],
        1.into()
    );
    db.rollback(reader).unwrap();
    // Receipts written by v1 and v2 commits share one identity space.
    let v2_receipt = db
        .receipt(&RequestId::new("writer", "v2-commit").unwrap())
        .unwrap()
        .unwrap();
    assert!(v2_receipt.replayed);
    assert_eq!(v2_receipt.sequence, 5);
    assert_eq!(v2_receipt.writes, 2);
    let v1_receipt = db
        .receipt(&RequestId::new("writer", "write-commit").unwrap())
        .unwrap()
        .unwrap();
    assert!(v1_receipt.replayed);
    assert_eq!(v1_receipt.sequence, 3);
    // History spans both formats in commit order.
    let history = db.history(0, 100).unwrap();
    assert_eq!(history.len(), 6);
    for (index, entry) in history.iter().enumerate() {
        assert_eq!(entry.sequence, index as u64 + 1);
    }
    let first_v2 = &history[4];
    assert_eq!(first_v2.receipt.as_ref().unwrap().request, "v2-commit");
    assert_eq!(first_v2.changes[0].id, RowId::new("badge", "v2-badge"));
    assert_eq!(first_v2.changes[1].id, RowId::new("branch", 8));
    let second_v2 = &history[5];
    assert_eq!(second_v2.changes[0].id, RowId::new("badge", "v2-badge"));
    assert_eq!(second_v2.changes[0].values, None);
    assert_eq!(
        second_v2.changes[1].values.as_ref().unwrap()["code"],
        "v2_updated".into()
    );
    let tail = db.history(4, 100).unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].sequence, 5);
    assert_eq!(tail[1].sequence, 6);
}

#[test]
fn corrupt_complete_v2_frame_reports_its_offset_and_stops() {
    for damage in [Damage::PayloadByte, Damage::Checksum, Damage::Magic] {
        let scratch = Scratch::new();
        let path = scratch.db();
        let file = path.join(JOURNAL_FILE);
        let frame_offset;
        {
            let db = engine(&path);
            seed(&db);
            frame_offset = fs::metadata(&file).unwrap().len();
            let tx = db.begin(Snapshot).unwrap().id;
            db.execute(tx, branch(9, "doomed")).unwrap();
            commit(&db, tx, None);
        }
        let mut bytes = fs::read(&file).unwrap();
        assert_eq!(&bytes[frame_offset as usize..][..2], b"L2");
        match damage {
            Damage::PayloadByte => bytes[frame_offset as usize + 6] ^= 0x5a,
            Damage::Checksum => {
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
            }
            Damage::Magic => bytes[frame_offset as usize + 1] = b'X',
        }
        fs::write(&file, &bytes).unwrap();
        assert!(
            matches!(Engine::open(&path, Options::default()), Err(Error::Corrupt { file: JOURNAL_FILE, offset, .. })
            if offset == frame_offset),
            "{damage:?} must corrupt the frame at {frame_offset}"
        );
    }
}

#[test]
fn torn_v2_tail_is_truncated_and_completed_prefix_is_kept() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let file = path.join(JOURNAL_FILE);
    let len;
    {
        let db = engine(&path);
        seed(&db);
        len = fs::metadata(&file).unwrap().len();
    }
    // A half-written v2 frame: tag and length, then the payload stops.
    append(&file, b"L2\x30\x02\x03\x01");
    let committed_len;
    {
        let db = Engine::open(&path, Options::default()).unwrap();
        assert_eq!(db.sequence().unwrap(), 1);
        assert_eq!(fs::metadata(&file).unwrap().len(), len);
        let tx = db.begin(Snapshot).unwrap().id;
        db.execute(tx, branch(7, "after_torn")).unwrap();
        commit(&db, tx, None);
        committed_len = fs::metadata(&file).unwrap().len();
    }
    // A lone magic byte is also a torn frame.
    append(&file, b"L");
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
    assert_eq!(fs::metadata(&file).unwrap().len(), committed_len);
    let reader = db.begin(Snapshot).unwrap().id;
    assert!(get(&db, reader, RowId::new("branch", 7)).is_some());
    assert!(get(&db, reader, RowId::new("branch", 1)).is_some());
    db.rollback(reader).unwrap();
    let history = db.history(0, 10).unwrap();
    assert_eq!(history.len(), 2);
}

#[test]
fn checkpoint_anchored_to_v2_frame_reopens_and_replays_tail() {
    let scratch = Scratch::new();
    let path = scratch.db();
    {
        let db = engine(&path);
        seed(&db);
        db.checkpoint().unwrap();
        let tx = db.begin(Snapshot).unwrap().id;
        db.execute(tx, badge("tail", 1)).unwrap();
        let id = RequestId::new("client", "tail-request").unwrap();
        assert_eq!(commit(&db, tx, Some(id)).sequence, 2);
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
    let receipt = db
        .receipt(&RequestId::new("client", "tail-request").unwrap())
        .unwrap()
        .unwrap();
    assert!(receipt.replayed);
    assert_eq!(receipt.sequence, 2);
    let reader = db.begin(Snapshot).unwrap().id;
    assert!(get(&db, reader, RowId::new("badge", "tail")).is_some());
    assert!(get(&db, reader, RowId::new("badge", "seed")).is_some());
    db.rollback(reader).unwrap();
    let history = db.history(0, 10).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].changes[0].id, RowId::new("badge", "tail"));
}

#[test]
fn v2_commit_roundtrips_every_value_shape() {
    let scratch = Scratch::new();
    let path = scratch.db();
    {
        let db = engine(&path);
        seed(&db);
        let tx = db.begin(Snapshot).unwrap().id;
        db.execute(tx, branch(-5, "neg")).unwrap();
        db.execute(
            tx,
            Operation::Insert(Record::new(
                RowId::new("badge", "noted"),
                Row::from([
                    ("branch_id".to_owned(), 1.into()),
                    ("note".to_owned(), "héllo ✓".into()),
                ]),
            )),
        )
        .unwrap();
        db.execute(
            tx,
            Operation::Insert(Record::new(
                RowId::new("badge", "blank"),
                Row::from([
                    ("branch_id".to_owned(), 2.into()),
                    ("note".to_owned(), "".into()),
                ]),
            )),
        )
        .unwrap();
        commit(&db, tx, None);
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
    let reader = db.begin(Snapshot).unwrap().id;
    assert_eq!(
        get(&db, reader, RowId::new("branch", -5)).unwrap().values["code"],
        "neg".into()
    );
    let noted = get(&db, reader, RowId::new("badge", "noted")).unwrap();
    assert_eq!(noted.values["note"], "héllo ✓".into());
    let blank = get(&db, reader, RowId::new("badge", "blank")).unwrap();
    assert_eq!(blank.values["note"], "".into());
    assert_eq!(
        get(&db, reader, RowId::new("badge", "seed")).unwrap().values["note"],
        Value::Null
    );
    db.rollback(reader).unwrap();
    let history = db.history(1, 10).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].changes.len(), 3);
    assert_eq!(history[0].changes[0].id, RowId::new("badge", "blank"));
    assert_eq!(history[0].changes[2].id, RowId::new("branch", -5));
}

#[test]
fn empty_v2_commit_roundtrips() {
    let scratch = Scratch::new();
    let path = scratch.db();
    {
        let db = engine(&path);
        seed(&db);
        let tx = db.begin(Snapshot).unwrap().id;
        let result = commit(&db, tx, None);
        assert_eq!(result.sequence, 2);
        assert_eq!(result.writes, 0);
    }
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
    let history = db.history(0, 10).unwrap();
    assert_eq!(history.len(), 2);
    assert!(history[1].changes.is_empty());
    assert!(history[1].receipt.is_none());
}

#[test]
fn v2_frames_are_more_compact_than_equivalent_v1_frames() {
    // The fixture's third commit is one branch write with a durable request
    // as a v1 text frame; the same logical commit is written here as v2.
    let original = include_bytes!("../fixtures/v1/journal.v1").to_vec();
    let data = original
        .windows(5)
        .position(|window| window == b"DATA\n")
        .unwrap()
        + 5;
    let frames = &original[data..];
    let v1_len = frames
        .split(|byte| *byte == b'\n')
        .nth(2)
        .map(|line| line.len() + 1)
        .unwrap();
    let scratch = Scratch::new();
    let path = scratch.db();
    let file = path.join(JOURNAL_FILE);
    let before;
    let v2_len;
    {
        let db = engine(&path);
        before = fs::metadata(&file).unwrap().len();
        let tx = db.begin(Snapshot).unwrap().id;
        db.execute(tx, branch(3, "south")).unwrap();
        let id = RequestId::new("writer", "write-commit").unwrap();
        commit(&db, tx, Some(id));
        v2_len = fs::metadata(&file).unwrap().len() - before;
    }
    assert!(
        v2_len < v1_len as u64,
        "v2 frame ({v2_len} bytes) should beat the v1 frame ({v1_len} bytes)"
    );
}

#[test]
fn no_single_byte_corruption_of_a_v2_frame_is_silently_accepted() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let file = path.join(JOURNAL_FILE);
    let frame_offset;
    {
        let db = engine(&path);
        seed(&db);
        frame_offset = fs::metadata(&file).unwrap().len() as usize;
        let tx = db.begin(Snapshot).unwrap().id;
        db.execute(tx, branch(9, "target")).unwrap();
        commit(&db, tx, None);
    }
    let original = fs::read(&file).unwrap();
    for position in frame_offset..original.len() {
        let mut damaged = original.clone();
        damaged[position] ^= 0xff;
        fs::write(&file, &damaged).unwrap();
        match Engine::open(&path, Options::default()) {
            Err(Error::Corrupt {
                file: JOURNAL_FILE,
                offset,
                ..
            }) => assert_eq!(offset, frame_offset as u64, "byte {position}"),
            Err(other) => panic!("byte {position}: unexpected error {other}"),
            Ok(db) => {
                // The only acceptable recovery is dropping the damaged tail:
                // the completed prefix stays and the file is truncated to it.
                assert_eq!(db.sequence().unwrap(), 1, "byte {position}");
                assert_eq!(
                    fs::metadata(&file).unwrap().len(),
                    frame_offset as u64,
                    "byte {position}"
                );
                drop(db);
            }
        }
    }
    fs::write(&file, &original).unwrap();
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), 2);
}

#[derive(Debug)]
enum Damage {
    PayloadByte,
    Checksum,
    Magic,
}

fn commit(db: &Engine, tx: u64, id: Option<RequestId>) -> lockmere::CommitResult {
    match db.execute(tx, Operation::Commit(id)).unwrap() {
        OperationResult::Committed(result) => result,
        other => panic!("expected commit, got {other:?}"),
    }
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}
