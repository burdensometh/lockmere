#![allow(dead_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use lockmere::{Engine, Isolation, Operation, OperationResult, Options, Record, Row, RowId};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Scratch(PathBuf);

impl Scratch {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("lockmere-{}-{now}-{serial}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn db(&self) -> PathBuf {
        self.0.join("store")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn engine(path: impl AsRef<Path>) -> Engine {
    let schema =
        lockmere::session::parse_schema(include_str!("../../config/sample.schema")).unwrap();
    Engine::create(path, schema, Options::default()).unwrap()
}

pub fn branch(id: i64, code: &str) -> Operation {
    Operation::Insert(Record::new(
        RowId::new("branch", id),
        Row::from([("code".to_owned(), code.into())]),
    ))
}

pub fn badge(id: &str, branch: i64) -> Operation {
    Operation::Insert(Record::new(
        RowId::new("badge", id),
        Row::from([("branch_id".to_owned(), branch.into())]),
    ))
}

pub fn seed(db: &Engine) {
    let tx = db.begin(Isolation::Serializable).unwrap().id;
    db.execute(tx, branch(1, "north")).unwrap();
    db.execute(tx, branch(2, "south")).unwrap();
    db.execute(tx, badge("seed", 1)).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

pub fn get(db: &Engine, transaction: u64, id: RowId) -> Option<Record> {
    match db.execute(transaction, Operation::Get(id)).unwrap() {
        OperationResult::Row(result) => result,
        other => panic!("expected point result, got {other:?}"),
    }
}
