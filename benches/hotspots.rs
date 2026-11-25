use std::{
    env, fs,
    hint::black_box,
    ops::Bound,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use lockmere::{
    locks::{LockManager, LockMode, LockRequest},
    rows::{PendingRows, RowIndex, RowVersion},
    Engine, KeyKind, Operation, Options, ReadInterval, Record, Row, RowId, Schema, Table,
};

fn main() {
    let root = WorkDir::new();
    let scale = env::var("LOCKMERE_BENCH_SCALE")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("numeric LOCKMERE_BENCH_SCALE")
        })
        .unwrap_or(1);
    assert!((1..=20).contains(&scale), "bench scale must be 1..=20");
    index_scan(100_000 * scale, 1_500);
    wait_graph(64 * scale.min(4), 600);
    recovery(&root.0, 30 * scale, 48);
}

fn index_scan(rows: usize, rounds: usize) {
    let mut index = RowIndex::new();
    for key in 0..rows as i64 {
        let id = RowId::new("rows", key);
        for version in 1..=4 {
            let values = Row::from([("value".to_owned(), (key + version as i64).into())]);
            index.push(id.clone(), RowVersion::new(version, version, Some(values)));
        }
    }
    let pending = PendingRows::default();
    let timer = Instant::now();
    let mut returned = 0;
    for i in 0..rounds {
        let from = ((i * 131) % (rows - 128)) as i64;
        let interval = ReadInterval::new(
            "rows",
            Bound::Excluded(from.into()),
            Bound::Included((from + 128).into()),
        );
        let page = index.scan(black_box(&interval), 2, &pending, 128);
        assert_eq!(page.first().unwrap().values["value"], (from + 3).into());
        returned += black_box(page.len());
    }
    assert_eq!(returned, rounds * 128);
    println!(
        "index_scan rows={rows} versions_per_row=4 scans={rounds} returned={returned} micros={}",
        timer.elapsed().as_micros()
    );
}

fn wait_graph(waiters: usize, rounds: usize) {
    let mut locks = LockManager::default();
    let interval = ReadInterval::whole("rows");
    locks
        .enqueue(LockRequest::new(1, interval.clone(), LockMode::Exclusive))
        .unwrap();
    for id in 2..=(waiters as u64 + 1) {
        locks
            .enqueue(LockRequest::new(id, interval.clone(), LockMode::Shared))
            .unwrap();
    }
    let timer = Instant::now();
    let mut count = 0;
    for _ in 0..rounds {
        let graph = locks.graph();
        assert!(graph.cycle_through(2).is_none());
        count += black_box(graph.edges().len());
    }
    assert_eq!(count, waiters * rounds);
    println!(
        "wait_graph waiters={waiters} graphs={rounds} observed_edges={count} micros={}",
        timer.elapsed().as_micros()
    );
}

fn recovery(root: &Path, commits: usize, writes: usize) {
    use lockmere::Isolation::Snapshot;
    let schema = Schema::new(vec![Table::new("rows", KeyKind::Integer)], vec![]).unwrap();
    let plain = root.join("journal");
    let with_checkpoint = root.join("checkpoint");
    {
        let engine = Engine::create(&plain, schema, Options::default()).unwrap();
        for batch in 0..commits {
            let writer = engine.begin(Snapshot).unwrap().id;
            for offset in 0..writes {
                let id = RowId::new("rows", (batch * writes + offset) as i64);
                engine
                    .execute(writer, Operation::Insert(Record::new(id, Row::new())))
                    .unwrap();
            }
            engine.execute(writer, Operation::Commit(None)).unwrap();
        }
        engine.checkpoint().unwrap();
    }
    fs::create_dir(&with_checkpoint).unwrap();
    for file in [lockmere::wal::JOURNAL_FILE, lockmere::wal::CHECKPOINT_FILE] {
        fs::copy(plain.join(file), with_checkpoint.join(file)).unwrap();
    }
    fs::remove_file(plain.join(lockmere::wal::CHECKPOINT_FILE)).unwrap();
    for (label, path) in [("journal", plain), ("checkpoint", with_checkpoint)] {
        let timer = Instant::now();
        for _ in 0..20 {
            let reader = Engine::open(&path, Options::default()).unwrap();
            assert_eq!(reader.sequence().unwrap(), commits as u64);
            let tx = reader.begin(Snapshot).unwrap().id;
            let last = RowId::new("rows", (commits * writes - 1) as i64);
            assert!(matches!(
                reader.execute(tx, Operation::Get(last)).unwrap(),
                lockmere::OperationResult::Row(Some(_))
            ));
            reader.rollback(tx).unwrap();
        }
        println!(
            "recovery source={label} commits={commits} rows={} opens=20 micros={}",
            commits * writes,
            timer.elapsed().as_micros()
        );
    }
}

struct WorkDir(PathBuf);

impl WorkDir {
    fn new() -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("lockmere-bench-{}-{time}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
