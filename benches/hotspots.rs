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
    version_reclamation(100_000 * scale, 64, 1_000);
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

fn version_reclamation(cold_keys: usize, hot_keys: usize, rounds: usize) {
    let mut index = RowIndex::new();
    let empty = Row::new();
    // The bulk of the store: cold keys with exactly one committed head.
    for key in 0..cold_keys as i64 {
        let id = RowId::new("rows", key);
        index.push(id, RowVersion::new(1, 1, Some(empty.clone())));
    }
    // A small set of hot keys, each starting with five versions.
    let grow = |index: &mut RowIndex, base: u64| {
        for key in cold_keys as i64..cold_keys as i64 + hot_keys as i64 {
            let id = RowId::new("rows", key);
            for offset in 1..=4u64 {
                let sequence = base + offset;
                let values = Row::from([("value".to_owned(), (sequence as i64).into())]);
                index.push(
                    id.clone(),
                    RowVersion::new(sequence, sequence, Some(values)),
                );
            }
        }
    };
    grow(&mut index, 1);
    let timer = Instant::now();
    let mut collected = 0;
    for round in 0..rounds {
        collected += black_box(index.collect_versions(u64::MAX));
        // Regrow the hot history so every round has the same amount of work;
        // cold keys remain single-version and must never be revisited.
        if round + 1 < rounds {
            grow(&mut index, 1 + 4 * (round as u64 + 1));
        }
    }
    // The first checkpoint keeps the original sequence-1 head (3 collected per
    // hot key); every later checkpoint keeps only that round's newest head
    // (4 collected per hot key). Cold keys never contribute.
    assert_eq!(collected, (4 * rounds - 1) * hot_keys);
    // Every committed head survives reclamation.
    assert_eq!(index.heads().count(), cold_keys + hot_keys);
    for key in 0..cold_keys as i64 {
        assert_eq!(index.latest(&RowId::new("rows", key)).unwrap().sequence, 1);
    }
    println!(
        "version_reclamation cold_keys={cold_keys} hot_keys={hot_keys} checkpoints={rounds} collected={collected} micros={}",
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
