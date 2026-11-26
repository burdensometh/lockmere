mod support;

use std::{
    fs::{self, OpenOptions},
    io::Write,
};

use lockmere::{
    wal::{HISTORY_FILE, JOURNAL_FILE},
    Engine, Error, Isolation, Operation, Options,
};

use support::{branch, engine, Scratch};

/// A commit whose only write is the given branch row.
fn commit_branch(db: &Engine, id: i64, code: &str) {
    let tx = db.begin(Isolation::Snapshot).unwrap().id;
    db.execute(tx, branch(id, code)).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

/// Default workload is small and fast under the debug test profile. Setting
/// `LOCKMERE_HISTORY_SCALE=100000` exercises the hundred-thousand-commit tail.
fn history_scale(default: u64) -> u64 {
    std::env::var("LOCKMERE_HISTORY_SCALE")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 257)
        .unwrap_or(default)
}

#[test]
fn tail_page_over_a_hundred_thousand_commits_decodes_at_most_256_frames() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    let total = history_scale(1_000);
    for sequence in 1..=total {
        commit_branch(&db, sequence as i64, &format!("code-{sequence}"));
    }

    // The unindexed implementation decoded every frame in the journal here.
    let (page, decoded) = db.history_measured(0, 10).unwrap();
    assert_eq!(page.len(), 10);
    assert_eq!(page.first().unwrap().sequence, total - 9);
    assert_eq!(page.last().unwrap().sequence, total);
    assert!(
        decoded <= 256,
        "tail page decoded {decoded} frames, expected at most 256"
    );
    // Commit order is ascending even though the page is the journal's tail.
    let sequences: Vec<_> = page.iter().map(|record| record.sequence).collect();
    let mut ordered = sequences.clone();
    ordered.sort_unstable();
    assert_eq!(sequences, ordered);
    assert!(sequences.windows(2).all(|pair| pair[0] + 1 == pair[1]));
}

#[test]
fn cursor_pagination_keeps_commit_order_and_respects_the_after_cursor() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    for sequence in 1..=600u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }

    // A small page walks the tail without re-reading the whole journal.
    let (first, decoded_first) = db.history_measured(590, 5).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![591, 592, 593, 594, 595]
    );
    assert!(decoded_first <= 256);

    let (second, _) = db.history_measured(595, 5).unwrap();
    assert_eq!(
        second
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![596, 597, 598, 599, 600]
    );

    // A windowed tail page returns the newest frames, not the oldest.
    let tail = db.history(0, 3).unwrap();
    assert_eq!(
        tail.iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![598, 599, 600]
    );

    // Every page stays in ascending commit order.
    for page in [first, second, tail] {
        assert!(page
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
    }
}

#[test]
fn missing_history_index_is_built_without_rewriting_the_journal() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    for sequence in 1..=300u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }
    drop(db);

    let journal_bytes = fs::read(path.join(JOURNAL_FILE)).unwrap();
    let journal_len = journal_bytes.len() as u64;
    fs::remove_file(path.join(HISTORY_FILE)).unwrap();

    // Reopening rebuilds the sidecar from the journal alone.
    let db = Engine::open(&path, Options::default()).unwrap();
    assert!(path.join(HISTORY_FILE).exists());
    assert_eq!(
        fs::metadata(path.join(JOURNAL_FILE)).unwrap().len(),
        journal_len,
        "the v1 journal must never be rewritten to build the index"
    );

    let (page, decoded) = db.history_measured(0, 10).unwrap();
    assert_eq!(page.len(), 10);
    assert_eq!(page.last().unwrap().sequence, 300);
    assert!(decoded <= 256);
}

#[test]
fn corrupt_history_index_is_discarded_and_rebuilt() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    for sequence in 1..=300u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }
    drop(db);
    assert!(path.join(HISTORY_FILE).exists());

    // Garbage the derived sidecar; the journal remains the source of truth.
    fs::write(path.join(HISTORY_FILE), b"LOCKMERE-HISTORY\nbroken\n").unwrap();
    let db = Engine::open(&path, Options::default()).unwrap();
    let page = db.history(0, 5).unwrap();
    assert_eq!(page.len(), 5);
    assert_eq!(page.last().unwrap().sequence, 300);
}

#[test]
fn stale_history_index_is_rebuilt_after_the_journal_grows() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    for sequence in 1..=260u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }
    drop(db);
    let stale_index = fs::read(path.join(HISTORY_FILE)).unwrap();

    // Append frames out of band so the persisted index falls behind.
    {
        let db = Engine::open(&path, Options::default()).unwrap();
        commit_branch(&db, 261, "late-1");
        commit_branch(&db, 262, "late-2");
    }
    let current_index = fs::read(path.join(HISTORY_FILE)).unwrap();
    assert_ne!(stale_index, current_index);

    let db = Engine::open(&path, Options::default()).unwrap();
    let page = db.history(0, 4).unwrap();
    assert_eq!(
        page.iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![259, 260, 261, 262]
    );
}

#[test]
fn torn_tail_still_serves_confirmed_pages_and_malformed_frames_error() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let journal = path.join(JOURNAL_FILE);
    {
        let db = engine(&path);
        for sequence in 1..=280u64 {
            commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
        }
    }
    let durable_len = fs::metadata(&journal).unwrap().len();

    // An incomplete final frame is truncated on open; confirmed frames survive.
    append(&journal, b"C\t281\t9\t-\t-\t0\tE281\t#10");
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(fs::metadata(&journal).unwrap().len(), durable_len);
    let page = db.history(0, 5).unwrap();
    assert_eq!(page.last().unwrap().sequence, 280);
    assert_eq!(page.len(), 5);
    drop(db);

    // A complete but malformed frame is an explicit corruption, not a silent gap.
    append(&journal, b"C\t281\t9\t-\t-\t0\tE281\t#100\n");
    let result = Engine::open(&path, Options::default());
    assert!(matches!(
        result,
        Err(Error::Corrupt {
            file: JOURNAL_FILE,
            ..
        })
    ));
}

#[test]
fn index_survives_reopen_and_serves_every_history_page_within_the_bound() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    for sequence in 1..=1_024u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }
    drop(db);

    let db = Engine::open(&path, Options::default()).unwrap();

    // Pages near the head, middle and tail all stay within the decode bound.
    for after in [0u64, 1, 255, 256, 511, 768, 1_014] {
        let (page, decoded) = db.history_measured(after, 10).unwrap();
        assert!(decoded <= 256, "after={after} decoded {decoded}");
        assert!(page.len() <= 10);
        assert!(page
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
        if after == 0 {
            assert_eq!(page.last().unwrap().sequence, 1_024);
        }
    }

    // Once rebuilt on open, the sidecar is stable across further reopens.
    let on_disk = fs::read(path.join(HISTORY_FILE)).unwrap();
    drop(db);
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(fs::read(path.join(HISTORY_FILE)).unwrap(), on_disk);
    assert_eq!(db.history(0, 1).unwrap()[0].sequence, 1_024);
}

#[test]
fn reopen_persists_the_otherwise_in_memory_partial_span() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    // 300 commits leave the third span (257..) open, so its entry is in memory.
    for sequence in 1..=300u64 {
        commit_branch(&db, sequence as i64, &format!("c-{sequence}"));
    }
    drop(db);

    // Rebuild on open records the open span and writes a fresh sidecar.
    let db = Engine::open(&path, Options::default()).unwrap();
    assert!(path.join(HISTORY_FILE).exists());
    let tail = db.history(0, 5).unwrap();
    assert_eq!(tail.len(), 5);
    assert_eq!(tail.last().unwrap().sequence, 300);
    // A second reopen now loads the sidecar without another rebuild.
    let on_disk = fs::read(path.join(HISTORY_FILE)).unwrap();
    drop(db);
    let db = Engine::open(&path, Options::default()).unwrap();
    assert_eq!(fs::read(path.join(HISTORY_FILE)).unwrap(), on_disk);
    assert_eq!(db.history(0, 1).unwrap()[0].sequence, 300);
}

fn append(path: &std::path::Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn legacy_v1_journal_gains_a_query_index_without_being_rewritten() {
    let scratch = Scratch::new();
    let path = scratch.db();
    fs::create_dir(&path).unwrap();
    // The shipped fixture predates history.v1 and ships only journal/checkpoint.
    fs::write(
        path.join(JOURNAL_FILE),
        include_bytes!("../fixtures/v1/journal.v1"),
    )
    .unwrap();
    fs::write(
        path.join(lockmere::wal::CHECKPOINT_FILE),
        include_bytes!("../fixtures/v1/checkpoint.v1"),
    )
    .unwrap();
    let original_journal = fs::read(path.join(JOURNAL_FILE)).unwrap();
    assert!(!path.join(HISTORY_FILE).exists());

    let db = Engine::open(&path, Options::default()).unwrap();
    // Opening a legacy store materializes the derived index on its own.
    assert!(path.join(HISTORY_FILE).exists());
    // The journal bytes are untouched: no frame is rewritten.
    assert_eq!(fs::read(path.join(JOURNAL_FILE)).unwrap(), original_journal);

    let page = db.history(0, 10).unwrap();
    assert_eq!(page.len(), 4);
    assert!(page
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence));
    assert_eq!(page.last().unwrap().sequence, 4);
}
