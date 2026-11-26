mod support;

use std::sync::Arc;

use lockmere::{
    session::{encode_response, parse, Command as Sql, Response, Session},
    Error, Isolation, Operation, OperationResult, ReadInterval, RowId,
};
use std::ops::Bound;

use support::{badge, branch, engine, get, seed, Scratch};

fn insert_and_commit(db: &lockmere::Engine, statement: Operation) -> u64 {
    let tx = db.begin(Isolation::Snapshot).unwrap().id;
    db.execute(tx, statement).unwrap();
    match db.execute(tx, Operation::Commit(None)).unwrap() {
        OperationResult::Committed(result) => result.sequence,
        other => panic!("expected commit, got {other:?}"),
    }
}

#[test]
fn parser_accepts_history_begin_forms() {
    match parse("BEGIN SNAPSHOT AT 3").unwrap() {
        Sql::BeginHistory { sequence } => assert_eq!(sequence, 3),
        other => panic!("expected BeginHistory, got {other:?}"),
    }
    assert!(matches!(
        parse("BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT AT 7;").unwrap(),
        Sql::BeginHistory { sequence: 7 }
    ));
    assert!(matches!(
        parse("BEGIN AT 1").unwrap(),
        Sql::BeginHistory { sequence: 1 }
    ));
    // Plain begins still parse to a live transaction command.
    assert!(matches!(parse("BEGIN").unwrap(), Sql::Begin(_)));
    assert!(matches!(parse("BEGIN SNAPSHOT").unwrap(), Sql::Begin(_)));
}

#[test]
fn point_reads_return_rows_that_existed_at_each_sequence() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db); // sequence 1: branch 1, branch 2, badge 'seed'
    let seq2 = insert_and_commit(&db, branch(3, "west"));
    assert_eq!(seq2, 2);
    let seq3 = insert_and_commit(
        &db,
        Operation::Update {
            id: RowId::new("branch", 3),
            patch: std::collections::BTreeMap::from([(
                "code".to_owned(),
                lockmere::Value::Text("east".into()),
            )]),
        },
    );
    assert_eq!(seq3, 3);
    let seq4 = insert_and_commit(&db, Operation::Delete(RowId::new("branch", 3)));
    assert_eq!(seq4, 4);

    // Sequence 0 is the empty pre-commit view: every point read misses.
    let view0 = db.begin_history(0).unwrap();
    assert!(view0.read_only);
    assert_eq!(view0.snapshot, 0);
    assert!(get(&db, view0.id, RowId::new("branch", 1)).is_none());
    db.rollback(view0.id).unwrap();

    // At sequence 2 the row exists with its first value.
    let view2 = db.begin_history(2).unwrap();
    let row = get(&db, view2.id, RowId::new("branch", 3)).unwrap();
    assert_eq!(row.values["code"], "west".into());
    // The update at sequence 3 is invisible to the older view.
    let live = db.begin(Isolation::Snapshot).unwrap();
    let current = get(&db, live.id, RowId::new("branch", 3));
    // Live head is the tombstone (sequence 4 deleted it).
    assert!(current.is_none());
    db.rollback(live.id).unwrap();

    // At sequence 3 the updated value is visible.
    let view3 = db.begin_history(3).unwrap();
    let row = get(&db, view3.id, RowId::new("branch", 3)).unwrap();
    assert_eq!(row.values["code"], "east".into());

    // At sequence 4 the delete is in effect: the point read misses.
    let view4 = db.begin_history(4).unwrap();
    assert!(get(&db, view4.id, RowId::new("branch", 3)).is_none());
    // Earlier rows remain readable in the newest view.
    assert!(get(&db, view4.id, RowId::new("branch", 1)).is_some());

    // A deleted-then-reinserted row is interpreted per its effective sequence.
    let seq5 = insert_and_commit(&db, branch(3, "reborn"));
    assert_eq!(seq5, 5);
    let view5 = db.begin_history(5).unwrap();
    let row = get(&db, view5.id, RowId::new("branch", 3)).unwrap();
    assert_eq!(row.values["code"], "reborn".into());
    assert!(get(&db, view4.id, RowId::new("branch", 3)).is_none());
}

#[test]
fn range_scans_return_only_rows_live_in_the_pinned_view() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db); // branch 1, 2
    insert_and_commit(&db, branch(3, "west")); // seq 2
    insert_and_commit(&db, Operation::Delete(RowId::new("branch", 2))); // seq 3
    insert_and_commit(&db, branch(4, "east")); // seq 4

    let interval = ReadInterval::whole("branch");
    let scan = |view: &lockmere::TransactionInfo| match db
        .execute(
            view.id,
            Operation::Scan {
                interval: interval.clone(),
                limit: 100,
            },
        )
        .unwrap()
    {
        OperationResult::Rows(rows) => rows
            .into_iter()
            .map(|record| match record.id.key {
                lockmere::Key::Integer(n) => n,
                other => panic!("unexpected key {other:?}"),
            })
            .collect::<Vec<_>>(),
        other => panic!("expected rows, got {other:?}"),
    };

    let view1 = db.begin_history(1).unwrap();
    assert_eq!(scan(&view1), [1, 2]);

    let view2 = db.begin_history(2).unwrap();
    assert_eq!(scan(&view2), [1, 2, 3]);

    // The sequence-3 delete of branch 2 disappears from the scan.
    let view3 = db.begin_history(3).unwrap();
    assert_eq!(scan(&view3), [1, 3]);

    // The newest view matches the committed head.
    let view4 = db.begin_history(4).unwrap();
    assert_eq!(scan(&view4), [1, 3, 4]);

    // Bounded intervals honor the same key semantics as live scans.
    let bounded = ReadInterval::new(
        "branch",
        Bound::Included(2.into()),
        Bound::Excluded(4.into()),
    );
    let OperationResult::Rows(rows) = db
        .execute(
            view3.id,
            Operation::Scan {
                interval: bounded,
                limit: 10,
            },
        )
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id.key, 3.into());
}

#[test]
fn future_and_invalid_sequences_are_rejected_without_creating_a_view() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    assert_eq!(db.sequence().unwrap(), 1);

    let error = db.begin_history(2).unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidSequence {
            requested: 2,
            latest: 1
        }
    ));
    assert_eq!(error.code(), "INVALID_SEQUENCE");

    // A rejected open leaves no view behind: a following valid open gets a
    // fresh identity and the active-view count is unchanged.
    let view = db.begin_history(1).unwrap();
    assert!(db.begin_history(99).is_err());
    db.rollback(view.id).unwrap();
}

#[test]
fn history_views_reject_writes_and_commit_and_never_publish() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let before = db.sequence().unwrap();
    let view = db.begin_history(1).unwrap();

    let write = branch(99, "denied");
    let error = db.execute(view.id, write).unwrap_err();
    assert!(matches!(error, Error::ReadOnlyHistory(1)));
    assert_eq!(error.code(), "READ_ONLY_HISTORY");

    for rejected in [
        Operation::Delete(RowId::new("branch", 1)),
        Operation::Update {
            id: RowId::new("branch", 1),
            patch: std::collections::BTreeMap::from([(
                "code".to_owned(),
                lockmere::Value::Text("x".into()),
            )]),
        },
        Operation::Savepoint("sp".to_owned()),
        Operation::RollbackTo("sp".to_owned()),
        Operation::Release("sp".to_owned()),
        Operation::Commit(None),
    ] {
        assert!(matches!(
            db.execute(view.id, rejected),
            Err(Error::ReadOnlyHistory(1))
        ));
    }

    // The rejected statements neither advanced the durable sequence nor
    // changed any committed row.
    assert_eq!(db.sequence().unwrap(), before);
    let audit = db.begin(Isolation::Snapshot).unwrap();
    assert!(get(&db, audit.id, RowId::new("branch", 99)).is_none());
    db.rollback(audit.id).unwrap();

    // The view stays usable after a rejected write (it is not aborted).
    assert!(get(&db, view.id, RowId::new("branch", 1)).is_some());
    let info = db.transaction(view.id).unwrap();
    assert!(info.read_only);
    assert!(info.reads >= 1);
    db.rollback(view.id).unwrap();
    assert!(matches!(
        db.transaction(view.id),
        Err(Error::UnknownTransaction(_))
    ));
}

#[test]
fn history_survives_checkpoint_version_collection_and_reopen() {
    let scratch = Scratch::new();
    let path = scratch.db();
    let db = engine(&path);
    seed(&db); // seq 1
    insert_and_commit(&db, branch(5, "temp")); // seq 2
    insert_and_commit(&db, Operation::Delete(RowId::new("branch", 5))); // seq 3
    let seq4 = insert_and_commit(&db, badge("later", 1)); // seq 4

    // Open a view at an old sequence before checkpointing.
    let old_view = db.begin_history(2).unwrap();
    db.checkpoint().unwrap();
    // Old view still serves its pinned data from its replayed index.
    let row = get(&db, old_view.id, RowId::new("branch", 5)).unwrap();
    assert_eq!(row.values["code"], "temp".into());

    // A new view after the checkpoint reconstructs the same old state even
    // though the live index collected those versions.
    let rebuilt = db.begin_history(2).unwrap();
    let row = get(&db, rebuilt.id, RowId::new("branch", 5)).unwrap();
    assert_eq!(row.values["code"], "temp".into());
    let deleted_view = db.begin_history(3).unwrap();
    assert!(get(&db, deleted_view.id, RowId::new("branch", 5)).is_none());
    let head_view = db.begin_history(seq4).unwrap();
    assert!(get(&db, head_view.id, RowId::new("badge", "later")).is_some());
    drop(db);

    // Reopening from the journal (plus checkpoint) keeps every history order.
    let db = lockmere::Engine::open(&path, lockmere::Options::default()).unwrap();
    assert_eq!(db.sequence().unwrap(), seq4);
    let reopened = db.begin_history(2).unwrap();
    let row = get(&db, reopened.id, RowId::new("branch", 5)).unwrap();
    assert_eq!(row.values["code"], "temp".into());
    let reopened_deleted = db.begin_history(3).unwrap();
    assert!(get(&db, reopened_deleted.id, RowId::new("branch", 5)).is_none());
    let reopened_head = db.begin_history(4).unwrap();
    assert!(get(&db, reopened_head.id, RowId::new("badge", "later")).is_some());
    assert!(matches!(
        db.begin_history(5),
        Err(Error::InvalidSequence {
            requested: 5,
            latest: 4
        })
    ));
}

#[test]
fn session_drives_history_views_over_the_line_protocol() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    seed(&db);
    let mut client = Session::new(db.clone(), "historian").unwrap();

    let began = client.execute("open", "BEGIN SNAPSHOT AT 1").unwrap();
    let Response::Began(info) = began else {
        panic!("expected began response");
    };
    assert!(info.read_only);
    assert_eq!(info.snapshot, 1);
    let encoded = encode_response("open", &Ok(Response::Began(info.clone())));
    assert!(encoded.contains(r#""read_only":true"#));

    // Reads work and return the pinned rows.
    let ok = client.execute("point", "GET branch 1").unwrap();
    assert!(matches!(
        ok,
        Response::Operation(OperationResult::Row(Some(_)))
    ));

    // Writes are rejected with the dedicated error and do not end the view.
    let denied = client.execute("write", "INSERT INTO branch (id, code) VALUES (42, 'nope')");
    assert!(matches!(denied, Err(Error::ReadOnlyHistory(1))));
    let encoded = encode_response("write", &denied);
    assert!(encoded.contains(r#""code":"READ_ONLY_HISTORY""#));
    assert!(encoded.contains(r#""transaction_ended":false"#));

    let commit = client.execute("try-commit", "COMMIT");
    assert!(matches!(commit, Err(Error::ReadOnlyHistory(1))));

    // A future sequence is rejected at open time once the session is idle.
    let closed = client.execute("close", "ROLLBACK").unwrap();
    assert!(matches!(closed, Response::RolledBack { .. }));
    let future = client.execute("future", "BEGIN SNAPSHOT AT 99");
    assert!(matches!(future, Err(Error::InvalidSequence { .. })));
    let encoded = encode_response("future", &future);
    assert!(encoded.contains(r#""code":"INVALID_SEQUENCE""#));

    // After the rejected open the session has no active view, so a live
    // transaction can start normally.
    client.execute("live", "BEGIN SNAPSHOT").unwrap();
    client
        .execute(
            "live-write",
            "INSERT INTO branch (id, code) VALUES (7, 'ok')",
        )
        .unwrap();
    let committed = client.execute("live-commit", "COMMIT").unwrap();
    assert!(matches!(
        committed,
        Response::Operation(OperationResult::Committed(_))
    ));
    assert_eq!(db.sequence().unwrap(), 2);
}

#[test]
fn empty_history_view_at_sequence_zero_reads_nothing() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let view = db.begin_history(0).unwrap();
    assert!(get(&db, view.id, RowId::new("branch", 1)).is_none());
    let OperationResult::Rows(rows) = db
        .execute(
            view.id,
            Operation::Scan {
                interval: ReadInterval::whole("branch"),
                limit: 10,
            },
        )
        .unwrap()
    else {
        panic!("expected rows result");
    };
    assert!(rows.is_empty());
}

#[test]
fn history_views_coexist_with_live_transactions_and_do_not_block_writes() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db); // seq 1
    insert_and_commit(&db, branch(5, "first")); // seq 2

    // A view pinned before the newest commit...
    let view = db.begin_history(1).unwrap();
    // ...coexists with a live serializable writer that advances the head.
    let writer = db.begin(Isolation::Serializable).unwrap().id;
    db.execute(writer, branch(6, "second")).unwrap();
    let sequence = match db.execute(writer, Operation::Commit(None)).unwrap() {
        OperationResult::Committed(result) => result.sequence,
        other => panic!("expected commit, got {other:?}"),
    };
    assert_eq!(sequence, 3);

    // The pinned view is unaffected by the new commit...
    assert!(get(&db, view.id, RowId::new("branch", 6)).is_none());
    assert!(get(&db, view.id, RowId::new("branch", 5)).is_none());
    assert!(get(&db, view.id, RowId::new("branch", 1)).is_some());
    // ...and a view opened now sees the new head, proving no durable state
    // was disturbed by the read-only session.
    let fresh = db.begin_history(3).unwrap();
    assert!(get(&db, fresh.id, RowId::new("branch", 6)).is_some());
    db.rollback(view.id).unwrap();
    db.rollback(fresh.id).unwrap();

    // Multiple views at different orders are independently readable.
    let v1 = db.begin_history(1).unwrap();
    let v2 = db.begin_history(2).unwrap();
    let v3 = db.begin_history(3).unwrap();
    assert!(get(&db, v1.id, RowId::new("branch", 5)).is_none());
    assert!(get(&db, v2.id, RowId::new("branch", 5)).is_some());
    assert!(get(&db, v3.id, RowId::new("branch", 6)).is_some());
}

#[test]
fn text_key_history_respects_effective_sequences() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db); // badge 'seed' at seq 1
    insert_and_commit(&db, badge("alpha", 1)); // seq 2
    insert_and_commit(&db, Operation::Delete(RowId::new("badge", "alpha"))); // seq 3
    insert_and_commit(&db, badge("alpha", 1)); // seq 4 reinsert

    let view2 = db.begin_history(2).unwrap();
    assert!(get(&db, view2.id, RowId::new("badge", "alpha")).is_some());
    let view3 = db.begin_history(3).unwrap();
    assert!(get(&db, view3.id, RowId::new("badge", "alpha")).is_none());
    let view4 = db.begin_history(4).unwrap();
    assert!(get(&db, view4.id, RowId::new("badge", "alpha")).is_some());
    assert!(get(&db, view3.id, RowId::new("badge", "seed")).is_some());
}
