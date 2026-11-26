mod support;

use std::{
    ops::Bound,
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use lockmere::{
    constraints::{CheckOutcome, Phase},
    locks::Blocker,
    ConstraintMode, Error, Event,
    Isolation::{Serializable, Snapshot},
    Key, Operation, OperationResult, ReadInterval, Record, Row, RowId, Step, Value,
};

use support::{badge, branch, engine, get, seed, Scratch};

#[test]
fn ordered_scan_merges_pending_inserts_updates_and_deletes() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let tx = db.begin(Snapshot).unwrap().id;
    db.execute(
        tx,
        Operation::Update {
            id: RowId::new("branch", 1),
            patch: Row::from([("code".to_owned(), "east".into())]),
        },
    )
    .unwrap();
    db.execute(tx, Operation::Delete(RowId::new("branch", 2)))
        .unwrap();
    db.execute(tx, branch(3, "west")).unwrap();
    let range = ReadInterval::new(
        "branch",
        Bound::Included(1.into()),
        Bound::Excluded(4.into()),
    );
    let OperationResult::Rows(rows) = db
        .execute(
            tx,
            Operation::Scan {
                interval: range,
                limit: 3,
            },
        )
        .unwrap()
    else {
        panic!("expected range rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.id.key.clone())
            .collect::<Vec<_>>(),
        [1.into(), 3.into()]
    );
    assert_eq!(rows[0].values["code"], "east".into());
    assert_eq!(rows[1].values["code"], "west".into());
    db.rollback(tx).unwrap();
}

#[test]
fn text_keys_have_stable_case_sensitive_order() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let tx = db.begin(Snapshot).unwrap().id;
    for key in ["z", "Á", "Z", "", "a"] {
        db.execute(tx, badge(key, 1)).unwrap();
    }
    let OperationResult::Rows(rows) = db
        .execute(
            tx,
            Operation::Scan {
                interval: ReadInterval::new(
                    "badge",
                    Bound::Included("".into()),
                    Bound::Excluded("z".into()),
                ),
                limit: 100,
            },
        )
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.id.key.clone())
            .collect::<Vec<_>>(),
        ["".into(), "Z".into(), "a".into(), "seed".into()]
    );
    assert_eq!(rows[0].values["note"], Value::Null);
}

#[test]
fn checkpoint_compaction_respects_oldest_snapshot() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let old = db.begin(Snapshot).unwrap().id;
    let row = RowId::new("branch", 2);
    for code in ["newer", "latest"] {
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(
            writer,
            Operation::Update {
                id: row.clone(),
                patch: Row::from([("code".to_owned(), code.into())]),
            },
        )
        .unwrap();
        db.execute(writer, Operation::Commit(None)).unwrap();
    }
    assert_eq!(db.checkpoint().unwrap().collected_versions, 0);
    assert_eq!(get(&db, old, row).unwrap().values["code"], "south".into());
    db.rollback(old).unwrap();
    assert_eq!(db.checkpoint().unwrap().collected_versions, 2);
}

#[test]
fn checkpoint_reclamation_only_touches_revised_hot_keys() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);

    // A field of mostly cold, single-version rows; none can ever be reclaimed.
    let cold_count = 64u64;
    let cold_tx = db.begin(Snapshot).unwrap().id;
    for key in 10..10 + cold_count as i64 {
        let code = format!("cold-{key}");
        db.execute(
            cold_tx,
            Operation::Insert(Record::new(
                RowId::new("branch", key),
                Row::from([("code".to_owned(), Value::Text(code))]),
            )),
        )
        .unwrap();
    }
    db.execute(cold_tx, Operation::Commit(None)).unwrap();

    // A long-lived reader pins the committed sequence before the hot revisions.
    let reader = db.begin(Snapshot).unwrap().id;
    let pinned_snapshot = db.transaction(reader).unwrap().snapshot;

    // Only two hot keys accumulate history.
    let hot_a = RowId::new("branch", 1);
    let hot_b = RowId::new("branch", 2);
    for code in ["a2", "a3"] {
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(
            writer,
            Operation::Update {
                id: hot_a.clone(),
                patch: Row::from([("code".to_owned(), code.into())]),
            },
        )
        .unwrap();
        db.execute(writer, Operation::Commit(None)).unwrap();
    }
    for code in ["b2", "b3"] {
        let writer = db.begin(Snapshot).unwrap().id;
        db.execute(
            writer,
            Operation::Update {
                id: hot_b.clone(),
                patch: Row::from([("code".to_owned(), code.into())]),
            },
        )
        .unwrap();
        db.execute(writer, Operation::Commit(None)).unwrap();
    }

    // While the reader is pinned, no obsolete version is collectible and the
    // reader still observes the pre-revision state.
    assert_eq!(db.checkpoint().unwrap().collected_versions, 0);
    assert_eq!(
        get(&db, reader, hot_a.clone()).unwrap().values["code"],
        "north".into()
    );
    assert_eq!(
        get(&db, reader, hot_b.clone()).unwrap().values["code"],
        "south".into()
    );
    for key in 10..10 + cold_count as i64 {
        assert!(get(&db, reader, RowId::new("branch", key)).is_some());
    }

    // Once the old reader ends, only the four obsolete versions of the two hot
    // keys are collected; cold single-version heads stay intact.
    db.rollback(reader).unwrap();
    assert_eq!(db.checkpoint().unwrap().collected_versions, 4);
    // Repeated checkpoints have nothing left to reclaim.
    assert_eq!(db.checkpoint().unwrap().collected_versions, 0);

    // Committed heads are retained permanently, including the cold keys.
    let current = db.begin(Snapshot).unwrap().id;
    assert_eq!(
        get(&db, current, hot_a).unwrap().values["code"],
        "a3".into()
    );
    assert_eq!(
        get(&db, current, hot_b).unwrap().values["code"],
        "b3".into()
    );
    for key in 10..10 + cold_count as i64 {
        let record = get(&db, current, RowId::new("branch", key)).unwrap();
        assert_eq!(record.values["code"], Value::Text(format!("cold-{key}")));
    }
    assert_eq!(
        db.transaction(current).unwrap().snapshot,
        pinned_snapshot + 4
    );
    db.rollback(current).unwrap();
}

#[test]
fn a_waiting_writer_is_aborted_if_its_base_changes() {
    let root = Scratch::new();
    let db = engine(root.db());
    let left = db.begin(Snapshot).unwrap().id;
    let right = db.begin(Snapshot).unwrap().id;
    db.execute(left, branch(7, "east")).unwrap();
    let Step::Waiting(wait) = db.start(right, branch(7, "west")).unwrap() else {
        panic!("write must wait");
    };
    db.execute(left, Operation::Commit(None)).unwrap();
    assert!(
        matches!(db.resume(wait), Err(Error::Serialization { transaction, .. }) if transaction == right)
    );
    assert_eq!(db.transaction(right), Err(Error::UnknownTransaction(right)));
}

#[test]
fn serial_predicates_protect_excluded_and_included_endpoints() {
    let root = Scratch::new();
    let db = engine(root.db());
    let reader = db.begin(Serializable).unwrap().id;
    let writer = db.begin(Snapshot).unwrap().id;
    let interval = ReadInterval::new(
        "branch",
        Bound::Excluded(4.into()),
        Bound::Included(8.into()),
    );
    db.execute(reader, Operation::Scan { interval, limit: 1 })
        .unwrap();
    db.execute(writer, branch(4, "boundary")).unwrap();
    let Step::Waiting(ticket) = db.start(writer, branch(8, "inside")).unwrap() else {
        panic!("phantom must wait");
    };
    assert_eq!(db.wait_graph().unwrap().edges()[0].blocking, reader);
    db.execute(reader, Operation::Commit(None)).unwrap();
    assert!(matches!(db.resume(ticket), Ok(Step::Complete(_))));
    db.execute(writer, Operation::Commit(None)).unwrap();
}

#[test]
fn empty_interval_does_not_block_any_writer() {
    let root = Scratch::new();
    let db = engine(root.db());
    let reader = db.begin(Serializable).unwrap().id;
    let writer = db.begin(Snapshot).unwrap().id;
    let interval = ReadInterval::new(
        "branch",
        Bound::Excluded(2.into()),
        Bound::Excluded(2.into()),
    );
    assert_eq!(
        db.execute(reader, Operation::Scan { interval, limit: 1 })
            .unwrap(),
        OperationResult::Rows(vec![])
    );
    assert!(matches!(
        db.start(writer, branch(2, "west")),
        Ok(Step::Complete(_))
    ));
    assert!(db.wait_graph().unwrap().edges().is_empty());
}

#[test]
fn serial_predicate_rejects_a_committed_tombstone_at_a_missing_key() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let reader = db.begin(Serializable).unwrap().id;
    let writer = db.begin(Snapshot).unwrap().id;
    let row = RowId::new("branch", 2);
    db.execute(writer, Operation::Delete(row.clone())).unwrap();
    db.execute(writer, Operation::Commit(None)).unwrap();
    assert!(matches!(
        db.execute(reader, Operation::Get(row)),
        Err(Error::Serialization { .. })
    ));
}

#[test]
fn closing_a_deadlock_cycle_aborts_requester_and_wakes_other_holder() {
    let root = Scratch::new();
    let db = engine(root.db());
    let left = db.begin(Serializable).unwrap().id;
    let right = db.begin(Serializable).unwrap().id;
    db.execute(left, branch(1, "left")).unwrap();
    db.execute(right, branch(2, "right")).unwrap();
    let Step::Waiting(ticket) = db.start(left, branch(2, "second")).unwrap() else {
        panic!("wait expected");
    };
    assert_eq!(db.wait_graph().unwrap().edges()[0].reason, Blocker::Granted);
    assert_eq!(
        db.start(right, branch(1, "first")),
        Err(Error::Deadlock {
            victim: right,
            cycle: vec![right, left, right],
        })
    );
    assert!(db.wait_graph().unwrap().edges().is_empty());
    assert!(matches!(db.resume(ticket).unwrap(), Step::Complete(_)));
    db.execute(left, Operation::Commit(None)).unwrap();
    let (_, _, events) = db.events(0).unwrap();
    assert!(events
        .iter()
        .any(|(_, event)| matches!(event, Event::Ready { ticket: value } if *value == ticket)));
}

#[test]
fn earlier_conflicting_waiters_cannot_be_overtaken() {
    let root = Scratch::new();
    let db = engine(root.db());
    let a = db.begin(Serializable).unwrap().id;
    let b = db.begin(Snapshot).unwrap().id;
    let c = db.begin(Serializable).unwrap().id;
    let row = RowId::new("branch", 7);
    assert!(get(&db, a, row.clone()).is_none());
    let Step::Waiting(first) = db.start(b, branch(7, "locked")).unwrap() else {
        panic!("first wait");
    };
    let Step::Waiting(second) = db.start(c, Operation::Get(row)).unwrap() else {
        panic!("second wait");
    };
    assert!(db
        .wait_graph()
        .unwrap()
        .edges()
        .iter()
        .any(|edge| edge.waiting == c
            && edge.blocking == b
            && edge.reason == Blocker::EarlierWaiter));
    db.rollback(a).unwrap();
    assert_eq!(db.resume(second), Ok(Step::Waiting(second)));
    assert!(matches!(db.resume(first), Ok(Step::Complete(_))));
    db.rollback(b).unwrap();
    assert_eq!(
        db.resume(second),
        Ok(Step::Complete(OperationResult::Row(None)))
    );
}

#[test]
fn timeout_removes_pending_request_but_preserves_transaction() {
    let root = Scratch::new();
    let db = engine(root.db());
    let owner = db.begin(Snapshot).unwrap().id;
    let client = db.begin(Snapshot).unwrap().id;
    db.execute(owner, branch(1, "locked")).unwrap();
    let Step::Waiting(ticket) = db.start(client, branch(1, "wait")).unwrap() else {
        panic!("wait expected");
    };
    assert_eq!(
        db.wait(ticket, Duration::ZERO),
        Err(Error::LockTimeout(client))
    );
    assert_eq!(db.transaction(client).unwrap().waiting, None);
    assert!(db.wait_graph().unwrap().edges().is_empty());
    db.rollback(owner).unwrap();
    assert!(matches!(
        db.start(client, branch(1, "retry")),
        Ok(Step::Complete(_))
    ));
}

#[test]
fn blocked_thread_completes_after_foreign_thread_rolls_back() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let owner = db.begin(Snapshot).unwrap().id;
    let client = db.begin(Snapshot).unwrap().id;
    db.execute(owner, branch(1, "locked")).unwrap();
    let Step::Waiting(ticket) = db.start(client, branch(1, "wait")).unwrap() else {
        panic!("wait expected");
    };
    let (send, receive) = mpsc::channel();
    let waiter = {
        let db = db.clone();
        thread::spawn(move || {
            send.send(()).unwrap();
            db.wait(ticket, Duration::from_secs(3))
        })
    };
    receive.recv_timeout(Duration::from_secs(3)).unwrap();
    db.rollback(owner).unwrap();
    assert!(matches!(
        waiter.join().unwrap(),
        Ok(OperationResult::Changed { .. })
    ));
}

#[test]
fn deferred_foreign_key_can_be_repaired_after_failed_commit() {
    let root = Scratch::new();
    let db = engine(root.db());
    let tx = db.begin(Serializable).unwrap().id;
    db.execute(tx, badge("early", 2)).unwrap();
    assert!(
        matches!(db.execute(tx, Operation::Commit(None)), Err(Error::ConstraintViolation { name, .. }) if name == "badge_branch")
    );
    assert_eq!(db.sequence().unwrap(), 0);
    db.execute(tx, branch(2, "parent")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
    assert_eq!(db.sequence().unwrap(), 1);
    let (_, _, events) = db.events(0).unwrap();
    assert!(events.iter().any(|(_, event)| matches!(event, Event::Check { result, .. }
        if result.phase == Phase::Commit && matches!(result.outcome, CheckOutcome::MissingParent { .. }))));
}

#[test]
fn rollback_to_restores_constraint_mode_and_discarded_intents() {
    let root = Scratch::new();
    let db = engine(root.db());
    let tx = db.begin(Serializable).unwrap().id;
    db.execute(tx, Operation::Savepoint("outer".to_owned()))
        .unwrap();
    db.execute(
        tx,
        Operation::SetConstraints {
            name: None,
            mode: ConstraintMode::Immediate,
        },
    )
    .unwrap();
    assert!(matches!(
        db.execute(tx, badge("missing", 9)),
        Err(Error::ConstraintViolation { .. })
    ));
    db.execute(tx, branch(4, "rollback")).unwrap();
    db.execute(tx, Operation::Savepoint("inner".to_owned()))
        .unwrap();
    db.execute(tx, Operation::RollbackTo("outer".to_owned()))
        .unwrap();
    assert_eq!(db.transaction(tx).unwrap().savepoints, 1);
    assert!(get(&db, tx, RowId::new("branch", 4)).is_none());
    db.execute(tx, badge("parent_later", 9)).unwrap();
    assert_eq!(db.transaction(tx).unwrap().writes, 1);
    db.execute(tx, branch(9, "parent")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

#[test]
fn duplicate_unique_fails_statement_without_leaving_intent() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let tx = db.begin(Snapshot).unwrap().id;
    assert!(
        matches!(db.execute(tx, branch(4, "north")), Err(Error::ConstraintViolation { name, .. }) if name == "branch_code")
    );
    assert_eq!(db.transaction(tx).unwrap().writes, 0);
    assert!(get(&db, tx, RowId::new("branch", 4)).is_none());
    db.execute(tx, branch(4, "elsewhere")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

#[test]
fn commit_rechecks_unique_and_foreign_keys_against_latest_state() {
    let root = Scratch::new();
    let db = engine(root.db());
    seed(&db);
    let left = db.begin(Snapshot).unwrap().id;
    let right = db.begin(Snapshot).unwrap().id;
    db.execute(left, branch(10, "shared")).unwrap();
    db.execute(right, branch(11, "shared")).unwrap();
    db.execute(left, Operation::Commit(None)).unwrap();
    assert!(
        matches!(db.execute(right, Operation::Commit(None)), Err(Error::ConstraintViolation { name, .. }) if name == "branch_code")
    );
    db.rollback(right).unwrap();

    let parent = db.begin(Snapshot).unwrap().id;
    let child = db.begin(Snapshot).unwrap().id;
    db.execute(parent, Operation::Delete(RowId::new("branch", 2)))
        .unwrap();
    db.execute(child, badge("arrived", 2)).unwrap();
    db.execute(child, Operation::Commit(None)).unwrap();
    assert!(
        matches!(db.execute(parent, Operation::Commit(None)), Err(Error::ConstraintViolation { name, .. }) if name == "badge_branch")
    );
}

#[test]
fn pending_insert_deleted_before_commit_emits_no_invalid_tombstone() {
    let root = Scratch::new();
    let db = engine(root.db());
    let tx = db.begin(Snapshot).unwrap().id;
    db.execute(tx, branch(9, "temporary")).unwrap();
    db.execute(tx, Operation::Delete(RowId::new("branch", 9)))
        .unwrap();
    assert_eq!(db.transaction(tx).unwrap().writes, 0);
    db.execute(tx, Operation::Commit(None)).unwrap();
    assert!(db.history(0, 1).unwrap()[0].changes.is_empty());
    assert_eq!(Key::from(9), Key::Integer(9));
}
