mod support;

use lockmere::{
    constraints::ConstraintKind,
    session::{encode_response, parse_schema, Session},
    ConstraintMode, Engine, Error, OnUpdate, Operation, OperationResult, Options, Record, Row,
    RowId, Value,
};

use support::{get, Scratch};

const CASCADE_SCHEMA: &str = "\
table team INT code:TEXT! note:TEXT?
table member TEXT team_code:TEXT!
unique team_code team.code immediate
foreign member_team member.team_code team.code immediate on-update-cascade
";

const RESTRICT_SCHEMA: &str = "\
table team INT code:TEXT! note:TEXT?
table member TEXT team_code:TEXT!
unique team_code team.code immediate
foreign member_team member.team_code team.code immediate
";

const EXPLICIT_RESTRICT_SCHEMA: &str = "\
table team INT code:TEXT! note:TEXT?
table member TEXT team_code:TEXT!
unique team_code team.code immediate
foreign member_team member.team_code team.code immediate on-update-restrict
";

const DEFERRED_CASCADE_SCHEMA: &str = "\
table team INT code:TEXT! note:TEXT?
table member TEXT team_code:TEXT!
unique team_code team.code immediate
foreign member_team member.team_code team.code deferrable-deferred on-update-cascade
";

const CHAIN_SCHEMA: &str = "\
table team INT code:TEXT!
table member TEXT team_code:TEXT!
table tag TEXT member_ref:TEXT!
unique team_code team.code immediate
unique member_team_code member.team_code immediate
foreign member_team member.team_code team.code immediate on-update-cascade
foreign tag_member tag.member_ref member.team_code immediate on-update-cascade
";

const GUARDED_CHAIN_SCHEMA: &str = "\
table team INT code:TEXT!
table member TEXT team_code:TEXT!
table tag TEXT member_ref:TEXT!
unique team_code team.code immediate
unique member_team_code member.team_code immediate
foreign member_team member.team_code team.code immediate on-update-cascade
foreign tag_member tag.member_ref member.team_code immediate
";

fn engine(root: &Scratch, schema: &str) -> Engine {
    Engine::create(root.db(), parse_schema(schema).unwrap(), Options::default()).unwrap()
}

fn team(id: i64, code: &str) -> Operation {
    Operation::Insert(Record::new(
        RowId::new("team", id),
        Row::from([("code".to_owned(), code.into())]),
    ))
}

fn member(id: &str, team_code: &str) -> Operation {
    Operation::Insert(Record::new(
        RowId::new("member", id),
        Row::from([("team_code".to_owned(), team_code.into())]),
    ))
}

fn tag(id: &str, member_ref: &str) -> Operation {
    Operation::Insert(Record::new(
        RowId::new("tag", id),
        Row::from([("member_ref".to_owned(), member_ref.into())]),
    ))
}

fn rename(id: i64, code: &str) -> Operation {
    Operation::Update {
        id: RowId::new("team", id),
        patch: Row::from([("code".to_owned(), code.into())]),
    }
}

fn seed(db: &Engine) {
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(tx, team(1, "a")).unwrap();
    db.execute(tx, team(2, "b")).unwrap();
    db.execute(tx, member("X", "a")).unwrap();
    db.execute(tx, member("Y", "a")).unwrap();
    db.execute(tx, member("Z", "b")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

fn team_code(db: &Engine, tx: u64, id: i64) -> Value {
    get(db, tx, RowId::new("team", id)).unwrap().values["code"].clone()
}

fn member_team(db: &Engine, tx: u64, id: &str) -> Value {
    get(db, tx, RowId::new("member", id)).unwrap().values["team_code"].clone()
}

#[test]
fn restrict_is_the_default_and_rejects_referenced_updates() {
    let root = Scratch::new();
    let db = engine(&root, RESTRICT_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert!(
        matches!(db.execute(tx, rename(1, "c")), Err(Error::ConstraintViolation { name, .. }) if name == "member_team")
    );
    // The failed statement left no intents behind and the transaction is usable.
    assert_eq!(db.transaction(tx).unwrap().writes, 0);
    assert_eq!(team_code(&db, tx, 1), "a".into());
    assert_eq!(member_team(&db, tx, "X"), "a".into());
    db.execute(tx, rename(1, "a")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
}

#[test]
fn explicit_restrict_matches_the_default() {
    let root = Scratch::new();
    let db = engine(&root, EXPLICIT_RESTRICT_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert!(
        matches!(db.execute(tx, rename(1, "c")), Err(Error::ConstraintViolation { name, .. }) if name == "member_team")
    );
    db.rollback(tx).unwrap();
}

#[test]
fn cascade_rewrites_all_matching_children_only() {
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(tx, rename(1, "c")).unwrap();
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    assert_eq!(member_team(&db, tx, "Y"), "c".into());
    assert_eq!(member_team(&db, tx, "Z"), "b".into());
    let result = db.execute(tx, Operation::Commit(None)).unwrap();
    assert!(
        matches!(result, OperationResult::Committed(receipt) if receipt.writes == 3 && !receipt.replayed)
    );
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(member_team(&db, reader, "X"), "c".into());
    assert_eq!(member_team(&db, reader, "Z"), "b".into());
    db.rollback(reader).unwrap();
}

#[test]
fn untouched_columns_and_unchanged_values_do_not_cascade() {
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(
        tx,
        Operation::Update {
            id: RowId::new("team", 1),
            patch: Row::from([("note".to_owned(), "seen".into())]),
        },
    )
    .unwrap();
    db.execute(tx, rename(1, "a")).unwrap();
    assert_eq!(db.transaction(tx).unwrap().writes, 1);
    assert_eq!(member_team(&db, tx, "X"), "a".into());
    db.execute(tx, Operation::Commit(None)).unwrap();
}

#[test]
fn cascade_is_atomic_when_a_child_constraint_fails() {
    let root = Scratch::new();
    let db = engine(&root, GUARDED_CHAIN_SCHEMA);
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(tx, team(1, "a")).unwrap();
    db.execute(tx, member("X", "a")).unwrap();
    db.execute(tx, tag("T", "a")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();

    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    // The cascaded member update leaves tag T referencing a missing member.
    assert!(
        matches!(db.execute(tx, rename(1, "b")), Err(Error::ConstraintViolation { name, .. }) if name == "tag_member")
    );
    // The whole statement is withdrawn: parent and child intents are gone.
    assert_eq!(db.transaction(tx).unwrap().writes, 0);
    assert_eq!(team_code(&db, tx, 1), "a".into());
    assert_eq!(member_team(&db, tx, "X"), "a".into());
    // The transaction survives: remove the dangling tag, then the cascade succeeds whole.
    db.execute(tx, Operation::Delete(RowId::new("tag", "T")))
        .unwrap();
    db.execute(tx, rename(1, "b")).unwrap();
    db.execute(tx, tag("T", "b")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(member_team(&db, reader, "X"), "b".into());
    assert_eq!(
        get(&db, reader, RowId::new("tag", "T")).unwrap().values["member_ref"],
        "b".into()
    );
    db.rollback(reader).unwrap();
}

#[test]
fn cascade_chains_through_declared_children() {
    let root = Scratch::new();
    let db = engine(&root, CHAIN_SCHEMA);
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(tx, team(1, "a")).unwrap();
    db.execute(tx, member("X", "a")).unwrap();
    db.execute(tx, tag("T", "a")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();

    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(tx, rename(1, "b")).unwrap();
    assert_eq!(member_team(&db, tx, "X"), "b".into());
    assert_eq!(
        get(&db, tx, RowId::new("tag", "T")).unwrap().values["member_ref"],
        "b".into()
    );
    let result = db.execute(tx, Operation::Commit(None)).unwrap();
    assert!(matches!(result, OperationResult::Committed(receipt) if receipt.writes == 3));
}

#[test]
fn deferred_foreign_key_still_cascades_at_statement_time() {
    let root = Scratch::new();
    let db = engine(&root, DEFERRED_CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(tx, rename(1, "c")).unwrap();
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    db.execute(tx, Operation::Commit(None)).unwrap();
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(member_team(&db, reader, "Y"), "c".into());
    assert_eq!(member_team(&db, reader, "Z"), "b".into());
    db.rollback(reader).unwrap();
}

#[test]
fn cascade_rolls_back_to_a_savepoint() {
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(tx, Operation::Savepoint("before".to_owned()))
        .unwrap();
    db.execute(tx, rename(1, "c")).unwrap();
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    db.execute(tx, Operation::RollbackTo("before".to_owned()))
        .unwrap();
    assert_eq!(db.transaction(tx).unwrap().writes, 0);
    assert_eq!(team_code(&db, tx, 1), "a".into());
    assert_eq!(member_team(&db, tx, "X"), "a".into());
    db.execute(tx, rename(2, "d")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(team_code(&db, reader, 1), "a".into());
    assert_eq!(member_team(&db, reader, "Z"), "d".into());
    db.rollback(reader).unwrap();
}

#[test]
fn committed_cascade_survives_reopen_and_checkpoint() {
    let root = Scratch::new();
    let path = root.db();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(tx, rename(1, "c")).unwrap();
    db.execute(tx, Operation::Commit(None)).unwrap();
    assert_eq!(db.checkpoint().unwrap().sequence, 2);
    drop(db);

    let db = Engine::open(&path, Options::default()).unwrap();
    // The on-update action round-trips through the journal schema header.
    let constraint = db.schema().constraint("member_team").unwrap();
    assert!(
        matches!(constraint.kind, ConstraintKind::ForeignKey { on_update: OnUpdate::Cascade, .. })
    );
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(team_code(&db, tx, 1), "c".into());
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    assert_eq!(member_team(&db, tx, "Y"), "c".into());
    assert_eq!(member_team(&db, tx, "Z"), "b".into());
    db.rollback(tx).unwrap();
    // The cascaded rows are part of the durable commit frame.
    let history = db.history(0, 10).unwrap();
    let frame = history.iter().find(|frame| frame.sequence == 2).unwrap();
    assert_eq!(frame.changes.len(), 3);
    drop(db);

    let db = Engine::open(&path, Options::default()).unwrap();
    let tx = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    db.rollback(tx).unwrap();
}

#[test]
fn concurrent_old_reference_insert_restricts_the_commit_set() {
    // Inserter commits between the cascade statement and the cascade commit:
    // the cascade commit is rejected and the transaction repairs by reverting.
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let cascade = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    let inserter = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(cascade, rename(1, "c")).unwrap();
    db.execute(inserter, member("W", "a")).unwrap();
    db.execute(inserter, Operation::Commit(None)).unwrap();
    assert!(
        matches!(db.execute(cascade, Operation::Commit(None)), Err(Error::ConstraintViolation { name, .. }) if name == "member_team")
    );
    db.execute(cascade, rename(1, "a")).unwrap();
    db.execute(cascade, Operation::Commit(None)).unwrap();
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(team_code(&db, reader, 1), "a".into());
    assert_eq!(member_team(&db, reader, "W"), "a".into());
    assert_eq!(member_team(&db, reader, "X"), "a".into());
    db.rollback(reader).unwrap();
}

#[test]
fn cascade_commit_first_rejects_the_stale_inserter() {
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let cascade = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    let inserter = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(cascade, rename(1, "c")).unwrap();
    db.execute(inserter, member("W", "a")).unwrap();
    db.execute(cascade, Operation::Commit(None)).unwrap();
    assert!(
        matches!(db.execute(inserter, Operation::Commit(None)), Err(Error::ConstraintViolation { name, .. }) if name == "member_team")
    );
    db.rollback(inserter).unwrap();
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(member_team(&db, reader, "X"), "c".into());
    assert!(get(&db, reader, RowId::new("member", "W")).is_none());
    db.rollback(reader).unwrap();
}

#[test]
fn old_reference_committed_before_the_statement_aborts_the_cascade() {
    let root = Scratch::new();
    let db = engine(&root, CASCADE_SCHEMA);
    seed(&db);
    let cascade = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    let inserter = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    db.execute(inserter, member("W", "a")).unwrap();
    db.execute(inserter, Operation::Commit(None)).unwrap();
    assert!(matches!(
        db.execute(cascade, rename(1, "c")),
        Err(Error::Serialization { transaction, .. }) if transaction == cascade
    ));
    assert_eq!(
        db.transaction(cascade),
        Err(Error::UnknownTransaction(cascade))
    );
    let reader = db.begin(lockmere::Isolation::Snapshot).unwrap().id;
    assert_eq!(team_code(&db, reader, 1), "a".into());
    assert_eq!(member_team(&db, reader, "W"), "a".into());
    db.rollback(reader).unwrap();
}

#[test]
fn schema_config_parses_on_update_actions() {
    let action = |source: &str, name: &str| {
        let schema = parse_schema(source).unwrap();
        match schema.constraint(name).unwrap().kind {
            ConstraintKind::ForeignKey { on_update, .. } => on_update,
            _ => panic!("expected a foreign key"),
        }
    };
    assert_eq!(action(RESTRICT_SCHEMA, "member_team"), OnUpdate::Restrict);
    assert_eq!(
        action(EXPLICIT_RESTRICT_SCHEMA, "member_team"),
        OnUpdate::Restrict
    );
    assert_eq!(action(CASCADE_SCHEMA, "member_team"), OnUpdate::Cascade);
    assert!(parse_schema(
        "table team INT code:TEXT!\nforeign f team.code team.id immediate on-update-banana"
    )
    .is_err());
    assert!(parse_schema(
        "table team INT code:TEXT!\nforeign f team.code team.id immediate on-update-cascade extra"
    )
    .is_err());
}

#[test]
fn show_schema_reports_the_on_update_action() {
    let root = Scratch::new();
    let db = std::sync::Arc::new(engine(&root, CASCADE_SCHEMA));
    let mut client = Session::new(db, "client").unwrap();
    let response = client.execute("s", "SHOW SCHEMA");
    let encoded = encode_response("s", &response);
    assert!(encoded.contains(r#""name":"member_team""#));
    assert!(encoded.contains(r#""on_update":"cascade""#));
    assert!(encoded.contains(r#""on_update":null"#));
}

#[test]
fn set_constraints_mode_is_unaffected_by_cascade_actions() {
    let root = Scratch::new();
    let db = engine(&root, DEFERRED_CASCADE_SCHEMA);
    seed(&db);
    let tx = db.begin(lockmere::Isolation::Serializable).unwrap().id;
    db.execute(
        tx,
        Operation::SetConstraints {
            name: Some("member_team".to_owned()),
            mode: ConstraintMode::Immediate,
        },
    )
    .unwrap();
    db.execute(tx, rename(1, "c")).unwrap();
    assert_eq!(member_team(&db, tx, "X"), "c".into());
    db.execute(tx, Operation::Commit(None)).unwrap();
}
