mod support;

use lockmere::{
    Column, Engine, Error, Isolation, KeyKind, Operation, OperationResult, Options, Record, Row,
    RowId, Schema, Step, Table, Value,
};

use support::{get, Scratch};

#[derive(Default)]
struct Client {
    tx: u64,
    step: usize,
    observed: Option<i64>,
    wait: Option<lockmere::locks::WaitTicket>,
    committed: bool,
}

#[test]
fn all_small_serializable_histories_match_an_independent_serial_order() {
    let mut histories = Vec::new();
    enumerate(&mut Vec::new(), [3, 3], &mut histories);
    assert_eq!(histories.len(), 20);
    for ordering in histories {
        let (clients, values) = exercise(Isolation::Serializable, &ordering);
        assert!(
            has_serial_order(&clients, values),
            "{ordering:?} {values:?}"
        );
    }
}

#[test]
fn snapshot_write_skew_is_visible_to_oracle() {
    let (clients, values) = exercise(Isolation::Snapshot, &[0, 1, 0, 1, 0, 1]);
    assert_eq!(values, [0, 0]);
    assert!(clients.iter().all(|client| client.committed));
    assert!(!has_serial_order(&clients, values));
}

fn enumerate(prefix: &mut Vec<usize>, mut left: [usize; 2], result: &mut Vec<Vec<usize>>) {
    if left == [0, 0] {
        result.push(prefix.clone());
        return;
    }
    for id in 0..2 {
        if left[id] == 0 {
            continue;
        }
        left[id] -= 1;
        prefix.push(id);
        enumerate(prefix, left, result);
        prefix.pop();
        left[id] += 1;
    }
}

fn exercise(isolation: Isolation, order: &[usize]) -> ([Client; 2], [i64; 2]) {
    let scratch = Scratch::new();
    let table = Table::new("doctor", KeyKind::Integer)
        .column("available", Column::new(KeyKind::Integer, true));
    let schema = Schema::new(vec![table], vec![]).unwrap();
    let db = Engine::create(scratch.db(), schema, Options::default()).unwrap();
    let setup = db.begin(Isolation::Snapshot).unwrap().id;
    for index in 0..2 {
        db.execute(
            setup,
            Operation::Insert(Record::new(row(index), available(1))),
        )
        .unwrap();
    }
    db.execute(setup, Operation::Commit(None)).unwrap();
    let mut clients: [Client; 2] = std::array::from_fn(|_| Client {
        tx: db.begin(isolation).unwrap().id,
        ..Client::default()
    });
    for &index in order {
        next(&db, index, &mut clients[index]);
    }
    while clients.iter().any(|client| client.step != 3) {
        let left = next(&db, 0, &mut clients[0]);
        let right = next(&db, 1, &mut clients[1]);
        assert!(left || right, "schedule made no progress");
    }
    let reader = db.begin(Isolation::Snapshot).unwrap().id;
    let final_state = std::array::from_fn(|index| extract(get(&db, reader, row(index)).unwrap()));
    (clients, final_state)
}

fn next(db: &Engine, index: usize, client: &mut Client) -> bool {
    if client.step == 3 {
        return false;
    }
    let response = if let Some(ticket) = client.wait {
        db.resume(ticket)
    } else {
        let operation = match client.step {
            0 => Operation::Get(row(1 - index)),
            1 if client.observed == Some(1) => Operation::Update {
                id: row(index),
                patch: available(0),
            },
            1 => {
                client.step += 1;
                return true;
            }
            2 => Operation::Commit(None),
            _ => unreachable!(),
        };
        db.start(client.tx, operation)
    };
    match response {
        Ok(Step::Waiting(ticket)) => {
            client.wait = Some(ticket);
            false
        }
        Ok(Step::Complete(result)) => {
            match result {
                OperationResult::Row(Some(record)) => client.observed = Some(extract(record)),
                OperationResult::Committed(_) => client.committed = true,
                OperationResult::Changed { .. } => {}
                other => panic!("unexpected response {other:?}"),
            }
            client.wait = None;
            client.step += 1;
            true
        }
        Err(Error::Deadlock { .. } | Error::Serialization { .. }) => {
            client.wait = None;
            client.step = 3;
            true
        }
        Err(other) => panic!("unexpected error {other}"),
    }
}

fn has_serial_order(clients: &[Client; 2], observed_final: [i64; 2]) -> bool {
    [[0, 1], [1, 0]].into_iter().any(|order| {
        let mut state = [1, 1];
        for index in order {
            if clients[index].committed {
                let seen = state[1 - index];
                if clients[index].observed != Some(seen) {
                    return false;
                }
                if seen == 1 {
                    state[index] = 0;
                }
            }
        }
        state == observed_final
    })
}

fn row(i: usize) -> RowId {
    RowId::new("doctor", i as i64)
}

fn available(value: i64) -> Row {
    Row::from([("available".to_owned(), Value::Integer(value))])
}

fn extract(record: Record) -> i64 {
    match record.values["available"] {
        Value::Integer(value) => value,
        _ => panic!("expected availability"),
    }
}
