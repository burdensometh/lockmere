use std::{fmt::Write, ops::Bound};

use super::Response;
use crate::{
    constraints::{ConstraintCheck, ConstraintKind, Phase, Schema, Timing},
    error::{Error, Result},
    locks::{Blocker, WaitEdge, WaitTicket},
    rows::{Key, ReadInterval, Record, RowId, Value},
    tx::{CheckpointInfo, CommitResult, Event, OperationResult, TransactionInfo},
    wal::CommitLog,
};

pub fn encode_response(request: &str, response: &Result<Response>) -> String {
    match response {
        Ok(Response::Waiting(ticket)) => object([
            ("request", quoted(request)),
            ("status", quoted("waiting")),
            ("wait", wait(ticket)),
        ]),
        Ok(response) => object([
            ("request", quoted(request)),
            ("status", quoted("ok")),
            ("result", result(response)),
        ]),
        Err(error) => object([
            ("request", quoted(request)),
            ("status", quoted("error")),
            ("error", encoded_error(error)),
        ]),
    }
}

fn result(response: &Response) -> String {
    match response {
        Response::Began(info) | Response::Transaction(info) => transaction(info),
        Response::Operation(value) => operation(value),
        Response::Waiting(ticket) => wait(ticket),
        Response::RolledBack { id } => object([
            ("kind", quoted("rolled_back")),
            ("transaction", id.to_string()),
        ]),
        Response::Checkpoint(info) => checkpoint(info),
        Response::Locks(graph) => object([("edges", collection(graph.edges().iter().map(edge)))]),
        Response::Events {
            oldest,
            newest,
            events,
        } => {
            object([
                ("oldest", oldest.to_string()),
                ("newest", newest.to_string()),
                (
                    "events",
                    collection(events.iter().map(|(id, value)| {
                        object([("id", id.to_string()), ("event", event(value))])
                    })),
                ),
            ])
        }
        Response::History(log) => object([("commits", collection(log.iter().map(commit_log)))]),
        Response::Schema(value) => schema(value),
    }
}

fn transaction(info: &TransactionInfo) -> String {
    object([
        ("transaction", info.id.to_string()),
        ("snapshot", info.snapshot.to_string()),
        ("isolation", quoted(info.isolation.name())),
        ("writes", info.writes.to_string()),
        ("savepoints", info.savepoints.to_string()),
        ("reads", info.reads.to_string()),
        (
            "waiting",
            info.waiting.as_ref().map(wait).unwrap_or_else(null),
        ),
    ])
}

fn operation(result: &OperationResult) -> String {
    match result {
        OperationResult::Row(value) => object([
            ("kind", quoted("row")),
            ("row", value.as_ref().map(record).unwrap_or_else(null)),
        ]),
        OperationResult::Rows(values) => object([
            ("kind", quoted("rows")),
            ("rows", collection(values.iter().map(record))),
        ]),
        OperationResult::Changed { id } => {
            object([("kind", quoted("changed")), ("row", row_id(id))])
        }
        OperationResult::Savepoint { name } => named("savepoint", name),
        OperationResult::RolledBack { name } => named("rolled_back_to", name),
        OperationResult::Released { name } => named("released", name),
        OperationResult::ConstraintsSet { count } => object([
            ("kind", quoted("constraints_set")),
            ("count", count.to_string()),
        ]),
        OperationResult::Committed(result) => commit(result),
    }
}

fn named(kind: &str, name: &str) -> String {
    object([("kind", quoted(kind)), ("name", quoted(name))])
}

fn checkpoint(info: &CheckpointInfo) -> String {
    object([
        ("sequence", info.sequence.to_string()),
        ("journal_bytes", info.journal_bytes.to_string()),
        ("collected_versions", info.collected_versions.to_string()),
    ])
}

fn commit(info: &CommitResult) -> String {
    object([
        ("kind", quoted("committed")),
        ("transaction", info.transaction.to_string()),
        ("sequence", info.sequence.to_string()),
        ("writes", info.writes.to_string()),
        ("replayed", info.replayed.to_string()),
    ])
}

fn commit_log(item: &CommitLog) -> String {
    object([
        ("sequence", item.sequence.to_string()),
        ("transaction", item.transaction.to_string()),
        (
            "receipt",
            item.receipt
                .as_ref()
                .map(|id| {
                    object([
                        ("client", quoted(&id.client)),
                        ("request", quoted(&id.request)),
                    ])
                })
                .unwrap_or_else(null),
        ),
        (
            "writes",
            collection(item.changes.iter().map(|change| {
                object([
                    ("row", row_id(&change.id)),
                    (
                        "values",
                        change
                            .values
                            .as_ref()
                            .map(|row| {
                                object(row.iter().map(|(name, value)| (name.as_str(), cell(value))))
                            })
                            .unwrap_or_else(null),
                    ),
                ])
            })),
        ),
    ])
}

fn event(item: &Event) -> String {
    let (event_type, payload) = match item {
        Event::Began(info) => ("began", transaction(info)),
        Event::Waiting {
            ticket,
            operation,
            interval: range,
        } => (
            "waiting",
            object([
                ("ticket", wait(ticket)),
                ("operation", quoted(operation)),
                ("interval", interval(range)),
            ]),
        ),
        Event::Ready { ticket } => ("ready", wait(ticket)),
        Event::Resumed { ticket } => ("resumed", wait(ticket)),
        Event::TimedOut { ticket } => ("timed_out", wait(ticket)),
        Event::Check {
            transaction,
            result,
        } => ("constraint", constraint_check(*transaction, result)),
        Event::Aborted {
            transaction,
            reason,
        } => (
            "aborted",
            object([
                ("transaction", transaction.to_string()),
                ("reason", quoted(reason)),
            ]),
        ),
        Event::Committed(result) => ("committed", commit(result)),
        Event::Checkpoint(result) => ("checkpoint", checkpoint(result)),
    };
    object([("kind", quoted(event_type)), ("data", payload)])
}

fn constraint_check(tx: u64, check: &ConstraintCheck) -> String {
    use crate::constraints::CheckOutcome;
    let phase = match check.phase {
        Phase::Statement => "statement",
        Phase::ModeChange => "mode_change",
        Phase::Commit => "commit",
        Phase::Recovery => "recovery",
    };
    let (verdict, detail) = match &check.outcome {
        CheckOutcome::Deferred => ("deferred", null()),
        CheckOutcome::Passed => ("passed", null()),
        CheckOutcome::Duplicate {
            value,
            first,
            second,
        } => (
            "duplicate",
            object([
                ("value", cell(value)),
                ("first", row_id(first)),
                ("second", row_id(second)),
            ]),
        ),
        CheckOutcome::MissingParent { value, child } => (
            "missing_parent",
            object([("value", cell(value)), ("child", row_id(child))]),
        ),
    };
    object([
        ("transaction", tx.to_string()),
        ("constraint", quoted(&check.name)),
        ("phase", quoted(phase)),
        ("examined", check.examined.to_string()),
        ("verdict", quoted(verdict)),
        ("detail", detail),
    ])
}

fn schema(value: &Schema) -> String {
    object([
        (
            "tables",
            collection(value.tables().map(|table| {
                object([
                    ("name", quoted(&table.name)),
                    ("primary", quoted(table.primary.label())),
                    (
                        "columns",
                        collection(table.columns.iter().map(|(name, column)| {
                            object([
                                ("name", quoted(name)),
                                ("type", quoted(column.kind.label())),
                                ("required", column.required.to_string()),
                            ])
                        })),
                    ),
                ])
            })),
        ),
        (
            "constraints",
            collection(value.constraints().map(|constraint| {
                let (table, column) = constraint.kind.source();
                let timing = match constraint.timing {
                    Timing::Immediate => "immediate",
                    Timing::Deferrable(crate::ConstraintMode::Immediate) => "deferrable_immediate",
                    Timing::Deferrable(crate::ConstraintMode::Deferred) => "deferrable_deferred",
                };
                let (kind, parent) = match &constraint.kind {
                    ConstraintKind::Unique { .. } => ("unique", null()),
                    ConstraintKind::ForeignKey {
                        parent,
                        parent_column,
                        ..
                    } => (
                        "foreign_key",
                        object([("table", quoted(parent)), ("column", quoted(parent_column))]),
                    ),
                };
                object([
                    ("name", quoted(&constraint.name)),
                    ("kind", quoted(kind)),
                    ("timing", quoted(timing)),
                    ("table", quoted(table)),
                    ("column", quoted(column)),
                    ("parent", parent),
                ])
            })),
        ),
    ])
}

fn edge(item: &WaitEdge) -> String {
    let reason = match item.reason {
        Blocker::Granted => "granted",
        Blocker::EarlierWaiter => "earlier_waiter",
    };
    object([
        ("waiting", item.waiting.to_string()),
        ("blocking", item.blocking.to_string()),
        ("reason", quoted(reason)),
        ("interval", interval(&item.interval)),
    ])
}

fn wait(ticket: &WaitTicket) -> String {
    object([
        ("transaction", ticket.transaction.to_string()),
        ("ticket", ticket.serial.to_string()),
    ])
}

fn interval(item: &ReadInterval) -> String {
    object([
        ("table", quoted(&item.table)),
        ("lower", bound(&item.lower)),
        ("upper", bound(&item.upper)),
    ])
}

fn bound(item: &Bound<Key>) -> String {
    match item {
        Bound::Unbounded => null(),
        Bound::Included(key) | Bound::Excluded(key) => object([
            ("inclusive", matches!(item, Bound::Included(_)).to_string()),
            ("key", cell(&key.value())),
        ]),
    }
}

fn encoded_error(error: &Error) -> String {
    let detail = match error {
        Error::Deadlock { victim, cycle } => object([
            ("victim", victim.to_string()),
            ("cycle", collection(cycle.iter().map(u64::to_string))),
        ]),
        Error::Serialization {
            transaction,
            detail,
        } => object([
            ("transaction", transaction.to_string()),
            ("detail", quoted(detail)),
        ]),
        Error::LockTimeout(transaction) => object([("transaction", transaction.to_string())]),
        Error::ConstraintViolation { name, detail } => {
            object([("constraint", quoted(name)), ("detail", quoted(detail))])
        }
        Error::Corrupt { file, offset, .. } => {
            object([("file", quoted(file)), ("offset", offset.to_string())])
        }
        _ => null(),
    };
    object([
        ("code", quoted(error.code())),
        ("message", quoted(&error.to_string())),
        ("retryable", error.retryable().to_string()),
        ("transaction_ended", error.ends_transaction().to_string()),
        ("detail", detail),
    ])
}

fn record(item: &Record) -> String {
    object([
        ("row", row_id(&item.id)),
        (
            "values",
            object(
                item.values
                    .iter()
                    .map(|(column, value)| (column.as_str(), cell(value))),
            ),
        ),
    ])
}

fn row_id(id: &RowId) -> String {
    object([("table", quoted(&id.table)), ("id", cell(&id.key.value()))])
}

fn cell(value: &Value) -> String {
    match value {
        Value::Null => null(),
        Value::Integer(value) => value.to_string(),
        Value::Text(value) => quoted(value),
    }
}

fn object<'a>(entries: impl IntoIterator<Item = (&'a str, String)>) -> String {
    let mut output = String::from("{");
    for (index, (name, value)) in entries.into_iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&quoted(name));
        output.push(':');
        output.push_str(&value);
    }
    output.push('}');
    output
}

fn collection(entries: impl IntoIterator<Item = String>) -> String {
    let mut output = String::from("[");
    for (index, entry) in entries.into_iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&entry);
    }
    output.push(']');
    output
}

fn quoted(value: &str) -> String {
    let mut output = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            ch if ch < '\u{20}' => write!(output, "\\u{:04x}", ch as u32).expect("write to string"),
            ch => output.push(ch),
        }
    }
    output.push('"');
    output
}

fn null() -> String {
    "null".to_owned()
}
