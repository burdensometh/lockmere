use std::collections::{BTreeSet, VecDeque};

use crate::{
    constraints::{ConstraintMode, ConstraintModes, Evaluator, Phase, Schema, PRIMARY_KEY},
    error::{Error, Result},
    locks::WaitTicket,
    rows::{
        PendingRows, ReadInterval, Row, RowId, RowIndex, RowVersion, Sequence, TxId, Value,
        WriteIntent,
    },
};

use super::{Event, Isolation, Operation, Options, TransactionInfo};

#[derive(Debug)]
pub(crate) struct Workspace {
    pub info: TransactionInfo,
    pub intents: PendingRows,
    pub parked: Option<(WaitTicket, Operation)>,
    modes: ConstraintModes,
    undo: Vec<Undo>,
    savepoints: Vec<Savepoint>,
}

#[derive(Debug)]
struct Undo {
    id: RowId,
    previous: Option<WriteIntent>,
}

#[derive(Debug)]
struct Savepoint {
    name: String,
    undo_at: usize,
    modes: ConstraintModes,
}

pub(crate) struct StatementContext<'a> {
    pub schema: &'a Schema,
    pub index: &'a RowIndex,
    pub latest: Sequence,
    pub options: &'a Options,
    pub events: &'a mut Vec<Event>,
}

pub(crate) enum Change {
    Insert(Row),
    Update(Row),
    Delete,
}

impl Workspace {
    pub fn new(id: TxId, snapshot: Sequence, isolation: Isolation) -> Self {
        Self {
            info: TransactionInfo {
                id,
                snapshot,
                isolation,
                writes: 0,
                savepoints: 0,
                reads: 0,
                waiting: None,
            },
            intents: PendingRows::default(),
            parked: None,
            modes: ConstraintModes::default(),
            undo: Vec::new(),
            savepoints: Vec::new(),
        }
    }

    pub fn info(&self) -> TransactionInfo {
        let mut info = self.info.clone();
        info.writes = self.intents.len();
        info.savepoints = self.savepoints.len();
        info.waiting = self.parked.as_ref().map(|(ticket, _)| *ticket);
        info
    }

    pub fn stage(
        &mut self,
        context: &mut StatementContext<'_>,
        id: &RowId,
        change: Change,
    ) -> Result<()> {
        let mut touched = Vec::new();
        let mut tables = BTreeSet::new();
        let staged = self
            .stage_statement(context, id, change, &mut touched, &mut tables)
            .and_then(|()| {
                if !self.savepoints.is_empty()
                    && self.undo.len() + touched.len() > context.options.max_undo
                {
                    return Err(Error::Limit("savepoint undo records"));
                }
                Ok(())
            });
        if let Err(error) = staged {
            for (row, previous) in touched.into_iter().rev() {
                self.intents.replace(row, previous);
            }
            return Err(error);
        }
        if !self.savepoints.is_empty() {
            self.undo.extend(
                touched
                    .into_iter()
                    .map(|(id, previous)| Undo { id, previous }),
            );
        }
        Ok(())
    }

    fn stage_statement(
        &mut self,
        context: &mut StatementContext<'_>,
        id: &RowId,
        change: Change,
        touched: &mut Vec<(RowId, Option<WriteIntent>)>,
        tables: &mut BTreeSet<String>,
    ) -> Result<()> {
        let updated = matches!(change, Change::Update(_));
        let (before, after) = self.apply(context, id, change, touched)?;
        tables.insert(id.table.clone());
        if updated {
            self.cascade(context, id, &before, &after, touched, tables)?;
        }
        let mut evaluator =
            Evaluator::new(context.schema, context.index, context.latest, &self.intents);
        let tables: BTreeSet<&str> = tables.iter().map(String::as_str).collect();
        let checks = evaluator.statement_over(&tables, &self.modes);
        verdict(self.info.id, context.events, checks)
    }

    fn cascade(
        &mut self,
        context: &mut StatementContext<'_>,
        id: &RowId,
        before: &Option<Row>,
        after: &Option<Row>,
        touched: &mut Vec<(RowId, Option<WriteIntent>)>,
        tables: &mut BTreeSet<String>,
    ) -> Result<()> {
        let mut worklist = VecDeque::new();
        if let (Some(before), Some(after)) = (before, after) {
            enqueue_cascades(context.schema, id, before, after, &mut worklist);
        }
        let mut visited = BTreeSet::new();
        while let Some((name, old, new)) = worklist.pop_front() {
            if !visited.insert((name.clone(), old.clone())) {
                continue;
            }
            let schema = context.schema;
            let constraint = schema.constraint(&name)?;
            let crate::constraints::ConstraintKind::ForeignKey { table, column, .. } =
                &constraint.kind
            else {
                continue;
            };
            let children: Vec<RowId> = context
                .index
                .scan(
                    &ReadInterval::whole(table),
                    context.latest,
                    &self.intents,
                    usize::MAX,
                )
                .into_iter()
                .filter(|record| referenced(&record.id, &record.values, column) == old)
                .map(|record| record.id)
                .collect();
            if children.is_empty() {
                continue;
            }
            for child in children {
                let patch = Row::from([(column.clone(), new.clone())]);
                let (child_before, child_after) =
                    self.apply(context, &child, Change::Update(patch), touched)?;
                if let (Some(child_before), Some(child_after)) = (child_before, child_after) {
                    enqueue_cascades(
                        context.schema,
                        &child,
                        &child_before,
                        &child_after,
                        &mut worklist,
                    );
                }
            }
            tables.insert(table.clone());
        }
        Ok(())
    }

    fn apply(
        &mut self,
        context: &StatementContext<'_>,
        id: &RowId,
        change: Change,
        touched: &mut Vec<(RowId, Option<WriteIntent>)>,
    ) -> Result<(Option<Row>, Option<Row>)> {
        let source = context.index.version_at(id, self.info.snapshot);
        let newest = context.index.latest(id);
        let base = source.map(|version| version.sequence);
        let latest = newest.map(|version| version.sequence);
        if base != latest {
            return Err(self.stale_write(id, newest));
        }
        let current = context.index.read(id, self.info.snapshot, &self.intents);
        let before = current.cloned();
        let values = match (change, current) {
            (Change::Insert(row), None) => Some(context.schema.normalize_row(id, row)?),
            (Change::Insert(_), Some(_)) => return Err(Error::DuplicateKey(id.clone())),
            (Change::Update(patch), Some(values)) => {
                let mut merged = values.clone();
                merged.extend(patch);
                Some(merged)
            }
            (Change::Delete, Some(_)) => None,
            (Change::Update(_) | Change::Delete, None) => {
                return Err(Error::MissingRow(id.clone()))
            }
        };
        let creates_intent = self.intents.get(id).is_none();
        let keep_intent =
            values.is_some() || source.is_some_and(|version| version.values.is_some());
        if keep_intent && creates_intent && self.intents.len() >= context.options.max_intents {
            return Err(Error::Limit("transaction writes"));
        }
        let intent = keep_intent.then_some(WriteIntent {
            base,
            values: values.clone(),
        });
        let previous = self.intents.replace(id.clone(), intent);
        touched.push((id.clone(), previous));
        Ok((before, values))
    }

    pub fn savepoint(&mut self, name: String, max: usize) -> Result<()> {
        if name.is_empty() || name.len() > 64 || name.chars().any(char::is_control) {
            return Err(Error::InvalidSavepoint(name));
        }
        if self.savepoints.len() >= max {
            return Err(Error::Limit("savepoints"));
        }
        self.savepoints.push(Savepoint {
            name,
            undo_at: self.undo.len(),
            modes: self.modes.clone(),
        });
        Ok(())
    }

    pub fn rollback_to(&mut self, name: &str) -> Result<()> {
        let position = self.find(name)?;
        let mark = &self.savepoints[position];
        self.modes = mark.modes.clone();
        while self.undo.len() > mark.undo_at {
            let undo = self.undo.pop().expect("undo after savepoint");
            self.intents.replace(undo.id, undo.previous);
        }
        self.savepoints.truncate(position + 1);
        Ok(())
    }

    pub fn release(&mut self, name: &str) -> Result<()> {
        let position = self.find(name)?;
        self.savepoints.truncate(position);
        if self.savepoints.is_empty() {
            self.undo.clear();
        }
        Ok(())
    }

    pub fn set_constraints(
        &mut self,
        context: &mut StatementContext<'_>,
        name: Option<&str>,
        mode: ConstraintMode,
    ) -> Result<usize> {
        let constraints = match name {
            Some(name) => vec![context.schema.constraint(name)?],
            None => context
                .schema
                .constraints()
                .filter(|constraint| constraint.timing.deferrable())
                .collect(),
        };
        let mut next = self.modes.clone();
        for constraint in &constraints {
            next.set(constraint, mode)?;
        }
        if mode == ConstraintMode::Immediate {
            let mut evaluator =
                Evaluator::new(context.schema, context.index, context.latest, &self.intents);
            let checks = constraints
                .iter()
                .map(|constraint| evaluator.check(constraint, Phase::ModeChange))
                .collect();
            verdict(self.info.id, context.events, checks)?;
        }
        self.modes = next;
        Ok(constraints.len())
    }

    pub fn validate_commit(&self, context: &mut StatementContext<'_>) -> Result<()> {
        for (table, key, intent) in self.intents.iter() {
            let id = RowId::new(table, key.clone());
            let newest = context.index.latest(&id);
            if intent.base != newest.map(|version| version.sequence) {
                return Err(self.stale_write(&id, newest));
            }
        }
        let mut evaluator =
            Evaluator::new(context.schema, context.index, context.latest, &self.intents);
        verdict(self.info.id, context.events, evaluator.commit())
    }

    fn find(&self, name: &str) -> Result<usize> {
        self.savepoints
            .iter()
            .rposition(|point| point.name == name)
            .ok_or_else(|| Error::InvalidSavepoint(name.to_owned()))
    }

    fn stale_write(&self, id: &RowId, newest: Option<&RowVersion>) -> Error {
        Error::Serialization {
            transaction: self.info.id,
            detail: format!(
                "write to {id} has newer version {:?} than snapshot {}",
                newest.map(|version| version.sequence),
                self.info.snapshot
            ),
        }
    }
}

fn verdict(
    transaction: TxId,
    events: &mut Vec<Event>,
    checks: Vec<crate::constraints::ConstraintCheck>,
) -> Result<()> {
    let error = checks.iter().find_map(|check| check.as_error());
    events.extend(checks.into_iter().map(|result| Event::Check {
        transaction,
        result,
    }));
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn enqueue_cascades(
    schema: &Schema,
    id: &RowId,
    before: &Row,
    after: &Row,
    worklist: &mut VecDeque<(String, Value, Value)>,
) {
    for constraint in schema.update_cascades(&id.table) {
        let crate::constraints::ConstraintKind::ForeignKey { parent_column, .. } =
            &constraint.kind
        else {
            continue;
        };
        let old = referenced(id, before, parent_column);
        let new = referenced(id, after, parent_column);
        if old != new {
            worklist.push_back((constraint.name.clone(), old, new));
        }
    }
}

fn referenced(id: &RowId, values: &Row, column: &str) -> Value {
    if column == PRIMARY_KEY {
        id.key.value()
    } else {
        values.get(column).cloned().unwrap_or(Value::Null)
    }
}
