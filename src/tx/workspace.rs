use crate::{
    constraints::{ConstraintMode, ConstraintModes, Evaluator, Phase, Schema},
    error::{Error, Result},
    locks::WaitTicket,
    rows::{PendingRows, Row, RowId, RowIndex, RowVersion, Sequence, TxId, WriteIntent},
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
    pub index: &'a mut RowIndex,
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
        let source = context.index.version_at(id, self.info.snapshot);
        let newest = context.index.latest(id);
        let base = source.map(|version| version.sequence);
        let latest = newest.map(|version| version.sequence);
        if base != latest {
            return Err(self.stale_write(id, newest));
        }
        let current = context.index.read(id, self.info.snapshot, &self.intents);
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
        if !self.savepoints.is_empty() && self.undo.len() >= context.options.max_undo {
            return Err(Error::Limit("savepoint undo records"));
        }
        let intent = keep_intent.then_some(WriteIntent { base, values });
        let previous = self.intents.replace(id.clone(), intent);
        let mut evaluator = Evaluator::new(context.schema, context.index, &self.intents);
        let checks = evaluator.statement(&id.table, &self.modes);
        let verdict = verdict(self.info.id, context.events, checks);
        if let Err(error) = verdict {
            self.intents.replace(id.clone(), previous);
            return Err(error);
        }
        if !self.savepoints.is_empty() {
            self.undo.push(Undo {
                id: id.clone(),
                previous,
            });
        }
        Ok(())
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
            let mut evaluator = Evaluator::new(context.schema, context.index, &self.intents);
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
        let mut evaluator = Evaluator::new(context.schema, context.index, &self.intents);
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
