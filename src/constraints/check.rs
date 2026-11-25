use std::collections::{BTreeMap, BTreeSet};

use super::{Constraint, ConstraintKind, ConstraintMode, Schema, PRIMARY_KEY};
use crate::{
    error::{Error, Result},
    rows::{PendingRows, ReadInterval, Record, RowId, RowIndex, Sequence, Value},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConstraintModes(BTreeMap<String, ConstraintMode>);

impl ConstraintModes {
    pub fn mode(&self, constraint: &Constraint) -> ConstraintMode {
        self.0
            .get(&constraint.name)
            .copied()
            .unwrap_or_else(|| constraint.timing.initial())
    }

    pub fn set(&mut self, constraint: &Constraint, mode: ConstraintMode) -> Result<()> {
        if !constraint.timing.deferrable() {
            return Err(Error::NotDeferrable(constraint.name.clone()));
        }
        if mode == constraint.timing.initial() {
            self.0.remove(&constraint.name);
        } else {
            self.0.insert(constraint.name.clone(), mode);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Statement,
    ModeChange,
    Commit,
    Recovery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Deferred,
    Passed,
    Duplicate {
        value: Value,
        first: RowId,
        second: RowId,
    },
    MissingParent {
        value: Value,
        child: RowId,
    },
}

impl CheckOutcome {
    pub fn accepted(&self) -> bool {
        matches!(self, Self::Deferred | Self::Passed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintCheck {
    pub name: String,
    pub phase: Phase,
    pub examined: usize,
    pub outcome: CheckOutcome,
}

impl ConstraintCheck {
    pub fn as_error(&self) -> Option<Error> {
        let detail = match &self.outcome {
            CheckOutcome::Duplicate {
                value,
                first,
                second,
            } => {
                format!("{first} and {second} use the same value {value:?}")
            }
            CheckOutcome::MissingParent { value, child } => {
                format!("{child} references missing value {value:?}")
            }
            _ => return None,
        };
        Some(Error::ConstraintViolation {
            name: self.name.clone(),
            detail,
        })
    }
}

pub struct Evaluator<'a> {
    schema: &'a Schema,
    index: &'a RowIndex,
    sequence: Sequence,
    pending: &'a PendingRows,
    projections: BTreeMap<String, Vec<Record>>,
}

impl<'a> Evaluator<'a> {
    pub fn new(
        schema: &'a Schema,
        index: &'a RowIndex,
        sequence: Sequence,
        pending: &'a PendingRows,
    ) -> Self {
        Self {
            schema,
            index,
            sequence,
            pending,
            projections: BTreeMap::new(),
        }
    }

    pub fn statement(&mut self, table: &str, modes: &ConstraintModes) -> Vec<ConstraintCheck> {
        let schema = self.schema;
        schema
            .constraints()
            .filter(|constraint| constraint.kind.observes(table))
            .map(|constraint| {
                if modes.mode(constraint) == ConstraintMode::Deferred {
                    ConstraintCheck {
                        name: constraint.name.clone(),
                        phase: Phase::Statement,
                        examined: 0,
                        outcome: CheckOutcome::Deferred,
                    }
                } else {
                    self.check(constraint, Phase::Statement)
                }
            })
            .collect()
    }

    pub fn commit(&mut self) -> Vec<ConstraintCheck> {
        let changed: BTreeSet<&str> = self.pending.iter().map(|(table, _, _)| table).collect();
        let schema = self.schema;
        schema
            .constraints()
            .filter(|constraint| changed.iter().any(|table| constraint.kind.observes(table)))
            .map(|constraint| self.check(constraint, Phase::Commit))
            .collect()
    }

    pub fn recovery(&mut self) -> Vec<ConstraintCheck> {
        let schema = self.schema;
        schema
            .constraints()
            .map(|constraint| self.check(constraint, Phase::Recovery))
            .collect()
    }

    pub fn check(&mut self, constraint: &Constraint, phase: Phase) -> ConstraintCheck {
        let (examined, outcome) = match &constraint.kind {
            ConstraintKind::Unique { table, column } => self.unique(table, column),
            ConstraintKind::ForeignKey {
                table,
                column,
                parent,
                parent_column,
            } => self.foreign_key(table, column, parent, parent_column),
        };
        ConstraintCheck {
            name: constraint.name.clone(),
            phase,
            examined,
            outcome,
        }
    }

    fn projection(&mut self, table: &str) -> &[Record] {
        let index = self.index;
        let sequence = self.sequence;
        let pending = self.pending;
        self.projections.entry(table.to_owned()).or_insert_with(|| {
            index.scan(&ReadInterval::whole(table), sequence, pending, usize::MAX)
        })
    }

    fn unique(&mut self, table: &str, column: &str) -> (usize, CheckOutcome) {
        let mut seen = BTreeMap::new();
        let records = self.projection(table);
        for (index, record) in records.iter().enumerate() {
            let value = Self::column(record, column);
            if let Some(first) = seen.insert(value.clone(), &record.id) {
                return (
                    index + 1,
                    CheckOutcome::Duplicate {
                        value,
                        first: first.clone(),
                        second: record.id.clone(),
                    },
                );
            }
        }
        (records.len(), CheckOutcome::Passed)
    }

    fn foreign_key(
        &mut self,
        table: &str,
        column: &str,
        parent: &str,
        parent_column: &str,
    ) -> (usize, CheckOutcome) {
        let parents: BTreeSet<_> = self
            .projection(parent)
            .iter()
            .map(|record| Self::column(record, parent_column))
            .collect();
        let children = self.projection(table);
        for (index, child) in children.iter().enumerate() {
            let value = Self::column(child, column);
            if !parents.contains(&value) {
                return (
                    index + 1,
                    CheckOutcome::MissingParent {
                        value,
                        child: child.id.clone(),
                    },
                );
            }
        }
        (children.len(), CheckOutcome::Passed)
    }

    fn column(record: &Record, column: &str) -> Value {
        if column == PRIMARY_KEY {
            record.id.key.value()
        } else {
            record.values[column].clone()
        }
    }
}
