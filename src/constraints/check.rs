use std::collections::{BTreeMap, BTreeSet};

use super::{Constraint, ConstraintKind, ConstraintMode, Schema, PRIMARY_KEY};
use crate::{
    error::{Error, Result},
    rows::{IndexEntry, Key, PendingRows, ReadInterval, RowId, RowIndex, Value, WriteIntent},
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

/// A pending row reduced to the fields a constraint check needs.
#[derive(Debug, Clone)]
struct PendingRow<'a> {
    key: &'a Key,
    intent: &'a WriteIntent,
}

impl<'a> PendingRow<'a> {
    fn value(&self, column: &str) -> Option<Value> {
        if column == PRIMARY_KEY {
            Some(self.key.value())
        } else {
            self.intent.values.as_ref()?.get(column).cloned()
        }
    }
}

pub struct Evaluator<'a> {
    schema: &'a Schema,
    index: &'a mut RowIndex,
    pending: &'a PendingRows,
}

impl<'a> Evaluator<'a> {
    pub fn new(schema: &'a Schema, index: &'a mut RowIndex, pending: &'a PendingRows) -> Self {
        Self {
            schema,
            index,
            pending,
        }
    }

    pub fn statement(&mut self, table: &str, modes: &ConstraintModes) -> Vec<ConstraintCheck> {
        self.schema
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
                    self.incremental(constraint, Phase::Statement)
                }
            })
            .collect()
    }

    pub fn commit(&mut self) -> Vec<ConstraintCheck> {
        let changed: BTreeSet<&str> = self.pending.iter().map(|(table, _, _)| table).collect();
        self.schema
            .constraints()
            .filter(|constraint| changed.iter().any(|table| constraint.kind.observes(table)))
            .map(|constraint| self.incremental(constraint, Phase::Commit))
            .collect()
    }

    pub fn recovery(&mut self) -> Vec<ConstraintCheck> {
        self.schema
            .constraints()
            .map(|constraint| self.full_scan(constraint, Phase::Recovery))
            .collect()
    }

    pub fn check(&mut self, constraint: &Constraint, phase: Phase) -> ConstraintCheck {
        if phase == Phase::Recovery {
            self.full_scan(constraint, phase)
        } else {
            self.incremental(constraint, phase)
        }
    }

    fn full_scan(&mut self, constraint: &Constraint, phase: Phase) -> ConstraintCheck {
        let (examined, outcome) = match &constraint.kind {
            ConstraintKind::Unique { table, column } => {
                let mut seen = BTreeMap::new();
                let records = self.index.scan(
                    &ReadInterval::whole(table),
                    self.latest_sequence(),
                    self.pending,
                    usize::MAX,
                );
                let mut examined = 0;
                let mut outcome = CheckOutcome::Passed;
                for (position, record) in records.iter().enumerate() {
                    examined = position + 1;
                    let value = Self::column_value(&record.id, &record.values, column);
                    if let Some(first) = seen.insert(value.clone(), record.id.clone()) {
                        outcome = CheckOutcome::Duplicate {
                            value,
                            first,
                            second: record.id.clone(),
                        };
                        break;
                    }
                }
                (examined, outcome)
            }
            ConstraintKind::ForeignKey {
                table,
                column,
                parent,
                parent_column,
            } => {
                let sequence = self.latest_sequence();
                let parents: BTreeSet<Value> = self
                    .index
                    .scan(
                        &ReadInterval::whole(parent),
                        sequence,
                        self.pending,
                        usize::MAX,
                    )
                    .iter()
                    .map(|record| Self::column_value(&record.id, &record.values, parent_column))
                    .collect();
                let children = self.index.scan(
                    &ReadInterval::whole(table),
                    sequence,
                    self.pending,
                    usize::MAX,
                );
                let mut examined = 0;
                let mut outcome = CheckOutcome::Passed;
                for (position, child) in children.iter().enumerate() {
                    examined = position + 1;
                    let value = Self::column_value(&child.id, &child.values, column);
                    if !parents.contains(&value) {
                        outcome = CheckOutcome::MissingParent {
                            value,
                            child: child.id.clone(),
                        };
                        break;
                    }
                }
                (examined, outcome)
            }
        };
        ConstraintCheck {
            name: constraint.name.clone(),
            phase,
            examined,
            outcome,
        }
    }

    /// Incremental validation. Reads are charged only for the distinct values
    /// the transaction actually changed and the index entries those values
    /// resolve to; untouched child rows are never scanned.
    fn incremental(&mut self, constraint: &Constraint, phase: Phase) -> ConstraintCheck {
        let (examined, outcome) = match &constraint.kind {
            ConstraintKind::Unique { table, column } => self.incremental_unique(table, column),
            ConstraintKind::ForeignKey {
                table,
                column,
                parent,
                parent_column,
            } => self.incremental_foreign_key(table, column, parent, parent_column),
        };
        ConstraintCheck {
            name: constraint.name.clone(),
            phase,
            examined,
            outcome,
        }
    }

    fn incremental_unique(&mut self, table: &str, column: &str) -> (usize, CheckOutcome) {
        let mut examined = 0;
        // Only rows whose value for this column was inserted or updated can
        // introduce a duplicate. Deletes cannot create one.
        let candidates: BTreeSet<Value> = self
            .pending_rows(table)
            .filter_map(|row| row.value(column))
            .collect();
        for value in candidates {
            let entries = self.commit_view(table, column, &value);
            examined += entries.len();
            let live: Vec<&IndexEntry> = entries.iter().filter(|entry| entry.live).collect();
            if live.len() > 1 {
                let first = RowId::new(table, live[0].row_key.clone());
                let second = RowId::new(table, live[1].row_key.clone());
                return (
                    examined,
                    CheckOutcome::Duplicate {
                        value,
                        first,
                        second,
                    },
                );
            }
        }
        (examined, CheckOutcome::Passed)
    }

    fn incremental_foreign_key(
        &mut self,
        table: &str,
        column: &str,
        parent: &str,
        parent_column: &str,
    ) -> (usize, CheckOutcome) {
        let mut examined = 0;

        // 1. New or reassigned child values must resolve against the true
        //    commit view: latest committed parents overlaid with this
        //    transaction's own parent writes.
        let child_values: BTreeSet<Value> = self
            .pending_rows(table)
            .filter_map(|row| row.value(column))
            .collect();
        for value in child_values {
            let entries = self.commit_view(table, column, &value);
            examined += entries.len();
            // Resolve the parent through the same merged view so pending
            // parent inserts/updates count and pending parent deletes do not.
            let parent_entries = self.commit_view(parent, parent_column, &value);
            examined += parent_entries.len();
            let parent_exists = parent_entries.iter().any(|entry| entry.live);
            if !parent_exists {
                let child = entries
                    .iter()
                    .find(|entry| entry.live)
                    .map(|entry| RowId::new(table, entry.row_key.clone()))
                    .expect("a pending child value belongs to a live pending row");
                return (examined, CheckOutcome::MissingParent { value, child });
            }
        }

        // 2. A parent delete (or parent-key reassignment) can orphan children
        //    that never changed. For every parent value this transaction
        //    removes, probe the child index for surviving references, again
        //    merging pending child deletes so a repaired reference is allowed.
        let removed_parent_values = self.removed_values(parent, parent_column);
        for value in removed_parent_values {
            let entries = self.commit_view(table, column, &value);
            examined += entries.len();
            if let Some(orphan) = entries.iter().find(|entry| entry.live) {
                return (
                    examined,
                    CheckOutcome::MissingParent {
                        value,
                        child: RowId::new(table, orphan.row_key.clone()),
                    },
                );
            }
        }

        (examined, CheckOutcome::Passed)
    }

    /// Merge a committed secondary-index point lookup with this transaction's
    /// pending writes so the result is the view that would exist at commit.
    /// Pending tombstones shadow committed entries; pending upserts add or
    /// replace the row's membership.
    fn commit_view(&mut self, table: &str, column: &str, value: &Value) -> Vec<IndexEntry> {
        let mut entries: BTreeMap<Key, IndexEntry> = self
            .index
            .lookup_column(self.schema, table, column, value)
            .into_iter()
            .map(|entry| (entry.row_key.clone(), entry))
            .collect();
        for row in self.pending_rows(table) {
            if row.value(column).as_ref() == Some(value) {
                entries.insert(
                    row.key.clone(),
                    IndexEntry {
                        row_key: row.key.clone(),
                        live: row.intent.values.is_some(),
                    },
                );
            } else if let Some(committed) = entries.get(row.key) {
                if committed.live {
                    // A pending row that no longer carries this value leaves the
                    // value's entry set: it was reassigned or deleted.
                    entries.insert(
                        row.key.clone(),
                        IndexEntry {
                            row_key: row.key.clone(),
                            live: false,
                        },
                    );
                }
            }
        }
        entries.into_values().collect()
    }

    /// Values present in the latest committed view that this transaction no
    /// longer carries on any live pending row of the table. The pre-change
    /// value is read at the intent's base sequence, never from the chain head:
    /// a previously committed delete would otherwise hide the value entirely.
    /// A tombstone intent (values == None) carries no columns at all, so every
    /// base value it used to hold counts as removed.
    fn removed_values(&mut self, table: &str, column: &str) -> BTreeSet<Value> {
        let mut removed = BTreeSet::new();
        for row in self.pending_rows(table) {
            let id = RowId::new(table, row.key.clone());
            let Some(base) = row.intent.base else {
                continue;
            };
            let Some(previous) = self.index.version_at(&id, base) else {
                continue;
            };
            let Some(committed) = previous.values.as_ref() else {
                continue;
            };
            let old = Self::column_value(&id, committed, column);
            // A deleted row holds no value. An upsert holds a value read
            // straight from the pending row; a changed value differs.
            let current = row.intent.values.as_ref().and_then(|_| row.value(column));
            if current.as_ref() != Some(&old) {
                removed.insert(old);
            }
        }
        removed
    }

    fn pending_rows(&self, table: &str) -> impl Iterator<Item = PendingRow<'_>> {
        self.pending
            .table(table)
            .into_iter()
            .flat_map(|rows| rows.iter().map(|(key, intent)| PendingRow { key, intent }))
    }

    fn latest_sequence(&self) -> crate::rows::Sequence {
        self.index
            .heads()
            .map(|(_, head)| head.sequence)
            .max()
            .unwrap_or(0)
    }

    fn column_value(id: &RowId, values: &crate::rows::Row, column: &str) -> Value {
        if column == PRIMARY_KEY {
            id.key.value()
        } else {
            values
                .get(column)
                .cloned()
                .expect("constrained columns are non-null and present on canonical rows")
        }
    }
}
