use std::collections::{BTreeMap, BTreeSet};

use super::{
    Key, PendingRows, ReadInterval, Record, Row, RowId, RowVersion, Sequence, TxId, Value,
    VersionChange,
};
use crate::constraints::{ConstraintKind, Schema, PRIMARY_KEY};

type VersionChain = Vec<RowVersion>;
type TableIndex = BTreeMap<Key, VersionChain>;

/// A constrained non-primary column that needs a value-to-rows lookup path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ColumnRef {
    pub table: String,
    pub column: String,
}

/// One row's membership under an indexed value. Tombstones let pending deletes
/// shadow committed entries while the overlay is merged for a constraint check.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IndexEntry {
    pub row_key: Key,
    pub live: bool,
}

/// Secondary indexes keyed by constrained column. Each index maps a column
/// value to the set of live row keys currently holding that value.
#[derive(Debug, Default)]
pub(crate) struct SecondaryIndex {
    columns: BTreeMap<ColumnRef, BTreeMap<Value, BTreeSet<Key>>>,
}

impl SecondaryIndex {
    fn add(&mut self, column: &ColumnRef, value: Value, row_key: Key) {
        self.columns
            .entry(ColumnRef {
                table: column.table.clone(),
                column: column.column.clone(),
            })
            .or_default()
            .entry(value)
            .or_default()
            .insert(row_key);
    }

    fn remove(&mut self, column: &ColumnRef, value: &Value, row_key: &Key) {
        if let Some(values) = self.columns.get_mut(column) {
            if let Some(rows) = values.get_mut(value) {
                rows.remove(row_key);
                if rows.is_empty() {
                    values.remove(value);
                }
            }
            if values.is_empty() {
                self.columns.remove(column);
            }
        }
    }

    fn entries(&self, column: &ColumnRef, value: &Value) -> Vec<IndexEntry> {
        self.columns
            .get(column)
            .and_then(|values| values.get(value))
            .map(|rows| {
                rows.iter()
                    .map(|row_key| IndexEntry {
                        row_key: row_key.clone(),
                        live: true,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Default)]
pub struct RowIndex {
    tables: BTreeMap<String, TableIndex>,
    secondary: SecondaryIndex,
}

impl RowIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latest(&self, id: &RowId) -> Option<&RowVersion> {
        self.tables.get(&id.table)?.get(&id.key)?.last()
    }

    pub fn version_at(&self, id: &RowId, snapshot: Sequence) -> Option<&RowVersion> {
        Self::as_of(self.tables.get(&id.table)?.get(&id.key)?, snapshot)
    }

    pub fn read<'a>(
        &'a self,
        id: &RowId,
        snapshot: Sequence,
        pending: &'a PendingRows,
    ) -> Option<&'a Row> {
        match pending.get(id) {
            Some(intent) => intent.values.as_ref(),
            None => self.version_at(id, snapshot)?.values.as_ref(),
        }
    }

    pub fn scan(
        &self,
        interval: &ReadInterval,
        snapshot: Sequence,
        pending: &PendingRows,
        limit: usize,
    ) -> Vec<Record> {
        if interval.is_empty() || limit == 0 {
            return Vec::new();
        }

        let mut stored = self
            .tables
            .get(&interval.table)
            .into_iter()
            .flat_map(|table| table.range::<Key, _>(interval.bounds()))
            .peekable();
        let mut staged = pending
            .table(&interval.table)
            .into_iter()
            .flat_map(|table| table.range::<Key, _>(interval.bounds()))
            .peekable();
        let mut records = Vec::new();

        while records.len() < limit {
            let pair = match (stored.peek(), staged.peek()) {
                (None, None) => break,
                (Some((committed_key, _)), Some((pending_key, _)))
                    if committed_key == pending_key =>
                {
                    stored.next();
                    staged
                        .next()
                        .map(|(key, intent)| (key, intent.values.as_ref()))
                }
                (Some((committed_key, _)), Some((pending_key, _)))
                    if pending_key < committed_key =>
                {
                    staged
                        .next()
                        .map(|(key, intent)| (key, intent.values.as_ref()))
                }
                (None, Some(_)) => staged
                    .next()
                    .map(|(key, intent)| (key, intent.values.as_ref())),
                (Some(_), _) => stored.next().map(|(key, chain)| {
                    (
                        key,
                        Self::as_of(chain, snapshot).and_then(|version| version.values.as_ref()),
                    )
                }),
            };
            if let Some((key, Some(values))) = pair {
                records.push(Record::new(
                    RowId::new(&interval.table, key.clone()),
                    values.clone(),
                ));
            }
        }
        records
    }

    pub fn first_change_after(
        &self,
        interval: &ReadInterval,
        snapshot: Sequence,
    ) -> Option<(Key, Sequence)> {
        if interval.is_empty() {
            return None;
        }
        self.tables
            .get(&interval.table)?
            .range::<Key, _>(interval.bounds())
            .find_map(|(key, chain)| {
                let head = chain.last()?;
                (head.sequence > snapshot).then(|| (key.clone(), head.sequence))
            })
    }

    pub fn heads(&self) -> impl Iterator<Item = (RowId, &RowVersion)> {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.iter().filter_map(move |(key, chain)| {
                Some((RowId::new(table, key.clone()), chain.last()?))
            })
        })
    }

    /// Point lookup through a constrained column. Primary-key columns are
    /// served by the primary index; every other constrained column uses a
    /// lazily materialized secondary index. Each returned entry is one index
    /// item actually touched by the lookup.
    pub(crate) fn lookup_column(
        &mut self,
        schema: &Schema,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Vec<IndexEntry> {
        if column == PRIMARY_KEY {
            let Ok(key) = value.clone().into_key() else {
                return Vec::new();
            };
            let live = self
                .tables
                .get(table)
                .and_then(|rows| rows.get(&key))
                .and_then(|chain| chain.last())
                .is_some_and(|head| head.values.is_some());
            return live
                .then(|| {
                    vec![IndexEntry {
                        row_key: key,
                        live: true,
                    }]
                })
                .unwrap_or_default();
        }
        self.ensure_secondary(schema);
        self.secondary.entries(
            &ColumnRef {
                table: table.to_owned(),
                column: column.to_owned(),
            },
            value,
        )
    }

    fn ensure_secondary(&mut self, schema: &Schema) {
        let needed: BTreeSet<ColumnRef> = schema
            .constraints()
            .flat_map(|constraint| match &constraint.kind {
                ConstraintKind::Unique { table, column } => {
                    vec![ColumnRef {
                        table: table.clone(),
                        column: column.clone(),
                    }]
                }
                ConstraintKind::ForeignKey {
                    table,
                    column,
                    parent,
                    parent_column,
                } => vec![
                    ColumnRef {
                        table: table.clone(),
                        column: column.clone(),
                    },
                    ColumnRef {
                        table: parent.clone(),
                        column: parent_column.clone(),
                    },
                ],
            })
            .filter(|column| column.column != PRIMARY_KEY)
            .collect();
        let have: BTreeSet<ColumnRef> = self.secondary.columns.keys().cloned().collect();
        for column in needed.difference(&have) {
            self.build_column(column);
        }
        // Constraints never change for the life of a store; drop any columns
        // that are no longer referenced so the cache cannot serve stale shapes.
        for stale in have.difference(&needed) {
            self.secondary.columns.remove(stale);
        }
    }

    fn build_column(&mut self, column: &ColumnRef) {
        let mut index: BTreeMap<Value, BTreeSet<Key>> = BTreeMap::new();
        if let Some(table) = self.tables.get(&column.table) {
            for (key, chain) in table {
                if let Some(head) = chain.last() {
                    if let Some(values) = &head.values {
                        if let Some(value) = values.get(&column.column) {
                            index.entry(value.clone()).or_default().insert(key.clone());
                        }
                    }
                }
            }
        }
        self.secondary.columns.insert(column.clone(), index);
    }

    fn index_change(&mut self, column: &ColumnRef, change: &VersionChange, previous: Option<&Row>) {
        if column.column == PRIMARY_KEY {
            return;
        }
        if !self.secondary.columns.contains_key(column) {
            return;
        }
        // The caller captures the row's prior head before appending the new
        // version, so this stays correct after old MVCC versions are collected.
        if let Some(old) = previous.and_then(|row| row.get(&column.column)) {
            self.secondary.remove(column, old, &change.id.key);
        }
        if let Some(row) = change.values.as_ref() {
            if let Some(value) = row.get(&column.column) {
                self.secondary
                    .add(column, value.clone(), change.id.key.clone());
            }
        }
    }

    pub fn push(&mut self, id: RowId, version: RowVersion) {
        let chain = self
            .tables
            .entry(id.table)
            .or_default()
            .entry(id.key)
            .or_default();
        debug_assert!(chain
            .last()
            .map_or(true, |head| head.sequence < version.sequence));
        chain.push(version);
    }

    pub fn publish(
        &mut self,
        sequence: Sequence,
        transaction: TxId,
        changes: impl IntoIterator<Item = VersionChange>,
    ) {
        let changes: Vec<VersionChange> = changes.into_iter().collect();
        let indexed: Vec<ColumnRef> = self.secondary.columns.keys().cloned().collect();
        // Capture each changed row's committed head before any chain is
        // appended; the secondary index deltas are computed against that view.
        let previous: Vec<Option<Row>> = changes
            .iter()
            .map(|change| {
                self.tables
                    .get(&change.id.table)
                    .and_then(|table| table.get(&change.id.key))
                    .and_then(|chain| chain.last())
                    .and_then(|head| head.values.clone())
            })
            .collect();
        for change in &changes {
            self.push(
                change.id.clone(),
                RowVersion::new(sequence, transaction, change.values.clone()),
            );
        }
        for column in &indexed {
            for (change, before) in changes.iter().zip(previous.iter()) {
                if change.id.table == column.table {
                    self.index_change(column, change, before.as_ref());
                }
            }
        }
    }

    pub fn collect_versions(&mut self, oldest_snapshot: Sequence) -> usize {
        let mut collected = 0;
        for table in self.tables.values_mut() {
            for chain in table.values_mut() {
                let through = chain.partition_point(|version| version.sequence <= oldest_snapshot);
                let remove = through.saturating_sub(1);
                chain.drain(..remove);
                collected += remove;
            }
        }
        collected
    }

    fn as_of(chain: &[RowVersion], snapshot: Sequence) -> Option<&RowVersion> {
        let through = chain.partition_point(|version| version.sequence <= snapshot);
        through.checked_sub(1).map(|position| &chain[position])
    }
}
