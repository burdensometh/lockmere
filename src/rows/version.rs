use std::collections::{btree_map, BTreeMap};

use super::{Key, Row, RowId};

pub type TxId = u64;
pub type Sequence = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVersion {
    pub sequence: Sequence,
    pub transaction: TxId,
    pub values: Option<Row>,
}

impl RowVersion {
    pub fn new(sequence: Sequence, transaction: TxId, values: Option<Row>) -> Self {
        Self {
            sequence,
            transaction,
            values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionChange {
    pub id: RowId,
    pub values: Option<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteIntent {
    pub base: Option<Sequence>,
    pub values: Option<Row>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingRows {
    tables: BTreeMap<String, BTreeMap<Key, WriteIntent>>,
}

impl PendingRows {
    pub fn table(&self, table: &str) -> Option<&BTreeMap<Key, WriteIntent>> {
        self.tables.get(table)
    }

    pub fn get(&self, id: &RowId) -> Option<&WriteIntent> {
        self.table(&id.table)?.get(&id.key)
    }

    pub fn replace(&mut self, id: RowId, value: Option<WriteIntent>) -> Option<WriteIntent> {
        match value {
            Some(intent) => self
                .tables
                .entry(id.table)
                .or_default()
                .insert(id.key, intent),
            None => {
                let btree_map::Entry::Occupied(mut table) = self.tables.entry(id.table) else {
                    return None;
                };
                let previous = table.get_mut().remove(&id.key);
                if table.get().is_empty() {
                    table.remove_entry();
                }
                previous
            }
        }
    }

    pub fn len(&self) -> usize {
        self.tables.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Key, &WriteIntent)> {
        self.tables.iter().flat_map(|(table, rows)| {
            rows.iter()
                .map(move |(key, intent)| (table.as_str(), key, intent))
        })
    }

    pub fn changes(&self) -> impl Iterator<Item = VersionChange> + '_ {
        self.iter().map(|(table, key, intent)| VersionChange {
            id: RowId::new(table, key.clone()),
            values: intent.values.clone(),
        })
    }
}
