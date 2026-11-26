use std::collections::{BTreeMap, BTreeSet};

use super::{
    Key, PendingRows, ReadInterval, Record, Row, RowId, RowVersion, Sequence, TxId, VersionChange,
};

type VersionChain = Vec<RowVersion>;
type TableIndex = BTreeMap<Key, VersionChain>;

#[derive(Debug, Default)]
pub struct RowIndex {
    tables: BTreeMap<String, TableIndex>,
    // Keys whose chain currently holds more than one version. Reclamation walks
    // only these keys, so checkpoints over mostly single-version cold keys stay
    // proportional to the rows that actually hold reclaimable history.
    versioned: BTreeMap<String, BTreeSet<Key>>,
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

    pub fn push(&mut self, id: RowId, version: RowVersion) {
        let RowId { table, key } = id;
        let became_versioned = match self.tables.get_mut(&table) {
            Some(rows) => match rows.get_mut(&key) {
                Some(chain) => {
                    debug_assert!(chain
                        .last()
                        .map_or(true, |head| head.sequence < version.sequence));
                    let was_single_version = chain.len() == 1;
                    chain.push(version);
                    was_single_version
                }
                None => {
                    rows.insert(key.clone(), vec![version]);
                    false
                }
            },
            None => {
                let mut rows = TableIndex::new();
                rows.insert(key.clone(), vec![version]);
                self.tables.insert(table.clone(), rows);
                false
            }
        };
        if became_versioned {
            self.versioned.entry(table).or_default().insert(key);
        }
    }

    pub fn publish(
        &mut self,
        sequence: Sequence,
        transaction: TxId,
        changes: impl IntoIterator<Item = VersionChange>,
    ) {
        for change in changes {
            self.push(
                change.id,
                RowVersion::new(sequence, transaction, change.values),
            );
        }
    }

    pub fn collect_versions(&mut self, oldest_snapshot: Sequence) -> usize {
        if self.versioned.is_empty() {
            return 0;
        }
        let mut collected = 0;
        for (table, keys) in &self.versioned {
            let Some(rows) = self.tables.get_mut(table) else {
                continue;
            };
            for key in keys {
                let Some(chain) = rows.get_mut(key) else {
                    continue;
                };
                // Keep the newest version visible at or before the oldest pinned
                // snapshot; only strictly older history is reclaimable. A chain
                // with one version (the committed head) is always retained.
                let through = chain.partition_point(|version| version.sequence <= oldest_snapshot);
                let remove = through.saturating_sub(1);
                if remove > 0 {
                    chain.drain(..remove);
                    collected += remove;
                }
            }
        }
        // Rebuild the tracking set from the chains that still hold history. A
        // key leaves the set once its chain is back to a single head version.
        self.versioned.retain(|table, keys| {
            let Some(rows) = self.tables.get(table) else {
                return false;
            };
            keys.retain(|key| rows.get(key).is_some_and(|chain| chain.len() > 1));
            !keys.is_empty()
        });
        collected
    }

    fn as_of(chain: &[RowVersion], snapshot: Sequence) -> Option<&RowVersion> {
        let through = chain.partition_point(|version| version.sequence <= snapshot);
        through.checked_sub(1).map(|position| &chain[position])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Value;

    fn row(code: &str) -> Row {
        Row::from([("code".to_owned(), Value::Text(code.to_owned()))])
    }

    fn version(sequence: Sequence, code: &str) -> RowVersion {
        RowVersion::new(sequence, sequence, Some(row(code)))
    }

    fn id(key: i64) -> RowId {
        RowId::new("branch", key)
    }

    #[test]
    fn single_version_cold_keys_are_never_tracked() {
        let mut index = RowIndex::new();
        for key in 0..128 {
            index.push(id(key), version(1, "cold"));
        }
        assert!(index.versioned.is_empty());
        // No history anywhere: reclamation must do no work.
        assert_eq!(index.collect_versions(u64::MAX), 0);
        assert!(index.versioned.is_empty());
        // The committed heads are still readable.
        for key in 0..128 {
            assert_eq!(
                index.latest(&id(key)).unwrap().values.as_ref().unwrap()["code"],
                "cold".into()
            );
        }
    }

    #[test]
    fn only_revised_keys_enter_and_leave_reclamation() {
        let mut index = RowIndex::new();
        for key in 0..128 {
            index.push(id(key), version(1, "cold"));
        }
        let hot = id(7);
        index.push(hot.clone(), version(2, "newer"));
        index.push(hot.clone(), version(3, "latest"));
        assert_eq!(
            index
                .versioned
                .get("branch")
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![&Key::Integer(7)]
        );

        // A snapshot pinned at sequence 1 must still observe its own version.
        assert_eq!(
            index.version_at(&hot, 1).unwrap().values.as_ref().unwrap()["code"],
            "cold".into()
        );
        assert_eq!(index.collect_versions(1), 0);
        assert!(index
            .versioned
            .get("branch")
            .unwrap()
            .contains(&Key::Integer(7)));

        // Once the old reader leaves, the two obsolete versions are collected and
        // the key drops back out of the reclamation set.
        assert_eq!(index.collect_versions(3), 2);
        assert!(index.versioned.is_empty());

        // The committed head is retained permanently.
        let head = index.latest(&hot).unwrap();
        assert_eq!(head.sequence, 3);
        assert_eq!(head.values.as_ref().unwrap()["code"], "latest".into());
        // Every cold key still has its single head.
        for key in 0..128 {
            let row_id = id(key);
            if row_id == hot {
                continue;
            }
            assert_eq!(index.latest(&row_id).unwrap().sequence, 1);
        }
    }

    #[test]
    fn revisions_after_collection_are_tracked_again() {
        let mut index = RowIndex::new();
        let key = id(4);
        index.push(key.clone(), version(1, "one"));
        index.push(key.clone(), version(2, "two"));
        assert_eq!(index.collect_versions(2), 1);
        assert!(index.versioned.is_empty());

        index.push(key.clone(), version(3, "three"));
        assert!(index
            .versioned
            .get("branch")
            .unwrap()
            .contains(&Key::Integer(4)));
        assert_eq!(
            index.version_at(&key, 2).unwrap().values.as_ref().unwrap()["code"],
            "two".into()
        );
        assert_eq!(index.collect_versions(3), 1);
        assert_eq!(index.latest(&key).unwrap().sequence, 3);
        assert!(index.versioned.is_empty());
    }

    #[test]
    fn pinned_snapshot_keeps_exactly_one_visible_version() {
        let mut index = RowIndex::new();
        let key = id(9);
        for (sequence, code) in [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")] {
            index.push(key.clone(), version(sequence, code));
        }
        // Oldest reader is pinned at sequence 3: versions 1..2 are obsolete,
        // version 3 stays readable, newer versions stay untouched.
        assert_eq!(index.collect_versions(3), 2);
        assert_eq!(
            index.version_at(&key, 3).unwrap().values.as_ref().unwrap()["code"],
            "c".into()
        );
        assert_eq!(index.latest(&key).unwrap().sequence, 5);
        // After that reader leaves, only the head survives.
        assert_eq!(index.collect_versions(5), 2);
        assert!(index.versioned.is_empty());
        assert_eq!(
            index.latest(&key).unwrap().values.as_ref().unwrap()["code"],
            "e".into()
        );
    }

    #[test]
    fn tracking_covers_multiple_tables_independently() {
        let mut index = RowIndex::new();
        let first = RowId::new("branch", 1);
        let second = RowId::new("badge", "x");
        index.push(first.clone(), version(1, "north"));
        index.push(second.clone(), RowVersion::new(1, 1, Some(Row::new())));
        index.push(first.clone(), version(2, "south"));
        assert_eq!(index.versioned.len(), 1);
        assert!(index.versioned.contains_key("branch"));
        assert!(!index.versioned.contains_key("badge"));

        assert_eq!(index.collect_versions(u64::MAX), 1);
        assert!(index.versioned.is_empty());
        assert_eq!(index.latest(&first).unwrap().sequence, 2);
        assert_eq!(index.latest(&second).unwrap().sequence, 1);
    }
}
