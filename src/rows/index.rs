use std::collections::BTreeMap;

use super::{
    Key, PendingRows, ReadInterval, Record, Row, RowId, RowVersion, Sequence, TxId, VersionChange,
};

type VersionChain = Vec<RowVersion>;
type TableIndex = BTreeMap<Key, VersionChain>;

#[derive(Debug, Default)]
pub struct RowIndex {
    tables: BTreeMap<String, TableIndex>,
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
        for change in changes {
            self.push(
                change.id,
                RowVersion::new(sequence, transaction, change.values),
            );
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
