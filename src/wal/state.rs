use std::collections::BTreeMap;

use super::{record, CommitLog, CommitResult, RequestId};
use crate::{
    constraints::{Evaluator, Schema},
    rows::{PendingRows, RowIndex, Sequence, TxId},
};

#[derive(Default)]
pub(crate) struct DurableState {
    pub sequence: Sequence,
    pub highest_tx: TxId,
    pub index: RowIndex,
    pub committed: BTreeMap<TxId, Sequence>,
    pub receipts: BTreeMap<RequestId, CommitResult>,
}

impl DurableState {
    pub fn receipt(&self, request: &RequestId) -> Option<CommitResult> {
        let mut result = self.receipts.get(request)?.clone();
        result.replayed = true;
        Some(result)
    }

    pub fn apply_recovered(
        &mut self,
        schema: &Schema,
        record: CommitLog,
    ) -> std::result::Result<(), String> {
        self.validate_identity(&record)?;
        let mut staged = PendingRows::default();
        for change in &record.changes {
            schema
                .validate_id(&change.id)
                .map_err(|error| error.to_string())?;
            let previous = self.index.latest(&change.id);
            if let Some(values) = &change.values {
                if let Err(shape) = schema.check_durable_row(&change.id, values) {
                    return Err(shape.message_for(&change.id));
                }
            } else if !previous.is_some_and(|version| version.values.is_some()) {
                return Err(format!("journal deletes missing row {}", change.id));
            }
            staged.replace(
                change.id.clone(),
                Some(crate::rows::WriteIntent {
                    base: previous.map(|version| version.sequence),
                    values: change.values.clone(),
                }),
            );
        }
        let mut checks = Evaluator::new(schema, &self.index, self.sequence, &staged);
        if let Some(error) = checks.commit().iter().find_map(|check| check.as_error()) {
            return Err(error.to_string());
        }
        self.publish(record);
        Ok(())
    }

    pub fn publish(&mut self, record: CommitLog) {
        debug_assert!(self.validate_identity(&record).is_ok());
        let result = record.result();
        self.sequence = record.sequence;
        self.highest_tx = self.highest_tx.max(record.transaction);
        self.committed.insert(record.transaction, record.sequence);
        self.index
            .publish(record.sequence, record.transaction, record.changes);
        if let Some(receipt) = record.receipt {
            self.receipts.insert(receipt, result);
        }
    }

    pub fn validate_identity(&self, record: &CommitLog) -> std::result::Result<(), String> {
        if self.sequence.checked_add(1) != Some(record.sequence) {
            return Err("journal sequence is not consecutive".to_owned());
        }
        if record.transaction == 0 || self.committed.contains_key(&record.transaction) {
            return Err("journal transaction identity is zero or repeated".to_owned());
        }
        if record.changes.len() > record::MAX_CHANGES {
            return Err("commit contains too many changes".to_owned());
        }
        if record
            .receipt
            .as_ref()
            .is_some_and(|id| self.receipts.contains_key(id))
        {
            return Err("journal repeats a durable client request".to_owned());
        }
        Ok(())
    }
}
