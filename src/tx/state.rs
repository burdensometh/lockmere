use std::collections::{BTreeMap, VecDeque};

use crate::{
    constraints::Schema,
    error::{Error, Result},
    locks::{Acquire, LockManager, LockMode, LockRequest, WaitTicket},
    rows::{ReadInterval, Record, Sequence, TxId},
    wal::{CommitLog, DurableState, FaultPoint},
};

use super::{
    workspace::{Change, StatementContext, Workspace},
    CheckpointInfo, Event, Isolation, Operation, OperationResult, Options, RequestId, Step,
    TransactionInfo,
};

pub(crate) struct State {
    journal: crate::wal::Journal,
    pub durable: DurableState,
    pub locks: LockManager,
    transactions: BTreeMap<TxId, Workspace>,
    events: VecDeque<(u64, Event)>,
    ready: Vec<WaitTicket>,
    newest_event: u64,
    newest_transaction: TxId,
    poisoned: bool,
}

impl State {
    pub fn new(journal: crate::wal::Journal, durable: DurableState) -> Self {
        Self {
            newest_transaction: durable.highest_tx,
            journal,
            durable,
            locks: LockManager::default(),
            transactions: BTreeMap::new(),
            ready: Vec::new(),
            events: VecDeque::new(),
            newest_event: 0,
            poisoned: false,
        }
    }

    pub fn available(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::StorageUnavailable)
        } else {
            Ok(())
        }
    }

    pub fn begin(&mut self, isolation: Isolation, options: &Options) -> Result<TransactionInfo> {
        self.available()?;
        if self.transactions.len() >= options.max_transactions {
            return Err(Error::Limit("active transactions"));
        }
        let id = self
            .newest_transaction
            .checked_add(1)
            .ok_or(Error::Limit("transaction identity space"))?;
        self.newest_transaction = id;
        let transaction = Workspace::new(id, self.durable.sequence, isolation);
        let info = transaction.info();
        self.transactions.insert(id, transaction);
        self.note(Event::Began(info.clone()), options);
        Ok(info)
    }

    pub fn start(
        &mut self,
        id: TxId,
        operation: Operation,
        schema: &Schema,
        options: &Options,
    ) -> Result<Step> {
        self.available()?;
        let transaction = self
            .transactions
            .get(&id)
            .ok_or(Error::UnknownTransaction(id))?;
        if transaction.parked.is_some() {
            return Err(Error::TransactionBusy(id));
        }
        let requested =
            requested_lock(id, transaction.info.isolation, &operation, schema, options)?;
        if let Some(request) = requested {
            let interval = request.interval.clone();
            match self.locks.enqueue(request)? {
                Acquire::Granted => {}
                Acquire::Waiting(ticket) => {
                    let label = operation.label();
                    self.transactions
                        .get_mut(&id)
                        .expect("active transaction")
                        .parked = Some((ticket, operation));
                    self.note(
                        Event::Waiting {
                            ticket,
                            operation: label,
                            interval,
                        },
                        options,
                    );
                    return Ok(Step::Waiting(ticket));
                }
                Acquire::Deadlock(cycle) => {
                    self.finish(id, Some("deadlock"), options);
                    return Err(Error::Deadlock { victim: id, cycle });
                }
            }
        }
        self.execute(id, operation, schema, options)
            .map(Step::Complete)
    }

    pub fn resume(
        &mut self,
        ticket: WaitTicket,
        schema: &Schema,
        options: &Options,
    ) -> Result<Step> {
        self.available()?;
        let tx = self
            .transactions
            .get_mut(&ticket.transaction)
            .ok_or(Error::UnknownTransaction(ticket.transaction))?;
        if tx.parked.as_ref().map(|entry| entry.0) != Some(ticket) {
            return Err(Error::BadWaitTicket);
        }
        if self.locks.claim(ticket)? == Acquire::Waiting(ticket) {
            return Ok(Step::Waiting(ticket));
        }
        let (_, operation) = tx.parked.take().expect("parked operation");
        self.note(Event::Resumed { ticket }, options);
        self.update_ready(options);
        self.execute(ticket.transaction, operation, schema, options)
            .map(Step::Complete)
    }

    pub fn timeout(&mut self, ticket: WaitTicket, options: &Options) -> Result<()> {
        let tx = self
            .transactions
            .get_mut(&ticket.transaction)
            .ok_or(Error::UnknownTransaction(ticket.transaction))?;
        if tx.parked.as_ref().map(|entry| entry.0) != Some(ticket) {
            return Err(Error::BadWaitTicket);
        }
        self.locks.cancel(ticket)?;
        tx.parked = None;
        self.note(Event::TimedOut { ticket }, options);
        self.update_ready(options);
        Ok(())
    }

    pub fn rollback(&mut self, transaction: TxId, options: &Options) -> Result<()> {
        self.available()?;
        if !self.transactions.contains_key(&transaction) {
            return Err(Error::UnknownTransaction(transaction));
        }
        self.finish(transaction, Some("rollback"), options);
        Ok(())
    }

    pub fn transaction(&self, transaction: TxId) -> Result<TransactionInfo> {
        self.available()?;
        self.transactions
            .get(&transaction)
            .map(Workspace::info)
            .ok_or(Error::UnknownTransaction(transaction))
    }

    pub fn history(&self, after: Sequence, limit: usize) -> Result<Vec<CommitLog>> {
        self.available()?;
        self.journal.history(after, limit)
    }

    pub fn checkpoint(&mut self, options: &Options) -> Result<CheckpointInfo> {
        self.available()?;
        let journal_bytes = self
            .journal
            .checkpoint(&self.durable, options.fault.as_ref())?;
        let oldest_snapshot = self
            .transactions
            .values()
            .map(|tx| tx.info.snapshot)
            .min()
            .unwrap_or(self.durable.sequence);
        let collected_versions = self.durable.index.collect_versions(oldest_snapshot);
        let info = CheckpointInfo {
            sequence: self.durable.sequence,
            journal_bytes,
            collected_versions,
        };
        self.note(Event::Checkpoint(info.clone()), options);
        Ok(info)
    }

    pub fn events(&self, after: u64) -> (u64, u64, Vec<(u64, Event)>) {
        let oldest = self
            .events
            .front()
            .map_or(self.newest_event.saturating_add(1), |(id, _)| *id);
        let events = self
            .events
            .iter()
            .filter(|(id, _)| *id > after)
            .cloned()
            .collect();
        (oldest, self.newest_event, events)
    }

    fn execute(
        &mut self,
        id: TxId,
        operation: Operation,
        schema: &Schema,
        options: &Options,
    ) -> Result<OperationResult> {
        let mut events = Vec::new();
        let outcome = match operation {
            Operation::Commit(receipt) => self.commit(id, receipt, schema, options, &mut events),
            operation => {
                let tx = self.transactions.get_mut(&id).expect("active transaction");
                let mut context = StatementContext {
                    schema,
                    index: &mut self.durable.index,
                    options,
                    events: &mut events,
                };
                execute_statement(tx, operation, &mut context)
            }
        };
        for event in events {
            self.note(event, options);
        }
        match &outcome {
            Ok(OperationResult::Committed(result)) => {
                self.note(Event::Committed(result.clone()), options);
                self.finish(id, None, options);
            }
            Err(error) if error.ends_transaction() => self.finish(id, Some(error.code()), options),
            _ => {}
        }
        outcome
    }

    fn commit(
        &mut self,
        id: TxId,
        receipt: Option<RequestId>,
        schema: &Schema,
        options: &Options,
        events: &mut Vec<Event>,
    ) -> Result<OperationResult> {
        if receipt
            .as_ref()
            .is_some_and(|request| self.durable.receipts.contains_key(request))
        {
            return Err(Error::DuplicateRequest);
        }
        let tx = self.transactions.get(&id).expect("active transaction");
        let mut context = StatementContext {
            schema,
            index: &mut self.durable.index,
            options,
            events,
        };
        tx.validate_commit(&mut context)?;
        let sequence = self
            .durable
            .sequence
            .checked_add(1)
            .ok_or(Error::Limit("commit sequence space"))?;
        let record = CommitLog {
            sequence,
            transaction: id,
            receipt,
            changes: tx.intents.changes().collect(),
        };
        let bytes = record.encode()?;
        hit(options, FaultPoint::BeforeAppend)?;
        if let Err(error) = self
            .journal
            .append(&bytes)
            .and_then(|()| hit(options, FaultPoint::AfterSyncBeforePublish))
        {
            self.poisoned = true;
            let other_ids: Vec<_> = self
                .transactions
                .keys()
                .copied()
                .filter(|transaction| *transaction != id)
                .collect();
            for transaction in other_ids {
                self.finish(transaction, Some("STORAGE_UNAVAILABLE"), options);
            }
            return Err(error);
        }
        let result = record.result();
        self.durable.publish(record);
        Ok(OperationResult::Committed(result))
    }

    fn finish(&mut self, id: TxId, reason: Option<&'static str>, options: &Options) {
        self.transactions.remove(&id);
        self.locks.release(id);
        if let Some(reason) = reason {
            self.note(
                Event::Aborted {
                    transaction: id,
                    reason,
                },
                options,
            );
        }
        self.update_ready(options);
    }

    fn update_ready(&mut self, options: &Options) {
        let now = self.locks.ready();
        for &ticket in &now {
            if !self.ready.contains(&ticket) {
                self.note(Event::Ready { ticket }, options);
            }
        }
        self.ready = now;
    }

    fn note(&mut self, event: Event, options: &Options) {
        self.newest_event = self.newest_event.saturating_add(1);
        if options.event_capacity != 0 {
            if self.events.len() == options.event_capacity {
                self.events.pop_front();
            }
            self.events.push_back((self.newest_event, event));
        }
    }
}

fn hit(options: &Options, point: FaultPoint) -> Result<()> {
    options
        .fault
        .hit(point)
        .map_err(|_| Error::Injected(point.name()))
}

fn requested_lock(
    id: TxId,
    isolation: Isolation,
    operation: &Operation,
    schema: &Schema,
    options: &Options,
) -> Result<Option<LockRequest>> {
    let lock = match operation {
        Operation::Get(row) => {
            schema.validate_id(row)?;
            (isolation == Isolation::Serializable)
                .then(|| (ReadInterval::point(row), LockMode::Shared))
        }
        Operation::Scan { interval, limit } => {
            schema.validate_interval(interval)?;
            if *limit == 0 || *limit > options.max_scan {
                return Err(Error::Limit("scan rows"));
            }
            (isolation == Isolation::Serializable).then(|| (interval.clone(), LockMode::Shared))
        }
        Operation::Insert(record) => {
            schema.validate_fields(&record.id, &record.values)?;
            Some((ReadInterval::point(&record.id), LockMode::Exclusive))
        }
        Operation::Update { id, patch } => {
            schema.validate_fields(id, patch)?;
            if patch.is_empty() {
                return Err(Error::InvalidValue(
                    "an update must assign a column".to_owned(),
                ));
            }
            Some((ReadInterval::point(id), LockMode::Exclusive))
        }
        Operation::Delete(row) => {
            schema.validate_id(row)?;
            Some((ReadInterval::point(row), LockMode::Exclusive))
        }
        Operation::Commit(request) => {
            if let Some(request) = request {
                request.validate()?;
            }
            None
        }
        _ => None,
    };
    Ok(lock.map(|(interval, mode)| LockRequest::new(id, interval, mode)))
}

fn execute_statement(
    tx: &mut Workspace,
    operation: Operation,
    context: &mut StatementContext<'_>,
) -> Result<OperationResult> {
    match operation {
        Operation::Get(id) => {
            validate_read(tx, context, &ReadInterval::point(&id))?;
            let record = context
                .index
                .read(&id, tx.info.snapshot, &tx.intents)
                .map(|values| Record::new(id, values.clone()));
            Ok(OperationResult::Row(record))
        }
        Operation::Scan { interval, limit } => {
            validate_read(tx, context, &interval)?;
            Ok(OperationResult::Rows(context.index.scan(
                &interval,
                tx.info.snapshot,
                &tx.intents,
                limit,
            )))
        }
        Operation::Insert(record) => {
            tx.stage(context, &record.id, Change::Insert(record.values))?;
            Ok(OperationResult::Changed { id: record.id })
        }
        Operation::Update { id, patch } => {
            tx.stage(context, &id, Change::Update(patch))?;
            Ok(OperationResult::Changed { id })
        }
        Operation::Delete(id) => {
            tx.stage(context, &id, Change::Delete)?;
            Ok(OperationResult::Changed { id })
        }
        Operation::Savepoint(name) => {
            tx.savepoint(name.clone(), context.options.max_savepoints)?;
            Ok(OperationResult::Savepoint { name })
        }
        Operation::RollbackTo(name) => {
            tx.rollback_to(&name)?;
            Ok(OperationResult::RolledBack { name })
        }
        Operation::Release(name) => {
            tx.release(&name)?;
            Ok(OperationResult::Released { name })
        }
        Operation::SetConstraints { name, mode } => {
            let count = tx.set_constraints(context, name.as_deref(), mode)?;
            Ok(OperationResult::ConstraintsSet { count })
        }
        Operation::Commit(_) => unreachable!("commit handled by coordinator"),
    }
}

fn validate_read(
    tx: &mut Workspace,
    context: &StatementContext<'_>,
    interval: &ReadInterval,
) -> Result<()> {
    if tx.info.isolation == Isolation::Serializable {
        if let Some((key, sequence)) = context.index.first_change_after(interval, tx.info.snapshot)
        {
            return Err(Error::Serialization {
                transaction: tx.info.id,
                detail: format!("predicate {interval} changed at key {key} in commit {sequence}"),
            });
        }
    }
    tx.info.reads = tx.info.reads.saturating_add(1);
    Ok(())
}
