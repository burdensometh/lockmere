use std::{
    path::Path,
    sync::{Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{
    constraints::Schema,
    error::{Error, Result},
    locks::{WaitGraph, WaitTicket},
    rows::{Sequence, TxId},
    wal::{CommitLog, Journal},
};

use super::{
    state::State, CheckpointInfo, CommitResult, Event, Isolation, Operation, OperationResult,
    Options, RequestId, Step, TransactionInfo,
};

pub struct Engine {
    schema: Schema,
    options: Options,
    state: Mutex<State>,
    wake: Condvar,
}

impl Engine {
    pub fn create(path: impl AsRef<Path>, schema: Schema, options: Options) -> Result<Self> {
        options.validate()?;
        let (journal, schema, durable) = Journal::create(path.as_ref(), &schema)?;
        Ok(Self::new(schema, options, State::new(journal, durable)))
    }

    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        Self::open_with_schema(path, None, options)
    }

    pub fn open_with_schema(
        path: impl AsRef<Path>,
        schema: Option<&Schema>,
        options: Options,
    ) -> Result<Self> {
        options.validate()?;
        let (journal, schema, durable) = Journal::open(path.as_ref(), schema)?;
        Ok(Self::new(schema, options, State::new(journal, durable)))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn begin(&self, isolation: Isolation) -> Result<TransactionInfo> {
        self.guard()?.begin(isolation, &self.options)
    }

    pub fn begin_history(&self, sequence: Sequence) -> Result<TransactionInfo> {
        self.guard()?.begin_history(sequence, &self.options)
    }

    pub fn start(&self, id: TxId, operation: Operation) -> Result<Step> {
        let mut state = self.guard()?;
        if state.is_history_view(id) {
            return state
                .history_read(id, &operation, &self.schema, &self.options)
                .map(Step::Complete);
        }
        let result = state.start(id, operation, &self.schema, &self.options);
        self.wake.notify_all();
        result
    }

    pub fn resume(&self, ticket: WaitTicket) -> Result<Step> {
        let result = self.guard()?.resume(ticket, &self.schema, &self.options);
        if !matches!(result, Ok(Step::Waiting(_))) {
            self.wake.notify_all();
        }
        result
    }

    pub fn wait(&self, ticket: WaitTicket, timeout: Duration) -> Result<OperationResult> {
        let mut state = self.guard()?;
        let started = Instant::now();
        loop {
            match state.resume(ticket, &self.schema, &self.options) {
                Ok(Step::Complete(result)) => {
                    self.wake.notify_all();
                    return Ok(result);
                }
                Err(error) => {
                    self.wake.notify_all();
                    return Err(error);
                }
                Ok(Step::Waiting(_)) => {}
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                state.timeout(ticket, &self.options)?;
                self.wake.notify_all();
                return Err(Error::LockTimeout(ticket.transaction));
            }
            state = self
                .wake
                .wait_timeout(state, remaining)
                .map_err(|_| Error::StorageUnavailable)?
                .0;
        }
    }

    pub fn execute(&self, id: TxId, operation: Operation) -> Result<OperationResult> {
        match self.start(id, operation)? {
            Step::Complete(result) => Ok(result),
            Step::Waiting(ticket) => self.wait(ticket, self.options.lock_timeout),
        }
    }

    pub fn transaction(&self, id: TxId) -> Result<TransactionInfo> {
        self.guard()?.transaction(id)
    }

    pub fn rollback(&self, id: TxId) -> Result<()> {
        let result = self.guard()?.rollback(id, &self.options);
        self.wake.notify_all();
        result
    }

    pub fn receipt(&self, id: &RequestId) -> Result<Option<CommitResult>> {
        let state = self.guard()?;
        state.available()?;
        Ok(state.durable.receipt(id))
    }

    pub fn wait_graph(&self) -> Result<WaitGraph> {
        let state = self.guard()?;
        state.available()?;
        Ok(state.locks.graph())
    }

    pub fn events(&self, after: u64) -> Result<(u64, u64, Vec<(u64, Event)>)> {
        Ok(self.guard()?.events(after))
    }

    pub fn sequence(&self) -> Result<Sequence> {
        let state = self.guard()?;
        state.available()?;
        Ok(state.durable.sequence)
    }

    pub fn checkpoint(&self) -> Result<CheckpointInfo> {
        self.guard()?.checkpoint(&self.options)
    }

    pub fn history(&self, after: Sequence, limit: usize) -> Result<Vec<CommitLog>> {
        self.guard()?.history(after, limit)
    }

    fn guard(&self) -> Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Error::StorageUnavailable)
    }

    fn new(schema: Schema, options: Options, state: State) -> Self {
        Self {
            schema,
            options,
            state: Mutex::new(state),
            wake: Condvar::new(),
        }
    }
}
