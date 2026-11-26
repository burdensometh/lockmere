use std::{
    path::Path,
    sync::{Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{
    constraints::Schema,
    error::{Error, Result},
    locks::{WaitGraph, WaitPhase, WaitTicket},
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

    pub fn start(&self, id: TxId, operation: Operation) -> Result<Step> {
        let result = self
            .guard()?
            .start(id, operation, &self.schema, &self.options);
        self.wake.notify_all();
        result
    }

    /// Drive a parked operation by one non-blocking step.
    ///
    /// This is the manual-polling half of the wait lifecycle; blocking
    /// [`Engine::wait`] loops over the same [`State::resume`] transition.
    pub fn resume(&self, ticket: WaitTicket) -> Result<Step> {
        let result = self.guard()?.resume(ticket, &self.schema, &self.options);
        if !matches!(result, Ok(Step::Waiting(_))) {
            self.wake.notify_all();
        }
        result
    }

    /// Consolidated manual-poll entry point; identical to [`Engine::resume`].
    pub fn poll(&self, ticket: WaitTicket) -> Result<Step> {
        self.resume(ticket)
    }

    /// Non-blocking poll that maps a delivered step to its result.
    ///
    /// Returns `Ok(None)` while the ticket is still waiting and `Ok(Some(_))`
    /// exactly once, when the parked operation completes.
    pub fn try_wait(&self, ticket: WaitTicket) -> Result<Option<OperationResult>> {
        match self.resume(ticket)? {
            Step::Complete(result) => Ok(Some(result)),
            Step::Waiting(_) => Ok(None),
        }
    }

    /// Apply a lock-wait timeout to a ticket from the manual polling path.
    ///
    /// Only the pending wait/operation is discarded. Granted locks and the
    /// transaction workspace (writes, savepoints, constraint modes) are
    /// retained, so the transaction can issue another statement or retry.
    /// The blocking [`Engine::wait`] uses the same [`State::timeout`] cleanup
    /// when its deadline expires.
    pub fn timeout(&self, ticket: WaitTicket) -> Result<()> {
        let result = self.guard()?.timeout(ticket, &self.options);
        if result.is_ok() {
            self.wake.notify_all();
        }
        result
    }

    /// Alias for [`Engine::timeout`] using the lock manager's cancellation verb.
    pub fn cancel(&self, ticket: WaitTicket) -> Result<()> {
        self.timeout(ticket)
    }

    /// Tickets whose blockers have cleared and are ready to claim, in order.
    pub fn ready(&self) -> Result<Vec<WaitTicket>> {
        let state = self.guard()?;
        state.available()?;
        Ok(state.locks.ready())
    }

    /// Observable lifecycle phase of a ticket (`waiting`/`ready`/terminal).
    pub fn wait_phase(&self, ticket: WaitTicket) -> Result<Option<WaitPhase>> {
        let state = self.guard()?;
        state.available()?;
        Ok(state.locks.phase(ticket))
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
