use std::sync::Arc;

use super::{parser, Command};
use crate::{
    constraints::Schema,
    error::{Error, Result},
    locks::{WaitGraph, WaitTicket},
    rows::TxId,
    tx::{
        CheckpointInfo, Engine, Event, Operation, OperationResult, RequestId, Step, TransactionInfo,
    },
    wal::CommitLog,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Began(TransactionInfo),
    Operation(OperationResult),
    Waiting(WaitTicket),
    RolledBack {
        id: TxId,
    },
    Checkpoint(CheckpointInfo),
    Locks(WaitGraph),
    Events {
        oldest: u64,
        newest: u64,
        events: Vec<(u64, Event)>,
    },
    History(Vec<CommitLog>),
    Transaction(TransactionInfo),
    Schema(Schema),
}

pub struct Session {
    client: String,
    engine: Arc<Engine>,
    active: Option<TxId>,
    waiting: Option<WaitTicket>,
}

impl Session {
    pub fn new(engine: Arc<Engine>, client: impl Into<String>) -> Result<Self> {
        let client = client.into();
        RequestId::new(&client, "connect")?;
        Ok(Self {
            client,
            engine,
            active: None,
            waiting: None,
        })
    }

    pub fn transaction(&self) -> Option<TxId> {
        self.active
    }

    pub fn start(&mut self, request: &str, sql: &str) -> Result<Response> {
        let command = parser::parse(sql)?;
        let identity = RequestId::new(&self.client, request)?;
        if let Some(result) = self.engine.receipt(&identity)? {
            if matches!(command, Command::Commit) && self.active.is_none() {
                return Ok(Response::Operation(OperationResult::Committed(result)));
            }
            return Err(Error::DuplicateRequest);
        }
        match command {
            Command::Begin(isolation) => {
                if self.active.is_some() {
                    return Err(Error::TransactionAlreadyOpen);
                }
                let transaction = self.engine.begin(isolation)?;
                self.active = Some(transaction.id);
                Ok(Response::Began(transaction))
            }
            Command::Transaction(operation) => self.start_operation(operation),
            Command::Select { interval, limit } => {
                let limit = limit.unwrap_or(self.engine.options().max_scan);
                self.start_operation(Operation::Scan { interval, limit })
            }
            Command::Commit => self.start_operation(Operation::Commit(Some(identity))),
            Command::Rollback => {
                let id = self.require_active()?;
                let result = self.engine.rollback(id);
                if result.is_ok() || result.as_ref().is_err_and(|error| error.ends_transaction()) {
                    self.clear();
                }
                result.map(|()| Response::RolledBack { id })
            }
            Command::Checkpoint => self.engine.checkpoint().map(Response::Checkpoint),
            Command::ShowLocks => self.engine.wait_graph().map(Response::Locks),
            Command::ShowEvents { after } => {
                self.engine
                    .events(after)
                    .map(|(oldest, newest, events)| Response::Events {
                        oldest,
                        newest,
                        events,
                    })
            }
            Command::ShowHistory { after, limit } => {
                self.engine.history(after, limit).map(Response::History)
            }
            Command::ShowTransaction => self
                .engine
                .transaction(self.require_active()?)
                .map(Response::Transaction),
            Command::ShowSchema => Ok(Response::Schema(self.engine.schema().clone())),
        }
    }

    pub fn poll(&mut self) -> Result<Response> {
        let ticket = self.waiting.ok_or(Error::BadWaitTicket)?;
        let response = self.engine.resume(ticket);
        self.step(response)
    }

    pub fn execute(&mut self, request: &str, sql: &str) -> Result<Response> {
        match self.start(request, sql)? {
            Response::Waiting(ticket) => {
                let result = self
                    .engine
                    .wait(ticket, self.engine.options().lock_timeout)
                    .map(Step::Complete);
                self.step(result)
            }
            response => Ok(response),
        }
    }

    fn start_operation(&mut self, operation: Operation) -> Result<Response> {
        let transaction = self.require_active()?;
        if self.waiting.is_some() {
            return Err(Error::TransactionBusy(transaction));
        }
        let result = self.engine.start(transaction, operation);
        self.step(result)
    }

    fn step(&mut self, result: Result<Step>) -> Result<Response> {
        match result {
            Ok(Step::Waiting(ticket)) => {
                self.waiting = Some(ticket);
                Ok(Response::Waiting(ticket))
            }
            Ok(Step::Complete(value)) => {
                self.waiting = None;
                if matches!(value, OperationResult::Committed(_)) {
                    self.clear();
                }
                Ok(Response::Operation(value))
            }
            Err(error) => {
                self.waiting = None;
                if error.ends_transaction() {
                    self.clear();
                }
                Err(error)
            }
        }
    }

    fn require_active(&self) -> Result<TxId> {
        self.active.ok_or(Error::NoTransaction)
    }

    fn clear(&mut self) {
        self.active = None;
        self.waiting = None;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(id) = self.active {
            let _ = self.engine.rollback(id);
        }
    }
}
