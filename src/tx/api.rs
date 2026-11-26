use std::{sync::Arc, time::Duration};

use crate::{
    constraints::{ConstraintCheck, ConstraintMode},
    error::{Error, Result},
    locks::WaitTicket,
    rows::{ReadInterval, Record, Row, RowId, Sequence, TxId},
    wal::{CommitResult, FaultInjector, NoFault, RequestId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    Snapshot,
    Serializable,
}

impl Isolation {
    pub fn name(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Serializable => "serializable",
        }
    }
}

#[derive(Clone)]
pub struct Options {
    pub lock_timeout: Duration,
    pub max_transactions: usize,
    pub max_intents: usize,
    pub max_scan: usize,
    pub max_savepoints: usize,
    pub max_undo: usize,
    pub event_capacity: usize,
    pub fault: Arc<dyn FaultInjector>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(10),
            max_transactions: 256,
            max_intents: 4096,
            max_scan: 4096,
            max_savepoints: 128,
            max_undo: 16384,
            event_capacity: 2048,
            fault: Arc::new(NoFault),
        }
    }
}

impl Options {
    pub fn validate(&self) -> Result<()> {
        if self.max_transactions == 0
            || self.max_intents == 0
            || self.max_intents > crate::wal::MAX_CHANGES
            || self.max_scan == 0
            || self.max_savepoints == 0
            || self.max_undo == 0
            || self.lock_timeout > Duration::from_secs(3600)
        {
            return Err(Error::InvalidRequest("invalid engine limits".to_owned()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Get(RowId),
    Scan {
        interval: ReadInterval,
        limit: usize,
    },
    Insert(Record),
    Update {
        id: RowId,
        patch: Row,
    },
    Delete(RowId),
    Savepoint(String),
    RollbackTo(String),
    Release(String),
    SetConstraints {
        name: Option<String>,
        mode: ConstraintMode,
    },
    Commit(Option<RequestId>),
}

impl Operation {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Get(_) => "get",
            Self::Scan { .. } => "scan",
            Self::Insert(_) => "insert",
            Self::Update { .. } => "update",
            Self::Delete(_) => "delete",
            Self::Savepoint(_) => "savepoint",
            Self::RollbackTo(_) => "rollback_to",
            Self::Release(_) => "release",
            Self::SetConstraints { .. } => "set_constraints",
            Self::Commit(_) => "commit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationResult {
    Row(Option<Record>),
    Rows(Vec<Record>),
    Changed { id: RowId },
    Savepoint { name: String },
    RolledBack { name: String },
    Released { name: String },
    ConstraintsSet { count: usize },
    Committed(CommitResult),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Complete(OperationResult),
    Waiting(WaitTicket),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInfo {
    pub id: TxId,
    pub snapshot: Sequence,
    pub isolation: Isolation,
    pub writes: usize,
    pub savepoints: usize,
    pub reads: u64,
    pub waiting: Option<WaitTicket>,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub sequence: Sequence,
    pub journal_bytes: u64,
    pub collected_versions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Began(TransactionInfo),
    Waiting {
        ticket: WaitTicket,
        operation: &'static str,
        interval: ReadInterval,
    },
    Ready {
        ticket: WaitTicket,
    },
    Resumed {
        ticket: WaitTicket,
    },
    TimedOut {
        ticket: WaitTicket,
    },
    Check {
        transaction: TxId,
        result: ConstraintCheck,
    },
    Aborted {
        transaction: TxId,
        reason: &'static str,
    },
    Committed(CommitResult),
    Checkpoint(CheckpointInfo),
}
