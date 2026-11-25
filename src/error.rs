use std::{fmt, io};

use crate::rows::{RowId, TxId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Io {
        operation: &'static str,
        message: String,
    },
    Corrupt {
        file: &'static str,
        offset: u64,
        message: String,
    },
    StorageUnavailable,
    StoreInUse,
    StoreExists,
    InvalidSchema(String),
    SchemaMismatch,
    InvalidValue(String),
    UnknownTable(String),
    UnknownColumn {
        table: String,
        column: String,
    },
    MissingRow(RowId),
    DuplicateKey(RowId),
    ConstraintViolation {
        name: String,
        detail: String,
    },
    UnknownConstraint(String),
    NotDeferrable(String),
    UnknownTransaction(TxId),
    TransactionBusy(TxId),
    BadWaitTicket,
    LockTimeout(TxId),
    Deadlock {
        victim: TxId,
        cycle: Vec<TxId>,
    },
    Serialization {
        transaction: TxId,
        detail: String,
    },
    InvalidSavepoint(String),
    NoTransaction,
    TransactionAlreadyOpen,
    DuplicateRequest,
    InvalidRequest(String),
    Limit(&'static str),
    Injected(&'static str),
}

impl Error {
    pub(crate) fn io(operation: &'static str, error: io::Error) -> Self {
        Self::Io {
            operation,
            message: error.to_string(),
        }
    }

    pub(crate) fn corrupt(file: &'static str, offset: u64, message: impl Into<String>) -> Self {
        Self::Corrupt {
            file,
            offset,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "IO",
            Self::Corrupt { .. } => "CORRUPT",
            Self::StorageUnavailable => "STORAGE_UNAVAILABLE",
            Self::StoreInUse => "STORE_IN_USE",
            Self::StoreExists => "STORE_EXISTS",
            Self::InvalidSchema(_) => "INVALID_SCHEMA",
            Self::SchemaMismatch => "SCHEMA_MISMATCH",
            Self::InvalidValue(_) => "INVALID_VALUE",
            Self::UnknownTable(_) => "UNKNOWN_TABLE",
            Self::UnknownColumn { .. } => "UNKNOWN_COLUMN",
            Self::MissingRow(_) => "MISSING_ROW",
            Self::DuplicateKey(_) => "DUPLICATE_KEY",
            Self::ConstraintViolation { .. } => "CONSTRAINT_VIOLATION",
            Self::UnknownConstraint(_) => "UNKNOWN_CONSTRAINT",
            Self::NotDeferrable(_) => "NOT_DEFERRABLE",
            Self::UnknownTransaction(_) => "UNKNOWN_TRANSACTION",
            Self::TransactionBusy(_) => "TRANSACTION_BUSY",
            Self::BadWaitTicket => "BAD_WAIT_TICKET",
            Self::LockTimeout(_) => "LOCK_TIMEOUT",
            Self::Deadlock { .. } => "DEADLOCK",
            Self::Serialization { .. } => "SERIALIZATION",
            Self::InvalidSavepoint(_) => "INVALID_SAVEPOINT",
            Self::NoTransaction => "NO_TRANSACTION",
            Self::TransactionAlreadyOpen => "TRANSACTION_ALREADY_OPEN",
            Self::DuplicateRequest => "DUPLICATE_REQUEST",
            Self::InvalidRequest(_) => "INVALID_REQUEST",
            Self::Limit(_) => "LIMIT",
            Self::Injected(_) => "INJECTED",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Deadlock { .. } | Self::Serialization { .. } | Self::LockTimeout(_)
        )
    }

    pub fn ends_transaction(&self) -> bool {
        matches!(
            self,
            Self::Deadlock { .. }
                | Self::Serialization { .. }
                | Self::UnknownTransaction(_)
                | Self::StorageUnavailable
                | Self::Injected(_)
                | Self::Io { .. }
                | Self::Corrupt { .. }
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, message } => write!(output, "{operation}: {message}"),
            Self::Corrupt {
                file,
                offset,
                message,
            } => {
                write!(output, "{file} at byte {offset}: {message}")
            }
            Self::StorageUnavailable => write!(output, "storage is unavailable until reopened"),
            Self::StoreInUse => write!(output, "database directory is already open"),
            Self::StoreExists => write!(output, "database already exists"),
            Self::InvalidSchema(message)
            | Self::InvalidValue(message)
            | Self::InvalidRequest(message) => output.write_str(message),
            Self::SchemaMismatch => write!(output, "schema differs from the persisted schema"),
            Self::UnknownTable(table) => write!(output, "unknown table {table}"),
            Self::UnknownColumn { table, column } => {
                write!(output, "unknown column {table}.{column}")
            }
            Self::MissingRow(row) => write!(output, "row {row} does not exist in this view"),
            Self::DuplicateKey(row) => write!(output, "row {row} already exists"),
            Self::ConstraintViolation { name, detail } => write!(output, "{name}: {detail}"),
            Self::UnknownConstraint(name) => write!(output, "unknown constraint {name}"),
            Self::NotDeferrable(name) => write!(output, "constraint {name} is not deferrable"),
            Self::UnknownTransaction(id) => write!(output, "transaction {id} is no longer active"),
            Self::TransactionBusy(id) => write!(output, "transaction {id} has a pending operation"),
            Self::BadWaitTicket => {
                write!(output, "wait ticket does not match the pending operation")
            }
            Self::LockTimeout(id) => write!(output, "lock wait expired for transaction {id}"),
            Self::Deadlock { victim, cycle } => {
                write!(output, "transaction {victim} closed wait cycle {cycle:?}")
            }
            Self::Serialization {
                transaction,
                detail,
            } => write!(output, "transaction {transaction}: {detail}"),
            Self::InvalidSavepoint(name) => write!(output, "unknown or invalid savepoint {name}"),
            Self::NoTransaction => write!(output, "no transaction is active"),
            Self::TransactionAlreadyOpen => write!(output, "a transaction is already active"),
            Self::DuplicateRequest => {
                write!(output, "request identity already belongs to a commit")
            }
            Self::Limit(detail) => write!(output, "limit exceeded: {detail}"),
            Self::Injected(point) => write!(output, "fault injected at {point}"),
        }
    }
}

impl std::error::Error for Error {}
