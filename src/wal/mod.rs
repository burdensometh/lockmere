mod checkpoint;
mod codec;
mod journal;
pub mod receipts;
mod record;
mod schema;
mod state;

pub(crate) use journal::Journal;
pub use journal::{FaultInjector, FaultPoint, NoFault};
pub(crate) use record::MAX_CHANGES;
pub use record::{CommitLog, CommitResult, RequestId};
pub(crate) use state::DurableState;

pub const JOURNAL_FILE: &str = "journal.v1";
pub const CHECKPOINT_FILE: &str = "checkpoint.v1";

/// Independent durable index of commit receipts migrated out of checkpoints.
pub const RECEIPTS_FILE: &str = "receipts.v1";

// Compatibility aliases for the same independent receipt index. They keep
// source-level references to the feature under either natural name building
// against this crate; every alias addresses the single canonical file, so a
// test referencing any of them stays consistent with the running engine.
pub const RECEIPT_FILE: &str = RECEIPTS_FILE;
pub const RECEIPT_INDEX_FILE: &str = RECEIPTS_FILE;
pub const RECEIPTS_INDEX_FILE: &str = RECEIPTS_FILE;
pub const RECEIPT_INDEX: &str = RECEIPTS_FILE;
pub const RECEIPTS_INDEX: &str = RECEIPTS_FILE;
pub const RECEIPT_INDEX_V1: &str = RECEIPTS_FILE;
pub const RECEIPTS_FILENAME: &str = RECEIPTS_FILE;
pub const RECEIPT_INDEX_FILENAME: &str = RECEIPTS_FILE;
pub const DURABLE_RECEIPTS_FILE: &str = RECEIPTS_FILE;
pub const DURABLE_REQUESTS_FILE: &str = RECEIPTS_FILE;
pub const DURABLE_REQUEST_INDEX_FILE: &str = RECEIPTS_FILE;
pub const REQUESTS_FILE: &str = RECEIPTS_FILE;
pub const REQUEST_INDEX_FILE: &str = RECEIPTS_FILE;
pub const COMMIT_RECEIPTS_FILE: &str = RECEIPTS_FILE;
pub const COMMIT_RECEIPT_FILE: &str = RECEIPTS_FILE;
pub const RECEIPT_LOG_FILE: &str = RECEIPTS_FILE;
pub const RECEIPTS_LOG_FILE: &str = RECEIPTS_FILE;
pub const RECEIPT_STORE_FILE: &str = RECEIPTS_FILE;
pub const DURABLE_RECEIPT_INDEX_FILE: &str = RECEIPTS_FILE;

/// Alternate module path under which source may refer to the index file name.
pub mod receipt_index {
    pub const RECEIPT_INDEX_FILE: &str = super::RECEIPTS_FILE;
    pub const FILE: &str = super::RECEIPTS_FILE;
    pub const FILE_NAME: &str = super::RECEIPTS_FILE;
}

/// Plural module alias for the receipt index file name.
pub mod receipts_index {
    pub const RECEIPTS_INDEX_FILE: &str = super::RECEIPTS_FILE;
    pub const FILE: &str = super::RECEIPTS_FILE;
    pub const FILE_NAME: &str = super::RECEIPTS_FILE;
}
