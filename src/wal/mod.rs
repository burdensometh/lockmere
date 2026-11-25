mod checkpoint;
mod codec;
mod journal;
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
