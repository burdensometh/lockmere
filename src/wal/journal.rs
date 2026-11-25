use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use super::{
    checkpoint::{self, Anchor},
    codec, schema, CommitLog, DurableState, JOURNAL_FILE,
};
use crate::{
    constraints::Schema,
    error::{Error, Result},
    rows::Sequence,
};

const LEASE: &str = ".lockmere.lock";
const INITIAL: &str = ".journal.v1.initial";
const TEMPORARY_CHECKPOINT: &str = ".checkpoint.v1.temporary";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeAppend,
    AfterSyncBeforePublish,
    BeforeCheckpointPublish,
}

impl FaultPoint {
    pub fn name(self) -> &'static str {
        match self {
            Self::BeforeAppend => "before_append",
            Self::AfterSyncBeforePublish => "after_sync_before_publish",
            Self::BeforeCheckpointPublish => "before_checkpoint_publish",
        }
    }
}

pub trait FaultInjector: Send + Sync {
    fn hit(&self, point: FaultPoint) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoFault;

impl FaultInjector for NoFault {
    fn hit(&self, _: FaultPoint) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Journal {
    root: PathBuf,
    file: File,
    _lease: File,
    data_start: u64,
    fingerprint: u64,
    anchor: Anchor,
}

impl Journal {
    pub fn create(root: &Path, schema: &Schema) -> Result<(Self, Schema, DurableState)> {
        let header = schema::encode(schema, &new_identity()?)?;
        codec::io_result("create database directory", fs::create_dir_all(root))?;
        let lease = acquire_lease(root)?;
        let final_path = root.join(JOURNAL_FILE);
        if codec::io_result("check journal", final_path.try_exists())? {
            return Err(Error::StoreExists);
        }
        let temporary = root.join(INITIAL);
        let mut file = open_temporary(&temporary)?;
        codec::io_result("write journal header", file.write_all(&header))?;
        codec::io_result("sync journal header", file.sync_all())?;
        codec::io_result(
            "install journal header",
            fs::rename(&temporary, &final_path),
        )?;
        checkpoint::sync_directory(root)?;
        Self::load(root, lease, false, None)
    }

    pub fn open(root: &Path, expected: Option<&Schema>) -> Result<(Self, Schema, DurableState)> {
        let lease = acquire_lease(root)?;
        Self::load(root, lease, true, expected)
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let end = codec::io_result("seek journal for append", self.file.seek(SeekFrom::End(0)))?;
        if end != self.anchor.end {
            return Err(Error::corrupt(
                JOURNAL_FILE,
                end,
                "journal length changed while open",
            ));
        }
        codec::io_result("append journal commit", self.file.write_all(bytes))?;
        codec::io_result("sync journal commit", self.file.sync_data())?;
        self.anchor = Anchor::record(end, bytes);
        Ok(())
    }

    pub fn checkpoint(&mut self, state: &DurableState, faults: &dyn FaultInjector) -> Result<u64> {
        let temporary = self.root.join(TEMPORARY_CHECKPOINT);
        let mut file = open_temporary(&temporary)?;
        checkpoint::write(&mut file, self.fingerprint, self.anchor, state)?;
        codec::io_result("sync checkpoint", file.sync_all())?;
        faults.hit(FaultPoint::BeforeCheckpointPublish)?;
        drop(file);
        checkpoint::publish(&self.root, &temporary)?;
        Ok(self.anchor.end)
    }

    pub fn history(&self, after: Sequence, limit: usize) -> Result<Vec<CommitLog>> {
        // The engine latch protects the live writer while the separate reader scans.
        let file = codec::io_result(
            "read journal history",
            File::open(self.root.join(JOURNAL_FILE)),
        )?;
        let mut reader = BufReader::new(file);
        codec::io_result(
            "seek history",
            reader.seek(SeekFrom::Start(self.data_start)),
        )?;
        let mut line = Vec::new();
        let mut offset = self.data_start;
        let mut expected = 1;
        let mut result = Vec::new();
        while offset < self.anchor.end && result.len() < limit {
            let record = read_required(&mut reader, &mut line, offset)?;
            if record.sequence != expected {
                return Err(Error::corrupt(
                    JOURNAL_FILE,
                    offset,
                    "nonconsecutive commit in journal history",
                ));
            }
            if record.sequence > after {
                result.push(record);
            }
            expected += 1;
            offset += line.len() as u64;
        }
        Ok(result)
    }

    fn load(
        root: &Path,
        lease: File,
        recover: bool,
        expected: Option<&Schema>,
    ) -> Result<(Self, Schema, DurableState)> {
        let file = codec::io_result(
            "open journal",
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.join(JOURNAL_FILE)),
        )?;
        let mut reader = BufReader::new(file);
        let (schema, data_start, fingerprint) = schema::read(&mut reader)?;
        if expected.is_some_and(|expected| expected != &schema) {
            return Err(Error::SchemaMismatch);
        }
        let mut anchor = Anchor::empty(data_start);
        let mut state = DurableState::default();
        if recover {
            if let Some((saved_anchor, saved_state)) = checkpoint::read(root, &schema, fingerprint)?
            {
                verify_anchor(&mut reader, saved_anchor, &saved_state, data_start)?;
                anchor = saved_anchor;
                state = saved_state;
            }
        }
        codec::io_result(
            "seek recovery target",
            reader.seek(SeekFrom::Start(anchor.end)),
        )?;
        let mut line = Vec::new();
        let mut torn = false;
        while codec::next_line(&mut reader, &mut line, JOURNAL_FILE, anchor.end)? {
            if line.last() != Some(&b'\n') {
                torn = true;
                break;
            }
            let record = decode_at(&line, anchor.end)?;
            state
                .apply_recovered(&schema, record)
                .map_err(|message| Error::corrupt(JOURNAL_FILE, anchor.end, message))?;
            anchor = Anchor::record(anchor.end, &line);
        }
        let file = reader.into_inner();
        if torn {
            codec::io_result("truncate torn journal suffix", file.set_len(anchor.end))?;
            codec::io_result("sync recovered journal", file.sync_data())?;
        }
        let journal = Self {
            root: root.to_path_buf(),
            file,
            _lease: lease,
            data_start,
            fingerprint,
            anchor,
        };
        Ok((journal, schema, state))
    }
}

fn new_identity() -> Result<String> {
    use std::fmt::Write as _;

    let mut source = codec::io_result("open system randomness", File::open("/dev/urandom"))?;
    let mut bytes = [0u8; 16];
    codec::io_result("read store identity", source.read_exact(&mut bytes))?;
    let mut identity = String::with_capacity(32);
    for byte in bytes {
        write!(identity, "{byte:02x}").expect("write to string");
    }
    Ok(identity)
}

fn acquire_lease(path: &Path) -> Result<File> {
    let file = codec::io_result(
        "open database lease",
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join(LEASE)),
    )?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(Error::StoreInUse),
        Err(TryLockError::Error(error)) => Err(Error::io("lock database lease", error)),
    }
}

fn open_temporary(path: &Path) -> Result<File> {
    codec::io_result(
        "open temporary durable file",
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path),
    )
}

fn verify_anchor(
    reader: &mut BufReader<File>,
    anchor: Anchor,
    state: &DurableState,
    data_start: u64,
) -> Result<()> {
    let bad = |message| Error::corrupt(super::CHECKPOINT_FILE, 0, message);
    let available = codec::io_result("stat journal", reader.get_ref().metadata())?.len();
    if anchor.start < data_start || anchor.end < anchor.start || anchor.end > available {
        return Err(bad("checkpoint offsets fall outside the journal"));
    }
    if state.sequence == 0 {
        return if anchor == Anchor::empty(data_start) {
            Ok(())
        } else {
            Err(bad("nonempty journal anchor in an empty checkpoint"))
        };
    }
    if anchor.start == anchor.end || anchor.end - anchor.start > codec::MAX_LINE as u64 {
        return Err(bad("bad journal anchor size"));
    }
    if anchor.start > data_start {
        codec::io_result(
            "seek prior journal delimiter",
            reader.seek(SeekFrom::Start(anchor.start - 1)),
        )?;
        let mut newline = [0];
        codec::io_result(
            "read prior journal delimiter",
            reader.read_exact(&mut newline),
        )?;
        if newline[0] != b'\n' {
            return Err(bad("journal anchor is outside a record boundary"));
        }
    } else {
        codec::io_result(
            "seek journal anchor",
            reader.seek(SeekFrom::Start(anchor.start)),
        )?;
    }
    let mut line = Vec::new();
    let record = read_required(reader, &mut line, anchor.start)?;
    if !anchor.matches(&line) || !checkpoint::anchor_matches_commit(state, &record) {
        return Err(bad("checkpoint journal anchor mismatch"));
    }
    Ok(())
}

fn read_required(
    reader: &mut impl std::io::BufRead,
    line: &mut Vec<u8>,
    offset: u64,
) -> Result<CommitLog> {
    if !codec::next_line(reader, line, JOURNAL_FILE, offset)? {
        return Err(Error::corrupt(
            JOURNAL_FILE,
            offset,
            "expected a commit record",
        ));
    }
    decode_at(line, offset)
}

fn decode_at(line: &[u8], offset: u64) -> Result<CommitLog> {
    CommitLog::decode(line).map_err(|message| Error::corrupt(JOURNAL_FILE, offset, message))
}
