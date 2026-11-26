use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::Path,
};

use super::{
    codec,
    receipts::ReceiptBinding,
    record, CommitLog, CommitResult, DurableState, CHECKPOINT_FILE,
};
use crate::{
    constraints::{Evaluator, Schema},
    error::{Error, Result},
    rows::{PendingRows, RowId, RowVersion, Sequence, TxId},
};

/// How a checkpoint generation supplies its durable request receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReceiptSource {
    /// Legacy v1 checkpoint: `R` lines are embedded in the checkpoint file and
    /// were decoded directly into [`DurableState::receipts`].
    Embedded,
    /// Current checkpoint: receipts live in the independent index bound by the
    /// enclosed fingerprint.
    Index(ReceiptBinding),
}

pub(crate) struct OpenedCheckpoint {
    pub anchor: Anchor,
    pub state: DurableState,
    pub receipts: ReceiptSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Anchor {
    pub start: u64,
    pub end: u64,
    pub hash: u64,
}

impl Anchor {
    pub fn empty(data_start: u64) -> Self {
        Self {
            start: data_start,
            end: data_start,
            hash: 0,
        }
    }

    pub fn record(start: u64, line: &[u8]) -> Self {
        Self {
            start,
            end: start + line.len() as u64,
            hash: codec::hash(line),
        }
    }

    pub fn matches(self, bytes: &[u8]) -> bool {
        self.end.checked_sub(self.start) == Some(bytes.len() as u64)
            && self.hash == codec::hash(bytes)
    }
}

pub(crate) fn write(
    file: &mut File,
    fingerprint: u64,
    anchor: Anchor,
    state: &DurableState,
    receipt_index: ReceiptBinding,
) -> Result<()> {
    let mut out = BufferedCheckpoint::new(file);
    // Version 2 keeps row heads and commit identities here, but commit
    // receipts live in the independent `receipts.v1` index bound by size/hash.
    out.line(&format!(
        "LOCKMERE-SNAPSHOT\t2\t{fingerprint}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        state.sequence,
        state.highest_tx,
        anchor.start,
        anchor.end,
        anchor.hash,
        receipt_index.size,
        receipt_index.hash
    ))?;
    for (id, version) in state.index.heads() {
        let values = codec::row(version.values.as_ref());
        out.line(&format!(
            "H\t{}\t{}\t{}\t{}\t{values}",
            id.table,
            codec::key(&id.key),
            version.sequence,
            version.transaction
        ))?;
    }
    for (transaction, sequence) in &state.committed {
        out.line(&format!("X\t{transaction}\t{sequence}"))?;
    }
    out.finish()
}

pub(crate) fn read(
    path: &Path,
    schema: &Schema,
    fingerprint: u64,
) -> Result<Option<OpenedCheckpoint>> {
    let file = match File::open(path.join(CHECKPOINT_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io("open checkpoint", error)),
    };
    if codec::io_result("stat checkpoint", file.metadata())?.len() > codec::MAX_CHECKPOINT {
        return Err(bad(0, "checkpoint exceeds the size limit"));
    }
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut offset = 0;
    let mut rolling = Hash::new();
    if !codec::next_line(&mut reader, &mut line, CHECKPOINT_FILE, offset)? {
        return Err(bad(0, "missing checkpoint header"));
    }
    let content = decode(&line, offset)?;
    let ParsedHeader {
        anchor,
        sequence,
        highest_tx,
        receipts,
        version,
    } = header(content, fingerprint).map_err(|message| bad(offset, message))?;
    let mut state = DurableState {
        sequence,
        highest_tx,
        ..DurableState::default()
    };
    rolling.update(&line);
    offset += line.len() as u64;
    let mut last_head = None;
    let mut last_tx = 0;
    let mut sequences = BTreeSet::new();
    let mut receipts_seen = BTreeSet::new();
    let mut last_request = None;
    let mut step = 0;

    while codec::next_line(&mut reader, &mut line, CHECKPOINT_FILE, offset)? {
        let content = decode(&line, offset)?;
        let fields: Vec<_> = content.split('\t').collect();
        match fields.as_slice() {
            ["H", table, key, seq, tx, values] if step == 0 => {
                let (id, version) = decode_head(schema, table, key, seq, tx, values)
                    .map_err(|message| bad(offset, message))?;
                if last_head.as_ref().is_some_and(|previous| previous >= &id) {
                    return Err(bad(offset, "row heads are repeated or out of order"));
                }
                last_head = Some(id.clone());
                state.index.push(id, version);
            }
            ["X", tx, seq] if step <= 1 => {
                step = 1;
                let transaction: TxId = number(tx, offset)?;
                let sequence: Sequence = number(seq, offset)?;
                if transaction <= last_tx
                    || sequence == 0
                    || sequence > state.sequence
                    || !sequences.insert(sequence)
                {
                    return Err(bad(offset, "invalid checkpoint commit identity"));
                }
                last_tx = transaction;
                state.committed.insert(transaction, sequence);
            }
            // Legacy v1 checkpoints embed the receipts as ordered R records.
            // They remain readable in their original format.
            ["R", client, request, tx, seq, writes] if version == 1 && step <= 2 => {
                step = 2;
                let id = record::decode_request(client, request)
                    .map_err(|message| bad(offset, message))?
                    .ok_or_else(|| bad(offset, "empty checkpoint receipt"))?;
                let transaction: TxId = number(tx, offset)?;
                let sequence: Sequence = number(seq, offset)?;
                let writes: usize = number(writes, offset)?;
                if last_request
                    .as_ref()
                    .is_some_and(|previous| previous >= &id)
                    || state.committed.get(&transaction) != Some(&sequence)
                    || writes > record::MAX_CHANGES
                    || !receipts_seen.insert(transaction)
                {
                    return Err(bad(offset, "invalid checkpoint receipt"));
                }
                last_request = Some(id.clone());
                state.receipts.insert(
                    id,
                    CommitResult {
                        transaction,
                        sequence,
                        writes,
                        replayed: false,
                    },
                );
            }
            ["END", expected_size, expected_hash] => {
                if number::<u64>(expected_size, offset)? != offset
                    || number::<u64>(expected_hash, offset)? != rolling.0
                {
                    return Err(bad(offset, "checkpoint checksum mismatch"));
                }
                if codec::next_line(&mut reader, &mut line, CHECKPOINT_FILE, offset)? {
                    return Err(bad(offset, "unexpected data after checkpoint footer"));
                }
                validate_state(schema, &state, last_tx, sequences.len(), offset)?;
                return Ok(Some(OpenedCheckpoint {
                    anchor,
                    state,
                    receipts,
                }));
            }
            _ => return Err(bad(offset, "unknown or out-of-order checkpoint record")),
        }
        rolling.update(&line);
        offset += line.len() as u64;
    }
    Err(bad(offset, "missing checkpoint footer"))
}

pub(crate) fn publish(root: &Path, temporary: &Path) -> Result<()> {
    codec::io_result(
        "rename checkpoint",
        fs::rename(temporary, root.join(CHECKPOINT_FILE)),
    )?;
    sync_directory(root)
}

pub(crate) fn sync_directory(root: &Path) -> Result<()> {
    let directory = codec::io_result("open database directory", File::open(root))?;
    codec::io_result("sync database directory", directory.sync_all())
}

pub(crate) fn anchor_matches_commit(state: &DurableState, commit: &CommitLog) -> bool {
    commit.sequence == state.sequence
        && state.committed.get(&commit.transaction) == Some(&commit.sequence)
        && commit.receipt.as_ref().map_or(true, |request| {
            state
                .receipts
                .get(request)
                .is_some_and(|item| item == &commit.result())
        })
}

struct ParsedHeader {
    anchor: Anchor,
    sequence: Sequence,
    highest_tx: TxId,
    receipts: ReceiptSource,
    version: u32,
}

fn header(content: &str, fingerprint: u64) -> std::result::Result<ParsedHeader, String> {
    let fields: Vec<_> = content.split('\t').collect();
    match fields.as_slice() {
        // Original v1 header; receipts are embedded as R records.
        ["LOCKMERE-SNAPSHOT", "1", digest, seq, tx, start, end, hash] => {
            if codec::num::<u64>(digest)? != fingerprint {
                return Err("checkpoint is bound to a different schema".to_owned());
            }
            let anchor = Anchor {
                start: codec::num(start)?,
                end: codec::num(end)?,
                hash: codec::num(hash)?,
            };
            Ok(ParsedHeader {
                anchor,
                sequence: codec::num(seq)?,
                highest_tx: codec::num(tx)?,
                receipts: ReceiptSource::Embedded,
                version: 1,
            })
        }
        // v2 header binds the checkpoint generation to an independent receipt
        // index through its byte length and rolling hash.
        [
            "LOCKMERE-SNAPSHOT",
            "2",
            digest,
            seq,
            tx,
            start,
            end,
            hash,
            receipts_size,
            receipts_hash,
        ] => {
            if codec::num::<u64>(digest)? != fingerprint {
                return Err("checkpoint is bound to a different schema".to_owned());
            }
            let anchor = Anchor {
                start: codec::num(start)?,
                end: codec::num(end)?,
                hash: codec::num(hash)?,
            };
            let binding = ReceiptBinding {
                size: codec::num(receipts_size)?,
                hash: codec::num(receipts_hash)?,
            };
            Ok(ParsedHeader {
                anchor,
                sequence: codec::num(seq)?,
                highest_tx: codec::num(tx)?,
                receipts: ReceiptSource::Index(binding),
                version: 2,
            })
        }
        _ => Err("invalid checkpoint header".to_owned()),
    }
}

fn decode_head(
    schema: &Schema,
    table: &str,
    key: &str,
    seq: &str,
    tx: &str,
    values: &str,
) -> std::result::Result<(RowId, RowVersion), String> {
    let id = RowId::new(table, codec::decode_key(key)?);
    schema.validate_id(&id).map_err(|error| error.to_string())?;
    let values = codec::decode_row(values)?;
    if let Some(row) = &values {
        if schema
            .normalize_row(&id, row.clone())
            .map_err(|error| error.to_string())?
            != *row
        {
            return Err(format!("checkpoint row {id} omits a canonical column"));
        }
    }
    let version = RowVersion::new(codec::num(seq)?, codec::num(tx)?, values);
    Ok((id, version))
}

fn validate_state(
    schema: &Schema,
    state: &DurableState,
    last_tx: TxId,
    commits: usize,
    offset: u64,
) -> Result<()> {
    if last_tx != state.highest_tx || u64::try_from(commits).ok() != Some(state.sequence) {
        return Err(bad(
            offset,
            "checkpoint commit watermark does not match its commit identities",
        ));
    }
    for (_, head) in state.index.heads() {
        if state.committed.get(&head.transaction) != Some(&head.sequence) {
            return Err(bad(offset, "row head refers to an unknown commit"));
        }
    }
    let empty = PendingRows::default();
    let mut evaluator = Evaluator::new(schema, &state.index, state.sequence, &empty);
    if let Some(error) = evaluator
        .recovery()
        .iter()
        .find_map(|check| check.as_error())
    {
        return Err(bad(offset, error.to_string()));
    }
    Ok(())
}

fn decode(line: &[u8], offset: u64) -> Result<&str> {
    codec::unseal(line).map_err(|message| bad(offset, message))
}

fn number<T: std::str::FromStr + ToString>(value: &str, offset: u64) -> Result<T> {
    codec::num(value).map_err(|message| bad(offset, message))
}

fn bad(offset: u64, message: impl Into<String>) -> Error {
    Error::corrupt(CHECKPOINT_FILE, offset, message)
}

struct BufferedCheckpoint<'a> {
    writer: BufWriter<&'a mut File>,
    offset: u64,
    hash: Hash,
}

impl<'a> BufferedCheckpoint<'a> {
    fn new(writer: &'a mut File) -> Self {
        Self {
            writer: BufWriter::new(writer),
            offset: 0,
            hash: Hash::new(),
        }
    }

    fn line(&mut self, content: &str) -> Result<()> {
        let line = codec::seal(content)?;
        let offset = self.offset + line.len() as u64;
        if offset > codec::MAX_CHECKPOINT.saturating_sub(128) {
            return Err(Error::Limit("checkpoint bytes"));
        }
        self.hash.update(&line);
        self.offset = offset;
        codec::io_result("write checkpoint", self.writer.write_all(&line))
    }

    fn finish(mut self) -> Result<()> {
        let footer = codec::seal(&format!("END\t{}\t{}", self.offset, self.hash.0))?;
        codec::io_result("write checkpoint footer", self.writer.write_all(&footer))?;
        codec::io_result("flush checkpoint", self.writer.flush())
    }
}

struct Hash(u64);

impl Hash {
    fn new() -> Self {
        Self(0xcbf29ce484222325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
    }
}
