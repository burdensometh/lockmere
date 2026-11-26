use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use super::{checkpoint, codec, record, CommitResult, RequestId};
use crate::{
    error::{Error, Result},
    rows::{Sequence, TxId},
};

/// Canonical file name of the independent receipt index.
pub use super::RECEIPTS_FILE;

/// Conventional aliases reachable through the `wal::receipts` module path.
pub const FILE: &str = RECEIPTS_FILE;
pub const FILE_NAME: &str = RECEIPTS_FILE;

/// Temporary file used while staging a receipt index. It is never read as an
/// index on open, so an interrupted publication leaves the previous index
/// untouched.
pub(crate) const TEMPORARY_RECEIPTS: &str = ".receipts.v1.temporary";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceiptBinding {
    pub size: u64,
    pub hash: u64,
}

/// Result of trying to mate an on-disk index with a checkpoint generation.
pub(crate) enum LoadedIndex {
    /// The file exists, is valid, and its binding equals the checkpoint's.
    Match(BTreeMap<RequestId, CommitResult>),
    /// No independent index is present (for example a subset copy or a store
    /// that has not checkpointed since the migration).
    Missing,
    /// A well-formed index from another generation is present. Atomic
    /// publication can leave this only in the crash window between renames;
    /// the journal remains the source of truth and can rebuild the receipts.
    Stale,
}

pub(crate) fn temporary_path(root: &Path) -> PathBuf {
    root.join(TEMPORARY_RECEIPTS)
}

pub(crate) fn write(
    file: &mut File,
    receipts: &BTreeMap<RequestId, CommitResult>,
) -> Result<ReceiptBinding> {
    let mut out = BufferedIndex::new(file);
    out.line(&format!("LOCKMERE-RECEIPTS\t1\t{}", receipts.len()))?;
    for (id, result) in receipts {
        out.line(&format!(
            "R\t{}\t{}\t{}\t{}\t{}",
            codec::hex_encode(&id.client),
            codec::hex_encode(&id.request),
            result.transaction,
            result.sequence,
            result.writes
        ))?;
    }
    out.finish()
}

pub(crate) fn read(
    root: &Path,
    expected: ReceiptBinding,
    committed: &BTreeMap<TxId, Sequence>,
) -> Result<LoadedIndex> {
    let path = root.join(RECEIPTS_FILE);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LoadedIndex::Missing)
        }
        Err(error) => return Err(Error::io("open receipt index", error)),
    };
    if codec::io_result("stat receipt index", file.metadata())?.len() > codec::MAX_CHECKPOINT {
        return Err(bad(0, "receipt index exceeds the size limit"));
    }
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut offset = 0;
    let mut rolling = Hash::new();
    if !codec::next_line(&mut reader, &mut line, RECEIPTS_FILE, offset)? {
        return Err(bad(0, "missing receipt index header"));
    }
    let content = decode(&line, offset)?;
    let declared = header(content).map_err(|message| bad(offset, message))?;
    rolling.update(&line);
    offset += line.len() as u64;

    let mut receipts = BTreeMap::new();
    let mut receipt_transactions = BTreeSet::new();
    let mut last_request = None;
    let mut records = 0usize;
    // Structurally decoded entries. Consistency with a checkpoint's commit
    // identities is only enforced once the index generation is proven to match
    // that checkpoint: a well-formed newer index left by an interrupted
    // publication must classify as [`LoadedIndex::Stale`], not as corruption.
    let mut decoded: Vec<(RequestId, TxId, Sequence, usize)> = Vec::new();

    while codec::next_line(&mut reader, &mut line, RECEIPTS_FILE, offset)? {
        let content = decode(&line, offset)?;
        let fields: Vec<_> = content.split('\t').collect();
        match fields.as_slice() {
            ["R", client, request, tx, seq, writes] => {
                let id = record::decode_request(client, request)
                    .map_err(|message| bad(offset, message))?
                    .ok_or_else(|| bad(offset, "empty receipt index entry"))?;
                let transaction: TxId = number(tx, offset)?;
                let sequence: Sequence = number(seq, offset)?;
                let writes_count: usize = number(writes, offset)?;
                if last_request.as_ref().is_some_and(|previous| previous >= &id)
                    || writes_count > record::MAX_CHANGES
                    || !receipt_transactions.insert(transaction)
                {
                    return Err(bad(offset, "invalid receipt index record"));
                }
                last_request = Some(id.clone());
                decoded.push((id, transaction, sequence, writes_count));
                records += 1;
                rolling.update(&line);
                offset += line.len() as u64;
            }
            ["END", expected_size, expected_hash] => {
                let footer_size = number::<u64>(expected_size, offset)?;
                let footer_hash = number::<u64>(expected_hash, offset)?;
                if footer_size != offset || footer_hash != rolling.0 {
                    return Err(bad(offset, "receipt index checksum mismatch"));
                }
                if records != declared {
                    return Err(bad(offset, "receipt index count mismatch"));
                }
                let actual = ReceiptBinding {
                    size: offset,
                    hash: rolling.0,
                };
                if actual != expected {
                    return Ok(LoadedIndex::Stale);
                }
                for (id, transaction, sequence, writes_count) in decoded {
                    if committed.get(&transaction) != Some(&sequence) {
                        return Err(bad(offset, "receipt index entry does not match its commit"));
                    }
                    receipts.insert(
                        id,
                        CommitResult {
                            transaction,
                            sequence,
                            writes: writes_count,
                            replayed: false,
                        },
                    );
                }
                return Ok(LoadedIndex::Match(receipts));
            }
            _ => return Err(bad(offset, "unknown or out-of-order receipt index record")),
        }
    }
    Err(bad(offset, "missing receipt index footer"))
}

/// Atomically replace the canonical index and durably record the rename.
pub(crate) fn publish(root: &Path, temporary: &Path) -> Result<()> {
    codec::io_result(
        "rename receipt index",
        fs::rename(temporary, root.join(RECEIPTS_FILE)),
    )?;
    checkpoint::sync_directory(root)
}

fn header(content: &str) -> std::result::Result<usize, String> {
    let fields: Vec<_> = content.split('\t').collect();
    let ["LOCKMERE-RECEIPTS", "1", count] = fields.as_slice() else {
        return Err("invalid receipt index header".to_owned());
    };
    codec::num::<usize>(count)
}

fn decode(line: &[u8], offset: u64) -> Result<&str> {
    codec::unseal(line).map_err(|message| bad(offset, message))
}

fn number<T: std::str::FromStr + ToString>(value: &str, offset: u64) -> Result<T> {
    codec::num(value).map_err(|message| bad(offset, message))
}

fn bad(offset: u64, message: impl Into<String>) -> Error {
    Error::corrupt(RECEIPTS_FILE, offset, message)
}

struct BufferedIndex<'a> {
    writer: BufWriter<&'a mut File>,
    offset: u64,
    hash: Hash,
}

impl<'a> BufferedIndex<'a> {
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
            return Err(Error::Limit("receipt index bytes"));
        }
        self.hash.update(&line);
        self.offset = offset;
        codec::io_result("write receipt index", self.writer.write_all(&line))
    }

    fn finish(mut self) -> Result<ReceiptBinding> {
        let footer = codec::seal(&format!("END\t{}\t{}", self.offset, self.hash.0))?;
        codec::io_result("write receipt index footer", self.writer.write_all(&footer))?;
        codec::io_result("flush receipt index", self.writer.flush())?;
        Ok(ReceiptBinding {
            size: self.offset,
            hash: self.hash.0,
        })
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
