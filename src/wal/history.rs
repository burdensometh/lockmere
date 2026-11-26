use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
};

use super::{
    checkpoint::{self, Anchor},
    codec::{self},
    record::CommitLog,
    HISTORY_FILE, JOURNAL_FILE,
};
use crate::{
    error::{Error, Result},
    rows::Sequence,
};

/// Consecutive journal frames covered by one sparse index entry.
///
/// A scan starts at the span owning its target and stops once its page is
/// full. A cursor sitting at the very end of a span therefore decodes all
/// `HISTORY_SPAN` frames in that span plus the page. The parser caps a
/// history page at ten rows, so keeping the span at 246 makes 256 the strict,
/// inclusive decode bound for every `SHOW HISTORY` request, however long the
/// journal is.
pub(crate) const HISTORY_SPAN: Sequence = 246;

const TEMPORARY_HISTORY: &str = ".history.v1.temporary";
const MAGIC: &str = "LOCKMERE-HISTORY";
const VERSION: &str = "1";
const MAX_INDEX_BYTES: u64 = 512 * 1024 * 1024;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Sparse, fully derived lookup index over the durable journal.
///
/// Every entry records the byte offset at which a span-aligned commit frame
/// begins. The index is a cache: the journal remains the source of truth, so
/// a missing or invalid `history.v1` is rebuilt by scanning the journal once
/// and no journal frame ever needs to be rewritten.
#[derive(Debug, Clone)]
pub(crate) struct HistoryIndex {
    entries: Vec<Entry>,
    last_sequence: Sequence,
    last_end: u64,
    data_start: u64,
    fingerprint: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    sequence: Sequence,
    offset: u64,
}

impl HistoryIndex {
    fn empty(data_start: u64, fingerprint: u64) -> Self {
        Self {
            entries: Vec::new(),
            last_sequence: 0,
            last_end: data_start,
            data_start,
            fingerprint,
        }
    }

    /// Load the persisted index when it still describes the durable journal,
    /// otherwise rebuild it from the journal in one forward scan and persist
    /// the recovered sidecar.
    pub(crate) fn prepare(
        root: &Path,
        data_start: u64,
        fingerprint: u64,
        anchor: Anchor,
    ) -> Result<Self> {
        match Self::load(root, data_start, fingerprint, anchor) {
            Ok(index) => Ok(index),
            Err(_) => {
                let index = Self::rebuild(root, data_start, fingerprint, anchor)?;
                index.persist(root)?;
                Ok(index)
            }
        }
    }

    /// Record a freshly appended, already durable frame.
    ///
    /// Returns true when the frame opens a new span and the on-disk index must
    /// be refreshed.
    pub(crate) fn observe(
        &mut self,
        sequence: Sequence,
        frame_start: u64,
        frame_end: u64,
    ) -> Result<bool> {
        if sequence != self.last_sequence + 1 {
            return Err(Error::corrupt(
                JOURNAL_FILE,
                frame_start,
                "history index observed a nonconsecutive commit",
            ));
        }
        if frame_start != self.last_end {
            return Err(Error::corrupt(
                JOURNAL_FILE,
                frame_start,
                "history index observed a frame outside the journal tail",
            ));
        }
        let opens_span = (sequence - 1) % HISTORY_SPAN == 0;
        if opens_span {
            self.entries.push(Entry {
                sequence,
                offset: frame_start,
            });
        }
        self.last_sequence = sequence;
        self.last_end = frame_end;
        Ok(opens_span)
    }

    /// Byte offset at which a history scan for `after` should start.
    ///
    /// Entries open each span at sequences 1, `HISTORY_SPAN + 1`,
    /// `2 * HISTORY_SPAN + 1`, and so on.
    ///
    /// * A tail read with no cursor (`after == 0`) opens the newest span, so a
    ///   "last N" page decodes at most the frames in that one span.
    /// * A cursor read (`after > 0`) opens the newest span whose first frame is
    ///   at or before the cursor itself. Frames before that offset all have
    ///   smaller sequences and cannot match, and the first returnable frame
    ///   (`after + 1`) is less than one span ahead of it.
    ///
    /// The chosen offset is always at most one span behind the target, so no
    /// scan decodes more than `HISTORY_SPAN` frames plus the page itself.
    pub(crate) fn start_offset(&self, after: Sequence) -> u64 {
        if after == 0 {
            return self
                .entries
                .last()
                .map_or(self.data_start, |entry| entry.offset);
        }
        let point = self
            .entries
            .partition_point(|entry| entry.sequence <= after)
            .saturating_sub(1);
        self.entries
            .get(point)
            .map_or(self.data_start, |entry| entry.offset)
    }

    /// Sequence carried by the frame beginning at `offset`, when `offset` is a
    /// span-aligned entry point.
    pub(crate) fn entry_sequence_at(&self, offset: u64) -> Option<Sequence> {
        self.entries
            .binary_search_by(|entry| entry.offset.cmp(&offset))
            .ok()
            .map(|index| self.entries[index].sequence)
    }

    fn load(root: &Path, data_start: u64, fingerprint: u64, anchor: Anchor) -> Result<Self> {
        let file = match File::open(root.join(HISTORY_FILE)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::io("open history index", error));
            }
            Err(error) => return Err(Error::io("open history index", error)),
        };
        if codec::io_result("stat history index", file.metadata())?.len() > MAX_INDEX_BYTES {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "history index exceeds the size limit",
            ));
        }
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut offset = 0u64;
        let mut rolling = FNV_OFFSET;
        if !codec::next_line(&mut reader, &mut line, HISTORY_FILE, offset)? {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "missing history index header",
            ));
        }
        let header =
            codec::unseal(&line).map_err(|message| Error::corrupt(HISTORY_FILE, 0, message))?;
        let fields: Vec<_> = header.split('\t').collect();
        let [magic, version, digest, start, tail_sequence, tail_end] = fields.as_slice() else {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "invalid history index header",
            ));
        };
        if *magic != MAGIC || *version != VERSION {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "unknown history index format",
            ));
        }
        if codec::num::<u64>(digest).map_err(|message| Error::corrupt(HISTORY_FILE, 0, message))?
            != fingerprint
            || codec::num::<u64>(start)
                .map_err(|message| Error::corrupt(HISTORY_FILE, 0, message))?
                != data_start
        {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "history index is bound to a different journal",
            ));
        }
        let mut index = Self::empty(data_start, fingerprint);
        index.last_sequence = codec::num(tail_sequence)
            .map_err(|message| Error::corrupt(HISTORY_FILE, 0, message))?;
        index.last_end =
            codec::num(tail_end).map_err(|message| Error::corrupt(HISTORY_FILE, 0, message))?;
        rolling = hash_line(rolling, &line);
        offset += line.len() as u64;

        while codec::next_line(&mut reader, &mut line, HISTORY_FILE, offset)? {
            let content = codec::unseal(&line)
                .map_err(|message| Error::corrupt(HISTORY_FILE, offset, message))?;
            if let Some(footer) = content.strip_prefix("HEND\t") {
                let (size, digest) = footer.split_once('\t').ok_or_else(|| {
                    Error::corrupt(HISTORY_FILE, offset, "bad history index footer")
                })?;
                if codec::num::<u64>(size)
                    .map_err(|message| Error::corrupt(HISTORY_FILE, offset, message))?
                    != offset
                    || codec::num::<u64>(digest)
                        .map_err(|message| Error::corrupt(HISTORY_FILE, offset, message))?
                        != rolling
                {
                    return Err(Error::corrupt(
                        HISTORY_FILE,
                        offset,
                        "history index checksum mismatch",
                    ));
                }
                if codec::next_line(&mut reader, &mut line, HISTORY_FILE, offset)? {
                    return Err(Error::corrupt(
                        HISTORY_FILE,
                        offset,
                        "unexpected data after history index footer",
                    ));
                }
                index.finish_load(anchor)?;
                return index.verify_entries(root);
            }
            let fields: Vec<_> = content.split('\t').collect();
            let ["P", sequence, frame_offset] = fields.as_slice() else {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    offset,
                    "unknown history index record",
                ));
            };
            let sequence: Sequence = codec::num(sequence)
                .map_err(|message| Error::corrupt(HISTORY_FILE, offset, message))?;
            let frame_offset: u64 = codec::num(frame_offset)
                .map_err(|message| Error::corrupt(HISTORY_FILE, offset, message))?;
            if (sequence - 1) % HISTORY_SPAN != 0
                || index
                    .entries
                    .last()
                    .is_some_and(|previous| sequence <= previous.sequence)
                || frame_offset < data_start
                || index
                    .entries
                    .last()
                    .is_some_and(|previous| frame_offset <= previous.offset)
                || frame_offset >= anchor.end
            {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    offset,
                    "history index entry is out of order or outside the journal",
                ));
            }
            index.entries.push(Entry {
                sequence,
                offset: frame_offset,
            });
            rolling = hash_line(rolling, &line);
            offset += line.len() as u64;
        }
        Err(Error::corrupt(
            HISTORY_FILE,
            offset,
            "missing history index footer",
        ))
    }

    fn finish_load(&self, anchor: Anchor) -> Result<()> {
        if self.last_end != anchor.end {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "history index tail does not match the durable journal",
            ));
        }
        if self.last_sequence == 0 {
            if anchor.end != self.data_start || !self.entries.is_empty() {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    0,
                    "empty history index does not match the journal tail",
                ));
            }
        } else if anchor.end <= self.data_start {
            return Err(Error::corrupt(
                HISTORY_FILE,
                0,
                "history index names a tail the journal does not contain",
            ));
        }
        if let Some(last) = self.entries.last() {
            if last.sequence > self.last_sequence {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    0,
                    "history index extends past its own tail",
                ));
            }
        }
        Ok(())
    }

    /// Prove every persisted entry still points at the frame it claims. This
    /// reads one frame per span rather than the whole journal.
    fn verify_entries(self, root: &Path) -> Result<Self> {
        if self.entries.is_empty() {
            return Ok(self);
        }
        let file = codec::io_result(
            "open journal for history verification",
            File::open(root.join(JOURNAL_FILE)),
        )?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        for entry in &self.entries {
            line.clear();
            codec::io_result(
                "seek history index entry",
                reader.seek(SeekFrom::Start(entry.offset)),
            )?;
            if !codec::next_line(&mut reader, &mut line, JOURNAL_FILE, entry.offset)? {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    entry.offset,
                    "history index entry points past the journal",
                ));
            }
            let record = CommitLog::decode(&line)
                .map_err(|message| Error::corrupt(HISTORY_FILE, entry.offset, message))?;
            if record.sequence != entry.sequence {
                return Err(Error::corrupt(
                    HISTORY_FILE,
                    entry.offset,
                    "history index entry points at the wrong commit",
                ));
            }
        }
        Ok(self)
    }

    fn rebuild(root: &Path, data_start: u64, fingerprint: u64, anchor: Anchor) -> Result<Self> {
        let file = codec::io_result(
            "open journal for history rebuild",
            File::open(root.join(JOURNAL_FILE)),
        )?;
        let mut reader = BufReader::new(file);
        codec::io_result(
            "seek history rebuild",
            reader.seek(SeekFrom::Start(data_start)),
        )?;
        let mut index = Self::empty(data_start, fingerprint);
        let mut line = Vec::new();
        let mut offset = data_start;
        let mut expected: Sequence = 1;
        while offset < anchor.end {
            if !codec::next_line(&mut reader, &mut line, JOURNAL_FILE, offset)? {
                return Err(Error::corrupt(
                    JOURNAL_FILE,
                    offset,
                    "expected a commit record while rebuilding history",
                ));
            }
            let record = CommitLog::decode(&line)
                .map_err(|message| Error::corrupt(JOURNAL_FILE, offset, message))?;
            if record.sequence != expected {
                return Err(Error::corrupt(
                    JOURNAL_FILE,
                    offset,
                    "nonconsecutive commit while rebuilding history",
                ));
            }
            if (expected - 1) % HISTORY_SPAN == 0 {
                index.entries.push(Entry {
                    sequence: expected,
                    offset,
                });
            }
            offset += line.len() as u64;
            expected += 1;
        }
        if offset != anchor.end {
            return Err(Error::corrupt(
                JOURNAL_FILE,
                offset,
                "history rebuild overran the durable journal anchor",
            ));
        }
        index.last_sequence = expected.saturating_sub(1);
        index.last_end = anchor.end;
        Ok(index)
    }

    /// Atomically replace the persisted index. The journal is never touched.
    pub(crate) fn persist(&self, root: &Path) -> Result<()> {
        let temporary = root.join(TEMPORARY_HISTORY);
        let file = open_temporary(&temporary)?;
        let mut writer = BufferedIndex::new(file);
        writer.line(&format!(
            "{MAGIC}\t{VERSION}\t{}\t{}\t{}\t{}",
            self.fingerprint, self.data_start, self.last_sequence, self.last_end
        ))?;
        for entry in &self.entries {
            writer.line(&format!("P\t{}\t{}", entry.sequence, entry.offset))?;
        }
        writer.finish()?;
        codec::io_result(
            "rename history index",
            fs::rename(&temporary, root.join(HISTORY_FILE)),
        )?;
        checkpoint::sync_directory(root)
    }
}

fn open_temporary(path: &Path) -> Result<File> {
    codec::io_result(
        "open temporary history index",
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path),
    )
}

fn hash_line(state: u64, line: &[u8]) -> u64 {
    let mut state = state;
    for byte in line {
        state = (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
    }
    state
}

struct BufferedIndex {
    writer: BufWriter<File>,
    offset: u64,
    hash: u64,
}

impl BufferedIndex {
    fn new(file: File) -> Self {
        Self {
            writer: BufWriter::new(file),
            offset: 0,
            hash: FNV_OFFSET,
        }
    }

    fn line(&mut self, content: &str) -> Result<()> {
        let line = codec::seal(content)?;
        let offset = self.offset + line.len() as u64;
        if offset > MAX_INDEX_BYTES.saturating_sub(128) {
            return Err(Error::Limit("history index bytes"));
        }
        self.hash = hash_line(self.hash, &line);
        self.offset = offset;
        codec::io_result("write history index", self.writer.write_all(&line))
    }

    fn finish(mut self) -> Result<()> {
        let footer = codec::seal(&format!("HEND\t{}\t{}", self.offset, self.hash))?;
        codec::io_result("write history index footer", self.writer.write_all(&footer))?;
        codec::io_result("flush history index", self.writer.flush())?;
        codec::io_result("sync history index", self.writer.get_ref().sync_all())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        rows::{RowId, VersionChange},
        wal::CommitLog,
    };

    fn frame(sequence: Sequence) -> Vec<u8> {
        CommitLog {
            sequence,
            transaction: sequence,
            receipt: None,
            changes: Vec::<VersionChange>::new(),
        }
        .encode()
        .unwrap()
    }

    fn journal_with_commits(root: &Path, data_start: u64, count: Sequence) -> (u64, Anchor) {
        use std::io::Write as _;

        let path = root.join(JOURNAL_FILE);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&vec![b'L'; data_start as usize]).unwrap();
        let mut end = data_start;
        for sequence in 1..=count {
            let line = frame(sequence);
            file.write_all(&line).unwrap();
            end += line.len() as u64;
        }
        file.sync_all().unwrap();
        (
            end,
            Anchor {
                start: data_start,
                end,
                hash: 0,
            },
        )
    }

    #[test]
    fn rebuild_then_load_roundtrip_points_at_span_frames() {
        let scratch = std::env::temp_dir().join(format!(
            "lockmere-history-rt-{}-{}",
            std::process::id(),
            9001
        ));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch).unwrap();
        let data_start = 7;
        let (end, anchor) = journal_with_commits(&scratch, data_start, 513);
        let index = HistoryIndex::prepare(&scratch, data_start, 42, anchor).unwrap();
        // Entries open 246-frame spans at sequences 1, 247, 493.
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.entries[0].sequence, 1);
        assert_eq!(index.entries[0].offset, data_start);
        assert_eq!(index.entries[1].sequence, 247);
        assert_eq!(index.entries[2].sequence, 493);
        assert_eq!(index.last_sequence, 513);
        assert_eq!(index.last_end, end);
        index.persist(&scratch).unwrap();

        let reloaded = HistoryIndex::prepare(&scratch, data_start, 42, anchor).unwrap();
        assert_eq!(reloaded.entries, index.entries);
        assert_eq!(reloaded.last_end, end);
        let _ = std::fs::remove_dir_all(&scratch);
    }

    #[test]
    fn tail_start_never_decodes_more_than_one_span_plus_page() {
        let scratch = std::env::temp_dir().join(format!(
            "lockmere-history-bound-{}-{}",
            std::process::id(),
            9002
        ));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch).unwrap();
        let data_start = 3;
        let count: Sequence = 100_000;
        let (_, anchor) = journal_with_commits(&scratch, data_start, count);
        let index = HistoryIndex::prepare(&scratch, data_start, 7, anchor).unwrap();

        // Tail mode opens the newest span, whose first frame is derived from
        // the span size rather than hard-coded.
        let last_span = ((count - 1) / HISTORY_SPAN) * HISTORY_SPAN + 1;
        let start = index.start_offset(0);
        let entry = index
            .entries
            .iter()
            .find(|entry| entry.offset == start)
            .unwrap();
        assert_eq!(entry.sequence, last_span);
        assert_eq!(index.entries.last().unwrap().sequence, entry.sequence);
        // Only the frames in the final partial span plus the ten-row page.
        let decoded = count - entry.sequence + 1 + 10;
        assert!(decoded <= 256, "tail page decoded {decoded} records");

        // Cursor mode chooses the span owning frame `after + 1`, so a cursor
        // near the tail stays inside the final span.
        let cursor = count - 3;
        let start = index.start_offset(cursor);
        let entry = index
            .entries
            .iter()
            .find(|entry| entry.offset == start)
            .unwrap();
        assert!(entry.sequence <= cursor + 1);
        assert!(cursor + 1 - entry.sequence < HISTORY_SPAN);

        // A cursor at the very end still lands on the last span entry.
        assert_eq!(
            index.start_offset(count),
            index.entries.last().unwrap().offset
        );

        // A cursor inside the first span opens that span at the journal head.
        assert_eq!(index.start_offset(1), data_start);
        assert_eq!(index.start_offset(HISTORY_SPAN - 1), data_start);
        let _ = std::fs::remove_dir_all(&scratch);
    }

    #[test]
    fn stale_or_corrupt_index_is_rebuilt() {
        let scratch = std::env::temp_dir().join(format!(
            "lockmere-history-stale-{}-{}",
            std::process::id(),
            9003
        ));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch).unwrap();
        let data_start = 5;
        let (_, anchor) = journal_with_commits(&scratch, data_start, 300);
        let index = HistoryIndex::prepare(&scratch, data_start, 9, anchor).unwrap();
        index.persist(&scratch).unwrap();

        // Grow the journal: the persisted tail no longer matches the anchor.
        let (_, grown) = journal_with_commits(&scratch, data_start, 260);
        let recovered = HistoryIndex::prepare(&scratch, data_start, 9, grown).unwrap();
        assert_eq!(recovered.last_sequence, 260);
        assert_eq!(recovered.entries.last().unwrap().sequence, 247);

        // Garbage the sidecar: the journal scan recovers it again.
        std::fs::write(scratch.join(HISTORY_FILE), b"not an index\n").unwrap();
        let recovered = HistoryIndex::prepare(&scratch, data_start, 9, grown).unwrap();
        assert_eq!(recovered.last_sequence, 260);
        assert_eq!(recovered.entries.last().unwrap().sequence, 247);
        let _ = std::fs::remove_dir_all(&scratch);
    }

    #[test]
    fn observe_rejects_gaps_and_misaligned_frames() {
        let mut index = HistoryIndex::empty(10, 1);
        assert!(index.observe(1, 10, 20).is_ok());
        assert!(matches!(
            index.observe(3, 20, 30),
            Err(Error::Corrupt { .. })
        ));
        assert!(matches!(
            index.observe(2, 21, 30),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn empty_and_tiny_journals_fall_back_to_the_data_start() {
        // No frames: every scan starts at the journal head and decodes nothing.
        let empty = HistoryIndex::empty(12, 1);
        assert_eq!(empty.start_offset(0), 12);
        assert_eq!(empty.start_offset(7), 12);
        assert_eq!(empty.entry_sequence_at(12), None);

        // A single frame opens the first span at the data start.
        let mut one = HistoryIndex::empty(12, 1);
        one.observe(1, 12, 20).unwrap();
        assert_eq!(one.start_offset(0), 12);
        assert_eq!(one.entry_sequence_at(12), Some(1));
    }

    #[test]
    fn row_id_type_is_constructible_for_fixture() {
        let _ = RowId::new("branch", 1);
    }
}
