use super::codec;
use crate::{
    error::{Error, Result},
    rows::{Key, Row, RowId, Sequence, TxId, Value, VersionChange},
};

pub(crate) const MAX_CHANGES: usize = 16_384;
const MAX_REQUEST_PART: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestId {
    pub client: String,
    pub request: String,
}

impl RequestId {
    pub fn new(client: impl Into<String>, request: impl Into<String>) -> Result<Self> {
        let id = Self {
            client: client.into(),
            request: request.into(),
        };
        id.validate()?;
        Ok(id)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if [&self.client, &self.request].into_iter().all(|part| {
            !part.is_empty()
                && part.len() <= MAX_REQUEST_PART
                && !part.chars().any(char::is_control)
        }) {
            Ok(())
        } else {
            Err(Error::InvalidRequest(
                "invalid client or request identity".to_owned(),
            ))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub sequence: Sequence,
    pub transaction: TxId,
    pub writes: usize,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLog {
    pub sequence: Sequence,
    pub transaction: TxId,
    pub receipt: Option<RequestId>,
    pub changes: Vec<VersionChange>,
}

impl CommitLog {
    pub fn result(&self) -> CommitResult {
        CommitResult {
            sequence: self.sequence,
            transaction: self.transaction,
            writes: self.changes.len(),
            replayed: false,
        }
    }

    /// Encodes this commit as a compact v2 frame: an `L2` tag, a varint
    /// payload length, a binary payload, and a CRC-32 trailer. The payload
    /// holds the sequence and transaction as varints, the optional durable
    /// request, and the sorted row changes with tagged values. A journal may
    /// mix these frames with older v1 text frames; readers dispatch on the
    /// first byte of each frame.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        codec::write_varint(&mut payload, self.sequence);
        codec::write_varint(&mut payload, self.transaction);
        match &self.receipt {
            Some(id) => {
                payload.push(1);
                write_text(&mut payload, &id.client);
                write_text(&mut payload, &id.request);
            }
            None => payload.push(0),
        }
        codec::write_varint(&mut payload, self.changes.len() as u64);
        for change in &self.changes {
            write_text(&mut payload, &change.id.table);
            match &change.id.key {
                Key::Integer(number) => {
                    payload.push(1);
                    codec::write_zigzag(&mut payload, *number);
                }
                Key::Text(text) => {
                    payload.push(2);
                    write_text(&mut payload, text);
                }
            }
            match &change.values {
                None => payload.push(0),
                Some(values) => {
                    payload.push(1);
                    codec::write_varint(&mut payload, values.len() as u64);
                    for (column, value) in values {
                        write_text(&mut payload, column);
                        write_value(&mut payload, value);
                    }
                }
            }
        }
        let mut frame = Vec::with_capacity(payload.len() + 16);
        frame.extend_from_slice(&codec::V2_MAGIC);
        codec::write_varint(&mut frame, payload.len() as u64);
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&codec::checksum(&payload).to_le_bytes());
        if frame.len() > codec::MAX_LINE {
            return Err(Error::Limit("journal record bytes"));
        }
        Ok(frame)
    }

    pub(crate) fn decode_v1(line: &[u8]) -> std::result::Result<Self, String> {
        let body = codec::unseal(line)?;
        let mut fields = body.split('\t');
        if fields.next() != Some("C") {
            return Err("unknown journal record".to_owned());
        }
        let sequence = codec::num(next(&mut fields, "sequence")?)?;
        let transaction = codec::num(next(&mut fields, "transaction")?)?;
        let receipt = decode_request(next(&mut fields, "client")?, next(&mut fields, "request")?)?;
        let count: usize = codec::num(next(&mut fields, "write count")?)?;
        if count > MAX_CHANGES {
            return Err("too many writes in commit record".to_owned());
        }
        let mut changes = Vec::<VersionChange>::with_capacity(count);
        for _ in 0..count {
            let raw = next(&mut fields, "write")?;
            let mut parts = raw.splitn(3, ':');
            let table = next(&mut parts, "table")?;
            let key = codec::decode_key(next(&mut parts, "key")?)?;
            let values = codec::decode_row(next(&mut parts, "row")?)?;
            let id = RowId::new(table, key);
            if changes.last().is_some_and(|previous| previous.id >= id) {
                return Err("writes must appear in strict canonical order".to_owned());
            }
            changes.push(VersionChange { id, values });
        }
        let end = next(&mut fields, "commit footer")?
            .strip_prefix('E')
            .ok_or("invalid commit footer")?;
        if codec::num::<Sequence>(end)? != sequence || fields.next().is_some() {
            return Err("commit footer does not match commit header".to_owned());
        }
        Ok(Self {
            sequence,
            transaction,
            receipt,
            changes,
        })
    }

    pub(crate) fn decode_v2(frame: &[u8]) -> std::result::Result<Self, String> {
        let rest = frame
            .strip_prefix(codec::V2_MAGIC.as_slice())
            .ok_or_else(|| "unknown frame tag".to_owned())?;
        let (length, used) = codec::read_varint(rest)?;
        let rest = &rest[used..];
        let length = usize::try_from(length)
            .map_err(|_| "frame length overflows this platform".to_owned())?;
        if rest.len() != length.saturating_add(4) {
            return Err("frame length mismatch".to_owned());
        }
        let (payload, digest) = rest.split_at(length);
        let expected = u32::from_le_bytes(digest.try_into().expect("digest is four bytes"));
        if codec::checksum(payload) != expected {
            return Err("frame checksum mismatch".to_owned());
        }
        let mut cursor = Cursor::new(payload);
        let sequence = cursor.varint()?;
        let transaction = cursor.varint()?;
        let receipt = match cursor.byte()? {
            0 => None,
            1 => {
                let client = cursor.text()?;
                let request = cursor.text()?;
                RequestId::new(client, request)
                    .map(Some)
                    .map_err(|error| error.to_string())?
            }
            tag => return Err(format!("unknown receipt tag {tag} in v2 commit")),
        };
        let count = usize::try_from(cursor.varint()?)
            .map_err(|_| "too many writes in commit record".to_owned())?;
        if count > MAX_CHANGES {
            return Err("too many writes in commit record".to_owned());
        }
        let mut changes = Vec::<VersionChange>::with_capacity(count);
        for _ in 0..count {
            let table = cursor.text()?;
            let key = match cursor.byte()? {
                1 => Key::Integer(cursor.zigzag()?),
                2 => Key::Text(cursor.text()?),
                tag => return Err(format!("unknown key tag {tag} in v2 commit")),
            };
            let values = match cursor.byte()? {
                0 => None,
                1 => Some(read_row(&mut cursor)?),
                tag => return Err(format!("unknown row tag {tag} in v2 commit")),
            };
            let id = RowId::new(table, key);
            if changes
                .last()
                .is_some_and(|previous| previous.id >= id)
            {
                return Err("writes must appear in strict canonical order".to_owned());
            }
            changes.push(VersionChange { id, values });
        }
        if !cursor.done() {
            return Err("trailing bytes in v2 commit frame".to_owned());
        }
        Ok(Self {
            sequence,
            transaction,
            receipt,
            changes,
        })
    }
}

pub(crate) fn decode_request(
    client: &str,
    request: &str,
) -> std::result::Result<Option<RequestId>, String> {
    if client == "-" && request == "-" {
        return Ok(None);
    }
    RequestId::new(codec::hex_decode(client)?, codec::hex_decode(request)?)
        .map(Some)
        .map_err(|error| error.to_string())
}

pub(crate) fn next<'a>(
    fields: &mut impl Iterator<Item = &'a str>,
    name: &'static str,
) -> std::result::Result<&'a str, String> {
    fields.next().ok_or_else(|| format!("missing {name}"))
}

fn write_text(output: &mut Vec<u8>, text: &str) {
    codec::write_varint(output, text.len() as u64);
    output.extend_from_slice(text.as_bytes());
}

fn write_value(output: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => output.push(0),
        Value::Integer(number) => {
            output.push(1);
            codec::write_zigzag(output, *number);
        }
        Value::Text(text) => {
            output.push(2);
            write_text(output, text);
        }
    }
}

fn read_row(cursor: &mut Cursor<'_>) -> std::result::Result<Row, String> {
    let columns = usize::try_from(cursor.varint()?)
        .map_err(|_| "too many columns in v2 row".to_owned())?;
    let mut row = Row::new();
    let mut previous = String::new();
    for _ in 0..columns {
        let column = cursor.text()?;
        if column <= previous {
            return Err("columns must appear in strict canonical order".to_owned());
        }
        let value = match cursor.byte()? {
            0 => Value::Null,
            1 => Value::Integer(cursor.zigzag()?),
            2 => Value::Text(cursor.text()?),
            tag => return Err(format!("unknown value tag {tag} in v2 commit")),
        };
        previous = column.clone();
        row.insert(column, value);
    }
    Ok(row)
}

struct Cursor<'a> {
    input: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    fn done(&self) -> bool {
        self.input.is_empty()
    }

    fn byte(&mut self) -> std::result::Result<u8, String> {
        let (&byte, rest) = self
            .input
            .split_first()
            .ok_or_else(|| "truncated v2 commit payload".to_owned())?;
        self.input = rest;
        Ok(byte)
    }

    fn take(&mut self, length: usize) -> std::result::Result<&'a [u8], String> {
        if self.input.len() < length {
            return Err("truncated v2 commit payload".to_owned());
        }
        let (head, rest) = self.input.split_at(length);
        self.input = rest;
        Ok(head)
    }

    fn varint(&mut self) -> std::result::Result<u64, String> {
        let (value, used) = codec::read_varint(self.input)?;
        self.input = &self.input[used..];
        Ok(value)
    }

    fn zigzag(&mut self) -> std::result::Result<i64, String> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn text(&mut self) -> std::result::Result<String, String> {
        let length = usize::try_from(self.varint()?)
            .map_err(|_| "text length overflows this platform".to_owned())?;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| "text is not UTF-8".to_owned())
    }
}
