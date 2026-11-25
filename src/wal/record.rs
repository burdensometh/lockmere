use super::codec;
use crate::{
    error::{Error, Result},
    rows::{RowId, Sequence, TxId, VersionChange},
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

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let receipt = match &self.receipt {
            Some(id) => format!(
                "{}\t{}",
                codec::hex_encode(&id.client),
                codec::hex_encode(&id.request)
            ),
            None => "-\t-".to_owned(),
        };
        let mut body = format!(
            "C\t{}\t{}\t{receipt}\t{}",
            self.sequence,
            self.transaction,
            self.changes.len()
        );
        for change in &self.changes {
            body.push('\t');
            body.push_str(&format!(
                "{}:{}:{}",
                change.id.table,
                codec::key(&change.id.key),
                codec::row(change.values.as_ref())
            ));
            if body.len() > codec::MAX_LINE {
                return Err(Error::Limit("journal record bytes"));
            }
        }
        body.push_str(&format!("\tE{}", self.sequence));
        codec::seal(&body)
    }

    pub(crate) fn decode(line: &[u8]) -> std::result::Result<Self, String> {
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
