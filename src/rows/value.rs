use std::{collections::BTreeMap, fmt, ops::Bound};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyKind {
    Integer,
    Text,
}

impl KeyKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Integer => "INT",
            Self::Text => "TEXT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    Integer(i64),
    Text(String),
}

impl Key {
    pub fn kind(&self) -> KeyKind {
        match self {
            Self::Integer(_) => KeyKind::Integer,
            Self::Text(_) => KeyKind::Text,
        }
    }

    pub fn value(&self) -> Value {
        match self {
            Self::Integer(n) => Value::Integer(*n),
            Self::Text(s) => Value::Text(s.clone()),
        }
    }
}

impl From<i64> for Key {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl fmt::Display for Key {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(number) => write!(out, "{number}"),
            Self::Text(text) => write!(out, "{text:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
}

impl Value {
    pub fn kind(&self) -> Option<KeyKind> {
        match self {
            Self::Null => None,
            Self::Integer(_) => Some(KeyKind::Integer),
            Self::Text(_) => Some(KeyKind::Text),
        }
    }

    pub fn into_key(self) -> Result<Key> {
        match self {
            Self::Integer(number) => Ok(Key::Integer(number)),
            Self::Text(text) => Ok(Key::Text(text)),
            Self::Null => Err(Error::InvalidValue(
                "a primary key cannot be null".to_owned(),
            )),
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId {
    pub table: String,
    pub key: Key,
}

impl RowId {
    pub fn new(table: impl Into<String>, key: impl Into<Key>) -> Self {
        Self {
            table: table.into(),
            key: key.into(),
        }
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{}[{}]", self.table, self.key)
    }
}

pub type Row = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub id: RowId,
    pub values: Row,
}

impl Record {
    pub fn new(id: RowId, values: Row) -> Self {
        Self { id, values }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadInterval {
    pub table: String,
    pub lower: Bound<Key>,
    pub upper: Bound<Key>,
}

impl ReadInterval {
    pub fn new(table: impl Into<String>, lower: Bound<Key>, upper: Bound<Key>) -> Self {
        Self {
            table: table.into(),
            lower,
            upper,
        }
    }

    pub fn whole(table: impl Into<String>) -> Self {
        Self::new(table, Bound::Unbounded, Bound::Unbounded)
    }

    pub fn point(row: &RowId) -> Self {
        Self::new(
            row.table.clone(),
            Bound::Included(row.key.clone()),
            Bound::Included(row.key.clone()),
        )
    }

    pub fn endpoint_keys(&self) -> impl Iterator<Item = &Key> {
        [&self.lower, &self.upper]
            .into_iter()
            .filter_map(|bound| match bound {
                Bound::Included(key) | Bound::Excluded(key) => Some(key),
                Bound::Unbounded => None,
            })
    }

    pub fn is_empty(&self) -> bool {
        Self::before(&self.upper, &self.lower)
    }

    pub fn contains(&self, key: &Key) -> bool {
        let after_start = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(start) => key >= start,
            Bound::Excluded(start) => key > start,
        };
        let before_end = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
        };
        after_start && before_end
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.table == other.table
            && !self.is_empty()
            && !other.is_empty()
            && !Self::before(&self.upper, &other.lower)
            && !Self::before(&other.upper, &self.lower)
    }

    pub(crate) fn bounds(&self) -> (Bound<&Key>, Bound<&Key>) {
        (self.lower.as_ref(), self.upper.as_ref())
    }

    fn before(end: &Bound<Key>, start: &Bound<Key>) -> bool {
        match (end, start) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Included(a), Bound::Included(b)) => a < b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a <= b
            }
        }
    }
}

impl fmt::Display for ReadInterval {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lower = match &self.lower {
            Bound::Unbounded => "(-inf".to_owned(),
            Bound::Included(key) => format!("[{key}"),
            Bound::Excluded(key) => format!("({key}"),
        };
        let upper = match &self.upper {
            Bound::Unbounded => "+inf)".to_owned(),
            Bound::Included(key) => format!("{key}]"),
            Bound::Excluded(key) => format!("{key})"),
        };
        write!(out, "{}{lower},{upper}", self.table)
    }
}
