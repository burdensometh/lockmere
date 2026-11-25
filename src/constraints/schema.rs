use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result};
use crate::rows::{KeyKind, ReadInterval, Row, RowId, Value};

pub const PRIMARY_KEY: &str = "id";
pub(crate) const MAX_IDENTIFIER: usize = 64;
pub(crate) const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_SCHEMA_OBJECTS: usize = 1024;

pub(crate) fn identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= MAX_IDENTIFIER
        && matches!(bytes.next(), Some(b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintMode {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    Immediate,
    Deferrable(ConstraintMode),
}

impl Timing {
    pub fn initial(self) -> ConstraintMode {
        match self {
            Self::Immediate => ConstraintMode::Immediate,
            Self::Deferrable(mode) => mode,
        }
    }

    pub fn deferrable(self) -> bool {
        matches!(self, Self::Deferrable(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub kind: KeyKind,
    pub required: bool,
}

impl Column {
    pub fn new(kind: KeyKind, required: bool) -> Self {
        Self { kind, required }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub primary: KeyKind,
    pub columns: BTreeMap<String, Column>,
}

impl Table {
    pub fn new(name: impl Into<String>, primary: KeyKind) -> Self {
        Self {
            name: name.into(),
            primary,
            columns: BTreeMap::new(),
        }
    }

    pub fn column(mut self, name: impl Into<String>, column: Column) -> Self {
        self.columns.insert(name.into(), column);
        self
    }

    pub fn kind(&self, column: &str) -> Option<KeyKind> {
        if column == PRIMARY_KEY {
            Some(self.primary)
        } else {
            self.columns.get(column).map(|column| column.kind)
        }
    }

    pub fn required(&self, column: &str) -> bool {
        column == PRIMARY_KEY
            || self
                .columns
                .get(column)
                .is_some_and(|column| column.required)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    Unique {
        table: String,
        column: String,
    },
    ForeignKey {
        table: String,
        column: String,
        parent: String,
        parent_column: String,
    },
}

impl ConstraintKind {
    pub fn source(&self) -> (&str, &str) {
        match self {
            Self::Unique { table, column } | Self::ForeignKey { table, column, .. } => {
                (table, column)
            }
        }
    }

    pub fn observes(&self, name: &str) -> bool {
        self.source().0 == name || matches!(self, Self::ForeignKey { parent, .. } if parent == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    pub name: String,
    pub timing: Timing,
    pub kind: ConstraintKind,
}

impl Constraint {
    pub fn unique(name: &str, table: &str, column: &str, timing: Timing) -> Self {
        Self {
            name: name.to_owned(),
            timing,
            kind: ConstraintKind::Unique {
                table: table.to_owned(),
                column: column.to_owned(),
            },
        }
    }

    pub fn foreign_key(
        name: &str,
        table: &str,
        column: &str,
        parent: &str,
        parent_column: &str,
        timing: Timing,
    ) -> Self {
        Self {
            name: name.to_owned(),
            timing,
            kind: ConstraintKind::ForeignKey {
                table: table.to_owned(),
                column: column.to_owned(),
                parent: parent.to_owned(),
                parent_column: parent_column.to_owned(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    tables: BTreeMap<String, Table>,
    constraints: BTreeMap<String, Constraint>,
}

impl Schema {
    pub fn new(tables: Vec<Table>, constraints: Vec<Constraint>) -> Result<Self> {
        if tables.is_empty()
            || tables.len() > MAX_SCHEMA_OBJECTS
            || constraints.len() > MAX_SCHEMA_OBJECTS
        {
            return Err(Error::InvalidSchema(
                "invalid number of tables or constraints".to_owned(),
            ));
        }
        let mut schema = Self {
            tables: BTreeMap::new(),
            constraints: BTreeMap::new(),
        };
        for table in tables {
            let name = table.name.clone();
            if !identifier(&name) || table.columns.len() > MAX_SCHEMA_OBJECTS {
                return Err(Error::InvalidSchema(format!("invalid table {name:?}")));
            }
            for column in table.columns.keys() {
                if !identifier(column) || column == PRIMARY_KEY {
                    return Err(Error::InvalidSchema(format!(
                        "invalid column {name}.{column}"
                    )));
                }
            }
            if schema.tables.insert(name.clone(), table).is_some() {
                return Err(Error::InvalidSchema(format!("repeated table {name}")));
            }
        }
        let mut unique_columns = BTreeSet::new();
        for constraint in constraints {
            let name = constraint.name.clone();
            if !identifier(&name) {
                return Err(Error::InvalidSchema(format!(
                    "invalid constraint name {name:?}"
                )));
            }
            let (table, column) = constraint.kind.source();
            schema.non_null_kind(table, column)?;
            if matches!(&constraint.kind, ConstraintKind::Unique { .. })
                && !unique_columns.insert((table.to_owned(), column.to_owned()))
            {
                return Err(Error::InvalidSchema(format!(
                    "repeated uniqueness for {table}.{column}"
                )));
            }
            if schema
                .constraints
                .insert(name.clone(), constraint)
                .is_some()
            {
                return Err(Error::InvalidSchema(format!("repeated constraint {name}")));
            }
        }
        for constraint in schema.constraints() {
            if let ConstraintKind::ForeignKey {
                table,
                column,
                parent,
                parent_column,
            } = &constraint.kind
            {
                let source = schema.non_null_kind(table, column)?;
                let target = schema.non_null_kind(parent, parent_column)?;
                let unique = parent_column == PRIMARY_KEY
                    || unique_columns.contains(&(parent.clone(), parent_column.clone()));
                if source != target || !unique {
                    return Err(Error::InvalidSchema(format!(
                        "{} must reference an equally typed unique column",
                        constraint.name
                    )));
                }
            }
        }
        Ok(schema)
    }

    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }

    pub fn constraints(&self) -> impl Iterator<Item = &Constraint> {
        self.constraints.values()
    }

    pub fn table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::UnknownTable(name.to_owned()))
    }

    pub fn constraint(&self, name: &str) -> Result<&Constraint> {
        self.constraints
            .get(name)
            .ok_or_else(|| Error::UnknownConstraint(name.to_owned()))
    }

    pub fn validate_id(&self, id: &RowId) -> Result<()> {
        let table = self.table(&id.table)?;
        if id.key.kind() != table.primary {
            return Err(Error::InvalidValue(format!(
                "{id} requires a {} primary key",
                table.primary.label()
            )));
        }
        Self::text_limit(&id.key.value())
    }

    pub fn validate_interval(&self, interval: &ReadInterval) -> Result<()> {
        let table = self.table(&interval.table)?;
        for key in interval.endpoint_keys() {
            if key.kind() != table.primary {
                return Err(Error::InvalidValue(format!(
                    "{} range requires {} endpoints",
                    table.name,
                    table.primary.label()
                )));
            }
            Self::text_limit(&key.value())?;
        }
        Ok(())
    }

    pub fn normalize_row(&self, id: &RowId, mut values: Row) -> Result<Row> {
        self.validate_fields(id, &values)?;
        for (name, column) in &self.table(&id.table)?.columns {
            if !values.contains_key(name) {
                if column.required {
                    return Err(Error::InvalidValue(format!(
                        "missing required column {}.{name}",
                        id.table
                    )));
                }
                values.insert(name.clone(), Value::Null);
            }
        }
        Ok(values)
    }

    pub fn validate_fields(&self, id: &RowId, fields: &Row) -> Result<()> {
        self.validate_id(id)?;
        let table = self.table(&id.table)?;
        for (name, value) in fields {
            let column = table
                .columns
                .get(name)
                .ok_or_else(|| Error::UnknownColumn {
                    table: table.name.clone(),
                    column: name.clone(),
                })?;
            Self::text_limit(value)?;
            match value.kind() {
                Some(kind) if kind == column.kind => {}
                None if !column.required => {}
                _ => {
                    return Err(Error::InvalidValue(format!(
                        "{}.{} requires {}{}",
                        table.name,
                        name,
                        column.kind.label(),
                        if column.required { " NOT NULL" } else { "" }
                    )));
                }
            }
        }
        Ok(())
    }

    fn non_null_kind(&self, table: &str, column: &str) -> Result<KeyKind> {
        self.tables
            .get(table)
            .filter(|entry| entry.required(column))
            .and_then(|entry| entry.kind(column))
            .ok_or_else(|| {
                Error::InvalidSchema(format!(
                    "constraint requires a non-null column {table}.{column}"
                ))
            })
    }

    fn text_limit(value: &Value) -> Result<()> {
        if matches!(value, Value::Text(text) if text.len() > MAX_TEXT_BYTES) {
            return Err(Error::Limit("text bytes"));
        }
        Ok(())
    }
}
