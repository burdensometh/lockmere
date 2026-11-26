use std::io::BufRead;

use super::{codec, JOURNAL_FILE};
use crate::{
    constraints::{Column, Constraint, ConstraintKind, ConstraintMode, OnUpdate, Schema, Table, Timing},
    error::{Error, Result},
    rows::KeyKind,
};

const MAGIC: &str = "LOCKMERE\t1\n";
const DATA: &str = "DATA\n";
const MAX_HEADER: usize = 16 * 1024 * 1024;

pub(crate) fn encode(schema: &Schema, identity: &str) -> Result<Vec<u8>> {
    let mut header = format!(
        "{MAGIC}STORE	{identity}
"
    );
    for table in schema.tables() {
        header.push_str(&format!("T\t{}\t{}", table.name, kind(table.primary)));
        for (name, column) in &table.columns {
            header.push_str(&format!(
                "\t{name}:{}:{}",
                kind(column.kind),
                if column.required { "R" } else { "O" }
            ));
        }
        header.push('\n');
    }
    for constraint in schema.constraints() {
        let timing = match constraint.timing {
            Timing::Immediate => "I",
            Timing::Deferrable(ConstraintMode::Immediate) => "DI",
            Timing::Deferrable(ConstraintMode::Deferred) => "DD",
        };
        match &constraint.kind {
            ConstraintKind::Unique { table, column } => {
                header.push_str(&format!(
                    "U\t{}\t{table}\t{column}\t{timing}\n",
                    constraint.name
                ));
            }
            ConstraintKind::ForeignKey {
                table,
                column,
                parent,
                parent_column,
                on_update,
            } => {
                let action = match on_update {
                    OnUpdate::Restrict => "",
                    OnUpdate::Cascade => "\tC",
                };
                header.push_str(&format!(
                    "F\t{}\t{table}\t{column}\t{parent}\t{parent_column}\t{timing}{action}\n",
                    constraint.name
                ));
            }
        }
    }
    header.push_str(DATA);
    if header.len() > MAX_HEADER {
        return Err(Error::Limit("journal schema bytes"));
    }
    Ok(header.into_bytes())
}

pub(crate) fn read(reader: &mut impl BufRead) -> Result<(Schema, u64, u64)> {
    let mut bytes = Vec::new();
    let mut line = Vec::new();
    while codec::next_line(reader, &mut line, JOURNAL_FILE, bytes.len() as u64)? {
        if bytes.len().saturating_add(line.len()) > MAX_HEADER {
            return Err(Error::corrupt(
                JOURNAL_FILE,
                0,
                "schema header exceeds the size limit",
            ));
        }
        bytes.extend_from_slice(&line);
        if bytes.len() == line.len() && line != MAGIC.as_bytes() {
            return Err(Error::corrupt(JOURNAL_FILE, 0, "missing Lockmere v1 magic"));
        }
        if line == DATA.as_bytes() {
            let (schema, identity) =
                decode(&bytes).map_err(|detail| Error::corrupt(JOURNAL_FILE, 0, detail))?;
            if encode(&schema, identity)? != bytes {
                return Err(Error::corrupt(
                    JOURNAL_FILE,
                    0,
                    "schema header is not canonical",
                ));
            }
            return Ok((schema, bytes.len() as u64, codec::hash(&bytes)));
        }
    }
    Err(Error::corrupt(JOURNAL_FILE, 0, "incomplete schema header"))
}

fn decode(bytes: &[u8]) -> std::result::Result<(Schema, &str), String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "schema header is not UTF-8")?;
    let mut tables = Vec::new();
    let mut constraints = Vec::new();
    let mut lines = text.trim_end_matches("DATA\n").lines().skip(1);
    let identity = lines
        .next()
        .and_then(|line| line.strip_prefix("STORE\t"))
        .ok_or("missing store identity")?;
    if identity.len() != 32
        || !identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("invalid store identity".to_owned());
    }
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["T", name, primary, columns @ ..] => {
                let mut table = Table::new(*name, decode_kind(primary)?);
                for column in columns {
                    let parts = column.split(':').collect::<Vec<_>>();
                    let [name, data_type, nullness] = parts.as_slice() else {
                        return Err(format!("invalid column definition {column:?}"));
                    };
                    let required = match *nullness {
                        "R" => true,
                        "O" => false,
                        _ => return Err(format!("invalid nullability {nullness:?}")),
                    };
                    let definition = Column::new(decode_kind(data_type)?, required);
                    if table
                        .columns
                        .insert((*name).to_owned(), definition)
                        .is_some()
                    {
                        return Err(format!("repeated column {name}"));
                    }
                }
                tables.push(table);
            }
            ["U", name, table, column, timing] => {
                constraints.push(Constraint::unique(
                    name,
                    table,
                    column,
                    decode_timing(timing)?,
                ));
            }
            ["F", name, table, column, parent, target, timing, action @ ..] => {
                let on_update = match action {
                    [] => OnUpdate::Restrict,
                    ["C"] => OnUpdate::Cascade,
                    _ => return Err(format!("invalid on-update action in {line:?}")),
                };
                constraints.push(Constraint::foreign_key(
                    name,
                    table,
                    column,
                    parent,
                    target,
                    decode_timing(timing)?,
                    on_update,
                ));
            }
            _ => return Err(format!("invalid schema line {line:?}")),
        }
    }
    Schema::new(tables, constraints)
        .map(|schema| (schema, identity))
        .map_err(|error| error.to_string())
}

fn kind(kind: KeyKind) -> &'static str {
    match kind {
        KeyKind::Integer => "I",
        KeyKind::Text => "S",
    }
}

fn decode_kind(token: &str) -> std::result::Result<KeyKind, String> {
    match token {
        "I" => Ok(KeyKind::Integer),
        "S" => Ok(KeyKind::Text),
        _ => Err(format!("unknown column type {token:?}")),
    }
}

fn decode_timing(token: &str) -> std::result::Result<Timing, String> {
    match token {
        "I" => Ok(Timing::Immediate),
        "DI" => Ok(Timing::Deferrable(ConstraintMode::Immediate)),
        "DD" => Ok(Timing::Deferrable(ConstraintMode::Deferred)),
        _ => Err(format!("unknown constraint timing {token:?}")),
    }
}
