use std::{collections::BTreeSet, fs, path::Path, time::Duration};

use crate::{
    constraints::{Column, Constraint, ConstraintMode, OnUpdate, Schema, Table, Timing},
    error::{Error, Result},
    rows::KeyKind,
    tx::Options,
};

const MAX_FILE_BYTES: u64 = 1024 * 1024;

pub fn read_options(path: impl AsRef<Path>) -> Result<Options> {
    let source = read(path.as_ref())?;
    let mut options = Options::default();
    let mut seen = BTreeSet::new();
    for (line, content) in entries(&source) {
        let (key, value) = content
            .split_once('=')
            .ok_or_else(|| bad(line, "expected key = integer"))?;
        let key = key.trim();
        let value: u64 = value
            .trim()
            .parse()
            .map_err(|_| bad(line, "invalid unsigned option value"))?;
        if !seen.insert(key) {
            return Err(bad(line, "option repeated"));
        }
        let size = || usize::try_from(value).map_err(|_| bad(line, "option exceeds platform size"));
        match key {
            "lock_timeout_ms" => options.lock_timeout = Duration::from_millis(value),
            "max_transactions" => options.max_transactions = size()?,
            "max_intents" => options.max_intents = size()?,
            "max_scan" => options.max_scan = size()?,
            "max_savepoints" => options.max_savepoints = size()?,
            "max_undo" => options.max_undo = size()?,
            "event_capacity" => options.event_capacity = size()?,
            _ => return Err(bad(line, &format!("unknown option {key}"))),
        }
    }
    options.validate()?;
    Ok(options)
}

pub fn read_schema(path: impl AsRef<Path>) -> Result<Schema> {
    parse_schema(&read(path.as_ref())?)
}

pub fn parse_schema(source: &str) -> Result<Schema> {
    let mut tables = Vec::new();
    let mut constraints = Vec::new();
    for (line, content) in entries(&source) {
        let tokens: Vec<_> = content.split_whitespace().collect();
        match tokens.as_slice() {
            ["table", name, primary, columns @ ..] => {
                let mut table = Table::new(*name, kind(primary, line)?);
                for spec in columns {
                    let (name, column) = spec
                        .split_once(':')
                        .ok_or_else(|| bad(line, "expected column:TYPE! or column:TYPE?"))?;
                    let (data_type, required) = if let Some(value) = column.strip_suffix('!') {
                        (value, true)
                    } else if let Some(value) = column.strip_suffix('?') {
                        (value, false)
                    } else {
                        return Err(bad(line, "column requires ! or ? nullability"));
                    };
                    if table
                        .columns
                        .insert(
                            name.to_owned(),
                            Column::new(kind(data_type, line)?, required),
                        )
                        .is_some()
                    {
                        return Err(bad(line, "column repeated"));
                    }
                }
                tables.push(table);
            }
            ["unique", name, source, when] => {
                let (table, column) = reference(source, line)?;
                constraints.push(Constraint::unique(name, table, column, timing(when, line)?));
            }
            ["foreign", name, source, target, when, action @ ..] => {
                let (table, column) = reference(source, line)?;
                let (parent, target) = reference(target, line)?;
                let on_update = match action {
                    [] => OnUpdate::Restrict,
                    ["on-update-restrict"] => OnUpdate::Restrict,
                    ["on-update-cascade"] => OnUpdate::Cascade,
                    _ => return Err(bad(line, "expected on-update-restrict or on-update-cascade")),
                };
                constraints.push(Constraint::foreign_key(
                    name,
                    table,
                    column,
                    parent,
                    target,
                    timing(when, line)?,
                    on_update,
                ));
            }
            _ => return Err(bad(line, "expected table, unique, or foreign declaration")),
        }
    }
    Schema::new(tables, constraints)
}

fn entries(source: &str) -> impl Iterator<Item = (usize, &str)> {
    source.lines().enumerate().filter_map(|(line, raw)| {
        let content = raw.split('#').next().unwrap_or("").trim();
        (!content.is_empty()).then_some((line + 1, content))
    })
}

fn kind(raw: &str, line: usize) -> Result<KeyKind> {
    match raw {
        "INT" => Ok(KeyKind::Integer),
        "TEXT" => Ok(KeyKind::Text),
        _ => Err(bad(line, "expected INT or TEXT")),
    }
}

fn timing(raw: &str, line: usize) -> Result<Timing> {
    match raw {
        "immediate" => Ok(Timing::Immediate),
        "deferrable-immediate" => Ok(Timing::Deferrable(ConstraintMode::Immediate)),
        "deferrable-deferred" => Ok(Timing::Deferrable(ConstraintMode::Deferred)),
        _ => Err(bad(line, "unknown constraint timing")),
    }
}

fn reference(raw: &str, line: usize) -> Result<(&str, &str)> {
    raw.split_once('.')
        .ok_or_else(|| bad(line, "expected table.column"))
}

fn read(path: &Path) -> Result<String> {
    let size = fs::metadata(path)
        .map_err(|error| Error::io("stat configuration", error))?
        .len();
    if size > MAX_FILE_BYTES {
        return Err(Error::Limit("configuration bytes"));
    }
    fs::read_to_string(path).map_err(|error| Error::io("read configuration", error))
}

fn bad(line: usize, detail: &str) -> Error {
    Error::InvalidRequest(format!("configuration at line {line}: {detail}"))
}
