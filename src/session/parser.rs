use std::{cmp::Ordering, ops::Bound, vec};

use super::lexer::{self, Item, Token};
use crate::{
    constraints::{ConstraintMode, PRIMARY_KEY},
    error::{Error, Result},
    rows::{Key, ReadInterval, Record, Row, RowId, Value},
    tx::{Isolation, Operation},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Begin {
        isolation: Isolation,
        priority: i64,
    },
    Transaction(Operation),
    Select {
        interval: ReadInterval,
        limit: Option<usize>,
    },
    Commit,
    Rollback,
    Checkpoint,
    ShowLocks,
    ShowEvents {
        after: u64,
    },
    ShowHistory {
        after: u64,
        limit: usize,
    },
    ShowTransaction,
    ShowSchema,
}

pub fn parse(sql: &str) -> Result<Command> {
    let mut input = Parser::new(lexer::tokenize(sql)?, sql.len());
    let verb = input.word()?;
    let command = match verb.as_str() {
        "begin" => input.begin()?,
        "get" => {
            let table = input.word()?;
            Command::Transaction(Operation::Get(RowId::new(
                table,
                input.literal()?.into_key()?,
            )))
        }
        "select" => input.select()?,
        "insert" => input.insert()?,
        "update" => input.update()?,
        "delete" => input.delete()?,
        "savepoint" => Command::Transaction(Operation::Savepoint(input.word()?)),
        "rollback" => {
            if input.keyword("to") {
                input.keyword("savepoint");
                Command::Transaction(Operation::RollbackTo(input.word()?))
            } else {
                Command::Rollback
            }
        }
        "release" => {
            input.keyword("savepoint");
            Command::Transaction(Operation::Release(input.word()?))
        }
        "set" => input.set_constraints()?,
        "commit" => Command::Commit,
        "checkpoint" => Command::Checkpoint,
        "show" => input.show()?,
        _ => return Err(lexer::syntax(0, &format!("unknown command {verb}"))),
    };
    input.take(&Token::Semicolon);
    if input.peek().is_some() {
        return Err(input.error("unexpected token after statement"));
    }
    Ok(command)
}

struct Parser {
    tokens: std::iter::Peekable<vec::IntoIter<Item>>,
    end: usize,
}

impl Parser {
    fn new(tokens: Vec<Item>, end: usize) -> Self {
        Self {
            tokens: tokens.into_iter().peekable(),
            end,
        }
    }

    fn begin(&mut self) -> Result<Command> {
        self.keyword("transaction");
        if self.keyword("isolation") {
            self.expect_word("level")?;
        }
        let isolation = match self.peek() {
            None | Some(Token::Semicolon) => Isolation::Serializable,
            Some(Token::Word(word)) if word == "priority" => Isolation::Serializable,
            _ => match self.word()?.as_str() {
                "snapshot" => Isolation::Snapshot,
                "serializable" => Isolation::Serializable,
                _ => return Err(self.error("expected SNAPSHOT or SERIALIZABLE")),
            },
        };
        let mut priority = 0;
        if self.keyword("priority") {
            let offset = self.offset();
            priority = match self.tokens.next().map(|item| item.token) {
                Some(Token::Integer(value)) => value,
                _ => return Err(lexer::syntax(offset, "expected a priority integer")),
            };
        }
        Ok(Command::Begin {
            isolation,
            priority,
        })
    }

    fn select(&mut self) -> Result<Command> {
        self.expect(Token::Star, "'*'")?;
        self.expect_word("from")?;
        let table = self.word()?;
        let mut interval = ReadInterval::whole(table);
        if self.keyword("where") {
            loop {
                self.expect_word(PRIMARY_KEY)?;
                let offset = self.offset();
                let comparison = self
                    .tokens
                    .next()
                    .ok_or_else(|| self.error("expected comparison"))?
                    .token;
                let key = self.literal()?.into_key()?;
                match comparison {
                    Token::Equal => {
                        set_lower(&mut interval, Bound::Included(key.clone()))?;
                        set_upper(&mut interval, Bound::Included(key))?;
                    }
                    Token::Greater => set_lower(&mut interval, Bound::Excluded(key))?,
                    Token::GreaterEqual => set_lower(&mut interval, Bound::Included(key))?,
                    Token::Less => set_upper(&mut interval, Bound::Excluded(key))?,
                    Token::LessEqual => set_upper(&mut interval, Bound::Included(key))?,
                    _ => return Err(lexer::syntax(offset, "expected primary-key comparison")),
                }
                if !self.keyword("and") {
                    break;
                }
            }
        }
        let limit = if self.keyword("limit") {
            Some(usize::try_from(self.nonnegative()?).map_err(|_| Error::Limit("scan rows"))?)
        } else {
            None
        };
        Ok(Command::Select { interval, limit })
    }

    fn insert(&mut self) -> Result<Command> {
        self.expect_word("into")?;
        let table = self.word()?;
        self.expect(Token::Left, "'('")?;
        let mut columns = vec![self.word()?];
        while self.take(&Token::Comma) {
            columns.push(self.word()?);
        }
        self.expect(Token::Right, "')'")?;
        self.expect_word("values")?;
        self.expect(Token::Left, "'('")?;
        let mut values = Row::new();
        for (position, column) in columns.into_iter().enumerate() {
            if position != 0 {
                self.expect(Token::Comma, "','")?;
            }
            let value = self.literal()?;
            if values.insert(column.clone(), value).is_some() {
                return Err(self.error(&format!("repeated column {column}")));
            }
        }
        self.expect(Token::Right, "')'")?;
        let key = values
            .remove(PRIMARY_KEY)
            .ok_or_else(|| self.error("INSERT requires a value for id"))?
            .into_key()?;
        Ok(Command::Transaction(Operation::Insert(Record::new(
            RowId::new(table, key),
            values,
        ))))
    }

    fn update(&mut self) -> Result<Command> {
        let table = self.word()?;
        self.expect_word("set")?;
        let mut patch = Row::new();
        loop {
            let column = self.word()?;
            if column == PRIMARY_KEY {
                return Err(self.error("UPDATE cannot change a primary key"));
            }
            self.expect(Token::Equal, "'='")?;
            let value = self.literal()?;
            if patch.insert(column.clone(), value).is_some() {
                return Err(self.error(&format!("repeated column {column}")));
            }
            if !self.take(&Token::Comma) {
                break;
            }
        }
        let id = self.write_predicate(table)?;
        Ok(Command::Transaction(Operation::Update { id, patch }))
    }

    fn delete(&mut self) -> Result<Command> {
        self.expect_word("from")?;
        let table = self.word()?;
        let id = self.write_predicate(table)?;
        Ok(Command::Transaction(Operation::Delete(id)))
    }

    fn write_predicate(&mut self, table: String) -> Result<RowId> {
        self.expect_word("where")?;
        self.expect_word(PRIMARY_KEY)?;
        self.expect(Token::Equal, "'='")?;
        Ok(RowId::new(table, self.literal()?.into_key()?))
    }

    fn set_constraints(&mut self) -> Result<Command> {
        self.expect_word("constraints")?;
        let name = self.word()?;
        let mode = match self.word()?.as_str() {
            "immediate" => ConstraintMode::Immediate,
            "deferred" => ConstraintMode::Deferred,
            _ => return Err(self.error("expected IMMEDIATE or DEFERRED")),
        };
        let name = (name != "all").then_some(name);
        Ok(Command::Transaction(Operation::SetConstraints {
            name,
            mode,
        }))
    }

    fn show(&mut self) -> Result<Command> {
        match self.word()?.as_str() {
            "locks" => Ok(Command::ShowLocks),
            "transaction" => Ok(Command::ShowTransaction),
            "schema" => Ok(Command::ShowSchema),
            "events" => Ok(Command::ShowEvents {
                after: self.after()?,
            }),
            "history" => {
                let after = self.after()?;
                let limit = if self.keyword("limit") {
                    usize::try_from(self.nonnegative()?)
                        .map_err(|_| Error::Limit("history rows"))?
                } else {
                    100
                };
                if limit == 0 || limit > 4096 {
                    return Err(Error::Limit("history rows"));
                }
                Ok(Command::ShowHistory { after, limit })
            }
            _ => Err(self.error("unknown SHOW target")),
        }
    }

    fn after(&mut self) -> Result<u64> {
        if self.keyword("after") {
            self.nonnegative()
        } else {
            Ok(0)
        }
    }

    fn literal(&mut self) -> Result<Value> {
        let offset = self.offset();
        match self.tokens.next().map(|item| item.token) {
            Some(Token::Integer(value)) => Ok(Value::Integer(value)),
            Some(Token::Text(value)) => Ok(Value::Text(value)),
            Some(Token::Word(word)) if word == "null" => Ok(Value::Null),
            _ => Err(lexer::syntax(offset, "expected a literal")),
        }
    }

    fn nonnegative(&mut self) -> Result<u64> {
        let offset = self.offset();
        match self.tokens.next().map(|item| item.token) {
            Some(Token::Integer(value)) if value >= 0 => Ok(value as u64),
            _ => Err(lexer::syntax(offset, "expected a nonnegative integer")),
        }
    }

    fn word(&mut self) -> Result<String> {
        let offset = self.offset();
        match self.tokens.next().map(|item| item.token) {
            Some(Token::Word(word)) => Ok(word),
            _ => Err(lexer::syntax(offset, "expected an identifier")),
        }
    }

    fn expect_word(&mut self, expected: &str) -> Result<()> {
        if self.keyword(expected) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {}", expected.to_ascii_uppercase())))
        }
    }

    fn expect(&mut self, token: Token, description: &str) -> Result<()> {
        if self.take(&token) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {description}")))
        }
    }

    fn keyword(&mut self, expected: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(word)) if word == expected) {
            self.tokens.next();
            true
        } else {
            false
        }
    }

    fn take(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.tokens.next();
            true
        } else {
            false
        }
    }

    fn peek(&mut self) -> Option<&Token> {
        self.tokens.peek().map(|item| &item.token)
    }

    fn offset(&mut self) -> usize {
        self.tokens.peek().map_or(self.end, |item| item.offset)
    }

    fn error(&mut self, detail: &str) -> Error {
        lexer::syntax(self.offset(), detail)
    }
}

fn set_lower(interval: &mut ReadInterval, new: Bound<Key>) -> Result<()> {
    validate_kind(interval, &new)?;
    if compare(&new, &interval.lower, true) == Ordering::Greater {
        interval.lower = new;
    }
    Ok(())
}

fn set_upper(interval: &mut ReadInterval, new: Bound<Key>) -> Result<()> {
    validate_kind(interval, &new)?;
    if compare(&new, &interval.upper, false) == Ordering::Less {
        interval.upper = new;
    }
    Ok(())
}

fn validate_kind(interval: &ReadInterval, bound: &Bound<Key>) -> Result<()> {
    if let Bound::Included(value) | Bound::Excluded(value) = bound {
        if interval
            .endpoint_keys()
            .any(|key| key.kind() != value.kind())
        {
            return Err(Error::InvalidValue(
                "mixed primary-key types in predicate".to_owned(),
            ));
        }
    }
    Ok(())
}

fn compare(a: &Bound<Key>, b: &Bound<Key>, lower: bool) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => {
            if lower {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Bound::Unbounded) => {
            if lower {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(if lower {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(if lower {
            Ordering::Greater
        } else {
            Ordering::Less
        }),
    }
}
