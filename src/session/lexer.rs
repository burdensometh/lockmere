use crate::error::{Error, Result};

pub(crate) const MAX_REQUEST_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token {
    Word(String),
    Integer(i64),
    Text(String),
    Comma,
    Left,
    Right,
    Star,
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Semicolon,
}

#[derive(Debug, Clone)]
pub(crate) struct Item {
    pub offset: usize,
    pub token: Token,
}

pub(crate) fn tokenize(input: &str) -> Result<Vec<Item>> {
    if input.len() > MAX_REQUEST_BYTES {
        return Err(Error::Limit("request bytes"));
    }
    let mut tokens = Vec::new();
    let mut cursor = input.char_indices().peekable();
    while let Some((offset, ch)) = cursor.next() {
        if ch.is_ascii_whitespace() {
            continue;
        }
        let token = match ch {
            ',' => Token::Comma,
            '(' => Token::Left,
            ')' => Token::Right,
            '*' => Token::Star,
            ';' => Token::Semicolon,
            '=' => Token::Equal,
            '<' if cursor.next_if(|(_, next)| *next == '=').is_some() => Token::LessEqual,
            '>' if cursor.next_if(|(_, next)| *next == '=').is_some() => Token::GreaterEqual,
            '<' => Token::Less,
            '>' => Token::Greater,
            '\'' => {
                let mut value = String::new();
                loop {
                    match cursor.next() {
                        Some((_, '\'')) if cursor.next_if(|(_, next)| *next == '\'').is_some() => {
                            value.push('\'')
                        }
                        Some((_, '\'')) => break,
                        Some((_, next)) => value.push(next),
                        None => return Err(syntax(offset, "unterminated string literal")),
                    }
                }
                Token::Text(value)
            }
            '-' if cursor.peek().is_some_and(|(_, next)| *next == '-') => break,
            '-' | '0'..='9' => {
                while cursor.next_if(|(_, next)| next.is_ascii_digit()).is_some() {}
                let end = cursor.peek().map_or(input.len(), |(at, _)| *at);
                let integer = input[offset..end]
                    .parse()
                    .map_err(|_| syntax(offset, "invalid signed integer"))?;
                Token::Integer(integer)
            }
            'A'..='Z' | 'a'..='z' | '_' => {
                while cursor
                    .next_if(|(_, next)| next.is_ascii_alphanumeric() || *next == '_')
                    .is_some()
                {}
                let end = cursor.peek().map_or(input.len(), |(at, _)| *at);
                Token::Word(input[offset..end].to_ascii_lowercase())
            }
            _ => return Err(syntax(offset, &format!("unexpected character {ch:?}"))),
        };
        tokens.push(Item { offset, token });
    }
    Ok(tokens)
}

pub(crate) fn syntax(offset: usize, message: &str) -> Error {
    Error::InvalidRequest(format!("at byte {}: {message}", offset + 1))
}
