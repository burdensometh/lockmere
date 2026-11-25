use std::io::{self, BufRead};

use crate::{
    error::{Error, Result},
    rows::{Key, Row, Value},
};

pub(crate) const MAX_LINE: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CHECKPOINT: u64 = 512 * 1024 * 1024;

pub(crate) fn num<T>(text: &str) -> std::result::Result<T, String>
where
    T: std::str::FromStr + ToString,
{
    let number: T = text
        .parse()
        .map_err(|_| format!("invalid number {text:?}"))?;
    if number.to_string() != text {
        return Err(format!("noncanonical number {text:?}"));
    }
    Ok(number)
}

pub(crate) fn hex_encode(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len() * 2);
    for byte in input.bytes() {
        result.push(HEX[usize::from(byte >> 4)] as char);
        result.push(HEX[usize::from(byte & 15)] as char);
    }
    result
}

pub(crate) fn hex_decode(input: &str) -> std::result::Result<String, String> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }

    let parts = input.as_bytes().chunks_exact(2);
    if !parts.remainder().is_empty() {
        return Err("odd number of hexadecimal digits".to_owned());
    }
    let bytes = parts
        .map(|pair| {
            digit(pair[0])
                .zip(digit(pair[1]))
                .map(|(high, low)| high << 4 | low)
                .ok_or_else(|| "invalid hexadecimal text".to_owned())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    String::from_utf8(bytes).map_err(|_| "text is not UTF-8".to_owned())
}

pub(crate) fn value(value: &Value) -> String {
    match value {
        Value::Null => "N".to_owned(),
        Value::Integer(number) => format!("I{number}"),
        Value::Text(text) => format!("S{}", hex_encode(text)),
    }
}

pub(crate) fn decode_value(text: &str) -> std::result::Result<Value, String> {
    match text.as_bytes().first() {
        Some(b'N') if text.len() == 1 => Ok(Value::Null),
        Some(b'I') => num(&text[1..]).map(Value::Integer),
        Some(b'S') => hex_decode(&text[1..]).map(Value::Text),
        _ => Err(format!("unknown value tag in {text:?}")),
    }
}

pub(crate) fn key(key: &Key) -> String {
    value(&key.value())
}

pub(crate) fn decode_key(text: &str) -> std::result::Result<Key, String> {
    decode_value(text)?
        .into_key()
        .map_err(|error| error.to_string())
}

pub(crate) fn row(row: Option<&Row>) -> String {
    match row {
        None => "D".to_owned(),
        Some(values) => {
            let mut result = "U".to_owned();
            for (column, item) in values {
                result.push(',');
                result.push_str(column);
                result.push('=');
                result.push_str(&value(item));
            }
            result
        }
    }
}

pub(crate) fn decode_row(text: &str) -> std::result::Result<Option<Row>, String> {
    if text == "D" {
        return Ok(None);
    }
    let mut fields = text.split(',');
    if fields.next() != Some("U") {
        return Err("unknown row operation".to_owned());
    }
    let mut previous = "";
    let mut row = Row::new();
    for field in fields {
        let (column, item) = field.split_once('=').ok_or("missing column value")?;
        if column <= previous {
            return Err("columns must appear in strict canonical order".to_owned());
        }
        previous = column;
        row.insert(column.to_owned(), decode_value(item)?);
    }
    Ok(Some(row))
}

pub(crate) fn seal(payload: &str) -> Result<Vec<u8>> {
    let full = format!(
        "{payload}\t#{}:{:08x}\n",
        payload.len(),
        checksum(payload.as_bytes())
    )
    .into_bytes();
    if full.len() > MAX_LINE {
        return Err(Error::Limit("journal record bytes"));
    }
    Ok(full)
}

pub(crate) fn unseal(raw: &[u8]) -> std::result::Result<&str, String> {
    let text = std::str::from_utf8(raw).map_err(|_| "invalid frame UTF-8")?;
    let body = text.strip_suffix('\n').ok_or("missing line terminator")?;
    let (payload, footer) = body.rsplit_once("\t#").ok_or("missing frame footer")?;
    let (length, digest) = footer.split_once(':').ok_or("missing frame checksum")?;
    if num::<usize>(length)? != payload.len() {
        return Err("frame length mismatch".to_owned());
    }
    let calculated = checksum(payload.as_bytes());
    if digest.len() != 8
        || u32::from_str_radix(digest, 16).ok() != Some(calculated)
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("frame checksum mismatch".to_owned());
    }
    Ok(payload)
}

pub(crate) fn next_line(
    reader: &mut impl BufRead,
    target: &mut Vec<u8>,
    file: &'static str,
    offset: u64,
) -> Result<bool> {
    target.clear();
    loop {
        let data = reader
            .fill_buf()
            .map_err(|error| Error::io("read durable frame", error))?;
        if data.is_empty() {
            return Ok(!target.is_empty());
        }
        let newline = data.iter().position(|byte| *byte == b'\n');
        let length = newline.map_or(data.len(), |index| index + 1);
        if length > MAX_LINE.saturating_sub(target.len()) {
            return Err(Error::corrupt(
                file,
                offset,
                "durable frame exceeds the size limit",
            ));
        }
        target.extend_from_slice(&data[..length]);
        reader.consume(length);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

const CRC_TABLE: [u32; 256] = crc_table();

const fn crc_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut byte = 0;
    while byte < table.len() {
        let mut entry = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            entry = (entry >> 1) ^ if entry & 1 == 0 { 0 } else { 0xedb88320 };
            bit += 1;
        }
        table[byte] = entry;
        byte += 1;
    }
    table
}

fn checksum(bytes: &[u8]) -> u32 {
    !bytes.iter().fold(u32::MAX, |state, byte| {
        let index = ((state ^ u32::from(*byte)) & 0xff) as usize;
        (state >> 8) ^ CRC_TABLE[index]
    })
}

pub(crate) fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |state, byte| {
        (state ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

pub(crate) fn io_result<T>(action: &'static str, value: io::Result<T>) -> Result<T> {
    value.map_err(|error| Error::io(action, error))
}
