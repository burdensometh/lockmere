use std::{
    io::{self, BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use super::{encode_response, lexer::MAX_REQUEST_BYTES, Session};
use crate::{
    error::{Error, Result},
    tx::Engine,
};

const IO_TIMEOUT: Duration = Duration::from_secs(120);

pub fn run_console(engine: Arc<Engine>, input: impl BufRead, output: impl Write) -> Result<()> {
    run_connection(engine, input, output)
}

pub fn run_server(engine: Arc<Engine>, address: SocketAddr, max_connections: usize) -> Result<()> {
    if !address.ip().is_loopback() || max_connections == 0 {
        return Err(Error::InvalidRequest(
            "server requires a loopback address and a positive connection limit".to_owned(),
        ));
    }
    let listener =
        TcpListener::bind(address).map_err(|error| Error::io("bind session listener", error))?;
    let active = Arc::new(AtomicUsize::new(0));
    eprintln!(
        "lockmere listening on {}",
        listener
            .local_addr()
            .map_err(|error| Error::io("get listener address", error))?
    );
    for client in listener.incoming() {
        let mut client = match client {
            Ok(client) => client,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::io("accept connection", error)),
        };
        let permit = ConnectionPermit::acquire(active.clone(), max_connections);
        let Ok(permit) = permit else {
            let _ = client.set_write_timeout(Some(IO_TIMEOUT));
            let _ = answer(&mut client, "", Err(Error::Limit("TCP connections")));
            continue;
        };
        let engine = engine.clone();
        thread::Builder::new()
            .name("lockmere-client".to_owned())
            .spawn(move || {
                let _permit = permit;
                if let Err(error) = serve_socket(engine, client) {
                    eprintln!("lockmere session closed: {error}");
                }
            })
            .map_err(|error| Error::io("start connection worker", error))?;
    }
    Ok(())
}

fn serve_socket(engine: Arc<Engine>, socket: TcpStream) -> Result<()> {
    socket
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|error| Error::io("set connection read timeout", error))?;
    socket
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|error| Error::io("set connection write timeout", error))?;
    let input = socket
        .try_clone()
        .map_err(|error| Error::io("clone connection", error))?;
    run_connection(engine, BufReader::new(input), socket)
}

fn run_connection(
    engine: Arc<Engine>,
    mut input: impl BufRead,
    mut output: impl Write,
) -> Result<()> {
    let mut frame = Vec::new();
    if !read_frame(&mut input, &mut frame)? {
        return Ok(());
    }
    let greeting = match text(&frame).and_then(read_hello) {
        Ok(client) => client,
        Err(error) => return answer(&mut output, "", Err(error)),
    };
    let mut session = match Session::new(engine, greeting) {
        Ok(session) => session,
        Err(error) => return answer(&mut output, "", Err(error)),
    };
    output
        .write_all(b"{\"status\":\"hello\",\"protocol\":1}\n")
        .and_then(|()| output.flush())
        .map_err(|error| Error::io("write protocol greeting", error))?;

    while read_frame(&mut input, &mut frame)? {
        let request = match text(&frame).and_then(read_request) {
            Ok(request) => request,
            Err(error) => {
                answer(&mut output, "", Err(error))?;
                continue;
            }
        };
        let (id, sql) = request;
        answer(&mut output, id, session.execute(id, sql))?;
    }
    Ok(())
}

fn read_hello(input: &str) -> Result<&str> {
    let mut parts = input.split_ascii_whitespace();
    match (parts.next(), parts.next(), parts.next()) {
        (Some("HELLO"), Some(client), None) => Ok(client),
        _ => Err(Error::InvalidRequest(
            "first frame must be HELLO <client>".to_owned(),
        )),
    }
}

fn read_request(input: &str) -> Result<(&str, &str)> {
    let tail = input.strip_prefix("REQUEST ").ok_or_else(|| {
        Error::InvalidRequest("expected REQUEST <request> <statement>".to_owned())
    })?;
    let (id, sql) = tail
        .split_once(' ')
        .filter(|(id, sql)| {
            !id.is_empty() && !sql.trim().is_empty() && !id.chars().any(char::is_whitespace)
        })
        .ok_or_else(|| {
            Error::InvalidRequest("request identity or statement is missing".to_owned())
        })?;
    Ok((id, sql))
}

fn text(frame: &[u8]) -> Result<&str> {
    let text = std::str::from_utf8(frame)
        .map_err(|_| Error::InvalidRequest("invalid UTF-8 frame".to_owned()))?;
    let body = text
        .strip_suffix('\n')
        .ok_or_else(|| Error::InvalidRequest("incomplete request frame".to_owned()))?;
    Ok(body.strip_suffix('\r').unwrap_or(body))
}

fn read_frame(input: &mut impl BufRead, frame: &mut Vec<u8>) -> Result<bool> {
    frame.clear();
    loop {
        let buffer = input
            .fill_buf()
            .map_err(|error| Error::io("read session request", error))?;
        if buffer.is_empty() {
            return Ok(!frame.is_empty());
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let size = newline.map_or(buffer.len(), |position| position + 1);
        if size > MAX_REQUEST_BYTES.saturating_sub(frame.len()) {
            return Err(Error::Limit("protocol frame bytes"));
        }
        frame.extend_from_slice(&buffer[..size]);
        input.consume(size);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

fn answer(output: &mut impl Write, request: &str, response: Result<super::Response>) -> Result<()> {
    let json = encode_response(request, &response);
    output
        .write_all(json.as_bytes())
        .and_then(|()| output.write_all(b"\n"))
        .and_then(|()| output.flush())
        .map_err(|error| Error::io("write session response", error))
}

struct ConnectionPermit(Arc<AtomicUsize>);

impl ConnectionPermit {
    fn acquire(open: Arc<AtomicUsize>, max: usize) -> Result<Self> {
        open.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < max).then(|| current + 1)
        })
        .map_err(|_| Error::Limit("TCP connections"))?;
        Ok(Self(open))
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}
