use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    path::Path,
    sync::Arc,
};

use lockmere::{
    session::{encode_response, Response, Session},
    Engine, Error, Options, Result,
};

pub fn run(
    path: &Path,
    options: Options,
    input: impl BufRead,
    mut output: impl Write,
) -> Result<()> {
    let mut schedule = Schedule::new(path, options)?;
    for (index, line) in input.lines().enumerate() {
        let line = line.map_err(|error| crate::io_error("read schedule", error))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Err(error) = schedule.step(line, &mut output) {
            return Err(Error::InvalidRequest(format!(
                "schedule line {}: {error}",
                index + 1
            )));
        }
    }
    if schedule
        .clients
        .values()
        .any(|state| state.session.transaction().is_some())
    {
        return Err(Error::InvalidRequest(
            "schedule ended with an active transaction".to_owned(),
        ));
    }
    Ok(())
}

struct Schedule<'a> {
    engine: Option<Arc<Engine>>,
    path: &'a Path,
    options: Options,
    clients: BTreeMap<String, Client>,
}

struct Client {
    session: Session,
    waiting: Option<String>,
}

impl<'a> Schedule<'a> {
    fn new(path: &'a Path, options: Options) -> Result<Self> {
        let engine = Arc::new(Engine::open(path, options.clone())?);
        Ok(Self {
            engine: Some(engine),
            path,
            options,
            clients: BTreeMap::new(),
        })
    }

    fn step(&mut self, line: &str, output: &mut impl Write) -> Result<()> {
        let (verb, rest) = split(line);
        match verb {
            "DO" => {
                let (client, rest) = split(rest);
                let (request, rest) = split(rest);
                let (expected, sql) = split(rest);
                if client.is_empty() || request.is_empty() || expected.is_empty() || sql.is_empty()
                {
                    return Err(bad("use DO <client> <request> <expected> <statement>"));
                }
                let state = self.client(client)?;
                let result = state.session.start(request, sql);
                describe(output, client, request, expected, &result)?;
                if matches!(result, Ok(Response::Waiting(_))) {
                    state.waiting = Some(request.to_owned());
                } else if state.session.transaction().is_none() {
                    state.waiting = None;
                }
                Ok(())
            }
            "POLL" => {
                let (client, expected) = split(rest);
                let state = self
                    .clients
                    .get_mut(client)
                    .ok_or_else(|| bad("unknown schedule client"))?;
                let pending = state
                    .waiting
                    .as_deref()
                    .ok_or_else(|| bad("client has no parked operation"))?;
                let result = state.session.poll();
                describe(output, client, pending, expected, &result)?;
                if !matches!(result, Ok(Response::Waiting(_))) {
                    state.waiting = None;
                }
                Ok(())
            }
            "ASSERT_SEQUENCE" => {
                let number = rest
                    .parse()
                    .map_err(|_| bad("expected a sequence number"))?;
                let actual = self
                    .engine
                    .as_ref()
                    .expect("open schedule store")
                    .sequence()?;
                expect_count(output, "sequence", number, actual)
            }
            "ASSERT_EDGES" => {
                let number = rest.parse().map_err(|_| bad("expected an edge count"))?;
                let actual = self
                    .engine
                    .as_ref()
                    .expect("open schedule store")
                    .wait_graph()?
                    .edges()
                    .len() as u64;
                expect_count(output, "edges", number, actual)
            }
            "REOPEN" if rest.is_empty() => {
                if self
                    .clients
                    .values()
                    .any(|state| state.session.transaction().is_some())
                {
                    return Err(bad("reopen requires idle clients"));
                }
                self.clients.clear();
                self.reopen(output)
            }
            _ => Err(bad("unknown schedule directive")),
        }
    }

    fn client(&mut self, name: &str) -> Result<&mut Client> {
        use std::collections::btree_map::Entry;
        match self.clients.entry(name.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let session = Session::new(
                    self.engine.as_ref().expect("open schedule store").clone(),
                    name,
                )?;
                Ok(entry.insert(Client {
                    session,
                    waiting: None,
                }))
            }
        }
    }

    fn reopen(&mut self, output: &mut impl Write) -> Result<()> {
        // Drop the owning Arc, plus the sessions above, before opening the same directory.
        drop(self.engine.take());
        let engine = Engine::open(self.path, self.options.clone())?;
        let sequence = engine.sequence()?;
        self.engine = Some(Arc::new(engine));
        writeln!(output, "reopen sequence {sequence}")
            .map_err(|error| crate::io_error("write schedule reopen", error))
    }
}

fn split(input: &str) -> (&str, &str) {
    input
        .split_once(char::is_whitespace)
        .map_or((input, ""), |(first, rest)| (first, rest.trim_start()))
}

fn describe(
    out: &mut impl Write,
    client: &str,
    request: &str,
    expected: &str,
    result: &Result<Response>,
) -> Result<()> {
    let observed = match result {
        Ok(Response::Waiting(_)) => "WAITING",
        Ok(Response::Operation(lockmere::OperationResult::Committed(receipt)))
            if receipt.replayed =>
        {
            "REPLAYED"
        }
        Ok(_) => "OK",
        Err(error) => error.code(),
    };
    writeln!(out, "{client} {}", encode_response(request, result))
        .map_err(|error| crate::io_error("write schedule response", error))?;
    if observed != expected {
        return Err(bad(format!("expected {expected}, observed {observed}")));
    }
    Ok(())
}

fn expect_count(out: &mut impl Write, property: &str, expected: u64, observed: u64) -> Result<()> {
    if expected != observed {
        return Err(bad(format!(
            "expected {property} {expected}, observed {observed}"
        )));
    }
    writeln!(out, "assert {property} {observed}")
        .map_err(|error| crate::io_error("write schedule assertion", error))
}

fn bad(detail: impl Into<String>) -> Error {
    Error::InvalidRequest(detail.into())
}
