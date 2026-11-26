mod support;

use std::{
    io::{BufRead, BufReader, Cursor, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use lockmere::{
    session::{encode_response, parse, run_console, Command as Sql, Response, Session},
    Error, OperationResult,
};

use support::{engine, Scratch};

#[test]
fn parser_normalizes_bounds_and_rejects_mixed_types() {
    assert!(
        matches!(parse("select * from branch where id > 1 and id <= 1;"),
        Ok(Sql::Select { interval, .. }) if interval.is_empty())
    );
    assert!(matches!(
        parse("SELECT * FROM branch WHERE id > 'x' AND id < 3"),
        Err(Error::InvalidValue(_))
    ));
    assert!(parse("INSERT INTO branch (id,id) VALUES (2,3)").is_err());
    assert!(parse("GET branch 1; SELECT * FROM branch").is_err());
    assert!(parse("UPDATE branch SET id=1 WHERE id=2").is_err());
    assert!(parse("GET badge 'not closed").is_err());
    assert!(parse("GET branch -9223372036854775808; -- signed lower bound").is_ok());
}

#[test]
fn parser_accepts_begin_priority_and_rejects_malformed_forms() {
    assert!(matches!(
        parse("BEGIN PRIORITY 7"),
        Ok(Sql::Begin {
            isolation: lockmere::Isolation::Serializable,
            priority: 7,
        })
    ));
    assert!(matches!(
        parse("BEGIN SNAPSHOT PRIORITY -3"),
        Ok(Sql::Begin {
            isolation: lockmere::Isolation::Snapshot,
            priority: -3,
        })
    ));
    assert!(matches!(
        parse("BEGIN ISOLATION LEVEL SERIALIZABLE PRIORITY 2"),
        Ok(Sql::Begin { priority: 2, .. })
    ));
    assert!(matches!(
        parse("BEGIN"),
        Ok(Sql::Begin {
            isolation: lockmere::Isolation::Serializable,
            priority: 0,
        })
    ));
    // A non-integer priority is a syntax error.
    assert!(parse("BEGIN PRIORITY high").is_err());
    assert!(parse("BEGIN PRIORITY").is_err());
}

#[test]
fn console_returns_structured_results_and_recovers_after_malformed_request() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let commands = "\
HELLO console
REQUEST begin BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
not a frame
REQUEST ins INSERT INTO branch (id, code) VALUES (1, 'someone''s \"key\"');
REQUEST commit COMMIT
REQUEST commit COMMIT
REQUEST wrong USE missing
REQUEST schema SHOW SCHEMA
";
    let mut output = Vec::new();
    run_console(db, Cursor::new(commands), &mut output).unwrap();
    let output = String::from_utf8(output).unwrap();
    let lines: Vec<_> = output.lines().collect();
    assert_eq!(lines.len(), 8);
    assert_eq!(lines[0], r#"{"status":"hello","protocol":1}"#);
    assert!(lines[2].contains(r#""code":"INVALID_REQUEST""#));
    assert!(lines[4].contains(r#""replayed":false"#));
    assert!(lines[5].contains(r#""replayed":true"#));
    assert!(lines[6].contains(r#""code":"INVALID_REQUEST""#));
    assert!(lines[7].contains(r#""name":"badge_branch""#));
}

#[test]
fn session_rollback_and_identity_are_connection_scoped() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let mut first = Session::new(db.clone(), "first").unwrap();
    let mut second = Session::new(db.clone(), "second").unwrap();
    first.execute("start", "BEGIN SNAPSHOT").unwrap();
    first
        .execute("stage", "INSERT INTO branch (id, code) VALUES (1, 'left')")
        .unwrap();
    first.execute("same", "COMMIT").unwrap();
    second.execute("start", "BEGIN SNAPSHOT").unwrap();
    assert!(
        matches!(second.execute("same", "COMMIT"), Ok(Response::Operation(OperationResult::Committed(item)))
        if item.sequence == 2 && !item.replayed)
    );
    assert_eq!(first.execute("same", "BEGIN"), Err(Error::DuplicateRequest));
    first.execute("new", "BEGIN").unwrap();
    first
        .execute(
            "stage2",
            "INSERT INTO branch (id, code) VALUES (2, 'right')",
        )
        .unwrap();
    let transaction = first.transaction().unwrap();
    drop(first);
    assert_eq!(
        db.transaction(transaction),
        Err(Error::UnknownTransaction(transaction))
    );
}

#[test]
fn json_quotes_text_values_and_preserves_retry_fields() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let mut client = Session::new(db, "client").unwrap();
    client.execute("begin", "BEGIN").unwrap();
    client
        .execute("save", "INSERT INTO branch (id,code) VALUES (1,'a\"\\z')")
        .unwrap();
    let result = client.execute("point", "GET branch 1");
    assert!(encode_response("point", &result).contains(r#""code":"a\"\\z""#));
    let error = Err(Error::Deadlock {
        victim: 1,
        cycle: vec![1, 2, 1],
    });
    assert!(
        encode_response("dead", &error).contains(r#""retryable":true,"transaction_ended":true"#)
    );
}

#[test]
fn session_deadlock_names_the_actual_victim_and_cycle() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let mut low = Session::new(db.clone(), "low").unwrap();
    let mut high = Session::new(db.clone(), "high").unwrap();
    let low_tx = low
        .execute("begin", "BEGIN SERIALIZABLE PRIORITY 1")
        .map(|response| match response {
            Response::Began(info) => info.id,
            other => panic!("expected begin, got {other:?}"),
        })
        .unwrap();
    let high_tx = high
        .execute("begin", "BEGIN SERIALIZABLE PRIORITY 9")
        .map(|response| match response {
            Response::Began(info) => info.id,
            other => panic!("expected begin, got {other:?}"),
        })
        .unwrap();
    low.execute("a", "INSERT INTO branch (id, code) VALUES (1, 'low')")
        .unwrap();
    high.execute("a", "INSERT INTO branch (id, code) VALUES (2, 'high')")
        .unwrap();
    // The low priority transaction blocks on the high holder's row.
    assert!(matches!(
        low.start("b", "INSERT INTO branch (id, code) VALUES (2, 'low')"),
        Ok(Response::Waiting(_))
    ));
    // The high priority requester closes the cycle. Only the queued low
    // transaction is the victim, so the requester proceeds immediately.
    assert!(matches!(
        high.start("b", "INSERT INTO branch (id, code) VALUES (1, 'other')"),
        Ok(Response::Operation(OperationResult::Changed { .. }))
    ));
    // The low session's parked wait reports the real victim and cycle.
    let error = low.poll().unwrap_err();
    assert_eq!(
        error,
        Error::Deadlock {
            victim: low_tx,
            cycle: vec![low_tx, high_tx, low_tx],
        }
    );
    assert_eq!(low.transaction(), None);
    assert!(matches!(
        high.execute("c", "COMMIT"),
        Ok(Response::Operation(OperationResult::Committed(_)))
    ));
}

#[test]
fn tcp_sessions_expose_real_blocking_and_release() {
    let root = Scratch::new();
    let path = root.db();
    drop(engine(&path));
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_lockmere"))
            .arg("serve")
            .arg(&path)
            .arg("127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut header = String::new();
    let mut log = BufReader::new(server.0.stderr.take().unwrap());
    log.read_line(&mut header).unwrap();
    let address: SocketAddr = header
        .strip_prefix("lockmere listening on ")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mut reader = Client::new(address, "reader");
    let mut writer = Client::new(address, "writer");
    let mut observer = Client::new(address, "observer");
    assert!(reader.call("REQUEST a BEGIN").contains(r#""status":"ok""#));
    assert!(reader
        .call("REQUEST b SELECT * FROM branch WHERE id >= 1")
        .contains(r#""rows":[]"#));
    writer.call("REQUEST a BEGIN SNAPSHOT");
    writer.send("REQUEST b INSERT INTO branch (id, code) VALUES (2, 'east')");
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    loop {
        let graph = observer.call("REQUEST check SHOW LOCKS");
        if graph.contains(r#""reason":"granted""#) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "blocked writer missing from graph"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    reader.call("REQUEST c COMMIT");
    assert!(writer.receive().contains(r#""kind":"changed""#));
    assert!(writer.call("REQUEST c COMMIT").contains(r#""sequence":2"#));
    assert!(observer
        .call("REQUEST check SHOW LOCKS")
        .contains(r#""edges":[]"#));
}

struct Client(BufReader<TcpStream>);

impl Client {
    fn new(address: SocketAddr, name: &str) -> Self {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(4)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let mut client = Self(BufReader::new(stream));
        assert!(client
            .call(&format!("HELLO {name}"))
            .contains(r#""status":"hello""#));
        client
    }

    fn send(&mut self, command: &str) {
        writeln!(self.0.get_mut(), "{command}").unwrap();
    }

    fn receive(&mut self) -> String {
        let mut response = String::new();
        assert_ne!(self.0.read_line(&mut response).unwrap(), 0);
        response
    }

    fn call(&mut self, command: &str) -> String {
        self.send(command);
        self.receive()
    }
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
