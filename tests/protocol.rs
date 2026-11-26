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
    Error, Isolation, OperationResult,
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
fn parser_accepts_read_committed_in_both_spellings() {
    assert!(matches!(
        parse("BEGIN READ COMMITTED"),
        Ok(Sql::Begin(Isolation::ReadCommitted))
    ));
    assert!(matches!(
        parse("begin transaction isolation level read committed;"),
        Ok(Sql::Begin(Isolation::ReadCommitted))
    ));
    assert!(matches!(
        parse("BEGIN READ_COMMITTED"),
        Ok(Sql::Begin(Isolation::ReadCommitted))
    ));
    assert!(parse("BEGIN READ").is_err());
    assert!(parse("BEGIN REPEATABLE READ").is_err());
}

#[test]
fn read_committed_session_reports_isolation_and_sees_intermediate_commits() {
    let root = Scratch::new();
    let db = Arc::new(engine(root.db()));
    let mut reader = Session::new(db.clone(), "reader").unwrap();
    let mut writer = Session::new(db, "writer").unwrap();
    reader.execute("begin", "BEGIN READ COMMITTED").unwrap();
    let transaction = reader.transaction().unwrap();
    let shown = reader.execute("show", "SHOW TRANSACTION").unwrap();
    assert!(encode_response("show", &Ok(shown)).contains(r#""isolation":"read_committed""#));
    writer.execute("begin", "BEGIN").unwrap();
    writer
        .execute("ins", "INSERT INTO branch (id, code) VALUES (1, 'north')")
        .unwrap();
    writer.execute("commit", "COMMIT").unwrap();
    let seen = reader.execute("get", "GET branch 1").unwrap();
    assert!(encode_response("get", &Ok(seen)).contains(r#""code":"north""#));
    assert_eq!(reader.execute("end", "ROLLBACK").unwrap(), {
        Response::RolledBack { id: transaction }
    });
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
