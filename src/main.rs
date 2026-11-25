mod scenario;

use std::{
    env,
    fs::{self, File},
    io::{self, BufReader},
    net::SocketAddr,
    path::Path,
    process::ExitCode,
    sync::Arc,
};

use lockmere::{
    session::{read_options, read_schema, run_console, run_server},
    Engine, Error, Options, Result,
};

const HELP: &str = "\
Lockmere v1

lockmere init DATABASE SCHEMA [OPTIONS]
lockmere console DATABASE [OPTIONS]
lockmere serve DATABASE LOOPBACK:PORT [OPTIONS]
lockmere replay DATABASE SCHEDULE [OPTIONS]
lockmere demo NEW_DATABASE [OPTIONS]
lockmere help
";

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {error}", error.code());
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<()> {
    let values: Vec<&str> = args.iter().map(String::as_str).collect();
    match values.as_slice() {
        ["help"] | ["--help"] | [] => {
            print!("{HELP}");
            Ok(())
        }
        ["init", path, schema, opt @ ..] => {
            let engine = Engine::create(path, read_schema(schema)?, options(opt)?)?;
            println!("created v1 database, sequence {}", engine.sequence()?);
            Ok(())
        }
        ["console", path, opt @ ..] => {
            let engine = Arc::new(Engine::open(path, options(opt)?)?);
            run_console(engine, io::stdin().lock(), io::stdout().lock())
        }
        ["serve", path, address, opt @ ..] => {
            let address: SocketAddr = address
                .parse()
                .map_err(|_| Error::InvalidRequest("invalid loopback socket address".to_owned()))?;
            let options = options(opt)?;
            let connections = options.max_transactions;
            let engine = Arc::new(Engine::open(path, options)?);
            run_server(engine, address, connections)
        }
        ["replay", path, steps, opt @ ..] => {
            let file = File::open(steps).map_err(|error| io_error("open schedule", error))?;
            let body = BufReader::new(file);
            scenario::run(Path::new(path), options(opt)?, body, io::stdout().lock())
        }
        ["demo", path, opt @ ..] => {
            let options = options(opt)?;
            prepare_demo(Path::new(path), options.clone())?;
            let body = BufReader::new(include_bytes!("../demo/history.lm").as_slice());
            scenario::run(Path::new(path), options, body, io::stdout().lock())
        }
        _ => Err(Error::InvalidRequest(format!(
            "invalid arguments\n\n{HELP}"
        ))),
    }
}

fn prepare_demo(path: &Path, options: Options) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => return Err(Error::StoreExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("inspect demo destination", error)),
    }
    let schema = lockmere::session::parse_schema(include_str!("../config/sample.schema"))?;
    Engine::create(path, schema, options)?;
    Ok(())
}

fn options(args: &[&str]) -> Result<Options> {
    match args {
        [] => Ok(Options::default()),
        [path] => read_options(path),
        _ => Err(Error::InvalidRequest(
            "expected a single optional options file".to_owned(),
        )),
    }
}

fn io_error(action: &'static str, error: io::Error) -> Error {
    Error::Io {
        operation: action,
        message: error.to_string(),
    }
}
