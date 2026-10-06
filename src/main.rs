use celastro::{server, store::Store, VERSION};
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

const HELP: &str = "\
celastro — a minimal single-node document database

USAGE:
  celastro serve [--dir DIR] [--bind ADDR] [--port N]
  celastro version
  celastro help

serve     open DIR (./data by default) and answer HTTP on ADDR:N
          (127.0.0.1:8787 by default)

ENVIRONMENT:
  CELASTRO_TOKEN   required as `Authorization: Bearer <token>` on every
                   request but /health; serving beyond loopback needs it

The API is in the README.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("serve") => match serve(&args[1..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("celastro: {e}");
                ExitCode::from(2)
            }
        },
        Some("version" | "--version" | "-V") => {
            println!("celastro {VERSION}");
            ExitCode::SUCCESS
        }
        None | Some("help" | "--help" | "-h") => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("celastro: unknown command `{other}`; `celastro help` lists them");
            ExitCode::from(2)
        }
    }
}

fn serve(args: &[String]) -> Result<(), String> {
    let mut dir = "./data".to_string();
    let mut bind: IpAddr = [127, 0, 0, 1].into();
    let mut port: u16 = 8787;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--dir" => dir = value()?.clone(),
            "--bind" => {
                bind = value()?
                    .parse()
                    .map_err(|_| "--bind: not an IP address".to_string())?
            }
            "--port" => {
                port = value()?
                    .parse()
                    .map_err(|_| "--port: not a port".to_string())?
            }
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    let token = std::env::var("CELASTRO_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if !bind.is_loopback() && token.is_none() {
        return Err(format!("serving on {bind} needs CELASTRO_TOKEN set"));
    }
    let store = Store::open(&dir).map_err(|e| e.to_string())?;
    let addr = SocketAddr::new(bind, port);
    let listener = TcpListener::bind(addr).map_err(|e| format!("{addr}: {e}"))?;
    eprintln!(
        "celastro {VERSION}: {dir} on http://{}",
        listener.local_addr().unwrap_or(addr)
    );
    server::serve(listener, Arc::new(Mutex::new(store)), token);
    Ok(())
}
