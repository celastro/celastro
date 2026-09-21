//! `celastro` — the command-line tool, and the local browser UI it serves.
//!
//! ```text
//! celastro serve --open                    the UI, on 127.0.0.1 only
//! celastro --dir ./data exec 'SELECT 1'    one statement, then exit
//! celastro --dir ./data run setup.sql      a script of statements
//! celastro repl                            statements on stdin
//! celastro demo                            a small hybrid corpus, end to end
//! celastro catalog                         collections and their indexes
//! ```
//!
//! The older `celastro` binary keeps the interface it documented — a REPL and a
//! script runner — and this one is additive. Nothing is shared between them but
//! the library, because a binary cannot import another binary.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use celastro::catalog::{IndexDef, IndexKind};
use celastro::codec::Rng;
use celastro::engine::{Db, DbOpts, Outcome};
use celastro::error::Result;
use celastro::json;
use celastro::lock::RwLock;
use celastro::plan::exec::{QueryResult, Row};
use celastro::serve::Server;
use celastro::tls::Tls;
use celastro::value::Value;
use std::net::{IpAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The command did what it was asked.
const EXIT_OK: i32 = 0;
/// The command ran and failed: a SQL error, a file that would not read, a save
/// that would not write.
const EXIT_FAIL: i32 = 1;
/// The command line itself was wrong. Nothing was opened and nothing ran.
const EXIT_USAGE: i32 = 2;

/// The port `serve` binds when none is given. A fixed default is what makes the
/// URL predictable enough to bookmark; `--port 0` asks the operating system for
/// a free one instead, which is what a test harness wants.
const DEFAULT_PORT: u16 = 8787;

/// How wide a table cell may get before it is clipped. A document body is
/// prose, and a column that renders all of it is not a table.
const MAX_CELL: usize = 48;

const HELP: &str = "\
celastro — hybrid document database

USAGE:
  celastro [global flags] <command> [args]

COMMANDS:
  serve [--port N] [--open]  serve the browser UI on 127.0.0.1
        [--bind ADDR]            or on ADDR, answering the token in CELASTRO_TOKEN
        [--shard-bind ADDR[:PORT]] and this node's shards to other nodes (port 2352)
  exec <SQL>                 run one statement and print the result
  run <FILE>                 run a script of statements
  repl                       interactive session on stdin
  demo                       build a small hybrid corpus and show it working
  catalog                    list collections and their indexes
  health [--port N]          exit 0 if a console is serving on 127.0.0.1:N
        [--attached N]           and has verified N other nodes since it started
  export <COLLECTION> <DIR>  copy a collection, as of now, into a new database directory
  import <DIR>               adopt a collection an export wrote into this database
  key master <FILE>          write a new master key (64 hex digits) to FILE, readable by you only
  key init <FILE>            write a new data key to FILE, wrapped under the master key: the KEY
                             every node of an encrypted cluster starts with (CELASTRO_KEY_FILE)
  key rekey <KEY> <MASTER>   rewrap the data key in KEY under the master key in the file MASTER
  key rotate <DIR>           a new data key for the database at DIR: every file sealed again,
                             KEY rewrapped; with no process serving DIR; resumable if interrupted
  check <DIR>                open every frame of every file under DIR and name what does not open
  tls init <DIR> <NAME> [<NAMES>] [<DAYS>]
                             write a CA and a certificate for NAME (and NAMES, comma-separated
                             DNS names and IP addresses) into DIR, valid DAYS days (3650)
  tls secret <SECRET> <NAME> [<NAMES>] [<DAYS>]
                             the same material as the Secret SECRET, through the cluster's API
                             from inside a pod; a Secret already there is left as it is
  send <URL> <SQL>           one statement to the console at URL (http:// or https://, the CA
                             from CELASTRO_TLS_CA), the token from CELASTRO_TOKEN; prints the
                             console's answer, exit 1 when it says ok:false
  help                       this
  version                    print the version

GLOBAL FLAGS:
  --dir <DIR>                open a persistent database (default: in memory)
  --url <URL>                talk to a console that is already serving, local or remote:
                             exec, run, repl and catalog then go over HTTP to it
  --json                     machine-readable output instead of tables

`--url http://host:8787` (or https://, verified by the CA in CELASTRO_TLS_CA)
makes `exec`, `run`, `repl` and `catalog` clients of a running console rather
than openers of a directory: every statement is sent as a request and the
answer rendered as it would be locally. The token is CELASTRO_TOKEN, or the
`?t=` of the URL `serve` printed, so that line can be pasted as it is. A
cluster's nodes all coordinate, so the URL may name any node's console or a
load balancer in front of them.

LOG: `serve` writes one line per event to stderr with a timestamp and a
level; CELASTRO_LOG=json makes them JSON lines for a collector.

TUNING: the CELASTRO_* variables in docs/tuning.md (insert batch, memtable
and residency budgets, compaction, the vector build, the console's
connection cap) are read at start; a value that does not parse is refused.

`serve` prints its URL — token included — on stdout before it starts serving,
so the line can be piped or clicked; the notes go to stderr. Under `--json` that
first line is a JSON object carrying `url`, `addr` and `token` instead, so write
`jq -r .url` when a bare URL is what you wanted. Without `--port` it binds 8787,
and `--port 0` asks the operating system for a free one.

A running server saves after every statement that changed something, and saves
again when it is asked to stop through `POST /api/shutdown`, so a closed
terminal does not cost committed writes.

A node in a cluster is started with CELASTRO_NODE=tcp://host:port, its address
in every placement map, and CELASTRO_WIRE_TOKEN, the secret every node shares;
`serve --shard-bind ADDR[:PORT]` then serves its shards to the others (2352 when
no port is given, in addresses too), and CELASTRO_ATTACH=tcp://a,tcp://b:2352
names the peers it attaches as they
come up, its own address skipped, so every node of a cluster can be given the
same list. CELASTRO_ROLE=coordinator makes a node hold no shards -- a
placement, REBALANCE and MOVE SHARD never land one on it -- and only
coordinate; it is started and attached like any other node. ATTACH NODE, CREATE COLLECTION ... WITH (nodes = [...]), MOVE SHARD,
REBALANCE and LOCAL are the statements that go with it; docs/design.md has the
rest.

`serve --bind 0.0.0.0` (or another routable address) puts the console on a
network, for nodes behind a Service or a load balancer: every node then answers
the token in CELASTRO_TOKEN -- at least sixteen printable bytes, the same at
every node -- instead of a per-run one, and any of them coordinates a statement
over every node's shards. Plain HTTP unless certificates are given:
CELASTRO_TLS_CERT, CELASTRO_TLS_KEY and CELASTRO_TLS_CA -- all three, PEM,
Ed25519 -- make the console and the wire TLS 1.3, every peer verified against
that CA by the name it dialled, and the health probe verifying its own console
as `localhost`, which the certificate has to name. `tls init` makes such a set.

`--json` covers every command, `help` and `version` included, and stdout carries
the whole answer: a failure that ends the command is written there too, as
{\"ok\":false,\"error\":...}, so a pipeline never has to read stderr to find out
what went wrong.

Encryption at rest: with CELASTRO_MASTER_KEY_FILE (a file `key master` wrote,
or 32 raw bytes) or CELASTRO_MASTER_KEY (the hex itself), `--dir` makes an
encrypted database -- every file under it, and every backup and export it
writes, framed under a data key that `<DIR>/KEY` holds wrapped under the master
-- and opens one made before; without the master an encrypted database is
refused. A cluster's nodes share one data key: `key init` writes it, and
CELASTRO_KEY_FILE names it at every node's first start. SECURITY.md has the
rest.

The `archived` tier is a local directory unless CELASTRO_ARCHIVE_ENDPOINT
(`host:port`, plain HTTP) and CELASTRO_ARCHIVE_BUCKET name an S3-compatible
store, with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for it.

Statements end with `;` or a blank line. Everything after a bare `--` is an
argument rather than a flag, which is how a statement that opens with a SQL
comment gets through.

Exit codes: 0 success, 1 a runtime or SQL error, 2 a usage error.
";

// --------------------------------------------------------------------------
// The command line
// --------------------------------------------------------------------------

/// What the command line asked for, once the arguments have been checked
/// against each other. Parsing is kept apart from doing, so that every
/// rejection below is testable without opening a database or binding a socket.
#[derive(Debug, PartialEq, Eq)]
enum Cli {
    Run {
        dir: Option<PathBuf>,
        url: Option<String>,
        json: bool,
        cmd: Cmd,
    },
    /// `--json` is carried rather than dropped: a script that asks for
    /// machine-readable output gets it here too, or it has to special-case the
    /// two commands that quietly went back to prose.
    Help {
        json: bool,
    },
    Version {
        json: bool,
    },
    /// The command line was wrong; the message says how. The caller prints it,
    /// prints the help, and exits 2.
    Usage(String),
}

#[derive(Debug, PartialEq, Eq)]
enum Cmd {
    Serve {
        port: u16,
        open: bool,
        shard_bind: Option<String>,
        bind: Option<IpAddr>,
    },
    Exec(String),
    Script(PathBuf),
    Repl,
    Demo,
    Catalog,
    Health {
        port: u16,
        attached: Option<u64>,
    },
    Export {
        collection: String,
        to: PathBuf,
    },
    Import {
        from: PathBuf,
    },
    /// `key master <FILE>`, `key init <FILE>`, `key rekey <KEY> <MASTER>`.
    KeyMaster {
        file: PathBuf,
    },
    KeyInit {
        file: PathBuf,
    },
    KeyRekey {
        key: PathBuf,
        master: PathBuf,
    },
    /// `key rotate <DIR>`: a new data key for the database at DIR, every
    /// file sealed again; `check <DIR>`: every frame of it opened.
    KeyRotate {
        dir: PathBuf,
    },
    Check {
        dir: PathBuf,
    },
    /// `tls init <DIR> <NAME> [<NAMES>] [<DAYS>]`: a CA and a certificate.
    TlsInit {
        dir: PathBuf,
        name: String,
        names: Vec<String>,
        days: i64,
    },
    /// `tls secret <SECRET> <NAME> [<NAMES>] [<DAYS>]`: the same, written as
    /// a Secret through the cluster's API from inside a pod.
    TlsSecret {
        secret: String,
        name: String,
        names: Vec<String>,
        days: i64,
    },
    /// `send <URL> <SQL>`: one statement to a running console, with the
    /// token in `CELASTRO_TOKEN`.
    Send {
        url: String,
        sql: String,
    },
}

fn parse_args<I: IntoIterator<Item = String>>(argv: I) -> Cli {
    let args: Vec<String> = argv.into_iter().collect();
    let mut dir: Option<PathBuf> = None;
    let mut url: Option<String> = None;
    let mut json = false;
    let mut port: Option<u16> = None;
    let mut open = false;
    let mut shard_bind: Option<String> = None;
    let mut bind: Option<IpAddr> = None;
    let mut attached: Option<u64> = None;
    let mut verb: Option<String> = None;
    // `-h` and `-V` are answered after the whole line is read rather than at
    // the moment they are seen, so that `celastro -h --json` honours the
    // `--json` written after them.
    let mut want_help = false;
    let mut want_version = false;
    let mut rest: Vec<String> = Vec::new();
    let mut only_positional = false;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].clone();
        i += 1;
        // A bare `--` ends the flags. A statement that begins with a SQL
        // comment, and a path that begins with a dash, both have to be sayable.
        if !only_positional && arg == "--" {
            only_positional = true;
            continue;
        }
        if only_positional || arg == "-" || !arg.starts_with('-') {
            if verb.is_none() {
                verb = Some(arg);
            } else {
                rest.push(arg);
            }
            continue;
        }
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        match name.as_str() {
            "--dir" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => dir = Some(PathBuf::from(v)),
                None => return Cli::Usage(missing_value(&name)),
            },
            "--url" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => url = Some(v),
                None => return Cli::Usage(missing_value(&name)),
            },
            "--port" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => match v.parse::<u16>() {
                    Ok(p) => port = Some(p),
                    Err(_) => return Cli::Usage(bad_port(&v)),
                },
                None => return Cli::Usage(missing_value(&name)),
            },
            "--json" => {
                if inline.is_some() {
                    return Cli::Usage("`--json` takes no value".to_string());
                }
                json = true;
            }
            "--open" => {
                if inline.is_some() {
                    return Cli::Usage("`--open` takes no value".to_string());
                }
                open = true;
            }
            "--shard-bind" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => shard_bind = Some(v),
                None => return Cli::Usage(missing_value(&name)),
            },
            "--attached" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => match v.parse::<u64>() {
                    Ok(n) => attached = Some(n),
                    Err(_) => return Cli::Usage(format!("`--attached` wants a count, not `{v}`")),
                },
                None => return Cli::Usage(missing_value(&name)),
            },
            "--bind" => match value_for(inline.as_deref(), &args, &mut i) {
                Some(v) => match v.parse::<IpAddr>() {
                    Ok(ip) => bind = Some(ip),
                    Err(_) => {
                        return Cli::Usage(format!(
                            "`--bind` wants an IP address such as 0.0.0.0, not `{v}`"
                        ))
                    }
                },
                None => return Cli::Usage(missing_value(&name)),
            },
            "-h" | "--help" => want_help = true,
            "-V" | "--version" => want_version = true,
            other => return Cli::Usage(format!("unknown flag `{other}`")),
        }
    }

    if want_help {
        return Cli::Help { json };
    }
    if want_version {
        return Cli::Version { json };
    }
    let verb = match verb {
        Some(v) => v,
        None => return Cli::Usage("no command given".to_string()),
    };
    let cmd = match verb.as_str() {
        // A stray argument is a typo or a quoting mistake, and both are better
        // said out loud than ignored.
        "serve" | "repl" | "demo" | "catalog" | "health" if !rest.is_empty() => {
            return Cli::Usage(format!("`{verb}` takes no arguments"));
        }
        "serve" => Cmd::Serve {
            port: port.unwrap_or(DEFAULT_PORT),
            open,
            shard_bind: shard_bind.clone(),
            bind,
        },
        "exec" => match rest.len() {
            1 => Cmd::Exec(rest[0].clone()),
            0 => return Cli::Usage("`exec` needs a statement to run".to_string()),
            _ => return Cli::Usage("quote the whole statement as one argument".to_string()),
        },
        "run" => match rest.len() {
            1 => Cmd::Script(PathBuf::from(&rest[0])),
            0 => return Cli::Usage("`run` needs a file to read".to_string()),
            _ => return Cli::Usage("`run` takes one file".to_string()),
        },
        "repl" => Cmd::Repl,
        "demo" => Cmd::Demo,
        "catalog" => Cmd::Catalog,
        "health" => Cmd::Health { port: port.unwrap_or(DEFAULT_PORT), attached },
        "send" => match rest.len() {
            2 => Cmd::Send { url: rest[0].clone(), sql: rest[1].clone() },
            _ => {
                return Cli::Usage(
                    "`send <URL> <SQL>`: one statement to the console at URL (http:// or https://), \
                     the token from CELASTRO_TOKEN"
                        .to_string(),
                )
            }
        },
        "tls" => match rest.first().map(String::as_str) {
            Some("secret") if rest.len() >= 3 && rest.len() <= 5 => {
                let names: Vec<String> = rest
                    .get(3)
                    .map(|s| {
                        s.split(',')
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                let days = match rest.get(4) {
                    Some(d) => match d.parse::<i64>() {
                        Ok(n) if n > 0 => n,
                        _ => {
                            return Cli::Usage(format!(
                                "`tls secret` wants a positive number of days, not `{d}`"
                            ))
                        }
                    },
                    None => 3650,
                };
                Cmd::TlsSecret { secret: rest[1].clone(), name: rest[2].clone(), names, days }
            }
            Some("init") if rest.len() >= 3 && rest.len() <= 5 => {
                let names: Vec<String> = rest
                    .get(3)
                    .map(|s| s.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect())
                    .unwrap_or_default();
                let days = match rest.get(4) {
                    Some(d) => match d.parse::<i64>() {
                        Ok(n) if n > 0 => n,
                        _ => return Cli::Usage(format!("`tls init` wants a positive number of days, not `{d}`")),
                    },
                    None => 3650,
                };
                Cmd::TlsInit { dir: PathBuf::from(&rest[1]), name: rest[2].clone(), names, days }
            }
            _ => {
                return Cli::Usage(
                    "`tls init <DIR> <NAME> [<NAMES>] [<DAYS>]`: write a CA and a certificate for NAME \
                     into DIR; NAMES is a comma-separated list of DNS names and IP addresses the \
                     certificate also carries, DAYS the validity (3650). `tls secret <SECRET> <NAME> \
                     [<NAMES>] [<DAYS>]`: the same material, written as the Secret SECRET through \
                     the cluster's API from inside a pod"
                        .to_string(),
                )
            }
        },
        "key" => match (rest.first().map(String::as_str), rest.len()) {
            (Some("master"), 2) => Cmd::KeyMaster { file: PathBuf::from(&rest[1]) },
            (Some("init"), 2) => Cmd::KeyInit { file: PathBuf::from(&rest[1]) },
            (Some("rotate"), 2) => Cmd::KeyRotate { dir: PathBuf::from(&rest[1]) },
            (Some("rekey"), 3) => {
                Cmd::KeyRekey { key: PathBuf::from(&rest[1]), master: PathBuf::from(&rest[2]) }
            }
            _ => {
                return Cli::Usage(
                    "`key master <FILE>`: write a new master key to FILE. `key init <FILE>`: write \
                     a new data key to FILE, wrapped under the master key in CELASTRO_MASTER_KEY_FILE \
                     or CELASTRO_MASTER_KEY. `key rekey <KEY> <MASTER>`: rewrap the data key in KEY \
                     under the master key in the file MASTER"
                        .to_string(),
                )
            }
        },
        "export" => match rest.len() {
            2 => Cmd::Export { collection: rest[0].clone(), to: PathBuf::from(&rest[1]) },
            _ => {
                return Cli::Usage(
                    "`export` takes a collection and a directory to write".to_string(),
                )
            }
        },
        "check" => match rest.len() {
            1 => Cmd::Check { dir: PathBuf::from(&rest[0]) },
            _ => return Cli::Usage("`check` takes the database directory".to_string()),
        },
        "import" => match rest.len() {
            1 => Cmd::Import { from: PathBuf::from(&rest[0]) },
            _ => return Cli::Usage("`import` takes the directory an export wrote".to_string()),
        },
        "help" => return Cli::Help { json },
        "version" => return Cli::Version { json },
        other => return Cli::Usage(format!("unknown command `{other}`")),
    };

    // A flag that does nothing where it was written is a mistake, not a
    // courtesy: someone who wrote `--port` expected a server to be listening.
    if !matches!(cmd, Cmd::Serve { .. } | Cmd::Health { .. }) {
        if port.is_some() {
            return Cli::Usage("`--port` only means something to `serve` and `health`".to_string());
        }
        if open {
            return Cli::Usage("`--open` only means something to `serve`".to_string());
        }
        if shard_bind.is_some() {
            return Cli::Usage("`--shard-bind` only means something to `serve`".to_string());
        }
        if bind.is_some() {
            return Cli::Usage("`--bind` only means something to `serve`".to_string());
        }
    }
    if attached.is_some() && !matches!(cmd, Cmd::Health { .. }) {
        return Cli::Usage("`--attached` only means something to `health`".to_string());
    }
    // The demo builds its own database, with build options no persistent
    // database should inherit. Accepting `--dir` alongside it would run the
    // demo in memory and leave the directory the operator named empty, with
    // nothing said about it.
    if matches!(cmd, Cmd::Demo) && dir.is_some() {
        let msg = "demo builds its own in-memory database, so it cannot be combined with --dir";
        return Cli::Usage(msg.to_string());
    }
    // `--url` is the other side of `--dir`: a client of a console that has
    // the directory open. The commands that open, serve or make files have
    // no meaning through it, and saying so beats opening an in-memory
    // database and quietly answering from that.
    if url.is_some() {
        if dir.is_some() {
            return Cli::Usage("`--dir` opens a directory and `--url` talks to a console that has one open; one or the other".to_string());
        }
        if !matches!(cmd, Cmd::Exec(_) | Cmd::Script(_) | Cmd::Repl | Cmd::Catalog) {
            return Cli::Usage("`--url` means something to `exec`, `run`, `repl` and `catalog`; the other commands run where they are".to_string());
        }
    }
    Cli::Run { dir, url, json, cmd }
}

/// The value of a flag, written either `--flag value` or `--flag=value`.
///
/// A value that is itself a flag is not a value: `--dir --json` names no
/// directory, and swallowing `--json` as one would open a database somewhere
/// nobody asked for and drop the flag on the floor. `--dir=--json` is how a
/// path that really does begin with a dash gets through.
fn value_for(inline: Option<&str>, args: &[String], i: &mut usize) -> Option<String> {
    if let Some(v) = inline {
        if v.is_empty() {
            return None;
        }
        return Some(v.to_string());
    }
    match args.get(*i) {
        Some(v) if v == "-" || !v.starts_with('-') => {
            *i += 1;
            Some(v.clone())
        }
        _ => None,
    }
}

fn missing_value(flag: &str) -> String {
    format!("`{flag}` needs a value; write `{flag}=<value>` if the value starts with a dash")
}

fn bad_port(given: &str) -> String {
    format!("`--port` must be a whole number from 0 to 65535, not `{given}`")
}

// --------------------------------------------------------------------------
// Dispatch
// --------------------------------------------------------------------------

fn main() {
    celastro::log::from_env();
    match parse_args(std::env::args().skip(1)) {
        Cli::Help { json } => print!("{}", help_output(json)),
        Cli::Version { json } => println!("{}", version_output(json)),
        Cli::Usage(msg) => {
            // Both go to stderr: `--json` promises that stdout carries the
            // output and nothing else, and a usage error produced no output.
            eprintln!("{msg}");
            eprint!("\n{HELP}");
            std::process::exit(EXIT_USAGE);
        }
        Cli::Run { dir, url, json, cmd } => std::process::exit(run(dir, url, json, cmd)),
    }
}

/// The help, as prose or as the document `--json` promised. The JSON form
/// carries the same text, because a wrapper that shells out for `--help` still
/// has to show it to somebody.
fn help_output(json: bool) -> String {
    if !json {
        return HELP.to_string();
    }
    let out = Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("help".to_string(), Value::Str(HELP.to_string())),
    ]);
    format!("{}\n", json::to_string(&out))
}

fn version_output(json: bool) -> String {
    let version = env!("CARGO_PKG_VERSION");
    if !json {
        return format!("celastro {version}");
    }
    let out = Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("version".to_string(), Value::Str(version.to_string())),
    ]);
    json::to_string(&out)
}

fn run(dir: Option<PathBuf>, url: Option<String>, json: bool, cmd: Cmd) -> i32 {
    // A client of a running console: nothing here opens a directory, and
    // the TLS material below is the server's, not the client's -- the
    // client verifies by CELASTRO_TLS_CA alone.
    if let Some(url) = url {
        let console = match Console::connect(&url) {
            Ok(c) => c,
            Err(e) => return fail(json, &e),
        };
        let mut session = Session::Remote(&console);
        return match cmd {
            Cmd::Exec(sql) => statement_on(&mut session, &sql, json),
            Cmd::Script(file) => run_script(&mut session, &file, json),
            Cmd::Repl => repl(&mut session, json),
            Cmd::Catalog => statement_on(&mut session, "SHOW CATALOG", json),
            _ => unreachable!("refused by the parser"),
        };
    }
    // Before any directory is opened: the probe asks a RUNNING console, and
    // opening its directory from a second process is the one thing the data
    // directory does not support.
    // Certificates, when the environment names them, before anything
    // listens or dials: a half-set or unreadable one stops the command here.
    // `send` needs no TLS material of its own: the CA it verifies a console by
    // is CELASTRO_TLS_CA alone, and it answers before the environment is read.
    if let Cmd::Send { url, sql } = cmd {
        return send(&url, &sql, json);
    }
    let tls = match Tls::from_env() {
        Ok(t) => t.map(Arc::new),
        Err(e) => return fail(json, &e.to_string()),
    };
    if let Cmd::Health { port, attached } = cmd {
        return health(port, attached, tls.as_ref(), json);
    }
    if let Cmd::TlsInit { dir, name, names, days } = cmd {
        return tls_init(&dir, &name, &names, days, json);
    }
    if let Cmd::KeyMaster { file } = cmd {
        return key_master(&file, json);
    }
    if let Cmd::KeyInit { file } = cmd {
        return key_init(&file, json);
    }
    if let Cmd::KeyRekey { key, master } = cmd {
        return key_rekey(&key, &master, json);
    }
    if let Cmd::KeyRotate { dir } = cmd {
        return key_rotate(&dir, json);
    }
    if let Cmd::Check { dir } = cmd {
        return check_dir(&dir, json);
    }
    if let Cmd::TlsSecret { secret, name, names, days } = cmd {
        return tls_secret(&secret, &name, &names, days, json);
    }
    let mut opts = match db_opts() {
        Ok(o) => o,
        Err(e) => return fail(json, &e),
    };
    opts.tls = tls.clone();
    let mut db = match &dir {
        Some(d) => match Db::open(d, opts) {
            Ok(db) => db,
            Err(e) => return fail(json, &format!("could not open {}: {e}", d.display())),
        },
        // `demo` never has a directory — the combination is refused above — so
        // this is the one place its build options can be applied.
        None if matches!(cmd, Cmd::Demo) => Db::with_opts(demo_opts()),
        None => Db::in_memory(),
    };
    let code = match cmd {
        Cmd::Serve { port, open, shard_bind, bind } => {
            serve(&mut db, port, open, shard_bind, bind, tls, json)
        }
        Cmd::Exec(sql) => statement(&mut db, &sql, json),
        Cmd::Script(file) => run_script(&mut Session::Local(&mut db), &file, json),
        Cmd::Repl => repl(&mut Session::Local(&mut db), json),
        Cmd::Demo => demo(&mut db, json),
        Cmd::Catalog => {
            print_catalog(&db, json);
            EXIT_OK
        }
        Cmd::Health { .. } => unreachable!("answered before the database was opened"),
        Cmd::TlsInit { .. }
        | Cmd::TlsSecret { .. }
        | Cmd::Send { .. }
        | Cmd::KeyMaster { .. }
        | Cmd::KeyInit { .. }
        | Cmd::KeyRekey { .. }
        | Cmd::KeyRotate { .. }
        | Cmd::Check { .. } => {
            unreachable!("answered before the database was opened")
        }
        Cmd::Export { collection, to } => match db.export_collection(&collection) {
            Ok(export) => match export.write_to(&to) {
                Ok(()) => {
                    ack(
                        json,
                        &format!(
                            "exported `{collection}` at ts {} to {}",
                            export.timestamp(),
                            to.display()
                        ),
                    );
                    EXIT_OK
                }
                Err(e) => fail(json, &format!("could not write {}: {e}", to.display())),
            },
            Err(e) => fail(json, &format!("could not export `{collection}`: {e}")),
        },
        Cmd::Import { from } => match db.import_collection(&from) {
            Ok(name) => {
                ack(json, &format!("imported `{name}` from {}", from.display()));
                EXIT_OK
            }
            Err(e) => fail(json, &format!("could not import {}: {e}", from.display())),
        },
    };
    // Every path that opened a directory saves before it leaves, including the
    // ones that failed: the statements that ran before the failing one are
    // writes the operator expects to find on disk. `serve` reaches here too —
    // `Server::run` returns when the console asks it to shut down — so a
    // session's writes are saved on the way out rather than left in a memtable
    // the process is about to drop.
    let saved = match &dir {
        Some(_) => persist_status(&mut db, json),
        None => EXIT_OK,
    };
    // A failure the command already reported wins over a clean save: the
    // operator needs to hear about the statement, not about the write.
    if code != EXIT_OK {
        return code;
    }
    saved
}

/// Save, and turn a failure into a non-zero exit. A discarded `persist` error
/// is a process that exits 0 having written nothing, which the script that ran
/// `celastro --dir ./data run setup.sql` cannot tell apart from success.
fn persist_status(db: &mut Db, json: bool) -> i32 {
    match db.persist() {
        Ok(()) => EXIT_OK,
        // Under `--json` the command has usually printed its result document
        // already; this second document says the result was not made durable,
        // which is the one thing a reader of the first would otherwise assume.
        Err(e) => fail(json, &format!("could not save: {e}")),
    }
}

/// Report a failure that ends the command, and carry it out as `EXIT_FAIL`.
///
/// Under `--json` this is the only output the caller will get, so it has to be
/// the error document on stdout: a pipeline that ends in `jq` cannot read
/// stderr, and an empty stdout makes a database error look like a broken tool.
fn fail(json: bool, msg: &str) -> i32 {
    let (stdout, line) = failure_report(json, msg);
    if stdout {
        println!("{line}");
    } else {
        eprintln!("{line}");
    }
    EXIT_FAIL
}

/// What a terminal failure says, and whether it belongs on stdout. Split out
/// from `fail` so the choice of stream is a value a test can assert on.
fn failure_report(json: bool, msg: &str) -> (bool, String) {
    if json {
        (true, json::to_string(&error_json(msg)))
    } else {
        (false, msg.to_string())
    }
}

// --------------------------------------------------------------------------
// serve
// --------------------------------------------------------------------------

/// One acknowledgement line, in whichever shape the run asked for.
fn ack(json: bool, message: &str) {
    if json {
        let out = Value::obj(vec![
            ("ok".to_string(), Value::Bool(true)),
            ("kind".to_string(), Value::Str("ack".into())),
            ("message".to_string(), Value::Str(message.into())),
        ]);
        println!("{}", json::to_string(&out));
    } else {
        println!("{message}");
    }
}

/// `health`: exit 0 when a console on `--port` answers that it is serving,
/// 1 otherwise. A container's liveness and readiness probe, since the image
/// has no shell and the console binds loopback.
fn health(port: u16, attached: Option<u64>, tls: Option<&Arc<Tls>>, json: bool) -> i32 {
    match celastro::serve::probe_health(port, tls) {
        Ok(true) => {
            // Readiness for a node of a cluster: serving is not enough while
            // the peers it was given have not answered it since it started,
            // because a statement it coordinates reaches shards it does not
            // hold through them. Liveness asks without `--attached`.
            if let Some(want) = attached {
                let have = celastro::serve::probe_attached(port, tls).unwrap_or(0);
                if have < want {
                    return fail(
                        json,
                        &format!("serving on 127.0.0.1:{port}, but {have} of {want} other node(s) attached so far"),
                    );
                }
            }
            if json {
                println!(r#"{{"ok":true,"kind":"health","port":{port}}}"#);
            } else {
                println!("serving on 127.0.0.1:{port}");
            }
            EXIT_OK
        }
        Ok(false) => fail(json, &format!("127.0.0.1:{port} answered, but not that it is serving")),
        Err(e) => fail(json, &format!("no console on 127.0.0.1:{port}: {e}")),
    }
}

fn serve(
    db: &mut Db,
    port: u16,
    open: bool,
    shard_bind: Option<String>,
    bind: Option<IpAddr>,
    tls: Option<Arc<Tls>>,
    json: bool,
) -> i32 {
    let connections = match max_connections() {
        Ok(n) => n,
        Err(e) => return fail(json, &e),
    };
    let auto_compact = !matches!(
        std::env::var("CELASTRO_AUTO_COMPACT")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false" | "no"
    );
    let server = match bind {
        // The operator's token, or nothing: a per-run token is printed where
        // a client on the network cannot read it, and differs per node.
        Some(ip) => {
            let Some(token) = celastro::serve::token_from_env() else {
                return fail(
                    json,
                    &format!(
                        "--bind needs {} in the environment: a console on a network answers a \
                         token you chose, the same at every node, never a per-run one",
                        celastro::serve::TOKEN_ENV
                    ),
                );
            };
            match Server::bind_network(ip, port, token) {
                Ok(s) => s
                    .with_tls(tls.clone())
                    .with_max_connections(connections)
                    .with_auto_compact(auto_compact),
                Err(e) => return fail(json, &format!("could not bind {ip}:{port}: {e}")),
            }
        }
        None => match Server::bind(port) {
            Ok(s) => s
                .with_tls(tls.clone())
                .with_max_connections(connections)
                .with_auto_compact(auto_compact),
            Err(e) => return fail(json, &format!("could not bind 127.0.0.1:{port}: {e}")),
        },
    };
    let url = server.url();
    // The URL reaches stdout before the first request is served, so it can be
    // piped or clicked. Everything else about the run is a diagnostic.
    if json {
        println!("{}", json::to_string(&serve_json(&server)));
    } else {
        println!("{url}");
    }
    let _ = io::stdout().flush();
    // Installed here and not for the REPL: see `celastro::signal`. The banner
    // says what actually stops the server, which used to be false in a
    // container, where PID 1 with a default disposition never receives the
    // interrupt the banner promised.
    let stops = if celastro::signal::install_shutdown_handlers() {
        "Ctrl-C, SIGTERM or POST /api/shutdown to stop"
    } else {
        "POST /api/shutdown to stop"
    };
    eprintln!("celastro serving on {} — {stops}", server.local_addr());
    if server.reach() == celastro::serve::Reach::Network {
        eprintln!(
            "The console is on a routable address, answering any client presenting the token \
             in {}: {}. The token is what protects this database.",
            celastro::serve::TOKEN_ENV,
            if server.is_tls() {
                "over TLS, with the certificate the environment named"
            } else {
                "this is plain HTTP, for a network you trust or an ingress that terminates TLS \
                 in front of it"
            }
        );
    } else {
        eprintln!(
            "The token in that URL is the only thing protecting this database. Anyone who can\n\
             read this terminal, this process's environment or its command line can use it, and\n\
             the server answers every request that carries it. Treat the URL as a password, and\n\
             stop the server when you are done."
        );
    }
    if open {
        // Not fatal: the URL is already printed, so a desktop without an opener
        // costs a copy and paste rather than the session.
        if let Err(e) = open_in_browser(&url) {
            eprintln!("could not open a browser ({e}) — open the URL above yourself");
        }
    }
    // `run` returns when the console asks the server to shut down. The caller
    // saves after it returns, which is what makes a browser session's writes
    // outlive the process.
    // The console and the wire share the database under one lock; the wire
    // is served from its own thread, stopped when the console stops.
    let shared = Arc::new(RwLock::new(std::mem::replace(db, Db::in_memory())));
    let stop = Arc::new(AtomicBool::new(false));
    if let Some(bind) = shard_bind {
        let Some(token) = celastro::wire::token_from_env() else {
            return fail(
                json,
                &format!("--shard-bind needs {} in the environment", celastro::wire::TOKEN_ENV),
            );
        };
        let bind = celastro::wire::with_default_port(&bind);
        let listener = match TcpListener::bind(&bind) {
            Ok(l) => l,
            Err(e) => return fail(json, &format!("could not bind {bind} for the wire: {e}")),
        };
        eprintln!(
            "celastro serving shards on {} to any node presenting the wire token; {}",
            listener.local_addr().map(|a| a.to_string()).unwrap_or(bind),
            if tls.is_some() {
                "over TLS, peers verified against the CA the environment named"
            } else {
                "this is plain TCP, for a network you trust"
            }
        );
        let (wire_db, wire_stop, wire_tls) = (shared.clone(), stop.clone(), tls.clone());
        std::thread::spawn(move || {
            if let Err(e) = celastro::wire::serve(listener, wire_db, token, wire_stop, wire_tls) {
                eprintln!("celastro: the wire stopped: {e}");
            }
        });
        // CELASTRO_ATTACH: the peers this node attaches as they come up.
        // What a StatefulSet's pods are given -- every pod's address, each
        // skipping its own -- so that a cluster assembles itself and a pod
        // that restarts re-attaches without an operator. ATTACH NODE is
        // idempotent, and a peer that is not answering yet is retried
        // until it does or the node stops.
        let peers: Vec<String> = std::env::var(ATTACH_ENV)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        if !peers.is_empty() {
            let (db, stop) = (shared.clone(), stop.clone());
            std::thread::spawn(move || attach_peers(&db, &stop, &peers));
        }
    }
    let outcome = server.run(&shared);
    stop.store(true, Ordering::Relaxed);
    // Whatever stopped the console, the last writes are on the disk before
    // the process ends.
    if let Err(e) = shared.write().unwrap_or_else(|p| p.into_inner()).persist() {
        eprintln!("celastro: could not save at exit: {e}");
    }
    match outcome {
        Ok(()) => EXIT_OK,
        Err(e) => fail(json, &format!("serving stopped: {e}")),
    }
}

/// The environment variable naming the peers `serve` attaches: a
/// comma-separated list of `tcp://host:port`.
const ATTACH_ENV: &str = "CELASTRO_ATTACH";

/// Attach every peer, retrying each until it answers or the node stops. A
/// peer that is this node's own address is skipped, so the same list can be
/// handed to every member of a cluster.
fn attach_peers(db: &RwLock<Db>, stop: &AtomicBool, peers: &[String]) {
    let (me, tls) = {
        let g = db.read().unwrap_or_else(|p| p.into_inner());
        (g.node().map(str::to_string), g.tls())
    };
    let token = celastro::wire::token_from_env();
    let mut pending: Vec<&String> = peers.iter().filter(|p| me.as_ref() != Some(*p)).collect();
    let mut attempt = 0u32;
    while !pending.is_empty() && !stop.load(Ordering::Relaxed) {
        attempt += 1;
        let mut still = Vec::new();
        for url in pending {
            // Dial first, without the database lock: a peer that is not up
            // yet costs this thread its connect timeout and nobody else's.
            // Dialling under the lock held every statement and every probe
            // behind it for as long as the peers took to start, and a pod
            // failed its liveness probe on its own health that way.
            // And the catalog too, so nothing is dialled under the lock: a
            // peer that vanished between the dial and the attach -- a
            // rolling restart -- held every statement for a deadline.
            let dialled = celastro::wire::Node::new(url, token.as_deref(), tls.clone())
                .and_then(|n| n.hello().map(|h| (h, n.catalog().ok())));
            let r = dialled.and_then(|(hello, theirs)| {
                db.write().unwrap_or_else(|p| p.into_inner()).attach_prepared(url, &hello, theirs)
            });
            match r {
                Ok(_) => eprintln!("celastro: attached {url}"),
                Err(e) => {
                    if attempt == 1 || attempt % 30 == 0 {
                        eprintln!("celastro: {url} not attached yet ({e}); retrying");
                    }
                    still.push(url);
                }
            }
        }
        pending = still;
        if !pending.is_empty() {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
}

/// `send`: one statement to a console, the way the chart's backup CronJob
/// runs `BACKUP TO` on each pod. The answer is the console's JSON, printed
/// as it came (`--json`) or as its message; the exit code follows `ok`.
/// There is no timeout on the read: a backup answers when it is done.
/// A running console, as a client sees it: where it is, how it is verified,
/// and the token every request carries. `--url` and `send` both talk
/// through this.
struct Console {
    url: String,
    addr: String,
    host: String,
    /// The CA to verify an `https://` console by; `None` for plain HTTP.
    ca: Option<String>,
    token: String,
}

impl Console {
    /// From the URL alone: `http://host[:port]` or `https://`, the port 8787
    /// when absent, the token from `CELASTRO_TOKEN` or from the URL's `?t=`
    /// -- the line `serve` printed pastes as it is -- and for https the CA
    /// from `CELASTRO_TLS_CA`. Nothing is sent yet.
    fn connect(url: &str) -> std::result::Result<Console, String> {
        let (https, rest) = match (url.strip_prefix("https://"), url.strip_prefix("http://")) {
            (Some(r), _) => (true, r),
            (_, Some(r)) => (false, r),
            _ => return Err(format!("`{url}` is not an http:// or https:// URL")),
        };
        let (path_part, query) = rest.split_once('?').unwrap_or((rest, ""));
        let host_port = path_part.split('/').next().unwrap_or("").trim_end_matches('/');
        if host_port.is_empty() {
            return Err(format!("`{url}` names no host"));
        }
        let host = host_port.rsplit_once(':').map(|(h, _)| h).unwrap_or(host_port).to_string();
        let addr = if host_port.contains(':') {
            host_port.to_string()
        } else {
            format!("{host_port}:{DEFAULT_PORT}")
        };
        let from_url = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("t="))
            .filter(|t| !t.is_empty())
            .map(str::to_string);
        let token = match std::env::var("CELASTRO_TOKEN").ok().filter(|t| !t.is_empty()) {
            Some(t) => t,
            None => match from_url {
                Some(t) => t,
                None => {
                    return Err("no token: set CELASTRO_TOKEN, or give the URL `serve` printed, \
                                which carries it as ?t="
                        .into())
                }
            },
        };
        let ca = if https {
            match std::env::var("CELASTRO_TLS_CA").ok().map(std::fs::read_to_string) {
                Some(Ok(ca)) => Some(ca),
                Some(Err(e)) => return Err(format!("CELASTRO_TLS_CA: {e}")),
                None => {
                    return Err(
                        "an https:// console needs CELASTRO_TLS_CA, the CA to verify it by".into()
                    )
                }
            }
        } else {
            None
        };
        let url = format!("{}://{addr}", if https { "https" } else { "http" });
        Ok(Console { url, addr, host, ca, token })
    }

    /// One statement to `/api/query`: the HTTP status and the body.
    fn query(&self, sql: &str) -> std::result::Result<(u16, String), String> {
        let body =
            json::to_string(&Value::obj(vec![("sql".to_string(), Value::Str(sql.to_string()))]));
        let headers =
            [("X-Celastro-Token", self.token.as_str()), ("Content-Type", "application/json")];
        let req = celastro::tls::HttpRequest {
            method: "POST",
            path: "/api/query",
            headers: &headers,
            body: Some(&body),
        };
        let answer = match &self.ca {
            Some(ca) => {
                celastro::tls::https_request(&self.addr, &self.host, ca, &req, Duration::ZERO)
            }
            None => celastro::tls::http_request(&self.addr, &self.host, &req, Duration::ZERO),
        };
        answer.map_err(|e| format!("reaching {}: {e}", self.url))
    }

    /// One statement, as the console's JSON document and the exit code it
    /// earns; a transport failure is an error document like any other.
    fn statement(&self, sql: &str) -> (Value, i32) {
        match self.query(sql) {
            Ok((status, text)) => {
                let doc = json::parse(&text).unwrap_or_else(|_| {
                    error_json(&format!(
                        "the console answered HTTP {status} with something that is not JSON: {}",
                        text.trim()
                    ))
                });
                let ok = doc.get("ok").and_then(Value::as_bool).unwrap_or(false);
                (doc, if ok && (200..300).contains(&status) { EXIT_OK } else { EXIT_FAIL })
            }
            Err(e) => (error_json(&e), EXIT_FAIL),
        }
    }
}

/// A console's answer rendered as the local path renders the same outcome:
/// rows as a table with the count, the missing shards and the cuts; an
/// acknowledgement as its line; a plan or a recall report as its text; a
/// refusal as `error:` on stderr.
fn print_answer(doc: &Value) {
    let ok = doc.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !ok {
        let e = doc.get("error").and_then(Value::as_str).unwrap_or("the console refused");
        eprintln!("error: {e}");
        return;
    }
    let text = |k: &str| doc.get(k).and_then(Value::as_str).map(str::to_string);
    match doc.get("kind").and_then(Value::as_str).unwrap_or("") {
        "rows" => {
            let strings = |k: &str| -> Vec<String> {
                doc.get(k)
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                    .unwrap_or_default()
            };
            let rows = doc
                .get("rows")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|r| celastro::plan::exec::Row {
                            key: r.get("key").and_then(Value::as_str).unwrap_or("").to_string(),
                            doc: r.get("doc").cloned().unwrap_or(Value::Null),
                            score: r.get("score").and_then(Value::as_f64).map(|f| f as f32),
                            distance: r.get("distance").and_then(Value::as_f64).map(|f| f as f32),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let mut r = QueryResult::of_rows(rows);
            r.missing = strings("missing");
            r.truncated_prefixes = strings("truncated_prefixes");
            r.cut_walks = strings("cut_walks");
            r.next_cursor = text("next_cursor");
            print_rows(&r);
        }
        "ack" => println!("{}", text("message").unwrap_or_default()),
        "explain" | "recall" => print!("{}", text("text").unwrap_or_default()),
        _ => println!("{}", json::to_string(doc)),
    }
    if let Some(ms) = doc.get("elapsed_ms").and_then(Value::as_f64) {
        println!("({ms:.0} ms at the console)");
    }
}

fn send(url: &str, sql: &str, json: bool) -> i32 {
    let console = match Console::connect(url) {
        Ok(c) => c,
        Err(e) => return fail(json, &e),
    };
    let (status, text) = match console.query(sql) {
        Ok(a) => a,
        Err(e) => return fail(json, &e),
    };
    let parsed = json::parse(&text).ok();
    let ok = parsed.as_ref().and_then(|v| v.get("ok")).and_then(Value::as_bool).unwrap_or(false);
    if json {
        println!("{}", text.trim());
    } else {
        let message = parsed
            .as_ref()
            .and_then(|v| v.get("message").or_else(|| v.get("error")).or_else(|| v.get("text")))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| text.trim().to_string());
        if ok {
            println!("{message}");
        } else {
            eprintln!("error: {message} (HTTP {status})");
        }
    }
    if ok && (200..300).contains(&status) {
        EXIT_OK
    } else {
        EXIT_FAIL
    }
}

/// `tls init`: a self-signed CA and a certificate it signed, as the four PEM
/// files `serve` reads through `CELASTRO_TLS_CERT`, `_KEY` and `_CA` (the
/// CA's key is written beside them for a later certificate and is not read
/// by anything). The certificate names `name`, every entry of `names`, and
/// `localhost` with 127.0.0.1, which the health probe needs.
/// The names a certificate for `name` carries: `name`, `names` as DNS names
/// or IP addresses, and `localhost` with 127.0.0.1 for the health probe.
fn certificate_names(name: &str, names: &[String]) -> (Vec<String>, Vec<std::net::IpAddr>) {
    let mut dns: Vec<String> = vec![name.to_string(), "localhost".to_string()];
    let mut ips: Vec<std::net::IpAddr> = vec!["127.0.0.1".parse().expect("a literal")];
    for n in names {
        match n.parse::<std::net::IpAddr>() {
            Ok(ip) => {
                if !ips.contains(&ip) {
                    ips.push(ip);
                }
            }
            Err(_) => {
                let lower = n.to_ascii_lowercase();
                if !dns.contains(&lower) {
                    dns.push(lower);
                }
            }
        }
    }
    (dns, ips)
}

/// `tls secret`: what the chart's Job runs. The pod's service account (its
/// token, the cluster's CA and the namespace, under
/// `/var/run/secrets/kubernetes.io/serviceaccount`) reaches the API at
/// `KUBERNETES_SERVICE_HOST:KUBERNETES_SERVICE_PORT`, verified as
/// `kubernetes.default.svc` over this crate's TLS -- which is what the
/// cluster's RSA or P-256 certificate is verified for. A Secret already
/// present is kept, so an upgrade keeps the material the pods hold.
fn tls_secret(secret: &str, name: &str, names: &[String], days: i64, json: bool) -> i32 {
    const SA: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
    let read = |f: &str| std::fs::read_to_string(format!("{SA}/{f}"));
    let (token, ca, namespace) = match (read("token"), read("ca.crt"), read("namespace")) {
        (Ok(t), Ok(c), Ok(n)) => (t.trim().to_string(), c, n.trim().to_string()),
        _ => {
            return fail(
                json,
                &format!("no service account under {SA}; `tls secret` runs inside a pod"),
            )
        }
    };
    let host = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default();
    let port: u16 =
        std::env::var("KUBERNETES_SERVICE_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(443);
    if host.is_empty() {
        return fail(json, "KUBERNETES_SERVICE_HOST is not set; `tls secret` runs inside a pod");
    }
    let addr = format!("{host}:{port}");
    let path = format!("/api/v1/namespaces/{namespace}/secrets");
    let bearer = format!("Bearer {token}");
    let api = |method: &str, path: &str, body: Option<&str>| {
        let req = celastro::tls::HttpRequest {
            method,
            path,
            headers: &[("Authorization", bearer.as_str())],
            body,
        };
        celastro::tls::https_request(
            &addr,
            "kubernetes.default.svc",
            &ca,
            &req,
            Duration::from_secs(30),
        )
    };
    let name_json = json::to_string(&Value::Str(secret.to_string()));
    match api("GET", &format!("{path}/{secret}"), None) {
        Ok((200, _)) => {
            if json {
                println!(r#"{{"ok":true,"kind":"tls","secret":{name_json},"created":false}}"#);
            } else {
                println!("the Secret {namespace}/{secret} is already there; leaving it as it is");
            }
            return EXIT_OK;
        }
        Ok((404, _)) => {}
        Ok((status, body)) => {
            return fail(
                json,
                &format!("reading the Secret: the API answered {status}: {}", body.trim()),
            )
        }
        Err(e) => return fail(json, &format!("reaching the API at {addr}: {e}")),
    }
    let (dns, ips) = certificate_names(name, names);
    let material = match celastro::tls::make_material(name, &dns, &ips, days) {
        Ok(m) => m,
        Err(e) => return fail(json, &format!("could not make the certificates: {e}")),
    };
    let b64 = |s: &str| celastro::tls::base64(s.as_bytes());
    let body = format!(
        r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":{name_json}}},"type":"kubernetes.io/tls","data":{{"tls.crt":"{}","tls.key":"{}","ca.crt":"{}"}}}}"#,
        b64(&material.cert),
        b64(&material.key),
        b64(&material.ca_cert)
    );
    match api("POST", &path, Some(&body)) {
        Ok((201, _)) | Ok((409, _)) => {
            if json {
                println!(
                    r#"{{"ok":true,"kind":"tls","secret":{name_json},"created":true,"names":{}}}"#,
                    json::to_string(&Value::Array(
                        dns.iter().map(|n| Value::Str(n.clone())).collect()
                    ))
                );
            } else {
                println!(
                    "wrote the Secret {namespace}/{secret}: a certificate for {} valid {days} days, \
                     signed by a CA of its own",
                    dns.join(", ")
                );
            }
            EXIT_OK
        }
        Ok((status, body)) => {
            fail(json, &format!("writing the Secret: the API answered {status}: {}", body.trim()))
        }
        Err(e) => fail(json, &format!("reaching the API at {addr}: {e}")),
    }
}

/// `key master <FILE>`: 32 random bytes as hex, written readable by the
/// owner only, never over a file that is there.
fn key_master(file: &Path, json: bool) -> i32 {
    if file.exists() {
        return fail(json, &format!("{} exists; not overwriting a key", file.display()));
    }
    let key = match celastro::cipher::new_master_hex() {
        Ok(k) => k,
        Err(e) => return fail(json, &format!("could not draw a key: {e}")),
    };
    if let Err(e) = write_private(file, &format!("{key}\n"), true) {
        return fail(json, &format!("could not write {}: {e}", file.display()));
    }
    ack(json, &format!("wrote a master key to {}", file.display()));
    EXIT_OK
}

/// `key init <FILE>`: a new data key wrapped under the environment's master
/// key -- what every node of a cluster is given as `CELASTRO_KEY_FILE`, so
/// they all write under one key.
fn key_init(file: &Path, json: bool) -> i32 {
    if file.exists() {
        return fail(json, &format!("{} exists; not overwriting a key", file.display()));
    }
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let master =
        match celastro::cipher::master_from_env(&var) {
            Ok(Some(m)) => m,
            Ok(None) => return fail(
                json,
                "`key init` wraps the data key under the master key: set CELASTRO_MASTER_KEY_FILE \
                 or CELASTRO_MASTER_KEY",
            ),
            Err(e) => return fail(json, &e.to_string()),
        };
    let wrapped = match celastro::cipher::Cipher::generate().and_then(|c| c.wrap(&master)) {
        Ok(w) => w,
        Err(e) => return fail(json, &format!("could not make the key: {e}")),
    };
    if let Err(e) = std::fs::write(file, &wrapped) {
        return fail(json, &format!("could not write {}: {e}", file.display()));
    }
    ack(json, &format!("wrote a data key, wrapped under the master key, to {}", file.display()));
    EXIT_OK
}

/// `key rekey <KEY> <MASTER>`: the data key in KEY, opened under the
/// environment's master key, rewrapped under the one in the file MASTER
/// and written back in place. The data is untouched -- it is under the
/// data key, which does not change -- so a master key rotates in the time
/// it takes to write one small file, and every node then starts with the
/// new master.
fn key_rekey(key: &Path, master: &Path, json: bool) -> i32 {
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let old = match celastro::cipher::master_from_env(&var) {
        Ok(Some(m)) => m,
        Ok(None) => {
            return fail(
                json,
                "`key rekey` opens the key under the current master: set \
                 CELASTRO_MASTER_KEY_FILE or CELASTRO_MASTER_KEY",
            )
        }
        Err(e) => return fail(json, &e.to_string()),
    };
    let new = match std::fs::read(master)
        .map_err(|e| e.to_string())
        .and_then(|b| celastro::cipher::parse_master(&b).map_err(|e| e.to_string()))
    {
        Ok(m) => m,
        Err(e) => return fail(json, &format!("{}: {e}", master.display())),
    };
    if let Err(e) = celastro::cipher::rekey_file(key, &old, &new) {
        return fail(json, &format!("could not rekey {}: {e}", key.display()));
    }
    ack(
        json,
        &format!("{} is now wrapped under the master key in {}", key.display(), master.display()),
    );
    EXIT_OK
}

/// The master key the environment names, or the reason there is none.
fn master_or_fail(json: bool, what: &str) -> std::result::Result<[u8; 32], i32> {
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    match celastro::cipher::master_from_env(&var) {
        Ok(Some(m)) => Ok(m),
        Ok(None) => Err(fail(
            json,
            &format!("`{what}` opens the database's KEY under its master: set CELASTRO_MASTER_KEY_FILE or CELASTRO_MASTER_KEY"),
        )),
        Err(e) => Err(fail(json, &e.to_string())),
    }
}

/// `key rotate <DIR>`: every file under DIR sealed again under a fresh data
/// key, then KEY rewrapped; with no process serving DIR.
fn key_rotate(dir: &Path, json: bool) -> i32 {
    let master = match master_or_fail(json, "key rotate") {
        Ok(m) => m,
        Err(code) => return code,
    };
    match celastro::cipher::rotate_data_key(dir, &master) {
        Ok(w) => {
            ack(
                json,
                &format!(
                    "{}: {} file(s) and {} log record(s) sealed under a new data key{}; KEY rewrapped",
                    dir.display(),
                    w.files,
                    w.records,
                    if w.already > 0 {
                        format!(" ({} file(s) were under it already: a rotation resumed)", w.already)
                    } else {
                        String::new()
                    }
                ),
            );
            EXIT_OK
        }
        Err(e) => fail(
            json,
            &format!(
                "could not rotate {}: {e} (run `key rotate` again to finish a rotation this interrupted)",
                dir.display()
            ),
        ),
    }
}

/// `check <DIR>`: every frame of every file under DIR opened under its data
/// key, and every file that does not open named.
fn check_dir(dir: &Path, json: bool) -> i32 {
    let master = match master_or_fail(json, "check") {
        Ok(m) => m,
        Err(code) => return code,
    };
    let wrapped = match std::fs::read(dir.join("KEY")) {
        Ok(b) => b,
        Err(e) => {
            return fail(
                json,
                &format!("{}/KEY: {e} (an encrypted database has one)", dir.display()),
            )
        }
    };
    let cipher = match celastro::cipher::Cipher::unwrap(&wrapped, &master) {
        Ok(c) => c,
        Err(e) => return fail(json, &e.to_string()),
    };
    match celastro::cipher::check_dir(dir, &cipher) {
        Ok(w) if w.failures.is_empty() => {
            ack(
                json,
                &format!(
                    "{}: {} file(s) and {} log record(s) open under the data key; nothing is damaged",
                    dir.display(),
                    w.files,
                    w.records
                ),
            );
            EXIT_OK
        }
        Ok(w) => fail(
            json,
            &format!(
                "{}: {} file(s) open, {} do(es) not:\n  {}",
                dir.display(),
                w.files,
                w.failures.len(),
                w.failures.join("\n  ")
            ),
        ),
        Err(e) => fail(json, &format!("could not check {}: {e}", dir.display())),
    }
}

fn tls_init(dir: &Path, name: &str, names: &[String], days: i64, json: bool) -> i32 {
    let (dns, ips) = certificate_names(name, names);
    let material = match celastro::tls::make_material(name, &dns, &ips, days) {
        Ok(m) => m,
        Err(e) => return fail(json, &format!("could not make the certificates: {e}")),
    };
    if let Err(e) = std::fs::create_dir_all(dir) {
        return fail(json, &format!("could not create {}: {e}", dir.display()));
    }
    for (file, text, private) in [
        ("ca.crt", &material.ca_cert, false),
        ("ca.key", &material.ca_key, true),
        ("tls.crt", &material.cert, false),
        ("tls.key", &material.key, true),
    ] {
        let path = dir.join(file);
        if path.exists() {
            return fail(
                json,
                &format!("{} exists; not overwriting a key or certificate", path.display()),
            );
        }
        if let Err(e) = write_private(&path, text, private) {
            return fail(json, &format!("could not write {}: {e}", path.display()));
        }
    }
    if json {
        println!(
            r#"{{"ok":true,"kind":"tls","dir":{},"names":{},"days":{days}}}"#,
            json::to_string(&Value::Str(dir.display().to_string())),
            json::to_string(&Value::Array(dns.iter().map(|n| Value::Str(n.clone())).collect()))
        );
    } else {
        println!(
            "wrote ca.crt, ca.key, tls.crt and tls.key into {}: a certificate for {} valid {days} days, \
             signed by a CA of its own",
            dir.display(),
            dns.join(", ")
        );
    }
    EXIT_OK
}

/// A file that only its owner may read, when it is a key.
fn write_private(path: &Path, text: &str, private: bool) -> std::io::Result<()> {
    use std::io::Write;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(if private { 0o600 } else { 0o644 });
    }
    let mut f = o.open(path)?;
    f.write_all(text.as_bytes())
}

fn serve_json(server: &Server) -> Value {
    Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("url".to_string(), Value::Str(server.url())),
        ("addr".to_string(), Value::Str(server.local_addr().to_string())),
        ("token".to_string(), Value::Str(server.token().to_string())),
    ])
}

/// Hand the URL to whatever this desktop uses to open one.
fn open_in_browser(url: &str) -> io::Result<()> {
    let mut cmd = if cfg!(target_os = "macos") {
        Command::new("open")
    } else if cfg!(target_os = "windows") {
        Command::new("cmd")
    } else {
        Command::new("xdg-open")
    };
    if cfg!(target_os = "windows") {
        // `start` reads a lone quoted argument as the window title, so the
        // empty one is what keeps the URL from being taken for a title.
        cmd.args(["/C", "start", "", url]);
    } else {
        cmd.arg(url);
    }
    // The opener's own chatter must not land in the middle of the server's
    // output, and it is not waited on: `xdg-open` outlives this call by design.
    cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    cmd.spawn()?;
    Ok(())
}

// --------------------------------------------------------------------------
// exec, run, repl
// --------------------------------------------------------------------------

/// Run one statement and render it. A SQL error is a result rather than a
/// crash: it is reported, and the exit code carries it to the caller.
fn statement(db: &mut Db, sql: &str, json: bool) -> i32 {
    let t0 = Instant::now();
    let outcome = db.execute(sql).and_then(Outcome::finished);
    let took = t0.elapsed();
    match outcome {
        Ok(o) => {
            if json {
                println!("{}", json::to_string(&outcome_json(&o, took)));
            } else {
                print_outcome(&o, took);
            }
            EXIT_OK
        }
        Err(e) => {
            if json {
                println!("{}", json::to_string(&error_json(&e.to_string())));
            } else {
                eprintln!("error: {e}");
            }
            EXIT_FAIL
        }
    }
}

/// Where a session's statements go: a database this process opened, or a
/// console another process is serving, reached over HTTP. `exec`, `run`
/// and `repl` are the same loop over either.
enum Session<'a> {
    Local(&'a mut Db),
    Remote(&'a Console),
}

/// One statement on the session, printed as the run asked, its exit code.
fn statement_on(s: &mut Session<'_>, sql: &str, json: bool) -> i32 {
    match s {
        Session::Local(db) => statement(db, sql, json),
        Session::Remote(c) => {
            let (doc, code) = c.statement(sql);
            if json {
                println!("{}", json::to_string(&doc));
            } else {
                print_answer(&doc);
            }
            code
        }
    }
}

/// One statement's result as the `--json` document: what the local path
/// prints, or what the console answered, which is the same shape.
fn result_json(s: &mut Session<'_>, sql: &str) -> (Value, i32) {
    match s {
        Session::Local(db) => {
            let t0 = Instant::now();
            let outcome = db.execute(sql).and_then(Outcome::finished);
            let took = t0.elapsed();
            match outcome {
                Ok(o) => (outcome_json(&o, took), EXIT_OK),
                Err(e) => (error_json(&e.to_string()), EXIT_FAIL),
            }
        }
        Session::Remote(c) => c.statement(sql),
    }
}

fn run_script(s: &mut Session<'_>, file: &Path, json: bool) -> i32 {
    let text = match std::fs::read_to_string(file) {
        Ok(t) => t,
        Err(e) => return fail(json, &format!("could not read {}: {e}", file.display())),
    };
    let stmts = split_statements(&text);
    if json {
        return script_json(s, &stmts);
    }
    for stmt in &stmts {
        // Stop at the first failure: the statements after it were written
        // expecting the one before to have happened.
        if statement_on(s, stmt, false) != EXIT_OK {
            return EXIT_FAIL;
        }
    }
    EXIT_OK
}

/// A script is a stream of statements, but `--json` promises a single document
/// on stdout — so the results are collected and printed once, at the end.
fn script_json(s: &mut Session<'_>, stmts: &[String]) -> i32 {
    let mut results: Vec<Value> = Vec::new();
    let mut code = EXIT_OK;
    for stmt in stmts {
        let (doc, c) = result_json(s, stmt);
        results.push(doc);
        if c != EXIT_OK {
            code = EXIT_FAIL;
            break;
        }
    }
    let out = Value::obj(vec![
        ("ok".to_string(), Value::Bool(code == EXIT_OK)),
        ("kind".to_string(), Value::Str("script".to_string())),
        ("results".to_string(), Value::Array(results)),
    ]);
    println!("{}", json::to_string(&out));
    code
}

fn repl(s: &mut Session<'_>, json: bool) -> i32 {
    let stdin = io::stdin();
    let mut buf = String::new();
    let mut code = EXIT_OK;
    if !json {
        println!("celastro — SQL, BM25 and vector search in one query plan.");
        println!("`\\h` for help, `exit` to leave. Statements end with `;` or a blank line.\n");
    }
    loop {
        // In `--json` the session is a stream of JSON documents, one per
        // statement, and a prompt written among them would spoil every one.
        if !json {
            print!("{}", if buf.trim().is_empty() { "celastro> " } else { "     -> " });
            let _ = io::stdout().flush();
        }
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {e}");
                code = EXIT_FAIL;
                break;
            }
        }
        let trimmed = line.trim().to_string();
        let fresh = buf.trim().is_empty();
        if fresh && matches!(trimmed.as_str(), "exit" | "quit" | "\\q") {
            break;
        }
        if fresh && trimmed == "\\h" {
            // The help is a diagnostic even when it was asked for, so it does
            // not get mixed into the results on stdout.
            eprint!("{HELP}");
            continue;
        }
        buf.push_str(&line);
        let ready = buf.trim_end().ends_with(';') || (trimmed.is_empty() && !fresh);
        if !ready {
            continue;
        }
        let taken = std::mem::take(&mut buf);
        let stmt = taken.trim().trim_end_matches(';');
        if stmt.trim().is_empty() {
            continue;
        }
        // A failed statement does not end the session — the operator is right
        // there — but it does decide the exit code, so a piped script that hit
        // an error is not reported afterwards as a clean run.
        if statement_on(s, stmt, json) != EXIT_OK {
            code = EXIT_FAIL;
        }
    }
    code
}

/// Split a script on `;`, respecting string literals — a semicolon inside one
/// is data, and doubled quotes escape a quote inside it.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_string && chars.peek() == Some(&'\'') {
                cur.push(c);
                if let Some(next) = chars.next() {
                    cur.push(next);
                }
                continue;
            }
            in_string = !in_string;
        }
        if c == ';' && !in_string {
            if !cur.trim().is_empty() {
                out.push(cur.trim().to_string());
            }
            cur.clear();
            continue;
        }
        cur.push(c);
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

// --------------------------------------------------------------------------
// catalog
// --------------------------------------------------------------------------

fn print_catalog(db: &Db, json: bool) {
    if json {
        println!("{}", json::to_string(&catalog_json(db)));
        return;
    }
    if db.catalog.collections.is_empty() {
        println!("no collections yet");
        return;
    }
    for (name, c) in &db.catalog.collections {
        println!("{name}  {} document(s)", c.doc_count);
        println!("  primary key   {}", c.primary_key);
        match &c.partition_key {
            Some(p) => println!("  partition by  {p}"),
            None => println!("  partition by  (none)"),
        }
        if c.indexes.is_empty() {
            println!("  indexes       (none)");
        }
        for idx in &c.indexes {
            let kind = index_kind(&idx.kind);
            println!("  index {} on {} — {kind}, tier {}", idx.name, idx.path, idx.tier.name());
        }
    }
}

/// The document `GET /api/catalog` returns, field for field. `catalog --json`
/// and the browser console are two readers of one shape; a script that learned
/// it from either must not have to learn it again from the other.
fn catalog_json(db: &Db) -> Value {
    let mut colls: Vec<Value> = Vec::new();
    for c in db.catalog.collections.values() {
        let indexes: Vec<Value> = c.indexes.iter().map(index_json).collect();
        // The inferred paths, as the API sends them: a console that offers
        // field names reads these, and so does anyone scripting against it.
        let paths: Vec<Value> = c.paths.keys().map(|p| Value::Str(p.clone())).collect();
        let partition = match &c.partition_key {
            Some(p) => Value::Str(p.clone()),
            None => Value::Null,
        };
        colls.push(Value::obj(vec![
            ("name".to_string(), Value::Str(c.name.clone())),
            ("primary_key".to_string(), Value::Str(c.primary_key.clone())),
            ("partition_key".to_string(), partition),
            ("doc_count".to_string(), Value::Int(c.doc_count as i64)),
            ("indexes".to_string(), Value::Array(indexes)),
            ("paths".to_string(), Value::Array(paths)),
        ]));
    }
    Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("collections".to_string(), Value::Array(colls)),
    ])
}

/// The four fields `/api/catalog` sends per index, and no others: an extra
/// field here is a shape the console does not have, which is how the two
/// renderings of one catalog started to drift in the first place.
fn index_json(idx: &IndexDef) -> Value {
    Value::obj(vec![
        ("name".to_string(), Value::Str(idx.name.clone())),
        ("path".to_string(), Value::Str(idx.path.clone())),
        ("kind".to_string(), Value::Str(kind_name(&idx.kind).to_string())),
        ("tier".to_string(), Value::Str(idx.tier.name().to_string())),
    ])
}

fn kind_name(kind: &IndexKind) -> &'static str {
    match kind {
        IndexKind::FullText { .. } => "fulltext",
        IndexKind::Vector { .. } => "vector",
        IndexKind::Secondary => "secondary",
        IndexKind::Adjacency { .. } => "adjacency",
    }
}

fn index_kind(kind: &IndexKind) -> String {
    match kind {
        IndexKind::FullText { analyzer } => format!("fulltext, analyzer {analyzer}"),
        IndexKind::Vector { dims, metric } => format!("vector, {dims} dims, {}", metric.name()),
        IndexKind::Secondary => "secondary".to_string(),
        IndexKind::Adjacency { to } => format!("adjacency, reads {to}"),
    }
}

// --------------------------------------------------------------------------
// Rendering: tables for people, JSON for programs
// --------------------------------------------------------------------------

/// Elapsed time as a table prints it. The JSON envelopes carry whole
/// milliseconds instead — see `elapsed_ms` — because that is what the HTTP API
/// emits and what the browser console is written against.
fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `elapsed_ms`, spelled as `GET`/`POST /api/query` spells it: whole
/// milliseconds, an integer, so that `typeof res.elapsed_ms` and any `==`
/// against it mean the same thing whichever side produced the document.
fn elapsed_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn print_outcome(o: &Outcome, d: Duration) {
    match o {
        Outcome::Ack(m) => println!("{m}"),
        Outcome::Explain(t) => print!("{t}"),
        Outcome::Recall(r) => print!("{}", r.render()),
        Outcome::Rows(r) => print_rows(r),
        Outcome::Deferred(_) => unreachable!("finished before it is printed"),
    }
    println!("({:.2} ms)", millis(d));
}

fn print_rows(r: &QueryResult) {
    let stdout = io::stdout();
    render_rows(r, &mut stdout.lock());
}

/// The whole of what a query prints, into `out`, so that a test can pin
/// where each part lands and not only what each part says. A write that
/// fails is a closed pipe; the row count has nowhere to go and neither has
/// the complaint.
fn render_rows(r: &QueryResult, out: &mut dyn Write) {
    if r.rows.is_empty() {
        let _ = writeln!(out, "(no rows)");
    } else {
        write_table(&r.rows, out);
    }
    if !r.missing.is_empty() {
        let _ = writeln!(out, "PARTIAL RESULTS — missing: {:?}", r.missing);
    }
    // Between the table and the count: a reader who stops at the count has
    // read the complaint, and a reader who scans for the count finds it where
    // it always is.
    let _ = write!(out, "{}", truncation_report(r));
    let _ = writeln!(out, "{} row(s)", r.rows.len());
    if let Some(c) = &r.next_cursor {
        let _ = writeln!(out, "next cursor: {}", c.replace('\u{1}', "/"));
    }
}

/// The `TRUNCATED —` lines a cut query prints, as one block of text.
///
/// Returned rather than printed, because this shell is one of the surfaces the
/// design notes promise reports a cut prefix expansion and a `println!` inside
/// `print_rows` is reachable from no test at all: `rows_json` below and the
/// HTTP server's copy of it are pinned, and the line a person reading the
/// table sees was not. Empty when nothing was cut, so the caller prints it
/// unconditionally.
///
/// The `celastro` binary carries its own copy, for the reason this file's
/// header gives: a binary cannot import another binary, and one private
/// rendering detail is not a reason to grow the library's public API.
fn truncation_report(r: &QueryResult) -> String {
    let mut out = String::new();
    for t in &r.truncated_prefixes {
        out.push_str("TRUNCATED — ");
        out.push_str(t);
        out.push('\n');
    }
    // A walk a cap bound is the third sibling of `missing`, rendered in the
    // same block for the same reason: the table is short and says so.
    for t in &r.cut_walks {
        out.push_str("CUT — ");
        out.push_str(t);
        out.push('\n');
    }
    out
}

/// A column of the rendered table. `key`, `score` and `distance` belong to the
/// row rather than to the document, and a document field of the same name is a
/// different thing — so the columns are told apart by construction rather than
/// by their headings.
enum Col {
    Key,
    Score,
    Distance,
    Field(String),
}

impl Col {
    fn header(&self) -> &str {
        match self {
            Col::Key => "key",
            Col::Score => "score",
            Col::Distance => "distance",
            Col::Field(f) => f.as_str(),
        }
    }

    fn cell(&self, row: &Row) -> String {
        match self {
            Col::Key => flatten(&row.key.replace('\u{1}', "/")),
            Col::Score => match row.score {
                Some(s) => format!("{s:.6}"),
                None => String::new(),
            },
            Col::Distance => match row.distance {
                Some(d) => format!("{d:.6}"),
                None => String::new(),
            },
            Col::Field(f) => match row.doc.get(f) {
                Some(v) => cell_text(v),
                None => String::new(),
            },
        }
    }
}

fn columns(rows: &[Row]) -> Vec<Col> {
    let mut cols = vec![Col::Key];
    if rows.iter().any(|r| r.score.is_some()) {
        cols.push(Col::Score);
    }
    if rows.iter().any(|r| r.distance.is_some()) {
        cols.push(Col::Distance);
    }
    // Every field any row carries gets a column; a row without it leaves the
    // cell empty, which is what a projection over polymorphic documents looks
    // like and should not be hidden.
    let mut seen: Vec<String> = Vec::new();
    for row in rows {
        if let Value::Object(fields) = &row.doc {
            for (k, _) in fields {
                if !seen.iter().any(|s| s == k) {
                    seen.push(k.clone());
                }
            }
        }
    }
    for name in seen {
        cols.push(Col::Field(name));
    }
    cols
}

fn write_table(rows: &[Row], out: &mut dyn Write) {
    let cols = columns(rows);
    let header: Vec<String> = cols.iter().map(|c| c.header().to_string()).collect();
    let mut lines: Vec<Vec<String>> = vec![header];
    for row in rows {
        lines.push(cols.iter().map(|c| c.cell(row)).collect());
    }
    let mut width = vec![0usize; cols.len()];
    for line in &lines {
        for (i, cell) in line.iter().enumerate() {
            width[i] = width[i].max(cell.chars().count());
        }
    }
    // The rule is drawn from the measured widths rather than a fixed string, so
    // it lines up with whatever the widest cell turned out to be.
    let rule: Vec<String> = width.iter().map(|w| "-".repeat(*w)).collect();
    let _ = writeln!(out, "{}", pad_join(&lines[0], &width));
    let _ = writeln!(out, "{}", rule.join("-+-"));
    for line in &lines[1..] {
        let _ = writeln!(out, "{}", pad_join(line, &width));
    }
}

fn pad_join(line: &[String], width: &[usize]) -> String {
    let mut out = String::new();
    for (i, cell) in line.iter().enumerate() {
        if i > 0 {
            out.push_str(" | ");
        }
        out.push_str(cell);
        out.push_str(&" ".repeat(width[i].saturating_sub(cell.chars().count())));
    }
    // Padding on the last column is trailing whitespace nobody wants in a paste
    // or a diff.
    out.trim_end().to_string()
}

fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Str(s) => flatten(s),
        other => flatten(&json::to_string(other)),
    }
}

/// A cell is one line by definition, so control characters are folded to spaces
/// before the widths are measured — otherwise the columns are computed from
/// text that does not appear where they claim it does.
fn flatten(s: &str) -> String {
    let folded: String = s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    clip(&folded)
}

fn clip(s: &str) -> String {
    if s.chars().count() <= MAX_CELL {
        return s.to_string();
    }
    let head: String = s.chars().take(MAX_CELL - 1).collect();
    format!("{head}…")
}

/// The envelopes below are spelled exactly as the browser UI reads them over
/// HTTP, so that a script and the UI see one shape rather than two.
fn outcome_json(o: &Outcome, d: Duration) -> Value {
    match o {
        Outcome::Ack(m) => tagged_json("ack", "message", m),
        Outcome::Explain(t) => tagged_json("explain", "text", t),
        Outcome::Recall(r) => tagged_json("recall", "text", &r.render()),
        Outcome::Rows(r) => rows_json(r, d),
        Outcome::Deferred(_) => unreachable!("finished before it is rendered"),
    }
}

fn tagged_json(kind: &str, field: &str, body: &str) -> Value {
    Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("kind".to_string(), Value::Str(kind.to_string())),
        (field.to_string(), Value::Str(body.to_string())),
    ])
}

fn error_json(msg: &str) -> Value {
    Value::obj(vec![
        ("ok".to_string(), Value::Bool(false)),
        ("error".to_string(), Value::Str(msg.to_string())),
    ])
}

fn rows_json(r: &QueryResult, d: Duration) -> Value {
    let rows: Vec<Value> = r.rows.iter().map(row_json).collect();
    let missing: Vec<Value> = r.missing.iter().map(|m| Value::Str(m.clone())).collect();
    let truncated: Vec<Value> =
        r.truncated_prefixes.iter().map(|t| Value::Str(t.clone())).collect();
    let cut_walks: Vec<Value> = r.cut_walks.iter().map(|t| Value::Str(t.clone())).collect();
    let cursor = match &r.next_cursor {
        Some(c) => Value::Str(c.clone()),
        None => Value::Null,
    };
    Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("kind".to_string(), Value::Str("rows".to_string())),
        ("count".to_string(), Value::Int(r.rows.len() as i64)),
        ("elapsed_ms".to_string(), Value::Int(elapsed_ms(d))),
        ("missing".to_string(), Value::Array(missing)),
        ("truncated_prefixes".to_string(), Value::Array(truncated)),
        ("cut_walks".to_string(), Value::Array(cut_walks)),
        ("next_cursor".to_string(), cursor),
        ("rows".to_string(), Value::Array(rows)),
    ])
}

fn row_json(row: &Row) -> Value {
    // The key keeps its `\u{1}` separator here, escaped by the JSON writer: the
    // table replaces it with `/` to be read, but a program is given the key it
    // would have to hand back.
    Value::obj(vec![
        ("key".to_string(), Value::Str(row.key.clone())),
        ("score".to_string(), measure_json(row.score)),
        ("distance".to_string(), measure_json(row.distance)),
        ("doc".to_string(), row.doc.clone()),
    ])
}

/// A score or a distance, written as the HTTP API writes it: the shortest
/// decimal that round-trips the `f32`, so the same row read from the console
/// and from a pipe does not disagree about its seventh digit. Non-finite is
/// `null` there — JSON has no `NaN` — and is `null` here for the same reason.
fn measure_json(v: Option<f32>) -> Value {
    match v {
        Some(f) if f.is_finite() => {
            // `f32::to_string` is the shortest decimal that reads back as this
            // same `f32`, which is the number the server sends. Parsing that
            // decimal keeps it; `f as f64` would widen the binary value and
            // print `0.30000001192092896` where the server printed `0.3`.
            let shortest = f.to_string().parse::<f64>().unwrap_or(f as f64);
            Value::Float(shortest)
        }
        _ => Value::Null,
    }
}

// --------------------------------------------------------------------------
// demo
// --------------------------------------------------------------------------

const DEMO_TOPICS: &[(&str, &str)] = &[
    ("vector", "approximate nearest neighbour search over quantized codes"),
    ("lexical", "block max wand postings and the bm25 saturation curve"),
    ("hybrid", "reciprocal rank fusion over lexical and vector candidates"),
    ("storage", "immutable segments compaction and the delete log"),
    ("planner", "runtime strategy selection on measured selectivity"),
];

/// Build options the demo needs and a persistent database must not inherit: a
/// low flat-tier threshold so that a few hundred documents actually reach the
/// HNSW tier, and every vector query logged so that the recall harness replays
/// real queries rather than synthesising friendlier ones.
/// The options a persistent database opens with. The `archived` tier's
/// object store is configured from the environment, because a container
/// has nowhere else to say it: `CELASTRO_ARCHIVE_ENDPOINT` (`host:port`,
/// plain HTTP), `CELASTRO_ARCHIVE_BUCKET`, and optionally
/// `CELASTRO_ARCHIVE_PREFIX` and `CELASTRO_ARCHIVE_REGION`. The credentials
/// are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, read by the library.
fn db_opts() -> std::result::Result<DbOpts, String> {
    let mut o = DbOpts::default();
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    tuning_into(&mut o, &var)?;
    o.node = var("CELASTRO_NODE");
    if let Some(v) = var("CELASTRO_REPLICATION") {
        o.replication_sync = match v.trim().to_ascii_lowercase().as_str() {
            "sync" => true,
            "async" => false,
            other => return Err(format!("CELASTRO_REPLICATION: `{other}` is not sync or async")),
        };
    }
    o.steward = var("CELASTRO_STEWARD");
    o.stewards = var("CELASTRO_STEWARDS")
        .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());
    o.region = var("CELASTRO_REGION").filter(|r| !r.trim().is_empty());
    if o.region.is_none() {
        // A list picked by the node's ordinal (`celastro-2.celastro` is the
        // third): what a StatefulSet's identical environment can carry.
        if let (Some(list), Some(node)) = (var("CELASTRO_REGIONS"), &o.node) {
            let host = node.trim_start_matches("tcp://").split([':', '.']).next().unwrap_or("");
            let ordinal = host.rsplit('-').next().and_then(|n| n.parse::<usize>().ok());
            let regions: Vec<&str> = list.split(',').map(str::trim).collect();
            if let Some(r) = ordinal.and_then(|i| regions.get(i)) {
                if !r.is_empty() {
                    o.region = Some(r.to_string());
                }
            }
        }
    }
    if let Some(v) = var("CELASTRO_AUTO_FAILOVER") {
        o.auto_failover = match v.trim().to_ascii_lowercase().as_str() {
            "on" | "1" | "true" | "yes" => true,
            "off" | "0" | "false" | "no" => false,
            other => return Err(format!("CELASTRO_AUTO_FAILOVER: `{other}` is not on or off")),
        };
    }
    if let Some(v) = var("CELASTRO_LEASE_SECS") {
        o.lease_secs =
            v.trim().parse().map_err(|_| format!("CELASTRO_LEASE_SECS: `{v}` is not a number"))?;
    }
    if let Some(v) = var("CELASTRO_REPLACE_SECS") {
        o.replace_secs = v
            .trim()
            .parse()
            .map_err(|_| format!("CELASTRO_REPLACE_SECS: `{v}` is not a number"))?;
    }
    if let Some(v) = var("CELASTRO_ROLE") {
        o.role = celastro::engine::Role::parse(&v)
            .ok_or_else(|| format!("CELASTRO_ROLE: `{v}` is not data or coordinator"))?;
    }
    if let Some(v) = var("CELASTRO_CLOCK_OFFSET_MICROS") {
        let off: i64 = v
            .parse()
            .map_err(|_| format!("CELASTRO_CLOCK_OFFSET_MICROS: `{v}` is not a number"))?;
        celastro::time::set_clock_offset(off);
        eprintln!(
            "celastro: DRILL: the clock is offset by {off} us; not for a database anyone relies on"
        );
    }
    if let Some(v) = var("CELASTRO_CATALOG_FORMAT") {
        let f: u8 =
            v.parse().map_err(|_| format!("CELASTRO_CATALOG_FORMAT: `{v}` is not a number"))?;
        celastro::catalog::pin_format(f).map_err(|e| format!("CELASTRO_CATALOG_FORMAT: {e}"))?;
    }
    if let Some(endpoint) = var("CELASTRO_ARCHIVE_ENDPOINT") {
        o.archive.endpoint = Some(endpoint);
        o.archive.bucket = var("CELASTRO_ARCHIVE_BUCKET").unwrap_or_default();
        o.archive.prefix = var("CELASTRO_ARCHIVE_PREFIX").unwrap_or_default();
        o.archive.region = var("CELASTRO_ARCHIVE_REGION").unwrap_or_default();
        o.archive.ca = var("CELASTRO_ARCHIVE_CA").map(PathBuf::from);
    }
    o.archive.dir = var("CELASTRO_ARCHIVE_DIR").map(PathBuf::from);
    o.backup_dir = var("CELASTRO_BACKUP_DIR").map(PathBuf::from);
    o.master_key =
        celastro::cipher::master_from_env(&var).map_err(|e| e.to_string())?.map(Into::into);
    o.key_file = var("CELASTRO_KEY_FILE").map(PathBuf::from);
    if o.key_file.is_some() && o.master_key.is_none() {
        return Err("CELASTRO_KEY_FILE needs the master key that wraps it: set \
                    CELASTRO_MASTER_KEY_FILE or CELASTRO_MASTER_KEY"
            .into());
    }
    Ok(o)
}

/// The performance tunables, from the environment: each `CELASTRO_*` below
/// overrides one field of the options, and a value that does not parse is
/// refused with its name rather than silently defaulted. `docs/tuning.md`
/// says what each does and when to change it.
fn tuning_into(
    o: &mut DbOpts,
    var: &dyn Fn(&str) -> Option<String>,
) -> std::result::Result<(), String> {
    fn num<T: std::str::FromStr>(name: &str, v: &str) -> std::result::Result<T, String> {
        v.trim().parse::<T>().map_err(|_| format!("{name}: `{v}` is not a number"))
    }
    /// Bytes, with an optional K, M or G (binary) suffix: `64M`, `4G`.
    fn bytes(name: &str, v: &str) -> std::result::Result<usize, String> {
        let t = v.trim();
        let (digits, mult) = match t.chars().last().map(|c| c.to_ascii_uppercase()) {
            Some('K') => (&t[..t.len() - 1], 1usize << 10),
            Some('M') => (&t[..t.len() - 1], 1usize << 20),
            Some('G') => (&t[..t.len() - 1], 1usize << 30),
            _ => (t, 1),
        };
        let n: usize = num(name, digits)?;
        n.checked_mul(mult).ok_or_else(|| format!("{name}: `{v}` is too large"))
    }
    if let Some(v) = var("CELASTRO_INSERT_BATCH") {
        let n: usize = num("CELASTRO_INSERT_BATCH", &v)?;
        if n == 0 {
            return Err("CELASTRO_INSERT_BATCH: at least 1".into());
        }
        o.insert_batch = n;
    }
    if let Some(v) = var("CELASTRO_MEMTABLE_MAX_BYTES") {
        o.thresholds.max_bytes = bytes("CELASTRO_MEMTABLE_MAX_BYTES", &v)?;
    }
    if let Some(v) = var("CELASTRO_MEMTABLE_MAX_VECTORS") {
        o.thresholds.max_vectors = num("CELASTRO_MEMTABLE_MAX_VECTORS", &v)?;
    }
    if let Some(v) = var("CELASTRO_MEMTABLE_MAX_VERSIONS") {
        o.thresholds.max_versions = num("CELASTRO_MEMTABLE_MAX_VERSIONS", &v)?;
    }
    if let Some(v) = var("CELASTRO_MEMTABLE_BUDGET_BYTES") {
        o.memtable_budget_bytes = bytes("CELASTRO_MEMTABLE_BUDGET_BYTES", &v)?;
    }
    if let Some(v) = var("CELASTRO_RESIDENCY_BUDGET_BYTES") {
        o.residency.budget_bytes = bytes("CELASTRO_RESIDENCY_BUDGET_BYTES", &v)?;
    }
    if let Some(v) = var("CELASTRO_CACHED_IDLE_UNLOAD_SECS") {
        o.residency.cached_idle_unload =
            Duration::from_secs(num("CELASTRO_CACHED_IDLE_UNLOAD_SECS", &v)?);
    }
    if let Some(v) = var("CELASTRO_ARCHIVED_IDLE_UNLOAD_SECS") {
        o.residency.archived_idle_unload =
            Duration::from_secs(num("CELASTRO_ARCHIVED_IDLE_UNLOAD_SECS", &v)?);
    }
    if let Some(v) = var("CELASTRO_ARCHIVED_ACCESS") {
        o.residency.archived_access = match v.trim().to_ascii_lowercase().as_str() {
            "fault-in" | "fault_in" | "faultin" => celastro::residency::ArchivedAccess::FaultIn,
            "refuse" => celastro::residency::ArchivedAccess::Refuse,
            other => {
                return Err(format!(
                    "CELASTRO_ARCHIVED_ACCESS: `{other}` is not fault-in or refuse"
                ))
            }
        };
    }
    if let Some(v) = var("CELASTRO_STATEMENT_DEADLINE_MS") {
        let n: u64 = num("CELASTRO_STATEMENT_DEADLINE_MS", &v)?;
        o.statement_deadline_ms = if n == 0 { None } else { Some(n) };
    }
    if let Some(v) = var("CELASTRO_RECALL_SAMPLE_RATE") {
        o.recall_sample_rate = num("CELASTRO_RECALL_SAMPLE_RATE", &v)?;
    }
    if let Some(v) = var("CELASTRO_LIFECYCLE_INTERVAL_WRITES") {
        o.lifecycle_interval_writes = num("CELASTRO_LIFECYCLE_INTERVAL_WRITES", &v)?;
    }
    if let Some(v) = var("CELASTRO_COMPACTION_TIER_FANOUT") {
        let n: usize = num("CELASTRO_COMPACTION_TIER_FANOUT", &v)?;
        if n < 2 {
            return Err("CELASTRO_COMPACTION_TIER_FANOUT: at least 2".into());
        }
        o.compaction.tier_fanout = n;
    }
    if let Some(v) = var("CELASTRO_COMPACTION_SEGMENT_CAP") {
        o.compaction.segment_cap = num("CELASTRO_COMPACTION_SEGMENT_CAP", &v)?;
    }
    if let Some(v) = var("CELASTRO_COMPACTION_DEBT") {
        o.compaction.debt_segments = num("CELASTRO_COMPACTION_DEBT", &v)?;
    }
    if let Some(v) = var("CELASTRO_COMPACTION_DEBT_WAIT_MS") {
        o.compaction.debt_wait_ms = num("CELASTRO_COMPACTION_DEBT_WAIT_MS", &v)?;
    }
    if let Some(v) = var("CELASTRO_COMPACTION_DEAD_RATIO") {
        let r: f64 = num("CELASTRO_COMPACTION_DEAD_RATIO", &v)?;
        if !(0.0..=1.0).contains(&r) {
            return Err("CELASTRO_COMPACTION_DEAD_RATIO: between 0 and 1".into());
        }
        o.compaction.dead_ratio = r;
    }
    if let Some(v) = var("CELASTRO_VECTOR_QUANTIZER") {
        o.build.quantizer = match v.trim().to_ascii_lowercase().as_str() {
            "sq8" => celastro::vector::quant::Quantizer::Sq8,
            "one-bit" | "onebit" | "1bit" => celastro::vector::quant::Quantizer::OneBit,
            other => {
                return Err(format!("CELASTRO_VECTOR_QUANTIZER: `{other}` is not sq8 or one-bit"))
            }
        };
    }
    if let Some(v) = var("CELASTRO_HNSW_M") {
        o.build.hnsw.m = num("CELASTRO_HNSW_M", &v)?;
    }
    if let Some(v) = var("CELASTRO_HNSW_M0") {
        o.build.hnsw.m0 = num("CELASTRO_HNSW_M0", &v)?;
    }
    if let Some(v) = var("CELASTRO_HNSW_EF_CONSTRUCTION") {
        o.build.hnsw.ef_construction = num("CELASTRO_HNSW_EF_CONSTRUCTION", &v)?;
    }
    if let Some(v) = var("CELASTRO_FLAT_TIER_MAX") {
        o.build.flat_tier_max = num("CELASTRO_FLAT_TIER_MAX", &v)?;
    }
    Ok(())
}

/// `CELASTRO_MAX_CONNECTIONS`, or the server's default.
fn max_connections() -> std::result::Result<usize, String> {
    match std::env::var("CELASTRO_MAX_CONNECTIONS").ok().filter(|v| !v.is_empty()) {
        None => Ok(celastro::serve::MAX_CONNECTIONS),
        Some(v) => match v.trim().parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => Err(format!("CELASTRO_MAX_CONNECTIONS: `{v}` is not a count of at least 1")),
        },
    }
}

fn demo_opts() -> DbOpts {
    let mut opts = DbOpts::default();
    opts.build.flat_tier_max = 64;
    opts.recall_sample_rate = 1;
    opts
}

fn demo(db: &mut Db, json: bool) -> i32 {
    let mut steps: Vec<Value> = Vec::new();
    match demo_body(db, json, &mut steps) {
        Ok(()) => {
            if json {
                let done = Value::obj(vec![
                    ("ok".to_string(), Value::Bool(true)),
                    ("kind".to_string(), Value::Str("demo".to_string())),
                    ("steps".to_string(), Value::Array(steps)),
                ]);
                println!("{}", json::to_string(&done));
            }
            EXIT_OK
        }
        Err(e) => {
            if json {
                println!("{}", json::to_string(&error_json(&e.to_string())));
            } else {
                eprintln!("demo failed: {e}");
            }
            EXIT_FAIL
        }
    }
}

fn demo_body(db: &mut Db, json: bool, out: &mut Vec<Value>) -> Result<()> {
    let dims = 16usize;
    let docs = 400usize;
    if !json {
        println!("celastro demo — building a small hybrid corpus.");
    }
    db.execute(
        "CREATE COLLECTION notes (
           id TEXT PRIMARY KEY,
           tenant_id TEXT NOT NULL,
           topic TEXT,
           published_at TIMESTAMP
         ) PARTITION BY (tenant_id) WITH (splits = ['t1', 't2'])",
    )?;
    db.execute(
        "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
    )?;
    db.execute(&format!(
        "CREATE INDEX notes_emb ON notes USING vector (embedding) \
         WITH (dims = {dims}, metric = 'cosine')"
    ))?;

    // Five topic centroids; each document is a centroid plus noise, so that a
    // vector neighbourhood means something.
    let mut rng = Rng::new(20260910);
    let centroids: Vec<Vec<f32>> =
        (0..DEMO_TOPICS.len()).map(|_| (0..dims).map(|_| rng.next_normal()).collect()).collect();
    for i in 0..docs {
        db.insert("notes", demo_doc(i, dims, &centroids, &mut rng))?;
    }
    if !json {
        println!("{docs} documents written across 3 shards.");
    }

    // A query vector near the "hybrid" centroid.
    let qv: Vec<String> = centroids[2].iter().map(|x| format!("{:.6}", x + 0.05)).collect();
    let qlit = format!("[{}]", qv.join(","));
    let nearest = format!(
        "SELECT id, topic FROM notes WHERE tenant_id = 't1' \
         ORDER BY embedding <=> {qlit} LIMIT 5"
    );
    let hybrid = format!(
        "EXPLAIN ANALYZE SELECT id, topic FROM notes \
         WHERE tenant_id = 't1' AND ANY(tags) = 'starred' \
         ORDER BY hybrid(text_match(body, 'fusion candidates'), embedding <=> {qlit}, \
         method => 'rrf') LIMIT 5"
    );
    let phrase = "SELECT id, topic FROM notes WHERE text_match(body, '\"delete log\"') LIMIT 5";
    let recall = "MEASURE RECALL ON notes WITH (k = 10, samples = 24)";

    demo_step(db, json, out, "Fresh writes are searchable before any flush", &nearest)?;
    demo_step(db, json, out, "Sealing the memtable into a segment", "FLUSH notes")?;
    demo_step(db, json, out, "What is on disk now", "SHOW SEGMENTS notes")?;
    demo_step(db, json, out, "The same query, over sealed segments", &nearest)?;
    demo_step(db, json, out, "Filter, text and vector in one plan", &hybrid)?;
    demo_step(db, json, out, "text_match in WHERE is a must, not a should", phrase)?;
    demo_step(db, json, out, "Recall, measured rather than assumed", recall)?;
    demo_step(db, json, out, "Merging segments", "COMPACT notes")?;
    demo_step(db, json, out, "The same recall, after compaction", recall)?;
    demo_step(db, json, out, "What the catalog inferred", "SHOW CATALOG notes")?;
    demo_step(db, json, out, "Where each index lives", "SHOW RESIDENCY notes")?;
    Ok(())
}

fn demo_doc(i: usize, dims: usize, centroids: &[Vec<f32>], rng: &mut Rng) -> Value {
    let t = i % DEMO_TOPICS.len();
    let emb: Vec<Value> = (0..dims)
        .map(|d| Value::Float((centroids[t][d] + rng.next_normal() * 0.35) as f64))
        .collect();
    let starred = if i % 7 == 0 { "starred" } else { "plain" };
    let tags = vec![Value::Str(DEMO_TOPICS[t].0.to_string()), Value::Str(starred.to_string())];
    let body = format!("{} — note {i} on {}", DEMO_TOPICS[t].1, DEMO_TOPICS[t].0);
    let published = celastro::time::now_micros() - (i as i64) * 3_600_000_000;
    Value::obj(vec![
        ("id".to_string(), Value::Str(format!("note-{i:04}"))),
        ("tenant_id".to_string(), Value::Str(format!("t{}", i % 3))),
        ("topic".to_string(), Value::Str(DEMO_TOPICS[t].0.to_string())),
        ("body".to_string(), Value::Str(body)),
        ("tags".to_string(), Value::Array(tags)),
        ("published_at".to_string(), Value::Timestamp(published)),
        ("embedding".to_string(), Value::Array(emb)),
    ])
}

/// One demonstrated statement: a heading, the SQL, and what came back.
fn demo_step(db: &mut Db, json: bool, out: &mut Vec<Value>, title: &str, sql: &str) -> Result<()> {
    let t0 = Instant::now();
    let outcome = db.execute(sql)?.finished()?;
    let took = t0.elapsed();
    if json {
        out.push(demo_step_json(title, sql, &outcome, took));
    } else {
        println!("\n\x1b[1m── {title} ──\x1b[0m");
        println!("{sql}\n");
        print_outcome(&outcome, took);
    }
    Ok(())
}

fn demo_step_json(title: &str, sql: &str, o: &Outcome, d: Duration) -> Value {
    Value::obj(vec![
        ("title".to_string(), Value::Str(title.to_string())),
        ("sql".to_string(), Value::Str(sql.to_string())),
        ("result".to_string(), outcome_json(o, d)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn a_bare_invocation_is_refused_rather_than_silently_starting_a_repl() {
        match parse(&[]) {
            Cli::Usage(msg) => assert!(msg.contains("no command")),
            other => panic!("a bare invocation must be a usage error, got {other:?}"),
        }
    }

    #[test]
    fn demo_with_a_dir_is_refused_rather_than_running_in_memory_and_saving_nothing() {
        match parse(&["--dir", "data", "demo"]) {
            Cli::Usage(msg) => assert!(msg.contains("demo") && msg.contains("--dir")),
            other => panic!("`demo` alongside `--dir` must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_dir_without_demo_is_not_swept_up_by_that_rejection() {
        match parse(&["--dir", "data", "repl"]) {
            Cli::Run { dir, cmd, .. } => {
                assert_eq!(dir, Some(PathBuf::from("data")));
                assert_eq!(cmd, Cmd::Repl);
            }
            other => panic!("`--dir` with a real command is valid, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_command_is_refused_rather_than_run_as_a_statement() {
        match parse(&["slect"]) {
            Cli::Usage(msg) => assert!(msg.contains("slect")),
            other => panic!("an unknown command must be refused, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_flag_is_refused_rather_than_ignored() {
        match parse(&["--colour", "repl"]) {
            Cli::Usage(msg) => assert!(msg.contains("--colour")),
            other => panic!("an unknown flag must be refused, got {other:?}"),
        }
    }

    #[test]
    fn an_attached_count_reaches_health_and_nothing_else() {
        match parse(&["--attached", "2", "health"]) {
            Cli::Run { cmd: Cmd::Health { attached, .. }, .. } => assert_eq!(attached, Some(2)),
            other => panic!("`--attached 2 health` must parse, got {other:?}"),
        }
        match parse(&["--attached", "two", "health"]) {
            Cli::Usage(msg) => assert!(msg.contains("two")),
            other => panic!("a count that is not a number must be refused, got {other:?}"),
        }
        match parse(&["--attached", "2", "serve"]) {
            Cli::Usage(msg) => assert!(msg.contains("--attached")),
            other => panic!("`--attached` outside health must be refused, got {other:?}"),
        }
    }

    /// Every tunable parses from its variable, with byte suffixes and the
    /// named choices, and a bad value names itself instead of defaulting.
    #[test]
    fn the_tunables_parse_from_the_environment_and_a_bad_one_is_named() {
        let env: std::collections::BTreeMap<&str, &str> = [
            ("CELASTRO_INSERT_BATCH", "250"),
            ("CELASTRO_MEMTABLE_MAX_BYTES", "16M"),
            ("CELASTRO_MEMTABLE_MAX_VECTORS", "1000"),
            ("CELASTRO_MEMTABLE_BUDGET_BYTES", "2G"),
            ("CELASTRO_RESIDENCY_BUDGET_BYTES", "512M"),
            ("CELASTRO_CACHED_IDLE_UNLOAD_SECS", "10"),
            ("CELASTRO_ARCHIVED_ACCESS", "refuse"),
            ("CELASTRO_STATEMENT_DEADLINE_MS", "0"),
            ("CELASTRO_RECALL_SAMPLE_RATE", "0"),
            ("CELASTRO_COMPACTION_DEAD_RATIO", "0.5"),
            ("CELASTRO_VECTOR_QUANTIZER", "one-bit"),
            ("CELASTRO_HNSW_M", "24"),
            ("CELASTRO_FLAT_TIER_MAX", "100"),
        ]
        .into_iter()
        .collect();
        let mut o = DbOpts::default();
        tuning_into(&mut o, &|k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(o.insert_batch, 250);
        assert_eq!(o.thresholds.max_bytes, 16 << 20);
        assert_eq!(o.thresholds.max_vectors, 1000);
        assert_eq!(o.memtable_budget_bytes, 2 << 30);
        assert_eq!(o.residency.budget_bytes, 512 << 20);
        assert_eq!(o.residency.cached_idle_unload, Duration::from_secs(10));
        assert!(matches!(o.residency.archived_access, celastro::residency::ArchivedAccess::Refuse));
        assert_eq!(o.statement_deadline_ms, None);
        assert_eq!(o.recall_sample_rate, 0);
        assert_eq!(o.compaction.dead_ratio, 0.5);
        assert!(matches!(o.build.quantizer, celastro::vector::quant::Quantizer::OneBit));
        assert_eq!(o.build.hnsw.m, 24);
        assert_eq!(o.build.flat_tier_max, 100);
        let untouched = DbOpts::default();
        assert_eq!(o.compaction.tier_fanout, untouched.compaction.tier_fanout);
        for (k, v) in [
            ("CELASTRO_INSERT_BATCH", "0"),
            ("CELASTRO_MEMTABLE_MAX_BYTES", "lots"),
            ("CELASTRO_COMPACTION_DEAD_RATIO", "2"),
            ("CELASTRO_VECTOR_QUANTIZER", "float16"),
            ("CELASTRO_COMPACTION_TIER_FANOUT", "1"),
        ] {
            let e = tuning_into(&mut DbOpts::default(), &|q| (q == k).then(|| v.to_string()))
                .unwrap_err();
            assert!(e.contains(k), "{k}: {e}");
        }
    }

    #[test]
    fn send_takes_a_url_and_one_statement_and_wants_the_token() {
        match parse(&["send", "http://c:8787", "BACKUP TO '/b'"]) {
            Cli::Run { cmd: Cmd::Send { url, sql }, .. } => {
                assert_eq!((url.as_str(), sql.as_str()), ("http://c:8787", "BACKUP TO '/b'"));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(parse(&["send", "http://c:8787"]), Cli::Usage(_)));
        std::env::remove_var("CELASTRO_TOKEN");
        assert_eq!(send("http://127.0.0.1:1", "SELECT 1", false), EXIT_FAIL);
    }

    #[test]
    fn tls_secret_parses_like_tls_init_and_wants_a_pod_around_it() {
        match parse(&["tls", "secret", "celastro-tls", "celastro", "a.example,10.0.0.1", "30"]) {
            Cli::Run { cmd: Cmd::TlsSecret { secret, name, names, days }, .. } => {
                assert_eq!(
                    (secret.as_str(), name.as_str(), names, days),
                    (
                        "celastro-tls",
                        "celastro",
                        vec!["a.example".to_string(), "10.0.0.1".to_string()],
                        30
                    )
                );
            }
            other => panic!("{other:?}"),
        }
        match parse(&["tls", "secret", "s", "n"]) {
            Cli::Run { cmd: Cmd::TlsSecret { names, days, .. }, .. } => {
                assert!(names.is_empty());
                assert_eq!(days, 3650);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(parse(&["tls", "secret", "s"]), Cli::Usage(_)));
        assert!(matches!(parse(&["tls", "secret", "s", "n", "", "-1"]), Cli::Usage(_)));
        let (dns, ips) = certificate_names(
            "c",
            &["B.example".to_string(), "10.0.0.1".to_string(), "c".to_string()],
        );
        assert_eq!(dns, vec!["c", "localhost", "b.example"]);
        assert_eq!(ips.len(), 2);
        // Outside a pod there is no service account, and the command says so
        // before touching the network.
        if !std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
            assert_eq!(tls_secret("s", "n", &[], 1, false), EXIT_FAIL);
        }
    }

    #[test]
    fn tls_init_writes_a_ca_and_a_certificate_and_refuses_to_overwrite() {
        match parse(&["tls", "init", "/tmp/x", "celastro", "a.example,10.0.0.1", "30"]) {
            Cli::Run { cmd: Cmd::TlsInit { dir, name, names, days }, .. } => {
                assert_eq!(
                    (dir, name.as_str(), names, days),
                    (
                        PathBuf::from("/tmp/x"),
                        "celastro",
                        vec!["a.example".to_string(), "10.0.0.1".to_string()],
                        30
                    )
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(parse(&["tls", "init", "/tmp/x"]), Cli::Usage(_)));
        assert!(matches!(parse(&["tls", "init", "/tmp/x", "n", "", "zero"]), Cli::Usage(_)));
        let dir = std::env::temp_dir().join(format!("celastro-tls-init-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            tls_init(&dir, "celastro", &["c-0.c".to_string(), "10.1.2.3".to_string()], 7, false),
            EXIT_OK
        );
        for f in ["ca.crt", "ca.key", "tls.crt", "tls.key"] {
            let text = std::fs::read_to_string(dir.join(f)).unwrap();
            assert!(text.starts_with("-----BEGIN "), "{f}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join("tls.key")).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(dir.join("tls.crt")).unwrap().permissions().mode() & 0o777,
                0o644
            );
        }
        // The material loads as `serve` would load it, and the certificate names the probe's host.
        std::env::set_var(celastro::tls::CERT_ENV, dir.join("tls.crt"));
        std::env::set_var(celastro::tls::KEY_ENV, dir.join("tls.key"));
        std::env::set_var(celastro::tls::CA_ENV, dir.join("ca.crt"));
        let loaded = Tls::from_env();
        std::env::remove_var(celastro::tls::CERT_ENV);
        std::env::remove_var(celastro::tls::KEY_ENV);
        std::env::remove_var(celastro::tls::CA_ENV);
        let _ = loaded;
        assert_ne!(
            tls_init(&dir, "celastro", &[], 7, false),
            EXIT_OK,
            "an existing file is not overwritten"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_bind_address_reaches_serve_and_nothing_else() {
        match parse(&["--bind", "0.0.0.0", "serve"]) {
            Cli::Run { cmd: Cmd::Serve { bind, .. }, .. } => {
                assert_eq!(bind, Some("0.0.0.0".parse::<IpAddr>().unwrap()))
            }
            other => panic!("`--bind 0.0.0.0 serve` must parse, got {other:?}"),
        }
        match parse(&["--bind", "everywhere", "serve"]) {
            Cli::Usage(msg) => assert!(msg.contains("everywhere")),
            other => panic!("a bind that is not an address must be refused, got {other:?}"),
        }
        match parse(&["--bind", "0.0.0.0", "exec", "SELECT 1"]) {
            Cli::Usage(msg) => assert!(msg.contains("--bind")),
            other => panic!("`--bind` outside serve must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_port_that_is_not_a_number_is_refused_rather_than_falling_back_to_the_default() {
        match parse(&["--port", "eight", "serve"]) {
            Cli::Usage(msg) => assert!(msg.contains("eight")),
            other => panic!("`--port eight` must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_port_above_65535_is_refused_rather_than_wrapping_into_a_different_port() {
        match parse(&["--port", "65536", "serve"]) {
            Cli::Usage(msg) => assert!(msg.contains("65536")),
            other => panic!("a port past the range must be refused, got {other:?}"),
        }
    }

    #[test]
    fn port_zero_is_kept_rather_than_replaced_by_the_default() {
        match parse(&["--port", "0", "serve"]) {
            Cli::Run { cmd: Cmd::Serve { port, .. }, .. } => assert_eq!(port, 0),
            other => panic!("`--port 0` asks the OS for a free port, got {other:?}"),
        }
    }

    #[test]
    fn a_flag_with_no_value_is_refused_rather_than_swallowing_the_next_flag() {
        match parse(&["--dir", "--json", "repl"]) {
            Cli::Usage(msg) => assert!(msg.contains("--dir")),
            other => panic!("`--dir --json` names no directory, got {other:?}"),
        }
    }

    #[test]
    fn a_flag_with_no_value_at_the_end_of_the_line_is_refused_rather_than_ignored() {
        match parse(&["repl", "--dir"]) {
            Cli::Usage(msg) => assert!(msg.contains("--dir")),
            other => panic!("a trailing `--dir` names no directory, got {other:?}"),
        }
    }

    #[test]
    fn a_dash_leading_value_is_still_reachable_through_the_equals_form() {
        match parse(&["--dir=-weird", "repl"]) {
            Cli::Run { dir, .. } => assert_eq!(dir, Some(PathBuf::from("-weird"))),
            other => panic!("`--dir=-weird` names a directory, got {other:?}"),
        }
    }

    #[test]
    fn arguments_after_a_double_dash_are_the_statement_rather_than_flags() {
        match parse(&["exec", "--", "--json"]) {
            Cli::Run { json, cmd, .. } => {
                assert!(!json, "`--json` after `--` is part of the statement");
                assert_eq!(cmd, Cmd::Exec("--json".to_string()));
            }
            other => panic!("`--` ends the flags, got {other:?}"),
        }
    }

    #[test]
    fn a_global_flag_written_after_the_command_is_still_a_flag() {
        match parse(&["exec", "--json", "SELECT 1"]) {
            Cli::Run { json, cmd, .. } => {
                assert!(json);
                assert_eq!(cmd, Cmd::Exec("SELECT 1".to_string()));
            }
            other => panic!("flags are accepted on either side of the command, got {other:?}"),
        }
    }

    #[test]
    fn exec_without_a_statement_is_refused_rather_than_running_an_empty_one() {
        match parse(&["exec"]) {
            Cli::Usage(msg) => assert!(msg.contains("exec")),
            other => panic!("`exec` needs a statement, got {other:?}"),
        }
    }

    #[test]
    fn an_unquoted_statement_is_refused_rather_than_run_as_its_first_word() {
        match parse(&["exec", "SELECT", "1"]) {
            Cli::Usage(msg) => assert!(msg.contains("quote")),
            other => panic!("an unquoted statement must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_port_given_to_a_command_that_cannot_listen_is_refused_rather_than_ignored() {
        match parse(&["--port", "9000", "exec", "SELECT 1"]) {
            Cli::Usage(msg) => assert!(msg.contains("--port")),
            other => panic!("`--port` outside `serve` must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_semicolon_inside_a_string_literal_does_not_split_the_statement() {
        let script = "SELECT 'a;b' FROM t; SELECT 2";
        assert_eq!(split_statements(script), vec!["SELECT 'a;b' FROM t", "SELECT 2"]);
    }

    #[test]
    fn a_doubled_quote_does_not_end_the_string_it_is_escaped_inside() {
        let script = "SELECT 'it''s here; still' FROM t";
        assert_eq!(split_statements(script).len(), 1);
    }

    #[test]
    fn a_field_containing_a_newline_does_not_break_the_table_alignment() {
        let cell = cell_text(&Value::Str("two\nlines".to_string()));
        assert_eq!(cell, "two lines");
    }

    #[test]
    fn a_long_field_is_clipped_so_one_prose_column_does_not_own_the_terminal() {
        let long = Value::Str("x".repeat(MAX_CELL + 10));
        assert_eq!(cell_text(&long).chars().count(), MAX_CELL);
    }

    #[test]
    fn a_row_payload_carries_the_field_names_the_browser_ui_reads() {
        let doc = Value::obj(vec![("id".to_string(), Value::Str("a".to_string()))]);
        let row = Row { key: "t1\u{1}a".to_string(), doc, score: Some(0.5), distance: None };
        let mut r = QueryResult::default();
        r.rows = vec![row];
        let out = json::to_string(&rows_json(&r, Duration::from_millis(7)));
        let want = [
            "\"ok\":true",
            "\"kind\":\"rows\"",
            "\"count\":1",
            "\"elapsed_ms\":7",
            "\"missing\":[]",
            "\"next_cursor\":null",
            "\"doc\":{\"id\":\"a\"}",
            "\"score\":0.5",
            "\"distance\":null",
        ];
        for field in want {
            assert!(out.contains(field), "{field} missing from {out}");
        }
    }

    #[test]
    fn json_asked_for_alongside_help_is_carried_rather_than_dropped_with_the_verb() {
        assert_eq!(parse(&["--json", "help"]), Cli::Help { json: true });
        assert_eq!(parse(&["help"]), Cli::Help { json: false });
    }

    #[test]
    fn json_asked_for_alongside_version_is_carried_rather_than_dropped_with_the_verb() {
        assert_eq!(parse(&["version", "--json"]), Cli::Version { json: true });
        assert_eq!(parse(&["version"]), Cli::Version { json: false });
    }

    #[test]
    fn a_help_flag_does_not_end_the_parse_before_the_json_written_after_it() {
        assert_eq!(parse(&["-h", "--json"]), Cli::Help { json: true });
        assert_eq!(parse(&["-V", "--json"]), Cli::Version { json: true });
    }

    #[test]
    fn version_under_json_is_a_document_rather_than_a_line_of_prose() {
        let parsed = json::parse(&version_output(true)).expect("`--json version` must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        let want = Some(env!("CARGO_PKG_VERSION"));
        assert_eq!(parsed.get("version").and_then(|v| v.as_str()), want);
        assert!(!version_output(false).starts_with('{'), "without `--json` it stays prose");
    }

    #[test]
    fn help_under_json_is_a_document_carrying_the_same_text() {
        let text = help_output(true);
        let parsed = json::parse(&text).expect("`--json help` must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(parsed.get("help").and_then(|v| v.as_str()), Some(HELP));
        assert_eq!(help_output(false), HELP);
    }

    #[test]
    fn a_terminal_failure_under_json_goes_to_stdout_as_a_document_not_to_stderr() {
        let (stdout, line) = failure_report(true, "could not open /nowhere: no such file");
        assert!(stdout, "`--json` promises stdout carries the output, failures included");
        let parsed = json::parse(&line).expect("a terminal failure must be valid JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(false)));
        let msg = parsed.get("error").and_then(|v| v.as_str()).unwrap_or_default();
        assert!(msg.contains("/nowhere"), "{line}");
        let (stdout, line) = failure_report(false, "could not save: disk full");
        assert!(!stdout, "without `--json` a failure is a diagnostic, not output");
        assert_eq!(line, "could not save: disk full");
    }

    #[test]
    fn elapsed_ms_is_a_whole_number_as_the_http_api_sends_it() {
        // A float here and an integer over HTTP is one field with two types,
        // and the console branches on `typeof res.elapsed_ms`.
        let r = QueryResult::default();
        let out = json::to_string(&rows_json(&r, Duration::from_micros(1_500)));
        // The text, not the parsed number: `Value`'s own `==` reads `1.0` and
        // `1` as one value, and the difference under test is exactly the one it
        // hides. The keys are written in sorted order, so a comma follows.
        assert!(out.contains("\"elapsed_ms\":1,"), "{out}");
        let parsed = json::parse(&out).expect("a rows envelope must be valid JSON");
        assert_eq!(parsed.get("elapsed_ms"), Some(&Value::Int(1)), "{out}");
    }

    #[test]
    fn a_score_keeps_the_f32_the_http_api_sends_rather_than_six_decimals_of_it() {
        let score: f32 = 1.234_567_8;
        let doc = Value::obj(vec![("id".to_string(), Value::Str("a".to_string()))]);
        let row = Row { key: "k".to_string(), doc, score: Some(score), distance: Some(score) };
        let mut r = QueryResult::default();
        r.rows = vec![row];
        let out = json::to_string(&rows_json(&r, Duration::from_millis(0)));
        // `{score}` is exactly what the server writes for the same `f32`.
        assert!(out.contains(&format!("\"score\":{score}")), "{out}");
        assert!(out.contains(&format!("\"distance\":{score}")), "{out}");
    }

    #[test]
    fn a_non_finite_score_is_null_rather_than_a_token_no_json_reader_accepts() {
        let row = Row {
            key: "k".to_string(),
            doc: Value::obj(Vec::new()),
            score: Some(f32::NAN),
            distance: Some(f32::INFINITY),
        };
        let mut r = QueryResult::default();
        r.rows = vec![row];
        let out = json::to_string(&rows_json(&r, Duration::from_millis(0)));
        let parsed = json::parse(&out).expect("one strange row must not spoil the document");
        let rows = parsed.get("rows").and_then(|v| v.as_array()).unwrap();
        assert_eq!(rows[0].get("score"), Some(&Value::Null), "{out}");
        assert_eq!(rows[0].get("distance"), Some(&Value::Null), "{out}");
    }

    #[test]
    fn a_cut_prefix_reaches_the_json_the_way_a_missing_tablet_does() {
        // `truncated_prefixes` is `missing`'s sibling on the wire, and the
        // `--json` output is the half of that pair a script reads: the
        // `TRUNCATED —` line the interactive shells print is for a human, and
        // a program piping `--json` sees nothing of it. This field was added
        // with no test on this side at all, so a rename or a dropped entry
        // would have been caught only by the HTTP server's copy.
        let mut r = QueryResult::default();
        r.truncated_prefixes =
            vec![r#"text_match(body, 'a"b*') expanded to 512 terms"#.to_string()];
        let out = json::to_string(&rows_json(&r, Duration::from_millis(0)));
        let parsed = json::parse(&out).expect("a cut report must not spoil the document");
        let cut = parsed.get("truncated_prefixes").and_then(|v| v.as_array()).unwrap();
        assert_eq!(cut.len(), 1, "{out}");
        // Escaped rather than spliced: a prefix may hold a quote, and the
        // report quotes the leaf back.
        assert_eq!(
            cut[0].as_str(),
            Some(r#"text_match(body, 'a"b*') expanded to 512 terms"#),
            "{out}"
        );

        // Present and empty when nothing was cut, so a reader can index it
        // unconditionally rather than testing for the key.
        let clean = json::to_string(&rows_json(&QueryResult::default(), Duration::from_millis(0)));
        let parsed = json::parse(&clean).unwrap();
        assert_eq!(parsed.get("truncated_prefixes"), Some(&Value::Array(Vec::new())), "{clean}");
        assert_eq!(parsed.get("cut_walks"), Some(&Value::Array(Vec::new())), "{clean}");
        let mut r = QueryResult::default();
        r.cut_walks = vec!["WITHIN 2 HOPS OF 'p\"1' VIA cites was cut at hop 2".to_string()];
        let out = json::to_string(&rows_json(&r, Duration::from_millis(0)));
        let parsed = json::parse(&out).unwrap();
        let cut = parsed.get("cut_walks").and_then(|v| v.as_array()).unwrap();
        assert_eq!(
            cut[0].as_str(),
            Some("WITHIN 2 HOPS OF 'p\"1' VIA cites was cut at hop 2"),
            "{out}"
        );
    }

    #[test]
    fn the_catalog_carries_the_fields_the_http_api_sends_and_no_others() {
        let mut db = Db::in_memory();
        db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY)").unwrap();
        let index = "CREATE INDEX items_body ON items USING fulltext (body) \
                     WITH (analyzer = 'english')";
        db.execute(index).unwrap();
        let out = json::to_string(&catalog_json(&db));
        let parsed = json::parse(&out).expect("the catalog must be valid JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        let colls = parsed.get("collections").and_then(|v| v.as_array()).unwrap();
        assert_eq!(colls.len(), 1, "{out}");
        assert_eq!(colls[0].get("name").and_then(|v| v.as_str()), Some("items"));
        assert_eq!(colls[0].get("primary_key").and_then(|v| v.as_str()), Some("id"));
        assert_eq!(colls[0].get("partition_key"), Some(&Value::Null));
        assert_eq!(colls[0].get("doc_count"), Some(&Value::Int(0)));
        // `paths` is what the console reads to offer field names, and it was
        // missing from this side of the same catalog.
        assert!(colls[0].get("paths").and_then(|v| v.as_array()).is_some(), "{out}");
        let indexes = colls[0].get("indexes").and_then(|v| v.as_array()).unwrap();
        let mut fields: Vec<&str> = match &indexes[0] {
            Value::Object(f) => f.iter().map(|(k, _)| k.as_str()).collect(),
            other => panic!("an index must be an object, got {other:?}"),
        };
        fields.sort_unstable();
        assert_eq!(fields, vec!["kind", "name", "path", "tier"], "{out}");
    }

    #[test]
    fn a_sql_error_is_reported_as_a_result_rather_than_thrown_away() {
        let out = json::to_string(&error_json("syntax error: no"));
        assert!(out.contains("\"ok\":false"));
        assert!(out.contains("\"error\":\"syntax error: no\""));
    }

    #[test]
    fn a_save_that_cannot_be_written_exits_non_zero_rather_than_reporting_success() {
        let dir = std::env::temp_dir().join(format!("celastro-persist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(persist_status(&mut db, false), 0);
        // A regular file where the database directory was: the catalog write
        // now has nowhere to land, which is the failure the exit code has to
        // carry out to the shell.
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        assert_eq!(persist_status(&mut db, false), 1);
        let _ = std::fs::remove_file(&dir);
    }

    /// Where the block lands, not only what it says: after the table, before
    /// the row count, before the cursor. The block used to be tested for what
    /// it built while `print_rows` wrote straight to stdout, so a change that
    /// moved it below the count -- or dropped it -- passed every gate.
    #[test]
    fn a_cut_prefix_is_printed_between_the_table_and_the_row_count() {
        let mut r = QueryResult::default();
        for k in ["a", "b"] {
            r.rows.push(Row {
                key: k.to_string(),
                doc: Value::obj(vec![("id".into(), Value::Str(k.into()))]),
                score: Some(0.5),
                distance: None,
            });
        }
        r.truncated_prefixes = vec!["text_match(body, 'a*') was cut: documents are missing".into()];
        r.cut_walks =
            vec!["WITHIN 2 HOPS OF 'x' VIA cites was cut at hop 1: max_fanout = 4 bound 1 node(s)"
                .into()];
        r.next_cursor = Some("#00000000|2|b".into());
        let mut out = Vec::new();
        render_rows(&r, &mut out);
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        let at = |needle: &str| {
            lines
                .iter()
                .position(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("{needle}: {text}"))
        };
        let last_row = at("b   | 0.500000 | b");
        let cut = at("TRUNCATED — text_match(body, 'a*') was cut");
        let walk = at("CUT — WITHIN 2 HOPS OF 'x' VIA cites was cut at hop 1");
        let count = at("2 row(s)");
        let cursor = at("next cursor:");
        assert!(last_row < cut && cut < walk && walk < count && count < cursor, "{text}");
        assert_eq!(count - cut, 2, "something came between the block and the count: {text}");
    }

    #[test]
    fn a_cut_prefix_is_reported_by_this_shell_one_line_per_leaf() {
        // The shell half of "every query that was cut says so". Both JSON
        // surfaces are pinned; this text went to stdout inline, so a dropped
        // loop or a mangled prefix would have been caught only by somebody
        // running a wide `a*` by hand and noticing the silence.
        assert_eq!(truncation_report(&QueryResult::default()), "", "a clean query says nothing");

        let mut r = QueryResult::default();
        r.truncated_prefixes = vec![
            "text_match(body, 'a*') was cut: documents are missing".to_string(),
            "text_match(body, '-a*') was cut: documents it excludes are here".to_string(),
        ];
        // Byte for byte what the loop wrote: the em dash with a space either
        // side, the message unaltered, one line per leaf in the order the
        // result carries them — a statement spelling one prefix in both
        // polarities gets a line for each.
        assert_eq!(
            truncation_report(&r),
            "TRUNCATED — text_match(body, 'a*') was cut: documents are missing\n\
             TRUNCATED — text_match(body, '-a*') was cut: documents it excludes are here\n"
        );
    }
    /// `health` takes `--port` like `serve` does, takes no arguments, and
    /// needs no directory: it asks a running console rather than opening one.
    #[test]
    fn health_takes_a_port_and_nothing_else() {
        match parse_args(vec!["health".to_string()]) {
            Cli::Run { cmd: Cmd::Health { port, .. }, dir, .. } => {
                assert_eq!(port, DEFAULT_PORT);
                assert!(dir.is_none());
            }
            other => panic!("{other:?}"),
        }
        match parse_args(vec!["--port".into(), "9".into(), "health".into()]) {
            Cli::Run { cmd: Cmd::Health { port, .. }, .. } => assert_eq!(port, 9),
            other => panic!("{other:?}"),
        }
        assert!(matches!(parse_args(vec!["health".into(), "x".into()]), Cli::Usage(_)));
        assert!(matches!(
            parse_args(vec!["--port".into(), "9".into(), "catalog".into()]),
            Cli::Usage(_)
        ));
    }

    /// `export` takes a collection and a directory, `import` a directory,
    /// and neither takes more.
    #[test]
    fn export_and_import_take_their_arguments_and_no_more() {
        match parse_args(vec![
            "--dir".into(),
            "d".into(),
            "export".into(),
            "notes".into(),
            "out".into(),
        ]) {
            Cli::Run { cmd: Cmd::Export { collection, to }, .. } => {
                assert_eq!(collection, "notes");
                assert_eq!(to, PathBuf::from("out"));
            }
            other => panic!("{other:?}"),
        }
        match parse_args(vec!["--dir".into(), "d".into(), "import".into(), "out".into()]) {
            Cli::Run { cmd: Cmd::Import { from }, .. } => assert_eq!(from, PathBuf::from("out")),
            other => panic!("{other:?}"),
        }
        assert!(matches!(parse_args(vec!["export".into(), "notes".into()]), Cli::Usage(_)));
        assert!(matches!(parse_args(vec!["import".into()]), Cli::Usage(_)));
        assert!(matches!(parse_args(vec!["import".into(), "a".into(), "b".into()]), Cli::Usage(_)));
    }
}
