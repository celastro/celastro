//! `celastro-cli` — the command-line tool, and the local browser UI it serves.
//!
//! ```text
//! celastro-cli serve --open                    the UI, on 127.0.0.1 only
//! celastro-cli --dir ./data exec 'SELECT 1'    one statement, then exit
//! celastro-cli --dir ./data run setup.sql      a script of statements
//! celastro-cli repl                            statements on stdin
//! celastro-cli demo                            a small hybrid corpus, end to end
//! celastro-cli catalog                         collections and their indexes
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
use celastro::plan::exec::{QueryResult, Row};
use celastro::serve::Server;
use celastro::value::Value;

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
celastro-cli — hybrid document database

USAGE:
  celastro-cli [global flags] <command> [args]

COMMANDS:
  serve [--port N] [--open]  serve the browser UI on 127.0.0.1
  exec <SQL>                 run one statement and print the result
  run <FILE>                 run a script of statements
  repl                       interactive session on stdin
  demo                       build a small hybrid corpus and show it working
  catalog                    list collections and their indexes
  help                       this
  version                    print the version

GLOBAL FLAGS:
  --dir <DIR>                open a persistent database (default: in memory)
  --json                     machine-readable output instead of tables

`serve` prints its URL — token included — on stdout before it starts serving,
so the line can be piped or clicked; the notes go to stderr. Under `--json` that
first line is a JSON object carrying `url`, `addr` and `token` instead, so write
`jq -r .url` when a bare URL is what you wanted. Without `--port` it binds 8787,
and `--port 0` asks the operating system for a free one.

A running server saves after every statement that changed something, and saves
again when it is asked to stop through `POST /api/shutdown`, so a closed
terminal does not cost committed writes.

`--json` covers every command, `help` and `version` included, and stdout carries
the whole answer: a failure that ends the command is written there too, as
{\"ok\":false,\"error\":...}, so a pipeline never has to read stderr to find out
what went wrong.

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
    Serve { port: u16, open: bool },
    Exec(String),
    Script(PathBuf),
    Repl,
    Demo,
    Catalog,
}

fn parse_args<I: IntoIterator<Item = String>>(argv: I) -> Cli {
    let args: Vec<String> = argv.into_iter().collect();
    let mut dir: Option<PathBuf> = None;
    let mut json = false;
    let mut port: Option<u16> = None;
    let mut open = false;
    let mut verb: Option<String> = None;
    // `-h` and `-V` are answered after the whole line is read rather than at
    // the moment they are seen, so that `celastro-cli -h --json` honours the
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
        "serve" | "repl" | "demo" | "catalog" if !rest.is_empty() => {
            return Cli::Usage(format!("`{verb}` takes no arguments"));
        }
        "serve" => Cmd::Serve { port: port.unwrap_or(DEFAULT_PORT), open },
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
        "help" => return Cli::Help { json },
        "version" => return Cli::Version { json },
        other => return Cli::Usage(format!("unknown command `{other}`")),
    };

    // A flag that does nothing where it was written is a mistake, not a
    // courtesy: someone who wrote `--port` expected a server to be listening.
    if !matches!(cmd, Cmd::Serve { .. }) {
        if port.is_some() {
            return Cli::Usage("`--port` only means something to `serve`".to_string());
        }
        if open {
            return Cli::Usage("`--open` only means something to `serve`".to_string());
        }
    }
    // The demo builds its own database, with build options no persistent
    // database should inherit. Accepting `--dir` alongside it would run the
    // demo in memory and leave the directory the operator named empty, with
    // nothing said about it.
    if matches!(cmd, Cmd::Demo) && dir.is_some() {
        let msg = "demo builds its own in-memory database, so it cannot be combined with --dir";
        return Cli::Usage(msg.to_string());
    }
    Cli::Run { dir, json, cmd }
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
        Cli::Run { dir, json, cmd } => std::process::exit(run(dir, json, cmd)),
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
        return format!("celastro-cli {version}");
    }
    let out = Value::obj(vec![
        ("ok".to_string(), Value::Bool(true)),
        ("version".to_string(), Value::Str(version.to_string())),
    ]);
    json::to_string(&out)
}

fn run(dir: Option<PathBuf>, json: bool, cmd: Cmd) -> i32 {
    let mut db = match &dir {
        Some(d) => match Db::open(d, DbOpts::default()) {
            Ok(db) => db,
            Err(e) => return fail(json, &format!("could not open {}: {e}", d.display())),
        },
        // `demo` never has a directory — the combination is refused above — so
        // this is the one place its build options can be applied.
        None if matches!(cmd, Cmd::Demo) => Db::with_opts(demo_opts()),
        None => Db::in_memory(),
    };
    let code = match cmd {
        Cmd::Serve { port, open } => serve(&mut db, port, open, json),
        Cmd::Exec(sql) => statement(&mut db, &sql, json),
        Cmd::Script(file) => run_script(&mut db, &file, json),
        Cmd::Repl => repl(&mut db, json),
        Cmd::Demo => demo(&mut db, json),
        Cmd::Catalog => {
            print_catalog(&db, json);
            EXIT_OK
        }
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
/// `celastro-cli --dir ./data run setup.sql` cannot tell apart from success.
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

fn serve(db: &mut Db, port: u16, open: bool, json: bool) -> i32 {
    let server = match Server::bind(port) {
        Ok(s) => s,
        Err(e) => return fail(json, &format!("could not bind 127.0.0.1:{port}: {e}")),
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
    eprintln!("celastro-cli serving on {} — Ctrl-C to stop", server.local_addr());
    eprintln!(
        "The token in that URL is the only thing protecting this database. Anyone who can\n\
         read this terminal, this process's environment or its command line can use it, and\n\
         the server answers every request that carries it. Treat the URL as a password, and\n\
         stop the server when you are done."
    );
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
    match server.run(db) {
        Ok(()) => EXIT_OK,
        Err(e) => fail(json, &format!("serving stopped: {e}")),
    }
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
    let outcome = db.execute(sql);
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

fn run_script(db: &mut Db, file: &Path, json: bool) -> i32 {
    let text = match std::fs::read_to_string(file) {
        Ok(t) => t,
        Err(e) => return fail(json, &format!("could not read {}: {e}", file.display())),
    };
    let stmts = split_statements(&text);
    if json {
        return script_json(db, &stmts);
    }
    for stmt in &stmts {
        // Stop at the first failure: the statements after it were written
        // expecting the one before to have happened.
        if statement(db, stmt, false) != EXIT_OK {
            return EXIT_FAIL;
        }
    }
    EXIT_OK
}

/// A script is a stream of statements, but `--json` promises a single document
/// on stdout — so the results are collected and printed once, at the end.
fn script_json(db: &mut Db, stmts: &[String]) -> i32 {
    let mut results: Vec<Value> = Vec::new();
    let mut code = EXIT_OK;
    for stmt in stmts {
        let t0 = Instant::now();
        let outcome = db.execute(stmt);
        let took = t0.elapsed();
        match outcome {
            Ok(o) => results.push(outcome_json(&o, took)),
            Err(e) => {
                results.push(error_json(&e.to_string()));
                code = EXIT_FAIL;
                break;
            }
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

fn repl(db: &mut Db, json: bool) -> i32 {
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
        if statement(db, stmt, json) != EXIT_OK {
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
    }
}

fn index_kind(kind: &IndexKind) -> String {
    match kind {
        IndexKind::FullText { analyzer } => format!("fulltext, analyzer {analyzer}"),
        IndexKind::Vector { dims, metric } => format!("vector, {dims} dims, {}", metric.name()),
        IndexKind::Secondary => "secondary".to_string(),
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
    }
    println!("({:.2} ms)", millis(d));
}

fn print_rows(r: &QueryResult) {
    if r.rows.is_empty() {
        println!("(no rows)");
    } else {
        print_table(&r.rows);
    }
    if !r.missing.is_empty() {
        println!("PARTIAL RESULTS — missing: {:?}", r.missing);
    }
    print!("{}", truncation_report(r));
    println!("{} row(s)", r.rows.len());
    if let Some(c) = &r.next_cursor {
        println!("next cursor: {}", c.replace('\u{1}', "/"));
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

fn print_table(rows: &[Row]) {
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
    println!("{}", pad_join(&lines[0], &width));
    println!("{}", rule.join("-+-"));
    for line in &lines[1..] {
        println!("{}", pad_join(line, &width));
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
    let outcome = db.execute(sql)?;
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
        let dir = std::env::temp_dir().join(format!("celastro-cli-persist-{}", std::process::id()));
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
}
