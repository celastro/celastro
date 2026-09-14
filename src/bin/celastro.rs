//! `celastro` — a REPL and script runner for the engine.
//!
//! ```text
//! celastro                      an in-memory database, statements on stdin
//! celastro --dir ./data         persistent, reopened from disk
//! celastro --file setup.sql     run a script, then exit
//! celastro --demo               build a small hybrid corpus and show it working
//! ```
//!
//! Statements are terminated by `;` or by a blank line.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use celastro::codec::Rng;
use celastro::engine::{Db, DbOpts, Outcome};
use celastro::error::Result;
use celastro::json;
use celastro::plan::exec::QueryResult;
use celastro::value::Value;

/// What the command line asked for, once the arguments have been checked
/// against each other.
enum Cli {
    Run { dir: Option<PathBuf>, file: Option<PathBuf>, demo: bool },
    Help,
    Reject(String),
}

fn parse_args<I: IntoIterator<Item = String>>(argv: I) -> Cli {
    let mut dir: Option<PathBuf> = None;
    let mut file: Option<PathBuf> = None;
    let mut demo = false;
    let mut args = argv.into_iter();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" => dir = args.next().map(PathBuf::from),
            "--file" => file = args.next().map(PathBuf::from),
            "--demo" => demo = true,
            "-h" | "--help" => return Cli::Help,
            other => return Cli::Reject(format!("unknown argument `{other}`")),
        }
    }
    // The demo builds its own database, with build options no persistent
    // database should inherit. Accepting `--dir` alongside it would run the
    // demo in memory and leave the directory the operator named empty, with
    // nothing said about it.
    if demo && dir.is_some() {
        let msg = "--demo builds its own in-memory database, so it cannot be combined with --dir";
        return Cli::Reject(msg.to_string());
    }
    Cli::Run { dir, file, demo }
}

/// Save, and turn a failure into a non-zero exit. A discarded `persist` error
/// is a process that exits 0 having written nothing, which the script that ran
/// `celastro --dir ./data --file setup.sql` cannot tell apart from success.
fn persist_status(db: &mut Db) -> i32 {
    match db.persist() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("could not save: {e}");
            1
        }
    }
}

fn main() {
    let (dir, file, demo) = match parse_args(std::env::args().skip(1)) {
        Cli::Run { dir, file, demo } => (dir, file, demo),
        Cli::Help => {
            print_help();
            return;
        }
        Cli::Reject(msg) => {
            eprintln!("{msg}");
            print_help();
            std::process::exit(2);
        }
    };

    if demo {
        // The demo runs with a low flat-tier threshold so that a few hundred
        // documents actually reach the HNSW tier, and with every vector query
        // logged, so the recall harness replays real queries rather than
        // synthesising friendlier ones.
        let mut opts = DbOpts::default();
        opts.build.flat_tier_max = 64;
        opts.recall_sample_rate = 1;
        let mut db = Db::with_opts(opts);
        if let Err(e) = run_demo(&mut db) {
            eprintln!("demo failed: {e}");
            std::process::exit(1);
        }
        return;
    }

    let mut db = match &dir {
        Some(d) => match Db::open(d, DbOpts::default()) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("could not open {}: {e}", d.display());
                std::process::exit(1);
            }
        },
        None => Db::in_memory(),
    };

    if let Some(f) = file {
        let text = match std::fs::read_to_string(&f) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("could not read {}: {e}", f.display());
                std::process::exit(1);
            }
        };
        for stmt in split_statements(&text) {
            if let Err(e) = run_one(&mut db, &stmt) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        std::process::exit(persist_status(&mut db));
    }

    repl(&mut db);
    std::process::exit(persist_status(&mut db));
}

fn print_help() {
    println!(
        "celastro — hybrid document database\n\
         \n\
         USAGE:\n  \
           celastro [--dir <path>] [--file <script.sql>] [--demo]\n\
         \n\
         Statements end with `;` or a blank line. Try:\n  \
           CREATE COLLECTION articles (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL) PARTITION BY (tenant_id);\n  \
           CREATE INDEX a_body ON articles USING fulltext (body) WITH (analyzer = 'english');\n  \
           CREATE INDEX a_emb ON articles USING vector (embedding) WITH (dims = 8, metric = 'cosine');\n  \
           DROP INDEX a_emb ON articles;\n  \
           DROP COLLECTION articles;\n  \
           SHOW SEGMENTS articles;\n  \
           EXPLAIN ANALYZE SELECT id FROM articles ORDER BY hybrid(text_match(body, 'x'), embedding <=> [..]) LIMIT 5;\n"
    );
}

fn repl(db: &mut Db) {
    let stdin = io::stdin();
    let mut buf = String::new();
    print_banner();
    loop {
        print!("{}", if buf.trim().is_empty() { "celastro> " } else { "     -> " });
        let _ = io::stdout().flush();
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
        let trimmed = line.trim();
        if buf.trim().is_empty() && (trimmed == "exit" || trimmed == "quit" || trimmed == "\\q") {
            break;
        }
        if buf.trim().is_empty() && trimmed == "\\h" {
            print_help();
            continue;
        }
        buf.push_str(&line);
        let ready = buf.trim_end().ends_with(';') || (trimmed.is_empty() && !buf.trim().is_empty());
        if !ready {
            continue;
        }
        let stmt = std::mem::take(&mut buf);
        if stmt.trim().is_empty() {
            continue;
        }
        if let Err(e) = run_one(db, stmt.trim().trim_end_matches(';')) {
            println!("error: {e}");
        }
    }
}

fn print_banner() {
    println!("celastro — SQL, BM25 and vector search in one query plan.");
    println!("`\\h` for help, `exit` to leave. Statements end with `;` or a blank line.\n");
}

fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            // Doubled quotes escape inside a string literal.
            if in_string && chars.peek() == Some(&'\'') {
                cur.push(c);
                cur.push(chars.next().unwrap());
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

fn run_one(db: &mut Db, sql: &str) -> Result<()> {
    let t0 = std::time::Instant::now();
    match db.execute(sql)? {
        Outcome::Ack(m) => println!("{m}"),
        Outcome::Explain(text) => print!("{text}"),
        Outcome::Recall(r) => print!("{}", r.render()),
        Outcome::Rows(r) => print_rows(&r),
    }
    println!("({:.2} ms)", t0.elapsed().as_secs_f64() * 1000.0);
    Ok(())
}

fn print_rows(r: &QueryResult) {
    let stdout = io::stdout();
    render_rows(r, &mut stdout.lock());
}

/// The whole of what a query prints, into `out`, so that a test can pin
/// where each part lands and not only what each part says.
fn render_rows(r: &QueryResult, out: &mut dyn Write) {
    for row in &r.rows {
        let key = row.key.replace('\u{1}', "/");
        let _ = match (row.score, row.distance) {
            (_, Some(d)) => writeln!(out, "{key}  distance={d:.6}  {}", json::to_string(&row.doc)),
            (Some(s), None) => writeln!(out, "{key}  score={s:.6}  {}", json::to_string(&row.doc)),
            _ => writeln!(out, "{key}  {}", json::to_string(&row.doc)),
        };
    }
    if !r.missing.is_empty() {
        let _ = writeln!(out, "PARTIAL RESULTS — missing: {:?}", r.missing);
    }
    // Between the rows and the count, where a reader who stops at the count
    // has already read it.
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
/// `print_rows` is reachable from no test at all: both JSON renderings of the
/// same report are pinned, and the two shells' line was not. Empty when
/// nothing was cut, so the caller prints it unconditionally.
///
/// `celastro-cli` carries its own copy for the reason its header gives — a
/// binary cannot import another binary — and one private rendering detail is
/// not a reason to grow the library's public API.
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

// --------------------------------------------------------------------------
// Demo
// --------------------------------------------------------------------------

const TOPICS: &[(&str, &str)] = &[
    ("vector", "approximate nearest neighbour search over quantized codes"),
    ("lexical", "block max wand postings and the bm25 saturation curve"),
    ("hybrid", "reciprocal rank fusion over lexical and vector candidates"),
    ("storage", "immutable segments compaction and the delete log"),
    ("planner", "runtime strategy selection on measured selectivity"),
];

fn run_demo(db: &mut Db) -> Result<()> {
    println!("celastro demo — building a small hybrid corpus.\n");
    let dims = 16usize;
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
        "CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = {dims}, metric = 'cosine')"
    ))?;

    let mut rng = Rng::new(20260908);
    // Five topic centroids; each document is a centroid plus noise, so vector
    // neighbourhoods mean something.
    let centroids: Vec<Vec<f32>> =
        (0..TOPICS.len()).map(|_| (0..dims).map(|_| rng.next_normal()).collect()).collect();

    let n = 900;
    for i in 0..n {
        let t = i % TOPICS.len();
        let emb: Vec<Value> = (0..dims)
            .map(|d| Value::Float((centroids[t][d] + rng.next_normal() * 0.35) as f64))
            .collect();
        let doc = Value::obj(vec![
            ("id".into(), Value::Str(format!("note-{i:04}"))),
            ("tenant_id".into(), Value::Str(format!("t{}", i % 3))),
            ("topic".into(), Value::Str(TOPICS[t].0.into())),
            ("body".into(), Value::Str(format!("{} — note {i} on {}", TOPICS[t].1, TOPICS[t].0))),
            (
                "tags".into(),
                Value::Array(vec![
                    Value::Str(TOPICS[t].0.into()),
                    Value::Str(if i % 7 == 0 { "starred".into() } else { "plain".into() }),
                ]),
            ),
            (
                "published_at".into(),
                Value::Timestamp(celastro::time::now_micros() - (i as i64) * 3_600_000_000),
            ),
            ("embedding".into(), Value::Array(emb)),
        ]);
        db.insert("notes", doc)?;
    }
    println!("{n} documents written across 3 shards.\n");

    // A query vector near the "hybrid" centroid.
    let qv: Vec<String> = centroids[2].iter().map(|x| format!("{:.6}", x + 0.05)).collect();
    let qlit = format!("[{}]", qv.join(","));

    section("Fresh writes are searchable at exact recall, before any flush");
    run_one(
        db,
        &format!("SELECT id, topic FROM notes WHERE tenant_id = 't1' ORDER BY embedding <=> {qlit} LIMIT 5"),
    )?;

    section("Flush and compact: the same query over sealed segments");
    run_one(db, "FLUSH notes")?;
    run_one(db, "SHOW SEGMENTS notes")?;
    run_one(
        db,
        &format!("SELECT id, topic FROM notes WHERE tenant_id = 't1' ORDER BY embedding <=> {qlit} LIMIT 5"),
    )?;

    section("Hybrid retrieval: filter, text and vector in one plan");
    run_one(
        db,
        &format!(
            "EXPLAIN ANALYZE SELECT id, topic FROM notes \
             WHERE tenant_id = 't1' AND ANY(tags) = 'starred' \
             ORDER BY hybrid(text_match(body, 'fusion candidates'), embedding <=> {qlit}, method => 'rrf') \
             LIMIT 5"
        ),
    )?;

    section("text_match in WHERE is a must; in hybrid() it is a should");
    run_one(db, "SELECT id, topic FROM notes WHERE text_match(body, '\"delete log\"') LIMIT 5")?;

    section("Deletes, and the recall they would silently cost without measurement");
    // Spread across tenants, so every shard accumulates tombstones.
    let mut deleted = 0;
    for i in (0..n).step_by(2) {
        if db.delete_key("notes", &format!("t{}\u{1}note-{i:04}", i % 3))? {
            deleted += 1;
        }
    }
    println!("{deleted} documents deleted.");
    run_one(db, "MEASURE RECALL ON notes WITH (k = 10, samples = 24)")?;
    run_one(db, "COMPACT notes")?;
    run_one(db, "SHOW SEGMENTS notes")?;
    run_one(db, "MEASURE RECALL ON notes WITH (k = 10, samples = 24)")?;

    section("The catalog inferred what the DDL did not declare");
    run_one(db, "SHOW CATALOG notes")?;

    section("Tiers: where each index wants to live, and what is actually in RAM");
    println!(
        "  Note what compaction did to the ledger: the retired segments' components\n  \
         are gone from it, not merely marked unloaded.\n"
    );
    run_one(db, "SHOW RESIDENCY notes")?;
    run_one(db, "ALTER INDEX notes_emb ON notes SET TIER 'cached'")?;
    println!(
        "  The tier reaches the ledger, so eviction order changes with it — the\n  \
         cached index is now what a squeezed node gives up first, and an idle\n  \
         sweep releases it after its window rather than never:\n"
    );
    run_one(db, "SHOW RESIDENCY notes")?;
    run_one(db, "UNLOAD IDLE ON notes")?;
    println!("  Nothing yet — it was queried seconds ago, and the cached window is 60s.");
    let before: Vec<String> = db
        .query(&format!("SELECT id FROM notes ORDER BY embedding <=> {qlit} LIMIT 3"))?
        .rows
        .iter()
        .map(|r| r.key.clone())
        .collect();
    // Force the release the sweeper would do on its own once the window passed.
    let freed = db.sweep_all()?;
    println!("\n  Releasing every component by hand instead: {freed} byte(s) freed.");
    let after: Vec<String> = db
        .query(&format!("SELECT id FROM notes ORDER BY embedding <=> {qlit} LIMIT 3"))?
        .rows
        .iter()
        .map(|r| r.key.clone())
        .collect();
    println!(
        "  Same query, decoded from scratch: {} rows, identical: {}.\n  \
         A tier changes latency and memory. It never changes an answer.",
        after.len(),
        before == after
    );
    run_one(db, "SHOW RESIDENCY notes")?;

    section("`minimal`: one decoded copy in the cluster, whatever the replica count");
    run_one(db, "ALTER INDEX notes_body ON notes SET TIER 'minimal'")?;
    run_one(db, "SHOW CATALOG notes")?;
    println!(
        "  This node is a single-node deployment, so it is the designated holder and\n  \
         keeps the index decoded. Given a replica list, exactly one of the replicas\n  \
         would — every node computes the same answer from the same list, so nobody\n  \
         has to be told. The others resolve `minimal` to `cached`: they still answer,\n  \
         they pay one segment read to do it."
    );
    for n in 1..=5usize {
        let replicas: Vec<String> = (0..n).map(|i| format!("node-{i}")).collect();
        let holders = replicas
            .iter()
            .filter(|me| {
                celastro::residency::Placement::new((*me).clone(), replicas.clone())
                    .holds("notes/text:body")
            })
            .count();
        println!("    {n} replica(s) -> {holders} decoded copy(ies)");
    }

    section("A lifecycle policy, and what it would do");
    run_one(
        db,
        "CREATE LIFECYCLE POLICY cool_down ON notes \
           MOVE TO minimal  AFTER 30 minutes OF INACTIVITY, \
           MOVE TO cached   AFTER 6 hours OF INACTIVITY, \
           MOVE TO archived AFTER 7 days OF INACTIVITY, \
           MOVE TO archived AFTER 90 days SINCE CREATION",
    )?;
    run_one(db, "SHOW LIFECYCLE")?;
    run_one(db, "RUN LIFECYCLE ON notes")?;
    println!(
        "  nothing is due yet — every index was created and queried seconds ago.\n  \
         The clocks are persisted, so the countdown survives a restart."
    );
    Ok(())
}

fn section(title: &str) {
    println!("\n\x1b[1m── {title} ──\x1b[0m");
}

#[cfg(test)]
mod tests {
    use super::*;
    use celastro::plan::exec::Row;

    #[test]
    fn demo_with_a_dir_is_refused_rather_than_running_in_memory_and_saving_nothing() {
        let argv = vec!["--dir".to_string(), "data".to_string(), "--demo".to_string()];
        match parse_args(argv) {
            Cli::Reject(msg) => assert!(msg.contains("--demo") && msg.contains("--dir")),
            _ => panic!("`--demo` alongside `--dir` must be refused, not silently ignored"),
        }
    }

    #[test]
    fn a_dir_without_demo_is_not_swept_up_by_that_rejection() {
        match parse_args(vec!["--dir".to_string(), "data".to_string()]) {
            Cli::Run { dir, demo, .. } => {
                assert_eq!(dir, Some(PathBuf::from("data")));
                assert!(!demo);
            }
            _ => panic!("`--dir` on its own is a valid invocation"),
        }
    }

    #[test]
    fn a_save_that_cannot_be_written_exits_non_zero_rather_than_reporting_success() {
        let dir = std::env::temp_dir().join(format!("celastro-cli-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(persist_status(&mut db), 0);
        // A regular file where the database directory was: the catalog write
        // now has nowhere to land, which is the failure the exit code has to
        // carry out to the shell.
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        assert_eq!(persist_status(&mut db), 1);
        let _ = std::fs::remove_file(&dir);
    }

    /// Where the block lands: after the rows, before the count, before the
    /// cursor. Tested through the writer, because a `println!` is reachable
    /// from no test and a block that moved below the count passed every gate.
    #[test]
    fn a_cut_prefix_is_printed_between_the_rows_and_the_row_count() {
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
        let last_row = at("b  score=0.500000");
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
}
