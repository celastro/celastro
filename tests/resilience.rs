//! The resilience suite: the slow, cluster-shaped tests, ignored by
//! default so the six gates stay fast, and run when asked:
//!
//! ```sh
//! cargo test --release --test resilience -- --ignored --test-threads=1
//! ```
//!
//! Each test is a property a cluster is expected to keep under something
//! that goes wrong, drawn from the drills and the pitfalls the design
//! notes record: the reconciliation converging over more nodes and more
//! seeds than the gate runs, no acknowledged write lost across a node
//! that restarts under load, a large write-ahead log replaying, and a
//! cluster backup taken under load restoring to one consistent cut.

mod common;

use celastro::lock::RwLock;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::plan::exec::QueryResult;
use celastro::Value;

static ENV: Mutex<()> = Mutex::new(());
const TOKEN: &str = "resilience-token";

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-res-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

struct Node {
    url: String,
    db: Arc<RwLock<Db>>,
    stop: Arc<AtomicBool>,
    dir: PathBuf,
}

impl Node {
    fn start(tag: &str) -> Node {
        Node::start_at(tag, 0, None)
    }

    fn start_at(tag: &str, port: u16, reuse: Option<PathBuf>) -> Node {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let dir = reuse.unwrap_or_else(|| dir(tag));
        let mut opts = DbOpts::default();
        opts.node = Some(url.clone());
        let db = Arc::new(RwLock::new(Db::open(&dir, opts).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Node { url, db, stop, dir }
    }

    fn port(&self) -> u16 {
        self.url.rsplit(':').next().unwrap().parse().unwrap()
    }

    fn exec(&self, sql: &str) -> celastro::Result<Outcome> {
        self.db.write().unwrap().execute(sql)
    }

    fn ack(&self, sql: &str) -> String {
        // `exec` let go of the lock; a deferred copy runs here without it.
        let out = self.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        match out.finished().unwrap_or_else(|e| panic!("{sql}: {e}")) {
            Outcome::Ack(m) => m,
            other => panic!("{sql}: {other:?}"),
        }
    }

    fn query(&self, sql: &str) -> celastro::Result<QueryResult> {
        self.db.write().unwrap().query(sql)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn settle() {
    std::thread::sleep(Duration::from_millis(700));
}

fn remove(nodes: Vec<Node>) {
    for n in nodes {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

fn doc(i: usize) -> Value {
    Value::obj(vec![
        ("id".into(), Value::Str(format!("k-{i:06}"))),
        ("n".into(), Value::Int(i as i64)),
        ("body".into(), Value::Str(format!("row {i} of the resilience suite"))),
    ])
}

/// The gate runs 24 seeds over three nodes and 20 statements; this runs
/// 400 over four nodes and 40, with reconciliations in the middle.
#[test]
#[ignore]
fn reconciliation_converges_over_four_nodes_and_four_hundred_seeds() {
    let started = Instant::now();
    common::converge(400, 4, 40);
    eprintln!("resilience: 400 seeds x 4 nodes x 40 statements in {:.1?}", started.elapsed());
}

/// A node that restarts five times under a load of writes and reads: every
/// write the cluster acknowledged is there when it is back, and every
/// failure meanwhile named the node or a deadline, nothing else.
#[test]
#[ignore]
fn no_acknowledged_write_is_lost_across_a_node_that_restarts_under_load() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("flap-a");
    let b = Node::start("flap-b");
    let c = Node::start("flap-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['k-2', 'k-4'])");
    let (c_port, c_dir) = (c.port(), c.dir.clone());
    let c_slot: Arc<Mutex<Option<Node>>> = Arc::new(Mutex::new(Some(c)));
    let flapping = Arc::new(AtomicBool::new(true));
    let flapper = {
        let (slot, flapping) = (c_slot.clone(), flapping.clone());
        std::thread::spawn(move || {
            for k in 0..5 {
                std::thread::sleep(Duration::from_millis(1500));
                let taken = slot.lock().unwrap().take();
                drop(taken);
                settle();
                let t0 = Instant::now();
                *slot.lock().unwrap() = Some(Node::start_at("flap-c", c_port, Some(c_dir.clone())));
                eprintln!("resilience: restart {} of c took {:.1?}", k + 1, t0.elapsed());
            }
            std::thread::sleep(Duration::from_millis(1000));
            flapping.store(false, Ordering::Relaxed);
        })
    };
    let mut acked = Vec::new();
    let (mut ok_reads, mut failed_writes, mut failed_reads) = (0usize, 0usize, 0usize);
    let mut i = 0usize;
    let began = Instant::now();
    while flapping.load(Ordering::Relaxed) {
        assert!(began.elapsed() < Duration::from_secs(120), "the flapping never ended");
        if i % 50 == 0 {
            eprintln!("resilience: statement {i} at {:.1?}", began.elapsed());
        }
        let key = format!("k-{i:06}");
        // Spread over every shard, including the flapping node's.
        let d = Value::obj(vec![
            ("id".into(), Value::Str(key.clone())),
            ("n".into(), Value::Int(i as i64)),
        ]);
        let wrote = a.db.write().unwrap().insert("items", d);
        match wrote {
            Ok(_) => acked.push(key.clone()),
            Err(e) => {
                let e = e.to_string();
                assert!(
                    e.contains("did not answer")
                        || e.contains("deadline")
                        || e.contains("connect")
                        || e.contains("refused")
                        || e.contains("unreachable"),
                    "a failure that is not the node or a deadline: {e}"
                );
                failed_writes += 1;
            }
        }
        match b.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 1")) {
            Ok(r) if r.rows.len() == 1 => ok_reads += 1,
            Ok(_) => assert!(!acked.contains(&key), "an acknowledged write is not readable"),
            Err(_) => failed_reads += 1,
        }
        i += 1;
    }
    flapper.join().unwrap();
    std::thread::sleep(Duration::from_millis(2500));
    let c = c_slot.lock().unwrap().take().unwrap();
    eprintln!(
        "resilience: {} writes acknowledged, {failed_writes} refused, {ok_reads} reads answered, \
         {failed_reads} refused, across five restarts",
        acked.len()
    );
    assert!(acked.len() > 50, "the load ran: {} writes", acked.len());
    let r = a.query("SELECT count(*) AS c FROM items").unwrap();
    let count = r.rows[0].doc.path("c").and_then(|v| v.as_i64()).unwrap() as usize;
    assert_eq!(count, acked.len(), "every acknowledged write, and nothing else, is there");
    for key in acked.iter().step_by(7) {
        let r = c.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 1")).unwrap();
        assert_eq!(r.rows.len(), 1, "{key} through the node that flapped");
    }
    remove(vec![a, b, c]);
}

/// A write-ahead log of two hundred thousand rows, never sealed, replays
/// on reopen with every row -- and the time it takes is printed, since a
/// replay is the length of a restart.
#[test]
#[ignore]
fn a_large_write_ahead_log_replays_every_row() {
    let d = dir("wal");
    let mut opts = DbOpts::default();
    opts.thresholds.max_bytes = 1 << 30;
    opts.memtable_budget_bytes = 1 << 30;
    let n = 200_000usize;
    {
        let mut db = Db::open(&d, opts.clone()).unwrap();
        db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)").unwrap();
        let started = Instant::now();
        for batch in (0..n).step_by(1000) {
            let docs: Vec<Value> = (batch..batch + 1000).map(doc).collect();
            db.insert_many("items", docs).unwrap();
        }
        eprintln!("resilience: {n} rows written in {:.1?}", started.elapsed());
    }
    let started = Instant::now();
    let mut db = Db::open(&d, opts).unwrap();
    let replay = started.elapsed();
    let r = db.query("SELECT count(*) AS c FROM items").unwrap();
    let count = r.rows[0].doc.path("c").and_then(|v| v.as_i64()).unwrap() as usize;
    eprintln!(
        "resilience: {n} rows replayed in {replay:.1?} ({:.0} us/row)",
        replay.as_micros() as f64 / n as f64
    );
    assert_eq!(count, n);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}

/// A node killed outright keeps every row it acknowledged, and a second
/// open agrees with the first.
///
/// The drill of 2026-09-16 measured this by hand and nothing has held it
/// since. The restart tests above drop a `Db` and open it again, which
/// runs every destructor and flushes what a power cut would not: this one
/// spawns the binary, writes through its console until several thousand
/// rows are acknowledged, and sends SIGKILL (`Child::kill` on Unix) with a
/// batch still in flight.
///
/// What must hold: every acknowledged row is there, nothing that was never
/// sent is, and the count does not move on a second open. The batch in
/// flight may be there or not -- it was never acknowledged, and both
/// answers are correct -- so which way it fell is printed, not asserted.
#[test]
#[ignore]
fn a_node_killed_outright_keeps_every_row_it_acknowledged() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Command, Stdio};
    use std::sync::atomic::AtomicUsize;

    const BATCH: usize = 500;
    const BEFORE_THE_KILL: usize = 8; // batches, so four thousand rows

    // One connection per statement, like any client: after the kill the
    // connect fails, which is how the writer learns to stop.
    fn post(addr: &str, token: &str, sql: &str) -> std::io::Result<String> {
        let body =
            celastro::json::to_string(&Value::obj(vec![("sql".into(), Value::Str(sql.into()))]));
        let mut s = std::net::TcpStream::connect(addr)?;
        write!(
            s,
            "POST /api/query?t={token} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: \
             application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )?;
        let mut reply = String::new();
        s.read_to_string(&mut reply)?;
        Ok(reply)
    }

    let d = dir("kill9");
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro"))
        .args(["--json", "--dir", d.to_str().unwrap(), "serve", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn celastro");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
    let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();
    let r = post(&addr, &token, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)").unwrap();
    assert!(r.contains("\"ok\":true"), "{r}");

    // The writer counts only what came back ok: a batch whose answer never
    // arrived is not acknowledged, whatever reached the disk.
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (addr, token, acked) = (addr.clone(), token.clone(), acked.clone());
        std::thread::spawn(move || {
            for batch in 0usize.. {
                let rows: Vec<String> = (0..BATCH)
                    .map(|i| {
                        let n = batch * BATCH + i;
                        format!("(\'{{\"id\":\"k-{n:06}\",\"n\":{n}}}\')")
                    })
                    .collect();
                let sql = format!("INSERT INTO items VALUES {}", rows.join(", "));
                match post(&addr, &token, &sql) {
                    Ok(r) if r.contains("\"ok\":true") => {
                        acked.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => break, // the console is gone: the kill landed
                }
            }
        })
    };

    let waited = Instant::now();
    while acked.load(Ordering::SeqCst) < BEFORE_THE_KILL {
        assert!(waited.elapsed() < Duration::from_secs(120), "the writer never got going");
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("SIGKILL");
    let _ = child.wait();
    let _ = writer.join();
    // Read after the writer has stopped: a batch acknowledged between the
    // loop above and the kill is still acknowledged and still has to be
    // there.
    let batches = acked.load(Ordering::SeqCst);
    let rows_acked = batches * BATCH;
    assert!(rows_acked >= BEFORE_THE_KILL * BATCH, "only {rows_acked} rows were acknowledged");

    let started = Instant::now();
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    let replay = started.elapsed();
    let count = |db: &mut Db, sql: &str| -> usize {
        let r = db.query(sql).unwrap();
        r.rows[0].doc.path("c").and_then(|v| v.as_i64()).unwrap() as usize
    };
    let total = count(&mut db, "SELECT count(*) AS c FROM items");
    let kept = count(&mut db, &format!("SELECT count(*) AS c FROM items WHERE n < {rows_acked}"));
    assert_eq!(kept, rows_acked, "the kill lost a row the console had acknowledged");
    assert!(
        total <= rows_acked + BATCH,
        "{total} rows after {rows_acked} acknowledged and one batch in flight"
    );
    drop(db);

    let mut again = Db::open(&d, DbOpts::default()).unwrap();
    let second = count(&mut again, "SELECT count(*) AS c FROM items");
    assert_eq!(second, total, "the second open disagreed with the first");
    drop(again);
    eprintln!(
        "resilience: SIGKILL after {batches} acknowledged batches ({rows_acked} rows); reopened \
         in {replay:.1?} with {total} rows, {} of them from the batch in flight",
        total - rows_acked
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A cluster backup taken while a writer keeps every edge's endpoints
/// ahead of the edge restores to a cut where that still holds on every
/// node, which three per-node backups at their own instants need not.
#[test]
#[ignore]
fn a_cluster_backup_under_load_restores_to_one_consistent_cut() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("cut-a");
    let b = Node::start("cut-b");
    let c = Node::start("cut-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack("CREATE COLLECTION people (id TEXT PRIMARY KEY) WITH (splits = ['p-3', 'p-6'])");
    a.ack("CREATE COLLECTION follows (id TEXT PRIMARY KEY, src TEXT, dst TEXT) WITH (splits = ['f-3', 'f-6'])");
    let writing = Arc::new(AtomicBool::new(true));
    let writer = {
        let (db, writing) = (a.db.clone(), writing.clone());
        std::thread::spawn(move || {
            let mut i = 0usize;
            while writing.load(Ordering::Relaxed) {
                // The person first, acknowledged, then the edge that names
                // them: at every instant an edge's endpoints exist.
                let person = Value::obj(vec![("id".into(), Value::Str(format!("p-{}", i % 10)))]);
                let _ = person;
                let p = Value::obj(vec![("id".into(), Value::Str(format!("p-{i:05}")))]);
                db.write().unwrap().insert("people", p).unwrap();
                if i > 0 {
                    let f = Value::obj(vec![
                        ("id".into(), Value::Str(format!("f-{i:05}"))),
                        ("src".into(), Value::Str(format!("p-{i:05}"))),
                        ("dst".into(), Value::Str(format!("p-{:05}", i - 1))),
                    ]);
                    db.write().unwrap().insert("follows", f).unwrap();
                }
                i += 1;
            }
            i
        })
    };
    std::thread::sleep(Duration::from_millis(1500));
    let dest = dir("cut-dest");
    let out = a.exec(&format!("BACKUP CLUSTER TO '{}'", dest.display())).unwrap();
    let m = match out.finished().unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    };
    let ts: u64 = m.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(!m.contains("NOT on"), "{m}");
    let urls = [a.url.clone(), b.url.clone(), c.url.clone()];
    std::thread::sleep(Duration::from_millis(500));
    writing.store(false, Ordering::Relaxed);
    let written = writer.join().unwrap();
    // The cluster goes away: a restored database's placement still names
    // the nodes it was backed up from, and a query would reach them.
    remove(vec![a, b, c]);
    // Each node's backup at the instant, restored into a fresh database,
    // read from the shards it holds.
    let mut people = std::collections::BTreeSet::new();
    let mut edges = Vec::new();
    let mut restored = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let d = dir(&format!("cut-restore-{i}"));
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        let sql = format!("RESTORE FROM '{}' NODE '{url}' AS OF {ts}", dest.display());
        match db.execute(&sql).unwrap().finished().unwrap() {
            Outcome::Ack(_) => {}
            other => panic!("{other:?}"),
        }
        let rows = |db: &mut Db, sql: &str| {
            db.query(&format!("{sql} LIMIT 1000000 WITH (partial_results, deadline_ms = 20000)"))
                .unwrap()
                .rows
        };
        for row in rows(&mut db, "SELECT id FROM people") {
            people.insert(row.doc.path("id").unwrap().as_str().unwrap().to_string());
        }
        for row in rows(&mut db, "SELECT src, dst FROM follows") {
            let src = row.doc.path("src").unwrap().as_str().unwrap().to_string();
            let dst = row.doc.path("dst").unwrap().as_str().unwrap().to_string();
            edges.push((src, dst));
        }
        drop(db);
        restored.push(d);
    }
    eprintln!(
        "resilience: {written} persons written, {} in the cut, {} edges in the cut",
        people.len(),
        edges.len()
    );
    assert!(!edges.is_empty() && people.len() < written, "the cut is inside the load");
    for (src, dst) in &edges {
        assert!(people.contains(src) && people.contains(dst), "edge {src}->{dst} lost an endpoint");
    }
    let _ = std::fs::remove_dir_all(&dest);
    for d in restored {
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// The pause that looked like a partition: a seal of a large vector
/// memtable built its graph under the write lock, and a node answered
/// nothing meanwhile. With the console's maintenance thread the seal
/// freezes and builds off the lock, and a point lookup through the
/// console answers while it does.
#[test]
#[ignore]
fn a_point_lookup_answers_while_a_large_vector_seal_builds() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Command, Stdio};
    let d = dir("seal-console");
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro"))
        .args(["--json", "--dir", d.to_str().unwrap(), "serve", "--port", "0"])
        .env("CELASTRO_MEMTABLE_MAX_VECTORS", "20000")
        .env("CELASTRO_MEMTABLE_MAX_BYTES", "1073741824")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn celastro");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
    let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();
    let post = |sql: &str| -> (String, Duration) {
        let body =
            celastro::json::to_string(&Value::obj(vec![("sql".into(), Value::Str(sql.into()))]));
        let t0 = Instant::now();
        let mut s = std::net::TcpStream::connect(&addr).unwrap();
        write!(
            s,
            "POST /api/query?t={token} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut reply = String::new();
        s.read_to_string(&mut reply).unwrap();
        (reply, t0.elapsed())
    };
    let (r, _) = post("CREATE COLLECTION v (id TEXT PRIMARY KEY)");
    assert!(r.contains("\"ok\":true"), "{r}");
    let (r, _) = post(
        "CREATE INDEX v_emb ON v USING vector (embedding) WITH (dims = 32, metric = 'cosine')",
    );
    assert!(r.contains("\"ok\":true"), "{r}");
    // 20,000 vectors in batches: the cap is reached on the last batch and
    // the seal freezes there; the graph builds on the maintenance thread.
    let mut seed = 7u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 40) as f32 / (1u64 << 24) as f32) - 0.5
    };
    let started = Instant::now();
    for batch in 0..20 {
        let mut rows = Vec::new();
        for i in 0..1000 {
            let n = batch * 1000 + i;
            let emb: Vec<String> = (0..32).map(|_| format!("{:.4}", next())).collect();
            rows.push(format!("('{{\"id\":\"v{n:05}\",\"embedding\":[{}]}}')", emb.join(",")));
        }
        let (r, _) = post(&format!("INSERT INTO v VALUES {}", rows.join(", ")));
        assert!(r.contains("\"ok\":true"), "batch {batch}: {}", &r[r.len().saturating_sub(300)..]);
    }
    eprintln!("resilience: 20,000 vectors written in {:.1?}", started.elapsed());
    // Lookups while the seal builds: each must answer within a second, and
    // some must land before the segment exists.
    let (mut before_seal, mut slowest) = (0usize, Duration::ZERO);
    let t0 = Instant::now();
    let mut sealed_at = None;
    while t0.elapsed() < Duration::from_secs(240) {
        let (r, took) = post("SELECT id FROM v WHERE id = 'v00042' LIMIT 1");
        assert!(r.contains("v00042"), "{r}");
        slowest = slowest.max(took);
        assert!(took < Duration::from_secs(1), "a lookup took {took:?} during the seal");
        let (seg, _) = post("SHOW SEGMENTS v");
        // The summary lists one line per segment with its document count;
        // the sealed one holds every vector.
        if seg.contains("20000") {
            sealed_at = Some(t0.elapsed());
            break;
        }
        before_seal += 1;
        std::thread::sleep(Duration::from_millis(200));
    }
    eprintln!(
        "resilience: {before_seal} lookups before the seal landed, slowest {slowest:?}, seal in {:?}",
        sealed_at
    );
    assert!(before_seal > 0, "the seal landed before a single lookup could run");
    assert!(sealed_at.is_some(), "the seal never landed");
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&d);
}

/// R2(17): the map changing under a statement. Shards move at every
/// step -- one shard after another, round-robin over three nodes -- under
/// a write load and a scan that never stops. The property: a scan answers
/// every key acknowledged before it began exactly once, or is refused
/// naming the move; never a short answer, never a key twice. And the
/// moves themselves complete: every shard ends where its last move sent
/// it, and every acknowledged key answers there.
#[test]
#[ignore]
fn a_scan_under_moves_at_every_step_answers_each_key_once_or_is_refused() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("scan-a");
    let b = Node::start("scan-b");
    let c = Node::start("scan-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['k-2', 'k-4', 'k-6'])");
    for i in 0..300usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    let urls = [a.url.clone(), b.url.clone(), c.url.clone()];
    let acked: Arc<Mutex<Vec<String>>> =
        Arc::new(Mutex::new((0..300).map(|i| format!("k-{i:06}")).collect()));
    let moving = Arc::new(AtomicBool::new(true));
    // The mover: every 400 ms, the next shard to the next node.
    let mover = {
        let (db, urls, moving) = (a.db.clone(), urls.clone(), moving.clone());
        std::thread::spawn(move || {
            let (mut done, mut refused) = (0usize, Vec::new());
            for step in 0..24usize {
                std::thread::sleep(Duration::from_millis(400));
                let shard = step % 4;
                let to = &urls[(step / 4 + shard + 1) % 3];
                let sql = format!("MOVE SHARD {shard} OF items TO '{to}'");
                let t0 = Instant::now();
                let out = db.write().unwrap().execute(&sql);
                let under_lock = t0.elapsed();
                match out.and_then(|o| o.finished_with(&db)) {
                    Ok(Outcome::Ack(m)) => {
                        assert!(m.contains("moved") || m.contains("already on"), "{sql}: {m}");
                        eprintln!(
                            "resilience: step {step}: {sql} in {:.1?} ({under_lock:.1?} under the lock)",
                            t0.elapsed()
                        );
                        done += 1;
                    }
                    Ok(other) => panic!("{sql}: {other:?}"),
                    Err(e) => {
                        eprintln!(
                            "resilience: step {step}: {sql} refused after {:.1?}: {e}",
                            t0.elapsed()
                        );
                        refused.push(format!("{sql}: {e}"));
                    }
                }
            }
            moving.store(false, Ordering::Relaxed);
            (done, refused)
        })
    };
    // The load: keys over every shard, through b as the console would run
    // them -- an INSERT statement, its carry to the holder with b's lock
    // let go -- each acknowledged one remembered; a write refused while
    // its shard moves is not a failure.
    let load = {
        let (db, acked, moving) = (b.db.clone(), acked.clone(), moving.clone());
        std::thread::spawn(move || {
            let (mut n, mut refused, mut disagreed) = (300usize, 0usize, 0usize);
            while moving.load(Ordering::Relaxed) {
                let sql = format!(
                    r#"INSERT INTO items VALUES ('{{"id":"k-{n:06}","n":{n},"body":"row {n}"}}')"#
                );
                let out = db.write().unwrap().execute(&sql);
                match out.and_then(|o| o.finished_with(&db)) {
                    Ok(_) => acked.lock().unwrap().push(format!("k-{n:06}")),
                    Err(e) => {
                        let e = e.to_string();
                        // A key refused as another node's is the map moved
                        // twice between the plan and the carry: once is
                        // followed; twice is refused, and retried by a client.
                        if e.contains("placement maps disagree") {
                            disagreed += 1;
                        } else {
                            assert!(
                                e.contains("moving")
                                    || e.contains("did not answer")
                                    || e.contains("deadline"),
                                "a refusal that is not the move: {e}"
                            );
                        }
                        refused += 1;
                    }
                }
                n += 1;
                std::thread::sleep(Duration::from_millis(5));
            }
            (n - 300, refused, disagreed)
        })
    };
    // The scan: through c, under c's read lock as the console runs a
    // read, every key, again and again.
    let (mut scans, mut complete, mut refused_scans) = (0usize, 0usize, Vec::new());
    while moving.load(Ordering::Relaxed) {
        let before: Vec<String> = acked.lock().unwrap().clone();
        let r =
            c.db.read().unwrap().read("SELECT id FROM items LIMIT 100000").and_then(|o| o.rows());
        scans += 1;
        match r {
            Ok(r) => {
                let mut ids: Vec<String> = r
                    .rows
                    .iter()
                    .map(|x| x.doc.path("id").and_then(|v| v.as_str()).unwrap().to_string())
                    .collect();
                let n = ids.len();
                ids.sort();
                ids.dedup();
                assert_eq!(ids.len(), n, "scan {scans} answered a key twice");
                for k in &before {
                    assert!(
                        ids.binary_search(k).is_ok(),
                        "scan {scans} lost {k}, acknowledged before it began"
                    );
                }
                assert!(
                    r.missing.is_empty(),
                    "scan {scans} was short without asking: {:?}",
                    r.missing
                );
                complete += 1;
            }
            Err(e) => {
                let e = e.to_string();
                assert!(
                    e.contains("not on this node")
                        || e.contains("moving")
                        || e.contains("did not answer")
                        || e.contains("deadline"),
                    "scan {scans} refused for something that is not the move: {e}"
                );
                refused_scans.push(e);
            }
        }
    }
    let (moves, move_refusals) = mover.join().unwrap();
    let (writes, write_refusals, disagreed) = load.join().unwrap();
    eprintln!(
        "resilience: {moves} moves done, {} refused; {writes} writes, {write_refusals} refused \
         ({disagreed} as the map moved twice); {scans} scans, {complete} complete, {} refused naming \
         the move",
        move_refusals.len(),
        refused_scans.len()
    );
    for r in &move_refusals {
        eprintln!("resilience: move refused: {r}");
    }
    for r in &move_refusals {
        assert!(
            r.contains("already on")
                || r.contains("not on this node")
                || r.contains("already moving"),
            "a move refused for something that is not the map moving under it: {r}"
        );
    }
    assert!(moves >= 12, "the moves ran: {moves} of 24, refused: {move_refusals:?}");
    assert!(complete >= 10, "scans completed between the moves: {complete} of {scans}");
    // After the last move: every node agrees, every key answers everywhere.
    std::thread::sleep(Duration::from_millis(500));
    let acked = acked.lock().unwrap().clone();
    for n in [&a, &b, &c] {
        let r = n.query("SELECT count(*) AS c FROM items").unwrap();
        let count = r.rows[0].doc.path("c").and_then(|v| v.as_i64()).unwrap() as usize;
        assert_eq!(
            count,
            acked.len(),
            "every acknowledged write, and nothing else, through {}",
            n.url
        );
    }
    for key in acked.iter().step_by(11) {
        let r = c.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 1")).unwrap();
        assert_eq!(r.rows.len(), 1, "{key} after the moves");
    }
    remove(vec![a, b, c]);
}
