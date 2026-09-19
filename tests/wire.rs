//! Three nodes in one process, each with its own directory and its own wire
//! listener, sharing a collection whose shards are spread one per node.
//! Every node coordinates; every node answers what one process answers.

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::plan::exec::QueryResult;
use celastro::{Error, Value};

/// The token is process-wide, so the tests in this file take turns.
static ENV: Mutex<()> = Mutex::new(());
const TOKEN: &str = "wire-test-token";

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-wire-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A node: a database with an address, served on a loopback port.
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

    /// A node on a given port with a given directory: what a node that
    /// comes back after a restart is -- the same address, the same data.
    fn start_at(tag: &str, port: u16, reuse: Option<PathBuf>) -> Node {
        Node::start_role(tag, port, reuse, celastro::engine::Role::Data)
    }

    fn start_role(
        tag: &str,
        port: u16,
        reuse: Option<PathBuf>,
        role: celastro::engine::Role,
    ) -> Node {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let dir = reuse.unwrap_or_else(|| dir(tag));
        let mut opts = DbOpts::default();
        opts.role = role;
        opts.node = Some(url.clone());
        let db = Arc::new(RwLock::new(Db::open(&dir, opts).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Node { url, db, stop, dir }
    }

    fn exec(&self, sql: &str) -> celastro::Result<Outcome> {
        self.db.write().unwrap().execute(sql)
    }

    fn ack(&self, sql: &str) -> String {
        // `exec` let go of the lock; a deferred copy runs here without it.
        let out = self.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        match out.finished_with(&self.db).unwrap_or_else(|e| panic!("{sql}: {e}")) {
            Outcome::Ack(m) => m,
            other => panic!("{sql}: {other:?}"),
        }
    }

    fn query(&self, sql: &str) -> celastro::Result<QueryResult> {
        self.db.write().unwrap().query(sql)
    }

    fn local_shards(&self, collection: &str) -> Vec<usize> {
        let db = self.db.write().unwrap();
        db.shards(collection).unwrap().iter().map(|s| s.index).collect()
    }

    fn docs_here(&self, collection: &str) -> usize {
        let db = self.db.write().unwrap();
        let ts = db.now_ts();
        db.shards(collection).unwrap().iter().map(|s| s.num_docs(ts)).sum()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // The listener and every open connection let go within the wire's
        // idle poll; the directory is left for the test to remove, since a
        // restart reopens it.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// A stopped node has let go of its port and its connections after this.
fn settle() {
    std::thread::sleep(std::time::Duration::from_millis(700));
}

fn shape(r: &QueryResult) -> Vec<(String, String, Option<u32>, Option<u32>)> {
    r.rows
        .iter()
        .map(|row| {
            (
                row.key.clone(),
                celastro::json::to_string(&row.doc),
                row.score.map(f32::to_bits),
                row.distance.map(f32::to_bits),
            )
        })
        .collect()
}

fn doc(i: usize) -> Value {
    let words = ["graph", "search", "vector", "index", "segment", "fusion", "rank"];
    let body = format!("{} {} {}", words[i % 7], words[(i * 3) % 7], words[(i * 5) % 7]);
    celastro::json::parse(&format!(
        r#"{{"id":"doc-{i:03}","tenant":"t{}","n":{i},"body":"{body}","embedding":[{},{},{},1.0]}}"#,
        i % 3,
        (i % 7) as f32 / 7.0,
        (i % 5) as f32 / 5.0,
        (i % 3) as f32 / 3.0,
    ))
    .unwrap()
}

const CREATE: &str = "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) \
                      PARTITION BY (tenant) WITH (splits = ['t1', 't2'])";
const INDEXES: &[&str] = &[
    "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
    "CREATE INDEX items_emb ON items USING vector (embedding) WITH (dims = 4, metric = 'cosine')",
];
const QUERIES: &[&str] = &[
    "SELECT id FROM items ORDER BY hybrid(text_match(body, 'graph search'), embedding <=> \
     [0.5, 0.5, 0.5, 1.0], method => 'linear') LIMIT 10",
    "SELECT id FROM items ORDER BY embedding <=> [0.1, 0.9, 0.2, 1.0] LIMIT 7",
    "SELECT id FROM items WHERE text_match(body, 'seg*') LIMIT 100",
    "SELECT id, n FROM items ORDER BY n DESC LIMIT 12 OFFSET 3",
    "SELECT id FROM items LIMIT 20",
    "SELECT id FROM items WHERE tenant = 't1' AND n > 40 LIMIT 50",
    "SELECT id FROM items ORDER BY hybrid(text_match(body, 'rank segment'), method => \
     'linear') LIMIT 8",
];

/// A coordinator holds no shards: a placement, a rebalance and a move never
/// land one on it; every DDL reaches it, so it plans and answers over the
/// data nodes' shards exactly as they do; and `SHOW HEALTH` says what it is.
#[test]
fn a_coordinator_holds_no_shards_and_answers_over_the_data_nodes() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let c = Node::start_role("coord-c", 0, None, celastro::engine::Role::Coordinator);
    let a = Node::start("coord-a");
    let b = Node::start("coord-b");
    // Everyone attaches everyone, as the chart does.
    for (x, y) in [(&c, &a), (&c, &b), (&a, &b), (&a, &c), (&b, &a), (&b, &c)] {
        x.ack(&format!("ATTACH NODE '{}'", y.url));
    }
    // Created at the coordinator: three shards over the two data nodes,
    // none here.
    let m = c.ack(CREATE);
    assert!(m.contains("3 shard(s) on"), "{m}");
    assert!(c.local_shards("items").is_empty(), "the coordinator holds nothing");
    assert_eq!(a.local_shards("items").len() + b.local_shards("items").len(), 3);
    // A DDL run at a data node reaches the coordinator's catalog.
    a.ack(INDEXES[0]);
    a.ack(INDEXES[1]);
    let cat = c.ack("SHOW CATALOG items");
    assert!(cat.contains("index items_body") && cat.contains("index items_emb"), "{cat}");
    // Writes through the coordinator land on the owners; queries fan out.
    for i in 0..60usize {
        c.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    a.ack("FLUSH items");
    let one_dir = dir("coord-one");
    let mut one = Db::open(&one_dir, DbOpts::default()).unwrap();
    for sql in [CREATE, INDEXES[0], INDEXES[1]] {
        one.execute(sql).unwrap();
    }
    for i in 0..60usize {
        one.insert("items", doc(i)).unwrap();
    }
    for q in QUERIES {
        let want = shape(&one.query(q).unwrap());
        assert_eq!(shape(&c.query(q).unwrap()), want, "{q} at the coordinator");
    }
    // Nothing moves a shard onto it.
    let e = a.exec(&format!("MOVE SHARD 0 OF items TO '{}'", c.url)).unwrap_err().to_string();
    assert!(e.contains("is a coordinator"), "{e}");
    let sql = format!("CREATE COLLECTION more (id TEXT PRIMARY KEY) WITH (nodes = ['{}'])", c.url);
    let e = c.exec(&sql).unwrap_err().to_string();
    assert!(e.contains("is a coordinator"), "{e}");
    c.ack("REBALANCE items");
    assert!(c.local_shards("items").is_empty(), "a rebalance skips the coordinator");
    let health = a.ack("SHOW HEALTH");
    assert!(health.contains(&format!("node {}: up, coordinator, ", c.url)), "{health}");
    assert!(health.starts_with(&format!("this node: {}, data, ", a.url)), "{health}");
    // A coordinator that arrives later -- or restarts from an empty volume
    // -- learns the collections at ATTACH and answers at once.
    let d = Node::start_role("coord-d", 0, None, celastro::engine::Role::Coordinator);
    assert!(d.exec("SHOW CATALOG items").is_err(), "knows nothing yet");
    d.ack(&format!("ATTACH NODE '{}'", a.url));
    let cat = d.ack("SHOW CATALOG items");
    assert!(cat.contains("index items_body") && cat.contains("shard 2 on"), "{cat}");
    assert!(d.local_shards("items").is_empty());
    let want = shape(&one.query(QUERIES[0]).unwrap());
    assert_eq!(shape(&d.query(QUERIES[0]).unwrap()), want, "the late coordinator answers");
    let d_dir = d.dir.clone();
    drop(d);
    settle();
    let _ = std::fs::remove_dir_all(&d_dir);
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&one_dir);
}

/// `SHOW HEALTH` names every node with whether it answers and every shard
/// with whether its holder does; a node that went away is DOWN and its
/// shard UNREACHABLE, from any other node.
#[test]
fn show_health_names_every_node_and_shard_and_a_lost_node_is_down() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("health-a");
    let b = Node::start("health-b");
    let c = Node::start("health-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    let text = a.ack("SHOW HEALTH");
    assert!(text.starts_with(&format!("this node: {}", a.url)), "{text}");
    for n in [&b, &c] {
        assert!(text.contains(&format!("node {}: up, data, celastro ", n.url)), "{text}");
    }
    assert!(text.contains("shard 0 of `items`: on") && text.contains("reachable"), "{text}");
    assert!(text.ends_with("3 of 3 node(s) answer; 0 shard(s) unreachable"), "{text}");
    let c_url = c.url.clone();
    let c_dir = c.dir.clone();
    drop(c);
    settle();
    let text = b.ack("SHOW HEALTH");
    assert!(text.contains(&format!("node {c_url}: DOWN")), "{text}");
    assert!(text.contains(&format!("shard 2 of `items`: on {c_url}, UNREACHABLE")), "{text}");
    assert!(text.ends_with("2 of 3 node(s) answer; 1 shard(s) unreachable"), "{text}");
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&c_dir);
}

/// An aggregate over shards on three nodes is the aggregate over the rows:
/// each holder folds its own, the coordinator merges the partials, and
/// every node answers what one process answers.
#[test]
fn an_aggregate_over_three_nodes_answers_what_one_process_answers() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("agg-a");
    let b = Node::start("agg-b");
    let c = Node::start("agg-c");
    let one_dir = dir("agg-one");
    let mut one = Db::open(&one_dir, DbOpts::default()).unwrap();
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    for sql in [CREATE, INDEXES[0]] {
        a.ack(sql);
        one.execute(sql).unwrap();
    }
    for i in 0..90usize {
        if i == 60 {
            a.ack("FLUSH items");
            one.execute("FLUSH items").unwrap();
        }
        [&a, &b, &c][i % 3].db.write().unwrap().insert("items", doc(i)).unwrap();
        one.insert("items", doc(i)).unwrap();
    }
    assert!(b.db.write().unwrap().delete_key("items", "t1\u{1}doc-031").unwrap());
    assert!(one.delete_key("items", "t1\u{1}doc-031").unwrap());
    let rows = |r: &QueryResult| -> Vec<(String, String)> {
        r.rows.iter().map(|row| (row.key.clone(), celastro::json::to_string(&row.doc))).collect()
    };
    for q in [
        "SELECT count(*) FROM items",
        "SELECT count(*), sum(n), min(n), max(n), avg(n) FROM items WHERE n >= 10",
        "SELECT tenant, count(*) AS c, sum(n) AS s FROM items GROUP BY tenant ORDER BY s DESC",
        "SELECT count(*) FROM items WHERE text_match(body, 'graph')",
        "SELECT tenant, count(*) FROM items WHERE tenant = 't2' GROUP BY tenant",
    ] {
        let want = rows(&one.query(q).unwrap());
        assert!(!want.is_empty(), "{q}");
        for n in [&a, &b, &c] {
            assert_eq!(rows(&n.query(q).unwrap()), want, "{q} on {}", n.url);
        }
    }
    let r = a.query("SELECT count(*) FROM items").unwrap();
    assert_eq!(rows(&r), vec![(String::new(), r#"{"count(*)":89}"#.to_string())]);
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&one_dir);
}

/// The whole of M1's exit criterion: a collection spread one shard per
/// node, written through any node, answers on every node exactly what the
/// same corpus answers in one process -- bit for bit, through a real
/// transport -- with DDL and the operational statements reaching every
/// holder and a placement that survives a restart.
#[test]
fn a_collection_spread_over_three_nodes_answers_what_one_process_answers() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("a");
    let b = Node::start("b");
    let c = Node::start("c");
    // The reference: the same corpus in one process.
    let one_dir = dir("one");
    let mut one = Db::open(&one_dir, DbOpts::default()).unwrap();

    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    let e = a.exec(&format!("ATTACH NODE '{}'", a.url)).unwrap_err().to_string();
    assert!(e.contains("itself"), "{e}");
    let e = a.exec("ATTACH NODE 'tcp://127.0.0.1:1'").unwrap_err().to_string();
    assert!(
        e.contains("did not answer") || e.contains("refused"),
        "a dead address is refused: {e}"
    );

    let m = a.ack(CREATE);
    assert!(m.contains("3 shard(s) on"), "{m}");
    one.execute(CREATE).unwrap();
    for ix in INDEXES {
        let m = a.ack(ix);
        assert!(m.contains("and on"), "the index reached the other holders: {m}");
        one.execute(ix).unwrap();
    }
    // One shard per node, by index, and every node knows the whole map.
    assert_eq!(a.local_shards("items"), vec![0]);
    assert_eq!(b.local_shards("items"), vec![1]);
    assert_eq!(c.local_shards("items"), vec![2]);
    for n in [&a, &b, &c] {
        let cat = n.ack("SHOW CATALOG items");
        assert!(cat.contains("shard 0 on") && cat.contains("shard 2 on"), "{cat}");
        assert!(cat.contains("index items_body"), "{cat}");
    }

    // Writes through every node, routed to the owner by key.
    for i in 0..90usize {
        let via = [&a, &b, &c][i % 3];
        if i == 60 {
            a.ack("FLUSH items");
            one.execute("FLUSH items").unwrap();
        }
        via.db.write().unwrap().insert("items", doc(i)).unwrap();
        one.insert("items", doc(i)).unwrap();
    }
    for key in ["t0\u{1}doc-003", "t1\u{1}doc-031", "t2\u{1}doc-071"] {
        assert!(b.db.write().unwrap().delete_key("items", key).unwrap(), "{key}");
        assert!(one.delete_key("items", key).unwrap());
    }
    assert_eq!((a.docs_here("items"), b.docs_here("items"), c.docs_here("items")), (29, 29, 29));

    // Every node answers what one process answers, bit for bit.
    for q in QUERIES {
        let want = shape(&one.query(q).unwrap());
        assert!(!want.is_empty(), "{q}");
        for n in [&a, &b, &c] {
            assert_eq!(shape(&n.query(q).unwrap()), want, "{q} on {}", n.url);
        }
    }
    // A plan lists every shard, the remote ones as their holders rendered them.
    let plan = match a.exec(&format!("EXPLAIN ANALYZE {}", QUERIES[0])).unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    for i in 0..3 {
        assert!(plan.contains(&format!("shard {i} (manifest")), "{plan}");
    }
    assert!(plan.contains("memtable") && plan.contains("docs="), "{plan}");

    // A partition-scoped statement prunes the shards on other nodes.
    let plan = match c.exec(&format!("EXPLAIN ANALYZE {}", QUERIES[5])).unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(plan.contains("shard 0: PRUNED") && plan.contains("shard 2: PRUNED"), "{plan}");

    // DELETE with a predicate: the fan-out finds the keys, the owners delete.
    let m = c.ack("DELETE FROM items WHERE text_match(body, 'fusion')");
    let one_m = match one.execute("DELETE FROM items WHERE text_match(body, 'fusion')").unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    };
    assert_eq!(m, one_m);
    for q in QUERIES {
        let want = shape(&one.query(q).unwrap());
        for n in [&a, &b, &c] {
            assert_eq!(shape(&n.query(q).unwrap()), want, "after the delete: {q} on {}", n.url);
        }
    }

    // Operational statements fan out; a LOCAL one does not.
    let m = b.ack("FLUSH items");
    assert!(m.contains("and on"), "{m}");
    one.execute("FLUSH items").unwrap();
    let m = b.ack("LOCAL FLUSH items");
    assert!(!m.contains("and on"), "{m}");
    // A row in a memtable when the index is dropped is sealed without it
    // and answers no rows for that path until compaction, on both sides.
    c.db.write().unwrap().insert("items", doc(90)).unwrap();
    one.insert("items", doc(90)).unwrap();
    let m = a.ack("DROP INDEX items_body ON items");
    assert!(m.contains("and on"), "{m}");
    let e = c.query(QUERIES[2]).unwrap_err().to_string();
    assert!(e.contains("no full-text index"), "the drop reached c: {e}");
    a.ack(INDEXES[0]);
    one.execute("DROP INDEX items_body ON items").unwrap();
    one.execute(INDEXES[0]).unwrap();
    assert_eq!(shape(&c.query(QUERIES[2]).unwrap()), shape(&one.query(QUERIES[2]).unwrap()));

    // The statistics epoch ages by every holder's writes: after more writes
    // than the refresh interval, issued at one node, a text score at another
    // is the fresh corpus's, as it is in one process.
    // Every one of them lands on `b` or `c`, none on `a`: `a`'s own counter
    // does not move, so only the other holders' counters can tell it the
    // corpus changed. More than the refresh interval (512) in all, and all
    // of them about one term, so the change is not a uniform shift of every
    // IDF that a normalised fusion would cancel.
    // Warm both caches first: the DROP INDEX above emptied them, and a cold
    // cache gathers fresh statistics whatever the counters say.
    for q in [QUERIES[0], QUERIES[6]] {
        assert_eq!(shape(&a.query(q).unwrap()), shape(&one.query(q).unwrap()), "warm: {q}");
    }
    for i in (100..1000usize).filter(|i| i % 3 != 0) {
        let d = celastro::json::parse(&format!(
            r#"{{"id":"doc-{i:03}","tenant":"t{}","n":{i},"body":"graph","embedding":[0.5,0.5,0.5,1.0]}}"#,
            i % 3
        ))
        .unwrap();
        b.db.write().unwrap().insert("items", d.clone()).unwrap();
        one.insert("items", d).unwrap();
    }
    for q in [QUERIES[0], QUERIES[6]] {
        assert_eq!(shape(&a.query(q).unwrap()), shape(&one.query(q).unwrap()), "epoch: {q}");
    }

    // A node cannot be detached while it holds a shard.
    let e = a.exec(&format!("DETACH NODE '{}'", b.url)).unwrap_err().to_string();
    assert!(e.contains("holds 1 shard(s); move them first: MOVE SHARD 1 OF items TO"), "{e}");
    // And an export needs every shard here.
    let e = match a.db.write().unwrap().export_collection("items") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("an export of a spread collection was accepted"),
    };
    assert!(e.contains("every shard on this node"), "{e}");

    // The placement survives a restart of a node, and its answers with it.
    let a_dir = a.dir.clone();
    let a_url = a.url.clone();
    drop(a);
    settle();
    let a = {
        let listener = TcpListener::bind(a_url.trim_start_matches("tcp://")).unwrap();
        let mut opts = DbOpts::default();
        opts.node = Some(a_url.clone());
        let db = Arc::new(RwLock::new(Db::open(&a_dir, opts).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Node { url: a_url, db, stop, dir: a_dir }
    };
    assert_eq!(a.local_shards("items"), vec![0]);
    for q in QUERIES {
        assert_eq!(shape(&a.query(q).unwrap()), shape(&one.query(q).unwrap()), "{q}");
        assert_eq!(shape(&b.query(q).unwrap()), shape(&one.query(q).unwrap()), "{q}");
    }

    // DROP COLLECTION reaches every holder.
    let m = b.ack("DROP COLLECTION items");
    assert!(m.contains("and on"), "{m}");
    for n in [&a, &b, &c] {
        assert!(n.exec("SHOW CATALOG items").is_err(), "{} still has it", n.url);
    }
    a.ack(&format!("DETACH NODE '{}'", b.url));
    for d in [&a.dir, &b.dir, &c.dir, &one_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A node that stops answering is a deadline at the coordinator -- the same
/// rule the simulator pinned -- so `partial_results` reports its shard and
/// nothing else changes; a write to its shard is refused naming it; and a
/// request without the token, or with another wire version, is refused.
#[test]
fn a_node_that_does_not_answer_is_a_deadline_and_nothing_quieter() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("dead-a");
    let b = Node::start("dead-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(
        "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL) \
         PARTITION BY (tenant) WITH (splits = ['t1'])",
    );
    a.ack(INDEXES[0]);
    for i in 0..20usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    let all = a.query("SELECT id FROM items LIMIT 100").unwrap();
    assert_eq!(all.rows.len(), 20);

    // b goes away: its listener stops and its port closes.
    let b_url = b.url.clone();
    let b_dir = b.dir.clone();
    drop(b);
    settle();
    let e = a.query("SELECT id FROM items LIMIT 100").unwrap_err();
    assert!(matches!(e, Error::Deadline(_)), "{e}");
    assert!(e.to_string().contains(&b_url), "{e}");
    let r = a.query("SELECT id FROM items LIMIT 100 WITH (partial_results)").unwrap();
    assert_eq!(r.missing, vec!["shard 1"]);
    assert_eq!(r.rows.len(), 7, "a's own tenant only: every third document");
    assert!(r.rows.iter().all(|row| row.key.starts_with("t0\u{1}")));
    let e = a.db.write().unwrap().insert("items", doc(1)).unwrap_err().to_string();
    assert!(e.contains(&b_url), "a write to the dead node's shard names it: {e}");

    // The wrong token, and the wrong version, are refused by name.
    std::env::set_var(celastro::wire::TOKEN_ENV, "another");
    // A node that accepts and never answers is the case a deadline exists
    // for: the attach fails within the budget, naming the node, rather than
    // waiting on it.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_url = format!("tcp://127.0.0.1:{}", silent.local_addr().unwrap().port());
    std::thread::spawn(move || {
        let mut kept = Vec::new();
        for s in silent.incoming().flatten() {
            kept.push(s);
        }
    });
    let hurried_dir = dir("hurried");
    let mut opts = DbOpts::default();
    opts.node = Some("tcp://127.0.0.1:1".into());
    opts.statement_deadline_ms = Some(500);
    let mut hurried = Db::open(&hurried_dir, opts).unwrap();
    let t0 = std::time::Instant::now();
    let e = hurried.execute(&format!("ATTACH NODE '{silent_url}'")).unwrap_err();
    assert!(matches!(e, Error::Deadline(_)), "{e}");
    assert!(e.to_string().contains(&silent_url), "{e}");
    assert!(t0.elapsed() < std::time::Duration::from_secs(5), "waited {:?}", t0.elapsed());
    let _ = std::fs::remove_dir_all(&hurried_dir);

    let stranger = Node::start("stranger");
    let e = stranger.exec(&format!("ATTACH NODE '{}'", a.url)).unwrap_err().to_string();
    assert!(e.contains("token refused"), "{e}");
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let mut s = std::net::TcpStream::connect(a.url.trim_start_matches("tcp://")).unwrap();
    use std::io::{Read, Write};
    let mut frame = vec![9u8]; // a wire version nobody speaks
    frame.extend_from_slice(&[0, 1, 0, 0, 0]);
    let mut out = (frame.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&frame);
    s.write_all(&out).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut resp = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut resp).unwrap();
    let text = String::from_utf8_lossy(&resp);
    assert!(resp[0] == 1 && text.contains("wire version 9"), "{text}");
    for d in [&a.dir, &b_dir, &stranger.dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A walk whose edges and nodes are both spread over three nodes answers,
/// from any node, what one process answers -- the frontier crosses the wire
/// at every hop through `expand` and `present`, the shards on other nodes
/// bind the same key set the coordinator did, and the plan shows the hops.
#[test]
fn a_walk_over_collections_spread_over_three_nodes_answers_what_one_process_answers() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("walk-a");
    let b = Node::start("walk-b");
    let c = Node::start("walk-c");
    let one_dir = dir("walk-one");
    let mut one = Db::open(&one_dir, DbOpts::default()).unwrap();
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));

    const EDGES: &str =
        "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT \
                         NOT NULL) WITH (nodes_of = 'items', splits = ['e090', 'e180'])";
    const ADJ: &str = "CREATE INDEX cites_adj ON cites USING adjacency (src, dst)";
    for sql in [CREATE, INDEXES[0], INDEXES[1], EDGES, ADJ] {
        let m = a.ack(sql);
        assert!(m.contains(" on ") || m.contains("and on"), "{sql}: {m}");
        one.execute(sql).unwrap();
    }
    assert_eq!(b.local_shards("cites"), vec![1]);
    for i in 0..90usize {
        let via = [&a, &b, &c][i % 3];
        if i == 60 {
            a.ack("FLUSH items");
            a.ack("FLUSH cites");
            one.execute("FLUSH items").unwrap();
            one.execute("FLUSH cites").unwrap();
        }
        via.db.write().unwrap().insert("items", doc(i)).unwrap();
        one.insert("items", doc(i)).unwrap();
        for (j, step) in [1usize, 3, 11].iter().enumerate() {
            let e = celastro::json::parse(&format!(
                r#"{{"id":"e{:03}","src":"doc-{i:03}","dst":"doc-{:03}","w":{j}}}"#,
                i * 3 + j,
                (i + step) % 90
            ))
            .unwrap();
            via.db.write().unwrap().insert("cites", e.clone()).unwrap();
            one.insert("cites", e).unwrap();
        }
    }
    assert!(b.db.write().unwrap().delete_key("items", "t1\u{1}doc-013").unwrap());
    assert!(one.delete_key("items", "t1\u{1}doc-013").unwrap());

    let walks = [
        "SELECT id FROM items WHERE id WITHIN 2 HOPS OF 'doc-010' VIA cites AND text_match(body, \
         'graph') ORDER BY embedding <=> [0.5, 0.5, 0.5, 1.0] LIMIT 10",
        "SELECT id FROM items WHERE id WITHIN 3 HOPS OF 'doc-002' VIA cites WHERE w > 0 ORDER BY \
         hybrid(text_match(body, 'vector index'), embedding <=> [0.2, 0.2, 0.9, 1.0], method => \
         'linear') LIMIT 6",
        "SELECT id FROM items WHERE id WITHIN 2 HOPS OF 'doc-050' VIA cites REVERSE LIMIT 100",
        "SELECT id FROM items ORDER BY hybrid(text_match(body, 'graph search'), \
         hops(id WITHIN 3 HOPS OF 'doc-005' VIA cites WHERE w > 1)) LIMIT 12 WITH (exact)",
        "SELECT id FROM items WHERE id WITHIN 2 HOPS OF 'doc-011' VIA cites WHERE w > 1 THEN WHERE w < 1 \
         ORDER BY embedding <=> [0.9, 0.1, 0.1, 1.0] LIMIT 10 WITH (exact)",
        "SELECT id FROM items WHERE id WITHIN 2 HOPS OF 'doc-030' VIA cites LIMIT 100 WITH \
         (max_frontier = 5, max_fanout = 2)",
    ];
    for q in walks {
        let r = one.query(q).unwrap();
        let want = (shape(&r), r.cut_walks.clone());
        assert!(!want.0.is_empty(), "{q}");
        for n in [&a, &b, &c] {
            let r = n.query(q).unwrap();
            assert_eq!((shape(&r), r.cut_walks.clone()), want, "{q} on {}", n.url);
            assert!(r.missing.is_empty(), "{q} on {}: {:?}", n.url, r.missing);
        }
    }
    let plan = match c.exec(&format!("EXPLAIN ANALYZE {}", walks[0])).unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(
        plan.contains("walk: WITHIN 2 HOPS OF 'doc-010' VIA cites (index cites_adj, outgoing)"),
        "{plan}"
    );
    // doc-010 cites doc-011, doc-013 (deleted) and doc-021.
    assert!(
        plan.contains("hop 1: 1 key(s) expanded over 3 edge(s): 3 new, 1 dangling, frontier 2"),
        "doc-013 is deleted:\n{plan}"
    );
    assert!(
        plan.contains("hop 2: 2 key(s) expanded over 6 edge(s): 5 new, 0 dangling, frontier 5"),
        "{plan}"
    );

    // A holder of the edges that stops answering is a deadline, or under
    // partial_results a named absence, never a quietly shorter neighbourhood.
    drop(b);
    settle();
    let e = a.query(walks[2]).unwrap_err().to_string();
    assert!(e.contains("did not answer") && e.contains("partial_results"), "{e}");
    let r = a.query(&format!("{} WITH (partial_results)", walks[2])).unwrap();
    assert!(r.rows.len() < 100, "the neighbourhood is short without b's edges");
    assert!(
        r.missing.iter().any(|m| m.starts_with("cites shard 1") || m == "shard 1"),
        "{:?}",
        r.missing
    );

    for d in [&a.dir, &c.dir, &one_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// M4: a shard moves between nodes with no row lost or duplicated, every
/// node agrees on the new map, writes to the shard are refused naming the
/// move while it is pinned, and a node emptied by moves can be detached --
/// and `DETACH NODE` of a node still holding shards says which moves would
/// empty it. Every route is covered: source and target both elsewhere,
/// target here, source here; the answers on every node equal one
/// process's throughout, and the map survives a restart.
#[test]
fn a_shard_moves_between_nodes_and_every_node_agrees() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("move-a");
    let b = Node::start("move-b");
    let c = Node::start("move-c");
    let one_dir = dir("move-one");
    let mut one = Db::open(&one_dir, DbOpts::default()).unwrap();
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    for sql in [CREATE, INDEXES[0], INDEXES[1]] {
        a.ack(sql);
        one.execute(sql).unwrap();
    }
    for i in 0..90usize {
        if i == 60 {
            a.ack("FLUSH items");
            one.execute("FLUSH items").unwrap();
        }
        [&a, &b, &c][i % 3].db.write().unwrap().insert("items", doc(i)).unwrap();
        one.insert("items", doc(i)).unwrap();
    }
    assert!(b.db.write().unwrap().delete_key("items", "t1\u{1}doc-031").unwrap());
    assert!(one.delete_key("items", "t1\u{1}doc-031").unwrap());
    // Exact statistics on both sides: what a move has to keep is the rows
    // and their order, and the cached statistics' refresh points differ
    // between a node that restarted and the single-process reference by
    // design (the epoch is per node), which is not what this test is about.
    let agree = |what: &str, one: &mut Db, nodes: &[&Node]| {
        for base in QUERIES {
            let q = &format!("{base} WITH (exact_scoring)");
            let want = shape(&one.query(q).unwrap());
            for n in nodes {
                let got = n.query(q).unwrap_or_else(|e| panic!("{what}: {q} on {}: {e}", n.url));
                assert_eq!(shape(&got), want, "{what}: {q} on {}", n.url);
            }
        }
    };
    agree("before", &mut one, &[&a, &b, &c]);

    // A node holding shards is not detached; the refusal is the plan.
    let e = a.exec(&format!("DETACH NODE '{}'", b.url)).unwrap_err().to_string();
    assert!(e.contains(&format!("MOVE SHARD 1 OF items TO '{}'", a.url)), "{e}");

    // Source and target both elsewhere: b's shard 1 goes to c.
    let m = a.ack(&format!("MOVE SHARD 1 OF items TO '{}'", c.url));
    assert!(m.contains("moved from") && m.contains("map switched"), "{m}");
    assert_eq!(b.local_shards("items"), Vec::<usize>::new());
    assert_eq!(c.local_shards("items"), vec![1, 2]);
    for n in [&a, &b] {
        let cat = n.ack("SHOW CATALOG items");
        assert!(cat.contains(&format!("shard 1 on {}", c.url)), "{}: {cat}", n.url);
    }
    assert!(c.ack("SHOW CATALOG items").contains("shard 1 on this node"));
    assert!(
        !b.dir.join("collections").join("items").join("shard-0001").exists(),
        "b dropped its copy"
    );
    // A node holding two shards is offered two targets, round-robin over
    // the nodes that remain, so the plan empties it without piling both
    // onto one.
    let e = a.exec(&format!("DETACH NODE '{}'", c.url)).unwrap_err().to_string();
    assert!(e.contains(&format!("MOVE SHARD 1 OF items TO '{}'", a.url)), "{e}");
    assert!(e.contains(&format!("MOVE SHARD 2 OF items TO '{}'", b.url)), "{e}");
    agree("after 1 -> c", &mut one, &[&a, &b, &c]);
    // Writes to the moved shard's keys route to c now, through any node.
    b.db.write().unwrap().insert("items", doc(91)).unwrap();
    one.insert("items", doc(91)).unwrap();
    assert_eq!(c.docs_here("items"), 30 + 29 + 1 - 1 + 1, "shards 1 and 2, plus doc-091 in t1");
    agree("after a write", &mut one, &[&a, &b, &c]);

    // Target here: a pulls shard 2 from c.
    a.ack(&format!("MOVE SHARD 2 OF items TO '{}'", a.url));
    assert_eq!(a.local_shards("items"), vec![0, 2]);
    assert_eq!(c.local_shards("items"), vec![1]);
    agree("after 2 -> a", &mut one, &[&a, &b, &c]);

    // Source here: a's shard 0 goes to b, which held nothing.
    a.ack(&format!("MOVE SHARD 0 OF items TO '{}'", b.url));
    assert_eq!(a.local_shards("items"), vec![2]);
    assert_eq!(b.local_shards("items"), vec![0]);
    agree("after 0 -> b", &mut one, &[&a, &b, &c]);
    for (sql, why) in [
        (format!("MOVE SHARD 0 OF items TO '{}'", b.url), "already on"),
        ("MOVE SHARD 7 OF items TO 'tcp://127.0.0.1:1'".to_string(), "no shard 7"),
        ("MOVE SHARD 0 OF items TO 'tcp://127.0.0.1:1'".to_string(), "not attached"),
    ] {
        let e = a.exec(&sql).unwrap_err().to_string();
        assert!(e.contains(why), "{sql}: {e}");
    }

    // A pinned shard refuses writes naming the move, and takes them again
    // once the pin is let go.
    let files = b.db.write().unwrap().begin_move("items", 0, &c.url).unwrap();
    assert!(files.iter().any(|(n, _)| n == "MANIFEST"), "{files:?}");
    let e = a.db.write().unwrap().insert("items", doc(93)).unwrap_err().to_string();
    assert!(e.contains("shard 0 of `items` is moving to") && e.contains(&c.url), "{e}");
    b.db.write().unwrap().abort_move("items", 0);
    a.db.write().unwrap().insert("items", doc(93)).unwrap();
    one.insert("items", doc(93)).unwrap();
    agree("after the aborted move", &mut one, &[&a, &b, &c]);

    // REBALANCE puts shard i back on the i-th node in attach order.
    let m = a.ack("REBALANCE items");
    assert!(m.contains("moved"), "{m}");
    assert_eq!(a.local_shards("items"), vec![0]);
    assert_eq!(b.local_shards("items"), vec![1]);
    assert_eq!(c.local_shards("items"), vec![2]);
    assert!(a.ack("REBALANCE items").contains("nothing moved"));
    agree("after the rebalance", &mut one, &[&a, &b, &c]);

    // Empty a node, and it detaches.
    a.ack(&format!("MOVE SHARD 1 OF items TO '{}'", c.url));
    a.ack(&format!("DETACH NODE '{}'", b.url));
    agree("after the detach", &mut one, &[&a, &c]);

    // The map survives a restart of the node that pulled.
    let c_dir = c.dir.clone();
    let c_url = c.url.clone();
    drop(c);
    settle();
    let c = {
        let listener = TcpListener::bind(c_url.trim_start_matches("tcp://")).unwrap();
        let mut opts = DbOpts::default();
        opts.node = Some(c_url.clone());
        let db = Arc::new(RwLock::new(Db::open(&c_dir, opts).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Node { url: c_url, db, stop, dir: c_dir }
    };
    assert_eq!(c.local_shards("items"), vec![1, 2]);
    agree("after c's restart", &mut one, &[&a, &c]);

    for d in [&a.dir, &b.dir, &c.dir, &one_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A lost node costs its shards and nothing else. Three nodes, a collection
/// split three ways by key, the third node gone: a statement whose
/// predicate pins the key to a live shard answers without
/// `partial_results`; one that needs the lost shard fails naming that
/// shard and the node (not "shard 0", the placeholder the per-node
/// counters call is sent with); with `partial_results` the scan names the
/// missing shard.
#[test]
fn a_statement_that_never_asks_the_lost_shard_answers_without_partial_results() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("lost-a");
    let b = Node::start("lost-b");
    let c = Node::start("lost-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    // Shard 0: keys below `g` (on a); shard 1: `g`..`p` (on b); shard 2: `p` and up (on c).
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['g', 'p'])");
    a.ack("CREATE INDEX items_body ON items USING fulltext (body)");
    for (i, prefix) in
        ["a", "h", "t"].iter().enumerate().flat_map(|(k, p)| (0..5).map(move |i| (i + k * 5, *p)))
    {
        let d = Value::obj(vec![
            ("id".into(), Value::Str(format!("{prefix}{i:04}"))),
            ("n".into(), Value::Int(i as i64)),
            ("body".into(), Value::Str(format!("row {i}"))),
        ]);
        a.db.write().unwrap().insert("items", d).unwrap();
    }
    assert_eq!(a.query("SELECT id FROM items LIMIT 100").unwrap().rows.len(), 15);

    let c_url = c.url.clone();
    drop(c);
    settle();
    // A key on a's own shard: no need for c, no need for partial_results.
    let r = a.query("SELECT id FROM items WHERE id = 'a0000' LIMIT 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert!(r.missing.is_empty());
    // A key on b's shard, asked at a: the same.
    let r = a.query("SELECT id FROM items WHERE id = 'h0005' LIMIT 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    // A key on c's shard: refused, naming shard 2 and c.
    let e = a.query("SELECT id FROM items WHERE id = 't0010' LIMIT 1").unwrap_err().to_string();
    assert!(e.contains("shard 2") && e.contains(&c_url), "{e}");
    assert!(!e.contains("shard 0"), "the per-node placeholder leaked: {e}");
    // A scan needs every shard: refused without partial_results, short with it.
    let e = a.query("SELECT id FROM items LIMIT 100").unwrap_err().to_string();
    assert!(e.contains(&c_url) && !e.contains("shard 0"), "{e}");
    let r = a.query("SELECT id FROM items LIMIT 100 WITH (partial_results)").unwrap();
    assert_eq!(r.missing, vec!["shard 2"]);
    assert_eq!(r.rows.len(), 10);
    // A text query is scored against every holder's term statistics, so
    // even one pinned to a live shard needs the lost node: refused without
    // partial_results (naming the shard the statistics call failed on),
    // answered from the rest with it.
    let e = a
        .query("SELECT id FROM items WHERE id = 'a0001' AND text_match(body, 'row') LIMIT 5")
        .unwrap_err()
        .to_string();
    assert!(e.contains("shard 2") && e.contains("term_stats"), "{e}");
    let r = a
        .query(
            "SELECT id FROM items WHERE id = 'a0001' AND text_match(body, 'row') LIMIT 5 \
             WITH (partial_results)",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.missing, vec!["shard 2"]);
}

/// A node that comes back is reached: its port refuses for a moment and
/// the dial retries within the statement's deadline instead of failing the
/// statement at the first refusal.
#[test]
fn a_node_that_comes_back_within_the_dial_retry_is_reached() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("back-a");
    let b = Node::start("back-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL) PARTITION BY (tenant) WITH (splits = ['t1'])");
    for i in 0..12usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    assert_eq!(a.query("SELECT id FROM items LIMIT 100").unwrap().rows.len(), 12);
    let port: u16 = b.url.rsplit(':').next().unwrap().parse().unwrap();
    let b_dir = b.dir.clone();
    drop(b);
    settle();
    // b comes back on the same port, with its data, 600 ms from now.
    let back = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(600));
        Node::start_at("back-b-again", port, Some(b_dir))
    });
    let t0 = std::time::Instant::now();
    let r = a.query("SELECT id FROM items LIMIT 100 WITH (deadline_ms = 5000)").unwrap();
    assert_eq!(r.rows.len(), 12, "the statement waited for b and got everything");
    assert!(t0.elapsed() < std::time::Duration::from_secs(4), "{:?}", t0.elapsed());
    let _b = back.join().unwrap();
}

/// What a node misses while it is down or across a split reaches it when
/// it reconnects: the index made and the one dropped while it was away, a
/// collection created without it, whose shard it then builds. The DDL that
/// could not reach it answered with a note, not a refusal.
#[test]
fn a_node_away_through_ddl_catches_up_when_it_reattaches() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("recon-a");
    let b = Node::start("recon-b");
    let c = Node::start("recon-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    a.ack(INDEXES[0]);
    let (c_url, c_dir) = (c.url.clone(), c.dir.clone());
    let c_port: u16 = c_url.rsplit(':').next().unwrap().parse().unwrap();
    drop(c);
    settle();
    // Made while c is away: an index, a drop, and a whole collection whose
    // map names c.
    let m = a.ack(INDEXES[1]);
    assert!(m.contains(&format!("not on {c_url}")) && m.contains("adopt it"), "{m}");
    let m = a.ack("DROP INDEX items_body ON items");
    assert!(m.contains(&format!("not on {c_url}")), "{m}");
    let m = a.ack("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['m', 'x'])");
    assert!(m.contains("created with 3 shard(s)") && m.contains(&format!("not on {c_url}")), "{m}");
    // An ALTER is not carried by the reconciliation, so it is still refused
    // naming the node to run it on.
    let e = a
        .exec("ALTER INDEX items_emb ON items SET TIER 'cached'")
        .unwrap()
        .finished()
        .unwrap_err()
        .to_string();
    assert!(e.contains("applied here") && e.contains(&format!("not on {c_url}")), "{e}");
    // c comes back at the same address with the same directory, and
    // attaches a as a restarted pod attaches its peers.
    let c = Node::start_at("recon-c", c_port, Some(c_dir.clone()));
    assert!(c.db.read().unwrap().collection("notes").is_err());
    c.ack(&format!("ATTACH NODE '{}'", a.url));
    {
        let db = c.db.read().unwrap();
        let items = db.collection("items").unwrap();
        assert!(items.index_by_name("items_emb").is_some(), "the index made while away");
        assert!(items.index_by_name("items_body").is_none(), "the index dropped while away");
        db.collection("notes").unwrap();
    }
    assert_eq!(c.local_shards("notes"), vec![2], "c built the shard the map gives it");
    // The collection works end to end: a write routed to c's shard lands.
    a.ack(r#"INSERT INTO notes VALUES ('{"id":"zeta"}')"#);
    assert_eq!(c.docs_here("notes"), 1);
    // A second reconciliation changes nothing.
    let notes = c.db.write().unwrap().reconcile(&a.db.read().unwrap().catalog.clone()).unwrap();
    assert!(notes.is_empty(), "{notes:?}");
    // The index is made again after its drop, on a alone: younger than the
    // tombstone, so the tombstone does not keep it from c.
    a.ack(&format!("LOCAL {}", INDEXES[0]));
    let notes = c.db.write().unwrap().reconcile(&a.db.read().unwrap().catalog.clone()).unwrap();
    assert_eq!(notes, vec!["adopted index `items_body` on `items`"]);
    // And c's tombstone reaches a: c drops the index, a reconciles from c.
    c.ack("LOCAL DROP INDEX items_emb ON items");
    let notes = a.db.write().unwrap().reconcile(&c.db.read().unwrap().catalog.clone()).unwrap();
    assert_eq!(notes, vec!["dropped index `items_emb` on `items`"]);
    assert!(a.db.read().unwrap().collection("items").unwrap().index_by_name("items_emb").is_none());
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A data node that comes back with an empty directory is not a node that
/// missed a definition: the map names it for data it does not have. The
/// reconciliation refuses to grow an empty shard for it and says what to
/// do instead; a coordinator, which holds nothing, adopts everything.
#[test]
fn a_fresh_directory_does_not_grow_empty_shards_for_an_older_collection() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("born-a");
    let c = Node::start("born-c");
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    let c_url = c.url.clone();
    let c_port: u16 = c_url.rsplit(':').next().unwrap().parse().unwrap();
    let old_dir = c.dir.clone();
    drop(c);
    settle();
    std::thread::sleep(std::time::Duration::from_millis(20));
    // The same address, a directory that never held the shard.
    let c = Node::start_at("born-c2", c_port, None);
    let notes = c.db.write().unwrap().reconcile(&a.db.read().unwrap().catalog.clone()).unwrap();
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert!(notes[0].contains("older than this data directory") && notes[0].contains("RESTORE"));
    assert!(c.db.read().unwrap().collection("items").is_err());
    let h = c.ack("SHOW HEALTH");
    assert!(h.contains("collection `items`: NOT ADOPTED"), "{h}");
    // A collection made after the directory was, while c is away again, is
    // one it simply missed.
    let new_dir = c.dir.clone();
    drop(c);
    settle();
    let m = a.ack("CREATE COLLECTION later (id TEXT PRIMARY KEY) WITH (splits = ['m'])");
    assert!(m.contains(&format!("not on {c_url}")), "{m}");
    let c = Node::start_at("born-c2", c_port, Some(new_dir));
    let notes = c.db.write().unwrap().reconcile(&a.db.read().unwrap().catalog.clone()).unwrap();
    // The refusal is said once per process, so the restarted c says it again.
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert_eq!(notes[1], "adopted collection `later` with 0 index(es)");
    assert_eq!(c.local_shards("later").len(), 1);
    let d = Node::start_role("born-d", 0, None, celastro::engine::Role::Coordinator);
    d.ack(&format!("ATTACH NODE '{}'", a.url));
    d.db.read().unwrap().collection("items").unwrap();
    for n in [a, c, d] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&old_dir);
}

/// Every timestamp and every tombstone compares by the clock, so a peer
/// whose clock is far off is refused at ATTACH, and one that is a little
/// off is named by SHOW HEALTH.
#[test]
fn a_peer_whose_clock_is_off_is_refused_or_named() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("clock-a");
    let b = Node::start("clock-b");
    b.db.write().unwrap().pretend(None, 6_000_000);
    let e = a.exec(&format!("ATTACH NODE '{}'", b.url)).unwrap_err().to_string();
    assert!(e.contains("clock at") && e.contains("+6.0 s") && e.contains("NTP"), "{e}");
    b.db.write().unwrap().pretend(None, -800_000);
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("clock -0.8 s CLOCK OFF"), "{h}");
    assert_eq!(a.db.read().unwrap().peer_seen(&b.url).unwrap().skew_micros / 100_000, -8);
    b.db.write().unwrap().pretend(None, 0);
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("clock +0.0 s") || h.contains("clock -0.0 s"), "{h}");
    assert!(!h.contains("CLOCK OFF"), "{h}");
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A hello carries the epoch of the process behind the address. A newer
/// one is a restart; an older one after a newer is a second process
/// answering at the same address, and SHOW HEALTH says so.
#[test]
fn an_older_process_answering_at_an_attached_address_is_named() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("epoch-a");
    let b = Node::start("epoch-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    let epoch = b.db.read().unwrap().epoch();
    assert_eq!(a.db.read().unwrap().peer_seen(&b.url).unwrap().epoch, epoch);
    b.db.write().unwrap().pretend(Some(epoch + 1_000_000), 0);
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("restarted since last seen"), "{h}");
    let h = a.ack("SHOW HEALTH");
    assert!(!h.contains("restarted"), "said once: {h}");
    b.db.write().unwrap().pretend(Some(epoch), 0);
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("AN OLDER PROCESS ANSWERS HERE TOO"), "{h}");
    assert_eq!(a.db.read().unwrap().peer_seen(&b.url).unwrap().epoch, epoch + 1_000_000);
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A write through one node is read through any other at once, and two
/// reads through different nodes never go backwards: a read's snapshot is
/// the maximum of the holders' clocks, fetched per statement, so it covers
/// every commit any node has acknowledged.
#[test]
fn a_write_through_one_node_is_read_through_every_other_at_once() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("ryw-a");
    let b = Node::start("ryw-b");
    let c = Node::start("ryw-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    let nodes = [&a, &b, &c];
    let count = |n: &Node| -> usize {
        let r = n.query("SELECT count(*) AS c FROM items").unwrap();
        r.rows[0].doc.path("c").and_then(|v| v.as_i64()).unwrap() as usize
    };
    let mut last = 0;
    for i in 0..30usize {
        nodes[i % 3].db.write().unwrap().insert("items", doc(i)).unwrap();
        for (j, n) in nodes.iter().enumerate() {
            let r =
                n.query(&format!("SELECT id FROM items WHERE id = 'doc-{i:03}' LIMIT 1")).unwrap();
            assert_eq!(r.rows.len(), 1, "row {i} written through {} read through {j}", i % 3);
            let c = count(n);
            assert!(c >= last && c == i + 1, "count through {j} after row {i}: {c}, last {last}");
            last = c;
        }
    }
    assert!(b.db.write().unwrap().delete_key("items", "t1\u{1}doc-004").unwrap());
    for (j, n) in nodes.iter().enumerate() {
        let r = n.query("SELECT id FROM items WHERE id = 'doc-004' LIMIT 1").unwrap();
        assert_eq!(r.rows.len(), 0, "the delete through b is read through {j}");
        assert_eq!(count(n), 29);
    }
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// `BACKUP CLUSTER` backs every data node up at one instant this node
/// chooses, so the set restores to one cut: each node's backup is
/// verifiable at that instant.
#[test]
fn a_cluster_backup_is_one_instant_on_every_node() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("cb-a");
    let b = Node::start("cb-b");
    let c = Node::start("cb-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    for i in 0..30usize {
        [&a, &b, &c][i % 3].db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    let dest = dir("cb-dest");
    let out = a.exec(&format!("BACKUP CLUSTER TO '{}'", dest.display())).unwrap();
    let m = match out.finished().unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    };
    let ts: u64 = m.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(m.contains(&format!("at the same instant {ts} on {}: backup {ts} ", b.url)), "{m}");
    assert!(m.contains(&format!("{}: backup {ts} ", c.url)) && !m.contains("NOT on"), "{m}");
    for n in [&a, &b, &c] {
        let sql = format!("VERIFY BACKUP '{}' NODE '{}' AS OF {ts}", dest.display(), n.url);
        let v = match a.exec(&sql).unwrap().finished().unwrap() {
            Outcome::Ack(m) => m,
            other => panic!("{other:?}"),
        };
        assert!(v.starts_with(&format!("verified backup {ts} of node")), "{v}");
    }
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&dest);
}

/// One process, two names, would be two holders in a placement that are
/// one node: ATTACH takes a node only by the address it calls itself.
#[test]
fn a_node_is_attached_only_by_the_name_it_calls_itself() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("names-a");
    let b = Node::start("names-b");
    let by_other_name = b.url.replace("127.0.0.1", "localhost");
    let e = a.exec(&format!("ATTACH NODE '{by_other_name}'")).unwrap_err().to_string();
    assert!(e.contains("calls itself") && e.contains(&b.url), "{e}");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    assert_eq!(a.db.read().unwrap().catalog.nodes, vec![b.url.clone()]);
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A peer that opens connections and never closes them cannot take the
/// wire with it: past the cap a connection is closed at once, and one that
/// carries no frame for the idle time is closed, so the next call
/// reconnects and is served.
#[test]
fn idle_wire_connections_are_capped_and_closed() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    std::env::set_var("CELASTRO_WIRE_MAX_CONNECTIONS", "2");
    std::env::set_var("CELASTRO_WIRE_IDLE_SECS", "1");
    let a = Node::start("idle-a");
    // The wire reads its knobs as it starts, on its own thread.
    std::thread::sleep(std::time::Duration::from_millis(300));
    std::env::remove_var("CELASTRO_WIRE_MAX_CONNECTIONS");
    std::env::remove_var("CELASTRO_WIRE_IDLE_SECS");
    let addr = a.url.trim_start_matches("tcp://").to_string();
    let idle1 = std::net::TcpStream::connect(&addr).unwrap();
    let idle2 = std::net::TcpStream::connect(&addr).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    let peer = celastro::wire::Node::new(&a.url, Some(TOKEN), None).unwrap();
    let refused_before = celastro::wire::refused_connections();
    let e = peer.hello().unwrap_err().to_string();
    assert!(celastro::wire::refused_connections() > refused_before, "{e}");
    // The idle time passes: the two are closed, and the next call is served.
    std::thread::sleep(std::time::Duration::from_millis(1800));
    let mut buf = [0u8; 1];
    use std::io::Read;
    let _ = idle1.set_read_timeout(Some(std::time::Duration::from_millis(500)));
    assert_eq!(
        idle1.take(1).read(&mut buf).unwrap_or(0),
        0,
        "the server closed the idle connection"
    );
    drop(idle2);
    assert_eq!(peer.hello().unwrap().node.as_deref(), Some(a.url.as_str()));
    let d = a.dir.clone();
    drop(a);
    settle();
    let _ = std::fs::remove_dir_all(&d);
}

/// A move made while a node could not be reached leaves that node's map
/// naming the old holder. When it reconnects, the holders' own word about
/// what they hold, or held, corrects its map, and its statements route
/// to the shard where it is.
#[test]
fn a_move_made_while_a_node_was_away_reaches_its_map_when_it_reconnects() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("mv-a");
    let b = Node::start("mv-b");
    let c = Node::start("mv-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    for i in 0..30usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    let (c_url, c_dir) = (c.url.clone(), c.dir.clone());
    let c_port: u16 = c_url.rsplit(':').next().unwrap().parse().unwrap();
    drop(c);
    settle();
    // The move completes between a and b; the map switch does not reach c.
    let m = a.ack(&format!("MOVE SHARD 0 OF items TO '{}'", b.url));
    assert!(m.contains("map switched") && m.contains(&format!("not on {c_url}")), "{m}");
    assert_eq!(a.local_shards("items"), vec![] as Vec<usize>);
    assert_eq!(b.local_shards("items"), vec![0, 1]);
    let c = Node::start_at("mv-c", c_port, Some(c_dir));
    assert_eq!(c.db.read().unwrap().catalog.placement["items"][0].node, a.url, "stale");
    // Attaching a, the old holder, is enough: a's word that it gave the
    // shard away is final.
    c.ack(&format!("ATTACH NODE '{}'", a.url));
    assert_eq!(c.db.read().unwrap().catalog.placement["items"][0].node, b.url);
    let r = c.query("SELECT count(*) AS n FROM items").unwrap();
    assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(30));
    // And from the new holder's word alone, on a fresh stale map.
    let mut stale = c.db.read().unwrap().catalog.clone();
    stale.placement.get_mut("items").unwrap()[0].node = a.url.clone();
    c.db.write().unwrap().catalog = stale;
    let notes =
        c.db.write()
            .unwrap()
            .reconcile_from(&b.url, &b.db.read().unwrap().catalog.clone())
            .unwrap();
    assert!(notes.iter().any(|n| n.starts_with("shard 0 of `items`: now on")), "{notes:?}");
    assert_eq!(c.db.read().unwrap().catalog.placement["items"][0].node, b.url);
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A rotation of the wire token rolls only if a node accepts the new token
/// before it sends it: `CELASTRO_WIRE_TOKEN_ALSO` is the second token the
/// wire accepts, for the rollout in between.
#[test]
fn the_wire_accepts_a_second_token_for_a_rotation() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    std::env::set_var(celastro::wire::TOKEN_ALSO_ENV, "the-next-token");
    let a = Node::start("also-a");
    std::thread::sleep(std::time::Duration::from_millis(300));
    std::env::remove_var(celastro::wire::TOKEN_ALSO_ENV);
    let old = celastro::wire::Node::new(&a.url, Some(TOKEN), None).unwrap();
    let new = celastro::wire::Node::new(&a.url, Some("the-next-token"), None).unwrap();
    let wrong = celastro::wire::Node::new(&a.url, Some("neither"), None).unwrap();
    assert!(old.hello().is_ok());
    assert!(new.hello().is_ok());
    assert!(wrong.hello().unwrap_err().to_string().contains("wire token refused"));
    let d = a.dir.clone();
    drop(a);
    settle();
    let _ = std::fs::remove_dir_all(&d);
}

/// Two processes at one address, the old one reached through a stale
/// name: a fresh connection that answers with an older epoch than the
/// newest seen there is refused before a statement goes down it, so a
/// write cannot land on the zombie. A hello still answers, since that is
/// how SHOW HEALTH names it.
#[test]
fn a_fresh_connection_to_an_older_process_is_refused() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    std::env::set_var("CELASTRO_WIRE_IDLE_SECS", "1");
    let a = Node::start("fence-a");
    let b = Node::start("fence-b");
    std::thread::sleep(std::time::Duration::from_millis(300));
    std::env::remove_var("CELASTRO_WIRE_IDLE_SECS");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(CREATE);
    a.db.write().unwrap().insert("items", doc(1)).unwrap();
    let epoch = b.db.read().unwrap().epoch();
    // A newer process was seen at b's address; then the old one answers.
    b.db.write().unwrap().pretend(Some(epoch + 5_000_000), 0);
    a.ack("SHOW HEALTH");
    b.db.write().unwrap().pretend(Some(epoch), 0);
    // A hello over the pooled connection shows the older process: the
    // connection is let go, and the next call, dialling afresh, is refused.
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("AN OLDER PROCESS ANSWERS HERE TOO"), "{h}");
    let e = a.db.write().unwrap().insert("items", doc(1)).unwrap_err().to_string();
    assert!(e.contains("an older process answers at") && e.contains("refused"), "{e}");
    // And with no hello in between, once the pooled connection closes idle.
    std::thread::sleep(std::time::Duration::from_millis(1800));
    let e = a.db.write().unwrap().insert("items", doc(1)).unwrap_err().to_string();
    assert!(e.contains("an older process answers at"), "{e}");
    // The newer process again: served.
    b.db.write().unwrap().pretend(Some(epoch + 5_000_000), 0);
    std::thread::sleep(std::time::Duration::from_millis(1800));
    a.db.write().unwrap().insert("items", doc(2)).unwrap();
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A move issued to the source while writes flow through the source: the
/// copy holds no lock on either end, so a reader on the source never
/// waits long, and the move ends in seconds.
#[test]
fn a_move_from_a_busy_source_holds_no_lock_long() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("busy-a");
    let b = Node::start("busy-b");
    let c = Node::start("busy-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    for i in 0..3000usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    a.ack("FLUSH items");
    let writing = Arc::new(AtomicBool::new(true));
    let writer = {
        let (db, writing) = (a.db.clone(), writing.clone());
        std::thread::spawn(move || {
            let (mut ok, mut refused, mut slowest) = (0usize, 0usize, std::time::Duration::ZERO);
            let mut i = 5000usize;
            while writing.load(Ordering::Relaxed) {
                let t0 = std::time::Instant::now();
                let r = db.write().unwrap().insert("items", doc(i));
                slowest = slowest.max(t0.elapsed());
                std::thread::sleep(std::time::Duration::from_millis(1));
                match r {
                    Ok(_) => ok += 1,
                    Err(_) => refused += 1,
                }
                i += 1;
            }
            (ok, refused, slowest)
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(300));
    let t0 = std::time::Instant::now();
    let m = a.ack(&format!("MOVE SHARD 0 OF items TO '{}'", c.url));
    let took = t0.elapsed();
    writing.store(false, Ordering::Relaxed);
    let (ok, refused, slowest) = writer.join().unwrap();
    eprintln!("move: {m}\nmove took {took:?}; writes {ok} ok, {refused} refused, slowest lock wait {slowest:?}");
    assert!(m.contains("map switched here and on"), "{m}");
    assert!(!m.contains("not on"), "{m}");
    assert!(took < std::time::Duration::from_secs(10), "the move took {took:?}");
    assert!(slowest < std::time::Duration::from_secs(2), "a write waited {slowest:?}");
    assert_eq!(a.local_shards("items"), vec![] as Vec<usize>);
    assert_eq!(c.local_shards("items"), vec![0, 2]);
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// The fence's server half: a frame carries its caller's address and
/// epoch, and a holder that has seen a newer process at that address
/// refuses the call. The older process's own forwards are what this
/// stops; the client half stops calls toward it.
#[test]
fn a_call_from_an_older_process_is_refused_by_a_holder() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("fence2-a");
    let b = Node::start("fence2-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    b.ack(&format!("ATTACH NODE '{}'", a.url));
    a.ack(CREATE);
    a.db.write().unwrap().insert("items", doc(1)).unwrap();
    let epoch = a.db.read().unwrap().epoch();
    // A newer process at a's address calls b: its frames say so, and b
    // raises what it has seen of a.
    // Tenant t1's shard is b's: doc(1), doc(4), doc(7) go over the wire.
    a.db.write().unwrap().pretend(Some(epoch + 5_000_000), 0);
    a.db.write().unwrap().insert("items", doc(4)).unwrap();
    assert_eq!(b.db.read().unwrap().peer_seen(&a.url).unwrap().epoch, epoch + 5_000_000);
    // The older process calls again: refused by b, naming both.
    a.db.write().unwrap().pretend(Some(epoch), 0);
    let e = a.db.write().unwrap().insert("items", doc(7)).unwrap_err().to_string();
    assert!(e.contains("a call from an older process at") && e.contains(&a.url), "{e}");
    // b's own writes go on.
    b.db.write().unwrap().insert("items", doc(10)).unwrap();
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A statement whose fan-out reaches a holder that never answers holds
/// no lock while it waits: the definition and the forwarded write are
/// applied here under the lock and carried as deferred work, so a reader
/// on this node is answered meanwhile. Ten such holders under the lock
/// was a console that answered nothing for the better part of a minute,
/// and a liveness probe restarted it. A DELETE ... WHERE selects its keys
/// first, so it asks the holders whether they answer with no lock held
/// and is refused by the one that does not, nothing deleted; the test
/// below.
#[test]
fn a_statement_waiting_on_a_holder_that_never_answers_holds_no_lock() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("hang-a");
    let b = Node::start("hang-b");
    let c = Node::start("hang-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    // c goes away and a listener that accepts and never answers takes its
    // port: every call to it waits out the deadline, here two seconds.
    let c_port: u16 = c.url.rsplit(':').next().unwrap().parse().unwrap();
    let c_dir = c.dir.clone();
    drop(c);
    settle();
    let hole = std::net::TcpListener::bind(("127.0.0.1", c_port)).unwrap();
    hole.set_nonblocking(true).unwrap();
    let plug = Arc::new(AtomicBool::new(true));
    let holding = {
        let plug = plug.clone();
        std::thread::spawn(move || {
            let mut kept = Vec::new();
            while plug.load(Ordering::Relaxed) {
                if let Ok((s, _)) = hole.accept() {
                    kept.push(s);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            drop(kept);
        })
    };
    a.db.write().unwrap().opts.statement_deadline_ms = Some(2000);
    let read_wait = |n: &Node| {
        let t0 = std::time::Instant::now();
        let _g = n.db.read().unwrap();
        t0.elapsed()
    };
    for sql in [
        "CREATE INDEX ix ON items USING secondary (n)",
        r#"INSERT INTO items VALUES ('{"id":"doc-900","tenant":"t2","n":900}')"#,
    ] {
        let t0 = std::time::Instant::now();
        let out = a.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            t0.elapsed() < std::time::Duration::from_millis(500),
            "{sql} held the lock {:?}",
            t0.elapsed()
        );
        let finishing = std::thread::spawn(move || out.finished());
        std::thread::sleep(std::time::Duration::from_millis(300));
        let waited = read_wait(&a);
        assert!(waited < std::time::Duration::from_millis(200), "{sql}: a read waited {waited:?}");
        match finishing.join().unwrap() {
            Ok(Outcome::Ack(m)) => assert!(m.contains("not on"), "{sql}: {m}"),
            Ok(other) => panic!("{sql}: {other:?}"),
            Err(e) => assert!(e.to_string().contains("did not answer"), "{sql}: {e}"),
        }
        assert!(t0.elapsed() > std::time::Duration::from_millis(1500), "{sql} never waited for c");
    }
    plug.store(false, Ordering::Relaxed);
    holding.join().unwrap();
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&c_dir);
}

/// A partial statement over two holders that never answer pays one
/// deadline, not one per holder: the holders' clocks are asked at once,
/// so the near shards are still asked within the budget and only the far
/// ones are missing. Ten holders across a split, asked one after another,
/// had spent the budget before the near ones were reached.
#[test]
fn a_partial_statement_pays_one_deadline_for_every_holder_that_never_answers() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("par-a");
    let b = Node::start("par-b");
    let c = Node::start("par-c");
    let d = Node::start("par-d");
    for n in [&b, &c, &d] {
        a.ack(&format!("ATTACH NODE '{}'", n.url));
    }
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['c', 'f', 'k'])");
    for i in 0..40usize {
        let key = format!("{}{i:04}", "adgm".chars().nth(i % 4).unwrap());
        let doc =
            Value::obj(vec![("id".into(), Value::Str(key)), ("n".into(), Value::Int(i as i64))]);
        a.db.write().unwrap().insert("items", doc).unwrap();
    }
    let mut holes = Vec::new();
    let plug = Arc::new(AtomicBool::new(true));
    for n in [c, d] {
        let port: u16 = n.url.rsplit(':').next().unwrap().parse().unwrap();
        let dir = n.dir.clone();
        drop(n);
        settle();
        let hole = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
        hole.set_nonblocking(true).unwrap();
        let plug = plug.clone();
        holes.push((
            dir,
            std::thread::spawn(move || {
                let mut kept = Vec::new();
                while plug.load(Ordering::Relaxed) {
                    if let Ok((s, _)) = hole.accept() {
                        kept.push(s);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }),
        ));
    }
    let t0 = std::time::Instant::now();
    let r = a
        .query("SELECT count(*) AS n FROM items WITH (partial_results, deadline_ms = 3000)")
        .unwrap();
    let took = t0.elapsed();
    assert_eq!(r.missing.len(), 2, "{:?}", r.missing);
    assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(20), "a's and b's rows");
    assert!(took < std::time::Duration::from_millis(3500), "two holders cost {took:?}");
    plug.store(false, Ordering::Relaxed);
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    for (d, h) in holes {
        h.join().unwrap();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A delete by predicate that reaches a holder that never answers holds
/// no lock while it waits, and deletes nothing: the keys come from every
/// holder, so the holders are asked whether they answer with the lock let
/// go, and the one that does not refuses the delete before a key is
/// selected. The rows on the holders that answer are still there.
#[test]
fn a_delete_by_predicate_reaching_a_holder_that_never_answers_holds_no_lock_and_deletes_nothing() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("dhang-a");
    let b = Node::start("dhang-b");
    let c = Node::start("dhang-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack(CREATE);
    for i in 0..30usize {
        let doc = Value::obj(vec![
            ("id".into(), Value::Str(format!("doc-{i:03}"))),
            ("tenant".into(), Value::Str(format!("t{}", i % 3 + 1))),
            ("n".into(), Value::Int(i as i64)),
        ]);
        a.db.write().unwrap().insert("items", doc).unwrap();
    }
    let before = a.query("SELECT count(*) AS n FROM items").unwrap();
    assert_eq!(before.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(30));
    let c_port: u16 = c.url.rsplit(':').next().unwrap().parse().unwrap();
    let c_dir = c.dir.clone();
    drop(c);
    settle();
    let hole = std::net::TcpListener::bind(("127.0.0.1", c_port)).unwrap();
    hole.set_nonblocking(true).unwrap();
    let plug = Arc::new(AtomicBool::new(true));
    let holding = {
        let plug = plug.clone();
        std::thread::spawn(move || {
            let mut kept = Vec::new();
            while plug.load(Ordering::Relaxed) {
                if let Ok((s, _)) = hole.accept() {
                    kept.push(s);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            drop(kept);
        })
    };
    a.db.write().unwrap().opts.statement_deadline_ms = Some(2000);
    let sql = "DELETE FROM items WHERE n < 100";
    let t0 = std::time::Instant::now();
    let out = a.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert!(
        t0.elapsed() < std::time::Duration::from_millis(500),
        "held the lock {:?}",
        t0.elapsed()
    );
    let db = a.db.clone();
    let finishing = std::thread::spawn(move || out.finished_with(&db));
    std::thread::sleep(std::time::Duration::from_millis(300));
    let t1 = std::time::Instant::now();
    drop(a.db.read().unwrap());
    let waited = t1.elapsed();
    assert!(waited < std::time::Duration::from_millis(200), "a read waited {waited:?}");
    let e = match finishing.join().unwrap() {
        Err(e) => e.to_string(),
        Ok(other) => panic!("{other:?}"),
    };
    assert!(e.contains("NOTHING was deleted"), "{e}");
    assert!(t0.elapsed() > std::time::Duration::from_millis(1500), "never waited for c");
    plug.store(false, Ordering::Relaxed);
    holding.join().unwrap();
    // The splits are 't1' and 't2': shard 0 is empty, shard 1 holds t1's
    // ten rows, and c's shard 2 held t2's and t3's twenty.
    let after = a
        .query("SELECT count(*) AS n FROM items WITH (partial_results, deadline_ms = 2000)")
        .unwrap();
    assert_eq!(after.missing, vec!["shard 2".to_string()]);
    assert_eq!(
        after.rows[0].doc.path("n").and_then(|v| v.as_i64()),
        Some(10),
        "the rows still here"
    );
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir_all(&c_dir);
}

/// A split issued at a node that holds nothing goes to the holder, which
/// splits and tells every peer: from then on every node's map has the new
/// shard, a count from any node is whole, a write for a key past the split
/// lands on the new shard, and the new shard moves like any other.
#[test]
fn a_shard_splits_on_its_holder_and_every_node_learns_the_new_map() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("split-a");
    let b = Node::start("split-b");
    let c = Node::start("split-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['m'], nodes = ['{a}', '{b}'])"
        .replace("{a}", &a.url)
        .replace("{b}", &b.url)
        .as_str());
    for i in 0..60usize {
        let key = format!("{}{i:04}", if i % 2 == 0 { 'd' } else { 'r' });
        let doc =
            Value::obj(vec![("id".into(), Value::Str(key)), ("n".into(), Value::Int(i as i64))]);
        a.db.write().unwrap().insert("items", doc).unwrap();
    }
    // Issued at a, which holds shard 0: the holder of shard 1 is b, and
    // b's word comes back to a. (c, attached but holding nothing of
    // `items`, never got the definition; it is the move's target below.)
    let m = a.ack("SPLIT SHARD 1 OF items AT 't'");
    assert!(m.contains("shard 2 is [t, )") && m.contains("map switched"), "{m}");
    settle();
    for n in [&a, &b] {
        let cat = n.ack("SHOW CATALOG items");
        assert!(cat.contains("shard 1 on") && cat.contains("[m, t)"), "{}: {cat}", n.url);
        assert!(cat.contains("shard 2 on") && cat.contains("[t, )"), "{}: {cat}", n.url);
        let r = n.query("SELECT count(*) AS n FROM items").unwrap();
        assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(60), "{}", n.url);
    }
    // A key past the split, written through a, is on the new shard, and
    // the key pins the plan to it.
    a.ack(r#"INSERT INTO items VALUES ('{"id":"z0001","n":1}')"#);
    let plan = match a.exec("EXPLAIN SELECT id FROM items WHERE id = 'z0001' LIMIT 2").unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(plan.contains("1 of 3 shard(s) scanned") && plan.contains("shard 2 ("), "{plan}");
    let r = a.query("SELECT id FROM items WHERE id = 'z0001' LIMIT 2").unwrap();
    assert_eq!(r.rows.len(), 1);
    // The new shard moves to the node that held nothing.
    let m = a.ack(&format!("MOVE SHARD 2 OF items TO '{}'", c.url));
    assert!(m.contains("moved from"), "{m}");
    settle();
    let health = a.ack("SHOW HEALTH");
    assert!(health.contains(&format!("shard 2 of `items`: on {}", c.url)), "{health}");
    for n in [&a, &b, &c] {
        let r = n.query("SELECT count(*) AS n FROM items").unwrap();
        assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(61), "{}", n.url);
    }
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A merge of two shards on different nodes is refused with the move that
/// brings them together; made on their holder, it reaches every node's map,
/// a count from any node is whole, the merged index is skipped by every
/// read, and the holder picks the key when a split names none.
#[test]
fn shards_merge_on_their_holder_and_a_split_without_a_key_takes_the_median() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("merge-a");
    let b = Node::start("merge-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['m'], nodes = ['{a}', '{b}'])"
        .replace("{a}", &a.url)
        .replace("{b}", &b.url)
        .as_str());
    for i in 0..40usize {
        let key = format!("{}{i:04}", if i % 2 == 0 { 'd' } else { 'r' });
        let doc =
            Value::obj(vec![("id".into(), Value::Str(key)), ("n".into(), Value::Int(i as i64))]);
        a.db.write().unwrap().insert("items", doc).unwrap();
    }
    let e = a.exec("MERGE SHARDS 0 AND 1 OF items").unwrap_err().to_string();
    assert!(e.contains("different nodes") && e.contains("MOVE SHARD 1 OF items TO"), "{e}");
    a.ack(&format!("MOVE SHARD 1 OF items TO '{}'", a.url));
    settle();
    // Issued at b, which holds nothing now: a merges and tells b.
    let m = b.ack("MERGE SHARDS 0 AND 1 OF items");
    assert!(m.contains("shard 0 is [, )") && m.contains("20 row(s) of shard 1 rebuilt"), "{m}");
    assert!(m.contains("map switched"), "{m}");
    settle();
    for n in [&a, &b] {
        let cat = n.ack("SHOW CATALOG items");
        assert!(
            cat.contains("shard 0 on")
                && cat.contains("[, )")
                && cat.contains("shard 1 merged away"),
            "{}: {cat}",
            n.url
        );
        let r = n.query("SELECT count(*) AS n FROM items").unwrap();
        assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(40), "{}", n.url);
        let h = n.ack("SHOW HEALTH");
        assert!(!h.contains("shard 1 of `items`: on"), "{h}");
    }
    // A split with no key, issued away from the holder: the holder's median.
    let m = b.ack("SPLIT SHARD 0 OF items");
    assert!(m.contains("split at 'r0001'") && m.contains("shard 2 is [r0001, )"), "{m}");
    settle();
    for n in [&a, &b] {
        let r = n.query("SELECT count(*) AS n FROM items").unwrap();
        assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(40), "{}", n.url);
    }
    let m = a.ack(&format!("MOVE SHARD 2 OF items TO '{}'", b.url));
    assert!(m.contains("moved from"), "{m}");
    settle();
    let r = b.query("SELECT count(*) AS n FROM items WHERE n >= 20").unwrap();
    assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(20));
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// Every shard has a follower by default, and a write is acknowledged
/// once the follower has it on disk: the health names the follower live
/// and confirmed to the write's instant. A follower away degrades the
/// acknowledgement to this node's disk alone -- the health says so -- and
/// the follower is caught up when it returns, its copy whole again.
#[test]
fn a_write_is_confirmed_on_the_follower_and_a_follower_away_is_caught_up_on_return() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("rep-a");
    let b = Node::start("rep-b");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    let m = a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['m'])");
    assert!(m.contains("2 shard(s)"), "{m}");
    let cat = a.ack("SHOW CATALOG items");
    assert!(cat.contains(&format!("shard 0 on this node [, m) followed by {}", b.url)), "{cat}");
    assert!(cat.contains(&format!("shard 1 on {} [m, ) followed by {}", b.url, a.url)), "{cat}");
    for i in 0..40usize {
        let key = format!("{}{i:04}", if i % 2 == 0 { 'd' } else { 'r' });
        a.ack(&format!(r#"INSERT INTO items VALUES ('{{"id":"{key}","n":{i}}}')"#));
    }
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains(&format!("shard 0 of `items`: follower {} live", b.url)), "{h}");
    assert!(h.contains("follows shard 1 of `items` at term 0: caught up"), "{h}");
    assert!(b.dir.join("collections/items/followed/shard-0000").exists());
    // The follower goes away: writes to shard 0 are acknowledged on a
    // alone, and the health says it.
    let (b_port, b_dir) =
        (b.url.rsplit(':').next().unwrap().parse::<u16>().unwrap(), b.dir.clone());
    drop(b);
    settle();
    let t0 = std::time::Instant::now();
    for i in 40..60usize {
        a.ack(&format!(r#"INSERT INTO items VALUES ('{{"id":"d{i:04}","n":{i}}}')"#));
    }
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(25),
        "degraded writes did not wait a deadline each"
    );
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("DEGRADED"), "{h}");
    // Back: caught up from where it stood, live again, and a write is
    // confirmed on it once more.
    let b = Node::start_at("rep-b", b_port, Some(b_dir));
    b.ack(&format!("ATTACH NODE '{}'", a.url));
    let mut live = false;
    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let h = a.ack("SHOW HEALTH");
        if h.contains(&format!("shard 0 of `items`: follower {} live", b.url))
            && !h.contains("DEGRADED")
        {
            live = true;
            break;
        }
    }
    assert!(live, "{}", a.ack("SHOW HEALTH"));
    a.ack(r#"INSERT INTO items VALUES ('{"id":"d0099","n":99}')"#);
    let h = b.ack("SHOW HEALTH");
    assert!(h.contains("follows shard 0 of `items` at term 0: caught up"), "{h}");
    for n in [a, b] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A holder lost: its follower is promoted and answers every acknowledged
/// row at the next term; the old holder, back, hears the term at its
/// attach, demotes its copy and follows the new holder, and a write
/// through it lands on the new holder and is confirmed on the old one.
#[test]
fn a_follower_is_promoted_and_the_old_holder_demotes_when_it_returns() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let a = Node::start("pro-a");
    let b = Node::start("pro-b");
    let c = Node::start("pro-c");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    a.ack(&format!("ATTACH NODE '{}'", c.url));
    a.ack("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['m'], nodes = ['{a}', '{b}'])"
        .replace("{a}", &a.url)
        .replace("{b}", &b.url)
        .as_str());
    for i in 0..40usize {
        let key = format!("{}{i:04}", if i % 2 == 0 { 'd' } else { 'r' });
        a.ack(&format!(r#"INSERT INTO items VALUES ('{{"id":"{key}","n":{i}}}')"#));
    }
    let e = a.exec(&format!("PROMOTE SHARD 0 OF items ON '{}'", c.url)).unwrap_err().to_string();
    assert!(e.contains("does not follow"), "{e}");
    // a, the holder of shard 0, is lost.
    let (a_port, a_dir) =
        (a.url.rsplit(':').next().unwrap().parse::<u16>().unwrap(), a.dir.clone());
    let a_url = a.url.clone();
    drop(a);
    settle();
    b.db.write().unwrap().opts.statement_deadline_ms = Some(3000);
    let e = b
        .exec(r#"INSERT INTO items VALUES ('{"id":"d0900","n":900}')"#)
        .unwrap()
        .finished_with(&b.db)
        .unwrap_err()
        .to_string();
    assert!(e.contains("did not answer") || e.contains("deadline"), "{e}");
    b.db.write().unwrap().opts.statement_deadline_ms = Some(30_000);
    // Promoted at b itself (c, attached but holding nothing of `items`,
    // never got the definition).
    let m = b.ack(&format!("PROMOTE SHARD 0 OF items ON '{}'", b.url));
    assert!(m.contains("promoted here at term 1") && m.contains(&a_url), "{m}");
    settle();
    let cat = b.ack("SHOW CATALOG items");
    assert!(
        cat.contains(&format!("shard 0 on this node [, m) followed by {a_url} term 1")),
        "{cat}"
    );
    let r = b.query("SELECT count(*) AS n FROM items").unwrap();
    assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(40));
    assert_eq!(b.local_shards("items"), vec![0, 1]);
    // Writes flow again, acknowledged on b alone while a is away.
    b.ack(r#"INSERT INTO items VALUES ('{"id":"d0900","n":900}')"#);
    let h = b.ack("SHOW HEALTH");
    assert!(
        h.contains(&format!("shard 0 of `items`: follower {}", a_url)) && h.contains("DEGRADED"),
        "{h}"
    );
    // a returns with its old map, attaches, hears term 1, and follows.
    let a = Node::start_at("pro-a", a_port, Some(a_dir));
    assert_eq!(a.local_shards("items"), vec![0], "opened as it was left");
    a.ack(&format!("ATTACH NODE '{}'", b.url));
    assert_eq!(a.local_shards("items"), Vec::<usize>::new(), "demoted at the attach");
    let cat = a.ack("SHOW CATALOG items");
    assert!(
        cat.contains(&format!("shard 0 on {} [, m) followed by {} term 1", b.url, a.url)),
        "{cat}"
    );
    assert!(a.dir.join("collections/items/followed/shard-0000").exists());
    let mut live = false;
    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let h = b.ack("SHOW HEALTH");
        if h.contains(&format!("shard 0 of `items`: follower {} live", a.url)) {
            live = true;
            break;
        }
    }
    assert!(live, "{}", b.ack("SHOW HEALTH"));
    a.ack(r#"INSERT INTO items VALUES ('{"id":"d0901","n":901}')"#);
    let h = a.ack("SHOW HEALTH");
    assert!(h.contains("follows shard 0 of `items` at term 1: caught up"), "{h}");
    for n in [&a, &b] {
        let r = n.query("SELECT count(*) AS n FROM items").unwrap();
        assert_eq!(r.rows[0].doc.path("n").and_then(|v| v.as_i64()), Some(42), "{}", n.url);
    }
    for n in [a, b, c] {
        let d = n.dir.clone();
        drop(n);
        settle();
        let _ = std::fs::remove_dir_all(&d);
    }
}
