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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let dir = dir(tag);
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

    fn exec(&self, sql: &str) -> celastro::Result<Outcome> {
        self.db.write().unwrap().execute(sql)
    }

    fn ack(&self, sql: &str) -> String {
        match self.exec(sql).unwrap() {
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
