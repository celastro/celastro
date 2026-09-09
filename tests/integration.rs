//! End-to-end tests against the guarantees in §14.
//!
//! | criterion | test |
//! |---|---|
//! | hybrid queries provably correct; harness trusted | [`hybrid_retrieval_is_a_union_of_all_three_modes`], [`the_recall_harness_catches_a_deliberate_regression`] |
//! | recall@10 ≥ 0.95 under sustained deletes | [`recall_at_10_holds_under_sustained_deletes`] |
//! | in exact mode, results bit-identical regardless of shard count | [`exact_mode_is_bit_identical_across_shard_counts`] |
//! | state survives a reopen | [`a_database_survives_reopen`] |

use celastro::codec::Rng;
use celastro::engine::{Db, DbOpts, Outcome};
use celastro::json;
use celastro::value::Value;

// --------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------

const TOPICS: &[(&str, &str)] = &[
    ("vector", "approximate nearest neighbour graph traversal over quantized codes"),
    ("lexical", "block max wand postings and the bm25 saturation curve"),
    ("hybrid", "reciprocal rank fusion over lexical and vector candidates"),
    ("storage", "immutable segments compaction and the delete log"),
    ("planner", "runtime strategy selection on measured selectivity"),
];

struct Corpus {
    dims: usize,
    centroids: Vec<Vec<f32>>,
    rng: Rng,
}

impl Corpus {
    fn new(dims: usize, seed: u64) -> Corpus {
        let mut rng = Rng::new(seed);
        let centroids =
            (0..TOPICS.len()).map(|_| (0..dims).map(|_| rng.next_normal()).collect()).collect();
        Corpus { dims, centroids, rng }
    }

    fn doc(&mut self, i: usize) -> Value {
        let t = i % TOPICS.len();
        let emb: Vec<Value> = (0..self.dims)
            .map(|d| Value::Float((self.centroids[t][d] + self.rng.next_normal() * 0.35) as f64))
            .collect();
        Value::obj(vec![
            ("id".into(), Value::Str(format!("doc-{i:05}"))),
            ("tenant_id".into(), Value::Str(format!("t{}", i % 3))),
            ("topic".into(), Value::Str(TOPICS[t].0.into())),
            (
                "status".into(),
                Value::Str(if i % 4 == 0 { "draft".into() } else { "published".into() }),
            ),
            ("body".into(), Value::Str(format!("{} — item {i}", TOPICS[t].1))),
            (
                "tags".into(),
                Value::Array(vec![
                    Value::Str(TOPICS[t].0.into()),
                    Value::Str(if i % 7 == 0 { "starred".into() } else { "plain".into() }),
                ]),
            ),
            ("embedding".into(), Value::Array(emb)),
        ])
    }

    fn query_near(&self, topic: usize, jitter: f32) -> Vec<f32> {
        self.centroids[topic].iter().map(|x| x + jitter).collect()
    }
}

fn vec_literal(v: &[f32]) -> String {
    format!("[{}]", v.iter().map(|x| format!("{x:.8}")).collect::<Vec<_>>().join(","))
}

fn opts(flat_tier_max: usize) -> DbOpts {
    let mut o = DbOpts::default();
    // Small enough that a few thousand vectors actually reach the graph tier.
    o.build.flat_tier_max = flat_tier_max;
    o.recall_sample_rate = 1;
    o
}

fn setup(db: &mut Db, dims: usize, splits: &[&str]) {
    let splits_sql = if splits.is_empty() {
        String::new()
    } else {
        format!(
            " WITH (splits = [{}])",
            splits.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(",")
        )
    };
    db.execute(&format!(
        "CREATE COLLECTION items (
           id TEXT PRIMARY KEY,
           tenant_id TEXT NOT NULL,
           status TEXT
         ) PARTITION BY (tenant_id){splits_sql}"
    ))
    .unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
    )
    .unwrap();
    db.execute(&format!(
        "CREATE INDEX items_emb ON items USING vector (embedding) WITH (dims = {dims}, metric = 'cosine')"
    ))
    .unwrap();
}

fn build(db: &mut Db, n: usize, dims: usize, seed: u64) -> Corpus {
    let mut c = Corpus::new(dims, seed);
    for i in 0..n {
        let d = c.doc(i);
        db.insert("items", d).unwrap();
    }
    c
}

fn keys(r: &celastro::plan::QueryResult) -> Vec<String> {
    r.rows.iter().map(|x| x.key.clone()).collect()
}

// --------------------------------------------------------------------------
// Hybrid queries, provably correct
// --------------------------------------------------------------------------

#[test]
fn hybrid_retrieval_is_a_union_of_all_three_modes() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 16, &[]);
    let c = build(&mut db, 400, 16, 7);
    db.execute("FLUSH items").unwrap();

    // A document that only the text source can find: unique words, and an
    // embedding deliberately far from the query.
    db.insert(
        "items",
        json::parse(&format!(
            r#"{{"id":"text-only","tenant_id":"t1","status":"published",
                 "body":"quokka bandersnatch zzyzx",
                 "tags":["odd"],
                 "embedding":[{}]}}"#,
            (0..16).map(|_| "-9.0").collect::<Vec<_>>().join(",")
        ))
        .unwrap(),
    )
    .unwrap();

    let q = c.query_near(2, 0.02);
    let sql = format!(
        "SELECT id FROM items WHERE tenant_id = 't1' \
         ORDER BY hybrid(text_match(body, 'quokka bandersnatch'), embedding <=> {}, method => 'rrf') \
         LIMIT 10",
        vec_literal(&q)
    );
    let r = db.query(&sql).unwrap();
    let ks = keys(&r);
    assert!(
        ks.iter().any(|k| k.ends_with("text-only")),
        "a document found by only the text source must still surface: {ks:?}"
    );
    // And the vector source's own neighbourhood is there too.
    assert!(
        r.rows
            .iter()
            .filter(|x| x.doc.path("topic").and_then(|v| v.as_str()) == Some("hybrid"))
            .count()
            >= 3,
        "the vector source should contribute its neighbourhood: {:?}",
        r.rows.iter().map(|x| x.doc.path("topic").cloned()).collect::<Vec<_>>()
    );
    // Every row is from the requested tenant: the filter is a hard constraint,
    // not a ranking signal.
    assert!(ks.iter().all(|k| k.starts_with("t1\u{1}")), "{ks:?}");
}

#[test]
fn text_match_is_a_must_in_where_and_a_should_in_hybrid() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    let c = build(&mut db, 200, 8, 11);
    db.execute("FLUSH items").unwrap();
    let q = vec_literal(&c.query_near(0, 0.0));

    // Give one document a term nothing else has, so the text source can find
    // exactly one thing and the difference between must and should is visible.
    db.insert(
        "items",
        json::parse(
            r#"{"id":"rare-one","tenant_id":"t1","status":"published",
                "body":"bandersnatch","tags":["odd"],
                "embedding":[9,9,9,9,9,9,9,9]}"#,
        )
        .unwrap(),
    )
    .unwrap();

    // As a should: the source contributes one candidate, and the other nine
    // rows come from the vector source. A should never filters.
    let should = db
        .query(&format!(
            "SELECT id FROM items ORDER BY hybrid(text_match(body, 'bandersnatch'), embedding <=> {q}) LIMIT 10"
        ))
        .unwrap();
    assert_eq!(should.rows.len(), 10);
    let matching = should
        .rows
        .iter()
        .filter(|r| r.doc.path("body").unwrap().as_str().unwrap().contains("bandersnatch"))
        .count();
    assert_eq!(matching, 1, "the one text match should surface");
    assert_eq!(should.rows.len() - matching, 9, "a should must admit documents it does not match");

    // As a must: exactly the matching set, and nothing else.
    let must = db
        .query(&format!(
            "SELECT id FROM items WHERE text_match(body, 'bandersnatch') \
             ORDER BY embedding <=> {q} LIMIT 10"
        ))
        .unwrap();
    assert_eq!(must.rows.len(), 1);
    assert!(must.rows.iter().all(|r| r
        .doc
        .path("body")
        .unwrap()
        .as_str()
        .unwrap()
        .contains("bandersnatch")));

    // And both in one query: the must narrows the candidate set, the should
    // ranks inside it.
    let both = db
        .query(&format!(
            "SELECT id FROM items WHERE text_match(body, 'saturation') \
             ORDER BY hybrid(text_match(body, 'curve postings'), embedding <=> {q}) LIMIT 5"
        ))
        .unwrap();
    assert!(!both.rows.is_empty());
    assert!(both.rows.iter().all(|r| r
        .doc
        .path("body")
        .unwrap()
        .as_str()
        .unwrap()
        .contains("saturation")));
}

#[test]
fn fresh_writes_are_searchable_at_exact_recall_before_any_flush() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 12, &[]);
    let c = build(&mut db, 300, 12, 3);

    // Nothing has been flushed: this is the memtable's flat, exact index.
    assert_eq!(db.shards("items").unwrap()[0].segments.len(), 0);

    // Query exactly one stored document's vector back.
    let target = db.query("SELECT id FROM items LIMIT 1").unwrap();
    let key = target.rows[0].key.clone();
    let emb: Vec<f32> = target.rows[0]
        .doc
        .path("embedding")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_f64().unwrap() as f32)
        .collect();

    let r = db
        .query(&format!(
            "SELECT id FROM items ORDER BY embedding <=> {} LIMIT 1",
            vec_literal(&emb)
        ))
        .unwrap();
    assert_eq!(r.rows[0].key, key, "the memtable must find an exact match exactly");
    assert!(r.rows[0].distance.unwrap() < 1e-5, "distance {:?}", r.rows[0].distance);
    let _ = c.query_near(0, 0.0);
}

#[test]
fn structured_filters_are_admission_predicates_not_post_filters() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    let c = build(&mut db, 600, 8, 5);
    db.execute("FLUSH items").unwrap();
    let q = vec_literal(&c.query_near(1, 0.0));

    // 1/7 of documents are starred, and they are spread across the vector
    // space. A post-filter over a top-10 would return almost nothing.
    let r = db
        .query(&format!(
            "SELECT id FROM items WHERE ANY(tags) = 'starred' AND status = 'published' \
             ORDER BY embedding <=> {q} LIMIT 10"
        ))
        .unwrap();
    assert_eq!(r.rows.len(), 10, "the filter must not starve the result");
    for row in &r.rows {
        let tags: Vec<&str> = row
            .doc
            .path("tags")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert!(tags.contains(&"starred"), "{tags:?}");
        assert_eq!(row.doc.path("status").unwrap().as_str(), Some("published"));
    }
}

// --------------------------------------------------------------------------
// Recall under sustained deletes
// --------------------------------------------------------------------------

#[test]
fn recall_at_10_holds_under_sustained_deletes() {
    let mut db = Db::with_opts(opts(512));
    setup(&mut db, 32, &["t1", "t2"]);
    let c = build(&mut db, 6000, 32, 17);
    db.execute("FLUSH items").unwrap();

    // Run some real queries so the harness has a production sample to replay.
    for t in 0..TOPICS.len() {
        let q = vec_literal(&c.query_near(t, 0.01));
        db.query(&format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10")).unwrap();
    }

    let before = match db.execute("MEASURE RECALL ON items WITH (k = 10, samples = 40)").unwrap() {
        Outcome::Recall(r) => r,
        _ => panic!(),
    };
    assert!(before.recall >= 0.95, "recall before deletes = {}", before.recall);

    // Delete 40% of the collection. This is the case that never shows up in a
    // benchmark on fresh data (§6): the graph still routes through the dead
    // vectors, and without adaptive k amplification the heap comes up short.
    let mut deleted = 0;
    for i in (0..6000).step_by(5) {
        for j in 0..2 {
            let idx = i + j;
            if idx >= 6000 {
                break;
            }
            let key = format!("t{}\u{1}doc-{idx:05}", idx % 3);
            if db.delete_key("items", &key).unwrap() {
                deleted += 1;
            }
        }
    }
    assert!(deleted > 2000, "expected a substantial delete load, got {deleted}");

    let after = match db.execute("MEASURE RECALL ON items WITH (k = 10, samples = 40)").unwrap() {
        Outcome::Recall(r) => r,
        _ => panic!(),
    };
    assert!(
        after.recall >= 0.95,
        "recall@10 after {deleted} deletes = {} (worst {})",
        after.recall,
        after.worst
    );

    // Compaction should now reclaim, and recall must survive the rewrite.
    db.execute("COMPACT items").unwrap();
    let compacted = match db.execute("MEASURE RECALL ON items WITH (k = 10, samples = 40)").unwrap()
    {
        Outcome::Recall(r) => r,
        _ => panic!(),
    };
    assert!(compacted.recall >= 0.95, "recall after compaction = {}", compacted.recall);
}

#[test]
fn the_recall_harness_catches_a_deliberate_regression() {
    // The harness is only worth having if it fails when recall actually drops.
    // `ef_search = 1` is a crippled traversal; the measurement must notice.
    let mut db = Db::with_opts(opts(256));
    setup(&mut db, 32, &[]);
    let c = build(&mut db, 3000, 32, 23);
    db.execute("FLUSH items").unwrap();

    let q = vec_literal(&c.query_near(3, 0.02));
    let good =
        db.query(&format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10")).unwrap();
    let exact = db
        .query(&format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10 WITH (exact)"))
        .unwrap();
    let crippled = db
        .query(&format!(
            "SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10 WITH (ef_search = 1)"
        ))
        .unwrap();

    let overlap = |a: &celastro::plan::QueryResult, b: &celastro::plan::QueryResult| {
        let bk: Vec<String> = keys(b);
        keys(a).iter().filter(|k| bk.contains(k)).count()
    };
    assert!(overlap(&good, &exact) >= 9, "default settings should be near-exact");
    assert!(
        overlap(&crippled, &exact) < overlap(&good, &exact),
        "a crippled traversal must measurably lose recall"
    );
}

// --------------------------------------------------------------------------
// Exact mode: bit-identical regardless of shard count
// --------------------------------------------------------------------------

#[test]
fn exact_mode_is_bit_identical_across_shard_counts() {
    let dims = 16;
    let n = 900;

    let run = |splits: &[&str]| {
        let mut db = Db::with_opts(opts(64));
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        for i in 0..n {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        let q = vec_literal(&c.query_near(2, 0.03));
        // `k` is set above the candidate count on purpose. Truncating each
        // source to top-k' *per shard* is a documented approximation (§7.2),
        // and it is the one thing that legitimately differs with shard count —
        // so a bit-identity claim has to take it off the table.
        let sql = format!(
            "SELECT id FROM items \
             ORDER BY hybrid(text_match(body, 'fusion candidates traversal'), embedding <=> {q}, \
                             method => 'rrf', k => 100000) \
             LIMIT 20 WITH (exact, exact_scoring)"
        );
        let r = db.query(&sql).unwrap();
        r.rows.iter().map(|x| (x.key.clone(), x.score.unwrap().to_bits())).collect::<Vec<_>>()
    };

    let one = run(&[]);
    let three = run(&["t1", "t2"]);
    let six = run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);
    assert_eq!(one.len(), 20);
    assert_eq!(one, three, "1 shard vs 3 shards");
    assert_eq!(one, six, "1 shard vs 6 shards");
}

#[test]
fn approximate_mode_across_shard_counts_stays_within_recall_tolerance() {
    // The counterpart to the test above: with approximation on, results
    // legitimately depend on segment boundaries, so the criterion is recall,
    // not identity.
    let dims = 16;
    let run = |splits: &[&str]| {
        let mut db = Db::with_opts(opts(64));
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 99);
        for i in 0..1200 {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        let q = vec_literal(&c.query_near(1, 0.02));
        keys(
            &db.query(&format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 20"))
                .unwrap(),
        )
    };
    let a = run(&[]);
    let b = run(&["t1", "t2"]);
    let shared = a.iter().filter(|k| b.contains(k)).count();
    assert!(shared >= 19, "approximate agreement {shared}/20: {a:?} vs {b:?}");
}

// --------------------------------------------------------------------------
// Visibility, isolation and the lifecycle
// --------------------------------------------------------------------------

#[test]
fn snapshot_isolation_and_read_your_writes() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    build(&mut db, 50, 8, 1);
    let before = db.clock.peek();

    let mut d = json::parse(
        r#"{"id":"doc-00001","tenant_id":"t1","status":"published","body":"rewritten body","tags":["odd"],"embedding":[0,0,0,0,0,0,0,1]}"#,
    )
    .unwrap();
    d.set_path("status", Value::Str("archived".into()));
    db.insert("items", d).unwrap();

    // Read-your-writes: the very next query sees the update.
    let r = db.query("SELECT id FROM items WHERE status = 'archived' LIMIT 10").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].doc.path("body").unwrap().as_str(), Some("rewritten body"));

    // Exactly one live version, before and after.
    let all = db.query("SELECT id FROM items LIMIT 1000").unwrap();
    assert_eq!(all.rows.len(), 50);
    assert_eq!(all.rows.iter().filter(|x| x.key.ends_with("doc-00001")).count(), 1);

    // And the older snapshot still holds the old version.
    let shard = &db.shards("items").unwrap()[0];
    let old = shard.get("t1\u{1}doc-00001", before).unwrap().unwrap();
    assert_ne!(old.path("body").unwrap().as_str(), Some("rewritten body"));
}

#[test]
fn counts_are_stable_across_flush_and_compaction() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &["t1", "t2"]);
    build(&mut db, 600, 8, 13);

    let count = |db: &mut Db| db.query("SELECT id FROM items LIMIT 100000").unwrap().rows.len();
    assert_eq!(count(&mut db), 600);

    db.execute("FLUSH items").unwrap();
    assert_eq!(count(&mut db), 600);

    for i in (0..600).step_by(2) {
        db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
    }
    assert_eq!(count(&mut db), 300);

    db.execute("COMPACT items").unwrap();
    assert_eq!(count(&mut db), 300);

    // Text and vector agree with the structured scan after the rewrite.
    let text =
        db.query("SELECT id FROM items WHERE text_match(body, 'compaction') LIMIT 1000").unwrap();
    assert!(!text.rows.is_empty());
    assert!(text.rows.iter().all(|r| r.doc.path("topic").unwrap().as_str() == Some("storage")));
}

#[test]
fn a_database_survives_reopen() {
    let dir = std::env::temp_dir().join(format!("celastro-it-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let q;
    let before;
    {
        let mut db = Db::open(&dir, opts(64)).unwrap();
        setup(&mut db, 12, &["t1", "t2"]);
        let mut c = build(&mut db, 500, 12, 29);
        db.execute("FLUSH items").unwrap();
        // Writes after the flush live only in the WAL.
        for i in 500..560 {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        db.delete_key("items", "t0\u{1}doc-00000").unwrap();
        q = vec_literal(&c.query_near(0, 0.01));
        before = keys(
            &db.query(&format!(
                "SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10 WITH (exact)"
            ))
            .unwrap(),
        );
        db.persist().unwrap();
    }

    let mut db = Db::open(&dir, opts(64)).unwrap();
    let total = db.query("SELECT id FROM items LIMIT 100000").unwrap().rows.len();
    assert_eq!(total, 559, "500 + 60 written, 1 deleted");
    assert!(db
        .query("SELECT id FROM items WHERE id = 'doc-00000' LIMIT 10")
        .unwrap()
        .rows
        .is_empty());
    let after = keys(
        &db.query(&format!(
            "SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10 WITH (exact)"
        ))
        .unwrap(),
    );
    assert_eq!(before, after, "the same query must answer the same after reopen");
    let _ = std::fs::remove_dir_all(&dir);
}

// --------------------------------------------------------------------------
// Observability and semantics
// --------------------------------------------------------------------------

#[test]
fn explain_analyze_reports_every_runtime_decision() {
    let mut db = Db::with_opts(opts(256));
    setup(&mut db, 16, &["t1", "t2"]);
    let c = build(&mut db, 3000, 16, 31);
    db.execute("FLUSH items").unwrap();
    let q = vec_literal(&c.query_near(4, 0.02));

    let text = match db
        .execute(&format!(
            "EXPLAIN ANALYZE SELECT id FROM items WHERE tenant_id = 't1' AND status = 'published' \
             ORDER BY hybrid(text_match(body, 'selectivity runtime'), embedding <=> {q}) LIMIT 10"
        ))
        .unwrap()
    {
        Outcome::Explain(t) => t,
        _ => panic!("expected a plan"),
    };

    for needle in [
        "pruned by partition key", // §8.4 partition pruning
        "term statistics",         // §8.2
        "survivors=",              // measured selectivity, §5.3
        "strategy=",               // the runtime vector choice
        "block-max WAND",          // §5.1
        "fusion at coordinator",   // §7.2
        "[column]",                // access path
        "fetch:",                  // query-then-fetch, §8.1
    ] {
        assert!(text.contains(needle), "EXPLAIN is missing `{needle}`:\n{text}");
    }
    // Two of three shards pruned by the tenant predicate.
    assert_eq!(text.matches("PRUNED").count(), 2, "{text}");
}

#[test]
fn a_query_without_a_partition_key_says_so() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &["t1", "t2"]);
    build(&mut db, 300, 8, 37);
    db.execute("FLUSH items").unwrap();
    let text = match db
        .execute("EXPLAIN ANALYZE SELECT id FROM items WHERE status = 'published' LIMIT 5")
        .unwrap()
    {
        Outcome::Explain(t) => t,
        _ => panic!(),
    };
    assert!(text.contains("fans out to every shard"), "{text}");
}

#[test]
fn collapse_by_keeps_the_best_child_per_parent() {
    // The v1 answer to multi-vector documents (§5.4): chunks are documents with
    // a parent_id, and COLLAPSE BY keeps the best-scoring child per parent.
    let mut db = Db::with_opts(opts(64));
    db.execute("CREATE COLLECTION chunks (id TEXT PRIMARY KEY, parent_id TEXT NOT NULL)").unwrap();
    db.execute(
        "CREATE INDEX chunks_emb ON chunks USING vector (embedding) WITH (dims = 4, metric = 'l2')",
    )
    .unwrap();
    let mut rng = Rng::new(101);
    for parent in 0..20 {
        for chunk in 0..8 {
            let emb: Vec<Value> = (0..4)
                .map(|d| {
                    Value::Float(
                        (parent as f32 * 0.1 + d as f32 * 0.01 + rng.next_normal() * 0.02) as f64,
                    )
                })
                .collect();
            db.insert(
                "chunks",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("p{parent:02}-c{chunk}"))),
                    ("parent_id".into(), Value::Str(format!("p{parent:02}"))),
                    ("embedding".into(), Value::Array(emb)),
                ]),
            )
            .unwrap();
        }
    }
    db.execute("FLUSH chunks").unwrap();

    let plain = db
        .query("SELECT id FROM chunks ORDER BY embedding <-> [0.5,0.51,0.52,0.53] LIMIT 5")
        .unwrap();
    let parents_plain: Vec<String> = plain
        .rows
        .iter()
        .map(|r| r.doc.path("parent_id").unwrap().as_str().unwrap().to_string())
        .collect();
    let distinct_plain = {
        let mut p = parents_plain.clone();
        p.sort();
        p.dedup();
        p.len()
    };

    let collapsed = db
        .query(
            "SELECT id FROM chunks ORDER BY embedding <-> [0.5,0.51,0.52,0.53] LIMIT 5 \
             COLLAPSE BY parent_id",
        )
        .unwrap();
    let parents: Vec<String> = collapsed
        .rows
        .iter()
        .map(|r| r.doc.path("parent_id").unwrap().as_str().unwrap().to_string())
        .collect();
    let mut sorted = parents.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), parents.len(), "collapse must not repeat a parent: {parents:?}");
    assert_eq!(collapsed.rows.len(), 5);
    assert!(
        distinct_plain < parents.len() || distinct_plain == 5,
        "without collapse the top-5 should be dominated by a few parents ({distinct_plain} distinct)"
    );
}

#[test]
fn pagination_by_cursor_walks_the_whole_result() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    let c = build(&mut db, 200, 8, 41);
    db.execute("FLUSH items").unwrap();
    let q = vec_literal(&c.query_near(0, 0.0));

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..5 {
        let sql = match &cursor {
            None => format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10"),
            Some(cur) => {
                format!("SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 10 AFTER '{cur}'")
            }
        };
        let r = db.query(&sql).unwrap();
        if r.rows.is_empty() {
            break;
        }
        seen.extend(keys(&r));
        cursor = r.next_cursor.clone();
    }
    let mut dedup = seen.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), seen.len(), "cursor paging must not repeat a row");
    assert_eq!(seen.len(), 50);
}

#[test]
fn polymorphic_paths_do_not_coerce_silently() {
    let mut db = Db::with_opts(opts(64));
    db.execute("CREATE COLLECTION mixed (id TEXT PRIMARY KEY)").unwrap();
    for i in 0..50 {
        db.insert(
            "mixed",
            json::parse(&format!(r#"{{"id":"n{i:03}","x":{i},"when":"2026-01-0{}"}}"#, i % 9 + 1))
                .unwrap(),
        )
        .unwrap();
    }
    for i in 50..100 {
        db.insert("mixed", json::parse(&format!(r#"{{"id":"s{i:03}","x":"{i}"}}"#)).unwrap())
            .unwrap();
    }
    db.execute("FLUSH mixed").unwrap();

    // A numeric literal matches only the numeric half; the string half is NULL,
    // not coerced.
    let nums = db.query("SELECT id FROM mixed WHERE x < 10 LIMIT 1000").unwrap();
    assert_eq!(nums.rows.len(), 10);
    let strs = db.query("SELECT id FROM mixed WHERE x = '75' LIMIT 1000").unwrap();
    assert_eq!(strs.rows.len(), 1);

    // An undeclared ISO-8601 string is a string, so a timestamp comparison
    // against it finds nothing rather than guessing.
    let ts =
        db.query("SELECT id FROM mixed WHERE when > timestamp '2026-01-01' LIMIT 1000").unwrap();
    assert_eq!(ts.rows.len(), 0, "undeclared ISO-8601 stays text (§2.1)");
    let as_text = db.query("SELECT id FROM mixed WHERE when > '2026-01-05' LIMIT 1000").unwrap();
    assert!(!as_text.rows.is_empty());
}

#[test]
fn errors_name_the_thing_that_is_wrong() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    build(&mut db, 20, 8, 2);

    let cases: Vec<(&str, &str)> = vec![
        ("SELECT id FROM items ORDER BY hybrid(text_match(body, 'x'))", "LIMIT"),
        ("SELECT id FROM items ORDER BY embedding <=> [1.0,2.0] LIMIT 5", "dimensions"),
        ("SELECT id FROM items ORDER BY embedding <-> [1,2,3,4,5,6,7,8] LIMIT 5", "metric"),
        ("SELECT id FROM nope LIMIT 1", "no such collection"),
        (
            "SELECT id FROM items ORDER BY missing_field <=> [1,2,3,4,5,6,7,8] LIMIT 5",
            "no vector index",
        ),
        ("DELETE FROM items", "without WHERE"),
    ];
    for (sql, needle) in cases {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(e.contains(needle), "`{sql}` gave `{e}`, expected to mention `{needle}`");
    }
}

#[test]
fn a_deadline_fails_loudly_unless_partial_results_is_requested() {
    let mut db = Db::with_opts(opts(256));
    setup(&mut db, 32, &["t1", "t2"]);
    build(&mut db, 3000, 32, 53);
    db.execute("FLUSH items").unwrap();

    // A deadline of zero is exceeded before the first shard is reached.
    let e =
        db.execute("SELECT id FROM items LIMIT 10 WITH (deadline_ms = 0)").unwrap_err().to_string();
    assert!(e.contains("deadline"), "{e}");
    assert!(e.contains("partial_results"), "the error should name the opt-in: {e}");

    let r =
        db.query("SELECT id FROM items LIMIT 10 WITH (deadline_ms = 0, partial_results)").unwrap();
    assert!(!r.missing.is_empty(), "a partial answer must carry the list of missing tablets");
}

// --------------------------------------------------------------------------
// Regressions found in review
// --------------------------------------------------------------------------

#[test]
fn an_update_before_a_flush_does_not_lose_the_document() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    build(&mut db, 40, 8, 61);
    let mut d = json::parse(
        r#"{"id":"doc-00007","tenant_id":"t1","status":"published","body":"rewritten",
            "tags":["odd"],"embedding":[1,0,0,0,0,0,0,0]}"#,
    )
    .unwrap();
    d.set_path("status", Value::Str("archived".into()));
    db.insert("items", d).unwrap();
    db.execute("FLUSH items").unwrap();

    assert_eq!(db.query("SELECT id FROM items LIMIT 1000").unwrap().rows.len(), 40);
    let got = db.query("SELECT id FROM items WHERE id = 'doc-00007' LIMIT 5").unwrap();
    assert_eq!(got.rows.len(), 1);
    assert_eq!(got.rows[0].doc.path("body").unwrap().as_str(), Some("rewritten"));
}

#[test]
fn a_collection_smaller_than_a_bloom_filter_can_be_flushed() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 4, &[]);
    for i in 0..3 {
        db.insert(
            "items",
            json::parse(&format!(
                r#"{{"id":"d{i}","tenant_id":"t0","status":"published","body":"tiny {i}",
                     "tags":["x"],"embedding":[1,0,0,0]}}"#
            ))
            .unwrap(),
        )
        .unwrap();
    }
    db.execute("FLUSH items").unwrap();
    assert_eq!(db.query("SELECT id FROM items LIMIT 10").unwrap().rows.len(), 3);
}

#[test]
fn hybrid_pagination_walks_past_the_first_page() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 8, &[]);
    let c = build(&mut db, 300, 8, 67);
    db.execute("FLUSH items").unwrap();
    let q = vec_literal(&c.query_near(2, 0.01));

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 0..4 {
        let sql = match &cursor {
            None => format!(
                "SELECT id FROM items ORDER BY hybrid(text_match(body, 'fusion candidates'), \
                 embedding <=> {q}, method => 'rrf') LIMIT 5"
            ),
            Some(cur) => format!(
                "SELECT id FROM items ORDER BY hybrid(text_match(body, 'fusion candidates'), \
                 embedding <=> {q}, method => 'rrf') LIMIT 5 AFTER '{cur}'"
            ),
        };
        let r = db.query(&sql).unwrap();
        assert!(!r.rows.is_empty(), "page {page} of a hybrid query came back empty");
        seen.extend(keys(&r));
        cursor = r.next_cursor.clone();
    }
    let mut dedup = seen.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), seen.len(), "hybrid pagination repeated a row");
    assert_eq!(seen.len(), 20);
}

#[test]
fn negation_is_three_valued_and_agrees_with_not_equals() {
    let mut db = Db::with_opts(opts(64));
    db.execute("CREATE COLLECTION t (id TEXT PRIMARY KEY)").unwrap();
    db.insert("t", json::parse(r#"{"id":"has","status":"published"}"#).unwrap()).unwrap();
    db.insert("t", json::parse(r#"{"id":"other","status":"draft"}"#).unwrap()).unwrap();
    db.insert("t", json::parse(r#"{"id":"missing"}"#).unwrap()).unwrap();

    let ids = |db: &mut Db, sql: &str| {
        let mut v: Vec<String> = db
            .query(sql)
            .unwrap()
            .rows
            .iter()
            .map(|r| r.doc.path("id").unwrap().as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };

    for stage in ["memtable", "segment"] {
        // `NOT (a = b)` and `a <> b` must agree, and neither may select a row
        // where the value is absent — that comparison is NULL, not false.
        assert_eq!(
            ids(&mut db, "SELECT * FROM t WHERE status <> 'published' LIMIT 10"),
            vec!["other"],
            "{stage}"
        );
        assert_eq!(
            ids(&mut db, "SELECT * FROM t WHERE NOT (status = 'published') LIMIT 10"),
            vec!["other"],
            "{stage}"
        );
        assert_eq!(
            ids(&mut db, "SELECT * FROM t WHERE NOT (status <> 'published') LIMIT 10"),
            vec!["has"],
            "{stage}"
        );
        // And a type-mismatched literal is NULL, so neither side selects it.
        assert_eq!(
            ids(&mut db, "SELECT * FROM t WHERE status <> 5 LIMIT 10"),
            Vec::<String>::new(),
            "{stage}"
        );
        assert_eq!(
            ids(&mut db, "SELECT * FROM t WHERE status IS NULL LIMIT 10"),
            vec!["missing"],
            "{stage}"
        );
        db.execute("FLUSH t").unwrap();
    }
}

#[test]
fn a_numeric_partition_key_routes_and_prunes_consistently() {
    let mut db = Db::with_opts(opts(64));
    db.execute(
        "CREATE COLLECTION p (id TEXT PRIMARY KEY, tid NUMBER NOT NULL) \
         PARTITION BY (tid) WITH (splits = ['5'])",
    )
    .unwrap();
    // The same partition key written three ways.
    db.insert("p", json::parse(r#"{"id":"a","tid":5}"#).unwrap()).unwrap();
    db.insert("p", json::parse(r#"{"id":"b","tid":5.0}"#).unwrap()).unwrap();
    db.insert("p", json::parse(r#"{"id":"c","tid":7}"#).unwrap()).unwrap();
    db.execute("FLUSH p").unwrap();

    let mut got: Vec<String> = db
        .query("SELECT id FROM p WHERE tid = 5 LIMIT 10")
        .unwrap()
        .rows
        .iter()
        .map(|r| r.doc.path("id").unwrap().as_str().unwrap().to_string())
        .collect();
    got.sort();
    assert_eq!(got, vec!["a", "b"], "5 and 5.0 are the same partition key");
    // And the same answer with the pruning turned off by a range predicate.
    let range = db.query("SELECT id FROM p WHERE tid >= 5 AND tid <= 5 LIMIT 10").unwrap();
    assert_eq!(range.rows.len(), 2);
}

#[test]
fn a_key_value_cannot_forge_another_partition() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 4, &[]);
    // A control character is not legal raw in JSON, so it arrives escaped —
    // which is exactly how a real client would send it.
    let doc = json::parse(
        r#"{"id":"x","tenant_id":"a\u0001b","status":"published","body":"x","tags":[],
            "embedding":[1,0,0,0]}"#,
    )
    .unwrap();
    assert_eq!(doc.path("tenant_id").unwrap().as_str(), Some("a\u{1}b"));
    let e = db.insert("items", doc).unwrap_err().to_string();
    assert!(e.contains("separator"), "{e}");
}

#[test]
fn queries_that_would_allocate_the_address_space_are_refused() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 4, &[]);
    build(&mut db, 20, 4, 71);
    for (sql, needle) in [
        (
            "SELECT id FROM items ORDER BY hybrid(text_match(body, 'x'), k => 0) LIMIT 5",
            "positive",
        ),
        (
            "SELECT id FROM items ORDER BY hybrid(text_match(body, 'x'), k => -1) LIMIT 5",
            "positive",
        ),
        (
            "SELECT id FROM items WHERE published_at > now() - interval '9000000000000 days' LIMIT 5",
            "out of range",
        ),
    ] {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(e.contains(needle), "`{sql}` gave `{e}`");
    }
    // A huge but legal k' is clamped rather than allocated.
    db.query("SELECT id FROM items ORDER BY hybrid(text_match(body, 'item'), k => 1000000000000) LIMIT 5")
        .unwrap();
    // And a DELETE, whose internal limit is unbounded, does not overflow.
    db.execute("DELETE FROM items WHERE tenant_id = 't0'").unwrap();
}

#[test]
fn unicode_literals_and_query_text_do_not_panic() {
    let mut db = Db::with_opts(opts(64));
    setup(&mut db, 4, &[]);
    db.insert(
        "items",
        json::parse(
            r#"{"id":"u","tenant_id":"t0","status":"published","body":"Café Crème aππed",
                "tags":["日本語"],"embedding":[1,0,0,0]}"#,
        )
        .unwrap(),
    )
    .unwrap();
    db.execute("FLUSH items").unwrap();
    db.query("SELECT id FROM items WHERE status = '日本語日本語日本語日本語' LIMIT 5").unwrap();
    db.query("SELECT id FROM items WHERE text_match(body, 'aππed') LIMIT 5").unwrap();
    db.query("SELECT id FROM items WHERE ANY(tags) = '日本語' LIMIT 5").unwrap();
    // A folded prefix reaches a folded dictionary.
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'Caf*') LIMIT 5").unwrap();
    assert_eq!(r.rows.len(), 1);
}
