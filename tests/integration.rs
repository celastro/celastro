//! End-to-end tests against the guarantees in §14.
//!
//! | criterion | test |
//! |---|---|
//! | hybrid queries provably correct; harness trusted | [`hybrid_retrieval_is_a_union_of_all_three_modes`], [`the_recall_harness_catches_a_deliberate_regression`] |
//! | recall@10 ≥ 0.95 under sustained deletes | [`recall_at_10_holds_under_sustained_deletes`] |
//! | in exact mode, results bit-identical regardless of shard count | [`exact_mode_is_bit_identical_across_shard_counts`], [`exact_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes`], [`exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes`] |
//! | in default mode, a freshly refreshed gather matches `WITH (exact_scoring)` for Term, Phrase and Prefix queries, and the triple is identical at every shard count fresh or stale | [`default_statistics_are_identical_across_shard_counts_under_updates_and_deletes`], [`default_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes`] |
//! | a prefix query names the same terms and ranks them the same way at every shard count | [`a_prefix_query_ranks_the_same_at_every_shard_count`], [`a_prefix_query_finds_the_same_documents_at_every_shard_count`] |
//! | a prefix names the LIVE vocabulary, so a dead term cannot displace a live one out of the cap | [`a_prefix_query_names_the_live_vocabulary_not_the_physical_one`] |
//! | a cut EXCLUSION set is reported as keeping rows, not as losing them | [`a_truncated_exclusion_says_rows_were_kept_not_that_rows_are_missing`] |
//! | a prefix expansion the cap cut says so, on every query shape | `engine::tests::a_truncated_prefix_says_so_on_a_plain_query_of_either_shape`, `engine::tests::an_expansion_of_exactly_the_cap_dropped_nothing_and_must_not_say_it_did` |
//! | state survives a reopen | [`a_database_survives_reopen`] |

use std::collections::BTreeMap;

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
fn exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes() {
    // The primary pin for the exact-mode invariant, asserted on the statistic
    // itself rather than through a fused score. The test above cannot see a
    // BM25 statistic at all — see the comment on it — so a shard-count
    // dependent `avg_doc_len` sat under a green suite. Here the quantity is
    // compared directly, by bits.
    //
    // Updates and deletes are both needed, because they are what makes a
    // physical row differ from a live one: a seal drops a version the write
    // path had already superseded, a compaction drops a deleted one, and each
    // shard reaches its own threshold at its own moment. A statistic summed
    // over physical rows therefore acquires a dependency on the shard count,
    // which is exactly what this asserts is absent.
    let dims = 16;
    let n = 900;

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        // Small enough that the shards seal at their own pace rather than all
        // holding everything in one memtable until the final `FLUSH`.
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        for i in 0..n {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(7) {
            let mut d = c.doc(i);
            d.set_path("body", Value::Str(format!("rewritten fusion item {i}")));
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
        }
        db.execute("FLUSH items").unwrap();

        let want = BTreeMap::from([(
            "body".to_string(),
            vec!["fusion".to_string(), "traversal".to_string()],
        )]);
        let ts = db.clock.peek();
        let s = db.gather_stats("items", &want, ts, true).unwrap();
        let g = &s["body"];
        // By bits: an average is a float, and "identical" has to mean it.
        (g.num_docs, g.avg_doc_len.to_bits(), g.doc_freq.clone())
    };

    let one = run(&[]);
    let three = run(&["t1", "t2"]);
    let six = run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);
    // An absolute anchor first, because the three legs are otherwise compared
    // only against each other. `Db::gather_stats` answers `(0, bits of 1.0,
    // {})` when it finds no documents, so a change that made the analyzed
    // dictionary stop matching these terms, or made `text_handle` quietly
    // return `None`, would agree at every shard count and pass green.
    assert_eq!(one.0, 818, "900 written, one in eleven deleted");
    assert_eq!(
        one.2,
        BTreeMap::from([("fusion".to_string(), 258u64), ("traversal".to_string(), 140)]),
        "both terms are really in the corpus, at their real frequencies"
    );
    assert!(
        f64::from_bits(one.1) > 2.0,
        "and the length norm is a real average, not the num_docs == 0 fallback of 1.0"
    );
    assert_eq!(one, three, "1 shard vs 3 shards");
    assert_eq!(one, six, "1 shard vs 6 shards");
}

#[test]
fn default_statistics_are_identical_across_shard_counts_under_updates_and_deletes() {
    // The sibling of `exact_statistics_...` above, on the path almost every
    // query actually takes. The exact path buys its invariance with a gather
    // per query; this one is what the default costs, and until the cached
    // statistics became live sums at one instant it did not have it. Measured
    // on the code this test was written against, this exact corpus answered
    // 831 documents at one shard, 853 at three and 965 at six, with
    // df{fusion} 270 / 271 / 295 and df{traversal} 140 / 145 / 168, against an
    // exact 818 / 258 / 140 at all three: the cache counted physical rows, and
    // how many of those survive is each shard's own seal and compaction
    // decision.
    //
    // THE TRAP, and it is easy to fall into: `Db::run_select` gathers EXACT
    // statistics when `sel.with.exact_scoring || sel.with.exact`, so borrowing
    // the `WITH (exact, exact_scoring)` idiom from the tests above and
    // dropping only `exact_scoring` still measures the exact path, and the
    // test passes without proving anything. This one calls `gather_stats`
    // with `exact: false` directly, and its score-level sibling below passes
    // no `WITH` clause at all.
    //
    // The low `max_bytes`, the rewrites and the deletes are all load-bearing:
    // without per-shard seal schedules over dead rows the three legs agree for
    // the wrong reason.
    let dims = 16;
    let n = 900;

    let want = || {
        BTreeMap::from([("body".to_string(), vec!["fusion".to_string(), "traversal".to_string()])])
    };

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        for i in 0..n {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(7) {
            let mut d = c.doc(i);
            d.set_path("body", Value::Str(format!("rewritten fusion item {i}")));
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        // Compaction as well as a seal, because they drop different rows: a
        // seal drops a superseded version, a compaction drops a deleted one,
        // and both run on each shard's own schedule.
        db.execute("COMPACT items").unwrap();

        let ts = db.clock.peek();
        let s = db.gather_stats("items", &want(), ts, false).unwrap();
        let g = &s["body"];
        assert!(!g.exact, "this leg has to be measuring the cached path");
        let cached = (g.num_docs, g.avg_doc_len.to_bits(), g.doc_freq.clone());

        // And the exact triple on the same corpus at the same instant, to be
        // the absolute anchor: three legs compared only against each other
        // pass just as happily on three equal wrong numbers.
        let s = db.gather_stats("items", &want(), ts, true).unwrap();
        let g = &s["body"];
        let exact = (g.num_docs, g.avg_doc_len.to_bits(), g.doc_freq.clone());

        // The stale leg. These writes are far short of `STATS_REFRESH_WRITES`,
        // so the next gather reads the cache without rebuilding it. That read
        // is deliberately NOT expected to match the corpus any more — it is
        // expected to match what the last refresh point measured, at every
        // shard count. Staleness is the residual this design keeps; a
        // shard-count dependence is the one it removes, and this is what
        // separates them.
        for i in n..n + 200 {
            let d = c.doc(i);
            db.insert("items", d).unwrap();
        }
        let ts = db.clock.peek();
        let s = db.gather_stats("items", &want(), ts, false).unwrap();
        let g = &s["body"];
        let stale = (g.num_docs, g.avg_doc_len.to_bits(), g.doc_freq.clone());

        (cached, exact, stale)
    };

    let one = run(&[]);
    let three = run(&["t1", "t2\u{1}doc-00450"]);
    let six = run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);

    // The anchor: at one shard the cached triple is the exact triple. Nothing
    // about the shard count can make this true by accident, and it is what
    // stops the equality assertions below from passing on a cache that has
    // quietly stopped finding these terms at all.
    assert_eq!(one.0, one.1, "a fresh cache answers what the exact gather answers");
    assert_eq!(one.0 .0, 818, "900 written, one in eleven deleted");
    assert_eq!(
        one.0 .2,
        BTreeMap::from([("fusion".to_string(), 258u64), ("traversal".to_string(), 140)]),
        "both terms are really in the corpus, at their real frequencies"
    );
    assert!(
        f64::from_bits(one.0 .1) > 2.0,
        "and the length norm is a real average, not the num_docs == 0 fallback of 1.0"
    );

    assert_eq!(one.0, three.0, "cached, 1 shard vs 3 shards");
    assert_eq!(one.0, six.0, "cached, 1 shard vs 6 shards");
    assert_eq!(one.1, three.1, "exact, 1 shard vs 3 shards");
    assert_eq!(one.1, six.1, "exact, 1 shard vs 6 shards");

    // The stale read is still the value the refresh point measured, and it is
    // still the same at every shard count.
    assert_eq!(one.2, one.0, "no refresh point passed, so the cache did not move");
    assert_eq!(one.2, three.2, "stale, 1 shard vs 3 shards");
    assert_eq!(one.2, six.2, "stale, 1 shard vs 6 shards");
}

#[test]
fn default_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes() {
    // The score-level pin for the statistic the test above asserts directly,
    // and the sibling of `exact_mode_is_bit_identical_...`: the same workload,
    // the same four load-bearing ingredients described there, but with NO
    // `WITH` clause, so the query takes the path an ordinary query takes.
    //
    // The trap again, because it is the one way to write this test and prove
    // nothing: `Db::run_select` gathers exact statistics for
    // `sel.with.exact_scoring || sel.with.exact`, so `WITH (exact)` alone
    // silently upgrades the STATISTICS too. `k => 100000` on its own is
    // enough to neutralise per-shard `k'` truncation, which is what the
    // `WITH` clause was doing for the exact tests.
    //
    // The assertion is on score BITS, not on order, and deliberately so: on
    // this fixture all twenty scores differed at both 1-vs-3 and 1-vs-6 before
    // the fix while the top-20 ORDER happened to survive, because these
    // documents have homogeneous term composition and a uniform statistic
    // shift is then a monotone rescaling that min-max normalisation absorbs.
    // Order is not generally safe: on a corpus of heterogeneous term
    // composition (4000 documents, cubic-zipf vocabulary, 200 two-term
    // queries) six shards reordered the top 10 for 17 queries and returned a
    // different document SET for 6 of them, against 0 of 200 with
    // `WITH (exact_scoring)` on the identical corpus.
    let dims = 16;
    let n = 900;

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        let varied = |c: &mut Corpus, i: usize| {
            let mut d = c.doc(i);
            let base = d.path("body").unwrap().as_str().unwrap().to_string();
            let filler = "padding ".repeat(1 + i % 9);
            d.set_path("body", Value::Str(format!("{base} {filler}")));
            d
        };
        for i in 0..n {
            let d = varied(&mut c, i);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(7) {
            let mut d = varied(&mut c, i);
            d.set_path("body", Value::Str(format!("rewritten fusion item {i}")));
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        db.execute("COMPACT items").unwrap();
        // Text only: a vector source would put an ANN traversal between the
        // statistic and the assertion, and this is a test about the statistic.
        let sql = "SELECT id FROM items \
                   ORDER BY hybrid(text_match(body, 'fusion traversal'), \
                                   method => 'linear', k => 100000) \
                   LIMIT 20";
        let r = db.query(sql).unwrap();
        r.rows.iter().map(|x| (x.key.clone(), x.score.unwrap().to_bits())).collect::<Vec<_>>()
    };

    let one = run(&[]);
    let three = run(&["t1", "t2\u{1}doc-00450"]);
    let six = run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);
    assert_eq!(one.len(), 20);
    assert_eq!(one, three, "1 shard vs 3 shards");
    assert_eq!(one, six, "1 shard vs 6 shards");
}

// --------------------------------------------------------------------------
// Prefix expansion: the same query at every shard count
// --------------------------------------------------------------------------

/// The vocabulary both prefix tests draw from, as a body string for document
/// `i`: deterministic in `i` alone, so the same document carries the same
/// terms in every leg however the legs are split.
fn zipf_body(i: usize, vocab: usize, seed: u64) -> String {
    let mut rng = Rng::new(seed ^ i as u64);
    let count = 3 + rng.next_usize(6);
    let mut terms = Vec::with_capacity(count);
    for _ in 0..count {
        // Cubic zipf. A FLAT draw is the trap: it gives every term in the
        // expansion nearly the same document frequency, so a segment-local df
        // and the collection-wide df agree by construction and the defect this
        // test exists to catch cannot reach an assertion. Cubing the uniform
        // draw crowds it onto the low indices and spreads the frequencies
        // inside one expansion over orders of magnitude.
        let u = rng.next_f64();
        terms.push(format!("pterm{:03}", ((vocab as f64 * u * u * u) as usize).min(vocab - 1)));
    }
    let filler = "padding ".repeat(1 + i % 9);
    format!("{} {filler}", terms.join(" "))
}

#[test]
fn a_prefix_query_ranks_the_same_at_every_shard_count() {
    // The prefix sibling of `default_mode_is_bit_identical_...`, and the
    // reason it needed one: `TextQuery::leaf_terms` skipped `Prefix`, so the
    // coordinator gathered no global df for an expanded term and every
    // searchable unit fell back to its own segment-local dictionary count.
    // `scorer::compile` runs once per UNIT, so the weight a term carries was a
    // function of which units exist — that is, of the shard count and of the
    // flush and compaction schedule.
    //
    // Measured on the code this test was written against: all 25 queries below
    // returned a different top-10 key ORDER *and* a different top-10 key SET at
    // three shards and at six than at one — 25/25 on each of the four
    // comparisons. Adding `WITH (exact_scoring)` changed nothing, 25/25 again,
    // which is what rules the statistics cache out and points at the prefix
    // hole. None of these queries truncates — a 40-term vocabulary against a
    // cap of 512 — so this leg isolates the document-frequency half of the
    // defect; its sibling below isolates the expansion-set half.
    //
    // Load-bearing, and the test proves nothing without all of them: the
    // cubic-zipf vocabulary (see `zipf_body`), `method => 'linear'` (RRF ranks,
    // so a weight shift is invisible unless it flips one), per-document length
    // variance, updates, deletes and a small `max_bytes` so the units really do
    // hold different rows, a split that cuts through the middle of a tenant,
    // and `k => 100000` so per-shard `k'` truncation is off the table.
    //
    // The assertion is the KEY SEQUENCE, not score bits, and deliberately:
    // `DisjunctionScorer::score` sums f32 in cursor order, so a query with
    // three or more disjuncts — which every prefix expansion is — is not
    // bit-identical across shard counts for reasons that have nothing to do
    // with prefixes. Bit equality is asserted on the gathered triple instead.
    let dims = 16;
    let n = 4000;
    const VOCAB: usize = 40;

    let queries = || {
        // Four ten-term expansions, twenty one-term expansions and the
        // whole forty-term vocabulary. The one-term ones matter as much as
        // the wide ones: a single expanded term still took its weight from
        // the unit that scored it, and units disagree, so documents that
        // live in different units were compared on different scales.
        let mut q: Vec<String> = (0..4).map(|d| format!("pterm0{d}*")).collect();
        q.extend((0..10).map(|d| format!("pterm00{d}*")));
        q.extend((0..10).map(|d| format!("pterm01{d}*")));
        q.push("pterm*".to_string());
        q
    };

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        let varied = |c: &mut Corpus, i: usize, seed: u64| {
            let mut d = c.doc(i);
            d.set_path("body", Value::Str(zipf_body(i, VOCAB, seed)));
            d
        };
        for i in 0..n {
            let d = varied(&mut c, i, 0x9157);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(7) {
            let d = varied(&mut c, i, 0x2b41);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        db.execute("COMPACT items").unwrap();
        queries()
            .into_iter()
            .map(|p| {
                let sql = format!(
                    "SELECT id FROM items \
                     ORDER BY hybrid(text_match(body, '{p}'), method => 'linear', k => 100000) \
                     LIMIT 10"
                );
                keys(&db.query(&sql).unwrap())
            })
            .collect::<Vec<_>>()
    };

    let one = run(&[]);
    let three = run(&["t1", "t2\u{1}doc-00450"]);
    let six = run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);

    // The anchor: every query really found something, so the equalities below
    // cannot pass on three empty answers.
    for (p, r) in queries().iter().zip(&one) {
        assert_eq!(r.len(), 10, "`{p}` returned {} of 10 documents at one shard", r.len());
    }
    for ((p, a), b) in queries().iter().zip(&one).zip(&three) {
        assert_eq!(a, b, "`{p}`: 1 shard vs 3 shards");
    }
    for ((p, a), b) in queries().iter().zip(&one).zip(&six) {
        assert_eq!(a, b, "`{p}`: 1 shard vs 6 shards");
    }
}

#[test]
fn a_prefix_query_finds_the_same_documents_at_every_shard_count() {
    // The second, larger half of the same defect, with no scoring in it at
    // all. `PREFIX_EXPANSION_LIMIT` used to be applied per searchable UNIT
    // inside `scorer::build`, so each unit expanded its own dictionary and
    // truncated at its own lexicographic cut. More units means each one's
    // dictionary is smaller, so its first 512 covers a larger fraction of it
    // and the union grows: the SET OF TERMS the query means was a function of
    // the flush and compaction schedule, and the match set moved with it.
    //
    // Measured on the code this test was written against, on this fixture:
    // `a*` returned 2473 rows at one shard, 2523 at three and 2611 at six, of
    // 6000 that match, and `zed -a*` the complements — 3527 / 3477 / 3389 —
    // silently, on the two most ordinary prefix shapes there are. Pinning the
    // lexicographically first 512 terms of the UNION over every unit at the
    // coordinator answers 1511 and 4489 at all three.
    //
    // Note what the trade is, because it is not a free win: the invariant
    // answer is SMALLER than the largest of the three it replaces. A wide
    // prefix is a partial answer by construction either way; what changes is
    // that the partiality stops depending on the flush and compaction
    // schedule, and starts being a property of the collection — the terms
    // nearest the start of the alphabet, which is arbitrary but stable.
    //
    // This also covers the WHERE call site, which the ranking test above does
    // not reach: a `text_match` predicate compiles a scorer and throws the
    // scores away.
    let dims = 16;
    let n = 6000;
    const VOCAB: usize = 4000;
    // Measured, and re-measure it if the fixture moves rather than adjusting
    // it until it passes.
    const PINNED: usize = 1511;

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        for i in 0..n {
            let mut d = c.doc(i);
            let mut rng = Rng::new(0x51ed ^ i as u64);
            let (x, y) = (rng.next_usize(VOCAB), rng.next_usize(VOCAB));
            // `zed` is in every document, so the negated leg below has a
            // positive clause that admits everything and measures nothing but
            // the exclusion set.
            d.set_path("body", Value::Str(format!("zed a{x:05} a{y:05}")));
            db.insert("items", d).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        db.execute("COMPACT items").unwrap();
        let rows = |db: &mut Db, q: &str| {
            let sql = format!("SELECT id FROM items WHERE text_match(body, '{q}') LIMIT 1000000");
            let mut ks = keys(&db.query(&sql).unwrap());
            ks.sort();
            ks
        };
        // The negated leg is here and not in a test of its own because it is
        // the same fixture and the same cap: `leaf_prefixes` descends into
        // `Not` where `leaf_terms` does not, precisely because a negated
        // prefix's expansion is the EXCLUSION set. Copy `leaf_terms`' `Not`
        // arm and every unit re-derives its own exclusion list, so which
        // documents the query drops moves with the layout — the same defect,
        // wearing the opposite sign.
        (rows(&mut db, "a*"), rows(&mut db, "zed -a*"))
    };

    let (one, none) = run(&[]);
    let (three, nthree) = run(&["t1", "t2\u{1}doc-00450"]);
    let (six, nsix) =
        run(&["t0\u{1}doc-00300", "t1", "t1\u{1}doc-00600", "t2", "t2\u{1}doc-00600"]);

    // The expansion really does truncate here — 4000 distinct terms against a
    // cap of 512 — so this is a PARTIAL answer by construction at every shard
    // count. That is the point: partial and invariant, not partial and moving.
    // The anchor is the count itself: a change that made `a*` stop matching
    // would agree at every shard count and pass green without it.
    assert_eq!(one.len(), PINNED, "the pinned first 512 terms cover this many of the {n}");
    assert_eq!(none.len(), n - PINNED, "and `-a*` excludes exactly the complement");
    // Row counts first: the sets differ by hundreds of rows when this
    // regresses, and a diff of two 1500-element key vectors says much less
    // than the two counts do.
    assert_eq!(one.len(), three.len(), "row count, 1 shard vs 3 shards");
    assert_eq!(one.len(), six.len(), "row count, 1 shard vs 6 shards");
    assert_eq!(one, three, "1 shard vs 3 shards");
    assert_eq!(one, six, "1 shard vs 6 shards");
    assert_eq!(none.len(), nthree.len(), "`-a*` row count, 1 shard vs 3 shards");
    assert_eq!(none.len(), nsix.len(), "`-a*` row count, 1 shard vs 6 shards");
    assert_eq!(none, nthree, "`-a*`, 1 shard vs 3 shards");
    assert_eq!(none, nsix, "`-a*`, 1 shard vs 6 shards");
}

/// A collection of `n` documents `k00000..`, each holding `zed a#####` with its
/// own distinct `a` term, of which the first `dead` are then rewritten to hold
/// a `b` term instead — so `a00000..a0{dead}` are terms no live document holds
/// and they sort BEFORE every surviving one.
///
/// The FLUSH between the two loops is load-bearing and not tidiness: it puts
/// the dead terms in a SEALED segment, where they stay in the dictionary until
/// a compaction rewrites it. With `dead/n` under `CompactionOpts::dead_ratio`
/// (0.30) a `COMPACT` leaves that segment alone and the dead terms survive,
/// which is the whole state this fixture exists to reach. Change the ratio and
/// the fixture disarms itself silently.
fn dead_run_fixture(splits: &[&str], n: usize, dead: usize) -> Db {
    let splits_sql = if splits.is_empty() {
        String::new()
    } else {
        format!(
            " WITH (splits = [{}])",
            splits.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(",")
        )
    };
    let mut db = Db::with_opts(DbOpts::default());
    db.execute(&format!("CREATE COLLECTION items (id TEXT PRIMARY KEY){splits_sql}")).unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
    )
    .unwrap();
    let put = |db: &mut Db, i: usize, t: char| {
        db.insert(
            "items",
            Value::obj(vec![
                ("id".into(), Value::Str(format!("k{i:05}"))),
                // `zed` is in every document so the negated shape has a
                // positive clause that admits everything.
                ("body".into(), Value::Str(format!("zed {t}{i:05}"))),
            ]),
        )
        .unwrap();
    };
    for i in 0..n {
        put(&mut db, i, 'a');
    }
    db.execute("FLUSH items").unwrap();
    for i in 0..dead {
        put(&mut db, i, 'b');
    }
    db.execute("FLUSH items").unwrap();
    db
}

fn prefix_rows(db: &mut Db, q: &str) -> Vec<String> {
    let sql = format!("SELECT id FROM items WHERE text_match(body, '{q}') LIMIT 1000000");
    let mut ks = keys(&db.query(&sql).unwrap());
    ks.sort();
    ks
}

#[test]
fn a_prefix_query_names_the_live_vocabulary_not_the_physical_one() {
    // The leg the two tests above are STRUCTURALLY unable to reach, and the
    // combination matters: `a_prefix_query_finds_the_same_documents_...` makes
    // the cap bind but never updates or deletes, so its live and physical
    // dictionaries are identical; `a_prefix_query_ranks_the_same_...` updates
    // and deletes but has a 40-term vocabulary against a cap of 512, so the cap
    // cannot bind. A dead term only does damage when the cap binds, because
    // what it does is DISPLACE a live term out of it.
    //
    // Measured on the code this test was written against — 2000 documents,
    // the first 500 rewritten, FLUSH, COMPACT, 1500 still matching `a*`:
    //
    //     1 shard                                   12 rows, k00500..k00511
    //     4 shards (k00500, k01000, k01500)        512 rows, k00500..k01011
    //
    // Twelve rows out of 1500, from a query whose answer is supposed to be a
    // property of the collection. The mechanism: at one shard the first
    // segment's dead ratio is 0.25, under `CompactionOpts::dead_ratio` 0.30, so
    // it is not rewritten and the 500 dead `a` terms — which sort first — eat
    // 500 of the 512 slots. Split by key, the k00000..k00499 shard is 100% dead
    // and IS rewritten, so its terms leave the union and the cap goes to live
    // ones.
    //
    // Note what is NOT asserted, and why. Cross-shard equality alone would pass
    // on the bug: the second fixture below measured 0 rows at one shard and 0
    // rows at six on the defective code. Equal, and both wrong. So both legs
    // assert the ABSOLUTE answer — the first `PREFIX_EXPANSION_LIMIT` LIVE
    // matching terms — and anyone simplifying these back to `assert_eq!(one,
    // six)` is removing the only assertion that has teeth.
    const CAP: usize = 512;
    let expect: Vec<String> = (500..500 + CAP).map(|i| format!("k{i:05}")).collect();

    let run = |splits: &[&str]| {
        let mut db = dead_run_fixture(splits, 2000, 500);
        db.execute("COMPACT items").unwrap();
        prefix_rows(&mut db, "a*")
    };
    let one = run(&[]);
    let four = run(&["k00500", "k01000", "k01500"]);
    assert_eq!(one.len(), CAP, "the first {CAP} LIVE terms, one document each");
    assert_eq!(one, expect, "`a00500`..`a01011`, not whatever the compactor left behind");
    assert_eq!(one, four, "1 shard vs 4 shards");

    // The second axis, and the one that keeps its teeth at a FIXED shard
    // count: the same database, queried before and after a `COMPACT`. The
    // split points in the first leg happen to correlate with the dead run, so
    // that leg alone would stop meaning anything if they ever drifted apart.
    //
    // 1000 documents with the first 600 rewritten leaves 400 matching `a*` and
    // a live vocabulary of 400 terms — comfortably UNDER the cap, so nothing is
    // truncated and the honest answer is all 400 documents, before and after
    // any compaction. Measured on the code this test was written against:
    //
    //                                 before COMPACT     after COMPACT
    //       1 shard                         0 rows           400 rows
    //       6 shards                        0 rows           400 rows
    //
    // Zero. All 512 pinned terms were dead ones, the query matched nothing, and
    // it reported truncation while it did so — on a collection whose entire
    // matching vocabulary fits in the cap eight times over.
    let expect: Vec<String> = (600..1000).map(|i| format!("k{i:05}")).collect();
    let splits: Vec<&[&str]> = vec![&[], &["k00200", "k00400", "k00600", "k00800", "k00900"][..]];
    for sp in splits {
        let mut db = dead_run_fixture(sp, 1000, 600);
        let before = prefix_rows(&mut db, "a*");
        db.execute("COMPACT items").unwrap();
        let after = prefix_rows(&mut db, "a*");
        assert_eq!(before, expect, "400 live terms, {} shard(s), before COMPACT", sp.len() + 1);
        assert_eq!(after, expect, "and the same after it");
    }

    // And nothing says the answer is short, because it is not: the truncation
    // verdict is taken over live terms too, so a complete answer no longer
    // claims to be missing documents.
    let mut db = dead_run_fixture(&[], 1000, 600);
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'a*') LIMIT 1000000").unwrap();
    assert!(r.truncated_prefixes.is_empty(), "{:?}", r.truncated_prefixes);
}

#[test]
fn a_truncated_exclusion_says_rows_were_kept_not_that_rows_are_missing() {
    // Truncating an EXCLUSION set does not lose rows. It fails to remove them,
    // so the answer has extra ones — the exact opposite of what the report used
    // to say, on a shape `leaf_prefixes` supports on purpose and the test above
    // exercises. Reporting the inverse fact is worse than reporting nothing:
    // it sends the reader looking for documents that are all present.
    //
    // 1000 distinct `a` terms against a cap of 512, no deletes, so the cut is
    // the cap doing its job rather than any of the liveness machinery.
    let mut db = dead_run_fixture(&[], 1000, 0);
    let r =
        db.query("SELECT id FROM items WHERE text_match(body, 'zed -a*') LIMIT 1000000").unwrap();
    assert_eq!(r.rows.len(), 1000 - 512, "the 512 excluded terms are the cap's worth");
    assert_eq!(r.truncated_prefixes.len(), 1, "{:?}", r.truncated_prefixes);
    let m = &r.truncated_prefixes[0];
    // Rendered as it was written, sign included, or a reader cannot tell which
    // of `a*` and `-a*` was cut when a statement holds both.
    assert!(m.contains("'-a*'"), "the leaf as written: {m}");
    assert!(
        m.contains("should have excluded are still in this answer"),
        "the consequence of cutting an exclusion set: {m}"
    );
    assert!(!m.contains("documents are missing"), "which is NOT what happened: {m}");

    // The positive leaf on the same collection, for contrast: same cap, same
    // cut, opposite consequence.
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'a*') LIMIT 1000000").unwrap();
    let m = &r.truncated_prefixes[0];
    assert!(m.contains("'a*'") && m.contains("documents are missing from this answer"), "{m}");
}

#[test]
fn exact_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes() {
    // The sibling the test above needed, and the reason it needed one: that
    // test pins fusion and `k'`, which is what it is good for, but it is
    // structurally blind to BM25. Three changes had to be made before a
    // length-norm shift could reach an assertion at all, and all three are
    // load-bearing — drop any one and this test passes with the bug present:
    //
    //   * `method => 'linear'`. RRF scores by rank, so a uniform score shift
    //     is invisible unless it flips one, and it never did.
    //   * per-document length variance. `plan::fusion::fuse` min-max
    //     normalises each source over the merged candidate set, and
    //     `Corpus::doc` draws bodies from five templates — with a handful of
    //     distinct lengths, normalisation maps the shifted scores straight
    //     back onto the same bits.
    //   * updates, deletes and a small `max_bytes`. With no dead row there is
    //     nothing for a physical-row sum to over-count, and with every shard
    //     holding everything until the final `FLUSH` the over-count would be
    //     the same at every shard count anyway.
    //   * where the splits fall. This is the fourth ingredient and the most
    //     fragile one: with the fix reverted and the three-way split taken on
    //     tenant boundaries alone (`["t1", "t2"]`), the 1-vs-3 leg PASSED and
    //     only the 1-vs-6 leg fired — min-max normalisation in
    //     `plan::fusion::fuse` absorbed the shift for that particular split.
    //     Cutting the second boundary through the middle of a tenant instead
    //     gives the two shard sets genuinely different seal schedules over the
    //     same keys, and both legs then discriminate. Verified by reverting
    //     `src/shard.rs`'s `visible_doc_len` call and watching the 1-vs-3
    //     assertion fail on the last five rows.
    let dims = 16;
    let n = 900;

    let run = |splits: &[&str]| {
        let mut o = opts(64);
        o.thresholds.max_bytes = 96 << 10;
        let mut db = Db::with_opts(o);
        setup(&mut db, dims, splits);
        let mut c = Corpus::new(dims, 42);
        let varied = |c: &mut Corpus, i: usize| {
            let mut d = c.doc(i);
            let base = d.path("body").unwrap().as_str().unwrap().to_string();
            let filler = "padding ".repeat(1 + i % 9);
            d.set_path("body", Value::Str(format!("{base} {filler}")));
            d
        };
        for i in 0..n {
            let d = varied(&mut c, i);
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(7) {
            let mut d = varied(&mut c, i);
            d.set_path("body", Value::Str(format!("rewritten fusion item {i}")));
            db.insert("items", d).unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete_key("items", &format!("t{}\u{1}doc-{i:05}", i % 3)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        let q = vec_literal(&c.query_near(2, 0.03));
        // Same `k` guard as the test above, for the same reason.
        let sql = format!(
            "SELECT id FROM items \
             ORDER BY hybrid(text_match(body, 'fusion candidates traversal'), embedding <=> {q}, \
                             method => 'linear', k => 100000) \
             LIMIT 20 WITH (exact, exact_scoring)"
        );
        let r = db.query(&sql).unwrap();
        r.rows.iter().map(|x| (x.key.clone(), x.score.unwrap().to_bits())).collect::<Vec<_>>()
    };

    let one = run(&[]);
    let three = run(&["t1", "t2\u{1}doc-00450"]);
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
