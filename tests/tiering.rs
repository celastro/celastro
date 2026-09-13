//! Storage tiers, memory residency, and lifecycle policies.
//!
//! The design's claim is that a tier is a *declaration of intent* with a
//! mechanical consequence — where the bytes sit and how long a decoded copy is
//! kept — and never a correctness boundary. These tests hold that line: after
//! any amount of unloading, faulting in, demoting and archiving, the same
//! query returns the same rows.

use std::collections::BTreeMap;

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::lifecycle::{Every, IndexActivity, LifecyclePolicy, Rule, Trigger, Unit};
use celastro::residency::{ArchivedAccess, Placement, Tier};
use celastro::value::Value;

const MIN: u64 = 60_000_000;
const HOUR: u64 = 3_600_000_000;
const DAY: u64 = 86_400_000_000;

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-tier-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn doc(i: usize) -> Value {
    let emb: Vec<Value> =
        (0..8).map(|d| Value::Float(((i % 17) as f64) * 0.1 + d as f64 * 0.01)).collect();
    Value::obj(vec![
        ("id".into(), Value::Str(format!("d-{i:04}"))),
        ("kind".into(), Value::Str(if i % 3 == 0 { "note".into() } else { "page".into() })),
        (
            "body".into(),
            Value::Str(format!("segments and postings and vectors, document number {i}")),
        ),
        ("embedding".into(), Value::Array(emb)),
    ])
}

/// A collection whose three indexes each sit on a different tier.
fn setup(db: &mut Db, n: usize) {
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, kind TEXT)").unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) \
         WITH (analyzer = 'english', tier = 'hot')",
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_emb ON items USING vector (embedding) \
         WITH (dims = 8, metric = 'cosine', tier = 'cold')",
    )
    .unwrap();
    db.execute("CREATE INDEX items_kind ON items USING secondary (kind) WITH (tier = 'hot')")
        .unwrap();
    for i in 0..n {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
}

/// Like [`setup`], but the text index is declared `minimal`.
fn minimal_setup(db: &mut Db, n: usize) {
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, kind TEXT)").unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) \
         WITH (analyzer = 'english', tier = 'minimal')",
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_emb ON items USING vector (embedding) \
         WITH (dims = 8, metric = 'cosine', tier = 'cached')",
    )
    .unwrap();
    db.execute("CREATE INDEX items_kind ON items USING secondary (kind) WITH (tier = 'cached')")
        .unwrap();
    for i in 0..n {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
}

fn ack(db: &mut Db, sql: &str) -> String {
    match db.execute(sql).unwrap() {
        Outcome::Ack(s) => s,
        other => panic!("expected an ack from `{sql}`, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// The declaration
// --------------------------------------------------------------------------

#[test]
fn a_tier_is_part_of_the_index_definition_and_survives_a_reopen() {
    let d = dir("decl");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 40);
        let c = db.catalog.get("items").unwrap();
        assert_eq!(c.index_by_name("items_body").unwrap().tier, Tier::Active);
        assert_eq!(c.index_by_name("items_emb").unwrap().tier, Tier::Cached);
        db.persist().unwrap();
    }
    let db = Db::open(&d, DbOpts::default()).unwrap();
    let c = db.catalog.get("items").unwrap();
    assert_eq!(
        c.index_by_name("items_emb").unwrap().tier,
        Tier::Cached,
        "the declared tier is catalog state, not a session setting"
    );
    assert_eq!(c.index_by_name("items_emb").unwrap().declared_tier, Tier::Cached);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn every_spelling_of_a_tier_parses_and_an_unknown_one_is_refused() {
    for (s, want) in [
        ("active", Tier::Active),
        ("ACTIVE", Tier::Active),
        ("hot", Tier::Active),
        ("ram", Tier::Active),
        ("memory", Tier::Active),
        ("resident", Tier::Active),
        ("minimal", Tier::Minimal),
        ("warm", Tier::Minimal),
        ("pinned", Tier::Minimal),
        ("single", Tier::Minimal),
        ("one_copy", Tier::Minimal),
        ("cached", Tier::Cached),
        ("cold", Tier::Cached),
        ("disk", Tier::Cached),
        ("nvme", Tier::Cached),
        ("archived", Tier::Archived),
        ("s3", Tier::Archived),
        ("object_store", Tier::Archived),
    ] {
        assert_eq!(Tier::parse(s).unwrap(), want, "spelling `{s}`");
    }
    let e = Tier::parse("lukewarm").unwrap_err().to_string();
    assert!(
        e.contains("lukewarm") && e.contains("minimal"),
        "the error should list the tiers: {e}"
    );
}

/// The ladder runs most-ready to least, which is what "demote" means and what
/// the budget sheds in reverse.
#[test]
fn the_ladder_is_ordered_from_most_ready_to_least() {
    assert!(Tier::Active < Tier::Minimal);
    assert!(Tier::Minimal < Tier::Cached);
    assert!(Tier::Cached < Tier::Archived);
    assert!(Tier::Minimal.is_colder_than(Tier::Active));
    assert!(!Tier::Active.is_colder_than(Tier::Minimal));
    // And the byte encoding matches, so a persisted catalog compares the same
    // way it did in memory.
    for t in [Tier::Active, Tier::Minimal, Tier::Cached, Tier::Archived] {
        assert_eq!(Tier::from_u8(t.as_u8()), t);
        assert_eq!(Tier::parse(t.name()).unwrap(), t);
    }
    assert_eq!(Tier::default(), Tier::Active, "an index with no tier is fully resident");
}

#[test]
fn alter_index_set_tier_moves_the_declaration_too() {
    let mut db = Db::in_memory();
    setup(&mut db, 20);
    let msg = ack(&mut db, "ALTER INDEX items_emb ON items SET TIER 'active'");
    assert!(msg.contains("cached -> active"), "{msg}");
    let c = db.catalog.get("items").unwrap();
    let i = c.index_by_name("items_emb").unwrap();
    assert_eq!(i.tier, Tier::Active);
    assert_eq!(
        i.declared_tier,
        Tier::Active,
        "an operator setting a tier is restating the baseline, so an access cannot undo it"
    );
}

// --------------------------------------------------------------------------
// Residency
// --------------------------------------------------------------------------

#[test]
fn a_reopened_shard_decodes_nothing_until_it_is_queried() {
    let d = dir("lazy");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 300);
        db.persist().unwrap();
    }
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(
        db.residency().resident_bytes(),
        0,
        "opening a shard reads segment footers, not segment bodies"
    );
    let r = db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 5").unwrap();
    assert_eq!(r.rows.len(), 5);
    assert!(
        db.residency().resident_bytes() > 0,
        "the query had to decode the term dictionary and postings"
    );
    assert!(db.residency().loads() > 0);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn unloading_an_idle_index_frees_memory_and_the_next_query_is_unchanged() {
    let d = dir("idle");
    let mut o = DbOpts::default();
    // Everything is idle the instant it stops being used.
    o.residency.active_idle_unload = Some(std::time::Duration::from_secs(0));
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    let mut db = Db::open(&d, o).unwrap();
    setup(&mut db, 400);

    let sql = "SELECT * FROM items WHERE text_match(body, 'postings') AND kind = 'note' LIMIT 8";
    let before: Vec<String> = db.query(sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    let peak = db.residency().resident_bytes();
    assert!(peak > 0, "a query decodes something");

    let msg = ack(&mut db, "UNLOAD IDLE ON items");
    assert!(msg.contains("released"), "{msg}");
    assert_eq!(
        db.residency().resident_bytes(),
        0,
        "with a zero idle window every component is releasable: {msg}"
    );

    let after: Vec<String> = db.query(sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(before, after, "unloading is a memory decision, never a correctness one");
    assert!(db.residency().unloads() > 0 && db.residency().loads() > 1);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn a_cold_text_index_still_reports_the_right_visible_length() {
    // The exact-statistics gather reaches the text index through
    // `text_handle`, so an unloaded component has to be faulted back in rather
    // than read as "no text on this path" — which would count the segment's
    // documents towards `num_docs` with no length and no postings, deflating
    // avgdl and inflating IDF for the whole collection. The numbers must not
    // depend on residency, and the fault-in has to actually happen.
    let d = dir("cold-stats");
    let mut o = DbOpts::default();
    o.residency.active_idle_unload = Some(std::time::Duration::from_secs(0));
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    let mut db = Db::open(&d, o).unwrap();
    setup(&mut db, 400);
    // Deleted rows make the visible length differ from the physical one, which
    // is the quantity residency could plausibly disturb.
    for i in (0..400).step_by(5) {
        db.delete_key("items", &format!("d-{i:04}")).unwrap();
    }

    let want = BTreeMap::from([("body".to_string(), vec!["postings".to_string()])]);
    let ts = db.clock.peek();
    let hot = db.gather_stats("items", &want, ts, true).unwrap();
    assert_eq!(hot["body"].num_docs, 320, "400 written, one in five deleted");
    assert!(components(&db).iter().any(|c| c.starts_with("text:")), "the gather decoded it");

    let msg = ack(&mut db, "UNLOAD IDLE ON items");
    assert!(!components(&db).iter().any(|c| c.starts_with("text:")), "{msg}");
    let loads = db.residency().loads();

    let cold = db.gather_stats("items", &want, ts, true).unwrap();
    assert!(db.residency().loads() > loads, "the gather faults the text index back in");
    assert_eq!(cold["body"].num_docs, hot["body"].num_docs);
    assert_eq!(
        cold["body"].avg_doc_len.to_bits(),
        hot["body"].avg_doc_len.to_bits(),
        "a statistic is not a residency decision"
    );
    assert_eq!(cold["body"].doc_freq, hot["body"].doc_freq);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn a_hot_index_with_no_idle_window_stays_resident_but_a_cold_one_does_not() {
    let d = dir("window");
    let mut o = DbOpts::default();
    o.residency.active_idle_unload = None; // the default: hot means resident
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    let mut db = Db::open(&d, o).unwrap();
    setup(&mut db, 300);

    // Touch the hot text index and the cold vector index.
    db.query("SELECT * FROM items WHERE text_match(body, 'segments') LIMIT 3").unwrap();
    db.query(
        "SELECT * FROM items ORDER BY embedding <=> [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] LIMIT 3",
    )
    .unwrap();

    let loaded: Vec<String> = components(&db);
    assert!(loaded.iter().any(|c| c.starts_with("text:")), "text index is loaded: {loaded:?}");
    assert!(loaded.iter().any(|c| c.starts_with("vec:")), "vector index is loaded: {loaded:?}");

    db.execute("UNLOAD IDLE").unwrap();
    let after = components(&db);
    assert!(
        after.iter().any(|c| c.starts_with("text:")),
        "a hot index with no idle window is kept: {after:?}"
    );
    assert!(
        !after.iter().any(|c| c.starts_with("vec:")),
        "the cold index is exactly what an idle sweep is for: {after:?}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

fn components(db: &Db) -> Vec<String> {
    db.residency().snapshot().into_iter().filter(|c| c.loaded).map(|c| c.component).collect()
}

#[test]
fn a_node_over_its_budget_releases_the_colder_index_first() {
    let d = dir("budget");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 400);

    let text_sql = "SELECT * FROM items WHERE text_match(body, 'vectors') LIMIT 6";
    let vec_sql =
        "SELECT * FROM items ORDER BY embedding <=> [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] LIMIT 6";
    let text_before: Vec<String> =
        db.query(text_sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    let vec_before: Vec<String> =
        db.query(vec_sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();

    // Squeeze the node by exactly the size of the cold vector index. The
    // eviction is then a genuine choice, not a purge.
    let resident = db.residency().resident_bytes();
    let cold: usize = db
        .residency()
        .snapshot()
        .iter()
        .filter(|c| c.loaded && c.tier == Tier::Cached)
        .map(|c| c.bytes)
        .sum();
    assert!(cold > 0 && cold < resident, "cold {cold} of {resident} resident");
    let mut o = db.residency().opts();
    o.budget_bytes = resident - cold;
    db.residency().set_opts(o);

    let freed = db.sweep_residency();
    assert!(freed >= cold, "freed {freed}, needed {cold}");
    let left = components(&db);
    assert!(
        !left.iter().any(|c| c.starts_with("vec:")),
        "the cold index is what a squeezed node gives up: {left:?}"
    );
    assert!(
        left.iter().any(|c| c.starts_with("text:")),
        "the hot index is what it keeps: {left:?}"
    );

    // And the answers are the answers, whatever is resident.
    let text_after: Vec<String> =
        db.query(text_sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    let vec_after: Vec<String> =
        db.query(vec_sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(text_before, text_after, "eviction under pressure must not change an answer");
    assert_eq!(vec_before, vec_after);
    let _ = std::fs::remove_dir_all(&d);
}

// --------------------------------------------------------------------------
// minimal: decoded on exactly one node
// --------------------------------------------------------------------------

/// The whole promise: one node keeps it, whatever the replica count says.
#[test]
fn exactly_one_replica_holds_a_minimal_index() {
    let replicas: Vec<String> = (0..5).map(|i| format!("node-{i}")).collect();
    for key in ["items/text:body", "items/vec:embedding", "orders/col:status"] {
        let holders: Vec<String> = replicas
            .iter()
            .filter(|n| Placement::new((*n).clone(), replicas.clone()).holds(key))
            .cloned()
            .collect();
        assert_eq!(holders.len(), 1, "`{key}` is held by {holders:?}, not by exactly one node");
    }
}

/// And it is one *regardless of the replication factor* — the point of the tier.
#[test]
fn the_holder_count_does_not_grow_with_the_replica_count() {
    for n in [1usize, 2, 3, 5, 9, 30] {
        let replicas: Vec<String> = (0..n).map(|i| format!("node-{i:02}")).collect();
        let holders = replicas
            .iter()
            .filter(|x| Placement::new((*x).clone(), replicas.clone()).holds("items/text:body"))
            .count();
        assert_eq!(holders, 1, "{n} replicas produced {holders} holders");
    }
}

/// Every node computes the same answer without being told, and the answer does
/// not wander.
#[test]
fn the_designation_is_agreed_without_coordination_and_is_stable() {
    let replicas: Vec<String> = vec!["b".into(), "a".into(), "c".into()];
    let shuffled: Vec<String> = vec!["c".into(), "b".into(), "a".into()];
    let from =
        |r: &Vec<String>, me: &str| Placement::new(me, r.clone()).holder_for("items/text:body");
    let want = from(&replicas, "a");
    assert!(want.is_some());
    for me in ["a", "b", "c"] {
        assert_eq!(from(&replicas, me), want, "node `{me}` disagrees about the holder");
        assert_eq!(
            from(&shuffled, me),
            want,
            "the answer must not depend on the order the tablet map listed the replicas in"
        );
    }
    // Stable across repeated evaluation: an index does not migrate its holder
    // because a query ran.
    for _ in 0..5 {
        assert_eq!(from(&replicas, "a"), want);
    }
    // Duplicates in the list do not skew it either.
    let dupes = vec!["a".into(), "a".into(), "b".into(), "c".into(), "c".into()];
    assert_eq!(from(&dupes, "a"), want);
}

/// On the designated node, `minimal` behaves like `active`.
#[test]
fn the_holder_keeps_a_minimal_index_resident() {
    let d = dir("min-hold");
    let mut o = DbOpts::default();
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    o.placement = Placement::single("only");
    let mut db = Db::open(&d, o).unwrap();
    minimal_setup(&mut db, 300);

    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3").unwrap();
    db.query(
        "SELECT * FROM items ORDER BY embedding <=> [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] LIMIT 3",
    )
    .unwrap();
    db.execute("UNLOAD IDLE").unwrap();

    let left = components(&db);
    assert!(
        left.iter().any(|c| c.starts_with("text:")),
        "a single-node deployment holds every minimal index: {left:?}"
    );
    assert!(!left.iter().any(|c| c.starts_with("vec:")), "and the cached one still goes: {left:?}");
    let _ = std::fs::remove_dir_all(&d);
}

/// On every other node, `minimal` resolves to `cached` — still answers, pays a
/// segment read.
#[test]
fn a_node_that_is_not_the_holder_treats_minimal_as_cached() {
    let replicas: Vec<String> = vec!["n0".into(), "n1".into(), "n2".into()];
    let holder = Placement::new("n0", replicas.clone()).holder_for("items/text:body").unwrap();
    let bystander = replicas.iter().find(|n| **n != holder).unwrap().clone();

    let d = dir("min-bystander");
    let mut o = DbOpts::default();
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    o.placement = Placement::new(bystander.clone(), replicas);
    let mut db = Db::open(&d, o).unwrap();
    minimal_setup(&mut db, 300);

    let sql = "SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 4";
    let before: Vec<String> = db.query(sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(before.len(), 4);

    // The ledger records what this node actually does with it, which is cache
    // it — the declaration is still `minimal` in the catalog.
    let text = db
        .residency()
        .snapshot()
        .into_iter()
        .find(|c| c.component == "text:body" && c.loaded)
        .expect("the text index was decoded");
    assert_eq!(text.tier, Tier::Cached, "a node that is not the holder resolves minimal to cached");
    assert_eq!(
        tier(&db, "items_body"),
        Tier::Minimal,
        "but the declaration is unchanged; residency is a fact about a node, not about the index"
    );

    db.execute("UNLOAD IDLE").unwrap();
    assert!(
        !components(&db).iter().any(|c| c.starts_with("text:")),
        "so it is released on the cached schedule"
    );
    let after: Vec<String> = db.query(sql).unwrap().rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(before, after, "a node that is not the holder answers, it just pays");
    let _ = std::fs::remove_dir_all(&d);
}

/// Eviction order runs the full ladder.
#[test]
fn eviction_walks_the_ladder_from_the_bottom() {
    let mut db = Db::in_memory();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, kind TEXT)").unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) \
         WITH (analyzer='english', tier='active')",
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_emb ON items USING vector (embedding) \
         WITH (dims=8, metric='cosine', tier='minimal')",
    )
    .unwrap();
    db.execute("CREATE INDEX items_kind ON items USING secondary (kind) WITH (tier='cached')")
        .unwrap();
    for i in 0..300 {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
    db.query("SELECT * FROM items WHERE text_match(body, 'postings') AND kind = 'note' LIMIT 3")
        .unwrap();
    db.query(
        "SELECT * FROM items ORDER BY embedding <=> [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] LIMIT 3",
    )
    .unwrap();

    let plan = db.residency().plan_evictions(usize::MAX);
    let order: Vec<String> = plan.iter().map(|(_, c)| c.clone()).collect();
    let pos = |prefix: &str| order.iter().position(|c| c.starts_with(prefix));
    let (col, vec_, text) = (pos("col:"), pos("vec:"), pos("text:"));
    assert!(col.is_some() && vec_.is_some() && text.is_some(), "{order:?}");
    assert!(col < vec_, "cached goes before minimal: {order:?}");
    assert!(vec_ < text, "minimal goes before active: {order:?}");
}

/// A lifecycle policy can step an index down the whole ladder.
#[test]
fn a_policy_can_walk_an_index_down_the_whole_ladder() {
    let mut db = Db::in_memory();
    setup(&mut db, 60);
    db.execute(
        "CREATE LIFECYCLE POLICY ladder ON items FOR (items_body) \
           MOVE TO minimal  AFTER 30 minutes OF INACTIVITY, \
           MOVE TO cached   AFTER 6 hours OF INACTIVITY, \
           MOVE TO archived AFTER 30 days OF INACTIVITY",
    )
    .unwrap();
    for (idle, want) in
        [(45 * MIN, Tier::Minimal), (8 * HOUR, Tier::Cached), (40 * DAY, Tier::Archived)]
    {
        age("items", "items_body", &mut db, idle, 0);
        let m = moves(&mut db, None);
        assert_eq!(m.len(), 1, "idle {idle}: {m:?}");
        assert_eq!(m[0].to, want, "idle {idle}");
        assert_eq!(tier(&db, "items_body"), want);
    }
    // And use brings it back to the declaration in one step, not one rung.
    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").unwrap();
    assert_eq!(tier(&db, "items_body"), Tier::Active);
}

/// A collection with a `minimal` index is not archived away underneath it.
#[test]
fn a_minimal_index_keeps_the_segment_file_local() {
    let d = dir("min-file");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    minimal_setup(&mut db, 150);
    let segs = d.join("collections/items/shard-0000/segments");
    let arch = d.join("collections/items/shard-0000/archive");

    for i in ["items_emb", "items_kind"] {
        db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")).unwrap();
    }
    assert!(
        count(&segs) > 0 && count(&arch) == 0,
        "one index that is not archived keeps the bytes local"
    );
    db.execute("ALTER INDEX items_body ON items SET TIER 'archived'").unwrap();
    assert!(count(&arch) > 0, "and once nothing wants them locally, they move");
    let _ = std::fs::remove_dir_all(&d);
}

/// A catalog from a build with a different ladder must be refused, not
/// reinterpreted: its tier bytes would shift every index one rung.
#[test]
fn a_catalog_from_an_older_format_version_is_refused() {
    let d = dir("v1");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 20);
        db.persist().unwrap();
    }
    let p = d.join("CATALOG");
    let mut b = std::fs::read(&p).unwrap();
    assert_eq!(b[4], 2, "this build writes version 2");
    b[4] = 1;
    // Re-frame so the checksum is valid: the version check must be what
    // refuses it, not the checksum.
    let body = &b[5..b.len() - 4];
    let mut v1 = b[..5].to_vec();
    v1.extend_from_slice(body);
    v1.extend_from_slice(&celastro::codec::crc32(body).to_le_bytes());
    std::fs::write(&p, &v1).unwrap();

    let e = match Db::open(&d, DbOpts::default()) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a v1 catalog was accepted; its tier bytes mean something else here"),
    };
    assert!(e.contains("version 1") && e.contains("ladder"), "{e}");
    let _ = std::fs::remove_dir_all(&d);
}

/// A placement whose guarantee cannot hold must be refused, not silently
/// obeyed.
#[test]
fn a_node_missing_from_its_own_replica_list_is_refused() {
    let d = dir("misplaced");
    let mut o = DbOpts::default();
    // The likeliest mistake: set the replica list, leave node_id at default.
    o.placement = Placement::new("node-0", vec!["a".into(), "b".into(), "c".into()]);
    let e = match Db::open(&d, o) {
        Err(e) => e.to_string(),
        Ok(_) => panic!(
            "accepted a placement in which no node holds anything, so `minimal` guarantees zero \
             copies rather than one"
        ),
    };
    assert!(e.contains("node-0") && e.contains("does not include it"), "{e}");

    // An empty list is the single-node case and is fine.
    let mut o = DbOpts::default();
    o.placement = Placement::new("whoever", Vec::new());
    assert!(Db::open(&d, o).is_ok());
    let _ = std::fs::remove_dir_all(&d);
}

/// The catalog holds what was declared; only segments hold what this node
/// resolved it to.
#[test]
fn resolution_never_leaks_back_into_the_catalog() {
    let replicas: Vec<String> = vec!["n0".into(), "n1".into(), "n2".into()];
    let holder = Placement::new("n0", replicas.clone()).holder_for("items/text:body").unwrap();
    let bystander = replicas.iter().find(|n| **n != holder).unwrap().clone();

    let d = dir("no-leak");
    let mut o = DbOpts::default();
    o.placement = Placement::new(bystander, replicas);
    let mut db = Db::open(&d, o).unwrap();
    minimal_setup(&mut db, 60);

    // Anything that pushes tiers down to the segments — a query, a tier change
    // on a different index, a lifecycle run — must leave the declaration alone.
    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").unwrap();
    db.execute("ALTER INDEX items_kind ON items SET TIER 'archived'").unwrap();
    db.execute("CREATE LIFECYCLE POLICY p ON items FOR (items_emb) MOVE TO archived AFTER 1 day")
        .unwrap();
    age("items", "items_emb", &mut db, 2 * DAY, 0);
    moves(&mut db, None);
    db.persist().unwrap();

    assert_eq!(
        tier(&db, "items_body"),
        Tier::Minimal,
        "a node that merely caches a minimal index must not write that back as the declaration; \
         once the catalog is cluster state that would demote the index for the holder too"
    );
    let re = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(
        re.catalog.get("items").unwrap().index_by_name("items_body").unwrap().tier,
        Tier::Minimal
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A promotion moves files, so it needs the same rollback a demotion has.
#[test]
fn a_promotion_that_cannot_move_its_files_changes_nothing() {
    let d = dir("promo-rollback");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 60);
    db.execute(
        "CREATE LIFECYCLE POLICY cool ON items MOVE TO archived AFTER 5 minutes OF INACTIVITY",
    )
    .unwrap();
    for i in ["items_body", "items_emb", "items_kind"] {
        age("items", i, &mut db, HOUR, 0);
    }
    assert_eq!(moves(&mut db, None).len(), 3);
    let segs = d.join("collections/items/shard-0000/segments");
    assert_eq!(count(&segs), 0, "everything archived, so the files moved");

    // Block the way back.
    std::fs::remove_dir_all(&segs).unwrap();
    std::fs::write(&segs, b"not a directory").unwrap();

    let r = db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2");
    assert!(r.is_err(), "the query promotes, and the promotion cannot land");
    assert_eq!(
        tier(&db, "items_body"),
        Tier::Archived,
        "a tier the files could not follow must not be left in the catalog"
    );
    // Unblock the way back before persisting, so the reopen is exercising the
    // catalog rather than the sabotage.
    std::fs::remove_file(&segs).unwrap();
    std::fs::create_dir_all(&segs).unwrap();
    db.persist().unwrap();
    let re = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(
        re.catalog.get("items").unwrap().index_by_name("items_body").unwrap().tier,
        Tier::Archived,
        "and must not be persisted"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// An operator must be able to see a tier without querying the index first.
#[test]
fn show_catalog_reports_the_tier_of_every_index() {
    let mut db = Db::in_memory();
    minimal_setup(&mut db, 20);
    let out = ack(&mut db, "SHOW CATALOG items");
    assert!(out.contains("tier=minimal"), "{out}");
    assert!(out.contains("tier=cached"), "{out}");

    db.execute("CREATE LIFECYCLE POLICY p ON items FOR (items_body) MOVE TO archived AFTER 1 day")
        .unwrap();
    age("items", "items_body", &mut db, 2 * DAY, 0);
    moves(&mut db, None);
    let out = ack(&mut db, "SHOW CATALOG items");
    assert!(
        out.contains("tier=archived (declared minimal)"),
        "a demoted index should show both what it is and what it was asked to be: {out}"
    );
}

/// An unknown tier byte is a catalog this build should not interpret.
#[test]
fn an_unknown_tier_byte_is_refused_rather_than_read_as_archived() {
    let mut db = Db::in_memory();
    minimal_setup(&mut db, 10);
    let good = db.catalog.encode();
    let reframe = |body: &[u8]| {
        let mut out = good[..5].to_vec();
        out.extend_from_slice(body);
        out.extend_from_slice(&celastro::codec::crc32(body).to_le_bytes());
        out
    };
    let body = &good[5..good.len() - 4];
    assert!(celastro::catalog::Catalog::decode(&reframe(body)).is_ok());

    // The tier bytes are 0..=3; find one and push it out of range.
    let mut hits = 0;
    for i in 0..body.len() {
        if body[i] > 3 {
            continue;
        }
        let mut b = body.to_vec();
        b[i] = 9;
        if let Err(e) = celastro::catalog::Catalog::decode(&reframe(&b)) {
            if e.to_string().contains("unknown tier byte") {
                hits += 1;
            }
        }
    }
    assert!(hits > 0, "no byte position exercised the tier bound");
}

// --------------------------------------------------------------------------
// Archived
// --------------------------------------------------------------------------

#[test]
fn archiving_a_collection_relocates_its_segment_files() {
    let d = dir("archive");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 200);

    let segs = d.join("collections/items/shard-0000/segments");
    let arch = d.join("collections/items/shard-0000/archive");
    assert!(count(&segs) > 0 && count(&arch) == 0);

    for i in ["items_body", "items_emb", "items_kind"] {
        db.execute(&format!("ALTER INDEX {i} ON items SET TIER 's3'")).unwrap();
    }
    assert_eq!(count(&segs), 0, "nothing local still wants a copy");
    assert!(count(&arch) > 0, "the file moved to the archive");

    // Still answerable — the archive is a location, not a tombstone.
    let r = db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 4").unwrap();
    assert_eq!(r.rows.len(), 4);
    assert!(db.residency().faults() > 0, "reading an archived segment is a fault-in");

    db.execute("ALTER INDEX items_body ON items SET TIER 'hot'").unwrap();
    assert!(count(&segs) > 0 && count(&arch) == 0, "one hot index brings the file back");
    let _ = std::fs::remove_dir_all(&d);
}

fn count(p: &std::path::Path) -> usize {
    std::fs::read_dir(p).map(|d| d.filter_map(|e| e.ok()).count()).unwrap_or(0)
}

#[test]
fn refusing_archived_access_fails_loudly_rather_than_stalling() {
    let d = dir("refuse");
    let mut o = DbOpts::default();
    o.residency.archived_access = ArchivedAccess::Refuse;
    let mut db = Db::open(&d, o).unwrap();
    setup(&mut db, 120);
    for i in ["items_body", "items_emb", "items_kind"] {
        db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")).unwrap();
    }
    let e = db
        .query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3")
        .unwrap_err()
        .to_string();
    assert!(e.contains("text:body"), "the error should name the archived component: {e}");
    assert!(
        e.contains("ALTER INDEX") && e.contains("fault_in"),
        "and should say what to do about it: {e}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn an_archived_segment_reopens_from_the_archive() {
    let d = dir("reopen-arch");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 150);
        for i in ["items_body", "items_emb", "items_kind"] {
            db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")).unwrap();
        }
        db.persist().unwrap();
    }
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    let r = db.query("SELECT * FROM items WHERE text_match(body, 'segments') LIMIT 5").unwrap();
    assert_eq!(r.rows.len(), 5, "a reopened archive is still readable");
    let _ = std::fs::remove_dir_all(&d);
}

// --------------------------------------------------------------------------
// Lifecycle policies
// --------------------------------------------------------------------------

#[test]
fn a_policy_demotes_an_index_that_has_gone_unused() {
    let mut db = Db::in_memory();
    setup(&mut db, 100);
    db.execute(
        "CREATE LIFECYCLE POLICY cool_down ON items FOR (items_body) \
           MOVE TO cold AFTER 30 minutes OF INACTIVITY, \
           MOVE TO archived AFTER 7 days OF INACTIVITY",
    )
    .unwrap();

    // Nothing is overdue yet.
    assert!(moves(&mut db, None).is_empty());

    // Put the last access an hour into the past.
    age("items", "items_body", &mut db, HOUR, 0);
    let m = moves(&mut db, None);
    assert_eq!(m.len(), 1, "{m:?}");
    assert_eq!(m[0].to, Tier::Cached);
    assert!(m[0].reason.contains("idle"), "{}", m[0].reason);

    // Eight days idle skips straight to the furthest matching rule, so the
    // outcome does not depend on how often the runner happens to fire.
    age("items", "items_body", &mut db, 8 * DAY, 0);
    let m = moves(&mut db, None);
    assert_eq!(m.len(), 1, "{m:?}");
    assert_eq!(m[0].to, Tier::Archived);
    assert_eq!(tier(&db, "items_body"), Tier::Archived);

    // A second run is a no-op: the index is already where the policy wants it.
    assert!(moves(&mut db, None).is_empty(), "a policy is not a repeating alarm");
}

#[test]
fn a_policy_can_key_on_creation_age_instead_of_use() {
    let mut db = Db::in_memory();
    setup(&mut db, 60);
    db.execute(
        "CREATE LIFECYCLE POLICY retention ON items \
           MOVE TO archived AFTER 90 days SINCE CREATION",
    )
    .unwrap();
    // Constantly used, but old.
    age("items", "items_body", &mut db, 0, 100 * DAY);
    let moves = moves(&mut db, Some("items"));
    assert_eq!(moves.len(), 1, "only the aged index moves: {moves:?}");
    assert_eq!(moves[0].index, "items_body");
    assert!(moves[0].reason.contains("age"), "{}", moves[0].reason);
}

#[test]
fn a_policy_with_no_index_list_covers_every_index_including_later_ones() {
    let mut db = Db::in_memory();
    setup(&mut db, 40);
    db.execute("CREATE LIFECYCLE POLICY all_cold ON items MOVE TO cold AFTER 10 minutes").unwrap();
    db.execute(
        "CREATE INDEX items_extra ON items USING fulltext (body) WITH (analyzer = 'standard')",
    )
    .unwrap();
    for i in ["items_body", "items_kind", "items_extra"] {
        age("items", i, &mut db, HOUR, 0);
    }
    let moved: Vec<String> = moves(&mut db, None).into_iter().map(|t| t.index).collect();
    assert!(
        moved.contains(&"items_extra".to_string()),
        "an index created after the policy is covered by it: {moved:?}"
    );
    assert!(
        !moved.contains(&"items_emb".to_string()),
        "items_emb is already cold; a policy never re-reports a move it has made"
    );
}

#[test]
fn an_access_promotes_a_demoted_index_back_to_its_declared_tier() {
    let mut db = Db::in_memory();
    setup(&mut db, 80);
    db.execute(
        "CREATE LIFECYCLE POLICY cool ON items FOR (items_body) MOVE TO cold AFTER 5 minutes",
    )
    .unwrap();
    age("items", "items_body", &mut db, HOUR, 0);
    assert_eq!(moves(&mut db, None).len(), 1);
    assert_eq!(tier(&db, "items_body"), Tier::Cached);

    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").unwrap();
    assert_eq!(
        tier(&db, "items_body"),
        Tier::Active,
        "use restores an index to what its definition asked for"
    );
    assert_eq!(
        tier(&db, "items_emb"),
        Tier::Cached,
        "an index declared cold is never promoted past its declaration by traffic"
    );
}

#[test]
fn a_policy_naming_an_index_that_does_not_exist_is_refused_at_creation() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    let e = db
        .execute(
            "CREATE LIFECYCLE POLICY typo ON items FOR (items_embedding) MOVE TO cold AFTER 1 day",
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("items_embedding"), "{e}");
    let e = db
        .execute("CREATE LIFECYCLE POLICY nope ON nosuch MOVE TO cold AFTER 1 day")
        .unwrap_err()
        .to_string();
    assert!(e.contains("nosuch"), "{e}");
}

#[test]
fn durations_are_accepted_in_minutes_hours_and_days() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    for (sql, want) in [
        ("MOVE TO cold AFTER 90 minutes", 90 * MIN),
        ("MOVE TO cold AFTER 1 minute", MIN),
        ("MOVE TO cold AFTER 6 hours", 6 * HOUR),
        ("MOVE TO cold AFTER 1 hr", HOUR),
        ("MOVE TO cold AFTER 30 days", 30 * DAY),
        ("MOVE TO cold AFTER 1 day", DAY),
    ] {
        db.execute(&format!("CREATE LIFECYCLE POLICY p ON items {sql}")).unwrap();
        assert_eq!(db.catalog.policies["p"].rules[0].after.micros(), want, "{sql}");
        db.execute("DROP LIFECYCLE POLICY p").unwrap();
    }
    let e = db
        .execute("CREATE LIFECYCLE POLICY p ON items MOVE TO cold AFTER 3 fortnights")
        .unwrap_err()
        .to_string();
    assert!(e.contains("fortnight"), "{e}");
    let e = db
        .execute("CREATE LIFECYCLE POLICY p ON items MOVE TO cold AFTER 0 days")
        .unwrap_err()
        .to_string();
    assert!(e.contains("positive"), "{e}");
}

#[test]
fn policies_and_activity_survive_a_reopen() {
    let d = dir("policy-reopen");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 50);
        db.execute(
            "CREATE LIFECYCLE POLICY archive_old ON items FOR (items_emb, items_body) \
               MOVE TO cold AFTER 2 hours OF INACTIVITY, \
               MOVE TO archived AFTER 45 days SINCE CREATION",
        )
        .unwrap();
        age("items", "items_body", &mut db, 3 * HOUR, 0);
        db.persist().unwrap();
    }
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    let p = &db.catalog.policies["archive_old"];
    assert_eq!(p.indexes, vec!["items_emb".to_string(), "items_body".to_string()]);
    assert_eq!(p.rules.len(), 2);
    assert_eq!(p.rules[1].trigger, Trigger::SinceCreation);
    assert!(
        db.catalog.activity.contains_key(&("items".into(), "items_body".into())),
        "\"idle for seven days\" must not be reset by a restart"
    );
    // The idle clock kept running across the restart.
    let m = moves(&mut db, None);
    assert!(m.iter().any(|t| t.index == "items_body" && t.to == Tier::Cached), "{m:?}");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn dropping_a_policy_stops_it_moving_anything() {
    let mut db = Db::in_memory();
    setup(&mut db, 30);
    db.execute("CREATE LIFECYCLE POLICY p ON items FOR (items_body) MOVE TO cold AFTER 1 minute")
        .unwrap();
    ack(&mut db, "DROP LIFECYCLE POLICY p");
    age("items", "items_body", &mut db, DAY, 0);
    assert!(moves(&mut db, None).is_empty());
    let e = db.execute("DROP LIFECYCLE POLICY p").unwrap_err().to_string();
    assert!(e.contains("no lifecycle policy"), "{e}");
}

#[test]
fn the_reports_say_what_is_resident_and_what_is_scheduled() {
    let mut db = Db::in_memory();
    setup(&mut db, 200);
    db.execute("CREATE LIFECYCLE POLICY cool ON items MOVE TO cold AFTER 2 hours OF INACTIVITY")
        .unwrap();
    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3").unwrap();

    let r = ack(&mut db, "SHOW RESIDENCY");
    assert!(r.contains("text:body"), "{r}");
    assert!(r.contains("resident") && r.contains("budget"), "{r}");
    assert!(r.contains("idle unload:"), "the report doubles as the tuning knobs: {r}");

    let l = ack(&mut db, "SHOW LIFECYCLE");
    assert!(l.contains("cool") && l.contains("2 hours"), "{l}");
    assert!(l.contains("items_body") && l.contains("declared="), "{l}");
}

#[test]
fn explain_attributes_a_fault_in_to_the_unit_that_paid_for_it() {
    let d = dir("explain");
    let mut o = DbOpts::default();
    o.residency.active_idle_unload = Some(std::time::Duration::from_secs(0));
    o.residency.cached_idle_unload = std::time::Duration::from_secs(0);
    let mut db = Db::open(&d, o).unwrap();
    setup(&mut db, 300);

    let sql = "SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 4";
    db.query(sql).unwrap();
    db.execute("UNLOAD IDLE").unwrap();

    let cold = match db.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(
        cold.contains("residency:") && cold.contains("decoded on demand"),
        "a query that had to decode should say so:\n{cold}"
    );

    let warm = match db.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(
        !warm.contains("residency:"),
        "and a query that decoded nothing should stay quiet:\n{warm}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

// --------------------------------------------------------------------------
// Regressions
// --------------------------------------------------------------------------

/// A storage fault must not read as "no rows matched".
#[test]
fn a_refused_archived_read_fails_the_query_instead_of_shortening_it() {
    let d = dir("silent");
    let mut o = DbOpts::default();
    o.residency.archived_access = ArchivedAccess::Refuse;
    let mut db = Db::open(&d, o).unwrap();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY)").unwrap();
    db.execute("CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer='english')")
        .unwrap();
    // `rare` is present in under half the documents, so it is Sparse: never
    // shredded, always read from the document blobs.
    for i in 0..60 {
        let mut fields = vec![
            ("id".to_string(), Value::Str(format!("d-{i:03}"))),
            ("body".to_string(), Value::Str(format!("postings and segments {i}"))),
        ];
        if i % 4 == 0 {
            fields.push(("rare".to_string(), Value::Str("vanilla".into())));
        }
        db.insert("items", Value::obj(fields)).unwrap();
    }
    db.execute("FLUSH items").unwrap();

    let before = db.query("SELECT id FROM items WHERE rare = 'vanilla' LIMIT 100").unwrap();
    assert_eq!(before.rows.len(), 15);

    db.execute("ALTER INDEX items_body ON items SET TIER 'archived'").unwrap();
    let r = db.query("SELECT id FROM items WHERE rare = 'vanilla' LIMIT 100");
    match r {
        Err(e) => assert!(e.to_string().contains("archived"), "{e}"),
        Ok(q) => assert_eq!(
            q.rows.len(),
            15,
            "an unreadable document store must be an error, never a shorter answer"
        ),
    }
    let _ = std::fs::remove_dir_all(&d);
}

/// The same refusal, on the path a PREFIX expansion takes.
///
/// The test above uses a non-text predicate, so it never enters
/// `Shard::prefix_terms` — and that function opens its text handle with the
/// fallible spelling precisely so that an archived segment configured to refuse
/// reads surfaces the refusal instead of looking like a path with no index and
/// silently dropping its terms out of the expansion. A silently shorter
/// expansion is a silently shorter ANSWER, which is the failure mode the whole
/// truncation report exists to make impossible.
#[test]
fn a_refused_archived_read_fails_a_prefix_expansion_instead_of_shortening_it() {
    let d = dir("silent-prefix");
    let mut o = DbOpts::default();
    o.residency.archived_access = ArchivedAccess::Refuse;
    let mut db = Db::open(&d, o).unwrap();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY)").unwrap();
    db.execute("CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer='english')")
        .unwrap();
    for i in 0..60 {
        db.insert(
            "items",
            Value::obj(vec![
                ("id".to_string(), Value::Str(format!("d-{i:03}"))),
                ("body".to_string(), Value::Str(format!("alpha{i:03}"))),
            ]),
        )
        .unwrap();
    }
    db.execute("FLUSH items").unwrap();

    let sql = "SELECT id FROM items WHERE text_match(body, 'alpha*') LIMIT 1000";
    assert_eq!(db.query(sql).unwrap().rows.len(), 60, "60 distinct terms, all readable");

    db.execute("ALTER INDEX items_body ON items SET TIER 'archived'").unwrap();
    // Strictly `Err`, unlike its sibling above: the dictionary this expansion
    // has to read IS the archived index, so there is no arrangement in which
    // the query legitimately succeeds. An `Ok` here is the silent shortening.
    let e = db.query(sql).expect_err("a refused dictionary read must fail the query").to_string();
    assert!(e.contains("archived"), "{e}");
    let _ = std::fs::remove_dir_all(&d);
}

/// The ledger must not keep charging for segments compaction has retired.
#[test]
fn compaction_releases_the_ledger_entries_of_the_segments_it_retires() {
    let d = dir("ghosts");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 100);
    for round in 0..4 {
        for i in 0..100 {
            db.insert("items", doc(1000 * (round + 1) + i)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3").unwrap();
    }
    db.execute("COMPACT items").unwrap();

    let live: usize = db.residency().snapshot().iter().filter(|c| c.loaded).map(|c| c.bytes).sum();
    assert_eq!(
        db.residency().resident_bytes(),
        live,
        "the resident total is the sum of what is actually loaded, after compaction as before"
    );
    let uids: std::collections::BTreeSet<u64> =
        db.residency().snapshot().iter().map(|c| c.uid).collect();
    assert!(uids.len() <= 4, "retired segments left {} uids behind", uids.len());
    let _ = std::fs::remove_dir_all(&d);
}

/// Two shards each have a segment 1; their ledger entries must not merge.
#[test]
fn two_collections_with_the_same_segment_id_are_accounted_separately() {
    let d = dir("collide");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    for name in ["alpha", "beta"] {
        db.execute(&format!("CREATE COLLECTION {name} (id TEXT PRIMARY KEY, kind TEXT)")).unwrap();
        db.execute(&format!(
            "CREATE INDEX {name}_body ON {name} USING fulltext (body) WITH (analyzer='english')"
        ))
        .unwrap();
        for i in 0..80 {
            db.insert(name, doc(i)).unwrap();
        }
        db.execute(&format!("FLUSH {name}")).unwrap();
        db.query(&format!("SELECT * FROM {name} WHERE text_match(body, 'postings') LIMIT 3"))
            .unwrap();
    }
    let text: Vec<&celastro::residency::ComponentStat> = {
        let snap: &'static Vec<celastro::residency::ComponentStat> =
            Box::leak(Box::new(db.residency().snapshot()));
        snap.iter().filter(|c| c.component == "text:body" && c.loaded).collect()
    };
    assert_eq!(
        text.len(),
        2,
        "each collection's own text index needs its own ledger row, not a shared one"
    );
    assert_eq!(text[0].segment, text[1].segment, "and they genuinely share a segment id");
    assert_ne!(text[0].uid, text[1].uid);
    let _ = std::fs::remove_dir_all(&d);
}

/// A retention demotion is not undone by traffic.
#[test]
fn an_age_rule_does_not_flap_against_access_promotion() {
    let d = dir("flap");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 60);
    db.execute(
        "CREATE LIFECYCLE POLICY retention ON items MOVE TO archived AFTER 90 days SINCE CREATION",
    )
    .unwrap();
    age("items", "items_body", &mut db, 0, 100 * DAY);
    assert_eq!(moves(&mut db, None).len(), 1);
    assert_eq!(tier(&db, "items_body"), Tier::Archived);

    // Query it repeatedly. Each query would otherwise promote it, and each
    // lifecycle run would archive it again — moving the segment files both
    // ways, forever.
    for _ in 0..3 {
        let _ = db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2");
        assert_eq!(
            tier(&db, "items_body"),
            Tier::Archived,
            "an index archived for its age stays archived; use is not an argument against age"
        );
        assert!(moves(&mut db, None).is_empty());
    }
    let _ = std::fs::remove_dir_all(&d);
}

/// Sorting on a path is not a use of that path's full-text index.
#[test]
fn ordering_on_a_field_does_not_count_as_using_its_text_index() {
    let mut db = Db::in_memory();
    setup(&mut db, 40);
    db.execute(
        "CREATE LIFECYCLE POLICY cool ON items FOR (items_body) MOVE TO cold AFTER 5 minutes",
    )
    .unwrap();
    age("items", "items_body", &mut db, HOUR, 0);
    assert_eq!(moves(&mut db, None).len(), 1);
    assert_eq!(tier(&db, "items_body"), Tier::Cached);

    db.query("SELECT * FROM items ORDER BY body ASC LIMIT 3").unwrap();
    assert_eq!(
        tier(&db, "items_body"),
        Tier::Cached,
        "a lexicographic sort reads no postings, so it is not evidence the index is wanted"
    );

    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3").unwrap();
    assert_eq!(tier(&db, "items_body"), Tier::Active, "an actual search is");
}

/// Two indexes on one path must tier independently.
#[test]
fn a_full_text_and_a_secondary_index_on_one_path_do_not_share_a_tier() {
    let mut db = Db::in_memory();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE INDEX b_text ON items USING fulltext (body) \
         WITH (analyzer='english', tier='hot')",
    )
    .unwrap();
    db.execute("CREATE INDEX b_col ON items USING secondary (body) WITH (tier='archived')")
        .unwrap();
    let tiers = db.catalog.get("items").unwrap().index_tiers();
    assert_eq!(tiers.get("text:body"), Some(&Tier::Active));
    assert_eq!(
        tiers.get("col:body"),
        Some(&Tier::Archived),
        "different components, so a shared path does not make a shared tier"
    );
}

/// The activity clock has to be on disk, not only in memory.
///
/// Written so it can actually fail: the clock is put a long way in the past and
/// persisted, so if the read path writes nothing the stale value is what comes
/// back. Asserting only that *some* recent clock survives passes even with the
/// feature removed, because `CREATE INDEX` already wrote one.
#[test]
fn a_query_persists_its_access_clock_without_any_explicit_flush() {
    let d = dir("clock");
    let stale;
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 40);
        age("items", "items_body", &mut db, 10 * DAY, 0);
        stale = db.catalog.activity[&("items".into(), "items_body".into())].last_access_micros;
        db.persist().unwrap();
    }
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        assert_eq!(
            db.catalog.activity[&("items".into(), "items_body".into())].last_access_micros,
            stale,
            "the stale clock is what was on disk"
        );
        db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").unwrap();
        // No persist(), no DDL, no writes: a reader that simply stops.
    }
    let db = Db::open(&d, DbOpts::default()).unwrap();
    let back = db.catalog.activity[&("items".into(), "items_body".into())].last_access_micros;
    assert!(
        back > stale + 9 * DAY,
        "a query has to move the clock on disk, not only in memory ({back} vs {stale})"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A promotion moves segment files, so it must be on disk before the process is.
#[test]
fn a_promotion_is_persisted_before_the_process_can_lose_it() {
    let d = dir("promo");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 60);
        db.execute(
            "CREATE LIFECYCLE POLICY cool ON items FOR (items_body) MOVE TO archived AFTER 5 minutes",
        )
        .unwrap();
        age("items", "items_body", &mut db, HOUR, 0);
        assert_eq!(moves(&mut db, None).len(), 1);
        db.persist().unwrap();
    }
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        assert_eq!(tier(&db, "items_body"), Tier::Archived);
        db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").unwrap();
        assert_eq!(tier(&db, "items_body"), Tier::Active);
        // Dropped without persist().
    }
    let db = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(
        tier(&db, "items_body"),
        Tier::Active,
        "the promotion relocated segment files; a catalog that still says archived is a lie \
         nothing later reconciles"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A tier change has to reach the eviction order, not wait for a query.
#[test]
fn demoting_an_index_reorders_eviction_immediately() {
    let d = dir("order");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 300);
    db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 3").unwrap();
    db.query(
        "SELECT * FROM items ORDER BY embedding <=> [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] LIMIT 3",
    )
    .unwrap();

    // items_emb is cold, items_body hot: the vector index evicts first.
    let first = db.residency().plan_evictions(1);
    assert!(first[0].1.starts_with("vec:"), "{first:?}");

    // Demote the text index past it — and do not query anything afterwards,
    // which is the whole point: an index demoted because nobody queries it
    // must not keep its old priority until somebody does.
    db.execute("ALTER INDEX items_body ON items SET TIER 'archived'").unwrap();
    let after = db.residency().plan_evictions(1);
    assert!(
        after[0].1.starts_with("text:"),
        "the newly archived index should now be first to go: {after:?}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A tier change whose files will not move must leave nothing behind.
#[test]
fn a_tier_change_that_cannot_move_its_files_changes_nothing() {
    let d = dir("rollback");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    setup(&mut db, 80);
    db.persist().unwrap();

    // Make the archive directory un-renameable-into.
    let arch = d.join("collections/items/shard-0000/archive");
    std::fs::remove_dir_all(&arch).unwrap();
    std::fs::write(&arch, b"not a directory").unwrap();

    let before = tier(&db, "items_body");
    for i in ["items_emb", "items_kind"] {
        db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")).unwrap();
    }
    // The last one tips the collection into fully-archived, so files must move.
    let e = db.execute("ALTER INDEX items_body ON items SET TIER 'archived'");
    assert!(e.is_err(), "a rename onto a regular file cannot succeed");
    assert_eq!(
        tier(&db, "items_body"),
        before,
        "a tier the files could not follow must not be left in the catalog"
    );
    assert!(
        db.query("SELECT * FROM items WHERE text_match(body, 'postings') LIMIT 2").is_ok(),
        "and the collection is still queryable"
    );
    // Nor on disk.
    std::fs::remove_file(&arch).unwrap();
    std::fs::create_dir_all(&arch).unwrap();
    db.persist().unwrap();
    let re = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(re.catalog.get("items").unwrap().index_by_name("items_body").unwrap().tier, before);
    let _ = std::fs::remove_dir_all(&d);
}

/// A damaged catalog must be detected, not decoded into nonsense.
///
/// Truncation alone is a weak test — the body decoder catches most of it on its
/// own — so this flips single bits across the whole file, which only the frame's
/// checksum can catch.
#[test]
fn a_damaged_catalog_is_reported_rather_than_silently_accepted() {
    let d = dir("torn");
    {
        let mut db = Db::open(&d, DbOpts::default()).unwrap();
        setup(&mut db, 20);
        db.execute("CREATE LIFECYCLE POLICY p ON items MOVE TO cold AFTER 2 hours").unwrap();
        db.persist().unwrap();
    }
    let p = d.join("CATALOG");
    let good = std::fs::read(&p).unwrap();
    assert_eq!(&good[0..4], b"CLSC", "the catalog is framed");

    let mut checked = 0;
    for i in (0..good.len()).step_by(7) {
        for bit in [0x01u8, 0x80] {
            let mut b = good.clone();
            b[i] ^= bit;
            if b == good {
                continue;
            }
            std::fs::write(&p, &b).unwrap();
            match Db::open(&d, DbOpts::default()) {
                Err(_) => checked += 1,
                Ok(_) => panic!("byte {i} bit {bit:#x}: a corrupt catalog was accepted"),
            }
        }
    }
    assert!(checked > 40, "only {checked} corruptions exercised");

    // And truncation, which is what a crash mid-write actually leaves.
    for cut in [0, 1, 5, good.len() / 3, good.len() / 2, good.len() - 1] {
        std::fs::write(&p, &good[..cut]).unwrap();
        assert!(Db::open(&d, DbOpts::default()).is_err(), "truncation to {cut} was accepted");
    }
    std::fs::write(&p, &good).unwrap();
    assert!(Db::open(&d, DbOpts::default()).is_ok(), "and the intact file still opens");
    let _ = std::fs::remove_dir_all(&d);
}

/// The reason a transition gives has to name the rule that actually fired.
#[test]
fn the_reason_names_the_rule_that_fired_not_one_that_merely_targets_the_tier() {
    let mut db = Db::in_memory();
    setup(&mut db, 30);
    db.execute(
        "CREATE LIFECYCLE POLICY both ON items FOR (items_body) \
           MOVE TO archived AFTER 1 hours OF INACTIVITY, \
           MOVE TO archived AFTER 60 days SINCE CREATION",
    )
    .unwrap();
    // Idle for two hours, but only a day old: only the inactivity rule fired.
    age("items", "items_body", &mut db, 2 * HOUR, DAY);
    let m = moves(&mut db, None);
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].trigger, Trigger::Inactivity, "{}", m[0].reason);
    assert!(
        m[0].reason.starts_with("idle") && m[0].reason.contains("1 hour"),
        "the reason should quote the rule that fired: {}",
        m[0].reason
    );

    // Now both fire, reaching the same tier. The honest explanation is the one
    // with the longest window that was actually satisfied — the *stronger*
    // statement about this index, not merely the first rule in the list.
    let mut db = Db::in_memory();
    setup(&mut db, 30);
    db.execute(
        "CREATE LIFECYCLE POLICY both ON items FOR (items_body) \
           MOVE TO archived AFTER 1 hours OF INACTIVITY, \
           MOVE TO archived AFTER 60 days SINCE CREATION",
    )
    .unwrap();
    age("items", "items_body", &mut db, 2 * HOUR, 100 * DAY);
    let m = moves(&mut db, None);
    assert_eq!(m.len(), 1);
    assert_eq!(
        m[0].trigger,
        Trigger::SinceCreation,
        "60 days beats 1 hour when both fired: {}",
        m[0].reason
    );
    assert!(m[0].reason.contains("60 days"), "{}", m[0].reason);
}

/// A policy that only demotes should say so rather than accept a dead rule.
#[test]
fn a_rule_that_would_promote_is_refused_at_creation() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    let e = db
        .execute("CREATE LIFECYCLE POLICY warm ON items MOVE TO hot AFTER 1 day")
        .unwrap_err()
        .to_string();
    assert!(e.contains("never fire") && e.contains("declared tier"), "{e}");
}

/// A policy name is global, so a second CREATE must not silently delete one.
#[test]
fn creating_a_policy_that_already_exists_is_refused() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    db.execute("CREATE COLLECTION other (id TEXT PRIMARY KEY)").unwrap();
    db.execute("CREATE INDEX other_b ON other USING fulltext (body) WITH (analyzer='standard')")
        .unwrap();
    db.execute("CREATE LIFECYCLE POLICY retention ON items MOVE TO cold AFTER 1 day").unwrap();
    let e = db
        .execute("CREATE LIFECYCLE POLICY retention ON other MOVE TO cold AFTER 1 day")
        .unwrap_err()
        .to_string();
    assert!(e.contains("already exists") && e.contains("items"), "{e}");
    assert_eq!(db.catalog.policies["retention"].collection, "items");
}

/// `SINCE ACCESS` is a documented synonym for the default trigger.
#[test]
fn since_access_is_accepted_as_a_synonym_for_of_inactivity() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    db.execute("CREATE LIFECYCLE POLICY a ON items MOVE TO cold AFTER 2 hours SINCE ACCESS")
        .unwrap();
    db.execute("CREATE LIFECYCLE POLICY b ON items MOVE TO cold AFTER 2 hours SINCE CREATION")
        .unwrap();
    assert_eq!(db.catalog.policies["a"].rules[0].trigger, Trigger::Inactivity);
    assert_eq!(db.catalog.policies["b"].rules[0].trigger, Trigger::SinceCreation);
}

/// A zero duration must not survive a catalog round trip.
#[test]
fn a_zero_duration_is_refused_on_the_way_in_from_disk() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    db.execute("CREATE LIFECYCLE POLICY p ON items MOVE TO cold AFTER 1 minute").unwrap();
    let good = db.catalog.encode();
    assert!(celastro::catalog::Catalog::decode(&good).is_ok());

    // The rule's count is a uvarint `1`; every other `1` byte in the encoding
    // is either a length or a discriminant, so flip each in turn and require
    // that no rewrite ever yields a policy with a zero-length window.
    let mut seen = 0;
    for i in 0..good.len() {
        if good[i] != 1 {
            continue;
        }
        let mut b = good.clone();
        b[i] = 0;
        // Re-frame so the checksum matches; we are testing the decoder's own
        // validation, not the frame's.
        let body = &b[5..b.len() - 4];
        let mut reframed = b[..5].to_vec();
        reframed.extend_from_slice(body);
        reframed.extend_from_slice(&celastro::codec::crc32(body).to_le_bytes());
        if let Ok(c) = celastro::catalog::Catalog::decode(&reframed) {
            for p in c.policies.values() {
                for r in &p.rules {
                    assert!(
                        r.after.micros() > 0,
                        "a zero window is satisfied by every index at every instant"
                    );
                    seen += 1;
                }
            }
        }
    }
    assert!(seen > 0, "no variant decoded, so nothing was actually checked");
}

/// A corrupt length prefix is an error, not an overflow.
#[test]
fn a_hostile_length_prefix_does_not_overflow_the_decoder() {
    use celastro::codec::{get_bytes, get_str, put_uvarint};
    for n in [u64::MAX, u64::MAX - 1, 1 << 63, usize::MAX as u64 - 3] {
        let mut b = Vec::new();
        put_uvarint(&mut b, n);
        b.extend_from_slice(b"payload");
        let mut i = 0usize;
        assert!(get_str(&b, &mut i).is_none(), "length {n}");
        let mut i = 0usize;
        assert!(get_bytes(&b, &mut i).is_none(), "length {n}");
    }
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Push an index's clocks into the past. Lifecycle durations are days; tests
/// are milliseconds.
fn age(collection: &str, index: &str, db: &mut Db, idle: u64, age: u64) {
    let now = celastro::lifecycle::now_micros(&db.clock);
    let e = db
        .catalog
        .activity
        .entry((collection.to_string(), index.to_string()))
        .or_insert_with(|| IndexActivity::new(now));
    e.last_access_micros = now.saturating_sub(idle);
    e.created_micros = now.saturating_sub(age.max(idle));
}

/// The transitions one lifecycle run made, asserting it hit no failures.
fn moves(db: &mut Db, collection: Option<&str>) -> Vec<celastro::lifecycle::Transition> {
    let r = db.run_lifecycle(collection).unwrap();
    assert!(r.failures.is_empty(), "lifecycle run reported failures: {:?}", r.failures);
    r.moves
}

fn tier(db: &Db, index: &str) -> Tier {
    db.catalog.get("items").unwrap().index_by_name(index).unwrap().tier
}

#[test]
fn a_policy_built_by_hand_matches_the_one_the_parser_builds() {
    let mut db = Db::in_memory();
    setup(&mut db, 10);
    db.execute(
        "CREATE LIFECYCLE POLICY p ON items FOR (items_body) \
           MOVE TO cold AFTER 30 minutes OF INACTIVITY, \
           MOVE TO archived AFTER 2 days SINCE CREATION",
    )
    .unwrap();
    let want = LifecyclePolicy {
        name: "p".into(),
        collection: "items".into(),
        indexes: vec!["items_body".into()],
        rules: vec![
            Rule {
                to: Tier::Cached,
                after: Every::new(30, Unit::Minutes).unwrap(),
                trigger: Trigger::Inactivity,
            },
            Rule {
                to: Tier::Archived,
                after: Every::new(2, Unit::Days).unwrap(),
                trigger: Trigger::SinceCreation,
            },
        ],
    };
    assert_eq!(db.catalog.policies["p"], want);
}
