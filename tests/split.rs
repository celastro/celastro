//! `SPLIT SHARD`: one shard's range becomes two shards on the same node,
//! no row moving, and every read from then on sees each key once.

use std::path::PathBuf;

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::Value;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-split-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn ack(db: &mut Db, sql: &str) -> String {
    match db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")).finished().unwrap() {
        Outcome::Ack(m) | Outcome::Explain(m) => m,
        other => panic!("{sql}: {other:?}"),
    }
}

fn count(db: &mut Db, sql: &str) -> i64 {
    let r = db.query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    r.rows[0].doc.path("n").and_then(|v| v.as_i64()).unwrap()
}

fn doc(i: usize) -> Value {
    Value::obj(vec![
        ("id".into(), Value::Str(format!("k{i:04}"))),
        ("n".into(), Value::Int(i as i64)),
        ("body".into(), Value::Str(format!("row {i} of the split test"))),
    ])
}

/// A shard split at its median: every key answers once, from one shard,
/// with the same count, point lookups, text matches and ranked answers as
/// before; writes after the split land by the new map; the shard that
/// shrank drops the rows outside its range at the next compaction; and a
/// reopen finds every range where it was.
#[test]
fn a_split_shard_answers_every_key_once_and_a_compaction_drops_what_moved() {
    let d = dir("median");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    ack(&mut db, "CREATE INDEX items_body ON items USING fulltext (body)");
    for i in 0..400 {
        db.insert("items", doc(i)).unwrap();
    }
    ack(&mut db, "FLUSH items");
    for i in 400..500 {
        db.insert("items", doc(i)).unwrap();
    }
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 500);
    let m = ack(&mut db, "SPLIT SHARD 0 OF items AT 'k0250'");
    assert!(m.contains("shard 1 is [k0250, )"), "{m}");
    let cat = ack(&mut db, "SHOW CATALOG items");
    assert!(cat.contains("shard 0 on this node [, k0250)"), "{cat}");
    assert!(cat.contains("shard 1 on this node [k0250, )"), "{cat}");
    // Every key once, whichever side it fell on, sealed or in memory.
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 500);
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items WHERE n < 250"), 250);
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items WHERE n >= 250"), 250);
    assert_eq!(
        count(&mut db, "SELECT count(*) AS n FROM items WHERE text_match(body, 'row')"),
        500
    );
    for key in ["k0000", "k0249", "k0250", "k0499"] {
        let r = db.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 5")).unwrap();
        assert_eq!(r.rows.len(), 1, "{key} answers once");
    }
    let r = db
        .query("SELECT id FROM items ORDER BY hybrid(text_match(body, 'split')) LIMIT 1000")
        .unwrap();
    assert_eq!(r.rows.len(), 500, "a ranked answer names each key once");
    let plan = ack(&mut db, "EXPLAIN SELECT id FROM items WHERE id = 'k0300' LIMIT 1");
    assert!(plan.contains("1 of 2 shard(s) scanned"), "the key pins the new shard: {plan}");
    // Writes after the split go by the new map, and a replaced key stays one.
    for i in 500..520 {
        db.insert("items", doc(i)).unwrap();
    }
    db.insert("items", doc(100)).unwrap();
    db.insert("items", doc(300)).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 520);
    ack(&mut db, "DELETE FROM items WHERE id = 'k0260'");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 519);
    // The rows outside each range are still on disk until a compaction.
    let before = ack(&mut db, "SHOW SEGMENTS items");
    assert!(before.contains("400"), "both shards still hold the sealed 400: {before}");
    ack(&mut db, "FLUSH items");
    let mut jobs = 0;
    for _ in 0..8 {
        let compacted = ack(&mut db, "COMPACT items");
        if compacted.starts_with("0 compaction") {
            break;
        }
        jobs += 1;
    }
    assert!(jobs >= 1, "a COMPACT ran the rewrites: {jobs}");
    let after = ack(&mut db, "SHOW SEGMENTS items");
    assert!(!after.contains(" 400 "), "the halves were dropped: {after}");
    assert!(!after.contains("99.0%") && !after.contains("37."), "nothing dead is left: {after}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 519);
    // Wrong keys are refused before anything happens.
    for (sql, why) in [
        ("SPLIT SHARD 0 OF items AT 'k0250'", "strictly inside"),
        ("SPLIT SHARD 1 OF items AT 'k0100'", "strictly inside"),
        ("SPLIT SHARD 5 OF items AT 'k0300'", "no shard 5"),
        ("SPLIT SHARD 1 OF items AT ''", "non-empty"),
    ] {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(e.contains(why), "{sql}: {e}");
    }
    drop(db);
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 519);
    for key in ["k0000", "k0249", "k0250", "k0499", "k0519"] {
        let r = db.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 5")).unwrap();
        assert_eq!(r.rows.len(), 1, "{key} after the reopen");
    }
    let m = ack(&mut db, "SPLIT SHARD 1 OF items AT 'k0400'");
    assert!(m.contains("shard 2 is [k0400, )"), "{m}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 519);
    let _ = std::fs::remove_dir_all(&d);
}

/// The same under encryption at rest: the new shard's files are sealed
/// under their own name, and a reopen with the key reads both.
#[test]
fn a_split_of_an_encrypted_shard_reseals_every_file_under_its_new_name() {
    let d = dir("enc");
    let mut opts = DbOpts::default();
    opts.master_key = Some([7u8; 32].into());
    let mut db = Db::open(&d, opts.clone()).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    for i in 0..100 {
        db.insert("items", doc(i)).unwrap();
    }
    ack(&mut db, "FLUSH items");
    for i in 100..120 {
        db.insert("items", doc(i)).unwrap();
    }
    ack(&mut db, "SPLIT SHARD 0 OF items AT 'k0050'");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 120);
    drop(db);
    let mut db = Db::open(&d, opts).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 120);
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items WHERE n >= 50"), 70);
    let _ = std::fs::remove_dir_all(&d);
}

/// A split with no key takes the middle of the shard's keys, sealed and in
/// memory alike; a merge rebuilds the second shard's rows into the first,
/// widens its range, and leaves the second's entry as a marker that owns no
/// key, refuses a move and a split, and is skipped by every read. Every key
/// answers once throughout, across a reopen, and a compaction of the
/// merged shard changes nothing it answers.
#[test]
fn a_median_split_and_a_merge_are_each_other_s_inverse() {
    let d = dir("merge");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    ack(&mut db, "CREATE INDEX items_body ON items USING fulltext (body)");
    for i in 0..300 {
        db.insert("items", doc(i)).unwrap();
    }
    ack(&mut db, "FLUSH items");
    for i in 300..400 {
        db.insert("items", doc(i)).unwrap();
    }
    let m = ack(&mut db, "SPLIT SHARD 0 OF items");
    assert!(m.contains("split at 'k0200'") && m.contains("shard 1 is [k0200, )"), "{m}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 400);
    // The new shard splits again at its own middle: k0200..k0399 -> k0300.
    let m = ack(&mut db, "SPLIT SHARD 1 OF items");
    assert!(m.contains("split at 'k0300'") && m.contains("shard 2 is [k0300, )"), "{m}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 400);
    // Not adjacent: 0 is [, k0200), 2 is [k0300, ).
    let e = db.execute("MERGE SHARDS 0 AND 2 OF items").unwrap_err().to_string();
    assert!(e.contains("not adjacent"), "{e}");
    // Adjacent, named in either order: the first named survives.
    for i in 400..420 {
        db.insert("items", doc(i)).unwrap();
    }
    ack(&mut db, "DELETE FROM items WHERE id = 'k0350'");
    let m = ack(&mut db, "MERGE SHARDS 1 AND 2 OF items");
    assert!(m.contains("shard 1 is [k0200, )") && m.contains("shard 2 owns no key"), "{m}");
    assert!(m.contains("row(s) of shard 2 rebuilt"), "{m}");
    let cat = ack(&mut db, "SHOW CATALOG items");
    assert!(cat.contains("shard 1 on this node [k0200, )"), "{cat}");
    assert!(cat.contains("shard 2 merged away"), "{cat}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 419);
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items WHERE n >= 200"), 219);
    assert_eq!(
        count(&mut db, "SELECT count(*) AS n FROM items WHERE text_match(body, 'row')"),
        419
    );
    for key in ["k0199", "k0200", "k0299", "k0300", "k0419"] {
        let r = db.query(&format!("SELECT id FROM items WHERE id = '{key}' LIMIT 5")).unwrap();
        assert_eq!(r.rows.len(), 1, "{key} answers once");
    }
    let r = db.query("SELECT id FROM items WHERE id = 'k0350' LIMIT 5").unwrap();
    assert!(r.rows.is_empty(), "the delete before the merge holds");
    for (sql, why) in [
        ("SPLIT SHARD 2 OF items", "merged away"),
        ("MERGE SHARDS 1 AND 2 OF items", "merged away"),
        ("MERGE SHARDS 1 AND 1 OF items", "two different"),
    ] {
        let e = db.execute(sql).unwrap_err().to_string();
        assert!(e.contains(why), "{sql}: {e}");
    }
    // A write past the old boundary lands on the widened shard.
    db.insert("items", doc(500)).unwrap();
    let plan = ack(&mut db, "EXPLAIN SELECT id FROM items WHERE id = 'k0500' LIMIT 1");
    assert!(plan.contains("shard 1 (") && !plan.contains("shard 2 ("), "{plan}");
    ack(&mut db, "FLUSH items");
    for _ in 0..4 {
        if ack(&mut db, "COMPACT items").starts_with("0 compaction") {
            break;
        }
    }
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 420);
    drop(db);
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 420);
    assert!(
        !d.join("collections/items/shard-0002").exists(),
        "the merged shard's directory is gone"
    );
    // The whole way back: one shard again, every key.
    let m = ack(&mut db, "MERGE SHARDS 0 AND 1 OF items");
    assert!(m.contains("shard 0 is [, )"), "{m}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 420);
    let m = ack(&mut db, "SPLIT SHARD 0 OF items AT 'k0100'");
    assert!(m.contains("shard 3 is [k0100, )"), "a split after a merge takes the next index: {m}");
    assert_eq!(count(&mut db, "SELECT count(*) AS n FROM items"), 420);
    let _ = std::fs::remove_dir_all(&d);
}
