//! A client that times out and retries delivers a statement twice. For every
//! statement the contract is: delivered twice with nothing else written in
//! between, the database is what delivering it once leaves -- the second
//! delivery is either the same effect or a refusal that changes nothing.
//! What the contract does not cover is named by the last test: a
//! predicate's second run sees the rows written since the first.

use std::path::PathBuf;

use celastro::engine::{Db, DbOpts};

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-retry-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Everything visible: the rows of every collection, sorted, and the
/// catalog's shape.
fn visible(db: &mut Db) -> Vec<String> {
    let mut out = Vec::new();
    let names: Vec<String> = db.catalog.collections.keys().cloned().collect();
    for name in names {
        let c = db.collection(&name).unwrap();
        out.push(format!(
            "{name}: pk={} prefix={:?} indexes={:?}",
            c.primary_key,
            c.prefix_expansion,
            c.indexes.iter().map(|i| format!("{}@{}", i.name, i.tier.name())).collect::<Vec<_>>()
        ));
        let r = db.query(&format!("SELECT id, n FROM {name} LIMIT 10000")).unwrap();
        let mut rows: Vec<String> = r
            .rows
            .iter()
            .map(|row| format!("{}={}", row.key, celastro::json::to_string(&row.doc)))
            .collect();
        rows.sort();
        out.extend(rows);
    }
    out
}

const HISTORY: &[&str] = &[
    "CREATE COLLECTION t (id TEXT PRIMARY KEY, n INT)",
    "CREATE INDEX t_n ON t USING secondary (n)",
    r#"INSERT INTO t VALUES ('{"id":"a","n":1}'), ('{"id":"b","n":2}'), ('{"id":"c","n":3}')"#,
    r#"INSERT INTO t VALUES ('{"id":"b","n":20}')"#,
    "DELETE FROM t WHERE id = 'a'",
    "DELETE FROM t WHERE n > 10",
    "ALTER COLLECTION t SET (prefix_expansion = 100)",
    "ALTER INDEX t_n ON t SET TIER 'cached'",
    "FLUSH t",
    r#"INSERT INTO t VALUES ('{"id":"d","n":4}')"#,
    "COMPACT t",
    "DROP INDEX t_n ON t",
    "CREATE COLLECTION u (id TEXT PRIMARY KEY, n INT)",
    "DROP COLLECTION u",
];

#[test]
fn every_statement_delivered_twice_leaves_what_once_leaves() {
    let (d1, d2) = (dir("once"), dir("twice"));
    let mut once = Db::open(&d1, DbOpts::default()).unwrap();
    let mut twice = Db::open(&d2, DbOpts::default()).unwrap();
    for sql in HISTORY {
        once.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        twice.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        // The retry: the same statement again, its refusal allowed.
        let _ = twice.execute(sql);
        assert_eq!(visible(&mut once), visible(&mut twice), "after a retry of `{sql}`");
    }
    drop(once);
    drop(twice);
    let _ = std::fs::remove_dir_all(&d1);
    let _ = std::fs::remove_dir_all(&d2);
}

/// The one shape a retry can change: a DELETE by predicate delivered again
/// after a write it did not see. The second run deletes the new row too,
/// which is the contract -- a predicate is evaluated when it runs -- and
/// the reason a client should retry a DELETE ... WHERE only when it knows
/// nothing wrote in between, or delete by key.
#[test]
fn a_delete_by_predicate_retried_after_a_write_takes_the_new_row_too() {
    let d = dir("predicate");
    let mut db = Db::open(&d, DbOpts::default()).unwrap();
    db.execute("CREATE COLLECTION t (id TEXT PRIMARY KEY, n INT)").unwrap();
    db.execute(r#"INSERT INTO t VALUES ('{"id":"a","n":11}')"#).unwrap();
    db.execute("DELETE FROM t WHERE n > 10").unwrap();
    db.execute(r#"INSERT INTO t VALUES ('{"id":"z","n":12}')"#).unwrap();
    db.execute("DELETE FROM t WHERE n > 10").unwrap();
    assert_eq!(db.query("SELECT id FROM t LIMIT 10").unwrap().rows.len(), 0);
    // By key, the retry is exact: a re-insert of `a` after the delete stays.
    db.execute(r#"INSERT INTO t VALUES ('{"id":"a","n":11}')"#).unwrap();
    db.execute("DELETE FROM t WHERE id = 'a'").unwrap();
    db.execute(r#"INSERT INTO t VALUES ('{"id":"a","n":11}')"#).unwrap();
    db.execute("DELETE FROM t WHERE id = 'a'").unwrap();
    assert_eq!(db.query("SELECT id FROM t LIMIT 10").unwrap().rows.len(), 0);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}
