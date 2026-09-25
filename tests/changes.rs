//! `Db::changes_since` and `GET /api/changes`: what changed after an
//! instant, as a follower is shipped it, paged, over one snapshot; a reset
//! when a compaction has forgotten a delete since then.

use celastro::engine::Db;
use celastro::replication::{SHIP_DELETE, SHIP_INSERT};

fn ack(db: &mut Db, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")).finished().unwrap();
}

fn setup() -> Db {
    let mut db = Db::in_memory();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    for i in 0..40usize {
        ack(&mut db, &format!(r#"INSERT INTO items VALUES ('{{"id":"k{i:03}","n":{i}}}')"#));
    }
    db
}

#[test]
fn a_round_pages_the_rows_after_since_in_key_order_over_one_snapshot() {
    let mut db = setup();
    let first = db.changes_since("items", 0, None, 15).unwrap();
    assert!(!first.reset);
    assert_eq!(first.items.len(), 15);
    assert!(first.items.iter().all(|i| i.kind == SHIP_INSERT));
    assert_eq!(first.items[0].key, "k000");
    let cursor = first.next.clone().expect("more pages");
    // Rows written during the round are not in it: the cursor pins the
    // snapshot, and the next round, from this round's `upto`, has them.
    ack(&mut db, r#"INSERT INTO items VALUES ('{"id":"k000","n":1000}')"#);
    ack(&mut db, r#"INSERT INTO items VALUES ('{"id":"k999","n":999}')"#);
    let mut keys: Vec<String> = first.items.iter().map(|i| i.key.clone()).collect();
    let mut cursor = Some(cursor);
    while let Some(c) = cursor {
        let page = db.changes_since("items", 0, Some(&c), 15).unwrap();
        assert_eq!(page.upto, first.upto, "a round reads one snapshot");
        keys.extend(page.items.iter().map(|i| i.key.clone()));
        cursor = page.next;
    }
    let want: Vec<String> = (0..40).map(|i| format!("k{i:03}")).collect();
    assert_eq!(keys, want, "every row once, in key order, none from during the round");
    // The next round has what the first did not: the replacement of k000 as
    // the delete of its old version then the insert of the new, and k999.
    let second = db.changes_since("items", first.upto, None, 100).unwrap();
    let kinds: Vec<(u8, &str)> = second.items.iter().map(|i| (i.kind, i.key.as_str())).collect();
    assert_eq!(kinds, [(SHIP_DELETE, "k000"), (SHIP_INSERT, "k000"), (SHIP_INSERT, "k999")]);
    let n = second.items[1].doc.as_ref().and_then(|d| d.path("n")).and_then(|v| v.as_i64());
    assert_eq!(n, Some(1000));
    assert!(second.next.is_none());
}

#[test]
fn deletes_come_first_and_a_compaction_that_forgot_one_resets_the_stream() {
    let mut db = setup();
    let t0 = db.changes_since("items", 0, None, 1).unwrap().upto;
    ack(&mut db, "DELETE FROM items WHERE id = 'k005'");
    ack(&mut db, r#"INSERT INTO items VALUES ('{"id":"k006","n":6006}')"#);
    let page = db.changes_since("items", t0, None, 100).unwrap();
    assert!(!page.reset);
    let kinds: Vec<(u8, &str)> = page.items.iter().map(|i| (i.kind, i.key.as_str())).collect();
    // k006 was replaced: its old version's delete, then the new row.
    assert_eq!(kinds, [(SHIP_DELETE, "k005"), (SHIP_DELETE, "k006"), (SHIP_INSERT, "k006")]);
    // Sealed, then the dead row collected: the delete is forgotten, and a
    // stream from before it cannot be served exactly.
    ack(&mut db, "FLUSH items");
    for i in 0..3 {
        ack(&mut db, &format!(r#"INSERT INTO items VALUES ('{{"id":"f{i}","n":{i}}}')"#));
        ack(&mut db, "FLUSH items");
    }
    ack(&mut db, "COMPACT items");
    let page = db.changes_since("items", t0, None, 1000).unwrap();
    assert!(page.reset, "the compaction forgot k005's delete");
    assert!(page.items.iter().all(|i| i.kind == SHIP_INSERT));
    assert!(!page.items.iter().any(|i| i.key == "k005"), "the deleted row is not in the rebuild");
    assert_eq!(page.items.len(), 42, "every live row, from nothing");
    // From the reset round's instant on, the stream is exact again.
    let later = db.changes_since("items", page.upto, None, 1000).unwrap();
    assert!(!later.reset);
    assert!(later.items.is_empty());
}

#[test]
fn a_bad_cursor_and_a_missing_collection_are_refused() {
    let db = setup();
    assert!(db.changes_since("items", 0, Some("nonsense"), 10).is_err());
    assert!(db.changes_since("nowhere", 0, None, 10).is_err());
}
