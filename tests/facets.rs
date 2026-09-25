//! `FACET`: each path's top values by count over every row the predicate
//! admits, beside the rows, over a memtable and a flushed segment alike.

use celastro::engine::Db;
use celastro::value::Value;
use std::collections::BTreeMap;

fn setup() -> Db {
    let mut db = Db::in_memory();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT)")
        .unwrap();
    for i in 0..30usize {
        if i == 20 {
            db.execute("FLUSH items").unwrap();
        }
        let w = if i % 2 == 0 { format!(r#","w":"{}""#, (i / 2) % 4) } else { String::new() };
        db.execute(&format!(
            r#"INSERT INTO items VALUES ('{{"id":"doc-{i:03}","tenant":"t{}","n":{i},"tag":"tag-{}"{w}}}')"#,
            i % 3,
            (i * 7) % 5
        ))
        .unwrap();
    }
    db
}

/// The counts the data has for rows with `n >= from`, by the same
/// arithmetic, the most first and equal counts by value.
fn model(from: usize, key: impl Fn(usize) -> Value) -> Vec<(Value, u64)> {
    let mut counts: BTreeMap<String, (Value, u64)> = BTreeMap::new();
    for i in from..30 {
        let v = key(i);
        counts.entry(celastro::json::to_string(&v)).or_insert((v, 0)).1 += 1;
    }
    let mut out: Vec<(Value, u64)> = counts.into_values().collect();
    out.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| celastro::json::to_string(&a.0).cmp(&celastro::json::to_string(&b.0)))
    });
    out
}

#[test]
fn a_facet_counts_every_row_the_predicate_admits_not_the_page() {
    let mut db = setup();
    let r = db.query("SELECT id FROM items WHERE n >= 7 FACET tenant, tag TOP 2 LIMIT 3").unwrap();
    assert_eq!(r.rows.len(), 3, "the page");
    assert_eq!(r.facets.len(), 2);
    let (path, values) = &r.facets[0];
    assert_eq!(path, "tenant");
    let want = model(7, |i| Value::Str(format!("t{}", i % 3)));
    assert_eq!(values, &want[..2], "top two tenants of {want:?}");
    let (path, values) = &r.facets[1];
    assert_eq!(path, "tag");
    let want = model(7, |i| Value::Str(format!("tag-{}", (i * 7) % 5)));
    assert_eq!(values, &want[..2], "top two tags of {want:?}");
}

#[test]
fn ten_values_unless_said_a_missing_path_counts_null_and_no_facet_means_none() {
    let mut db = setup();
    let r = db.query("SELECT id FROM items FACET w LIMIT 1").unwrap();
    let (_, values) = &r.facets[0];
    // Four values of `w` on the fifteen even rows (four, four, four, three)
    // and null on the fifteen odd ones.
    assert_eq!(values.len(), 5, "{values:?}");
    assert_eq!(values[0], (Value::Null, 15));
    let counts: Vec<u64> = values[1..].iter().map(|(_, n)| *n).collect();
    assert_eq!(counts, [4, 4, 4, 3], "{values:?}");
    let r = db.query("SELECT id FROM items FACET n TOP 3 LIMIT 1").unwrap();
    assert_eq!(r.facets[0].1.len(), 3, "TOP bounds the values");
    let r = db.query("SELECT id FROM items FACET n LIMIT 1").unwrap();
    assert_eq!(r.facets[0].1.len(), 10, "ten unless said");
    let r = db.query("SELECT id FROM items LIMIT 1").unwrap();
    assert!(r.facets.is_empty());
}

#[test]
fn a_facet_beside_an_aggregate_is_refused() {
    let mut db = setup();
    let e = db.query("SELECT count(*) FROM items FACET tenant").unwrap_err().to_string();
    assert!(e.contains("FACET"), "{e}");
    let e = db
        .query("SELECT tenant, count(*) FROM items GROUP BY tenant FACET tag")
        .unwrap_err()
        .to_string();
    assert!(e.contains("FACET"), "{e}");
}
