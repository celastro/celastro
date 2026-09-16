//! `count`, `sum`, `min`, `max`, `avg` and `GROUP BY`: one row, or one per
//! group, over every row the predicate admits, folded per shard and
//! merged at the coordinator; the answer over a flushed segment and a
//! memtable is the answer over the rows.

use celastro::engine::{Db, Outcome};
use celastro::plan::exec::QueryResult;
use celastro::value::Value;

fn setup() -> Db {
    let mut db = Db::in_memory();
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT)")
        .unwrap();
    db.execute("CREATE INDEX items_tenant ON items USING secondary (tenant)").unwrap();
    for i in 0..30usize {
        if i == 20 {
            db.execute("FLUSH items").unwrap();
        }
        // n is 0..29; `w` is n/2 as a float on even rows and absent on odd
        // ones; `tag` is a string per tenant.
        let mut fields = vec![
            ("id".to_string(), Value::Str(format!("doc-{i:03}"))),
            ("tenant".to_string(), Value::Str(format!("t{}", i % 3))),
            ("n".to_string(), Value::Int(i as i64)),
            ("tag".to_string(), Value::Str(format!("tag-{}", (i * 7) % 5))),
        ];
        if i % 2 == 0 {
            fields.push(("w".to_string(), Value::Float(i as f64 / 2.0)));
        }
        db.insert("items", Value::obj(fields)).unwrap();
    }
    db.execute("DELETE FROM items WHERE id = 'doc-005'").unwrap();
    db
}

fn rows(r: &QueryResult) -> Vec<(String, String)> {
    r.rows.iter().map(|row| (row.key.clone(), celastro::json::to_string(&row.doc))).collect()
}

fn one(db: &mut Db, sql: &str) -> String {
    let r = db.query(sql).unwrap();
    assert_eq!(r.rows.len(), 1, "{sql}: {:?}", rows(&r));
    celastro::json::to_string(&r.rows[0].doc)
}

#[test]
fn count_sum_min_max_avg_over_every_matching_row() {
    let mut db = setup();
    // 30 rows less one deleted; n sums 0..29 = 435 less 5.
    assert_eq!(one(&mut db, "SELECT count(*) FROM items"), r#"{"count(*)":29}"#);
    assert_eq!(
        one(&mut db, "SELECT count(*) AS n, sum(n) AS total, min(n), max(n), avg(n) FROM items"),
        r#"{"avg(n)":14.827586206896552,"max(n)":29,"min(n)":0,"n":29,"total":430}"#
    );
    // A predicate: the rows it admits, and only those.
    assert_eq!(
        one(&mut db, "SELECT count(*), sum(n) FROM items WHERE n >= 20"),
        r#"{"count(*)":10,"sum(n)":245}"#
    );
    assert_eq!(
        one(&mut db, "SELECT count(*), max(n) FROM items WHERE tenant = 't2'"),
        r#"{"count(*)":9,"max(n)":29}"#
    );
    // `count(path)` counts the rows that have it; `sum`/`avg` skip the rest.
    assert_eq!(
        one(&mut db, "SELECT count(w), sum(w), avg(w), count(*) FROM items"),
        r#"{"avg(w)":7.0,"count(*)":29,"count(w)":15,"sum(w)":105.0}"#
    );
    // Strings order as strings.
    assert_eq!(
        one(&mut db, "SELECT min(tag), max(tag) FROM items"),
        r#"{"max(tag)":"tag-4","min(tag)":"tag-0"}"#
    );
    // Nothing admitted: still one row, a count of zero and no other value.
    assert_eq!(
        one(&mut db, "SELECT count(*), sum(n), min(n), avg(n) FROM items WHERE n > 1000"),
        r#"{"avg(n)":null,"count(*)":0,"min(n)":null,"sum(n)":null}"#
    );
}

#[test]
fn group_by_gives_one_row_per_value_ordered_and_paged_by_the_result_fields() {
    let mut db = setup();
    let r = db
        .query("SELECT tenant, count(*) AS n, sum(n) AS total FROM items GROUP BY tenant")
        .unwrap();
    assert_eq!(
        rows(&r),
        vec![
            (r#""t0""#.into(), r#"{"n":10,"tenant":"t0","total":135}"#.into()),
            (r#""t1""#.into(), r#"{"n":10,"tenant":"t1","total":145}"#.into()),
            (r#""t2""#.into(), r#"{"n":9,"tenant":"t2","total":150}"#.into()),
        ]
    );
    // Ordered by an aggregate's alias, descending, one page.
    let r = db
        .query(
            "SELECT tenant, sum(n) AS total FROM items GROUP BY tenant ORDER BY total DESC LIMIT 2",
        )
        .unwrap();
    assert_eq!(
        rows(&r).iter().map(|(_, d)| d.as_str()).collect::<Vec<_>>(),
        vec![r#"{"tenant":"t2","total":150}"#, r#"{"tenant":"t1","total":145}"#]
    );
    let r = db
        .query("SELECT tenant, sum(n) AS total FROM items GROUP BY tenant ORDER BY total DESC LIMIT 2 OFFSET 2")
        .unwrap();
    assert_eq!(rows(&r).len(), 1);
    // A group by a path some rows lack: those rows are the `null` group.
    let r = db.query("SELECT w, count(*) FROM items GROUP BY w ORDER BY w LIMIT 100").unwrap();
    assert_eq!(r.rows.len(), 16, "{:?}", rows(&r));
    assert_eq!(rows(&r)[15].1, r#"{"count(*)":14,"w":null}"#, "null last");
    // The grouped path alone: the distinct values.
    let r = db.query("SELECT tag FROM items GROUP BY tag").unwrap();
    assert_eq!(r.rows.len(), 5);
    // With `EXPLAIN ANALYZE` the plan says so.
    let t = match db
        .execute("EXPLAIN ANALYZE SELECT tenant, count(*) FROM items GROUP BY tenant")
        .unwrap()
    {
        Outcome::Explain(t) => t,
        other => panic!("{other:?}"),
    };
    assert!(t.contains("3 group(s)"), "{t}");
}

#[test]
fn what_an_aggregate_refuses_and_why() {
    let mut db = setup();
    let e = |db: &mut Db, sql: &str| db.query(sql).unwrap_err().to_string();
    assert!(e(&mut db, "SELECT tenant, count(*) FROM items")
        .contains("neither aggregated nor the GROUP BY path"));
    assert!(e(&mut db, "SELECT *, count(*) FROM items")
        .contains("cannot be listed beside an aggregate"));
    assert!(e(&mut db, "SELECT sum(tag) FROM items").contains("not a number"));
    let mut mixed = Db::in_memory();
    mixed.execute("CREATE COLLECTION m (id TEXT PRIMARY KEY)").unwrap();
    mixed
        .insert(
            "m",
            Value::obj(vec![("id".into(), Value::Str("a".into())), ("v".into(), Value::Int(1))]),
        )
        .unwrap();
    mixed
        .insert(
            "m",
            Value::obj(vec![
                ("id".into(), Value::Str("b".into())),
                ("v".into(), Value::Str("x".into())),
            ]),
        )
        .unwrap();
    assert!(e(&mut mixed, "SELECT max(v) FROM m").contains("mixed kinds"));
    assert!(e(&mut db, "SELECT count(*) FROM items ORDER BY n LIMIT 1")
        .contains("orders by a field of its result"));
    assert!(e(&mut db, "SELECT count(*) FROM items COLLAPSE BY tenant").contains("COLLAPSE BY"));
    assert!(e(&mut db, "SELECT count(*) FROM items AFTER 'doc-001'").contains("AFTER"));
    let mut v = Db::in_memory();
    v.execute("CREATE COLLECTION vecs (id TEXT PRIMARY KEY)").unwrap();
    v.execute("CREATE INDEX vecs_e ON vecs USING vector (e) WITH (dims = 2, metric = 'l2')")
        .unwrap();
    assert!(e(&mut v, "SELECT count(*) FROM vecs ORDER BY e <-> [0.0, 1.0] LIMIT 5")
        .contains("ranked ORDER BY"));
}
