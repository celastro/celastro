//! `snippet(path, n)`: why a row matched, as `n` of the field's words
//! around the query's first match with the matched words marked.

use celastro::engine::Db;
use celastro::value::Value;

fn setup() -> Db {
    let mut db = Db::in_memory();
    db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY, n INT)").unwrap();
    db.execute(
        "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
    )
    .unwrap();
    db.execute("CREATE INDEX notes_tag ON notes USING fulltext (tag) WITH (analyzer = 'keyword')")
        .unwrap();
    for (id, body, tag) in [
        (
            "n1",
            r#""a seal writes the segments it froze and the memtable is free again""#,
            "HotItem",
        ),
        ("n2", r#"["seal one", "memtable two"]"#, "cold"),
        ("n3", r#""nothing of note here""#, "cold"),
    ] {
        db.execute(&format!(
            r#"INSERT INTO notes VALUES ('{{"id":"{id}","n":7,"title":"one two three four","body":{body},"tag":"{tag}"}}')"#
        ))
        .unwrap();
    }
    db
}

fn first(db: &mut Db, sql: &str) -> Value {
    let r = db.query(sql).unwrap();
    assert_eq!(r.rows.len(), 1, "{sql}: {} rows", r.rows.len());
    r.rows[0].doc.clone()
}

#[test]
fn a_snippet_is_the_words_around_the_first_match_with_the_match_marked() {
    let mut db = setup();
    let d = first(
        &mut db,
        "SELECT id, snippet(body, 6) FROM notes WHERE text_match(body, 'segments') AND id = 'n1'",
    );
    assert_eq!(
        d.path("snippet(body)").and_then(|v| v.as_str()),
        Some("a seal writes the <em>segments</em> it …")
    );
    // Cut at both ends when the match is deep in the field.
    let d = first(
        &mut db,
        "SELECT snippet(body, 3) FROM notes WHERE text_match(body, 'memtable') AND id = 'n1'",
    );
    assert_eq!(
        d.path("snippet(body)").and_then(|v| v.as_str()),
        Some("… and the <em>memtable</em> …")
    );
}

#[test]
fn the_window_is_placed_to_cover_the_most_matches() {
    let mut db = setup();
    let d = first(
        &mut db,
        "SELECT snippet(body, 9) AS why FROM notes WHERE text_match(body, 'seal memtable') AND id = 'n1'",
    );
    assert_eq!(
        d.path("why").and_then(|v| v.as_str()),
        Some("… <em>seal</em> writes the segments it froze and the <em>memtable</em> …")
    );
    // When every match fits with room to spare, the room goes before the
    // first: a window starting at the match would show no lead-in.
    let d = first(
        &mut db,
        "SELECT snippet(body, 9) AS why FROM notes WHERE text_match(body, 'segments froze') AND id = 'n1'",
    );
    assert_eq!(
        d.path("why").and_then(|v| v.as_str()),
        Some("a seal writes the <em>segments</em> it <em>froze</em> and the …")
    );
}

#[test]
fn a_prefix_marks_the_words_it_expanded_to_and_a_hybrid_source_counts() {
    let mut db = setup();
    let d = first(
        &mut db,
        "SELECT snippet(body, 5) FROM notes WHERE text_match(body, 'seg*') AND id = 'n1'",
    );
    assert_eq!(
        d.path("snippet(body)").and_then(|v| v.as_str()),
        Some("a seal writes the <em>segments</em> …")
    );
    let d = first(
        &mut db,
        "SELECT snippet(body, 2) FROM notes WHERE id = 'n1' ORDER BY hybrid(text_match(body, 'froze')) LIMIT 1",
    );
    assert_eq!(d.path("snippet(body)").and_then(|v| v.as_str()), Some("… it <em>froze</em> …"));
}

#[test]
fn a_field_the_query_did_not_name_a_non_text_field_and_an_array() {
    let mut db = setup();
    let d = first(
        &mut db,
        "SELECT snippet(title, 3), snippet(n, 3), snippet(body, 4) FROM notes WHERE text_match(body, 'seal') AND id = 'n2'",
    );
    assert_eq!(d.path("snippet(title)").and_then(|v| v.as_str()), Some("one two three …"));
    assert_eq!(d.path("snippet(n)"), Some(&Value::Null));
    assert_eq!(
        d.path("snippet(body)").and_then(|v| v.as_str()),
        Some("<em>seal</em> one memtable two")
    );
}

#[test]
fn a_keyword_field_is_one_word_marked_whole() {
    let mut db = setup();
    let d = first(&mut db, "SELECT snippet(tag, 5) FROM notes WHERE text_match(tag, 'hotitem')");
    assert_eq!(d.path("snippet(tag)").and_then(|v| v.as_str()), Some("<em>HotItem</em>"));
    let d = first(&mut db, "SELECT snippet(tag, 5) FROM notes WHERE id = 'n3'");
    assert_eq!(d.path("snippet(tag)").and_then(|v| v.as_str()), Some("cold"));
}
