//! The `archived` tier against an S3-compatible object store, with the store
//! in-process: a small HTTP server that speaks the four operations the
//! client uses, checks that every request is signed, and remembers what it
//! holds, so these tests need no MinIO and prove the wire.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::residency::ArchivedAccess;
use celastro::value::Value;

const ACCESS_KEY: &str = "AKIAFAKEFAKEFAKEFAKE";

/// Serialises the moments the environment is read, since `Db::open` reads
/// the credentials from it and tests run in parallel.
static ENV: Mutex<()> = Mutex::new(());

struct FakeS3 {
    addr: String,
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl FakeS3 {
    fn start() -> FakeS3 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let objects: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::default();
        let requests: Arc<Mutex<Vec<String>>> = Arc::default();
        let (o, r) = (objects.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (o, r) = (o.clone(), r.clone());
                std::thread::spawn(move || serve(stream, o, r));
            }
        });
        FakeS3 { addr, objects, requests }
    }

    fn keys(&self) -> Vec<String> {
        let mut k: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        k.sort();
        k
    }

    fn requests(&self, method: &str) -> usize {
        self.requests.lock().unwrap().iter().filter(|r| r.starts_with(method)).count()
    }
}

fn serve(
    mut s: TcpStream,
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<String>>>,
) {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    let head_end = loop {
        let n = match s.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        raw.extend_from_slice(&buf[..n]);
        if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
    };
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut req = lines.next().unwrap_or("").split_whitespace();
    let method = req.next().unwrap_or("").to_string();
    let path = req.next().unwrap_or("").to_string();
    let mut headers: HashMap<String, String> = HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let want: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut body = raw[head_end + 4..].to_vec();
    while body.len() < want {
        let n = match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        body.extend_from_slice(&buf[..n]);
    }
    let reply = |s: &mut TcpStream, code: u16, reason: &str, body: &[u8], len_only: bool| {
        let mut out = format!(
            "HTTP/1.1 {code} {reason}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        if !len_only {
            out.extend_from_slice(body);
        }
        let _ = s.write_all(&out);
        let _ = s.flush();
    };
    // Every request is signed: the header shape S3 checks first.
    let hex64 = |v: &str| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit());
    let auth = headers.get("authorization").cloned().unwrap_or_default();
    let signed = auth.starts_with(&format!("AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/"))
        && auth.contains("SignedHeaders=host;")
        && auth.rsplit("Signature=").next().map(hex64).unwrap_or(false)
        && headers.get("x-amz-content-sha256").map(|v| hex64(v)).unwrap_or(false)
        && headers.get("x-amz-date").map(|v| v.len() == 16 && v.ends_with('Z')).unwrap_or(false);
    if !signed {
        reply(&mut s, 403, "Forbidden", b"<Error><Code>AccessDenied</Code></Error>", false);
        return;
    }
    let key = path.strip_prefix("/b/").unwrap_or("").to_string();
    requests.lock().unwrap().push(format!("{method} {key}"));
    let mut objects = objects.lock().unwrap();
    match method.as_str() {
        "PUT" => {
            objects.insert(key, body);
            reply(&mut s, 200, "OK", b"", false);
        }
        "HEAD" => match objects.get(&key) {
            Some(o) => reply(&mut s, 200, "OK", o, true),
            None => reply(&mut s, 404, "Not Found", b"", true),
        },
        "GET" => match objects.get(&key) {
            None => {
                reply(&mut s, 404, "Not Found", b"<Error><Code>NoSuchKey</Code></Error>", false)
            }
            Some(o) => match headers.get("range") {
                Some(r) => {
                    let (a, b) = r.trim_start_matches("bytes=").split_once('-').unwrap();
                    let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                    reply(&mut s, 206, "Partial Content", &o[a..=b.min(o.len() - 1)], false);
                }
                None => reply(&mut s, 200, "OK", o, false),
            },
        },
        "DELETE" => {
            objects.remove(&key);
            reply(&mut s, 204, "No Content", b"", false);
        }
        _ => reply(&mut s, 405, "Method Not Allowed", b"", false),
    }
}

fn opts(fake: &FakeS3) -> DbOpts {
    let mut o = DbOpts::default();
    o.archive.endpoint = Some(fake.addr.clone());
    o.archive.bucket = "b".into();
    o.archive.prefix = "celastro/".into();
    o.archive.region = "us-east-1".into();
    o
}

fn open(dir: &std::path::Path, o: DbOpts) -> celastro::Result<Db> {
    let _g = ENV.lock().unwrap();
    std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "fake-secret");
    Db::open(dir, o)
}

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-s3-{tag}-{}", std::process::id()));
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

fn setup(db: &mut Db, n: usize) {
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, kind TEXT)").unwrap();
    db.execute(
        "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english', tier = 'active')",
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_emb ON items USING vector (embedding) WITH (dims = 8, metric = 'cosine', tier = 'cached')",
    )
    .unwrap();
    db.execute("CREATE INDEX items_kind ON items USING secondary (kind) WITH (tier = 'active')")
        .unwrap();
    for i in 0..n {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
}

fn archive_all(db: &mut Db) {
    for i in ["items_body", "items_emb", "items_kind"] {
        db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")).unwrap();
    }
}

fn count(p: &std::path::Path) -> usize {
    std::fs::read_dir(p).map(|d| d.filter_map(|e| e.ok()).count()).unwrap_or(0)
}

/// Archiving puts the segment in the store under a key that reads like the
/// path it stands in for, and removes the local file; the collection still
/// answers, by ranged reads that count as fault-ins; bringing one index back
/// fetches the object into `segments/` and deletes it from the store.
#[test]
fn archiving_puts_the_segment_in_the_store_and_bringing_it_back_deletes_it() {
    let fake = FakeS3::start();
    let d = dir("roundtrip");
    let mut db = open(&d, opts(&fake)).unwrap();
    setup(&mut db, 200);
    let segs = d.join("collections/items/shard-0000/segments");
    let arch = d.join("collections/items/shard-0000/archive");
    assert!(count(&segs) > 0 && fake.keys().is_empty());

    archive_all(&mut db);
    let keys = fake.keys();
    assert!(!keys.is_empty(), "nothing reached the store");
    assert!(
        keys.iter().all(|k| k.starts_with("celastro/items/shard-0000/") && k.ends_with(".seg")),
        "{keys:?}"
    );
    assert_eq!(count(&segs), 0, "the local copy stayed");
    assert_eq!(count(&arch), 0, "the local archive directory was used instead of the store");

    let gets_before = fake.requests("GET");
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'postings') LIMIT 4").unwrap();
    assert_eq!(r.rows.len(), 4);
    assert!(db.residency().faults() > 0, "reading from the store is a fault-in");
    assert!(
        fake.requests("GET") > gets_before,
        "the answer came from somewhere other than the store"
    );

    db.execute("ALTER INDEX items_body ON items SET TIER 'active'").unwrap();
    assert!(fake.keys().is_empty(), "the object outlived its local copy: {:?}", fake.keys());
    assert!(count(&segs) > 0, "the file did not come back");
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'postings') LIMIT 4").unwrap();
    assert_eq!(r.rows.len(), 4);
    let _ = std::fs::remove_dir_all(&d);
}

/// A database whose segments are all in the store reopens with nothing
/// local but the manifest: the open asks the store for each object's size
/// and reads the footers by range, and a query then answers.
#[test]
fn an_archived_database_reopens_from_the_store() {
    let fake = FakeS3::start();
    let d = dir("reopen");
    {
        let mut db = open(&d, opts(&fake)).unwrap();
        setup(&mut db, 150);
        archive_all(&mut db);
        db.persist().unwrap();
    }
    let heads_before = fake.requests("HEAD");
    let mut db = open(&d, opts(&fake)).unwrap();
    assert!(fake.requests("HEAD") > heads_before, "the reopen never asked the store");
    let r = db.query("SELECT id FROM items WHERE text_match(body, 'segments') LIMIT 5").unwrap();
    assert_eq!(r.rows.len(), 5);
    // A plan over the component the first query did not touch: the vector
    // index faults in inside this unit, and the plan charges it there.
    let q = "[0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17]";
    let sql = format!("EXPLAIN ANALYZE SELECT id FROM items ORDER BY embedding <=> {q} LIMIT 3");
    let text = match db.execute(&sql).unwrap() {
        Outcome::Explain(t) => t,
        _ => panic!("expected a plan"),
    };
    assert!(text.contains("faulted in from the archive"), "{text}");
    let _ = std::fs::remove_dir_all(&d);
}

/// `Refuse` means refuse, against the store exactly as against a directory:
/// the query fails naming the component, and the store is never asked.
#[test]
fn refusing_archived_access_never_touches_the_store() {
    let fake = FakeS3::start();
    let d = dir("refuse");
    let mut o = opts(&fake);
    o.residency.archived_access = ArchivedAccess::Refuse;
    let mut db = open(&d, o).unwrap();
    setup(&mut db, 120);
    archive_all(&mut db);
    let gets = fake.requests("GET");
    let e = db
        .query("SELECT id FROM items WHERE text_match(body, 'postings') LIMIT 3")
        .unwrap_err()
        .to_string();
    assert!(e.contains("text:body") && e.contains("fault_in"), "{e}");
    assert_eq!(fake.requests("GET"), gets, "a refused read still reached the store");
    let _ = std::fs::remove_dir_all(&d);
}

/// A compaction that retires an archived segment deletes its object: nothing
/// else ever would, since the store is never listed.
#[test]
fn a_retired_archived_segment_is_deleted_from_the_store() {
    let fake = FakeS3::start();
    let d = dir("retire");
    let mut db = open(&d, opts(&fake)).unwrap();
    setup(&mut db, 120);
    archive_all(&mut db);
    let before = fake.keys();
    assert_eq!(before.len(), 1, "{before:?}");
    // Enough local segments beside the archived one for a compaction to plan
    // a merge at all: one segment alone is below the fan-out.
    for round in 1..=3 {
        for i in round * 120..(round + 1) * 120 {
            db.insert("items", doc(i)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
    }
    db.execute("COMPACT items").unwrap();
    assert!(
        db.shards("items").unwrap()[0].segments.len() < 4,
        "nothing was compacted, so this says nothing about retirement"
    );
    let after = fake.keys();
    assert!(!after.contains(&before[0]), "the retired segment's object was left behind: {after:?}");
    let _ = std::fs::remove_dir_all(&d);
}

/// Dropping a collection deletes the objects its archived segments put in
/// the store, and deletes them while the shards can still name them: a
/// store is the one place a later open cannot sweep from a directory.
#[test]
fn dropping_a_collection_deletes_its_objects_from_the_store() {
    let fake = FakeS3::start();
    let d = dir("drop");
    let mut db = open(&d, opts(&fake)).unwrap();
    setup(&mut db, 120);
    archive_all(&mut db);
    assert_eq!(fake.keys().len(), 1, "{:?}", fake.keys());
    db.execute("DROP COLLECTION items").unwrap();
    assert!(fake.keys().is_empty(), "the object was left behind: {:?}", fake.keys());
    assert!(!d.join("collections").join("items").exists());
    let _ = std::fs::remove_dir_all(&d);
}

/// The credentials come from the environment and from nowhere else: with
/// an endpoint configured and no key in the environment, the open is
/// refused naming the variable, and nothing about the store is in the
/// catalog on disk.
#[test]
fn credentials_come_from_the_environment_and_are_never_written_down() {
    let fake = FakeS3::start();
    let d = dir("creds");
    {
        let _g = ENV.lock().unwrap();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let e = Db::open(&d, opts(&fake))
            .err()
            .map(|e| e.to_string())
            .expect("opened with no credentials");
        assert!(e.contains("AWS_ACCESS_KEY_ID"), "{e}");
    }
    let mut db = open(&d, opts(&fake)).unwrap();
    setup(&mut db, 60);
    archive_all(&mut db);
    db.persist().unwrap();
    drop(db);
    let catalog = std::fs::read(d.join("CATALOG")).unwrap();
    let text = String::from_utf8_lossy(&catalog);
    assert!(
        !text.contains("fake-secret") && !text.contains(ACCESS_KEY) && !text.contains(&fake.addr),
        "the catalog carries the store's configuration"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// The client refuses an https endpoint rather than sending a request that
/// could not be one: there is no TLS here, and the message says what to put
/// in front of the bucket instead.
#[test]
fn an_https_endpoint_is_refused_with_the_reason() {
    let mut o = DbOpts::default();
    o.archive.endpoint = Some("https://bucket.example".into());
    o.archive.bucket = "b".into();
    let _g = ENV.lock().unwrap();
    std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "fake-secret");
    let e = Db::open(&dir("https"), o).err().map(|e| e.to_string()).unwrap();
    assert!(e.contains("plain http") && e.contains("TLS"), "{e}");
}
