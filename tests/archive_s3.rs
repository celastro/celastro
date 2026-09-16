//! The `archived` tier against an S3-compatible object store, with the store
//! in-process: a small HTTP server that speaks the four operations the
//! client uses, checks that every request is signed, and remembers what it
//! holds, so these tests need no MinIO and prove the wire.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::residency::ArchivedAccess;
use celastro::tls::Tls;
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

/// A CA and a `localhost` certificate it signed, made once per process by
/// the crate's own generator, for the https fake.
fn fixture(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("celastro-s3-tls-{}", std::process::id()));
    if !dir.join("ca.crt").exists() {
        std::fs::create_dir_all(&dir).unwrap();
        let m = celastro::tls::make_material(
            "localhost",
            &["localhost".to_string()],
            &["127.0.0.1".parse().unwrap()],
            30,
        )
        .unwrap();
        std::fs::write(dir.join("ca.crt"), m.ca_cert).unwrap();
        std::fs::write(dir.join("localhost.crt"), m.cert).unwrap();
        std::fs::write(dir.join("localhost.key"), m.key).unwrap();
        let other = celastro::tls::make_material("localhost", &[], &[], 30).unwrap();
        std::fs::write(dir.join("other-ca.crt"), other.ca_cert).unwrap();
    }
    dir.join(name).display().to_string()
}

impl FakeS3 {
    fn start() -> FakeS3 {
        FakeS3::start_with(None)
    }

    /// The same fake behind TLS, serving the fixture's `localhost` certificate.
    fn start_tls() -> FakeS3 {
        let _g = ENV.lock().unwrap_or_else(|p| p.into_inner());
        std::env::set_var("CELASTRO_TLS_CERT", fixture("localhost.crt"));
        std::env::set_var("CELASTRO_TLS_KEY", fixture("localhost.key"));
        std::env::set_var("CELASTRO_TLS_CA", fixture("ca.crt"));
        let tls = Arc::new(Tls::from_env().unwrap().unwrap());
        std::env::remove_var("CELASTRO_TLS_CERT");
        std::env::remove_var("CELASTRO_TLS_KEY");
        std::env::remove_var("CELASTRO_TLS_CA");
        FakeS3::start_with(Some(tls))
    }

    fn start_with(tls: Option<Arc<Tls>>) -> FakeS3 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let objects: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::default();
        let requests: Arc<Mutex<Vec<String>>> = Arc::default();
        let (o, r) = (objects.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (o, r) = (o.clone(), r.clone());
                let Ok(stream) = celastro::tls::accept(tls.as_ref(), stream) else { continue };
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
    mut s: Box<dyn celastro::tls::Stream>,
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
    let reply = |s: &mut dyn Write, code: u16, reason: &str, body: &[u8], len_only: bool| {
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
    // `ListObjectsV2`: `GET /b/?list-type=2&prefix=...`, one page.
    if method == "GET" && key.starts_with('?') {
        let prefix = key[1..]
            .split('&')
            .find_map(|kv| kv.strip_prefix("prefix="))
            .map(percent_decode)
            .unwrap_or_default();
        let mut keys: Vec<&String> = objects.keys().filter(|k| k.starts_with(&prefix)).collect();
        keys.sort();
        let mut xml = String::from("<ListBucketResult><IsTruncated>false</IsTruncated>");
        for k in keys {
            xml.push_str(&format!("<Contents><Key>{}</Key></Contents>", k.replace('&', "&amp;")));
        }
        xml.push_str("</ListBucketResult>");
        reply(&mut s, 200, "OK", xml.as_bytes(), false);
        return;
    }
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

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 <= b.len() - 1 + 1 {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
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
fn the_archived_tier_over_https_verifies_the_store_by_the_ca_it_is_given() {
    let fake = FakeS3::start_tls();
    let port = fake.addr.rsplit(':').next().unwrap();
    let mut o = DbOpts::default();
    o.archive.endpoint = Some(format!("https://localhost:{port}"));
    o.archive.bucket = "b".into();
    o.archive.prefix = "celastro/".into();
    o.archive.region = "us-east-1".into();
    o.archive.ca = Some(fixture("ca.crt").into());
    let d = dir("https");
    let mut db = open(&d, o.clone()).unwrap();
    setup(&mut db, 120);
    archive_all(&mut db);
    assert!(!fake.keys().is_empty(), "the objects went over TLS: {:?}", fake.keys());
    let rows = db.query("SELECT id FROM items WHERE text_match(body, 'number') LIMIT 200").unwrap();
    assert_eq!(rows.rows.len(), 120, "ranged reads over TLS answer the query");
    drop(db);
    let mut db = open(&d, o.clone()).unwrap();
    let rows = db.query("SELECT id FROM items WHERE kind = 'note' LIMIT 200").unwrap();
    assert!(!rows.rows.is_empty());
    drop(db);
    // Another CA: the chain does not reach it, and the first request says so.
    let mut wrong = o.clone();
    wrong.archive.ca = Some(fixture("other-ca.crt").into());
    let d2 = dir("https-wrong");
    // A segment moves once every index on it is archived, so the third
    // ALTER is the one that reaches the store.
    let first_error = |db: &mut Db| -> String {
        for i in ["items_body", "items_emb", "items_kind"] {
            if let Err(e) = db.execute(&format!("ALTER INDEX {i} ON items SET TIER 'archived'")) {
                return e.to_string();
            }
        }
        String::new()
    };
    let mut db = open(&d2, wrong).unwrap();
    setup(&mut db, 20);
    let e = first_error(&mut db);
    assert!(e.contains("does not reach"), "{e}");
    assert!(
        std::fs::read_dir(d2.join("collections/items/shard-0000/segments")).unwrap().count() > 0
    );
    // No CA named: the system's bundle if there is one (which does not hold
    // this CA), or the variable is named.
    let mut none = o.clone();
    none.archive.ca = None;
    let d3 = dir("https-none");
    match open(&d3, none) {
        Ok(mut db) => {
            setup(&mut db, 5);
            let e = first_error(&mut db);
            assert!(e.contains("does not reach"), "{e}");
        }
        Err(e) => assert!(e.to_string().contains("CELASTRO_ARCHIVE_CA"), "{e}"),
    }
    for p in [&d, &d2, &d3] {
        let _ = std::fs::remove_dir_all(p);
    }
}

#[test]
fn a_bad_endpoint_is_refused_with_the_reason() {
    let mut o = DbOpts::default();
    o.archive.endpoint = Some("https://".into());
    o.archive.bucket = "b".into();
    let _g = ENV.lock().unwrap();
    std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "fake-secret");
    let e = Db::open(&dir("https-bad"), o).err().map(|e| e.to_string()).unwrap();
    assert!(e.contains("names no host"), "{e}");
}

/// The same tier over a directory: `CELASTRO_ARCHIVE_DIR` names it, the
/// objects are files under the same keys, and a database with nothing
/// local reopens from them. An NFS mount is a directory to the binary.
#[test]
fn the_archived_tier_on_a_directory_store_holds_the_segments_and_reopens_from_them() {
    let store = dir("dirstore");
    let d = dir("dirdb");
    let mut o = DbOpts::default();
    o.archive.dir = Some(store.clone());
    o.archive.prefix = "celastro/".into();
    let mut db = Db::open(&d, o.clone()).unwrap();
    setup(&mut db, 60);
    let segments = d.join("collections").join("items").join("shard-0000").join("segments");
    assert!(count(&segments) >= 1);
    archive_all(&mut db);
    assert_eq!(count(&segments), 0, "the local files went to the store");
    let objects = celastro::objstore::DirStore::new(&store).unwrap();
    let keys =
        celastro::objstore::ObjectStore::list(&objects, "celastro/items/shard-0000/").unwrap();
    assert!(!keys.is_empty(), "{keys:?}");
    drop(db);
    let mut db = Db::open(&d, o).unwrap();
    let rows = db.query("SELECT id FROM items WHERE text_match(body, 'number') LIMIT 100").unwrap();
    assert_eq!(rows.rows.len(), 60);
    let _ = std::fs::remove_dir_all(&store);
    let _ = std::fs::remove_dir_all(&d);
}

/// A backup to `s3://` goes through the archive's endpoint and credentials
/// with the bucket the destination names; a restore from it lists what is
/// there through `ListObjectsV2` when asked for an instant it lacks.
#[test]
fn a_backup_to_a_bucket_restores_from_it() {
    let fake = FakeS3::start();
    let d = dir("s3backup-src");
    let mut db = open(&d, opts(&fake)).unwrap();
    setup(&mut db, 40);
    let ack = match db.execute("BACKUP TO 's3://b/nightly'").unwrap().finished().unwrap() {
        celastro::engine::Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    };
    assert!(ack.starts_with("backup "), "{ack}");
    let ts: u64 = ack.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(
        fake.keys().iter().any(|k| k == &format!("nightly/nodes/local/backups/{ts:020}/BACKUP")),
        "{:?}",
        fake.keys()
    );
    assert!(fake.keys().iter().any(|k| k.starts_with("nightly/pool/items/shard-0000/")));
    let e = db.execute("RESTORE FROM 's3://b/nightly' AS OF 7").unwrap_err().to_string();
    assert!(e.contains("empty database"), "{e}");
    let d2 = dir("s3backup-dst");
    let mut db2 = open(&d2, opts(&fake)).unwrap();
    let e = db2.execute("RESTORE FROM 's3://b/nightly' AS OF 7").unwrap_err().to_string();
    assert!(e.contains(&format!("complete backups there: {ts}")), "{e}");
    let ack = match db2.execute("RESTORE FROM 's3://b/nightly'").unwrap() {
        celastro::engine::Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    };
    assert!(ack.contains("1 collection(s), 1 shard(s)"), "{ack}");
    let rows =
        db2.query("SELECT id FROM items WHERE text_match(body, 'number') LIMIT 100").unwrap();
    assert_eq!(rows.rows.len(), 40);
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&d2);
}
