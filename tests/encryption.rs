//! Encryption at rest: a database opened with a master key writes nothing
//! readable under its directory, its archive or its backups, opens again
//! under the same master and under no other, and its copies -- exports,
//! backups, moves -- carry the data key with them or refuse.

use celastro::lock::RwLock;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::Value;

/// A string no compression or encoding hides: what the grep below looks for.
const MARKER: &str = "xyzzy-plaintext-marker-";

static ENV: Mutex<()> = Mutex::new(());
const TOKEN: &str = "encryption-test-token";

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-enc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn master(seed: u8) -> [u8; 32] {
    let mut m = [seed; 32];
    for (i, b) in m.iter_mut().enumerate() {
        *b ^= i as u8;
    }
    m
}

fn opts(master: Option<[u8; 32]>) -> DbOpts {
    let mut o = DbOpts::default();
    o.master_key = master.map(Into::into);
    o
}

fn doc(i: usize) -> Value {
    let words = ["graph", "search", "vector", "index", "segment", "fusion", "rank"];
    celastro::json::parse(&format!(
        r#"{{"id":"doc-{i:03}","tenant":"t{}","n":{i},"body":"{MARKER}{} {}","embedding":[{},{},{},1.0]}}"#,
        i % 3,
        words[i % 7],
        words[(i * 3) % 7],
        (i % 7) as f32 / 7.0,
        (i % 5) as f32 / 5.0,
        (i % 3) as f32 / 3.0,
    ))
    .unwrap()
}

const CREATE: &str = "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT)";
const INDEXES: &[&str] = &[
    "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
    "CREATE INDEX items_emb ON items USING vector (embedding) WITH (dims = 4, metric = 'cosine')",
    "CREATE INDEX items_tenant ON items USING secondary (tenant)",
];
const QUERY: &str = "SELECT id FROM items WHERE text_match(body, 'segment') LIMIT 100";
const VECTOR: &str = "SELECT id FROM items ORDER BY embedding <=> [0.1, 0.9, 0.2, 1.0] LIMIT 7";

/// A collection with every kind of index, sealed twice with a delete in
/// between, then rows left in memory: segments, delete logs, a manifest and
/// a WAL with something in it.
fn setup(db: &mut Db, n: usize) {
    db.execute(CREATE).unwrap();
    for i in INDEXES {
        db.execute(i).unwrap();
    }
    for i in 0..n {
        db.insert("items", doc(i)).unwrap();
        if i == n / 2 {
            db.execute("FLUSH items").unwrap();
        }
    }
    db.execute("FLUSH items").unwrap();
    db.execute("DELETE FROM items WHERE id = 'doc-001'").unwrap();
    for i in n..n + 5 {
        db.insert("items", doc(i)).unwrap();
    }
}

fn ids(db: &mut Db, sql: &str) -> Vec<String> {
    let r = db.query(sql).unwrap();
    let mut out: Vec<String> = r.rows.iter().map(|row| format!("{:?}", row.key)).collect();
    out.sort();
    out
}

fn ack(db: &mut Db, sql: &str) -> String {
    match db.execute(sql).unwrap().finished().unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{sql}: {other:?}"),
    }
}

/// Every file under `root` that holds the marker, as paths: the empty list
/// is the property.
fn leaks(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(root) else { return out };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(leaks(&p));
        } else if let Ok(b) = std::fs::read(&p) {
            if b.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()) {
                out.push(p);
            }
        }
    }
    out
}

fn files(root: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(root) else { return 0 };
    rd.flatten().map(|e| if e.path().is_dir() { files(&e.path()) } else { 1 }).sum()
}

#[test]
fn an_encrypted_database_holds_no_plaintext_and_opens_under_its_master_only() {
    let d = dir("holds");
    let store = dir("holds-store");
    let backups = dir("holds-backups");
    let export = dir("holds-export");
    let mut o = opts(Some(master(1)));
    o.archive.dir = Some(store.clone());
    o.archive.prefix = "celastro/".into();
    let mut db = Db::open(&d, o.clone()).unwrap();
    setup(&mut db, 60);
    let text = ids(&mut db, QUERY);
    let vec = ids(&mut db, VECTOR);
    assert!(!text.is_empty() && vec.len() == 7);
    assert!(d.join("KEY").exists(), "the wrapped data key is written at the first open");
    // A compaction under the cipher: the merged segment is written framed
    // and read back through the ranged reader.
    ack(&mut db, "COMPACT items");
    assert_eq!(ids(&mut db, QUERY), text);
    // The archived tier: the object in the store is the framed file, and a
    // query faults it in by ranged reads that decrypt frame by frame.
    for i in ["items_body", "items_emb", "items_tenant"] {
        ack(&mut db, &format!("ALTER INDEX {i} ON items SET TIER 'archived'"));
    }
    assert!(files(&store) > 0, "the segments went to the store");
    assert_eq!(leaks(&store), Vec::<PathBuf>::new(), "plaintext in the store");
    assert_eq!(ids(&mut db, QUERY), text);
    assert_eq!(ids(&mut db, VECTOR), vec);
    // Brought back: fetched from the store into `segments/`, framed still.
    ack(&mut db, "ALTER INDEX items_body ON items SET TIER 'active'");
    assert_eq!(ids(&mut db, QUERY), text);
    // A backup and an export: copies of the files, so framed as they lie.
    ack(&mut db, &format!("BACKUP TO '{}'", backups.display()));
    db.export_collection("items").unwrap().write_to(&export).unwrap();
    assert!(export.join("KEY").exists(), "an encrypted export carries the wrapped key");
    drop(db);

    for root in [&d, &backups, &export] {
        assert!(files(root) > 0, "{} has nothing in it", root.display());
        assert_eq!(leaks(root), Vec::<PathBuf>::new(), "plaintext under {}", root.display());
    }

    // Reopen: the WAL's rows, the sealed rows and the delete all come back.
    let mut db = Db::open(&d, o.clone()).unwrap();
    assert_eq!(ids(&mut db, QUERY), text);
    assert_eq!(ids(&mut db, VECTOR), vec);
    assert!(!ids(&mut db, "SELECT id FROM items LIMIT 100").contains(&"\"doc-001\"".to_string()));
    db.insert("items", doc(500)).unwrap();
    drop(db);

    // Without the master: refused, with the reason. Under another: refused.
    let e = Db::open(&d, opts(None)).err().expect("refused").to_string();
    assert!(e.contains("encrypted") && e.contains("CELASTRO_MASTER_KEY"), "{e}");
    let e = Db::open(&d, opts(Some(master(2)))).err().expect("refused").to_string();
    assert!(e.contains("master key") || e.contains("KEY"), "{e}");
    // And the right one still opens after those refusals touched nothing.
    let mut db = Db::open(&d, o).unwrap();
    assert!(ids(&mut db, "SELECT id FROM items LIMIT 200").contains(&"\"doc-500\"".to_string()));
    drop(db);
    for p in [&d, &store, &backups, &export] {
        let _ = std::fs::remove_dir_all(p);
    }
}

#[test]
fn a_plain_database_with_data_is_not_encrypted_in_place() {
    let d = dir("inplace");
    let mut db = Db::open(&d, opts(None)).unwrap();
    setup(&mut db, 10);
    drop(db);
    let e = Db::open(&d, opts(Some(master(1)))).err().expect("refused").to_string();
    assert!(e.contains("plain data") && e.contains("export"), "{e}");
    // An empty plain directory takes a key: nothing was written under none.
    let empty = dir("inplace-empty");
    drop(Db::open(&empty, opts(None)).unwrap());
    let mut db = Db::open(&empty, opts(Some(master(1)))).unwrap();
    setup(&mut db, 5);
    assert_eq!(leaks(&empty), Vec::<PathBuf>::new());
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&empty);
}

#[test]
fn a_torn_wal_tail_stops_the_replay_where_the_last_whole_record_ended() {
    let d = dir("torn");
    let o = opts(Some(master(3)));
    let mut db = Db::open(&d, o.clone()).unwrap();
    db.execute(CREATE).unwrap();
    for i in 0..20 {
        db.insert("items", doc(i)).unwrap();
    }
    drop(db);
    let wal = d.join("collections/items/shard-0000/wal.log");
    let bytes = std::fs::read(&wal).unwrap();
    assert!(bytes.len() > 100);
    std::fs::write(&wal, &bytes[..bytes.len() - 7]).unwrap();
    let mut db = Db::open(&d, o).unwrap();
    let n = ids(&mut db, "SELECT id FROM items LIMIT 100").len();
    assert_eq!(n, 19, "the torn last record is gone, every whole one is there");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn a_backup_restores_under_the_same_master_and_is_refused_without_it() {
    let src = dir("restore-src");
    let backups = dir("restore-backups");
    let o = opts(Some(master(4)));
    let mut db = Db::open(&src, o.clone()).unwrap();
    setup(&mut db, 40);
    let before = ids(&mut db, "SELECT id FROM items LIMIT 200");
    ack(&mut db, &format!("BACKUP TO '{}'", backups.display()));
    drop(db);
    assert_eq!(leaks(&backups), Vec::<PathBuf>::new());

    // Into a fresh encrypted database under the same master: the backup's
    // KEY replaces the one made at open, and every file opens under it.
    let dst = dir("restore-dst");
    let mut db = Db::open(&dst, o.clone()).unwrap();
    let key_at_open = std::fs::read(dst.join("KEY")).unwrap();
    let m = ack(&mut db, &format!("RESTORE FROM '{}'", backups.display()));
    assert!(m.contains("restored backup"), "{m}");
    assert_ne!(std::fs::read(dst.join("KEY")).unwrap(), key_at_open, "the backup's key is adopted");
    assert_eq!(ids(&mut db, "SELECT id FROM items LIMIT 200"), before);
    assert!(!ids(&mut db, QUERY).is_empty());
    drop(db);
    let mut db = Db::open(&dst, o.clone()).unwrap();
    assert_eq!(ids(&mut db, "SELECT id FROM items LIMIT 200"), before);
    assert_eq!(leaks(&dst), Vec::<PathBuf>::new());
    drop(db);

    // Into a plain database: refused, and nothing adopted.
    let plain = dir("restore-plain");
    let mut db = Db::open(&plain, opts(None)).unwrap();
    let e = db.execute(&format!("RESTORE FROM '{}'", backups.display())).unwrap_err().to_string();
    assert!(e.contains("encrypted") && e.contains("CELASTRO_MASTER_KEY"), "{e}");
    assert!(db.execute("SHOW CATALOG items").is_err());
    drop(db);
    // Under another master: refused.
    let other = dir("restore-other");
    let mut db = Db::open(&other, opts(Some(master(5)))).unwrap();
    let e = db.execute(&format!("RESTORE FROM '{}'", backups.display())).unwrap_err().to_string();
    assert!(e.contains("does not open under this master key"), "{e}");
    drop(db);
    for p in [&src, &backups, &dst, &plain, &other] {
        let _ = std::fs::remove_dir_all(p);
    }
}

#[test]
fn an_import_crosses_key_regimes_which_is_how_a_database_takes_or_changes_a_key() {
    let plain_dir = dir("import-plain");
    let mut plain = Db::open(&plain_dir, opts(None)).unwrap();
    setup(&mut plain, 30);
    let want = ids(&mut plain, "SELECT id FROM items LIMIT 200");
    let plain_export = dir("import-plain-export");
    plain.export_collection("items").unwrap().write_to(&plain_export).unwrap();
    assert!(!plain_export.join("KEY").exists());
    drop(plain);

    // Plain export into an encrypted database: every file is sealed on the
    // way in, and the marker is nowhere under the directory.
    let enc_dir = dir("import-enc");
    let o = opts(Some(master(6)));
    let mut enc = Db::open(&enc_dir, o.clone()).unwrap();
    enc.import_collection(&plain_export).unwrap();
    assert_eq!(ids(&mut enc, "SELECT id FROM items LIMIT 200"), want);
    assert!(!ids(&mut enc, QUERY).is_empty());
    assert_eq!(leaks(&enc_dir), Vec::<PathBuf>::new());
    let enc_export = dir("import-enc-export");
    enc.export_collection("items").unwrap().write_to(&enc_export).unwrap();
    assert_eq!(leaks(&enc_export), Vec::<PathBuf>::new());
    drop(enc);
    let mut enc = Db::open(&enc_dir, o.clone()).unwrap();
    assert_eq!(ids(&mut enc, "SELECT id FROM items LIMIT 200"), want);
    drop(enc);

    // Encrypted export into a plain database: refused, since a plain
    // database has no master to open it with. Dropping a key is not a
    // path; taking one and changing one are.
    let back_dir = dir("import-back");
    let mut back = Db::open(&back_dir, opts(None)).unwrap();
    let e = back.import_collection(&enc_export).unwrap_err().to_string();
    assert!(e.contains("encrypted export"), "{e}");
    drop(back);

    // Encrypted export into a database under another data key, both
    // wrapped by the same master: recoded from one key to the other.
    let other_dir = dir("import-other");
    let mut other = Db::open(&other_dir, o.clone()).unwrap();
    assert_ne!(
        std::fs::read(other_dir.join("KEY")).unwrap(),
        std::fs::read(enc_dir.join("KEY")).unwrap()
    );
    other.import_collection(&enc_export).unwrap();
    assert_eq!(ids(&mut other, "SELECT id FROM items LIMIT 200"), want);
    assert_eq!(leaks(&other_dir), Vec::<PathBuf>::new());
    drop(other);
    for p in [&plain_dir, &plain_export, &enc_dir, &enc_export, &back_dir, &other_dir] {
        let _ = std::fs::remove_dir_all(p);
    }
}

#[test]
fn a_key_file_makes_the_nodes_of_a_cluster_share_one_data_key_so_a_shard_moves() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    // The wrapped data key every node starts with, as `key init` writes it.
    let m = master(7);
    let key_file = dir("cluster-keys").join("KEY");
    std::fs::create_dir_all(key_file.parent().unwrap()).unwrap();
    let wrapped = celastro::cipher::Cipher::generate().unwrap().wrap(&m).unwrap();
    std::fs::write(&key_file, &wrapped).unwrap();

    struct Node {
        url: String,
        db: Arc<RwLock<Db>>,
        stop: Arc<AtomicBool>,
        dir: PathBuf,
    }
    let start = |tag: &str, key_file: Option<PathBuf>| {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let url = format!("tcp://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let dir = dir(tag);
        let mut o = opts(Some(m));
        o.key_file = key_file;
        o.node = Some(url.clone());
        let db = Arc::new(RwLock::new(Db::open(&dir, o).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Node { url, db, stop, dir }
    };
    let a = start("cluster-a", Some(key_file.clone()));
    let b = start("cluster-b", Some(key_file.clone()));
    assert_eq!(std::fs::read(a.dir.join("KEY")).unwrap(), wrapped);
    assert_eq!(std::fs::read(b.dir.join("KEY")).unwrap(), wrapped);
    // The lock let go before the deferred work runs: a move's copy holds
    // nothing, and a target's switch back here needs this lock.
    let exec = |n: &Node, sql: &str| {
        let out = n.db.write().unwrap().execute(sql).unwrap();
        match out.finished().unwrap() {
            Outcome::Ack(m) => m,
            other => panic!("{sql}: {other:?}"),
        }
    };
    exec(&a, &format!("ATTACH NODE '{}'", b.url));
    exec(
        &a,
        "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) \
         PARTITION BY (tenant) WITH (splits = ['t1', 't2'])",
    );
    exec(&a, INDEXES[0]);
    for i in 0..60usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
        if i == 30 {
            exec(&a, "FLUSH items");
        }
    }
    let want = ids(&mut a.db.write().unwrap(), "SELECT id FROM items LIMIT 200");
    let text = ids(&mut a.db.write().unwrap(), QUERY);
    // The shards were spread over both nodes at creation; one that is here
    // goes there, framed as it lies, and opens there under the shared key.
    let here: Vec<usize> =
        a.db.write().unwrap().shards("items").unwrap().iter().map(|s| s.index).collect();
    assert!(!here.is_empty());
    let m1 = exec(&a, &format!("MOVE SHARD {} OF items TO '{}'", here[0], b.url));
    assert!(m1.contains("moved"), "{m1}");
    assert_eq!(ids(&mut a.db.write().unwrap(), "SELECT id FROM items LIMIT 200"), want);
    assert_eq!(ids(&mut b.db.write().unwrap(), "SELECT id FROM items LIMIT 200"), want);
    assert_eq!(ids(&mut b.db.write().unwrap(), QUERY), text);
    assert_eq!(leaks(&a.dir), Vec::<PathBuf>::new());
    assert_eq!(leaks(&b.dir), Vec::<PathBuf>::new());
    a.stop.store(true, Ordering::Relaxed);
    b.stop.store(true, Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(700));
    for p in [&a.dir, &b.dir, key_file.parent().unwrap()] {
        let _ = std::fs::remove_dir_all(p);
    }
}
