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

/// A segment is sealed under its collection: moved to the same-index
/// shard of another collection it does not open, and the directory does
/// not open with it there. Before 0.84.0 the identity was the shard and
/// the file alone, and the moved segment opened as the other's.
#[test]
fn a_segment_moved_to_another_collections_shard_is_refused() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let d = dir("swap");
    let mut db = Db::open(&d, opts(Some(master(1)))).unwrap();
    setup(&mut db, 30);
    ack(&mut db, "CREATE COLLECTION other (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT)");
    for i in 0..30 {
        db.insert("other", doc(i)).unwrap();
    }
    ack(&mut db, "FLUSH other");
    drop(db);
    let seg_of = |coll: &str| -> PathBuf {
        let segs = d.join("collections").join(coll).join("shard-0000").join("segments");
        let mut names: Vec<PathBuf> = std::fs::read_dir(&segs)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .collect();
        names.sort();
        names.remove(0)
    };
    let (a, b) = (seg_of("items"), seg_of("other"));
    // The same segment number on both sides, so the swap is a swap of
    // identity alone; if the numbers differ, `other`'s file is put
    // under `items`'s first name and vice versa.
    let (ab, bb) = (std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap());
    std::fs::write(&a, &bb).unwrap();
    std::fs::write(&b, &ab).unwrap();
    let e = match Db::open(&d, opts(Some(master(1)))) {
        Ok(mut db) => {
            // Opened lazily: the first read of the moved segment refuses it.
            let r = db.query("SELECT id FROM items LIMIT 100");
            let r2 = db.query("SELECT id FROM other LIMIT 100");
            format!("{:?} {:?}", r.err(), r2.err())
        }
        Err(e) => e.to_string(),
    };
    assert!(e.contains("does not authenticate"), "a moved segment must be refused: {e}");
    let _ = std::fs::remove_dir_all(&d);
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

/// `celastro check` on a directory in the clear: every segment's regions,
/// the manifests and the catalog against their checksums, every log
/// record against its CRC -- and a byte flipped in a segment, a manifest
/// with a byte changed and a log cut mid-record are each named, in one
/// run, with the rest still counted.
#[test]
fn a_plain_directory_is_checked_and_every_damaged_file_is_named() {
    let d = dir("plain-check");
    let mut db = Db::open(&d, opts(None)).unwrap();
    setup(&mut db, 10);
    drop(db);
    let w = celastro::shard::check_plain_dir(&d).unwrap();
    assert!(w.failures.is_empty(), "{:?}", w.failures);
    assert!(w.files >= 4 && w.records >= 5, "{} file(s), {} record(s)", w.files, w.records);
    let shard = d.join("collections/items/shard-0000");
    let seg = std::fs::read_dir(shard.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "seg"))
        .expect("the flush wrote a segment");
    let mut bytes = std::fs::read(&seg).unwrap();
    let at = bytes.len() / 3;
    bytes[at] ^= 0x55;
    std::fs::write(&seg, &bytes).unwrap();
    let manifest = shard.join("MANIFEST");
    let mut bytes = std::fs::read(&manifest).unwrap();
    bytes[2] ^= 0x01;
    std::fs::write(&manifest, &bytes).unwrap();
    let wal = shard.join("wal.log");
    let bytes = std::fs::read(&wal).unwrap();
    std::fs::write(&wal, &bytes[..bytes.len() - 7]).unwrap();
    let w = celastro::shard::check_plain_dir(&d).unwrap();
    let named =
        |what: &str, why: &str| w.failures.iter().any(|f| f.contains(what) && f.contains(why));
    assert!(named(&seg.display().to_string(), "checksum"), "{:?}", w.failures);
    assert!(named("MANIFEST", "checksum mismatch"), "{:?}", w.failures);
    assert!(named("wal.log", "torn at byte"), "{:?}", w.failures);
    assert_eq!(w.failures.len(), 3, "{:?}", w.failures);
    assert!(w.files >= 2, "the files that check are still counted");
    let _ = std::fs::remove_dir_all(&d);
}

/// `BACKUP ... KEEP 1` on an encrypted database keeps the backup it made
/// restorable: the sweep opens the sealed records (until 0.97.0 it read
/// them raw, found no pool key named, and removed every segment of every
/// backup at the destination).
#[test]
fn a_keep_on_an_encrypted_database_leaves_its_backup_restorable() {
    let d = dir("keep");
    let dest = dir("keep-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let mut db = Db::open(&d, opts(Some(master(11)))).unwrap();
    setup(&mut db, 40);
    let first = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    db.insert("items", doc(100)).unwrap();
    ack(&mut db, "FLUSH items");
    let m = ack(&mut db, &format!("BACKUP TO '{}' KEEP 1", dest.display()));
    assert!(m.contains("kept 1, removed 1 older backup(s)"), "{m}");
    let ts: u64 = m.split_whitespace().nth(1).unwrap().parse().unwrap();
    let _ = first;
    drop(db);
    let fresh = dir("keep-fresh");
    let mut r = Db::open(&fresh, opts(Some(master(11)))).unwrap();
    let v = ack(&mut r, &format!("VERIFY BACKUP '{}' AS OF {ts}", dest.display()));
    assert!(v.contains("every one as recorded"), "{v}");
    let m = ack(&mut r, &format!("RESTORE FROM '{}' AS OF {ts}", dest.display()));
    assert!(m.contains("restored"), "{m}");
    assert_eq!(ids(&mut r, "SELECT id FROM items LIMIT 100").len(), 45);
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&dest);
    let _ = std::fs::remove_dir_all(&fresh);
}

/// A destination shared by two nodes under different data keys: a KEEP
/// from one cannot open the other's record, sweeps nothing of the pool,
/// and says so by name -- until 0.98.0 it was skipped in silence.
#[test]
fn a_keep_names_the_record_it_cannot_open() {
    let (da, db_, dest) = (dir("keep-two-a"), dir("keep-two-b"), dir("keep-two-dest"));
    std::fs::create_dir_all(&dest).unwrap();
    let node = |o: DbOpts, n: &str| {
        let mut o = o;
        o.node = Some(n.to_string());
        o
    };
    let mut a = Db::open(&da, node(opts(Some(master(21))), "tcp://a.example:1")).unwrap();
    setup(&mut a, 10);
    let mut b = Db::open(&db_, node(opts(Some(master(22))), "tcp://b.example:1")).unwrap();
    setup(&mut b, 10);
    ack(&mut b, &format!("BACKUP TO '{}'", dest.display()));
    ack(&mut a, &format!("BACKUP TO '{}'", dest.display()));
    a.insert("items", doc(100)).unwrap();
    ack(&mut a, "FLUSH items");
    let m = ack(&mut a, &format!("BACKUP TO '{}' KEEP 1", dest.display()));
    assert!(m.contains("removed 1 older backup(s)"), "{m}");
    assert!(m.contains("the pool was not swept") && m.contains("nodes/b.example_1/"), "{m}");
    drop(a);
    drop(b);
    for d in [&da, &db_, &dest] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A backup before a rotation and one after, at one destination, each
/// restore whole: the pool names an object by the key's fingerprint, so
/// the resealed segment is a new object and the old backup's is still
/// the old one (until 0.97.0 the size alone said "present").
#[test]
fn backups_across_a_key_rotation_each_restore_whole() {
    let d = dir("rotate-backup");
    let dest = dir("rotate-backup-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let mut db = Db::open(&d, opts(Some(master(12)))).unwrap();
    setup(&mut db, 30);
    let m = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    let before: u64 = m.split_whitespace().nth(1).unwrap().parse().unwrap();
    drop(db);
    let w = celastro::cipher::rotate_data_key(&d, &master(12), &[]).unwrap();
    assert!(w.files > 0);
    // And the ring retired: the second backup's KEY holds the new key
    // alone, so its restore opens nothing under the old one.
    assert_eq!(celastro::cipher::retire_keys(&d, &master(12), &[]).unwrap(), 1);
    let mut db = Db::open(&d, opts(Some(master(12)))).unwrap();
    db.insert("items", doc(200)).unwrap();
    ack(&mut db, "FLUSH items");
    let m = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    let after: u64 = m.split_whitespace().nth(1).unwrap().parse().unwrap();
    drop(db);
    // `setup(30)`: thirty rows, one deleted, five more; then one after.
    for (ts, want) in [(before, 34), (after, 35)] {
        let fresh = dir(&format!("rotate-backup-fresh-{ts}"));
        let mut r = Db::open(&fresh, opts(Some(master(12)))).unwrap();
        let v = ack(&mut r, &format!("VERIFY BACKUP '{}' AS OF {ts}", dest.display()));
        assert!(v.contains("every one as recorded"), "{v}");
        let m = ack(&mut r, &format!("RESTORE FROM '{}' AS OF {ts}", dest.display()));
        assert!(m.contains("restored"), "{m}");
        assert_eq!(ids(&mut r, "SELECT id FROM items LIMIT 100").len(), want, "{m}");
        let _ = std::fs::remove_dir_all(&fresh);
    }
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&dest);
}

/// A backup names a pool object by the key its bytes are under. A segment
/// at the archived tier stays under the old key after `key rotate` until
/// `key reseal`, and the reseal rewrites the tier's object in place at its
/// length: named by the current key (0.97.0, 0.98.0), the pool held the
/// old-key bytes under the new key's name, every backup after the reseal
/// found it present at its size and recorded the new bytes' hash, and
/// `VERIFY BACKUP` and `RESTORE` refused them with "does not match its
/// recorded checksum" for as long as the segment lived.
#[test]
fn backups_across_a_reseal_of_the_archived_tier_each_restore_whole() {
    let d = dir("reseal");
    let store = dir("reseal-store");
    let dest = dir("reseal-dest");
    let scratch = dir("reseal-scratch");
    std::fs::create_dir_all(&dest).unwrap();
    let m = master(14);
    let with_store = || {
        let mut o = opts(Some(m));
        o.archive.dir = Some(store.clone());
        o.archive.prefix = "celastro/".into();
        o
    };
    let backup = |db: &mut Db| -> u64 {
        let m = ack(db, &format!("BACKUP TO '{}'", dest.display()));
        m.split_whitespace().nth(1).unwrap().parse().unwrap()
    };
    let mut db = Db::open(&d, with_store()).unwrap();
    setup(&mut db, 30);
    for i in ["items_body", "items_emb", "items_tenant"] {
        ack(&mut db, &format!("ALTER INDEX {i} ON items SET TIER 'archived'"));
    }
    assert!(files(&store) > 0, "the segments went to the store");
    let want = ids(&mut db, "SELECT id FROM items LIMIT 100");
    let text = ids(&mut db, QUERY);
    let b1 = backup(&mut db);
    drop(db);
    // The rotation recodes the directory and keeps the old key in the
    // ring; the tier's objects stay under it.
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w.files > 0, "{w:?}");
    let mut db = Db::open(&d, with_store()).unwrap();
    assert_eq!(db.data_key_ring_size(), 1);
    let b2 = backup(&mut db);
    drop(db);
    // `key reseal`, then `key retire`: the tier's objects under the new key
    // at the same length, and the ring gone.
    let c = celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m).unwrap();
    let tier = celastro::objstore::DirStore::new(&store).unwrap();
    let resealed = celastro::cipher::reseal_archive(
        &c,
        &tier,
        "celastro/",
        &[("items".into(), "shard-0000".into())],
        &scratch,
    )
    .unwrap();
    assert!(resealed > 0, "the tier's objects were under the old key");
    assert_eq!(celastro::cipher::retire_keys(&d, &m, &[]).unwrap(), 1);
    let mut db = Db::open(&d, with_store()).unwrap();
    let b3 = backup(&mut db);
    drop(db);
    // The backup after the reseal first: today's refusal. Then the two
    // before it, still whole.
    for ts in [b3, b1, b2] {
        let fresh = dir(&format!("reseal-fresh-{ts}"));
        let mut r = Db::open(&fresh, with_store()).unwrap();
        let v = ack(&mut r, &format!("VERIFY BACKUP '{}' AS OF {ts}", dest.display()));
        assert!(v.contains("every one as recorded"), "{v}");
        let m = ack(&mut r, &format!("RESTORE FROM '{}' AS OF {ts}", dest.display()));
        assert!(m.contains("restored"), "{m}");
        assert_eq!(ids(&mut r, "SELECT id FROM items LIMIT 100"), want, "backup {ts}: {m}");
        assert_eq!(ids(&mut r, QUERY), text, "backup {ts}");
        drop(r);
        let _ = std::fs::remove_dir_all(&fresh);
    }
    // The pool holds an archived segment twice over: under the old key's
    // fingerprint for the backups before the reseal, under the new key's
    // for the one after.
    let mut copies: std::collections::BTreeMap<String, usize> = Default::default();
    for e in std::fs::read_dir(dest.join("pool/items/shard-0000")).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().to_string();
        *copies.entry(name.split('.').next().unwrap().to_string()).or_default() += 1;
    }
    assert!(copies.values().any(|n| *n >= 2), "{copies:?}");
    for p in [&d, &store, &dest, &scratch] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// The process-global pin of the pool's names to the id alone
/// (`CELASTRO_SEAL_IDENTITY=3`), set for a test and lifted when it ends,
/// however it ends.
struct PoolPin;

impl PoolPin {
    fn set() -> PoolPin {
        celastro::cipher::pin_pool_by_id(true);
        PoolPin
    }
}

impl Drop for PoolPin {
    fn drop(&mut self) {
        celastro::cipher::pin_pool_by_id(false);
    }
}

/// Under `CELASTRO_SEAL_IDENTITY=3` the pool names an object by its id
/// alone, as a node before 0.97.0 reads it. A rotation re-seals every
/// segment at its length, so after one the object is present at the
/// segment's size and was trusted: the backup with the ring still held
/// recalled the old-key hash from the last record, the one after `key
/// retire` did the same under a KEY holding the new key alone, `VERIFY
/// BACKUP` passed it, and its restore failed at the attach ("frame 0 does
/// not authenticate") -- the backup held nothing of the segment, and
/// nothing said so. A present object's first frame is read now, and one
/// under another key than the segment's puts the segment under its
/// fingerprint name, which the reply says a node before 0.97.0 cannot
/// restore.
#[test]
fn a_backup_under_the_id_pin_puts_a_segment_a_rotation_resealed_under_its_fingerprint() {
    let _pin = PoolPin::set();
    let d = dir("pin-rotate");
    let dest = dir("pin-rotate-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let m = master(15);
    let backup = |db: &mut Db| -> (u64, String) {
        let m = ack(db, &format!("BACKUP TO '{}'", dest.display()));
        (m.split_whitespace().nth(1).unwrap().parse().unwrap(), m)
    };
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    setup(&mut db, 30);
    let want1 = ids(&mut db, "SELECT id FROM items LIMIT 100");
    let (b1, m1) = backup(&mut db);
    assert!(!m1.contains("named by the key's fingerprint"), "{m1}");
    drop(db);
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w.files > 0, "{w:?}");
    // The ring still held: the segments present at their size under the
    // old key.
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    db.insert("items", doc(200)).unwrap();
    ack(&mut db, "FLUSH items");
    let want2 = ids(&mut db, "SELECT id FROM items LIMIT 100");
    let (b2, m2) = backup(&mut db);
    drop(db);
    // The ring retired: the KEY holds the new key alone.
    assert_eq!(celastro::cipher::retire_keys(&d, &m, &[]).unwrap(), 1);
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    let (b3, m3) = backup(&mut db);
    drop(db);
    for (ts, want) in [(b3, &want2), (b2, &want2), (b1, &want1)] {
        let fresh = dir(&format!("pin-rotate-fresh-{ts}"));
        let mut r = Db::open(&fresh, opts(Some(m))).unwrap();
        let v = ack(&mut r, &format!("VERIFY BACKUP '{}' AS OF {ts}", dest.display()));
        assert!(v.contains("every one as recorded"), "{v}");
        let msg = ack(&mut r, &format!("RESTORE FROM '{}' AS OF {ts}", dest.display()));
        assert!(msg.contains("restored"), "{msg}");
        assert_eq!(&ids(&mut r, "SELECT id FROM items LIMIT 100"), want, "backup {ts}: {msg}");
        drop(r);
        let _ = std::fs::remove_dir_all(&fresh);
    }
    for m in [&m2, &m3] {
        assert!(m.contains("named by the key's fingerprint") && m.contains("before 0.97.0"), "{m}");
    }
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&dest);
}

/// A write-ahead log that opens to no frame under the directory's key --
/// another database's log put in its place, under the same master, which
/// is what a copy by hand from another node's directory is -- refuses the
/// open whole rather than emptying the log as a torn tail (0.97.0; the
/// catalog opens, so the log is the first file that does not).
#[test]
fn a_log_under_another_key_refuses_the_open_and_is_not_emptied() {
    let (a, b) = (dir("foreign-log-a"), dir("foreign-log-b"));
    for d in [&a, &b] {
        let mut db = Db::open(d, opts(Some(master(13)))).unwrap();
        db.execute(CREATE).unwrap();
        for i in 0..5 {
            db.insert("items", doc(i)).unwrap();
        }
    }
    let wal = |d: &std::path::Path| d.join("collections/items/shard-0000/wal.log");
    let own = std::fs::read(wal(&a)).unwrap();
    let foreign = std::fs::read(wal(&b)).unwrap();
    assert!(!own.is_empty() && !foreign.is_empty());
    std::fs::write(wal(&a), &foreign).unwrap();
    let e = Db::open(&a, opts(Some(master(13)))).err().expect("a foreign log").to_string();
    assert!(e.contains("does not open under this database's key"), "{e}");
    assert_eq!(std::fs::read(wal(&a)).unwrap(), foreign, "nothing was cut");
    std::fs::write(wal(&a), &own).unwrap();
    let mut db = Db::open(&a, opts(Some(master(13)))).unwrap();
    assert_eq!(ids(&mut db, "SELECT id FROM items LIMIT 100").len(), 5);
    drop(db);
    for d in [&a, &b] {
        let _ = std::fs::remove_dir_all(d);
    }
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

/// An encrypted backup's record is sealed under its data key with the
/// object's key as its identity: it does not read in the clear, a byte
/// changed in it refuses the restore, and a record in the clear beside
/// the backup's KEY -- what every backup wrote before 0.88.0, and what a
/// hand on the bucket could put there -- is refused as a downgrade
/// unless the pin says an old node wrote it.
#[test]
fn a_backup_record_is_sealed_and_a_changed_one_is_refused() {
    let src = dir("record-src");
    let backups = dir("record-backups");
    let o = opts(Some(master(7)));
    let mut db = Db::open(&src, o.clone()).unwrap();
    setup(&mut db, 20);
    let before = ids(&mut db, "SELECT id FROM items LIMIT 200");
    ack(&mut db, &format!("BACKUP TO '{}'", backups.display()));
    drop(db);
    let instants = backups.join("nodes/local/backups");
    let ts_dir = std::fs::read_dir(&instants).unwrap().map(|e| e.unwrap().path()).next().unwrap();
    let record_path = ts_dir.join("BACKUP");
    let sealed = std::fs::read(&record_path).unwrap();
    assert!(!sealed.starts_with(b"celastro backup"), "the record is not in the clear");
    let restore = |tag: &str| -> celastro::Result<String> {
        let d = dir(tag);
        let mut db = Db::open(&d, o.clone())?;
        let r = db
            .execute(&format!("RESTORE FROM '{}'", backups.display()))
            .and_then(|out| out.finished())
            .map(|out| format!("{out:?}"));
        let same = r.is_ok() && ids(&mut db, "SELECT id FROM items LIMIT 200") == before;
        drop(db);
        let _ = std::fs::remove_dir_all(&d);
        r.map(|m| format!("{m} rows-same={same}"))
    };
    assert!(restore("record-ok").unwrap().contains("rows-same=true"));
    // A byte of the record changed: refused, naming the record.
    let mut changed = sealed.clone();
    let mid = changed.len() / 2;
    changed[mid] ^= 1;
    std::fs::write(&record_path, &changed).unwrap();
    let e = restore("record-changed").unwrap_err().to_string();
    assert!(e.contains("BACKUP does not open"), "{e}");
    // The record in the clear beside the backup's KEY: what a node before
    // 0.88.0 wrote, and what whoever writes the bucket could put there to
    // name other objects. Refused as a downgrade (0.97.0), naming the pin
    // that reads a backup an old node really wrote.
    let key = std::fs::read(ts_dir.join("KEY")).unwrap();
    let cipher = celastro::cipher::Cipher::unwrap(&key, &master(7)).unwrap();
    let ts_name = ts_dir.file_name().unwrap().to_string_lossy().to_string();
    let identity = format!("nodes/local/backups/{ts_name}/BACKUP");
    let plain = cipher.open_file(&celastro::cipher::Ids::same(&identity), &sealed).unwrap();
    assert!(plain.starts_with(b"celastro backup\nversion 2\n"));
    std::fs::write(&record_path, &plain).unwrap();
    let e = restore("record-plain").unwrap_err().to_string();
    assert!(e.contains("refused as a downgrade") && e.contains("CELASTRO_SEAL_IDENTITY=2"), "{e}");
    for p in [&src, &backups] {
        let _ = std::fs::remove_dir_all(p);
    }
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

/// The data key rotates: every file and every log record sealed again
/// under a fresh key and `KEY` rewrapped, nothing opening under the old
/// key after, the database answering the same when reopened; a rotation
/// cut short leaves `KEY.next`, the node refuses to open until it is
/// finished, and running it again finishes it, recognising the files
/// already under the new key.
#[test]
fn a_data_key_rotation_reseals_every_file_and_a_cut_short_one_resumes() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let d = dir("rotate");
    let m = master(3);
    let keys = |db: &mut Db, sql: &str| -> Vec<String> {
        db.query(sql).unwrap().rows.iter().map(|r| r.key.clone()).collect()
    };
    let (q_before, v_before) = {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        setup(&mut db, 40);
        // Rows after the last flush: the log holds records to reseal.
        for i in 40..50 {
            db.insert("items", doc(i)).unwrap();
        }
        (keys(&mut db, QUERY), keys(&mut db, VECTOR))
    };
    let all_before = {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        // The harness's setup deletes a row: what is there is the reference.
        let all = keys(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100");
        assert!(all.len() >= 45, "before the rotation, reopened: {all:?}");
        all
    };
    let key_before = std::fs::read(d.join("KEY")).unwrap();
    let old = celastro::cipher::Cipher::unwrap(&key_before, &m).unwrap();
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w.files > 5 && w.records >= 10 && w.already == 0, "{w:?}");
    assert!(w.failures.is_empty(), "{w:?}");
    let key_after = std::fs::read(d.join("KEY")).unwrap();
    assert_ne!(key_before, key_after, "KEY was rewrapped around a new data key");
    assert!(!d.join("KEY.next").exists());
    // Under the new key everything opens; under the old, nothing sealed.
    let new = celastro::cipher::Cipher::unwrap(&key_after, &m).unwrap();
    let c = celastro::cipher::check_dir(&d, &new).unwrap();
    assert!(c.failures.is_empty() && c.files >= w.files && c.records == w.records, "{c:?}");
    let c = celastro::cipher::check_dir(&d, &old).unwrap();
    assert!(c.failures.iter().any(|f| f.contains("segments")), "{c:?}");
    assert!(c.failures.iter().any(|f| f.contains("wal")), "{c:?}");
    {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        assert_eq!(keys(&mut db, QUERY), q_before);
        assert_eq!(keys(&mut db, VECTOR), v_before);
        let got = keys(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100");
        assert_eq!(got, all_before, "after the rotation");
    }
    // Cut short after every file moved but before KEY was replaced: the
    // node refuses the directory, and a second run finishes the rotation
    // with nothing to reseal.
    let fresh = celastro::cipher::Cipher::generate().unwrap();
    std::fs::write(d.join("KEY.next"), fresh.wrap(&m).unwrap()).unwrap();
    let moved = celastro::cipher::recode_dir(&d, &new, &fresh).unwrap();
    assert!(moved.files >= w.files && moved.already == 0, "{moved:?}");
    let e = Db::open(&d, opts(Some(m))).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(e.contains("rotation was interrupted"), "{e}");
    let w2 = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w2.files == 0 && w2.records == 0 && w2.already == moved.files, "{w2:?}");
    assert!(!d.join("KEY.next").exists());
    {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        assert_eq!(keys(&mut db, QUERY), q_before);
        let got = keys(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100");
        assert_eq!(got, all_before, "after the resumed rotation");
    }
    // A plain directory has no key to rotate.
    let p = dir("rotate-plain");
    {
        let mut db = Db::open(&p, opts(None)).unwrap();
        setup(&mut db, 5);
    }
    let e = celastro::cipher::rotate_data_key(&p, &m, &[]).unwrap_err().to_string();
    assert!(e.contains("KEY"), "{e}");
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&p);
}

/// A ring with nothing behind it is kept by the open and retired by
/// `key retire`.
///
/// The ring exists so an archived object sealed under an older data key
/// still opens. Until 0.96.0 the open retired a ring the catalog said
/// nothing needed -- and the catalog was wrong once a segment had moved
/// back from the archived tier under the old key, so the open locked it
/// out. The open changes no key now; retiring is the operator's `key
/// retire`, which walks the archive first and ends in the call made
/// here.
#[test]
fn a_key_ring_with_no_archived_index_is_kept_by_the_open_and_retired_by_the_command() {
    let d = dir("ring-self-retire");
    let m = master(11);
    {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        db.execute("CREATE COLLECTION docs (id TEXT PRIMARY KEY, n INT)").unwrap();
        db.execute("INSERT INTO docs VALUES ('{\"id\":\"a\",\"n\":1}')").unwrap();
    }
    // A rotation keeps the ring; it is put there by hand here: this is
    // about what the next open does with one.
    let wrapped = std::fs::read(d.join("KEY")).unwrap();
    let current = celastro::cipher::Cipher::unwrap(&wrapped, &m).unwrap();
    let mut with_ring = current.without_previous();
    with_ring.keep_previous(&celastro::cipher::Cipher::generate().unwrap());
    std::fs::write(d.join("KEY"), with_ring.wrap(&m).unwrap()).unwrap();
    let ring_on_disk = || {
        celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m)
            .unwrap()
            .previous_keys()
    };
    assert_eq!(ring_on_disk(), 1, "the ring is there before the open");

    {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        assert_eq!(db.data_key_ring_size(), 1, "the open keeps the ring: retiring is a command");
        assert_eq!(ring_on_disk(), 1, "and wrote nothing over KEY");
        let out = db.execute("SELECT count(*) FROM docs").unwrap();
        assert!(format!("{out:?}").contains('1'), "{out:?}");
    }
    // The command, with the database closed (it takes the directory lock):
    // nothing is under the previous key, so the ring goes.
    assert_eq!(celastro::cipher::retire_keys(&d, &m, &[]).unwrap(), 1, "one key retired");
    assert_eq!(ring_on_disk(), 0, "KEY on disk no longer keeps the ring");
    // The data is still there, which is the thing a wrong retirement breaks.
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    assert_eq!(db.data_key_ring_size(), 0);
    let out = db.execute("SELECT count(*) FROM docs").unwrap();
    assert!(format!("{out:?}").contains('1'), "{out:?}");
}

/// A master rotation across an interrupted data-key rotation locks nothing
/// out. `key rekey` on a database's `KEY` takes the directory's lock and
/// rewraps the `KEY.next` an interrupted `key rotate` left with it -- the
/// only copy of the new data key, which already seals the recoded files --
/// so once the old master is destroyed the rotation still finishes under
/// the new one. Until 0.99.0 it rewrapped `KEY` alone, with no lock: the
/// documented master rotation and the destruction of the old master left
/// those files under a key no master opened.
#[test]
fn a_master_rotation_across_an_interrupted_data_key_rotation_locks_nothing_out() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let d = dir("rekey-across");
    let (m1, m2, m3) = (master(31), master(32), master(33));
    let read = |name: &str| std::fs::read(d.join(name)).unwrap();
    let opens = |bytes: &[u8], m: &[u8; 32]| celastro::cipher::Cipher::unwrap(bytes, m).is_ok();
    let all_before = {
        let mut db = Db::open(&d, opts(Some(m1))).unwrap();
        setup(&mut db, 40);
        for i in 40..50 {
            db.insert("items", doc(i)).unwrap();
        }
        ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100")
    };
    // The rotation cut short after every file moved and before KEY was
    // replaced, as the rotation test builds it.
    let old = celastro::cipher::Cipher::unwrap(&read("KEY"), &m1).unwrap();
    let fresh = celastro::cipher::Cipher::generate().unwrap();
    std::fs::write(d.join("KEY.next"), fresh.wrap(&m1).unwrap()).unwrap();
    let moved = celastro::cipher::recode_dir(&d, &old, &fresh).unwrap();
    assert!(moved.files > 0, "{moved:?}");
    // The master rotation as documented: the directory's KEY to M2 with M1
    // in hand. Both files come out under M2 and neither stays under M1.
    let with_next = celastro::cipher::rekey_file(&d.join("KEY"), &m1, &m2).unwrap();
    assert!(with_next, "KEY.next was rewrapped with KEY");
    for name in ["KEY", "KEY.next"] {
        let b = read(name);
        assert!(opens(&b, &m2), "{name} is under the new master");
        assert!(!opens(&b, &m1), "{name} is not under the old master");
    }
    // M1 destroyed: the node refuses only until the rotation is finished,
    // and finishing it needs M2 alone.
    let e = Db::open(&d, opts(Some(m2))).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(e.contains("rotation was interrupted"), "{e}");
    let w = celastro::cipher::rotate_data_key(&d, &m2, &[]).unwrap();
    assert_eq!(w.already, moved.files, "{w:?}");
    assert!(!d.join("KEY.next").exists());
    {
        let mut db = Db::open(&d, opts(Some(m2))).unwrap();
        assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all_before);
    }
    let c = celastro::cipher::check_dir(&d, &old).unwrap();
    assert!(!c.failures.is_empty(), "nothing is under the old data key: {c:?}");
    // A node serving the directory: refused at once, KEY untouched.
    let key_before = read("KEY");
    {
        let _held = Db::open(&d, opts(Some(m2))).unwrap();
        let e = celastro::cipher::rekey_file(&d.join("KEY"), &m2, &m3).unwrap_err().to_string();
        assert!(e.contains("open in another process"), "{e}");
        assert_eq!(read("KEY"), key_before, "nothing was written under the node");
    }
    // A KEY.next under a third master: refused naming it, neither written.
    std::fs::write(d.join("KEY.next"), fresh.wrap(&m3).unwrap()).unwrap();
    let next_before = read("KEY.next");
    let e = celastro::cipher::rekey_file(&d.join("KEY"), &m2, &m1).unwrap_err().to_string();
    assert!(e.contains("KEY.next") && e.contains("does not open"), "{e}");
    assert_eq!(read("KEY"), key_before);
    assert_eq!(read("KEY.next"), next_before);
    std::fs::remove_file(d.join("KEY.next")).unwrap();
    // A bare key file -- `key init`'s, the chart's Secret, an export's --
    // is the one-file rewrap: no LOCK appears beside it, and its mode stays.
    let bare = dir("rekey-bare");
    std::fs::create_dir_all(&bare).unwrap();
    let data_key = bare.join("data.key");
    let wrapped = celastro::cipher::Cipher::generate().unwrap().wrap(&m1).unwrap();
    std::fs::write(&data_key, &wrapped).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(!celastro::cipher::rekey_file(&data_key, &m1, &m2).unwrap());
    assert!(opens(&std::fs::read(&data_key).unwrap(), &m2));
    assert!(!bare.join("LOCK").exists(), "a bare file takes no directory lock");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&data_key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the rewrap keeps the file's mode");
    }
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_dir_all(&bare);
}

/// A node whose `KEY` is under the previous master opens and rewraps it.
/// The chart's master rotation rekeys the Secret's `KEY`, but a pod that
/// has started holds its own `/data/KEY`, read first and still under the
/// old master: until 0.99.0 every pod then refused to start, and with the
/// old master destroyed every volume was lost. With the old master offered
/// as the previous one the open succeeds, rewraps `KEY` under the current
/// master and logs it, so one rolling restart converges a cluster; without
/// it the refusal stands.
#[test]
fn a_node_whose_key_is_under_the_previous_master_opens_and_rewraps_it() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let (m1, m2) = (master(41), master(42));
    let with = |current: [u8; 32], previous: Option<[u8; 32]>, key_file: &Path| {
        let mut o = opts(Some(current));
        o.previous_master_keys = previous.into_iter().map(Into::into).collect();
        o.key_file = Some(key_file.to_path_buf());
        o
    };
    let opens = |bytes: &[u8], m: &[u8; 32]| celastro::cipher::Cipher::unwrap(bytes, m).is_ok();
    // The Secret's KEY, as `key init` writes it, and a pod's first start.
    let key_file = dir("previous-keys").join("KEY");
    std::fs::create_dir_all(key_file.parent().unwrap()).unwrap();
    let wrapped = celastro::cipher::Cipher::generate().unwrap().wrap(&m1).unwrap();
    std::fs::write(&key_file, &wrapped).unwrap();
    let d = dir("previous");
    let before = {
        let mut db = Db::open(&d, with(m1, None, &key_file)).unwrap();
        setup(&mut db, 20);
        ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100")
    };
    assert_eq!(std::fs::read(d.join("KEY")).unwrap(), wrapped);
    // The Secret rekeyed to M2: the procedure's first step.
    celastro::cipher::rekey_file(&key_file, &m1, &m2).unwrap();
    // The pod restarted under M2 alone: refused, /data/KEY is under M1.
    let e =
        Db::open(&d, with(m2, None, &key_file)).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(e.contains("does not open this database's KEY"), "{e}");
    // With M1 as the previous master: opens, the rows as before, and KEY
    // now under M2 and not under M1.
    {
        let mut db = Db::open(&d, with(m2, Some(m1), &key_file)).unwrap();
        assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), before);
    }
    let key = std::fs::read(d.join("KEY")).unwrap();
    assert!(opens(&key, &m2) && !opens(&key, &m1), "KEY was rewrapped under the current master");
    // Converged: M2 alone opens it.
    {
        let mut db = Db::open(&d, with(m2, None, &key_file)).unwrap();
        assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), before);
    }
    // A pod first started across the rotation, its Secret's KEY still
    // under M1: adopted, and landing under M2.
    std::fs::write(&key_file, &wrapped).unwrap();
    let fresh = dir("previous-fresh");
    drop(Db::open(&fresh, with(m2, Some(m1), &key_file)).unwrap());
    let key = std::fs::read(fresh.join("KEY")).unwrap();
    assert!(
        opens(&key, &m2) && !opens(&key, &m1),
        "the adopted KEY lands under the current master"
    );
    drop(Db::open(&fresh, with(m2, None, &key_file)).unwrap());
    for p in [&d, &fresh, key_file.parent().unwrap()] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// A backup made under the old master restores under the new one. `key
/// rekey-backups` rewraps every backup's `KEY` at a destination and the
/// hash of it in each sealed record, so `RESTORE` and `VERIFY BACKUP` both
/// pass under the new master and the old one opens nothing there; until
/// then a restore accepts the previous master. Until 0.99.0 no command
/// rewrapped a backup's `KEY`, and a rewrap by hand passed the restore and
/// failed the verification on the record's hash: every backup needed the
/// old master for good, and nothing said so.
#[test]
fn a_backup_under_the_old_master_restores_under_the_new_one_once_rekeyed() {
    let (m1, m2) = (master(51), master(52));
    let src = dir("rekey-backups-src");
    let backups = dir("rekey-backups-dest");
    let mut db = Db::open(&src, opts(Some(m1))).unwrap();
    setup(&mut db, 20);
    let before = ids(&mut db, "SELECT id FROM items LIMIT 200");
    ack(&mut db, &format!("BACKUP TO '{}'", backups.display()));
    drop(db);
    let ts_dir = std::fs::read_dir(backups.join("nodes/local/backups"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .unwrap();
    let key_before = std::fs::read(ts_dir.join("KEY")).unwrap();
    let restore = |tag: &str, o: DbOpts| -> celastro::Result<bool> {
        let d = dir(tag);
        let mut db = Db::open(&d, o)?;
        let r = db
            .execute(&format!("RESTORE FROM '{}'", backups.display()))
            .and_then(|out| out.finished());
        let same = r.is_ok() && ids(&mut db, "SELECT id FROM items LIMIT 200") == before;
        drop(db);
        let _ = std::fs::remove_dir_all(&d);
        r.map(|_| same)
    };
    let verify = |o: DbOpts| -> celastro::Result<String> {
        let d = dir("rekey-backups-verify");
        let mut db = Db::open(&d, o)?;
        let r = db
            .execute(&format!("VERIFY BACKUP '{}'", backups.display()))
            .and_then(|out| out.finished())
            .map(|out| format!("{out:?}"));
        drop(db);
        let _ = std::fs::remove_dir_all(&d);
        r
    };
    let with_previous = || {
        let mut o = opts(Some(m2));
        o.previous_master_keys = vec![m1.into()];
        o
    };
    // Before the rewrap: under M2 alone refused; with M1 as the previous
    // master, restored whole.
    let e = restore("rekey-backups-m2-early", opts(Some(m2))).unwrap_err().to_string();
    assert!(e.contains("does not open under this master key"), "{e}");
    assert!(restore("rekey-backups-m2-prev", with_previous()).unwrap(), "with the previous master");
    // The rewrap: every backup at the destination, from M1 to M2.
    let archive = celastro::objstore::ArchiveOpts::default();
    let dest = backups.display().to_string();
    let r = celastro::backup::rekey_backups(&archive, None, &dest, &m1, &m2).unwrap();
    assert_eq!((r.rewrapped, r.already, r.nodes), (1, 0, 1), "{r:?}");
    assert_ne!(std::fs::read(ts_dir.join("KEY")).unwrap(), key_before, "KEY was rewritten");
    assert!(
        !std::fs::read(ts_dir.join("BACKUP")).unwrap().starts_with(b"celastro backup"),
        "the record is still sealed"
    );
    // Under M2 alone: restored whole, and every object as recorded.
    assert!(restore("rekey-backups-m2", opts(Some(m2))).unwrap(), "under the new master alone");
    let v = verify(opts(Some(m2))).unwrap();
    assert!(v.contains("every one as recorded"), "{v}");
    // Under M1 alone: nothing at the destination opens any more.
    let e = verify(opts(Some(m1))).unwrap_err().to_string();
    assert!(e.contains("does not open under this master key"), "{e}");
    // A second run finds everything under M2 already.
    let r = celastro::backup::rekey_backups(&archive, None, &dest, &m1, &m2).unwrap();
    assert_eq!((r.rewrapped, r.already), (0, 1), "{r:?}");
    for p in [&src, &backups] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// A node of a test cluster: a `Db` on its directory, served on the wire
/// from a thread, with the address its peers call it by.
struct Served {
    url: String,
    port: u16,
    dir: PathBuf,
    db: Arc<RwLock<Db>>,
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

impl Served {
    /// Open `dir` with `opts(url)` and serve it on `port` -- 0 for any free
    /// one; a node started again keeps its port, so its address stands.
    fn start(dir: PathBuf, port: u16, opts: &dyn Fn(&str) -> DbOpts) -> Served {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("tcp://127.0.0.1:{port}");
        let db = Arc::new(RwLock::new(Db::open(&dir, opts(&url)).unwrap()));
        let stop = Arc::new(AtomicBool::new(false));
        let (d, s) = (db.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            celastro::wire::serve(listener, d, TOKEN.to_string(), s, None).unwrap();
        });
        Served { url, port, dir, db, stop, thread }
    }

    /// A statement under the node's lock, the lock let go before its
    /// deferred work runs: a move's copy holds nothing, and the target's
    /// switch back here needs this lock.
    fn exec(&self, sql: &str) -> std::result::Result<String, String> {
        let out = self.db.write().unwrap().execute(sql).map_err(|e| e.to_string())?;
        match out.finished().map_err(|e| e.to_string())? {
            Outcome::Ack(m) => Ok(m),
            other => Err(format!("{sql}: {other:?}")),
        }
    }

    fn query(&self, sql: &str) -> Vec<String> {
        ids(&mut self.db.write().unwrap(), sql)
    }

    /// Stop serving and let go of the database, once every connection
    /// thread has: the directory's lock is free after this.
    fn stop(self) -> (PathBuf, u16) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().unwrap();
        let t = std::time::Instant::now();
        while Arc::strong_count(&self.db) > 1 && t.elapsed() < std::time::Duration::from_secs(20) {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(Arc::strong_count(&self.db), 1, "a connection thread still holds the node");
        drop(self.db);
        (self.dir, self.port)
    }
}

/// `doc(i)`'s keys for the tenant `t<n>`, as `ids` formats a partitioned
/// collection's: the partition key, a separator, the id.
fn tenant_ids(n: usize, count: usize) -> Vec<String> {
    let mut out: Vec<String> = (0..count)
        .filter(|i| i % 3 == n)
        .map(|i| format!("{:?}", format!("t{n}\u{1}doc-{i:03}")))
        .collect();
    out.sort();
    out
}

/// In a cluster every node puts its archived objects under the one
/// prefix, so a node's `key reseal` walks its own shards alone -- the
/// `shard-NNNN` directories its placement puts here, not a move's
/// half-pulled `shard-NNNN.incoming` -- and the other node's objects stay
/// under the key its ring holds: after this node's reseal and retire the
/// other still reads its archived rows. Through 0.98.0 the walk was of
/// every shard of the collection, and node A's reseal sealed node B's
/// objects under a key only A's KEY held.
#[test]
fn a_reseal_on_a_shared_archive_prefix_walks_this_nodes_shards_alone() {
    use celastro::objstore::ObjectStore;
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let m = master(13);
    let key_file = dir("prefix-keys").join("KEY");
    std::fs::create_dir_all(key_file.parent().unwrap()).unwrap();
    let wrapped = celastro::cipher::Cipher::generate().unwrap().wrap(&m).unwrap();
    std::fs::write(&key_file, &wrapped).unwrap();
    let store = dir("prefix-store");
    const PREFIX: &str = "celastro/";
    let node_opts = |url: &str| {
        let mut o = opts(Some(m));
        o.key_file = Some(key_file.clone());
        o.node = Some(url.to_string());
        o.archive.dir = Some(store.clone());
        o.archive.prefix = PREFIX.into();
        o
    };
    let a = Served::start(dir("prefix-a"), 0, &node_opts);
    let b = Served::start(dir("prefix-b"), 0, &node_opts);
    a.exec(&format!("ATTACH NODE '{}'", b.url)).unwrap();
    a.exec(
        "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) \
         PARTITION BY (tenant) WITH (splits = ['t1', 't2'])",
    )
    .unwrap();
    a.exec(INDEXES[0]).unwrap();
    for i in 0..60usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    a.exec("FLUSH items").unwrap();
    // The one index archived: every shard's segments go to the store,
    // each node putting its own under `<prefix>items/<shard>/`.
    a.exec("ALTER INDEX items_body ON items SET TIER 'archived'").unwrap();
    let want = a.query("SELECT id FROM items LIMIT 200");
    assert_eq!(want.len(), 60);
    let shard_dirs = |n: &Served| -> Vec<usize> {
        let mut v: Vec<usize> =
            n.db.read().unwrap().shards("items").unwrap().iter().map(|s| s.index).collect();
        v.sort();
        v
    };
    let (on_a, on_b) = (shard_dirs(&a), shard_dirs(&b));
    assert!(!on_a.is_empty() && !on_b.is_empty(), "spread over both: {on_a:?} {on_b:?}");
    let dirname = |i: &usize| format!("shard-{i:04}");
    let (a_url, b_url) = (a.url.clone(), b.url.clone());
    let (a_dir, a_port) = a.stop();
    let (b_dir, b_port) = b.stop();

    let dir_store = celastro::objstore::DirStore::new(&store).unwrap();
    let objects = |shard: &usize| -> Vec<String> {
        dir_store.list(&format!("{PREFIX}items/{}/", dirname(shard))).unwrap()
    };
    for s in on_a.iter().chain(on_b.iter()) {
        assert!(!objects(s).is_empty(), "shard {s} put nothing to the store");
    }
    let unwrap_key = |d: &Path| {
        celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m).unwrap()
    };
    // Which key of `c`'s ring opens each object of a shard.
    let opens_under = |shard: &usize, c: &celastro::cipher::Cipher| -> Vec<Option<(usize, bool)>> {
        objects(shard)
            .iter()
            .map(|k| {
                let (_, _, ids) = celastro::cipher::archived_seal_id(k).unwrap();
                let head = dir_store.head(k, celastro::cipher::HEAD_BYTES as u64).unwrap().unwrap();
                c.opening_key(&ids, &head, head.len() < celastro::cipher::HEAD_BYTES)
            })
            .collect()
    };
    let current = |v: &[Option<(usize, bool)>]| v.iter().all(|o| *o == Some((0, true)));

    // A rotates: its ring is {K_A, K0}; B's KEY is K0 alone.
    let w = celastro::cipher::rotate_data_key(&a_dir, &m, &[]).unwrap();
    assert_eq!(w.kept_keys, 1, "{w:?}");
    let (ca, cb) = (unwrap_key(&a_dir), unwrap_key(&b_dir));
    // A move of one of B's shards onto A, cut short: not a shard of A's.
    let incoming =
        a_dir.join("collections").join("items").join(format!("{}.incoming", dirname(&on_b[0])));
    std::fs::create_dir_all(incoming.join("segments")).unwrap();
    let catalog_a = {
        let bytes = std::fs::read(a_dir.join("CATALOG")).unwrap();
        let plain = ca.open_file(&celastro::cipher::Ids::same("CATALOG"), &bytes).unwrap();
        celastro::catalog::Catalog::decode(&plain).unwrap()
    };
    // Without the node's address the scope cannot be known: refused,
    // naming the nodes and the variable.
    let e =
        celastro::cipher::archived_shards_here(&a_dir, &catalog_a, None).unwrap_err().to_string();
    assert!(e.contains(&b_url) && e.contains("CELASTRO_NODE"), "{e}");
    let here = celastro::cipher::archived_shards_here(&a_dir, &catalog_a, Some(&a_url)).unwrap();
    let want_here: Vec<(String, String)> =
        on_a.iter().map(|s| ("items".to_string(), dirname(s))).collect();
    assert_eq!(here, want_here, "A's shards: not B's, not the half-pulled one");
    let walk = celastro::cipher::walk_archive(&ca, &dir_store, PREFIX, &here).unwrap();
    assert!(
        walk.objects > 0 && walk.under_previous == walk.objects && walk.unopenable.is_empty(),
        "{walk:?}"
    );
    let n = celastro::cipher::reseal_archive(
        &ca,
        &dir_store,
        PREFIX,
        &here,
        &a_dir.join("reseal.scratch"),
    )
    .unwrap();
    assert_eq!(n, walk.objects);
    for s in &on_a {
        assert!(current(&opens_under(s, &ca)), "shard {s}: A's objects under A's current key");
    }
    for s in &on_b {
        assert!(current(&opens_under(s, &cb)), "shard {s}: B's objects were re-sealed");
    }
    let walk = celastro::cipher::walk_archive(&ca, &dir_store, PREFIX, &here).unwrap();
    assert_eq!(walk.under_previous, 0, "{walk:?}");
    assert_eq!(celastro::cipher::retire_keys(&a_dir, &m, &[]).unwrap(), 1);
    std::fs::remove_dir_all(&incoming).unwrap();

    // Both back on their addresses. Every row reads from the store: A's
    // shards under A's new key, B's under the one key B's ring holds --
    // by tenant, which routes to the one shard that owns it, and whole.
    let a = Served::start(a_dir, a_port, &node_opts);
    let b = Served::start(b_dir, b_port, &node_opts);
    for (node, shards) in [(&a, &on_a), (&b, &on_b)] {
        for s in shards.iter() {
            let got = node.query(&format!("SELECT id FROM items WHERE tenant = 't{s}' LIMIT 100"));
            assert_eq!(got, tenant_ids(*s, 60), "shard {s}'s archived rows");
        }
    }
    assert_eq!(b.query("SELECT id FROM items LIMIT 200"), want);
    assert_eq!(a.query("SELECT id FROM items LIMIT 200"), want);
    let (a_dir, _) = a.stop();
    let (b_dir, _) = b.stop();
    for p in [&a_dir, &b_dir, &store, key_file.parent().unwrap()] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// A move copies the source's files as they lie, sealed under its data
/// key. Onto a node whose ring does not hold that key the move is refused
/// naming the key, and the target is as it was: no `shard-NNNN`, no
/// `shard-NNNN.incoming`, no placement naming it, and it opens again.
/// Through 0.98.0 the target renamed the directory in and recorded the
/// placement before it tried to open a byte, and its next open failed on
/// the shard it could not read. `key rotate --to` rotates both nodes to
/// one key, and the same move then lands.
#[test]
fn a_move_onto_a_node_whose_ring_lacks_the_sources_key_is_refused_and_leaves_nothing_behind() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var(celastro::wire::TOKEN_ENV, TOKEN);
    let m = master(17);
    let keys = dir("move-keys");
    std::fs::create_dir_all(&keys).unwrap();
    let key_file = keys.join("KEY");
    std::fs::write(&key_file, celastro::cipher::Cipher::generate().unwrap().wrap(&m).unwrap())
        .unwrap();
    let node_opts = |url: &str| {
        let mut o = opts(Some(m));
        o.key_file = Some(key_file.clone());
        o.node = Some(url.to_string());
        o
    };
    let holder = |db: &Db| db.catalog.placement.get("items").map(|t| t[0].node.clone());
    // A alone first: a collection, rows, sealed segments; then A's own
    // rotation, which draws a key B's KEY -- the shared one -- lacks.
    let a = Served::start(dir("move-a"), 0, &node_opts);
    a.exec(
        "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) \
         PARTITION BY (tenant) WITH (splits = ['t1', 't2'])",
    )
    .unwrap();
    a.exec(INDEXES[0]).unwrap();
    for i in 0..60usize {
        a.db.write().unwrap().insert("items", doc(i)).unwrap();
    }
    a.exec("FLUSH items").unwrap();
    let want = a.query("SELECT id FROM items LIMIT 200");
    assert_eq!(want.len(), 60);
    let (a_dir, a_port) = a.stop();
    let w = celastro::cipher::rotate_data_key(&a_dir, &m, &[]).unwrap();
    assert!(w.files > 0 && w.failures.is_empty(), "{w:?}");
    let a = Served::start(a_dir, a_port, &node_opts);
    let b = Served::start(dir("move-b"), 0, &node_opts);
    a.exec(&format!("ATTACH NODE '{}'", b.url)).unwrap();
    let e = a.exec(&format!("MOVE SHARD 0 OF items TO '{}'", b.url)).unwrap_err();
    assert!(e.contains("not adopted") && e.contains("data key"), "{e}");
    // The target as it was: nothing renamed in, nothing left, no placement
    // naming it; the source still holds and answers.
    let b_items = b.dir.join("collections").join("items");
    assert!(!b_items.join("shard-0000").exists(), "the pulled directory was renamed in");
    assert!(!b_items.join("shard-0000.incoming").exists(), "the pulled directory was left");
    assert!(
        !b_items.exists() || std::fs::read_dir(&b_items).unwrap().next().is_none(),
        "{} holds something",
        b_items.display()
    );
    let on_b = holder(&b.db.read().unwrap());
    assert!(on_b.is_none() || on_b.as_deref() == Some(a.url.as_str()), "{on_b:?}");
    assert_eq!(holder(&a.db.read().unwrap()).as_deref(), Some(a.url.as_str()));
    assert_eq!(a.query("SELECT id FROM items LIMIT 200"), want);
    let (a_dir, a_port) = a.stop();
    let (b_dir, b_port) = b.stop();
    let (a_url, b_url) = (format!("tcp://127.0.0.1:{a_port}"), format!("tcp://127.0.0.1:{b_port}"));
    {
        let b = Db::open(&b_dir, node_opts(&b_url)).unwrap();
        let on_b = holder(&b);
        assert!(on_b.is_none() || on_b.as_deref() == Some(a_url.as_str()), "{on_b:?}");
    }
    // Rotated to one key -- the same wrapped key given to both -- the
    // move lands, and the key given again is nothing to rotate to.
    let shared = celastro::cipher::Cipher::generate().unwrap().wrap(&m).unwrap();
    for d in [&a_dir, &b_dir] {
        let w = celastro::cipher::rotate_data_key_to(d, &m, &[], Some(&shared)).unwrap();
        assert!(w.failures.is_empty(), "{w:?}");
    }
    let fingerprint = |d: &Path| {
        celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m)
            .unwrap()
            .fingerprint()
    };
    assert_eq!(fingerprint(&a_dir), fingerprint(&b_dir), "one current key on both");
    let e = celastro::cipher::rotate_data_key_to(&a_dir, &m, &[], Some(&shared))
        .unwrap_err()
        .to_string();
    assert!(e.contains("already holds"), "{e}");
    let a = Served::start(a_dir, a_port, &node_opts);
    let b = Served::start(b_dir, b_port, &node_opts);
    let moved = a.exec(&format!("MOVE SHARD 0 OF items TO '{}'", b.url)).unwrap();
    assert!(moved.contains("moved"), "{moved}");
    assert_eq!(holder(&a.db.read().unwrap()).as_deref(), Some(b.url.as_str()));
    assert_eq!(b.query("SELECT id FROM items WHERE tenant = 't0' LIMIT 100"), tenant_ids(0, 60));
    assert_eq!(a.query("SELECT id FROM items LIMIT 200"), want);
    assert_eq!(b.query("SELECT id FROM items LIMIT 200"), want);
    let (a_dir, _) = a.stop();
    let (b_dir, _) = b.stop();
    for (d, url) in [(&a_dir, &a_url), (&b_dir, &b_url)] {
        drop(Db::open(d, node_opts(url)).unwrap());
    }
    for p in [&a_dir, &b_dir, &keys] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// A database with sealed segments, a delete and a log with records in
/// it, closed: what a rotation walks, and the rows it holds.
fn closed_db(tag: &str, m: [u8; 32]) -> (PathBuf, Vec<String>) {
    let d = dir(tag);
    let all = {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        setup(&mut db, 20);
        ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100")
    };
    (d, all)
}

/// Whether `CATALOG` opens under the key in `KEY`: what a rotation that
/// wrote nothing leaves true.
fn catalog_opens_under_key(d: &Path, m: &[u8; 32]) -> bool {
    let c = celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), m).unwrap();
    let bytes = std::fs::read(d.join("CATALOG")).unwrap();
    c.open_file(&celastro::cipher::Ids::same("CATALOG"), &bytes).is_ok()
}

/// `from` copied under `to`, as a restore's or a move's staging leaves it.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.path().is_dir() {
            copy_tree(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), dest).unwrap();
        }
    }
}

/// A log a crash left torn -- bytes after the last whole record, or a
/// zero-filled extension -- stops a rotation before it writes anything:
/// no `KEY.next`, `CATALOG` still under `KEY`, the refusal naming the log
/// and saying to open the database once, which cuts the tail; the
/// rotation then goes through. Through 0.98.0 `KEY.next` was written and
/// `CATALOG` re-sealed under its key before the walk stopped on the log,
/// with a message that read as if nothing had changed.
#[test]
fn a_rotation_refuses_a_torn_log_tail_before_writing_anything_and_the_open_cuts_it() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let m = master(21);
    for (tag, tail) in [("rotate-torn", vec![0xA5u8; 40]), ("rotate-zeroed", vec![0u8; 4096])] {
        let (d, all) = closed_db(tag, m);
        let log = d.join("collections").join("items").join("shard-0000").join("wal.log");
        let whole = std::fs::read(&log).unwrap();
        assert!(whole.len() > 64, "the log holds records");
        let mut torn = whole.clone();
        torn.extend_from_slice(&tail);
        std::fs::write(&log, &torn).unwrap();
        let e = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap_err().to_string();
        assert!(
            e.contains("nothing was written")
                && e.contains("wal.log")
                && e.contains("open the database once"),
            "{e}"
        );
        assert!(!d.join("KEY.next").exists(), "{tag}: KEY.next was written before the refusal");
        assert!(catalog_opens_under_key(&d, &m), "{tag}: CATALOG was re-sealed before the refusal");
        {
            let mut db = Db::open(&d, opts(Some(m))).unwrap();
            assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
        }
        assert_eq!(std::fs::read(&log).unwrap().len(), whole.len(), "{tag}: the open cut the tail");
        let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
        assert!(w.files > 0 && w.records > 0 && w.failures.is_empty(), "{w:?}");
        assert!(!d.join("KEY.next").exists());
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
        drop(db);
        let _ = std::fs::remove_dir_all(&d);
    }
}

/// A move cut short leaves `shard-NNNN.incoming` beside the shards, its
/// files sealed under the source's name: a rotation refuses it before it
/// writes anything, naming the directory as a move in flight. Through
/// 0.98.0 the guard looked for a directory named `incoming`, which nothing
/// writes, and the walk took the directory for a shard of that name after
/// `KEY.next` was written.
#[test]
fn a_rotation_refuses_a_half_pulled_shard_before_writing_anything() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let m = master(22);
    let (d, all) = closed_db("rotate-incoming", m);
    let shard = d.join("collections").join("items").join("shard-0000");
    let incoming = d.join("collections").join("items").join("shard-0001.incoming");
    copy_tree(&shard, &incoming);
    let e = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap_err().to_string();
    assert!(e.contains("shard-0001.incoming") && e.contains("move is in flight"), "{e}");
    assert!(!d.join("KEY.next").exists(), "KEY.next was written before the refusal");
    assert!(catalog_opens_under_key(&d, &m), "CATALOG was re-sealed before the refusal");
    std::fs::remove_dir_all(&incoming).unwrap();
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w.files > 0 && w.failures.is_empty(), "{w:?}");
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}

/// A restore's or an import's staging directory left beside the
/// collections -- its files sealed under the final name, not the
/// directory's -- is passed over by a rotation and counted, the shard
/// beside it re-sealed under the new key alone, and the staging files
/// left as they were. Through 0.98.0 the walk took the directory for a
/// collection of that name, no file in it opened, and the rotation
/// stopped there after writing `KEY.next`.
#[test]
fn a_rotation_passes_over_a_staging_directory_and_reseals_the_shard_beside_it() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let m = master(23);
    let (d, all) = closed_db("rotate-staging", m);
    let shard = d.join("collections").join("items").join("shard-0000");
    copy_tree(&shard, &d.join("collections").join("items.restore.tmp").join("shard-0000"));
    copy_tree(&shard, &d.join("collections").join("items.import.tmp").join("shard-0000"));
    let before =
        celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m).unwrap();
    let staged =
        d.join("collections").join("items.restore.tmp").join("shard-0000").join("MANIFEST");
    let staged_bytes = std::fs::read(&staged).unwrap();
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert_eq!(w.leftovers, 2, "{w:?}");
    assert!(w.files > 0 && w.failures.is_empty(), "{w:?}");
    // The shard beside them is under the new key alone; the staging files
    // are as they were, under the old.
    let after =
        celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m).unwrap();
    let c = celastro::cipher::check_dir(&d, &after.without_previous()).unwrap();
    assert!(c.failures.is_empty() && c.leftovers == 2 && c.files == w.files, "{c:?}");
    assert_eq!(std::fs::read(&staged).unwrap(), staged_bytes, "a staging file was rewritten");
    let ids_of = celastro::cipher::Ids::new("items/shard-0000/MANIFEST", "shard-0000/MANIFEST");
    assert!(before.open_file(&ids_of, &staged_bytes).is_ok());
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}

/// A drop cut short leaves `<name>.dropping` beside the collections: a
/// rotation passes over it, and the next open completes the drop as it
/// would have. Through 0.98.0 the rotation took the directory for a
/// collection of that name and stopped on it after writing `KEY.next`,
/// and the open never ran to complete the drop.
#[test]
fn a_rotation_passes_over_a_drop_left_aside_and_the_next_open_completes_it() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let m = master(24);
    let d = dir("rotate-dropping");
    let all = {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        setup(&mut db, 20);
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY, n INT)").unwrap();
        db.execute("INSERT INTO notes VALUES ('{\"id\":\"a\",\"n\":1}')").unwrap();
        ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100")
    };
    let aside = d.join("collections").join("notes.dropping");
    std::fs::rename(d.join("collections").join("notes"), &aside).unwrap();
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert_eq!(w.leftovers, 1, "{w:?}");
    assert!(w.files > 0 && w.failures.is_empty(), "{w:?}");
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    assert!(db.execute("SHOW CATALOG notes").is_err(), "the interrupted drop was not completed");
    assert!(!aside.exists(), "the directory aside was not removed");
    assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}

/// `KEY.next` holds the new key and the previous one, so a rotation that
/// stopped after writing it is never undone by removing it: renaming it
/// over `KEY` opens the database under both keys -- the files re-sealed
/// before the stop and the rest -- and a later rotation finishes under a
/// fresh key with nothing already under it.
#[test]
fn a_rotation_cut_short_is_recovered_by_renaming_key_next_over_key() {
    let _env = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let m = master(25);
    let (d, all) = closed_db("rotate-recover", m);
    let old = celastro::cipher::Cipher::unwrap(&std::fs::read(d.join("KEY")).unwrap(), &m).unwrap();
    let mut fresh = celastro::cipher::Cipher::generate().unwrap();
    fresh.keep_previous(&old);
    std::fs::write(d.join("KEY.next"), fresh.wrap(&m).unwrap()).unwrap();
    // The stop as 0.98.0 left it: CATALOG and one segment under the new
    // key, everything else under the old.
    let reseal = |rel: PathBuf, ids: celastro::cipher::Ids| {
        let p = d.join(rel);
        let plain = old.open_file(&ids, &std::fs::read(&p).unwrap()).unwrap();
        std::fs::write(&p, fresh.seal_file(&ids, &plain).unwrap()).unwrap();
    };
    reseal(PathBuf::from("CATALOG"), celastro::cipher::Ids::same("CATALOG"));
    let segments = d.join("collections").join("items").join("shard-0000").join("segments");
    let seg = std::fs::read_dir(&segments)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .find(|n| n.ends_with(".seg"))
        .expect("a sealed segment");
    reseal(
        segments.join(&seg).strip_prefix(&d).unwrap().to_path_buf(),
        celastro::cipher::Ids::new(format!("items/shard-0000/{seg}"), format!("shard-0000/{seg}")),
    );
    let e = Db::open(&d, opts(Some(m))).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(e.contains("rotation was interrupted"), "{e}");
    std::fs::rename(d.join("KEY.next"), d.join("KEY")).unwrap();
    {
        let mut db = Db::open(&d, opts(Some(m))).unwrap();
        assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
    }
    let w = celastro::cipher::rotate_data_key(&d, &m, &[]).unwrap();
    assert!(w.files > 0 && w.already == 0 && w.failures.is_empty(), "{w:?}");
    assert_eq!(w.kept_keys, 2, "both keys kept behind the fresh one");
    let mut db = Db::open(&d, opts(Some(m))).unwrap();
    assert_eq!(ids(&mut db, "SELECT id FROM items ORDER BY id LIMIT 100"), all);
    drop(db);
    let _ = std::fs::remove_dir_all(&d);
}
