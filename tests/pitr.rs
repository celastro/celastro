//! A restore to any instant: the newest backup at or before it, then the
//! archived write-ahead logs replayed up to it -- the logs a seal rotated
//! and the live log `BACKUP LOG` copied -- and the answer says how far it
//! reached.

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::memtable::FlushThresholds;
use celastro::value::Value;
use std::path::{Path, PathBuf};

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-pitr-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn ack(db: &mut Db, sql: &str) -> String {
    match db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")).finished().unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{sql}: {other:?}"),
    }
}

fn insert(db: &mut Db, from: usize, to: usize) {
    for i in from..to {
        let doc = Value::obj(vec![
            ("id".into(), Value::Str(format!("k{i:03}"))),
            ("n".into(), Value::Int(i as i64)),
        ]);
        db.insert("items", doc).unwrap();
    }
}

fn ids(db: &mut Db) -> Vec<String> {
    let r = db.query("SELECT id FROM items LIMIT 1000").unwrap();
    let mut out: Vec<String> = r.rows.iter().map(|row| format!("{:?}", row.key)).collect();
    out.sort();
    out
}

fn expected(rows: impl Iterator<Item = usize>) -> Vec<String> {
    let mut out: Vec<String> = rows.map(|i| format!("{:?}", format!("k{i:03}"))).collect();
    out.sort();
    out
}

/// A fresh database restored from `dest` as of `t`: the answer and what
/// it holds.
fn restore_at(data: &Path, dest: &Path, as_of: u64) -> (String, Vec<String>) {
    let _ = std::fs::remove_dir_all(data);
    let mut db = Db::open(data, DbOpts::default()).unwrap();
    let m = ack(&mut db, &format!("RESTORE FROM '{}' AS OF {as_of}", dest.display()));
    let held = ids(&mut db);
    (m, held)
}

#[test]
fn a_restore_reaches_any_instant_the_archive_covers_and_says_where_it_stopped() {
    let dest = dir("dest");
    std::fs::create_dir_all(&dest).unwrap();
    let data = dir("data");
    let mut opts = DbOpts::default();
    opts.log_archive = Some(dest.display().to_string());
    let mut db = Db::open(&data, opts).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    insert(&mut db, 0, 10);
    let backup = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    let base: u64 = backup.split_whitespace().nth(1).unwrap().parse().unwrap();
    // After the backup: rows, an instant inside them, a seal that rotates
    // the log into the archive, more rows and a delete, the live log
    // copied, and rows the archive never sees.
    insert(&mut db, 10, 15);
    let t15 = db.now_ts();
    insert(&mut db, 15, 20);
    let t20 = db.now_ts();
    ack(&mut db, "FLUSH items");
    insert(&mut db, 20, 30);
    ack(&mut db, "DELETE FROM items WHERE id = 'k003'");
    let t30 = db.now_ts();
    let m = ack(&mut db, &format!("BACKUP LOG TO '{}'", dest.display()));
    assert!(m.starts_with("archived the live log of 1 shard(s)"), "{m}");
    insert(&mut db, 30, 40);
    let t40 = db.now_ts();
    assert_eq!(ids(&mut db).len(), 39);
    drop(db);

    let fresh = dir("fresh");
    // Exactly the backup: the backup, as it was.
    let (m, held) = restore_at(&fresh, &dest, base);
    assert_eq!(held, expected(0..10), "{m}");
    assert!(!m.contains("replayed"), "{m}");
    // Inside the rotated log: cut at the instant.
    let (m, held) = restore_at(&fresh, &dest, t15);
    assert_eq!(held, expected(0..15), "{m}");
    assert!(m.contains(&format!("1 archived log(s) replayed, reaching {t15}")), "{m}");
    assert!(!m.contains("archive ends"), "{m}");
    // The rotation whole, then the live log's copy with the delete in it.
    let (m, held) = restore_at(&fresh, &dest, t20);
    assert_eq!(held, expected(0..20), "{m}");
    assert!(m.contains(&format!("reaching {t20}")) && !m.contains("archive ends"), "{m}");
    let (m, held) = restore_at(&fresh, &dest, t30);
    assert_eq!(held, expected((0..30).filter(|i| *i != 3)), "{m}");
    assert!(m.contains("2 archived log(s) replayed"), "{m}");
    // Past the archive's end: as far as it goes, and the answer says so.
    let (m, held) = restore_at(&fresh, &dest, t40);
    assert_eq!(held, expected((0..30).filter(|i| *i != 3)), "{m}");
    assert!(m.contains(&format!("of the {t40} asked: the archive ends there")), "{m}");
    // The restored database goes on, on a timeline of its own: its first
    // archived log opens it, and nothing archived was replaced.
    let mut db = Db::open(&fresh, {
        let mut o = DbOpts::default();
        o.log_archive = Some(dest.display().to_string());
        o
    })
    .unwrap();
    let logs_of = |d: &Path| -> Vec<std::ffi::OsString> {
        std::fs::read_dir(d.join("nodes/local/logs/items/shard-0000"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n.to_string_lossy().ends_with(".log"))
            .collect()
    };
    let before = logs_of(&dest);
    insert(&mut db, 40, 45);
    ack(&mut db, "FLUSH items");
    let mut after = logs_of(&dest);
    assert_eq!(after.len(), before.len() + 1, "{after:?}");
    after.retain(|n| !before.contains(n));
    let newest = after[0].to_string_lossy().to_string();
    assert!(
        newest.starts_with("0001-"),
        "{newest} is not on the fork's timeline; before: {before:?}"
    );
    assert!(dest.join("nodes/local/logs/items/shard-0000/timeline-0001").exists());
    drop(db);
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&dest);
    let _ = std::fs::remove_dir_all(&fresh);
}

#[test]
fn a_gap_in_the_archived_sequence_stops_the_restore_before_it() {
    let dest = dir("gap-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let data = dir("gap-data");
    let mut opts = DbOpts::default();
    opts.log_archive = Some(dest.display().to_string());
    let mut db = Db::open(&data, opts).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    insert(&mut db, 0, 5);
    ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    insert(&mut db, 5, 10);
    ack(&mut db, "FLUSH items");
    insert(&mut db, 10, 15);
    ack(&mut db, "FLUSH items");
    insert(&mut db, 15, 20);
    ack(&mut db, "FLUSH items");
    let t20 = db.now_ts();
    drop(db);
    // The second log lost from the archive: what follows it cannot be
    // claimed, and the answer says how far the restore got.
    let logs = dest.join("nodes/local/logs/items/shard-0000");
    let mut names: Vec<_> = std::fs::read_dir(&logs).unwrap().map(|e| e.unwrap().path()).collect();
    names.sort();
    assert_eq!(names.len(), 3, "{names:?}");
    std::fs::remove_file(&names[1]).unwrap();
    let fresh = dir("gap-fresh");
    let (m, held) = restore_at(&fresh, &dest, t20);
    assert_eq!(held, expected(0..10), "the first log only: {m}");
    assert!(m.contains("1 archived log(s) replayed"), "{m}");
    assert!(m.contains("the archive ends there"), "{m}");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&dest);
    let _ = std::fs::remove_dir_all(&fresh);
}

/// The archived logs of the one shard, by name.
fn archived(dest: &Path) -> Vec<String> {
    let logs = dest.join("nodes/local/logs/items/shard-0000");
    let mut out: Vec<String> = match std::fs::read_dir(&logs) {
        Ok(rd) => rd.map(|e| e.unwrap().file_name().to_string_lossy().to_string()).collect(),
        Err(_) => Vec::new(),
    };
    out.sort();
    out
}

#[test]
fn a_seal_archives_the_log_it_rotated_before_the_install_removes_it_and_keeps_it_when_the_archive_refuses(
) {
    let dest = dir("seal-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let data = dir("seal-data");
    let with_archive = |max_bytes: usize| {
        let mut o = DbOpts::default();
        o.log_archive = Some(dest.display().to_string());
        o.background_seal = true;
        let mut thresholds = FlushThresholds::default();
        thresholds.max_bytes = max_bytes;
        o.thresholds = thresholds;
        o
    };
    let mut db = Db::open(&data, with_archive(64 << 20)).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    insert(&mut db, 0, 10);
    ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    drop(db);
    // Reopened with a memtable that freezes for the sealer at the first
    // row: the live log, holding the ten rows and the one, is rotated.
    let mut db = Db::open(&data, with_archive(1)).unwrap();
    insert(&mut db, 10, 11);
    let job = db.seal_reserve().expect("the frozen memtable");
    let shard = data.join("collections/items/shard-0000");
    let rotated = |d: &Path| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(d)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("wal.") && n != "wal.log")
            .collect();
        v.sort();
        v
    };
    assert_eq!(rotated(&shard), vec!["wal.000001.log".to_string()]);
    assert!(archived(&dest).is_empty(), "{:?}", archived(&dest));
    // The archive refuses: the build fails before a segment is written,
    // the log stays where it was, and the seal waits to be tried again.
    let node = dest.join("nodes/local");
    let mut ro = std::fs::metadata(&node).unwrap().permissions();
    let rw = ro.clone();
    std::os::unix::fs::PermissionsExt::set_mode(&mut ro, 0o555);
    std::fs::set_permissions(&node, ro).unwrap();
    let refused = Db::seal_build(&job);
    std::fs::set_permissions(&node, rw).unwrap();
    let err = match refused {
        Ok(_) => panic!("the build went through with the archive refusing"),
        Err(e) => e,
    };
    db.seal_requeue(job, &err);
    assert_eq!(db.seal_failures().0, 1);
    assert_eq!(rotated(&shard), vec!["wal.000001.log".to_string()]);
    assert!(archived(&dest).is_empty());
    // Tried again: archived by the build, and only then removed by the
    // install.
    let job = db.seal_reserve().expect("the seal put back");
    let built = Db::seal_build(&job).unwrap();
    let names = archived(&dest);
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].starts_with("000001-"), "{names:?}");
    assert_eq!(rotated(&shard), vec!["wal.000001.log".to_string()]);
    assert!(db.seal_install(job, built).unwrap());
    assert!(rotated(&shard).is_empty());
    let t = db.now_ts();
    drop(db);
    // The log straddles the backup's instant: what the backup holds is
    // not applied again, the row after it is.
    let fresh = dir("seal-fresh");
    let (m, held) = restore_at(&fresh, &dest, t);
    assert_eq!(held, expected(0..11), "{m}");
    assert!(m.contains("1 archived log(s) replayed"), "{m}");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&dest);
    let _ = std::fs::remove_dir_all(&fresh);
}

/// A restore forks a timeline of its own: what the restored node writes
/// is archived apart from the run it left, and a later restore -- from
/// the same backup, to an instant after the fork -- follows the chain of
/// timelines: the old run to the fork, the new one after, and none of
/// what the old run wrote past the fork.
#[test]
fn a_restore_forks_a_timeline_and_a_later_restore_follows_it() {
    let dest = dir("tl-dest");
    std::fs::create_dir_all(&dest).unwrap();
    let data = dir("tl-data");
    let with_archive = |d: &Path| {
        let mut o = DbOpts::default();
        o.log_archive = Some(d.display().to_string());
        o
    };
    let mut db = Db::open(&data, with_archive(&dest)).unwrap();
    ack(&mut db, "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)");
    insert(&mut db, 0, 10);
    ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    insert(&mut db, 10, 15);
    let t15 = db.now_ts();
    insert(&mut db, 15, 20);
    let t20 = db.now_ts();
    ack(&mut db, "FLUSH items");
    insert(&mut db, 20, 25);
    let t25 = db.now_ts();
    insert(&mut db, 25, 30);
    ack(&mut db, &format!("BACKUP LOG TO '{}'", dest.display()));
    drop(db);
    // The first restore, to t20: rows 0..19, and a fork recorded in the
    // shard -- not in the archive yet, which still reads as one run.
    let fresh = dir("tl-fresh");
    let (m, held) = restore_at(&fresh, &dest, t20);
    assert_eq!(held, expected(0..20), "{m}");
    let marker = dest.join("nodes/local/logs/items/shard-0000/timeline-0001");
    assert!(!marker.exists(), "a restore that wrote nothing forked the archive");
    let archived_file = fresh.join("collections/items/shard-0000/ARCHIVED");
    let text = std::fs::read_to_string(&archived_file).unwrap();
    assert!(text.starts_with(&format!("fork 0 {t20} ")), "{text}");
    // It goes on: its first archived log opens timeline 1 in the archive,
    // forked from 0 at t20.
    let mut db = Db::open(&fresh, with_archive(&dest)).unwrap();
    insert(&mut db, 100, 105);
    ack(&mut db, "FLUSH items");
    let t105 = db.now_ts();
    drop(db);
    let text = std::fs::read_to_string(&marker).expect("the fork's marker");
    assert!(text.starts_with(&format!("0 {t20}")), "{text}");
    let names = archived(&dest);
    assert!(names.iter().any(|n| n.starts_with("0001-")), "{names:?}");
    // From the same backup to an instant after the fork: the old run to
    // the fork, the new one after it.
    let fresh2 = dir("tl-fresh2");
    let (m, held) = restore_at(&fresh2, &dest, t105);
    assert_eq!(held, expected((0..20).chain(100..105)), "{m}");
    // To an instant the old run wrote at, past the fork: the fork's
    // history, not the rows the old run went on to write.
    let (m, held) = restore_at(&fresh2, &dest, t25);
    assert_eq!(held, expected(0..20), "{m}");
    // Before the fork: as ever.
    let (m, held) = restore_at(&fresh2, &dest, t15);
    assert_eq!(held, expected(0..15), "{m}");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&dest);
    let _ = std::fs::remove_dir_all(&fresh);
    let _ = std::fs::remove_dir_all(&fresh2);
}
