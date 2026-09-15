//! `BACKUP TO` and `RESTORE FROM` against a directory: what comes back is
//! what was there at the pin, a second backup copies only what is new, a
//! damaged destination is refused before a byte is written, an older
//! instant is a choice, and the console runs the copy with its lock let
//! go.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use celastro::engine::{Db, DbOpts, Outcome};
use celastro::value::Value;

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-backup-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn doc(i: usize) -> Value {
    Value::obj(vec![
        ("id".into(), Value::Str(format!("d-{i:04}"))),
        ("n".into(), Value::Int(i as i64)),
        ("body".into(), Value::Str(format!("row number {i} of the backup test"))),
    ])
}

fn setup(db: &mut Db, n: usize) {
    db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)").unwrap();
    db.execute("CREATE INDEX items_body ON items USING fulltext (body)").unwrap();
    db.execute("CREATE COLLECTION tags (id TEXT PRIMARY KEY)").unwrap();
    for i in 0..n {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
    for i in n..n + 5 {
        db.insert("items", doc(i)).unwrap(); // stays in memory: the fresh segment
    }
    db.execute("DELETE FROM items WHERE id = 'd-0001'").unwrap();
    db.insert("tags", Value::obj(vec![("id".into(), Value::Str("t1".into()))])).unwrap();
}

fn ack(db: &mut Db, sql: &str) -> String {
    match db.execute(sql).unwrap().finished().unwrap() {
        Outcome::Ack(m) => m,
        other => panic!("{other:?}"),
    }
}

fn ids(db: &mut Db, sql: &str) -> Vec<String> {
    let r = db.query(sql).unwrap();
    let mut out: Vec<String> = r.rows.iter().map(|row| format!("{:?}", row.key)).collect();
    out.sort();
    out
}

fn open(d: &Path) -> Db {
    Db::open(d, DbOpts::default()).unwrap()
}

#[test]
fn a_backup_restores_what_was_there_at_the_pin_and_a_second_one_copies_only_what_is_new() {
    let src = dir("src");
    let dest = dir("dest");
    let mut db = open(&src);
    setup(&mut db, 30);
    let before = ids(&mut db, "SELECT id FROM items LIMIT 1000");
    assert_eq!(before.len(), 34);
    let first = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    assert!(first.contains("2 collection(s), 2 shard(s)"), "{first}");
    assert!(first.contains("0 already there"), "{first}");
    let ts1: u64 = first.split_whitespace().nth(1).unwrap().parse().unwrap();
    // Writes after the pin belong to the source, not the backup.
    for i in 100..110 {
        db.insert("items", doc(i)).unwrap();
    }
    db.execute("FLUSH items").unwrap();
    let second = ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    assert!(second.contains("1 segment(s) copied"), "{second}");
    assert!(second.contains("1 already there"), "{second}");
    let ts2: u64 = second.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(ts2 > ts1);
    let mine = dest.join("nodes").join("local");
    assert!(mine.join("LATEST").exists());
    assert!(mine.join("backups").join(format!("{ts2:020}")).join("BACKUP").exists());

    // The newest one, then the older one by instant.
    let d2 = dir("dst2");
    let mut db2 = open(&d2);
    let r = ack(&mut db2, &format!("RESTORE FROM '{}'", dest.display()));
    assert!(r.contains(&format!("restored backup {ts2}")), "{r}");
    assert_eq!(ids(&mut db2, "SELECT id FROM items LIMIT 1000").len(), 44);
    assert_eq!(
        ids(&mut db2, "SELECT id FROM items WHERE text_match(body, 'number') LIMIT 1000").len(),
        44
    );
    assert!(
        ids(&mut db2, "SELECT id FROM items WHERE id = 'd-0001' LIMIT 1").is_empty(),
        "the delete came back"
    );
    assert_eq!(ids(&mut db2, "SELECT id FROM tags LIMIT 10").len(), 1);
    let e = db2.execute(&format!("RESTORE FROM '{}'", dest.display())).unwrap_err().to_string();
    assert!(e.contains("empty database"), "{e}");
    drop(db2);
    let mut db2 = open(&d2);
    assert_eq!(
        ids(&mut db2, "SELECT id FROM items LIMIT 1000").len(),
        44,
        "the restore is on disk"
    );
    db2.insert("items", doc(500)).unwrap();
    db2.execute("FLUSH items").unwrap();
    assert_eq!(
        ids(&mut db2, "SELECT id FROM items LIMIT 1000").len(),
        45,
        "the restored database writes"
    );

    let d3 = dir("dst3");
    let mut db3 = open(&d3);
    let r = ack(&mut db3, &format!("RESTORE FROM '{}' AS OF {ts1}", dest.display()));
    assert!(r.contains(&format!("restored backup {ts1}")), "{r}");
    assert_eq!(ids(&mut db3, "SELECT id FROM items LIMIT 1000"), before);

    for d in [&src, &dest, &d2, &d3] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn a_damaged_destination_is_refused_before_anything_is_written_and_paths_are_confined() {
    let src = dir("dmg-src");
    let dest = dir("dmg-dest");
    let mut db = open(&src);
    setup(&mut db, 10);
    ack(&mut db, &format!("BACKUP TO '{}'", dest.display()));
    let pool = dest.join("pool").join("items").join("shard-0000");
    let seg = std::fs::read_dir(&pool).unwrap().next().unwrap().unwrap().path();
    std::fs::write(&seg, b"short").unwrap();
    let d2 = dir("dmg-dst");
    let mut db2 = open(&d2);
    let e = db2.execute(&format!("RESTORE FROM '{}'", dest.display())).unwrap_err().to_string();
    assert!(e.contains("damaged") && e.contains("5 bytes"), "{e}");
    assert!(!d2.join("collections").join("items").exists(), "nothing was written");
    assert!(db2.catalog.collections.is_empty());
    std::fs::remove_file(&seg).unwrap();
    let e = db2.execute(&format!("RESTORE FROM '{}'", dest.display())).unwrap_err().to_string();
    assert!(e.contains("missing"), "{e}");
    let e = db2.execute("RESTORE FROM 'relative/name'").unwrap_err().to_string();
    assert!(e.contains("absolute"), "{e}");
    let e = db2
        .execute(&format!("RESTORE FROM '{}'", dir("nothing-here").display()))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("no backup of node `local`") && e.contains("nodes backed up there: none"),
        "{e}"
    );

    // With a backup directory set, names resolve under it and nothing climbs out.
    let base = dir("confined");
    let mut o = DbOpts::default();
    o.backup_dir = Some(base.clone());
    let src2 = dir("confined-src");
    let mut db3 = Db::open(&src2, o).unwrap();
    setup(&mut db3, 3);
    let m = ack(&mut db3, "BACKUP TO 'nightly'");
    assert!(m.contains(&base.join("nightly").display().to_string()), "{m}");
    assert!(base.join("nightly").join("nodes").join("local").join("LATEST").exists());
    for bad in ["../elsewhere", "/tmp/elsewhere", "a/../../b"] {
        let e = db3.execute(&format!("BACKUP TO '{bad}'")).unwrap_err().to_string();
        assert!(e.contains("outside") || e.contains("climb"), "{bad}: {e}");
    }
    let inside = base.join("deep").display().to_string();
    assert!(ack(&mut db3, &format!("BACKUP TO '{inside}'")).contains(&inside));
    for d in [&src, &dest, &d2, &base, &src2] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Through the console: `send` carries the statement, the copy runs after
/// the lock is let go (a query on another connection is answered while a
/// backup is in flight), and the restore of what it wrote counts.
#[test]
fn the_console_runs_a_backup_without_holding_its_lock_and_send_carries_the_statement() {
    let src = dir("console-src");
    let dest = dir("console-dest");
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro-cli"))
        .args(["--json", "--dir", src.to_str().unwrap(), "serve", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn celastro-cli");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
    let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();
    let post = |sql: &str| -> String {
        let body =
            celastro::json::to_string(&Value::obj(vec![("sql".into(), Value::Str(sql.into()))]));
        let mut s = TcpStream::connect(&addr).unwrap();
        write!(
            s,
            "POST /api/query?t={token} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut reply = String::new();
        s.read_to_string(&mut reply).unwrap();
        reply
    };
    assert!(post("CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT)").contains("\"ok\":true"));
    let docs: Vec<String> = (0..200)
        .map(|i| format!("('{}')", celastro::json::to_string(&doc(i)).replace('\'', "''")))
        .collect();
    assert!(post(&format!("INSERT INTO items VALUES {}", docs.join(","))).contains("\"ok\":true"));
    assert!(post("FLUSH items").contains("\"ok\":true"));

    let sent = Command::new(env!("CARGO_BIN_EXE_celastro-cli"))
        .env("CELASTRO_TOKEN", &token)
        .args(["send", &format!("http://{addr}"), &format!("BACKUP TO '{}'", dest.display())])
        .output()
        .unwrap();
    let out = String::from_utf8_lossy(&sent.stdout).to_string();
    assert!(sent.status.success(), "{out} {}", String::from_utf8_lossy(&sent.stderr));
    assert!(out.starts_with("backup ") && out.contains("1 collection(s)"), "{out}");
    assert!(
        post("SELECT id FROM items LIMIT 5").contains("\"ok\":true"),
        "the console still answers"
    );
    let _ = child.kill();
    let _ = child.wait();

    let d2 = dir("console-dst");
    let restored = Command::new(env!("CARGO_BIN_EXE_celastro-cli"))
        .args([
            "--dir",
            d2.to_str().unwrap(),
            "exec",
            &format!("RESTORE FROM '{}'", dest.display()),
        ])
        .output()
        .unwrap();
    let out = String::from_utf8_lossy(&restored.stdout).to_string();
    assert!(restored.status.success() && out.contains("restored backup"), "{out}");
    let mut db2 = open(&d2);
    assert_eq!(ids(&mut db2, "SELECT id FROM items LIMIT 1000").len(), 200);
    for d in [&src, &dest, &d2] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A node with an address backs up under its own name, so the pods of a
/// cluster share one destination; a node of another name finds nothing of
/// its own and is told what is there, and `NODE` takes the named one.
#[test]
fn nodes_back_up_side_by_side_and_each_restores_its_own_unless_told_otherwise() {
    let dest = dir("nodes-dest");
    let a = dir("nodes-a");
    let mut oa = DbOpts::default();
    oa.node = Some("tcp://a.example:9000".into());
    let mut dba = Db::open(&a, oa).unwrap();
    setup(&mut dba, 12);
    let m = ack(&mut dba, &format!("BACKUP TO '{}'", dest.display()));
    assert!(m.contains("backup "), "{m}");
    assert!(dest.join("nodes").join("a.example_9000").join("LATEST").exists());
    let b = dir("nodes-b");
    let mut ob = DbOpts::default();
    ob.node = Some("tcp://b.example:9000".into());
    let mut dbb = Db::open(&b, ob).unwrap();
    let e = dbb.execute(&format!("RESTORE FROM '{}'", dest.display())).unwrap_err().to_string();
    assert!(
        e.contains("no backup of node `b.example_9000`")
            && e.contains("nodes backed up there: a.example_9000"),
        "{e}"
    );
    let m =
        ack(&mut dbb, &format!("RESTORE FROM '{}' NODE 'tcp://a.example:9000'", dest.display()));
    assert!(m.contains("of node `a.example_9000`"), "{m}");
    assert_eq!(ids(&mut dbb, "SELECT id FROM items LIMIT 1000").len(), 16);
    for d in [&dest, &a, &b] {
        let _ = std::fs::remove_dir_all(d);
    }
}
