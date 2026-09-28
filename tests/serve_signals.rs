//! `serve` is a well-behaved PID 1: a SIGTERM ends it cleanly, promptly, and
//! after the last acknowledged write is on the disk. No container is needed
//! to show that -- the real binary, a real signal, and a reopen.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use celastro::engine::{Db, DbOpts};

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
const SIGTERM: i32 = 15;

/// The process used to have no handler at all, so as PID 1 it never received
/// a SIGTERM and `docker stop` killed it after ten seconds: exit 137. It now
/// exits 0 within a moment, and the collection created over HTTP just before
/// the signal is there when the directory is reopened, because the caller
/// saves after the accept loop returns. A handler that set the flag but a
/// loop that blocked in `accept` would pass the first assertion only once the
/// next connection arrived, which is what the bound on the exit time is for.
#[test]
fn sigterm_shuts_the_console_down_cleanly_and_the_last_write_survives() {
    let dir = std::env::temp_dir().join(format!("celastro-sigterm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro"))
        .args(["--json", "--dir", dir.to_str().unwrap(), "serve", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn celastro");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
    let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();

    let body = r#"{"sql":"CREATE COLLECTION notes (id TEXT PRIMARY KEY)"}"#;
    let mut s = TcpStream::connect(&addr).unwrap();
    write!(
        s,
        "POST /api/query HTTP/1.1\r\nHost: {addr}\r\nX-Celastro-Token: {token}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut reply = String::new();
    s.read_to_string(&mut reply).unwrap();
    assert!(reply.contains("\"ok\":true"), "{reply}");
    drop(s);

    let t0 = Instant::now();
    assert_eq!(unsafe { kill(child.id() as i32, SIGTERM) }, 0, "kill failed");
    let status = loop {
        if let Some(st) = child.try_wait().unwrap() {
            break st;
        }
        if t0.elapsed() > Duration::from_secs(5) {
            let _ = child.kill();
            panic!("serve did not exit within five seconds of SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let took = t0.elapsed();
    assert!(status.success(), "exited {status:?}");
    assert!(took < Duration::from_secs(2), "a clean exit took {took:?}");

    let db = Db::open(&dir, DbOpts::default()).unwrap();
    assert!(db.shards("notes").is_ok(), "the collection created before the signal was not saved");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A request pays nothing to be accepted. Until 0.29.1 the accept loop
/// slept 25 ms between polls of the listener, so a connection waited 12.5
/// ms on average before its first byte was read, and every statement at
/// concurrency one cost that -- a point lookup measured 25 ms. Forty
/// sequential health requests over fresh connections now average well under
/// that; the bound is loose enough for a loaded machine and tight enough
/// that the sleep cannot pass it.
/// A peer named in `CELASTRO_ATTACH` is attached: two processes, the
/// second told of the first, and the second's health says both answer.
/// The dial runs after `serve` has scrubbed the secrets from the
/// environment, and from 0.87.0 to 0.91.0 it read the wire token from
/// there and found nothing, so no chart or role deployment ever formed a
/// cluster.
#[test]
fn a_peer_named_in_the_environment_is_attached() {
    let free = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let (wire_a, wire_b) = (free(), free());
    let root = std::env::temp_dir().join(format!("celastro-attach-env-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let start = |name: &str, wire: u16, attach: Option<u16>| {
        let dir = root.join(name);
        let mut c = Command::new(env!("CARGO_BIN_EXE_celastro"));
        c.args([
            "--json",
            "--dir",
            dir.to_str().unwrap(),
            "serve",
            "--port",
            "0",
            "--shard-bind",
            &format!("127.0.0.1:{wire}"),
        ])
        .env("CELASTRO_WIRE_TOKEN", "the-wire-token-every-node-shares")
        .env("CELASTRO_NODE", format!("tcp://127.0.0.1:{wire}"))
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
        if let Some(p) = attach {
            c.env("CELASTRO_ATTACH", format!("tcp://127.0.0.1:{p}"));
        }
        let mut child = c.spawn().expect("spawn celastro");
        let mut first = String::new();
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
        let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
        let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
        let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();
        (child, addr, token)
    };
    let (mut a, _, _) = start("a", wire_a, None);
    let (mut b, addr, token) = start("b", wire_b, Some(wire_a));
    let health = || {
        let body = r#"{"sql":"SHOW HEALTH"}"#;
        let mut s = TcpStream::connect(&addr).unwrap();
        write!(
            s,
            "POST /api/query HTTP/1.1\r\nHost: {addr}\r\nX-Celastro-Token: {token}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut reply = String::new();
        s.read_to_string(&mut reply).unwrap();
        reply
    };
    let t0 = Instant::now();
    let mut last = String::new();
    while t0.elapsed() < Duration::from_secs(30) {
        last = health();
        if last.contains("2 of 2 node(s) answer") {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let _ = a.kill();
    let _ = b.kill();
    let _ = a.wait();
    let _ = b.wait();
    let _ = std::fs::remove_dir_all(&root);
    assert!(last.contains("2 of 2 node(s) answer"), "the peer never attached: {last}");
}

#[test]
fn a_connection_is_accepted_when_it_arrives_and_not_when_a_clock_says() {
    let dir = std::env::temp_dir().join(format!("celastro-accept-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro"))
        .args(["--json", "--dir", dir.to_str().unwrap(), "serve", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn celastro");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let addr = hello.get("addr").and_then(|v| v.as_str()).unwrap().to_string();
    let token = hello.get("token").and_then(|v| v.as_str()).unwrap().to_string();
    let health = || {
        let mut s = TcpStream::connect(&addr).unwrap();
        write!(
            s,
            "GET /api/health HTTP/1.1\r\nHost: {addr}\r\nX-Celastro-Token: {token}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut reply = String::new();
        s.read_to_string(&mut reply).unwrap();
        assert!(reply.contains("\"ok\":true"), "{reply}");
    };
    health();
    let t0 = Instant::now();
    for _ in 0..40 {
        health();
    }
    let mean = t0.elapsed() / 40;
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(mean < Duration::from_millis(8), "a request averaged {mean:?}");
}
