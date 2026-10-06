//! The API end to end: a server on a loopback port, raw HTTP against it, and a
//! restart over the same directory.

use celastro::{server, store::Store};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const TOKEN: &str = "0123456789abcdef";

fn start(dir: &PathBuf) -> String {
    let store = Store::open(dir).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let store = Arc::new(Mutex::new(store));
    std::thread::spawn(move || server::serve(listener, store, Some(TOKEN.into())));
    addr
}

/// One request; the status and the body.
fn call(addr: &str, method: &str, path: &str, body: &str, token: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let status = out[9..12].parse().unwrap();
    let body = out.split_once("\r\n\r\n").unwrap().1.to_string();
    (status, body)
}

fn api(addr: &str, method: &str, path: &str, body: &str) -> (u16, String) {
    call(addr, method, path, body, Some(TOKEN))
}

#[test]
fn documents_are_written_read_listed_deleted_and_kept_across_a_restart() {
    let dir = std::env::temp_dir().join(format!("celastro-http-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = start(&dir);

    assert_eq!(
        call(&addr, "GET", "/health", "", None),
        (200, r#"{"ok":true}"#.into())
    );
    assert_eq!(call(&addr, "GET", "/collections", "", None).0, 401);
    assert_eq!(call(&addr, "GET", "/collections", "", Some("wrong")).0, 401);

    for (id, topic) in [("n1", "search"), ("n2", "storage"), ("n3", "storage")] {
        let doc = format!(r#"{{"topic":"{topic}","words":7}}"#);
        assert_eq!(
            api(&addr, "PUT", &format!("/collections/notes/docs/{id}"), &doc).0,
            200
        );
    }
    assert_eq!(
        api(&addr, "GET", "/collections/notes/docs/n2", ""),
        (
            200,
            r#"{"ok":true,"id":"n2","doc":{"topic":"storage","words":7}}"#.into()
        )
    );
    assert_eq!(api(&addr, "GET", "/collections/notes/docs/nope", "").0, 404);
    let (status, body) = api(
        &addr,
        "GET",
        "/collections/notes/docs?where.topic=storage&limit=1",
        "",
    );
    assert_eq!(status, 200);
    assert_eq!(
        body,
        r#"{"ok":true,"docs":[{"id":"n2","doc":{"topic":"storage","words":7}}],"next":"n2"}"#
    );
    let (_, body) = api(
        &addr,
        "GET",
        "/collections/notes/docs?where.topic=storage&after=n2",
        "",
    );
    assert_eq!(
        body,
        r#"{"ok":true,"docs":[{"id":"n3","doc":{"topic":"storage","words":7}}],"next":null}"#
    );
    assert_eq!(
        api(&addr, "DELETE", "/collections/notes/docs/n1", ""),
        (200, r#"{"ok":true,"deleted":true}"#.into())
    );
    assert_eq!(
        api(&addr, "PUT", "/collections/notes/docs/bad", "{nope").0,
        400
    );
    assert_eq!(
        api(&addr, "PUT", "/collections/notes/docs/bad", "[1]").0,
        400
    );
    assert_eq!(
        api(&addr, "PUT", "/collections/..%2Fetc/docs/x", "{}").0,
        400
    );
    assert_eq!(
        api(&addr, "GET", "/collections/notes/docs?sort=x", "").0,
        400
    );
    assert_eq!(
        api(&addr, "POST", "/collections/notes/docs/n2", "{}").0,
        405
    );
    assert_eq!(api(&addr, "GET", "/nowhere", "").0, 404);
    assert_eq!(
        api(&addr, "GET", "/collections", ""),
        (
            200,
            r#"{"ok":true,"collections":[{"name":"notes","documents":2}]}"#.into()
        )
    );

    // A second server over the same directory is refused while the first holds it.
    assert!(Store::open(&dir).is_err());
}

#[test]
fn a_reopened_directory_answers_what_was_acknowledged() {
    let dir = std::env::temp_dir().join(format!("celastro-http-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let mut s = Store::open(&dir).unwrap();
        let doc = celastro::json::parse(r#"{"a":1}"#).unwrap();
        s.put("c", "k", &doc).unwrap();
    }
    let addr = start(&dir);
    assert_eq!(
        api(&addr, "GET", "/collections/c/docs/k", ""),
        (200, r#"{"ok":true,"id":"k","doc":{"a":1}}"#.into())
    );
}
