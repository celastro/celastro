//! The HTTP API: a thread per connection, one request per connection, JSON in
//! and out.
//!
//! ```text
//! GET    /health                               no token needed
//! GET    /collections                          names and document counts
//! PUT    /collections/{c}/docs/{id}            body: a JSON object
//! GET    /collections/{c}/docs/{id}
//! DELETE /collections/{c}/docs/{id}
//! GET    /collections/{c}/docs?where.{path}={value}&after={id}&limit={n}
//! ```

use crate::json::{self, quote};
use crate::store::{self, Store};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The most bytes a request's line and headers may take.
const MAX_HEAD: usize = 16 << 10;
/// The most bytes a request's body may take: a document and some room.
const MAX_BODY: usize = store::MAX_DOC + (64 << 10);
/// Connections served at once; past it a connection is closed unanswered.
const MAX_CONNECTIONS: usize = 128;
/// How long a client may take to send its request or read the answer.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LIMIT: usize = 100;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: Vec<String>,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

fn ok(body: String) -> Response {
    Response { status: 200, body }
}

fn fail(status: u16, message: &str) -> Response {
    Response {
        status,
        body: format!("{{\"ok\":false,\"error\":{}}}", quote(message)),
    }
}

/// Serve `listener` until the process ends. `token`, when set, is required as
/// `Authorization: Bearer <token>` on every request but `/health`.
pub fn serve(listener: TcpListener, store: Arc<Mutex<Store>>, token: Option<String>) {
    let active = Arc::new(AtomicUsize::new(0));
    let token = Arc::new(token);
    for conn in listener.incoming() {
        let stream = match conn {
            Ok(s) => s,
            Err(e) => {
                eprintln!("celastro: accept: {e}");
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if active.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
            active.fetch_sub(1, Ordering::SeqCst);
            continue;
        }
        let (store, token, active) = (store.clone(), token.clone(), active.clone());
        std::thread::spawn(move || {
            handle(stream, &store, token.as_deref());
            active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

fn handle(mut stream: TcpStream, store: &Mutex<Store>, token: Option<&str>) {
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let response = match read_request(&mut stream) {
        Ok(req) => route(&req, store, token),
        Err(r) => r,
    };
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        response.status,
        response.body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(response.body.as_bytes());
    let _ = stream.flush();
}

/// Read one request: the line and headers, then exactly `Content-Length`
/// bytes of body.
pub fn read_request(stream: &mut impl Read) -> Result<Request, Response> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(i) = find(&buf, b"\r\n\r\n") {
            if i > MAX_HEAD {
                return Err(fail(431, "the request's headers are too long"));
            }
            break i;
        }
        if buf.len() > MAX_HEAD {
            return Err(fail(431, "the request's headers are too long"));
        }
        let n = stream
            .read(&mut chunk)
            .map_err(|_| fail(400, "the request did not arrive"))?;
        if n == 0 {
            return Err(fail(400, "the request ended early"));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head =
        std::str::from_utf8(&buf[..head_end]).map_err(|_| fail(400, "headers are not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let line = lines.next().unwrap_or("");
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(fail(400, "malformed request line"));
    };
    if !version.starts_with("HTTP/1.") || parts.next().is_some() {
        return Err(fail(400, "malformed request line"));
    }
    let mut headers = Vec::new();
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            return Err(fail(400, "malformed header"));
        };
        headers.push((k.trim().to_string(), v.trim().to_string()));
    }
    let mut req = Request {
        method: method.to_string(),
        path: Vec::new(),
        query: Vec::new(),
        headers,
        body: Vec::new(),
    };
    if req.header("transfer-encoding").is_some() {
        return Err(fail(
            501,
            "chunked bodies are not supported; send Content-Length",
        ));
    }
    let len = match req.header("content-length") {
        None => 0,
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| fail(400, "malformed Content-Length"))?,
    };
    if len > MAX_BODY {
        return Err(fail(413, "the body is too large"));
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    for seg in path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
    {
        req.path
            .push(decode(seg, false).ok_or_else(|| fail(400, "malformed path"))?);
    }
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = decode(k, true).ok_or_else(|| fail(400, "malformed query"))?;
        let v = decode(v, true).ok_or_else(|| fail(400, "malformed query"))?;
        req.query.push((k, v));
    }
    let mut body = buf.split_off(head_end + 4);
    if body.len() > len {
        return Err(fail(400, "more body than Content-Length says"));
    }
    let have = body.len();
    body.resize(len, 0);
    stream
        .read_exact(&mut body[have..])
        .map_err(|_| fail(400, "the body ended early"))?;
    req.body = body;
    Ok(req)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Percent-decoding; in a query, `+` is a space.
fn decode(s: &str, plus_is_space: bool) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = s
                    .get(i + 1..i + 3)
                    .filter(|h| h.bytes().all(|c| c.is_ascii_hexdigit()))?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// Compare without stopping at the first difference.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn route(req: &Request, store: &Mutex<Store>, token: Option<&str>) -> Response {
    let path: Vec<&str> = req.path.iter().map(String::as_str).collect();
    if path == ["health"] {
        return if req.method == "GET" {
            ok("{\"ok\":true}".into())
        } else {
            fail(405, "GET only")
        };
    }
    if let Some(t) = token {
        let given = req
            .header("authorization")
            .and_then(|v| v.strip_prefix("Bearer "));
        if !given.is_some_and(|g| same(g.as_bytes(), t.as_bytes())) {
            return fail(401, "a token is required: Authorization: Bearer <token>");
        }
    }
    let mut store = match store.lock() {
        Ok(s) => s,
        Err(poisoned) => poisoned.into_inner(),
    };
    let result = match (req.method.as_str(), path.as_slice()) {
        ("GET", ["collections"]) => {
            let items: Vec<String> = store
                .collections()
                .iter()
                .map(|(n, count)| format!("{{\"name\":{},\"documents\":{count}}}", quote(n)))
                .collect();
            Ok(ok(format!(
                "{{\"ok\":true,\"collections\":[{}]}}",
                items.join(",")
            )))
        }
        ("PUT", ["collections", c, "docs", id]) => {
            let Ok(text) = std::str::from_utf8(&req.body) else {
                return fail(400, "the body is not UTF-8");
            };
            match json::parse(text) {
                Ok(doc) => store.put(c, id, &doc).map(|()| ok("{\"ok\":true}".into())),
                Err(e) => return fail(400, &format!("the body is not JSON: {e}")),
            }
        }
        ("GET", ["collections", c, "docs", id]) => match store.get(c, id) {
            Some(doc) => Ok(ok(format!(
                "{{\"ok\":true,\"id\":{},\"doc\":{doc}}}",
                quote(id)
            ))),
            None => Ok(fail(404, "no such document")),
        },
        ("DELETE", ["collections", c, "docs", id]) => store
            .delete(c, id)
            .map(|deleted| ok(format!("{{\"ok\":true,\"deleted\":{deleted}}}"))),
        ("GET", ["collections", c, "docs"]) => list(&store, c, &req.query),
        (_, ["collections"] | ["collections", _, "docs"] | ["collections", _, "docs", _]) => {
            return fail(405, "method not allowed here")
        }
        _ => return fail(404, "no such endpoint"),
    };
    match result {
        Ok(r) => r,
        Err(store::Error::Invalid(m)) => fail(400, &m),
        Err(store::Error::Io(m)) => fail(500, &m),
    }
}

fn list(store: &Store, c: &str, query: &[(String, String)]) -> store::Result<Response> {
    let mut filters = Vec::new();
    let mut after = None;
    let mut limit = DEFAULT_LIMIT;
    for (k, v) in query {
        if let Some(path) = k.strip_prefix("where.") {
            filters.push((path.to_string(), v.clone()));
        } else if k == "after" {
            after = Some(v.as_str());
        } else if k == "limit" {
            limit = v.parse().map_err(|_| {
                store::Error::Invalid(format!("limit is 1 to {}", store::MAX_LIMIT))
            })?;
        } else {
            return Err(store::Error::Invalid(format!(
                "unknown parameter `{k}`: use where.<path>, after or limit"
            )));
        }
    }
    let page = store.list(c, &filters, after, limit)?;
    let docs: Vec<String> = page
        .docs
        .iter()
        .map(|(id, doc)| format!("{{\"id\":{},\"doc\":{doc}}}", quote(id)))
        .collect();
    let next = page
        .next
        .as_deref()
        .map(quote)
        .unwrap_or_else(|| "null".into());
    Ok(ok(format!(
        "{{\"ok\":true,\"docs\":[{}],\"next\":{next}}}",
        docs.join(",")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(raw: &str) -> Result<Request, Response> {
        read_request(&mut raw.as_bytes())
    }

    #[test]
    fn a_request_is_read_with_its_path_query_and_body() {
        let r = req(
            "PUT /collections/notes/docs/a%2Fb?where.topic=a+b&x=%41 HTTP/1.1\r\n\
                     Host: x\r\nContent-Length: 2\r\n\r\n{}",
        )
        .unwrap();
        assert_eq!(r.method, "PUT");
        assert_eq!(r.path, ["collections", "notes", "docs", "a/b"]);
        assert_eq!(
            r.query,
            [
                ("where.topic".into(), "a b".into()),
                ("x".into(), "A".into())
            ]
        );
        assert_eq!(r.body, b"{}");
    }

    #[test]
    fn malformed_requests_are_refused_by_status() {
        let status = |raw: &str| req(raw).err().map(|r| r.status);
        assert_eq!(status("GET /\r\n\r\n"), Some(400));
        assert_eq!(status("GET / HTTP/1.1\r\nbad header\r\n\r\n"), Some(400));
        assert_eq!(
            status("PUT / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Some(501)
        );
        let big = format!("PUT / HTTP/1.1\r\nContent-Length: {}\r\n\r\n", MAX_BODY + 1);
        assert_eq!(status(&big), Some(413));
        assert_eq!(
            status("PUT / HTTP/1.1\r\nContent-Length: 5\r\n\r\n{}"),
            Some(400)
        );
        let long = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(MAX_HEAD + 10));
        assert_eq!(status(&long), Some(431));
        assert_eq!(status("GET /%zz HTTP/1.1\r\n\r\n"), Some(400));
    }

    #[test]
    fn the_token_comparison_needs_the_whole_token() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"ab"));
    }
}
