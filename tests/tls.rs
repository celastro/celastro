//! Encryption in transit: the console and the wire serve TLS when given
//! certificates, a client with the CA is answered, a client without one is
//! not, and the material is all three files or none.

use celastro::lock::RwLock;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use celastro::engine::{Db, DbOpts};
use celastro::serve::Server;
use celastro::tls::{self, Tls, CA_ENV, CERT_ENV, KEY_ENV};

/// The environment is process-wide; the tests take turns.
static ENV: Mutex<()> = Mutex::new(());

/// Where this process's test material lives: a CA and a `localhost`
/// certificate it signed, made once by the crate's own generator.
fn fixture(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("celastro-tls-fixtures-{}", std::process::id()));
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
        std::fs::write(dir.join("README"), "test material, made by the crate\n").unwrap();
    }
    dir.join(name).display().to_string()
}

/// The test CA and the `localhost` certificate it signed, from the
/// environment as `celastro` reads them.
fn material() -> Arc<Tls> {
    std::env::set_var(CERT_ENV, fixture("localhost.crt"));
    std::env::set_var(KEY_ENV, fixture("localhost.key"));
    std::env::set_var(CA_ENV, fixture("ca.crt"));
    let t = Tls::from_env().expect("the material parses").expect("all three are set");
    Arc::new(t)
}

#[test]
fn the_console_serves_tls_and_answers_only_a_client_that_verifies_it() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let tls = material();
    let server = Server::bind(0).unwrap().with_tls(Some(tls.clone()));
    assert!(server.url().starts_with("https://127.0.0.1:"));
    let port = server.local_addr().port();
    let token = server.token().to_string();
    let db = Arc::new(RwLock::new(Db::in_memory()));
    let serving = {
        let db = db.clone();
        std::thread::spawn(move || server.run(&db))
    };
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let request = |path: &str| {
        format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-Celastro-Token: {token}\r\n\
             Connection: close\r\n\r\n"
        )
    };

    // With the CA, by the name the certificate carries.
    let sock = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut s = tls::connect(Some(&tls), sock, "localhost").unwrap();
    s.write_all(request("/api/health").as_bytes()).unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    assert!(raw.starts_with("HTTP/1.1 200 "), "{raw}");
    assert!(raw.contains(r#""ok":true"#), "{raw}");

    // The probe a container runs is the same client.
    assert!(celastro::serve::probe_health(port, Some(&tls)).unwrap());

    // A connection after the first resumes on the ticket the first brought
    // back: one round trip, no certificate, and the server counts it.
    let before = tls::resumed_handshakes();
    let sock = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut s = tls::connect(Some(&tls), sock, "localhost").unwrap();
    s.write_all(request("/api/health").as_bytes()).unwrap();
    let mut again = String::new();
    s.read_to_string(&mut again).unwrap();
    assert!(again.starts_with("HTTP/1.1 200 "), "{again}");
    assert_eq!(tls::resumed_handshakes(), before + 1, "the handshake resumed");

    // With the CA but the wrong name: refused by the client before a byte
    // of HTTP is sent.
    let sock = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut s = tls::connect(Some(&tls), sock, "elsewhere.example").unwrap();
    let wrong = s
        .write_all(request("/api/health").as_bytes())
        .and_then(|_| s.read_to_string(&mut String::new()));
    assert!(wrong.is_err(), "a name the certificate does not carry must not verify");

    // Plain HTTP to a TLS console: no status line comes back.
    let mut plain = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    plain.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    plain.write_all(request("/api/health").as_bytes()).unwrap();
    let mut raw = Vec::new();
    let _ = plain.read_to_end(&mut raw);
    assert!(!raw.starts_with(b"HTTP/"), "a plain client got an HTTP answer: {raw:?}");
    assert!(
        !celastro::serve::probe_health(port, None).unwrap_or(false),
        "a plain probe is not well"
    );

    let sock = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    let mut s = tls::connect(Some(&tls), sock, "localhost").unwrap();
    let shutdown = format!(
        "POST /api/shutdown HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-Celastro-Token: {token}\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(shutdown.as_bytes()).unwrap();
    let _ = s.read_to_string(&mut String::new());
    serving.join().unwrap().unwrap();
}

#[test]
fn the_wire_serves_tls_and_a_node_without_the_ca_cannot_attach() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let tls = material();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("tcp://localhost:{port}");
    let mut opts = DbOpts::default();
    opts.node = Some(url.clone());
    opts.tls = Some(tls.clone());
    let db = Arc::new(RwLock::new(Db::with_opts(opts)));
    let stop = Arc::new(AtomicBool::new(false));
    {
        let (d, s, t) = (db.clone(), stop.clone(), tls.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, "wire-tls-token".to_string(), s, Some(t)).unwrap()
        });
    }
    // A node with the CA, dialling by the name in the certificate.
    let peer = celastro::wire::Node::new(&url, Some("wire-tls-token"), Some(tls.clone())).unwrap();
    let hello = peer.hello().unwrap();
    assert_eq!(hello.node.as_deref(), Some(url.as_str()));
    assert_eq!(hello.version, env!("CARGO_PKG_VERSION"));
    // The same node without certificates: plain frames into a TLS listener
    // are not answered, and the error names the call, not a hang.
    let plain = celastro::wire::Node::new(&url, Some("wire-tls-token"), None).unwrap();
    let refused = plain.hello();
    assert!(refused.is_err(), "a plain node must not be served by a TLS wire");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// A peer that connects to a TLS wire and says nothing for longer than the
/// serve loop's idle poll, then sends a plain frame carrying the token, is
/// not answered: the handshake runs first under its own timeout and a
/// failed one leaves the stream dead. Before 0.83.0 the poll's timeout
/// failed the handshake, the loop took the timeout for an idle
/// connection, and the frame was read and answered in the clear.
#[test]
fn a_peer_silent_past_the_idle_poll_is_not_served_in_the_clear() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let tls = material();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("tcp://localhost:{port}");
    let mut opts = DbOpts::default();
    opts.node = Some(url.clone());
    opts.tls = Some(tls.clone());
    let db = Arc::new(RwLock::new(Db::with_opts(opts)));
    let stop = Arc::new(AtomicBool::new(false));
    {
        let (d, s, t) = (db.clone(), stop.clone(), tls.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, "wire-tls-token".to_string(), s, Some(t)).unwrap()
        });
    }
    // A plain hello frame with the right token, as a node without TLS
    // would send it, captured by sending one into a listener of our own.
    // The sender is not waited for: a hello is a read, and a read whose
    // connection was closed under it is dialled again, for as long as
    // the wire's own timeouts allow; the thread ends with the process.
    let capture = TcpListener::bind("127.0.0.1:0").unwrap();
    let cport = capture.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let plain = celastro::wire::Node::new(
            &format!("tcp://127.0.0.1:{cport}"),
            Some("wire-tls-token"),
            None,
        )
        .unwrap();
        let _ = plain.hello();
    });
    let (mut from_plain, _) = capture.accept().unwrap();
    from_plain.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut frame = Vec::new();
    let mut chunk = [0u8; 4096];
    let n = from_plain.read(&mut chunk).unwrap();
    frame.extend_from_slice(&chunk[..n]);
    drop(from_plain);
    drop(capture);
    assert!(frame.len() > 8, "a frame was captured: {} byte(s)", frame.len());

    let mut raw = TcpStream::connect(("127.0.0.1", port)).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    raw.write_all(&frame).unwrap();
    raw.flush().unwrap();
    // Whatever comes back is a TLS alert or nothing, never a wire answer:
    // a wire answer to a hello starts with a zero status byte after the
    // length and carries this node's url; an alert record starts 0x15.
    let mut back = Vec::new();
    let _ = raw.read_to_end(&mut back);
    assert!(
        back.is_empty() || back[0] == 0x15,
        "answered in the clear: {} byte(s), first {:?}",
        back.len(),
        &back[..back.len().min(8)]
    );
    assert!(
        !String::from_utf8_lossy(&back).contains("localhost"),
        "the answer carried this node's url in the clear"
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn certificates_are_all_three_or_none_and_a_bad_file_is_named() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    std::env::remove_var(CERT_ENV);
    std::env::remove_var(KEY_ENV);
    std::env::remove_var(CA_ENV);
    assert!(Tls::from_env().unwrap().is_none(), "nothing set is plain");
    std::env::set_var(CERT_ENV, fixture("localhost.crt"));
    let half = Tls::from_env();
    assert!(half.unwrap_err().to_string().contains("all three"), "one of three is refused");
    std::env::set_var(KEY_ENV, fixture("localhost.key"));
    std::env::set_var(CA_ENV, fixture("nowhere.crt"));
    let missing = Tls::from_env().unwrap_err().to_string();
    assert!(missing.contains("nowhere.crt"), "{missing}");
    std::env::set_var(CA_ENV, fixture("README"));
    let not_pem = Tls::from_env().unwrap_err().to_string();
    assert!(not_pem.contains("README"), "{not_pem}");
    std::env::remove_var(CERT_ENV);
    std::env::remove_var(KEY_ENV);
    std::env::remove_var(CA_ENV);
}

/// Material made for a test with a given lifetime, under its own directory.
fn material_for(tag: &str, days: i64) -> Arc<Tls> {
    let dir = std::env::temp_dir().join(format!("celastro-tls-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let m = celastro::tls::make_material(
        "localhost",
        &["localhost".to_string()],
        &["127.0.0.1".parse().unwrap()],
        days,
    )
    .unwrap();
    std::fs::write(dir.join("ca.crt"), m.ca_cert).unwrap();
    std::fs::write(dir.join("localhost.crt"), m.cert).unwrap();
    std::fs::write(dir.join("localhost.key"), m.key).unwrap();
    std::env::set_var(CERT_ENV, dir.join("localhost.crt"));
    std::env::set_var(KEY_ENV, dir.join("localhost.key"));
    std::env::set_var(CA_ENV, dir.join("ca.crt"));
    Arc::new(Tls::from_env().unwrap().unwrap())
}

/// At its certificate's end a node loses every peer at once: the peer's
/// refusal names the time, and SHOW HEALTH and the metrics say when that
/// is, and say so two weeks ahead.
#[test]
fn an_expired_certificate_is_refused_by_a_peer_and_named_by_health_ahead_of_time() {
    let _turn = ENV.lock().unwrap_or_else(|p| p.into_inner());
    let expired = material_for("expired", -1);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("tcp://localhost:{port}");
    let mut opts = DbOpts::default();
    opts.node = Some(url.clone());
    opts.tls = Some(expired.clone());
    let db = Arc::new(RwLock::new(Db::with_opts(opts)));
    let stop = Arc::new(AtomicBool::new(false));
    {
        let (d, s, t) = (db.clone(), stop.clone(), expired.clone());
        std::thread::spawn(move || {
            celastro::wire::serve(listener, d, "wire-tls-token".to_string(), s, Some(t)).unwrap()
        });
    }
    let peer =
        celastro::wire::Node::new(&url, Some("wire-tls-token"), Some(expired.clone())).unwrap();
    let e = peer.hello().unwrap_err().to_string();
    assert!(e.contains("not valid at this time"), "{e}");
    let h = db.read().unwrap().show_health();
    assert!(h.contains("tls: certificate expires") && h.contains("EXPIRED"), "{h}");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    let soon = material_for("soon", 3);
    let mut opts = DbOpts::default();
    opts.tls = Some(soon.clone());
    let db = Db::with_opts(opts);
    let h = db.show_health();
    assert!(h.contains("(3 day(s)) EXPIRES SOON") || h.contains("(2 day(s)) EXPIRES SOON"), "{h}");
    assert!(h.contains("CA expires"), "{h}");
    let now = celastro::time::now_micros() / 1_000_000;
    assert!((soon.expires_at() - now - 3 * 86_400).abs() < 120);
    assert_eq!(
        soon.anchors_expire_at(),
        soon.expires_at(),
        "the test CA lives as long as its leaf"
    );
}
