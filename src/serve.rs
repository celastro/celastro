//! The local browser console: a hand-rolled HTTP/1.1 server on
//! `std::net::TcpListener`.
//!
//! There is no HTTP crate here and there will not be one, so this file parses
//! the request head itself, decides everything it can decide without touching
//! the database, and writes exactly one response. It implements the subset the
//! console needs and nothing else: no keep-alive, no chunked transfer, no
//! compression, no ranges, no CORS.
//!
//! The endpoint runs arbitrary SQL against the user's database, so it is
//! treated as hostile ground: a loopback-only bind, a per-run token from
//! `/dev/urandom` on every request including the HTML, a `Host` allow-list
//! against DNS rebinding, a cross-site guard on the routes that change
//! something, hard caps on everything read from the socket, and a deadline —
//! not a per-read timeout — on the request as a whole.
//!
//! Writes are made durable as they happen: a statement that comes back as an
//! ack is followed by `Db::persist`, and `POST /api/shutdown` ends the loop so
//! the caller can close the database properly. A console whose committed
//! writes evaporate on Ctrl-C would be the worst failure this tool has.
//!
//! The functions that make those decisions — `parse_head`, `host_is_local`,
//! `token_matches`, `dispatch`, `read_body`, `answer` — are ordinary functions
//! over a `Wire`, not methods on a live socket, because a security check that
//! can only be exercised by opening a listener is a security check that does
//! not get tested.

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};

use crate::crypto::hex;
use crate::sql;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::catalog::IndexKind;
use crate::engine::{Db, Outcome};
use crate::error::{Error, Result};
use crate::json;
use crate::plan::exec::QueryResult;
use crate::tls::{self, Stream, Tls};
use crate::value::Value;

// The three assets are compiled in. A console that reads its own UI off the
// disk at request time is a console with a path-traversal bug waiting to be
// found in it.
const INDEX_HTML: &str = include_str!("serve/index.html");
const APP_JS: &str = include_str!("serve/app.js");
const STYLE_CSS: &str = include_str!("serve/style.css");

const CT_HTML: &str = "text/html; charset=utf-8";
const CT_JS: &str = "text/javascript; charset=utf-8";
const CT_CSS: &str = "text/css; charset=utf-8";
const CT_JSON: &str = "application/json";

/// The largest request body accepted, in bytes. A SQL statement that does not
/// fit in a mebibyte is not a statement anybody typed into a console.
const MAX_BODY: usize = 1024 * 1024;
/// The largest request line or header line accepted, in bytes, *not* counting
/// the CRLF that ends it. `read_head` and `parse_head` measure the same thing.
const MAX_LINE: usize = 8 * 1024;
/// The most header fields accepted.
const MAX_HEADERS: usize = 64;

/// How long a client has to deliver its request head, and its whole body,
/// counted from the moment the connection was accepted.
///
/// These are deadlines and not per-read timeouts, and the difference is the
/// whole point. `SO_RCVTIMEO` bounds one `recv`, so a client that sends a
/// single byte just before each timeout expires restarts the clock forever and
/// holds one of the connection threads open without a token, without a route and
/// without ever finishing a request. Every read re-arms the socket with the
/// time that is actually left, so the exchange is bounded by the clock rather
/// than by the client's willingness to dribble.
const HEAD_DEADLINE: Duration = Duration::from_secs(2);
const BODY_DEADLINE: Duration = Duration::from_secs(15);
/// How long the kernel may take to accept our response.
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a refused request's unwanted body is swallowed before closing, and
/// the most of it that is read. See `drain`.
const DRAIN_DEADLINE: Duration = Duration::from_millis(500);
const MAX_DRAIN: u64 = 4 * MAX_BODY as u64;

/// How long the accept loop waits on the listener before re-reading the
/// shutdown flag: the bound on noticing a SIGTERM that arrived between the
/// read and the wait, and nothing a request ever pays (`signal::wait_readable`).
const ACCEPT_WAIT: Duration = Duration::from_millis(100);
/// How long the loop sleeps when every one of `MAX_CONNECTIONS` is taken
/// before looking again; the kernel's backlog holds what arrives meanwhile.
const SATURATED_PAUSE: Duration = Duration::from_millis(25);
/// How long the accept loop pauses after a failure that will repeat, and how
/// many of those in a row it tolerates before giving up. See `accept_backoff`.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);
const MAX_ACCEPT_FAILURES: u32 = 40;

/// Connections served at once. Past this the listener stops accepting and
/// the kernel's backlog holds the rest; a thread per connection is what a
/// stalled client costs, and sixty-four of them is a limit, not a pool.
const MAX_CONNECTIONS: usize = 64;

// ---------------------------------------------------------------- the server

pub struct Server {
    listener: TcpListener,
    addr: SocketAddr,
    token: String,
    reach: Reach,
    /// What every accepted connection is wrapped in, when the process has
    /// certificates; plain HTTP otherwise.
    tls: Option<Arc<Tls>>,
}

/// Where the console is reachable from, which decides two of the guards.
/// On loopback the `Host` allow-list refuses a rebound name and a browser's
/// `Origin` must be this machine's. On a network the console is reached by
/// whatever name routes to it -- a Service, a load balancer, a pod address
/// -- so `Host` may be anything and an `Origin`, when a browser sends one,
/// must be the `Host` the same request named: same-origin by the name the
/// client used. The token on every request is then what stands between the
/// network and the SQL prompt, which is why a network bind takes the
/// operator's token and never a per-run one ([`Server::bind_network`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    Loopback,
    Network,
}

/// The environment variable a network-bound console reads its token from,
/// the console's counterpart of the wire's `CELASTRO_WIRE_TOKEN`: every
/// node behind one Service has to answer the same token, so it is chosen
/// once by whoever runs them and given to each.
pub const TOKEN_ENV: &str = "CELASTRO_TOKEN";

/// The console token from the environment, or `None` when unset or empty.
pub fn token_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV).ok().filter(|t| !t.is_empty())
}

/// The least a network console's token may be. Sixteen bytes matches what
/// `new_token` draws from `/dev/urandom`; a shorter one is a guess away
/// from a SQL prompt on a routable address, and is refused.
pub const MIN_NETWORK_TOKEN: usize = 16;

impl Server {
    /// Bind the console to the loopback interface.
    ///
    /// 127.0.0.1 and nothing else: never 0.0.0.0, never `::`, never a name that
    /// might resolve to a routable address. This endpoint executes arbitrary
    /// SQL, so a bind reachable from the LAN is a remote code execution
    /// surface, not a convenience.
    ///
    /// `port` 0 asks the OS for a free port; [`Server::local_addr`] reports
    /// which one it gave.
    pub fn bind(port: u16) -> Result<Server> {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))?;
        let addr = listener.local_addr()?;
        Ok(Server { listener, addr, token: new_token()?, reach: Reach::Loopback, tls: None })
    }

    /// Bind the console to an address of the operator's choosing, for the one
    /// deployment where loopback is not enough: several nodes behind a
    /// Service or a load balancer, each answering the same token, any of
    /// them coordinating a statement over every node's shards.
    ///
    /// Everything `bind` says about the surface still holds -- this is an
    /// arbitrary-SQL endpoint on a routable address, plain HTTP, for a
    /// network that is trusted or an ingress that terminates TLS in front
    /// of it -- so the bind is explicit, the token is the operator's rather
    /// than drawn per run (a per-run token differs per node, which is
    /// useless behind a balancer, and is printed where a client cannot read
    /// it), and a token shorter than [`MIN_NETWORK_TOKEN`] bytes or holding
    /// anything but printable ASCII is refused. A loopback address here is
    /// the same console `bind` gives, with the operator's token.
    pub fn bind_network(addr: IpAddr, port: u16, token: String) -> Result<Server> {
        if token.len() < MIN_NETWORK_TOKEN || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::Io(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "the console token must be at least {MIN_NETWORK_TOKEN} printable ASCII \
                     bytes with no whitespace; this one is {} byte(s)",
                    token.len()
                ),
            )));
        }
        let listener = TcpListener::bind(SocketAddr::from((addr, port)))?;
        let bound = listener.local_addr()?;
        let reach = if addr.is_loopback() { Reach::Loopback } else { Reach::Network };
        Ok(Server { listener, addr: bound, token, reach, tls: None })
    }

    /// Serve over TLS with `tls`, or plain with `None`.
    pub fn with_tls(mut self, tls: Option<Arc<Tls>>) -> Server {
        self.tls = tls;
        self
    }

    /// Whether connections are encrypted.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Where this console is reachable from.
    pub fn reach(&self) -> Reach {
        self.reach
    }

    /// The address actually bound, which is only known after `bind` when the
    /// caller asked for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The per-run access token.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// The URL to open, token included.
    pub fn url(&self) -> String {
        format!(
            "{}://{}:{}/?t={}",
            if self.tls.is_some() { "https" } else { "http" },
            self.addr.ip(),
            self.addr.port(),
            self.token
        )
    }

    /// Serve until a shutdown is requested. Returns `Ok(())` on clean shutdown.
    ///
    /// A thread per connection, at most `MAX_CONNECTIONS` (sixty-four) of them, and the
    /// database locked only around the statement: reading a request, parsing
    /// it, checking it and writing its answer all happen outside the lock, so
    /// a client that is slow to send or slow to read delays nobody but
    /// itself. Reads -- a `SELECT`, an `EXPLAIN` of one -- run under the
    /// shared side of an `RwLock` and proceed side by side on every core
    /// (`Db::read` takes `&self`); a statement that changes something takes
    /// the exclusive side and runs alone. Until 0.31.0 every statement took
    /// one mutex and a node ran one at a time whatever the thread count;
    /// until 0.25.0 the loop was one thread, request and all under the lock,
    /// which was right for a console on loopback and wrong for one behind a
    /// Service.
    ///
    /// A per-connection failure is logged and the loop continues. A client that
    /// hangs up mid-request, sends garbage, or trips a deadline is not a reason
    /// to take the user's database console down. A `POST /api/shutdown` is, and
    /// returning is what lets the caller persist and exit.
    ///
    /// A SIGTERM or SIGINT ends it the same way, once the caller has installed
    /// the handlers (`signal::install_shutdown_handlers`). The listener is
    /// not blocked on -- a blocking `accept` is restarted after a handler
    /// runs, so the flag would be read only when the next connection happened
    /// to arrive, which for a `docker stop` is never -- but waited on with
    /// `poll(2)`, which a handler interrupts and a connection ends at once.
    /// Until 0.29.1 the wait was a 25 ms sleep, and every request paid up to
    /// that much before it was accepted.
    pub fn run(self, db: &RwLock<Db>) -> Result<()> {
        self.listener.set_nonblocking(true)?;
        let mut failures = 0u32;
        // Set by the connection that was asked to shut down; the loop reads
        // it between accepts, and the scope waits for the connections in
        // flight before `run` returns and the caller persists.
        let stop = AtomicBool::new(false);
        let active = AtomicUsize::new(0);
        let server = &self;
        std::thread::scope(|scope| loop {
            if crate::signal::shutdown_requested() || stop.load(AtomicOrdering::Acquire) {
                return Ok(());
            }
            if active.load(AtomicOrdering::Acquire) >= MAX_CONNECTIONS {
                std::thread::sleep(SATURATED_PAUSE);
                continue;
            }
            match server.listener.accept() {
                Ok((s, _)) => {
                    failures = 0;
                    // The listener's flag is not inherited on every platform
                    // and must not be here: a connection is served blocking.
                    s.set_nonblocking(false)?;
                    active.fetch_add(1, AtomicOrdering::AcqRel);
                    let (active, stop) = (&active, &stop);
                    scope.spawn(move || {
                        match server.serve_one(s, db) {
                            Ok(Next::Serve) => {}
                            Ok(Next::Stop) => stop.store(true, AtomicOrdering::Release),
                            Err(e) => eprintln!("celastro-cli: connection dropped: {e}"),
                        }
                        active.fetch_sub(1, AtomicOrdering::AcqRel);
                    });
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    crate::signal::wait_readable(&server.listener, ACCEPT_WAIT);
                }
                Err(e) => {
                    eprintln!("celastro-cli: accept failed: {e}");
                    match accept_backoff(e.kind(), failures + 1) {
                        // Nothing of ours went wrong, and it will not repeat by
                        // itself: the run of failures starts over.
                        Backoff::Now => failures = 0,
                        Backoff::After(pause) => {
                            failures += 1;
                            std::thread::sleep(pause);
                        }
                        Backoff::GiveUp => {
                            let run = failures + 1;
                            eprintln!("celastro-cli: {run} accept failures in a row, stopping");
                            return Err(e.into());
                        }
                    }
                }
            }
        })
    }

    fn serve_one(&self, stream: TcpStream, db: &RwLock<Db>) -> std::io::Result<Next> {
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let stream = tls::accept(self.tls.as_ref(), stream)?;
        // The read side is armed per read, from the deadline, by `Wire::arm`.
        let deadlines = Deadlines::from_now();
        let mut io = BufReader::new(stream);
        // The lock is taken inside `answer`, around the statement and nothing
        // else; the socket is never read or written under it.
        let served = answer(&mut io, &self.token, self.addr.port(), self.reach, db, deadlines);
        let socket = io.get_mut();
        served.response.write_to(socket)?;
        socket.flush()?;
        // No keep-alive: close the write half so the client sees the end of the
        // body without having to trust `Content-Length` alone.
        let _ = socket.shutdown_write();
        Ok(served.next)
    }
}

/// Whether the accept loop carries on after a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    Serve,
    Stop,
}

/// What to do about a failed `accept`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backoff {
    /// Retry immediately: nothing was actually wrong.
    Now,
    /// Retry after a pause.
    After(Duration),
    /// Stop serving.
    GiveUp,
}

/// Decide what a failed `accept` means.
///
/// Under `EMFILE` or `ENFILE` the pending connection stays on the queue, so the
/// next call fails identically: retrying at once is a loop that spins a core at
/// 100% for as long as the descriptor table stays full, and it does it with
/// nobody watching. Those pause, and a run of them that never clears ends the
/// loop rather than burning the machine.
///
/// The other kinds are not failures of ours and do not repeat on their own — a
/// client that hung up before we accepted it, a signal, an empty queue — so
/// they retry at once and do not count towards giving up. Otherwise a flood of
/// half-open connections would shut the console down, which is the outcome the
/// flood was after.
fn accept_backoff(kind: ErrorKind, consecutive: u32) -> Backoff {
    match kind {
        ErrorKind::WouldBlock | ErrorKind::Interrupted => Backoff::Now,
        ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset => Backoff::Now,
        _ if consecutive >= MAX_ACCEPT_FAILURES => Backoff::GiveUp,
        _ => Backoff::After(ACCEPT_BACKOFF),
    }
}

/// The per-run access token, presented as `?t=` or `X-Celastro-Token`.
///
/// Sixteen bytes straight from `/dev/urandom`, hex-encoded: this is the only
/// thing between a page in the user's browser and a SQL prompt on their
/// database, so it is the kernel's randomness and nothing cleverer.
///
/// There is deliberately no fallback. A clock-and-pid derivation is searchable
/// by anyone who can read the pid and the process start time, and testable one
/// request at a time by a page that loads `/app.js?t=GUESS` and watches onload
/// against onerror. On a machine where `/dev/urandom` cannot be opened the
/// console refuses to start, because an arbitrary-SQL endpoint behind a
/// guessable secret is worse than no console at all.
fn new_token() -> Result<String> {
    token_from(urandom_bytes())
}

/// The decision half of [`new_token`], separated from the read so that it can
/// be tested.
///
/// A machine that has `/dev/urandom` cannot be made to lose it, so the refusal
/// is the one branch no test can reach through the real file — and a branch no
/// test reaches is a branch anyone can quietly delete. Handing this function
/// the error the read would have returned is what holds the decision in place.
fn token_from(bytes: std::io::Result<[u8; 16]>) -> Result<String> {
    match bytes {
        Ok(bytes) => Ok(hex(&bytes)),
        Err(e) => Err(Error::Io(std::io::Error::new(
            e.kind(),
            format!("cannot read /dev/urandom for the console token: {e}"),
        ))),
    }
}

fn urandom_bytes() -> std::io::Result<[u8; 16]> {
    let mut source = std::fs::File::open("/dev/urandom")?;
    let mut bytes = [0u8; 16];
    source.read_exact(&mut bytes)?;
    Ok(bytes)
}

// ----------------------------------------------------------------- deadlines

/// A request source that can be held to a deadline.
///
/// `serve_one` reads a `BufReader<TcpStream>`, where arming means setting
/// `SO_RCVTIMEO` to the time that is left. The tests read a `Cursor`, which
/// cannot block but honours the same deadline, so the timeout path is
/// reachable without opening a listener.
trait Wire: BufRead {
    /// Bound the next read by `deadline`, or fail once it has passed.
    fn arm(&mut self, deadline: Instant) -> std::result::Result<(), Reject>;
}

impl Wire for BufReader<Box<dyn Stream>> {
    fn arm(&mut self, deadline: Instant) -> std::result::Result<(), Reject> {
        let left = match time_left(deadline) {
            Some(d) => d,
            None => return Err(Reject::RequestTimeout),
        };
        self.get_ref().set_read_timeout(Some(left)).map_err(|_| Reject::BadRequest)
    }
}

/// How long is left before `deadline`, or `None` once it has passed.
///
/// A zero duration is reported as gone rather than as time remaining, because
/// `set_read_timeout(Some(Duration::ZERO))` does not mean "do not wait" — it is
/// rejected outright on some platforms and means "wait forever" on others.
fn time_left(deadline: Instant) -> Option<Duration> {
    deadline.checked_duration_since(Instant::now()).filter(|left| !left.is_zero())
}

/// The absolute times by which one request's head and body must have arrived.
#[derive(Debug, Clone, Copy)]
struct Deadlines {
    head: Instant,
    body: Instant,
}

impl Deadlines {
    fn from_now() -> Deadlines {
        let now = Instant::now();
        Deadlines { head: now + HEAD_DEADLINE, body: now + BODY_DEADLINE }
    }
}

// ------------------------------------------------------------------ requests

/// A protocol-level rejection: the request itself was wrong.
///
/// A *SQL* error is not one of these. A statement that fails to parse or to
/// plan is a well-formed question with an unhappy answer, and it comes back as
/// HTTP 200 with `{"ok":false}` so the console can render it beside the query.
/// These codes are reserved for requests that should never have arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reject {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    /// Carries the `Allow` value for the path that refused the method.
    MethodNotAllowed(&'static str),
    RequestTimeout,
    PayloadTooLarge,
    UnsupportedMediaType,
}

impl Reject {
    fn status(self) -> (u16, &'static str) {
        match self {
            Reject::BadRequest => (400, "Bad Request"),
            Reject::Unauthorized => (401, "Unauthorized"),
            Reject::Forbidden => (403, "Forbidden"),
            Reject::NotFound => (404, "Not Found"),
            Reject::MethodNotAllowed(_) => (405, "Method Not Allowed"),
            Reject::RequestTimeout => (408, "Request Timeout"),
            Reject::PayloadTooLarge => (413, "Payload Too Large"),
            Reject::UnsupportedMediaType => (415, "Unsupported Media Type"),
        }
    }

    /// The body repeats the status and says nothing else. A rejected request
    /// has not shown it may talk to this process, so it learns nothing about
    /// the database, the token or which routes exist.
    ///
    /// No `WWW-Authenticate` on the 401, on purpose: it would make the browser
    /// pop up a password dialog for a credential that is not a password.
    fn response(self) -> Response {
        let (status, reason) = self.status();
        let mut response = Response::new(status, reason, CT_JSON, error_json(reason));
        // RFC 7231 §6.5.5: a 405 states what the path does accept.
        if let Reject::MethodNotAllowed(allow) = self {
            response.allow = Some(allow);
        }
        response
    }
}

/// A read that ran out of time is a 408; anything else off the wire is a 400.
fn read_failure(e: &std::io::Error) -> Reject {
    match e.kind() {
        ErrorKind::WouldBlock | ErrorKind::TimedOut => Reject::RequestTimeout,
        _ => Reject::BadRequest,
    }
}

/// A parsed request head. Header names are lowercased on the way in, so every
/// lookup in this file uses a lowercase literal.
struct Head {
    method: String,
    path: String,
    /// The raw query string, without the `?`.
    query: String,
    headers: Vec<(String, String)>,
    /// Already bounded by [`MAX_BODY`]; see `parse_head`.
    content_length: usize,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

/// Read one line, up to and including its `\n`, consulting the deadline before
/// every read of the wire.
///
/// `BufRead::read_until` cannot be used for this. It loops inside a single
/// call, so the deadline would be checked once per line rather than once per
/// read, and a client that sends one byte just before each timeout expires
/// would be granted a fresh timeout for every byte of every line — thousands of
/// them, from a socket that has presented no token and made no request. Here
/// the clock is consulted before each read, and the socket is re-armed with the
/// time that is genuinely left.
///
/// At most `limit` bytes plus the CRLF that ends them are buffered, so a line
/// exactly at the limit still arrives whole and one byte over it is refused by
/// the caller. Nothing unbounded is ever held for an unauthenticated client.
fn read_line<W: Wire>(
    r: &mut W,
    deadline: Instant,
    limit: usize,
) -> std::result::Result<Vec<u8>, Reject> {
    let cap = limit + 2;
    let mut line: Vec<u8> = Vec::new();
    loop {
        r.arm(deadline)?;
        let taken = {
            let chunk = match r.fill_buf() {
                Ok(b) => b,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(read_failure(&e)),
            };
            if chunk.is_empty() {
                // End of stream. Nothing at all means the head simply stopped;
                // a partial line goes back for the parser to refuse.
                return if line.is_empty() { Err(Reject::BadRequest) } else { Ok(line) };
            }
            let upto = match chunk.iter().position(|&b| b == b'\n') {
                Some(i) => i + 1,
                None => chunk.len(),
            };
            let take = upto.min(cap - line.len());
            line.extend_from_slice(&chunk[..take]);
            take
        };
        r.consume(taken);
        if line.last() == Some(&b'\n') || line.len() >= cap {
            return Ok(line);
        }
    }
}

/// Read the request head off the socket, bounded in every direction, and parse
/// it.
///
/// The caps here exist so that nothing unbounded is ever buffered for a client
/// that has not authenticated, and `deadline` is what stops a client spending
/// all day inside them one byte at a time. `parse_head` checks the same limits
/// again on the text it is given, because that is the function a test can reach
/// and the one that states the rule.
fn read_head<W: Wire>(r: &mut W, deadline: Instant) -> std::result::Result<Head, Reject> {
    let mut text = String::new();
    let mut lines = 0usize;
    loop {
        if lines >= MAX_HEADERS + 2 {
            return Err(Reject::BadRequest);
        }
        let raw = read_line(r, deadline, MAX_LINE)?;
        let line = if lines == 0 { request_line(&raw)? } else { header_line(&raw)? };
        if line.len() > MAX_LINE {
            return Err(Reject::BadRequest);
        }
        let blank = line.is_empty();
        text.push_str(&line);
        text.push_str("\r\n");
        lines += 1;
        if blank {
            break;
        }
    }
    parse_head(&text)
}

/// Strip one line terminator: the `\n`, and the `\r` in front of it if any.
fn trim_eol(raw: &[u8]) -> &[u8] {
    let mut end = raw.len();
    if end > 0 && raw[end - 1] == b'\n' {
        end -= 1;
    }
    if end > 0 && raw[end - 1] == b'\r' {
        end -= 1;
    }
    &raw[..end]
}

/// Decode the request line, strictly.
///
/// Every routing decision this file makes comes out of these bytes, so they
/// must be ASCII and free of control characters: a target that needs a byte
/// above 0x7f is a target the browser would have percent-encoded.
fn request_line(raw: &[u8]) -> std::result::Result<String, Reject> {
    let bytes = trim_eol(raw);
    if !bytes.is_ascii() || bytes.iter().any(|&b| b < 0x20 || b == 0x7f) {
        return Err(Reject::BadRequest);
    }
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

/// Decode one header line.
///
/// The field *name* is a token and must be ASCII, because names are what this
/// file matches on. A field *value* is opaque octets (RFC 7230 §3.2.6): a
/// `User-Agent` in Latin-1 is not a reason to refuse the request and leave the
/// console unusable, so a value that is not UTF-8 is decoded lossily instead.
/// Nothing is lost by that: the only values read here — `Host`, the token,
/// `Content-Length`, `Content-Type`, `Origin`, `Sec-Fetch-Site` — are ASCII in
/// any real request, and a byte that had to be repaired can only make a
/// comparison fail, never make one match. Control bytes are refused outright;
/// they are framing, not text.
fn header_line(raw: &[u8]) -> std::result::Result<String, Reject> {
    let bytes = trim_eol(raw);
    if bytes.is_empty() {
        return Ok(String::new());
    }
    let colon = match bytes.iter().position(|&b| b == b':') {
        Some(i) => i,
        None => return Err(Reject::BadRequest),
    };
    let (name, value) = bytes.split_at(colon);
    if name.is_empty() || !name.iter().copied().all(is_token_byte) {
        return Err(Reject::BadRequest);
    }
    if value.iter().any(|&b| b < 0x20 && b != b'\t') {
        return Err(Reject::BadRequest);
    }
    let mut line = String::from_utf8_lossy(name).into_owned();
    line.push_str(&String::from_utf8_lossy(value));
    Ok(line)
}

/// Parse a request head: CRLF-separated lines terminated by a blank one.
fn parse_head(text: &str) -> std::result::Result<Head, Reject> {
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    if request_line.len() > MAX_LINE {
        return Err(Reject::BadRequest);
    }
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let version = parts.next().unwrap_or("");
    // Exactly three fields. A fourth means a space in the target or a second
    // request line folded into the first, and neither is a thing to guess at.
    if parts.next().is_some() {
        return Err(Reject::BadRequest);
    }
    if method.is_empty() || !method.bytes().all(|b| b.is_ascii_uppercase()) {
        return Err(Reject::BadRequest);
    }
    if !version.starts_with("HTTP/1.") {
        return Err(Reject::BadRequest);
    }
    // Origin-form targets only. An absolute-form target (`GET http://host/ ...`)
    // carries its own authority, which would let a request name a host the
    // `Host` check below never looks at.
    if !target.starts_with('/') {
        return Err(Reject::BadRequest);
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if line.len() > MAX_LINE || headers.len() >= MAX_HEADERS {
            return Err(Reject::BadRequest);
        }
        let (name, value) = match line.split_once(':') {
            Some(nv) => nv,
            None => return Err(Reject::BadRequest),
        };
        // A field name is a token and nothing else. This refuses the whitespace
        // before the colon that header-smuggling tricks lean on, and refuses an
        // obs-fold continuation line, whose leading space is not a token byte.
        if name.is_empty() || !name.bytes().all(is_token_byte) {
            return Err(Reject::BadRequest);
        }
        headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }

    // Duplicates of the two headers that decide framing and origin are refused
    // rather than resolved. Preferring the first or the last is exactly the
    // disagreement between two parsers that request smuggling is built from.
    if headers.iter().filter(|(k, _)| k == "host").count() > 1 {
        return Err(Reject::BadRequest);
    }
    let mut lengths = headers.iter().filter(|(k, _)| k == "content-length");
    let declared = lengths.next().map(|(_, v)| v.as_str());
    if lengths.next().is_some() {
        return Err(Reject::BadRequest);
    }
    // No chunked encoding: the body framing is `Content-Length` or nothing.
    if headers.iter().any(|(k, _)| k == "transfer-encoding") {
        return Err(Reject::BadRequest);
    }
    let content_length = match declared {
        None => 0,
        Some(v) => parse_content_length(v)?,
    };

    Ok(Head {
        method: method.to_string(),
        path: path.to_string(),
        query: query.to_string(),
        headers,
        content_length,
    })
}

/// Bound the declared body length *before* anything can treat it as a size to
/// allocate. A number too large to be a `u64` is over the cap by inspection; it
/// is not a parse accident to report as a malformed request.
fn parse_content_length(v: &str) -> std::result::Result<usize, Reject> {
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Reject::BadRequest);
    }
    match v.parse::<u64>() {
        Ok(n) if n <= MAX_BODY as u64 => Ok(n as usize),
        _ => Err(Reject::PayloadTooLarge),
    }
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// Read exactly `len` bytes of body, all of them before `deadline`.
///
/// `len` has already been bounded by `parse_head`; it is bounded again here
/// because this is the line that allocates, and the rule is that no buffer is
/// ever sized from a `Content-Length` that has not been checked. The loop is
/// written out rather than left to `read_exact` so that every read is re-armed
/// from the deadline: `read_exact` would let a slow client restart the clock
/// with each byte it sends.
fn read_body<W: Wire>(
    r: &mut W,
    len: usize,
    deadline: Instant,
) -> std::result::Result<Vec<u8>, Reject> {
    if len > MAX_BODY {
        return Err(Reject::PayloadTooLarge);
    }
    let mut body = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        r.arm(deadline)?;
        match r.read(&mut body[filled..]) {
            // Short of the declared length: the client framed its own request
            // wrong, or hung up in the middle of it.
            Ok(0) => return Err(Reject::BadRequest),
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(read_failure(&e)),
        }
    }
    Ok(body)
}

/// Swallow a body nobody is going to read, so the response is not lost.
///
/// Closing a socket that still has unread data in its receive queue sends an
/// RST, and a client that gets an RST may discard the response already sitting
/// in its buffer — which is how pasting a statement over a mebibyte into the
/// console produces "network error" instead of the 413 this server actually
/// sent. Bounded in bytes and in time, because it runs for requests that have
/// already been refused: it reads what the client has sent, not what the client
/// might yet decide to send.
///
/// A fixed buffer rather than `io::copy` into `io::sink`: `copy` loops inside
/// one call, and a dribbling client would get a fresh timeout for each of those
/// reads. Here the deadline is re-checked before every one of them.
fn drain<W: Wire>(io: &mut W, declared: u64) {
    let deadline = Instant::now() + DRAIN_DEADLINE;
    let mut left = declared.min(MAX_DRAIN);
    let mut pit = [0u8; 8 * 1024];
    while left > 0 {
        if io.arm(deadline).is_err() {
            return;
        }
        let want = pit.len().min(left as usize);
        match io.read(&mut pit[..want]) {
            Ok(0) | Err(_) => return,
            Ok(n) => left -= n as u64,
        }
    }
}

/// Is this `Host` one of ours?
///
/// Only `localhost`, `127.0.0.1` and `[::1]`, each with an optional `:port`.
/// This is the DNS-rebinding guard: without it, any page the user visits can
/// point a name it controls at 127.0.0.1 and then read the replies, because as
/// far as the browser is concerned that page and this console share an origin.
/// Binding to loopback does not stop that. Checking `Host` does.
fn host_is_local(host: &str) -> bool {
    let (name, port) = match split_host_port(host.trim()) {
        Some(v) => v,
        None => return false,
    };
    if let Some(p) = port {
        if p.is_empty() || p.len() > 5 || !p.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
    }
    // Case-insensitive for the name, because DNS is and a browser may send
    // `LocalHost`. The literals are compared whole: `127.0.0.1.evil.example` is
    // not one prefix match away from being trusted.
    name.eq_ignore_ascii_case("localhost") || name == "127.0.0.1" || name == "[::1]"
}

fn split_host_port(host: &str) -> Option<(&str, Option<&str>)> {
    if host.starts_with('[') {
        let close = host.find(']')?;
        let (name, rest) = host.split_at(close + 1);
        if rest.is_empty() {
            return Some((name, None));
        }
        return Some((name, Some(rest.strip_prefix(':')?)));
    }
    match host.find(':') {
        // A bare IPv6 literal has more than one colon and is not a legal `Host`
        // unbracketed; refuse it rather than guess where the authority ends.
        Some(i) if host[i + 1..].contains(':') => None,
        Some(i) => Some((&host[..i], Some(&host[i + 1..]))),
        None => Some((host, None)),
    }
}

/// Compare the token without leaking where it first differs.
///
/// The accumulate-then-compare shape is the whole point. An `==` returns as
/// soon as two bytes differ, and how long it took says how much of the guess
/// was right, which turns one search of the token into 32 independent guesses
/// of a hex digit. Every byte of `expected` is read on every call.
fn token_matches(expected: &str, given: &str) -> bool {
    let a = expected.as_bytes();
    let b = given.as_bytes();
    if b.is_empty() {
        return a.is_empty();
    }
    // A length difference is folded into the accumulator rather than returned
    // on, so a short guess costs the same as a wrong one of the right length.
    let mut diff = (a.len() ^ b.len()) as u64;
    for (i, &x) in a.iter().enumerate() {
        diff |= (x ^ b[i % b.len()]) as u64;
    }
    diff == 0
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        let (k, v) = match pair.split_once('=') {
            Some(kv) => kv,
            None => (pair, ""),
        };
        if k == key {
            return Some(v);
        }
    }
    None
}

/// The token the request presented, if it presented one.
///
/// No percent-decoding: the token alphabet is `[0-9a-f]`, so a real token never
/// needs escaping, and a decoder here would only add ways for two different
/// strings to compare equal.
fn presented_token(head: &Head) -> Option<&str> {
    let from_header = head.header("x-celastro-token");
    from_header.or_else(|| query_param(&head.query, "t"))
}

/// What to do with a request, once everything decidable without the database
/// has been decided.
enum Action {
    Reply(Response),
    /// Serialise the catalog; needs `&Db`.
    Catalog,
    /// Answer the health probe; needs `&Db`, because a probe that does not
    /// touch the database measures the process and not the database.
    Health,
    /// Read the body and run the SQL in it; needs `&mut Db`.
    Query,
    /// Acknowledge, then stop serving so the caller can close the database.
    Shutdown,
}

fn asset(content_type: &'static str, body: &'static str) -> Action {
    Action::Reply(Response::new(200, "OK", content_type, body.to_string()))
}

fn page(token: &str) -> Action {
    Action::Reply(Response::new(
        200,
        "OK",
        CT_HTML,
        with_tokenised_assets(&with_source_offer(INDEX_HTML), token),
    ))
}

/// Stitch the token into the page's own asset URLs.
///
/// `index.html` links `/style.css` and `/app.js` as plain paths, and a `<link>`
/// or a `<script>` is an ordinary browser request: it carries no
/// `X-Celastro-Token` header, and no JavaScript can add one to it because the
/// script is the thing being fetched. Without this, the page the browser has
/// just loaded is answered 401 twice and renders unstyled and dead.
///
/// The alternative — exempting "just the static files" from the token — is a
/// hole: it would let any page on the machine confirm this console is running
/// and read its source. Rewriting the two URLs keeps the rule that every single
/// request carries a token. If the markup ever stops matching, the replacement
/// simply does not fire, and the test below is what notices.
fn with_tokenised_assets(html: &str, token: &str) -> String {
    let styled = html.replace("\"/style.css\"", &format!("\"/style.css?t={token}\""));
    styled.replace("\"/app.js\"", &format!("\"/app.js?t={token}\""))
}

/// The methods a known path answers, in `Allow` order, or `None` if the path is
/// not one of this console's. One table, so a 405 can say what it accepts.
fn allowed_methods(path: &str) -> Option<&'static str> {
    match path {
        "/" | "/app.js" | "/style.css" | "/api/health" | "/api/catalog" => Some("GET"),
        "/api/query" | "/api/shutdown" => Some("POST"),
        _ => None,
    }
}

/// Is this the console's own origin?
///
/// Serialised origins are `scheme://host:port` and nothing else, so the two
/// spellings of loopback with this server's port are the whole allow-list.
/// `null`, a file URL, an https origin and any other port are all somebody
/// else. The comparison ignores case in the host because host names are
/// case-insensitive; it cannot admit an origin the exact match would not.
fn origin_is_ours(origin: &str, port: u16) -> bool {
    let ours = [format!("http://127.0.0.1:{port}"), format!("http://localhost:{port}")];
    ours.iter().any(|o| origin.eq_ignore_ascii_case(o))
}

/// On a network the console's own origin is `http://` and whatever `Host`
/// the same request named: the name the client reached it by, which is the
/// name a page it served would carry as its origin. `https` is not ours --
/// the console serves none -- and neither is any other host, so a form on
/// a page from elsewhere is refused exactly as on loopback.
fn origin_is_host(origin: &str, host: &str) -> bool {
    let (scheme, rest) = match origin.is_char_boundary(7) {
        true => origin.split_at(7),
        false => return false,
    };
    scheme.eq_ignore_ascii_case("http://") && rest.eq_ignore_ascii_case(host.trim())
}

/// `application/json`, with or without parameters (`; charset=utf-8`).
fn is_json_media_type(value: &str) -> bool {
    let base = value.split(';').next().unwrap_or("").trim();
    base.eq_ignore_ascii_case("application/json")
}

/// The cross-site guard on the routes that change something.
///
/// `Host` and the token are not enough on their own. A form on any page can
/// POST to this port without a preflight, and the attacker never has to read
/// the response to have done the damage — the statement has already run. So a
/// state-changing request must either claim no origin at all (curl, a script,
/// anything that is not a browser) or claim this console's own, and a browser
/// that tells us where the request came from must say `same-origin`. It is only
/// applied to `POST`, because a user typing the console's URL into the address
/// bar sends `Sec-Fetch-Site: none` and that navigation must still work.
fn same_site_post(head: &Head, port: u16, reach: Reach) -> std::result::Result<(), Reject> {
    if let Some(origin) = head.header("origin") {
        let ours = origin_is_ours(origin, port)
            || (reach == Reach::Network
                && head.header("host").is_some_and(|h| origin_is_host(origin, h)));
        if !ours {
            return Err(Reject::Forbidden);
        }
    }
    if let Some(site) = head.header("sec-fetch-site") {
        if !site.eq_ignore_ascii_case("same-origin") {
            return Err(Reject::Forbidden);
        }
    }
    Ok(())
}

/// Check the request and route it.
///
/// The order matters. `Host` first, so a rebound request is refused whatever
/// token it managed to guess and the token comparison is never reached from a
/// foreign origin at all. Then the token, which every path requires — the HTML
/// console included, because handing out the page is handing out the shape of
/// the API and inviting the browser to call it. Only then the route, so an
/// unauthenticated client cannot map which paths exist by their status codes.
#[cfg(test)]
fn dispatch(head: &Head, token: &str, port: u16) -> std::result::Result<Action, Reject> {
    dispatch_for(head, token, port, Reach::Loopback)
}

/// [`dispatch`] for a console of either reach. On a network the `Host`
/// allow-list does not apply -- the console is reached by whatever name
/// routes to it -- but the header is still required, the token still
/// guards every path, and a browser's `Origin` must be that `Host`.
fn dispatch_for(
    head: &Head,
    token: &str,
    port: u16,
    reach: Reach,
) -> std::result::Result<Action, Reject> {
    // A missing `Host` is answered 403 rather than 400: HTTP/1.1 requires the
    // header, so its absence is a client declining to say where it thinks it is
    // talking, which deserves the same answer as saying the wrong thing.
    let host = match head.header("host") {
        Some(h) => h,
        None => return Err(Reject::Forbidden),
    };
    if reach == Reach::Loopback && !host_is_local(host) {
        return Err(Reject::Forbidden);
    }
    // Health needs no token. It is what a supervisor's probe asks, and a
    // probe cannot know the token a run printed; what it learns is that a
    // console is serving on this port and how many collections it holds,
    // which the loopback bind and the Host check above already confine to
    // this machine. It executes nothing.
    if head.method == "GET" && head.path == "/api/health" {
        return Ok(Action::Health);
    }
    let given = match presented_token(head) {
        Some(t) => t,
        None => return Err(Reject::Unauthorized),
    };
    if !token_matches(token, given) {
        return Err(Reject::Unauthorized);
    }
    match (head.method.as_str(), head.path.as_str()) {
        ("GET", "/") => Ok(page(token)),
        ("GET", "/app.js") => Ok(asset(CT_JS, APP_JS)),
        ("GET", "/style.css") => Ok(asset(CT_CSS, STYLE_CSS)),
        ("GET", "/api/catalog") => Ok(Action::Catalog),
        ("POST", "/api/query") => {
            same_site_post(head, port, reach)?;
            // A cross-origin form cannot set this content type, and asking for
            // it is what forces a preflight the browser will refuse to send.
            match head.header("content-type") {
                Some(v) if is_json_media_type(v) => Ok(Action::Query),
                _ => Err(Reject::UnsupportedMediaType),
            }
        }
        ("POST", "/api/shutdown") => {
            same_site_post(head, port, reach)?;
            Ok(Action::Shutdown)
        }
        _ => match allowed_methods(&head.path) {
            Some(allow) => Err(Reject::MethodNotAllowed(allow)),
            None => Err(Reject::NotFound),
        },
    }
}

/// One answered request: the response to write, and whether to keep serving.
struct Served {
    response: Response,
    next: Next,
}

impl Served {
    fn keep(response: Response) -> Served {
        Served { response, next: Next::Serve }
    }

    fn last(response: Response) -> Served {
        Served { response, next: Next::Stop }
    }
}

/// Read one request and produce one response. Never fails: a protocol problem
/// is a status code, and a SQL problem is a JSON body.
/// The database, whoever held it last: a statement that panicked with the
/// lock held has already been reported, and the console keeps serving.
/// The database for a read: many at once, beside no write. Poisoning is
/// recovered from, as everywhere in the crate: a panic in one request is
/// that request's failure, not the console's.
fn read(db: &RwLock<Db>) -> RwLockReadGuard<'_, Db> {
    db.read().unwrap_or_else(|p| p.into_inner())
}

/// The database for a statement that changes it: alone.
fn write(db: &RwLock<Db>) -> RwLockWriteGuard<'_, Db> {
    db.write().unwrap_or_else(|p| p.into_inner())
}

fn answer<W: Wire>(
    io: &mut W,
    token: &str,
    port: u16,
    reach: Reach,
    db: &RwLock<Db>,
    dl: Deadlines,
) -> Served {
    let head = match read_head(io, dl.head) {
        Ok(h) => h,
        // The head did not parse, so there is no declared length to believe:
        // swallow what was sent, bounded, so the client can read the status.
        Err(r) => {
            drain(io, MAX_DRAIN);
            return Served::keep(r.response());
        }
    };
    // The body is read only after the host, the token and the route have all
    // been accepted, so an unauthenticated client never gets us to allocate for
    // it and never gets a statement as far as the database. Every other route
    // ignores the body, and an ignored body is drained rather than left to make
    // the close an RST.
    let action = dispatch_for(&head, token, port, reach);
    if !matches!(action, Ok(Action::Query)) {
        drain(io, head.content_length as u64);
    }
    let served = match action {
        Err(r) => Served::keep(r.response()),
        Ok(Action::Reply(response)) => Served::keep(response),
        Ok(Action::Health) => Served::keep(Response::json(health_json(&read(db)))),
        Ok(Action::Catalog) => Served::keep(Response::json(catalog_json(&read(db)))),
        Ok(Action::Query) => Served::keep(query_response(io, &head, db, dl.body)),
        Ok(Action::Shutdown) => Served::last(Response::json(ack_json("shutting down"))),
    };
    // A HEAD gets the head a GET would have got and not one byte of the body.
    // Sending one is what desynchronises a client that is counting bytes.
    if head.method == "HEAD" {
        Served { response: served.response.without_body(), next: served.next }
    } else {
        served
    }
}

/// The body is read and parsed before the lock is taken, so a client that
/// sends its statement slowly holds up nobody's; the lock covers the
/// statement and the persist that follows it.
fn query_response<W: Wire>(
    io: &mut W,
    head: &Head,
    db: &RwLock<Db>,
    deadline: Instant,
) -> Response {
    let body = match read_body(io, head.content_length, deadline) {
        Ok(b) => b,
        Err(r) => return r.response(),
    };
    match sql_from_body(&body) {
        Ok(sql) => run_sql(db, &sql),
        Err(r) => r.response(),
    }
}

// ----------------------------------------------------------------- responses

struct Response {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    /// The `Allow` value, on the one status that owes the client one.
    allow: Option<&'static str>,
    body: String,
    /// False for a `HEAD`: the head is written, the body is not.
    send_body: bool,
}

impl Response {
    fn new(status: u16, reason: &'static str, content_type: &'static str, body: String) -> Self {
        Response { status, reason, content_type, allow: None, body, send_body: true }
    }

    fn json(body: String) -> Response {
        Response::new(200, "OK", CT_JSON, body)
    }

    /// The same response with its body withheld, for a `HEAD`. `Content-Length`
    /// still describes the body a `GET` would have received, which is what the
    /// header is for.
    fn without_body(mut self) -> Response {
        self.send_body = false;
        self
    }

    fn head_text(&self) -> String {
        let mut head = format!("HTTP/1.1 {} {}\r\n", self.status, self.reason);
        // Every response is dated. A client cannot compute a cache age or an
        // elapsed time from a response that will not say when it was made.
        head.push_str(&format!("Date: {}\r\n", http_date(SystemTime::now())));
        head.push_str(&format!("Content-Type: {}\r\n", self.content_type));
        // Bytes, not characters. A body holding one multi-byte character would
        // otherwise be announced short and truncated by the client.
        head.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
        if let Some(allow) = self.allow {
            head.push_str(&format!("Allow: {allow}\r\n"));
        }
        // The console serves JavaScript from the same origin as whatever a
        // query returns; content sniffing is how that becomes an execution of
        // a document somebody else stored.
        head.push_str("X-Content-Type-Options: nosniff\r\n");
        // Query results are the user's data. None of this is cacheable.
        head.push_str("Cache-Control: no-store\r\n");
        // The token rides in the URL, so a link followed out of the console
        // would otherwise put it in a `Referer` on somebody else's server.
        head.push_str("Referrer-Policy: no-referrer\r\n");
        // One request per connection, then close: no keep-alive state machine
        // to get wrong and no half-read pipeline to smuggle a request into.
        head.push_str("Connection: close\r\n");
        // Deliberately no `Access-Control-*` of any kind. There is no other
        // origin that should be reading this, and a CORS header is a written
        // invitation for one.
        head.push_str("\r\n");
        head
    }

    /// Write the head, then the body straight from where it already is.
    ///
    /// Rendering into one more buffer first would make peak memory twice the
    /// size of the response, and a result set is the largest thing this process
    /// holds after the data itself.
    fn write_to<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(self.head_text().as_bytes())?;
        if self.send_body {
            w.write_all(self.body.as_bytes())?;
        }
        Ok(())
    }
}

/// The current time as an HTTP-date: RFC 7231 §7.1.1.1 IMF-fixdate, GMT,
/// fixed width, C locale. The names are spelled out here because that format
/// is a protocol constant and has nothing to do with the machine's locale.
fn http_date(now: SystemTime) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let secs = now.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
    let days = secs.div_euclid(86_400);
    let rest = secs.rem_euclid(86_400);
    let (year, month, day) = crate::time::civil_from_days(days);
    // 1 January 1970 was a Thursday, which is where `DAYS` starts.
    let weekday = DAYS[days.rem_euclid(7) as usize];
    let month_name = MONTHS[(month as usize - 1).min(11)];
    let (h, m, s) = (rest / 3600, (rest / 60) % 60, rest % 60);
    format!("{weekday}, {day:02} {month_name} {year:04} {h:02}:{m:02}:{s:02} GMT")
}

/// One string as a JSON string literal, quotes included.
///
/// Delegated to the engine's own writer so the console cannot disagree with the
/// database about what a quote, a backslash, a newline or a control character
/// becomes. Every string that reaches here is attacker-controlled as far as
/// this file is concerned: it arrived through the same API.
fn jstr(s: &str) -> String {
    json::to_string(&Value::Str(s.to_string()))
}

/// A score or a distance as JSON. Non-finite becomes `null`, because JSON has
/// no `NaN` and emitting the bare token would make the whole response
/// unparsable — one strange row would blank the entire table.
fn jnum(v: Option<f32>) -> String {
    match v {
        Some(f) if f.is_finite() => f.to_string(),
        _ => "null".to_string(),
    }
}

fn error_json(message: &str) -> String {
    format!(r#"{{"ok":false,"error":{}}}"#, jstr(message))
}

/// The copyright holder, as `COPYRIGHT` at the repository root states it.
/// Here because the health endpoint is a machine's way of asking who granted
/// the licence it already reports, and it is not an interactive interface,
/// so it carries no obligation for a modifier (AGPL §5(d)).
pub const COPYRIGHT: &str = "Copyright (C) 2026 celastro";

fn health_json(db: &Db) -> String {
    let version = jstr(env!("CARGO_PKG_VERSION"));
    let source = jstr(&source_url());
    let license = jstr(env!("CARGO_PKG_LICENSE"));
    let copyright = jstr(COPYRIGHT);
    let collections = db.collection_count();
    // Which node answered: a client behind a Service that spreads its
    // requests can see them land, and a node without an address says so.
    let node = match db.node() {
        Some(n) => jstr(n),
        None => "null".to_string(),
    };
    // Verified since start, for a readiness probe: a node that has not yet
    // reached its peers can coordinate nothing that lives on them.
    let attached = db.attached_count();
    format!(
        r#"{{"ok":true,"name":"celastro","version":{version},"source":{source},"license":{license},"copyright":{copyright},"collections":{collections},"node":{node},"attached":{attached}}}"#
    )
}

/// Ask the console on `port` whether it is serving: `Ok(true)` for a 200
/// that says so, `Ok(false)` for any other answer, `Err` for no answer.
/// What `celastro-cli health` runs, and what a container's probe runs,
/// because the image has no shell and no curl and the console binds
/// loopback, which a probe from outside the pod cannot reach.
pub fn probe_health(port: u16, tls: Option<&Arc<Tls>>) -> Result<bool> {
    let raw = probe(port, tls)?;
    let ok_status = raw.starts_with("HTTP/1.1 200 ") || raw.starts_with("HTTP/1.0 200 ");
    Ok(ok_status && raw.contains(r#""ok":true"#))
}

/// How many other nodes the console on `port` has verified since it
/// started, from the same answer: what `celastro-cli health --attached N`
/// compares, so a readiness probe can wait for a node's peers. `Ok(0)` for
/// a console whose answer does not carry the count.
pub fn probe_attached(port: u16, tls: Option<&Arc<Tls>>) -> Result<u64> {
    let raw = probe(port, tls)?;
    let n = raw
        .split_once(r#""attached":"#)
        .map(|(_, rest)| rest.chars().take_while(char::is_ascii_digit).collect::<String>())
        .and_then(|d| d.parse().ok())
        .unwrap_or(0);
    Ok(n)
}

/// One `GET /api/health` on loopback, the raw answer. With `tls` the
/// console is expected to serve TLS and to name `localhost` in its
/// certificate, which is what the chart's certificates do.
fn probe(port: u16, tls: Option<&Arc<Tls>>) -> Result<String> {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let s = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut s = tls::connect(tls, s, "localhost")?;
    // One write: `write!` would send the request in pieces, one per
    // formatted fragment, and a server that reads once and closes answers
    // the first piece with a reset.
    let request =
        format!("GET /api/health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    s.write_all(request.as_bytes())?;
    let mut raw = String::new();
    s.read_to_string(&mut raw)?;
    Ok(raw)
}

/// Where the source of this build is offered, as AGPL §13 requires of a
/// program its users interact with over a network: the repository the crate
/// declares, at the tag of the version that is running. Absolute, so it means
/// the same thing from a browser on the host and from inside a container.
///
/// A modified fork that serves this console is offering someone else's source
/// unless it points `repository` in Cargo.toml at its own; that field is the
/// only input here, on purpose, so the fix is one line in the manifest.
fn source_url() -> String {
    format!("{}/tree/v{}", env!("CARGO_PKG_REPOSITORY"), env!("CARGO_PKG_VERSION"))
}

/// The console page with its source offer filled in. See [`source_url`].
fn with_source_offer(html: &str) -> String {
    html.replace("__CELASTRO_SOURCE__", &source_url())
        .replace("__CELASTRO_VERSION__", concat!("v", env!("CARGO_PKG_VERSION")))
}

fn index_kind_name(kind: &IndexKind) -> &'static str {
    match kind {
        IndexKind::FullText { .. } => "fulltext",
        IndexKind::Vector { .. } => "vector",
        IndexKind::Secondary => "secondary",
        IndexKind::Adjacency { .. } => "adjacency",
    }
}

fn catalog_json(db: &Db) -> String {
    let mut out = String::from(r#"{"ok":true,"collections":["#);
    for (i, (name, c)) in db.catalog.collections.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let partition = match &c.partition_key {
            Some(p) => jstr(p),
            None => "null".to_string(),
        };
        out.push_str(&format!(r#"{{"name":{},"primary_key":"#, jstr(name)));
        out.push_str(&jstr(&c.primary_key));
        out.push_str(&format!(r#","partition_key":{partition}"#));
        out.push_str(&format!(r#","doc_count":{},"indexes":["#, c.doc_count));
        for (j, idx) in c.indexes.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&format!(r#"{{"name":{},"path":"#, jstr(&idx.name)));
            out.push_str(&jstr(&idx.path));
            out.push_str(&format!(r#","kind":{}"#, jstr(index_kind_name(&idx.kind))));
            out.push_str(&format!(r#","tier":{}}}"#, jstr(idx.tier.name())));
        }
        // The inferred paths too, so the console can offer field names without
        // a second round trip.
        out.push_str(r#"],"paths":["#);
        for (j, p) in c.paths.keys().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&jstr(p));
        }
        out.push_str("]}");
    }
    out.push_str("]}");
    out
}

pub(crate) fn rows_json(r: &QueryResult, elapsed_ms: u128) -> String {
    let count = r.rows.len();
    let mut out = format!(r#"{{"ok":true,"kind":"rows","count":{count},"#);
    out.push_str(&format!(r#""elapsed_ms":{elapsed_ms},"missing":["#));
    for (i, m) in r.missing.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&jstr(m));
    }
    // Beside `missing`, and unconditional: a client that renders a row count
    // has no other way to learn the count is short because a `foo*` was cut.
    out.push_str(r#"],"truncated_prefixes":["#);
    for (i, t) in r.truncated_prefixes.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&jstr(t));
    }
    out.push_str(r#"],"cut_walks":["#);
    for (i, t) in r.cut_walks.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&jstr(t));
    }
    out.push_str(r#"],"next_cursor":"#);
    match &r.next_cursor {
        Some(c) => out.push_str(&jstr(c)),
        None => out.push_str("null"),
    }
    out.push_str(r#","rows":["#);
    for (i, row) in r.rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(r#"{{"key":{},"score":"#, jstr(&row.key)));
        out.push_str(&jnum(row.score));
        out.push_str(&format!(r#","distance":{}"#, jnum(row.distance)));
        out.push_str(&format!(r#","doc":{}}}"#, json::to_string(&row.doc)));
    }
    out.push_str("]}");
    out
}

/// Pull the statement out of a `POST /api/query` body.
///
/// A body that is not JSON, or JSON without a string `sql`, is a malformed
/// request — 400 — and not a SQL error: nothing was asked, so there is nothing
/// to report inline.
fn sql_from_body(body: &[u8]) -> std::result::Result<String, Reject> {
    let text = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(_) => return Err(Reject::BadRequest),
    };
    let v = match json::parse(text) {
        Ok(v) => v,
        Err(_) => return Err(Reject::BadRequest),
    };
    match v.get("sql").and_then(|s| s.as_str()) {
        Some(sql) => Ok(sql.to_string()),
        None => Err(Reject::BadRequest),
    }
}

/// Run one statement and shape the answer.
///
/// Every outcome, failure included, is HTTP 200. By the time the SQL runs the
/// request was well formed and authorised, so what the database thinks of the
/// statement is content, not protocol.
///
/// A statement that changed something is durable before it is acknowledged.
/// Without that, `celastro-cli --dir ./data serve` writes through the console
/// and loses every one of those writes to a Ctrl-C or a closed terminal, which
/// is the worst thing this tool could do to somebody.
///
/// Two things make that true and only the second one is here. The documents are
/// already on the disk when `execute` returns: `Shard::insert` and
/// `Shard::delete` fsync the WAL record before the change is visible to a
/// reader, one record per document, so a crash between `execute` and this line
/// takes back nothing the client is about to be told about. What `persist` adds
/// is the published state a reopen needs in order not to replay from the
/// beginning — the catalog and each shard's manifest — and it writes only the
/// ones whose bytes actually changed. For a stream of inserts into a collection
/// with no secondary index that is none of them: the segment set does not move
/// until a seal, and the catalog does not move at all. It is not none of them
/// in general — a query against an indexed collection moves that index's
/// activity clock, which lives in the catalog, so the next mutating statement
/// republishes CATALOG. The skip is what makes the common case free, not a
/// promise that nothing is ever written.
/// Persist is a no-op for an in-memory database, so this costs nothing where
/// there is nothing to lose; and when it fails the client is told, because an
/// ack that claims a durability the disk does not have is worse than an error.
fn run_sql(db: &RwLock<Db>, sql: &str) -> Response {
    let started = Instant::now();
    // Parsed first, without any lock, to know which lock: a read runs under
    // the shared one beside other reads; everything else takes the
    // exclusive one. Parsing twice for a write is cheap; a wrong lock is
    // not.
    let is_read = match sql::parse(sql, &[]) {
        Ok(stmt) => Db::is_read(&stmt),
        Err(e) => return Response::json(error_json(&e.to_string())),
    };
    let outcome = if is_read {
        let out = read(db).read(sql);
        // An index the read faulted in counts as used, and a demoted one is
        // promoted: that needs the write lock, taken now when nobody holds
        // the lock, so it lands in the request that caused it; with other
        // reads in flight it waits for the next writer instead of making
        // this read wait for them.
        if read(db).touches_pending() {
            if let Ok(mut db) = db.try_write() {
                if let Err(e) = db.apply_touches() {
                    return Response::json(error_json(&e.to_string()));
                }
            }
        }
        out
    } else {
        // Deferred work -- a backup's copy -- runs with the lock let go, so
        // the other connections are served while it copies.
        let mut guard = write(db);
        match guard.execute(sql) {
            Ok(Outcome::Deferred(d)) => {
                drop(guard);
                d.finish()
            }
            other => other,
        }
    };
    let elapsed = started.elapsed().as_millis();
    match outcome {
        Ok(Outcome::Rows(r)) => Response::json(rows_json(&r, elapsed)),
        Ok(Outcome::Ack(m)) => match write(db).persist() {
            Ok(()) => Response::json(ack_json(&m)),
            Err(e) => Response::json(error_json(&format!("{m}, but it is not on disk yet: {e}"))),
        },
        Ok(Outcome::Explain(t)) => Response::json(text_json("explain", &t)),
        Ok(Outcome::Recall(r)) => Response::json(text_json("recall", &r.render())),
        Ok(Outcome::Deferred(_)) => {
            Response::json(error_json("deferred work returned deferred work"))
        }
        Err(e) => Response::json(error_json(&e.to_string())),
    }
}

fn ack_json(message: &str) -> String {
    format!(r#"{{"ok":true,"kind":"ack","message":{}}}"#, jstr(message))
}

fn text_json(kind: &str, text: &str) -> String {
    format!(r#"{{"ok":true,"kind":"{kind}","text":{}}}"#, jstr(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::exec::Row;
    use crate::text::scorer::PREFIX_EXPANSION_LIMIT;
    use std::io::Cursor;
    use std::net::IpAddr;

    /// The port the tests pretend this console is bound to. Only the cross-site
    /// guard looks at it.
    const PORT: u16 = 7777;

    /// An in-memory request cannot block, but it is held to the same deadline,
    /// which is what makes every timeout path below reachable without a socket.
    impl<T: AsRef<[u8]>> Wire for Cursor<T> {
        fn arm(&mut self, deadline: Instant) -> std::result::Result<(), Reject> {
            if time_left(deadline).is_none() {
                return Err(Reject::RequestTimeout);
            }
            Ok(())
        }
    }

    /// A deadline no test trips by accident.
    fn far() -> Instant {
        Instant::now() + Duration::from_secs(600)
    }

    /// A client that sends one byte at a time and never finishes its head.
    ///
    /// This is the slowloris: every byte it sends would restart a per-`recv`
    /// timeout, and the accept loop serves one connection at a time.
    struct Dribble {
        pattern: &'static [u8],
        at: usize,
        pause: Duration,
    }

    impl Dribble {
        fn new(pause: Duration) -> Dribble {
            Dribble { pattern: b"X-Pad: aa\r\n", at: 0, pause }
        }
    }

    impl BufRead for Dribble {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            std::thread::sleep(self.pause);
            Ok(&self.pattern[self.at..self.at + 1])
        }

        fn consume(&mut self, n: usize) {
            self.at = (self.at + n) % self.pattern.len();
        }
    }

    impl Read for Dribble {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if out.is_empty() {
                return Ok(0);
            }
            let byte = self.fill_buf()?[0];
            self.consume(1);
            out[0] = byte;
            Ok(1)
        }
    }

    impl Wire for Dribble {
        fn arm(&mut self, deadline: Instant) -> std::result::Result<(), Reject> {
            if time_left(deadline).is_none() {
                return Err(Reject::RequestTimeout);
            }
            Ok(())
        }
    }

    fn wide() -> Deadlines {
        Deadlines { head: far(), body: far() }
    }

    fn head(text: &str) -> Head {
        parse_head(text).expect("this head should parse")
    }

    /// A GET with an optional token in the `X-Celastro-Token` header.
    fn get(target: &str, host: &str, token: Option<&str>) -> Head {
        let auth = match token {
            Some(t) => format!("X-Celastro-Token: {t}\r\n"),
            None => String::new(),
        };
        head(&format!("GET {target} HTTP/1.1\r\nHost: {host}\r\n{auth}\r\n"))
    }

    /// One whole request in, one whole response out, no socket involved.
    fn answer_to(request: &str) -> String {
        let mut db = Db::in_memory();
        serve_request(&mut db, request).0
    }

    /// The same, keeping the database and the caller's marching orders.
    fn serve_request(db: &mut Db, request: &str) -> (String, Next) {
        let mut io = Cursor::new(request.as_bytes());
        let shared = RwLock::new(std::mem::take(db));
        let served = answer(&mut io, "tok", PORT, Reach::Loopback, &shared, wide());
        *db = shared.into_inner().unwrap_or_else(|p| p.into_inner());
        let mut out = Vec::new();
        served.response.write_to(&mut out).expect("a Vec never fails to be written to");
        (String::from_utf8(out).expect("responses are utf-8"), served.next)
    }

    /// A whole response as bytes, the way the socket would have received it.
    fn rendered(response: &Response) -> Vec<u8> {
        let mut out = Vec::new();
        response.write_to(&mut out).expect("a Vec never fails to be written to");
        out
    }

    fn status_line(response: &str) -> &str {
        response.lines().next().unwrap_or("")
    }

    fn body_of(response: &str) -> &str {
        match response.split_once("\r\n\r\n") {
            Some((_, b)) => b,
            None => "",
        }
    }

    #[test]
    fn a_request_from_a_rebound_dns_name_is_refused() {
        let good = [
            "localhost",
            "LocalHost",
            "localhost:8080",
            "127.0.0.1",
            "127.0.0.1:8080",
            "[::1]",
            "[::1]:8080",
        ];
        for host in good {
            assert!(host_is_local(host), "{host} should be accepted");
        }
        let bad = [
            // The rebinding shape itself: a name the attacker controls whose A
            // record points at loopback.
            "console.evil.example",
            "127.0.0.1.evil.example",
            "localhost.evil.example",
            "evil.example:8080",
            // Neighbours in the text that are not loopback.
            "127.0.0.2",
            "0.0.0.0",
            "192.168.1.9",
            "10.0.0.1:8080",
            // Malformed authorities, which must be refused rather than guessed.
            "",
            "::1",
            "[::1].evil.example",
            "[::1",
            "localhost:",
            "localhost:port",
            "localhost:123456",
        ];
        for host in bad {
            assert!(!host_is_local(host), "{host} should be refused");
        }
    }

    #[test]
    fn a_rebound_host_is_refused_even_when_the_token_is_correct() {
        let h = get("/api/catalog?t=tok", "console.evil.example", None);
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::Forbidden));
        let h = get("/api/catalog?t=tok", "localhost", None);
        assert!(dispatch(&h, "tok", PORT).is_ok());
    }

    #[test]
    fn a_network_console_takes_any_host_but_still_the_token_and_its_own_origin() {
        let h = get("/api/catalog", "celastro-console:8787", Some("tok"));
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::Forbidden), "loopback refuses it");
        assert!(
            dispatch_for(&h, "tok", PORT, Reach::Network).is_ok(),
            "a network console serves it"
        );
        let wrong = get("/api/catalog", "celastro-console:8787", Some("nottok"));
        assert_eq!(
            dispatch_for(&wrong, "tok", PORT, Reach::Network).err(),
            Some(Reject::Unauthorized)
        );
        let none = get("/api/catalog", "celastro-console:8787", None);
        assert_eq!(
            dispatch_for(&none, "tok", PORT, Reach::Network).err(),
            Some(Reject::Unauthorized)
        );
        let no_host = head("GET /api/catalog HTTP/1.1\r\nX-Celastro-Token: tok\r\n");
        assert_eq!(
            dispatch_for(&no_host, "tok", PORT, Reach::Network).err(),
            Some(Reject::Forbidden)
        );
        // A browser on a page the console served names the console's own
        // host as its origin; a page from anywhere else does not.
        let post = |origin: &str| {
            head(&format!(
                "POST /api/query HTTP/1.1\r\nHost: celastro-console:8787\r\nX-Celastro-Token: tok\r\n\
                 Origin: {origin}\r\nContent-Type: application/json\r\nContent-Length: 0\r\n"
            ))
        };
        assert!(dispatch_for(&post("http://celastro-console:8787"), "tok", PORT, Reach::Network)
            .is_ok());
        assert!(dispatch_for(&post("HTTP://Celastro-Console:8787"), "tok", PORT, Reach::Network)
            .is_ok());
        for foreign in [
            "http://evil.example",
            "https://celastro-console:8787",
            "null",
            "http://celastro-console:9000",
        ] {
            assert_eq!(
                dispatch_for(&post(foreign), "tok", PORT, Reach::Network).err(),
                Some(Reject::Forbidden),
                "{foreign}"
            );
        }
        assert_eq!(
            dispatch(&post("http://celastro-console:8787"), "tok", PORT).err(),
            Some(Reject::Forbidden)
        );
    }

    #[test]
    fn a_network_bind_refuses_a_weak_token_and_keeps_the_one_it_is_given() {
        let lo = IpAddr::V4(Ipv4Addr::LOCALHOST);
        for weak in [
            "",
            "short",
            "fifteen-bytes..",
            "sixteen bytes ok",
            "sixteen\tbytes-ok",
            "sixteen-bytes-ok\n",
        ] {
            let refused = Server::bind_network(lo, 0, weak.to_string());
            assert!(refused.is_err(), "{weak:?} must be refused");
        }
        let good = "0123456789abcdef0123456789abcdef";
        let s = Server::bind_network(lo, 0, good.to_string()).unwrap();
        assert_eq!(s.token(), good);
        assert_eq!(s.reach(), Reach::Loopback, "a loopback address keeps the loopback guards");
        assert!(s.url().starts_with("http://127.0.0.1:"));
        assert!(s.url().ends_with(&format!("/?t={good}")));
        let any =
            Server::bind_network(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0, good.to_string()).unwrap();
        assert_eq!(any.reach(), Reach::Network);
        assert!(any.url().starts_with("http://0.0.0.0:"));
    }

    #[test]
    fn a_request_with_no_host_header_is_refused_rather_than_assumed_local() {
        let h = head("GET /?t=tok HTTP/1.1\r\n\r\n");
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::Forbidden));
    }

    #[test]
    fn a_wrong_token_is_refused_and_the_right_one_is_accepted() {
        let expected = "0123456789abcdef";
        let wrong = ["", "x", "0123456789abcde", "0123456789abcdef0", "0123456789abcdee"];
        for guess in wrong {
            let h = get(&format!("/api/catalog?t={guess}"), "localhost", None);
            let refused = dispatch(&h, expected, PORT).err();
            assert_eq!(refused, Some(Reject::Unauthorized), "`{guess}` must not be accepted");
        }
        let h = get(&format!("/api/catalog?t={expected}"), "localhost", None);
        assert!(dispatch(&h, expected, PORT).is_ok());
        // The header form is the same credential through another door.
        let h = get("/api/catalog", "localhost", Some(expected));
        assert!(dispatch(&h, expected, PORT).is_ok());
        let h = get("/api/catalog", "localhost", Some("nope"));
        assert_eq!(dispatch(&h, expected, PORT).err(), Some(Reject::Unauthorized));
    }

    #[test]
    fn the_html_console_itself_is_not_served_without_the_token() {
        // The page is not public just because it is only a page: serving it
        // hands out the API surface and invites the browser to go and use it.
        for path in ["/", "/app.js", "/style.css", "/api/catalog"] {
            let h = get(path, "localhost", None);
            let refused = dispatch(&h, "tok", PORT).err();
            assert_eq!(refused, Some(Reject::Unauthorized), "{path} must require the token");
        }
        // The one path served without it, by decision: a supervisor's probe
        // cannot know the token, and health executes nothing. Still behind
        // the Host check, which the probe test pins.
        let h = get("/api/health", "localhost", None);
        assert!(matches!(dispatch(&h, "tok", PORT), Ok(Action::Health)));
        let h = get("/?t=tok", "localhost", None);
        assert!(matches!(dispatch(&h, "tok", PORT), Ok(Action::Reply(_))));
    }

    #[test]
    fn the_pages_own_stylesheet_and_script_arrive_with_a_token_of_their_own() {
        // A `<link>` and a `<script>` cannot send the header, so a bare path in
        // the markup means the browser fetches those two anonymously and this
        // server refuses them: a console that renders unstyled and does nothing.
        let markup = "<link rel=\"stylesheet\" href=\"/style.css\">\n<script src=\"/app.js\">";
        let rewritten = with_tokenised_assets(markup, "abc");
        assert!(rewritten.contains("\"/style.css?t=abc\""), "{rewritten}");
        assert!(rewritten.contains("\"/app.js?t=abc\""), "{rewritten}");
        // Whatever the markup says, the page as served must not link an asset
        // path that carries no token.
        let served = with_tokenised_assets(INDEX_HTML, "tok");
        for asset in ["/style.css", "/app.js"] {
            for (at, _) in served.match_indices(asset) {
                let rest = &served[at + asset.len()..];
                assert!(rest.starts_with("?t="), "{asset} is linked without a token");
            }
        }
        // And the tokenised URLs are ones the router accepts.
        let h = get("/style.css?t=tok", "localhost", None);
        assert!(dispatch(&h, "tok", PORT).is_ok());
        let h = get("/app.js?t=tok", "localhost", None);
        assert!(dispatch(&h, "tok", PORT).is_ok());
    }

    #[test]
    fn a_token_comparison_reads_past_the_byte_where_the_guess_first_goes_wrong() {
        // What a constant-time comparison has to do is examine the whole input:
        // an `==` that returns at the first difference says, in how long it
        // took, how much of the guess was right, and that turns one search of a
        // 128-bit token into 32 independent guesses of a hex digit.
        //
        // The property tested is the one that is actually checkable — a
        // difference anywhere is caught, and a length difference at either end
        // is caught. Timing it and asserting a ratio of two measured durations
        // is a test of how busy the machine is, and it fails in CI for reasons
        // that have nothing to do with this function.
        let expected = "0123456789abcdef0123456789abcdef";
        assert!(token_matches(expected, expected));
        for i in 0..expected.len() {
            let mut guess = expected.to_string();
            let wrong = if expected.as_bytes()[i] == b'0' { "1" } else { "0" };
            guess.replace_range(i..i + 1, wrong);
            assert_ne!(guess, expected);
            assert!(!token_matches(expected, &guess), "a difference at byte {i} must be caught");
        }
        // Prefixes and extensions of the right token, at both ends.
        for cut in 1..expected.len() {
            assert!(!token_matches(expected, &expected[..cut]), "a {cut}-byte prefix is not it");
            assert!(!token_matches(expected, &expected[cut..]), "a {cut}-byte suffix is not it");
        }
        assert!(!token_matches(expected, &format!("{expected}0")));
        // And the wraparound the accumulator uses to keep short guesses costing
        // the same is not a way to match: a guess that repeats is still wrong.
        assert!(!token_matches(expected, "01234567"));
        assert!(!token_matches("aaaa", "a"));
    }

    #[test]
    fn a_token_comparison_ignores_neither_the_length_nor_the_tail() {
        assert!(token_matches("abcdef", "abcdef"));
        assert!(!token_matches("abcdef", "abcde"));
        assert!(!token_matches("abcdef", "abcdefg"));
        assert!(!token_matches("abcdef", "abcdeg"));
        assert!(!token_matches("abcdef", "abcabc"));
        assert!(!token_matches("abcdef", ""));
    }

    #[test]
    fn an_oversized_content_length_is_refused_before_a_buffer_is_sized_from_it() {
        assert_eq!(parse_content_length("1073741824"), Err(Reject::PayloadTooLarge));
        // Too large to be a `u64` at all: over the cap by inspection, not a
        // parse failure to be reported as a malformed request.
        let huge = "999999999999999999999999999";
        assert_eq!(parse_content_length(huge), Err(Reject::PayloadTooLarge));
        assert_eq!(parse_content_length(&(MAX_BODY + 1).to_string()), Err(Reject::PayloadTooLarge));
        // The cap is a limit, not an off-by-one: exactly MAX_BODY is accepted.
        assert_eq!(parse_content_length(&MAX_BODY.to_string()), Ok(MAX_BODY));
        assert_eq!(parse_content_length("11"), Ok(11));
        // A length that is not a number is malformed rather than large.
        assert_eq!(parse_content_length("12x"), Err(Reject::BadRequest));
        assert_eq!(parse_content_length("-1"), Err(Reject::BadRequest));
        assert_eq!(parse_content_length(""), Err(Reject::BadRequest));
        // The allocating function refuses a bad length on its own, so the bound
        // does not depend on one caller remembering to check first.
        let mut empty = Cursor::new(Vec::new());
        assert_eq!(read_body(&mut empty, MAX_BODY + 1, far()), Err(Reject::PayloadTooLarge));
        assert_eq!(Reject::PayloadTooLarge.status().0, 413);
    }

    #[test]
    fn a_huge_declared_body_is_answered_413_without_reading_a_byte_of_it() {
        // The request promises two mebibytes and sends none of them. Reading
        // before checking would block here until the read timeout.
        let mut request = String::from("POST /api/query?t=tok HTTP/1.1\r\n");
        request.push_str("Host: localhost\r\nContent-Length: 2097152\r\n\r\n");
        let response = answer_to(&request);
        assert_eq!(status_line(&response), "HTTP/1.1 413 Payload Too Large");
    }

    #[test]
    fn two_content_length_headers_are_refused_rather_than_resolved() {
        // Preferring one of them is the parser disagreement that request
        // smuggling is assembled from.
        let mut text = String::from("POST /api/query HTTP/1.1\r\nHost: localhost\r\n");
        text.push_str("Content-Length: 4\r\nContent-Length: 40\r\n\r\n");
        assert_eq!(parse_head(&text).err(), Some(Reject::BadRequest));
        let text = "GET / HTTP/1.1\r\nHost: localhost\r\nHost: evil.example\r\n\r\n";
        assert_eq!(parse_head(text).err(), Some(Reject::BadRequest));
        // Chunked framing is not implemented, so it is refused, not ignored.
        let mut text = String::from("POST /api/query HTTP/1.1\r\nHost: localhost\r\n");
        text.push_str("Transfer-Encoding: chunked\r\n\r\n");
        assert_eq!(parse_head(&text).err(), Some(Reject::BadRequest));
    }

    #[test]
    fn a_malformed_request_line_is_answered_with_400_rather_than_panicking() {
        let bad = [
            "",
            "\r\n",
            "GET\r\n\r\n",
            "GET /\r\n\r\n",
            "GET / HTTP/1.1 extra\r\n\r\n",
            "GET  / HTTP/1.1\r\n\r\n",
            "get / HTTP/1.1\r\n\r\n",
            "GET / SPDY/3.1\r\n\r\n",
            // Absolute-form: the authority in the target is not the one the
            // `Host` check looks at.
            "GET http://evil.example/ HTTP/1.1\r\nHost: localhost\r\n\r\n",
            // Header shapes that must not be guessed at.
            "GET / HTTP/1.1\r\nHost localhost\r\n\r\n",
            "GET / HTTP/1.1\r\nHost : localhost\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: localhost\r\n continued\r\n\r\n",
        ];
        for text in bad {
            assert_eq!(parse_head(text).err(), Some(Reject::BadRequest), "{text:?}");
        }
        assert_eq!(Reject::BadRequest.status().0, 400);
        // And the same shapes arriving on a stream, where a panic would take
        // the whole process down rather than one connection.
        for text in ["", "GET", "GET / HTTP/1.1\r\n", "\r\n\r\n", "\u{0}\u{0}\r\n\r\n"] {
            let response = answer_to(text);
            assert_eq!(status_line(&response), "HTTP/1.1 400 Bad Request", "{text:?}");
        }
    }

    #[test]
    fn a_head_that_never_ends_is_refused_instead_of_buffered_without_limit() {
        // One enormous header line.
        let pad = "a".repeat(MAX_LINE);
        let flood = format!("GET / HTTP/1.1\r\nHost: localhost\r\nX-Pad: {pad}\r\n\r\n");
        let mut io = Cursor::new(flood.as_bytes());
        assert_eq!(read_head(&mut io, far()).err(), Some(Reject::BadRequest));
        // Bytes that never contain a newline at all.
        let endless = "a".repeat(MAX_LINE * 4);
        let mut io = Cursor::new(endless.as_bytes());
        assert_eq!(read_head(&mut io, far()).err(), Some(Reject::BadRequest));
        // Many small header lines.
        let mut many = String::from("GET / HTTP/1.1\r\nHost: localhost\r\n");
        for i in 0..MAX_HEADERS + 8 {
            many.push_str(&format!("X-Pad-{i}: 1\r\n"));
        }
        many.push_str("\r\n");
        let mut io = Cursor::new(many.as_bytes());
        assert_eq!(read_head(&mut io, far()).err(), Some(Reject::BadRequest));
        assert_eq!(parse_head(&many).err(), Some(Reject::BadRequest));
        // A head inside every limit still parses, so the caps are not simply
        // refusing everything.
        let fine = "GET /?t=tok HTTP/1.1\r\nHost: localhost:9999\r\nAccept: */*\r\n\r\n";
        let h = read_head(&mut Cursor::new(fine.as_bytes()), far()).unwrap();
        assert_eq!(h.method, "GET");
        assert_eq!(h.path, "/");
        assert_eq!(h.query, "t=tok");
        assert_eq!(h.header("host"), Some("localhost:9999"));
        assert_eq!(h.content_length, 0);
        assert!(dispatch(&h, "tok", PORT).is_ok());
    }

    #[test]
    fn an_unknown_path_is_404_and_a_known_path_with_the_wrong_method_is_405() {
        let post = |path: &str| {
            let mut text = format!("POST {path} HTTP/1.1\r\nHost: localhost\r\n");
            text.push_str("X-Celastro-Token: tok\r\nContent-Type: application/json\r\n");
            text.push_str("Content-Length: 0\r\n\r\n");
            head(&text)
        };
        assert_eq!(dispatch(&post("/"), "tok", PORT).err(), Some(Reject::MethodNotAllowed("GET")));
        let refused = dispatch(&post("/api/health"), "tok", PORT).err();
        assert_eq!(refused, Some(Reject::MethodNotAllowed("GET")));
        let h = get("/api/query", "localhost", Some("tok"));
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::MethodNotAllowed("POST")));
        let h = get("/etc/passwd", "localhost", Some("tok"));
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::NotFound));
        let h = get("/../src/serve.rs", "localhost", Some("tok"));
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::NotFound));
        assert_eq!(Reject::NotFound.status().0, 404);
        assert_eq!(Reject::MethodNotAllowed("GET").status().0, 405);
        assert!(matches!(dispatch(&post("/api/query"), "tok", PORT), Ok(Action::Query)));
        assert!(matches!(dispatch(&post("/api/shutdown"), "tok", PORT), Ok(Action::Shutdown)));
        let h = get("/api/shutdown", "localhost", Some("tok"));
        assert_eq!(dispatch(&h, "tok", PORT).err(), Some(Reject::MethodNotAllowed("POST")));
    }

    #[test]
    fn every_protocol_failure_keeps_its_own_status_code_end_to_end() {
        let cases = [
            ("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n", "HTTP/1.1 401 Unauthorized"),
            ("GET /?t=nope HTTP/1.1\r\nHost: localhost\r\n\r\n", "HTTP/1.1 401 Unauthorized"),
            ("GET /?t=tok HTTP/1.1\r\nHost: evil.example\r\n\r\n", "HTTP/1.1 403 Forbidden"),
            ("GET /nope?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n", "HTTP/1.1 404 Not Found"),
            ("nonsense\r\n\r\n", "HTTP/1.1 400 Bad Request"),
        ];
        for (request, expected) in cases {
            assert_eq!(status_line(&answer_to(request)), expected, "{request:?}");
        }
        let health = answer_to("GET /api/health?t=tok HTTP/1.1\r\nHost: 127.0.0.1:9\r\n\r\n");
        assert_eq!(status_line(&health), "HTTP/1.1 200 OK");
        let parsed = json::parse(body_of(&health)).expect("health must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(parsed.get("name").and_then(|v| v.as_str()), Some("celastro"));
        let version = parsed.get("version").and_then(|v| v.as_str());
        assert_eq!(version, Some(env!("CARGO_PKG_VERSION")));
    }

    /// Health needs no token, touches the database, and is still behind the
    /// `Host` check: a probe from the pod's own network namespace gets a 200
    /// carrying the collection count, and a page on another origin that
    /// rebinds a name gets the 403 everything else gets. The count is real --
    /// it moves when a collection is created.
    #[test]
    fn the_health_probe_needs_no_token_and_reports_the_database() {
        let no_token = answer_to("GET /api/health HTTP/1.1\r\nHost: 127.0.0.1:9\r\n\r\n");
        assert_eq!(status_line(&no_token), "HTTP/1.1 200 OK");
        let parsed = json::parse(body_of(&no_token)).unwrap();
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(parsed.get("collections").and_then(|v| v.as_i64()), Some(0));
        let rebound = answer_to("GET /api/health HTTP/1.1\r\nHost: evil.example\r\n\r\n");
        assert_eq!(status_line(&rebound), "HTTP/1.1 403 Forbidden");
        let mut db = Db::in_memory();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        let parsed = json::parse(&health_json(&db)).unwrap();
        assert_eq!(parsed.get("collections").and_then(|v| v.as_i64()), Some(1));
    }

    /// The probe's client half: a serving console answers true, a server
    /// that is up but not well answers false, and a port with nothing on it
    /// is an error rather than a false -- different answers for a
    /// supervisor.
    /// Behind a Service every pod is one console; a client that opens a
    /// connection and sends nothing must not be the statement everybody
    /// else waits behind. The idle connection's head deadline (two seconds)
    /// is longer than this test allows the other request (one), so a loop
    /// that served one connection at a time fails it.
    #[test]
    fn an_idle_connection_does_not_delay_another_clients_statement() {
        use std::io::{Read, Write};
        let server = Server::bind(0).unwrap();
        let port = server.local_addr().port();
        let token = server.token().to_string();
        let db = std::sync::Arc::new(RwLock::new(Db::in_memory()));
        let serving = {
            let db = db.clone();
            std::thread::spawn(move || server.run(&db))
        };
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        let idle = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        // Sends nothing, stays open.
        let started = Instant::now();
        let mut busy = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        busy.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let request = format!(
            "GET /api/health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
        );
        busy.write_all(request.as_bytes()).unwrap();
        let mut raw = String::new();
        busy.read_to_string(&mut raw).unwrap();
        assert!(raw.starts_with("HTTP/1.1 200 "), "{raw}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the statement waited {:?} behind an idle connection",
            started.elapsed()
        );
        assert!(HEAD_DEADLINE > Duration::from_secs(1), "the test's bound is inside the deadline");
        drop(idle);
        let mut stop = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let shutdown = format!(
            "POST /api/shutdown HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-Celastro-Token: {token}\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stop.write_all(shutdown.as_bytes()).unwrap();
        let mut raw = String::new();
        let _ = stop.read_to_string(&mut raw);
        assert!(raw.starts_with("HTTP/1.1 200 "), "{raw}");
        serving.join().unwrap().unwrap();
    }

    #[test]
    fn the_probe_tells_serving_from_unwell_from_absent() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for (i, stream) in listener.incoming().enumerate() {
                let mut s = stream.unwrap();
                // Read the whole head before answering: closing with unread
                // bytes is a reset, not an answer.
                let mut raw = Vec::new();
                let mut buf = [0u8; 256];
                while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => raw.extend_from_slice(&buf[..n]),
                    }
                }
                let body =
                    if i == 0 { r#"{"ok":true,"collections":0}"# } else { r#"{"ok":false}"# };
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        assert!(probe_health(port, None).unwrap(), "a serving console");
        assert_eq!(probe_attached(port, None).unwrap(), 0, "a node that attached nothing");
        assert!(!probe_health(port, None).unwrap(), "an answer that is not well");
        let closed =
            std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        assert!(probe_health(closed, None).is_err(), "nothing listening is an error, not a false");
    }

    /// The console reads `truncated_prefixes` off the wire and renders it the
    /// way it renders `missing`. This is a static check on the script -- the
    /// console has no runtime here to execute it in -- so what it pins is
    /// that the field is read at all and that the rendering says what the
    /// shells say. A script that read the field into nothing would pass it;
    /// a script that dropped the field, which is what shipped, does not.
    #[test]
    fn the_console_script_reads_and_renders_a_truncated_expansion() {
        let read = APP_JS.find("res.truncated_prefixes").expect("the field is never read");
        let shown = APP_JS.find("'TRUNCATED — '").expect("the field is never rendered");
        assert!(read < shown, "rendered before it is read");
        let partial = APP_JS.find("PARTIAL RESULT").unwrap();
        assert!((read as i64 - partial as i64).abs() < 1200, "not beside the partial-result block");
        // The third sibling, a walk a cap bound, the same way.
        let read = APP_JS.find("res.cut_walks").expect("cut_walks is never read");
        let shown = APP_JS.find("'CUT — '").expect("cut_walks is never rendered");
        assert!(read < shown, "rendered before it is read");
        assert!((read as i64 - partial as i64).abs() < 1200, "not beside the partial-result block");
    }

    /// The console executes SQL for whoever holds the token, over HTTP, which
    /// is the interaction AGPL §13 attaches a source offer to. The offer is
    /// on the page a user sees, names THIS version, and is an absolute URL --
    /// a relative one would resolve to the loopback address it was served
    /// from, which from inside a container is nowhere. The same URL is on the
    /// health endpoint for a client that never renders the page.
    #[test]
    fn the_console_offers_the_source_of_the_running_version() {
        let expected =
            format!("{}/tree/v{}", env!("CARGO_PKG_REPOSITORY"), env!("CARGO_PKG_VERSION"));
        assert!(expected.starts_with("https://"), "{expected}");
        let page = answer_to("GET /?t=tok HTTP/1.1\r\nHost: 127.0.0.1:9\r\n\r\n");
        assert_eq!(status_line(&page), "HTTP/1.1 200 OK");
        let body = body_of(&page);
        assert!(body.contains(&format!("href=\"{expected}\"")), "no source link on the page");
        assert!(body.contains("AGPL"), "the licence is not named on the page");
        assert!(!body.contains("__CELASTRO_"), "a placeholder reached the browser: {body}");
        let health = answer_to("GET /api/health?t=tok HTTP/1.1\r\nHost: 127.0.0.1:9\r\n\r\n");
        let parsed = json::parse(body_of(&health)).unwrap();
        assert_eq!(parsed.get("source").and_then(|v| v.as_str()), Some(expected.as_str()));
        assert_eq!(parsed.get("license").and_then(|v| v.as_str()), Some("AGPL-3.0-only"));
        assert_eq!(
            parsed.get("copyright").and_then(|v| v.as_str()),
            Some("Copyright (C) 2026 celastro"),
            "the holder travels with the licence it granted"
        );
        let notice = include_str!("../COPYRIGHT");
        assert!(notice
            .starts_with("celastro — a hybrid document database\nCopyright (C) 2026 celastro\n"));
        assert!(
            notice.contains("version 3 of the License\nonly."),
            "the notice says -only, as Cargo.toml does"
        );
    }

    #[test]
    fn a_statement_arriving_over_http_is_run_and_its_answer_is_shaped_by_kind() {
        let sql = r#"{"sql":"CREATE COLLECTION items (id TEXT PRIMARY KEY, kind TEXT)"}"#;
        let mut request = String::from("POST /api/query?t=tok HTTP/1.1\r\nHost: localhost\r\n");
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n\r\n{sql}", sql.len()));
        let response = answer_to(&request);
        assert_eq!(status_line(&response), "HTTP/1.1 200 OK");
        let parsed = json::parse(body_of(&response)).expect("the reply must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)), "{response}");
        assert_eq!(parsed.get("kind").and_then(|v| v.as_str()), Some("ack"));
        assert!(parsed.get("message").and_then(|v| v.as_str()).is_some());
    }

    #[test]
    fn a_body_shorter_than_its_declared_length_is_a_bad_request_not_a_hang() {
        let mut request = String::from("POST /api/query?t=tok HTTP/1.1\r\n");
        request.push_str("Host: localhost\r\nContent-Type: application/json\r\n");
        request.push_str("Content-Length: 400\r\n\r\n{\"sql\":\"SELECT 1\"}");
        assert_eq!(status_line(&answer_to(&request)), "HTTP/1.1 400 Bad Request");
    }

    #[test]
    fn a_document_containing_a_quote_cannot_break_out_of_the_json_response() {
        let nasty = "he said \"hi\", then \\ and a newline\n and \u{1} and </script>";
        let doc = Value::obj(vec![
            ("text".to_string(), Value::Str(nasty.to_string())),
            ("tab\there".to_string(), Value::Str("\r\n\u{7}".to_string())),
        ]);
        let key = "key\"with\\escapes\nand\u{2}control".to_string();
        let mut r = QueryResult::default();
        r.rows = vec![Row { key: key.clone(), doc, score: Some(1.5), distance: None }];
        r.missing = vec!["tablet\"1".to_string()];
        r.truncated_prefixes = vec![r#"text_match(body, 'a"b*') was cut\"#.to_string()];
        r.cut_walks = vec!["WITHIN 2 HOPS OF 'p\"1' VIA cites was cut at hop 2".to_string()];
        r.next_cursor = Some("cursor\\\"value".to_string());
        let text = rows_json(&r, 7);

        // No raw control character survives into the response: if one did, the
        // envelope would be broken open at exactly that byte.
        assert!(!text.contains('\n'), "{text}");
        assert!(!text.chars().any(|c| (c as u32) < 0x20));

        let parsed = json::parse(&text).expect("the response must be parsable JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(parsed.get("kind").and_then(|v| v.as_str()), Some("rows"));
        assert_eq!(parsed.get("count").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(parsed.get("elapsed_ms").and_then(|v| v.as_i64()), Some(7));
        let cursor = parsed.get("next_cursor").and_then(|v| v.as_str());
        assert_eq!(cursor, Some("cursor\\\"value"));
        let missing = parsed.get("missing").and_then(|v| v.as_array()).unwrap();
        assert_eq!(missing[0].as_str(), Some("tablet\"1"));
        // `truncated_prefixes` is `missing`'s sibling on the wire and the most
        // user-facing sentence the design notes make ("every query that was cut
        // says so"), so it gets the same escaping proof rather than being left to an
        // envelope that only ever carried it empty. The prefix here is a legal
        // one — `text_match` takes a quoted string — so this is the real shape,
        // not a contrived one.
        let cut = parsed.get("truncated_prefixes").and_then(|v| v.as_array()).unwrap();
        assert_eq!(cut.len(), 1);
        assert_eq!(cut[0].as_str(), Some(r#"text_match(body, 'a"b*') was cut\"#));
        let walks = parsed.get("cut_walks").and_then(|v| v.as_array()).unwrap();
        assert_eq!(walks[0].as_str(), Some("WITHIN 2 HOPS OF 'p\"1' VIA cites was cut at hop 2"));
        let rows = parsed.get("rows").and_then(|v| v.as_array()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some(key.as_str()));
        assert_eq!(rows[0].get("distance"), Some(&Value::Null));
        let back = rows[0].get("doc").expect("the document comes back whole");
        assert_eq!(back.get("text").and_then(|v| v.as_str()), Some(nasty));
        assert_eq!(back.get("tab\there").and_then(|v| v.as_str()), Some("\r\n\u{7}"));
    }

    #[test]
    fn an_error_message_containing_a_quote_cannot_break_out_of_the_json_response() {
        let msg = "syntax error: unexpected \"'\\\n\u{3}";
        let parsed = json::parse(&error_json(msg)).expect("must be parsable JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(false)));
        assert_eq!(parsed.get("error").and_then(|v| v.as_str()), Some(msg));
        // The same for the two fixed text shapes and for an ack.
        let parsed = json::parse(&ack_json(msg)).expect("must be parsable JSON");
        assert_eq!(parsed.get("message").and_then(|v| v.as_str()), Some(msg));
        let parsed = json::parse(&text_json("explain", msg)).expect("must be parsable JSON");
        assert_eq!(parsed.get("kind").and_then(|v| v.as_str()), Some("explain"));
        assert_eq!(parsed.get("text").and_then(|v| v.as_str()), Some(msg));
    }

    #[test]
    fn a_non_finite_score_is_null_rather_than_a_bare_nan_token() {
        // `NaN` is not JSON. One strange row emitting it would make the whole
        // response unparsable and blank the table.
        let mut r = QueryResult::default();
        let row = Row {
            key: "k".to_string(),
            doc: Value::obj(Vec::new()),
            score: Some(f32::NAN),
            distance: Some(f32::INFINITY),
        };
        r.rows = vec![row];
        let text = rows_json(&r, 0);
        assert!(!text.contains("NaN") && !text.contains("inf"), "{text}");
        let parsed = json::parse(&text).unwrap();
        let rows = parsed.get("rows").and_then(|v| v.as_array()).unwrap();
        assert_eq!(rows[0].get("score"), Some(&Value::Null));
        assert_eq!(rows[0].get("distance"), Some(&Value::Null));
    }

    #[test]
    fn a_post_body_that_is_not_json_is_a_protocol_error_not_a_sql_error() {
        assert_eq!(sql_from_body(b""), Err(Reject::BadRequest));
        assert_eq!(sql_from_body(b"SELECT 1"), Err(Reject::BadRequest));
        assert_eq!(sql_from_body(br#"{"sql":1}"#), Err(Reject::BadRequest));
        assert_eq!(sql_from_body(br#"{"statement":"SELECT 1"}"#), Err(Reject::BadRequest));
        assert_eq!(sql_from_body(&[0xff, 0xfe]), Err(Reject::BadRequest));
        assert_eq!(sql_from_body(br#"{"sql":"SELECT 1"}"#), Ok("SELECT 1".to_string()));
        // A quote inside the statement is data, not a parse problem.
        let quoted = sql_from_body(br#"{"sql":"SELECT \"a\\b\""}"#);
        assert_eq!(quoted, Ok(r#"SELECT "a\b""#.to_string()));
    }

    #[test]
    fn a_sql_error_is_reported_inline_with_200_rather_than_as_a_protocol_failure() {
        let db = RwLock::new(Db::in_memory());
        let response = run_sql(&db, "SELECT FROM WHERE nonsense");
        assert_eq!(response.status, 200, "a SQL error must not become an HTTP error");
        assert_eq!(response.content_type, CT_JSON);
        let parsed = json::parse(&response.body).expect("the error must still be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(false)));
        assert!(parsed.get("error").and_then(|v| v.as_str()).is_some());
    }

    /// Reads share the lock and writes take it alone: a read completes
    /// while another reader holds the lock, and a write waits for that
    /// reader to let go. With one mutex the first would deadlock and the
    /// second would be indistinguishable from it.
    #[test]
    fn reads_run_beside_each_other_and_a_write_waits_for_them() {
        let db = Arc::new(RwLock::new(Db::in_memory()));
        assert!(run_sql(&db, "CREATE COLLECTION items (id TEXT PRIMARY KEY)")
            .body
            .contains("\"ok\":true"));
        assert!(run_sql(&db, "INSERT INTO items VALUES ('{\"id\":\"a\"}')")
            .body
            .contains("\"ok\":true"));
        let held = read(&db);
        // A read beside the held read: answered.
        let rows = run_sql(&db, "SELECT id FROM items LIMIT 10");
        assert!(rows.body.contains("\"count\":1"), "{}", rows.body);
        // A write beside it: waits until the read is let go.
        let (tx, rx) = std::sync::mpsc::channel();
        let db2 = db.clone();
        let writer = std::thread::spawn(move || {
            let r = run_sql(&db2, "INSERT INTO items VALUES ('{\"id\":\"b\"}')");
            tx.send(()).unwrap();
            r
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(300)).is_err(),
            "the write ran while a read held the lock"
        );
        drop(held);
        rx.recv_timeout(std::time::Duration::from_secs(10))
            .expect("the write ran once the read let go");
        assert!(writer.join().unwrap().body.contains("\"ok\":true"));
        let rows = run_sql(&db, "SELECT id FROM items LIMIT 10");
        assert!(rows.body.contains("\"count\":2"), "{}", rows.body);
    }

    #[test]
    fn the_catalog_describes_the_collections_that_exist() {
        let db = RwLock::new(Db::in_memory());
        let created = run_sql(&db, "CREATE COLLECTION items (id TEXT PRIMARY KEY)");
        assert_eq!(created.status, 200);
        let rows = run_sql(&db, "SELECT * FROM items LIMIT 10");
        let parsed = json::parse(&rows.body).unwrap();
        assert_eq!(parsed.get("kind").and_then(|v| v.as_str()), Some("rows"), "{}", rows.body);
        assert_eq!(parsed.get("count").and_then(|v| v.as_i64()), Some(0));

        let parsed =
            json::parse(&catalog_json(&read(&db))).expect("the catalog must be valid JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        let colls = parsed.get("collections").and_then(|v| v.as_array()).unwrap();
        assert_eq!(colls.len(), 1);
        assert_eq!(colls[0].get("name").and_then(|v| v.as_str()), Some("items"));
        assert_eq!(colls[0].get("primary_key").and_then(|v| v.as_str()), Some("id"));
        assert_eq!(colls[0].get("partition_key"), Some(&Value::Null));
        assert!(colls[0].get("indexes").and_then(|v| v.as_array()).is_some());
    }

    #[test]
    fn a_collection_named_with_a_quote_cannot_break_out_of_the_catalog_response() {
        let mut db = Db::in_memory();
        let mut c = crate::catalog::Collection::new("odd\"name\n", "id", None);
        c.paths.insert("a\\b".to_string(), crate::catalog::PathStats::default());
        db.catalog.create(c).unwrap();
        let text = catalog_json(&db);
        assert!(!text.contains('\n'), "{text}");
        let parsed = json::parse(&text).expect("the catalog must survive an odd name");
        let colls = parsed.get("collections").and_then(|v| v.as_array()).unwrap();
        assert_eq!(colls[0].get("name").and_then(|v| v.as_str()), Some("odd\"name\n"));
        let paths = colls[0].get("paths").and_then(|v| v.as_array()).unwrap();
        assert_eq!(paths[0].as_str(), Some("a\\b"));
    }

    #[test]
    fn a_response_reports_its_length_in_bytes_rather_than_characters() {
        // A count of characters would announce a short body, and the client
        // would truncate the last row of any result containing non-ASCII.
        let body = r#"{"ok":true,"kind":"ack","message":"café ☃"}"#.to_string();
        let response = Response::json(body.clone());
        let raw = rendered(&response);
        let text = String::from_utf8(raw.clone()).unwrap();
        let in_bytes = format!("Content-Length: {}\r\n", body.len());
        let in_chars = format!("Content-Length: {}\r\n", body.chars().count());
        assert!(text.contains(&in_bytes), "{text}");
        assert!(!text.contains(&in_chars), "{text}");
        assert!(raw.ends_with(body.as_bytes()));
        assert_eq!(raw.len(), text.find("\r\n\r\n").unwrap() + 4 + body.len());
    }

    #[test]
    fn no_response_invites_another_origin_to_read_it() {
        let responses = [
            Response::new(200, "OK", CT_HTML, "<!doctype html>".to_string()),
            Response::json(health_json(&Db::in_memory())),
            Reject::Unauthorized.response(),
            Reject::Forbidden.response(),
        ];
        for response in &responses {
            let text = String::from_utf8(rendered(response)).unwrap();
            let lower = text.to_ascii_lowercase();
            // A CORS header is a written invitation for a page on another
            // origin to read a SQL console's replies.
            assert!(!lower.contains("access-control"), "{text}");
            assert!(!lower.contains("cross-origin"), "{text}");
            assert!(text.contains("X-Content-Type-Options: nosniff\r\n"), "{text}");
            assert!(text.contains("Cache-Control: no-store\r\n"), "{text}");
            assert!(text.contains("Connection: close\r\n"), "{text}");
            assert!(text.starts_with("HTTP/1.1 "), "{text}");
        }
    }

    #[test]
    fn the_listener_is_bound_where_the_lan_cannot_reach_it() {
        // A console that runs arbitrary SQL and answers on 0.0.0.0 is remote
        // code execution with a text box in front of it.
        //
        // This one test opens a real socket, because the address a listener is
        // bound to is not observable any other way. A sandbox that forbids even
        // a loopback bind is an environment this assertion cannot be made in,
        // not a regression in it: skip rather than fail the suite for that.
        let server = match Server::bind(0) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("skipping: this environment will not bind loopback ({e})");
                return;
            }
        };
        let addr = server.local_addr();
        assert!(addr.ip().is_loopback(), "bound {addr}");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_ne!(addr.port(), 0, "port 0 must be resolved to a real port");
        let url = server.url();
        let prefix = format!("http://127.0.0.1:{}/?t=", addr.port());
        assert!(url.starts_with(&prefix), "{url}");
        assert!(url.ends_with(server.token()));
        assert_eq!(server.token().len(), 32);
        assert!(server.token().bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn two_consoles_in_one_process_do_not_share_a_token() {
        // A fixed token would be a password published in the source, and a
        // token that is a pure function of the clock is one on a machine whose
        // clock a caller can read — and, when the clock is coarse, two consoles
        // started in the same tick would share it outright.
        let a = new_token().expect("this platform has /dev/urandom");
        let b = new_token().expect("this platform has /dev/urandom");
        assert_ne!(a, b);
        assert_ne!(a, "0".repeat(32));
        for token in [&a, &b] {
            assert_eq!(token.len(), 32, "128 bits, hex-encoded");
            assert!(token.bytes().all(|c| c.is_ascii_hexdigit()), "{token}");
        }
    }

    #[test]
    fn the_token_is_kernel_randomness_and_not_a_function_of_the_clock() {
        // A token derived from the wall clock and the pid is searchable by
        // anyone who can read `/proc`, and testable a request at a time by a
        // page that loads `/app.js?t=GUESS` and watches onload versus onerror.
        // So the token is kernel randomness, with no derivation to fall back
        // to — `a_console_without_kernel_randomness_does_not_start` pins the
        // refusal that replaced it.
        let token = token_from(urandom_bytes()).expect("this platform has /dev/urandom");
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|c| c.is_ascii_hexdigit()), "{token}");
        // Sixteen fresh bytes every time: a repeat is not a flake, it is the
        // whole guarantee failing.
        let again = token_from(urandom_bytes()).expect("this platform has /dev/urandom");
        assert_ne!(token, again);
        assert_eq!(hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
    }

    #[test]
    fn a_console_without_kernel_randomness_does_not_start() {
        // The refusal is the whole of the decision, and it is unreachable
        // through the real file: this machine has `/dev/urandom` and cannot be
        // talked out of it. Handed the error the read would have returned,
        // `token_from` must refuse rather than invent something — restoring
        // the old clock-and-pid fallback passes every other test in this file,
        // so this is the one that notices.
        let refused =
            token_from(Err(std::io::Error::new(ErrorKind::NotFound, "no such file or directory")));
        let e = refused.expect_err("a token that cannot be random must not be issued");
        // The operator has to be able to tell what failed from the message
        // alone: the console refusing to start is the visible failure the
        // whole trade is bought with, and it is worthless if it is unreadable.
        let msg = e.to_string();
        assert!(msg.contains("/dev/urandom"), "{msg}");

        // The same seam carries the success case, so a token is exactly the
        // bytes that were read and nothing derived from them.
        assert_eq!(token_from(Ok([0xab; 16])).expect("bytes in hand"), "ab".repeat(16));
    }

    #[test]
    fn a_client_that_dribbles_forever_is_cut_off_by_the_deadline() {
        // `SO_RCVTIMEO` bounds one `recv`, not the request, so a byte sent
        // every fourteen seconds against a fifteen-second timeout holds this
        // connection thread open for as long as the client likes — with no
        // token, no route and no request ever completed. Any web page can do
        // it. The deadline is absolute, so the dribble does not extend it.
        let mut wire = Dribble::new(Duration::from_millis(2));
        let deadline = Instant::now() + Duration::from_millis(40);
        assert_eq!(read_head(&mut wire, deadline).err(), Some(Reject::RequestTimeout));
        // Without the deadline this returns only when the header *count* runs
        // out, minutes later at a byte every fourteen seconds, and it returns
        // `BadRequest` when it does. The status is how the test tells them
        // apart, and 408 is also the right thing to tell the client.
        assert_eq!(Reject::RequestTimeout.status().0, 408);
    }

    #[test]
    fn a_read_whose_deadline_has_already_passed_is_not_attempted() {
        let gone = Instant::now() - Duration::from_secs(1);
        let head = "GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let mut io = Cursor::new(head.as_bytes());
        assert_eq!(read_head(&mut io, gone).err(), Some(Reject::RequestTimeout));
        // Not one byte of it was taken: the deadline is checked before the read.
        assert_eq!(io.position(), 0);
        let mut io = Cursor::new(b"hello".to_vec());
        assert_eq!(read_body(&mut io, 5, gone).err(), Some(Reject::RequestTimeout));
        assert_eq!(io.position(), 0);
        // A deadline that has arrived exactly is gone, not zero time left:
        // `set_read_timeout(Some(ZERO))` means "wait forever", not "do not".
        assert!(time_left(Instant::now() - Duration::from_nanos(1)).is_none());
        assert!(time_left(far()).is_some());
    }

    #[test]
    fn read_head_and_parse_head_measure_a_line_the_same_way() {
        // One counted the CRLF and the other did not, so a header line exactly
        // at the limit was refused by the reader and accepted by the parser,
        // and the comment claiming they enforced the same limit was false.
        let exact = format!("X-Pad: {}", "a".repeat(MAX_LINE - 7));
        assert_eq!(exact.len(), MAX_LINE);
        let request = format!("GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n{exact}\r\n\r\n");
        let h = read_head(&mut Cursor::new(request.as_bytes()), far());
        let h = h.expect("a line exactly at the limit is within the limit");
        assert_eq!(h.header("x-pad").map(|v| v.len()), Some(MAX_LINE - 7));
        assert!(parse_head(&request).is_ok(), "the parser must agree with the reader");
        // And one byte over is refused by both.
        let over = format!("X-Pad: {}", "a".repeat(MAX_LINE - 6));
        assert_eq!(over.len(), MAX_LINE + 1);
        let request = format!("GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n{over}\r\n\r\n");
        let refused = read_head(&mut Cursor::new(request.as_bytes()), far()).err();
        assert_eq!(refused, Some(Reject::BadRequest));
        assert_eq!(parse_head(&request).err(), Some(Reject::BadRequest));
    }

    #[test]
    fn a_header_value_that_is_not_utf8_does_not_take_the_whole_request_down() {
        // RFC 7230 §3.2.6: a field value is opaque octets. A `User-Agent` in
        // Latin-1 is a request to answer, not a console to make unusable.
        let mut request = Vec::from(&b"GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n"[..]);
        request.extend_from_slice(b"User-Agent: caf\xe9 browser\r\n\r\n");
        let h = read_head(&mut Cursor::new(request), far()).expect("Latin-1 is not a rejection");
        assert_eq!(h.header("host"), Some("localhost"));
        assert!(h.header("user-agent").is_some());
        assert!(dispatch(&h, "tok", PORT).is_ok(), "the request still routes");
        // The bytes this file makes decisions from are still ASCII-only: the
        // request line and every field name.
        let mut request = Vec::from(&b"GET /caf\xe9?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n"[..]);
        request.extend_from_slice(b"\r\n");
        let refused = read_head(&mut Cursor::new(request), far()).err();
        assert_eq!(refused, Some(Reject::BadRequest));
        let mut request = Vec::from(&b"GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n"[..]);
        request.extend_from_slice(b"X-\xe9: 1\r\n\r\n");
        let refused = read_head(&mut Cursor::new(request), far()).err();
        assert_eq!(refused, Some(Reject::BadRequest));
        // Control bytes in a value are framing, not text.
        let mut request = Vec::from(&b"GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n"[..]);
        request.extend_from_slice(b"X-Pad: a\x00b\r\n\r\n");
        let refused = read_head(&mut Cursor::new(request), far()).err();
        assert_eq!(refused, Some(Reject::BadRequest));
    }

    #[test]
    fn a_refused_request_has_its_body_drained_so_the_client_can_read_the_answer() {
        // A socket closed with data still in its receive queue is closed with
        // an RST, and the client may throw away the response it was given. That
        // is why pasting a large statement into the console reported "network
        // error" rather than the 413 this server actually sent.
        let sql = r#"{"sql":"SELECT 1"}"#;
        let mut request = String::from("POST /api/query?t=nope HTTP/1.1\r\nHost: localhost\r\n");
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n\r\n{sql}", sql.len()));
        let db = Db::in_memory();
        let mut io = Cursor::new(request.as_bytes());
        let shared = RwLock::new(db);
        let served = answer(&mut io, "tok", PORT, Reach::Loopback, &shared, wide());
        assert_eq!(served.response.status, 401);
        assert_eq!(io.position() as usize, request.len(), "the refused body must be drained");

        // Bounded in bytes, so a body that keeps coming does not get read for
        // as long as it keeps coming.
        let mut io = Cursor::new(vec![b'x'; 8 * 1024]);
        drain(&mut io, u64::MAX);
        assert_eq!(io.position(), 8 * 1024, "everything sent is swallowed");
        let mut io = Cursor::new(vec![b'x'; 64]);
        drain(&mut io, 8);
        assert_eq!(io.position(), 8, "and no more than was declared");
        // Nothing declared is nothing read: a rejected GET costs no time.
        let mut io = Cursor::new(vec![b'x'; 64]);
        drain(&mut io, 0);
        assert_eq!(io.position(), 0);
        assert!(!DRAIN_DEADLINE.is_zero(), "the drain has to be bounded in time too");
    }

    #[test]
    fn a_failed_accept_never_retries_at_once_on_an_error_that_will_repeat() {
        // Under EMFILE or ENFILE the pending connection stays queued and the
        // next `accept` fails identically, so retrying immediately spins a core
        // at 100% for as long as the descriptor table stays full.
        for kind in [ErrorKind::PermissionDenied, ErrorKind::Other, ErrorKind::OutOfMemory] {
            assert_eq!(accept_backoff(kind, 1), Backoff::After(ACCEPT_BACKOFF), "{kind:?}");
        }
        assert!(!ACCEPT_BACKOFF.is_zero(), "a backoff of zero is not a backoff");
        // A run of those that never clears ends the loop instead of burning the
        // machine until somebody notices.
        assert_eq!(accept_backoff(ErrorKind::Other, MAX_ACCEPT_FAILURES), Backoff::GiveUp);
        // What is not our failure retries at once, and never gives up: a flood
        // of half-open connections must not be able to shut the console down.
        for kind in [ErrorKind::WouldBlock, ErrorKind::Interrupted, ErrorKind::ConnectionAborted] {
            assert_eq!(accept_backoff(kind, 1), Backoff::Now, "{kind:?}");
            assert_eq!(accept_backoff(kind, MAX_ACCEPT_FAILURES * 100), Backoff::Now, "{kind:?}");
        }
    }

    #[test]
    fn a_cross_site_post_is_refused_even_when_it_carries_the_right_token() {
        // `POST /api/query` with `Content-Type: text/plain` is a CORS *simple*
        // request: no preflight, and a page on any origin can send it from a
        // form without ever reading the reply — by which time the statement has
        // run. Host and token alone do not stop that.
        let post = |extra: &str| {
            let mut text = String::from("POST /api/query HTTP/1.1\r\nHost: localhost\r\n");
            text.push_str("X-Celastro-Token: tok\r\nContent-Length: 0\r\n");
            text.push_str(extra);
            text.push_str("\r\n");
            head(&text)
        };
        let json = "Content-Type: application/json\r\n";
        let ours = format!("Origin: http://127.0.0.1:{}\r\n", PORT);
        // What the console's own page sends.
        let mine = format!("{ours}Sec-Fetch-Site: same-origin\r\n{json}");
        assert!(matches!(dispatch(&post(&mine), "tok", PORT), Ok(Action::Query)));
        let local = format!("Origin: http://localhost:{}\r\n{json}", PORT);
        assert!(matches!(dispatch(&post(&local), "tok", PORT), Ok(Action::Query)));
        // What a page somewhere else sends.
        for origin in [
            "http://evil.example".to_string(),
            "https://evil.example".to_string(),
            "null".to_string(),
            format!("http://127.0.0.1:{}", PORT + 1),
            // The scheme is part of the origin: an https page on this host is
            // not this console.
            format!("https://127.0.0.1:{}", PORT),
        ] {
            let text = format!("Origin: {origin}\r\n{json}");
            let refused = dispatch(&post(&text), "tok", PORT).err();
            assert_eq!(refused, Some(Reject::Forbidden), "{origin} must not be trusted");
        }
        for site in ["cross-site", "same-site", "none"] {
            let text = format!("Sec-Fetch-Site: {site}\r\n{json}");
            let refused = dispatch(&post(&text), "tok", PORT).err();
            assert_eq!(refused, Some(Reject::Forbidden), "{site} is not this page");
        }
        // The content type a cross-origin form cannot set is required, which is
        // what forces the preflight the attacker's browser will not send.
        for ct in [
            "",
            "Content-Type: text/plain\r\n",
            "Content-Type: multipart/form-data\r\n",
            "Content-Type: application/x-www-form-urlencoded\r\n",
        ] {
            let refused = dispatch(&post(ct), "tok", PORT).err();
            assert_eq!(refused, Some(Reject::UnsupportedMediaType), "{ct:?} must be refused");
        }
        assert_eq!(Reject::UnsupportedMediaType.status().0, 415);
        // Parameters on the media type are fine; the type itself is not
        // case-sensitive.
        for ct in ["application/json; charset=utf-8", "APPLICATION/JSON", " application/json "] {
            assert!(is_json_media_type(ct), "{ct:?}");
        }
        for ct in ["text/plain", "application/jsonx", "", "application/x-www-form-urlencoded"] {
            assert!(!is_json_media_type(ct), "{ct:?}");
        }
        assert!(origin_is_ours(&format!("http://127.0.0.1:{}", PORT), PORT));
        assert!(origin_is_ours(&format!("HTTP://LOCALHOST:{}", PORT), PORT));
        assert!(!origin_is_ours("http://127.0.0.1", PORT));
        assert!(!origin_is_ours(&format!("http://127.0.0.1:{}.evil.example", PORT), PORT));
        // A GET navigation is not a state change and must still work: a user
        // typing this console's URL sends `Sec-Fetch-Site: none`.
        let mut text = String::from("GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n");
        text.push_str("Sec-Fetch-Site: none\r\nSec-Fetch-Mode: navigate\r\n\r\n");
        assert!(dispatch(&head(&text), "tok", PORT).is_ok(), "the page must still open");
    }

    #[test]
    fn a_shutdown_request_is_acknowledged_and_then_ends_the_loop() {
        // `run` looping forever is what made the console lose writes: nothing
        // after it could run, so nothing could close the database.
        let mut db = Db::in_memory();
        let mut request = String::from("POST /api/shutdown?t=tok HTTP/1.1\r\nHost: localhost\r\n");
        request.push_str(&format!("Origin: http://127.0.0.1:{}\r\n", PORT));
        request.push_str("Sec-Fetch-Site: same-origin\r\nContent-Length: 0\r\n\r\n");
        let (response, next) = serve_request(&mut db, &request);
        assert_eq!(status_line(&response), "HTTP/1.1 200 OK");
        assert_eq!(next, Next::Stop, "the caller has to get its turn to persist");
        let parsed = json::parse(body_of(&response)).expect("the reply must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(parsed.get("kind").and_then(|v| v.as_str()), Some("ack"));
        assert_eq!(parsed.get("message").and_then(|v| v.as_str()), Some("shutting down"));
        // Every other request leaves the console serving.
        let health = "GET /api/health?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(serve_request(&mut db, health).1, Next::Serve);
        let page = "GET /?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert_eq!(serve_request(&mut db, page).1, Next::Serve);
        // And it is a route like any other: no token, no shutdown.
        let mut anonymous = String::from("POST /api/shutdown HTTP/1.1\r\nHost: localhost\r\n");
        anonymous.push_str("Content-Length: 0\r\n\r\n");
        let (response, next) = serve_request(&mut db, &anonymous);
        assert_eq!(status_line(&response), "HTTP/1.1 401 Unauthorized");
        assert_eq!(next, Next::Serve, "an anonymous request must not stop the console");
    }

    #[test]
    fn a_statement_that_changed_something_is_on_disk_before_it_is_acknowledged() {
        // Otherwise `celastro-cli --dir ./data serve` takes writes through the
        // console all afternoon and loses them to the Ctrl-C that stops it.
        let tag = format!("celastro-serve-durable-{}", std::process::id());
        let dir = std::env::temp_dir().join(tag);
        let _ = std::fs::remove_dir_all(&dir);
        let db = RwLock::new(
            Db::open(&dir, crate::engine::DbOpts::default()).expect("a temp dir opens"),
        );
        let response = run_sql(&db, "CREATE COLLECTION items (id TEXT PRIMARY KEY)");
        let parsed = json::parse(&response.body).expect("the reply must be JSON");
        assert_eq!(parsed.get("ok"), Some(&Value::Bool(true)), "{}", response.body);
        // The manifest is written by `persist` and by nothing on the CREATE
        // path, so its existence is the persist having happened.
        let manifest = dir.join("collections").join("items").join("shard-0000").join("MANIFEST");
        assert!(manifest.exists(), "an acknowledged write must be on disk: {manifest:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_head_request_gets_the_headers_alone_and_a_405_says_what_it_allows() {
        // A 405 with a body desynchronises a client that is counting bytes off
        // the connection, and a 405 that will not say what it allows makes the
        // client guess. `allowed_methods` knows, so it costs nothing to say.
        let response = answer_to("HEAD /?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert_eq!(status_line(&response), "HTTP/1.1 405 Method Not Allowed");
        assert!(response.contains("Allow: GET\r\n"), "{response}");
        assert_eq!(body_of(&response), "", "a HEAD response carries no body");
        let declared = response
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .expect("a HEAD response still announces the length a GET would send");
        assert!(declared > 0, "{response}");
        let response = answer_to("HEAD /api/query?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(response.contains("Allow: POST\r\n"), "{response}");
        assert_eq!(body_of(&response), "");
        // A GET is unaffected: it still gets the body it asked for.
        let response = answer_to("GET /api/health?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(!body_of(&response).is_empty(), "{response}");
        assert!(!response.contains("Allow:"), "only a 405 owes an Allow: {response}");
        assert_eq!(allowed_methods("/api/catalog"), Some("GET"));
        assert_eq!(allowed_methods("/api/shutdown"), Some("POST"));
        assert_eq!(allowed_methods("/etc/passwd"), None);
    }

    #[test]
    fn every_response_says_when_it_was_made() {
        // A client cannot compute an age or an elapsed time from a response
        // that will not say when it was made, and `Date` is not optional.
        let response = answer_to("GET /api/health?t=tok HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(response.contains("\r\nDate: "), "{response}");
        let refused = String::from_utf8(rendered(&Reject::Unauthorized.response())).unwrap();
        assert!(refused.contains("\r\nDate: "), "{refused}");
        // IMF-fixdate, GMT, C locale: a protocol constant, not a locale.
        assert_eq!(http_date(UNIX_EPOCH), "Thu, 01 Jan 1970 00:00:00 GMT");
        let day_two = UNIX_EPOCH + Duration::from_secs(86_400);
        assert_eq!(http_date(day_two), "Fri, 02 Jan 1970 00:00:00 GMT");
        let then = UNIX_EPOCH + Duration::from_secs(1_614_861_296);
        assert_eq!(http_date(then), "Thu, 04 Mar 2021 12:34:56 GMT");
        let later = UNIX_EPOCH + Duration::from_secs(1_789_084_799);
        assert_eq!(http_date(later), "Thu, 10 Sep 2026 23:59:59 GMT");
    }

    #[test]
    fn a_response_is_written_head_then_body_rather_than_copied_into_one_buffer() {
        // Rendering into a second buffer first makes peak memory twice the size
        // of the response, and a result set is the biggest thing here.
        let body = r#"{"ok":true,"kind":"ack","message":"done"}"#.to_string();
        let response = Response::json(body.clone());
        let raw = rendered(&response);
        let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a head and a body");
        assert_eq!(&raw[split + 4..], body.as_bytes());
        assert_eq!(response.head_text().len(), split + 4);
        // The head alone is what a HEAD gets.
        let raw = rendered(&response.without_body());
        assert_eq!(raw.len(), split + 4);
        let text = String::from_utf8(raw).unwrap();
        assert!(text.contains(&format!("Content-Length: {}", body.len())), "{text}");
    }

    /// A collection whose live `a*` vocabulary is wider than the expansion
    /// cap, with `zed` on every document so a negated shape has a positive
    /// clause that admits everything and therefore measures only the
    /// exclusion. The shape `engine`'s `cap_fixture` builds, in memory: this
    /// test is about the wire, and a directory on disk would add failure modes
    /// that have nothing to do with it.
    fn cut_prefix_db() -> Db {
        let mut db = Db::in_memory();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..PREFIX_EXPANSION_LIMIT + 200 {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".to_string(), Value::Str(format!("n{i:05}"))),
                    ("body".to_string(), Value::Str(format!("zed a{i:05}"))),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        db
    }

    /// How many documents the collection still holds. Every document carries
    /// `zed`, and `zed` is one term, so this count is never itself cut.
    fn live_documents(db: &mut Db) -> usize {
        db.query("SELECT id FROM notes WHERE text_match(body, 'zed') LIMIT 100000")
            .unwrap()
            .rows
            .len()
    }

    /// One statement, posted the way the console posts it.
    fn post(sql: &str) -> String {
        let body = format!(r#"{{"sql":{}}}"#, jstr(sql));
        let mut request = String::from("POST /api/query?t=tok HTTP/1.1\r\nHost: localhost\r\n");
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
        request
    }

    #[test]
    fn a_delete_whose_predicate_was_cut_is_refused_over_http_and_deletes_nothing() {
        // That the engine refuses a cut DELETE is `engine`'s claim and
        // `engine`'s test. What was untested is the SHAPE that refusal takes
        // on the wire, which is all the console and any script posting to
        // `/api/query` can see: a 200 carrying `ok:false` and the reason, like
        // every other statement the database declines — not a protocol
        // failure, and above all not an ack for work that did not happen. The
        // generic error-shape test posts nonsense SQL, which the parser
        // rejects before a collection is even named; this one is refused by
        // the executor, with a real collection and real rows to lose.
        let mut db = cut_prefix_db();
        let before = live_documents(&mut db);
        assert_eq!(before, PREFIX_EXPANSION_LIMIT + 200, "the fixture, before anything");

        // Both shapes, because they are cut for opposite reasons: `a*` names
        // fewer documents than it describes, `zed -a*` excludes fewer, and the
        // refusal quotes the leaf as it was written so a reader can tell which.
        for (statement, leaf) in [
            ("DELETE FROM notes WHERE text_match(body, 'a*')", "'a*'"),
            ("DELETE FROM notes WHERE text_match(body, 'zed -a*')", "'-a*'"),
        ] {
            let response = serve_request(&mut db, &post(statement)).0;
            assert_eq!(status_line(&response), "HTTP/1.1 200 OK", "{response}");
            let parsed = json::parse(body_of(&response)).expect("a refusal is still JSON");
            assert_eq!(parsed.get("ok"), Some(&Value::Bool(false)), "{response}");
            // `kind` is what the console switches on, and `ack` here would
            // print a cheerful count for a DELETE that did nothing.
            assert_eq!(parsed.get("kind"), None, "a refusal is not an outcome: {response}");
            let error = match parsed.get("error").and_then(|v| v.as_str()) {
                Some(e) => e,
                None => panic!("a refusal has to say why: {response}"),
            };
            assert!(error.contains("refused"), "{error}");
            assert!(error.contains("NOTHING was deleted"), "{error}");
            assert!(error.contains(leaf), "the leaf that was cut, as written: {error}");
            assert_eq!(live_documents(&mut db), before, "refused means nothing was written");
        }
    }
}
