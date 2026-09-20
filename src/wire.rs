//! The wire between nodes.
//!
//! A cluster is a set of nodes, each with its own directory, its own
//! console, and one advertised address (`DbOpts::node`, `--node`). A
//! collection's shards are spread over nodes — each shard on exactly one —
//! and every node holding a shard carries the same definition and the same
//! placement map, so any of them can coordinate a statement: it answers from
//! its own shards by direct call and reaches the others through this module.
//!
//! What crosses it is exactly [`crate::plan::service::ShardService`], plus
//! the writes and the DDL a coordinator forwards: a document to insert or a
//! key to delete goes to the node whose shard owns the key, and a DDL
//! statement that changed this node's catalog is re-run on every other
//! holder with a `LOCAL` prefix so it does not fan out again.
//!
//! **Framing.** A frame is a little-endian `u32` length and that many bytes.
//! A request carries the wire version, the shared token, the call, the
//! collection, the shard index, the remaining statement deadline in
//! milliseconds, and the call's body; a response carries a status byte and
//! either the body or an error's kind and message. Everything inside is the
//! crate's own codec: varints, length-prefixed strings, and documents as
//! variant bytes. Both ends run the same crate version; a request with
//! another wire version is refused naming both.
//!
//! **The token.** Every request carries `CELASTRO_WIRE_TOKEN`, read from the
//! environment on both sides and compared in constant time. The wire is
//! plain TCP without TLS, like the archive tier's client: it is for a
//! network you trust, and the token keeps a stray connection from running
//! statements.
//!
//! **The deadline.** A request carries what is left of the coordinator's
//! statement deadline — every statement has one unless `WITH (no_deadline)`
//! lifted it, writes and DDL included, so no call across the wire waits
//! forever by accident; the holder arms it for the call, and the coordinator
//! stops waiting when it runs out. A holder that does not answer in time is
//! therefore [`Error::Deadline`] at the coordinator — the same thing a slow
//! shard in this process produces — and the `partial_results` rule applies
//! unchanged. That is the property the simulator pinned before this module
//! existed, and it is why the simulator's tests are the tests a transport has
//! to pass.

use crate::lock::RwLock;
use std::collections::BTreeMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::catalog::{Catalog, Collection, Tablet, CATALOG_VERSION};
use crate::codec::*;
use crate::engine::Db;
use crate::error::{Error, Result};
use crate::plan::exec;
use crate::plan::explain::{self, ShardExplain};
use crate::plan::fusion::Candidate;
use crate::plan::service::{
    CandidatesRequest, Local, ScanHit, ScanRequest, ShardCandidates, ShardScan, ShardService,
    TermStats,
};
use crate::plan::walk::{self, ExpandRequest, HopExpansion};
use crate::sql::ast::{Expr, Select, Statement};
use crate::text::scorer::{Expansion, GlobalStats, PrefixUse};
use crate::time::Timestamp;
use crate::tls::{self, Stream, Tls};
use crate::value::Value;

/// Refused on mismatch, in both directions.
pub const WIRE_VERSION: u8 = 4;
/// The newest frame this node accepts and sends: version 5 carries the
/// caller's identity -- its address and epoch -- after the token, so a
/// holder refuses a call from a process older than the newest it has seen
/// at that address (the zombie fence's server half). A node sends 5 only
/// to a peer whose hello said it accepts 5, and accepts 4 from anyone, so
/// a rolling upgrade across the bump still talks in both directions.
pub const WIRE_VERSION_MAX: u8 = 6;
/// The environment variable both ends read the token from.
pub const TOKEN_ENV: &str = "CELASTRO_WIRE_TOKEN";
/// The port a node serves its shards on when none is given: `tcp://host`
/// means `tcp://host:2352`, and `--shard-bind ADDR` means `ADDR:2352`.
/// Until 0.35.0 every example said 9000 and no default existed.
pub const DEFAULT_WIRE_PORT: u16 = 2352;
const MAX_FRAME: u32 = 256 << 20;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// `d`, or what is left of the statement's deadline if that is less.
fn within_deadline(d: Duration) -> Duration {
    match crate::deadline::remaining_ms() {
        Some(ms) => d.min(Duration::from_millis(ms.max(1))),
        None => d,
    }
}
/// How long a dial that fails outright -- a name that does not resolve, a
/// port that refuses -- is retried with backoff before the node is given up
/// on for this call, within what is left of the statement's deadline. Two
/// seconds covers a pod that is restarting; it does not cover a name the
/// cluster's DNS has cached as absent (thirty seconds on kube-dns), and is
/// not meant to: a dead node has to fail statements, not stall them.
const DIAL_RETRY: Duration = Duration::from_secs(2);
/// A pooled connection idle longer than this is asked a hello before it
/// carries a call, within `REVALIDATE_TIMEOUT`.
const POOL_REVALIDATE: Duration = Duration::from_secs(10);
const REVALIDATE_TIMEOUT: Duration = Duration::from_secs(2);
/// Connections a node keeps to one peer.
const POOL_SLOTS: usize = 8;

/// One pooled connection: the stream when dialled, and when it last
/// carried a call -- one idle longer than `POOL_REVALIDATE` is asked a
/// hello before the next call, so a peer that restarted meanwhile (the
/// old socket half-open, a write into it succeeding and a read waiting
/// out the deadline) costs a short hello and a redial.
struct Slot {
    stream: Option<Box<dyn Stream>>,
    last_used: Instant,
}
/// How long the listener is waited on before `stop` and the shutdown flag
/// are read again; a connection ends the wait at once (`signal::wait_readable`).
const ACCEPT_WAIT: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Call {
    Hello = 1,
    Counters = 2,
    TermStats = 3,
    PrefixTerms = 4,
    Candidates = 5,
    Scan = 6,
    Documents = 7,
    Get = 8,
    Insert = 9,
    Delete = 10,
    Statement = 11,
    CreateCollection = 12,
    Expand = 13,
    Present = 14,
    BeginMove = 15,
    ReadFile = 16,
    PullShard = 17,
    AbortMove = 18,
    /// The target holds every file of a pinned shard: the source answers
    /// no read of it from here on, so a write landing on the target is
    /// missing from no answer.
    FenceMove = 20,
    /// A batch of a held shard's log for a follower, with the shipper's
    /// term; the follower answers where it stands.
    Ship = 21,
    /// Where a follower stands: whether its copy is whole, and the instant.
    ShipStatus = 22,
    /// The steward's lease renewal, its own address in the body.
    Lease = 23,
    /// The node's catalog as it persists it: what a coordinator pulls at
    /// `ATTACH` so it plans over collections made before it was there.
    Catalog = 19,
}

impl Call {
    fn from_u8(b: u8) -> Option<Call> {
        Some(match b {
            1 => Call::Hello,
            2 => Call::Counters,
            3 => Call::TermStats,
            4 => Call::PrefixTerms,
            5 => Call::Candidates,
            6 => Call::Scan,
            7 => Call::Documents,
            8 => Call::Get,
            9 => Call::Insert,
            10 => Call::Delete,
            11 => Call::Statement,
            12 => Call::CreateCollection,
            13 => Call::Expand,
            14 => Call::Present,
            15 => Call::BeginMove,
            16 => Call::ReadFile,
            17 => Call::PullShard,
            18 => Call::AbortMove,
            20 => Call::FenceMove,
            21 => Call::Ship,
            22 => Call::ShipStatus,
            23 => Call::Lease,
            19 => Call::Catalog,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Call::Hello => "hello",
            Call::Counters => "counters",
            Call::TermStats => "term_stats",
            Call::PrefixTerms => "prefix_terms",
            Call::Candidates => "candidates",
            Call::Scan => "scan",
            Call::Documents => "documents",
            Call::Get => "get",
            Call::Insert => "insert",
            Call::Delete => "delete",
            Call::Statement => "statement",
            Call::CreateCollection => "create_collection",
            Call::Expand => "expand",
            Call::Present => "present",
            Call::BeginMove => "begin_move",
            Call::ReadFile => "read_file",
            Call::PullShard => "pull_shard",
            Call::AbortMove => "abort_move",
            Call::FenceMove => "fence_move",
            Call::Ship => "ship",
            Call::ShipStatus => "ship_status",
            Call::Lease => "lease",
            Call::Catalog => "catalog",
        }
    }
}

/// `tcp://host:port` to `host:port`, refusing anything else: the scheme is
/// written down so that a URL for the console (`http://`) cannot be handed
/// to the wire by mistake.
pub fn parse_url(url: &str) -> Result<String> {
    let Some(rest) = url.strip_prefix("tcp://") else {
        return Err(Error::Plan(format!("a node address is `tcp://host[:port]`, not `{url}`")));
    };
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
            Ok(rest.to_string())
        }
        Some(_) => Err(Error::Plan(format!("a node address is `tcp://host[:port]`, not `{url}`"))),
        None if !rest.is_empty() => Ok(with_default_port(rest)),
        None => Err(Error::Plan(format!("a node address is `tcp://host[:port]`, not `{url}`"))),
    }
}

/// `host:port` as given, or `host:2352` for a bare host.
pub fn with_default_port(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((_, port)) if port.parse::<u16>().is_ok() => addr.to_string(),
        _ => format!("{addr}:{DEFAULT_WIRE_PORT}"),
    }
}

/// The token this process presents and expects, from the environment.
pub fn token_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV).ok().filter(|t| !t.is_empty())
}

/// A second token the wire accepts from a peer, `CELASTRO_WIRE_TOKEN_ALSO`:
/// what makes a rotation roll. A rotation by rolling update alone
/// deadlocks -- the first pod on the new token can attach nobody, is never
/// ready, and the rollout never moves -- so it goes in three rollouts:
/// every pod accepts the new token too, then every pod sends the new one
/// and still accepts the old, then the old is dropped.
pub const TOKEN_ALSO_ENV: &str = "CELASTRO_WIRE_TOKEN_ALSO";

fn token_also_from_env() -> Option<String> {
    std::env::var(TOKEN_ALSO_ENV).ok().filter(|t| !t.is_empty())
}

fn truncated() -> Error {
    Error::Storage("wire: truncated frame".into())
}

// ------------------------------------------------------------------ frames

fn write_frame(w: &mut impl Write, payload: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    put_u32(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    w.write_all(&buf)?;
    w.flush()
}

fn read_frame(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_le_bytes(len);
    if n > MAX_FRAME {
        return Err(std::io::Error::new(ErrorKind::InvalidData, "wire: frame too large"));
    }
    let mut buf = vec![0u8; n as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

// ------------------------------------------------------------------ codecs

fn put_bool(out: &mut Vec<u8>, b: bool) {
    out.push(b as u8);
}

fn get_bool(b: &[u8], i: &mut usize) -> Result<bool> {
    let v = *b.get(*i).ok_or_else(truncated)?;
    *i += 1;
    Ok(v != 0)
}

fn get_u8(b: &[u8], i: &mut usize) -> Result<u8> {
    let v = *b.get(*i).ok_or_else(truncated)?;
    *i += 1;
    Ok(v)
}

/// A count of items that follow: refused when larger than the bytes left,
/// so a hostile count never sizes an allocation.
fn get_count(b: &[u8], i: &mut usize) -> Result<usize> {
    crate::codec::get_count(b, i).ok_or_else(truncated)
}

/// A number that is not a count of items -- an index, a cap, a depth --
/// and so is not bounded by what follows it.
fn get_num(b: &[u8], i: &mut usize) -> Result<usize> {
    Ok(get_uvarint(b, i).ok_or_else(truncated)? as usize)
}

fn get_string(b: &[u8], i: &mut usize) -> Result<String> {
    get_str(b, i).ok_or_else(truncated)
}

fn get_opt(b: &[u8], i: &mut usize) -> Result<Option<String>> {
    get_opt_str(b, i).ok_or_else(truncated)
}

fn put_strs(out: &mut Vec<u8>, v: &[String]) {
    put_uvarint(out, v.len() as u64);
    for s in v {
        put_str(out, s);
    }
}

fn get_strs(b: &[u8], i: &mut usize) -> Result<Vec<String>> {
    let n = get_count(b, i)?;
    (0..n).map(|_| get_string(b, i)).collect()
}

fn put_frontiers(out: &mut Vec<u8>, v: &[Vec<String>]) {
    put_uvarint(out, v.len() as u64);
    for f in v {
        put_strs(out, f);
    }
}

fn get_frontiers(b: &[u8], i: &mut usize) -> Result<Vec<Vec<String>>> {
    let n = get_count(b, i)?;
    (0..n).map(|_| get_strs(b, i)).collect()
}

fn put_pairs(out: &mut Vec<u8>, v: &[(String, String)]) {
    put_uvarint(out, v.len() as u64);
    for (a, b) in v {
        put_str(out, a);
        put_str(out, b);
    }
}

fn get_pairs(b: &[u8], i: &mut usize) -> Result<Vec<(String, String)>> {
    let n = get_count(b, i)?;
    (0..n).map(|_| Ok((get_string(b, i)?, get_string(b, i)?))).collect()
}

fn put_value(out: &mut Vec<u8>, v: &Value) {
    put_bytes(out, &crate::variant::encode_to_vec(v));
}

fn get_value(b: &[u8], i: &mut usize) -> Result<Value> {
    let bytes = get_bytes(b, i).ok_or_else(truncated)?;
    crate::variant::decode(bytes, &mut 0)
}

fn put_values(out: &mut Vec<u8>, v: &[Value]) {
    put_uvarint(out, v.len() as u64);
    for x in v {
        put_value(out, x);
    }
}

fn get_values(b: &[u8], i: &mut usize) -> Result<Vec<Value>> {
    let n = get_count(b, i)?;
    (0..n).map(|_| get_value(b, i)).collect()
}

fn put_ts(out: &mut Vec<u8>, ts: Timestamp) {
    put_u64(out, ts);
}

fn get_ts(b: &[u8], i: &mut usize) -> Result<Timestamp> {
    get_u64(b, i).ok_or_else(truncated)
}

fn put_stats(out: &mut Vec<u8>, stats: &BTreeMap<String, GlobalStats>) {
    put_uvarint(out, stats.len() as u64);
    for (path, g) in stats {
        put_str(out, path);
        put_u64(out, g.num_docs);
        put_u64(out, g.avg_doc_len.to_bits());
        put_uvarint(out, g.doc_freq.len() as u64);
        for (t, c) in &g.doc_freq {
            put_str(out, t);
            put_uvarint(out, *c);
        }
        put_uvarint(out, g.expansions.len() as u64);
        for (leaf, e) in &g.expansions {
            put_str(out, leaf);
            put_strs(out, &e.terms);
            put_bool(out, e.truncated);
            put_bool(out, e.used.positive);
            put_bool(out, e.used.negated);
        }
        put_uvarint(out, g.prefix_cap as u64);
        put_bool(out, g.exact);
    }
}

fn get_stats(b: &[u8], i: &mut usize) -> Result<BTreeMap<String, GlobalStats>> {
    let n = get_count(b, i)?;
    let mut out = BTreeMap::new();
    for _ in 0..n {
        let path = get_string(b, i)?;
        let mut g = GlobalStats::default();
        g.num_docs = get_u64(b, i).ok_or_else(truncated)?;
        g.avg_doc_len = f64::from_bits(get_u64(b, i).ok_or_else(truncated)?);
        let nd = get_count(b, i)?;
        for _ in 0..nd {
            let t = get_string(b, i)?;
            let c = get_uvarint(b, i).ok_or_else(truncated)?;
            g.doc_freq.insert(t, c);
        }
        let ne = get_count(b, i)?;
        for _ in 0..ne {
            let leaf = get_string(b, i)?;
            let mut e = Expansion::default();
            e.terms = get_strs(b, i)?;
            e.truncated = get_bool(b, i)?;
            let mut used = PrefixUse::default();
            used.positive = get_bool(b, i)?;
            used.negated = get_bool(b, i)?;
            e.used = used;
            g.expansions.insert(leaf, e);
        }
        // A cap, not a count of items: not bounded by the bytes left.
        g.prefix_cap = get_uvarint(b, i).ok_or_else(truncated)? as usize;
        g.exact = get_bool(b, i)?;
        out.insert(path, g);
    }
    Ok(out)
}

fn put_explain(out: &mut Vec<u8>, sx: &ShardExplain) {
    put_uvarint(out, sx.index as u64);
    put_u64(out, sx.manifest_version);
    put_u64(out, sx.micros as u64);
    put_bool(out, sx.timed_out);
    put_str(out, &explain::render_shard(sx));
}

fn get_explain(b: &[u8], i: &mut usize) -> Result<ShardExplain> {
    let mut sx = ShardExplain::default();
    sx.index = get_num(b, i)?;
    sx.manifest_version = get_u64(b, i).ok_or_else(truncated)?;
    sx.micros = get_u64(b, i).ok_or_else(truncated)? as u128;
    sx.timed_out = get_bool(b, i)?;
    sx.rendered = Some(get_string(b, i)?);
    Ok(sx)
}

fn put_candidates(out: &mut Vec<u8>, a: &ShardCandidates) {
    put_uvarint(out, a.per_source.len() as u64);
    for list in &a.per_source {
        put_uvarint(out, list.len() as u64);
        for c in list {
            put_str(out, &c.key);
            put_f32(out, c.raw_score);
        }
    }
    put_explain(out, &a.explain);
    put_bool(out, a.timed_out);
}

fn get_candidates(b: &[u8], i: &mut usize) -> Result<ShardCandidates> {
    let n = get_count(b, i)?;
    let mut per_source = Vec::with_capacity(n);
    for _ in 0..n {
        let m = get_count(b, i)?;
        let mut list = Vec::with_capacity(m);
        for _ in 0..m {
            let key = get_string(b, i)?;
            let raw_score = get_f32(b, i).ok_or_else(truncated)?;
            list.push(Candidate { key, raw_score });
        }
        per_source.push(list);
    }
    let explain = get_explain(b, i)?;
    let timed_out = get_bool(b, i)?;
    Ok(ShardCandidates { per_source, explain, timed_out })
}

fn put_scan(out: &mut Vec<u8>, a: &ShardScan) {
    put_uvarint(out, a.hits.len() as u64);
    for h in &a.hits {
        put_values(out, &h.sort);
        put_str(out, &h.key);
        match &h.doc {
            Some(d) => {
                put_bool(out, true);
                put_value(out, d);
            }
            None => put_bool(out, false),
        }
        put_uvarint(out, h.handle.0 as u64);
        put_u32(out, h.handle.1);
        match &h.parent {
            Some(p) => {
                put_bool(out, true);
                put_bytes(out, p);
            }
            None => put_bool(out, false),
        }
    }
    put_explain(out, &a.explain);
    put_bool(out, a.timed_out);
}

fn get_scan(b: &[u8], i: &mut usize) -> Result<ShardScan> {
    let n = get_count(b, i)?;
    let mut hits = Vec::with_capacity(n);
    for _ in 0..n {
        let sort = get_values(b, i)?;
        let key = get_string(b, i)?;
        let doc = if get_bool(b, i)? { Some(get_value(b, i)?) } else { None };
        let ui = get_num(b, i)?;
        let ord = get_u32(b, i).ok_or_else(truncated)?;
        let parent = if get_bool(b, i)? {
            Some(get_bytes(b, i).ok_or_else(truncated)?.to_vec())
        } else {
            None
        };
        hits.push(ScanHit { sort, key, doc, handle: (ui, ord), parent });
    }
    let explain = get_explain(b, i)?;
    let timed_out = get_bool(b, i)?;
    Ok(ShardScan { hits, explain, timed_out })
}

/// Tablets in a frame: the term and the followers ride along from wire
/// version 6; a version 5 peer reads and writes the older shape.
fn put_tablets(out: &mut Vec<u8>, tablets: &[Tablet], six: bool) {
    put_uvarint(out, tablets.len() as u64);
    for t in tablets {
        put_str(out, &t.node);
        put_opt_str(out, t.lo.as_deref());
        put_opt_str(out, t.hi.as_deref());
        if six {
            put_u64(out, t.term);
            put_uvarint(out, t.followers.len() as u64);
            for f in &t.followers {
                put_str(out, f);
            }
        }
    }
}

fn get_tablets(b: &[u8], i: &mut usize, six: bool) -> Result<Vec<Tablet>> {
    let n = get_count(b, i)?;
    (0..n)
        .map(|_| {
            let mut t = Tablet {
                node: get_string(b, i)?,
                lo: get_opt(b, i)?,
                hi: get_opt(b, i)?,
                ..Default::default()
            };
            if six {
                t.term = get_u64(b, i).ok_or_else(truncated)?;
                let nf = get_count(b, i)?;
                for _ in 0..nf {
                    t.followers.push(get_string(b, i)?);
                }
            }
            Ok(t)
        })
        .collect()
}

fn error_kind(e: &Error) -> u8 {
    match e {
        Error::Sql(_) => 1,
        Error::Plan(_) => 2,
        Error::Schema(_) => 3,
        Error::Storage(_) => 4,
        Error::Io(_) => 5,
        Error::SnapshotGone(_) => 6,
        Error::Deadline(_) => 7,
        Error::NotYet(_) => 8,
    }
}

fn error_message(e: &Error) -> String {
    match e {
        Error::Sql(m)
        | Error::Plan(m)
        | Error::Schema(m)
        | Error::Storage(m)
        | Error::SnapshotGone(m)
        | Error::Deadline(m) => m.clone(),
        Error::Io(e) => e.to_string(),
        Error::NotYet(m) => m.to_string(),
    }
}

fn error_from(kind: u8, m: String) -> Error {
    match kind {
        1 => Error::Sql(m),
        2 => Error::Plan(m),
        3 => Error::Schema(m),
        5 => Error::Io(std::io::Error::other(m)),
        6 => Error::SnapshotGone(m),
        7 => Error::Deadline(m),
        _ => Error::Storage(m),
    }
}

// ------------------------------------------------------------------ client

/// One other node, reached over one connection that is opened on demand and
/// dropped on any failure. Shared by every remote shard that node holds.
impl Drop for Node {
    fn drop(&mut self) {
        crate::cipher::wipe_string(&mut self.token);
    }
}

pub struct Node {
    url: String,
    addr: String,
    token: String,
    /// The pooled connections, one call at a time each: a call takes the
    /// first free slot, or waits for one within its deadline. One
    /// connection carried every call once, and under concurrent fan-out
    /// the calls queued on its lock for far longer than any of them took.
    slots: Vec<Mutex<Slot>>,
    /// What the connection is wrapped in and verified by, when the process
    /// has certificates; plain TCP otherwise.
    tls: Option<Arc<Tls>>,
    /// When a dial last gave up, so the next calls within `DIAL_RETRY` of
    /// it fail at once instead of each retrying for the whole window: a
    /// dead node costs a statement one window, not one per call it makes.
    dial_failed: Mutex<Option<Instant>>,

    /// The newest catalog format the peer said it reads, from its hello;
    /// zero until then.
    peer_format: std::sync::atomic::AtomicU8,
    /// The newest epoch a hello from this address has carried. A fresh
    /// connection that answers with an older one is a second process at
    /// the address -- a pod replaced while its predecessor still runs --
    /// and is refused before a statement reaches it.
    max_epoch: std::sync::atomic::AtomicU64,
    /// The epoch of the process behind the pooled connection, from the
    /// hello that checked it; zero when unknown. A connection to a process
    /// older than the newest seen since is dropped before the next call,
    /// which then dials afresh and is refused.
    conn_epoch: std::sync::atomic::AtomicU64,
    /// The newest wire version the peer's hello said it accepts; zero
    /// until a hello, which means 4.
    peer_wire: std::sync::atomic::AtomicU8,
    /// This node's address and epoch, sent in version-5 frames; the epoch
    /// a cell shared with the engine, so a test's pretended one shows.
    identity: Option<(String, Arc<std::sync::atomic::AtomicU64>)>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({})", self.url)
    }
}

/// What a node says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// The address the node was started with, its name in placement maps.
    pub node: Option<String>,
    pub version: String,
    /// What the node is for; a node from before roles says nothing and is
    /// a data node.
    pub role: crate::engine::Role,
    /// The node's clock as it answered, microseconds; zero from a node
    /// that predates the field. What `ATTACH` and `SHOW HEALTH` measure
    /// skew by.
    pub now_micros: u64,
    /// When the process behind the address started, microseconds; zero
    /// from a node that predates the field. A later hello with a smaller
    /// epoch is an older process answering at the same address.
    pub epoch: u64,
    /// The newest catalog format the node reads: what a catalog sent to it
    /// is encoded as. A node from before the field is placed by its version.
    pub catalog_format: u8,
    /// The newest wire version the node accepts; a node from before the
    /// field accepts 4.
    pub wire_max: u8,
    /// This node's wall clock as the answer was read, microseconds: what
    /// `now_micros` is compared against. Taken here and not by whoever
    /// looks at the hello later, since a lock waited for or a call made in
    /// between is not skew -- a sweep once accused a peer of eleven seconds
    /// that way.
    pub received_micros: u64,
}

impl Node {
    pub fn new(url: &str, token: Option<&str>, tls: Option<Arc<Tls>>) -> Result<Node> {
        let addr = parse_url(url)?;
        let token = token
            .ok_or_else(|| {
                Error::Plan(format!(
                    "{TOKEN_ENV} is not set; the wire to {url} needs the token every node shares"
                ))
            })?
            .to_string();
        Ok(Node {
            url: url.to_string(),
            addr,
            token,
            slots: (0..POOL_SLOTS)
                .map(|_| Mutex::new(Slot { stream: None, last_used: Instant::now() }))
                .collect(),
            tls,
            dial_failed: Mutex::new(None),

            peer_format: std::sync::atomic::AtomicU8::new(0),
            max_epoch: std::sync::atomic::AtomicU64::new(0),
            conn_epoch: std::sync::atomic::AtomicU64::new(0),
            peer_wire: std::sync::atomic::AtomicU8::new(0),
            identity: None,
        })
    }

    /// Who this node is, for the frames it sends: its address and epoch,
    /// carried from wire version 5 on so a holder can refuse a call from
    /// a process older than the newest seen at the address.
    pub fn with_identity(mut self, node: &str, epoch: Arc<std::sync::atomic::AtomicU64>) -> Node {
        self.identity = Some((node.to_string(), epoch));
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn connect(&self) -> std::io::Result<Box<dyn Stream>> {
        // Bounded by the statement's budget, the dial and the hello alike:
        // a holder behind a partition drops the packets, and a fixed five
        // seconds for each such holder is what spent a partial statement's
        // budget before the shards that could answer were asked.
        let mut last = None;
        for a in self.addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&a, within_deadline(CONNECT_TIMEOUT)) {
                Ok(s) => {
                    s.set_nodelay(true)?;
                    // Verified by the name the URL gave, which is the name
                    // in the peer's certificate: the pod's, in a cluster.
                    let host = self.addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(&self.addr);
                    return tls::connect(self.tls.as_ref(), s, host);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| std::io::Error::other("no address")))
    }

    /// One call: connect if needed, send, wait for the answer within what is
    /// left of the statement deadline. A failed connection is dropped, and a
    /// call that failed before anything was read is retried once over a
    /// fresh connection — every call here is a read or is idempotent on the
    /// holder, so a connection the holder closed while idle costs nothing.
    /// A request frame: the head every call carries, then the body.
    fn request(&self, call: Call, collection: &str, shard: usize, body: &[u8]) -> Vec<u8> {
        // Version 5 to a peer that accepts it, when this node has an
        // identity to carry; 4 otherwise, and to a peer not asked yet.
        let five = self.identity.is_some() && self.peer_wire.load(Ordering::Relaxed) >= 5;
        let mut req = vec![self.frame_version()];
        put_str(&mut req, &self.token);
        if five {
            let (node, epoch) = self.identity.as_ref().expect("checked");
            put_str(&mut req, node);
            put_u64(&mut req, epoch.load(Ordering::Relaxed));
        }
        req.push(call as u8);
        put_str(&mut req, collection);
        put_uvarint(&mut req, shard as u64);
        match crate::deadline::remaining_ms() {
            Some(ms) => {
                put_bool(&mut req, true);
                put_uvarint(&mut req, ms);
            }
            None => put_bool(&mut req, false),
        }
        req.extend_from_slice(body);
        req
    }

    /// The frame version a call to this peer carries: 4 until the peer's
    /// hello said more and this node has an identity to carry, then the
    /// highest both speak.
    fn frame_version(&self) -> u8 {
        let peer = self.peer_wire.load(Ordering::Relaxed);
        if self.identity.is_some() && peer >= 5 {
            peer.min(WIRE_VERSION_MAX)
        } else {
            WIRE_VERSION
        }
    }

    /// A fresh connection is asked who answers before a statement goes
    /// down it: a hello, whose epoch must not be older than the newest this
    /// node has seen at the address. One round trip per connection, and
    /// connections are pooled. What it refuses is the zombie: two processes
    /// at one address, the old one reached through a stale name, taking a
    /// write the new one never sees.
    fn check_fresh(&self, s: &mut Box<dyn Stream>) -> Result<()> {
        let req = self.request(Call::Hello, "", 0, &[]);
        s.set_read_timeout(Some(within_deadline(CONNECT_TIMEOUT)))?;
        write_frame(s, &req)?;
        let resp = read_frame(s)?;
        let h = self.decode_hello(&decode_response(resp)?)?;
        if h.epoch > 0 {
            let max = self.max_epoch.load(Ordering::Relaxed);
            if h.epoch < max {
                return Err(Error::Plan(format!(
                    "an older process answers at {}: it started at {} while one started at {} was \
                     seen there; two processes share the address, and this one is refused",
                    self.url,
                    crate::time::format_micros(h.epoch as i64),
                    crate::time::format_micros(max as i64)
                )));
            }
            self.max_epoch.fetch_max(h.epoch, Ordering::Relaxed);
            self.conn_epoch.store(h.epoch, Ordering::Relaxed);
        }
        Ok(())
    }

    fn call(&self, call: Call, collection: &str, shard: usize, body: &[u8]) -> Result<Vec<u8>> {
        let deadline_ms = crate::deadline::remaining_ms();
        let req = self.request(call, collection, shard, body);
        // A free slot, or the wait for one within the deadline.
        let started = Instant::now();
        let mut slot = loop {
            let free = self.slots.iter().find_map(|m| m.try_lock().ok());
            if let Some(g) = free {
                break g;
            }
            let waited = started.elapsed().as_millis() as u64;
            if deadline_ms.is_some_and(|ms| waited >= ms) {
                return Err(Error::Deadline(self.refusal(
                    call,
                    collection,
                    shard,
                    "(every connection to it busy for the whole statement deadline)",
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let guard = &mut slot.stream;
        // A pooled connection to a process an older hello has since shown to
        // be superseded is let go here; the redial below asks again.
        let conn = self.conn_epoch.load(Ordering::Relaxed);
        if guard.is_some() && conn > 0 && conn < self.max_epoch.load(Ordering::Relaxed) {
            *guard = None;
        }
        let idle = slot.last_used.elapsed();
        let guard = &mut slot.stream;
        if call != Call::Hello && idle > POOL_REVALIDATE {
            if let Some(s) = guard.as_mut() {
                let fresh = s
                    .set_read_timeout(Some(within_deadline(REVALIDATE_TIMEOUT)))
                    .and_then(|_| {
                        write_frame(s, &self.request(Call::Hello, "", 0, &[]))?;
                        read_frame(s)
                    })
                    .ok()
                    .and_then(|resp| decode_response(resp).ok())
                    .and_then(|b| self.decode_hello(&b).ok())
                    .is_some();
                if !fresh {
                    *guard = None;
                }
            }
        }
        let mut attempt = 0;
        loop {
            attempt += 1;
            if guard.is_none() {
                // A dial that fails is retried with backoff for `DIAL_RETRY`
                // (and never past the statement's deadline): a pod that is
                // coming back refuses for a moment and then answers.
                let dial_started = Instant::now();
                let recently_failed = self
                    .dial_failed
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_some_and(|t| t.elapsed() < DIAL_RETRY);
                let mut backoff = Duration::from_millis(100);
                loop {
                    match self.connect() {
                        Ok(mut s) => {
                            *self.dial_failed.lock().unwrap_or_else(|p| p.into_inner()) = None;
                            // A hello asks for itself; every other call
                            // asks who answers first.
                            if call != Call::Hello {
                                if let Err(e) = self.check_fresh(&mut s) {
                                    let why = format!("({e})");
                                    return Err(Error::Deadline(
                                        self.refusal(call, collection, shard, &why),
                                    ));
                                }
                            }
                            *guard = Some(s);
                            break;
                        }
                        Err(e) => {
                            let left = crate::deadline::remaining_ms()
                                .map(Duration::from_millis)
                                .unwrap_or(DIAL_RETRY);
                            let spent = dial_started.elapsed();
                            if recently_failed || spent + backoff >= DIAL_RETRY.min(left) {
                                *self.dial_failed.lock().unwrap_or_else(|p| p.into_inner()) =
                                    Some(Instant::now());
                                return Err(Error::Deadline(self.refusal(
                                    call,
                                    collection,
                                    shard,
                                    &format!("({e})"),
                                )));
                            }
                            std::thread::sleep(backoff);
                            backoff = (backoff * 2).min(Duration::from_millis(800));
                        }
                    }
                }
            }
            let s = guard.as_mut().expect("connected above");
            let timeout = deadline_ms.map(|ms| Duration::from_millis(ms.max(1)));
            let r = s
                .set_read_timeout(timeout)
                .and_then(|_| write_frame(s, &req))
                .and_then(|_| read_frame(s));
            match r {
                Ok(resp) => {
                    slot.last_used = Instant::now();
                    return decode_response(resp);
                }
                Err(e) => {
                    *guard = None;
                    let timed_out = matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut);
                    if timed_out || attempt >= 2 {
                        // A node that cannot be reached and a node that does
                        // not answer in time are one case at the coordinator:
                        // the statement's rule for a shard past its deadline
                        // applies, and `partial_results` names the shard.
                        let why = if timed_out {
                            "within the statement deadline".to_string()
                        } else {
                            format!("({e})")
                        };
                        return Err(Error::Deadline(self.refusal(call, collection, shard, &why)));
                    }
                }
            }
        }
    }

    /// The one message for a node that did not answer. A per-node call --
    /// `counters`, `hello`, a forwarded statement -- names the node and the
    /// call; a shard call names the shard too. Until 0.34.0 every refusal
    /// said "shard 0", the placeholder a per-node call is sent with.
    fn refusal(&self, call: Call, collection: &str, shard: usize, why: &str) -> String {
        let per_node =
            matches!(call, Call::Hello | Call::Counters | Call::Statement | Call::CreateCollection);
        let what = if per_node {
            format!("{} did not answer `{}`", self.url, call.name())
        } else {
            format!(
                "shard {shard} of `{collection}` on {} did not answer `{}`",
                self.url,
                call.name()
            )
        };
        format!("{what} {why}; use WITH (partial_results) to opt in to incomplete answers")
    }

    pub fn hello(&self) -> Result<Hello> {
        let b = self.call(Call::Hello, "", 0, &[])?;
        let h = self.decode_hello(&b)?;
        // Noted, not refused: `SHOW HEALTH` names an older process from
        // what a hello says, and a hello is how it looks. But the connection
        // it came over is to that process, and is let go if a newer one has
        // been seen: the next call dials afresh and is refused.
        if h.epoch > 0 {
            let max = self.max_epoch.fetch_max(h.epoch, Ordering::Relaxed).max(h.epoch);
            self.conn_epoch.store(h.epoch, Ordering::Relaxed);
            if h.epoch < max {
                for m in &self.slots {
                    if let Ok(mut g) = m.try_lock() {
                        g.stream = None;
                    }
                }
            }
        }
        Ok(h)
    }

    fn decode_hello(&self, b: &[u8]) -> Result<Hello> {
        let mut i = 0;
        let node = get_opt(b, &mut i)?;
        let version = get_string(b, &mut i)?;
        let role = if i < b.len() {
            crate::engine::Role::parse(&get_string(b, &mut i)?).unwrap_or(crate::engine::Role::Data)
        } else {
            crate::engine::Role::Data
        };
        let now_micros = if i < b.len() { get_u64(b, &mut i).ok_or_else(truncated)? } else { 0 };
        let epoch = if i < b.len() { get_u64(b, &mut i).ok_or_else(truncated)? } else { 0 };
        let catalog_format = if i < b.len() {
            get_u8(b, &mut i)?
        } else {
            // What each release before the field read; a move or a create
            // sent in a newer format is refused by the peer as unreadable,
            // which a mixed-version drill showed.
            match version.split('.').nth(1).and_then(|m| m.parse::<u32>().ok()) {
                Some(46) => 7,
                Some(45) => 6,
                _ => 5,
            }
        };
        self.peer_format.store(catalog_format, Ordering::Relaxed);
        let wire_max = if i < b.len() { get_u8(b, &mut i)? } else { WIRE_VERSION };
        self.peer_wire.store(wire_max, Ordering::Relaxed);
        let received_micros = crate::time::now_micros().max(0) as u64;
        Ok(Hello {
            node,
            version,
            role,
            now_micros,
            epoch,
            catalog_format,
            wire_max,
            received_micros,
        })
    }

    /// A catalog as the peer reads it: the newest format it said it reads,
    /// or this build's when it has not been asked yet.
    fn encode_for_peer(&self, cat: &Catalog) -> Vec<u8> {
        let mut theirs = self.peer_format.load(Ordering::Relaxed);
        if theirs == 0 {
            // Not asked yet: a hello says what the peer reads, and a catalog
            // in this build's format sent blind refused a move to an older
            // node that had not been spoken to before.
            if self.hello().is_ok() {
                theirs = self.peer_format.load(Ordering::Relaxed);
            }
        }
        let format = if theirs == 0 { CATALOG_VERSION } else { theirs.min(CATALOG_VERSION) };
        cat.encode_as(format)
    }

    /// The holder's clock and its write counter for a collection: what a
    /// coordinator needs to pin a snapshot and to age its statistics cache.
    pub fn counters(&self, collection: &str) -> Result<(Timestamp, u64)> {
        // The first call a statement makes to a holder, so the first to
        // meet a node back on an empty volume, which has no collection: a
        // missing shard to the statement, as `Remote::call` says.
        let b = match self.call(Call::Counters, collection, 0, &[]) {
            Err(Error::Plan(m)) if m.starts_with("no such collection") => {
                return Err(Error::Deadline(format!(
                    "{} holds no `{collection}`: a node back on an empty volume? (RESTORE ... \
                     NODE on it); use WITH (partial_results) to opt in to incomplete answers",
                    self.url
                )))
            }
            other => other?,
        };
        let mut i = 0;
        Ok((get_ts(&b, &mut i)?, get_u64(&b, &mut i).ok_or_else(truncated)?))
    }

    pub fn insert(&self, collection: &str, doc: &Value) -> Result<Timestamp> {
        let mut body = Vec::new();
        put_value(&mut body, doc);
        let b = self.call(Call::Insert, collection, 0, &body)?;
        get_ts(&b, &mut 0)
    }

    pub fn delete(&self, collection: &str, key: &str) -> Result<bool> {
        let mut body = Vec::new();
        put_str(&mut body, key);
        let b = self.call(Call::Delete, collection, 0, &body)?;
        get_bool(&b, &mut 0)
    }

    /// Run a statement on the node, which must carry its `LOCAL` prefix so
    /// that it does not fan out again from there.
    pub fn statement(&self, sql: &str, params: &[Value]) -> Result<String> {
        let mut body = Vec::new();
        put_str(&mut body, sql);
        put_values(&mut body, params);
        let b = self.call(Call::Statement, "", 0, &body)?;
        get_string(&b, &mut 0)
    }

    /// Have the node adopt a collection: its definition and the whole
    /// placement map, building the shards the map puts on that node.
    /// The peer's catalog. A peer from before this call answers "unknown
    /// call", which the caller treats as nothing to adopt.
    pub fn catalog(&self) -> Result<Catalog> {
        let b = self.call(Call::Catalog, "", 0, &[])?;
        let mut i = 0;
        Catalog::decode(get_bytes(&b, &mut i).ok_or_else(truncated)?)
    }

    pub fn create_collection(&self, coll: &Collection, tablets: &[Tablet]) -> Result<()> {
        let mut cat = Catalog::default();
        cat.collections.insert(coll.name.clone(), coll.clone());
        let mut body = Vec::new();
        put_bytes(&mut body, &self.encode_for_peer(&cat));
        put_tablets(&mut body, tablets, self.frame_version() >= 6);
        self.call(Call::CreateCollection, &coll.name, 0, &body).map(|_| ())
    }

    /// Pin a shard the node holds for a move to `to`; the files to pull.
    pub fn begin_move(
        &self,
        collection: &str,
        shard: usize,
        to: &str,
    ) -> Result<Vec<(String, u64)>> {
        let mut body = Vec::new();
        put_str(&mut body, to);
        let b = self.call(Call::BeginMove, collection, shard, &body)?;
        let mut i = 0;
        let n = get_count(&b, &mut i)?;
        (0..n)
            .map(|_| Ok((get_string(&b, &mut i)?, get_u64(&b, &mut i).ok_or_else(truncated)?)))
            .collect()
    }

    pub fn abort_move(&self, collection: &str, shard: usize) -> Result<()> {
        self.call(Call::AbortMove, collection, shard, &[]).map(|_| ())
    }

    /// Tell the source its pinned shard is fenced: every file is here.
    pub fn fence_move(&self, collection: &str, shard: usize) -> Result<()> {
        self.call(Call::FenceMove, collection, shard, &[]).map(|_| ())
    }

    /// A batch of the log to a follower: whether it is caught up, and the
    /// instant it stands at.
    pub fn ship(
        &self,
        collection: &str,
        shard: usize,
        term: u64,
        items: &[&crate::replication::ShipItem],
    ) -> Result<(bool, Timestamp)> {
        let mut body = Vec::new();
        put_u64(&mut body, term);
        put_uvarint(&mut body, items.len() as u64);
        for it in items {
            body.push(it.kind);
            put_str(&mut body, &it.key);
            put_u64(&mut body, it.ts);
            match &it.doc {
                Some(d) => {
                    put_bool(&mut body, true);
                    put_value(&mut body, d);
                }
                None => put_bool(&mut body, false),
            }
        }
        let b = self.call(Call::Ship, collection, shard, &body)?;
        let mut i = 0;
        Ok((get_bool(&b, &mut i)?, get_ts(&b, &mut i)?))
    }

    /// Renew this node's lease on the peer, as the steward `me`.
    pub fn lease(&self, me: &str) -> Result<()> {
        let mut body = Vec::new();
        put_str(&mut body, me);
        self.call(Call::Lease, "", 0, &body).map(|_| ())
    }

    /// Where a follower stands.
    pub fn ship_status(
        &self,
        collection: &str,
        shard: usize,
        term: u64,
    ) -> Result<(bool, Timestamp)> {
        let mut body = Vec::new();
        put_u64(&mut body, term);
        let b = self.call(Call::ShipStatus, collection, shard, &body)?;
        let mut i = 0;
        Ok((get_bool(&b, &mut i)?, get_ts(&b, &mut i)?))
    }

    /// `len` bytes of a pinned shard's file from `offset`.
    pub fn read_file(
        &self,
        collection: &str,
        shard: usize,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        put_str(&mut body, name);
        put_u64(&mut body, offset);
        put_u64(&mut body, len);
        let b = self.call(Call::ReadFile, collection, shard, &body)?;
        Ok(get_bytes(&b, &mut 0).ok_or_else(truncated)?.to_vec())
    }

    /// Have the node pull a pinned shard from `from` and adopt it under
    /// `tablets`, the map as it will be once the move completes.
    pub fn pull_shard(
        &self,
        coll: &Collection,
        tablets: &[Tablet],
        shard: usize,
        from: &str,
    ) -> Result<String> {
        let mut cat = Catalog::default();
        cat.collections.insert(coll.name.clone(), coll.clone());
        let mut body = Vec::new();
        put_bytes(&mut body, &self.encode_for_peer(&cat));
        put_tablets(&mut body, tablets, self.frame_version() >= 6);
        put_str(&mut body, from);
        let b = self.call(Call::PullShard, &coll.name, shard, &body)?;
        // What the target switched; a target from before it says nothing.
        Ok(get_string(&b, &mut 0).unwrap_or_default())
    }
}

fn decode_response(resp: Vec<u8>) -> Result<Vec<u8>> {
    let mut i = 0;
    match get_u8(&resp, &mut i)? {
        0 => Ok(resp[i..].to_vec()),
        _ => {
            let kind = get_u8(&resp, &mut i)?;
            let m = get_string(&resp, &mut i)?;
            Err(error_from(kind, m))
        }
    }
}

/// A shard on another node, as the coordinator calls it.
pub struct Remote {
    node: Arc<Node>,
    collection: String,
    index: usize,
    range: Option<(Option<String>, Option<String>)>,
    manifest_version: u64,
}

impl Remote {
    pub fn new(node: Arc<Node>, collection: &str, index: usize, tablet: &Tablet) -> Remote {
        Remote {
            node,
            collection: collection.to_string(),
            index,
            range: Some((tablet.lo.clone(), tablet.hi.clone())),
            manifest_version: 0,
        }
    }

    fn call(&self, call: Call, body: &[u8]) -> Result<Vec<u8>> {
        match self.node.call(call, &self.collection, self.index, body) {
            // A holder that does not have the collection is a shard that is
            // not there -- a node back on an empty volume, which adopts
            // nothing older than its directory -- and to a statement that
            // is the same as a holder that does not answer: refused naming
            // the shard, or missing under `partial_results`.
            Err(Error::Plan(m)) if m.starts_with("no such collection") => {
                Err(Error::Deadline(format!(
                    "shard {} of `{}` on {} holds no `{}`: a node back on an empty volume? \
                     (RESTORE ... NODE on it); use WITH (partial_results) to opt in to \
                     incomplete answers",
                    self.index, self.collection, self.node.url, self.collection
                )))
            }
            other => other,
        }
    }
}

impl ShardService for Remote {
    fn index(&self) -> usize {
        self.index
    }

    fn manifest_version(&self) -> u64 {
        self.manifest_version
    }

    fn may_hold(&self, prefix: &str) -> bool {
        exec::range_may_hold(self.range.as_ref(), prefix)
    }

    fn term_stats(&self, path: &str, terms: &[String], ts: Timestamp) -> Result<TermStats> {
        let mut body = Vec::new();
        put_str(&mut body, path);
        put_strs(&mut body, terms);
        put_ts(&mut body, ts);
        let b = self.call(Call::TermStats, &body)?;
        let mut i = 0;
        let num_docs = get_u64(&b, &mut i).ok_or_else(truncated)?;
        let total_doc_len = get_u64(&b, &mut i).ok_or_else(truncated)?;
        let n = get_count(&b, &mut i)?;
        let mut doc_freq = BTreeMap::new();
        for _ in 0..n {
            let t = get_string(&b, &mut i)?;
            doc_freq.insert(t, get_uvarint(&b, &mut i).ok_or_else(truncated)?);
        }
        Ok(TermStats { num_docs, total_doc_len, doc_freq })
    }

    fn prefix_terms(
        &self,
        path: &str,
        prefix: &str,
        ts: Timestamp,
        limit: usize,
        key_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut body = Vec::new();
        put_str(&mut body, path);
        put_str(&mut body, prefix);
        put_ts(&mut body, ts);
        put_uvarint(&mut body, limit as u64);
        put_opt_str(&mut body, key_prefix);
        let b = self.call(Call::PrefixTerms, &body)?;
        get_strs(&b, &mut 0)
    }

    fn candidates(&self, req: &CandidatesRequest<'_>) -> Result<ShardCandidates> {
        let mut body = Vec::new();
        put_str(&mut body, req.statement);
        put_values(&mut body, req.params);
        put_ts(&mut body, req.ts);
        put_opt_str(&mut body, req.prefix);
        put_uvarint(&mut body, req.k_prime as u64);
        put_stats(&mut body, req.stats);
        put_bool(&mut body, req.analyze);
        put_frontiers(&mut body, req.frontiers);
        let b = self.call(Call::Candidates, &body)?;
        get_candidates(&b, &mut 0)
    }

    fn scan(&self, req: &ScanRequest<'_>) -> Result<ShardScan> {
        let mut body = Vec::new();
        put_str(&mut body, req.statement);
        put_values(&mut body, req.params);
        put_ts(&mut body, req.ts);
        put_opt_str(&mut body, req.prefix);
        put_stats(&mut body, req.stats);
        put_bool(&mut body, req.analyze);
        put_uvarint(&mut body, req.keep as u64);
        put_opt_str(&mut body, req.after);
        put_uvarint(&mut body, req.fields.len() as u64);
        for (f, asc) in req.fields {
            put_str(&mut body, f);
            put_bool(&mut body, *asc);
        }
        put_frontiers(&mut body, req.frontiers);
        let b = self.call(Call::Scan, &body)?;
        get_scan(&b, &mut 0)
    }

    fn documents(
        &self,
        manifest_version: u64,
        ts: Timestamp,
        handles: &[(usize, u32)],
    ) -> Result<Vec<Value>> {
        let mut body = Vec::new();
        put_u64(&mut body, manifest_version);
        put_ts(&mut body, ts);
        put_uvarint(&mut body, handles.len() as u64);
        for (ui, ord) in handles {
            put_uvarint(&mut body, *ui as u64);
            put_u32(&mut body, *ord);
        }
        let b = self.call(Call::Documents, &body)?;
        get_values(&b, &mut 0)
    }

    fn get(&self, key: &str, ts: Timestamp) -> Result<Option<Value>> {
        let mut body = Vec::new();
        put_str(&mut body, key);
        put_ts(&mut body, ts);
        let b = self.call(Call::Get, &body)?;
        let mut i = 0;
        if get_bool(&b, &mut i)? {
            Ok(Some(get_value(&b, &mut i)?))
        } else {
            Ok(None)
        }
    }

    fn expand(&self, req: &ExpandRequest<'_>) -> Result<HopExpansion> {
        // The filter travels as the statement it came from: the holder
        // parses the same text with the same crate and takes the `walk`-th
        // walk's filter, as it takes the statement's predicate for a scan.
        let mut body = Vec::new();
        put_str(&mut body, req.statement);
        put_values(&mut body, req.params);
        put_ts(&mut body, req.ts);
        put_strs(&mut body, req.frontier);
        match req.limit {
            Some(n) => {
                put_bool(&mut body, true);
                put_uvarint(&mut body, n as u64);
            }
            None => put_bool(&mut body, false),
        }
        put_bool(&mut body, req.reverse);
        put_uvarint(&mut body, req.walk as u64);
        put_uvarint(&mut body, req.hop as u64);
        let b = self.call(Call::Expand, &body)?;
        let mut i = 0;
        let pairs = get_pairs(&b, &mut i)?;
        let scanned = get_num(&b, &mut i)?;
        Ok(HopExpansion { pairs, scanned })
    }

    fn present(&self, keys: &[String], ts: Timestamp) -> Result<Vec<String>> {
        let mut body = Vec::new();
        put_strs(&mut body, keys);
        put_ts(&mut body, ts);
        let b = self.call(Call::Present, &body)?;
        get_strs(&b, &mut 0)
    }
}

// ------------------------------------------------------------------ server

/// Connections the wire refused because `CELASTRO_WIRE_MAX_CONNECTIONS`
/// were open: `celastro_wire_connections_refused_total`.
static REFUSED_CONNECTIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn refused_connections() -> u64 {
    REFUSED_CONNECTIONS.load(Ordering::Relaxed)
}

/// How many connections the wire serves at once, and how long an idle one
/// is kept. A peer that opens a connection per statement and never closes
/// one, or many peers that each keep a pool, would otherwise be a thread
/// per connection without end: past the cap a connection is accepted and
/// closed at once, counted, and a connection that carried no frame for the
/// idle time is closed -- a peer's next call reconnects, which the client
/// side does on its own for a connection the holder closed while idle.
const WIRE_MAX_CONNECTIONS: usize = 1024;
const WIRE_IDLE_SECS: u64 = 300;

fn env_num<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
}

/// Serve this node's shards to other nodes until `stop` is set or a
/// shutdown signal arrives. One thread per connection; every call runs
/// under the database's lock, as a console request does.
pub fn serve(
    listener: TcpListener,
    db: Arc<RwLock<Db>>,
    token: String,
    stop: Arc<AtomicBool>,
    tls: Option<Arc<Tls>>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    // The catch-ups' driver: every half second, the next chunk of each
    // follower that is catching up is cut under the lock and left for the
    // shipper. The console's maintenance thread does the same where there
    // is one; a node serving its shards always has this one.
    {
        let (db, stop) = (db.clone(), stop.clone());
        std::thread::Builder::new()
            .name("replication".into())
            .spawn(move || {
                let mut idle = true;
                while !stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(if idle { 500 } else { 20 }));
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    // A served read: never queued as a writer, which would
                    // hold every new reader behind it, and never held back
                    // by one, which under sustained reads kept the step from
                    // ever running and the followers from ever catching up.
                    idle =
                        db.read_served().unwrap_or_else(|p| p.into_inner()).replication_step() == 0;
                }
            })
            .expect("a thread for the replication driver");
    }
    let (moves, followed, lease, identity) = {
        let g = db.read().unwrap_or_else(|p| p.into_inner());
        (
            g.moves(),
            g.followed(),
            g.lease(),
            Arc::new(Identity {
                node: g.node().map(String::from),
                role: g.role(),
                epoch: g.epoch(),
                token,
                also: token_also_from_env(),
            }),
        )
    };
    let max_connections: usize = env_num("CELASTRO_WIRE_MAX_CONNECTIONS", WIRE_MAX_CONNECTIONS);
    let idle: u64 = env_num("CELASTRO_WIRE_IDLE_SECS", WIRE_IDLE_SECS);
    let idle = (idle > 0).then(|| Duration::from_secs(idle));
    let open = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    loop {
        if stop.load(Ordering::Relaxed) || crate::signal::shutdown_requested() {
            return Ok(());
        }
        match listener.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false)?;
                if open.load(Ordering::Relaxed) >= max_connections {
                    let n = REFUSED_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n % 1000 == 0 {
                        crate::log::warn(
                            "wire_connection_refused",
                            &[
                                ("open", max_connections.to_string()),
                                ("refused_total", n.to_string()),
                            ],
                        );
                    }
                    drop(s);
                    continue;
                }
                // The handshake happens on the connection's own thread, with
                // its first read; a peer that never speaks costs that thread
                // its idle poll and nothing else.
                let s = match tls::accept(tls.as_ref(), s) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::log::warn("wire_connection_failed", &[("error", e.to_string())]);
                        continue;
                    }
                };
                let db = db.clone();
                let stop = stop.clone();
                let moves = moves.clone();
                let followed = followed.clone();
                let lease = lease.clone();
                let identity = identity.clone();
                let open = open.clone();
                open.fetch_add(1, Ordering::Relaxed);
                std::thread::spawn(move || {
                    serve_connection(s, &db, &moves, &followed, &lease, &identity, &stop, idle);
                    open.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                crate::signal::wait_readable(&listener, ACCEPT_WAIT);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The idle wait between checks of `stop`, so a stopped node lets go of its
/// open connections rather than serving them until the process ends.
const IDLE_POLL: Duration = Duration::from_millis(500);

type Moves = Mutex<BTreeMap<(String, usize), Arc<crate::engine::MoveOut>>>;

/// What a hello says of this process, fixed for its life: answered
/// without the database lock when a statement holds it, since a peer's
/// fresh connection asks before every statement -- including the pull of
/// a move, whose source holds its lock for the whole statement.
struct Identity {
    node: Option<String>,
    role: crate::engine::Role,
    epoch: u64,
    /// The wire token, and the second one a rotation accepts.
    token: String,
    also: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn serve_connection(
    mut s: Box<dyn Stream>,
    db: &RwLock<Db>,
    moves: &Moves,
    followed: &crate::engine::Followed,
    lease: &crate::engine::Lease,
    identity: &Identity,
    stop: &AtomicBool,
    idle: Option<Duration>,
) {
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(IDLE_POLL));
    let mut last_frame = Instant::now();
    loop {
        // Checked per frame, not only when a read times out: a connection
        // that never pauses -- a peer writing as fast as it can -- kept a
        // stopped node serving, and holding its directory, until the peer
        // paused. The resilience suite's restart under load found it.
        if stop.load(Ordering::Relaxed) || crate::signal::shutdown_requested() {
            return;
        }
        let frame = match read_frame(&mut s) {
            Ok(f) => {
                last_frame = Instant::now();
                f
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                if idle.is_some_and(|d| last_frame.elapsed() >= d) {
                    return;
                }
                continue;
            }
            Err(_) => return,
        };
        let mut resp = Vec::new();
        match handle(db, moves, followed, lease, identity, &frame) {
            Ok(body) => {
                resp.push(0);
                resp.extend_from_slice(&body);
            }
            Err(e) => {
                resp.push(1);
                resp.push(error_kind(&e));
                put_str(&mut resp, &error_message(&e));
            }
        }
        if write_frame(&mut s, &resp).is_err() {
            return;
        }
    }
}

/// Compare without leaking where the first difference is.
fn token_matches(expected: &str, given: &str) -> bool {
    let (a, b) = (expected.as_bytes(), given.as_bytes());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

thread_local! {
    static SERVING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the current thread is answering a request from another node.
/// `Db::run` reads it to keep a forwarded statement from fanning out again.
pub fn serving() -> bool {
    SERVING.with(|s| s.get())
}

struct Serving;

impl Drop for Serving {
    fn drop(&mut self) {
        SERVING.with(|s| s.set(false));
    }
}

/// The lock a call holds: shared for a read, exclusive for the rest. Both
/// read as `&Db`; only the exclusive one hands out `&mut Db`.
enum Held<'a> {
    Read(crate::lock::RwLockReadGuard<'a, Db>),
    Write(crate::lock::RwLockWriteGuard<'a, Db>),
}

impl std::ops::Deref for Held<'_> {
    type Target = Db;
    fn deref(&self) -> &Db {
        match self {
            Held::Read(g) => g,
            Held::Write(g) => g,
        }
    }
}

impl Held<'_> {
    fn exclusive(&mut self) -> &mut Db {
        match self {
            Held::Write(g) => g,
            Held::Read(_) => unreachable!("a call that changes the node holds the exclusive lock"),
        }
    }
}

fn handle(
    db: &RwLock<Db>,
    moves: &Moves,
    followed: &crate::engine::Followed,
    lease: &crate::engine::Lease,
    identity: &Identity,
    frame: &[u8],
) -> Result<Vec<u8>> {
    SERVING.with(|s| s.set(true));
    let _serving = Serving;
    let mut i = 0;
    let version = get_u8(frame, &mut i)?;
    if !(WIRE_VERSION..=WIRE_VERSION_MAX).contains(&version) {
        return Err(Error::Storage(format!(
            "wire version {version} is not one this node speaks ({WIRE_VERSION} to \
             {WIRE_VERSION_MAX}); both nodes must run a celastro that shares one"
        )));
    }
    let given = get_string(frame, &mut i)?;
    let (token, also) = (identity.token.as_str(), identity.also.as_deref());
    if !token_matches(token, &given) && !also.is_some_and(|a| token_matches(a, &given)) {
        return Err(Error::Plan(format!("wire token refused; both nodes read {TOKEN_ENV}")));
    }
    // The caller's identity, from version 5: refused when a newer process
    // has been seen at its address -- the zombie fence's server half. Held
    // in the peers' record without the database lock.
    let caller = if version >= 5 {
        Some((get_string(frame, &mut i)?, get_u64(frame, &mut i).ok_or_else(truncated)?))
    } else {
        None
    };
    if let Some((node, epoch)) = &caller {
        if let Ok(g) = db.try_read() {
            g.observe_caller(node, *epoch)?;
        }
    }
    let call = Call::from_u8(get_u8(frame, &mut i)?)
        .ok_or_else(|| Error::Storage("wire: unknown call".into()))?;
    let collection = get_string(frame, &mut i)?;
    let shard = get_num(frame, &mut i)?;
    let deadline_ms = if get_bool(frame, &mut i)? {
        Some(get_uvarint(frame, &mut i).ok_or_else(truncated)?)
    } else {
        None
    };
    let body = &frame[i..];
    let _armed = crate::deadline::arm(deadline_ms);
    let mut out = Vec::new();
    // The move calls that must not wait for this node's lock. A target reads
    // a pinned shard's files from the shared pin, so a coordinator that is
    // also the source can hold its own lock for the whole statement while
    // the target pulls from it; and a target pulls without its own lock, so
    // it keeps serving its shards while the files arrive. Only the pin and
    // the adoption take the lock.
    match call {
        Call::ReadFile => {
            let mut j = 0;
            let name = get_string(body, &mut j)?;
            let offset = get_u64(body, &mut j).ok_or_else(truncated)?;
            let len = get_u64(body, &mut j).ok_or_else(truncated)?;
            let pinned = moves
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(collection.clone(), shard))
                .cloned()
                .ok_or_else(|| {
                    Error::Plan(format!("shard {shard} of `{collection}` is not pinned for a move"))
                })?;
            put_bytes(&mut out, &pinned.read(&name, offset, len)?);
            return Ok(out);
        }
        // A holder's log into a copy this node follows, and where that copy
        // stands: under the followed copies' lock and no other, so a write
        // this node forwarded under its own lock cannot wait on itself.
        Call::Ship => {
            let mut j = 0;
            let term = get_u64(body, &mut j).ok_or_else(truncated)?;
            let n = get_count(body, &mut j)?;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                let kind = get_u8(body, &mut j)?;
                let key = get_string(body, &mut j)?;
                let ts = get_ts(body, &mut j)?;
                let doc =
                    if get_bool(body, &mut j)? { Some(get_value(body, &mut j)?) } else { None };
                items.push(crate::replication::ShipItem { kind, key, ts, doc });
            }
            let (caught_up, at) =
                crate::engine::apply_shipped(followed, &collection, shard, term, &items)?;
            put_bool(&mut out, caught_up);
            put_ts(&mut out, at);
            return Ok(out);
        }
        Call::ShipStatus => {
            let term = get_u64(body, &mut 0).ok_or_else(truncated)?;
            let (caught_up, at) =
                crate::engine::follower_status(followed, &collection, shard, term)?;
            put_bool(&mut out, caught_up);
            put_ts(&mut out, at);
            return Ok(out);
        }
        Call::Lease => {
            let from = get_string(body, &mut 0)?;
            crate::engine::renew_lease(lease, &from)?;
            return Ok(out);
        }
        Call::FenceMove => {
            match moves.lock().unwrap_or_else(|p| p.into_inner()).get(&(collection.clone(), shard))
            {
                Some(m) => m.fence(),
                None => {
                    return Err(Error::Plan(format!(
                        "shard {shard} of `{collection}` is not pinned for a move here"
                    )))
                }
            }
            return Ok(out);
        }
        Call::BeginMove => {
            let to = get_string(body, &mut 0)?;
            let pinned = moves
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&(collection.clone(), shard))
                .cloned();
            let list = match pinned {
                Some(m) if m.to == to => m.list()?,
                _ => {
                    let mut db = db.write().unwrap_or_else(|p| p.into_inner());
                    db.begin_move(&collection, shard, &to)?
                }
            };
            put_uvarint(&mut out, list.len() as u64);
            for (name, len) in &list {
                put_str(&mut out, name);
                put_u64(&mut out, *len);
            }
            return Ok(out);
        }
        Call::PullShard => {
            let mut j = 0;
            let bytes = get_bytes(body, &mut j).ok_or_else(truncated)?;
            let cat = Catalog::decode(bytes)?;
            let coll =
                cat.collections.into_values().next().ok_or_else(|| {
                    Error::Storage("wire: no collection in the definition".into())
                })?;
            let tablets = get_tablets(body, &mut j, version >= 6)?;
            let from = get_string(body, &mut j)?;
            let (me, tls) = {
                let g = db.read().unwrap_or_else(|p| p.into_inner());
                (
                    g.node()
                        .map(str::to_string)
                        .ok_or_else(|| Error::Plan("this node has no address".into()))?,
                    g.tls(),
                )
            };
            let source = Arc::new(Node::new(&from, Some(token), tls)?);
            let files = source.begin_move(&collection, shard, &me)?;
            // The copy holds no lock: a write forwarded through this node
            // meanwhile is served, and so is a pull's read from here.
            let dir = db
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .dir()
                .map(Path::to_path_buf)
                .ok_or_else(|| Error::Plan("a move needs a persistent database (--dir)".into()))?;
            let incoming = Db::pull_files(&dir, &collection, shard, &source, &files)?;
            // Every file is here: the source stops answering reads of the
            // shard before this node takes its writes, or a scan planned on
            // the old map read the source's copy after a write landed here
            // and missed it. A source too old to know the fence keeps the
            // old window, and is not waited for.
            let _ = source.fence_move(&collection, shard);
            let switch = db
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .finish_move_here(&coll, &tablets, shard, &incoming, &from)?;
            // The switch is carried holding nothing, for the same reason the
            // copy is.
            let switched = Db::carry_switch(&switch);
            put_str(&mut out, &switched);
            return Ok(out);
        }
        _ => {}
    }
    // The shard reads a coordinator sends -- statistics, candidates, a scan,
    // documents, a hop -- take the shared lock and run beside each other
    // and beside this node's own reads; everything that changes something
    // takes the exclusive one.
    if call == Call::Hello {
        // Under the lock when it is free, so a test's pretended epoch or
        // clock shows; from the fixed identity when a statement holds it.
        let (node, role, clock, epoch) = match db.try_read() {
            Ok(g) => (g.node().map(String::from), g.role(), g.clock_micros(), g.epoch()),
            Err(std::sync::TryLockError::Poisoned(p)) => {
                let g = p.into_inner();
                (g.node().map(String::from), g.role(), g.clock_micros(), g.epoch())
            }
            Err(std::sync::TryLockError::WouldBlock) => (
                identity.node.clone(),
                identity.role,
                crate::time::now_micros().max(0) as u64,
                identity.epoch,
            ),
        };
        put_opt_str(&mut out, node.as_deref());
        put_str(&mut out, env!("CARGO_PKG_VERSION"));
        put_str(&mut out, role.name());
        put_u64(&mut out, clock);
        put_u64(&mut out, epoch);
        out.push(CATALOG_VERSION);
        out.push(WIRE_VERSION_MAX);
        return Ok(out);
    }
    // A read takes the shared lock, and the served kind: the statement
    // it serves holds its coordinator's lock across this call, so a
    // writer waiting here must not hold it back -- that writer waits for
    // this node's statements, which may be waiting on that coordinator.
    // The catalog fetch of every sweep is a read too; taken as a write it
    // queued a writer on every node every few seconds.
    let read_call = matches!(
        call,
        Call::Hello
            | Call::Counters
            | Call::Catalog
            | Call::TermStats
            | Call::PrefixTerms
            | Call::Candidates
            | Call::Scan
            | Call::Documents
            | Call::Get
            | Call::Expand
            | Call::Present
    );
    let shared = db;
    let mut db = if read_call {
        Held::Read(db.read_served().unwrap_or_else(|p| p.into_inner()))
    } else {
        Held::Write(db.write().unwrap_or_else(|p| p.into_inner()))
    };
    match call {
        Call::BeginMove
        | Call::ReadFile
        | Call::PullShard
        | Call::FenceMove
        | Call::Ship
        | Call::ShipStatus
        | Call::Lease => {
            unreachable!("answered above")
        }
        Call::AbortMove => {
            db.exclusive().abort_move(&collection, shard);
        }
        Call::Hello => unreachable!("answered above"),
        Call::Catalog => {
            put_bytes(&mut out, &db.catalog.encode());
        }
        Call::Counters => {
            db.collection(&collection)?;
            put_ts(&mut out, db.now_ts());
            put_u64(&mut out, db.writes_to(&collection));
        }
        Call::Insert => {
            let doc = get_value(body, &mut 0)?;
            // Confirmed on the followers with the lock let go: the wait is
            // the shipper's, and nothing under the lock waits for a peer.
            let (ts, confirm) = {
                let d = db.exclusive();
                let ts = d.insert_here(&collection, doc)?;
                (ts, d.confirmation())
            };
            drop(db);
            confirm.wait()?;
            put_ts(&mut out, ts);
            return Ok(out);
        }
        Call::Delete => {
            let key = get_string(body, &mut 0)?;
            let (gone, confirm) = {
                let d = db.exclusive();
                let gone = d.delete_key_here(&collection, &key)?;
                (gone, d.confirmation())
            };
            drop(db);
            confirm.wait()?;
            put_bool(&mut out, gone);
            return Ok(out);
        }

        Call::Statement => {
            let mut j = 0;
            let sql = get_string(body, &mut j)?;
            let params = get_values(body, &mut j)?;
            if !sql.trim_start().get(..5).is_some_and(|p| p.eq_ignore_ascii_case("LOCAL")) {
                return Err(Error::Plan(
                    "a forwarded statement must carry its LOCAL prefix, or it would fan out again"
                        .into(),
                ));
            }
            // Deferred work -- a backup's copy, a forwarded write's carry --
            // finishes with this node's lock let go.
            let outcome = db.exclusive().execute_with(&sql, &params)?;
            drop(db);
            let text = match outcome.finished_with(shared)? {
                crate::engine::Outcome::Ack(m) => m,
                other => format!("{other:?}"),
            };
            put_str(&mut out, &text);
            return Ok(out);
        }
        Call::CreateCollection => {
            let mut j = 0;
            let bytes = get_bytes(body, &mut j).ok_or_else(truncated)?;
            let cat = Catalog::decode(bytes)?;
            let coll =
                cat.collections.into_values().next().ok_or_else(|| {
                    Error::Storage("wire: no collection in the definition".into())
                })?;
            let tablets = get_tablets(body, &mut j, version >= 6)?;
            db.exclusive().adopt_collection(coll, tablets)?;
        }
        Call::TermStats
        | Call::PrefixTerms
        | Call::Candidates
        | Call::Scan
        | Call::Documents
        | Call::Get
        | Call::Expand
        | Call::Present => {
            // Reads see the same statistics a local statement would: the
            // inferred path classes are folded in before planning, as
            // `Db::run_select` does for its own shards.
            let coll = db.planning_collection(&collection)?;
            if let Some(m) =
                moves.lock().unwrap_or_else(|p| p.into_inner()).get(&(collection.clone(), shard))
            {
                if m.fenced() {
                    return Err(Error::Plan(crate::engine::fenced_message(
                        &collection,
                        shard,
                        &m.to,
                    )));
                }
            }
            let shards = db.shards(&collection)?;
            let Some(sh) = shards.iter().find(|s| s.index == shard) else {
                return Err(Error::Plan(format!(
                    "shard {shard} of `{collection}` is not on this node; the placement maps \
                     disagree"
                )));
            };
            let local = Local { shard: sh, index: shard };
            let mut j = 0;
            match call {
                Call::TermStats => {
                    let path = get_string(body, &mut j)?;
                    let terms = get_strs(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    let t = local.term_stats(&path, &terms, ts)?;
                    put_u64(&mut out, t.num_docs);
                    put_u64(&mut out, t.total_doc_len);
                    put_uvarint(&mut out, t.doc_freq.len() as u64);
                    for (term, c) in &t.doc_freq {
                        put_str(&mut out, term);
                        put_uvarint(&mut out, *c);
                    }
                }
                Call::PrefixTerms => {
                    let path = get_string(body, &mut j)?;
                    let prefix = get_string(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    let limit = get_num(body, &mut j)?;
                    let key_prefix = get_opt(body, &mut j)?;
                    let terms =
                        local.prefix_terms(&path, &prefix, ts, limit, key_prefix.as_deref())?;
                    put_strs(&mut out, &terms);
                }
                Call::Candidates => {
                    let sql = get_string(body, &mut j)?;
                    let params = get_values(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    let prefix = get_opt(body, &mut j)?;
                    let k_prime = get_num(body, &mut j)?;
                    let stats = get_stats(body, &mut j)?;
                    let analyze = get_bool(body, &mut j)?;
                    let frontiers = get_frontiers(body, &mut j)?;
                    let sel = walk::bind_hops(&select_of(&sql, &params)?, &frontiers);
                    let k = sel.limit.unwrap_or(10);
                    let planned = exec::plan_sources(&coll, &sel, k)?;
                    let req = CandidatesRequest {
                        coll: &coll,
                        select: &sel,
                        ts,
                        prefix: prefix.as_deref(),
                        sources: &planned.sources,
                        k_prime,
                        stats: &stats,
                        analyze,
                        statement: &sql,
                        params: &params,
                        frontiers: &frontiers,
                    };
                    put_candidates(&mut out, &local.candidates(&req)?);
                }
                Call::Scan => {
                    let sql = get_string(body, &mut j)?;
                    let params = get_values(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    let prefix = get_opt(body, &mut j)?;
                    let stats = get_stats(body, &mut j)?;
                    let analyze = get_bool(body, &mut j)?;
                    // A row cap, not a count of items that follow: it may
                    // exceed the body's length, so it is not bounded by it.
                    let keep = get_uvarint(body, &mut j).ok_or_else(truncated)? as usize;
                    let after = get_opt(body, &mut j)?;
                    let nf = get_count(body, &mut j)?;
                    let mut fields = Vec::with_capacity(nf);
                    for _ in 0..nf {
                        fields.push((get_string(body, &mut j)?, get_bool(body, &mut j)?));
                    }
                    let frontiers = get_frontiers(body, &mut j)?;
                    let sel = walk::bind_hops(&select_of(&sql, &params)?, &frontiers);
                    let req = ScanRequest {
                        coll: &coll,
                        select: &sel,
                        ts,
                        prefix: prefix.as_deref(),
                        stats: &stats,
                        analyze,
                        keep,
                        after: after.as_deref(),
                        fields: &fields,
                        statement: &sql,
                        params: &params,
                        frontiers: &frontiers,
                    };
                    put_scan(&mut out, &local.scan(&req)?);
                }
                Call::Documents => {
                    let mv = get_u64(body, &mut j).ok_or_else(truncated)?;
                    let ts = get_ts(body, &mut j)?;
                    let n = get_count(body, &mut j)?;
                    let mut handles = Vec::with_capacity(n);
                    for _ in 0..n {
                        let ui = get_num(body, &mut j)?;
                        let ord = get_u32(body, &mut j).ok_or_else(truncated)?;
                        handles.push((ui, ord));
                    }
                    put_values(&mut out, &local.documents(mv, ts, &handles)?);
                }
                Call::Get => {
                    let key = get_string(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    match local.get(&key, ts)? {
                        Some(d) => {
                            put_bool(&mut out, true);
                            put_value(&mut out, &d);
                        }
                        None => put_bool(&mut out, false),
                    }
                }
                Call::Expand => {
                    let sql = get_string(body, &mut j)?;
                    let params = get_values(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    let frontier = get_strs(body, &mut j)?;
                    let limit =
                        if get_bool(body, &mut j)? { Some(get_num(body, &mut j)?) } else { None };
                    let reverse = get_bool(body, &mut j)?;
                    let wi = get_num(body, &mut j)?;
                    let hop = get_num(body, &mut j)?;
                    let sel = select_of(&sql, &params)?;
                    let hops = walk::walks_of(&sel);
                    let Some(Expr::Hops { via, filters, .. }) = hops.get(wi) else {
                        return Err(Error::Plan(format!(
                            "the statement has no walk number {wi} to expand"
                        )));
                    };
                    if via != &collection {
                        return Err(Error::Plan(format!(
                            "walk {wi} is over `{via}`, and this call is for `{collection}`"
                        )));
                    }
                    let req = ExpandRequest {
                        coll: &coll,
                        frontier: &frontier,
                        ts,
                        limit,
                        reverse,
                        filter: walk::filter_for(filters, hop),
                        statement: &sql,
                        params: &params,
                        walk: wi,
                        hop,
                    };
                    let x = local.expand(&req)?;
                    put_pairs(&mut out, &x.pairs);
                    put_uvarint(&mut out, x.scanned as u64);
                }
                Call::Present => {
                    let keys = get_strs(body, &mut j)?;
                    let ts = get_ts(body, &mut j)?;
                    put_strs(&mut out, &local.present(&keys, ts)?);
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(out)
}

/// The SELECT a statement text carries, with or without an EXPLAIN prefix.
/// Both ends parse the same text with the same crate, so the AST the
/// coordinator planned against is the AST the holder evaluates.
fn select_of(sql: &str, params: &[Value]) -> Result<Select> {
    let mut stmt = crate::sql::parse(sql, params)?;
    loop {
        match stmt {
            Statement::Select(s) => return Ok(*s),
            Statement::Explain { inner, .. } => stmt = *inner,
            Statement::Local(inner) => stmt = *inner,
            // A DELETE finds its keys with the same SELECT the coordinator
            // built from it, from the same text.
            Statement::Delete(d) => {
                let Some(p) = &d.predicate else {
                    return Err(Error::Plan("DELETE without WHERE crosses no wire".into()));
                };
                return Ok(exec::select_for_delete(&d.collection, p));
            }
            _ => return Err(Error::Plan("the wire carries SELECT statements only".into())),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_wire_answers_never_panic() {
        let hit = ScanHit {
            sort: vec![Value::Int(3), Value::Str("s".into())],
            key: "k".into(),
            doc: Some(Value::obj(vec![(
                "a".into(),
                Value::Array(vec![Value::Null, Value::Float(1.5)]),
            )])),
            handle: (1, 2),
            parent: Some(vec![1, 2, 3]),
        };
        let mut scan = Vec::new();
        put_scan(
            &mut scan,
            &ShardScan { hits: vec![hit], explain: ShardExplain::default(), timed_out: true },
        );
        let mut cands = Vec::new();
        put_candidates(
            &mut cands,
            &ShardCandidates {
                per_source: vec![vec![Candidate { key: "a".into(), raw_score: 0.5 }], vec![]],
                explain: ShardExplain::default(),
                timed_out: false,
            },
        );
        let mut tablets = Vec::new();
        put_tablets(
            &mut tablets,
            &[Tablet {
                node: "tcp://a:2352".into(),
                lo: None,
                hi: Some("m".into()),
                ..Default::default()
            }],
            false,
        );
        let mut values = Vec::new();
        put_values(&mut values, &[Value::Str("x".into()), Value::Timestamp(7), Value::Bool(true)]);
        let mut frontiers = Vec::new();
        put_frontiers(&mut frontiers, &[vec!["a".into(), "b".into()], vec![]]);
        let mut pairs = Vec::new();
        put_pairs(&mut pairs, &[("k".into(), "v".into())]);
        crate::fuzz::sweep(21, &[scan], 5000, |b| {
            let _ = get_scan(b, &mut 0);
        });
        crate::fuzz::sweep(22, &[cands], 5000, |b| {
            let _ = get_candidates(b, &mut 0);
        });
        crate::fuzz::sweep(23, &[tablets, values, frontiers, pairs], 6000, |b| {
            let _ = get_tablets(b, &mut 0, false);
            let _ = get_values(b, &mut 0);
            let _ = get_frontiers(b, &mut 0);
            let _ = get_pairs(b, &mut 0);
            let _ = get_strs(b, &mut 0);
            let _ = get_opt(b, &mut 0);
            let _ = get_ts(b, &mut 0);
            let _ = get_explain(b, &mut 0);
        });
    }
    use super::*;

    #[test]
    fn addresses_are_tcp_host_port_and_nothing_else() {
        assert_eq!(parse_url("tcp://127.0.0.1:2352").unwrap(), "127.0.0.1:2352");
        assert_eq!(parse_url("tcp://db-b:9000").unwrap(), "db-b:9000");
        assert_eq!(parse_url("tcp://db-b").unwrap(), "db-b:2352", "the port defaults");
        for bad in
            ["http://127.0.0.1:2352", "tcp://:2352", "127.0.0.1:2352", "tcp://", "tcp://db-b:x"]
        {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
        assert_eq!(with_default_port("0.0.0.0"), "0.0.0.0:2352");
        assert_eq!(with_default_port("0.0.0.0:9000"), "0.0.0.0:9000");
    }

    #[test]
    fn statistics_and_answers_survive_the_codec() {
        let mut g = GlobalStats::default();
        g.num_docs = 7;
        g.avg_doc_len = 3.25;
        g.doc_freq.insert("graph".into(), 4);
        let mut e = Expansion::default();
        e.terms = vec!["seg".into(), "segment".into()];
        e.truncated = true;
        let mut used = PrefixUse::default();
        used.positive = true;
        e.used = used;
        g.expansions.insert("seg".into(), e);
        g.prefix_cap = 2048;
        g.exact = true;
        let stats = BTreeMap::from([("body".to_string(), g)]);
        let mut out = Vec::new();
        put_stats(&mut out, &stats);
        let back = get_stats(&out, &mut 0).unwrap();
        let b = &back["body"];
        assert_eq!((b.num_docs, b.avg_doc_len, b.prefix_cap, b.exact), (7, 3.25, 2048, true));
        assert_eq!(b.doc_freq["graph"], 4);
        assert_eq!(b.expansions["seg"].terms, vec!["seg", "segment"]);
        assert!(b.expansions["seg"].truncated && b.expansions["seg"].used.positive);

        let hit = ScanHit {
            sort: vec![Value::Int(3), Value::Null],
            key: "k".into(),
            doc: Some(Value::Str("d".into())),
            handle: (2, 9),
            parent: Some(vec![1, 2]),
        };
        let mut sx = ShardExplain::default();
        sx.index = 4;
        sx.manifest_version = 11;
        let a = ShardScan { hits: vec![hit], explain: sx, timed_out: true };
        let mut out = Vec::new();
        put_scan(&mut out, &a);
        let back = get_scan(&out, &mut 0).unwrap();
        assert_eq!(back.hits.len(), 1);
        assert_eq!(back.hits[0].key, "k");
        assert_eq!(back.hits[0].handle, (2, 9));
        assert_eq!(back.hits[0].parent, Some(vec![1, 2]));
        assert!(back.timed_out);
        assert_eq!((back.explain.index, back.explain.manifest_version), (4, 11));
        assert!(back.explain.rendered.is_some(), "the holder renders its own block");
    }

    #[test]
    fn a_token_is_compared_whole() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abc", "abd"));
        assert!(!token_matches("abc", "ab"));
        assert!(!token_matches("", "a"));
    }
}
