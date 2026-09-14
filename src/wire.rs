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

use std::collections::BTreeMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::catalog::{Catalog, Collection, Tablet};
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
use crate::plan::walk::{self, ExpandRequest};
use crate::sql::ast::{Expr, Select, Statement};
use crate::text::scorer::{Expansion, GlobalStats, PrefixUse};
use crate::time::Timestamp;
use crate::value::Value;

/// Refused on mismatch, in both directions.
pub const WIRE_VERSION: u8 = 2;
/// The environment variable both ends read the token from.
pub const TOKEN_ENV: &str = "CELASTRO_WIRE_TOKEN";
const MAX_FRAME: u32 = 256 << 20;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(25);

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
        }
    }
}

/// `tcp://host:port` to `host:port`, refusing anything else: the scheme is
/// written down so that a URL for the console (`http://`) cannot be handed
/// to the wire by mistake.
pub fn parse_url(url: &str) -> Result<String> {
    let Some(rest) = url.strip_prefix("tcp://") else {
        return Err(Error::Plan(format!("a node address is `tcp://host:port`, not `{url}`")));
    };
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
            Ok(rest.to_string())
        }
        _ => Err(Error::Plan(format!("a node address is `tcp://host:port`, not `{url}`"))),
    }
}

/// The token this process presents and expects, from the environment.
pub fn token_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV).ok().filter(|t| !t.is_empty())
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

fn get_count(b: &[u8], i: &mut usize) -> Result<usize> {
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
        g.prefix_cap = get_count(b, i)?;
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
    sx.index = get_count(b, i)?;
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
        let ui = get_count(b, i)?;
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

fn put_tablets(out: &mut Vec<u8>, tablets: &[Tablet]) {
    put_uvarint(out, tablets.len() as u64);
    for t in tablets {
        put_str(out, &t.node);
        put_opt_str(out, t.lo.as_deref());
        put_opt_str(out, t.hi.as_deref());
    }
}

fn get_tablets(b: &[u8], i: &mut usize) -> Result<Vec<Tablet>> {
    let n = get_count(b, i)?;
    (0..n)
        .map(|_| Ok(Tablet { node: get_string(b, i)?, lo: get_opt(b, i)?, hi: get_opt(b, i)? }))
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
pub struct Node {
    url: String,
    addr: String,
    token: String,
    stream: Mutex<Option<TcpStream>>,
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
}

impl Node {
    pub fn new(url: &str, token: Option<&str>) -> Result<Node> {
        let addr = parse_url(url)?;
        let token = token
            .ok_or_else(|| {
                Error::Plan(format!(
                    "{TOKEN_ENV} is not set; the wire to {url} needs the token every node shares"
                ))
            })?
            .to_string();
        Ok(Node { url: url.to_string(), addr, token, stream: Mutex::new(None) })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn connect(&self) -> std::io::Result<TcpStream> {
        let mut last = None;
        for a in self.addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&a, CONNECT_TIMEOUT) {
                Ok(s) => {
                    s.set_nodelay(true)?;
                    return Ok(s);
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
    fn call(&self, call: Call, collection: &str, shard: usize, body: &[u8]) -> Result<Vec<u8>> {
        let deadline_ms = crate::deadline::remaining_ms();
        let mut req = vec![WIRE_VERSION];
        put_str(&mut req, &self.token);
        req.push(call as u8);
        put_str(&mut req, collection);
        put_uvarint(&mut req, shard as u64);
        match deadline_ms {
            Some(ms) => {
                put_bool(&mut req, true);
                put_uvarint(&mut req, ms);
            }
            None => put_bool(&mut req, false),
        }
        req.extend_from_slice(body);
        let mut guard = self.stream.lock().unwrap_or_else(|p| p.into_inner());
        let mut attempt = 0;
        loop {
            attempt += 1;
            if guard.is_none() {
                match self.connect() {
                    Ok(s) => *guard = Some(s),
                    Err(e) => {
                        return Err(Error::Deadline(format!(
                            "shard {shard} of `{collection}` on {} did not answer `{}` ({e}); use \
                             WITH (partial_results) to opt in to incomplete answers",
                            self.url,
                            call.name()
                        )))
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
                Ok(resp) => return decode_response(resp),
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
                        return Err(Error::Deadline(format!(
                            "shard {shard} of `{collection}` on {} did not answer `{}` {why}; use \
                             WITH (partial_results) to opt in to incomplete answers",
                            self.url,
                            call.name()
                        )));
                    }
                }
            }
        }
    }

    pub fn hello(&self) -> Result<Hello> {
        let b = self.call(Call::Hello, "", 0, &[])?;
        let mut i = 0;
        Ok(Hello { node: get_opt(&b, &mut i)?, version: get_string(&b, &mut i)? })
    }

    /// The holder's clock and its write counter for a collection: what a
    /// coordinator needs to pin a snapshot and to age its statistics cache.
    pub fn counters(&self, collection: &str) -> Result<(Timestamp, u64)> {
        let b = self.call(Call::Counters, collection, 0, &[])?;
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
    pub fn create_collection(&self, coll: &Collection, tablets: &[Tablet]) -> Result<()> {
        let mut cat = Catalog::default();
        cat.collections.insert(coll.name.clone(), coll.clone());
        let mut body = Vec::new();
        put_bytes(&mut body, &cat.encode());
        put_tablets(&mut body, tablets);
        self.call(Call::CreateCollection, &coll.name, 0, &body).map(|_| ())
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
        self.node.call(call, &self.collection, self.index, body)
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

    fn expand(&self, req: &ExpandRequest<'_>) -> Result<Vec<(String, String)>> {
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
        let b = self.call(Call::Expand, &body)?;
        get_pairs(&b, &mut 0)
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

/// Serve this node's shards to other nodes until `stop` is set or a
/// shutdown signal arrives. One thread per connection; every call runs
/// under the database's lock, as a console request does.
pub fn serve(
    listener: TcpListener,
    db: Arc<Mutex<Db>>,
    token: String,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    let token = Arc::new(token);
    loop {
        if stop.load(Ordering::Relaxed) || crate::signal::shutdown_requested() {
            return Ok(());
        }
        match listener.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false)?;
                let db = db.clone();
                let token = token.clone();
                let stop = stop.clone();
                std::thread::spawn(move || serve_connection(s, &db, &token, &stop));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => std::thread::sleep(POLL),
            Err(e) => return Err(e.into()),
        }
    }
}

/// The idle wait between checks of `stop`, so a stopped node lets go of its
/// open connections rather than serving them until the process ends.
const IDLE_POLL: Duration = Duration::from_millis(500);

fn serve_connection(mut s: TcpStream, db: &Mutex<Db>, token: &str, stop: &AtomicBool) {
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(IDLE_POLL));
    loop {
        let frame = match read_frame(&mut s) {
            Ok(f) => f,
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                continue;
            }
            Err(_) => return,
        };
        let mut resp = Vec::new();
        match handle(db, token, &frame) {
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

fn handle(db: &Mutex<Db>, token: &str, frame: &[u8]) -> Result<Vec<u8>> {
    SERVING.with(|s| s.set(true));
    let _serving = Serving;
    let mut i = 0;
    let version = get_u8(frame, &mut i)?;
    if version != WIRE_VERSION {
        return Err(Error::Storage(format!(
            "wire version {version} is not this node's {WIRE_VERSION}; both nodes must run the \
             same celastro"
        )));
    }
    let given = get_string(frame, &mut i)?;
    if !token_matches(token, &given) {
        return Err(Error::Plan(format!("wire token refused; both nodes read {TOKEN_ENV}")));
    }
    let call = Call::from_u8(get_u8(frame, &mut i)?)
        .ok_or_else(|| Error::Storage("wire: unknown call".into()))?;
    let collection = get_string(frame, &mut i)?;
    let shard = get_count(frame, &mut i)?;
    let deadline_ms = if get_bool(frame, &mut i)? {
        Some(get_uvarint(frame, &mut i).ok_or_else(truncated)?)
    } else {
        None
    };
    let body = &frame[i..];
    let _armed = crate::deadline::arm(deadline_ms);
    let mut db = db.lock().unwrap_or_else(|p| p.into_inner());
    let mut out = Vec::new();
    match call {
        Call::Hello => {
            put_opt_str(&mut out, db.node());
            put_str(&mut out, env!("CARGO_PKG_VERSION"));
        }
        Call::Counters => {
            db.collection(&collection)?;
            put_ts(&mut out, db.now_ts());
            put_u64(&mut out, db.writes_to(&collection));
        }
        Call::Insert => {
            let doc = get_value(body, &mut 0)?;
            put_ts(&mut out, db.insert_here(&collection, doc)?);
        }
        Call::Delete => {
            let key = get_string(body, &mut 0)?;
            put_bool(&mut out, db.delete_key_here(&collection, &key)?);
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
            let text = match db.execute_with(&sql, &params)? {
                crate::engine::Outcome::Ack(m) => m,
                other => format!("{other:?}"),
            };
            put_str(&mut out, &text);
        }
        Call::CreateCollection => {
            let mut j = 0;
            let bytes = get_bytes(body, &mut j).ok_or_else(truncated)?;
            let cat = Catalog::decode(bytes)?;
            let coll =
                cat.collections.into_values().next().ok_or_else(|| {
                    Error::Storage("wire: no collection in the definition".into())
                })?;
            let tablets = get_tablets(body, &mut j)?;
            db.adopt_collection(coll, tablets)?;
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
            db.absorb_for_read(&collection)?;
            let coll = db.collection(&collection)?.clone();
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
                    let limit = get_count(body, &mut j)?;
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
                    let k_prime = get_count(body, &mut j)?;
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
                    let keep = get_count(body, &mut j)?;
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
                        let ui = get_count(body, &mut j)?;
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
                        if get_bool(body, &mut j)? { Some(get_count(body, &mut j)?) } else { None };
                    let reverse = get_bool(body, &mut j)?;
                    let wi = get_count(body, &mut j)?;
                    let sel = select_of(&sql, &params)?;
                    let hops = sel.predicate.as_ref().map(walk::hops_in).unwrap_or_default();
                    let Some(Expr::Hops { via, filter, .. }) = hops.get(wi) else {
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
                        filter: filter.as_deref(),
                        statement: &sql,
                        params: &params,
                        walk: wi,
                    };
                    put_pairs(&mut out, &local.expand(&req)?);
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
    use super::*;

    #[test]
    fn addresses_are_tcp_host_port_and_nothing_else() {
        assert_eq!(parse_url("tcp://127.0.0.1:9000").unwrap(), "127.0.0.1:9000");
        assert_eq!(parse_url("tcp://db-b:9000").unwrap(), "db-b:9000");
        for bad in ["http://127.0.0.1:9000", "tcp://127.0.0.1", "tcp://:9000", "127.0.0.1:9000"] {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
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
