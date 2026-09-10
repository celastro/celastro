//! A shard: one memtable, a set of immutable segments, and the delete logs
//! that sit beside them.
//!
//! In the distributed build this is a tablet — one Raft group, its own
//! documents, its own inverted index, its own vector index. **Reads fan out,
//! writes do not** (§3.1): a write touches one shard and commits through that
//! shard's Raft group, with no distributed transaction on the ingest path. That
//! is why [`Shard::insert`] can commit the document, its columns, its postings,
//! its vector entry and the delete-log entry for the superseded version as one
//! atomic append (§6).
//!
//! This build has one Raft group of one member and calls the append a WAL
//! write, but the shape is the shape: everything that must commit together goes
//! into one record.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use crate::bitmap::Bitmap;
use crate::catalog::Collection;
use crate::codec::*;
use crate::column::CmpOp;
use crate::error::{Error, Result};
use crate::memtable::{FlushThresholds, Memtable, MemtableBudget};
use crate::mvcc::{visibility, DeleteLog, VisibilityCache};
use crate::residency::{Placement, ResidencyManager, Tier};
use crate::segment::{BuildOpts, PendingDoc, Segment, SegmentBuilder, SegmentSource};
use crate::text::analyzer::Analyzer;
use crate::text::TextSource;
use crate::time::{Hlc, Timestamp, MAX_TS};
use crate::value::Value;
use crate::vector::VectorStore;

/// Separator between the partition key and the primary key in the composite
/// sort key. `\u{1}` sorts below every printable character, so a tenant's
/// documents form a contiguous run and a prefix range is exact.
pub const KEY_SEP: char = '\u{1}';

/// Render one value as a component of the composite sort key.
///
/// Canonical, because the same value has to produce the same bytes wherever it
/// is rendered. `5` and `5.0` are the same partition key, so they must not sort
/// into different tablets — and a query literal of `5` has to reproduce the
/// prefix that a document containing `5.0` was filed under.
pub fn key_component(v: &Value) -> Result<String> {
    let s = match v {
        Value::Str(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
                (*f as i64).to_string()
            } else {
                format!("{f}")
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Timestamp(t) => crate::time::format_micros(*t),
        Value::Null => return Err(Error::Schema("a key component may not be null".into())),
        other => crate::json::to_string(other),
    };
    if s.contains(KEY_SEP) {
        // Otherwise a tenant id containing the separator could forge a key
        // inside another tenant's range, and row-level security binds on the
        // partition key (§12.4).
        return Err(Error::Schema(format!(
            "a key value may not contain U+0001 (the key separator): {s:?}"
        )));
    }
    Ok(s)
}

pub fn sort_key(coll: &Collection, doc: &Value) -> Result<String> {
    let pk = doc.path(&coll.primary_key).ok_or_else(|| {
        Error::Schema(format!("document has no primary key `{}`", coll.primary_key))
    })?;
    if pk.is_null() {
        return Err(Error::Schema("primary key is null".into()));
    }
    let pk = key_component(pk)?;
    match &coll.partition_key {
        Some(p) => {
            let pv = doc
                .path(p)
                .ok_or_else(|| Error::Schema(format!("document has no partition key `{p}`")))?;
            Ok(format!("{}{KEY_SEP}{pk}", key_component(pv)?))
        }
        None => Ok(pk),
    }
}

pub fn partition_prefix(partition_value: &Value) -> Result<String> {
    Ok(format!("{}{KEY_SEP}", key_component(partition_value)?))
}

/// A delete a compaction has to re-apply to its output, identified by
/// `(key, version commit_ts, delete_ts)` — by key alone it would match whichever
/// version survived rather than the one that died.
pub type CarriedDelete = (String, Timestamp, Timestamp);

/// One resident component, as `SHOW RESIDENCY` prints it:
/// `(segment id, component name, tier, last access, bytes)`.
pub type ResidencyRow = (u64, String, Tier, u64, usize);

/// A sealed segment plus its mutable delete log.
pub struct SegmentHandle {
    pub segment: Arc<Segment>,
    deletes: RwLock<DeleteLog>,
    /// Bumped on every delete so the visibility cache cannot serve a stale
    /// bitmap.
    epoch: AtomicU64,
    vis: VisibilityCache,
    /// Where the segment file is *now*. Mutable because archiving relocates
    /// it: a handle whose path still says `segments/` after the file moved to
    /// `archive/` unlinks nothing at sweep time and leaks the file.
    path: RwLock<Option<PathBuf>>,
}

impl SegmentHandle {
    pub fn new(segment: Segment, deletes: DeleteLog, path: Option<PathBuf>) -> Arc<SegmentHandle> {
        Arc::new(SegmentHandle {
            segment: Arc::new(segment),
            deletes: RwLock::new(deletes),
            epoch: AtomicU64::new(0),
            vis: VisibilityCache::default(),
            path: RwLock::new(path),
        })
    }

    pub fn path(&self) -> Option<PathBuf> {
        self.path.read().unwrap().clone()
    }

    fn set_path(&self, p: Option<PathBuf>) {
        *self.path.write().unwrap() = p;
    }

    pub fn id(&self) -> u64 {
        self.segment.id
    }

    pub fn mark_deleted(&self, ord: u32, ts: Timestamp) {
        self.deletes.write().unwrap().mark(ord, ts);
        self.epoch.fetch_add(1, AtomicOrdering::AcqRel);
        self.vis.clear();
    }

    pub fn dead_count(&self, t: Timestamp) -> usize {
        self.deletes.read().unwrap().dead_count(t)
    }

    pub fn dead_ratio(&self, t: Timestamp) -> f64 {
        let n = self.segment.num_docs();
        if n == 0 {
            0.0
        } else {
            self.dead_count(t) as f64 / n as f64
        }
    }

    /// The visibility bitmap at `t`, materialised and cached (§4.4).
    pub fn visibility(&self, t: Timestamp) -> Bitmap {
        let epoch = self.epoch.load(AtomicOrdering::Acquire);
        self.vis.get_or_build(t, epoch, || {
            let d = self.deletes.read().unwrap();
            visibility(&self.segment.ordinals, &d, t)
        })
    }

    pub fn is_visible(&self, ord: u32, t: Timestamp) -> bool {
        self.segment.ordinals.commit_ts.get(ord as usize).map(|c| *c <= t).unwrap_or(false)
            && !self.deletes.read().unwrap().is_deleted_at(ord, t)
    }

    pub fn encode_deletes(&self) -> Vec<u8> {
        self.deletes.read().unwrap().encode()
    }
}

/// What a query iterates over: the memtable and every sealed segment, behind
/// one interface. Candidate generation is written once and neither knows nor
/// cares which it is looking at.
pub enum Searchable<'a> {
    Mem(&'a Memtable),
    Seg(&'a SegmentHandle),
}

impl<'a> Searchable<'a> {
    pub fn label(&self) -> String {
        match self {
            Searchable::Mem(_) => "memtable".to_string(),
            Searchable::Seg(h) => format!("segment {}", h.id()),
        }
    }

    /// `(loads, fault-ins)` so far, for attributing decode and archive I/O to
    /// the unit that caused it.
    pub fn io_counters(&self) -> Option<(u64, u64)> {
        match self {
            Searchable::Mem(_) => None,
            Searchable::Seg(h) => h.segment.io_counters(),
        }
    }

    pub fn num_docs(&self) -> usize {
        match self {
            Searchable::Mem(m) => m.len(),
            Searchable::Seg(h) => h.segment.num_docs(),
        }
    }

    pub fn visibility(&self, t: Timestamp) -> Bitmap {
        match self {
            Searchable::Mem(m) => {
                let mut bm = Bitmap::new(m.len());
                for (i, ts) in m.ordinals.commit_ts.iter().enumerate() {
                    if *ts <= t {
                        bm.set(i);
                    }
                }
                for (ord, dts) in m.delete_entries() {
                    if dts <= t && (ord as usize) < m.len() {
                        bm.clear(ord as usize);
                    }
                }
                bm
            }
            Searchable::Seg(h) => h.visibility(t),
        }
    }

    /// Ordinals whose composite key starts with `prefix`. For a segment this is
    /// a range over sorted keys, which is what makes tenant pruning free (§3.2);
    /// for the memtable it is a scan.
    pub fn key_prefix(&self, prefix: &str) -> Bitmap {
        match self {
            Searchable::Mem(m) => m.key_prefix(prefix),
            Searchable::Seg(h) => {
                let hi = format!("{prefix}\u{10FFFF}");
                let (lo, hi) = h.segment.ordinals.range(Some(prefix), Some(&hi));
                Bitmap::range(h.segment.num_docs(), lo, hi)
            }
        }
    }

    /// Evaluate a structured predicate. Access path in cost order (§8.3):
    /// shredded column when this segment has one, variant decode otherwise.
    ///
    /// A column that declined some of its ordinals (`Column::mismatch` — values
    /// that were not of the column's type) has those answered from the variant
    /// blob and unioned in, so the answer does not depend on whether the
    /// segment shredded the path.
    pub fn filter(
        &self,
        path: &str,
        op: CmpOp,
        lit: &Value,
        candidates: &Bitmap,
    ) -> Result<(Bitmap, bool)> {
        self.eval(path, op, lit, candidates, false)
    }

    /// Ordinals where the predicate is *defined*. `NOT p` is
    /// `comparable ∖ p`, never `visible ∖ p`: a value that cannot be compared
    /// with the literal makes the predicate NULL, and NULL is not selected by
    /// either side of a negation.
    pub fn comparable(
        &self,
        path: &str,
        op: CmpOp,
        lit: &Value,
        candidates: &Bitmap,
    ) -> Result<Bitmap> {
        Ok(self.eval(path, op, lit, candidates, true)?.0)
    }

    fn eval(
        &self,
        path: &str,
        op: CmpOp,
        lit: &Value,
        candidates: &Bitmap,
        want_comparable: bool,
    ) -> Result<(Bitmap, bool)> {
        Ok(match self {
            Searchable::Mem(m) => (m.eval(path, op, lit, want_comparable), false),
            Searchable::Seg(h) => {
                let n = h.segment.num_docs();
                match h.segment.column(path)? {
                    Some(c) => {
                        let mut bm = if want_comparable {
                            c.comparable_set(op, lit)
                        } else {
                            c.filter(op, lit)
                        };
                        // Only the values the column could not represent need
                        // the document blobs, so the document store is not
                        // faulted in at all for a clean column.
                        if !c.mismatch.is_empty() {
                            let reader = h.segment.blob_reader()?;
                            let blobs = |ord: u32| reader.get(ord).map(|o| o.map(|b| b.to_vec()));
                            let rest = if want_comparable {
                                crate::column::comparable_variant(
                                    &blobs,
                                    n,
                                    path,
                                    op,
                                    lit,
                                    &c.mismatch,
                                )?
                            } else {
                                crate::column::filter_variant(
                                    &blobs,
                                    n,
                                    path,
                                    op,
                                    lit,
                                    &c.mismatch,
                                )?
                            };
                            bm.or_inplace(&rest);
                        }
                        (bm, true)
                    }
                    None => {
                        let reader = h.segment.blob_reader()?;
                        let blobs = |ord: u32| reader.get(ord).map(|o| o.map(|b| b.to_vec()));
                        let bm = if want_comparable {
                            crate::column::comparable_variant(&blobs, n, path, op, lit, candidates)?
                        } else {
                            crate::column::filter_variant(&blobs, n, path, op, lit, candidates)?
                        };
                        (bm, false)
                    }
                }
            }
        })
    }

    /// A handle on a full-text index.
    ///
    /// The handle exists because a sealed index is behind an `Arc` that may be
    /// evicted at any moment: the caller holds the handle for as long as it is
    /// reading, and a `TextSource` borrows from the handle rather than from the
    /// segment. Returning a bare `TextSource` would mean borrowing from
    /// something the residency sweeper is entitled to drop.
    pub fn text_handle(&self, path: &str) -> Result<Option<TextHandle<'_>>> {
        Ok(match self {
            Searchable::Mem(m) => {
                if m.analyzer(path).is_some() {
                    Some(TextHandle::Mem(m))
                } else {
                    None
                }
            }
            Searchable::Seg(h) => h.segment.text_index(path)?.map(TextHandle::Sealed),
        })
    }

    /// Which analyzer a path's text index uses.
    ///
    /// Fallible, because on an archived segment even this small read can be
    /// refused — and swallowing that would make a refused segment look like a
    /// path with no text index, silently changing the plan instead of failing.
    pub fn analyzer(&self, path: &str) -> Result<Option<Analyzer>> {
        match self {
            Searchable::Mem(m) => Ok(m.analyzer(path)),
            Searchable::Seg(h) => h.segment.analyzer_of(path),
        }
    }

    pub fn vector_handle(&self, path: &str) -> Result<Option<VectorHandle<'_>>> {
        Ok(match self {
            Searchable::Mem(m) => m.vectors.get(path).map(VectorHandle::Mem),
            Searchable::Seg(h) => h.segment.vector_index(path)?.map(VectorHandle::Sealed),
        })
    }

    pub fn key(&self, ord: u32) -> Option<&str> {
        match self {
            Searchable::Mem(m) => m.docs.get(ord as usize).map(|d| d.sort_key.as_str()),
            Searchable::Seg(h) => h.segment.ordinals.key(ord),
        }
    }

    pub fn document(&self, ord: u32) -> Result<Value> {
        match self {
            Searchable::Mem(m) => m
                .docs
                .get(ord as usize)
                .map(|d| d.doc.clone())
                .ok_or_else(|| Error::Storage("memtable ordinal out of range".into())),
            Searchable::Seg(h) => h.segment.document(ord),
        }
    }
}

/// A borrowed or reference-counted full-text index.
pub enum TextHandle<'a> {
    Sealed(Arc<crate::segment::SealedText>),
    Mem(&'a Memtable),
}

impl<'a> TextHandle<'a> {
    pub fn source(&self, path: &str) -> Option<TextSource<'_>> {
        match self {
            TextHandle::Sealed(t) => Some(t.source()),
            TextHandle::Mem(m) => m.text_source(path),
        }
    }
}

/// A borrowed or reference-counted vector index.
pub enum VectorHandle<'a> {
    Sealed(Arc<VectorStore>),
    Mem(&'a VectorStore),
}

impl<'a> std::ops::Deref for VectorHandle<'a> {
    type Target = VectorStore;
    fn deref(&self) -> &VectorStore {
        match self {
            VectorHandle::Sealed(v) => v,
            VectorHandle::Mem(v) => v,
        }
    }
}

/// Everything a query reads, pinned at query start.
///
/// A reader pins **both** a timestamp and a manifest version — the segment set
/// — so a flush or compaction mid-query never changes what it reads (§4.4).
/// Holding `Arc`s to the segment handles and to any frozen memtable is what
/// makes "superseded segments stay available until no reader references them"
/// mechanical rather than a convention.
pub struct Snapshot<'a> {
    pub ts: Timestamp,
    pub manifest_version: u64,
    pub segments: Vec<Arc<SegmentHandle>>,
    /// Borrowed, not cloned. The borrow is the guarantee: while a snapshot is
    /// alive the compiler will not let a write or a flush touch this shard, so
    /// "a flush mid-query never changes what a reader reads" is checked rather
    /// than asserted. Under real concurrency this becomes an `Arc` swapped
    /// under a lock, and `frozen` below is where the sealed-but-not-committed
    /// memtable would live.
    pub memtable: &'a Memtable,
    pub frozen: Vec<Arc<Memtable>>,
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub id: u64,
    pub level: u32,
    pub num_docs: usize,
    pub num_vectors: usize,
    pub min_key: String,
    pub max_key: String,
}

/// The tablet map's per-shard half: which segments exist right now.
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub version: u64,
    pub segments: Vec<SegmentMeta>,
    pub next_segment_id: u64,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, self.version);
        put_u64(&mut out, self.next_segment_id);
        put_uvarint(&mut out, self.segments.len() as u64);
        for s in &self.segments {
            put_u64(&mut out, s.id);
            put_u32(&mut out, s.level);
            put_uvarint(&mut out, s.num_docs as u64);
            put_uvarint(&mut out, s.num_vectors as u64);
            put_str(&mut out, &s.min_key);
            put_str(&mut out, &s.max_key);
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<Manifest> {
        let bad = || Error::Storage("manifest: truncated".into());
        let mut i = 0usize;
        let version = get_u64(b, &mut i).ok_or_else(bad)?;
        let next_segment_id = get_u64(b, &mut i).ok_or_else(bad)?;
        let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let mut segments = Vec::with_capacity(n);
        for _ in 0..n {
            segments.push(SegmentMeta {
                id: get_u64(b, &mut i).ok_or_else(bad)?,
                level: get_u32(b, &mut i).ok_or_else(bad)?,
                num_docs: get_uvarint(b, &mut i).ok_or_else(bad)? as usize,
                num_vectors: get_uvarint(b, &mut i).ok_or_else(bad)? as usize,
                min_key: get_str(b, &mut i).ok_or_else(bad)?,
                max_key: get_str(b, &mut i).ok_or_else(bad)?,
            });
        }
        Ok(Manifest { version, segments, next_segment_id })
    }
}

// --------------------------------------------------------------------------
// Write-ahead log
// --------------------------------------------------------------------------

/// Write durably: temp file, fsync, rename. Exposed because every file the
/// database cannot afford to find half-written goes through it.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

pub const WAL_INSERT: u8 = 1;
pub const WAL_DELETE: u8 = 2;
pub const WAL_SEALED: u8 = 3;

/// One record per commit. A document, its indexes and the delete-log entry for
/// the version it supersedes are all in the same record, because they must all
/// become visible at the same instant or none of them may (§6).
pub struct WalRecord {
    pub kind: u8,
    pub key: String,
    pub ts: Timestamp,
    pub doc: Option<Value>,
    /// Whether this write supersedes an earlier version. The *location* of
    /// that version is deliberately not recorded: replay re-derives it by key,
    /// which is deterministic and immune to the segment renumbering a flush
    /// performs between the write and the replay.
    pub supersedes: bool,
    pub segment_id: u64,
}

pub struct Wal {
    file: fs::File,
    path: PathBuf,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Wal> {
        let file = fs::OpenOptions::new().create(true).append(true).read(true).open(path)?;
        Ok(Wal { file, path: path.to_path_buf() })
    }

    pub fn append(&mut self, r: &WalRecord) -> Result<()> {
        let mut body = Vec::new();
        body.push(r.kind);
        put_str(&mut body, &r.key);
        put_u64(&mut body, r.ts);
        match &r.doc {
            Some(d) => {
                body.push(1);
                crate::variant::encode(d, &mut body);
            }
            None => body.push(0),
        }
        body.push(r.supersedes as u8);
        put_u64(&mut body, r.segment_id);
        let mut out = Vec::with_capacity(body.len() + 8);
        put_u32(&mut out, body.len() as u32);
        put_u32(&mut out, crc32(&body));
        out.extend_from_slice(&body);
        self.file.write_all(&out)?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Replay. A torn tail — a record whose length or checksum does not check
    /// out — ends the replay rather than failing it: the process died mid-write
    /// and everything before that point is still good.
    pub fn replay(path: &Path) -> Result<Vec<WalRecord>> {
        let b = match fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        let mut i = 0usize;
        while i + 8 <= b.len() {
            let mut j = i;
            let len = get_u32(&b, &mut j).unwrap() as usize;
            let crc = get_u32(&b, &mut j).unwrap();
            // A crash leaves a zero-extended tail, and `crc32(&[]) == 0`, so
            // eight zero bytes look exactly like a valid empty record. A real
            // record is never empty.
            if len == 0 {
                break;
            }
            let Some(body) = b.get(j..j + len) else { break };
            if crc32(body) != crc {
                break;
            }
            i = j + len;
            let mut k = 0usize;
            let kind = body[k];
            k += 1;
            let Some(key) = get_str(body, &mut k) else { break };
            let Some(ts) = get_u64(body, &mut k) else { break };
            let has_doc = body[k];
            k += 1;
            let doc = if has_doc == 1 {
                match crate::variant::decode(body, &mut k) {
                    Ok(v) => Some(v),
                    Err(_) => break,
                }
            } else {
                None
            };
            let supersedes = body[k] == 1;
            k += 1;
            let segment_id = get_u64(body, &mut k).unwrap_or(0);
            out.push(WalRecord { kind, key, ts, doc, supersedes, segment_id });
        }
        Ok(out)
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.file =
            fs::OpenOptions::new().write(true).truncate(true).create(true).open(&self.path)?;
        self.file = fs::OpenOptions::new().create(true).append(true).read(true).open(&self.path)?;
        Ok(())
    }
}

/// Where a particular version of a document lives right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loc {
    Mem(u32),
    /// A memtable that has been frozen but whose segment is not committed yet.
    Frozen(usize, u32),
    Seg(u64, u32),
}

#[derive(Default)]
pub struct ShardOpts {
    pub thresholds: FlushThresholds,
    pub build: BuildOpts,
    pub budget: Option<Arc<MemtableBudget>>,
    /// Versions older than this are dropped at compaction (§6). A backup pins
    /// it (§12.5).
    pub gc_horizon: Timestamp,
    /// Node-level residency accounting. Shared by every shard on the node,
    /// because the budget is a property of the machine, not of a tablet.
    pub residency: Option<Arc<ResidencyManager>>,
    /// Who this node is, for resolving the `minimal` tier.
    pub placement: Placement,
}

/// What a seal wrote. The ids are ascending, so the last is the segment
/// holding the surviving version of every key. `segment_ids` is empty when a
/// pinned horizon's drain collected every row — the memtable was still sealed,
/// the manifest still moved and the WAL was still truncated, which is why
/// "sealed nothing" cannot be spelled `None`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Sealed {
    pub segment_ids: Vec<u64>,
}

impl Sealed {
    /// The segment holding the surviving version of every key this seal wrote.
    pub fn newest(&self) -> Option<u64> {
        self.segment_ids.last().copied()
    }
}

pub struct Shard {
    pub coll: Collection,
    /// This shard's half-open key range `[lo, hi)`, from the tablet map.
    /// `None` on either side means unbounded. Range partitioning on the
    /// composite `(partition_key, primary_key)` is what lets a query
    /// constrained on the partition key prune to one tenant's tablets (§3.2);
    /// hash partitioning cannot, because a large tenant would pin to one shard
    /// forever.
    pub key_range: Option<(Option<String>, Option<String>)>,
    pub memtable: Memtable,
    pub frozen: Vec<Arc<Memtable>>,
    pub segments: Vec<Arc<SegmentHandle>>,
    pub manifest_version: u64,
    pub next_segment_id: u64,
    pub opts: ShardOpts,
    pub clock: Arc<Hlc>,
    dir: Option<PathBuf>,
    wal: Option<Wal>,
    /// Counters for `EXPLAIN` and for the operator-visible flush/compaction
    /// metrics of §12.1.
    pub flushes: u64,
    pub compactions: u64,
    /// The horizon the last version-collecting operation actually ran at,
    /// maxed over every flush and compaction since this shard was opened. At
    /// or above it nothing has been collected; below it a snapshot read may
    /// have lost versions it could once see. Process-local, like the flush and
    /// compaction counters: a reopened shard starts at zero.
    ///
    /// It is also the only place the collection is visible, and a holder of an
    /// older timestamp — a client, another node — cannot consult it before
    /// reading. That is why the default must not narrow further than it
    /// already has: an unpinned seal forgets only versions a write had already
    /// superseded, never a row that a snapshot below the seal can still read.
    pub retain_floor: Timestamp,
    /// Segments removed from the manifest whose files are still referenced by
    /// a reader. Swept whenever the last reference goes away; without this the
    /// files are simply never unlinked.
    retiring: Vec<Arc<SegmentHandle>>,
}

impl Shard {
    pub fn new(coll: Collection, clock: Arc<Hlc>, opts: ShardOpts) -> Shard {
        let memtable = Memtable::new(&coll, opts.budget.clone());
        Shard {
            coll,
            key_range: None,
            memtable,
            frozen: Vec::new(),
            segments: Vec::new(),
            manifest_version: 0,
            next_segment_id: 1,
            opts,
            clock,
            dir: None,
            wal: None,
            flushes: 0,
            compactions: 0,
            retain_floor: 0,
            retiring: Vec::new(),
        }
    }

    /// Assign this shard's key range. Under replication this comes from the
    /// control
    /// plane's tablet map (§10) and changes on split and merge.
    pub fn with_key_range(mut self, lo: Option<String>, hi: Option<String>) -> Shard {
        self.key_range = Some((lo, hi));
        self
    }

    /// Does this shard own `key`?
    pub fn owns(&self, key: &str) -> bool {
        match &self.key_range {
            None => true,
            Some((lo, hi)) => {
                lo.as_ref().map(|l| key >= l.as_str()).unwrap_or(true)
                    && hi.as_ref().map(|h| key < h.as_str()).unwrap_or(true)
            }
        }
    }

    pub fn attach_dir(&mut self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir.join("segments"))?;
        fs::create_dir_all(dir.join("archive"))?;
        fs::create_dir_all(dir.join("deletes"))?;
        self.wal = Some(Wal::open(&dir.join("wal.log"))?);
        self.dir = Some(dir.to_path_buf());
        Ok(())
    }

    pub fn snapshot(&self) -> Snapshot<'_> {
        self.snapshot_at(self.clock.peek())
    }

    pub fn snapshot_at(&self, ts: Timestamp) -> Snapshot<'_> {
        Snapshot {
            ts,
            manifest_version: self.manifest_version,
            segments: self.segments.clone(),
            memtable: &self.memtable,
            frozen: self.frozen.clone(),
        }
    }

    /// Every searchable unit in a snapshot, newest first. Order matters only
    /// for point lookups, where the newest visible version wins.
    pub fn sources<'a>(&'a self, snap: &'a Snapshot<'a>) -> Vec<Searchable<'a>> {
        let mut v: Vec<Searchable<'a>> = vec![Searchable::Mem(snap.memtable)];
        for f in &snap.frozen {
            v.push(Searchable::Mem(f));
        }
        for s in &snap.segments {
            v.push(Searchable::Seg(s));
        }
        v
    }

    pub fn num_docs(&self, t: Timestamp) -> usize {
        let snap = self.snapshot_at(t);
        self.sources(&snap).iter().map(|s| s.visibility(t).popcount()).sum()
    }

    /// Where a version lives.
    fn locate(&self, key: &str, t: Timestamp) -> Option<Loc> {
        if let Some(ord) = self.memtable.find_at(key, t) {
            return Some(Loc::Mem(ord));
        }
        for (i, f) in self.frozen.iter().enumerate().rev() {
            if let Some(ord) = f.find_at(key, t) {
                return Some(Loc::Frozen(i, ord));
            }
        }
        // Segments are searched newest first; the first visible hit wins, and
        // `is_visible` checks both conjuncts of the visibility predicate.
        for h in self.segments.iter().rev() {
            if let Some(ord) = h.segment.ordinals.find(key) {
                if h.is_visible(ord, t) {
                    return Some(Loc::Seg(h.id(), ord));
                }
            }
        }
        None
    }

    fn mark_superseded(&mut self, target: Loc, ts: Timestamp) {
        match target {
            Loc::Mem(ord) => self.memtable.mark_deleted(ord, ts),
            Loc::Frozen(i, ord) => {
                if let Some(f) = self.frozen.get(i) {
                    f.mark_deleted(ord, ts);
                }
            }
            Loc::Seg(sid, ord) => {
                if let Some(h) = self.segments.iter().find(|h| h.id() == sid) {
                    h.mark_deleted(ord, ts);
                }
            }
        }
    }

    /// Insert or replace one document.
    ///
    /// The read-before-write that finds the previous version is a cost a
    /// document store with any secondary index pays anyway; in return, one
    /// delete-log entry invalidates that version across every index type at
    /// once (§4.4).
    pub fn insert(&mut self, mut doc: Value) -> Result<Timestamp> {
        self.coll.validate(&doc)?;
        self.coll.coerce(&mut doc);
        let key = sort_key(&self.coll, &doc)?;
        // Everything that can reject this document must reject it *here*,
        // before the WAL append and before the previous version is superseded.
        // A write that fails halfway would otherwise delete the version it is
        // replacing, leave the new one half-indexed, and poison the WAL so the
        // shard can never be reopened.
        self.validate_indexable(&doc)?;
        let ts = self.clock.now();
        let prev = self.locate(&key, MAX_TS);
        if let Some(w) = self.wal.as_mut() {
            w.append(&WalRecord {
                kind: WAL_INSERT,
                key: key.clone(),
                ts,
                doc: Some(doc.clone()),
                supersedes: prev.is_some(),
                segment_id: 0,
            })?;
        }
        if let Some(p) = prev {
            self.mark_superseded(p, ts);
        }
        self.coll.observe_doc(&doc);
        self.memtable.insert(key, ts, doc)?;
        self.maybe_flush()?;
        Ok(ts)
    }

    /// Check every index's precondition. Called before anything is durable.
    fn validate_indexable(&self, doc: &Value) -> Result<()> {
        for idx in &self.coll.indexes {
            let crate::catalog::IndexKind::Vector { dims, .. } = &idx.kind else { continue };
            let Some(v) = doc.path(&idx.path) else { continue };
            if v.is_null() {
                continue;
            }
            let Some(f) = crate::segment::extract_vector(v) else {
                return Err(Error::Schema(format!(
                    "`{}` is indexed as a vector but the document has {}",
                    idx.path,
                    v.ty().name()
                )));
            };
            if f.len() != *dims {
                return Err(Error::Schema(format!(
                    "`{}` has {} dimensions but the index declares {dims}",
                    idx.path,
                    f.len()
                )));
            }
            if let Some(bad) = f.iter().find(|x| !x.is_finite()) {
                // One non-finite component poisons a whole quantized code table
                // and turns every distance in the segment into NaN.
                return Err(Error::Schema(format!(
                    "`{}` contains a non-finite component ({bad})",
                    idx.path
                )));
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<Timestamp>> {
        let ts = self.clock.now();
        let Some(prev) = self.locate(key, MAX_TS) else { return Ok(None) };
        if let Some(w) = self.wal.as_mut() {
            w.append(&WalRecord {
                kind: WAL_DELETE,
                key: key.to_string(),
                ts,
                doc: None,
                supersedes: true,
                segment_id: 0,
            })?;
        }
        self.mark_superseded(prev, ts);
        Ok(Some(ts))
    }

    /// The version of `key` live at `t`.
    ///
    /// Reads at a `t` below [`Shard::retain_floor`] are best-effort: a version
    /// that a write had already superseded before the last seal or compaction
    /// may have been forgotten there, and this returns whatever survived.
    /// Holding history across those operations means pinning `opts.gc_horizon`
    /// (§12.5), which is what both collectors read through
    /// [`Shard::retain_from`]. A row that is merely *deleted* is not in that
    /// class at the default: an unpinned seal still writes it, so a snapshot
    /// below the delete keeps reading it until a compaction collects.
    pub fn get(&self, key: &str, t: Timestamp) -> Result<Option<Value>> {
        match self.locate(key, t) {
            None => Ok(None),
            Some(Loc::Mem(ord)) => Ok(self.memtable.docs.get(ord as usize).map(|d| d.doc.clone())),
            Some(Loc::Frozen(i, ord)) => {
                Ok(self.frozen[i].docs.get(ord as usize).map(|d| d.doc.clone()))
            }
            Some(Loc::Seg(sid, ord)) => {
                let h = self
                    .segments
                    .iter()
                    .find(|h| h.id() == sid)
                    .ok_or_else(|| Error::SnapshotGone(format!("segment {sid}")))?;
                Ok(Some(h.segment.document(ord)?))
            }
        }
    }

    /// Adopt a new catalog version. A new index has to reach the memtable's
    /// in-memory structures, and those are built from the collection definition
    /// at construction — so a non-empty memtable is sealed (its contents become
    /// searchable through the new index immediately) and an empty one is simply
    /// rebuilt. Older sealed segments pick the index up through a rolling
    /// rebuild at compaction (§12.3).
    pub fn adopt_catalog(&mut self, coll: Collection) -> Result<()> {
        // The seal below runs against the *new* definition and can fail — a
        // vector index whose declared dims disagree with a document already in
        // the memtable, say. Keep the definition being replaced so that failure
        // can be undone: left in place, the new one makes every later flush
        // fail identically, and a shard whose memtable can never be drained
        // stops accepting writes, never truncates its WAL, and can never be
        // sealed.
        let previous = self.coll.clone();
        self.adopt_definition(coll);
        // The tier is part of the index definition, so a DDL change to it has
        // to reach the segments that already exist — not only the ones a later
        // flush will write.
        for h in &self.segments {
            self.adopt_segment(&h.segment);
        }
        if self.memtable.is_empty() {
            self.memtable = Memtable::new(&self.coll, self.opts.budget.clone());
        } else if let Err(e) = self.flush() {
            // Roll back only while the memtable is still unsealed: past that
            // point the new segment was built against the new definition, and
            // reverting the catalog would misdescribe it.
            if !self.memtable.is_empty() {
                self.coll = previous;
                for h in &self.segments {
                    self.adopt_segment(&h.segment);
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// Take the control plane's *definition* of the collection — declared
    /// columns, indexes, tiers — while keeping the inferred statistics this
    /// shard accumulated.
    ///
    /// The catalog's copy holds the aggregate across every shard. Assigning it
    /// wholesale would give this shard credit for the other shards' documents,
    /// and the next aggregation would then multiply by the shard count.
    pub fn adopt_definition(&mut self, coll: Collection) {
        let docs = self.coll.doc_count;
        let paths = std::mem::take(&mut self.coll.paths);
        self.coll = coll;
        self.coll.doc_count = docs;
        self.coll.paths = paths;
    }

    /// Give a segment the node's residency accounting and the collection's
    /// declared tiers. Every path a segment can arrive by goes through here:
    /// flush, compaction, and reopen.
    fn adopt_segment(&self, seg: &Segment) {
        if let Some(m) = self.opts.residency.as_ref() {
            seg.attach_residency(m.clone());
        }
        seg.set_tiers(self.opts.placement.resolve_tiers(&self.coll));
    }

    /// Release every component whose tier says it has been idle too long.
    /// Returns the bytes freed.
    pub fn unload_idle(&self, now: u64) -> usize {
        self.segments.iter().map(|h| h.segment.unload_idle(now)).sum()
    }

    /// Move segment files to or from the archive so that where the bytes live
    /// matches what the catalog declares.
    ///
    /// The unit of archiving is the *file*, not the index: one segment holds
    /// every index for its documents, so a segment can only be archived when
    /// nothing in the collection still wants a local copy. That is coarser
    /// than the per-index tier, and deliberately so — the alternative is
    /// splitting a segment by index, which would break the ordinal-space
    /// invariant (§4.2) that makes hybrid retrieval a bitmap intersection.
    ///
    /// Returns the number of files relocated.
    pub fn sync_archive(&mut self) -> Result<usize> {
        let Some(dir) = self.dir.clone() else { return Ok(0) };
        let want_archive = self.coll.all_indexes_archived();
        let mut moved = 0;
        for h in &self.segments {
            let name = format!("{:016x}.seg", h.id());
            let local = dir.join("segments").join(&name);
            let arch = dir.join("archive").join(&name);
            let (from, to) = if want_archive { (&local, &arch) } else { (&arch, &local) };
            if !from.exists() {
                // Already where it belongs; still make sure the source kind
                // agrees, since a tier change with no file move must still
                // switch fault-in accounting.
                if to.exists() {
                    h.segment.set_source(if want_archive {
                        SegmentSource::Archive(to.clone())
                    } else {
                        SegmentSource::File(to.clone())
                    });
                    h.set_path(Some(to.clone()));
                }
                continue;
            }
            // Drop the decoded components first: they were charged against the
            // residency budget under the old tier, and the bytes behind them
            // are about to move.
            h.segment.unload_all();
            fs::rename(from, to)?;
            h.segment.set_source(if want_archive {
                SegmentSource::Archive(to.clone())
            } else {
                SegmentSource::File(to.clone())
            });
            h.set_path(Some(to.clone()));
            moved += 1;
        }
        Ok(moved)
    }

    /// Per-component residency for `SHOW RESIDENCY`.
    pub fn residency_rows(&self) -> Vec<ResidencyRow> {
        let mut out = Vec::new();
        for h in &self.segments {
            for (name, tier, last, bytes) in h.segment.loaded_components() {
                out.push((h.id(), name, tier, last, bytes));
            }
        }
        out
    }

    /// The horizon version GC actually runs at: a backup pins `gc_horizon`,
    /// which holds it back (§12.5), and otherwise it is now. Compaction
    /// collects at it; a flush collects at it only while something is pinned,
    /// because a seal is not a GC pass. Both read it from here, and both
    /// report it through [`Shard::retain_floor`], so it lives in one place.
    pub fn retain_from(&self, now: Timestamp) -> Timestamp {
        if self.opts.gc_horizon > 0 {
            self.opts.gc_horizon.min(now)
        } else {
            now
        }
    }

    pub fn maybe_flush(&mut self) -> Result<bool> {
        if self.memtable.should_flush(&self.opts.thresholds) {
            self.flush()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Seal the memtable into segments.
    ///
    /// The tablet leader builds small segments itself at flush: fast, local,
    /// and it keeps the freshness path simple (§4.5). Large merged segments go
    /// to compaction instead. Until the built segments are committed the
    /// memtable stays searchable — which is why it is swapped out after the
    /// build rather than before.
    ///
    /// Version GC happens here under the same rule as in [`compaction::run`],
    /// but only as far as a pin asks for: a row already dead at
    /// [`Shard::retain_from`] is not written at all *when a horizon is
    /// pinned*, and a version superseded after that horizon is kept — in a
    /// segment of its own, because one segment holds one version per key. So
    /// only a pinned `gc_horizon` can make one flush emit several segments.
    /// Unpinned, the seal writes the newest version of every key, dead or not,
    /// and each row's tombstone is re-resolved into the segment's delete log,
    /// exactly as it always was. Compaction stays the only collector.
    ///
    /// `None` means nothing was sealed, and it means only that: the memtable
    /// was empty. [`Sealed`] carries the ids a seal did write, newest last,
    /// and is empty when a pinned drain collected every row — which is still a
    /// seal, and `Db::flush` still counts it.
    ///
    /// [`compaction::run`]: crate::compaction::run
    pub fn flush(&mut self) -> Result<Option<Sealed>> {
        if self.memtable.is_empty() {
            return Ok(None);
        }
        // Build first. A build that fails must leave the shard exactly as it
        // was — not with the memtable already swapped out and its contents
        // stranded in a list nothing will ever retry. The segment ids are part
        // of that: they come from a local counter that is committed only once
        // every build has succeeded.
        // The horizon this seal collects at. A pinned `gc_horizon` holds it
        // back (§12.5), and a version superseded after it is kept — in a
        // segment of its own, because one segment holds one version per key.
        // With nothing pinned the seal collects no more than it ever did:
        // `drain_into(0)` keeps every row, and only the versions
        // `SegmentBuilder::build`'s dedup already threw away are dropped, by
        // keeping layer 0. Collecting at `now` here would make a seal a
        // version-GC pass, and a seal is not one: `term_stats` sums
        // `total_doc_len` over physical rows, so BM25's avgdl would move
        // whenever a shard happened to seal, and a snapshot read below the
        // seal would lose rows it could read a moment earlier.
        let retain_from = self.retain_from(self.clock.peek());
        let drain_at = if self.opts.gc_horizon > 0 { retain_from } else { 0 };
        let mut layers = crate::segment::layer_by_version(self.memtable.drain_into(drain_at));
        if self.opts.gc_horizon == 0 {
            // Exactly the old dedup: layer 0 holds the newest version of every
            // key, which is the one `SegmentBuilder::build` used to keep.
            layers.truncate(1);
        }
        let mut next_id = self.next_segment_id;
        let mut built: Vec<Segment> = Vec::new();
        // Deepest layer first, so the surviving version of a key lands in the
        // highest-numbered output. Not a correctness property — at most one
        // version of a key satisfies `commit_ts <= t < delete_ts`, so `locate`
        // finds the right one whichever ids the layers get — but it is where
        // `locate`'s newest-first scan stops soonest, and it is what makes
        // `Sealed::newest` the survivor's segment.
        for layer in layers.into_iter().rev() {
            let mut b = SegmentBuilder::new(self.opts.build);
            for pd in layer {
                b.add(pd);
            }
            built.push(b.build(next_id, 0, &self.coll)?);
            next_id += 1;
        }

        // A delete recorded against a memtable ordinal has to be re-resolved
        // against the sealed segments, which renumber. Resolving by key alone
        // is wrong: an update writes a *second* version of the key and marks
        // the first dead, and each sealed segment holds one of them — so a
        // key-only match would mark the surviving version deleted and lose the
        // document. Match the version by its commit timestamp; an entry whose
        // row was collected at the drain simply matches nothing.
        let deletes: Vec<(String, Timestamp, Timestamp)> = self
            .memtable
            .delete_entries()
            .into_iter()
            .filter_map(|(ord, dts)| {
                self.memtable.docs.get(ord as usize).map(|d| (d.sort_key.clone(), d.commit_ts, dts))
            })
            .collect();

        // Every fallible step first, into a local list, exactly as
        // `install_compaction` does: a persist that fails on the second output
        // must not leave the first one live in `self.segments` while the
        // memtable still holds all of its rows, because then the same key is
        // reachable twice and a retry makes the duplicate permanent.
        let mut handles: Vec<Arc<SegmentHandle>> = Vec::new();
        for seg in built {
            self.adopt_segment(&seg);
            let path = self.persist_segment(&seg)?;
            let handle = SegmentHandle::new(seg, DeleteLog::new(), path);
            for (key, version_ts, delete_ts) in &deletes {
                if let Some(ord) = handle.segment.ordinals.find(key) {
                    if handle.segment.ordinals.commit_ts[ord as usize] == *version_ts {
                        handle.mark_deleted(ord, *delete_ts);
                    }
                }
            }
            handles.push(handle);
        }

        // Nothing below here may fail before the memtable is swapped out: this
        // is the one point at which the seal becomes reader-visible.
        self.next_segment_id = next_id;
        let sealed = Sealed { segment_ids: handles.iter().map(|h| h.id()).collect() };
        self.segments.extend(handles);
        // Id order is what `locate`'s newest-first scan reads as version order,
        // so state it here rather than leaving it to the loop direction —
        // `install_compaction` sorts for the same reason.
        self.segments.sort_by_key(|h| h.id());
        self.manifest_version += 1;
        self.flushes += 1;

        let old = std::mem::replace(
            &mut self.memtable,
            Memtable::new(&self.coll, self.opts.budget.clone()),
        );
        old.release_budget();
        // Only now has anything been forgotten: until the swap the memtable
        // still held every row, and `persist_manifest` failing below does not
        // put them back. Claiming the floor earlier would claim a collection
        // that had not happened yet, and the field's contract is that at or
        // above it nothing has been collected.
        self.retain_floor = self.retain_floor.max(retain_from);
        self.persist_manifest()?;
        if let Some(w) = self.wal.as_mut() {
            w.truncate()?;
        }
        Ok(Some(sealed))
    }

    fn persist_segment(&self, seg: &Segment) -> Result<Option<PathBuf>> {
        let Some(dir) = self.dir.as_ref() else { return Ok(None) };
        let p = dir.join("segments").join(format!("{:016x}.seg", seg.id));
        // Durable before the manifest names it, and long before the WAL that
        // could rebuild it is truncated.
        let bytes = seg.encode()?;
        atomic_write(&p, &bytes)?;
        // The local file is now the source; the in-memory copy can go.
        seg.set_source(crate::segment::SegmentSource::File(p.clone()));
        Ok(Some(p))
    }

    pub fn manifest(&self) -> Manifest {
        Manifest {
            version: self.manifest_version,
            next_segment_id: self.next_segment_id,
            segments: self
                .segments
                .iter()
                .map(|h| SegmentMeta {
                    id: h.id(),
                    level: h.segment.level,
                    num_docs: h.segment.num_docs(),
                    num_vectors: h.segment.num_vectors(),
                    min_key: h.segment.min_key().unwrap_or("").to_string(),
                    max_key: h.segment.max_key().unwrap_or("").to_string(),
                })
                .collect(),
        }
    }

    /// Write the manifest durably: temp file, fsync, rename.
    ///
    /// A bare `fs::write` truncates in place, so a crash halfway through leaves
    /// a manifest that will not decode — every segment file intact and the
    /// shard unable to open. The rename is what makes the switch atomic.
    pub fn persist_manifest(&self) -> Result<()> {
        let Some(dir) = self.dir.as_ref() else { return Ok(()) };
        for h in &self.segments {
            let d = h.encode_deletes();
            if !d.is_empty() {
                atomic_write(&dir.join("deletes").join(format!("{:016x}.dlog", h.id())), &d)?;
            }
        }
        let mut body = self.manifest().encode();
        let crc = crc32(&body);
        put_u32(&mut body, crc);
        atomic_write(&dir.join("MANIFEST"), &body)
    }

    /// Reopen from disk: install the manifest, then replay the WAL.
    pub fn open(coll: Collection, clock: Arc<Hlc>, opts: ShardOpts, dir: &Path) -> Result<Shard> {
        let mut s = Shard::new(coll, clock, opts);
        s.attach_dir(dir)?;
        if let Ok(b) = fs::read(dir.join("MANIFEST")) {
            if b.len() < 4 {
                return Err(Error::Storage("manifest: truncated".into()));
            }
            let (body, tail) = b.split_at(b.len() - 4);
            if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
                return Err(Error::Storage("manifest: checksum mismatch".into()));
            }
            let m = Manifest::decode(body)?;
            s.manifest_version = m.version;
            s.next_segment_id = m.next_segment_id.max(1);
            for meta in &m.segments {
                // Reopen reads the footer, not the file. A shard with a hundred
                // archived segments must not pull a hundred segments' worth of
                // postings and vectors into RAM just to answer "what exists".
                let local = dir.join("segments").join(format!("{:016x}.seg", meta.id));
                let archived = dir.join("archive").join(format!("{:016x}.seg", meta.id));
                let (p, src) = if local.exists() {
                    (local.clone(), SegmentSource::File(local))
                } else if archived.exists() {
                    (archived.clone(), SegmentSource::Archive(archived))
                } else {
                    return Err(Error::Storage(format!(
                        "segment {:016x} named by the manifest is missing",
                        meta.id
                    )));
                };
                let seg = Segment::open(src)?;
                s.adopt_segment(&seg);
                let dl = match fs::read(dir.join("deletes").join(format!("{:016x}.dlog", meta.id)))
                {
                    Ok(d) => DeleteLog::decode(&d)?,
                    Err(_) => DeleteLog::new(),
                };
                s.segments.push(SegmentHandle::new(seg, dl, Some(p)));
            }
        }
        let records = Wal::replay(&dir.join("wal.log"))?;
        for r in records {
            s.clock.observe(r.ts);
            match r.kind {
                WAL_INSERT => {
                    // Unconditionally, not only when the record says it
                    // superseded something. A crash between writing the segment
                    // and truncating the WAL replays inserts whose documents are
                    // already sealed; without this the key ends up live twice.
                    // On a genuine first insert this is a no-op.
                    if let Some(p) = s.locate(&r.key, MAX_TS) {
                        s.mark_superseded(p, r.ts);
                    }
                    if let Some(d) = r.doc {
                        s.coll.observe_doc(&d);
                        s.memtable.insert(r.key, r.ts, d)?;
                    }
                }
                WAL_DELETE => {
                    if let Some(loc) = s.locate(&r.key, MAX_TS) {
                        s.mark_superseded(loc, r.ts);
                    }
                }
                _ => {}
            }
        }
        Ok(s)
    }

    /// Replace `inputs` with `outputs` atomically from a reader's point of
    /// view: the manifest version moves in one step, and readers already
    /// holding the old handles keep them alive through their `Arc`s.
    pub fn install_compaction(
        &mut self,
        input_ids: &[u64],
        outputs: Vec<Segment>,
        carried_deletes: &[(String, Timestamp, Timestamp)],
        retain_from: Timestamp,
    ) -> Result<()> {
        let mut handles = Vec::new();
        for seg in outputs {
            self.adopt_segment(&seg);
            let path = self.persist_segment(&seg)?;
            let h = SegmentHandle::new(seg, DeleteLog::new(), path);
            for (key, version_ts, delete_ts) in carried_deletes {
                if let Some(ord) = h.segment.ordinals.find(key) {
                    if h.segment.ordinals.commit_ts[ord as usize] == *version_ts {
                        h.mark_deleted(ord, *delete_ts);
                    }
                }
            }
            handles.push(h);
        }
        let removed: Vec<Arc<SegmentHandle>> =
            self.segments.iter().filter(|h| input_ids.contains(&h.id())).cloned().collect();
        self.segments.retain(|h| !input_ids.contains(&h.id()));
        self.segments.extend(handles);
        self.segments.sort_by_key(|h| h.id());
        // The commit point: the merged, collected set is reader-visible from
        // here, so the floor has to be up before `persist_manifest` below can
        // fail. Raising it in `run`, next to `collect_for_compaction`, would
        // claim a collection that a later `build` error could still abandon.
        // Both of `run`'s call sites come through here, including the one that
        // installs no outputs at all — which still collected.
        self.retain_floor = self.retain_floor.max(retain_from);
        self.manifest_version += 1;
        self.compactions += 1;
        self.persist_manifest()?;
        self.retiring.extend(removed);
        self.sweep_retired();
        Ok(())
    }

    /// Unlink the files of retired segments no reader still holds.
    ///
    /// `Arc::strong_count == 1` means this list is the last owner. A segment
    /// whose reader is still alive stays on the list and is swept next time —
    /// checking once at compaction and then forgetting leaks the file forever.
    pub fn sweep_retired(&mut self) {
        let Some(dir) = self.dir.clone() else {
            self.retiring.retain(|h| Arc::strong_count(h) > 1);
            return;
        };
        self.retiring.retain(|h| {
            if Arc::strong_count(h) > 1 {
                return true;
            }
            if let Some(p) = h.path() {
                let _ = fs::remove_file(&p);
            }
            let _ = fs::remove_file(dir.join("deletes").join(format!("{:016x}.dlog", h.id())));
            false
        });
    }

    /// Documents a compaction of `ids` must carry into its output.
    ///
    /// `retain_from` is a *retention* horizon, not a snapshot. A document is
    /// carried over unless it was already dead at that time, so a horizon
    /// pinned by a backup holds dead rows back without hiding anything written
    /// since (§12.5). Passing it to `visibility` instead — which also demands
    /// `commit_ts <= retain_from` — drops every document committed after the
    /// horizon, which is to say it erases everything written since the backup
    /// started.
    ///
    /// Returns the documents plus the delete-log entries that must be
    /// re-applied to the output, each identified by `(key, commit_ts)` rather
    /// than by key alone: a segment holds one version per key, so a bare key
    /// would match whichever version survived, not the one that died.
    pub fn collect_for_compaction(
        &self,
        ids: &[u64],
        retain_from: Timestamp,
    ) -> Result<(Vec<PendingDoc>, Vec<CarriedDelete>)> {
        let mut docs = Vec::new();
        let mut deletes = Vec::new();
        for h in self.segments.iter().filter(|h| ids.contains(&h.id())) {
            let log = h.deletes.read().unwrap();
            let n = h.segment.num_docs();
            for ord in 0..n as u32 {
                if log.is_deleted_at(ord, retain_from) {
                    continue; // dead before the horizon: this is the GC.
                }
                let key = h.segment.ordinals.key(ord).unwrap_or("").to_string();
                let commit_ts = h.segment.ordinals.commit_ts[ord as usize];
                let dts = log.delete_ts(ord);
                if dts != crate::time::MAX_TS {
                    deletes.push((key.clone(), commit_ts, dts));
                }
                docs.push(PendingDoc { sort_key: key, commit_ts, doc: h.segment.document(ord)? });
            }
        }
        Ok((docs, deletes))
    }

    pub fn segment_summary(&self, t: Timestamp) -> Vec<(u64, u32, usize, usize, f64)> {
        self.segments
            .iter()
            .map(|h| {
                (
                    h.id(),
                    h.segment.level,
                    h.segment.num_docs(),
                    h.segment.num_vectors(),
                    h.dead_ratio(t),
                )
            })
            .collect()
    }

    /// Local document frequencies for a set of terms, gathered across every
    /// searchable unit. This is the exact two-phase gather of §8.2 — in a
    /// cluster it is a broadcast; here it is a loop, but it is the same
    /// quantity and the same guarantee.
    pub fn term_stats(
        &self,
        path: &str,
        terms: &[String],
        t: Timestamp,
    ) -> Result<(u64, u64, BTreeMap<String, u64>)> {
        let snap = self.snapshot_at(t);
        let mut df: BTreeMap<String, u64> = BTreeMap::new();
        let mut total_len = 0u64;
        let mut ndocs = 0u64;
        for s in self.sources(&snap) {
            let vis = s.visibility(t);
            ndocs += vis.popcount() as u64;
            // Not `unwrap_or_default()`: an archived segment configured to
            // refuse reads would then look like a path with no text index, and
            // its documents would count towards `ndocs` with zero document
            // frequency — an inflated IDF and a silently mis-ranked answer in
            // place of the refusal the operator asked for.
            let handle = s.text_handle(path)?;
            if let Some(src) = handle.as_ref().and_then(|h| h.source(path)) {
                total_len += src.total_doc_len();
                for term in terms {
                    if let Some(mut c) = src.cursor(term) {
                        // Count only visible postings: a document frequency
                        // that counts tombstones drifts as deletes accumulate.
                        let mut n = 0u64;
                        let mut d = c.advance(0);
                        while d != crate::text::postings::EXHAUSTED {
                            if vis.get(d as usize) {
                                n += 1;
                            }
                            d = c.advance(d + 1);
                        }
                        *df.entry(term.clone()).or_insert(0) += n;
                    }
                }
            }
        }
        Ok((ndocs, total_len, df))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::catalog::{ColumnDef, IndexDef, IndexKind, Metric};
    use crate::json;
    use crate::value::ValueType;

    fn coll() -> Collection {
        let mut c = Collection::new("articles", "id", Some("tenant_id".into()));
        c.declared.push(ColumnDef { path: "tenant_id".into(), ty: ValueType::Str, not_null: true });
        c.indexes.push(IndexDef::new(
            "b",
            "body",
            IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::default(),
        ));
        c.indexes.push(IndexDef::new(
            "e",
            "emb",
            IndexKind::Vector { dims: 4, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        c
    }

    fn doc(i: usize) -> Value {
        json::parse(&format!(
            r#"{{"id":"d{i:04}","tenant_id":"t{}","body":"document {i} about vectors and search","emb":[{},{},1.0,0.5]}}"#,
            i % 3,
            i as f32 / 100.0,
            (i % 5) as f32
        ))
        .unwrap()
    }

    fn shard() -> Shard {
        Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default())
    }

    #[test]
    fn write_read_update_delete() {
        let mut s = shard();
        for i in 0..50 {
            s.insert(doc(i)).unwrap();
        }
        let t = s.clock.peek();
        assert_eq!(s.num_docs(t), 50);
        assert!(s.get(&format!("t1{KEY_SEP}d0001"), t).unwrap().is_some());

        // An update supersedes rather than duplicating.
        let mut d = doc(1);
        d.set_path("body", Value::Str("rewritten".into()));
        s.insert(d).unwrap();
        let t2 = s.clock.peek();
        assert_eq!(s.num_docs(t2), 50);
        let got = s.get(&format!("t1{KEY_SEP}d0001"), t2).unwrap().unwrap();
        assert_eq!(got.path("body").unwrap().as_str(), Some("rewritten"));
        // And the old version is still visible at the older snapshot.
        let old = s.get(&format!("t1{KEY_SEP}d0001"), t).unwrap().unwrap();
        assert!(old.path("body").unwrap().as_str().unwrap().contains("about vectors"));

        s.delete(&format!("t1{KEY_SEP}d0001")).unwrap();
        let t3 = s.clock.peek();
        assert_eq!(s.num_docs(t3), 49);
        assert!(s.get(&format!("t1{KEY_SEP}d0001"), t3).unwrap().is_none());
    }

    #[test]
    fn flush_preserves_visibility_and_deletes() {
        let mut s = shard();
        for i in 0..100 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap();
        let before = s.num_docs(s.clock.peek());
        s.flush().unwrap();
        assert_eq!(s.segments.len(), 1);
        assert!(s.memtable.is_empty());
        assert_eq!(s.num_docs(s.clock.peek()), before);
        assert!(s.get(&format!("t0{KEY_SEP}d0003"), s.clock.peek()).unwrap().is_none());
        assert!(s.get(&format!("t0{KEY_SEP}d0006"), s.clock.peek()).unwrap().is_some());
    }

    #[test]
    fn deletes_that_land_after_a_freeze_survive_the_seal() {
        // The failure this guards: the delete refers to a memtable ordinal,
        // and the sealed segment renumbers everything.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t2{KEY_SEP}d0029")).unwrap();
        s.flush().unwrap();
        let t = s.clock.peek();
        assert!(s.get(&format!("t2{KEY_SEP}d0029"), t).unwrap().is_none());
        assert_eq!(s.segments[0].dead_count(t), 1);
    }

    #[test]
    fn a_delete_re_resolves_across_the_segments_a_pinned_flush_emits() {
        // The same re-resolution as above, but under a pin, where one seal can
        // emit several segments and the tombstone has to find its row in
        // whichever of them the version landed in. Resolving by key alone
        // would mark a *different* version dead.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        let key = format!("t2{KEY_SEP}d0029");
        let horizon = s.clock.peek();
        s.delete(&key).unwrap();
        s.opts.gc_horizon = horizon;
        s.flush().unwrap();
        let t = s.clock.peek();
        assert!(s.get(&key, t).unwrap().is_none());
        // Summed over segments, because a pinned flush may emit more than one.
        assert_eq!(s.segments.iter().map(|h| h.dead_count(t)).sum::<usize>(), 1);
        // The renumbered ordinal was matched by version, not by key: the row
        // is still there and the reader at the horizon still reads it.
        assert!(s.get(&key, horizon).unwrap().is_some());
    }

    #[test]
    fn an_unpinned_flush_does_not_move_the_scoring_statistics() {
        // A seal is not a version-GC pass, and this is why it must not become
        // one. `term_stats` masks `ndocs` by visibility but sums
        // `total_doc_len` over *physical* rows, so a seal that dropped the
        // tombstoned row would shrink the numerator of BM25's avgdl while the
        // denominator stayed put — every score in the collection moves the
        // moment a shard happens to seal. And since each shard seals on its
        // own `should_flush` threshold, the same workload would then score
        // differently at different shard counts.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t2{KEY_SEP}d0029")).unwrap();
        assert_eq!(s.opts.gc_horizon, 0, "nothing is pinned");
        let t = s.clock.peek();
        let terms = ["document".to_string()];
        let before = s.term_stats("body", &terms, t).unwrap();
        s.flush().unwrap();
        let after = s.term_stats("body", &terms, s.clock.peek()).unwrap();
        assert_eq!(before.0, 29, "one of the 30 is deleted");
        assert_eq!(before.1, 150, "but its length still counts, before and after");
        assert_eq!(before, after, "the seal moved the scoring statistics");
    }

    #[test]
    fn an_unpinned_seal_of_an_update_emits_exactly_one_segment() {
        // The other half of the rule above, and the half only an update
        // reaches: with nothing pinned the seal keeps layer 0 and nothing else,
        // so it emits one segment holding the newest version of every key —
        // exactly the dedup `SegmentBuilder::build` always did. Keeping the
        // superseded version instead splits the seal in two and carries the
        // dead version's length back into `total_doc_len`, which is the
        // numerator of BM25's avgdl for the whole collection. Only a pin may
        // ask for that, and a shard that happened to seal must not.
        //
        // Its own test rather than an update folded into the one above: there
        // `before == after` holds exactly, and it should, while an update
        // legitimately drops a version the write path had already superseded —
        // so folding one in could only be paid for by loosening that equality.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        let mut updated = doc(1);
        updated.set_path("body", Value::Str("rewritten".into()));
        s.insert(updated).unwrap();
        assert_eq!(s.opts.gc_horizon, 0, "nothing is pinned");
        let terms = ["document".to_string()];
        let before = s.term_stats("body", &terms, s.clock.peek()).unwrap();
        assert_eq!(before.1, 151, "the memtable still physically holds both versions");

        s.flush().unwrap();
        assert_eq!(s.segments.len(), 1, "an unpinned seal is one segment, always");
        let after = s.term_stats("body", &terms, s.clock.peek()).unwrap();
        assert_eq!(after.0, 30, "the update superseded a document, it did not add one");
        assert_eq!(after.1, 146, "29 bodies of five terms and the one-term rewrite");
        assert_eq!(after.2, before.2, "and no document gained or lost the term");
    }

    #[test]
    fn a_horizon_pinned_ahead_of_the_clock_collects_no_further_than_now() {
        // A pin holds collection *back*; pinned ahead of the clock it cannot
        // hold anything forward, so `retain_from` clamps it to now. Without the
        // clamp the floor would name an instant the shard has not reached and
        // claim a collection over versions that do not exist yet — and every
        // read below that instant would be reported best-effort when nothing
        // had in fact been forgotten.
        let mut s = shard();
        for i in 0..10 {
            s.insert(doc(i)).unwrap();
        }
        let now = s.clock.peek();
        // Far enough ahead to be unreachable during the test, and far short of
        // `MAX_TS`, which is the "never deleted" sentinel rather than a time.
        let future = now + (1 << 40);
        s.opts.gc_horizon = future;
        assert_eq!(s.retain_from(now), now, "a pin above the clock is a pin at the clock");

        s.flush().unwrap();
        assert!(
            s.retain_floor <= s.clock.peek(),
            "the floor names {}, an instant the shard has not reached",
            s.retain_floor
        );
    }

    #[test]
    fn a_pinned_flush_that_collects_every_row_still_reports_a_seal() {
        // The memtable was swapped out, the manifest moved and the WAL was
        // truncated, so `FLUSH` must not answer "0 shard(s) flushed". `None`
        // means one thing and one thing only: there was nothing to seal.
        let mut s = shard();
        for i in 0..5 {
            s.insert(doc(i)).unwrap();
        }
        for i in 0..5 {
            s.delete(&format!("t{}{KEY_SEP}d{i:04}", i % 3)).unwrap();
        }
        // Pinned at an instant every one of those rows is already dead at, so
        // the drain collects the whole memtable.
        s.opts.gc_horizon = s.clock.peek();
        let before = s.flushes;
        let sealed = s.flush().unwrap().expect("a non-empty memtable always seals");
        assert!(sealed.segment_ids.is_empty(), "every row was collected at the drain");
        assert!(sealed.newest().is_none());
        assert_eq!(s.flushes, before + 1, "the seal happened and has to be counted");
        assert!(s.memtable.is_empty());
        assert!(s.segments.is_empty());
        assert_eq!(s.flush().unwrap(), None, "and now there really is nothing to seal");
    }

    #[test]
    fn an_unpinned_flush_keeps_a_snapshot_below_it_readable() {
        // Collecting at `now` would make a delete take effect retroactively for
        // every reader below it, at the default, with no pin asked for and
        // nothing a holder of the older timestamp could consult to find out.
        // A seal forgets only what a write already superseded.
        let mut s = shard();
        for i in 0..10 {
            s.insert(doc(i)).unwrap();
        }
        let key = format!("t0{KEY_SEP}d0003");
        let before_delete = s.clock.peek();
        s.delete(&key).unwrap();
        s.flush().unwrap();
        assert!(
            s.get(&key, before_delete).unwrap().is_some(),
            "the seal took the row away from a snapshot that was reading it"
        );
        assert!(s.get(&key, s.clock.peek()).unwrap().is_none());
    }

    #[test]
    fn a_flush_that_fails_partway_installs_nothing() {
        // Two outputs, and the second one cannot be written: a directory sits
        // where its file goes. Installing the first anyway would leave its rows
        // reachable both from the segment list and from the memtable, which
        // still holds every one of them — the same key visible twice, and a
        // retry that makes the duplicate permanent by putting it in the
        // manifest.
        let dir = std::env::temp_dir().join(format!("celastro-halfflush-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        let horizon = s.clock.peek();
        s.opts.gc_horizon = horizon;
        // Superseded after the pin, so the seal has to emit one segment per
        // version: two.
        let mut updated = doc(7);
        updated.set_path("body", Value::Str("rewritten".into()));
        s.insert(updated).unwrap();

        // Ids ascend in write order, so this is the second file the flush
        // writes; the first has already been persisted when it fails.
        let blocked = dir.join("segments").join(format!("{:016x}.seg", s.next_segment_id + 1));
        fs::create_dir_all(&blocked).unwrap();

        assert!(s.flush().is_err(), "the blocked path has to surface as an error");
        assert!(s.segments.is_empty(), "a half-flush installed a segment");
        assert!(!s.memtable.is_empty(), "the rows are still where the retry will find them");
        // Ids come from a counter committed only once every build has
        // succeeded, so the failed seal left them reusable rather than leaking
        // the two it had spoken for.
        assert_eq!(s.next_segment_id, 1, "the failed seal leaked segment ids");
        assert_eq!(s.num_docs(s.clock.peek()), 20);
        assert_eq!(s.num_docs(horizon), 20);

        // And the retry, once the path is free, lands the whole seal.
        fs::remove_dir_all(&blocked).unwrap();
        assert!(s.flush().unwrap().is_some());
        assert!(s.memtable.is_empty());
        assert_eq!(s.num_docs(s.clock.peek()), 20);
        assert_eq!(s.num_docs(horizon), 20);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_replays_the_wal() {
        let dir = std::env::temp_dir().join(format!("celastro-wal-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(Hlc::new());
        {
            let mut s = Shard::new(coll(), clock.clone(), ShardOpts::default());
            s.attach_dir(&dir).unwrap();
            for i in 0..40 {
                s.insert(doc(i)).unwrap();
            }
            s.flush().unwrap();
            for i in 40..60 {
                s.insert(doc(i)).unwrap();
            }
            s.delete(&format!("t0{KEY_SEP}d0000")).unwrap();
            s.wal.as_mut().unwrap().sync().unwrap();
            s.persist_manifest().unwrap();
        }
        let s2 = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        let t = MAX_TS;
        assert_eq!(s2.segments.len(), 1);
        assert_eq!(s2.num_docs(t), 59);
        assert!(s2.get(&format!("t0{KEY_SEP}d0000"), t).unwrap().is_none());
        assert!(s2.get(&format!("t2{KEY_SEP}d0059"), t).unwrap().is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_snapshot_pins_the_memtable_as_well_as_the_segments() {
        let mut s = shard();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        let t = s.clock.peek();
        {
            let snap = s.snapshot_at(t);
            // Everything is in the memtable, and the snapshot reaches it.
            assert_eq!(snap.segments.len(), 0);
            let seen: usize = s.sources(&snap).iter().map(|u| u.visibility(t).popcount()).sum();
            assert_eq!(seen, 40);

            // While `snap` is alive, `s.flush()` does not compile: the snapshot
            // borrows the shard, so the compiler enforces what the design only
            // states — a flush cannot change what an open reader sees. Before
            // the memtable was pinned, this same sequence compiled and the
            // snapshot silently went blank.
        }
        s.flush().unwrap();
        let t2 = s.clock.peek();
        let snap = s.snapshot_at(t2);
        assert_eq!(snap.segments.len(), 1);
        let seen: usize = s.sources(&snap).iter().map(|u| u.visibility(t2).popcount()).sum();
        assert_eq!(seen, 40, "the same 40 documents, now sealed");
    }

    #[test]
    fn an_update_then_a_flush_keeps_the_new_version() {
        // The delete recorded for the superseded version refers to a memtable
        // ordinal; the sealed segment renumbers and keeps only the survivor.
        // Re-resolving that delete by key alone killed the survivor.
        let mut s = shard();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        let mut updated = doc(7);
        updated.set_path("body", Value::Str("rewritten".into()));
        s.insert(updated).unwrap();
        s.flush().unwrap();
        let t = s.clock.peek();
        assert_eq!(s.num_docs(t), 20);
        let got = s.get(&format!("t1{KEY_SEP}d0007"), t).unwrap().unwrap();
        assert_eq!(got.path("body").unwrap().as_str(), Some("rewritten"));
    }

    #[test]
    fn a_flush_at_a_pinned_horizon_keeps_the_version_the_horizon_reads() {
        // The horizon is pinned first and only then is the key updated, so the
        // old version dies *after* the horizon and a reader there must still
        // see it. Both versions are in one memtable, and one segment holds one
        // version per key — so the seal has to emit two, exactly as a merge
        // does in `merging_an_updated_key_keeps_the_version_a_pinned_horizon_
        // still_reads`. Sealing them into one segment drops the older version
        // and, because the survivor commits after the horizon, the reader there
        // loses the key entirely.
        let mut s = shard();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        let horizon = s.clock.peek();
        s.opts.gc_horizon = horizon;
        let mut updated = doc(7);
        updated.set_path("body", Value::Str("rewritten".into()));
        s.insert(updated).unwrap();

        let key = format!("t1{KEY_SEP}d0007");
        let body_at = |s: &Shard, t| -> Option<String> {
            s.get(&key, t).unwrap().map(|d| d.path("body").unwrap().as_str().unwrap().to_string())
        };
        let old = body_at(&s, horizon).expect("PRE-FLUSH: the horizon reader sees the old version");
        assert!(old.contains("about vectors"), "{old}");

        let sealed = s.flush().unwrap().expect("a non-empty memtable seals");
        assert_eq!(
            body_at(&s, horizon).as_deref(),
            Some(old.as_str()),
            "the flush changed what the horizon reader sees"
        );
        assert_eq!(body_at(&s, s.clock.peek()).as_deref(), Some("rewritten"));
        assert_eq!(s.segments.len(), 2, "one segment per retained version");
        // The layer order is a claim `Sealed::newest` makes, so pin it: the
        // survivor goes to the highest id, which is the one `locate`'s
        // newest-first scan reaches first. Dropping the `.rev()` in `flush`
        // puts the superseded version there instead.
        assert_eq!(sealed.segment_ids.len(), 2);
        let newest = sealed.newest().unwrap();
        assert_eq!(newest, s.segments.iter().map(|h| h.id()).max().unwrap());
        let seg_body = |id: u64| -> String {
            let h = s.segments.iter().find(|h| h.id() == id).unwrap();
            let ord = h.segment.ordinals.find(&key).expect("both segments hold the updated key");
            let d = h.segment.document(ord).unwrap();
            d.path("body").unwrap().as_str().unwrap().to_string()
        };
        assert_eq!(seg_body(newest), "rewritten", "the survivor is not in the newest segment");
        let oldest = *sealed.segment_ids.first().unwrap();
        assert!(seg_body(oldest).contains("about vectors"), "{}", seg_body(oldest));
        // Exactly one version is visible at either timestamp: retaining the
        // superseded row must not double-count the key.
        assert_eq!(s.num_docs(horizon), 20);
        assert_eq!(s.num_docs(s.clock.peek()), 20);
    }

    #[test]
    fn a_delete_then_a_flush_still_deletes() {
        let mut s = shard();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t1{KEY_SEP}d0007")).unwrap();
        s.flush().unwrap();
        let t = s.clock.peek();
        assert_eq!(s.num_docs(t), 19);
        assert!(s.get(&format!("t1{KEY_SEP}d0007"), t).unwrap().is_none());
    }

    #[test]
    fn an_update_deleted_after_the_flush_of_its_predecessor() {
        // Update, delete, then flush: two entries for one key, only the second
        // of which refers to the surviving version.
        let mut s = shard();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        let mut updated = doc(7);
        updated.set_path("body", Value::Str("rewritten".into()));
        s.insert(updated).unwrap();
        s.delete(&format!("t1{KEY_SEP}d0007")).unwrap();
        s.flush().unwrap();
        let t = s.clock.peek();
        assert_eq!(s.num_docs(t), 19);
        assert!(s.get(&format!("t1{KEY_SEP}d0007"), t).unwrap().is_none());
    }

    #[test]
    fn a_rejected_write_leaves_the_previous_version_alone() {
        let mut s = shard();
        for i in 0..10 {
            s.insert(doc(i)).unwrap();
        }
        let before = s.num_docs(s.clock.peek());
        // `emb` is declared with 4 dimensions.
        let mut bad = doc(3);
        bad.set_path("emb", crate::json::parse("[1.0,2.0,3.0]").unwrap());
        let e = s.insert(bad).unwrap_err().to_string();
        assert!(e.contains("dimensions"), "{e}");
        assert_eq!(s.num_docs(s.clock.peek()), before, "a failed write must change nothing");
        assert!(s.get(&format!("t0{KEY_SEP}d0003"), s.clock.peek()).unwrap().is_some());
        // And the shard is still usable, including through a flush.
        s.insert(doc(99)).unwrap();
        s.flush().unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), before + 1);
    }

    #[test]
    fn a_non_finite_vector_is_refused_rather_than_poisoning_a_segment() {
        let mut s = shard();
        let mut bad = doc(0);
        bad.set_path("emb", crate::json::parse("[1e40, 1.0, 1.0, 1.0]").unwrap());
        let e = s.insert(bad).unwrap_err().to_string();
        assert!(e.contains("non-finite"), "{e}");
    }

    #[test]
    fn key_components_are_canonical_and_cannot_forge_a_partition() {
        // 5 and 5.0 are the same partition key and must render identically,
        // or they file into different tablets and a query finds only one.
        assert_eq!(key_component(&Value::Int(5)).unwrap(), "5");
        assert_eq!(key_component(&Value::Float(5.0)).unwrap(), "5");
        assert_eq!(key_component(&Value::Float(5.5)).unwrap(), "5.5");
        assert_eq!(
            partition_prefix(&Value::Int(5)).unwrap(),
            partition_prefix(&Value::Float(5.0)).unwrap()
        );
        // A separator in a key value would let one tenant address another's range.
        let e = key_component(&Value::Str("evil\u{1}tenant".into())).unwrap_err().to_string();
        assert!(e.contains("separator"), "{e}");
    }

    #[test]
    fn replay_is_idempotent_when_a_crash_lands_between_seal_and_truncate() {
        let dir = std::env::temp_dir().join(format!("celastro-replay-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let wal_bytes;
        {
            let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
            s.attach_dir(&dir).unwrap();
            for i in 0..12 {
                s.insert(doc(i)).unwrap();
            }
            s.wal.as_mut().unwrap().sync().unwrap();
            wal_bytes = fs::read(dir.join("wal.log")).unwrap();
            s.flush().unwrap();
            s.persist_manifest().unwrap();
        }
        // The segment and manifest are durable, the WAL truncation is not:
        // exactly the window a crash lands in.
        fs::write(dir.join("wal.log"), &wal_bytes).unwrap();
        let s2 = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s2.num_docs(MAX_TS), 12, "replaying sealed writes must not duplicate them");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The differential test behind "shredding is a physical decision, not a
    /// schema commitment" (§2.1): the same documents and the same predicate
    /// must give the same answer before and after a flush, whatever mixture of
    /// types a path happens to hold.
    ///
    /// Every disagreement this found was a real defect — a scalar at an array
    /// path vanishing, a string in a numeric column comparing equal to zero,
    /// `<>` matching every row against a type-mismatched literal, one array
    /// value flipping a whole column to multi-value.
    #[test]
    fn shredded_and_unshredded_paths_answer_identically() {
        use crate::column::CmpOp::*;

        let mut c = Collection::new("mixed", "id", None);
        let mut docs: Vec<Value> = Vec::new();
        let push = |docs: &mut Vec<Value>, i: usize, body: &str| {
            docs.push(crate::json::parse(&format!(r#"{{"id":"d{i:04}",{body}}}"#)).unwrap());
        };
        // 2000 documents so that a *single* off-type value stays under the
        // catalog's noise floor and the path is still promoted to a column.
        // That is the case that matters: a column that is "effectively" one
        // type, with a handful of documents it cannot represent.
        for i in 0..2000usize {
            match i {
                500 => push(&mut docs, i, r#""n":"not a number","tags":["hot"],"s":"alpha""#),
                700 => push(&mut docs, i, r#""n":700,"tags":"hot","s":"beta""#),
                900 => push(&mut docs, i, r#""n":900,"tags":["hot"],"s":["odd"]"#),
                1100 => push(&mut docs, i, r#""n":true,"tags":["cold"],"s":"gamma""#),
                _ => {
                    let n = (i % 13) as i64 - 4;
                    let tag = ["hot", "cold", "warm"][i % 3];
                    let sv = ["alpha", "beta", "gamma", "delta"][i % 4];
                    if i % 17 == 0 {
                        // Some documents omit paths entirely, and some are null.
                        push(&mut docs, i, &format!(r#""n":null,"tags":[],"s":"{sv}""#));
                    } else if i % 23 == 0 {
                        push(&mut docs, i, &format!(r#""tags":["{tag}"],"s":"{sv}""#));
                    } else if i % 29 == 0 {
                        push(
                            &mut docs,
                            i,
                            &format!(
                                r#""n":{n},"tags":["{tag}","hot"],"s":"{sv}","rare":"present""#
                            ),
                        );
                    } else {
                        push(&mut docs, i, &format!(r#""n":{n},"tags":["{tag}"],"s":"{sv}""#));
                    }
                }
            }
        }
        for d in &docs {
            c.observe_doc(d);
        }
        // The fixture is only meaningful if these actually become columns.
        for p in ["n", "tags", "s"] {
            assert!(
                c.shred_candidates().iter().any(|(x, _)| x == p),
                "`{p}` was not promoted to a column; the test would be vacuous"
            );
        }

        let mut s = Shard::new(c, Arc::new(Hlc::new()), ShardOpts::default());
        for d in &docs {
            s.insert(d.clone()).unwrap();
        }
        let t = s.clock.peek();

        // Read every answer from the memtable (variant path).
        let probes: Vec<(&str, crate::column::CmpOp, Value)> = vec![
            ("n", Eq, Value::Int(0)),
            ("n", Eq, Value::Int(9)),
            ("n", Ne, Value::Int(9)),
            ("n", Ne, Value::Str("x".into())),
            ("n", Lt, Value::Int(1)),
            ("n", Ge, Value::Float(3.5)),
            ("n", IsNull, Value::Null),
            ("n", IsNotNull, Value::Null),
            ("n", In, Value::Array(vec![Value::Int(1), Value::Int(9)])),
            ("tags", ArrayContains, Value::Str("hot".into())),
            ("tags", Eq, Value::Str("hot".into())),
            ("tags", Ne, Value::Str("cold".into())),
            ("tags", Prefix, Value::Str("wa".into())),
            ("tags", IsNull, Value::Null),
            ("s", Eq, Value::Str("alpha".into())),
            ("s", Ne, Value::Str("alpha".into())),
            ("s", Eq, Value::Int(5)),
            ("s", Ne, Value::Int(5)),
            ("s", Prefix, Value::Str("al".into())),
            ("s", ArrayContains, Value::Str("odd".into())),
            ("rare", Eq, Value::Str("present".into())),
            ("rare", IsNull, Value::Null),
            ("absent", Eq, Value::Str("x".into())),
            ("absent", IsNull, Value::Null),
        ];

        let read = |s: &Shard, t: Timestamp| -> Vec<(Vec<String>, Vec<String>)> {
            let snap = s.snapshot_at(t);
            probes
                .iter()
                .map(|(path, op, lit)| {
                    let mut hit = Vec::new();
                    let mut def = Vec::new();
                    for unit in s.sources(&snap) {
                        let vis = unit.visibility(t);
                        let (bm, _) = unit.filter(path, *op, lit, &vis).unwrap();
                        let cm = unit.comparable(path, *op, lit, &vis).unwrap();
                        for o in bm.and(&vis).iter() {
                            hit.push(unit.key(o).unwrap_or("").to_string());
                        }
                        for o in cm.and(&vis).iter() {
                            def.push(unit.key(o).unwrap_or("").to_string());
                        }
                    }
                    hit.sort();
                    def.sort();
                    (hit, def)
                })
                .collect()
        };

        let before = read(&s, t);
        s.flush().unwrap();
        let seg = &s.segments[0].segment;
        for p in ["n", "tags", "s"] {
            assert!(seg.is_shredded(p), "`{p}` was not shredded into the segment");
        }
        // And the strays were declined by the column rather than coerced.
        assert!(seg.column("n").unwrap().unwrap().mismatch.popcount() >= 2, "n mismatches");
        assert!(seg.column("s").unwrap().unwrap().mismatch.popcount() >= 1, "s mismatches");
        let after = read(&s, s.clock.peek());

        for (i, (b, a)) in before.iter().zip(after.iter()).enumerate() {
            let (path, op, lit) = &probes[i];
            assert_eq!(
                b.0,
                a.0,
                "`{path} {} {}`: {} rows unshredded, {} shredded",
                op.name(),
                crate::json::to_string(lit),
                b.0.len(),
                a.0.len()
            );
            assert_eq!(
                b.1,
                a.1,
                "`{path} {} {}` definedness disagrees",
                op.name(),
                crate::json::to_string(lit)
            );
        }
    }

    /// A collection small enough that every column holds a handful of values.
    #[test]
    fn a_tiny_collection_can_be_sealed() {
        let mut s = shard();
        for i in 0..3 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), 3);
    }

    #[test]
    fn a_zero_filled_wal_tail_ends_replay_instead_of_panicking() {
        let dir = std::env::temp_dir().join(format!("celastro-tail-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        {
            let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
            s.attach_dir(&dir).unwrap();
            for i in 0..5 {
                s.insert(doc(i)).unwrap();
            }
            s.wal.as_mut().unwrap().sync().unwrap();
        }
        let mut b = fs::read(dir.join("wal.log")).unwrap();
        b.extend_from_slice(&[0u8; 32]); // what a crash leaves behind
        fs::write(dir.join("wal.log"), &b).unwrap();
        let s2 = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s2.num_docs(MAX_TS), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_catalog_whose_seal_fails_leaves_the_shard_still_flushable() {
        let mut s = shard();
        for i in 0..10 {
            s.insert(doc(i)).unwrap();
        }
        // Every document in the memtable carries a four-dimensional `emb`, so
        // sealing it under an index that declares eight fails.
        let mut wrong = coll();
        wrong.indexes.retain(|i| i.path != "emb");
        wrong.indexes.push(IndexDef::new(
            "e",
            "emb",
            IndexKind::Vector { dims: 8, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        assert!(s.adopt_catalog(wrong).is_err());
        // The definition it could not seal must not be left behind: with it in
        // place every later flush fails the same way, so the memtable can never
        // be drained, the WAL never truncated, and the shard never sealed.
        assert!(s.flush().unwrap().is_some());
        assert_eq!(s.num_docs(s.clock.peek()), 10);
    }

    #[test]
    fn a_refused_archived_segment_fails_term_stats_instead_of_inflating_idf() {
        let mgr = Arc::new(ResidencyManager::new(crate::residency::ResidencyOpts {
            archived_access: crate::residency::ArchivedAccess::Refuse,
            ..Default::default()
        }));
        let mut opts = ShardOpts::default();
        opts.residency = Some(mgr);
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), opts);
        for i in 0..10 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        // The segment's bytes now live only in the archive, and this node is
        // configured to refuse archived reads.
        let gone = std::env::temp_dir().join("celastro-no-such-archive.seg");
        s.segments[0].segment.set_source(crate::segment::SegmentSource::Archive(gone));
        let e = s.term_stats("body", &["vectors".to_string()], MAX_TS).unwrap_err().to_string();
        // Swallowing this would make the segment look like a path with no text
        // index: its documents still count towards `ndocs`, so the IDF is
        // inflated and the query is silently mis-ranked.
        assert!(e.contains("archived"), "the refusal has to reach the caller: {e}");
    }
}
