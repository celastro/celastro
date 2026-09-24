//! A shard: one memtable, a set of immutable segments, and the delete logs
//! that sit beside them.
//!
//! In the distributed build this is a tablet — one Raft group, its own
//! documents, its own inverted index, its own vector index. **Reads fan out,
//! writes do not** (§3.1): a write touches one shard and commits through that
//! shard's Raft group, with no distributed transaction on the ingest path. That
//! is why `Shard::insert` can commit the document, its columns, its postings,
//! its vector entry and the delete-log entry for the superseded version as one
//! atomic append (§6).
//!
//! This build has one Raft group of one member and calls the append a WAL
//! write, but the shape is the shape: everything that must commit together goes
//! into one record.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crate::bitmap::Bitmap;
use crate::catalog::{Collection, PathTally};
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

/// Re-exported under the name the tests and [`crate::engine`] use it by: the
/// probe lives inside [`durable`] because the code that may write a sync
/// record has to be the code that makes the syscall.
#[cfg(test)]
pub(crate) use durable::probe as durability_probe;
#[cfg(test)]
use durable::probe::Op;
pub(crate) use durable::sync_dir;
use durable::sync_file;

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
    /// The ordinals the shard's key range keeps, `[a, b)` of a segment
    /// sorted by key, once the range shrank under a split; `None` keeps
    /// every one. What a read of the shard that shrank never sees, and
    /// what the next compaction drops.
    mask: RwLock<Option<(u32, u32)>>,
}

impl SegmentHandle {
    pub(crate) fn new(
        segment: Segment,
        deletes: DeleteLog,
        path: Option<PathBuf>,
    ) -> Arc<SegmentHandle> {
        Arc::new(SegmentHandle {
            segment: Arc::new(segment),
            deletes: RwLock::new(deletes),
            epoch: AtomicU64::new(0),
            vis: VisibilityCache::default(),
            path: RwLock::new(path),
            mask: RwLock::new(None),
        })
    }

    /// Keep only the ordinals in `[a, b)`, or every one: the visibility
    /// cache is invalidated with the epoch, as a delete invalidates it.
    pub(crate) fn set_mask(&self, mask: Option<(u32, u32)>) {
        *self.mask.write().unwrap() = mask;
        self.epoch.fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub(crate) fn mask(&self) -> Option<(u32, u32)> {
        *self.mask.read().unwrap()
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

    pub(crate) fn mark_deleted(&self, ord: u32, ts: Timestamp) {
        self.deletes.write().unwrap().mark(ord, ts);
        self.epoch.fetch_add(1, AtomicOrdering::AcqRel);
        self.vis.clear();
    }

    /// Rows a rewrite would drop: deleted or superseded at `t`, and the
    /// ones outside the shard's range since a split.
    pub fn dead_count(&self, t: Timestamp) -> usize {
        let masked = match self.mask() {
            Some((a, b)) => self.segment.num_docs().saturating_sub((b - a) as usize),
            None => 0,
        };
        self.deletes.read().unwrap().dead_count(t) + masked
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
            let mut bm = visibility(&self.segment.ordinals, &d, t);
            if let Some((a, b)) = self.mask() {
                bm.and_inplace(&Bitmap::range(self.segment.num_docs(), a as usize, b as usize));
            }
            bm
        })
    }

    pub fn is_visible(&self, ord: u32, t: Timestamp) -> bool {
        self.segment.ordinals.commit_ts.get(ord as usize).map(|c| *c <= t).unwrap_or(false)
            && !self.deletes.read().unwrap().is_deleted_at(ord, t)
            && self.mask().map_or(true, |(a, b)| ord >= a && ord < b)
    }

    /// `None` for a log with nothing in it. An empty log is never published:
    /// absent is how a reopen learns a segment has no deletions, and the
    /// encoding is not empty for an empty log -- it carries its frame -- so
    /// the decision is taken on the entries, not on the bytes.
    pub(crate) fn encode_deletes(&self) -> Option<Vec<u8>> {
        let d = self.deletes.read().unwrap();
        if d.is_empty() {
            None
        } else {
            Some(d.encode())
        }
    }
}

/// The strings at one path of one unit, by ordinal. See
/// [`Searchable::strings`].
pub struct StrReader<'a> {
    unit: &'a Searchable<'a>,
    path: &'a str,
    col: Option<Arc<crate::column::Column>>,
}

impl StrReader<'_> {
    /// The string at the ordinal, or `None` when the document has no string
    /// there. From the column when the unit has one and the value is of its
    /// type, and from the document otherwise.
    pub fn at(&self, ord: u32) -> Result<Option<String>> {
        if let Some(c) = &self.col {
            match c.get(ord) {
                Value::Str(s) => return Ok(Some(s)),
                // A value the column declined is in the document.
                Value::Null if c.mismatch.get(ord as usize) => {}
                _ => return Ok(None),
            }
        }
        Ok(self.unit.document(ord)?.path(self.path).and_then(|v| v.as_str()).map(str::to_string))
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
                    if *ts <= t && m.in_range(&m.docs[i].sort_key) {
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

    /// A reader of the strings at `path`, by ordinal, for a caller that
    /// reads many: the shredded column is resolved once here, because
    /// resolving it is a residency acquisition with a lock and an
    /// allocation, and a walk reads two strings per edge over tens of
    /// thousands of edges.
    pub fn strings(&'a self, path: &'a str) -> Result<StrReader<'a>> {
        let col = match self {
            Searchable::Seg(h) => h.segment.column(path)?,
            Searchable::Mem(_) => None,
        };
        Ok(StrReader { unit: self, path, col })
    }

    pub fn document(&self, ord: u32) -> Result<Value> {
        #[cfg(test)]
        DOCUMENTS_DECODED.with(|c| c.set(c.get() + 1));
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

/// How many documents `Searchable::document` has produced on this thread,
/// for the tests that pin what a scan does NOT decode. Counted here because
/// this is the one door a query's payloads come through; a scan that reached
/// a segment's decoder by another path would not be counted, which is the
/// residual. Per thread, because the test harness runs tests in parallel
/// and a statement runs on the thread that issued it: a process-wide count
/// measured across a query also counted every other test's decodes, and
/// failed one run in a few once enough tests decoded documents.
#[cfg(test)]
pub(crate) fn documents_decoded() -> u64 {
    DOCUMENTS_DECODED.with(|c| c.get())
}

#[cfg(test)]
thread_local! {
    static DOCUMENTS_DECODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
    /// The newest delete a compaction here has forgotten (`Shard::catchup_floor`),
    /// carried so a reopened holder still knows which followers must start
    /// from nothing. Trailing, so a manifest without it reads as zero.
    pub catchup_floor: Timestamp,
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
        put_u64(&mut out, self.catchup_floor);
        out
    }

    pub fn decode(b: &[u8]) -> Result<Manifest> {
        let bad = || Error::Storage("manifest: truncated".into());
        let mut i = 0usize;
        let version = get_u64(b, &mut i).ok_or_else(bad)?;
        let next_segment_id = get_u64(b, &mut i).ok_or_else(bad)?;
        let n = crate::codec::get_count(b, &mut i).ok_or_else(bad)?;
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
        // Absent in manifests written before 0.60.0.
        let catchup_floor = get_u64(b, &mut i).unwrap_or(0);
        Ok(Manifest { version, segments, next_segment_id, catchup_floor })
    }
}

// --------------------------------------------------------------------------
// Write-ahead log
// --------------------------------------------------------------------------

/// Write durably: temp file, fsync, rename, fsync the directory the rename
/// landed in. Exposed because every file the database cannot afford to find
/// half-written goes through it.
///
/// The last step is the one that is easy to leave out and impossible to notice
/// missing. `fs::rename` is atomic with respect to a *reader*, but the
/// directory entry it rewrites is dirty metadata like any other: a crash can
/// leave the new bytes durable and the name still resolving to the old file, or
/// to nothing at all. Syncing the temp file makes the contents durable; only
/// syncing the directory makes the publication durable, and a file nothing
/// names is not published.
///
/// The contents half holds on every target. The publication half is a POSIX
/// guarantee and only that: fsyncing a directory means opening it, which
/// Windows refuses, so there the bytes of the file are made durable and the
/// durability of the new directory entry is left to the platform. That is a
/// real difference in what this function promises, and it is written here
/// rather than left to be discovered.
/// Write `plain` as the content of `path`: framed under the cipher when
/// there is one, as it is otherwise. Returns the bytes that reached the
/// file, which is what `still_published` compares against later. The
/// copy paths (a backup, a move, an archive put) do not come here: they
/// move bytes as they lie.
pub(crate) fn write_content(
    cipher: &crate::cipher::Shared,
    id: &str,
    path: &Path,
    plain: &[u8],
) -> Result<Vec<u8>> {
    let bytes = match cipher {
        Some(c) => c.seal_file(id, plain)?,
        None => plain.to_vec(),
    };
    atomic_write(path, &bytes)?;
    Ok(bytes)
}

/// `write_content` without the directory fsync, as `publish` is to
/// `atomic_write`.
pub(crate) fn publish_content(
    cipher: &crate::cipher::Shared,
    id: &str,
    path: &Path,
    plain: &[u8],
) -> Result<Vec<u8>> {
    let bytes = match cipher {
        Some(c) => c.seal_file(id, plain)?,
        None => plain.to_vec(),
    };
    publish(path, &bytes)?;
    Ok(bytes)
}

/// The content of `path`, opened under the cipher when there is one;
/// `None` when the file is absent.
pub(crate) fn read_content(
    cipher: &crate::cipher::Shared,
    id: &str,
    path: &Path,
) -> Result<Option<Vec<u8>>> {
    let Some(bytes) = read_optional(path)? else { return Ok(None) };
    match cipher {
        Some(c) => c
            .open_file(id, &bytes)
            .map(Some)
            .map_err(|e| Error::Storage(format!("{}: {e}", path.display()))),
        None => Ok(Some(bytes)),
    }
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    publish(path, bytes)?;
    sync_dir_of(path)
}

/// `atomic_write` without the directory fsync: temp file, fsync, rename.
///
/// For a caller publishing several files into the SAME directory, which can
/// make all of their names durable with one fsync afterwards instead of one
/// each. It is not a weaker `atomic_write` -- a caller of this owes the
/// directory fsync before it may treat any of the names as published, and
/// `Shard::persist_manifest` is the only caller.
fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        sync_file(&f, &tmp)?;
    }
    fs::rename(&tmp, path)?;
    #[cfg(test)]
    durability_probe::note_rename(path);
    Ok(())
}

/// Is `path` still the file this process published `bytes` to?
///
/// The skip in `Shard::persist_manifest` and the matching one on the catalog
/// rest on this process being the only writer of these files, which is true --
/// but "the file is still there" is not something to take on trust when the
/// answer decides whether a save may report success. A database directory that
/// was removed underneath a running process has to keep failing loudly rather
/// than being quietly agreed with.
///
/// Byte for byte, not by length. The event this check exists for -- one of
/// these files being restored from a snapshot, or otherwise written by
/// something that is not this process -- routinely leaves a file of exactly the
/// length that was published and different contents: two manifests over the
/// same segment count, two delete logs over the same number of entries. A skip
/// that accepted one of those would leave the stale file in place for good, and
/// a reopen would read it instead of the state this shard is in. These files
/// are kilobytes and the bytes to compare are already in hand, so this is one
/// page-cache read against the two fsyncs and a rename it is deciding whether
/// to skip.
pub(crate) fn still_published(path: &Path, bytes: &[u8]) -> bool {
    fs::read(path).map(|b| b == bytes).unwrap_or(false)
}

/// Read a file that is allowed not to exist: `None` when it is genuinely
/// absent, its bytes when it is there, and an error naming it for anything
/// else.
///
/// The distinction is the whole point. CATALOG, MANIFEST and every delete log
/// used to be read with `if let Ok(b)`, which turned EIO and EACCES into "the
/// file was never there": a database with no collections, a shard with no
/// segments, a segment with no deletions -- each opened successfully, and the
/// next persist wrote that emptiness over the real file. The same code treated
/// a short or checksum-mismatched file as fatal two lines later, so a file
/// that could not be read at all was the one failure it believed. `NotFound`
/// is the only kind that means absent; everything else is reported, as
/// `Wal::replay` already did.
pub(crate) fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::Storage(format!("{}: {e}", path.display()))),
    }
}

/// The three fsyncs on the write path, and the only code in the crate that can
/// say one happened.
///
/// An fsync leaves nothing behind that a later read can find, so the only way a
/// test can learn that one happened is a record the code writes as it happens
/// -- and a record a call site can write is a record a call site can write
/// while syncing nothing. That is not hypothetical. The mutation that survived
/// the previous round of this work was a call site that performed its rename
/// correctly, wrote the probe's `DirSync` itself, called no fsync at all, and
/// passed the whole suite: in a release build, a database with no directory
/// fsync on the write path and no test able to say so.
///
/// So the syscalls and the records of them live in here together, and
/// [`probe::note_sync`] -- the only function that can put `DirSync`,
/// `TempSync` or `WalSync` into the log -- is private to this module. A call
/// site elsewhere cannot claim a sync it did not make, because it cannot write
/// the claim at all: it has to come through one of these three functions. What
/// those three do is pinned from outside, by
/// `the_three_fsyncs_are_syscalls_and_not_bookkeeping`, which hands each of
/// them a descriptor the kernel refuses to sync and requires the refusal to
/// come back, and by the injected-failure tests, which arm [`probe::check`] --
/// private here too -- and follow the `Err` out to the caller.
///
/// The limit of that argument belongs here rather than being left to be
/// discovered. It holds against a call site that stops calling, and against a
/// helper that stops syncing. It does not hold against an edit made INSIDE
/// this module: a fourth function here that recorded without syncing would be
/// believed. That is why the module is three functions long, and why the three
/// are exactly the ones the syscall proof names.
pub(crate) mod durable {
    use crate::error::Result;
    use std::fs;
    use std::path::Path;

    /// fsync a file's contents and metadata, and record that it happened.
    ///
    /// The syscall and the record of it are one statement at the call site,
    /// deliberately. A counter next to a call is satisfied by a counter that
    /// moves with no call at all -- which is how this crate came to have a WAL
    /// sync no test could tell apart from bookkeeping -- so the only line that
    /// can be deleted to silence the probe is the line that does the work.
    // `path` is read only by the probe, so it is unused once the probe is not
    // compiled -- which is the point: the seam costs a release build nothing.
    #[cfg_attr(not(test), allow(unused_variables))]
    pub(crate) fn sync_file(f: &fs::File, path: &Path) -> Result<()> {
        #[cfg(test)]
        probe::check(probe::Op::TempSync, path)?;
        f.sync_all()?;
        #[cfg(test)]
        probe::note_sync(probe::Op::TempSync, path);
        Ok(())
    }

    /// fdatasync a log's contents, which for the WAL is all of what matters:
    /// the file's size is part of its data, and nothing else about the inode
    /// is. It cannot create the directory entry that names the file --
    /// [`super::Shard::attach_dir`] does that, by fsyncing the directory the
    /// log was created in.
    #[cfg_attr(not(test), allow(unused_variables))]
    pub(crate) fn sync_data(f: &fs::File, path: &Path) -> Result<()> {
        #[cfg(test)]
        probe::check(probe::Op::WalSync, path)?;
        f.sync_data()?;
        // Recorded after the call returns `Ok`, never before: a sync that
        // failed made nothing durable and must not be able to satisfy an
        // assertion that one did.
        #[cfg(test)]
        probe::note_sync(probe::Op::WalSync, path);
        Ok(())
    }

    /// fsync `dir` itself, which is what makes the names it holds durable: a
    /// rename's new entry, a freshly created subdirectory, a WAL that has just
    /// been created. fdatasync on a file flushes that file's data and size and
    /// cannot create the directory entry that reaches it, so a file whose
    /// directory was never synced is a file nothing names after a crash.
    ///
    /// `pub(crate)` because directory *creation* has to be made durable too,
    /// and the code that creates the collection and shard directories lives in
    /// [`crate::engine`].
    ///
    /// Opening a directory in order to fsync it is a POSIX contract and only
    /// that: on Windows `File::open` on a directory fails, so doing it
    /// unconditionally would make every [`super::atomic_write`] return `Err`
    /// *after* the rename had already happened, and no on-disk database could
    /// be created there at all. The publication of a rename is therefore
    /// durable on unix and best-effort elsewhere. That is a real difference and
    /// it is written down rather than left to be found: the bytes of every file
    /// are fsynced on every target, and it is the directory entry -- and the
    /// crash guarantee that rests on it -- that unix gets and Windows does not.
    #[cfg(unix)]
    pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
        #[cfg(test)]
        probe::check(probe::Op::DirSync, dir)?;
        fs::File::open(dir)?.sync_all()?;
        #[cfg(test)]
        probe::note_sync(probe::Op::DirSync, dir);
        Ok(())
    }

    #[cfg(not(unix))]
    pub(crate) fn sync_dir(_dir: &Path) -> Result<()> {
        Ok(())
    }

    /// A test-only, ordered log of the durability operations on the write path,
    /// and the switch that makes one of them fail.
    ///
    /// Recording *that* an operation happened is not enough on its own: "the
    /// directory was synced" and "the directory was synced after the rename"
    /// are different claims, and only the second one is durability. A probe
    /// that cannot tell them apart passes on a publication that syncs the
    /// directory first and leaves the rename to be lost, which is the defect
    /// the directory fsync was added for. So the events are ordered, each
    /// carries the path it acted on, and the tests assert positions rather than
    /// presence.
    ///
    /// Who may write an event is the other half, and it is why this module is
    /// nested inside [`durable`] rather than sitting beside it. See that
    /// module's own comment: [`note_sync`] and [`check`] are private to it, so
    /// the three fsync records can only be written by the three fsyncs, while
    /// the events a call site DOES write for itself -- an append, a rename, a
    /// truncation, the creation of a log -- are all operations that leave
    /// something behind for the same test to read off the disk.
    ///
    /// [`check`] is the other half of the failure story: a comment saying a
    /// failed sync is reported to the caller is a claim about an error path,
    /// and an error path no test ever enters is decoration. Arming an operation
    /// makes it fail exactly where the real one fails -- immediately before the
    /// syscall, so nothing it was meant to make durable has happened -- and the
    /// test follows the `Err` out to the caller and checks what was left behind.
    ///
    /// Thread-local rather than process-wide because the test binary runs tests
    /// in parallel threads, and a shared log would let one test's syncs satisfy
    /// another test's assertion.
    #[cfg(test)]
    pub(crate) mod probe {
        use crate::error::{Error, Result};
        use std::cell::RefCell;
        use std::path::{Path, PathBuf};

        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        pub(crate) enum Op {
            /// A WAL file was opened, creating it if it was not there. The
            /// event the directory fsync that publishes its name comes after.
            WalCreate,
            /// A WAL record reached `write_all`.
            WalAppend,
            /// A WAL record was fdatasync'd.
            WalSync,
            /// The WAL was emptied, forgetting every record in it.
            WalTruncate,
            /// A temp file was fsynced.
            TempSync,
            /// A temp file was renamed over the name it publishes. The path is
            /// the destination, not the temp file.
            Rename,
            /// A directory was fsynced.
            DirSync,
        }

        #[derive(Clone, Debug)]
        pub(crate) struct Event {
            pub(crate) op: Op,
            pub(crate) path: PathBuf,
        }

        /// What the probe saw, oldest first, with the questions the tests ask
        /// of it.
        #[derive(Debug)]
        pub(crate) struct Events(Vec<Event>);

        impl Events {
            /// Where `op` first happened to `path`, or `None` if it never did.
            /// Comparing two of these is how a test asserts an order.
            pub(crate) fn at(&self, op: Op, path: &Path) -> Option<usize> {
                self.0.iter().position(|e| e.op == op && e.path == path)
            }

            /// Where `op` first happened to `path` after position `after`.
            /// "The directory was synced" is not the claim; "the directory was
            /// synced after the rename" is, and a directory that was synced for
            /// some earlier reason must not be able to answer it.
            pub(crate) fn at_after(&self, op: Op, path: &Path, after: usize) -> Option<usize> {
                self.0
                    .iter()
                    .enumerate()
                    .position(|(i, e)| i > after && e.op == op && e.path == path)
            }

            /// `a` happened, `b` happened, and `a` came first.
            ///
            /// Not `at(a) < at(b)`: `None < Some(_)` is true, so an operation
            /// that never happened at all would satisfy an assertion that it
            /// came first. A durability test whose assertion is satisfied by
            /// the absence of the operation it is about is the whole failure
            /// this module exists to stop being possible.
            pub(crate) fn ordered(&self, a: (Op, &Path), b: (Op, &Path)) -> bool {
                match (self.at(a.0, a.1), self.at(b.0, b.1)) {
                    (Some(x), Some(y)) => x < y,
                    _ => false,
                }
            }

            /// How many times `op` happened to `path`.
            pub(crate) fn count(&self, op: Op, path: &Path) -> usize {
                self.0.iter().filter(|e| e.op == op && e.path == path).count()
            }

            /// Every path `op` happened to, in order.
            pub(crate) fn paths(&self, op: Op) -> Vec<PathBuf> {
                self.0.iter().filter(|e| e.op == op).map(|e| e.path.clone()).collect()
            }

            /// Every rename in the log that no fsync of the directory it landed
            /// in followed -- which is to say, every publication whose bytes are
            /// durable and whose NAME is not.
            ///
            /// The invariant, rather than a list of call sites, and that is the
            /// point of it. Each of `atomic_write`, `publish` + a batched
            /// `sync_dir`, and the tablet map's own publication had a test of
            /// its own; the call site added after them did not, and published
            /// a `.seg` that nothing fsynced a directory for -- a segment named
            /// by a durable MANIFEST whose directory entry a crash could take,
            /// which reopens as `segment ... named by the manifest is missing`.
            /// A publication written tomorrow is covered by this the day it is
            /// written, without anyone remembering to add anything.
            pub(crate) fn unpublished_renames(&self) -> Vec<PathBuf> {
                self.0
                    .iter()
                    .enumerate()
                    .filter(|(i, e)| {
                        e.op == Op::Rename
                            && self.at_after(Op::DirSync, parent_of(&e.path), *i).is_none()
                    })
                    .map(|(_, e)| e.path.clone())
                    .collect()
            }
        }

        /// The directory a path names something in, the way `sync_dir_of`
        /// resolves it: `Path::parent` of a bare file name is `Some("")`.
        fn parent_of(path: &Path) -> &Path {
            match path.parent() {
                Some(p) if !p.as_os_str().is_empty() => p,
                _ => Path::new("."),
            }
        }

        thread_local! {
            static SEEN: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
            static ARMED: RefCell<Option<(Op, PathBuf)>> = const { RefCell::new(None) };
        }

        /// Start recording on this thread, discarding anything already recorded.
        pub(crate) fn start() {
            SEEN.with(|s| *s.borrow_mut() = Some(Vec::new()));
        }

        /// Stop recording and return what was seen, oldest first.
        pub(crate) fn take() -> Events {
            Events(SEEN.with(|s| s.borrow_mut().take()).unwrap_or_default())
        }

        fn note(op: Op, path: &Path) {
            SEEN.with(|s| {
                if let Some(seen) = s.borrow_mut().as_mut() {
                    seen.push(Event { op, path: path.to_path_buf() });
                }
            });
            HOOK.with(|h| {
                if let Some(f) = h.borrow_mut().as_mut() {
                    f(op, path);
                }
            });
        }

        thread_local! {
            static SLOW: RefCell<Option<(Op, std::time::Duration)>> = const { RefCell::new(None) };
        }

        /// Make every `op` on this thread take `d` longer, as a slow disk
        /// does: the sleep is where the syscall's wait would be. `None`
        /// puts the disk back.
        pub(crate) fn slow(op: Option<(Op, std::time::Duration)>) {
            SLOW.with(|s| *s.borrow_mut() = op);
        }

        type Hook = Box<dyn FnMut(Op, &Path)>;

        thread_local! {
            static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
        }

        /// Run `f` on this thread at every event, just after the operation
        /// it records: the moment a crash would leave the disk holding it.
        /// The crash sweep copies the directory there. `f` must not touch a
        /// shard's files through this crate, or it notes events of its own.
        pub(crate) fn on_event(f: Option<Hook>) {
            HOOK.with(|h| *h.borrow_mut() = f);
        }

        /// Record one of the three fsyncs. Private to [`super`], which is the
        /// whole of the seam's integrity: a call site that wanted to claim a
        /// directory fsync it never made cannot name this function.
        pub(super) fn note_sync(op: Op, path: &Path) {
            note(op, path);
        }

        // The events a call site writes for itself. Each of them leaves
        // something behind that the same test can read off the disk -- the
        // renamed file is there, the appended record replays, the truncated log
        // is empty -- so a forged one is caught by the assertion next to it,
        // and the record only has to be honest about WHEN it happened.
        pub(crate) fn note_create(path: &Path) {
            note(Op::WalCreate, path);
        }

        pub(crate) fn note_append(path: &Path) {
            note(Op::WalAppend, path);
        }

        pub(crate) fn note_truncate(path: &Path) {
            note(Op::WalTruncate, path);
        }

        pub(crate) fn note_rename(path: &Path) {
            note(Op::Rename, path);
        }

        /// Make the next `op` on `path` fail instead of happening. One shot:
        /// the arming is consumed by the operation it stops, so a test can
        /// watch what the caller does about it and then let the next attempt
        /// through.
        pub(crate) fn fail_next(op: Op, path: &Path) {
            ARMED.with(|a| *a.borrow_mut() = Some((op, path.to_path_buf())));
            SKIP.with(|s| *s.borrow_mut() = 0);
        }

        /// Fail the operation after `skip` matching ones have gone through:
        /// the third record of a batch, say.
        pub(crate) fn fail_after(op: Op, path: &Path, skip: usize) {
            fail_next(op, path);
            SKIP.with(|s| *s.borrow_mut() = skip);
        }

        thread_local! {
            static SKIP: RefCell<usize> = const { RefCell::new(0) };
        }

        /// Called immediately before the syscall, and returns the injected
        /// error in its place. Private to [`super`] for the same reason
        /// [`note_sync`] is: a call site that could arm and answer its own
        /// failures could pass the tests that exist to follow a real one out.
        pub(crate) fn check(op: Op, path: &Path) -> Result<()> {
            if let Some(d) = SLOW.with(|s| s.borrow().filter(|(o, _)| *o == op).map(|(_, d)| d)) {
                std::thread::sleep(d);
            }
            let armed = ARMED.with(|a| {
                let hit = matches!(&*a.borrow(), Some((o, p)) if *o == op && p == path);
                if hit {
                    let skipping = SKIP.with(|s| {
                        let mut s = s.borrow_mut();
                        if *s > 0 {
                            *s -= 1;
                            true
                        } else {
                            false
                        }
                    });
                    if skipping {
                        return false;
                    }
                    *a.borrow_mut() = None;
                }
                hit
            });
            if armed {
                return Err(Error::Io(std::io::Error::other(format!(
                    "injected {op:?} failure on {}",
                    path.display()
                ))));
            }
            Ok(())
        }
    }
}

/// fsync the directory `path` sits in, so the name is as durable as the bytes.
fn sync_dir_of(path: &Path) -> Result<()> {
    // `Path::parent` of a bare file name is `Some("")`, which opens as nothing.
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    sync_dir(dir)
}

/// The lowest segment id no file in `dir` already claims.
///
/// Every file this shard writes for a segment is named `{id:016x}` with an
/// extension: the segment, its delete log, and the temp file either is renamed
/// from. So the id of a file that was written under an id is recoverable from
/// its name whatever stopped before the manifest learned of it, and anything
/// whose name does not parse — a `MANIFEST`, a file somebody else left here —
/// claims nothing.
///
/// All three directories, because a segment's file is in `segments/` only
/// until it is tiered and in `archive/` afterwards, and a delete log is in
/// neither. `deletes/` is the one that carries the loss: an id whose `.seg`
/// has been removed but whose `.dlog` has not is still an id that must never
/// be handed out.
///
/// The `.tmp` leg is defence in depth rather than a live property, and is
/// recorded here rather than tested because a test for it would assert
/// nothing. A `.tmp` in `segments/` means the rename never happened, so that
/// id has no `.seg` — and no `.dlog` either, since a delete log is only ever
/// written for a segment whose own file was renamed first. Reusing the id
/// therefore overwrites a temp file and loses nothing. There is no path to a
/// loss without changing that write order, which is the change that would make
/// this line load-bearing; it is cheaper to keep it than to notice then.
fn first_unused_segment_id(dir: &Path) -> Result<u64> {
    let mut next = 1u64;
    for sub in ["segments", "archive", "deletes"] {
        let d = dir.join(sub);
        // A listing that fails is not a directory with nothing in it. This is
        // the guard against reusing an id whose delete log is still on the
        // disk, and an EACCES read as "empty" would hand that id out again --
        // the read-as-absent shape that `read_optional` closes for the files,
        // closed here for the directories that name them. Only `NotFound`
        // means empty. Untested, because the one injection is a permission
        // bit the test would have to be unprivileged to rely on.
        let entries = match fs::read_dir(&d) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::Storage(format!("{}: {e}", d.display()))),
        };
        for e in entries {
            let e = e.map_err(|e| Error::Storage(format!("{}: {e}", d.display())))?;
            let name = e.file_name();
            let Some(stem) = name.to_str().and_then(|n| n.split('.').next()) else { continue };
            if let Ok(id) = u64::from_str_radix(stem, 16) {
                next = next.max(id.saturating_add(1));
            }
        }
    }
    Ok(next)
}

/// Unlink the files of segment ids the manifest does not name.
///
/// A publication that fails leaves the segments it was about to install on the
/// disk: their `.seg`, and their `.dlog` when the failure was at or after
/// MANIFEST's own write. Nothing else ever reclaims them. `Shard::sweep_retired`
/// unlinks what the shard is still holding, and a handle that was never
/// installed was never on that list; `first_unused_segment_id` then guarantees
/// the ids are not handed out again, so they are not even overwritten. Left
/// alone that is one dead segment per failed seal or compaction for the life of
/// the database, and the case that produces them in bulk is a volume that is
/// failing because it is full, where every retry writes another output set
/// roughly the size of its inputs.
///
/// A reopen is the moment this is safe and the failure path is not. The
/// manifest has just been installed and names every segment that can be
/// reached, no reader exists yet to hold one, and — unlike the failure path —
/// a crash comes back through here too, which is the only way the orphans a
/// crash leaves are ever seen again.
///
/// The price of waiting for that moment is stated rather than hidden: a
/// process that runs for a month and fails a publication a day carries those
/// files for the month. This bounds the leak by the lifetime of a process
/// instead of the lifetime of the database, which is the most a reclamation
/// that needs a quiet moment can offer; unlinking them where they were
/// abandoned would need a promise that no reader is mid-open on the id, and
/// would still do nothing for the crash.
///
/// It runs only where a manifest was read, which is why the call sits inside
/// that branch rather than after it. A directory with no MANIFEST is either
/// brand new or has lost the one file that says what is live, and reading the
/// second as "nothing is live" would turn a directory a human could still
/// recover into an empty one.
///
/// It must run AFTER the id guard, and that is why the guard is in
/// `Shard::attach_dir`: the evidence this unlinks is the same evidence
/// `first_unused_segment_id` reads, so a reclamation that went first would
/// hand the ids it just freed straight back out. `attach_dir` runs before any
/// manifest is read, so the counter is already past every id the directory
/// held by the time anything here is unlinked.
///
/// Within a pass the order does not matter, and neither does finishing. An
/// interrupted pass leaves whichever file it had not reached yet in one of the
/// three directories that guard reads, so the next open refuses that id again
/// — and once BOTH files of an id are gone the id is genuinely free, which is
/// the only reason handing it out afterwards is safe. An unlink that fails is
/// dropped: the orphan survives to be reclaimed by the next reopen, and a
/// reopen must not fail because tidying did.
fn reclaim_orphans(dir: &Path, live: &[u64]) {
    for sub in ["segments", "archive", "deletes"] {
        let Ok(entries) = fs::read_dir(dir.join(sub)) else { continue };
        for e in entries.flatten() {
            let name = e.file_name();
            let Some(stem) = name.to_str().and_then(|n| n.split('.').next()) else { continue };
            let Ok(id) = u64::from_str_radix(stem, 16) else { continue };
            if !live.contains(&id) {
                let _ = fs::remove_file(e.path());
            }
        }
    }
}

pub(crate) const WAL_INSERT: u8 = 1;
pub(crate) const WAL_DELETE: u8 = 2;
/// A followed copy's mark: where the holder's log stood when this batch
/// was applied. Never in a held shard's log.
pub(crate) const WAL_SHIP_MARK: u8 = 3;

/// A logged record as the shipper carries it.
fn ship_item(r: &WalRecord) -> crate::replication::ShipItem {
    crate::replication::ShipItem {
        kind: if r.kind == WAL_DELETE {
            crate::replication::SHIP_DELETE
        } else {
            crate::replication::SHIP_INSERT
        },
        key: r.key.clone(),
        ts: r.ts,
        doc: r.doc.clone(),
    }
}
// Kind 3 was reserved for a seal marker and is never written; a record
// carrying it is skipped by the replay like any other unknown kind.

/// One record per commit. A document, its indexes and the delete-log entry for
/// the version it supersedes are all in the same record, because they must all
/// become visible at the same instant or none of them may (§6).
pub(crate) struct WalRecord {
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

/// A position in the log a statement can be rolled back to.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalMark {
    len: u64,
    records: u64,
}

pub(crate) struct Wal {
    file: fs::File,
    path: PathBuf,
    /// The log's syncs, shared with the writers that settle theirs after
    /// the database's lock is let go: see [`LogSync`].
    group: Arc<LogSync>,
    /// Encryption at rest: each appended record is a length-prefixed frame
    /// under the log's key with its ordinal in the AAD; `records` counts
    /// the frames in the file, so the next one continues the sequence.
    cipher: crate::cipher::Shared,
    id: String,
    records: u64,
}

/// Empty `path`, and record that it happened, in one statement.
///
/// Welded together for the same reason [`durable::sync_file`] is: a record
/// next to the work is satisfied by a record with no work at all, so the only
/// line that can be deleted to silence the probe has to be the line that does
/// the job. This was the last place in the write path where it was not true --
/// `WalTruncate` was noted two statements below the opens that empty the log,
/// so deleting both opens left a seal that recorded a truncation it had not
/// performed, and every reopen from then on replayed a log that is never
/// emptied.
///
/// Unlike an fsync the work here IS observable afterwards, so the record only
/// has to be honest about WHEN: `a_seal_publishes_the_manifest_before_it_
/// empties_the_wal` reads the log's length as well as the order.
fn truncate_file(path: &Path) -> Result<fs::File> {
    let f = fs::OpenOptions::new().write(true).truncate(true).create(true).open(path)?;
    #[cfg(test)]
    durability_probe::note_truncate(path);
    Ok(f)
}

/// A write's log sync, left for its caller to settle outside the lock:
/// the log, the record count to reach, and the write's first timestamp,
/// which reads are held below until it is settled.
pub(crate) struct PendingSync {
    pub(crate) log: Arc<LogSync>,
    pub(crate) seq: u64,
    pub(crate) first: Timestamp,
}

/// Group commit on one write-ahead log (§6).
///
/// A write statement appends its records and applies them under the
/// database's exclusive lock, lets the lock go, and only then waits for the
/// log's sync -- here, with every other writer that appended meanwhile. One
/// of them makes the `fdatasync` for all, the rest wait for it, so on a slow
/// disk the writes cost one sync per group rather than one each, and a
/// reader never waits one out behind a writer. What a reader may see is
/// held below every write still waiting (`Hlc::visible`), so a row is seen
/// only once it is durable, exactly as when the sync was under the lock.
///
/// A sync that fails is not retried. What the page cache holds after a
/// failed `fdatasync` cannot be trusted to reach the disk on a second try,
/// so the log is cut back to what the last good sync covered -- a refused
/// write does not come back on a reopen -- and every write to it is
/// refused from then on: the shard needs the node restarted, and its health
/// says so.
pub(crate) struct LogSync {
    path: PathBuf,
    state: Mutex<SyncState>,
    done: Condvar,
}

pub(crate) struct SyncState {
    file: Arc<fs::File>,
    /// Bumped when the log is rotated or emptied: a sync of the file before
    /// it has nothing to say about the length of the one after.
    generation: u64,
    /// Records appended, and how many of them a sync has covered.
    appended: u64,
    synced: u64,
    syncing: bool,
    /// The log's length at the last good sync: where a failed one cuts to.
    synced_len: u64,
    /// The shortest the log was cut to by a refused statement while a sync
    /// was in the air, so that sync does not claim bytes it may not have.
    low_water: u64,
    failed: Option<String>,
    /// Syncs made, and writers they settled: the grouping, for the metrics.
    pub(crate) syncs: u64,
    pub(crate) settled: u64,
}

impl LogSync {
    fn new(path: &Path, file: fs::File, len: u64) -> LogSync {
        LogSync {
            path: path.to_path_buf(),
            state: Mutex::new(SyncState {
                file: Arc::new(file),
                generation: 0,
                appended: 0,
                synced: 0,
                syncing: false,
                synced_len: len,
                low_water: u64::MAX,
                failed: None,
                syncs: 0,
                settled: 0,
            }),
            done: Condvar::new(),
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, SyncState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn appended(&self) -> u64 {
        self.lock().appended
    }

    fn refused(path: &Path, e: &str) -> Error {
        Error::Io(std::io::Error::other(format!(
            "the write-ahead log {} failed to sync ({e}) and takes no more writes; restart \
             the node to replay it",
            path.display()
        )))
    }

    /// Why this log takes no writes, if it does not.
    pub(crate) fn failure(&self) -> Option<String> {
        self.lock().failed.clone()
    }

    /// The log's file was replaced under the database's lock, after a sync
    /// or a publication made everything before it durable.
    fn replace(&self, file: fs::File) {
        let mut st = self.lock();
        st.file = Arc::new(file);
        st.generation += 1;
        st.synced = st.appended;
        st.synced_len = 0;
        st.low_water = u64::MAX;
        drop(st);
        self.done.notify_all();
    }

    /// Return once the first `seq` records appended to this log are on the
    /// disk: by making the sync, or by waiting for the one in the air and
    /// then, if it did not cover them, making the next. Called with no
    /// database lock held.
    pub(crate) fn settle(&self, seq: u64) -> Result<()> {
        let mut st = self.lock();
        loop {
            if st.synced >= seq {
                st.settled += 1;
                return Ok(());
            }
            if let Some(e) = &st.failed {
                return Err(LogSync::refused(&self.path, e));
            }
            if st.syncing {
                st = self.done.wait(st).unwrap_or_else(|p| p.into_inner());
                continue;
            }
            st.syncing = true;
            st.low_water = u64::MAX;
            let (target, file, generation) = (st.appended, st.file.clone(), st.generation);
            let len = file.metadata().map(|m| m.len());
            drop(st);
            let r = len
                .map_err(Error::from)
                .and_then(|len| durable::sync_data(&file, &self.path).map(|_| len));
            st = self.lock();
            st.syncing = false;
            match r {
                Ok(len) => {
                    st.synced = st.synced.max(target);
                    st.syncs += 1;
                    if st.generation == generation {
                        st.synced_len = len.min(st.low_water);
                    }
                }
                Err(e) => {
                    st.failed = Some(e.to_string());
                    if st.generation == generation {
                        let _ = st.file.set_len(st.synced_len);
                    }
                }
            }
            self.done.notify_all();
        }
    }
}

impl Wal {
    /// Open the log, creating it if this is a fresh shard directory.
    ///
    /// The creation is recorded because it is the event the directory fsync in
    /// `Shard::attach_dir` has to come AFTER. A directory fsync above this
    /// call is not a missing fsync -- the syscall count is the same and the
    /// event log still reads `DirSync(dir)` before `WalSync(wal.log)` -- it is
    /// a directory made durable before it named the log, so the first
    /// acknowledged insert is fdatasync'd into a file whose name a crash still
    /// takes. Recording the creation is what lets a test tell those apart.
    pub(crate) fn open(path: &Path, cipher: crate::cipher::Shared, id: String) -> Result<Wal> {
        let file = fs::OpenOptions::new().create(true).append(true).read(true).open(path)?;
        #[cfg(test)]
        durability_probe::note_create(path);
        let records = match &cipher {
            Some(c) => match read_optional(path)? {
                Some(b) => c.open_records(&id, &b).len() as u64,
                None => 0,
            },
            None => 0,
        };
        let group = Arc::new(LogSync::new(path, file.try_clone()?, file.metadata()?.len()));
        Ok(Wal { file, path: path.to_path_buf(), group, cipher, id, records })
    }

    /// The sync a writer that appended just now settles outside the lock:
    /// this log's group, and the record count it has to reach.
    pub(crate) fn pending(&self) -> (Arc<LogSync>, u64) {
        (self.group.clone(), self.group.appended())
    }

    /// Where the log ends now: what a statement takes before its records,
    /// so a statement that fails partway can be taken back off the log.
    pub(crate) fn mark(&self) -> Result<WalMark> {
        Ok(WalMark { len: self.file.metadata()?.len(), records: self.records })
    }

    /// The log's length on disk.
    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Cut the log back to `mark`: the records a refused statement wrote
    /// are gone, and a reopen replays only what was acknowledged. Best
    /// effort on a disk that is full -- shrinking a file needs no space --
    /// and on one that is gone, where the failure is the caller's anyway.
    pub(crate) fn rollback(&mut self, mark: WalMark) -> Result<()> {
        let mut st = self.group.lock();
        st.low_water = st.low_water.min(mark.len);
        self.file.set_len(mark.len)?;
        self.records = mark.records;
        Ok(())
    }

    pub(crate) fn append(&mut self, r: &WalRecord) -> Result<()> {
        #[cfg(test)]
        durability_probe::check(durability_probe::Op::WalAppend, &self.path)?;
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
        if let Some(c) = &self.cipher {
            out = c.seal_record(&self.id, self.records, &out)?;
            self.records += 1;
        }
        // Under the group's lock, so a sync that failed -- and cut the log
        // back to what it last made durable -- is never followed by a record
        // written past the cut.
        let mut st = self.group.lock();
        if let Some(e) = &st.failed {
            return Err(LogSync::refused(&self.path, e));
        }
        self.file.write_all(&out)?;
        st.appended += 1;
        drop(st);
        // Recorded so that a test can assert the sync below happens AFTER this.
        // Syncing before the append is not a missing sync -- the count is the
        // same and every call still makes a syscall -- it is record N reaching
        // the platter only when record N+1 arrives, so every acknowledged write
        // is one record behind durable.
        #[cfg(test)]
        durability_probe::note_append(&self.path);
        Ok(())
    }

    /// fdatasync this log: the records appended to it are on the disk when
    /// this returns `Ok`.
    ///
    /// `sync_data` rather than `sync_all` because the log's size is part of its
    /// data and nothing else about the inode matters. It cannot create the
    /// directory entry that names the file: `Shard::attach_dir` does that
    /// once, by fsyncing the directory the log was created in.
    ///
    /// The syscall is in `durable::sync_data` rather than here, with the
    /// record of it and the injected-failure check, because the code that may
    /// say an fsync happened has to be the code that makes it -- see that
    /// module.
    pub(crate) fn sync(&mut self) -> Result<()> {
        let mut st = self.group.lock();
        if let Some(e) = &st.failed {
            return Err(LogSync::refused(&self.path, e));
        }
        let (target, len) = (st.appended, self.file.metadata()?.len());
        durable::sync_data(&self.file, &self.path)?;
        st.synced = st.synced.max(target);
        st.synced_len = len;
        Ok(())
    }

    /// Replay. A torn tail — a record whose length or checksum does not check
    /// out — ends the replay rather than failing it: the process died mid-write
    /// and everything before that point is still good.
    pub(crate) fn replay(
        path: &Path,
        cipher: &crate::cipher::Shared,
        id: &str,
    ) -> Result<Vec<WalRecord>> {
        let Some(b) = read_optional(path)? else { return Ok(Vec::new()) };
        // Under a cipher the log is frames, each holding one record as the
        // plain log holds it. Opened in order and joined, the parse below
        // applies unchanged; a torn or foreign frame ends the replay where
        // a bad CRC would have.
        let b: Vec<u8> = match cipher {
            Some(c) => c.open_records(id, &b).concat(),
            None => b,
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

    /// Forget every record in the log.
    ///
    /// Only a caller that has already made the records' effect durable some
    /// other way may do this -- in practice `Shard::flush`, after its call to
    /// `publish_segments` has returned `Ok` for a set that names the segments
    /// they were sealed into. Not `persist_manifest`, which publishes the set
    /// the shard is already in and so has no ordering property to offer.
    /// Recorded for the probe because "the manifest is durable before the WAL
    /// is emptied" is an ordering claim, and the wrong order loses every
    /// document in the sealed memtable.
    /// Rename the log aside as `wal.<seq>.log` and start a fresh one: what
    /// a seal that runs off the lock does instead of truncating, so the
    /// rows written meanwhile keep their log. The rotated file is the
    /// frozen memtable's, deleted when its segments are installed, and
    /// replayed with the live log if the process ends before then.
    pub(crate) fn rotate(&mut self, seq: u64) -> Result<PathBuf> {
        self.sync()?;
        let rotated = self.path.with_file_name(format!("wal.{seq:06}.log"));
        fs::rename(&self.path, &rotated)?;
        sync_dir_of(&self.path)?;
        self.file = fs::OpenOptions::new().create(true).append(true).read(true).open(&self.path)?;
        self.records = 0;
        self.group.replace(self.file.try_clone()?);
        Ok(rotated)
    }

    pub(crate) fn truncate(&mut self) -> Result<()> {
        // Two opens: the first empties the file, the second is the append-mode
        // handle the log goes on being written through. Assigning both to
        // `self.file` closes the first at the second assignment.
        self.file = truncate_file(&self.path)?;
        self.file = fs::OpenOptions::new().create(true).append(true).read(true).open(&self.path)?;
        self.records = 0;
        // The records it held are in segments a published manifest names,
        // which is the only way a caller may get here, so every writer still
        // waiting on them is durable.
        self.group.replace(self.file.try_clone()?);
        Ok(())
    }
}

/// Where a particular version of a document lives right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Loc {
    Mem(u32),
    /// A memtable that has been frozen but whose segment is not committed yet.
    Frozen(usize, u32),
    Seg(u64, u32),
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct ShardOpts {
    /// A seal that is due freezes the memtable and leaves the build to the
    /// console's maintenance thread, which holds no lock for it
    /// (`Shard::seal_freeze`, `seal_build`, `seal_install`); off, a due
    /// seal builds inline under the caller's lock as `FLUSH` does.
    pub background_seal: bool,
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
    /// The object store the `archived` tier lives in, and the key prefix.
    /// `None` keeps the local `archive/` directory as the stand-in.
    pub archive: Option<crate::objstore::ArchiveHandle>,
    /// Encryption at rest, when the database has a key: every file this
    /// shard writes is framed under it and every read opens the frames.
    pub cipher: crate::cipher::Shared,
}

/// What a seal wrote. The ids are ascending, so the last is the segment
/// holding the surviving version of every key. `segment_ids` is empty when a
/// pinned horizon's drain collected every row — the memtable was still sealed,
/// the manifest still moved and the WAL was still truncated, which is why
/// "sealed nothing" cannot be spelled `None`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Sealed {
    pub segment_ids: Vec<u64>,
}

/// A memtable frozen for a seal, with everything its build needs and
/// nothing the shard's lock protects: what `seal_build` turns into
/// segments holding no lock, and `seal_install` commits under it.
pub struct SealTicket {
    frozen: Arc<Memtable>,
    layers: Vec<Vec<PendingDoc>>,
    first_id: u64,
    coll: Collection,
    build: BuildOpts,
    retain_from: Timestamp,
    tally: PathTally,
    wals: Vec<PathBuf>,
}

impl std::fmt::Debug for SealTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SealTicket({} layer(s) from segment {})", self.layers.len(), self.first_id)
    }
}

/// The segments a ticket's build produced, in memory, for `seal_install`.
pub struct SealBuilt {
    segments: Vec<Segment>,
}

impl Sealed {
    /// The segment holding the surviving version of every key this seal wrote.
    #[cfg(test)]
    pub(crate) fn newest(&self) -> Option<u64> {
        self.segment_ids.last().copied()
    }
}

/// One tablet: a key range of a collection, with its memtable, its sealed
/// segments and its write-ahead log.
///
/// Reachable through `Db::shards` for READING -- `coll`, `key_range`,
/// `segments`, `manifest_version`, a snapshot and what it holds, `get`,
/// `num_docs`, `manifest`, `segment_summary`, `residency_rows`. Everything
/// that writes or touches storage -- insert, delete, flush, compaction,
/// publication, the WAL, the statistics gathers with documented
/// preconditions -- is crate-private: a `Db` is the only writer, and a
/// precondition on a crate-private call is the crate's own to keep rather
/// than a contract offered to a caller who cannot see the reasoning.
pub struct Shard {
    pub coll: Collection,
    /// The documents this shard has sealed into segments since it was opened,
    /// and the documents still in its memtable, tallied separately from the
    /// statistics in `coll`, which hold both.
    ///
    /// What the catalog persists is `sealed`. The memtable's documents are
    /// also in the WAL, and a reopen replays the WAL and observes every record
    /// again -- so a persisted count that already included them was counted a
    /// second time at every reopen, and the sum written down to be counted
    /// again: four documents read as 8, 16, 24 across four sessions, with
    /// `SELECT *` answering 4 rows throughout. Persisting only what a replay
    /// will not re-observe makes the count exact in both directions: a record
    /// the WAL holds and the catalog does not -- a crash between the WAL sync
    /// and the persist -- is counted by the replay, once.
    pub(crate) sealed: PathTally,
    pub(crate) unsealed: PathTally,
    /// This shard's half-open key range `[lo, hi)`, from the tablet map.
    /// `None` on either side means unbounded. Range partitioning on the
    /// composite `(partition_key, primary_key)` is what lets a query
    /// constrained on the partition key prune to one tenant's tablets (§3.2);
    /// hash partitioning cannot, because a large tenant would pin to one shard
    /// forever.
    pub key_range: Option<(Option<String>, Option<String>)>,
    /// The shard's position in its collection's placement map, which is its
    /// name in every answer; the shards a node holds need not be contiguous.
    pub index: usize,
    pub(crate) memtable: Memtable,
    pub(crate) frozen: Vec<Arc<Memtable>>,
    pub segments: Vec<Arc<SegmentHandle>>,
    /// The shipper of this shard's log to its followers, when it is held
    /// here and has any.
    pub(crate) shipper: Option<Arc<crate::replication::Shipper>>,
    /// For a followed copy: the instant it stands at (the last mark the
    /// holder shipped and this node logged) and whether it is whole up to
    /// it.
    pub(crate) ship_ts: Timestamp,
    pub(crate) caught_up: bool,
    pub manifest_version: u64,
    pub(crate) next_segment_id: u64,
    pub(crate) opts: ShardOpts,
    /// How many terms `term_stats` has been asked for, summed over calls,
    /// for the tests that pin what a gather does NOT re-measure: the
    /// anchor's whole value is that a second gather at the same instant asks
    /// only for the terms it is missing. Per shard, not process-wide: the
    /// tests run as threads of one process, and a count every shard bumped
    /// made one of them fail whenever another test gathered at the same
    /// moment.
    #[cfg(test)]
    pub(crate) terms_gathered: AtomicU64,
    /// Reads and writes this shard has served since the process started:
    /// `celastro_shard_reads_total` and `_writes_total` on the metrics
    /// page, per shard, which is what shows a hot shard.
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub(crate) clock: Arc<Hlc>,
    dir: Option<PathBuf>,
    wal: Option<Wal>,
    /// Counters for `EXPLAIN` and for the operator-visible flush/compaction
    /// metrics of §12.1.
    pub(crate) flushes: u64,
    /// Memtables frozen by `seal_freeze` whose segments are not built yet,
    /// oldest first; each is also in `frozen`, where reads find it.
    pub(crate) pending_seals: Vec<SealTicket>,
    /// Rotated write-ahead logs whose rows are in the live memtable: the
    /// ones replayed at open. Deleted by the seal that covers them.
    unsealed_wals: Vec<PathBuf>,
    /// The next rotation's number, past every rotated log in the directory.
    wal_seq: u64,
    /// Seals that failed and were left for the next write to retry, and
    /// the last one's reason: a disk that is full or gone, seen here first.
    pub(crate) seal_failures: u64,
    pub(crate) last_seal_error: Option<String>,
    /// Group commit: a write appends and applies, and leaves its log's sync
    /// in `pending` for the caller to settle with the database's lock let go
    /// ([`LogSync`]). Off, the write syncs before it returns, as the embedded
    /// API promises.
    pub(crate) defer_sync: bool,
    pending: Vec<PendingSync>,
    pub(crate) compactions: u64,
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
    pub(crate) retain_floor: Timestamp,
    /// The instant before which a follower's catch-up from where it stood
    /// is not enough, so it starts from nothing: the newest delete a
    /// compaction here has forgotten (a follower from before it would keep
    /// a row this shard no longer remembers deleting), or the last time a
    /// merge absorbed rows into this shard (they keep their own, older
    /// timestamps, so a catch-up from later than them never carries
    /// them). Raised by nothing else -- a seal keeps every tombstone, and
    /// `retain_floor`, which every seal raises to now, is about superseded
    /// versions. Reading the catch-up off `retain_floor` reset every
    /// follower that had been away across a seal, which under a write
    /// load is every follower that was away at all, and the copy from
    /// nothing was minutes of writes waiting on it.
    pub(crate) catchup_floor: Timestamp,
    /// Segments removed from the manifest whose files are still referenced by
    /// a reader. Swept whenever the last reference goes away; without this the
    /// files are simply never unlinked.
    retiring: Vec<Arc<SegmentHandle>>,
    /// The MANIFEST body and each segment's delete log exactly as this shard
    /// last published them, so that a persist which would rewrite a file byte
    /// for byte can decline to.
    ///
    /// It is a record of what this process put on the disk, not a cache of
    /// what is there: nothing else writes these files, so the bytes cannot
    /// change behind it. They can still be taken *away*, which is what
    /// [`still_published`] checks before a skip is allowed. Empty until the
    /// first successful publication, so a fresh or reopened shard always writes
    /// once before it starts skipping.
    ///
    /// Behind a lock because `Shard::persist_manifest` is `&self` and public.
    published_manifest: RwLock<Option<Vec<u8>>>,
    published_deletes: RwLock<BTreeMap<u64, Vec<u8>>>,
}

impl Shard {
    /// `plain` as a file of this shard named `name` holds it: framed under
    /// the cipher when there is one, itself otherwise. What an export, a
    /// backup and a move carry for the files they make rather than copy.
    pub(crate) fn seal_content(&self, name: &str, plain: &[u8]) -> Result<Vec<u8>> {
        match &self.opts.cipher {
            Some(c) => c.seal_file(&self.file_id(name), plain),
            None => Ok(plain.to_vec()),
        }
    }

    pub(crate) fn new(coll: Collection, clock: Arc<Hlc>, opts: ShardOpts) -> Shard {
        let memtable = Memtable::new(&coll, opts.budget.clone());
        Shard {
            coll,
            sealed: PathTally::default(),
            unsealed: PathTally::default(),
            key_range: None,
            index: 0,
            memtable,
            frozen: Vec::new(),
            pending_seals: Vec::new(),
            unsealed_wals: Vec::new(),
            wal_seq: 1,
            segments: Vec::new(),
            manifest_version: 0,
            next_segment_id: 1,
            opts,
            #[cfg(test)]
            terms_gathered: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            clock,
            dir: None,
            wal: None,
            flushes: 0,
            seal_failures: 0,
            last_seal_error: None,
            defer_sync: false,
            pending: Vec::new(),
            compactions: 0,
            retain_floor: 0,
            catchup_floor: 0,
            shipper: None,
            ship_ts: 0,
            caught_up: false,
            retiring: Vec::new(),
            published_manifest: RwLock::new(None),
            published_deletes: RwLock::new(BTreeMap::new()),
        }
    }

    /// Assign this shard's key range. Under replication this comes from the
    /// control
    /// plane's tablet map (§10) and changes on split and merge.
    pub(crate) fn with_key_range(mut self, lo: Option<String>, hi: Option<String>) -> Shard {
        self.set_key_range(lo, hi);
        self
    }

    /// The shard's key range, applied to everything it holds: the memtable
    /// and the frozen ones answer no key outside it, and every segment
    /// keeps only the ordinals inside it. A range that shrank under a
    /// split leaves the rows outside it on disk, invisible, until a
    /// compaction drops them.
    /// The range this shard owns, as `set_key_range` left it: `(None,
    /// None)` for a shard never given one.
    pub(crate) fn key_range(&self) -> (Option<String>, Option<String>) {
        self.key_range.clone().unwrap_or((None, None))
    }

    pub(crate) fn set_key_range(&mut self, lo: Option<String>, hi: Option<String>) {
        self.key_range = Some((lo, hi));
        *self.memtable.range.write().unwrap() = self.key_range.clone();
        for f in &self.frozen {
            *f.range.write().unwrap() = self.key_range.clone();
        }
        for h in &self.segments {
            self.mask_handle(h);
        }
    }

    /// The mask the shard's range gives a segment: the ordinal range of
    /// its keys inside, or none when every key is.
    fn mask_handle(&self, h: &SegmentHandle) {
        let n = h.segment.num_docs();
        let mask = match &self.key_range {
            None => None,
            Some((lo, hi)) => {
                // `[lo, hi)`: the tablet's high bound is exclusive, where
                // `Ordinals::range` takes an inclusive one.
                let keys = &h.segment.ordinals.keys;
                let a = lo.as_deref().map_or(0, |l| keys.partition_point(|k| k.as_str() < l));
                let b = hi.as_deref().map_or(n, |x| keys.partition_point(|k| k.as_str() < x));
                if a == 0 && b >= n {
                    None
                } else {
                    Some((a as u32, b.min(n) as u32))
                }
            }
        };
        h.set_mask(mask);
    }

    /// A memtable for this shard: the definition, the budget, the range.
    fn fresh_memtable(&self) -> Memtable {
        let m = Memtable::new(&self.coll, self.opts.budget.clone());
        *m.range.write().unwrap() = self.key_range.clone();
        m
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

    pub(crate) fn attach_dir(&mut self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir.join("segments"))?;
        fs::create_dir_all(dir.join("archive"))?;
        fs::create_dir_all(dir.join("deletes"))?;
        // The directory first: the log's identity under the cipher is the
        // directory's name, and the log is opened under it.
        self.dir = Some(dir.to_path_buf());
        self.wal = Some(Wal::open(
            &dir.join("wal.log"),
            self.opts.cipher.clone(),
            self.file_id("wal.log"),
        )?);
        // Every one of those creations is a new entry in `dir`, and a directory
        // entry is dirty metadata like any other. The WAL's own fdatasync
        // flushes the record and cannot create the name that reaches it, so
        // without this the first acknowledged insert is durable inside a file a
        // crash still takes away -- the shard reopens empty, or `Db::open`'s
        // scan stops at a shard directory that is not there and the collection
        // is in the catalog with no shards at all. Once per attach, off the
        // per-write path. The directory `dir` itself is named in is the
        // caller's to sync, because only the caller knows where the tree stops:
        // see `Db::build_shards`.
        sync_dir(dir)?;
        // The lowest id this shard may use is what the DIRECTORY holds, and it
        // is decided here rather than in `Shard::open` because `open` is not
        // the only way in: `Db::build_shards` attaches without opening, and
        // this method is public API. A shard that attached to a populated
        // directory and resumed at id 1 would inherit the delete log already
        // sitting under that id, which is the silent loss below.
        //
        // The manifest is not the whole record of which ids have been spoken
        // for. A publication that failed — or a crash, which is the same story
        // with no error to catch — leaves a `.seg` and possibly a `.dlog` on
        // the disk under an id the manifest it never reached would have been
        // the first to name. Resuming at the manifest's counter hands that id
        // out again, and the `.dlog` is the reason that matters: delete logs
        // are found by id and nothing else, and an empty one is not written, so
        // the segment that reuses the id is reopened carrying the deletions of
        // the segment that was never published. Documents nobody deleted
        // disappear, and no error is returned anywhere.
        //
        // So the directory is asked instead. One listing per attach, against a
        // seal path that would otherwise have to make an id durable before it
        // could be used — a second manifest publication, two fsyncs and a
        // rename, on every flush, to insure against a failure that is rare.
        // Cleaning the orphans up on the failure path instead would leave the
        // crash unhandled, because nothing runs on that path at all; the
        // reclamation that does happen, in [`reclaim_orphans`], cannot stand in
        // for this either, because it runs only where a MANIFEST says what is
        // live and the directory that lost its manifest is exactly the one
        // where reusing an id costs documents.
        self.next_segment_id = self.next_segment_id.max(first_unused_segment_id(dir)?);
        // Another directory holds another MANIFEST, and what was published to
        // the last one says nothing about what is in this one. This keeps the
        // field's contract true rather than being the thing that prevents a
        // wrong skip -- `still_published` compares the candidate bytes against
        // the file, so a cache carried over from another directory cannot make
        // a skip happen that should not have. Both, deliberately: the check on
        // disk is what the skip rests on, and a cache that means what it says
        // is what the next reader of this code rests on.
        *self.published_manifest.write().unwrap() = None;
        self.published_deletes.write().unwrap().clear();
        Ok(())
    }

    /// The directory this shard persists to, if it persists.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    pub fn snapshot(&self) -> Snapshot<'_> {
        self.snapshot_at(self.clock.visible(self.clock.peek()))
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

    /// The middle key of the rows visible at `t`: what a split takes when
    /// no key is given. `None` under two distinct keys, since a split
    /// needs a key strictly inside the range, and the middle of two or
    /// more distinct keys is above the first.
    pub fn median_key(&self, t: Timestamp) -> Option<String> {
        let snap = self.snapshot_at(t);
        let mut keys: Vec<String> = Vec::new();
        for src in self.sources(&snap) {
            let vis = src.visibility(t);
            for ord in vis.iter() {
                if let Some(k) = src.key(ord) {
                    keys.push(k.to_string());
                }
            }
        }
        keys.sort_unstable();
        keys.dedup();
        if keys.len() < 2 {
            return None;
        }
        Some(keys[keys.len() / 2].clone())
    }

    /// Where a version lives.
    /// Whether a document with this sort key is visible at `t`: the locate
    /// without the decode, for a walk's liveness check, which asks it once
    /// per key over thousands of keys.
    pub fn contains(&self, key: &str, t: Timestamp) -> bool {
        self.locate(key, t).is_some()
    }

    /// Which of `keys` -- sorted and distinct -- are visible at `t`, one
    /// flag per key: the answer [`Shard::contains`] gives each key, reached
    /// by a merge instead of a lookup per key. The keys are walked against
    /// each segment's sorted keys in one pass, galloping, so a frontier far
    /// smaller than a segment costs the frontier and the logarithm of the
    /// gaps rather than a binary search per key over pointer-chased strings.
    /// A segment holds one version per key, so the merge's hit is the hit
    /// `find` would make, and it is visible or not by the same bitmap the
    /// scatter reads. The memtable and its frozen predecessors are maps and
    /// stay a probe per key; after a flush they are empty.
    pub fn present_sorted(&self, keys: &[String], t: Timestamp) -> Vec<bool> {
        debug_assert!(keys.windows(2).all(|w| w[0] < w[1]), "keys are sorted and distinct");
        let mut out: Vec<bool> = keys
            .iter()
            .map(|k| {
                self.memtable.find_at(k, t).is_some()
                    || self.frozen.iter().any(|f| f.find_at(k, t).is_some())
            })
            .collect();
        for h in &self.segments {
            let seg_keys = &h.segment.ordinals.keys;
            let mut vis: Option<Bitmap> = None;
            let mut pos = 0usize;
            for (i, key) in keys.iter().enumerate() {
                if out[i] {
                    continue;
                }
                pos = gallop(seg_keys, pos, key);
                if pos == seg_keys.len() {
                    break;
                }
                if seg_keys[pos] == *key && vis.get_or_insert_with(|| h.visibility(t)).get(pos) {
                    out[i] = true;
                }
            }
        }
        out
    }

    fn locate(&self, key: &str, t: Timestamp) -> Option<Loc> {
        if !self.owns(key) {
            return None;
        }
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

    /// The newest version of `key` anywhere in the shard, visible or not, with
    /// its own commit timestamp.
    ///
    /// [`Shard::locate`]'s sibling for WAL replay, and the difference is the
    /// visibility argument: `locate(key, MAX_TS)` finds the newest version not
    /// yet deleted, which is the right question for the write path and the
    /// wrong one for a replay that has to decide whether a record's effect is
    /// already on disk.
    fn latest_version(&self, key: &str) -> Option<(Loc, Timestamp)> {
        let mut best: Option<(Loc, Timestamp)> = None;
        let mut consider = |loc: Loc, ts: Timestamp| {
            if best.map(|(_, b)| ts > b).unwrap_or(true) {
                best = Some((loc, ts));
            }
        };
        if let Some(ord) = self.memtable.find(key) {
            if let Some(ts) = self.memtable.ordinals.commit_ts.get(ord as usize) {
                consider(Loc::Mem(ord), *ts);
            }
        }
        for (i, f) in self.frozen.iter().enumerate() {
            if let Some(ord) = f.find(key) {
                if let Some(ts) = f.ordinals.commit_ts.get(ord as usize) {
                    consider(Loc::Frozen(i, ord), *ts);
                }
            }
        }
        for h in &self.segments {
            if let Some(ord) = h.segment.ordinals.find(key) {
                if let Some(ts) = h.segment.ordinals.commit_ts.get(ord as usize) {
                    consider(Loc::Seg(h.id(), ord), *ts);
                }
            }
        }
        best
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
    pub(crate) fn insert(&mut self, mut doc: Value) -> Result<Timestamp> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
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
            let mark = w.mark()?;
            let record = WalRecord {
                kind: WAL_INSERT,
                key: key.clone(),
                ts,
                doc: Some(doc.clone()),
                supersedes: prev.is_some(),
                segment_id: 0,
            };
            // A record that could not be written whole, or synced, is taken
            // back off the log: a statement that was refused must not come
            // back on the next reopen as if it had been acknowledged. This
            // is what a full disk showed: the appends that fit were replayed
            // for a statement the client was told had failed.
            let defer = self.defer_sync;
            if let Err(e) = w.append(&record).and_then(|_| if defer { Ok(()) } else { w.sync() }) {
                let _ = w.rollback(mark);
                return Err(e);
            }
            if defer {
                let (log, seq) = w.pending();
                self.clock.begin(ts);
                self.pending.push(PendingSync { log, seq, first: ts });
            }
            if let Some(sh) = &self.shipper {
                sh.push(ship_item(&record));
            }
            // `append` ends at `write_all`, which reaches the page cache and
            // stops there, so without this the timestamp returned below names a
            // commit that a power loss still takes back. The record is framed
            // `len | crc32` and replay ends cleanly on a torn tail, so one
            // document's record is already all-or-nothing on disk; this is what
            // makes it *on* the disk. Per document here, because the unit that
            // is promised is the statement and this statement is one document;
            // `insert_many` syncs once for a statement of many, which is the
            // same promise. A sync that covered every document since the last
            // one, across statements, would be group commit, and is not here.
            //
            // It also sits above the mutations below rather than after them,
            // so a sync that fails returns `Err` with the in-memory shard
            // exactly as it was, and the log cut back to where it was, so a
            // rejected document does not live again on the next reopen.
        }
        if let Some(p) = prev {
            self.mark_superseded(p, ts);
        }
        self.coll.observe_doc(&doc);
        self.unsealed.observe_doc(&doc);
        self.memtable.insert(key, ts, doc)?;
        self.seal_if_due();
        Ok(ts)
    }

    /// Seal the memtable if its thresholds say so. A seal that fails --
    /// the disk is full, the directory is gone -- does not fail the write
    /// that triggered it: that write is on the log and in memory, which is
    /// what it was promised, and the seal is tried again by the next one.
    /// The failure is counted and kept, for the metrics and the log.
    fn seal_if_due(&mut self) {
        if let Err(e) = self.maybe_flush() {
            self.seal_failures += 1;
            self.last_seal_error = Some(e.to_string());
        }
    }

    /// `insert`, for the documents of one statement: every document is
    /// validated before any record is appended, every record is appended,
    /// the log is synced ONCE, and only then is the shard's memory changed.
    /// The promise is the statement's -- the caller acknowledges all of them
    /// together -- so one sync covers them, and a batch of a thousand costs
    /// one disk round trip rather than a thousand. A key that recurs within
    /// the batch takes the one-at-a-time path, where each record's
    /// `supersedes` is computed against the version the one before it made.
    /// Returns the timestamps in order.
    pub(crate) fn insert_many(&mut self, docs: Vec<Value>) -> Result<Vec<Timestamp>> {
        self.writes.fetch_add(docs.len() as u64, AtomicOrdering::Relaxed);
        let mut prepared: Vec<(String, Timestamp, Value, Option<Loc>)> =
            Vec::with_capacity(docs.len());
        let mut keys = std::collections::BTreeSet::new();
        let mut recurring = false;
        for mut doc in docs {
            self.coll.validate(&doc)?;
            self.coll.coerce(&mut doc);
            let key = sort_key(&self.coll, &doc)?;
            self.validate_indexable(&doc)?;
            if !keys.insert(key.clone()) {
                recurring = true;
            }
            let ts = self.clock.now();
            let prev = self.locate(&key, MAX_TS);
            prepared.push((key, ts, doc, prev));
        }
        if recurring {
            let mut out = Vec::with_capacity(prepared.len());
            for (_, _, doc, _) in prepared {
                out.push(self.insert(doc)?);
            }
            return Ok(out);
        }
        let defer = self.defer_sync;
        if let Some(w) = self.wal.as_mut() {
            let mark = w.mark()?;
            let written: Result<()> = 'log: {
                for (key, ts, doc, prev) in &prepared {
                    if let Err(e) = w.append(&WalRecord {
                        kind: WAL_INSERT,
                        key: key.clone(),
                        ts: *ts,
                        doc: Some(doc.clone()),
                        supersedes: prev.is_some(),
                        segment_id: 0,
                    }) {
                        break 'log Err(e);
                    }
                }
                // Above the mutations, as in `insert`: a sync that fails
                // leaves the shard's memory as it was for every document.
                // Deferred, it is the caller's, after the lock.
                if defer {
                    Ok(())
                } else {
                    w.sync()
                }
            };
            // All or nothing on the log too: the records that fit before the
            // disk filled are taken back, so the statement the client was
            // told had failed does not come back on the next reopen.
            if let Err(e) = written {
                let _ = w.rollback(mark);
                return Err(e);
            }
            if defer {
                let (log, seq) = w.pending();
                let first = prepared.iter().map(|p| p.1).min().unwrap_or(0);
                self.clock.begin(first);
                self.pending.push(PendingSync { log, seq, first });
            }
            if let Some(sh) = &self.shipper {
                for (key, ts, doc, _) in &prepared {
                    sh.push(crate::replication::ShipItem {
                        kind: crate::replication::SHIP_INSERT,
                        key: key.clone(),
                        ts: *ts,
                        doc: Some(doc.clone()),
                    });
                }
            }
        }
        let mut out = Vec::with_capacity(prepared.len());
        for (key, ts, doc, prev) in prepared {
            if let Some(p) = prev {
                self.mark_superseded(p, ts);
            }
            self.coll.observe_doc(&doc);
            self.unsealed.observe_doc(&doc);
            self.memtable.insert(key, ts, doc)?;
            out.push(ts);
        }
        // After every document, not between them: a seal that fails in the
        // middle of a batch used to fail the statement with half of it
        // applied in memory and all of it on the log.
        self.seal_if_due();
        Ok(out)
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

    pub(crate) fn delete(&mut self, key: &str) -> Result<Option<Timestamp>> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        let ts = self.clock.now();
        let Some(prev) = self.locate(key, MAX_TS) else { return Ok(None) };
        if let Some(w) = self.wal.as_mut() {
            let mark = w.mark()?;
            let record = WalRecord {
                kind: WAL_DELETE,
                key: key.to_string(),
                ts,
                doc: None,
                supersedes: true,
                segment_id: 0,
            };
            // Durable before `mark_superseded` below makes the removal visible,
            // for the same reason as in `insert` and more sharply: this shard
            // would otherwise drop the old version for a delete the log never
            // recorded, and a reopen would resurrect the document. A record
            // that did not make it is taken back off the log.
            let defer = self.defer_sync;
            if let Err(e) = w.append(&record).and_then(|_| if defer { Ok(()) } else { w.sync() }) {
                let _ = w.rollback(mark);
                return Err(e);
            }
            if defer {
                let (log, seq) = w.pending();
                self.clock.begin(ts);
                self.pending.push(PendingSync { log, seq, first: ts });
            }
            if let Some(sh) = &self.shipper {
                sh.push(ship_item(&record));
            }
        }
        self.mark_superseded(prev, ts);
        Ok(Some(ts))
    }

    /// The version of `key` live at `t`.
    ///
    /// Reads at a `t` below `Shard::retain_floor` are best-effort: a version
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
    pub(crate) fn adopt_catalog(&mut self, coll: Collection) -> Result<()> {
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
            self.memtable = self.fresh_memtable();
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
    pub(crate) fn adopt_definition(&mut self, coll: Collection) {
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
    pub(crate) fn unload_idle(&self, now: u64) -> usize {
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
    /// The object a segment of this shard is stored under when the archive
    /// is an object store: the prefix, the collection, the shard directory
    /// and the segment's file name, so that one bucket holds many databases
    /// and a key reads like the path it stands in for.
    fn object_key(&self, id: u64) -> Option<String> {
        let dir = self.dir.as_ref()?;
        let h = self.opts.archive.as_ref()?;
        let shard = dir.file_name()?.to_str()?;
        let coll = dir.parent()?.file_name()?.to_str()?;
        Some(format!("{}{coll}/{shard}/{id:016x}.seg", h.prefix))
    }

    pub(crate) fn sync_archive(&mut self) -> Result<usize> {
        let Some(dir) = self.dir.clone() else { return Ok(0) };
        if let Some(h) = self.opts.archive.clone() {
            return self.sync_object_store(&dir, &h);
        }
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
                    h.segment.set_source(self.wrap_source(
                        h.id(),
                        if want_archive {
                            SegmentSource::Archive(to.clone())
                        } else {
                            SegmentSource::File(to.clone())
                        },
                    ));
                    h.set_path(Some(to.clone()));
                }
                continue;
            }
            // Drop the decoded components first: they were charged against the
            // residency budget under the old tier, and the bytes behind them
            // are about to move.
            h.segment.unload_all();
            fs::rename(from, to)?;
            #[cfg(test)]
            durability_probe::note_rename(to);
            h.segment.set_source(self.wrap_source(
                h.id(),
                if want_archive {
                    SegmentSource::Archive(to.clone())
                } else {
                    SegmentSource::File(to.clone())
                },
            ));
            h.set_path(Some(to.clone()));
            moved += 1;
        }
        // A tier move is a publication: the manifest names these segments by
        // id and finds them by looking in `segments/` and then `archive/`, so
        // a rename whose directory entries a crash takes back is a segment
        // the manifest names and neither directory holds -- the open fails.
        // Both parents, once per batch rather than once per file, and the
        // destination first: with the new name durable and the old one not,
        // a crash leaves the file findable in both places, which the open
        // resolves; the other order leaves it in neither.
        if moved > 0 {
            let (from_dir, to_dir) =
                if want_archive { ("segments", "archive") } else { ("archive", "segments") };
            sync_dir(&dir.join(to_dir))?;
            sync_dir(&dir.join(from_dir))?;
        }
        Ok(moved)
    }

    /// The tier move against an object store. To the archive: the local file
    /// is `PUT` whole, and only once the store has acknowledged it is the
    /// local copy unlinked, so a failure between the two leaves both and the
    /// next move re-puts. Back: the object is fetched whole and published
    /// into `segments/` like any other file, and only then deleted from the
    /// store; a failure between the two leaves both, and the open prefers
    /// the local copy. A segment sitting in the legacy local `archive/`
    /// directory from before the store was configured is migrated by the
    /// first move that wants it archived.
    fn sync_object_store(
        &mut self,
        dir: &Path,
        h: &crate::objstore::ArchiveHandle,
    ) -> Result<usize> {
        let want_archive = self.coll.all_indexes_archived();
        let mut moved = 0;
        for handle in &self.segments {
            let Some(key) = self.object_key(handle.id()) else { continue };
            let name = format!("{:016x}.seg", handle.id());
            let local = dir.join("segments").join(&name);
            let legacy = dir.join("archive").join(&name);
            let is_remote =
                matches!(handle.segment.source().unwrapped(), SegmentSource::Remote { .. });
            if want_archive {
                let from = if local.exists() {
                    local.clone()
                } else if legacy.exists() {
                    legacy.clone()
                } else {
                    continue;
                };
                if is_remote {
                    continue;
                }
                handle.segment.unload_all();
                let bytes = fs::read(&from)?;
                h.store.put(&key, &bytes)?;
                let size = bytes.len() as u64;
                handle.segment.set_source(self.wrap_source(
                    handle.id(),
                    SegmentSource::Remote { store: h.store.clone(), key: key.clone(), size },
                ));
                handle.set_path(None);
                fs::remove_file(&from)?;
                #[cfg(test)]
                durability_probe::note_rename(&from);
                sync_dir(from.parent().unwrap_or(dir))?;
                moved += 1;
            } else if is_remote {
                handle.segment.unload_all();
                let bytes = h.store.get(&key)?;
                publish(&local, &bytes)?;
                sync_dir(&dir.join("segments"))?;
                handle
                    .segment
                    .set_source(self.wrap_source(handle.id(), SegmentSource::File(local.clone())));
                handle.set_path(Some(local.clone()));
                h.store.delete(&key)?;
                moved += 1;
            }
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
    /// which holds it back (§12.5), and otherwise it is now. Compaction DRAINS
    /// at it; a flush drains at it only while something is pinned — unpinned it
    /// keeps every row and drops superseded versions instead, by keeping layer
    /// 0. Both read it from here, and both report it through
    /// `Shard::retain_floor`, so it lives in one place.
    pub fn retain_from(&self, now: Timestamp) -> Timestamp {
        if self.opts.gc_horizon > 0 {
            self.opts.gc_horizon.min(now)
        } else {
            now
        }
    }

    /// Whether a seal must keep versions a newer one supersedes: a backup
    /// holds the horizon back, or a write is on a log and not yet synced, so
    /// readers are held below it and still read the version it replaced.
    fn keeps_history(&self) -> bool {
        self.opts.gc_horizon > 0 || self.clock.writes_in_flight()
    }

    /// The log syncs this shard's writes left for their caller since the
    /// last call: see [`Shard::defer_sync`].
    pub(crate) fn take_pending(&mut self) -> Vec<PendingSync> {
        std::mem::take(&mut self.pending)
    }

    /// This shard's log, for its sync statistics and its failure.
    pub(crate) fn log_sync(&self) -> Option<Arc<LogSync>> {
        self.wal.as_ref().map(|w| w.group.clone())
    }

    pub(crate) fn maybe_flush(&mut self) -> Result<bool> {
        let pinned = self.opts.gc_horizon > 0;
        if self.memtable.should_flush_pinned(&self.opts.thresholds, pinned) {
            // With a sealer running, a due seal is a freeze here and a build
            // there; two seals it has not caught up with are the bound, and
            // past it the write path builds inline, which is backpressure.
            if self.opts.background_seal && self.pending_seals.len() < 2 {
                self.seal_freeze()?;
            } else {
                self.flush()?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Freeze the memtable for a seal: the rows it holds, laid out as the
    /// segments a build makes of them, the segment ids reserved, the
    /// write-ahead log rotated aside, and the memtable itself moved to
    /// `frozen` where reads and deletes still find it. Nothing here builds
    /// an index; that is `seal_build`, holding no lock.
    pub(crate) fn seal_freeze(&mut self) -> Result<bool> {
        if self.memtable.is_empty() {
            return Ok(false);
        }
        let history = self.keeps_history();
        let retain_from = self.retain_from(self.clock.visible(self.clock.peek()));
        let drain_at = if history { retain_from } else { 0 };
        let mut layers = crate::segment::layer_by_version(self.memtable.drain_into(drain_at));
        if !history {
            layers.truncate(1);
        }
        let first_id = self.next_segment_id;
        self.next_segment_id += layers.len() as u64;
        let mut wals = std::mem::take(&mut self.unsealed_wals);
        if let Some(w) = self.wal.as_mut() {
            let seq = self.wal_seq;
            self.wal_seq += 1;
            wals.push(w.rotate(seq)?);
        }
        let fresh = self.fresh_memtable();
        let frozen = Arc::new(std::mem::replace(&mut self.memtable, fresh));
        self.frozen.push(frozen.clone());
        self.pending_seals.push(SealTicket {
            frozen,
            layers,
            first_id,
            coll: self.coll.clone(),
            build: self.opts.build,
            retain_from,
            tally: std::mem::take(&mut self.unsealed),
            wals,
        });
        Ok(true)
    }

    /// The oldest frozen memtable waiting for its build, if any.
    pub(crate) fn seal_take(&mut self) -> Option<SealTicket> {
        if self.pending_seals.is_empty() {
            None
        } else {
            Some(self.pending_seals.remove(0))
        }
    }

    /// Build a ticket's segments. No lock, no shard: the ticket has the
    /// rows, the ids and the options, and this is the time the graph takes.
    pub(crate) fn seal_build(t: &SealTicket) -> Result<SealBuilt> {
        let mut segments = Vec::new();
        for (id, layer) in (t.first_id..).zip(t.layers.iter().rev()) {
            let mut b = SegmentBuilder::new(t.build);
            for pd in layer {
                b.add(pd.clone());
            }
            segments.push(b.build(id, 0, &t.coll)?);
        }
        Ok(SealBuilt { segments })
    }

    /// Commit a build: the segments to disk and the manifest, the deletes
    /// the frozen memtable took before and during the build into their
    /// logs, the frozen memtable let go, its rotated log removed.
    ///
    /// An install that fails before the manifest is published -- a segment
    /// that cannot be written, a manifest that cannot be -- puts the ticket
    /// back as a failed build does. Until 0.75.0 it was dropped: the rows
    /// stayed readable in the frozen memtable and durable in the rotated
    /// log, but nothing queued them again before a restart, so a node whose
    /// disk filled during a seal and then emptied never sealed them.
    pub(crate) fn seal_install(&mut self, t: SealTicket, built: SealBuilt) -> Result<Sealed> {
        let deletes = Shard::deletes_of(&t.frozen);
        let committed =
            self.handles_for(built.segments, &deletes).and_then(|h| self.commit_handles(h));
        let sealed = match committed {
            Ok(sealed) => sealed,
            Err(e) => {
                self.seal_requeue(t, &e);
                return Err(e);
            }
        };
        self.sealed.merge(&t.tally);
        self.frozen.retain(|f| !Arc::ptr_eq(f, &t.frozen));
        t.frozen.release_budget();
        self.retain_floor = self.retain_floor.max(t.retain_from);
        for p in &t.wals {
            if p.exists() {
                fs::remove_file(p)?;
            }
        }
        if let Some(p) = t.wals.first() {
            sync_dir_of(p)?;
        }
        // The ship mark was in the log this seal removed; on disk in its
        // own file now, or a copy reopened after a seal with no write
        // since stood "not caught up" and was copied from nothing.
        self.persist_ship_mark()?;
        Ok(sealed)
    }

    /// `SHIPPED`: the instant a followed copy is caught up to, kept beside
    /// the log because a seal truncates the log and the marks with it.
    pub(crate) fn persist_ship_mark(&self) -> Result<()> {
        let Some(dir) = &self.dir else { return Ok(()) };
        if !self.caught_up || self.ship_ts == 0 {
            return Ok(());
        }
        atomic_write(&dir.join("SHIPPED"), self.ship_ts.to_string().as_bytes())
    }

    /// A build that failed: the ticket goes back to the front of the
    /// queue, the rows stay readable in the frozen memtable and durable in
    /// its rotated log, and the failure is counted.
    pub(crate) fn seal_requeue(&mut self, t: SealTicket, err: &Error) {
        self.seal_failures += 1;
        self.last_seal_error = Some(err.to_string());
        self.pending_seals.insert(0, t);
    }

    /// The deletes a memtable holds, as `(key, version, delete instant)`.
    fn deletes_of(m: &Memtable) -> Vec<(String, Timestamp, Timestamp)> {
        m.delete_entries()
            .into_iter()
            .filter_map(|(ord, dts)| {
                m.docs.get(ord as usize).map(|d| (d.sort_key.clone(), d.commit_ts, dts))
            })
            .collect()
    }

    /// Persist built segments and give each its handle, with the deletes
    /// that name its rows.
    fn handles_for(
        &self,
        built: Vec<Segment>,
        deletes: &[(String, Timestamp, Timestamp)],
    ) -> Result<Vec<Arc<SegmentHandle>>> {
        let mut handles: Vec<Arc<SegmentHandle>> = Vec::new();
        for seg in built {
            self.adopt_segment(&seg);
            let path = self.persist_segment(&seg)?;
            let handle = SegmentHandle::new(seg, DeleteLog::new(), path);
            self.mask_handle(&handle);
            for (key, version_ts, delete_ts) in deletes {
                if let Some(ord) = handle.segment.ordinals.find(key) {
                    if handle.segment.ordinals.commit_ts[ord as usize] == *version_ts {
                        handle.mark_deleted(ord, *delete_ts);
                    }
                }
            }
            handles.push(handle);
        }
        Ok(handles)
    }

    /// Publish a manifest naming the shard's segments and these, and adopt
    /// it: the one point at which a seal becomes reader-visible. Id order
    /// is what `locate`'s newest-first scan reads as version order.
    fn commit_handles(&mut self, handles: Vec<Arc<SegmentHandle>>) -> Result<Sealed> {
        let sealed = Sealed { segment_ids: handles.iter().map(|h| h.id()).collect() };
        let mut next = self.segments.clone();
        next.extend(handles);
        next.sort_by_key(|h| h.id());
        let version = self.manifest_version + 1;
        self.publish_segments(&next, version)?;
        self.segments = next;
        self.manifest_version = version;
        self.flushes += 1;
        Ok(sealed)
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
    /// exactly as it always was. Compaction stays the only collector of
    /// TOMBSTONED rows; superseded versions go here, as they always did — which
    /// is why an unpinned seal still raises `Shard::retain_floor`, and why a
    /// snapshot below that floor may have lost versions it could once see.
    ///
    /// `None` means nothing was sealed, and it means only that: the memtable
    /// was empty. `Sealed` carries the ids a seal did write, newest last,
    /// and is empty when a pinned drain collected every row — which is still a
    /// seal, and `Db::flush` still counts it.
    ///
    /// [`compaction::run`]: crate::compaction::run
    pub(crate) fn flush(&mut self) -> Result<Option<Sealed>> {
        // Seals frozen for the background first, inline: `FLUSH` means
        // everything sealed, and a shard with no sealer running has none.
        let mut drained = Vec::new();
        while let Some(t) = self.seal_take() {
            match Shard::seal_build(&t) {
                Ok(b) => drained.extend(self.seal_install(t, b)?.segment_ids),
                Err(e) => {
                    self.seal_requeue(t, &e);
                    return Err(e);
                }
            }
        }
        if self.memtable.is_empty() {
            return Ok(if drained.is_empty() {
                None
            } else {
                Some(Sealed { segment_ids: drained })
            });
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
        // version-GC pass, and a seal is not one: a snapshot read below the
        // seal would lose rows it could read a moment earlier. Note which
        // rows those are — `drain_into(0)` keeps every *tombstoned* row, so a
        // delete is untouched by a seal and survives to be collected by a
        // compaction; what a seal would drop, were it to collect, is a
        // version some later write had already superseded, and that is what
        // `layers.truncate(1)` below does. Scoring no longer enters into it
        // on either path: both take their statistics from `term_stats`, which
        // masks by visibility on both halves of the quotient, so a seal moves
        // no Term or Phrase score whatever it drops. It used to move the
        // default path, whose statistics came from `TextSource::all_terms` —
        // a physical count, so the very truncation described above dropped a
        // superseded version out of `doc_freq` and shifted IDF. Prefix
        // expansion used to be the exception — it reached the coordinator's
        // gather on neither path and scored against the segment's own
        // dictionary — and is no longer one: the coordinator resolves it once
        // for the statement, and `Shard::prefix_terms` enumerates the LIVE
        // dictionary at the query's own `t`, so neither which terms a prefix
        // names nor what they weigh depends on what this seal drops or keeps.
        let history = self.keeps_history();
        let retain_from = self.retain_from(self.clock.visible(self.clock.peek()));
        let drain_at = if history { retain_from } else { 0 };
        let mut layers = crate::segment::layer_by_version(self.memtable.drain_into(drain_at));
        if !history {
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
        let deletes = Shard::deletes_of(&self.memtable);
        // Every fallible step first, into a local list, exactly as
        // `install_compaction` does: a persist that fails on the second output
        // must not leave the first one live in `self.segments` while the
        // memtable still holds all of its rows, because then the same key is
        // reachable twice and a retry makes the duplicate permanent.
        let handles = self.handles_for(built, &deletes)?;
        // The ids are committed here rather than below, and deliberately not
        // as part of the commit: the files under them are already on the disk,
        // so they have been spoken for whatever happens next. Handing one out
        // again after a failed publication overwrites an orphaned `.seg` --
        // harmless -- and inherits an orphaned `.dlog`, which is not: delete
        // logs are found by id alone, and an empty log is not written, so the
        // segment that reuses the id would be reopened carrying deletions that
        // belong to a segment nothing ever published. Erring high costs an id
        // out of 2^64. `Shard::open` refuses the same reuse across a reopen,
        // where the counter itself is what was lost.
        self.next_segment_id = next_id;
        // The publication is the last thing that can fail, and it comes
        // before the log is emptied: until it returns `Ok` the shard is the
        // shard it was, the memtable holds every row, and the old segment
        // set is the one readers see and the manifest on the disk names.
        // Truncating the log first, or despite a publication that failed,
        // would leave a manifest that does not name the new segments beside
        // a log that no longer holds the documents.
        let mut sealed = self.commit_handles(handles)?;
        drained.extend(std::mem::take(&mut sealed.segment_ids));
        sealed.segment_ids = drained;
        // The memtable's documents are on the disk under a published
        // manifest, so the next persist may count them: from here a reopen
        // finds them in segments and not in the WAL, and will not observe
        // them again.
        self.sealed.merge(&self.unsealed);
        self.unsealed = PathTally::default();
        let fresh = self.fresh_memtable();
        let old = std::mem::replace(&mut self.memtable, fresh);
        old.release_budget();
        // Only now has anything been forgotten: until the swap the memtable
        // still held every row. Claiming the floor earlier would claim a
        // collection that had not happened yet — and before the publication
        // above, one that might never happen at all — and the field's contract
        // is that at or above it nothing has been collected.
        self.retain_floor = self.retain_floor.max(retain_from);
        // The log can go now, and only now. This one is still fallible and the
        // error is still reported: what it leaves behind is a log holding
        // records the manifest already names, which is the ordinary
        // crash-between-the-two state and the one `Shard::open`'s replay
        // converges on rather than re-applying.
        if let Some(w) = self.wal.as_mut() {
            w.truncate()?;
        }
        self.persist_ship_mark()?;
        // The rotated logs replayed at open are covered by this seal too.
        for p in std::mem::take(&mut self.unsealed_wals) {
            if p.exists() {
                fs::remove_file(&p)?;
            }
        }
        Ok(Some(sealed))
    }

    fn persist_segment(&self, seg: &Segment) -> Result<Option<PathBuf>> {
        let Some(dir) = self.dir.as_ref() else { return Ok(None) };
        let p = dir.join("segments").join(format!("{:016x}.seg", seg.id));
        // Durable before the manifest names it, and long before the WAL that
        // could rebuild it is truncated.
        let bytes = seg.encode()?;
        write_content(&self.opts.cipher, &self.segment_file_id(seg.id), &p, &bytes)?;
        // The local file is now the source; the in-memory copy can go.
        seg.set_source(self.wrap_source(seg.id, crate::segment::SegmentSource::File(p.clone())));
        Ok(Some(p))
    }

    /// The bytes a reopen of this shard would replay: the live
    /// write-ahead log, and every log a freeze rotated aside whose seal has
    /// not landed yet. Zero for a shard with no directory.
    ///
    /// Both halves matter, and the live log alone would hide the case worth
    /// seeing: a freeze rotates the log aside and starts an empty one, so a
    /// seal that keeps failing leaves the rows in `wal.NNNNNN.log` while
    /// the live log looks healthy. A number that only grows is a seal that
    /// is not landing, and it is also the length of the next restart, at
    /// the replay rate the resilience suite prints.
    pub fn wal_bytes(&self) -> u64 {
        let live = self.wal.as_ref().and_then(|w| w.len().ok()).unwrap_or(0);
        // A freeze moves the rotated logs into its ticket: the ones queued
        // for a seal are there, not in `unsealed_wals`.
        let rotated: u64 = self
            .unsealed_wals
            .iter()
            .chain(self.pending_seals.iter().flat_map(|t| t.wals.iter()))
            .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();
        live + rotated
    }

    pub fn manifest(&self) -> Manifest {
        Shard::manifest_of(
            &self.segments,
            self.manifest_version,
            self.next_segment_id,
            self.catchup_floor,
        )
    }

    /// The manifest a given segment set would publish.
    ///
    /// Taken as arguments rather than read off `self` because a publication
    /// happens BEFORE the shard is moved to the state it publishes: see
    /// [`Shard::publish_segments`]. The shard's own fields are one caller of
    /// this, not its definition.
    pub(crate) fn manifest_of(
        segments: &[Arc<SegmentHandle>],
        version: u64,
        next_segment_id: u64,
        catchup_floor: Timestamp,
    ) -> Manifest {
        Manifest {
            version,
            next_segment_id,
            catchup_floor,
            segments: segments
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

    /// Publish the segment set this shard is in right now.
    ///
    /// The shells call this after an acknowledged statement. It is the
    /// degenerate case of the publication a seal or a compaction performs: the
    /// set being published and the set the shard is already describing are the
    /// same one, so there is no order for it to get wrong.
    pub(crate) fn persist_manifest(&self) -> Result<()> {
        self.publish_segments(&self.segments, self.manifest_version)
    }

    /// Write a segment set's manifest durably: temp file, fsync, rename, fsync
    /// the directory.
    ///
    /// A bare `fs::write` truncates in place, so a crash halfway through leaves
    /// a manifest that will not decode — every segment file intact and the
    /// shard unable to open. The rename is what makes the switch atomic, and
    /// `atomic_write`'s directory fsync is what makes the rename survive.
    ///
    /// The delete logs go first and are durable before MANIFEST's rename is
    /// even attempted, because MANIFEST is what names the segments those logs
    /// belong to: a manifest that arrives without them describes segments whose
    /// deletes have been forgotten, and every deleted document comes back.
    ///
    /// The set is a PARAMETER, and that is the whole of the ordering property.
    /// `Shard::flush` and `Shard::install_compaction` call this with the
    /// set they are about to install and install it only if this returns `Ok`,
    /// so a publication that fails leaves the shard describing exactly what is
    /// on the disk. A publication that read `self.segments` could not be used
    /// that way: it would force its caller to move the shard first, and a
    /// caller that has moved first cannot move back — the reader it has
    /// already let in has seen a segment set no manifest records, and a failure
    /// is the moment that set stops being one a reopen can reproduce.
    ///
    /// The id counter is NOT a parameter, and the asymmetry is the point: the
    /// segment set and its version are the proposal, and the counter is already
    /// committed by the time any caller gets here — `Shard::flush` assigns it
    /// one line above the call, because the files under those ids are on the
    /// disk whatever happens next. A parameter would offer a degree of freedom
    /// that does not exist; all three call sites passed the field.
    ///
    /// What this ordering does NOT buy is worth stating beside it. Publishing
    /// is `atomic_write`'s rename followed by the fsync that makes the new
    /// name durable, so a publication that reports failure may still be the
    /// MANIFEST on the disk — the rename landed and only the fsync did not.
    /// The shard then rolls back and a later publication writes version v again
    /// for a different segment set. That is accepted rather than overlooked:
    /// `Manifest::version` is monotonic in memory, where it advances only on a
    /// publication that returned `Ok`, but the PERSISTED field is not, so it is
    /// a version and not a change token. Nothing may compare two of them across
    /// a restart to decide whether the segment set moved — the set itself is
    /// the record — and the cost of making it a token would be publishing the
    /// version before the set it names, which is the ordering this whole
    /// function exists to avoid.
    ///
    /// A file whose bytes have not changed since this shard published them is
    /// not rewritten. This is not an optimisation of a rare case — inserts land
    /// in the memtable and nothing touches the segment set until a seal, so in
    /// steady state *every* call would otherwise rewrite the same manifest, at
    /// a temp file, two fsyncs and a rename each, to say what the disk already
    /// says. Making the publication durable roughly doubles what that waste
    /// costs; not doing it is what pays for it.
    fn publish_segments(&self, segments: &[Arc<SegmentHandle>], version: u64) -> Result<()> {
        let Some(dir) = self.dir.as_ref() else { return Ok(()) };
        let ddir = dir.join("deletes");
        let mut published: Vec<(u64, Vec<u8>)> = Vec::new();
        for h in segments {
            let Some(d) = h.encode_deletes() else { continue };
            let p = ddir.join(format!("{:016x}.dlog", h.id()));
            if self.published_deletes.read().unwrap().get(&h.id()) == Some(&d)
                && still_published(&p, &d)
            {
                continue;
            }
            let written = publish_content(
                &self.opts.cipher,
                &self.file_id(&format!("{:016x}.dlog", h.id())),
                &p,
                &d,
            )?;
            published.push((h.id(), written));
        }
        if !published.is_empty() {
            // One fsync of `deletes/` for the whole batch rather than one per
            // log. They all land in the same directory, so the fsyncs after the
            // first say nothing new -- and a single `DELETE ... WHERE` spanning
            // twenty sealed segments makes twenty of them dirty at once. The
            // ordering the claim below rests on is untouched: this still
            // precedes MANIFEST's rename.
            sync_dir(&ddir)?;
            // Cached only now. Until that fsync the renames are not published,
            // and an entry recording a name a crash can still take away would
            // skip the rewrite that is the only thing that would put it back.
            let mut cache = self.published_deletes.write().unwrap();
            for (id, d) in published {
                cache.insert(id, d);
            }
        }
        // Segment ids ascend and are never reused, so a segment that has left
        // the manifest is never coming back and its entry is dead weight. A
        // publication that fails can drop an entry for a segment that is still
        // installed -- the set published was the one the caller then abandoned
        // -- and that costs one rewrite of a delete log whose bytes the disk
        // already has. Erring towards rewriting is the only direction this
        // cache may err in.
        self.published_deletes
            .write()
            .unwrap()
            .retain(|id, _| segments.iter().any(|h| h.id() == *id));
        let mut body =
            Shard::manifest_of(segments, version, self.next_segment_id, self.catchup_floor)
                .encode();
        let crc = crc32(&body);
        put_u32(&mut body, crc);
        let p = dir.join("MANIFEST");
        if self.published_manifest.read().unwrap().as_deref() == Some(body.as_slice())
            && still_published(&p, &body)
        {
            return Ok(());
        }
        let written = write_content(&self.opts.cipher, &self.file_id("MANIFEST"), &p, &body)?;
        // Only after the rename is durable: a failed write must leave the next
        // call willing to try again.
        *self.published_manifest.write().unwrap() = Some(written);
        Ok(())
    }

    /// Reopen from disk: install the manifest, then replay the WAL.
    pub(crate) fn open(
        coll: Collection,
        clock: Arc<Hlc>,
        opts: ShardOpts,
        dir: &Path,
    ) -> Result<Shard> {
        Shard::open_inner(coll, clock, opts, dir, None)
    }

    /// Reopen as it stood at `ceiling`: every log record above it is
    /// dropped and the log rewritten without them, so the copy holds what
    /// the followers had confirmed and nothing a lost holder took after.
    /// Refused when a sealed segment holds a version above the ceiling --
    /// then the copy has to start from nothing.
    pub(crate) fn open_at_most(
        coll: Collection,
        clock: Arc<Hlc>,
        opts: ShardOpts,
        dir: &Path,
        ceiling: Timestamp,
    ) -> Result<Shard> {
        Shard::open_inner(coll, clock, opts, dir, Some(ceiling))
    }

    fn open_inner(
        coll: Collection,
        clock: Arc<Hlc>,
        opts: ShardOpts,
        dir: &Path,
        ceiling: Option<Timestamp>,
    ) -> Result<Shard> {
        let mut s = Shard::new(coll, clock, opts);
        s.attach_dir(dir)?;
        // `Some` or an error, never "absent" for a file that is there and
        // cannot be read: that opened a shard with zero segments over a
        // directory full of them, and the next flush published the empty set.
        if let Some(b) =
            read_content(&s.opts.cipher, &s.file_id("MANIFEST"), &dir.join("MANIFEST"))?
        {
            if b.len() < 4 {
                return Err(Error::Storage("manifest: truncated".into()));
            }
            let (body, tail) = b.split_at(b.len() - 4);
            if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
                return Err(Error::Storage("manifest: checksum mismatch".into()));
            }
            let m = Manifest::decode(body)?;
            s.manifest_version = m.version;
            // Never below what `attach_dir` read off the disk: the manifest's
            // counter is the lowest id it would be safe to resume at if the
            // manifest were the whole record, and it is not.
            s.next_segment_id = s.next_segment_id.max(m.next_segment_id.max(1));
            s.catchup_floor = m.catchup_floor;
            for meta in &m.segments {
                // Reopen reads the footer, not the file. A shard with a hundred
                // archived segments must not pull a hundred segments' worth of
                // postings and vectors into RAM just to answer "what exists".
                let local = dir.join("segments").join(format!("{:016x}.seg", meta.id));
                let archived = dir.join("archive").join(format!("{:016x}.seg", meta.id));
                let missing = || {
                    Error::Storage(format!(
                        "segment {:016x} named by the manifest is missing",
                        meta.id
                    ))
                };
                let (p, src) = if local.exists() {
                    (Some(local.clone()), SegmentSource::File(local))
                } else if archived.exists() {
                    (Some(archived.clone()), SegmentSource::Archive(archived))
                } else if let Some(h) = s.opts.archive.clone() {
                    // Neither local copy: the object store is the only one.
                    // Its size is asked for once, here, and the footer and
                    // every component are ranged reads against it.
                    let key = s.object_key(meta.id).ok_or_else(missing)?;
                    match h.store.size(&key)? {
                        Some(size) => (None, SegmentSource::Remote { store: h.store, key, size }),
                        None => return Err(missing()),
                    }
                } else {
                    return Err(missing());
                };
                let seg = Segment::open(s.wrap_source(meta.id, src))?;
                s.adopt_segment(&seg);
                // An empty delete log is never written, so absent means no
                // deletions. Unreadable does not: it used to, and every
                // document the log recorded as deleted came back. Neither
                // does damaged, which is reported with the file's name.
                let dpath = dir.join("deletes").join(format!("{:016x}.dlog", meta.id));
                let dl = match read_content(
                    &s.opts.cipher,
                    &s.file_id(&format!("{:016x}.dlog", meta.id)),
                    &dpath,
                )? {
                    Some(d) => DeleteLog::decode(&d).map_err(|e| match e {
                        Error::Storage(m) => Error::Storage(format!("{}: {m}", dpath.display())),
                        e => e,
                    })?,
                    None => DeleteLog::new(),
                };
                s.segments.push(SegmentHandle::new(seg, dl, p));
            }
            // Inside the branch on purpose: the live set is only known where a
            // manifest was read. See [`reclaim_orphans`].
            let live: Vec<u64> = m.segments.iter().map(|meta| meta.id).collect();
            reclaim_orphans(dir, &live);
        }
        // Rotated logs first, oldest first, then the live one: a seal
        // frozen but not installed when the process ended left its rows in
        // a rotated log, and they come back into the memtable with the
        // rest; the next seal covers them all and removes the files.
        let mut rotated: Vec<PathBuf> = Vec::new();
        if let Ok(rd) = fs::read_dir(dir) {
            for e in rd.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wal.") && name.ends_with(".log") && name != "wal.log" {
                    rotated.push(e.path());
                }
            }
        }
        rotated.sort();
        for p in &rotated {
            let seq = p.file_name().and_then(|f| f.to_str()).and_then(|f| {
                f.trim_start_matches("wal.").trim_end_matches(".log").parse::<u64>().ok()
            });
            if let Some(seq) = seq {
                s.wal_seq = s.wal_seq.max(seq + 1);
            }
        }
        let mut records = Vec::new();
        for p in &rotated {
            records.extend(Wal::replay(p, &s.opts.cipher, &s.file_id("wal.log"))?);
        }
        records.extend(Wal::replay(&dir.join("wal.log"), &s.opts.cipher, &s.file_id("wal.log"))?);
        s.unsealed_wals = rotated;
        if let Some(c) = ceiling {
            for h in &s.segments {
                let above = h.segment.ordinals.commit_ts.iter().any(|t| *t > c);
                if above {
                    return Err(Error::Storage(format!(
                        "segment {:016x} holds a version above ts {c}; the copy cannot be \
                         cut there",
                        h.id()
                    )));
                }
            }
            let before = records.len();
            records.retain(|r| r.ts <= c);
            if records.len() < before {
                // The log rewritten as the kept records, rotated logs folded
                // in: what a replay of this directory finds from now on.
                let w = s.wal.as_mut().expect("attached above");
                w.truncate()?;
                for r in &records {
                    w.append(r)?;
                }
                w.sync()?;
                for p in std::mem::take(&mut s.unsealed_wals) {
                    let _ = fs::remove_file(&p);
                }
            }
        }
        for r in records {
            s.clock.observe(r.ts);
            match r.kind {
                WAL_INSERT => {
                    // A crash between writing the segments and truncating the
                    // WAL replays inserts whose documents are already sealed,
                    // so replay has to converge on the post-seal state rather
                    // than add to it.
                    //
                    // The question is whether THIS record's effect is already
                    // on disk, and the answer is "the key already carries a
                    // version at or after this record's ts". At: the seal wrote
                    // this very version. After: a later version of the key is
                    // sealed and this one was superseded before the seal — the
                    // version an unpinned seal deliberately forgets, which
                    // replay must not resurrect. Either way, re-applying the
                    // record makes the key live twice.
                    //
                    // `>=`, not `==`, for that second case, and
                    // `latest_version` rather than `locate(key, MAX_TS)` for a
                    // third: a seal under a pinned `gc_horizon` emits one
                    // segment per version layer, so the key's newest version is
                    // no longer the only one on disk. Superseding whatever is
                    // VISIBLE then wrote a tombstone onto the newest layer at
                    // the OLDEST record's timestamp — below that version's own
                    // commit — and left the older layer live beside the
                    // memtable copy it had just re-inserted, so every key read
                    // twice at exactly the horizon the pin exists to preserve.
                    //
                    // This rests on the WAL being truncated by the seal as a
                    // whole: a record whose ts is BELOW a sealed version of the
                    // same key was necessarily in the memtable when that seal
                    // ran, so the seal either wrote it into a layer or
                    // collapsed it on purpose. There is no third case.
                    let prev = s.latest_version(&r.key);
                    if prev.map(|(_, ts)| ts >= r.ts).unwrap_or(false) {
                        continue;
                    }
                    // A genuine post-seal write: supersede the newest version,
                    // which is now strictly older than this record.
                    if let Some((loc, _)) = prev {
                        s.mark_superseded(loc, r.ts);
                    }
                    if let Some(d) = r.doc {
                        // Into the live view and the unsealed tally, exactly
                        // as the insert that wrote the record did. The
                        // persisted catalog did not count this record: it
                        // persists sealed documents only, so that this
                        // observation is the record's first and not its
                        // second. See `Shard::sealed`.
                        s.coll.observe_doc(&d);
                        s.unsealed.observe_doc(&d);
                        s.memtable.insert(r.key, r.ts, d)?;
                    }
                }
                WAL_DELETE => {
                    if let Some(loc) = s.locate(&r.key, MAX_TS) {
                        s.mark_superseded(loc, r.ts);
                    }
                }
                WAL_SHIP_MARK => {
                    s.ship_ts = s.ship_ts.max(r.ts);
                    s.caught_up = true;
                }
                _ => {}
            }
        }
        if let Some(c) = ceiling {
            // Cut: caught up to the ceiling and no further, whatever the
            // marks said, and the file says the same for the next open.
            s.ship_ts = c;
            s.caught_up = true;
            s.persist_ship_mark()?;
        } else if let Some(mark) = read_optional(&dir.join("SHIPPED"))?
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|t| t.trim().parse::<Timestamp>().ok())
        {
            s.ship_ts = s.ship_ts.max(mark);
            s.caught_up = true;
        }
        Ok(s)
    }

    /// A followed copy from nothing: its files gone, its memory empty, the
    /// directory made again with the same range; what a catch-up from
    /// nothing begins with.
    pub(crate) fn reset_copy(&mut self) -> Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Err(Error::Storage("a followed copy needs a directory".into()));
        };
        let range = self.key_range.clone();
        let range_bytes = crate::shard::read_optional(&dir.join("RANGE"))?;
        self.retire_all();
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        if let Some(b) = range_bytes {
            atomic_write(&dir.join("RANGE"), &b)?;
        }
        let fresh = Shard::new(self.coll.clone(), self.clock.clone(), self.opts.clone());
        let index = self.index;
        *self = fresh;
        self.index = index;
        if let Some((lo, hi)) = range {
            self.set_key_range(lo, hi);
        }
        self.attach_dir(&dir)?;
        Ok(())
    }

    /// A batch of the holder's log into this followed copy: each record
    /// logged here and applied as a replay applies it, the batch's mark
    /// logged last, one sync for all of it. Idempotent: a version this
    /// copy already holds at that instant or later is skipped, as a replay
    /// skips it. Returns where the copy stands.
    pub(crate) fn apply_shipped(
        &mut self,
        items: &[crate::replication::ShipItem],
    ) -> Result<(bool, Timestamp)> {
        use crate::replication::{SHIP_CAUGHT_UP, SHIP_DELETE, SHIP_INSERT, SHIP_MARK};
        let mut mark_ts = 0;
        let mut caught_up_at = None;
        let Some(w) = self.wal.as_mut() else {
            return Err(Error::Storage("a followed copy needs a directory".into()));
        };
        let mark = w.mark()?;
        let mut applied: Vec<WalRecord> = Vec::new();
        let written: Result<()> = 'log: {
            for it in items {
                match it.kind {
                    SHIP_INSERT | SHIP_DELETE => {
                        let record = WalRecord {
                            kind: if it.kind == SHIP_INSERT { WAL_INSERT } else { WAL_DELETE },
                            key: it.key.clone(),
                            ts: it.ts,
                            doc: it.doc.clone(),
                            supersedes: true,
                            segment_id: 0,
                        };
                        if let Err(e) = w.append(&record) {
                            break 'log Err(e);
                        }
                        applied.push(record);
                        mark_ts = mark_ts.max(it.ts);
                    }
                    SHIP_MARK => mark_ts = mark_ts.max(it.ts),
                    SHIP_CAUGHT_UP => {
                        mark_ts = mark_ts.max(it.ts);
                        caught_up_at = Some(it.ts);
                    }
                    _ => {}
                }
            }
            if mark_ts > 0 && (caught_up_at.is_some() || self.caught_up) {
                let m = WalRecord {
                    kind: WAL_SHIP_MARK,
                    key: String::new(),
                    ts: mark_ts,
                    doc: None,
                    supersedes: false,
                    segment_id: 0,
                };
                if let Err(e) = w.append(&m) {
                    break 'log Err(e);
                }
            }
            w.sync()
        };
        if let Err(e) = written {
            let _ = w.rollback(mark);
            return Err(e);
        }
        for r in applied {
            self.clock.observe(r.ts);
            match r.kind {
                WAL_INSERT => {
                    let prev = self.latest_version(&r.key);
                    if prev.map(|(_, ts)| ts >= r.ts).unwrap_or(false) {
                        continue;
                    }
                    if let Some((loc, _)) = prev {
                        self.mark_superseded(loc, r.ts);
                    }
                    if let Some(d) = r.doc {
                        self.coll.observe_doc(&d);
                        self.unsealed.observe_doc(&d);
                        self.memtable.insert(r.key, r.ts, d)?;
                    }
                }
                WAL_DELETE => {
                    if let Some(loc) = self.locate(&r.key, MAX_TS) {
                        self.mark_superseded(loc, r.ts);
                    }
                }
                _ => {}
            }
        }
        if let Some(at) = caught_up_at {
            self.caught_up = true;
            self.ship_ts = self.ship_ts.max(at);
        } else if self.caught_up {
            self.ship_ts = self.ship_ts.max(mark_ts);
        }
        self.seal_if_due();
        Ok((self.caught_up, self.ship_ts))
    }

    /// The rows written after `from` and by `upto`, as the copy at `upto`
    /// shows them, in key order past `cursor`, at most `limit`: what a
    /// follower catching up is shipped, a chunk at a time. Returns the
    /// items and the cursor for the next chunk, or none when exhausted.
    pub(crate) fn changes_since(
        &self,
        from: Timestamp,
        cursor: Option<&str>,
        upto: Timestamp,
        limit: usize,
    ) -> Result<(Vec<crate::replication::ShipItem>, Option<String>)> {
        let snap = self.snapshot_at(upto);
        let sources = self.sources(&snap);
        let mut found: Vec<(String, Timestamp, usize, u32)> = Vec::new();
        for (si, src) in sources.iter().enumerate() {
            let vis = src.visibility(upto);
            for ord in vis.iter() {
                let Some(key) = src.key(ord) else { continue };
                if cursor.is_some_and(|c| key <= c) {
                    continue;
                }
                let ts = match src {
                    Searchable::Mem(m) => m.ordinals.commit_ts[ord as usize],
                    Searchable::Seg(h) => h.segment.ordinals.commit_ts[ord as usize],
                };
                if ts > from {
                    found.push((key.to_string(), ts, si, ord));
                }
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0));
        let more = found.len() > limit;
        found.truncate(limit);
        let mut items = Vec::with_capacity(found.len());
        for (key, ts, si, ord) in &found {
            let doc = match &sources[*si] {
                Searchable::Mem(m) => m.docs[*ord as usize].doc.clone(),
                Searchable::Seg(h) => h.segment.document(*ord)?,
            };
            items.push(crate::replication::ShipItem {
                kind: crate::replication::SHIP_INSERT,
                key: key.clone(),
                ts: *ts,
                doc: Some(doc),
            });
        }
        let next = if more { found.last().map(|f| f.0.clone()) } else { None };
        Ok((items, next))
    }

    /// The deletes after `from` this shard still remembers, for a follower
    /// catching up from an instant it stood at.
    pub(crate) fn deletes_since(&self, from: Timestamp) -> Vec<crate::replication::ShipItem> {
        let mut out = Vec::new();
        let mut push = |key: &str, ts: Timestamp| {
            if ts > from && ts != MAX_TS {
                out.push(crate::replication::ShipItem {
                    kind: crate::replication::SHIP_DELETE,
                    key: key.to_string(),
                    ts,
                    doc: None,
                });
            }
        };
        for (ord, ts) in self.memtable.delete_entries() {
            if let Some(d) = self.memtable.docs.get(ord as usize) {
                push(&d.sort_key, ts);
            }
        }
        for f in &self.frozen {
            for (ord, ts) in f.delete_entries() {
                if let Some(d) = f.docs.get(ord as usize) {
                    push(&d.sort_key, ts);
                }
            }
        }
        for h in &self.segments {
            let log = h.deletes.read().unwrap();
            for (ord, ts) in log.iter() {
                if let Some(k) = h.segment.ordinals.key(ord) {
                    push(k, ts);
                }
            }
        }
        out
    }

    /// Replace `inputs` with `outputs` atomically from a reader's point of
    /// view: the manifest version moves in one step, and readers already
    /// holding the old handles keep them alive through their `Arc`s.
    pub(crate) fn install_compaction(
        &mut self,
        input_ids: &[u64],
        outputs: Vec<Segment>,
        carried_deletes: &[(String, Timestamp, Timestamp)],
        retain_from: Timestamp,
    ) -> Result<()> {
        // What this compaction forgets: every delete at or before the
        // horizon in an input is dropped with its row (`collect_from_handles`
        // skips the row), and a follower that stood before the newest of
        // them has to start from nothing.
        let mut forgotten = 0;
        if input_ids.is_empty() && !outputs.is_empty() {
            // An absorb (a merge's rows from the shard that goes away):
            // rows with timestamps of their own, older than any follower's
            // stand, which no catch-up from there would carry.
            forgotten = retain_from;
        }
        for h in self.segments.iter().filter(|h| input_ids.contains(&h.id())) {
            let log = h.deletes.read().unwrap();
            for (_, ts) in log.iter() {
                if ts != crate::time::MAX_TS && ts <= retain_from {
                    forgotten = forgotten.max(ts);
                }
            }
        }
        self.catchup_floor = self.catchup_floor.max(forgotten);
        let mut handles = Vec::new();
        for seg in outputs {
            self.adopt_segment(&seg);
            let path = self.persist_segment(&seg)?;
            let h = SegmentHandle::new(seg, DeleteLog::new(), path);
            self.mask_handle(&h);
            for (key, version_ts, delete_ts) in carried_deletes {
                if let Some(ord) = h.segment.ordinals.find(key) {
                    if h.segment.ordinals.commit_ts[ord as usize] == *version_ts {
                        h.mark_deleted(ord, *delete_ts);
                    }
                }
            }
            handles.push(h);
        }
        // The set this compaction is proposing, in a local: the survivors of
        // the input list, plus the outputs, in id order.
        let mut next: Vec<Arc<SegmentHandle>> =
            self.segments.iter().filter(|h| !input_ids.contains(&h.id())).cloned().collect();
        next.extend(handles);
        next.sort_by_key(|h| h.id());
        let version = self.manifest_version + 1;

        // The publication, and the last thing here that can fail. A compaction
        // that swapped the set first and published afterwards lost its inputs
        // twice over when the publication failed: readers were on a merged set
        // no manifest recorded, and the input handles — dropped on the way out
        // of `retain`, with `retiring` only reached below the `?` — were gone
        // from the one list that unlinks files, while the manifest on the disk
        // still named them. Nothing afterwards could retire what the shard no
        // longer had, so both files stayed for the life of the database.
        self.publish_segments(&next, version)?;

        // The commit point: the merged, collected set is reader-visible from
        // here, and recorded before here. The floor moves with it — raising it
        // in `run`, next to `collect_for_compaction`, would claim a collection
        // that a later `build` error could still abandon, and raising it above
        // the publication would claim one that a failed publication abandons.
        // Both of `run`'s call sites come through here, including the one that
        // installs no outputs at all — which still collected.
        let removed: Vec<Arc<SegmentHandle>> =
            self.segments.iter().filter(|h| input_ids.contains(&h.id())).cloned().collect();
        self.segments = next;
        self.retain_floor = self.retain_floor.max(retain_from);
        self.manifest_version = version;
        self.compactions += 1;
        self.retiring.extend(removed);
        self.sweep_retired();
        Ok(())
    }

    /// Retire every segment at once: what a dropped collection's shard does
    /// before it is dropped, so its files and any objects it put in the store
    /// are removed by the same sweep a compaction uses rather than by a
    /// second deleter that would have to know the same things.
    pub(crate) fn retire_all(&mut self) {
        let all = std::mem::take(&mut self.segments);
        self.retiring.extend(all);
        self.sweep_retired();
    }

    /// Unlink the files of retired segments no reader still holds.
    ///
    /// `Arc::strong_count == 1` means this list is the last owner. A segment
    /// whose reader is still alive stays on the list and is swept next time —
    /// checking once at compaction and then forgetting leaks the file forever.
    pub(crate) fn sweep_retired(&mut self) {
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
            // A retired segment that lived in the object store is deleted
            // there; nothing else ever will, since orphans are reclaimed
            // only where a directory can be listed.
            if let SegmentSource::Remote { store, key, .. } = h.segment.source().unwrapped().clone()
            {
                let _ = store.delete(&key);
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
    pub(crate) fn collect_for_compaction(
        &self,
        ids: &[u64],
        retain_from: Timestamp,
    ) -> Result<(Vec<PendingDoc>, Vec<CarriedDelete>)> {
        collect_from_handles(&self.segments, ids, retain_from)
    }
}

/// The live rows of the segments `ids` name among `handles`, and the
/// deletes to carry: what a compaction reads, from the handles alone, so a
/// build can run with no lock on the shard (`compaction::build`).
pub(crate) fn collect_from_handles(
    handles: &[Arc<SegmentHandle>],
    ids: &[u64],
    retain_from: Timestamp,
) -> Result<(Vec<PendingDoc>, Vec<CarriedDelete>)> {
    {
        let mut docs = Vec::new();
        let mut deletes = Vec::new();
        for h in handles.iter().filter(|h| ids.contains(&h.id())) {
            let log = h.deletes.read().unwrap();
            let n = h.segment.num_docs();
            // Outside the shard's range since a split: dropped here, which
            // is how a split's rows leave the shard that shrank.
            let (lo, hi) = h.mask().unwrap_or((0, n as u32));
            for ord in lo..hi {
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
}

impl Shard {
    /// The identity a file of this shard is encrypted under: the shard's
    /// directory name and the file's, so a segment keeps its key whether it
    /// sits in `segments/`, `archive/` or the store, and a file cannot
    /// stand in for another.
    pub(crate) fn file_id(&self, name: &str) -> String {
        let shard = self
            .dir
            .as_ref()
            .and_then(|d| d.file_name())
            .and_then(|f| f.to_str())
            .unwrap_or("shard");
        format!("{shard}/{name}")
    }

    fn segment_file_id(&self, id: u64) -> String {
        self.file_id(&format!("{id:016x}.seg"))
    }

    /// `src`, framed under the shard's cipher when it has one.
    fn wrap_source(&self, id: u64, src: SegmentSource) -> SegmentSource {
        match &self.opts.cipher {
            Some(c) => SegmentSource::Encrypted {
                inner: Box::new(src),
                cipher: c.clone(),
                id: self.segment_file_id(id),
            },
            None => src,
        }
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

    /// Terms beginning with `prefix` that at least one document LIVE AT `t`
    /// holds, unioned into `out` across every searchable unit in the snapshot.
    /// `key_prefix` narrows "live" further to one partition, for a statement
    /// that names one.
    ///
    /// This is a dictionary read rather than a statistic — it answers which
    /// terms the query names, and `Shard::term_stats` then measures how many
    /// live documents hold each of them — but it is a read of the LIVE
    /// dictionary, and the difference is not cosmetic. A term whose every
    /// posting is dead does contribute a cursor that matches nothing; what it
    /// also does, once `limit` binds, is DISPLACE a live term out of the cap.
    /// Which dead terms a unit is still carrying is a seal and compaction
    /// decision, so an unmasked enumeration puts the layout back into the
    /// answer through the one door the cap leaves open — measured at 12 rows
    /// against 512 for the same collection at two shard counts, and at 0 rows
    /// against 400 before and after a `COMPACT` at one.
    ///
    /// Masking has to happen INSIDE the enumeration, not to its result: the cap
    /// counts live terms, so a dead run is stepped over rather than paid for.
    /// See [`TextSource::live_terms_with_prefix`](crate::text::TextSource::live_terms_with_prefix).
    ///
    /// Each unit is asked for its own first `limit` LIVE terms, and the union
    /// of those, cut at `limit`, is EXACTLY the first `limit` live matching
    /// terms of the collection — or, with `key_prefix`, of the partition the
    /// statement named. Either way it is a property of the DATA and not of the
    /// layout, which is the property that matters; the argument below runs over
    /// whichever live set was selected, since "live" is one predicate applied
    /// identically at both levels. It is monotonicity, not a coincidence, and
    /// the cap makes it non-obvious enough to write down:
    ///
    /// * live in some unit at `t` implies live globally at `t`, because the
    ///   global `df` is the sum of the per-unit visibility-masked `df`s taken
    ///   at that same `t`; and a globally live term is live in at least one
    ///   unit. The predicate never disagrees with itself across the two levels.
    /// * COMPLETENESS: let `x` be among the `limit` smallest globally live
    ///   matching terms. `x` is live in some unit `U`; the terms live-in-`U`
    ///   that precede `x` are a subset of the globally live terms that precede
    ///   it, of which there are fewer than `limit`. So `x` is inside `U`'s
    ///   first `limit` and reaches the union.
    /// * SOUNDNESS: every term in the union is live in the unit that emitted
    ///   it, hence live globally. The union holds no dead term at all.
    ///
    /// A term live in one unit and dead in another is handled by that, and the
    /// union is what handles it: the unit where it is dead returns nothing and
    /// spends no budget on it, the unit where it is live returns it, and every
    /// unit — including the first — then compiles a cursor for it. That
    /// cursor matches no visible document there, which is correct and costs one
    /// cursor open.
    ///
    /// Both the mask and the gather read the same `t`, which is why `vis` comes
    /// from `s.visibility(t)` here and from nowhere else. A liveness test taken
    /// at a different instant, or one that is not the predicate the gather
    /// uses, breaks the argument above.
    ///
    /// `text_handle` is the FALLIBLE spelling for the same reason
    /// `Shard::term_stats` uses it: an archived segment configured to refuse
    /// reads must surface the refusal, not look like a path carrying no index
    /// and silently drop its terms out of the expansion. A unit holding
    /// nothing visible at `t` is skipped before that call and so does not
    /// surface its refusal — which is the same answer either way, since the
    /// terms it could refuse to name are terms it holds no visible document
    /// for.
    pub(crate) fn prefix_terms(
        &self,
        path: &str,
        prefix: &str,
        t: Timestamp,
        limit: usize,
        key_prefix: Option<&str>,
        out: &mut BTreeSet<String>,
    ) -> Result<()> {
        let snap = self.snapshot_at(t);
        for s in self.sources(&snap) {
            // Once per unit, not once per term. For a segment this is the
            // cached bitmap `Shard::term_stats` reads at the same `t`; for
            // the memtable it is the same O(n) build the gather already pays
            // on every query.
            let mut vis = s.visibility(t);
            // AND in the statement's partition, when it names one. Narrowing
            // the live set cannot drop a term the answer needs: the partition
            // prefix restricts every row the statement can return, so a term
            // held by no document inside it contributes nothing — and what it
            // does otherwise is DISPLACE one that does, out of a cap the
            // statement then spends on another tenant's vocabulary. It costs
            // the fast path in `live_terms_with_prefix` on any unit holding
            // more than one partition, which is the same trade the liveness
            // mask itself already makes.
            if let Some(kp) = key_prefix {
                vis.and_inplace(&s.key_prefix(kp));
            }
            // Before the handle, not after. A unit with nothing visible at `t`
            // can contribute no term — `live_terms_with_prefix` over an empty
            // mask yields nothing — so the whole unit is skippable, and
            // skipping it before `text_handle` avoids decoding its dictionary.
            // That is not a micro-optimisation: a seal under a pinned
            // `gc_horizon` emits one segment per version layer, so a hot key
            // updated n times leaves n units of which all but one are entirely
            // invisible at any single `t`, and the walk below is per unit.
            if vis.popcount() == 0 {
                continue;
            }
            let handle = s.text_handle(path)?;
            if let Some(src) = handle.as_ref().and_then(|h| h.source(path)) {
                for term in src.live_terms_with_prefix(prefix, limit, &vis) {
                    out.insert(term);
                }
            }
        }
        Ok(())
    }

    /// Local document frequencies for a set of terms, gathered across every
    /// searchable unit. This is the exact two-phase gather of §8.2 — in a
    /// cluster it is a broadcast; here it is a loop, but it is the same
    /// quantity and the same guarantee.
    ///
    /// All three numbers are masked by visibility at `t`, so for `t >=`
    /// `Shard::retain_floor` the triple is a pure function of the keys live
    /// at `t`, their field lengths and their postings: independent of how many
    /// physical versions or tombstones are resident, of when any shard sealed,
    /// and of whether a compaction has run. Below the floor it stays
    /// best-effort in exactly the sense [`Shard::get`] documents — a
    /// compaction has physically dropped versions dead before the horizon and
    /// their lengths went with them — and a pinned `gc_horizon` holds the
    /// floor back and keeps those reads exact too.
    ///
    /// `terms` is walked element by element into one `df` map, so a repeated
    /// element is counted once per occurrence: this takes a set, and the
    /// caller owes it one. `Db::gather_stats`, which is the entry point for
    /// every query, deduplicates before it gets here — the repeat is not a
    /// supported spelling, it is a double count, and `df > num_docs` follows
    /// from it. Deduplicating is the caller's job rather than this function's
    /// because this one is per shard and runs once per shard per query, while
    /// the caller does it once.
    pub(crate) fn term_stats(
        &self,
        path: &str,
        terms: &[String],
        t: Timestamp,
    ) -> Result<(u64, u64, BTreeMap<String, u64>)> {
        #[cfg(test)]
        self.terms_gathered.fetch_add(terms.len() as u64, AtomicOrdering::Relaxed);
        let snap = self.snapshot_at(t);
        let mut df: BTreeMap<String, u64> = BTreeMap::new();
        let mut total_len = 0u64;
        let mut ndocs = 0u64;
        for s in self.sources(&snap) {
            let vis = s.visibility(t);
            ndocs += vis.popcount() as u64;
            // A unit with nothing visible at `t` contributes zero to all three
            // numbers — no documents, `visible_doc_len` over an empty mask is
            // zero, and the `df` loop counts only visible postings — so skip
            // it before paying for its dictionary. See `Shard::prefix_terms`
            // for why a shard can hold many such units at once.
            if vis.popcount() == 0 {
                continue;
            }
            // Not `unwrap_or_default()`: an archived segment configured to
            // refuse reads would then look like a path with no text index, and
            // its documents would count towards `ndocs` with zero document
            // frequency — an inflated IDF and a silently mis-ranked answer in
            // place of the refusal the operator asked for. The skip above is
            // not that hole: a unit it skips added nothing to `ndocs` either,
            // so there is no inflated denominator to go with the missing
            // frequencies.
            let handle = s.text_handle(path)?;
            if let Some(src) = handle.as_ref().and_then(|h| h.source(path)) {
                // Sum only visible lengths, for the same reason the
                // `doc_freq` loop below counts only visible postings: this is
                // the numerator of an average whose denominator is
                // `vis.popcount()`. Reading the source-wide sum instead
                // divides physical rows by live ones, and the quotient moves
                // whenever a seal drops a superseded version or a compaction
                // drops a dead one — which happens at each shard's own
                // threshold, so it moves with the shard count.
                //
                // The denominator is still diluted: `ndocs` counts every
                // visible document in the unit, including those carrying no
                // text on `path`, for which this now contributes zero. So
                // `avgdl` is the average over the collection, not over the
                // documents that have this field.
                total_len += src.visible_doc_len(&vis);
                for term in terms {
                    // FALLIBLE, for the same reason `text_handle` above is. An
                    // extent the dictionary says exists but that does not
                    // decode is corruption, and `cursor` cannot tell that from
                    // "this unit does not hold the term": answering `df = 0`
                    // both inflates this term's IDF against its neighbours and
                    // gets cached, so every later query in the epoch reads the
                    // zero — including queries that never touch the damaged
                    // unit. `live_terms_with_prefix` lets an unreadable extent
                    // through to the gather precisely so the error is raised
                    // here.
                    if let Some(mut c) = src.try_cursor(term)? {
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

/// The first index at or after `from` whose key is not below `target`, or
/// `keys.len()`: a gallop from `from` -- steps doubling until one lands at
/// or past the target -- then a binary search inside the last step. A run
/// of targets that all fall between two keys costs a comparison each, and a
/// target far ahead costs the logarithm of the distance, not of the segment.
fn gallop(keys: &[String], from: usize, target: &str) -> usize {
    let n = keys.len();
    if from >= n || keys[from].as_str() >= target {
        return from;
    }
    let (mut lo, mut step) = (from, 1usize);
    let hi = loop {
        let hi = lo.saturating_add(step).min(n);
        if hi == n || keys[hi].as_str() >= target {
            break hi;
        }
        lo = hi;
        step *= 2;
    };
    lo + 1 + keys[lo + 1..hi].partition_point(|k| k.as_str() < target)
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_manifest_and_wal_never_panic() {
        let m = Manifest {
            version: 3,
            catchup_floor: 0,
            segments: vec![SegmentMeta {
                id: 1,
                level: 0,
                num_docs: 10,
                num_vectors: 4,
                min_key: "a".into(),
                max_key: "z".into(),
            }],
            next_segment_id: 2,
        };
        let mut manifest = m.encode();
        put_u32(&mut manifest, crc32(&m.encode()));
        crate::fuzz::sweep(31, &[manifest], 6000, |b| {
            let _ = Manifest::decode(b);
        });
        let dir = std::env::temp_dir().join(format!("celastro-fuzz-wal-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        {
            let mut w = Wal::open(&log, None, "t/wal.log".into()).unwrap();
            for i in 0..5u64 {
                w.append(&WalRecord {
                    kind: (i % 2) as u8,
                    key: format!("k{i}"),
                    ts: 100 + i,
                    doc: Some(
                        crate::json::parse(&format!(
                            r#"{{"id":"k{i}","n":{i},"t":["a",{{"b":null}}]}}"#
                        ))
                        .unwrap(),
                    ),
                    supersedes: i % 2 == 1,
                    segment_id: i,
                })
                .unwrap();
            }
        }
        let sample = fs::read(&log).unwrap();
        let mutant = dir.join("mutant.log");
        crate::fuzz::sweep(32, &[sample], 3000, |b| {
            fs::write(&mutant, b).unwrap();
            let _ = Wal::replay(&mutant, &None, "t/wal.log");
        });
        let _ = fs::remove_dir_all(&dir);
    }
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

    /// A directory of this test's own, under a name no other test can build.
    ///
    /// The process id is not enough on its own, and the way that failed is
    /// worth keeping: libtest runs these as threads of ONE process, so two
    /// tests that picked the same label shared a directory, and each one's
    /// opening `remove_dir_all` deleted the other's live shard. It reported
    /// itself as a durability failure in a test that was working perfectly,
    /// which is the most expensive shape a flaky test can take.
    ///
    /// So the name is unique per CALL rather than per label: a duplicated
    /// label -- which is what the next person to copy a test will write --
    /// cannot collide any more, and the label is left as a hint for whoever is
    /// looking at the leftovers in `/tmp`. The removal is here too, because a
    /// previous run of this binary can have had the same pid.
    fn test_dir(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, AtomicOrdering::Relaxed);
        let d = std::env::temp_dir().join(format!("celastro-{label}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn a_merged_liveness_check_agrees_with_a_lookup_per_key_under_updates_and_deletes() {
        // Versions of one key spread over two segments and the memtable --
        // updated across a flush, deleted before one, deleted after its
        // update -- and keys nothing holds, so that a merge taking a
        // segment's dead version for a hit, or stopping at the first
        // segment, or losing its place after a miss, disagrees with
        // `contains` somewhere.
        let mut s = shard();
        for i in 0..60 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap();
        s.flush().unwrap();
        let t_first = s.clock.peek();
        for i in 30..90 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t1{KEY_SEP}d0031")).unwrap();
        s.flush().unwrap();
        for i in 80..100 {
            s.insert(doc(i)).unwrap();
        }
        s.delete(&format!("t2{KEY_SEP}d0095")).unwrap();
        assert_eq!(s.segments.len(), 2);
        assert!(!s.memtable.is_empty());
        let mut keys: Vec<String> =
            (0..110).map(|i| format!("t{}{KEY_SEP}d{i:04}", i % 3)).collect();
        keys.push(format!("t9{KEY_SEP}d0000"));
        keys.push(String::new());
        keys.sort();
        keys.dedup();
        for t in [t_first, s.clock.peek()] {
            let flags = s.present_sorted(&keys, t);
            assert_eq!(flags.len(), keys.len());
            for (k, f) in keys.iter().zip(&flags) {
                assert_eq!(*f, s.contains(k, t), "key {k:?} at {t:?}");
            }
        }
        let now = s.clock.peek();
        let at = |k: &str| keys.iter().position(|x| x == k).unwrap();
        let flags = s.present_sorted(&keys, now);
        assert!(!flags[at(&format!("t0{KEY_SEP}d0003"))], "deleted before the first flush");
        assert!(!flags[at(&format!("t1{KEY_SEP}d0031"))], "deleted after its update");
        assert!(flags[at(&format!("t1{KEY_SEP}d0040"))], "updated across the flush");
        assert!(flags[at(&format!("t2{KEY_SEP}d0089"))], "updated across the flush");
        assert!(flags[at(&format!("t0{KEY_SEP}d0099"))], "in the memtable");
        assert!(!flags[at(&format!("t2{KEY_SEP}d0095"))], "deleted in the memtable");
        assert!(!flags[at(&format!("t0{KEY_SEP}d0105"))], "never inserted");
        assert!(
            !flags[at("")] && !flags[at(&format!("t9{KEY_SEP}d0000"))],
            "below and above every key"
        );
        assert_eq!(flags.iter().filter(|f| **f).count(), 97);
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
        d.set_path("body", Value::Str("rewritten".into())).unwrap();
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

    /// A followed copy's caught-up instant survives the seal that
    /// truncates the log its marks were in: reopened after a seal with no
    /// write since, the copy stands where it stood, not "not caught up"
    /// -- which had every copy of a restarted cluster copied from nothing.
    #[test]
    fn a_copys_ship_mark_survives_a_seal_and_a_reopen() {
        use crate::replication::{ShipItem, SHIP_CAUGHT_UP, SHIP_INSERT};
        let dir = std::env::temp_dir().join(format!("celastro-shipmark-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        let mut items: Vec<ShipItem> = (0..20u64)
            .map(|i| ShipItem {
                kind: SHIP_INSERT,
                key: format!("t0{KEY_SEP}d{i:05}"),
                ts: 100 + i,
                doc: Some(doc(i as usize)),
            })
            .collect();
        items.push(ShipItem { kind: SHIP_CAUGHT_UP, key: String::new(), ts: 500, doc: None });
        let (caught_up, at) = s.apply_shipped(&items).unwrap();
        assert!(caught_up && at == 500);
        s.flush().unwrap();
        drop(s);
        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert!(s.caught_up, "the reopened copy forgot it was caught up");
        assert_eq!(s.ship_ts, 500);
        assert_eq!(s.num_docs(u64::MAX), 20);
        let _ = std::fs::remove_dir_all(&dir);
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
        // The same re-resolution as above, but under a pin, where one seal
        // emits one segment per version layer and the tombstone has to find its
        // row in whichever of them the version landed in. Resolving by key
        // alone marks a *different* version dead.
        //
        // The UPDATE is what makes that reachable, and the test used to lack
        // it: thirty distinct keys and one delete gives every key one version,
        // so `layer_by_version` produced one layer and the pinned seal emitted
        // one segment — the multi-segment case the name claims, structurally
        // out of reach, with assertions the single-segment test above already
        // makes. Dropping the `commit_ts == version_ts` guard from `flush`'s
        // re-resolution failed five tests and never this one.
        //
        // Note where the update goes: superseded AFTER the horizon, or the
        // pinned drain finds the old version already dead at the pin, collects
        // it, and the seal is single-segment again.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        let key = format!("t2{KEY_SEP}d0029");
        let upd = format!("t1{KEY_SEP}d0007");
        let horizon = s.clock.peek();
        s.delete(&key).unwrap();
        let mut updated = doc(7);
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
        s.insert(updated).unwrap();
        s.opts.gc_horizon = horizon;
        s.flush().unwrap();
        let t = s.clock.peek();
        assert_eq!(s.segments.len(), 2, "one segment per retained version layer");
        assert!(s.get(&key, t).unwrap().is_none());
        // The renumbered ordinal was matched by version, not by key: the row
        // is still there and the reader at the horizon still reads it.
        assert!(s.get(&key, horizon).unwrap().is_some());
        // And the update, which is the leg only a multi-segment seal has. Its
        // tombstone names the OLDER version; resolved by key alone it lands on
        // the survivor in the newer segment and the document disappears.
        let got = s.get(&upd, t).unwrap().expect("the surviving version was marked dead");
        assert_eq!(got.path("body").unwrap().as_str(), Some("rewritten"));
        assert!(s.get(&upd, horizon).unwrap().is_some(), "the horizon lost the old version");
        assert_eq!(s.num_docs(t), 29);
        assert_eq!(s.num_docs(horizon), 30);
        // Two tombstones survive the seal, summed over the segments it emitted:
        // the delete, and the one the update wrote against the version the pin
        // is still holding.
        assert_eq!(s.segments.iter().map(|h| h.dead_count(t)).sum::<usize>(), 2);
    }

    #[test]
    fn an_unpinned_flush_does_not_move_the_scoring_statistics() {
        // `term_stats` masks both halves of avgdl's quotient by visibility, so
        // `before == after` here says the statistics are a function of the
        // live corpus and not of what happens to be resident. It used to say
        // something weaker and wrong: the numerator was summed over *physical*
        // rows, so the equality held only because a seal is forbidden to drop
        // the tombstoned row — a rule about collection standing in for a
        // property of the statistic. `before.1` counted the deleted document's
        // length; it no longer does.
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
        assert_eq!(before.1, 145, "and its length goes with it: 29 bodies of five terms");
        assert_eq!(before, after, "the seal moved the scoring statistics");
    }

    #[test]
    fn an_unpinned_seal_of_an_update_emits_exactly_one_segment() {
        // The other half of the rule above, and the half only an update
        // reaches: with nothing pinned the seal keeps layer 0 and nothing else,
        // so it emits one segment holding the newest version of every key —
        // exactly the dedup `SegmentBuilder::build` always did. Keeping the
        // superseded version instead splits the seal in two, which is a thing
        // only a pin may ask for.
        //
        // `before.1` used to be 151 against an `after.1` of 146: the memtable
        // physically held both versions of the updated document, the seal
        // dropped the superseded one, and the numerator of BM25's avgdl fell
        // by five for the whole collection. That five-point step was written
        // down as correct, and it was the bug — the dead version was invisible
        // at the very snapshot the number was gathered at. Masked, the
        // superseded version contributes nothing before the seal either, so
        // the two agree and the seal moves nothing at all.
        let mut s = shard();
        for i in 0..30 {
            s.insert(doc(i)).unwrap();
        }
        let mut updated = doc(1);
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
        s.insert(updated).unwrap();
        assert_eq!(s.opts.gc_horizon, 0, "nothing is pinned");
        let terms = ["document".to_string()];
        let before = s.term_stats("body", &terms, s.clock.peek()).unwrap();
        assert_eq!(before.1, 146, "the superseded version is already invisible");

        s.flush().unwrap();
        assert_eq!(s.segments.len(), 1, "an unpinned seal is one segment, always");
        let after = s.term_stats("body", &terms, s.clock.peek()).unwrap();
        assert_eq!(after.0, 30, "the update superseded a document, it did not add one");
        assert_eq!(after.1, 146, "29 bodies of five terms and the one-term rewrite");
        assert_eq!(after, before, "and the seal moved nothing");
    }

    #[test]
    fn the_length_numerator_matches_a_brute_force_fold_at_every_snapshot() {
        // The property the two constants above are instances of, and the only
        // pin in this file that covers *historical* reads. At every timestamp
        // a write landed at — and either side of it — `term_stats`'s length
        // sum must equal a fold of `doc_lens` over exactly the ordinals
        // `visibility(t)` marks live. That is what the numerator of avgdl is
        // defined to be, and it has to survive a seal and a compaction
        // unchanged, so the fold is re-run after each with the horizon pinned
        // below every write: below the retain floor a compaction has already
        // dropped rows and the equality is only best-effort, and pinning is
        // how that qualifier is taken off the table.
        fn fold(s: &Shard, t: Timestamp) -> u64 {
            let snap = s.snapshot_at(t);
            let mut total = 0u64;
            for unit in s.sources(&snap) {
                let vis = unit.visibility(t);
                let handle = unit.text_handle("body").unwrap();
                if let Some(src) = handle.as_ref().and_then(|h| h.source("body")) {
                    let lens = src.doc_lens();
                    for o in vis.iter() {
                        total += lens.get(o as usize).copied().unwrap_or(0) as u64;
                    }
                }
            }
            total
        }

        let mut s = shard();
        let pin = s.clock.peek().max(1);
        let mut rng = crate::codec::Rng::new(7);
        let mut stamps: Vec<Timestamp> = Vec::new();
        let mut live: Vec<usize> = Vec::new();
        // Counted, not inferred. `live.len() < 120` would be satisfied by
        // deletes alone — the update arm pushes nothing onto `live` — so it
        // cannot see whether either arm was taken, and a change to `Rng` or to
        // the branch weights could silently empty one of them while the guard
        // stayed green.
        let (mut ndel, mut nupd) = (0usize, 0usize);
        for i in 0..120usize {
            match rng.next_u64() % 4 {
                0 if live.len() > 4 => {
                    let k = live.remove(rng.next_u64() as usize % live.len());
                    s.delete(&format!("t{}{KEY_SEP}d{k:04}", k % 3)).unwrap();
                    ndel += 1;
                }
                1 if !live.is_empty() => {
                    let k = live[rng.next_u64() as usize % live.len()];
                    let mut d = doc(k);
                    d.set_path("body", Value::Str("rewritten ".repeat(1 + k % 4))).unwrap();
                    s.insert(d).unwrap();
                    nupd += 1;
                }
                _ => {
                    s.insert(doc(i)).unwrap();
                    live.push(i);
                }
            }
            stamps.push(s.clock.peek());
        }
        assert!(
            ndel > 0 && nupd > 0,
            "the mix has to contain updates and deletes to mean anything: {ndel} deletes, \
             {nupd} updates"
        );

        let terms = ["document".to_string(), "rewritten".to_string()];
        let check = |s: &Shard, stage: &str| {
            for &ts in &stamps {
                for t in [ts.saturating_sub(1), ts, ts + 1] {
                    assert_eq!(
                        s.term_stats("body", &terms, t).unwrap().1,
                        fold(s, t),
                        "{stage}: the numerator disagrees with the fold at {t}"
                    );
                }
            }
        };

        check(&s, "in the memtable");
        s.opts.gc_horizon = pin;
        s.flush().unwrap();
        check(&s, "after a seal");
        let copts = crate::compaction::CompactionOpts { tier_fanout: 2, ..Default::default() };
        crate::compaction::run_to_quiescence(&mut s, &copts, 16).unwrap();
        assert!(!s.segments.is_empty());
        check(&s, "after a compaction");
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
        let dir = test_dir("halfflush");
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
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
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
        let dir = test_dir("wal");
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
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
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
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
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
        updated.set_path("body", Value::Str("rewritten".into())).unwrap();
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
        bad.set_path("emb", crate::json::parse("[1.0,2.0,3.0]").unwrap()).unwrap();
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
        bad.set_path("emb", crate::json::parse("[1e40, 1.0, 1.0, 1.0]").unwrap()).unwrap();
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

    /// Replay after a crash in the seal/truncate window has to reproduce the
    /// state the seal left, whatever shape that is, so the two shapes are
    /// tested side by side and against the PRE-CRASH counts rather than
    /// literals.
    ///
    /// The UPDATE is what this needed and what it used to lack: the test was
    /// twelve DISTINCT inserts, so no key ever had two versions,
    /// `layer_by_version` produced one layer, and the pinned multi-segment seal
    /// R4 added was structurally unreachable from it — that path landed with no
    /// replay coverage at all. Under a pin the seal emits one segment per
    /// version layer, and the replay arm's old `locate(key, MAX_TS)` then wrote a
    /// tombstone onto the NEWEST layer at the OLDEST version's timestamp and
    /// left the older layer live beside the memtable copy it had just
    /// re-inserted: four keys read as eight at exactly the horizon the pin
    /// exists to hold, and the next seal made it permanent.
    ///
    /// The unpinned leg is not decoration. The obvious repair — skip a record
    /// whose `(key, ts)` is already sealed, otherwise supersede the greatest
    /// version BELOW it — fixes the pinned case and breaks this one, because an
    /// unpinned seal DROPS the superseded version, so `(key, v1.ts)` is nowhere
    /// and v1 is re-inserted and never superseded. Without this leg that
    /// version of the fix looks correct.
    fn replay_reproduces_the_seal(pin: bool) {
        let dir = test_dir(if pin { "replay-pinned" } else { "replay-unpinned" });
        fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(Hlc::new());
        let wal_bytes;
        let horizon;
        let (pre_max, pre_pin);
        let mut opts = ShardOpts::default();
        {
            let mut s = Shard::new(coll(), clock.clone(), ShardOpts::default());
            s.attach_dir(&dir).unwrap();
            for i in 0..4 {
                s.insert(doc(i)).unwrap();
            }
            // Pinned at an instant where every key holds its FIRST version, so
            // the update below leaves two live layers rather than one.
            horizon = s.clock.peek();
            for i in 0..4 {
                let mut d = doc(i);
                d.set_path("body", Value::Str(format!("rewritten {i}"))).unwrap();
                s.insert(d).unwrap();
            }
            s.wal.as_mut().unwrap().sync().unwrap();
            wal_bytes = fs::read(dir.join("wal.log")).unwrap();
            if pin {
                s.opts.gc_horizon = horizon;
                opts.gc_horizon = horizon;
            }
            s.flush().unwrap();
            s.persist_manifest().unwrap();
            assert_eq!(
                s.segments.len(),
                if pin { 2 } else { 1 },
                "one segment per retained version layer"
            );
            pre_max = s.num_docs(MAX_TS);
            pre_pin = s.num_docs(horizon);
        }
        // The segments and manifest are durable, the WAL truncation is not.
        fs::write(dir.join("wal.log"), &wal_bytes).unwrap();
        let mut s2 = Shard::open(coll(), Arc::new(Hlc::new()), opts, &dir).unwrap();
        assert_eq!(s2.num_docs(MAX_TS), pre_max, "at the tip");
        assert_eq!(s2.num_docs(horizon), pre_pin, "at the pin");
        // Every record in that log describes a version the seal already wrote,
        // so convergence means replay adds NOTHING: the memtable is empty. This
        // is where `ts >= r.ts` earns the `=`. With `>`, the record whose
        // timestamp equals the sealed version's is re-applied -- the sealed row
        // is tombstoned at its own commit timestamp, which no read can see, and
        // a copy of the document lands back in the memtable to be written into
        // a second segment by the next seal. The counts above cannot see it,
        // because the row it duplicates was made invisible in the same breath.
        assert!(
            s2.memtable.is_empty(),
            "replay re-applied a record whose effect was already sealed: the document is now in \
             a segment and in the memtable, and the next seal makes that permanent"
        );
        // And it stays fixed: a duplicate pair survives the next seal, because
        // the memtable copy is dead only above the pin, so it is written into a
        // fresh segment and outlives the WAL that could explain it.
        s2.flush().unwrap();
        assert_eq!(s2.num_docs(horizon), pre_pin, "and the next seal does not fix it either");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_is_idempotent_when_a_crash_lands_between_seal_and_truncate() {
        replay_reproduces_the_seal(false);
    }

    #[test]
    fn replay_is_idempotent_when_a_pinned_seal_emitted_one_segment_per_version() {
        replay_reproduces_the_seal(true);
    }

    /// `D` retained versions cost `D` segments at a pinned seal, and the
    /// memtable's byte threshold was the only bound on `D`. Under a pin the
    /// flush now fires on version depth, so one seal emits at most
    /// `max_versions` segments; twelve versions of one key at a threshold of
    /// four are three seals of four, never one of twelve. Unpinned, depth is
    /// not a reason to seal — that seal keeps one version per key and emits
    /// one segment however deep the chain ran, and firing it early would be a
    /// flush policy change for no file it saves. Each half is the mutation
    /// that fails the other: dropping the pin guard seals the unpinned run,
    /// and dropping the depth term leaves the pinned one a burst of twelve.
    #[test]
    fn a_pinned_seal_fans_out_to_at_most_max_versions_segments() {
        let run = |pin: bool| {
            let dir = test_dir(if pin { "depth-pinned" } else { "depth-unpinned" });
            fs::create_dir_all(&dir).unwrap();
            let mut opts = ShardOpts::default();
            opts.thresholds.max_versions = 4;
            let mut s = Shard::new(coll(), Arc::new(Hlc::new()), opts);
            s.attach_dir(&dir).unwrap();
            s.insert(doc(0)).unwrap();
            if pin {
                s.opts.gc_horizon = s.clock.peek();
            }
            let (mut bursts, mut segs, mut flushes) = (Vec::new(), s.segments.len(), s.flushes);
            for v in 1..12 {
                let mut d = doc(0);
                d.set_path("body", Value::Str(format!("version {v}"))).unwrap();
                s.insert(d).unwrap();
                if s.flushes != flushes {
                    bursts.push(s.segments.len() - segs);
                    segs = s.segments.len();
                    flushes = s.flushes;
                }
            }
            let depth_before_seal = s.memtable.version_depth();
            s.flush().unwrap();
            bursts.push(s.segments.len() - segs);
            let _ = fs::remove_dir_all(&dir);
            (bursts, depth_before_seal)
        };
        let (pinned, _) = run(true);
        assert_eq!(
            pinned,
            vec![4, 4, 4, 0],
            "three seals of four, and nothing left over: {pinned:?}"
        );
        let (unpinned, depth) = run(false);
        assert_eq!(depth, 12, "the whole chain sat in one memtable");
        assert_eq!(
            unpinned,
            vec![1],
            "no seal on depth, and the one seal is one segment: {unpinned:?}"
        );
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
            ("s", In, Value::Array(vec![Value::Str("alpha".into()), Value::Str("nope".into())])),
            // Past the per-literal threshold: the set scan, on both sides.
            (
                "s",
                In,
                Value::Array(
                    ["alpha", "beta", "gamma", "delta", "odd", "x", "y"]
                        .iter()
                        .map(|s| Value::Str(s.to_string()))
                        .collect(),
                ),
            ),
            ("n", In, Value::Array((0..40).map(|i| Value::Int(i - 8)).collect())),
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
        let dir = test_dir("tail");
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
    fn a_pinned_seals_invisible_units_cost_the_read_path_nothing_and_change_nothing() {
        // A seal under a pinned `gc_horizon` emits one segment per version
        // layer, so a key updated `n` times leaves `n` units of which all but
        // one hold nothing visible at any single `t`. Every cost this read path
        // has is per unit — one `visibility`, one text handle, one dictionary
        // walk, one cursor per term — so the fan-out multiplies all of them: a
        // two-term gather over 202 such units measured 10.0 ms against 139 us
        // for the same live corpus in one segment, and compaction cannot
        // collapse them while the pin holds.
        //
        // Skipping a unit with an empty visibility mask deletes that multiplier
        // (10.0 ms to 322 us on the same fixture), and this test is the guard
        // that it is NEUTRAL, which is the risk the skip introduces: the
        // answers below must be the ones the unpinned single-segment shard
        // gives, term for term. It is not a regression test for a defect — the
        // skip is a cost fix — so it does not fail on the unskipped code.
        // `an_invisible_unit_is_not_opened_at_all` is the one that does.
        //
        // The multiplier reproduces on an independently built fixture of the
        // same shape — 202 units under a pin, 200 live documents, a two-term
        // gather — at 3.4 ms with the skips deleted against 23 us with them,
        // for identical answers. The absolute numbers move with how much text
        // each layer holds, because what the skip saves is per unit and
        // whole: one text handle, one dictionary decode, one cursor per term.
        // The ratio is the claim.
        let live = |pin: bool| {
            let mut s = shard();
            for i in 0..6 {
                s.insert(doc(i)).unwrap();
            }
            let horizon = s.clock.peek();
            // Six versions of ONE key, every one of them retained under the
            // pin, so the seal emits one layer per version.
            for v in 0..6 {
                let mut d = doc(3);
                d.set_path("body", Value::Str(format!("vector revision {v}"))).unwrap();
                s.insert(d).unwrap();
            }
            if pin {
                s.opts.gc_horizon = horizon;
            }
            s.flush().unwrap();
            let t = s.clock.peek();
            let mut terms = BTreeSet::new();
            s.prefix_terms("body", "vec", t, 512, None, &mut terms).unwrap();
            let stats = s.term_stats("body", &["vector".to_string()], t).unwrap();
            (s.segments.len(), s.num_docs(t), terms, stats)
        };

        let (n_pinned, docs_pinned, terms_pinned, stats_pinned) = live(true);
        let (n_plain, docs_plain, terms_plain, stats_plain) = live(false);
        assert!(n_pinned > 1, "the pinned seal has to emit the layers: {n_pinned}");
        assert_eq!(n_plain, 1, "and the unpinned one must not");
        assert_eq!(docs_pinned, docs_plain, "the same live corpus either way");
        assert_eq!(terms_pinned, terms_plain, "the same live vocabulary");
        assert_eq!(stats_pinned, stats_plain, "and the same triple: ndocs, length sum, df");
    }

    #[test]
    fn an_invisible_unit_is_not_opened_at_all() {
        // The pin for both `vis.popcount() == 0` skips, in `term_stats` and in
        // `prefix_terms`. What they buy is cost, and cost is not a thing a
        // test can assert on without becoming a timing test; what they also
        // buy — and this is the same statement — is that a unit holding
        // nothing visible at `t` is never OPENED, so nothing about its bytes
        // can reach the answer. That is assertable, because a segment can be
        // made to fail on being opened.
        //
        // So the refusal below is an instrument, not a scenario: no operator
        // archives the superseded layers of a shard and leaves the live one
        // local. It is here because an archived segment on a node configured
        // to refuse is the cheapest way to make opening a CHOSEN unit fail
        // loudly, and a gather that still answers then means the unit was not
        // opened. Delete either skip and both calls below return that refusal
        // instead of an answer.
        //
        // A pinned seal is what makes the fixture: six retained versions of one
        // key emit one layer per version, and all but one of them holds nothing
        // visible at the timestamp read at.
        let fixture = || {
            let mgr = Arc::new(ResidencyManager::new(crate::residency::ResidencyOpts {
                archived_access: crate::residency::ArchivedAccess::Refuse,
                ..Default::default()
            }));
            let mut opts = ShardOpts::default();
            opts.residency = Some(mgr);
            let mut s = Shard::new(coll(), Arc::new(Hlc::new()), opts);
            for i in 0..6 {
                s.insert(doc(i)).unwrap();
            }
            s.opts.gc_horizon = s.clock.peek();
            for v in 0..6 {
                let mut d = doc(3);
                d.set_path("body", Value::Str(format!("vector revision {v}"))).unwrap();
                s.insert(d).unwrap();
            }
            s.flush().unwrap();
            assert!(s.segments.len() > 1, "the pinned seal has to emit the layers");
            s
        };
        let gone = std::env::temp_dir().join("celastro-no-such-archive.seg");
        let archive = |h: &Arc<SegmentHandle>| {
            h.segment.set_source(crate::segment::SegmentSource::Archive(gone.clone()));
        };

        let s = fixture();
        let t = s.clock.peek();
        let mut sent = 0;
        for h in s.segments.iter().filter(|h| h.visibility(t).popcount() == 0) {
            archive(h);
            sent += 1;
        }
        assert!(sent > 0, "the fixture has to contain a unit with nothing visible at `t`");

        // Both readers of a unit's text index, and each has its own skip: the
        // gather opens postings, the expansion opens the dictionary.
        let (ndocs, _, df) = s.term_stats("body", &["vector".to_string()], t).unwrap();
        assert_eq!(ndocs, 6, "the live corpus, whatever the dead layers hold");
        assert_eq!(df.get("vector"), Some(&6), "and its document frequencies");
        let mut terms = BTreeSet::new();
        s.prefix_terms("body", "vec", t, 512, None, &mut terms).unwrap();
        assert!(terms.contains("vector"), "the live vocabulary: {terms:?}");

        // The control, on a FRESH fixture because the first one has decoded
        // what it was allowed to decode and a resident component is answered
        // without asking the archive again. It is what stops the assertions
        // above from passing on a gather that skipped every unit: a unit that
        // does hold something visible is still opened, and still refuses.
        let s = fixture();
        let t = s.clock.peek();
        s.segments.iter().for_each(archive);
        let e = s.term_stats("body", &["vector".to_string()], t).unwrap_err().to_string();
        assert!(e.contains("archived"), "a live unit's refusal still reaches the caller: {e}");
        let mut terms = BTreeSet::new();
        let e = s.prefix_terms("body", "vec", t, 512, None, &mut terms).unwrap_err().to_string();
        assert!(e.contains("archived"), "on the dictionary read too: {e}");
    }

    #[test]
    fn a_postings_extent_the_dictionary_names_but_cannot_decode_fails_the_gather() {
        // `term_stats` opened postings with the INFALLIBLE `cursor`, which
        // cannot tell "this unit does not hold the term" from "this term's
        // extent does not decode". The second answered `df = 0` — an inflated
        // IDF against every other term, and a zero that `Db::fill_term_stats`
        // then caches for the rest of the epoch, including for queries that
        // never touch the damaged unit. A query whose predicate leaves this
        // unit with no survivors never opens a cursor on the scoring path, so
        // nothing else raises it either.
        //
        // Region checksums catch random damage before any of this, which is why
        // this test does not truncate the file: it makes the damage CONSISTENT,
        // the way a directory entry lost or wrongly sized by a builder skew
        // would be. The dictionary still names `vectors` and still says where
        // its postings are; the region it points into is now empty.
        //
        // It also pins the promise `sealed_has_live_posting` makes when it lets
        // an unreadable extent through the prefix walk as "live": that the
        // gather which follows opens the same extent and raises there.
        let dir = test_dir("corrupt");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        for i in 0..8 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        let bytes = s.segments[0].segment.encode().unwrap();

        // Rewrite the `text/body/postings.blk` directory entry to claim a
        // zero-length region, with that region's checksum and the footer's own
        // recomputed so every integrity check still passes.
        let name = b"text/body/postings.blk";
        let at = bytes.windows(name.len()).position(|w| w == name).expect("region name");
        let mut patched = bytes.clone();
        let lo = at + name.len();
        patched[lo + 8..lo + 16].copy_from_slice(&0u64.to_le_bytes());
        patched[lo + 16..lo + 20].copy_from_slice(&crate::codec::crc32(&[]).to_le_bytes());
        let n = patched.len();
        // `[body][footer][flen u32][crc u32][MAGIC]`, and only the footer moved.
        let flen = u32::from_le_bytes(patched[n - 12..n - 8].try_into().unwrap()) as usize;
        let fstart = n - 12 - flen;
        let crc = crate::codec::crc32(&patched[fstart..n - 12]);
        patched[n - 8..n - 4].copy_from_slice(&crc.to_le_bytes());

        let path = dir.join("patched.seg");
        fs::write(&path, &patched).unwrap();
        let seg = crate::segment::Segment::open(SegmentSource::File(path)).unwrap();
        let mut s2 = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s2.adopt_segment(&seg);
        s2.segments.push(SegmentHandle::new(seg, DeleteLog::new(), None));

        let e = s2.term_stats("body", &["vector".to_string()], MAX_TS).unwrap_err().to_string();
        assert!(e.contains("unreadable"), "the corruption has to reach the caller: {e}");

        // The control: the same shard with the same segment intact answers.
        let (ndocs, _, df) = s.term_stats("body", &["vector".to_string()], MAX_TS).unwrap();
        assert_eq!(ndocs, 8);
        assert_eq!(df.get("vector"), Some(&8), "and a healthy gather is unaffected");
        let _ = fs::remove_dir_all(&dir);
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

        // And the same on the DICTIONARY read, which is a separate call with
        // the same obligation and its own failure mode: a swallowed refusal
        // there drops the segment's terms out of the expansion, so a wide
        // prefix quietly names fewer terms and the query returns fewer rows.
        //
        // Tested here rather than only end to end deliberately. Through a
        // query the refusal is MASKED — `Db::run_select` hands every path to
        // `gather_stats` whatever the expansion resolved to, and `term_stats`
        // above raises on the same handle a moment later, so the statement
        // still fails and a swallowed refusal here is invisible. It stops
        // being invisible the day the gather skips a path with nothing to
        // measure, and by then the expansion is short and nothing says so.
        let mut out = BTreeSet::new();
        let e = s.prefix_terms("body", "vec", MAX_TS, 512, None, &mut out).unwrap_err().to_string();
        assert!(e.contains("archived"), "the refusal has to reach the caller: {e}");
    }
    // ----------------------------------------------------------------------
    // Durability: the operations, and the order they happen in.
    //
    // These read the probe in `super::durability_probe`. What each of them is
    // worth is measured by what breaks it, so each says which production line
    // it is the red light for -- and the ORDER assertions are the point. A
    // probe that only records that a directory was synced cannot tell "synced
    // after the rename" from "synced before it", and the second one is the
    // defect the directory fsync was added to fix.
    // ----------------------------------------------------------------------

    /// Everything below asserts on events the probe recorded, and an event is
    /// worth nothing if the thing it records is not the syscall. This is the
    /// floor under all of it: hand each of the three fsync helpers a descriptor
    /// the kernel refuses to sync, and require the refusal to come back.
    ///
    /// A helper that had been reduced to bookkeeping -- the exact mutation that
    /// survived the previous round, `sync_data` deleted and the counter next to
    /// it left alone -- returns `Ok(())` here and this goes red.
    ///
    /// A statement refused on the log -- an append that fails, as a full
    /// disk fails it -- leaves no record and no row: the log is cut back to
    /// where it was, memory is untouched, and a reopen has exactly the
    /// acknowledged rows.
    #[test]
    fn a_write_the_log_refuses_leaves_no_record_and_no_row() {
        let dir = std::env::temp_dir().join(format!("celastro-refused-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        let doc = |i: usize| {
            crate::json::parse(&format!(r#"{{"id":"doc-{i:03}","tenant_id":"t1","n":{i}}}"#))
                .unwrap()
        };
        s.insert_many((0..3).map(doc).collect()).unwrap();
        let wal = dir.join("wal.log");
        let len_before = fs::metadata(&wal).unwrap().len();
        durability_probe::start();
        // The third record of the batch fails to append: the first two were
        // written whole, and must go.
        durability_probe::fail_after(durability_probe::Op::WalAppend, &wal, 2);
        let e = s.insert_many((3..8).map(doc).collect()).unwrap_err().to_string();
        assert!(e.contains("injected"), "{e}");
        assert_eq!(fs::metadata(&wal).unwrap().len(), len_before, "the log was cut back");
        assert_eq!(s.num_docs(MAX_TS), 3, "memory is as it was");
        // The same for one document, and for a delete.
        durability_probe::fail_next(durability_probe::Op::WalAppend, &wal);
        assert!(s.insert(doc(9)).is_err());
        durability_probe::fail_next(durability_probe::Op::WalAppend, &wal);
        assert!(s.delete("t1\u{1}doc-001").is_err());
        assert_eq!(fs::metadata(&wal).unwrap().len(), len_before);
        assert!(s.contains("t1\u{1}doc-001", MAX_TS));
        drop(s);
        let _ = durability_probe::take();
        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s.num_docs(MAX_TS), 3, "a reopen has the acknowledged rows and no other");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A seal that fails does not fail the write that was already on the
    /// log and in memory: the write is acknowledged, the failure is counted,
    /// the next write seals, and a reopen has every row.
    #[test]
    fn a_seal_that_fails_leaves_the_write_acknowledged_and_is_retried() {
        let dir = std::env::temp_dir().join(format!("celastro-sealfail-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut opts = ShardOpts::default();
        opts.thresholds.max_bytes = 1;
        let mut s = Shard::open(coll(), Arc::new(Hlc::new()), opts, &dir).unwrap();
        let doc = |i: usize| {
            crate::json::parse(&format!(r#"{{"id":"doc-{i:03}","tenant_id":"t1","n":{i}}}"#))
                .unwrap()
        };
        durability_probe::start();
        // The first segment's temporary file fails its fsync: the seal fails.
        let tmp = dir.join("segments").join("0000000000000001.tmp");
        durability_probe::fail_next(durability_probe::Op::TempSync, &tmp);
        let ts = s.insert_many((0..3).map(doc).collect()).unwrap();
        assert_eq!(ts.len(), 3, "the write was acknowledged");
        assert_eq!(s.seal_failures, 1, "{:?}", s.last_seal_error);
        assert!(s.last_seal_error.as_deref().is_some_and(|e| e.contains("injected")));
        assert_eq!(s.num_docs(MAX_TS), 3, "and every row is visible");
        assert!(s.segments.is_empty(), "nothing was sealed");
        // The next write seals what the failed one left, and itself.
        s.insert(doc(3)).unwrap();
        assert!(!s.segments.is_empty(), "the seal was retried");
        assert_eq!(s.num_docs(MAX_TS), 4);
        drop(s);
        let _ = durability_probe::take();
        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s.num_docs(MAX_TS), 4);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Linux-only because it names the descriptors: fsync on `/dev/null` and on
    /// a procfs directory is `EINVAL` there, since neither has a filesystem
    /// behind it to flush. The production code is not Linux-only.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_three_fsyncs_are_syscalls_and_not_bookkeeping() {
        let null = Path::new("/dev/null");
        // Recording, so that the second half of the claim is checked too: none
        // of these syncs happened, so none of them may be in the log. A probe
        // note written above its syscall rather than below it records a
        // durability that failed, and every assertion downstream believes it.
        durability_probe::start();
        let mut w = Wal::open(null, None, "t/wal.log".into()).unwrap();
        w.append(&WalRecord {
            kind: WAL_INSERT,
            key: "k".into(),
            ts: 1,
            doc: None,
            supersedes: false,
            segment_id: 0,
        })
        .unwrap();
        let e = w.sync().unwrap_err();
        assert!(
            matches!(e, Error::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidInput),
            "`Wal::sync` did not fdatasync its own descriptor: {e}"
        );

        let f = fs::File::open(null).unwrap();
        let e = sync_file(&f, null).unwrap_err();
        assert!(
            matches!(e, Error::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidInput),
            "`sync_file` did not fsync the file it was handed: {e}"
        );

        // procfs has no fsync operation either, so this is the same refusal for
        // the directory half.
        let e = sync_dir(Path::new("/proc/self")).unwrap_err();
        assert!(
            matches!(e, Error::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidInput),
            "`sync_dir` did not fsync the directory it was handed: {e}"
        );

        let ev = durability_probe::take();
        assert!(
            ev.paths(Op::WalSync).is_empty()
                && ev.paths(Op::TempSync).is_empty()
                && ev.paths(Op::DirSync).is_empty(),
            "a sync that failed was recorded as a sync that happened: {ev:?}"
        );
    }

    /// Publication, in order: the temp file's own contents are durable, then
    /// the rename puts the name over them, then the directory holding the name
    /// is durable.
    ///
    /// Delete `sync_file` from `publish` and the first assertion goes red; move
    /// the directory fsync above the rename -- which is the defect this whole
    /// line of work exists for, a publication whose new directory entry a crash
    /// takes back -- and the last one does.
    #[test]
    fn a_publication_syncs_the_bytes_then_renames_then_syncs_the_name() {
        let dir = test_dir("pub");
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("FILE");

        durability_probe::start();
        atomic_write(&p, b"published").unwrap();
        let ev = durability_probe::take();

        let bytes = ev.at(Op::TempSync, &p.with_extension("tmp"));
        let name = ev.at(Op::Rename, &p);
        let dirent = ev.at(Op::DirSync, &dir);
        assert!(bytes.is_some(), "the temp file's contents were never made durable: {ev:?}");
        assert!(name.is_some(), "nothing was renamed into place: {ev:?}");
        assert!(dirent.is_some(), "the directory the rename landed in was never synced: {ev:?}");
        assert!(bytes < name, "a rename published bytes that were still only in the cache: {ev:?}");
        assert!(
            name < dirent,
            "the directory was synced BEFORE the rename, which makes the new entry exactly as \
             durable as it was without the fsync -- not at all: {ev:?}"
        );
        assert_eq!(fs::read(&p).unwrap(), b"published");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The WAL sync has to be reached *from* `Shard::insert`, and it has to be
    /// reached AFTER the append. Syncing first is not a missing sync -- the
    /// syscall count is identical -- it is record N reaching the platter only
    /// when record N+1 arrives, so every acknowledged write is one behind
    /// durable. Only an ordered probe can tell the two apart.
    /// A statement of many documents is one promise, kept with one sync: the
    /// records are all appended, then the log is synced once, then the rows
    /// are there; a document that cannot be taken keeps every other one out
    /// of the log; and a key that recurs in the batch is two versions in
    /// order, as two statements would have made it.
    #[test]
    fn a_batch_of_inserts_is_appended_whole_and_synced_once() {
        let dir = test_dir("walsync-batch");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        durability_probe::start();
        s.attach_dir(&dir).unwrap();
        let ts = s.insert_many((1..=5).map(doc).collect()).unwrap();
        let ev = durability_probe::take();
        assert_eq!(ts.len(), 5);
        assert!(ts.windows(2).all(|w| w[0] < w[1]), "{ts:?}");
        assert_eq!(ev.count(Op::WalSync, &log), 1, "one sync for the batch: {ev:?}");
        assert_eq!(ev.count(Op::WalAppend, &log), 5, "{ev:?}");
        assert!(ev.ordered((Op::WalAppend, &log), (Op::WalSync, &log)), "{ev:?}");
        for i in 1..=5 {
            assert!(s.locate(&sort_key(&s.coll, &doc(i)).unwrap(), MAX_TS).is_some());
        }

        // A bad document in the middle: nothing of the batch reaches the log.
        durability_probe::start();
        let bad =
            json::parse(r#"{"tenant_id":"t1","body":"no key at all","emb":[0.1,0.2,1.0,0.5]}"#)
                .unwrap();
        let e = s.insert_many(vec![doc(6), bad, doc(8)]).unwrap_err();
        let ev = durability_probe::take();
        assert!(e.to_string().contains("id") || e.to_string().contains("primary"), "{e}");
        assert_eq!(ev.count(Op::WalAppend, &log), 0, "a refused batch appended: {ev:?}");
        assert!(s.locate(&sort_key(&s.coll, &doc(6)).unwrap(), MAX_TS).is_none());

        // A recurring key: the later version supersedes the earlier one.
        let second = json::parse(
            r#"{"id":"d0009","tenant_id":"t0","body":"the second version","emb":[0.09,4.0,1.0,0.5]}"#,
        )
        .unwrap();
        let ts = s.insert_many(vec![doc(9), second]).unwrap();
        assert_eq!(ts.len(), 2);
        let snap = s.snapshot_at(MAX_TS);
        let vis = crate::shard::Searchable::Mem(snap.memtable).visibility(MAX_TS);
        let key9 = sort_key(&s.coll, &doc(9)).unwrap();
        let live: Vec<&Value> = vis
            .iter()
            .map(|o| &snap.memtable.docs[o as usize])
            .filter(|d| d.sort_key == key9)
            .map(|d| &d.doc)
            .collect();
        assert_eq!(live.len(), 1, "two live versions of one key");
        assert_eq!(live[0].get("body").and_then(|v| v.as_str()), Some("the second version"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_insert_appends_its_wal_record_and_then_makes_it_durable() {
        let dir = test_dir("walsync");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());

        durability_probe::start();
        s.attach_dir(&dir).unwrap();
        s.insert(doc(1)).unwrap();
        let ev = durability_probe::take();

        assert_eq!(
            ev.count(Op::WalSync, &log),
            1,
            "`insert` returned a commit timestamp for a record that is still only in the page \
             cache: {ev:?}"
        );
        assert!(
            ev.ordered((Op::WalAppend, &log), (Op::WalSync, &log)),
            "the record was synced before it was appended, so it is the PREVIOUS write this \
             fsync made durable: {ev:?}"
        );
        // And the directory entry that names the log is durable before the
        // first record is: an fdatasync flushes the file's data and cannot
        // create the name that reaches it.
        assert!(
            ev.ordered((Op::DirSync, &dir), (Op::WalSync, &log)),
            "the WAL was fsynced inside a directory nobody had made durable, so the record is \
             on the disk in a file a crash can still take the name of: {ev:?}"
        );
        // And that claim is about the order the two happen in, not about which
        // one is in the log first. `dir` does not name `wal.log` until
        // `Wal::open` has created it, so a `sync_dir(dir)` moved ABOVE the
        // `Wal::open` in `attach_dir` fsyncs a directory that does not yet hold
        // the entry -- the log's name is left as dirty metadata for ever, which
        // is the defect the fsync was added for -- and it satisfies the
        // assertion above while doing it, because `DirSync` still precedes
        // `WalSync`. Only the creation being an event of its own separates
        // them.
        let created = ev
            .at(Op::WalCreate, &log)
            .unwrap_or_else(|| panic!("the log was never opened: {ev:?}"));
        assert!(
            ev.at_after(Op::DirSync, &dir, created).is_some(),
            "the directory was fsynced before it named {log:?}, so nothing has ever made that \
             name durable and a crash takes the whole log: {ev:?}"
        );

        durability_probe::start();
        s.insert(doc(2)).unwrap();
        let ev = durability_probe::take();
        assert_eq!(ev.count(Op::WalSync, &log), 1, "one sync per document, not one per batch");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The same for a write that supersedes an existing document, which is the
    /// leg that was never exercised: losing THIS record does not lose a new
    /// document, it resurrects an old one, because the memtable has already
    /// stopped answering for the version the log still describes as live.
    #[test]
    fn an_insert_that_supersedes_a_document_syncs_the_record_that_supersedes_it() {
        let dir = test_dir("resync");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        s.insert(doc(1)).unwrap();

        let mut second = doc(1);
        second.set_path("body", Value::Str("rewritten".into())).unwrap();
        durability_probe::start();
        s.insert(second).unwrap();
        let ev = durability_probe::take();

        assert_eq!(
            ev.count(Op::WalSync, &log),
            1,
            "the record that supersedes the previous version was left in the page cache: {ev:?}"
        );
        assert!(ev.ordered((Op::WalAppend, &log), (Op::WalSync, &log)), "{ev:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The delete path, where losing the record is worse still: the removal is
    /// visible in memory and the document comes back on reopen.
    #[test]
    fn a_delete_makes_its_wal_record_durable_before_it_returns() {
        let dir = test_dir("delsync");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        s.insert(doc(1)).unwrap();

        durability_probe::start();
        s.delete(&format!("t1{KEY_SEP}d0001")).unwrap();
        // A key that is not here appends nothing, so there is nothing to sync.
        assert!(s.delete("no-such-key").unwrap().is_none());
        let ev = durability_probe::take();

        assert_eq!(ev.count(Op::WalSync, &log), 1, "the delete record is not on disk: {ev:?}");
        assert_eq!(ev.count(Op::WalAppend, &log), 1, "a delete of nothing wrote a record");
        assert!(ev.ordered((Op::WalAppend, &log), (Op::WalSync, &log)), "{ev:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// "A sync that fails returns `Err` with the shard exactly as it was" is a
    /// claim about an error path, so it is worth what an error path costs to
    /// enter. The probe arms the next WAL sync to fail where the real one
    /// fails -- before the syscall, nothing made durable -- and this follows
    /// the `Err` out and looks at what was left behind.
    ///
    /// Three production lines are red lights here: swallowing the error from
    /// `w.sync()` in `insert` or in `delete`, and moving that `sync` below the
    /// `mark_superseded`/`memtable.insert` pair it deliberately sits above.
    #[test]
    fn a_wal_sync_that_fails_is_reported_and_leaves_the_shard_as_it_was() {
        let dir = test_dir("syncfail");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        let key = format!("t1{KEY_SEP}d0001");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        s.insert(doc(1)).unwrap();
        let before = s.get(&key, MAX_TS).unwrap().unwrap();
        let count = s.num_docs(MAX_TS);

        let mut second = doc(1);
        second.set_path("body", Value::Str("rewritten".into())).unwrap();
        durability_probe::start();
        durability_probe::fail_next(Op::WalSync, &log);
        let e = s.insert(second).unwrap_err();
        let ev = durability_probe::take();
        assert!(matches!(e, Error::Io(_)), "the failed sync was swallowed: {e}");
        assert_eq!(
            ev.count(Op::WalSync, &log),
            0,
            "a sync that failed made nothing durable and must not be recorded as one that did"
        );
        assert_eq!(
            s.get(&key, MAX_TS).unwrap().unwrap(),
            before,
            "the previous version was superseded for a record that was never made durable"
        );
        assert_eq!(s.num_docs(MAX_TS), count, "the rejected document is in the memtable");

        // The delete leg of the same claim.
        durability_probe::fail_next(Op::WalSync, &log);
        let e = s.delete(&key).unwrap_err();
        assert!(matches!(e, Error::Io(_)), "the failed sync was swallowed: {e}");
        assert_eq!(
            s.get(&key, MAX_TS).unwrap().unwrap(),
            before,
            "the document was removed for a delete record that was never made durable"
        );

        // The other half: `append` completed before the sync was reached, so
        // the log held a record for a write the caller was told had failed,
        // and a reopen replayed it. Fenced since a full disk showed it in
        // the wild: the log is cut back to where the statement found it, so
        // only the first insert's record is there.
        assert_eq!(
            Wal::replay(&log, &None, "t/wal.log").unwrap().len(),
            1,
            "the first insert only: the records of the two rejected writes were taken back off \
             the log"
        );

        // And the arming is one shot, so this is the control: the same insert
        // with nothing armed goes through.
        let mut third = doc(1);
        third.set_path("body", Value::Str("rewritten".into())).unwrap();
        s.insert(third).unwrap();
        assert_ne!(s.get(&key, MAX_TS).unwrap().unwrap(), before);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Two claims in one recording, because they are the same claim: a rename
    /// nobody fsynced the directory for can be lost, so MANIFEST's rename being
    /// ordered after the delete logs' means nothing until the logs' names are
    /// durable too.
    #[test]
    fn a_seal_publishes_the_delete_logs_durably_and_before_the_manifest() {
        let dir = test_dir("puborder");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        // A delete against the sealed segment, so its `.dlog` has something in
        // it the next publication has to carry.
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap();
        for i in 40..60 {
            s.insert(doc(i)).unwrap();
        }

        durability_probe::start();
        s.flush().unwrap();
        let ev = durability_probe::take();

        let dlog = ev
            .paths(Op::Rename)
            .into_iter()
            .find(|p| p.extension().is_some_and(|e| e == "dlog"))
            .unwrap_or_else(|| panic!("no delete log was published: {ev:?}"));
        let dlogs = ev.at(Op::DirSync, &dir.join("deletes"));
        let manifest = ev.at(Op::Rename, &dir.join("MANIFEST"));
        let shard = ev.at(Op::DirSync, &dir);
        assert!(
            ev.ordered((Op::Rename, &dlog), (Op::DirSync, &dir.join("deletes"))),
            "the `.dlog` rename is not durable: {ev:?}"
        );
        assert!(
            dlogs.is_some() && manifest.is_some() && dlogs < manifest,
            "MANIFEST names the segments those delete logs belong to, so a crash between them \
             must not be able to leave the manifest and lose the deletes -- which means the \
             logs' names must be DURABLE before MANIFEST is renamed, not merely written: {ev:?}"
        );
        assert!(shard.is_some() && manifest < shard, "the MANIFEST rename is not durable: {ev:?}");
        assert_eq!(
            ev.count(Op::DirSync, &dir.join("deletes")),
            1,
            "one fsync of `deletes/` covers every log renamed into it: {ev:?}"
        );

        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert!(re.get(&format!("t0{KEY_SEP}d0003"), MAX_TS).unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A seal publishes the manifest before it empties the log, and the log is
    /// the only other copy of the rows it just sealed. Truncating first leaves
    /// the old MANIFEST -- which does not name the new segments -- beside a WAL
    /// that no longer holds the documents, and every document in the sealed
    /// memtable is gone with no error anywhere.
    #[test]
    fn a_seal_publishes_the_manifest_before_it_empties_the_wal() {
        let dir = test_dir("sealorder");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }

        durability_probe::start();
        s.flush().unwrap();
        let ev = durability_probe::take();

        let truncated = ev.at(Op::WalTruncate, &dir.join("wal.log"));
        assert!(truncated.is_some(), "the seal did not empty the log: {ev:?}");
        // The event says when; the file says whether. Without this the only
        // thing pinning the truncation is a probe record, and a record is not
        // the work -- the log going on growing, and every reopen replaying
        // every record ever written, is invisible to an event log that says it
        // was emptied.
        assert_eq!(
            fs::metadata(dir.join("wal.log")).unwrap().len(),
            0,
            "the seal recorded a truncation it did not perform: the sealed records are still in \
             the log, which grows without bound and is replayed in full at every reopen"
        );
        assert!(
            ev.ordered(
                (Op::Rename, &dir.join("MANIFEST")),
                (Op::WalTruncate, &dir.join("wal.log"))
            ),
            "the log was emptied before the manifest that names what it was sealed into: {ev:?}"
        );
        assert!(
            ev.ordered((Op::DirSync, &dir), (Op::WalTruncate, &dir.join("wal.log"))),
            "the log was emptied while the manifest's own name was still only in the cache, \
             so a crash there loses the new MANIFEST and the WAL that could rebuild it: {ev:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// And when the manifest cannot be published at all, the seal says so and
    /// keeps the log. Injected by putting a directory where MANIFEST's temp
    /// file has to be created, which is a real `fs::File::create` failure at
    /// the first step of `atomic_write`.
    #[test]
    fn a_seal_whose_manifest_cannot_be_published_keeps_the_wal() {
        let dir = test_dir("sealfail");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        fs::create_dir_all(dir.join("MANIFEST.tmp")).unwrap();

        durability_probe::start();
        let e = s.flush().unwrap_err();
        let ev = durability_probe::take();
        assert!(matches!(e, Error::Io(_)), "a manifest that could not be written was not: {e}");
        assert_eq!(
            ev.count(Op::WalTruncate, &dir.join("wal.log")),
            0,
            "the log was emptied for a manifest that was never published: {ev:?}"
        );
        assert_eq!(
            Wal::replay(&dir.join("wal.log"), &None, "t/wal.log").unwrap().len(),
            40,
            "the only surviving copy of the sealed documents was thrown away"
        );

        // Cleared, the same shard publishes: the failed attempt cached nothing.
        fs::remove_dir(dir.join("MANIFEST.tmp")).unwrap();
        s.persist_manifest().unwrap();
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.num_docs(MAX_TS), 40);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A publication that failed AFTER the rename is the case the cache can
    /// get wrong and nothing else can catch: the bytes are on the disk, so
    /// `still_published` agrees with the cache, and a cache written before
    /// `atomic_write` returned makes every later call skip a file whose name is
    /// not durable. The directory fsync is armed to fail, which is exactly that
    /// window.
    #[test]
    fn a_publication_that_failed_after_the_rename_is_retried_rather_than_believed() {
        let dir = test_dir("retry");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }

        // The MANIFEST leg. A publication that writes the state the shard is
        // already in, so that the failure has nothing to roll back and the
        // bytes left on the disk are the ones the cache would have recorded.
        //
        // A seal cannot be used for this any more. Its publication happens
        // before it installs anything (R6), so a seal that fails leaves the
        // shard describing the state BEFORE it — and a cache written too early
        // would then hold bytes the shard never publishes again, and be missed
        // rather than believed. Attaching to a second directory republishes the
        // state in hand, which is the same rename with nothing behind it.
        s.flush().unwrap();
        let two = dir.join("elsewhere");
        fs::create_dir_all(&two).unwrap();
        s.attach_dir(&two).unwrap();
        durability_probe::fail_next(Op::DirSync, &two);
        let e = s.persist_manifest().unwrap_err();
        assert!(matches!(e, Error::Io(_)), "a directory fsync that failed was swallowed: {e}");
        assert_eq!(
            fs::read(two.join("MANIFEST")).unwrap(),
            manifest_bytes(&s),
            "the rename happened: the bytes on disk are the ones a cache written too early \
             would be believed against"
        );
        durability_probe::start();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &two.join("MANIFEST")).is_some(),
            "the manifest was believed published by a call that returned `Err`, so its name \
             will never be made durable: {ev:?}"
        );
        assert!(ev.at(Op::DirSync, &two).is_some(), "{ev:?}");

        // The delete-log leg, which caches the same way.
        s.delete(&format!("t0{KEY_SEP}d0006")).unwrap().expect("nothing was deleted");
        durability_probe::fail_next(Op::DirSync, &two.join("deletes"));
        let e = s.persist_manifest().unwrap_err();
        assert!(matches!(e, Error::Io(_)), "{e}");
        durability_probe::start();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.paths(Op::Rename).iter().any(|p| p.extension().is_some_and(|e| e == "dlog")),
            "the delete log was believed published by a call that returned `Err`: {ev:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// Inserts land in the memtable, so the segment set -- and therefore the
    /// manifest -- changes once per seal and not once per write. The shells
    /// persist after every acknowledged statement, so without this every one of
    /// them republished the same bytes at two fsyncs and a rename apiece.
    #[test]
    fn a_persist_that_would_rewrite_the_same_manifest_writes_nothing() {
        let dir = test_dir("nowrite");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap();
        s.persist_manifest().unwrap();
        let published = fs::read(dir.join("MANIFEST")).unwrap();

        durability_probe::start();
        for i in 60..70 {
            s.insert(doc(i)).unwrap();
        }
        s.persist_manifest().unwrap();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.paths(Op::Rename).is_empty(),
            "nothing changed on disk and something was \
             written: {ev:?}"
        );
        assert!(ev.paths(Op::DirSync).is_empty(), "{ev:?}");

        // Skipped, not lost: the file is still the one a reopen needs.
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), published);
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.segments.len(), 1);
        assert_eq!(re.num_docs(MAX_TS), 49, "40 sealed less one deleted, plus 10 replayed");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The skip believes what this process published, so it has to check that
    /// what it published is still there -- and that it is still the same file.
    /// Gone is the easy half. The hard half is a file of the same LENGTH: two
    /// manifests over the same segment count have it, and a length comparison
    /// accepts the wrong one and skips forever, leaving a stale manifest that a
    /// reopen reads in place of the state the shard is actually in.
    #[test]
    fn a_manifest_that_was_replaced_underneath_the_shard_is_republished() {
        let dir = test_dir("regone");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        let published = fs::read(dir.join("MANIFEST")).unwrap();
        fs::remove_file(dir.join("MANIFEST")).unwrap();

        durability_probe::start();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &dir.join("MANIFEST")).is_some(),
            "the manifest is gone and nothing rewrote it: {ev:?}"
        );

        // Same length, different bytes -- a restored snapshot of this file, or
        // any other writer's idea of it.
        let mut other = published.clone();
        let last = other.len() - 1;
        other[last] ^= 0xff;
        assert_eq!(other.len(), published.len());
        fs::write(dir.join("MANIFEST"), &other).unwrap();
        durability_probe::start();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &dir.join("MANIFEST")).is_some(),
            "a different file of the same length was accepted as the one this shard \
             published: {ev:?}"
        );
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), published);

        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.num_docs(MAX_TS), 40);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A shard that is attached to a second directory publishes into it. The
    /// caches say what was published to the FIRST one, and what was published
    /// there says nothing about what is here.
    #[test]
    fn a_reattached_shard_publishes_into_its_new_directory() {
        let base = test_dir("reattach");
        let (one, two) = (base.join("one"), base.join("two"));
        fs::create_dir_all(&one).unwrap();
        fs::create_dir_all(&two).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&one).unwrap();
        for i in 0..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap();
        s.persist_manifest().unwrap();
        let first = fs::read(one.join("MANIFEST")).unwrap();

        s.attach_dir(&two).unwrap();
        durability_probe::start();
        s.persist_manifest().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &two.join("MANIFEST")).is_some(),
            "the new directory has no manifest: {ev:?}"
        );
        assert!(
            ev.paths(Op::Rename).iter().any(|p| p.extension().is_some_and(|e| e == "dlog")),
            "the new directory has no delete log, so the deleted document is back: {ev:?}"
        );
        assert_eq!(fs::read(one.join("MANIFEST")).unwrap(), first, "the old directory was written");
        let _ = fs::remove_dir_all(&base);
    }

    /// Per-document atomicity is the guarantee, and the CRC is the whole of its
    /// implementation: a record that was half written when the power went is
    /// discarded, and everything before it is kept. Nothing tested that. Flip
    /// one byte inside the second record's body and replay has to stop there.
    ///
    /// THREE records, and the damaged one in the middle, because "stop" and
    /// "skip" are the same thing to a log whose last record is the damaged one.
    /// They are not the same thing at all: a torn tail means the process died
    /// mid-write, so everything after the tear was written by nobody, and
    /// resuming past it applies records the WAL never promised were contiguous
    /// -- a document committed after a commit that was lost. A `continue` where
    /// the `break` is replays two records here instead of one.
    #[test]
    fn replay_stops_at_a_record_whose_crc_does_not_match() {
        let dir = test_dir("walcrc");
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("wal.log");
        {
            let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
            s.attach_dir(&dir).unwrap();
            s.insert(doc(1)).unwrap();
            s.insert(doc(2)).unwrap();
            s.insert(doc(3)).unwrap();
        }
        let good = fs::read(&log).unwrap();
        // The control: all three records are there and all three replay.
        let replayed = Wal::replay(&log, &None, "t/wal.log").unwrap();
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].key, format!("t1{KEY_SEP}d0001"));

        // The frame is `len | crc | body`, so the second record's body starts
        // eight bytes into what follows the first.
        let first = u32::from_le_bytes(good[0..4].try_into().unwrap()) as usize;
        let body = 8 + first + 8;
        let mut torn = good.clone();
        torn[body + 1] ^= 0x01;
        assert_eq!(torn.len(), good.len(), "the damage is one flipped bit, not a truncation");
        fs::write(&log, &torn).unwrap();

        let replayed = Wal::replay(&log, &None, "t/wal.log").unwrap();
        assert_eq!(
            replayed.len(),
            1,
            "a record whose checksum does not match its body was applied, or the replay carried \
             on past it to the intact record after it: half a document, a document that was \
             never committed, or a write resumed across a gap the log never promised"
        );
        assert_eq!(replayed[0].key, format!("t1{KEY_SEP}d0001"), "the good record was dropped");

        // And through the shard, which is where it matters: the damaged record
        // is not there, the one before it is, and the intact record AFTER the
        // damage is not -- the replay stopped, it did not skip.
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.num_docs(MAX_TS), 1);
        assert!(re.get(&format!("t1{KEY_SEP}d0001"), MAX_TS).unwrap().is_some());
        assert!(
            re.get(&format!("t0{KEY_SEP}d0003"), MAX_TS).unwrap().is_none(),
            "the record after the damaged one was applied, so the replay skipped a torn record \
             instead of stopping at it"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // ----------------------------------------------------------------------
    // Publication is ordered: nothing the shard says is true until the
    // manifest that says it is on the disk (R6).
    //
    // R21 made these publications durable. These three are the other half:
    // durable is worth nothing if the shard has already moved on by the time
    // the publication fails, because then the in-memory state describes a
    // manifest nobody ever wrote, and a reader is reading a segment set that
    // no reopen will ever produce.
    //
    // `DirSync` on `deletes/` is the injected failure in all three. It is the
    // one that lands where a publication fails BEFORE MANIFEST's rename -- the
    // delete logs are published and fsynced first, precisely because MANIFEST
    // is what names the segments they belong to -- so the disk still holds the
    // pre-operation manifest and "what the shard says" and "what was written"
    // can be compared byte for byte.
    // ----------------------------------------------------------------------

    /// Everything a seal or a compaction may claim only once its manifest is
    /// on the disk.
    ///
    /// One value rather than a handful of assertions in each of two tests. The
    /// fields are claims of one kind -- the segment set readers are served
    /// from, the version they are served under, the floor that says versions
    /// below it may have been collected, and the counters that say the
    /// operation happened -- and they were pinned in complementary halves: the
    /// seal's test had the set and the version, the compaction's had the set
    /// and the floor, and neither had a counter, so a seal that counted itself
    /// without happening was invisible at both call sites. Comparing the whole
    /// record before and after an armed failure also covers the field somebody
    /// adds next, at both call sites, without their having to remember either
    /// test.
    #[derive(Debug, PartialEq)]
    struct Committed {
        segments: Vec<u64>,
        manifest_version: u64,
        retain_floor: Timestamp,
        flushes: u64,
        compactions: u64,
    }

    fn committed(s: &Shard) -> Committed {
        Committed {
            segments: s.segments.iter().map(|h| h.id()).collect(),
            manifest_version: s.manifest_version,
            retain_floor: s.retain_floor,
            flushes: s.flushes,
            compactions: s.compactions,
        }
    }

    /// The MANIFEST a shard's in-memory state claims is on the disk.
    fn manifest_bytes(s: &Shard) -> Vec<u8> {
        manifest_bytes_with(s, s.next_segment_id)
    }

    /// The same, with the id counter replaced.
    ///
    /// The counter is the one field of the manifest that may legitimately run
    /// ahead of the disk, because it may never run behind it: a failed
    /// publication leaves files under the ids it spoke for, and the shard has
    /// to keep refusing them. So the comparisons below substitute the disk's
    /// counter and assert on it separately, rather than dropping to comparing
    /// segment ids and letting the rest of the manifest go unchecked.
    fn manifest_bytes_with(s: &Shard, next_segment_id: u64) -> Vec<u8> {
        let mut m = s.manifest();
        m.next_segment_id = next_segment_id;
        let mut body = m.encode();
        let crc = crc32(&body);
        put_u32(&mut body, crc);
        body
    }

    /// The manifest on the disk, with its checksum checked the way a reopen
    /// checks it.
    fn manifest_on_disk(dir: &Path) -> Manifest {
        let b = fs::read(dir.join("MANIFEST")).unwrap();
        let (body, tail) = b.split_at(b.len() - 4);
        assert_eq!(crc32(body), u32::from_le_bytes(tail.try_into().unwrap()), "torn MANIFEST");
        Manifest::decode(body).unwrap()
    }

    /// A seal that cannot publish its manifest has not sealed anything.
    ///
    /// The segments were durable before the manifest was attempted, so the
    /// tempting reading is that installing them early is harmless. It is not:
    /// the rows are still in the memtable and in the WAL, so a shard that
    /// installs them anyway has the same key reachable twice, and it answers
    /// queries out of a segment set that the next reopen cannot reproduce --
    /// the manifest on the disk names the old one. The flush's counter, the
    /// memtable swap and the retain floor all say the same thing, and all of
    /// them are claims about a seal that did not happen.
    #[test]
    fn a_seal_whose_manifest_publication_fails_installs_nothing() {
        let dir = test_dir("sealpubfail");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        let published = fs::read(dir.join("MANIFEST")).unwrap();
        assert_eq!(published, manifest_bytes(&s), "the control: the two agree to begin with");

        // A second seal, with a delete of a row the memtable still holds: the
        // sealed segment carries it, so publishing the seal publishes a delete
        // log and fsyncs `deletes/` before it touches MANIFEST at all.
        for i in 20..40 {
            s.insert(doc(i)).unwrap();
        }
        let key = format!("t1{KEY_SEP}d0022");
        s.delete(&key).unwrap().expect("nothing was deleted");
        let before = s.num_docs(s.clock.peek());
        let committed_before = committed(&s);

        durability_probe::fail_next(Op::DirSync, &dir.join("deletes"));
        let e = s.flush().unwrap_err();
        assert!(matches!(e, Error::Io(_)), "the failed publication was swallowed: {e}");

        assert_eq!(
            fs::read(dir.join("MANIFEST")).unwrap(),
            published,
            "the failure is before MANIFEST's rename, so the disk still holds the old one"
        );
        let on_disk = manifest_on_disk(&dir);
        assert_eq!(
            manifest_bytes_with(&s, on_disk.next_segment_id),
            published,
            "the shard is describing a manifest that was never written"
        );
        assert!(
            s.next_segment_id > on_disk.next_segment_id,
            "the failed seal wrote a segment under id {} and the counter came back below it",
            on_disk.next_segment_id
        );
        assert_eq!(
            committed(&s),
            committed_before,
            "a seal whose publication failed committed part of it anyway: the segment set \
             readers see, the version they see it under, the floor below which versions may \
             have been collected, and the count of seals that happened"
        );
        assert!(!s.memtable.is_empty(), "the rows the retry needs were swapped out anyway");
        assert_eq!(s.num_docs(s.clock.peek()), before, "a reader lost rows to a seal that failed");

        // A reopen is the same state, which is the point: the WAL still holds
        // every row the seal was going to take out of it.
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.num_docs(MAX_TS), before, "the reopened shard is not the shard in memory");
        assert!(re.get(&key, MAX_TS).unwrap().is_none(), "the delete was lost");

        // And the retry, with nothing armed, lands the whole seal.
        assert!(s.flush().unwrap().is_some());
        assert!(s.memtable.is_empty());
        assert_eq!(s.num_docs(s.clock.peek()), before);
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), manifest_bytes(&s));
        assert!(s.get(&key, MAX_TS).unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    /// The compaction half of the same claim, and the leak that rides on it.
    ///
    /// A compaction that removes its inputs from the segment list before the
    /// publication that records the removal loses them twice over. The first
    /// is the segment set, as above. The second is the files: the inputs are
    /// handed to `retiring` only BELOW the publication, so a failure drops the
    /// last reference to them -- the manifest still names them, nothing in the
    /// shard holds them, and no later compaction can retire what it no longer
    /// has. The `.seg` stays on the disk for the life of the database.
    #[test]
    fn a_compaction_whose_publication_fails_keeps_its_inputs_and_retires_them_on_the_retry() {
        let dir = test_dir("compfail");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        for i in 20..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        // A delete inside an input, so the merge carries one into its output
        // and the publication goes through `deletes/` first.
        // Pinned below the delete, so the merge carries the tombstone into its
        // output rather than collecting it. That is what gives the output a
        // delete log of its own, and the publication a `deletes/` fsync to
        // fail on -- the step that happens before MANIFEST is touched.
        s.opts.gc_horizon = s.clock.peek();
        let key = format!("t0{KEY_SEP}d0003");
        s.delete(&key).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();
        let published = fs::read(dir.join("MANIFEST")).unwrap();
        let before = s.num_docs(s.clock.peek());
        let committed_before = committed(&s);
        let inputs: Vec<PathBuf> = s.segments.iter().filter_map(|h| h.path()).collect();
        assert_eq!(inputs.len(), 2, "two inputs, both with files of their own");

        let opts = crate::compaction::CompactionOpts { tier_fanout: 2, ..Default::default() };
        let job = crate::compaction::plan(&s, s.clock.peek(), &opts)
            .expect("two segments at a fanout of two");
        durability_probe::fail_next(Op::DirSync, &dir.join("deletes"));
        let e = crate::compaction::run(&mut s, &job, &opts).unwrap_err();
        assert!(matches!(e, Error::Io(_)), "the failed publication was swallowed: {e}");

        assert_eq!(
            committed(&s),
            committed_before,
            "a compaction whose publication failed committed part of it anyway: the segment \
             set, the version, the retain floor and the count of compactions that happened"
        );
        let on_disk = manifest_on_disk(&dir);
        assert_eq!(
            manifest_bytes_with(&s, on_disk.next_segment_id),
            published,
            "the shard is describing a manifest that was never written"
        );
        assert!(
            s.next_segment_id > on_disk.next_segment_id,
            "the failed compaction wrote its output under id {} and the counter came back \
             below it",
            on_disk.next_segment_id
        );
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), published, "MANIFEST moved");
        assert_eq!(s.num_docs(s.clock.peek()), before);
        assert!(s.get(&key, MAX_TS).unwrap().is_none(), "the delete came back");
        for p in &inputs {
            assert!(p.exists(), "an input was unlinked by a compaction that did not happen");
        }

        // The inputs are still this shard's, so the retry can retire them.
        // Nothing else ever can: they are named by no later manifest and held
        // by no list, so a shard that dropped them here leaks both files for
        // as long as the database exists.
        crate::compaction::run(&mut s, &job, &opts).unwrap();
        s.sweep_retired();
        for p in &inputs {
            assert!(
                !p.exists(),
                "{} is named by no manifest and owned by nothing: a leak no later compaction \
                 can clean up",
                p.display()
            );
        }
        assert_eq!(s.num_docs(s.clock.peek()), before);
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), manifest_bytes(&s));
        assert!(s.get(&key, MAX_TS).unwrap().is_none());
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(re.num_docs(MAX_TS), before);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A publication that failed leaves files behind under ids the manifest
    /// never learned about, so the manifest is not the whole record of which
    /// ids have been spoken for -- the directory is.
    ///
    /// This is not about tidiness. The orphaned `.seg` a reused id overwrites
    /// is worthless and nobody would miss it. The `.dlog` beside it is not:
    /// delete logs are found by id and nothing else, so the segment that
    /// reuses the id inherits the deletions of the segment that failed to be
    /// published. Its own log is empty -- an empty log is not written -- so
    /// nothing overwrites the stale one, and the next reopen silently deletes
    /// documents that were never deleted.
    ///
    /// A crash is the same story with no error to catch, which is why the fix
    /// cannot be cleanup on the failure path: the id must be refused on the
    /// way back in, by the reopen, whatever it was that stopped.
    #[test]
    fn a_reopen_does_not_hand_out_a_segment_id_the_disk_already_holds() {
        let dir = test_dir("idreuse");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        for i in 20..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        // Pinned below the delete, so the merge carries the tombstone into its
        // output rather than collecting it. That is what gives the output a
        // delete log of its own, and the publication a `deletes/` fsync to
        // fail on -- the step that happens before MANIFEST is touched.
        s.opts.gc_horizon = s.clock.peek();
        let key = format!("t0{KEY_SEP}d0003");
        s.delete(&key).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();
        let published = fs::read(dir.join("MANIFEST")).unwrap();

        // The merge's outputs are ids 3 and 4 -- the cap splits forty documents
        // into twenty-five and fifteen -- and the carried delete is in the
        // first, so both of id 3's files reach the disk before the publication
        // fails. TWO ids, deliberately: an id counter nudged past the one file
        // anybody thought to look for is not the property. The property is that
        // no id the disk already holds is handed out, however many of them a
        // single failure left behind.
        let opts = crate::compaction::CompactionOpts {
            tier_fanout: 2,
            segment_cap: 25,
            ..Default::default()
        };
        let job = crate::compaction::plan(&s, s.clock.peek(), &opts)
            .expect("two segments at a fanout of two");
        durability_probe::fail_next(Op::DirSync, &dir.join("deletes"));
        assert!(crate::compaction::run(&mut s, &job, &opts).is_err());
        for id in [3u64, 4] {
            assert!(
                dir.join("segments").join(format!("{id:016x}.seg")).exists(),
                "the output segments are written before the manifest, as they have to be"
            );
        }
        assert!(
            dir.join("deletes").join(format!("{:016x}.dlog", 3)).exists(),
            "the carried delete reached the disk, which is what makes the id dangerous"
        );
        assert_eq!(fs::read(dir.join("MANIFEST")).unwrap(), published, "MANIFEST moved");
        drop(s);

        let mut re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert!(
            re.next_segment_id > 4,
            "the reopen resumed at {}, and the disk already holds segments 3 and 4 and the \
             delete log of 3",
            re.next_segment_id
        );

        // A seal with no deletions in it at all, so whatever id it takes, it
        // writes no delete log -- and an empty log is not written, so a stale
        // one under the same id is not overwritten either.
        for i in 100..140 {
            re.insert(doc(i)).unwrap();
        }
        re.flush().unwrap();
        let expect = re.num_docs(MAX_TS);
        assert_eq!(expect, 20 + 20 - 1 + 40);
        drop(re);

        let again = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(
            again.num_docs(MAX_TS),
            expect,
            "the delete log of an unpublished segment was applied to the segment that reused \
             its id: documents nobody deleted are gone"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The delete-log half of the "already published" skip.
    ///
    /// `publish_segments` skips a delete log whose bytes it published last
    /// time, and the cache alone cannot decide that: a file that went away
    /// underneath the shard is not published however well the shard remembers
    /// writing it. The MANIFEST half of exactly this skip has a test of its
    /// own; this half had none, and it is the half that loses data quietly --
    /// a skipped rewrite leaves the segment to be reopened with an empty
    /// delete log, and every document deleted since it was sealed comes back
    /// while the shard reports success.
    #[test]
    fn a_delete_log_that_was_removed_underneath_the_shard_is_republished() {
        let dir = test_dir("dlogreplace");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        let key = format!("t0{KEY_SEP}d0003");
        s.delete(&key).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();

        let p = dir.join("deletes").join(format!("{:016x}.dlog", 1));
        let published = fs::read(&p).unwrap();
        fs::remove_file(&p).unwrap();
        s.persist_manifest().unwrap();

        assert!(p.exists(), "the shard believed its cache over the disk and skipped the rewrite");
        assert_eq!(fs::read(&p).unwrap(), published, "the delete log came back with other bytes");
        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert!(re.get(&key, MAX_TS).unwrap().is_none(), "the delete came back from the dead");
        assert_eq!(re.num_docs(MAX_TS), 19);
        let _ = fs::remove_dir_all(&dir);
    }

    /// An id can be hiding in any of the three directories, not just
    /// `segments/`.
    ///
    /// A tiered segment's file lives only in `archive/`, and a delete log
    /// lives in neither of the other two -- and the delete log is the one that
    /// costs documents when its id is handed out again. A reopen that asked
    /// `segments/` alone would resume at 1 here.
    ///
    /// The manifest is absent, which is both what makes the directory the only
    /// record and what keeps [`reclaim_orphans`] out of it: a lost MANIFEST
    /// must never be read as "none of these files are live", because that
    /// turns a directory somebody could still recover into an empty one.
    #[test]
    fn a_reopen_reads_every_directory_a_segment_id_can_be_hiding_in() {
        let dir = test_dir("archiveids");
        fs::create_dir_all(dir.join("archive")).unwrap();
        fs::create_dir_all(dir.join("deletes")).unwrap();
        let seg = dir.join("archive").join(format!("{:016x}.seg", 1));
        let dlog = dir.join("deletes").join(format!("{:016x}.dlog", 1));
        // Contents are never read: nothing names these, which is the point.
        fs::write(&seg, b"an archived segment no manifest names").unwrap();
        fs::write(&dlog, b"its delete log").unwrap();

        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(
            s.next_segment_id, 2,
            "the reopen resumed at {} with id 1 sitting in archive/ and deletes/",
            s.next_segment_id
        );
        assert!(seg.exists() && dlog.exists(), "a directory with no MANIFEST was emptied");
        let _ = fs::remove_dir_all(&dir);
    }

    /// An unreadable MANIFEST is not an absent one. Read as absent it opened a
    /// shard with zero segments over a directory full of them, and the next
    /// flush published that empty set -- while a MANIFEST that was short or
    /// checksum-mismatched was refused two lines later. A directory where the
    /// file goes is the injection: `fs::read` on a directory fails with
    /// EISDIR, which is neither `NotFound` nor decodable.
    ///
    /// Both halves are pinned, because each is the mutation that satisfies
    /// the other: a genuinely absent MANIFEST still opens (narrowing to
    /// nothing would refuse a fresh directory), and an unreadable one fails
    /// the open naming the file (narrowing to everything is the defect). The
    /// segment file is asserted still there afterwards: nothing that runs
    /// only where a manifest was read may run where it could not be.
    #[test]
    fn a_manifest_that_cannot_be_read_fails_the_open_rather_than_opening_empty() {
        let dir = test_dir("manifest-unreadable");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        drop(s);
        let manifest = dir.join("MANIFEST");
        let seg = dir.join("segments").join(format!("{:016x}.seg", 1));
        assert!(manifest.is_file() && seg.is_file());

        fs::rename(&manifest, dir.join("MANIFEST.aside")).unwrap();
        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir)
            .expect("an absent manifest is a shard with no segments");
        assert!(s.segments.is_empty());
        drop(s);

        fs::create_dir(&manifest).unwrap();
        match Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir) {
            Err(Error::Storage(m)) => assert!(m.contains("MANIFEST"), "{m}"),
            Err(e) => panic!("the wrong failure: {e}"),
            Ok(s) => panic!(
                "a manifest that could not be read opened a shard with {} segments",
                s.segments.len()
            ),
        }
        assert!(seg.is_file(), "a reopen that failed to read the manifest reclaimed a segment");
        assert!(manifest.is_dir(), "something replaced the manifest the open could not read");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The delete log's copy of the rule, where being read as absent costs
    /// the other direction: every document the log recorded as deleted comes
    /// back, and the next delete rewrites the log without them.
    #[test]
    fn a_delete_log_that_cannot_be_read_fails_the_open_rather_than_resurrecting() {
        let dir = test_dir("dlog-unreadable");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();
        drop(s);
        let dlog = dir.join("deletes").join(format!("{:016x}.dlog", 1));
        assert!(dlog.is_file(), "the delete was not published to a delete log");

        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), 19, "the delete log was not applied");
        drop(s);

        fs::remove_file(&dlog).unwrap();
        fs::create_dir(&dlog).unwrap();
        match Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir) {
            Err(Error::Storage(m)) => assert!(m.contains(".dlog"), "{m}"),
            Err(e) => panic!("the wrong failure: {e}"),
            Ok(s) => panic!(
                "a delete log that could not be read opened a shard with {} documents",
                s.num_docs(s.clock.peek())
            ),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A delete log that lost its tail is refused, naming the file, rather
    /// than opened with one deletion fewer. Before the frame this was the one
    /// file in the format where a cut at a record boundary read as a shorter
    /// log; now every cut and every flipped byte fails the open. The whole
    /// file is walked, because a frame that covers most of the file is the
    /// mutation that satisfies a test of one cut.
    #[test]
    fn a_damaged_delete_log_fails_the_open_rather_than_losing_a_deletion() {
        let dir = test_dir("dlog-damaged");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        for i in [3, 5, 8] {
            s.delete(&format!("t{}{KEY_SEP}d{i:04}", i % 3)).unwrap().expect("nothing was deleted");
        }
        s.persist_manifest().unwrap();
        drop(s);
        let dlog = dir.join("deletes").join(format!("{:016x}.dlog", 1));
        let published = fs::read(&dlog).unwrap();
        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), 17);
        drop(s);

        let refuse = |b: &[u8], what: &str| {
            fs::write(&dlog, b).unwrap();
            match Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir) {
                Err(Error::Storage(m)) => assert!(m.contains(".dlog"), "{what}: {m}"),
                Err(e) => panic!("{what}: the wrong failure: {e}"),
                Ok(s) => panic!(
                    "{what}: the open succeeded with {} documents where 17 were live",
                    s.num_docs(s.clock.peek())
                ),
            }
        };
        for n in 0..published.len() {
            refuse(&published[..n], &format!("cut to {n} bytes"));
        }
        for i in 0..published.len() {
            let mut b = published.clone();
            b[i] ^= 0x80;
            refuse(&b, &format!("flipped a bit at {i}"));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A database whose delete logs predate the frame opens with every
    /// deletion intact, and the next publication that touches a log rewrites
    /// it framed -- the cache of what this shard published is empty after a
    /// reopen, and the bytes on disk are not the bytes it would write, so the
    /// skip cannot fire. That rewrite is what closes the unframed window.
    #[test]
    fn a_delete_log_written_before_the_frame_opens_and_is_rewritten_framed() {
        let dir = test_dir("dlog-legacy");
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        s.delete(&format!("t0{KEY_SEP}d0003")).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();
        drop(s);
        let dlog = dir.join("deletes").join(format!("{:016x}.dlog", 1));
        let framed = fs::read(&dlog).unwrap();
        assert!(framed.starts_with(crate::mvcc::DELETE_LOG_MAGIC));
        // The same entries as a pre-frame publication wrote them: bare
        // records, which is the body of the framed file without its frame.
        let legacy = framed[4 + 4 + 8..framed.len() - 4].to_vec();
        assert_eq!(legacy.len(), 12);
        fs::write(&dlog, &legacy).unwrap();

        let s = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), 19, "the unframed delete log was not applied");
        s.persist_manifest().unwrap();
        assert_eq!(fs::read(&dlog).unwrap(), framed, "the unframed log was not rewritten framed");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The files a publication that never landed left behind are reclaimed by
    /// the next reopen.
    ///
    /// Publishing before installing keeps the INPUTS of a failed compaction --
    /// the shard still holds them, so the retry retires them. The outputs are
    /// the other side of that: they are on the disk, no manifest names them,
    /// no list owns them, and the id guard means no later segment overwrites
    /// them either. One per failed attempt, for the life of the database, on a
    /// volume that is most likely failing because it is already full.
    ///
    /// So the reopen unlinks them, and the two halves of that are both pinned
    /// here: what the manifest does not name goes, and what it names stays --
    /// including the delete log of a live segment, whose removal would bring
    /// every deleted document back.
    #[test]
    fn a_reopen_reclaims_the_files_of_a_publication_that_never_landed() {
        let dir = test_dir("orphans");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        for i in 20..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        // Pinned below the delete so the merge carries the tombstone into its
        // output, which gives the output a delete log of its own and the
        // publication a `deletes/` fsync to fail on.
        s.opts.gc_horizon = s.clock.peek();
        let key = format!("t0{KEY_SEP}d0003");
        s.delete(&key).unwrap().expect("nothing was deleted");
        s.persist_manifest().unwrap();
        let before = s.num_docs(s.clock.peek());

        let opts = crate::compaction::CompactionOpts { tier_fanout: 2, ..Default::default() };
        let job = crate::compaction::plan(&s, s.clock.peek(), &opts)
            .expect("two segments at a fanout of two");
        durability_probe::fail_next(Op::DirSync, &dir.join("deletes"));
        assert!(crate::compaction::run(&mut s, &job, &opts).is_err());

        let orphans = [
            dir.join("segments").join(format!("{:016x}.seg", 3)),
            dir.join("deletes").join(format!("{:016x}.dlog", 3)),
        ];
        for o in &orphans {
            assert!(o.exists(), "the output was durable before the manifest, as it has to be");
        }
        let live = [
            dir.join("segments").join(format!("{:016x}.seg", 1)),
            dir.join("segments").join(format!("{:016x}.seg", 2)),
            dir.join("deletes").join(format!("{:016x}.dlog", 1)),
        ];
        for l in &live {
            assert!(l.exists(), "the control: the manifest names these");
        }
        drop(s);

        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        for o in &orphans {
            assert!(
                !o.exists(),
                "{} is named by no manifest and owned by no list, and it survived the one \
                 moment at which anything can reclaim it",
                o.display()
            );
        }
        for l in &live {
            assert!(l.exists(), "{} is named by the manifest and was unlinked", l.display());
        }
        assert_eq!(re.num_docs(MAX_TS), before, "the reclamation took a live file with it");
        assert!(re.get(&key, MAX_TS).unwrap().is_none(), "a live delete log was reclaimed");
        assert!(
            re.next_segment_id > 3,
            "the reopen read the directory after emptying it and resumed at {}",
            re.next_segment_id
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The id guard is on the directory, not on `Shard::open`.
    ///
    /// `attach_dir` is public API and `Db::build_shards` reaches a shard
    /// through it without opening at all, so a guard that lived in `open`
    /// alone left the one entry point that resumes at id 1 over a directory
    /// that is not empty -- which is what a collection created over a
    /// surviving shard directory the catalog no longer lists does.
    #[test]
    fn an_attach_to_a_populated_directory_refuses_the_ids_it_already_holds() {
        let dir = test_dir("attachids");
        fs::create_dir_all(&dir).unwrap();
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        for i in [3, 6, 9] {
            s.delete(&format!("t0{KEY_SEP}d{i:04}")).unwrap().expect("nothing was deleted");
        }
        s.persist_manifest().unwrap();
        assert!(
            dir.join("deletes").join(format!("{:016x}.dlog", 1)).exists(),
            "the delete log under id 1 is what makes the id dangerous"
        );
        drop(s);

        let mut fresh = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        fresh.attach_dir(&dir).unwrap();
        assert!(
            fresh.next_segment_id > 1,
            "the attach resumed at {} and the disk already holds segment 1 and its delete log",
            fresh.next_segment_id
        );

        // A seal with no deletions in it, so it writes no delete log of its
        // own -- an empty one is not written -- and nothing overwrites a stale
        // one sitting under the id it takes.
        for i in 100..120 {
            fresh.insert(doc(i)).unwrap();
        }
        fresh.flush().unwrap();
        let expect = fresh.num_docs(MAX_TS);
        assert_eq!(expect, 20, "the fresh shard's own rows, none of them deleted");
        drop(fresh);

        let re = Shard::open(coll(), Arc::new(Hlc::new()), ShardOpts::default(), &dir).unwrap();
        assert_eq!(
            re.num_docs(MAX_TS),
            expect,
            "the delete log of the segment that was here first was applied to the segment that \
             reused its id: three documents nobody deleted are gone"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod background_seal_tests {
    use super::*;
    use crate::catalog::Collection;
    use crate::value::Value;

    fn coll() -> Collection {
        Collection::new("t", "id", None)
    }

    fn doc(i: usize) -> Value {
        crate::json::parse(&format!(r#"{{"id":"d{i:04}","n":{i},"body":"row {i}"}}"#)).unwrap()
    }

    fn dir(label: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "celastro-bgseal-{label}-{}-{}",
            std::process::id(),
            crate::time::now_micros()
        ));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    /// A due seal with a sealer running freezes rather than builds: the
    /// rows stay readable and deletable in the frozen memtable, the log is
    /// rotated aside, and the install commits the segment with the delete
    /// made meanwhile and removes the rotated log.
    #[test]
    fn a_frozen_memtable_is_read_and_deleted_until_its_seal_is_installed() {
        let d = dir("freeze");
        let mut opts = ShardOpts::default();
        opts.background_seal = true;
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), opts);
        s.attach_dir(&d).unwrap();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        // Due now, and not before: the write path seals when it is.
        s.opts.thresholds.max_bytes = 1;
        assert!(s.maybe_flush().unwrap(), "due");
        s.opts.thresholds = FlushThresholds::default();
        assert!(s.memtable.is_empty() && s.frozen.len() == 1 && s.pending_seals.len() == 1);
        assert!(s.segments.is_empty(), "nothing built under the lock");
        assert!(d.join("wal.000001.log").exists(), "the log was rotated aside");
        let now = s.clock.peek();
        assert!(s.get("d0003", now).unwrap().is_some(), "read from the frozen memtable");
        assert!(s.delete("d0003").unwrap().is_some(), "deleted in the frozen memtable");
        s.insert(doc(100)).unwrap();
        assert_eq!(s.num_docs(s.clock.peek()), 20, "19 frozen and 1 live");
        let t = s.seal_take().unwrap();
        let built = Shard::seal_build(&t).unwrap();
        let sealed = s.seal_install(t, built).unwrap();
        assert_eq!(sealed.segment_ids.len(), 1);
        assert!(s.frozen.is_empty() && s.pending_seals.is_empty());
        assert!(!d.join("wal.000001.log").exists(), "the rotated log is gone");
        let now = s.clock.peek();
        assert!(s.get("d0003", now).unwrap().is_none(), "the delete made meanwhile holds");
        assert!(s.get("d0004", now).unwrap().is_some());
        assert!(s.get("d0100", now).unwrap().is_some(), "the live row is still live");
        assert_eq!(s.num_docs(now), 20);
        drop(s);
        let _ = fs::remove_dir_all(&d);
    }

    /// The process ends between the freeze and the install: the rotated
    /// log and the live one both replay, every row comes back, and the
    /// next seal covers them and removes the rotated file.
    #[test]
    fn a_seal_frozen_but_not_installed_replays_from_its_rotated_log() {
        let d = dir("replay");
        let clock = Arc::new(Hlc::new());
        let mut opts = ShardOpts::default();
        opts.background_seal = true;
        {
            let mut s = Shard::new(coll(), clock.clone(), opts.clone());
            s.attach_dir(&d).unwrap();
            for i in 0..10 {
                s.insert(doc(i)).unwrap();
            }
            s.opts.thresholds.max_bytes = 1;
            assert!(s.maybe_flush().unwrap());
            s.opts.thresholds = FlushThresholds::default();
            s.insert(doc(50)).unwrap();
            s.delete("d0002").unwrap();
            assert_eq!(s.pending_seals.len(), 1, "frozen, never installed");
        }
        let mut s = Shard::open(coll(), clock, opts, &d).unwrap();
        let now = s.clock.peek();
        assert_eq!(s.num_docs(now), 10, "9 of the first ten and the one after");
        assert!(s.get("d0002", now).unwrap().is_none());
        assert!(s.get("d0050", now).unwrap().is_some());
        assert!(d.join("wal.000001.log").exists(), "kept until a seal covers it");
        s.flush().unwrap();
        assert!(!d.join("wal.000001.log").exists());
        assert_eq!(s.num_docs(s.clock.peek()), 10);
        drop(s);
        let _ = fs::remove_dir_all(&d);
    }
}
