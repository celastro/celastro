//! The segment: immutable, self-contained, and the unit everything else is
//! shaped around.
//!
//! ```text
//! segment/
//!   docs.variant        primary-key sorted document blobs
//!   cols/*.col          shredded columns, multi-value offsets, zone maps, blooms
//!   terms.dict          term dictionary
//!   postings.blk        block-max postings with skip lists
//!   positions.blk       term positions for phrase queries (inside postings.blk)
//!   vectors.codes       quantized vector codes
//!   vectors.full        full-precision vectors (cold)
//!   vectors.graph       ANN structure
//!   vec_ordinals.map    vector ordinal → document ordinal
//!   ordinals.map        document ordinal → primary key, commit timestamp
//!   footer              format version, shredded-path list, statistics, checksums
//! ```
//!
//! Everything is immutable once written. The only mutable per-segment state is
//! the delete log ([`crate::mvcc::DeleteLog`]), which lives outside the file.
//!
//! Two consequences that the rest of the engine leans on:
//!
//! * A segment interprets itself. Quantizer parameters, analyzer output, zone
//!   maps and the shredded-path list are all inside it, so a segment written by
//!   an older build with different settings is still readable — and changing
//!   any of those settings is a rolling rebuild through compaction, never a
//!   migration (§12.3).
//! * Ordinals are dense and segment-local. Every index in the file speaks them,
//!   which is what makes hybrid candidate generation a bitmap intersection
//!   (§4.2).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::catalog::{Collection, IndexKind, Metric};
use crate::codec::*;
use crate::column::{Column, ColumnBuilder};
use crate::error::{Error, Result};
use crate::mvcc::Ordinals;
use crate::residency::{ArchivedAccess, Lazy, ResidencyManager, Tier};
use crate::text::analyzer::{Analyzer, ARRAY_POSITION_GAP};
use crate::text::postings::{DictParts, InvertedBuilder};
use crate::text::TextSource;
use crate::time::Timestamp;
use crate::value::{Value, ValueType};
use crate::vector::hnsw::HnswParams;
use crate::vector::quant::Quantizer;
use crate::vector::VectorStore;

pub const FORMAT_VERSION: u32 = 1;
/// Readers support the current and previous major version (§12.3).
pub const MIN_READABLE_VERSION: u32 = 1;
const MAGIC: &[u8; 4] = b"CLST";

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct BuildOpts {
    pub quantizer: Quantizer,
    pub hnsw: HnswParams,
    /// Below this many vectors a segment stays flat and exact (§5.2). Tunable
    /// because the right value depends on dimensionality and on what the
    /// workload is willing to pay per query — and because tests need to reach
    /// the graph tier without a million vectors.
    pub flat_tier_max: usize,
}

impl Default for BuildOpts {
    fn default() -> Self {
        BuildOpts {
            quantizer: Quantizer::Sq8,
            hnsw: HnswParams::default(),
            flat_tier_max: crate::vector::FLAT_TIER_MAX,
        }
    }
}

/// A sealed full-text index for one path.
pub struct SealedText {
    pub dict: DictParts,
    pub postings: Vec<u8>,
    pub doc_lens: Vec<u32>,
    pub analyzer: Analyzer,
}

impl SealedText {
    pub fn source(&self) -> TextSource<'_> {
        TextSource::sealed(&self.dict, &self.postings, &self.doc_lens)
    }
}

/// Where a segment's bytes come from.
///
/// §4.5 puts sealed segments in object storage with a local NVMe cache, so the
/// local file is a *cache*, not the original. That is what makes archival
/// possible without violating segment immutability: nothing is rewritten, the
/// local copy is simply released.
#[derive(Debug, Clone)]
pub enum SegmentSource {
    /// The whole encoded segment, held in memory. What an unattached shard
    /// uses; components are still evictable because the bytes can be re-read.
    Bytes(Arc<Vec<u8>>),
    /// The local NVMe copy.
    File(PathBuf),
    /// The local copy has been released; reads go to the archive store and
    /// cost a round trip.
    Archive(PathBuf),
    /// The object store is the segment's only copy. Every read is a ranged
    /// `GET`: the footer at open, then each component as it faults in --
    /// the chain of dependent round trips the design budgets for.
    Remote { store: Arc<dyn crate::objstore::ObjectStore>, key: String, size: u64 },
    /// A source whose bytes are the frames of `crate::cipher`: every read is
    /// a ranged read of the frames covering it, opened under the file's
    /// key. Wraps a file, an archived file or an object alike.
    Encrypted { inner: Box<SegmentSource>, cipher: Arc<crate::cipher::Cipher>, id: String },
}

impl SegmentSource {
    fn read(&self, off: u64, len: u64) -> Result<Vec<u8>> {
        match self {
            SegmentSource::Encrypted { inner, cipher, id } => {
                let size = inner.len()?;
                let read = |o: u64, l: u64| -> Result<Vec<u8>> {
                    let l = l.min(size.saturating_sub(o));
                    inner.read(o, l)
                };
                cipher.read_range(id, &read, off, len)
            }
            SegmentSource::Bytes(b) => off
                .checked_add(len)
                .and_then(|end| b.get(off as usize..end as usize))
                .map(|s| s.to_vec())
                .ok_or_else(|| Error::Storage("segment: region out of range".into())),
            SegmentSource::File(p) | SegmentSource::Archive(p) => {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = std::fs::File::open(p)?;
                // A footer-declared length is checked against the file before
                // it becomes an allocation: a corrupt directory entry would
                // otherwise ask for gigabytes and abort on allocation failure
                // rather than reporting a damaged segment.
                let size = f.metadata()?.len();
                if off.checked_add(len).map(|e| e > size).unwrap_or(true) {
                    return Err(Error::Storage("segment: region lies outside the file".into()));
                }
                f.seek(SeekFrom::Start(off))?;
                let mut buf = vec![0u8; len as usize];
                f.read_exact(&mut buf)?;
                Ok(buf)
            }
            SegmentSource::Remote { store, key, size } => {
                if off.checked_add(len).map(|e| e > *size).unwrap_or(true) {
                    return Err(Error::Storage("segment: region lies outside the object".into()));
                }
                store.get_range(key, off, len)
            }
        }
    }

    pub fn is_archive(&self) -> bool {
        matches!(self, SegmentSource::Archive(_) | SegmentSource::Remote { .. })
    }
}

pub struct Segment {
    pub id: u64,
    /// Residency-ledger identity, unique per open segment on this node.
    /// Segment ids are per shard, so they collide across tablets.
    uid: u64,
    pub format_version: u32,
    /// Size tier. Segments at the cap (§9.1) are never merged again.
    pub level: u32,
    /// Always resident: visibility and key-range pruning need it before any
    /// index is touched, and it is small next to what it gates.
    pub ordinals: Ordinals,
    /// Also always resident, for the same reason — it is how an ordinal becomes
    /// a byte range.
    blob_offsets: Vec<u32>,
    /// Paths this segment stored as columns. The planner consults it per
    /// segment, never globally.
    pub shredded: Vec<String>,

    source: RwLock<SegmentSource>,
    /// Region name -> (offset, length, checksum).
    dir: BTreeMap<String, (u64, u64, u32)>,
    vec_meta: BTreeMap<String, (usize, Metric)>,
    /// Declared tier per *component name*, refreshed from the catalog. Absent
    /// means `active`, which is also what a component with no index (a column,
    /// the document store) gets. Already resolved for this node, so `minimal`
    /// appears here only on the node designated to hold it.
    tiers: RwLock<BTreeMap<String, Tier>>,
    residency: RwLock<Option<Arc<ResidencyManager>>>,

    docs: Lazy<Vec<u8>>,
    columns: BTreeMap<String, Lazy<Column>>,
    text: BTreeMap<String, Lazy<SealedText>>,
    vectors: BTreeMap<String, Lazy<VectorStore>>,
    adjacency: BTreeMap<String, Lazy<AdjIndex>>,
}

/// A segment that goes away takes its ledger entries with it.
///
/// Compaction retires four segments and installs one; without this the ledger
/// keeps four segments' worth of entries, all still flagged resident, and the
/// node's idea of its own memory use only ever grows. It then believes it is
/// permanently over budget and evicts live components on every sweep to
/// reclaim memory that was freed long ago.
impl Drop for Segment {
    fn drop(&mut self) {
        if let Some(m) = self.residency.get_mut().ok().and_then(|r| r.clone()) {
            m.forget_segment(self.uid);
        }
    }
}

/// Borrowed access to a segment's document blobs, held across a scan.
pub struct BlobReader<'a> {
    docs: Arc<Vec<u8>>,
    offsets: &'a [u32],
}

impl BlobReader<'_> {
    pub fn get(&self, ord: u32) -> Result<Option<&[u8]>> {
        let i = ord as usize;
        if i + 1 >= self.offsets.len() {
            return Ok(None);
        }
        let (a, b) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        match self.docs.get(a..b) {
            Some(s) => Ok(Some(s)),
            None => Err(Error::Storage(format!(
                "segment: document blob for ordinal {ord} is outside the document region"
            ))),
        }
    }
}

/// Component names, as they appear in `SHOW RESIDENCY` and in eviction plans.
pub fn docs_component() -> String {
    "docs".to_string()
}
pub fn column_component(path: &str) -> String {
    format!("col:{path}")
}
pub fn text_component(path: &str) -> String {
    format!("text:{path}")
}
pub fn vector_component(path: &str) -> String {
    format!("vec:{path}")
}
pub fn adjacency_component(path: &str) -> String {
    format!("adj:{path}")
}

/// One column's value-to-ordinals map: the region an adjacency index owns,
/// one per column it names, so that a hop probes a key instead of scanning
/// the column. Keys sorted, each with the run of ordinals -- ascending --
/// whose document holds that string at the path. A document whose value
/// there is not a string is in no run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdjIndex {
    keys: Vec<String>,
    /// `keys.len() + 1` offsets into `ords`.
    starts: Vec<u32>,
    ords: Vec<u32>,
}

impl AdjIndex {
    /// From `(value, ordinal)` pairs in any order.
    pub fn build(mut pairs: Vec<(String, u32)>) -> AdjIndex {
        pairs.sort();
        pairs.dedup();
        let mut out = AdjIndex::default();
        out.starts.push(0);
        for (k, ord) in pairs {
            if out.keys.last() != Some(&k) {
                out.keys.push(k);
                out.starts.push(out.ords.len() as u32);
            }
            out.ords.push(ord);
            *out.starts.last_mut().expect("pushed above") = out.ords.len() as u32;
        }
        out
    }

    /// The ordinals holding `key`, ascending; empty for a key no document
    /// holds.
    pub fn probe(&self, key: &str) -> &[u32] {
        match self.keys.binary_search_by(|k| k.as_str().cmp(key)) {
            Ok(i) => &self.ords[self.starts[i] as usize..self.starts[i + 1] as usize],
            Err(_) => &[],
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.keys.len() as u64);
        for (i, k) in self.keys.iter().enumerate() {
            put_str(&mut out, k);
            let run = &self.ords[self.starts[i] as usize..self.starts[i + 1] as usize];
            put_uvarint(&mut out, run.len() as u64);
            let mut prev = 0u32;
            for &o in run {
                put_uvarint(&mut out, (o - prev) as u64);
                prev = o;
            }
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<AdjIndex> {
        let bad = || Error::Storage("segment: adjacency region is malformed".into());
        let mut i = 0usize;
        let n = bounded_len(get_uvarint(b, &mut i).ok_or_else(bad)?, 2, b.len()).ok_or_else(bad)?;
        let mut out = AdjIndex { keys: Vec::with_capacity(n), starts: vec![0], ords: Vec::new() };
        for _ in 0..n {
            let k = get_str(b, &mut i).ok_or_else(bad)?;
            if out.keys.last().is_some_and(|last| last >= &k) {
                return Err(bad());
            }
            let run = bounded_len(get_uvarint(b, &mut i).ok_or_else(bad)?, 1, b.len() - i)
                .ok_or_else(bad)?;
            let mut prev = 0u32;
            for _ in 0..run {
                let d = get_uvarint(b, &mut i).ok_or_else(bad)?;
                prev = u32::try_from(d).ok().and_then(|d| prev.checked_add(d)).ok_or_else(bad)?;
                out.ords.push(prev);
            }
            out.keys.push(k);
            out.starts.push(out.ords.len() as u32);
        }
        Ok(out)
    }
}

impl Segment {
    pub fn num_docs(&self) -> usize {
        self.ordinals.len()
    }

    /// Vector count, from the map region, without decoding the index. Asking
    /// how big something is must not be a reason to load it.
    pub fn num_vectors(&self) -> usize {
        self.vectors
            .keys()
            .filter_map(|p| self.dir.get(&format!("vec/{p}/vec_ordinals.map")))
            .map(|(_, len, _)| (*len / 4) as usize)
            .max()
            .unwrap_or(0)
    }

    pub fn is_shredded(&self, path: &str) -> bool {
        self.columns.contains_key(path)
    }

    pub fn text_paths(&self) -> Vec<String> {
        self.text.keys().cloned().collect()
    }

    pub fn vector_paths(&self) -> Vec<String> {
        self.vectors.keys().cloned().collect()
    }

    pub fn column_paths(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    /// The columns this segment holds an adjacency map for.
    pub fn adjacency_paths(&self) -> Vec<String> {
        self.adjacency.keys().cloned().collect()
    }

    /// This segment's identity in the node's residency ledger.
    pub fn uid(&self) -> u64 {
        self.uid
    }

    pub fn attach_residency(&self, mgr: Arc<ResidencyManager>) {
        *self.residency.write().unwrap() = Some(mgr);
    }

    pub fn set_source(&self, src: SegmentSource) {
        *self.source.write().unwrap() = src;
    }

    pub fn source(&self) -> SegmentSource {
        self.source.read().unwrap().clone()
    }

    /// Adopt the catalog's declared tiers for this segment's indexes.
    pub fn set_tiers(&self, tiers: BTreeMap<String, Tier>) {
        *self.tiers.write().unwrap() = tiers;
    }

    /// The declared tier of a component, by component name (`text:body`,
    /// `vec:embedding`, `col:status`, `docs`).
    pub fn tier_of(&self, component: &str) -> Tier {
        self.tiers.read().unwrap().get(component).copied().unwrap_or(Tier::Active)
    }

    fn mgr(&self) -> Option<Arc<ResidencyManager>> {
        self.residency.read().unwrap().clone()
    }

    /// `(loads, fault-ins)` on the node so far. `EXPLAIN` samples this either
    /// side of a unit to attribute I/O to the unit that caused it — the one
    /// number that tells an operator a "fast" query was actually paid for by
    /// somebody's cold read.
    pub fn io_counters(&self) -> Option<(u64, u64)> {
        self.mgr().map(|m| (m.loads(), m.faults()))
    }

    fn region(&self, name: &str) -> Option<(u64, u64, u32)> {
        self.dir.get(name).copied()
    }

    /// Read one region and check its own checksum.
    ///
    /// Per region rather than per file, because the whole point of lazy
    /// loading is that opening a segment does not read it. A whole-body check
    /// at open would read every byte of every segment at every startup — while
    /// still leaving a bit-flip that happens *after* open undetected. Checking
    /// each region as it is decoded catches corruption exactly where it would
    /// otherwise be interpreted.
    fn read_region(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match self.region(name) {
            None => Ok(None),
            Some((off, len, crc)) => {
                let b = self.source.read().unwrap().read(off, len)?;
                if crc32(&b) != crc {
                    return Err(Error::Storage(format!(
                        "segment {}: region `{name}` failed its checksum",
                        self.id
                    )));
                }
                Ok(Some(b))
            }
        }
    }

    /// Read and check every region, including ones no query has touched.
    /// A scrub, not something any read path does.
    pub fn verify(&self) -> Result<()> {
        for name in self.dir.keys() {
            self.read_region(name)?;
        }
        Ok(())
    }

    /// Load a component if it is not resident, recording the access either way.
    ///
    /// The `Arc` is handed out before the caller does anything with it, so an
    /// eviction racing this call can only drop the segment's own reference —
    /// the reader's copy stays valid until it is finished. Same discipline as
    /// retired segment files.
    fn acquire<T>(
        &self,
        cell: &Lazy<T>,
        component: String,
        tier: Tier,
        build: impl FnOnce() -> Result<(T, usize)>,
    ) -> Result<Arc<T>> {
        let now = crate::time::now_micros() as u64;
        let mgr = self.mgr();
        let key = (self.uid, component);
        // The fast path: already resident, no lock beyond the cell's read lock.
        if let Some(a) = cell.peek() {
            cell.touch(now);
            if let Some(m) = &mgr {
                m.note_access(&key, tier, now);
            }
            return Ok(a);
        }
        let from_archive = self.source.read().unwrap().is_archive();
        if from_archive {
            if let Some(m) = &mgr {
                if m.opts().archived_access == ArchivedAccess::Refuse {
                    return Err(self.archived_refusal(&key.1));
                }
            }
        }
        // Slow path: `load_with` re-checks under the write lock, so two threads
        // that miss together decode once and charge once, and the ledger is
        // updated before any evictor can see the populated cell.
        let seg = self.id;
        let (a, _fresh) = cell.load_with(now, build, |bytes| {
            if let Some(m) = &mgr {
                m.note_load(key.clone(), seg, tier, bytes, from_archive, now);
            }
        })?;
        Ok(a)
    }

    fn archived_refusal(&self, component: &str) -> Error {
        Error::Storage(format!(
            "`{component}` is archived and this node is configured to refuse archived reads; \
             move it up the ladder (ALTER INDEX ... SET TIER cached) or set \
             archived_access = 'fault_in'"
        ))
    }

    /// Refuse an archived read that does not go through [`Segment::acquire`].
    ///
    /// Anything that reads segment bytes directly — a small metadata region, say
    /// — still costs the archive round trip the operator asked to refuse, so it
    /// has to ask the same question.
    fn check_archived(&self, component: &str) -> Result<()> {
        if !self.source.read().unwrap().is_archive() {
            return Ok(());
        }
        match self.mgr() {
            Some(m) if m.opts().archived_access == ArchivedAccess::Refuse => {
                Err(self.archived_refusal(component))
            }
            _ => Ok(()),
        }
    }

    pub fn docs(&self) -> Result<Arc<Vec<u8>>> {
        self.acquire(&self.docs, docs_component(), Tier::Active, || {
            let b = self.read_region("docs.variant")?.unwrap_or_default();
            let n = b.len();
            Ok((b, n))
        })
    }

    /// The adjacency map over `path`, if this segment was sealed with one:
    /// the collection declared `USING adjacency` naming the column before
    /// the seal. `None` otherwise, and the caller scans.
    pub fn adjacency(&self, path: &str) -> Result<Option<Arc<AdjIndex>>> {
        let Some(cell) = self.adjacency.get(path) else { return Ok(None) };
        let name = format!("adj/{path}.idx");
        let comp = adjacency_component(path);
        let tier = self.tier_of(&comp);
        let x = self.acquire(cell, comp, tier, || {
            let b = self
                .read_region(&name)?
                .ok_or_else(|| Error::Storage(format!("segment: missing region {name}")))?;
            let n = b.len();
            Ok((AdjIndex::decode(&b)?, n))
        })?;
        Ok(Some(x))
    }

    pub fn column(&self, path: &str) -> Result<Option<Arc<Column>>> {
        let Some(cell) = self.columns.get(path) else { return Ok(None) };
        let name = format!("cols/{path}.col");
        let comp = column_component(path);
        let tier = self.tier_of(&comp);
        let x = self.acquire(cell, comp, tier, || {
            let b = self
                .read_region(&name)?
                .ok_or_else(|| Error::Storage(format!("segment: missing region {name}")))?;
            let n = b.len();
            Ok((Column::decode(&b)?, n))
        })?;
        Ok(Some(x))
    }

    pub fn text_index(&self, path: &str) -> Result<Option<Arc<SealedText>>> {
        let Some(cell) = self.text.get(path) else { return Ok(None) };
        let comp = text_component(path);
        let tier = self.tier_of(&comp);
        let x = self.acquire(cell, comp, tier, || {
            let dictb = self
                .read_region(&format!("text/{path}/terms.dict"))?
                .ok_or_else(|| Error::Storage("segment: missing term dictionary".into()))?;
            let postings =
                self.read_region(&format!("text/{path}/postings.blk"))?.unwrap_or_default();
            let lensb = self.read_region(&format!("text/{path}/doclens"))?.unwrap_or_default();
            let mut k = 0usize;
            let mut doc_lens = Vec::with_capacity(lensb.len() / 4);
            while k < lensb.len() {
                doc_lens.push(
                    get_u32(&lensb, &mut k)
                        .ok_or_else(|| Error::Storage("segment: truncated doclens".into()))?,
                );
            }
            let an = self
                .read_region(&format!("text/{path}/analyzer"))?
                .map(|x| Analyzer::parse(&String::from_utf8_lossy(&x)))
                .unwrap_or(Analyzer::Standard);
            let bytes = dictb.len() + postings.len() + lensb.len();
            Ok((
                SealedText { dict: DictParts::parse(&dictb)?, postings, doc_lens, analyzer: an },
                bytes,
            ))
        })?;
        Ok(Some(x))
    }

    pub fn vector_index(&self, path: &str) -> Result<Option<Arc<VectorStore>>> {
        let Some(cell) = self.vectors.get(path) else { return Ok(None) };
        let Some((dims, metric)) = self.vec_meta.get(path).copied() else { return Ok(None) };
        let comp = vector_component(path);
        let tier = self.tier_of(&comp);
        let x = self.acquire(cell, comp, tier, || {
            let full = self.read_region(&format!("vec/{path}/vectors.full"))?.unwrap_or_default();
            let codes = self.read_region(&format!("vec/{path}/vectors.codes"))?.unwrap_or_default();
            let graph = self.read_region(&format!("vec/{path}/vectors.graph"))?.unwrap_or_default();
            let map =
                self.read_region(&format!("vec/{path}/vec_ordinals.map"))?.unwrap_or_default();
            let bytes = full.len() + codes.len() + graph.len() + map.len();
            Ok((VectorStore::open(dims, metric, &full, &codes, &graph, &map)?, bytes))
        })?;
        Ok(Some(x))
    }

    /// The analyzer for a text index, read from its own tiny region rather
    /// than by decoding the index. Asking which analyzer a field uses must not
    /// fault a gigabyte of postings into memory.
    pub fn analyzer_of(&self, path: &str) -> Result<Option<Analyzer>> {
        if !self.text.contains_key(path) {
            return Ok(None);
        }
        if let Some(t) = self.text[path].peek() {
            return Ok(Some(t.analyzer));
        }
        // Tiny region, but on an archived segment reading it is still the round
        // trip the operator asked to refuse.
        self.check_archived(&text_component(path))?;
        Ok(self
            .read_region(&format!("text/{path}/analyzer"))?
            .map(|x| Analyzer::parse(&String::from_utf8_lossy(&x))))
    }

    pub fn blob_bytes(&self, ord: u32) -> Result<Option<Vec<u8>>> {
        self.blob_reader()?.get(ord).map(|o| o.map(|b| b.to_vec()))
    }

    /// A reader that holds the document store for the length of a scan.
    ///
    /// A per-ordinal `blob_bytes` would take the node-wide residency lock and
    /// allocate a component-name `String` once per document, which over a
    /// five-million-document segment is five million lock acquisitions to read
    /// bytes that were already in RAM after the first. Acquire once, hold the
    /// `Arc`, slice.
    pub fn blob_reader(&self) -> Result<BlobReader<'_>> {
        Ok(BlobReader { docs: self.docs()?, offsets: &self.blob_offsets })
    }

    pub fn document(&self, ord: u32) -> Result<Value> {
        match self.blob_bytes(ord)? {
            Some(b) => crate::variant::decode_one(&b),
            None => Err(Error::Storage(format!("ordinal {ord} out of range"))),
        }
    }

    // --- Residency ------------------------------------------------------

    /// Everything currently decoded, for the sweeper and for `SHOW RESIDENCY`.
    pub fn loaded_components(&self) -> Vec<(String, Tier, u64, usize)> {
        let mut out = Vec::new();
        if self.docs.is_loaded() {
            out.push((docs_component(), Tier::Active, self.docs.last_access(), self.docs.bytes()));
        }
        for (p, c) in &self.columns {
            if c.is_loaded() {
                let n = column_component(p);
                let t = self.tier_of(&n);
                out.push((n, t, c.last_access(), c.bytes()));
            }
        }
        for (p, c) in &self.text {
            if c.is_loaded() {
                let n = text_component(p);
                let t = self.tier_of(&n);
                out.push((n, t, c.last_access(), c.bytes()));
            }
        }
        for (p, c) in &self.vectors {
            if c.is_loaded() {
                let n = vector_component(p);
                let t = self.tier_of(&n);
                out.push((n, t, c.last_access(), c.bytes()));
            }
        }
        for (p, c) in &self.adjacency {
            if c.is_loaded() {
                let n = adjacency_component(p);
                let t = self.tier_of(&n);
                out.push((n, t, c.last_access(), c.bytes()));
            }
        }
        out
    }

    /// Release one component. Returns the bytes freed.
    ///
    /// Refuses when the source cannot supply the bytes again, because an
    /// eviction that cannot be undone is data loss dressed as memory
    /// management.
    pub fn unload_component(&self, component: &str) -> usize {
        let mgr = self.mgr();
        let key = (self.uid, component.to_string());
        // Told on every real unload, including one that reclaims zero bytes:
        // a component that decodes to nothing still has a ledger entry, and an
        // entry left flagged resident is nominated for eviction on every sweep
        // for the rest of the process's life. Told *under the cell lock*, so a
        // reload racing this call cannot have its charge erased by it.
        let note = |_bytes: usize| {
            if let Some(m) = &mgr {
                m.note_unload(&key);
            }
        };
        let freed = if component == docs_component() {
            self.docs.unload_with(note)
        } else if let Some(p) = component.strip_prefix("col:") {
            self.columns.get(p).and_then(|c| c.unload_with(note))
        } else if let Some(p) = component.strip_prefix("text:") {
            self.text.get(p).and_then(|c| c.unload_with(note))
        } else if let Some(p) = component.strip_prefix("vec:") {
            self.vectors.get(p).and_then(|c| c.unload_with(note))
        } else if let Some(p) = component.strip_prefix("adj:") {
            self.adjacency.get(p).and_then(|c| c.unload_with(note))
        } else {
            None
        };
        freed.unwrap_or(0)
    }

    /// Push the current tiers into the residency ledger, so that a tier change
    /// reorders eviction immediately rather than at the next access.
    pub fn refresh_ledger_tiers(&self) {
        let Some(m) = self.mgr() else { return };
        for (name, tier, _, _) in self.loaded_components() {
            m.note_tier(&(self.uid, name), tier);
        }
    }

    /// Release every component whose tier says it has been idle long enough.
    pub fn unload_idle(&self, now: u64) -> usize {
        let Some(m) = self.mgr() else { return 0 };
        let mut freed = 0;
        for (name, tier, last, _) in self.loaded_components() {
            if m.idle_expired(tier, last, now) {
                freed += self.unload_component(&name);
            }
        }
        freed
    }

    pub fn unload_all(&self) -> usize {
        let mut freed = 0;
        for (name, _, _, _) in self.loaded_components() {
            freed += self.unload_component(&name);
        }
        freed
    }

    /// Decoded bytes currently held. Not the same as the on-disk size: this is
    /// the number the node budget is spent on.
    pub fn resident_bytes(&self) -> usize {
        self.loaded_components().iter().map(|(_, _, _, b)| b).sum()
    }

    /// On-disk size of the full-precision vectors — the one component §8.4
    /// allows to stay cold.
    pub fn cold_bytes(&self) -> usize {
        self.vectors
            .keys()
            .filter_map(|p| self.dir.get(&format!("vec/{p}/vectors.full")))
            .map(|(_, len, _)| *len as usize)
            .sum()
    }

    /// Total size of this segment's regions on whatever medium holds them.
    pub fn stored_bytes(&self) -> usize {
        self.dir.values().map(|(_, l, _)| *l as usize).sum()
    }

    pub fn min_key(&self) -> Option<&str> {
        self.ordinals.keys.first().map(|s| s.as_str())
    }

    pub fn max_key(&self) -> Option<&str> {
        self.ordinals.keys.last().map(|s| s.as_str())
    }

    // --- Encoding -------------------------------------------------------

    /// The encoded segment. Loads every component first, since encoding is by
    /// definition a whole-segment operation.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if let SegmentSource::Bytes(b) = self.source() {
            if !b.is_empty() {
                return Ok((*b).clone());
            }
        }
        let mut regions: Vec<(String, Vec<u8>)> = Vec::new();
        for name in self.dir.keys() {
            let bytes = self.read_region(name)?.unwrap_or_default();
            regions.push((name.clone(), bytes));
        }
        Ok(assemble(
            self.id,
            self.level,
            self.num_docs(),
            self.num_vectors(),
            &self.shredded,
            regions,
        ))
    }

    /// Open a segment over a source, reading only the footer.
    ///
    /// Nothing else is decoded: a segment that is never queried costs its
    /// ordinals map and its region directory, and not one byte of index.
    pub fn open(source: SegmentSource) -> Result<Segment> {
        let bad = || Error::Storage("segment: truncated".into());
        let total = source.len()?;
        if total < 12 {
            return Err(Error::Storage("segment: too small".into()));
        }
        let tail = source.read(total - 12, 12)?;
        if &tail[8..12] != MAGIC {
            return Err(Error::Storage("segment: bad magic".into()));
        }
        let mut i = 0usize;
        let flen = get_u32(&tail, &mut i).ok_or_else(bad)? as usize;
        let crc = get_u32(&tail, &mut i).ok_or_else(bad)?;
        if (flen + 12) as u64 > total {
            return Err(Error::Storage("segment: footer overruns the file".into()));
        }
        let fstart = total - 12 - flen as u64;
        let footer = source.read(fstart, flen as u64)?;
        if crc32(&footer) != crc {
            return Err(Error::Storage("segment: footer checksum mismatch".into()));
        }
        let mut i = 0usize;
        let format_version = get_u32(&footer, &mut i).ok_or_else(bad)?;
        if format_version < MIN_READABLE_VERSION || format_version > FORMAT_VERSION {
            return Err(Error::Storage(format!(
                "segment format version {format_version} is not readable by this build \
                 (supports {MIN_READABLE_VERSION}..={FORMAT_VERSION})"
            )));
        }
        // Read past the whole-body checksum without verifying it. Checking it
        // here would mean reading every byte of every segment at every open,
        // which is exactly the cost this file is organised to avoid -- and it
        // would detect nothing new, because the region directory tiles the
        // body and `read_region` checks every region against its own checksum.
        // Nothing else verifies it either; see `assemble`.
        let _body_crc = get_u32(&footer, &mut i).ok_or_else(bad)?;
        let id = get_u64(&footer, &mut i).ok_or_else(bad)?;
        let level = get_u32(&footer, &mut i).ok_or_else(bad)?;
        let _num_docs = get_uvarint(&footer, &mut i).ok_or_else(bad)?;
        let _num_vectors = get_uvarint(&footer, &mut i).ok_or_else(bad)?;
        // Every count below is read out of the footer and handed straight to
        // an allocator, so a damaged one is an allocation request of arbitrary
        // size -- an abort, instead of the `Error::Storage` a damaged segment
        // is supposed to produce. Bound each by the bytes actually left, at
        // the smallest number of bytes one element can occupy: a shredded path
        // is a length-prefixed string, so at least one byte.
        let ns = bounded_len(
            get_uvarint(&footer, &mut i).ok_or_else(bad)?,
            1,
            footer.len().saturating_sub(i),
        )
        .ok_or_else(bad)?;
        let mut shredded = Vec::with_capacity(ns);
        for _ in 0..ns {
            shredded.push(get_str(&footer, &mut i).ok_or_else(bad)?);
        }
        let nd = get_uvarint(&footer, &mut i).ok_or_else(bad)? as usize;
        let mut dir = BTreeMap::new();
        for _ in 0..nd {
            let name = get_str(&footer, &mut i).ok_or_else(bad)?;
            let off = get_u64(&footer, &mut i).ok_or_else(bad)?;
            let len = get_u64(&footer, &mut i).ok_or_else(bad)?;
            let rcrc = get_u32(&footer, &mut i).ok_or_else(bad)?;
            if off.checked_add(len).map(|e| e > fstart).unwrap_or(true) {
                return Err(Error::Storage(format!("segment: region `{name}` overruns the body")));
            }
            dir.insert(name, (off, len, rcrc));
        }

        // The two always-resident regions, and only those. Each is checked
        // against its own directory checksum by `read_one`.
        let read_one = |name: &str| -> Result<Vec<u8>> {
            let (off, len, rcrc) = *dir.get(name).ok_or_else(bad)?;
            let b = source.read(off, len)?;
            if crc32(&b) != rcrc {
                return Err(Error::Storage(format!(
                    "segment: region `{name}` failed its checksum"
                )));
            }
            Ok(b)
        };
        let ordinals = Ordinals::decode(&read_one("ordinals.map")?)?;
        let idx_bytes = read_one("docs.index")?;
        let idx = &idx_bytes[..];
        let mut j = 0usize;
        // One document is one varint delta here, so at least one byte.
        let n =
            bounded_len(get_uvarint(idx, &mut j).ok_or_else(bad)?, 1, idx.len().saturating_sub(j))
                .ok_or_else(bad)?;
        let mut blob_offsets = Vec::with_capacity(n);
        let mut prev = 0u32;
        for _ in 0..n {
            let delta = get_uvarint(idx, &mut j).ok_or_else(bad)?;
            prev = u32::try_from(delta)
                .ok()
                .and_then(|d| prev.checked_add(d))
                .ok_or_else(|| Error::Storage("segment: document offsets overflow".into()))?;
            blob_offsets.push(prev);
        }

        let mut columns = BTreeMap::new();
        let mut text = BTreeMap::new();
        let mut vectors = BTreeMap::new();
        let mut adjacency = BTreeMap::new();
        let mut vec_meta = BTreeMap::new();
        for name in dir.keys() {
            if let Some(rest) = name.strip_prefix("adj/") {
                let path = rest.strip_suffix(".idx").unwrap_or(rest).to_string();
                adjacency.insert(path, Lazy::default());
            } else if let Some(rest) = name.strip_prefix("cols/") {
                // `strip_suffix`, not `trim_end_matches`: the latter strips
                // every trailing repetition, so a path that itself ends in
                // `.col` decodes to the wrong name and two columns can collapse
                // onto one.
                let path = rest.strip_suffix(".col").unwrap_or(rest).to_string();
                columns.insert(path, Lazy::default());
            } else if let Some(rest) = name.strip_prefix("text/") {
                if let Some(path) = rest.strip_suffix("/terms.dict") {
                    text.insert(path.to_string(), Lazy::default());
                }
            } else if let Some(rest) = name.strip_prefix("vec/") {
                if let Some(path) = rest.strip_suffix("/meta") {
                    let meta_bytes = read_one(name)?;
                    let meta = &meta_bytes[..];
                    let mut k = 0usize;
                    let dims = get_uvarint(meta, &mut k).ok_or_else(bad)? as usize;
                    let metric = match *meta.get(k).ok_or_else(bad)? {
                        0 => Metric::Cosine,
                        1 => Metric::L2,
                        _ => Metric::InnerProduct,
                    };
                    vec_meta.insert(path.to_string(), (dims, metric));
                    vectors.insert(path.to_string(), Lazy::default());
                }
            }
        }

        Ok(Segment {
            id,
            uid: crate::residency::next_uid(),
            format_version,
            level,
            ordinals,
            blob_offsets,
            shredded,
            source: RwLock::new(source),
            dir,
            vec_meta,
            tiers: RwLock::new(BTreeMap::new()),
            residency: RwLock::new(None),
            docs: Lazy::default(),
            columns,
            text,
            vectors,
            adjacency,
        })
    }

    pub fn decode(b: &[u8]) -> Result<Segment> {
        Segment::open(SegmentSource::Bytes(Arc::new(b.to_vec())))
    }
}

impl SegmentSource {
    fn len(&self) -> Result<u64> {
        Ok(match self {
            SegmentSource::Encrypted { inner, .. } => {
                crate::cipher::Cipher::plain_len(inner.len()?)?
            }
            SegmentSource::Bytes(b) => b.len() as u64,
            SegmentSource::File(p) | SegmentSource::Archive(p) => std::fs::metadata(p)?.len(),
            SegmentSource::Remote { size, .. } => *size,
        })
    }

    /// The source under any encryption: where the bytes are, whatever they
    /// are. What a copy -- a backup, a move, an archive put -- reads, since
    /// it moves the frames as they are.
    pub fn unwrapped(&self) -> &SegmentSource {
        match self {
            SegmentSource::Encrypted { inner, .. } => inner.unwrapped(),
            other => other,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            SegmentSource::Encrypted { inner, .. } => inner.path(),
            SegmentSource::Bytes(_) | SegmentSource::Remote { .. } => None,
            SegmentSource::File(p) | SegmentSource::Archive(p) => Some(p),
        }
    }
}

/// Reject an element count the remaining bytes could not possibly hold, before
/// anything reserves for it.
///
/// A count read off disk is a claim, not a fact: reserved from directly it is
/// an allocation abort on a damaged file, which is the one outcome a decoder
/// whose job is to report damage must not produce. `min_bytes_each` is the
/// smallest encoded size of one element.
fn bounded_len(n: u64, min_bytes_each: usize, remaining: usize) -> Option<usize> {
    let n = usize::try_from(n).ok()?;
    if n > remaining / min_bytes_each.max(1) {
        return None;
    }
    Some(n)
}

/// Lay regions out and append the footer.
fn assemble(
    id: u64,
    level: u32,
    num_docs: usize,
    num_vectors: usize,
    shredded: &[String],
    regions: Vec<(String, Vec<u8>)>,
) -> Vec<u8> {
    let mut body = Vec::new();
    let mut dir: Vec<(String, u64, u64, u32)> = Vec::new();
    for (name, bytes) in regions {
        let off = body.len() as u64;
        let crc = crc32(&bytes);
        body.extend_from_slice(&bytes);
        dir.push((name, off, bytes.len() as u64, crc));
    }
    let mut footer = Vec::new();
    put_u32(&mut footer, FORMAT_VERSION);
    // A whole-body checksum as well as the per-region ones below. Nothing
    // reads it: the directory tiles the body, so every byte of it is already
    // covered by a region checksum that `read_region` checks when the region
    // is decoded, and `verify()` walks the directory for exactly that reason.
    // It stays in the footer because it is part of the format other builds
    // read, and because a future whole-file scrub would want it.
    put_u32(&mut footer, crc32(&body));
    put_u64(&mut footer, id);
    put_u32(&mut footer, level);
    put_uvarint(&mut footer, num_docs as u64);
    put_uvarint(&mut footer, num_vectors as u64);
    put_uvarint(&mut footer, shredded.len() as u64);
    for p in shredded {
        put_str(&mut footer, p);
    }
    put_uvarint(&mut footer, dir.len() as u64);
    for (name, off, len, crc) in &dir {
        put_str(&mut footer, name);
        put_u64(&mut footer, *off);
        put_u64(&mut footer, *len);
        // Per region, so that a component checks its own integrity when it is
        // decoded rather than the whole file being read at open.
        put_u32(&mut footer, *crc);
    }
    let crc = crc32(&footer);
    let mut out = body;
    let flen = footer.len() as u32;
    out.extend_from_slice(&footer);
    put_u32(&mut out, flen);
    put_u32(&mut out, crc);
    out.extend_from_slice(MAGIC);
    out
}

/// One document on its way into a segment.
pub struct PendingDoc {
    /// The composite `(partition_key, primary_key)` sort key. Segments are
    /// sorted on it, which is what makes a tenant a contiguous ordinal range
    /// (§3.2) and a point lookup a binary search.
    pub sort_key: String,
    pub commit_ts: Timestamp,
    pub doc: Value,
}

pub struct SegmentBuilder {
    docs: Vec<PendingDoc>,
    opts: BuildOpts,
}

/// Split documents into one layer per version depth: the newest version of a
/// key in layer 0, the one it superseded in layer 1, and so on. This is the
/// inverse of the dedup in [`SegmentBuilder::build`] — a caller holding two
/// versions of a key that a pinned horizon still needs builds one segment per
/// layer, because one segment holds one version per key.
///
/// The sort is part of the contract, not a convenience: the depth is counted by
/// comparing each document with its immediate predecessor, so it is only right
/// on input ordered by `(sort_key ASC, commit_ts DESC)`, and callers hand these
/// documents over in whatever order they happened to be stored in.
pub(crate) fn layer_by_version(mut docs: Vec<PendingDoc>) -> Vec<Vec<PendingDoc>> {
    docs.sort_by(|a, b| a.sort_key.cmp(&b.sort_key).then(b.commit_ts.cmp(&a.commit_ts)));
    let mut layers: Vec<Vec<PendingDoc>> = Vec::new();
    let mut depth = 0;
    for pd in docs {
        // Compare against the document just placed — `depth` still holds where
        // it went, so it is the last element of that layer. Keeping the key
        // itself instead would mean cloning one per document, on a path that
        // runs over every flush and every compaction input.
        depth = match layers.get(depth).and_then(|l| l.last()) {
            Some(prev) if prev.sort_key == pd.sort_key => depth + 1,
            _ => 0,
        };
        while layers.len() <= depth {
            layers.push(Vec::new());
        }
        layers[depth].push(pd);
    }
    layers
}

impl SegmentBuilder {
    pub fn new(opts: BuildOpts) -> SegmentBuilder {
        SegmentBuilder { docs: Vec::new(), opts }
    }

    pub fn add(&mut self, d: PendingDoc) {
        self.docs.push(d);
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Seal into a segment. Sorting happens here and nowhere else, so ordinal
    /// order and primary-key order are the same fact.
    pub fn build(mut self, id: u64, level: u32, coll: &Collection) -> Result<Segment> {
        self.docs.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        // A later commit for the same key wins; earlier ones are superseded and
        // never reach the segment. There is no room here for two versions of a
        // key, so a caller that must keep a superseded one — compaction, when a
        // pinned `gc_horizon` still needs it — has to build a second segment.
        self.docs.dedup_by(|a, b| {
            if a.sort_key == b.sort_key {
                if a.commit_ts > b.commit_ts {
                    std::mem::swap(a, b);
                }
                true
            } else {
                false
            }
        });

        let n = self.docs.len();
        let mut ordinals = Ordinals::default();
        let mut blob_offsets = Vec::with_capacity(n + 1);
        let mut blobs = Vec::new();
        blob_offsets.push(0u32);

        let shred: Vec<(String, ValueType)> = coll.shred_candidates();
        let mut col_builders: Vec<ColumnBuilder> =
            shred.iter().map(|(p, t)| ColumnBuilder::new(p, *t)).collect();

        let mut text_builders: BTreeMap<String, (InvertedBuilder, Analyzer)> = BTreeMap::new();
        let mut vec_stores: BTreeMap<String, VectorStore> = BTreeMap::new();
        // One value-to-ordinals map per column an adjacency index names,
        // both of them: a walk follows the index either way.
        let mut adj_pairs: BTreeMap<String, Vec<(String, u32)>> = BTreeMap::new();
        for idx in &coll.indexes {
            match &idx.kind {
                IndexKind::FullText { analyzer } => {
                    text_builders.insert(
                        idx.path.clone(),
                        (InvertedBuilder::new(), Analyzer::parse(analyzer)),
                    );
                }
                IndexKind::Vector { dims, metric } => {
                    vec_stores.insert(idx.path.clone(), VectorStore::new(*dims, *metric));
                }
                IndexKind::Adjacency { to } => {
                    adj_pairs.entry(idx.path.clone()).or_default();
                    adj_pairs.entry(to.clone()).or_default();
                }
                IndexKind::Secondary => {}
            }
        }

        for (ord, pd) in self.docs.iter().enumerate() {
            let ord = ord as u32;
            ordinals.push(pd.sort_key.clone(), pd.commit_ts);
            crate::variant::encode(&pd.doc, &mut blobs);
            // Offsets are u32, so a segment's document region cannot exceed
            // 4 GiB. Refuse to write one that does rather than truncating the
            // offset: the truncation is silent, and what comes back later is a
            // slice of the wrong document.
            if blobs.len() > u32::MAX as usize {
                return Err(Error::Storage(format!(
                    "segment {id}: document region exceeds 4 GiB ({} bytes); lower the \
                     compaction segment cap",
                    blobs.len()
                )));
            }
            blob_offsets.push(blobs.len() as u32);

            for (b, (path, _)) in col_builders.iter_mut().zip(shred.iter()) {
                b.push(ord, pd.doc.path(path).cloned());
            }
            for (path, (ib, an)) in text_builders.iter_mut() {
                let mut toks = Vec::new();
                if let Some(v) = pd.doc.path(path) {
                    analyze_field(v, *an, &mut toks);
                }
                ib.add_doc(ord, &toks);
            }
            for (path, pairs) in adj_pairs.iter_mut() {
                if let Some(Value::Str(k)) = pd.doc.path(path) {
                    pairs.push((k.clone(), ord));
                }
            }
            for (path, vs) in vec_stores.iter_mut() {
                if let Some(v) = pd.doc.path(path) {
                    if let Some(mut f) = extract_vector(v) {
                        if f.len() != vs.dims {
                            return Err(Error::Schema(format!(
                                "`{path}` has {} dimensions but the index declares {}",
                                f.len(),
                                vs.dims
                            )));
                        }
                        crate::vector::distance::prepare(vs.metric, &mut f);
                        vs.push(ord, &f)?;
                    }
                }
            }
        }

        let mut regions: Vec<(String, Vec<u8>)> = Vec::new();
        regions.push(("ordinals.map".into(), ordinals.encode()));
        let mut blob_idx = Vec::new();
        put_uvarint(&mut blob_idx, blob_offsets.len() as u64);
        let mut prev = 0u32;
        for o in &blob_offsets {
            put_uvarint(&mut blob_idx, (o - prev) as u64);
            prev = *o;
        }
        regions.push(("docs.index".into(), blob_idx));
        regions.push(("docs.variant".into(), blobs));

        for (b, (path, _)) in col_builders.into_iter().zip(shred.iter()) {
            regions.push((format!("cols/{path}.col"), b.finish(n).encode()));
        }
        for (path, pairs) in adj_pairs {
            regions.push((format!("adj/{path}.idx"), AdjIndex::build(pairs).encode()));
        }
        for (path, (ib, an)) in text_builders {
            let (dict, postings, lens) = ib.finish();
            regions.push((format!("text/{path}/terms.dict"), dict));
            regions.push((format!("text/{path}/postings.blk"), postings));
            regions.push((format!("text/{path}/doclens"), lens));
            regions.push((format!("text/{path}/analyzer"), an.name().as_bytes().to_vec()));
        }
        let mut num_vectors = 0usize;
        for (path, vs) in vec_stores.iter_mut() {
            vs.seal(self.opts.quantizer, self.opts.hnsw, self.opts.flat_tier_max);
            num_vectors = num_vectors.max(vs.len());
            regions.push((format!("vec/{path}/vectors.full"), vs.encode_full()));
            regions.push((format!("vec/{path}/vectors.codes"), vs.codes.encode_bytes()));
            regions.push((
                format!("vec/{path}/vectors.graph"),
                vs.graph.as_ref().map(|g| g.encode()).unwrap_or_default(),
            ));
            regions.push((format!("vec/{path}/vec_ordinals.map"), vs.encode_map()));
            let mut meta = Vec::new();
            put_uvarint(&mut meta, vs.dims as u64);
            meta.push(vs.metric as u8);
            regions.push((format!("vec/{path}/meta"), meta));
        }
        // Free the builders before opening: the segment reads what it needs
        // from the encoded bytes, so holding both shapes at once would double
        // the peak memory of every flush.
        drop(vec_stores);

        let shredded: Vec<String> = shred.into_iter().map(|(p, _)| p).collect();
        let bytes = assemble(id, level, n, num_vectors, &shredded, regions);
        Segment::open(SegmentSource::Bytes(Arc::new(bytes)))
    }
}

/// Analyze a text field. An array of strings is concatenated into the index
/// with a position gap between elements, so a phrase query cannot match across
/// element boundaries (§2.2).
pub fn analyze_field(v: &Value, an: Analyzer, out: &mut Vec<(String, u32)>) {
    match v {
        Value::Str(s) => an.analyze(s, 0, out),
        Value::Array(items) => {
            let mut pos = 0u32;
            for it in items {
                let before = out.len();
                if let Value::Str(s) = it {
                    an.analyze(s, pos, out);
                }
                let last = out[before..].last().map(|(_, p)| *p).unwrap_or(pos);
                pos = last + ARRAY_POSITION_GAP;
            }
        }
        _ => {}
    }
}

pub fn extract_vector(v: &Value) -> Option<Vec<f32>> {
    let a = v.as_array()?;
    let mut out = Vec::with_capacity(a.len());
    for x in a {
        out.push(x.as_f64()? as f32);
    }
    Some(out)
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_segment_and_component_decoding_never_panics() {
        let (s, _c) = seg();
        let bytes = s.encode().unwrap();
        crate::fuzz::sweep(61, &[bytes], 1500, |b| {
            if let Ok(seg) = Segment::decode(b) {
                // Whatever decoded is also read without panicking.
                let _ = seg.column("status");
                let _ = seg.column("tags");
                let _ = seg.num_docs();
            }
        });
        let col = s.column("status").unwrap().expect("a status column").encode();
        let tags = s.column("tags").unwrap().expect("a tags column").encode();
        crate::fuzz::sweep(62, &[col, tags], 5000, |b| {
            let _ = Column::decode(b);
        });
        let adj = AdjIndex::build(vec![
            ("a".into(), 1),
            ("a".into(), 5),
            ("b".into(), 2),
            ("zz".into(), 9),
        ])
        .encode();
        crate::fuzz::sweep(63, &[adj], 5000, |b| {
            if let Ok(a) = AdjIndex::decode(b) {
                let _ = a.probe("a");
                let _ = a.probe("nope");
            }
        });
    }
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::catalog::{ColumnDef, IndexDef};
    use crate::column::CmpOp;
    use crate::json;
    use crate::text::query::TextQuery;
    use crate::text::scorer::{collect_top_k, compile, Bm25Params, GlobalStats};
    use crate::vector::SearchOpts;

    fn collection() -> Collection {
        let mut c = Collection::new("articles", "id", Some("tenant_id".into()));
        c.declared.push(ColumnDef { path: "tenant_id".into(), ty: ValueType::Str, not_null: true });
        c.declared.push(ColumnDef {
            path: "published_at".into(),
            ty: ValueType::Timestamp,
            not_null: false,
        });
        c.indexes.push(IndexDef::new(
            "articles_body",
            "body",
            IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::default(),
        ));
        c.indexes.push(IndexDef::new(
            "articles_emb",
            "embedding",
            IndexKind::Vector { dims: 8, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        c
    }

    fn seg() -> (Segment, Collection) {
        let mut c = collection();
        let mut b = SegmentBuilder::new(BuildOpts::default());
        let mut rng = Rng::new(3);
        for i in 0..300u32 {
            let tenant = format!("t{}", i % 3);
            let emb: Vec<String> = (0..8).map(|_| format!("{:.4}", rng.next_normal())).collect();
            let doc = json::parse(&format!(
                r#"{{"id":"doc-{i:04}","tenant_id":"{tenant}","status":"{}",
                    "body":"vector search segment number {i} with hybrid retrieval",
                    "tags":["t{}","common"],
                    "published_at":{},
                    "embedding":[{}]}}"#,
                if i % 4 == 0 { "draft" } else { "published" },
                i % 5,
                1_700_000_000_000_000i64 + i as i64 * 1_000_000,
                emb.join(",")
            ))
            .unwrap();
            c.observe_doc(&doc);
            b.add(PendingDoc {
                sort_key: format!("{tenant}\u{1}doc-{i:04}"),
                commit_ts: 1000 + i as u64,
                doc,
            });
        }
        (b.build(1, 0, &c).unwrap(), c)
    }

    /// The adjacency region is one sorted value-to-ordinals map per column
    /// the index names, built at seal for both columns: a probe answers the
    /// ordinals holding a key, ascending, or nothing for a key no document
    /// holds, and a document whose value there is not a string is in no
    /// run. It survives the bytes, is listed as its own component under the
    /// index's tier, and a segment sealed without the index has none.
    #[test]
    fn an_adjacency_region_probes_both_columns_and_round_trips() {
        let mut c = Collection::new("cites", "id", None);
        c.nodes_of = Some("papers".into());
        c.declared.push(ColumnDef { path: "src".into(), ty: ValueType::Str, not_null: true });
        c.declared.push(ColumnDef { path: "dst".into(), ty: ValueType::Str, not_null: true });
        c.indexes.push(IndexDef::new(
            "cites_adj",
            "src",
            IndexKind::Adjacency { to: "dst".into() },
            Tier::Cached,
        ));
        let edges = [("e1", "a", "b"), ("e2", "a", "c"), ("e3", "b", "c"), ("e4", "c", "a")];
        let mut b = SegmentBuilder::new(BuildOpts::default());
        for (i, (id, src, dst)) in edges.iter().enumerate() {
            let doc =
                json::parse(&format!(r#"{{"id":"{id}","src":"{src}","dst":"{dst}"}}"#)).unwrap();
            b.add(PendingDoc { sort_key: id.to_string(), commit_ts: 10 + i as u64, doc });
        }
        // A fifth edge whose `src` is a number: in no run of the `src` map.
        let doc = json::parse(r#"{"id":"e5","src":7,"dst":"a"}"#).unwrap();
        b.add(PendingDoc { sort_key: "e5".into(), commit_ts: 20, doc });
        let s = b.build(1, 0, &c).unwrap();
        let back = Segment::decode(&s.encode().unwrap()).unwrap();
        for seg in [&s, &back] {
            let by_src = seg.adjacency("src").unwrap().expect("the probed column's map");
            let by_dst = seg.adjacency("dst").unwrap().expect("the read column's map too");
            // Ordinals are by sort key: e1=0, e2=1, e3=2, e4=3, e5=4.
            assert_eq!(by_src.probe("a"), &[0, 1]);
            assert_eq!(by_src.probe("b"), &[2]);
            assert_eq!(by_src.probe("c"), &[3]);
            assert_eq!(by_src.probe("nobody"), &[] as &[u32]);
            assert_eq!(by_src.len(), 3, "the numeric src is in no run");
            assert_eq!(by_dst.probe("a"), &[3, 4]);
            assert_eq!(by_dst.probe("c"), &[1, 2]);
            assert!(
                seg.adjacency("id").unwrap().is_none(),
                "no map for a column the index does not name"
            );
        }
        assert_eq!(*s.adjacency("src").unwrap().unwrap(), *back.adjacency("src").unwrap().unwrap());
        let listed: Vec<String> = back.loaded_components().into_iter().map(|(n, ..)| n).collect();
        assert!(
            listed.contains(&"adj:src".to_string()) && listed.contains(&"adj:dst".to_string()),
            "{listed:?}"
        );
        assert!(back.unload_component("adj:src") > 0);
        assert!(!back.loaded_components().iter().any(|(n, ..)| n == "adj:src"));

        // Without the index, no region: the walk scans such a segment.
        c.indexes.clear();
        let mut b = SegmentBuilder::new(BuildOpts::default());
        let doc = json::parse(r#"{"id":"e1","src":"a","dst":"b"}"#).unwrap();
        b.add(PendingDoc { sort_key: "e1".into(), commit_ts: 10, doc });
        let plain = b.build(2, 0, &c).unwrap();
        assert!(plain.adjacency("src").unwrap().is_none());

        // The codec refuses what it cannot have written: keys out of order.
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, 2);
        for k in ["b", "a"] {
            put_str(&mut bytes, k);
            put_uvarint(&mut bytes, 1);
            put_uvarint(&mut bytes, 0);
        }
        assert!(AdjIndex::decode(&bytes).is_err());
    }

    #[test]
    fn segment_round_trips_with_every_index() {
        let (s, _) = seg();
        let bytes = s.encode().unwrap();
        let back = Segment::decode(&bytes).unwrap();
        assert_eq!(back.id, 1);
        assert_eq!(back.num_docs(), 300);
        assert_eq!(back.num_vectors(), 300);
        assert_eq!(back.ordinals.keys, s.ordinals.keys);
        assert_eq!(back.document(0).unwrap(), s.document(0).unwrap());
        assert!(back.is_shredded("tenant_id"));
        assert!(back.is_shredded("published_at"));
        // `status` was never declared; inference promoted it anyway.
        assert!(back.is_shredded("status"), "shredded: {:?}", back.shredded);
        assert_eq!(back.analyzer_of("body").unwrap(), Some(Analyzer::English));
    }

    #[test]
    fn a_tenant_is_a_contiguous_ordinal_range() {
        let (s, _) = seg();
        let (lo, hi) = s.ordinals.range(Some("t1\u{1}"), Some("t1\u{1}\u{10FFFF}"));
        assert_eq!(hi - lo, 100);
        for o in lo..hi {
            assert!(s.ordinals.keys[o].starts_with("t1\u{1}"));
        }
        // The partition-key filter is a range, not a bitmap scan.
        let bm = Bitmap::range(s.num_docs(), lo, hi);
        assert_eq!(bm.popcount(), 100);
    }

    #[test]
    fn corrupt_footer_is_detected() {
        let (s, _) = seg();
        let mut bytes = s.encode().unwrap();
        let n = bytes.len();
        bytes[n - 20] ^= 0xFF;
        assert!(Segment::decode(&bytes).is_err());
    }

    /// A checksum over the footer alone leaves the body — which is all of the
    /// data — unprotected. Every corrupt byte must produce an error, never a
    /// panic and never a silently wrong segment.
    ///
    /// Open is deliberately *not* where most of that is caught any more: it
    /// reads two regions, so a bit-flip in the postings is found when the
    /// postings are decoded, and by `verify()` at any time. What must hold is
    /// that no corrupt byte is ever accepted as valid data, and that nothing
    /// panics on the way to saying so.
    #[test]
    fn every_corrupt_body_byte_is_rejected_cleanly() {
        let (s, _) = seg();
        let bytes = s.encode().unwrap();
        let body_len = bytes.len() - 64; // comfortably inside the body
        let mut checked = 0;
        for i in (0..body_len).step_by(37) {
            let mut b = bytes.clone();
            b[i] ^= 0xFF;
            let r = std::panic::catch_unwind(|| {
                let seg = Segment::decode(&b)?;
                seg.verify()?;
                // Everything a query would decode, too — the regions and the
                // structures built from them.
                seg.docs()?;
                for p in seg.column_paths() {
                    seg.column(&p)?;
                }
                for p in seg.text_paths() {
                    seg.text_index(&p)?;
                }
                for p in seg.vector_paths() {
                    seg.vector_index(&p)?;
                }
                Ok::<_, Error>(())
            });
            match r {
                Ok(Ok(())) => panic!("byte {i}: corruption accepted as a valid segment"),
                Ok(Err(_)) => checked += 1,
                Err(_) => panic!("byte {i}: decode panicked instead of reporting an error"),
            }
        }
        assert!(checked > 20, "only {checked} bytes exercised");
    }

    #[test]
    fn opening_a_segment_reads_only_the_regions_it_needs() {
        let (s, _) = seg();
        let bytes = s.encode().unwrap();
        let total = bytes.len();
        let d = std::env::temp_dir().join(format!("celastro-open-{}.seg", std::process::id()));
        std::fs::write(&d, &bytes).unwrap();
        let opened = Segment::open(SegmentSource::File(d.clone())).unwrap();
        assert_eq!(opened.num_docs(), s.num_docs());
        assert_eq!(opened.resident_bytes(), 0, "no component is decoded at open");
        // The always-resident regions are a small fraction of the file; if open
        // were still checksumming the whole body this bound would be the file.
        let eager: usize = ["ordinals.map", "docs.index"]
            .iter()
            .filter_map(|n| opened.region(n))
            .map(|(_, l, _)| l as usize)
            .sum();
        assert!(
            eager * 3 < total,
            "open read {eager} of {total} bytes; it should read the two resident regions only"
        );
        let _ = std::fs::remove_file(&d);
    }

    /// A path that itself ends in `.col` must survive the region-name round
    /// trip.
    #[test]
    fn a_column_path_ending_in_col_round_trips() {
        let mut c = Collection::new("t", "id", None);
        c.declared.push(ColumnDef { path: "meta.col".into(), ty: ValueType::Str, not_null: false });
        let mut b = SegmentBuilder::new(BuildOpts::default());
        for i in 0..8u32 {
            let doc = json::parse(&format!(r#"{{"id":"d{i}","meta":{{"col":"c{i}"}}}}"#)).unwrap();
            c.observe_doc(&doc);
            b.add(PendingDoc { sort_key: format!("d{i}"), commit_ts: 100 + i as u64, doc });
        }
        let seg = b.build(1, 0, &c).unwrap();
        assert!(seg.is_shredded("meta.col"));
        let back = Segment::decode(&seg.encode().unwrap()).unwrap();
        assert!(back.is_shredded("meta.col"), "shredded: {:?}", back.shredded);
        assert!(!back.is_shredded("meta"));
    }

    #[test]
    fn all_three_retrieval_modes_meet_in_one_ordinal_space() {
        let (s, _) = seg();
        let s = Segment::decode(&s.encode().unwrap()).unwrap();
        let n = s.num_docs();

        // Structured: a bitmap.
        let tenant = s.column("tenant_id").unwrap().unwrap();
        let status = s.column("status").unwrap().unwrap();
        let mut filter = tenant.filter(CmpOp::Eq, &Value::Str("t1".into()));
        filter.and_inplace(&status.filter(CmpOp::Eq, &Value::Str("published".into())));
        assert!(filter.popcount() > 0 && filter.popcount() < 100);

        // Text: posting lists of ordinals, admitted through that same bitmap.
        let sealed = s.text_index("body").unwrap().unwrap();
        let src = sealed.source();
        let stats = GlobalStats {
            num_docs: n as u64,
            avg_doc_len: src.total_doc_len() as f64 / n as f64,
            doc_freq: src.all_terms().into_iter().map(|(t, d)| (t, d as u64)).collect(),
            // No coordinator in this test: the segment is the whole world.
            expansions: Default::default(),
            exact: true,
            prefix_cap: crate::text::scorer::PREFIX_EXPANSION_LIMIT,
        };
        let q = TextQuery::parse("hybrid retrieval", Analyzer::English).unwrap();
        // No deletes in this fixture, so every ordinal is visible and a full
        // bitmap is this source's actual visibility.
        let live = Bitmap::all(n);
        let c = compile(&q, &src, &live, &stats, Bm25Params::default()).unwrap();
        let hits = collect_top_k(c.scorer.unwrap(), &filter, c.excluded.as_ref(), 10, None);
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|h| filter.get(h.ord as usize)));

        // Vector: an ordinal set through vec_ordinals.map, same bitmap.
        let vecs = s.vector_index("embedding").unwrap().unwrap();
        let query = vecs.vector(0).to_vec();
        let (vhits, report) = vecs.search(&query, 10, &filter, &SearchOpts::default());
        assert_eq!(vhits.len(), 10);
        assert!(vhits.iter().all(|(d, _)| filter.get(*d as usize)));
        assert!(report.selectivity > 0.0 && report.selectivity < 1.0);

        // And the three sets are directly intersectable, with no translation.
        let text_bm = Bitmap::from_sorted(n, hits.iter().map(|h| h.ord));
        let vec_bm = Bitmap::from_sorted(n, vhits.iter().map(|(d, _)| *d));
        let _union = {
            let mut u = text_bm.clone();
            u.or_inplace(&vec_bm);
            u
        };
        assert!(text_bm.and_popcount(&filter) == hits.len());
        assert!(vec_bm.and_popcount(&filter) == vhits.len());
    }

    #[test]
    fn unshredded_paths_fall_back_to_variant_decode() {
        let (s, _) = seg();
        // `tags` is an array; the builder shreds it as a multi-value column,
        // but an arbitrary nested path is not shredded at all.
        assert!(!s.is_shredded("nowhere.at.all"));
        let reader = s.blob_reader().unwrap();
        let blobs = |ord: u32| reader.get(ord).map(|o| o.map(|b| b.to_vec()));
        let out = crate::column::filter_variant(
            &blobs,
            s.num_docs(),
            "status",
            CmpOp::Eq,
            &Value::Str("draft".into()),
            &Bitmap::all(s.num_docs()),
        )
        .unwrap();
        // Same answer as the column, by a slower road.
        let col = s.column("status").unwrap().unwrap();
        assert_eq!(out, col.filter(CmpOp::Eq, &Value::Str("draft".into())));
    }

    /// Compaction depends on this: it is why a version that a pinned
    /// `gc_horizon` still needs is written to a different output segment than
    /// the version that superseded it.
    #[test]
    fn a_superseded_version_does_not_survive_beside_the_one_that_replaced_it() {
        let c = collection();
        let first = json::parse(
            r#"{"id":"doc-0001","tenant_id":"t0","body":"first",
                "embedding":[1,0,0,0,0,0,0,0]}"#,
        )
        .unwrap();
        let second = json::parse(
            r#"{"id":"doc-0001","tenant_id":"t0","body":"second",
                "embedding":[0,1,0,0,0,0,0,0]}"#,
        )
        .unwrap();
        let key = "t0\u{1}doc-0001";
        let mut b = SegmentBuilder::new(BuildOpts::default());
        // Newest first, so the survivor cannot be an artefact of input order.
        b.add(PendingDoc { sort_key: key.into(), commit_ts: 20, doc: second });
        b.add(PendingDoc { sort_key: key.into(), commit_ts: 10, doc: first });
        let s = b.build(7, 0, &c).unwrap();

        assert_eq!(s.num_docs(), 1);
        assert_eq!(s.ordinals.commit_ts, vec![20]);
        assert_eq!(s.document(0).unwrap().path("body").unwrap().as_str(), Some("second"));
    }

    /// A well-framed segment whose footer counts are lies: checksums all
    /// correct, so the decoder reaches the counts and believes them. Built by
    /// hand because `assemble` only ever writes honest ones.
    fn forged_segment(shredded: u64, docs_index: Option<Vec<u8>>) -> Vec<u8> {
        let mut body = Vec::new();
        let mut dir: Vec<(String, u64, u64, u32)> = Vec::new();
        let regions: Vec<(&str, Vec<u8>)> = match docs_index {
            Some(idx) => vec![("ordinals.map", Ordinals::default().encode()), ("docs.index", idx)],
            None => Vec::new(),
        };
        for (name, bytes) in &regions {
            let off = body.len() as u64;
            dir.push(((*name).to_string(), off, bytes.len() as u64, crc32(bytes)));
            body.extend_from_slice(bytes);
        }
        let mut footer = Vec::new();
        put_u32(&mut footer, FORMAT_VERSION);
        put_u32(&mut footer, crc32(&body));
        put_u64(&mut footer, 1); // id
        put_u32(&mut footer, 0); // level
        put_uvarint(&mut footer, 0); // documents
        put_uvarint(&mut footer, 0); // vectors
        put_uvarint(&mut footer, shredded);
        put_uvarint(&mut footer, dir.len() as u64);
        for (name, off, len, crc) in &dir {
            put_str(&mut footer, name);
            put_u64(&mut footer, *off);
            put_u64(&mut footer, *len);
            put_u32(&mut footer, *crc);
        }
        let crc = crc32(&footer);
        let flen = footer.len() as u32;
        let mut out = body;
        out.extend_from_slice(&footer);
        put_u32(&mut out, flen);
        put_u32(&mut out, crc);
        out.extend_from_slice(MAGIC);
        out
    }

    /// Both counts are reserved from before anything reads an element, so an
    /// impossible one has to be refused rather than allocated: the whole point
    /// of these decoders is to turn a damaged file into an error, and an
    /// allocation the process cannot serve is not an error, it is the end of
    /// the process.
    #[test]
    fn footer_counts_are_bounded_by_the_bytes_behind_them() {
        assert!(matches!(Segment::decode(&forged_segment(u64::MAX, None)), Err(Error::Storage(_))));
        // One shredded path claimed and no bytes to hold it is the same lie in
        // small; an honestly empty footer still decodes.
        assert!(Segment::decode(&forged_segment(1, None)).is_err());
        assert!(Segment::decode(&forged_segment(0, Some(Vec::new()))).is_err());

        let mut idx = Vec::new();
        put_uvarint(&mut idx, u64::MAX);
        assert!(matches!(Segment::decode(&forged_segment(0, Some(idx))), Err(Error::Storage(_))));

        // And the honest version of the same region decodes to one offset.
        let mut idx = Vec::new();
        put_uvarint(&mut idx, 1);
        put_uvarint(&mut idx, 4);
        let s = Segment::decode(&forged_segment(0, Some(idx))).unwrap();
        assert_eq!(s.blob_offsets, vec![4]);
    }
}
