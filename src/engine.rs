//! The engine: catalog, shards, and statement execution.
//!
//! [`Db`] plays two roles that the distributed design separates. It is the
//! **control plane** (§10) — collection and index definitions, the logical path
//! catalog, the tablet map, cached global term statistics — and it is the
//! **coordinator** (§8.1), which is stateless, caches the tablet map, and pins
//! a snapshot before scattering. Keeping them as distinct method groups on one
//! struct is deliberate: it is the seam a distributed build splits along.
//!
//! A collection can be created with more than one shard here, with explicit
//! key-range split points. Dynamic split and merge are not implemented, but
//! multiple shards in one process are enough to make the coordinator boundary
//! real — and, more usefully, to make the distributed exit criterion testable
//! now: *in exact mode, results are bit-identical regardless of shard count*.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::catalog::{Catalog, Collection, ColumnDef, IndexDef, IndexKind, PathTally, Tablet};
use crate::codec::{crc32, put_u32};
use crate::compaction::{self, CompactionOpts};
use crate::error::{Error, Result};
use crate::lifecycle::{self, IndexActivity, LifecyclePolicy};
use crate::memtable::{FlushThresholds, MemtableBudget};
use crate::mvcc::DeleteLog;
use crate::plan::exec::{self, ExecInput, QueryResult};
use crate::plan::fusion::Candidate;
use crate::plan::service::{Local, ShardService, TermStats};
use crate::plan::walk::{self, WalkSpec};
use crate::residency::{Placement, ResidencyManager, ResidencyOpts, Tier};
use crate::segment::BuildOpts;
use crate::segment::{PendingDoc, SegmentBuilder, SegmentSource};
use crate::shard::{sort_key, Searchable, SegmentHandle, Shard, ShardOpts};
use crate::sql::{self, ast::*};
use crate::text::scorer::{Expansion, GlobalStats};
use crate::time::{Hlc, Timestamp};
use crate::value::{Value, ValueType};

/// How stale cached global term statistics may get before a refresh. "Shards
/// publish per-term document frequency and average document length on a short
/// interval" (§8.2) — this is that interval, in writes rather than seconds,
/// because writes are what actually move the numbers.
const STATS_REFRESH_WRITES: u64 = 512;

/// How many terms the statistics cache keeps per indexed path. `doc_freq` is
/// filled on demand by the queries that ask for it, so without a cap it grows
/// towards the vocabulary — the corpus-wide dictionary this cache stopped
/// holding. Four thousand covers the head of any realistic query distribution;
/// past it the oldest fill is dropped and the next query that wants it pays
/// one masked walk to get it back.
const STATS_TERM_CAP: usize = 4096;

/// How many distinct `(path, prefix)` pairs one statement may name, SUMMED
/// over its indexed paths, on a collection whose prefix expansion cap is
/// `cap`.
///
/// Summed, not per path: `Db::run_select` compares the total against this, and
/// a rule stated per path is a rule an operator cannot use — five prefixes on
/// each of two paths is inside a per-path eight and outside this one.
///
/// Distinct is the whole cost model, and the bound and its refusal have to say
/// the same thing: `Db::run_select` resolves each `(path, prefix)` pair ONCE
/// and every clause spelling it reads that one expansion, so a statement
/// writing `a*` twelve times on `body` costs exactly what writing it once
/// costs, and is admitted. Two paths spelling the same prefix are two
/// enumerations and count two, because a term list is only valid for the
/// dictionary it came from.
///
/// Each one costs a dictionary enumeration in every unit of every shard plus
/// up to `cap` gathered document frequencies, and the `text_match` grammar
/// puts no bound on how many of them a query string holds — so without this
/// the multiplier is chosen by whoever writes the query. Measured over 20000
/// documents in 6 units at the default cap: one leaf 48 ms, 24 leaves 1.2 s,
/// and the 24-leaf statement never warms, because its 12288 gathered terms
/// evict each other out of a [`STATS_TERM_CAP`]-entry map.
///
/// The value is exactly the number of full-cap expansions that fit that cache,
/// so a statement whose leaves are ALL prefixes on one path holds every term it
/// names at once and a repeat of it is warm. The cache is per path, and the fit
/// covers the expanded terms alone: a statement that also names `n` ordinary
/// terms on the same path overruns by `n`, and since eviction is oldest-fill
/// first over a sorted fill order, the `n` lexicographically first terms of the
/// merged list are dropped and re-gathered by every repeat. That is one masked
/// walk for `n` terms per query, not the 12288-term thrash this bound exists to
/// stop. Over it the statement is REFUSED rather than quietly cut:
/// this change exists to stop prefix queries from silently answering less than
/// they were asked, and a silent aggregate cap would be the same failure one
/// level up.
///
/// DERIVED from the cap rather than set beside it, which is what keeps the two
/// from contradicting each other: an operator who raises a collection's
/// `prefix_expansion` has, by that one act, lowered how many prefixes its
/// statements may carry — eight at 512, two at 2048, one at the ceiling — and
/// no second setting can admit a statement the cache cannot hold.
fn prefix_leaves_limit(cap: usize) -> usize {
    // At least one: a cap the ceiling admits always fits the cache once, and a
    // budget of zero would refuse every prefix.
    (STATS_TERM_CAP / cap).max(1)
}

/// The most a collection's `prefix_expansion` can be set to: the size of the
/// per-path statistics cache, so that even a single expansion fits it whole.
/// Over this, one prefix would evict its own terms out of the cache and the
/// statement would never warm, which is the cost `prefix_leaves_limit`
/// exists to bound.
pub const PREFIX_EXPANSION_CEILING: usize = STATS_TERM_CAP;

/// Refuse a `prefix_expansion` the engine cannot honour: zero expands nothing,
/// and over the ceiling a single prefix overruns the statistics cache. Also
/// what an import checks, since an export carries its collection's cap and a
/// build with a smaller cache than the one that wrote it must refuse rather
/// than overrun.
fn check_prefix_cap(n: usize) -> Result<()> {
    if n == 0 || n > PREFIX_EXPANSION_CEILING {
        return Err(Error::Plan(format!(
            "prefix_expansion must be between 1 and {PREFIX_EXPANSION_CEILING}, not {n}: the \
             ceiling is the {STATS_TERM_CAP}-term statistics cache each indexed path keeps, and \
             one expansion wider than the cache would evict its own terms and never warm"
        )));
    }
    Ok(())
}

/// How coarsely the per-index access clocks are written to disk. The lifecycle
/// DSL's finest unit is a minute, so persisting to within a minute is exact at
/// the resolution anyone can express — and it keeps a read from becoming a
/// catalog write.
const ACTIVITY_PERSIST_MICROS: u64 = 60_000_000;

/// What a node is for. A data node holds shards and coordinates the
/// statements that reach it; a coordinator holds no shards and only
/// coordinates, so the fusion, the fetch and a walk's frontier run on a
/// node with no seal or compaction of its own to contend with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Data,
    Coordinator,
}

/// How often the console's sweep pulls every peer's catalog and
/// reconciles it with this node's, seconds; `CELASTRO_RECONCILE_SECS`
/// changes it, `0` turns the sweep off. `ATTACH NODE` reconciles at once.
pub const RECONCILE_SECS: u64 = 30;

/// A peer's clock this far from this node's is said in `SHOW HEALTH` and
/// logged at `ATTACH`; this far and more is refused at `ATTACH`. Every
/// timestamp, and every tombstone the reconciliation compares by, is a
/// wall clock through the HLC, so two nodes whose clocks disagree by more
/// than a statement takes disagree about which of two statements was last.
pub const CLOCK_WARN_MICROS: i64 = 500_000;
pub const CLOCK_REFUSE_MICROS: i64 = 5_000_000;

/// A certificate this close to its end is flagged by `SHOW HEALTH`: two
/// weeks, the time a rotation takes to notice and do.
pub const CERTIFICATE_WARN_SECS: i64 = 14 * 86_400;

/// What `SHOW HEALTH` and the sweep have seen of a peer's hello.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerSeen {
    /// The largest epoch a hello from the address has carried: the newest
    /// process seen there.
    pub epoch: u64,
    /// The peer's clock minus this node's at the last hello, microseconds.
    pub skew_micros: i64,
}

/// What a message says about a node a definition did not reach.
const RECONCILE_NOTE: &str =
    "the catalog reconciles at ATTACH and every 30 s, CELASTRO_RECONCILE_SECS";

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Data => "data",
            Role::Coordinator => "coordinator",
        }
    }

    /// `data` or `coordinator`, as `CELASTRO_ROLE` spells them.
    pub fn parse(s: &str) -> Option<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "data" => Some(Role::Data),
            "coordinator" => Some(Role::Coordinator),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DbOpts {
    pub thresholds: FlushThresholds,
    pub build: BuildOpts,
    pub compaction: CompactionOpts,
    /// Node-level memtable budget (§4.3).
    pub memtable_budget_bytes: usize,
    /// Sample rate for the continuous recall harness (§12.1): one in N vector
    /// queries is logged for replay.
    pub recall_sample_rate: u64,
    /// Node-level residency: how much of the segment corpus may be decoded at
    /// once, and how long an idle component of each tier is kept.
    pub residency: ResidencyOpts,
    /// Evaluate lifecycle policies automatically every N writes. `0` disables
    /// it, leaving `RUN LIFECYCLE` as the only trigger — which is what the
    /// tests want, and what an operator who prefers a cron job wants.
    pub lifecycle_interval_writes: u64,
    /// How long a statement may run before it is refused, in milliseconds,
    /// unless it says otherwise with `WITH (deadline_ms = N)`; `None` for no
    /// limit. On by default, because a statement's cost is otherwise bounded
    /// by nothing a caller did not opt into: a wide prefix, a filter-aware
    /// graph traversal at low selectivity or a scan of a large collection
    /// each run as long as they run. `WITH (no_deadline)` lifts it for one
    /// statement. Checked inside the expensive loops, not only between
    /// shards; see the `deadline` module.
    pub statement_deadline_ms: Option<u64>,
    /// Where the `archived` tier lives: an S3-compatible endpoint, or with no
    /// endpoint the local `archive/` directory that stands in for one. The
    /// credentials are read from the environment at open, never stored.
    pub archive: crate::objstore::ArchiveOpts,
    /// Where `BACKUP TO '<name>'` and `RESTORE FROM '<name>'` resolve a
    /// relative name, and the one directory an absolute path may point
    /// into when it is set: a console that takes SQL from the network
    /// should not be a way to write anywhere on the node. Unset, only
    /// absolute paths and `s3://` destinations are taken.
    pub backup_dir: Option<PathBuf>,
    /// How many documents of one `INSERT` a shard appends before it syncs
    /// the log: a statement of more is taken in chunks of this many, one
    /// `fdatasync` each. Larger means fewer disk round trips for a bulk
    /// load and more records unsynced at any one moment (the statement is
    /// not acknowledged until all of them are); 1000 by default.
    pub insert_batch: usize,
    /// A seal that is due freezes the memtable and leaves the build to the
    /// console's maintenance thread, holding no lock for it; off, it
    /// builds inline under the write lock. The console turns it on when it
    /// runs its maintenance thread ([`Db::set_background_seal`]).
    pub background_seal: bool,
    /// Encryption at rest: the master key that wraps the database's data
    /// key (`CELASTRO_MASTER_KEY_FILE`, 32 bytes, or `CELASTRO_MASTER_KEY`
    /// as hex). With it, `<dir>/KEY` is made at the first open of an empty
    /// directory and opened at every later one; without it an encrypted
    /// database is refused. Never written anywhere.
    pub master_key: Option<crate::cipher::Secret<32>>,
    /// A wrapped data key to adopt when the directory has none
    /// (`CELASTRO_KEY_FILE`): how the pods of a cluster share one data key,
    /// so a shard moves between them and a backup restores on any of them.
    pub key_file: Option<PathBuf>,
    /// What this node is for: a data node holds shards and coordinates; a
    /// coordinator holds no shards, takes none from a placement, a
    /// rebalance or a move, and only coordinates (`CELASTRO_ROLE`).
    pub role: Role,
    /// Whether a write is acknowledged only once every follower confirmed
    /// it on disk (`sync`, the default) or at once (`async`):
    /// `CELASTRO_REPLICATION`.
    pub replication_sync: bool,
    /// The node that renews every holder's lease and, with `auto_failover`,
    /// promotes a follower of a holder that stopped answering:
    /// `CELASTRO_STEWARD`, or the lowest attached address.
    pub steward: Option<String>,
    /// The group that elects the steward among themselves, addresses:
    /// `CELASTRO_STEWARDS`. Set, `steward` is ignored and the steward is
    /// whichever of the group holds the current term; a node not in the
    /// group follows the elected one's leases.
    pub stewards: Option<Vec<String>>,
    /// Whether the steward promotes on its own, and a holder whose lease
    /// ran out refuses writes: `CELASTRO_AUTO_FAILOVER`. Off by default:
    /// promotion is the operator's, and no lease gates a write.
    pub auto_failover: bool,
    /// How long a lease lasts, seconds: `CELASTRO_LEASE_SECS`.
    pub lease_secs: u64,
    /// Who this node is and which nodes share its tablets. Only the `minimal`
    /// tier consults it, and only to decide whether this node is the one
    /// keeping a given index decoded.
    pub placement: Placement,
    /// This node's advertised wire address, `tcp://host:port`: its name in
    /// every placement map, and what other nodes connect to. `None` is a
    /// single node that places every shard on itself and can attach nobody.
    /// `celastro` reads it from `CELASTRO_NODE`.
    pub node: Option<String>,
    /// Encryption in transit, when the process was given certificates
    /// (`crate::tls`): what the wire to another node is wrapped in. `None`
    /// is plain TCP.
    pub tls: Option<Arc<crate::tls::Tls>>,
}

impl Default for DbOpts {
    fn default() -> Self {
        DbOpts {
            thresholds: FlushThresholds::default(),
            build: BuildOpts::default(),
            compaction: CompactionOpts::default(),
            memtable_budget_bytes: 1 << 30,
            recall_sample_rate: 8,
            residency: ResidencyOpts::default(),
            lifecycle_interval_writes: 0,
            placement: Placement::default(),
            statement_deadline_ms: Some(DEFAULT_STATEMENT_DEADLINE_MS),
            archive: crate::objstore::ArchiveOpts::default(),
            backup_dir: None,
            insert_batch: 1000,
            background_seal: false,
            master_key: None,
            key_file: None,
            role: Role::Data,
            replication_sync: true,
            steward: None,
            stewards: None,
            auto_failover: false,
            lease_secs: 60,
            node: None,
            tls: None,
        }
    }
}

/// A collection pinned at one instant by [`Db::export_collection`], ready to
/// be written as a database directory of its own by [`write_to`](Self::write_to).
/// Holding it holds the source's segment files: drop it when done.
pub struct CollectionExport {
    /// `CATALOG` as the export directory holds it, and `KEY` when the
    /// database is encrypted: the export is then encrypted under the same
    /// data key, and opens or imports wherever that key's master is.
    catalog_bytes: Vec<u8>,
    key: Option<Vec<u8>>,
    name: String,
    ts: Timestamp,
    shards: Vec<ExportShard>,
}

pub(crate) struct ExportShard {
    /// `RANGE` as the file holds it -- see [`range_file`] -- and every
    /// other byte string here likewise: framed under the cipher when the
    /// database has one, so a copy of an export moves bytes and never
    /// reads them.
    pub(crate) range: Vec<u8>,
    pub(crate) sealed: Vec<Arc<SegmentHandle>>,
    pub(crate) deletes: Vec<(u64, Option<Vec<u8>>)>,
    pub(crate) fresh: Option<(u64, Vec<u8>)>,
    pub(crate) manifest: Vec<u8>,
}

/// The bytes of a sealed segment wherever they are: a file, a byte buffer,
/// or an object in the store.
pub(crate) fn handle_bytes(h: &SegmentHandle) -> Result<Vec<u8>> {
    // The bytes as they lie -- encrypted frames stay frames: a copy moves
    // them, it does not read them.
    Ok(match h.segment.source().unwrapped().clone() {
        SegmentSource::File(p) | SegmentSource::Archive(p) => fs::read(&p)?,
        SegmentSource::Bytes(b) => b.as_ref().clone(),
        SegmentSource::Remote { store, key, .. } => store.get(&key)?,
        SegmentSource::Encrypted { .. } => unreachable!("unwrapped above"),
    })
}

/// `RANGE` as it is written: the low bound, a newline, the high bound, an
/// absent bound empty.
pub(crate) fn range_file(range: &(Option<String>, Option<String>)) -> Vec<u8> {
    let (lo, hi) = range;
    format!("{}\n{}", lo.clone().unwrap_or_default(), hi.clone().unwrap_or_default()).into_bytes()
}

impl CollectionExport {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The instant the copy is pinned at.
    pub fn timestamp(&self) -> Timestamp {
        self.ts
    }

    /// Write the copy as a complete database directory at `dir`, which must
    /// not exist. Everything is written under a sibling temporary directory
    /// and renamed into place at the end, so `dir` is either absent or
    /// complete: a copy that fails partway, or a crash during one, leaves
    /// no half-populated destination to open by mistake.
    pub fn write_to(&self, dir: &Path) -> Result<()> {
        if dir.exists() {
            return Err(Error::Storage(format!("{} already exists", dir.display())));
        }
        let tmp = dir.with_extension("tmp");
        let _ = fs::remove_dir_all(&tmp);
        match self.write_tree(&tmp) {
            Ok(()) => {}
            Err(e) => {
                let _ = fs::remove_dir_all(&tmp);
                return Err(e);
            }
        }
        fs::rename(&tmp, dir)?;
        #[cfg(test)]
        crate::shard::durability_probe::note_rename(dir);
        let parent = dir.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        crate::shard::sync_dir(parent)?;
        Ok(())
    }

    fn write_tree(&self, root: &Path) -> Result<()> {
        fs::create_dir_all(root)?;
        crate::shard::atomic_write(&root.join("CATALOG"), &self.catalog_bytes)?;
        if let Some(k) = &self.key {
            crate::shard::atomic_write(&root.join("KEY"), k)?;
        }
        let cdir = root.join("collections").join(&self.name);
        for (i, sh) in self.shards.iter().enumerate() {
            let sdir = cdir.join(format!("shard-{i:04}"));
            fs::create_dir_all(sdir.join("segments"))?;
            fs::create_dir_all(sdir.join("deletes"))?;
            fs::create_dir_all(sdir.join("archive"))?;
            crate::shard::atomic_write(&sdir.join("RANGE"), &sh.range)?;
            for h in &sh.sealed {
                let bytes = handle_bytes(h)?;
                let name = format!("{:016x}.seg", h.id());
                crate::shard::atomic_write(&sdir.join("segments").join(name), &bytes)?;
            }
            for (id, log) in &sh.deletes {
                if let Some(bytes) = log {
                    let name = format!("{id:016x}.dlog");
                    crate::shard::atomic_write(&sdir.join("deletes").join(name), bytes)?;
                }
            }
            if let Some((id, bytes)) = &sh.fresh {
                let name = format!("{id:016x}.seg");
                crate::shard::atomic_write(&sdir.join("segments").join(name), bytes)?;
            }
            crate::shard::atomic_write(&sdir.join("MANIFEST"), &sh.manifest)?;
            crate::shard::sync_dir(&sdir)?;
        }
        crate::shard::sync_dir(&cdir)?;
        crate::shard::sync_dir(&root.join("collections"))?;
        crate::shard::sync_dir(root)?;
        Ok(())
    }
}

/// One shard of an export, pinned at `ts`: its sealed segments by handle,
/// its delete logs as they stand, the rows still in memory sealed into one
/// fresh segment, and a manifest naming all of it. What a collection export
/// writes to disk and what a move sends across the wire.
pub(crate) fn export_shard(
    coll: &Collection,
    s: &Shard,
    ts: Timestamp,
    build: BuildOpts,
) -> Result<ExportShard> {
    let snap = s.snapshot_at(ts);
    let sealed = snap.segments.clone();
    // Captured now, at the pin: a delete landed after this point
    // goes into the source's log and not into these bytes. The test
    // that pins this deletes sealed rows between the pin and the
    // write and finds them in the copy.
    let deletes: Vec<(u64, Option<Vec<u8>>)> =
        sealed.iter().map(|h| (h.id(), h.encode_deletes())).collect();
    // The memtable and any frozen memtable, visible at `ts`, in key
    // order: one version per key is visible, so one segment holds
    // them all.
    let mut pending: Vec<PendingDoc> = Vec::new();
    let mut units: Vec<Searchable<'_>> = vec![Searchable::Mem(snap.memtable)];
    units.extend(snap.frozen.iter().map(|f| Searchable::Mem(f)));
    for unit in &units {
        let Searchable::Mem(m) = unit else { continue };
        let vis = unit.visibility(ts);
        for ord in vis.iter() {
            let d = &m.docs[ord as usize];
            pending.push(PendingDoc {
                sort_key: d.sort_key.clone(),
                commit_ts: d.commit_ts,
                doc: d.doc.clone(),
            });
        }
    }
    pending.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    let next_id = sealed.iter().map(|h| h.id()).max().unwrap_or(0) + 1;
    let mut handles: Vec<Arc<SegmentHandle>> = sealed.clone();
    let mut fresh = None;
    if !pending.is_empty() {
        let mut b = SegmentBuilder::new(build);
        for d in pending {
            b.add(d);
        }
        let seg = b.build(next_id, 0, coll)?;
        let bytes = seg.encode()?;
        let handle = SegmentHandle::new(seg, DeleteLog::new(), None);
        handles.push(handle);
        fresh = Some((next_id, bytes));
    }
    let mut manifest = Shard::manifest_of(&handles, 1, next_id + 1, 0).encode();
    let crc = crc32(&manifest);
    put_u32(&mut manifest, crc);
    // Into the file regime: what a shard directory would hold, so the
    // export, the backup and the move copy these bytes as they are.
    let deletes = deletes
        .into_iter()
        .map(|(id, log)| {
            let sealed = match log {
                Some(b) => Some(s.seal_content(&format!("{id:016x}.dlog"), &b)?),
                None => None,
            };
            Ok((id, sealed))
        })
        .collect::<Result<Vec<_>>>()?;
    let fresh = match fresh {
        Some((id, b)) => Some((id, s.seal_content(&format!("{id:016x}.seg"), &b)?)),
        None => None,
    };
    let manifest = s.seal_content("MANIFEST", &manifest)?;
    let range =
        s.seal_content("RANGE", &range_file(&s.key_range.clone().unwrap_or((None, None))))?;
    Ok(ExportShard { range, sealed, deletes, fresh, manifest })
}

/// The shards pinned for moves, by `(collection, index)`, shared between
/// the engine and the wire server.
pub type Moves = Arc<std::sync::Mutex<BTreeMap<(String, usize), Arc<MoveOut>>>>;

/// A shard pinned for a move: every file the target has to pull, as it was
/// at the pin. Segment files are named by path -- the handles in `sealed`
/// keep a retired file from being unlinked under the pull -- and everything
/// captured at the pin is bytes.
pub struct MoveOut {
    pub to: String,
    #[allow(dead_code)]
    sealed: Vec<Arc<SegmentHandle>>,
    files: Vec<(String, MoveFile)>,
    /// Set by the target once it holds every file: from then on the source
    /// answers no read of the shard either, since the next write lands on
    /// the target and a read served here would not see it.
    fenced: std::sync::atomic::AtomicBool,
}

impl MoveOut {
    pub fn fence(&self) {
        self.fenced.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn fenced(&self) -> bool {
        self.fenced.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub(crate) enum MoveFile {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

impl MoveOut {
    pub(crate) fn files(&self) -> &[(String, MoveFile)] {
        &self.files
    }
}

/// How much of a file crosses the wire per call.
pub const MOVE_CHUNK: u64 = 4 << 20;

impl MoveOut {
    fn from_export(to: &str, ex: ExportShard) -> Result<MoveOut> {
        let mut files = Vec::new();
        files.push(("RANGE".to_string(), MoveFile::Bytes(ex.range.clone())));
        for h in &ex.sealed {
            let name = format!("segments/{:016x}.seg", h.id());
            let f = match h.segment.source().unwrapped().clone() {
                SegmentSource::File(p) | SegmentSource::Archive(p) => MoveFile::Path(p),
                SegmentSource::Bytes(b) => MoveFile::Bytes(b.as_ref().clone()),
                SegmentSource::Remote { store, key, .. } => MoveFile::Bytes(store.get(&key)?),
                SegmentSource::Encrypted { .. } => unreachable!("unwrapped above"),
            };
            files.push((name, f));
        }
        for (id, log) in &ex.deletes {
            if let Some(bytes) = log {
                files.push((format!("deletes/{id:016x}.dlog"), MoveFile::Bytes(bytes.clone())));
            }
        }
        if let Some((id, bytes)) = &ex.fresh {
            files.push((format!("segments/{id:016x}.seg"), MoveFile::Bytes(bytes.clone())));
        }
        files.push(("MANIFEST".to_string(), MoveFile::Bytes(ex.manifest.clone())));
        Ok(MoveOut {
            to: to.to_string(),
            sealed: ex.sealed,
            files,
            fenced: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Every file and its length, in the order the target writes them:
    /// MANIFEST last, so a directory that stops short has no manifest and
    /// cannot be mistaken for a shard.
    pub fn list(&self) -> Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        for (name, f) in &self.files {
            let len = match f {
                MoveFile::Path(p) => fs::metadata(p)?.len(),
                MoveFile::Bytes(b) => b.len() as u64,
            };
            out.push((name.clone(), len));
        }
        Ok(out)
    }

    /// `len` bytes of a file from `offset`, or fewer at its end.
    pub fn read(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let Some((_, f)) = self.files.iter().find(|(n, _)| n == name) else {
            return Err(Error::Plan(format!("the move has no file `{name}`")));
        };
        match f {
            MoveFile::Bytes(b) => {
                let a = (offset as usize).min(b.len());
                let z = (offset.saturating_add(len) as usize).min(b.len());
                Ok(b[a..z].to_vec())
            }
            MoveFile::Path(p) => {
                use std::io::{Read, Seek, SeekFrom};
                let mut fh = fs::File::open(p)?;
                fh.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len.min(MOVE_CHUNK) as usize];
                let mut got = 0;
                while got < buf.len() {
                    let n = fh.read(&mut buf[got..])?;
                    if n == 0 {
                        break;
                    }
                    got += n;
                }
                buf.truncate(got);
                Ok(buf)
            }
        }
    }
}

/// Copy a directory tree, every file published durably.
/// Copy a collection's tree from `from` to `to`, every file opened under
/// `src` and sealed under `dst` on the way -- a plain copy when both are
/// `None`. The file identity is what [`Shard`] gives it: the shard
/// directory's name and the file's own, whichever of `segments/`,
/// `archive/` or `deletes/` holds it.
fn recode_tree(
    from: &Path,
    to: &Path,
    src: &crate::cipher::Shared,
    dst: &crate::cipher::Shared,
) -> Result<()> {
    fn walk(
        from: &Path,
        to: &Path,
        shard: Option<&str>,
        src: &crate::cipher::Shared,
        dst: &crate::cipher::Shared,
    ) -> Result<()> {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            let dest = to.join(&name);
            if entry.file_type()?.is_dir() {
                let shard = if shard.is_none() && name.starts_with("shard-") {
                    Some(name.as_str())
                } else {
                    shard
                };
                walk(&entry.path(), &dest, shard, src, dst)?;
                continue;
            }
            let mut bytes = fs::read(entry.path())?;
            if src.is_some() || dst.is_some() {
                let id = match shard {
                    Some(s) => format!("{s}/{name}"),
                    None => name.clone(),
                };
                if let Some(c) = src {
                    bytes = c.open_file(&id, &bytes)?;
                }
                if let Some(c) = dst {
                    bytes = c.seal_file(&id, &bytes)?;
                }
            }
            crate::shard::atomic_write(&dest, &bytes)?;
        }
        crate::shard::sync_dir(to)?;
        Ok(())
    }
    walk(from, to, None, src, dst)
}

/// Thirty seconds: long enough that no statement the quick start or the demo
/// runs comes near it, short enough that a hostile one is a bounded cost.
pub const DEFAULT_STATEMENT_DEADLINE_MS: u64 = 30_000;

/// The periodically refreshed global statistics of §8.2. All three numbers —
/// `num_docs`, `total_doc_len` and `doc_freq` — are masked sums at one
/// instant: exactly the triple `Shard::term_stats` answers on the exact
/// path, summed over every shard of the collection, gathered at a timestamp
/// the current query pins. What the cache buys is not a cheaper *kind* of
/// number; it is not gathering one per query.
///
/// `doc_freq` is therefore not a vocabulary. It holds only terms some query
/// has asked for, filled on demand by `fill_term_stats` and capped at
/// [`STATS_TERM_CAP`] entries evicted oldest first. The cap bounds what is
/// RETAINED and never what is answered: the fill returns the triple it
/// gathered, because a single query may ask for more terms than the cap holds
/// and would otherwise evict its own terms before they were read back.
///
/// Every fill rewrites the two globals from the same `Shard::term_stats`
/// call that produced its frequencies, which is what keeps a freshly measured
/// `df` coherent with the `num_docs` it is about to be divided by — and it is
/// free, because that call computed them anyway. Gathering the two halves
/// separately is what sends IDF negative: a `df` counted over one corpus and
/// an `n` counted over another makes `df > n` reachable, and that is a sign
/// flip in the term weight, not a rescaling. The same reasoning is why a fill
/// that re-anchors the globals drops every frequency measured before them:
/// they were measured over a different corpus, so keeping them would put the
/// two halves back on different instants by another route.
///
/// So what this path answers is a set of live sums at ONE instant, and the one
/// error that remains is the one §8.2 licenses: staleness, up to
/// [`STATS_REFRESH_WRITES`] writes. Stale, never mixed. It is worth being
/// exact about why that is a different kind of error from the physical
/// counting it replaced. Refresh points are chosen by a write counter, not by
/// any shard's seal or compaction threshold, so one shard and six reach them
/// after the same writes and measure the same live corpus there. A stale
/// answer is a live quantity evaluated at an earlier instant — the *same*
/// earlier instant however many shards there are. A count over physical rows
/// is not: how many dead versions survive is each shard's own decision, so it
/// moved with the shard count and no refresh interval, however short,
/// converged it.
///
/// Mixing the two would not have been a second-order residual, which is why it
/// is not accepted as one. Staleness is bounded in the quantity it is measured
/// in: at most [`STATS_REFRESH_WRITES`] documents of drift. The error in IDF
/// is a function of `df / num_docs`, so a drift of 511 documents is nothing
/// against a million and everything against five hundred — measured, a term in
/// 60% of a 500-document corpus came back weighted 804x too light, where
/// leaving the globals alone would have been 3.1x. A bound that improves with
/// corpus size is not a bound for a database with no minimum corpus size.
///
/// Prefix expansion is covered, and used not to be: `run_select` resolves each
/// prefix against every unit of every shard before the gather and hands the
/// resolved terms to it like any other, so an expanded term is weighted by the
/// collection on both paths. What that leaves is the cap — a wide prefix names
/// the lexicographically first [`PREFIX_EXPANSION_LIMIT`] LIVE terms of the
/// union and no more, which is a partial answer by construction, though the
/// same partial answer at every shard count and on either side of a compaction.
///
/// One approximation is *not* addressed here, and it is shared with
/// `WITH (exact_scoring)` rather than particular to this cache: `avgdl` is
/// diluted by documents that carry no text on the path, which
/// `Shard::term_stats` documents where the dilution happens. So what this
/// buys is parity with `WITH (exact_scoring)` for Term, Phrase and Prefix
/// queries, not blanket invariance.
///
/// A note on the cadence, now that staleness is the only residual and the
/// cadence is the only knob left: `refreshed_at_writes` is compared against
/// the write count of THIS collection, so the interval describes the
/// staleness rather than bounding it -- traffic on an unrelated collection
/// neither ages an entry nor unanchors it. It used to compare against the
/// engine-wide counter, which was shard-count independent too (the
/// load-bearing property) but made the interval an upper bound on how fresh
/// these numbers could be,
/// not a description of how fresh they are.
///
/// And a note on what the cache costs, because "not gathering one per query"
/// is the hit price only. A miss gathers, and a gather is a corpus-linear pass
/// — one visibility bitmap and one masked length sum per unit — plus the
/// posting walk for the missing terms, which is very nearly what the exact arm
/// costs. Measured at 50k documents over two units, release, one term: a hit
/// 1 us, a miss 106-220 us, `WITH (exact_scoring)` 104-171 us. At an epoch
/// boundary, two terms, the two arms alternated in one process so that neither
/// always pays to warm the other: 870-1215 us cached against 786-1266 us
/// exact, which is the same number. So the cache pays for itself in proportion
/// to term repetition in the query mix; a stream of entirely distinct terms
/// gets the exact path's cost with the default path's staleness; and the
/// default path never costs MORE than the exact path for the same query, which
/// is the sentence that makes the staleness a straight win rather than a
/// trade. Against the walk of every term in every dictionary this replaced,
/// the shape that holds at every fixture is that a gather is linear in
/// (units x live documents) rather than in (units x vocabulary) — the RATIO
/// between the two is a property of the fixture's vocabulary, not of the
/// design, which is why no ratio is quoted here.
/// What the statistics path answers with, before it is dressed as a
/// [`GlobalStats`]: `num_docs`, the length sum, and one document frequency per
/// term the query asked about. The three travel together everywhere because
/// they are only meaningful together — see [`CachedStats`] for why a `df` and
/// the `num_docs` it is divided by must have been measured at one instant.
type StatsTriple = (u64, u64, BTreeMap<String, u64>);

/// What one read used of one collection: the paths and how.
type Touch = (String, Vec<(String, IndexUse)>);

/// The recall harness's sample of real vector queries.
#[derive(Default)]
struct RecallLog {
    queries_seen: u64,
    query_log: Vec<LoggedVectorQuery>,
}

/// A lock whose poisoning is not a reason to stop: what it guards is a
/// cache or a log, and the worst a panic left is a stale entry.
fn guard<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, Clone)]
struct CachedStats {
    num_docs: u64,
    total_doc_len: u64,
    doc_freq: BTreeMap<String, u64>,
    /// The terms of `doc_freq` in the order they were filled, oldest first.
    /// A `BTreeMap` orders by term, which is the wrong order to evict in; this
    /// is the right one, and it is the whole of what the cap needs.
    ///
    /// It is dropped wherever `doc_freq` is dropped, and the two places that
    /// do so both drop both. Keep this one and the map stops being bounded at
    /// all: the queue is only ever drained by over-cap eviction, so it grows
    /// without limit, and once it is longer than the map the eviction loop
    /// pops names that are no longer present — removing nothing while draining
    /// the queue.
    fill_order: VecDeque<String>,
    /// The collection's write count at the last epoch reset. The refresh gate.
    refreshed_at_writes: u64,
    /// The collection's write count when the numbers below were measured,
    /// meaningful only when `anchored`. Equal to the current count means no
    /// insert and no delete has landed in THIS collection since, so the live
    /// corpus has not moved and a further gather measures the same one.
    measured_at_writes: u64,
    /// Whether the globals have been measured in this epoch. An epoch starts
    /// unanchored, and the first fill of the epoch measures them. This cannot
    /// be inferred from `num_docs == 0`, which is the honest answer for an
    /// empty collection.
    anchored: bool,
}

/// A logged vector query, for the continuous recall measurement of §12.1.
#[derive(Debug, Clone)]
pub struct LoggedVectorQuery {
    pub collection: String,
    pub path: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub filter_sql: Option<String>,
}

/// What one lifecycle run did.
///
/// Failures are per collection rather than fatal: each collection commits
/// independently and durably, so a collection whose files will not move must
/// not hide the ones that already did.
#[derive(Debug, Default)]
pub struct LifecycleRun {
    pub moves: Vec<lifecycle::Transition>,
    pub failures: Vec<(String, String)>,
}

/// The result of one statement.
///
/// `Rows` is much larger than the other variants; that is deliberate rather
/// than boxed. Exactly one `Outcome` exists per statement, so the size costs a
/// single stack move, and boxing would put an allocation on the path every
/// query takes to save nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Outcome {
    Ack(String),
    Rows(QueryResult),
    Explain(String),
    Recall(crate::harness::RecallReport),
    /// Work the statement left for after the caller's lock is released:
    /// [`Deferred::finish`] runs it and gives the real outcome. `BACKUP`
    /// answers with one, so that a node copying gigabytes to a store is not
    /// a node that refuses every other statement meanwhile.
    Deferred(Deferred),
}

/// What a statement pinned under the lock and copies without it. Holds the
/// segment handles it reads, so a compaction that retires one cannot unlink
/// the file before the copy has it.
pub struct Deferred(Box<dyn FnOnce() -> Result<Step> + Send>);

/// What deferred work leaves behind: the outcome, or work that needs the
/// database again.
enum Step {
    Done(Box<Outcome>),
    Resume(Resume),
}

type ResumeFn = Box<dyn FnOnce(&mut Db) -> Result<Outcome> + Send>;

/// Work that goes back under the database lock after work that ran without
/// it: a `DELETE ... WHERE` asks its holders whether they answer with no lock
/// held, and selects its keys under the lock only once they have.
pub struct Resume(ResumeFn);

impl Resume {
    pub(crate) fn new(f: impl FnOnce(&mut Db) -> Result<Outcome> + Send + 'static) -> Resume {
        Resume(Box::new(f))
    }
}

impl Deferred {
    pub(crate) fn new(f: impl FnOnce() -> Result<Outcome> + Send + 'static) -> Deferred {
        Deferred(Box::new(move || f().map(|out| Step::Done(Box::new(out)))))
    }

    /// Work without the lock that then wants it back.
    pub(crate) fn then_under_lock(f: impl FnOnce() -> Result<Resume> + Send + 'static) -> Deferred {
        Deferred(Box::new(move || f().map(Step::Resume)))
    }

    /// Run the work. The caller holds no database lock here, and nothing
    /// the work does needs one; work that does (a `DELETE ... WHERE` across
    /// nodes) is refused here and finished by
    /// [`finish_with`](Self::finish_with).
    pub fn finish(self) -> Result<Outcome> {
        match (self.0)()? {
            Step::Done(out) => Ok(*out),
            Step::Resume(_) => Err(Error::Plan(
                "this statement's deferred work needs the database again; finish it with                  the lock at hand (`Outcome::finished_with`)"
                    .into(),
            )),
        }
    }

    /// Run the work, taking the database lock again for the steps that need
    /// it and holding it for those alone.
    pub fn finish_with(self, db: &crate::lock::RwLock<Db>) -> Result<Outcome> {
        let mut step = (self.0)()?;
        loop {
            step = match step {
                Step::Done(out) => match *out {
                    Outcome::Deferred(d) => (d.0)()?,
                    out => return Ok(out),
                },
                Step::Resume(r) => {
                    let mut guard = db.write().unwrap_or_else(|p| p.into_inner());
                    let out = (r.0)(&mut guard)?;
                    drop(guard);
                    Step::Done(Box::new(out))
                }
            };
        }
    }
}

impl std::fmt::Debug for Deferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Deferred(..)")
    }
}

impl Outcome {
    pub fn rows(self) -> Result<QueryResult> {
        match self {
            Outcome::Rows(r) => Ok(r),
            Outcome::Ack(m) => Err(Error::Plan(format!("statement returned no rows: {m}"))),
            Outcome::Explain(_) => Err(Error::Plan("statement returned a plan, not rows".into())),
            Outcome::Recall(_) => Err(Error::Plan("statement returned a recall report".into())),
            Outcome::Deferred(_) => {
                Err(Error::Plan("statement returned deferred work, not rows".into()))
            }
        }
    }

    /// The outcome with any deferred work done: what a caller that does not
    /// hold the database lock across statements calls before it looks.
    pub fn finished(self) -> Result<Outcome> {
        match self {
            Outcome::Deferred(d) => d.finish(),
            other => Ok(other),
        }
    }

    /// The same, with the database at hand for deferred work that goes
    /// back under its lock: what the console and the wire call, with the
    /// lock they took for the statement let go.
    pub fn finished_with(self, db: &crate::lock::RwLock<Db>) -> Result<Outcome> {
        match self {
            Outcome::Deferred(d) => d.finish_with(db),
            other => Ok(other),
        }
    }
}

pub struct Db {
    pub catalog: Catalog,
    shards: BTreeMap<String, Vec<Shard>>,
    /// The shards this node follows -- a copy of each, fed by the holder's
    /// log, under `follow-NNNN` beside the held ones -- with the term each
    /// follows at. Behind a lock of their own, shared with the wire, so a
    /// holder's batch is applied without this node's lock: a write
    /// forwarded from here under this lock waits for that holder, which
    /// waits for this node to confirm its own log, and this lock would
    /// close the cycle. Not read by any statement; a promotion makes one
    /// a held shard.
    followed: Followed,
    /// What the last write statement wrote here, shard by shard: what its
    /// acknowledgement waits on the followers for.
    recent_writes: Vec<(String, usize, Timestamp)>,
    /// The lease the steward renews, shared with the wire so a renewal
    /// takes no lock: when it was last renewed, and by which node it may
    /// be.
    lease: Lease,
    pub clock: Arc<Hlc>,
    pub opts: DbOpts,
    dir: Option<PathBuf>,
    /// The directory's lock, for as long as this `Db` is open; `None` in
    /// memory.
    lock: Option<crate::dirlock::DirLock>,
    /// Encryption at rest, when the database has a data key.
    cipher: crate::cipher::Shared,
    /// Writes that waited for compaction, and the time they waited.
    backpressure_waits: u64,
    backpressure_micros: u64,
    budget: Arc<MemtableBudget>,
    residency: Arc<ResidencyManager>,
    /// The statistics cache. Behind a lock, not `&mut self`: a read fills
    /// it, and reads run under a shared lock since 0.31.0. Held only to
    /// look and to apply; never across a shard call.
    stats: Mutex<BTreeMap<String, CachedStats>>,
    /// Inferred path statistics as the catalog on disk held them at the last
    /// reopen, per collection: the documents sealed into segments by then.
    ///
    /// A shard accumulates statistics for the documents *it* observes since it
    /// was opened, and `absorb_shard_catalogs` sums the baseline and the
    /// shards into the live view planning reads. On reopen each shard is handed
    /// a copy of the catalog's collection with its statistics cleared, and
    /// this holds what they no longer carry — summing three shards that each
    /// held the aggregate would treble it.
    ///
    /// The persisted catalog counts sealed documents only, never the
    /// memtable's: those are in the WAL, and a reopen observes them again as it
    /// replays. See `Shard::sealed`, and `persisted_catalog` below.
    stats_baseline: BTreeMap<String, PathTally>,
    /// The catalog exactly as this process last published it, so that a persist
    /// which would rewrite CATALOG byte for byte can decline to. See
    /// `Shard::persist_manifest`, which does the same for the manifests, and
    /// for the same reason: the shells persist after every acknowledged
    /// statement, and almost no statement changes the catalog.
    published_catalog: Option<Vec<u8>>,
    writes: u64,
    /// The object store behind the `archived` tier, built at open from
    /// `DbOpts::archive` and the environment; `None` for an in-memory
    /// database and for one whose archive is the local directory.
    archive: Option<crate::objstore::ArchiveHandle>,
    /// Writes per collection, for the statistics cache: its refresh gate and
    /// its anchor compare against the collection whose statistics they guard,
    /// so traffic on an unrelated collection neither ages an entry nor
    /// unanchors it. `writes` stays engine-wide for the lifecycle interval,
    /// which is about the node's activity rather than one collection's.
    collection_writes: BTreeMap<String, u64>,
    lifecycle_checked_at_writes: u64,
    activity_persisted_micros: u64,
    /// Recall sampling (§12.1), written by reads: behind a lock for the
    /// same reason as `stats`.
    recall: Mutex<RecallLog>,
    /// What reads touched -- (collection, the indexes and how) -- waiting
    /// for `apply_touches`, which needs `&mut self` (a touch can promote a
    /// demoted index, which renames files and persists the catalog). A read
    /// only notes; the next write, or the console right after the read,
    /// applies.
    touches: Mutex<Vec<Touch>>,
    /// The fault schedule every query's shard calls go through, when one is
    /// installed. See `crate::sim`.
    sim: Option<Arc<crate::sim::Sim>>,
    /// The token every request on the wire carries, from the environment at
    /// construction; `None` means this node can reach no other.
    wire_token: Option<String>,
    /// One connection per other node, opened on demand. See `crate::wire`.
    nodes: Mutex<BTreeMap<String, Arc<crate::wire::Node>>>,
    /// The nodes this process has verified since it started -- an `ATTACH
    /// NODE` that reached them and agreed on address and version. Not the
    /// catalog's list, which persists across a restart and so says nothing
    /// about whether a peer answers *now*; this is what readiness asks.
    attached: BTreeSet<String>,
    /// Collections a peer's catalog names this node as a holder of and
    /// `reconcile` refused to adopt, because they are older than this data
    /// directory: the data is not here. Said once in the notes and on
    /// every `SHOW HEALTH` until a restore or a drop settles it.
    not_adopted: BTreeSet<String>,
    /// When this process opened the database, microseconds: what `hello`
    /// carries as the epoch, so a peer can tell a restart from the same
    /// process, and an older process answering at the address from
    /// either.
    epoch: u64,
    /// What every hello from every peer has shown, by address. A mutex
    /// because `SHOW HEALTH` observes under the read lock.
    peers_seen: Mutex<BTreeMap<String, PeerSeen>>,
    /// The epoch as claimed, in a cell the wire's frames read: the real
    /// one, or what [`Db::pretend`] said.
    epoch_cell: Arc<std::sync::atomic::AtomicU64>,
    /// Test and drill hooks: an epoch to claim instead of the real one,
    /// and an offset on the clock `hello` reports.
    pretend_epoch: Option<u64>,
    pretend_clock_micros: i64,
    /// Shards of this node pinned for a move, by `(collection, index)`:
    /// the files the target pulls, as they were at the pin. Shared with the
    /// wire server, which answers a target's reads from it without this
    /// lock, so that a coordinator that is also the source can hold the
    /// lock for the whole statement while the target pulls.
    moves: Moves,
    /// The client's read-your-writes token: the last commit timestamp it
    /// observed (§6). Subsequent reads pin at least this.
    pub last_commit: Timestamp,
}

impl Default for Db {
    fn default() -> Self {
        Db::in_memory()
    }
}

impl Db {
    pub fn in_memory() -> Db {
        Db::with_opts(DbOpts::default())
    }

    /// Build a database, refusing a placement whose guarantee cannot hold.
    pub fn try_with_opts(opts: DbOpts) -> Result<Db> {
        opts.placement.validate()?;
        Ok(Db::with_opts(opts))
    }

    pub fn with_opts(opts: DbOpts) -> Db {
        let budget = MemtableBudget::new(opts.memtable_budget_bytes);
        let residency = Arc::new(ResidencyManager::new(opts.residency));
        Db {
            catalog: Catalog::default(),
            shards: BTreeMap::new(),
            followed: Arc::new(Mutex::new(BTreeMap::new())),
            recent_writes: Vec::new(),
            lease: Arc::new(Mutex::new(LeaseState {
                at: None,
                steward: None,
                term: 0,
                election: None,
                dir: None,
            })),
            clock: Arc::new(Hlc::new()),
            opts,
            dir: None,
            lock: None,
            cipher: None,
            backpressure_waits: 0,
            backpressure_micros: 0,
            budget,
            residency,
            stats: Mutex::new(BTreeMap::new()),
            stats_baseline: BTreeMap::new(),
            published_catalog: None,
            writes: 0,
            archive: None,
            collection_writes: BTreeMap::new(),
            lifecycle_checked_at_writes: 0,
            activity_persisted_micros: 0,
            recall: Mutex::new(RecallLog::default()),
            touches: Mutex::new(Vec::new()),
            sim: None,
            wire_token: crate::wire::token_from_env(),
            nodes: Mutex::new(BTreeMap::new()),
            attached: BTreeSet::new(),
            not_adopted: BTreeSet::new(),
            epoch: crate::time::now_micros().max(0) as u64,
            epoch_cell: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peers_seen: Mutex::new(BTreeMap::new()),
            pretend_epoch: None,
            pretend_clock_micros: 0,
            moves: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            last_commit: 0,
        }
    }

    /// Open a database rooted at `dir`, installing the catalog and every
    /// shard's manifest, then replaying each WAL.
    pub fn open(dir: &Path, opts: DbOpts) -> Result<Db> {
        opts.placement.validate()?;
        let archive = match (&opts.archive.endpoint, &opts.archive.dir) {
            (Some(_), Some(_)) => {
                return Err(Error::Storage(
                    "archive: an endpoint and a directory are both configured; the tier lives in one place"
                        .into(),
                ))
            }
            (Some(_), None) => Some(crate::objstore::ArchiveHandle {
                store: Arc::new(crate::objstore::S3Store::from_env(&opts.archive)?),
                prefix: opts.archive.prefix.clone(),
            }),
            (None, Some(d)) => Some(crate::objstore::ArchiveHandle {
                store: Arc::new(crate::objstore::DirStore::new(d)?),
                prefix: opts.archive.prefix.clone(),
            }),
            (None, None) => None,
        };
        let mut db = Db::with_opts(opts);
        db.archive = archive;
        fs::create_dir_all(dir)?;
        db.lock = Some(crate::dirlock::take(dir)?);
        db.dir = Some(dir.to_path_buf());
        db.cipher = open_key(dir, &db.opts)?;
        // Absent is a fresh database. Unreadable is not: read as absent it
        // opened a database with no collections, and the next DDL published
        // that catalog over the real one.
        if let Some(b) = crate::shard::read_content(&db.cipher, "CATALOG", &dir.join("CATALOG"))? {
            db.catalog = Catalog::decode(&b)?;
        } else {
            // A directory with no catalog is a new one, and when it was
            // made is what `reconcile` compares a definition's age
            // against: a collection older than this directory that names
            // this node is data this directory never had.
            db.catalog.born_micros = lifecycle::now_micros(&db.clock);
        }
        // A drop is complete once the collection's directory has been renamed
        // aside; the catalog catches up here if the process ended between
        // that rename and the catalog's publication. See `drop_collection`.
        let interrupted: Vec<String> = db
            .catalog
            .collections
            .keys()
            .filter(|n| {
                !dir.join("collections").join(n.as_str()).exists() && dropping_dir(dir, n).exists()
            })
            .cloned()
            .collect();
        for name in &interrupted {
            db.forget_collection(name);
        }
        Db::sweep_dropping(dir)?;
        let names: Vec<String> = db.catalog.collections.keys().cloned().collect();
        let mut derived = false;
        for name in names {
            derived |= db.attach_collection(dir, &name)?;
        }
        if !interrupted.is_empty() || derived {
            db.persist_catalog()?;
        }
        // The catalog as decoded describes sealed documents. Until the
        // replayed ones are folded in, `SHOW CATALOG` and the `catalog` verb
        // would report a database with an unflushed WAL as smaller than it
        // is, and a statement is not required before the first question.
        let names: Vec<String> = db.shards.keys().cloned().collect();
        for name in names {
            db.absorb_shard_catalogs(&name)?;
        }
        Ok(db)
    }

    /// Open the shards of one collection from its directory under `dir`,
    /// replaying each WAL. The catalog entry is already in place; what this
    /// adds is the shards, with the statistics baseline they start from.
    fn attach_collection(&mut self, dir: &Path, name: &str) -> Result<bool> {
        let db = self;
        let mut coll = db.catalog.get(name)?.clone();
        // What is already counted stays in the baseline; the shards start
        // from zero and count only what they see from here -- which
        // includes the WAL they are about to replay, and which the
        // baseline therefore must not.
        db.stats_baseline.insert(
            name.to_string(),
            PathTally { docs: coll.doc_count, paths: std::mem::take(&mut coll.paths) },
        );
        coll.doc_count = 0;
        let cdir = dir.join("collections").join(name);
        // A catalog from before placement existed names no tablets: every
        // shard directory is one, on this node, its range from RANGE. That
        // is exactly what a single-node database has always been.
        let mut derived = false;
        let tablets = match db.catalog.placement.get(name) {
            Some(t) => t.clone(),
            None => {
                let mut t = Vec::new();
                let mut i = 0usize;
                while cdir.join(format!("shard-{i:04}")).exists() {
                    let (lo, hi) =
                        read_range(&db.cipher, &cdir.join(format!("shard-{i:04}")), i, name)?;
                    t.push(Tablet {
                        node: db.opts.node.clone().unwrap_or_default(),
                        lo,
                        hi,
                        ..Default::default()
                    });
                    i += 1;
                }
                derived = true;
                db.catalog.placement.insert(name.to_string(), t.clone());
                t
            }
        };
        let mut shards = Vec::new();
        let mut followed = Vec::new();
        for (i, t) in tablets.iter().enumerate() {
            if t.is_merged() {
                continue;
            }
            if t.followers.iter().any(|f| db.is_self(f)) && !db.is_self(&t.node) {
                let fdir = cdir.join("followed").join(format!("shard-{i:04}"));
                if fdir.exists() {
                    let (lo, hi) = read_range(&db.cipher, &fdir, i, name)?;
                    let mut sh =
                        Shard::open(coll.clone(), db.clock.clone(), db.shard_opts(), &fdir)?;
                    sh.set_key_range(lo, hi);
                    sh.index = i;
                    followed.push(sh);
                }
                continue;
            }
            if !db.is_self(&t.node) {
                continue;
            }
            let sdir = cdir.join(format!("shard-{i:04}"));
            if !sdir.exists() {
                return Err(Error::Storage(format!(
                    "shard-{i:04} of `{name}` is placed on this node but its directory is missing"
                )));
            }
            let (lo, hi) = read_range(&db.cipher, &sdir, i, name)?;
            let mut sh = Shard::open(coll.clone(), db.clock.clone(), db.shard_opts(), &sdir)?;
            sh.set_key_range(lo, hi);
            sh.index = i;
            shards.push(sh);
        }
        if !shards.is_empty() {
            db.shards.insert(name.to_string(), shards);
        }
        if !followed.is_empty() {
            let mut g = db.followed.lock().unwrap_or_else(|p| p.into_inner());
            for sh in followed {
                let term = tablets.get(sh.index).map(|t| t.term).unwrap_or(0);
                g.insert((name.to_string(), sh.index), FollowedShard { shard: sh, term });
            }
        }
        Ok(derived)
    }

    fn shard_opts(&self) -> ShardOpts {
        ShardOpts {
            background_seal: self.opts.background_seal,
            thresholds: self.opts.thresholds,
            build: self.opts.build,
            budget: Some(self.budget.clone()),
            gc_horizon: 0,
            residency: Some(self.residency.clone()),
            placement: self.opts.placement.clone(),
            archive: self.archive.clone(),
            cipher: self.cipher.clone(),
        }
    }

    /// Node-level residency accounting: what is decoded, what it cost, and how
    /// often a read had to fault a component back in.
    pub fn residency(&self) -> &Arc<ResidencyManager> {
        &self.residency
    }

    /// Pin a collection at this instant and hand back everything a copy of
    /// it needs, without stopping writes.
    ///
    /// The copy is the collection as a reader at this instant sees it: the
    /// sealed segments the manifest names now, held by their `Arc`s so that
    /// no compaction can unlink a file before it is copied; each segment's
    /// delete log as it stands now, so a later delete does not reach the
    /// copy; and the memtable's visible rows sealed into one
    /// fresh segment of the copy's own, built here from the snapshot and
    /// touching nothing in the source. The source keeps taking writes
    /// between this call and `CollectionExport::write_to`, and none of them
    /// reach the copy -- the test that pins this interleaves inserts,
    /// deletes, a flush and a compaction between the two.
    ///
    /// No `gc_horizon` is pinned. The entry that planned this expected to
    /// need one, but the files are what the copy reads and an `Arc` on the
    /// handle is what keeps a file; version retention inside compaction
    /// outputs is beside the point when the outputs are not what is copied.
    pub fn export_collection(&mut self, name: &str) -> Result<CollectionExport> {
        let elsewhere = self.holders(name);
        if !elsewhere.is_empty() {
            return Err(Error::Plan(format!(
                "collection `{name}` has shards on {}; an export needs every shard on this node",
                elsewhere.join(", ")
            )));
        }
        self.absorb_shard_catalogs(name)?;
        let ts = self.clock.peek().max(self.last_commit);
        let coll = self.catalog.get(name)?.clone();
        let mut catalog = Catalog::default();
        catalog.collections.insert(name.to_string(), coll.clone());
        let mut shards = Vec::new();
        for s in self.shards(name)? {
            shards.push(export_shard(&coll, s, ts, self.opts.build)?);
        }
        let catalog_bytes = self.seal_root("CATALOG", &catalog.encode())?;
        let key = self.key_bytes()?;
        Ok(CollectionExport { catalog_bytes, key, name: name.to_string(), ts, shards })
    }

    /// `BACKUP TO '<dest>'`: every shard this node holds, pinned at one
    /// instant under the lock, then copied to the destination without it
    /// (the answer is [`Outcome::Deferred`]). A directory on any mount, or
    /// `s3://bucket/prefix` reached through the archive's endpoint and
    /// credentials. Only the segment files the destination lacks are
    /// written, so a second backup of a database that did not change copies
    /// nothing but its record. See `crate::backup` for the layout.
    pub fn backup(
        &mut self,
        dest: &str,
        keep: Option<usize>,
        as_of: Option<Timestamp>,
    ) -> Result<Outcome> {
        if self.dir.is_none() {
            return Err(Error::Plan("BACKUP needs a persistent database (--dir)".into()));
        }
        let target =
            crate::backup::target(&self.opts.archive, self.opts.backup_dir.as_deref(), dest)?;
        let ts = match as_of {
            None => self.clock.peek().max(self.last_commit),
            Some(t) => {
                // An instant another node chose: within this node's clock
                // and the skew ATTACH allows, or it is not an instant of
                // this cluster. Observed, so every commit from here on is
                // after it and the cut is exact.
                let now = crate::time::physical_micros(self.clock.peek());
                if crate::time::physical_micros(t) > now + CLOCK_REFUSE_MICROS {
                    return Err(Error::Plan(format!(
                        "AS OF {t} is more than {} s ahead of this node's clock",
                        CLOCK_REFUSE_MICROS / 1_000_000
                    )));
                }
                self.clock.observe(t);
                t
            }
        };
        let names: Vec<String> = self.catalog.collections.keys().cloned().collect();
        for name in &names {
            self.absorb_shard_catalogs(name)?;
        }
        let catalog = self.seal_root("CATALOG", &self.persisted_catalog().encode())?;
        let key = self.key_bytes()?;
        let mut colls: crate::backup::Exported = Vec::new();
        for name in names {
            let coll = self.catalog.get(&name)?.clone();
            let mut shards = Vec::new();
            for s in self.shards(&name)? {
                let index = shard_index(s.dir(), &name)?;
                shards.push((index, export_shard(&coll, s, ts, self.opts.build)?));
            }
            colls.push((name, shards));
        }
        let node = self.opts.node.clone().unwrap_or_default();
        Ok(Outcome::Deferred(crate::backup::job(target, ts, node, catalog, key, colls, keep)))
    }

    /// `BACKUP CLUSTER TO '<dest>' [KEEP n]`: this node's backup at an
    /// instant it chooses, then `LOCAL BACKUP ... AS OF` that instant on
    /// every other data node, one after another, after this node's copy
    /// and outside its lock. The set restores to one consistent cut with
    /// `RESTORE FROM '<dest>' AS OF <instant>` on each node. A node that
    /// did not take it is named; the others' backups stand.
    pub fn backup_cluster(&mut self, dest: &str, keep: Option<usize>) -> Result<Outcome> {
        let ts = self.clock.now();
        let Outcome::Deferred(local) = self.backup(dest, keep, Some(ts))? else {
            unreachable!("a backup is deferred work")
        };
        let mut peers = Vec::new();
        for url in self.data_nodes() {
            if !self.is_self(&url) {
                peers.push((url.clone(), self.node_conn(&url)?));
            }
        }
        let sql = format!(
            "LOCAL BACKUP TO '{}'{} AS OF {ts}",
            dest.replace('\'', "''"),
            keep.map(|k| format!(" KEEP {k}")).unwrap_or_default()
        );
        Ok(Outcome::Deferred(Deferred::new(move || {
            let mine = match local.finish()? {
                Outcome::Ack(m) => m,
                other => return Ok(other),
            };
            let mut done = Vec::new();
            let mut failed = Vec::new();
            for (url, node) in peers {
                // A copy takes what it takes; the statement deadline is
                // not the measure of it.
                let _no_deadline = crate::deadline::arm(None);
                match node.statement(&sql, &[]) {
                    Ok(m) => done.push(format!("{url}: {m}")),
                    Err(e) => failed.push(format!("{url}: {e}")),
                }
            }
            Ok(Outcome::Ack(format!(
                "{mine}; at the same instant {ts}{}{}",
                if done.is_empty() { String::new() } else { format!(" on {}", done.join("; ")) },
                if failed.is_empty() {
                    String::new()
                } else {
                    format!("; NOT on {}", failed.join("; "))
                }
            )))
        })))
    }

    /// `VERIFY BACKUP '<src>' [NODE '<address>'] [AS OF <ts>]`: read every
    /// object the backup's record names back from the store and check its
    /// size and, for a backup written since checksums were recorded, its
    /// SHA-256 against the record. Nothing is written; the reading runs
    /// after the statement let go of the lock, as a backup's copy does.
    /// The answer names the backup, counts the objects and bytes checked,
    /// and refuses with the first mismatches by name.
    pub fn verify_backup(
        &mut self,
        src: &str,
        node: Option<&str>,
        as_of: Option<u64>,
    ) -> Result<Outcome> {
        let target =
            crate::backup::target(&self.opts.archive, self.opts.backup_dir.as_deref(), src)?;
        let here = self.opts.node.clone().unwrap_or_default();
        let fetched = crate::backup::fetch(&target, node.unwrap_or(&here), as_of)?;
        let slug = crate::backup::node_slug(node.unwrap_or(&here));
        Ok(Outcome::Deferred(crate::backup::verify_job(target, slug, fetched)))
    }

    /// `RESTORE FROM '<src>' [AS OF <ts>]`: the newest complete backup at
    /// the source, or the one pinned at `ts`, into this database, which has
    /// to be empty. Every object the backup's record names is verified to
    /// be there at its recorded size before a byte is written; the shards
    /// come back placed on this node. The backup is this node's own --
    /// `CELASTRO_NODE`, or `local` -- unless `NODE '<address>'` names
    /// another's, which is how a pod restores what a pod of another name
    /// wrote.
    pub fn restore(
        &mut self,
        src: &str,
        node: Option<&str>,
        as_of: Option<u64>,
    ) -> Result<Outcome> {
        let Some(dir) = self.dir.clone() else {
            return Err(Error::Plan("RESTORE needs a persistent database (--dir)".into()));
        };
        if !self.catalog.collections.is_empty() {
            return Err(Error::Plan(format!(
                "RESTORE needs an empty database; this one has {} collection(s)",
                self.catalog.collections.len()
            )));
        }
        let target =
            crate::backup::target(&self.opts.archive, self.opts.backup_dir.as_deref(), src)?;
        let here = self.opts.node.clone().unwrap_or_default();
        let fetched = crate::backup::fetch(&target, node.unwrap_or(&here), as_of)?;
        // The backup's key regime has to be this database's: its files are
        // copied as they are, so an encrypted backup needs the master that
        // wraps its data key, and a plain one cannot land in an encrypted
        // directory. The backup's KEY replaces the one made at open -- the
        // database is empty, nothing was written under that one -- so the
        // files that follow open under it.
        self.cipher =
            match (&fetched.key, &self.opts.master_key) {
                (Some(wrapped), Some(master)) => {
                    let cipher = crate::cipher::Cipher::unwrap(wrapped, master).map_err(|e| {
                        Error::Storage(format!(
                            "RESTORE: the backup's KEY does not open under this master key: {e}"
                        ))
                    })?;
                    crate::shard::atomic_write(&dir.join("KEY"), wrapped)?;
                    Some(Arc::new(cipher))
                }
                (Some(_), None) => {
                    return Err(Error::Storage(
                        "RESTORE: the backup is encrypted; set CELASTRO_MASTER_KEY_FILE (or \
                     CELASTRO_MASTER_KEY) to the master key that wraps its data key"
                            .into(),
                    ))
                }
                (None, Some(_)) => return Err(Error::Storage(
                    "RESTORE: the backup is not encrypted and this database is; restore it into \
                     a plain database, then export and import into this one"
                        .into(),
                )),
                (None, None) => None,
            };
        let catalog_bytes = match &self.cipher {
            Some(c) => c.open_file("CATALOG", &fetched.catalog)?,
            None => fetched.catalog.clone(),
        };
        let mut catalog = Catalog::decode(&catalog_bytes)?;
        for (name, _) in &fetched.collections {
            if !catalog.collections.contains_key(name) {
                return Err(Error::Storage(format!(
                    "backup {} carries files of `{name}`, which its catalog does not name",
                    fetched.ts
                )));
            }
        }
        let mut shards_restored = 0usize;
        let mut bytes = 0u64;
        let mut elsewhere: Vec<String> = Vec::new();
        for (name, shards) in &fetched.collections {
            let dest = dir.join("collections").join(name);
            if dest.exists() {
                return Err(Error::Storage(format!(
                    "RESTORE: {} already exists; the database is not empty",
                    dest.display()
                )));
            }
            let tmp = dir.join("collections").join(format!("{name}.restore.tmp"));
            let _ = fs::remove_dir_all(&tmp);
            for (index, files) in shards {
                let sdir = tmp.join(format!("shard-{index:04}"));
                bytes += crate::backup::write_shard(&target, &sdir, files)?;
                shards_restored += 1;
            }
            crate::shard::sync_dir(&tmp)?;
            fs::rename(&tmp, &dest)?;
            #[cfg(test)]
            crate::shard::durability_probe::note_rename(&dest);
            crate::shard::sync_dir(&dir.join("collections"))?;
            // The shards that came back are here now, whatever node held
            // them; a tablet the backup did not carry keeps its holder and
            // is reported, because this node cannot answer for it.
            if let Some(tablets) = catalog.placement.get_mut(name) {
                for (i, t) in tablets.iter_mut().enumerate() {
                    if shards.iter().any(|(index, _)| *index == i) {
                        t.node = here.clone();
                    } else {
                        elsewhere.push(format!("shard {i} of `{name}` on {}", t.node));
                    }
                }
            }
        }
        catalog.nodes = self.catalog.nodes.clone();
        self.catalog = catalog;
        let names: Vec<String> = self.catalog.collections.keys().cloned().collect();
        for name in &names {
            self.attach_collection(&dir, name)?;
            self.absorb_shard_catalogs(name)?;
        }
        self.persist_catalog()?;
        let mut msg = format!(
            "restored backup {} of node `{}` from {}: {} collection(s), {shards_restored} shard(s), {bytes} bytes",
            fetched.ts,
            crate::backup::node_slug(node.unwrap_or(&here)),
            target.display,
            names.len()
        );
        if !elsewhere.is_empty() {
            msg.push_str(&format!(
                "; not in this backup, still placed where it was: {}",
                elsewhere.join(", ")
            ));
        }
        Ok(Outcome::Ack(msg))
    }

    /// Adopt a collection written by [`CollectionExport::write_to`] into
    /// this database: its files are copied under `collections/`, its catalog
    /// entry added, its shards opened, and the catalog persisted. Refused
    /// if a collection of that name exists, and on an in-memory database.
    /// Returns the collection's name.
    pub fn import_collection(&mut self, from: &Path) -> Result<String> {
        let Some(dir) = self.dir.clone() else {
            return Err(Error::Plan("import needs a persistent database (--dir)".into()));
        };
        // The export's key regime need not be this database's: an export
        // under one data key imports into a plain database, or one under
        // another key, or a plain export into an encrypted database --
        // which is how a database takes a key. Every file is opened under
        // the export's cipher and sealed under this one as it is copied.
        let src = match crate::shard::read_optional(&from.join("KEY"))? {
            Some(wrapped) => match &self.opts.master_key {
                Some(master) => Some(Arc::new(crate::cipher::Cipher::unwrap(&wrapped, master)?)),
                None => {
                    return Err(Error::Storage(format!(
                        "{} is an encrypted export; set CELASTRO_MASTER_KEY_FILE (or \
                         CELASTRO_MASTER_KEY) to the master key that wraps its data key",
                        from.display()
                    )))
                }
            },
            None => None,
        };
        let bytes = fs::read(from.join("CATALOG"))
            .map_err(|e| Error::Storage(format!("{}: CATALOG: {e}", from.display())))?;
        let bytes = match &src {
            Some(c) => c.open_file("CATALOG", &bytes)?,
            None => bytes,
        };
        let exported = Catalog::decode(&bytes)?;
        let (name, coll) = match exported.collections.iter().next() {
            Some((n, c)) if exported.collections.len() == 1 => (n.clone(), c.clone()),
            _ => return Err(Error::Storage("an export holds exactly one collection".into())),
        };
        if self.catalog.collections.contains_key(&name) {
            return Err(Error::Plan(format!("collection `{name}` already exists here")));
        }
        if let Some(n) = coll.prefix_expansion {
            check_prefix_cap(n)?;
        }
        let src_dir = from.join("collections").join(&name);
        let dest = dir.join("collections").join(&name);
        let tmp = dir.join("collections").join(format!("{name}.import.tmp"));
        let _ = fs::remove_dir_all(&tmp);
        let dst = self.cipher.clone();
        if let Err(e) = recode_tree(&src_dir, &tmp, &src, &dst) {
            let _ = fs::remove_dir_all(&tmp);
            return Err(e);
        }
        fs::rename(&tmp, &dest)?;
        #[cfg(test)]
        crate::shard::durability_probe::note_rename(&dest);
        crate::shard::sync_dir(&dir.join("collections"))?;
        self.catalog.collections.insert(name.clone(), coll);
        self.attach_collection(&dir, &name)?;
        self.absorb_shard_catalogs(&name)?;
        self.persist_catalog()?;
        Ok(name)
    }

    /// Drop a collection: its catalog entry, every shard, every file, every
    /// object it put in the store, and every statistic and access clock
    /// recorded against it. Irreversible. Refused while a lifecycle policy
    /// names the collection, so the policy is dropped knowingly rather than
    /// left naming nothing.
    ///
    /// The order is what makes a crash anywhere in the middle safe to open
    /// after. The directory is renamed aside first -- one atomic step, the
    /// drop's point of no return -- then the catalog is published without the
    /// entry, then the renamed directory is removed. `Db::open` completes a
    /// drop that stopped after the rename (a catalog naming a collection
    /// whose directory is aside) and removes any directory left aside. The
    /// objects an archived tier put in the store are deleted before the
    /// shards are dropped, while they can still be named; a crash between the
    /// rename and that deletion leaves them in the store under the
    /// collection's prefix, which is the one thing a later open cannot find
    /// from a directory.
    ///
    /// The statistics cache and the access clocks go with the entry, so a
    /// collection recreated under the same name starts from nothing: the
    /// cache key carries no catalog identity, and without this a recreated
    /// collection would be answered from its predecessor's frequencies.
    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.catalog.get(name)?;
        if let Some(p) = self.catalog.policies.values().find(|p| p.collection == name) {
            return Err(Error::Plan(format!(
                "collection `{name}` is named by lifecycle policy `{}`; drop the policy first",
                p.name
            )));
        }
        let aside = match &self.dir {
            Some(dir) => {
                let live = dir.join("collections").join(name);
                let aside = dropping_dir(dir, name);
                if aside.exists() {
                    fs::remove_dir_all(&aside)?;
                }
                if live.exists() {
                    fs::rename(&live, &aside)?;
                    crate::shard::sync_dir(&dir.join("collections"))?;
                }
                Some((dir.join("collections"), aside))
            }
            None => None,
        };
        // Past the point of no return. The store's objects go first, while
        // the shards are still open to name them.
        if let Some(mut shards) = self.shards.remove(name) {
            for s in shards.iter_mut() {
                s.retire_all();
            }
        }
        self.forget_collection(name);
        self.catalog.dropped.insert(name.to_string(), lifecycle::now_micros(&self.clock));
        self.persist_catalog()?;
        if let Some((parent, aside)) = aside {
            if aside.exists() {
                fs::remove_dir_all(&aside)?;
                crate::shard::sync_dir(&parent)?;
            }
        }
        Ok(())
    }

    /// Everything the engine records against a collection by name, gone.
    fn forget_collection(&mut self, name: &str) {
        self.catalog.collections.remove(name);
        self.catalog.placement.remove(name);
        self.catalog.activity.retain(|(c, _), _| c != name);
        let prefix = cache_key(name, "");
        guard(&self.stats).retain(|k, _| !k.starts_with(&prefix));
        self.stats_baseline.remove(name);
        self.collection_writes.remove(name);
        guard(&self.recall).query_log.retain(|q| q.collection != name);
    }

    /// Remove every directory a drop renamed aside and did not get to remove.
    fn sweep_dropping(dir: &Path) -> Result<()> {
        let parent = dir.join("collections");
        let Ok(entries) = fs::read_dir(&parent) else { return Ok(()) };
        let mut swept = false;
        for e in entries {
            let e = e?;
            if e.file_name().to_string_lossy().ends_with(DROPPING_SUFFIX) {
                fs::remove_dir_all(e.path())?;
                swept = true;
            }
        }
        if swept {
            crate::shard::sync_dir(&parent)?;
        }
        Ok(())
    }

    /// Drop an index: the catalog no longer declares it, so the planner stops
    /// using it; its decoded component is released from every segment, its
    /// access clock and its statistics go, the memtables are rebuilt without
    /// it, and the segments' tiers are re-resolved over the indexes that
    /// remain. The regions already written into sealed segments stay until
    /// compaction rewrites those segments, which mirrors CREATE INDEX
    /// writing nothing into them: an index is a declaration the next seal
    /// honours -- and, since the backfill trigger, the next compaction
    /// pass -- and a drop is its withdrawal. Refused while a lifecycle policy names
    /// the index by name; a policy covering every index of the collection
    /// simply covers one fewer.
    pub fn drop_index(&mut self, collection: &str, index: &str) -> Result<()> {
        if let Some(p) = self
            .catalog
            .policies
            .values()
            .find(|p| p.collection == collection && p.indexes.iter().any(|i| i == index))
        {
            return Err(Error::Plan(format!(
                "index `{index}` is named by lifecycle policy `{}`; drop the policy first",
                p.name
            )));
        }
        let c = self.catalog.get_mut(collection)?;
        let Some(pos) = c.indexes.iter().position(|i| i.name == index) else {
            return Err(Error::Plan(format!("no index `{index}` on collection `{collection}`")));
        };
        let def = c.indexes.remove(pos);
        let component = Collection::index_component(&def);
        let coll = c.clone();
        self.catalog.activity.remove(&(collection.to_string(), index.to_string()));
        self.catalog
            .dropped
            .insert(Catalog::tombstone(collection, index), lifecycle::now_micros(&self.clock));
        guard(&self.stats).remove(&cache_key(collection, &def.path));
        if let Some(shards) = self.shards.get_mut(collection) {
            for s in shards.iter_mut() {
                s.adopt_catalog(coll.clone())?;
                for h in &s.segments {
                    h.segment.unload_component(&component);
                }
            }
        }
        for f in followed_of(&self.followed, collection).iter_mut() {
            f.shard.adopt_catalog(coll.clone())?;
            for h in &f.shard.segments {
                h.segment.unload_component(&component);
            }
        }
        self.apply_tiers(collection)?;
        self.persist_catalog()
    }

    /// This node's advertised address, if it has one.
    pub fn node(&self) -> Option<&str> {
        self.opts.node.as_deref()
    }

    /// What this process wraps its wire connections in, if certificates
    /// were given; what a node dials another with.
    pub fn tls(&self) -> Option<Arc<crate::tls::Tls>> {
        self.opts.tls.clone()
    }

    /// The data directory, or `None` in memory.
    pub fn data_dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// The shards pinned for a move, shared with the wire server. See
    /// [`MoveOut`].
    pub fn moves(&self) -> Moves {
        self.moves.clone()
    }

    /// Where the shard owning `key` is going, if a move has pinned it: a
    /// write to it is refused naming the move until the map has switched.
    fn moving_to(&self, collection: &str, key: &str) -> Option<(usize, String)> {
        let shards = self.shards.get(collection)?;
        let s = shards.iter().find(|s| s.owns(key))?;
        let moves = self.moves.lock().unwrap_or_else(|p| p.into_inner());
        moves.get(&(collection.to_string(), s.index)).map(|m| (s.index, m.to.clone()))
    }

    fn refuse_if_moving(&self, collection: &str, key: &str) -> Result<()> {
        match self.moving_to(collection, key) {
            Some((i, to)) => Err(Error::Plan(format!(
                "shard {i} of `{collection}` is moving to {to}; the write is refused until the \
                 move completes, retry then"
            ))),
            None => Ok(()),
        }
    }

    /// The instant a statement started now would read at.
    pub fn now_ts(&self) -> Timestamp {
        self.clock.peek().max(self.last_commit)
    }

    /// A collection's definition.
    pub fn collection(&self, name: &str) -> Result<&Collection> {
        self.catalog.get(name)
    }

    /// Fold the shards' inferred statistics into the definition before a
    /// read plans against it; what `run_select` does for its own shards.
    /// Put a fault schedule between every query and the shards. See
    /// `crate::sim`; `None` takes it out again.
    pub fn install_sim(&mut self, sim: Arc<crate::sim::Sim>) {
        self.sim = Some(sim);
    }

    pub fn remove_sim(&mut self) {
        self.sim = None;
    }

    /// How many collections the catalog holds. What a health probe asks,
    /// because answering it means the catalog is there to be read.
    /// `SHOW HEALTH`: this node, every attached node dialled once with the
    /// wire's timeout, and every collection's shards with their holder and
    /// whether that holder answered -- what an operator asks first when a
    /// statement is refused naming a shard. One line each, a summary last.
    pub fn show_health(&self) -> String {
        let here = self.opts.node.clone().unwrap_or_else(|| "local".to_string());
        let mut out = String::new();
        let (seal_failures, last_seal) = self.seal_failures();
        out.push_str(&format!(
            "this node: {here}, {}, celastro {}, {} collection(s), {} shard(s) held, directory {}{}\n",
            self.opts.role.name(),
            env!("CARGO_PKG_VERSION"),
            self.catalog.collections.len(),
            self.shards.values().map(|s| s.len()).sum::<usize>(),
            if self.directory_present() { "present" } else { "GONE" },
            if seal_failures > 0 {
                format!(
                    ", {seal_failures} seal failure(s), last: {}",
                    last_seal.unwrap_or_default()
                )
            } else {
                String::new()
            }
        ));
        let lead = self.hlc_lead_micros();
        if lead > 1_000_000 {
            out.push_str(&format!(
                "clock: the HLC runs {:.1} s ahead of the wall clock, pushed there by a peer's \
                 timestamps; every commit from here on carries it\n",
                lead as f64 / 1e6
            ));
        }
        if let Some(tls) = &self.opts.tls {
            let now = crate::time::now_micros() / 1_000_000;
            let describe = |what: &str, at: i64| -> String {
                let left = at - now;
                let flag = if left < 0 {
                    " EXPIRED"
                } else if left < CERTIFICATE_WARN_SECS {
                    " EXPIRES SOON"
                } else {
                    ""
                };
                format!(
                    "{what} expires {} ({} day(s)){flag}",
                    crate::time::format_micros(at * 1_000_000),
                    left / 86_400
                )
            };
            out.push_str(&format!(
                "tls: {}; {}\n",
                describe("certificate", tls.expires_at()),
                describe("CA", tls.anchors_expire_at())
            ));
        }
        let mut up: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        up.insert(here.clone());
        // Every node this one knows of: the ones it attached, and every
        // holder a placement names, since a node that was attached by
        // another still holds shards this one's statements reach.
        let mut known: std::collections::BTreeSet<String> =
            self.catalog.nodes.iter().cloned().collect();
        for tablets in self.catalog.placement.values() {
            for t in tablets {
                if !t.node.is_empty() {
                    known.insert(t.node.clone());
                }
            }
        }
        let mut peers = 0usize;
        for url in &known {
            if *url == here {
                continue;
            }
            peers += 1;
            let started = std::time::Instant::now();
            let answer = self.node_conn(url).and_then(|n| n.hello());
            match answer {
                Ok(h) => {
                    up.insert(url.clone());
                    let notes = self.observe_peer(url, &h);
                    let clock = if h.now_micros > 0 {
                        let skew = h.now_micros as i64 - self.received_at(&h) as i64;
                        format!(
                            ", clock {:+.1} s{}",
                            skew as f64 / 1e6,
                            if skew.abs() > CLOCK_WARN_MICROS { " CLOCK OFF" } else { "" }
                        )
                    } else {
                        String::new()
                    };
                    let older = notes.iter().any(|n| n.starts_with("an older process"));
                    out.push_str(&format!(
                        "node {url}: up, {}, celastro {}, {} ms{clock}{}{}\n",
                        h.role.name(),
                        h.version,
                        started.elapsed().as_millis(),
                        if older { ", AN OLDER PROCESS ANSWERS HERE TOO" } else { "" },
                        if notes.iter().any(|n| n.contains(" restarted at ")) {
                            ", restarted since last seen"
                        } else {
                            ""
                        }
                    ));
                }
                Err(e) => out.push_str(&format!("node {url}: DOWN: {e}\n")),
            }
        }
        let mut unreachable = 0usize;
        for (name, tablets) in &self.catalog.placement {
            for (i, t) in tablets.iter().enumerate() {
                if t.is_merged() {
                    continue;
                }
                let holder = if t.node.is_empty() { here.clone() } else { t.node.clone() };
                let ok = up.contains(&holder);
                if !ok {
                    unreachable += 1;
                }
                out.push_str(&format!(
                    "shard {i} of `{name}`: on {holder}, {}\n",
                    if ok { "reachable" } else { "UNREACHABLE" }
                ));
            }
        }
        for name in &self.not_adopted {
            out.push_str(&format!(
                "collection `{name}`: NOT ADOPTED, a peer's map names this node as a holder \
                 and the data is not in this directory; restore it or drop it\n"
            ));
        }
        if self.opts.stewards.is_some() && self.steward().is_none() {
            out.push_str("steward: none elected yet; automatic failover waits for one\n");
        }
        if let Some(s) = self.steward() {
            let g = self.lease.lock().unwrap_or_else(|p| p.into_inner());
            out.push_str(&format!(
                "steward: {}{}{}; automatic failover {}{}\n",
                s,
                if self.is_self(&s) { " (this node)" } else { "" },
                match &g.election {
                    Some(e) => format!(" (elected, term {})", e.term()),
                    None => String::new(),
                },
                if self.opts.auto_failover { "on" } else { "off" },
                match (self.is_self(&s), g.at) {
                    (true, _) => String::new(),
                    (false, Some(at)) =>
                        format!("; lease renewed {} s ago", at.elapsed().as_secs()),
                    (false, None) => "; no lease yet".to_string(),
                }
            ));
        }
        for (name, shards) in &self.shards {
            for s in shards {
                if let Some(sh) = &s.shipper {
                    for f in sh.report() {
                        out.push_str(&format!(
                            "shard {} of `{name}`: follower {} {}, confirmed to ts {}, {} behind{}{}\n",
                            s.index,
                            f.url,
                            f.state,
                            f.acked,
                            f.backlog,
                            f.last_error.map(|e| format!(" ({e})")).unwrap_or_default(),
                            if f.state == "asking" {
                                "; DEGRADED: writes to this shard are acknowledged on this node's disk alone"
                            } else {
                                ""
                            }
                        ));
                    }
                }
            }
        }
        for ((name, i), f) in self.followed.lock().unwrap_or_else(|p| p.into_inner()).iter() {
            out.push_str(&format!(
                "follows shard {i} of `{name}` at term {}: {}\n",
                f.term,
                if f.shard.caught_up {
                    format!("caught up to ts {}", f.shard.ship_ts)
                } else {
                    "copying".into()
                }
            ));
        }
        out.push_str(&format!(
            "{} of {} node(s) answer; {unreachable} shard(s) unreachable",
            up.len(),
            peers + 1
        ));
        out
    }

    /// The data directory, or `None` in memory.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// Whether the data directory is still where it was opened: `true` in
    /// memory, and on disk while the `LOCK` this process holds is there. A
    /// health probe asks, so a node whose volume went away is not well.
    pub fn directory_present(&self) -> bool {
        self.dir.as_ref().map(|d| d.join("LOCK").exists()).unwrap_or(true)
    }

    /// Seals that failed and were left for a later write to retry, over
    /// every shard held here, with the last reason seen.
    pub fn seal_failures(&self) -> (u64, Option<String>) {
        let mut n = 0;
        let mut last = None;
        for shards in self.shards.values() {
            for s in shards {
                n += s.seal_failures;
                if s.last_seal_error.is_some() {
                    last = s.last_seal_error.clone();
                }
            }
        }
        (n, last)
    }

    pub fn collection_count(&self) -> usize {
        self.catalog.collections.len()
    }

    pub fn placement(&self) -> &Placement {
        &self.opts.placement
    }

    pub fn shards(&self, collection: &str) -> Result<&[Shard]> {
        self.catalog.get(collection)?;
        Ok(self.shards.get(collection).map(|v| v.as_slice()).unwrap_or(&[]))
    }

    pub fn memtable_budget(&self) -> &MemtableBudget {
        &self.budget
    }

    // ------------------------------------------------------------- control

    /// Create a collection. `splits` are the boundary keys of the tablet map:
    /// `n` split points make `n+1` shards, range-partitioned on the composite
    /// `(partition_key, primary_key)`.
    pub fn create_collection(&mut self, mut coll: Collection, splits: &[String]) -> Result<()> {
        let _deadline = self.arm_default_deadline();
        if coll.replicas == 0 {
            coll.replicas = DEFAULT_REPLICAS as u8;
        }
        let tablets = self.plan_tablets(splits, &[], coll.replicas as usize)?;
        // A library caller waits for the spread; the statement defers it.
        let _ = self.create_spread(coll, tablets)?.carry();
        Ok(())
    }

    /// Whether a placement entry means this node.
    /// This node's role.
    pub fn role(&self) -> Role {
        self.opts.role
    }

    /// The nodes a shard may be placed on: this one when it is a data
    /// node, then every attached node that is not a coordinator, in the
    /// order they were attached.
    fn data_nodes(&self) -> Vec<String> {
        let mut v = Vec::new();
        if let Some(me) = &self.opts.node {
            if self.opts.role == Role::Data {
                v.push(me.clone());
            }
        }
        v.extend(
            self.catalog.nodes.iter().filter(|n| !self.catalog.coordinators.contains(*n)).cloned(),
        );
        v
    }

    /// Whether `node` is a coordinator: this one by its role, another by
    /// what its `hello` said at `ATTACH`.
    fn is_coordinator(&self, node: &str) -> bool {
        if self.is_self(node) {
            self.opts.role == Role::Coordinator
        } else {
            self.catalog.coordinators.contains(node)
        }
    }

    /// The nodes a DDL reaches besides this one: every holder of the
    /// collection, and every coordinator, which plans over it.
    fn ddl_targets(&self, collection: &str) -> Vec<String> {
        let mut v = self.holders(collection);
        for f in self.followers_of(collection) {
            if !v.contains(&f) {
                v.push(f);
            }
        }
        for c in &self.catalog.coordinators {
            if !self.is_self(c) && !v.contains(c) {
                v.push(c.clone());
            }
        }
        v
    }

    /// The other nodes following a shard of `collection`, each once.
    fn followers_of(&self, collection: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .catalog
            .placement
            .get(collection)
            .map(|t| {
                t.iter()
                    .filter(|x| !x.is_merged())
                    .flat_map(|x| x.followers.iter().cloned())
                    .filter(|n| !self.is_self(n))
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v.dedup();
        v
    }

    /// The shards this node follows, as the map says: a followed shard
    /// directory for every tablet that names this node and has none, and
    /// none for a tablet that no longer does. A copy made here starts
    /// empty; the holder's log fills it.
    pub(crate) fn ensure_followed(&mut self, collection: &str) -> Result<()> {
        let Some(tablets) = self.catalog.placement.get(collection).cloned() else {
            return Ok(());
        };
        let coll = self.catalog.get(collection)?.clone();
        let wanted: Vec<usize> = tablets
            .iter()
            .enumerate()
            .filter(|(_, t)| {
                !t.is_merged()
                    && !self.is_self(&t.node)
                    && t.followers.iter().any(|f| self.is_self(f))
            })
            .map(|(i, _)| i)
            .collect();
        let have: Vec<usize> = self
            .followed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .filter(|(c, _)| c == collection)
            .map(|(_, i)| *i)
            .collect();
        for i in have.iter().filter(|i| !wanted.contains(i)) {
            self.drop_followed(collection, *i);
        }
        {
            // The terms and the ranges as the map says them now, for the
            // copies kept. The range moves at a split or a merge, and a
            // copy left with the old one masked out the rows a merge
            // absorbed: they arrived, counted for nothing, and a copy
            // promoted after the merge answered two rows of three.
            let mut g = self.followed.lock().unwrap_or_else(|p| p.into_inner());
            for i in wanted.iter().filter(|i| have.contains(i)) {
                if let Some(f) = g.get_mut(&(collection.to_string(), *i)) {
                    let t = &tablets[*i];
                    f.term = t.term;
                    if f.shard.key_range() != (t.lo.clone(), t.hi.clone()) {
                        f.shard.set_key_range(t.lo.clone(), t.hi.clone());
                        if let Some(d) = f.shard.dir().map(|d| d.to_path_buf()) {
                            crate::shard::write_content(
                                &self.cipher,
                                &format!("shard-{i:04}/RANGE"),
                                &d.join("RANGE"),
                                format!(
                                    "{}\n{}",
                                    t.lo.clone().unwrap_or_default(),
                                    t.hi.clone().unwrap_or_default()
                                )
                                .as_bytes(),
                            )?;
                        }
                    }
                }
            }
        }
        for i in wanted.iter().filter(|i| !have.contains(i)) {
            let t = &tablets[*i];
            let mut sh = Shard::new(coll.clone(), self.clock.clone(), self.shard_opts())
                .with_key_range(t.lo.clone(), t.hi.clone());
            sh.index = *i;
            if let Some(dir) = &self.dir {
                let fdir = dir
                    .join("collections")
                    .join(collection)
                    .join("followed")
                    .join(format!("shard-{i:04}"));
                if fdir.exists() {
                    fs::remove_dir_all(&fdir)?;
                }
                fs::create_dir_all(&fdir)?;
                crate::shard::write_content(
                    &self.cipher,
                    &format!("shard-{i:04}/RANGE"),
                    &fdir.join("RANGE"),
                    format!(
                        "{}\n{}",
                        t.lo.clone().unwrap_or_default(),
                        t.hi.clone().unwrap_or_default()
                    )
                    .as_bytes(),
                )?;
                sh.attach_dir(&fdir)?;
                crate::shard::sync_dir(&dir.join("collections").join(collection))?;
            }
            self.followed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert((collection.to_string(), *i), FollowedShard { shard: sh, term: t.term });
        }
        Ok(())
    }

    /// Let go of a followed copy: its directory removed.
    fn drop_followed(&mut self, collection: &str, shard: usize) {
        let gone = self
            .followed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(collection.to_string(), shard))
            .map(|f| f.shard);
        if let Some(mut s) = gone {
            s.retire_all();
            if let Some(d) = s.dir().map(|d| d.to_path_buf()) {
                let _ = fs::remove_dir_all(&d);
                if let Some(p) = d.parent() {
                    let _ = crate::shard::sync_dir(p);
                }
            }
        }
    }

    fn is_self(&self, node: &str) -> bool {
        node.is_empty() || Some(node) == self.opts.node.as_deref()
    }

    /// The other nodes holding a shard of `collection`, each once.
    fn holders(&self, collection: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .catalog
            .placement
            .get(collection)
            .map(|t| {
                t.iter()
                    .filter(|x| !x.is_merged())
                    .map(|x| x.node.clone())
                    .filter(|n| !self.is_self(n))
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v.dedup();
        v
    }

    fn node_conn(&self, url: &str) -> Result<Arc<crate::wire::Node>> {
        if let Some(n) = guard(&self.nodes).get(url) {
            return Ok(n.clone());
        }
        let n = Arc::new(self.wire_node(url)?);
        guard(&self.nodes).insert(url.to_string(), n.clone());
        Ok(n)
    }

    /// What dials a holder with the lock let go, as `wire_node` does under
    /// it: for a carry whose holder names another, the map having moved
    /// under the statement.
    fn dialer(&self) -> Dialer {
        Dialer {
            token: self.wire_token.clone(),
            tls: self.opts.tls.clone(),
            me: self.opts.node.clone(),
            epoch: self.epoch_cell(),
        }
    }

    /// A wire peer as this node speaks to it: the token, the TLS, and this
    /// node's identity for the frames that carry one.
    fn wire_node(&self, url: &str) -> Result<crate::wire::Node> {
        let n = crate::wire::Node::new(url, self.wire_token.as_deref(), self.opts.tls.clone())?;
        Ok(match &self.opts.node {
            Some(me) => n.with_identity(me, self.epoch_cell()),
            None => n,
        })
    }

    /// Placement for a new collection: `splits` make `n+1` shards, shard `i`
    /// goes to the `i`-th of `nodes`, wrapping. No nodes named means this
    /// node and every attached one; a node with no address places every
    /// shard on itself.
    fn plan_tablets(
        &self,
        splits: &[String],
        nodes: &[String],
        replicas: usize,
    ) -> Result<Vec<Tablet>> {
        for k in splits {
            if k.is_empty() || k.contains('\n') {
                return Err(Error::Schema(format!(
                    "split key {k:?} cannot be stored in the tablet map: \
                     a split key must be non-empty and must not contain a line break"
                )));
            }
        }
        let nodes: Vec<String> = if !nodes.is_empty() {
            if self.opts.node.is_none() {
                return Err(Error::Plan(
                    "this node has no address, so it cannot place shards on other nodes; start \
                     it with CELASTRO_NODE=tcp://host:port (DbOpts::node)"
                        .into(),
                ));
            }
            for n in nodes {
                if !self.is_self(n) && !self.catalog.nodes.iter().any(|x| x == n) {
                    return Err(Error::Plan(format!(
                        "node {n} is not attached; ATTACH NODE '{n}' first"
                    )));
                }
                if self.is_coordinator(n) {
                    return Err(Error::Plan(format!(
                        "node {n} is a coordinator and holds no shards; name a data node"
                    )));
                }
            }
            nodes.to_vec()
        } else {
            match &self.opts.node {
                Some(_) => {
                    let v = self.data_nodes();
                    if v.is_empty() {
                        return Err(Error::Plan(
                            "no data node to place shards on: this node is a coordinator and no \
                             data node is attached; ATTACH NODE the data nodes first"
                                .into(),
                        ));
                    }
                    v
                }
                None => vec![String::new()],
            }
        };
        let mut tablets: Vec<Tablet> = (0..=splits.len())
            .map(|i| Tablet {
                node: nodes[i % nodes.len()].clone(),
                lo: if i == 0 { None } else { Some(splits[i - 1].clone()) },
                hi: splits.get(i).cloned(),
                ..Default::default()
            })
            .collect();
        for (i, t) in tablets.iter_mut().enumerate() {
            t.followers = Self::followers_for(&nodes, i, replicas);
        }
        Ok(tablets)
    }

    /// The followers of shard `i` under `replicas` copies: the `replicas -
    /// 1` nodes after its holder in the placement order, wrapping, each
    /// once, the holder never; fewer when the nodes run out.
    fn followers_for(nodes: &[String], i: usize, replicas: usize) -> Vec<String> {
        let holder = &nodes[i % nodes.len()];
        let mut out = Vec::new();
        let mut k = i + 1;
        while out.len() + 1 < replicas && k < i + nodes.len() {
            let n = &nodes[k % nodes.len()];
            if n != holder && !out.contains(n) && !n.is_empty() {
                out.push(n.clone());
            }
            k += 1;
        }
        out
    }

    /// Create a collection under a placement: here, then on every other
    /// node the placement names, each adopting the same definition and the
    /// same map and building the shards placed on it. A node that does not
    /// take it is reported by name; the statement is idempotent on a node
    /// that already holds the identical collection, so it can be re-run.
    /// Make the collection here and on every holder and coordinator. The
    /// string is empty, or names the nodes it did not reach: they adopt the
    /// collection when they reconnect, so that is a note and not a failure.
    fn create_spread(&mut self, coll: Collection, tablets: Vec<Tablet>) -> Result<Spread> {
        self.adopt_collection(coll.clone(), tablets.clone())?;
        let mut failures = Vec::new();
        // Every holder, and every coordinator: a coordinator holds none of
        // it but plans over it, so it needs the definition and the map. All
        // at once, holding nothing, as a definition's fan-out does.
        let remaining = crate::deadline::remaining_ms();
        let mut conns = Vec::new();
        for url in self.ddl_targets(&coll.name) {
            match self.node_conn(&url) {
                Ok(n) => conns.push((url, n)),
                Err(e) => failures.push(format!("{url}: {e}")),
            }
        }
        Ok(Spread { coll, tablets, conns, failures, remaining })
    }

    /// This process's epoch: when it opened the database, or what
    /// [`Db::pretend`] said.
    pub fn epoch(&self) -> u64 {
        self.pretend_epoch.unwrap_or(self.epoch)
    }

    /// The claimed epoch as a shared cell, for the wire's frames.
    fn epoch_cell(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.epoch_cell.store(self.epoch(), std::sync::atomic::Ordering::Relaxed);
        self.epoch_cell.clone()
    }

    /// This node's wall clock as `hello` reports it, microseconds. The wall
    /// clock and not the HLC: the HLC runs ahead of the wall by whatever a
    /// peer's timestamps pushed it to, which is not skew, and a drill saw
    /// two pods on one kernel accuse each other of ten seconds by it.
    pub fn clock_micros(&self) -> u64 {
        (crate::time::now_micros() + self.pretend_clock_micros).max(0) as u64
    }

    /// How far this node's HLC runs ahead of its wall clock, microseconds:
    /// what a peer's timestamps pushed it to. Named by `SHOW HEALTH` past a
    /// second, since every commit from here on carries it.
    pub fn hlc_lead_micros(&self) -> i64 {
        lifecycle::now_micros(&self.clock) as i64 - crate::time::now_micros()
    }

    /// Claim another epoch, or a clock this far off, in every `hello` from
    /// now on. For tests and drills of what peers do about a zombie and a
    /// skewed clock; nothing else changes.
    pub fn pretend(&mut self, epoch: Option<u64>, clock_offset_micros: i64) {
        self.pretend_epoch = epoch;
        self.pretend_clock_micros = clock_offset_micros;
        self.epoch_cell.store(self.epoch(), std::sync::atomic::Ordering::Relaxed);
    }

    /// When a hello was read, on this node's clock as `hello` reports it:
    /// the receipt instant the wire stamped, offset like `clock_micros`
    /// for a test that pretends, or now for a hello made by hand.
    fn received_at(&self, hello: &crate::wire::Hello) -> u64 {
        if hello.received_micros > 0 {
            (hello.received_micros as i64 + self.pretend_clock_micros).max(0) as u64
        } else {
            self.clock_micros()
        }
    }

    /// A call's caller, from a version-5 frame: refused when a newer process
    /// has been seen at its address, which is a zombie calling -- the pod
    /// replaced while its predecessor still runs, forwarding its writes.
    /// A newer epoch than seen raises the record, as a hello's would.
    pub fn observe_caller(&self, node: &str, epoch: u64) -> Result<()> {
        if epoch == 0 {
            return Ok(());
        }
        let mut seen = guard(&self.peers_seen);
        let entry = seen.entry(node.to_string()).or_default();
        if entry.epoch > 0 && epoch < entry.epoch {
            return Err(Error::Plan(format!(
                "a call from an older process at {node}: it started at {} while one started at {} \
                 was seen there; two processes share the address, and this one is refused",
                crate::time::format_micros(epoch as i64),
                crate::time::format_micros(entry.epoch as i64)
            )));
        }
        entry.epoch = entry.epoch.max(epoch);
        Ok(())
    }

    /// What has been seen of a peer, if a hello from it was observed.
    pub fn peer_seen(&self, url: &str) -> Option<PeerSeen> {
        guard(&self.peers_seen).get(url).copied()
    }

    /// Take note of what a peer's hello says about it, against what earlier
    /// ones said. The notes name what an operator should know: an older
    /// process answering at the address (two processes, one address -- a
    /// pod that was replaced while its predecessor still runs), a restart,
    /// a clock further from this node's than `CLOCK_WARN_MICROS`.
    pub fn observe_peer(&self, url: &str, hello: &crate::wire::Hello) -> Vec<String> {
        let mut notes = Vec::new();
        let mut seen = guard(&self.peers_seen);
        let entry = seen.entry(url.to_string()).or_default();
        if hello.epoch > 0 {
            if entry.epoch > 0 && hello.epoch < entry.epoch {
                notes.push(format!(
                    "an older process answers at {url}: it started at {} while one started at {} \
                     was seen there; two processes share the address",
                    crate::time::format_micros(hello.epoch as i64),
                    crate::time::format_micros(entry.epoch as i64)
                ));
            } else if entry.epoch > 0 && hello.epoch > entry.epoch {
                notes.push(format!(
                    "{url} restarted at {}",
                    crate::time::format_micros(hello.epoch as i64)
                ));
            }
            entry.epoch = entry.epoch.max(hello.epoch);
        }
        if hello.now_micros > 0 {
            entry.skew_micros = hello.now_micros as i64 - self.received_at(hello) as i64;
            if entry.skew_micros.abs() > CLOCK_WARN_MICROS {
                notes.push(format!(
                    "the clock at {url} is {:+.1} s from this node's",
                    entry.skew_micros as f64 / 1e6
                ));
            }
        }
        notes
    }

    /// Every peer this node knows of, with a connection to each: what the
    /// console's sweep pulls a catalog from, dialled outside the lock.
    pub fn peers(&self) -> Vec<(String, Arc<crate::wire::Node>)> {
        let mut known: BTreeSet<String> = self.catalog.nodes.iter().cloned().collect();
        known.extend(self.attached.iter().cloned());
        for tablets in self.catalog.placement.values() {
            known.extend(tablets.iter().map(|t| t.node.clone()));
        }
        known
            .into_iter()
            .filter(|n| !self.is_self(n))
            .filter_map(|n| self.node_conn(&n).ok().map(|c| (n, c)))
            .collect()
    }

    /// Fold what another node's catalog knows into this one: the
    /// definitions made while the two could not reach each other -- across
    /// a split, or while this node was down -- and the drops. Each note
    /// returned names one change made, or one refused.
    ///
    /// The rule is per name and last writer wins: a collection or an index
    /// the peer has and this node lacks is adopted unless this node holds a
    /// tombstone for it younger than the definition; a tombstone the peer
    /// holds drops the definition here if the definition is older. Policies
    /// and the activity clocks are unioned. Placement is
    /// [`Db::reconcile_from`]'s, by the holders' own word. An `ALTER` is
    /// not merged: it is applied by the statement's own fan-out or by an
    /// operator. What this closes is the split's harm named in the design
    /// notes: two catalogs that stay different after the link returns.
    ///
    /// A data node adopts a collection whose map names it as a holder only
    /// if the collection is younger than the node's data directory. Older
    /// means the directory never had those shards' data: a node restarted
    /// from an empty volume, which needs a restore, not empty shards that
    /// answer as if nothing were lost.
    /// [`Db::reconcile`], then the placement: a peer's word about the shards
    /// it holds, or held, is final. For a collection both have, a shard
    /// the peer's map puts on the peer and this node's map puts elsewhere
    /// moves to the peer in this node's map; a shard this node's map puts
    /// on the peer and the peer's map puts elsewhere moves there. What a
    /// move made while the two could not reach each other left behind: the
    /// far side's map naming the old holder, which then refused the
    /// forwarded writes as not its own. A shard both this node and the
    /// peer claim to hold is a conflict, kept as it is and named.
    pub fn reconcile_from(&mut self, peer: &str, theirs: &Catalog) -> Result<Vec<String>> {
        let mut notes = self.reconcile(theirs)?;
        let mut changed = false;
        let mut demote: Vec<(String, usize)> = Vec::new();
        for (name, their_tablets) in &theirs.placement {
            let Some(mine) = self.catalog.placement.get(name) else { continue };
            if mine.len() != their_tablets.len() {
                // A longer map whose added shards the peer holds is a split
                // the peer made while the two could not reach each other:
                // its word about its own shards, the ranges of the ones it
                // already held included, is taken; a longer map that names
                // others for the added shards is not, since it is not the
                // holders' word.
                if their_tablets.len() > mine.len()
                    && their_tablets[mine.len()..].iter().all(|t| t.node == peer)
                {
                    let mut updated = mine.clone();
                    for (i, t) in their_tablets.iter().enumerate() {
                        if i >= updated.len() {
                            updated.push(t.clone());
                        } else if t.node == peer && updated[i].node == peer {
                            updated[i] = t.clone();
                        }
                    }
                    notes.push(format!(
                        "`{name}`: {} shard(s) added by {peer}'s word (a split there)",
                        their_tablets.len() - mine.len()
                    ));
                    self.catalog.placement.insert(name.clone(), updated);
                    changed = true;
                }
                continue;
            }
            let mut updated = mine.clone();
            for (i, (m, t)) in mine.iter().zip(their_tablets).enumerate() {
                // A higher term is a promotion this node has not seen: the
                // map at that term wins, whoever carries it, and a copy this
                // node still holds at the old term follows from now on.
                if t.term > m.term {
                    let held_here = self.is_self(&m.node)
                        && self.shards.get(name).is_some_and(|s| s.iter().any(|s| s.index == i));
                    if held_here {
                        demote.push((name.clone(), i));
                    }
                    updated[i] = t.clone();
                    changed = true;
                    notes.push(format!(
                        "shard {i} of `{name}`: term {} on {} (was term {} on {})",
                        t.term,
                        t.node,
                        m.term,
                        if m.node.is_empty() { "this node" } else { m.node.as_str() }
                    ));
                    continue;
                }
                if t.term < m.term {
                    continue;
                }
                let peer_claims = t.node == peer && m.node != peer;
                let peer_gave_away = m.node == peer && t.node != peer;
                // The peer's word about its own shards' ranges too: a split
                // or a merge it made while the two could not reach each
                // other changed no holder, only where a key goes.
                if t.node == peer && m.node == peer && (t.lo != m.lo || t.hi != m.hi) {
                    updated[i] = t.clone();
                    changed = true;
                    notes.push(format!("shard {i} of `{name}`: its range by {peer}'s word"));
                    continue;
                }
                if !(peer_claims || peer_gave_away) {
                    continue;
                }
                let held_here = self.is_self(&m.node)
                    && self.shards.get(name).is_some_and(|s| s.iter().any(|s| s.index == i));
                if peer_claims && held_here {
                    notes.push(format!(
                        "shard {i} of `{name}`: held here and claimed by {peer}; kept here, \
                         resolve by MOVE SHARD"
                    ));
                    continue;
                }
                updated[i] = t.clone();
                changed = true;
                notes.push(format!(
                    "shard {i} of `{name}`: now on {} (was {})",
                    t.node,
                    if m.node.is_empty() { "this node" } else { m.node.as_str() }
                ));
            }
            if changed {
                self.catalog.placement.insert(name.clone(), updated);
            }
        }
        for (name, i) in demote {
            self.demote_here(&name, i)?;
            notes.push(format!("shard {i} of `{name}`: demoted here, following the new holder"));
        }
        if changed {
            self.persist_catalog()?;
        }
        Ok(notes)
    }

    pub fn reconcile(&mut self, theirs: &Catalog) -> Result<Vec<String>> {
        let mut notes = Vec::new();
        // Their tombstones first, so a definition they dropped is not
        // adopted back from a third node's catalog in the same sweep.
        for (key, &t) in &theirs.dropped {
            let mine = self.catalog.dropped.get(key).copied().unwrap_or(0);
            let t = t.max(mine);
            match key.split_once('/') {
                None => {
                    let created = self.catalog.get(key).map(|c| c.created_micros);
                    if let Ok(created) = created {
                        if created < t {
                            match self.drop_collection(key) {
                                Ok(()) => notes.push(format!("dropped collection `{key}`")),
                                Err(e) => {
                                    notes.push(format!("collection `{key}` not dropped: {e}"))
                                }
                            }
                        } else {
                            // This incarnation outlives the drop; an index
                            // made on one that did not, and merged here
                            // before the drop was known, does not.
                            let stale: Vec<String> = self
                                .catalog
                                .get(key)
                                .map(|c| {
                                    c.indexes
                                        .iter()
                                        .filter(|i| i.on_micros < t)
                                        .map(|i| i.name.clone())
                                        .collect()
                                })
                                .unwrap_or_default();
                            for idx in stale {
                                let own = self
                                    .catalog
                                    .dropped
                                    .get(&Catalog::tombstone(key, &idx))
                                    .copied();
                                match self.drop_index(key, &idx) {
                                    Ok(()) => notes.push(format!(
                                        "dropped index `{idx}` on `{key}`: made on an \
                                         incarnation since dropped"
                                    )),
                                    Err(e) => notes.push(format!("index `{idx}` not dropped: {e}")),
                                }
                                // Its own tombstone is not the collection's;
                                // a re-creation on the live incarnation stands,
                                // and a drop of its own from before still counts.
                                let tomb = Catalog::tombstone(key, &idx);
                                match own {
                                    Some(t) => self.catalog.dropped.insert(tomb, t),
                                    None => self.catalog.dropped.remove(&tomb),
                                };
                            }
                        }
                    }
                }
                Some((coll, idx)) => {
                    let has = self.catalog.get(coll).map(|c| c.index_by_name(idx).is_some());
                    if has.unwrap_or(false) {
                        let created = self
                            .catalog
                            .activity
                            .get(&(coll.to_string(), idx.to_string()))
                            .map(|a| a.created_micros)
                            .unwrap_or(0);
                        if created < t {
                            match self.drop_index(coll, idx) {
                                Ok(()) => notes.push(format!("dropped index `{idx}` on `{coll}`")),
                                Err(e) => notes.push(format!("index `{idx}` not dropped: {e}")),
                            }
                        }
                    }
                }
            }
            // The drop above stamped a tombstone of its own, at now; the
            // instant that counts is the peer's, or a re-creation between
            // the two would be dropped by a drop that came before it.
            self.catalog.dropped.insert(key.clone(), t);
        }
        for (name, coll) in &theirs.collections {
            let Some(tablets) = theirs.placement.get(name) else { continue };
            if self.catalog.get(name).is_err() {
                if self.catalog.dropped.get(name).copied().unwrap_or(0) >= coll.created_micros {
                    continue;
                }
                let names_me = tablets.iter().any(|t| self.is_self(&t.node));
                if self.opts.role == Role::Data
                    && names_me
                    && coll.created_micros < self.catalog.born_micros
                {
                    if self.not_adopted.insert(name.clone()) {
                        notes.push(format!(
                            "collection `{name}` not adopted: its map names this node as a \
                             holder and it is older than this data directory, so the data is \
                             not here; restore it (RESTORE ... NODE) rather than starting from \
                             empty shards"
                        ));
                    }
                    continue;
                }
                // Without the indexes a tombstone here outranks: the peer
                // may not have heard of the drop yet.
                let mut coll = coll.clone();
                let coll_tomb = self.catalog.dropped.get(name).copied().unwrap_or(0);
                coll.indexes.retain(|idx| {
                    let created = theirs
                        .activity
                        .get(&(name.clone(), idx.name.clone()))
                        .map(|a| a.created_micros)
                        .unwrap_or(0);
                    let tomb = Catalog::tombstone(name, &idx.name);
                    self.catalog.dropped.get(&tomb).copied().unwrap_or(0) < created
                        && idx.on_micros > coll_tomb
                });
                match self.adopt_collection(coll.clone(), tablets.clone()) {
                    Ok(()) => {
                        self.not_adopted.remove(name);
                        for (key, activity) in &theirs.activity {
                            if key.0 == *name {
                                self.catalog.activity.insert(key.clone(), *activity);
                            }
                        }
                        notes.push(format!(
                            "adopted collection `{name}` with {} index(es)",
                            coll.indexes.len()
                        ));
                    }
                    Err(e) => notes.push(format!("collection `{name}` not adopted: {e}")),
                }
                continue;
            }
            for idx in &coll.indexes {
                if self.catalog.get(name)?.index_by_name(&idx.name).is_some() {
                    continue;
                }
                let key = (name.clone(), idx.name.clone());
                let created = theirs.activity.get(&key).map(|a| a.created_micros).unwrap_or(0);
                let tomb = Catalog::tombstone(name, &idx.name);
                if self.catalog.dropped.get(&tomb).copied().unwrap_or(0) >= created {
                    continue;
                }
                // Made on an incarnation this node knows was dropped: the
                // peer has not heard of the drop yet, and will.
                if idx.on_micros <= self.catalog.dropped.get(name).copied().unwrap_or(0) {
                    continue;
                }
                match self.add_index(name, idx.clone()) {
                    Ok(()) => {
                        if let Some(a) = theirs.activity.get(&key) {
                            self.catalog.activity.insert(key, *a);
                        }
                        notes.push(format!("adopted index `{}` on `{name}`", idx.name));
                    }
                    Err(e) => notes.push(format!("index `{}` not adopted: {e}", idx.name)),
                }
            }
        }
        for (name, policy) in &theirs.policies {
            if self.catalog.get(&policy.collection).is_err() {
                continue;
            }
            if !self.catalog.policies.contains_key(name) {
                self.catalog.policies.insert(name.clone(), policy.clone());
                notes.push(format!("adopted policy `{name}`"));
            }
        }
        for (key, activity) in &theirs.activity {
            self.catalog.activity.entry(key.clone()).or_insert(*activity);
        }
        if !notes.is_empty() {
            self.persist_catalog()?;
        }
        Ok(notes)
    }

    /// Take a collection's definition and placement as another node planned
    /// them, building the shards the map puts here. What the wire calls on
    /// the other holders, and what `create_collection` does locally.
    /// Idempotent for an identical definition and map.
    pub fn adopt_collection(&mut self, coll: Collection, tablets: Vec<Tablet>) -> Result<()> {
        let name = coll.name.clone();
        if let Ok(existing) = self.catalog.get(&name) {
            let same = existing.primary_key == coll.primary_key
                && existing.partition_key == coll.partition_key
                && existing.declared == coll.declared
                && existing.prefix_expansion == coll.prefix_expansion
                && self.catalog.placement.get(&name) == Some(&tablets);
            return if same {
                Ok(())
            } else {
                Err(Error::Schema(format!(
                    "collection `{name}` already exists here with a different definition or \
                     placement"
                )))
            };
        }
        self.catalog.create(coll.clone())?;
        let shards = match self.build_shards(&coll, &tablets) {
            Ok(s) => s,
            Err(e) => {
                self.catalog.collections.remove(&name);
                return Err(e);
            }
        };
        self.catalog.placement.insert(name.clone(), tablets);
        if !shards.is_empty() {
            self.shards.insert(name.clone(), shards);
        }
        self.ensure_followed(&name)?;
        self.persist_catalog()?;
        Ok(())
    }

    /// Build the shards of `tablets` placed on this node, each under its
    /// index. Separate from [`Db::adopt_collection`] so that a failure part
    /// way through has one place to unwind from.
    fn build_shards(&self, coll: &Collection, tablets: &[Tablet]) -> Result<Vec<Shard>> {
        let mut shards = Vec::new();
        for (i, t) in tablets.iter().enumerate() {
            if !self.is_self(&t.node) {
                continue;
            }
            let (lo, hi) = (t.lo.clone(), t.hi.clone());
            let mut sh = Shard::new(coll.clone(), self.clock.clone(), self.shard_opts())
                .with_key_range(lo.clone(), hi.clone());
            sh.index = i;
            if let Some(dir) = &self.dir {
                let sdir = dir.join("collections").join(&coll.name).join(format!("shard-{i:04}"));
                fs::create_dir_all(&sdir)?;
                // The tablet map is the first thing a shard directory holds,
                // and the CREATE COLLECTION that writes it is acknowledged
                // durably -- so an atomic, synced write, not a bare
                // `fs::write` that a crash could disagree with the catalog
                // about.
                crate::shard::write_content(
                    &self.cipher,
                    &format!("shard-{i:04}/RANGE"),
                    &sdir.join("RANGE"),
                    format!("{}\n{}", lo.unwrap_or_default(), hi.unwrap_or_default()).as_bytes(),
                )?;
                sh.attach_dir(&sdir)?;
            }
            shards.push(sh);
        }
        if let Some(dir) = &self.dir {
            // A node that holds none of the collection's shards -- a
            // coordinator, or a data node the placement skipped -- still
            // gets the collection's directory, so the sync below has one
            // to sync and a reopen finds the collection where it looks.
            fs::create_dir_all(dir.join("collections").join(&coll.name))?;
            // The shard directories now hold durable files under durable
            // names. The names of the DIRECTORIES are a separate question:
            // `create_dir_all` leaves them as dirty metadata in their parents,
            // and fsyncing a file cannot create the directory entry that
            // reaches it. Without this walk an INSERT leaves a durable CATALOG
            // naming a collection whose directory a crash can lose.
            crate::shard::sync_dir(dir)?;
            crate::shard::sync_dir(&dir.join("collections"))?;
            crate::shard::sync_dir(&dir.join("collections").join(&coll.name))?;
        }
        Ok(shards)
    }

    /// Declare a node this one may place shards on. The node is asked who it
    /// is, so a typo is refused now rather than at the first CREATE. The
    /// asking happens under the caller's lock; a node attaching its peers
    /// at start dials first and calls [`Db::attach_prepared`], so a peer
    /// that vanished between the two does not hold every statement for a
    /// deadline.
    pub fn attach_node(&mut self, url: &str) -> Result<()> {
        let _deadline = self.arm_default_deadline();
        self.attachable(url)?;
        let hello = self.node_conn(url)?.hello()?;
        let theirs = self.node_conn(url).and_then(|n| n.catalog()).ok();
        self.attach_prepared(url, &hello, theirs)
    }

    /// What `ATTACH` refuses before it dials.
    fn attachable(&self, url: &str) -> Result<String> {
        let me = self.opts.node.clone().ok_or_else(|| {
            Error::Plan(
                "this node has no address; start it with CELASTRO_NODE=tcp://host:port \
                 (DbOpts::node) before attaching others"
                    .into(),
            )
        })?;
        crate::wire::parse_url(url)?;
        if url == me {
            return Err(Error::Plan("a node cannot attach itself".into()));
        }
        Ok(me)
    }

    /// Attach a node whose hello, and catalog, the caller already fetched
    /// without holding the lock. What a node at start does for each peer.
    pub fn attach_prepared(
        &mut self,
        url: &str,
        hello: &crate::wire::Hello,
        theirs: Option<Catalog>,
    ) -> Result<()> {
        self.attachable(url)?;
        match hello.node.as_deref() {
            Some(a) if a == url => {}
            Some(a) => {
                return Err(Error::Plan(format!(
                    "the node at {url} calls itself {a}; attach it by that address"
                )))
            }
            None => {
                return Err(Error::Plan(format!(
                    "the node at {url} has no address; start it with CELASTRO_NODE={url}"
                )))
            }
        }
        // The hello came back over a frame the peer checked against its
        // wire version, and that is the compatibility that matters: two
        // crate versions with one wire version speak. Until 0.34.0 the crate
        // versions had to be equal, which made a rolling upgrade impossible
        // -- the first pod on the new version could attach nobody, was
        // never ready, and the rollout never moved.
        let _ = &hello.version;
        if hello.now_micros > 0 {
            let skew = hello.now_micros as i64 - self.received_at(hello) as i64;
            if skew.abs() > CLOCK_REFUSE_MICROS {
                return Err(Error::Plan(format!(
                    "the clock at {url} is {:+.1} s from this node's, more than {} s: every \
                     timestamp and every tombstone compares by the clock, so the two would \
                     disagree about which of two statements was last; fix the clocks (NTP) \
                     before attaching",
                    skew as f64 / 1e6,
                    CLOCK_REFUSE_MICROS / 1_000_000
                )));
            }
        }
        for note in self.observe_peer(url, hello) {
            crate::log::warn("peer", &[("node", url.to_string()), ("note", note)]);
        }
        self.attached.insert(url.to_string());
        if !self.catalog.nodes.iter().any(|n| n == url) {
            self.catalog.nodes.push(url.to_string());
        }
        if hello.role == Role::Coordinator {
            self.catalog.coordinators.insert(url.to_string());
        } else {
            self.catalog.coordinators.remove(url);
        }
        // A node that attaches another learns what it knows: the
        // collections, indexes, drops and policies made before this node
        // existed or while it was away, so a coordinator restarted from an
        // empty volume plans as soon as it has attached and a data node
        // that was down catches up on the definitions it missed. What a
        // data node does not do is grow empty shards for a map that names
        // it from before its directory existed; `reconcile` refuses that
        // and says so. A peer from before this call answers nothing, and
        // nothing is adopted.
        if let Some(theirs) = theirs {
            for note in self.reconcile_from(url, &theirs)? {
                crate::log::info(
                    "catalog_reconciled",
                    &[("peer", url.to_string()), ("change", note)],
                );
            }
        }
        self.persist_catalog()
    }

    /// How many other nodes this process has verified since it started.
    /// What a readiness probe compares with the peers a node was given, so
    /// a pod is not routed to before it can reach the shards it does not
    /// hold.
    pub fn attached_count(&self) -> usize {
        self.attached.len()
    }

    /// Forget a node. Refused while a placement still names it: the shards
    /// there would become unreachable with nothing saying so.
    pub fn detach_node(&mut self, url: &str) -> Result<()> {
        if !self.catalog.nodes.iter().any(|n| n == url) {
            return Err(Error::Plan(format!("node {url} is not attached")));
        }
        // A node that still holds shards is not detached; the refusal is
        // the plan that would empty it: one MOVE SHARD per shard, targets
        // round-robin over the nodes that remain, in attach order.
        let mut remaining: Vec<String> = self.opts.node.iter().cloned().collect();
        remaining.extend(self.catalog.nodes.iter().filter(|n| *n != url).cloned());
        let mut moves = Vec::new();
        for (name, tablets) in &self.catalog.placement {
            for (i, t) in tablets.iter().enumerate() {
                if t.node == url && !t.is_merged() {
                    let target = &remaining[moves.len() % remaining.len()];
                    moves.push(format!("MOVE SHARD {i} OF {name} TO '{target}'"));
                }
            }
        }
        if !moves.is_empty() {
            return Err(Error::Plan(format!(
                "node {url} holds {} shard(s); move them first: {}",
                moves.len(),
                moves.join("; ")
            )));
        }
        self.catalog.nodes.retain(|n| n != url);
        guard(&self.nodes).remove(url);
        self.persist_catalog()
    }

    /// Run `LOCAL <sql>` on every other holder of a collection, after it
    /// ran here. A holder that did not take it is named, with what to run
    /// there by hand.
    /// The refusal for a propagation that did not reach every node and
    /// whose statement nothing reconciles.
    fn not_propagated(done: &[String], failures: &[String], sql: &str) -> Error {
        Error::Plan(format!(
            "applied here{}, but not on {}. Run `LOCAL {sql}` on those nodes once they are \
             reachable",
            if done.is_empty() { String::new() } else { format!(" and on {}", done.join(", ")) },
            failures.join("; ")
        ))
    }

    // ------------------------------------------------------------- moves

    /// Pin a shard this node holds for a move to `to`: from this call until
    /// the map switches, writes to it are refused naming the move, and the
    /// files as they are at this instant are what the target pulls. Returns
    /// the file list. Idempotent for a move already begun to the same node,
    /// so a target that retries its pull sees the same files.
    pub fn begin_move(
        &mut self,
        collection: &str,
        shard: usize,
        to: &str,
    ) -> Result<Vec<(String, u64)>> {
        let key = (collection.to_string(), shard);
        {
            let moves = self.moves.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(m) = moves.get(&key) {
                if m.to == to {
                    return m.list();
                }
                return Err(Error::Plan(format!(
                    "shard {shard} of `{collection}` is already moving to {}",
                    m.to
                )));
            }
        }
        // A pin asked for past its deadline is not made: the coordinator
        // has given the move up by now, and a pin it never learns of would
        // refuse the shard's writes until someone aborted it.
        if crate::deadline::expired() {
            return Err(Error::Deadline(format!(
                "shard {shard} of `{collection}` was not pinned: the move's deadline had passed \
                 when the pin was asked for"
            )));
        }
        let coll = self.catalog.get(collection)?.clone();
        self.absorb_shard_catalogs(collection)?;
        let ts = self.clock.peek().max(self.last_commit);
        let s = self
            .shards
            .get(collection)
            .and_then(|v| v.iter().find(|s| s.index == shard))
            .ok_or_else(|| {
                Error::Plan(format!("shard {shard} of `{collection}` is not on this node"))
            })?;
        let ex = export_shard(&coll, s, ts, self.opts.build)?;
        let out = Arc::new(MoveOut::from_export(to, ex)?);
        let list = out.list()?;
        self.moves.lock().unwrap_or_else(|p| p.into_inner()).insert(key, out);
        Ok(list)
    }

    /// A shard of `collection` here whose move the target has fenced --
    /// the target holds every file and is about to take the map -- and
    /// where it went: a read of the collection here is refused meanwhile,
    /// since a write landing on the target now would be missing from it.
    /// The window is the switch's carry; before the fence a scan planned
    /// on the old map read the source's copy after the target took a
    /// write, and a key acknowledged was missing from the answer.
    pub fn fenced_shard(&self, collection: &str) -> Option<(usize, String)> {
        let moves = self.moves.lock().unwrap_or_else(|p| p.into_inner());
        moves
            .iter()
            .find(|((c, _), m)| c == collection && m.fenced())
            .map(|((_, i), m)| (*i, m.to.clone()))
    }

    fn refuse_if_fenced(&self, collection: &str) -> Result<()> {
        match self.fenced_shard(collection) {
            Some((i, to)) => Err(Error::Plan(fenced_message(collection, i, &to))),
            None => Ok(()),
        }
    }

    /// Let go of a pinned move: the shard takes writes again and its files
    /// are its own. What a coordinator does when the pull failed.
    pub fn abort_move(&mut self, collection: &str, shard: usize) {
        self.moves
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(collection.to_string(), shard));
    }

    /// `MOVE SHARD i OF c TO 'node'`, from wherever it was issued: the
    /// source pins the shard, the target pulls its files and adopts it, and
    /// then every holder is told the new map -- the target first, the source
    /// last, so the source's copy goes only once everyone else can find the
    /// new one. A holder the map did not reach is named with the `LOCAL
    /// PLACE SHARD` that repairs it.
    /// `MOVE SHARD`: under the caller's lock, the checks and the pin on the
    /// source; the copy as deferred work holding nothing (the target pulls
    /// the pinned files and, done, switches the map everywhere itself); so
    /// neither the coordinator's lock nor the target's is held across the
    /// copy. The first shape held both, and a write forwarded through the
    /// target to a shard on the coordinator closed a cycle that only the
    /// deadline broke.
    pub fn move_shard(&mut self, collection: &str, shard: usize, to: &str) -> Result<Outcome> {
        let plan = self.move_begin(collection, shard, to)?;
        Ok(Outcome::Deferred(Deferred::new(move || Db::move_run(plan))))
    }

    /// The checks and the pin: what a move does under the lock.
    fn move_begin(&mut self, collection: &str, shard: usize, to: &str) -> Result<MovePlan> {
        let _deadline = self.arm_default_deadline();
        crate::wire::parse_url(to)?;
        let me = self.opts.node.clone().ok_or_else(|| {
            Error::Plan("this node has no address; a move needs CELASTRO_NODE".into())
        })?;
        let coll = self.catalog.get(collection)?.clone();
        let tablets = self.catalog.placement.get(collection).cloned().ok_or_else(|| {
            Error::Plan(format!("collection `{collection}` has no placement map"))
        })?;
        let Some(t) = tablets.get(shard) else {
            return Err(Error::Plan(format!(
                "`{collection}` has {} shard(s); there is no shard {shard}",
                tablets.len()
            )));
        };
        if t.is_merged() {
            return Err(Error::Plan(format!(
                "shard {shard} of `{collection}` was merged away and owns no key"
            )));
        }
        let from = if t.node.is_empty() { me.clone() } else { t.node.clone() };
        if !self.is_self(to) && !self.catalog.nodes.iter().any(|n| n == to) {
            return Err(Error::Plan(format!(
                "node {to} is not attached; ATTACH NODE '{to}' first"
            )));
        }
        if from == to || (self.is_self(&from) && self.is_self(to)) {
            return Err(Error::Plan(format!("shard {shard} of `{collection}` is already on {to}")));
        }
        if self.is_coordinator(to) {
            return Err(Error::Plan(format!(
                "node {to} is a coordinator and holds no shards; move shard {shard} to a data node"
            )));
        }
        if self.dir.is_none() {
            return Err(Error::Plan("a move needs a persistent database (--dir)".into()));
        }
        let mut new = tablets.clone();
        new[shard].node = to.to_string();
        // The pin: here, now, when this node is the source; on the source
        // as deferred work otherwise. Asked for under this lock it closed a
        // cycle -- the source waiting for this node's lock to answer a
        // scan it was serving, this node waiting for the source's to pin
        // -- that only the deadline broke.
        let files =
            if self.is_self(&from) { Some(self.begin_move(collection, shard, to)?) } else { None };
        // The target and the source as wire peers, this node included: the
        // job holds no lock and reaches this node the way any other does.
        // Connections of their own, not the pool's: a pool connection is
        // one call at a time, and a pull that takes the copy's length on
        // it made a write forwarded to the same peer wait for it under this
        // node's lock, which the target's switch back here then waited on.
        let target = Arc::new(self.wire_node(to)?);
        let source = Arc::new(self.wire_node(&from)?);
        Ok(MovePlan {
            collection: collection.to_string(),
            shard,
            from,
            to: to.to_string(),
            coll,
            new,
            files,
            source,
            target,
            deadline_ms: crate::deadline::remaining_ms(),
        })
    }

    /// The copy, holding nothing: the target pulls, adopts, and switches
    /// the map on every node; a pull that fails releases the pin.
    fn move_run(mut plan: MovePlan) -> Result<Outcome> {
        let files = match plan.files.take() {
            Some(files) => files,
            // The pin on a source elsewhere, within the statement's budget:
            // a source that does not answer is a move that did not begin.
            None => {
                let _deadline = crate::deadline::arm(plan.deadline_ms);
                match plan.source.begin_move(&plan.collection, plan.shard, &plan.to) {
                    Ok(files) => files,
                    Err(e) => {
                        Db::move_abort(&plan);
                        return Err(e);
                    }
                }
            }
        };
        // A copy takes what it takes.
        let _no_deadline = crate::deadline::arm(None);
        match plan.target.pull_shard(&plan.coll, &plan.new, plan.shard, &plan.from) {
            Ok(switched) => Ok(Outcome::Ack(format!(
                "shard {} of `{}` moved from {} to {}; {} file(s), {switched}",
                plan.shard,
                plan.collection,
                plan.from,
                plan.to,
                files.len()
            ))),
            Err(e) => {
                Db::move_abort(&plan);
                Err(e)
            }
        }
    }

    /// Let the source go of the pin, briefly: a source that does not answer
    /// the abort is not waited for.
    fn move_abort(plan: &MovePlan) {
        let _deadline = crate::deadline::arm(Some(5_000));
        let _ = plan.source.abort_move(&plan.collection, plan.shard);
    }

    /// The target's half of a move, on this node: pull every file of the
    /// pinned shard from `source` (a node, or this process when the source
    /// is this node too) into a directory beside the shard's, then adopt it.
    /// The target's copy, holding no lock: every pinned file of the shard
    /// from `source` into a directory beside the shard's, then synced.
    /// Returns that directory, for [`Db::finish_move_here`].
    pub fn pull_files(
        dir: &Path,
        collection: &str,
        shard: usize,
        source: &Arc<crate::wire::Node>,
        files: &[(String, u64)],
    ) -> Result<PathBuf> {
        let cdir = dir.join("collections").join(collection);
        let incoming = cdir.join(format!("shard-{shard:04}.incoming"));
        let _ = fs::remove_dir_all(&incoming);
        fs::create_dir_all(incoming.join("segments"))?;
        fs::create_dir_all(incoming.join("deletes"))?;
        fs::create_dir_all(incoming.join("archive"))?;
        for (name, len) in files {
            let mut bytes = Vec::with_capacity(*len as usize);
            let mut off = 0u64;
            while off < *len {
                let want = (*len - off).min(MOVE_CHUNK);
                let chunk = source.read_file(collection, shard, name, off, want)?;
                if chunk.is_empty() {
                    return Err(Error::Storage(format!(
                        "shard {shard} of `{collection}`: `{name}` ended at {off} of {len} bytes"
                    )));
                }
                off += chunk.len() as u64;
                bytes.extend_from_slice(&chunk);
            }
            crate::shard::atomic_write(&incoming.join(name), &bytes)?;
        }
        crate::shard::sync_dir(&incoming)?;
        Ok(incoming)
    }

    /// The target's end of a move under its lock: adopt the pulled shard
    /// and switch the map here. Returns the switch to carry to every other
    /// node -- the statement and the peers, the source last so its pin is
    /// released once everyone else routes to the new holder -- for
    /// [`Db::carry_switch`], which holds no lock: a switch carried under
    /// this lock waited on the source's, held by a write forwarded into
    /// this node, until the deadline.
    pub fn finish_move_here(
        &mut self,
        coll: &Collection,
        tablets: &[Tablet],
        shard: usize,
        incoming: &Path,
        from: &str,
    ) -> Result<Switch> {
        let _deadline = self.arm_default_deadline();
        let me = self.opts.node.clone().unwrap_or_default();
        let old = self.catalog.placement.get(&coll.name).cloned().unwrap_or_default();
        // Only this shard's entry from the plan: the plan's map is as of its
        // pin, and a rebalance pins several moves before any copies, so a
        // later move's map still names an earlier move's old holder.
        let mut map = if old.len() == tablets.len() { old.clone() } else { tablets.to_vec() };
        map[shard] = tablets[shard].clone();
        self.adopt_shard(coll, &map, shard, incoming)?;
        self.place_shard(&coll.name, shard, &me)?;
        let mut order: Vec<String> = Vec::new();
        // Every node this one knows: the holders old and new, the nodes
        // and coordinators the catalog names, and the peers that attached
        // -- a node that coordinates moves and holds no shard of the
        // collection kept a stale map, and issued the next move to the old
        // holder, until the peers that attached were counted.
        let others: Vec<String> = old
            .iter()
            .chain(tablets.iter())
            .map(|t| t.node.clone())
            .chain(self.catalog.nodes.iter().cloned())
            .chain(self.catalog.coordinators.iter().cloned())
            .chain(self.attached.iter().cloned())
            .collect();
        for n in others {
            if !n.is_empty() && !self.is_self(&n) && n != from && !order.contains(&n) {
                order.push(n);
            }
        }
        if !self.is_self(from) {
            order.push(from.to_string());
        }
        let sql = format!("PLACE SHARD {shard} OF {} ON '{me}'", coll.name);
        let mut peers = Vec::new();
        for n in order {
            peers.push((n.clone(), self.node_conn(&n)?));
        }
        Ok(Switch { sql, peers })
    }

    /// Carry a map switch to the peers, holding nothing. What was switched
    /// where, and where it was not.
    pub fn carry_switch(switch: &Switch) -> String {
        let local = format!("LOCAL {}", switch.sql);
        let mut done = Vec::new();
        let mut failures = Vec::new();
        for (url, node) in &switch.peers {
            match node.statement(&local, &[]) {
                Ok(_) => done.push(url.clone()),
                Err(e) => failures.push(format!("{url}: {e}")),
            }
        }
        format!(
            "map switched here{}{}",
            if done.is_empty() { String::new() } else { format!(" and on {}", done.join(", ")) },
            if failures.is_empty() {
                String::new()
            } else {
                format!(
                    "; not on {}: they learn the map when they reconnect ({RECONCILE_NOTE})",
                    failures.join("; ")
                )
            }
        )
    }

    /// Take a pulled shard directory into this node: it becomes
    /// `shard-<i>`, the collection is known here (adopted with the new map
    /// if it was not), the shard is opened and the map recorded.
    fn adopt_shard(
        &mut self,
        coll: &Collection,
        tablets: &[Tablet],
        shard: usize,
        incoming: &Path,
    ) -> Result<()> {
        let dir = self
            .dir
            .clone()
            .ok_or_else(|| Error::Plan("a move needs a persistent database (--dir)".into()))?;
        let name = coll.name.clone();
        let cdir = dir.join("collections").join(&name);
        let sdir = cdir.join(format!("shard-{shard:04}"));
        if self.shards.get(&name).is_some_and(|v| v.iter().any(|s| s.index == shard)) {
            return Err(Error::Plan(format!("shard {shard} of `{name}` is already on this node")));
        }
        if sdir.exists() {
            fs::remove_dir_all(&sdir)?;
        }
        fs::rename(incoming, &sdir)?;
        crate::shard::sync_dir(&cdir)?;
        crate::shard::sync_dir(&dir.join("collections"))?;
        if self.catalog.get(&name).is_err() {
            let mut def = coll.clone();
            def.doc_count = 0;
            def.paths.clear();
            self.catalog.create(def)?;
        }
        self.catalog.placement.insert(name.clone(), tablets.to_vec());
        let def = self.catalog.get(&name)?.clone();
        let (lo, hi) = read_range(&self.cipher, &sdir, shard, &name)?;
        let mut sh = Shard::open(def, self.clock.clone(), self.shard_opts(), &sdir)?;
        sh.set_key_range(lo, hi);
        sh.index = shard;
        let v = self.shards.entry(name.clone()).or_default();
        v.push(sh);
        v.sort_by_key(|s| s.index);
        self.absorb_shard_catalogs(&name)?;
        self.persist_catalog()
    }

    /// `SPLIT SHARD i OF c AT 'key'`: shard `i`, `[lo, hi)`, keeps `[lo,
    /// key)` and a new shard, the next index, holds `[key, hi)` on the same
    /// node; `MOVE SHARD` then spreads it. The holder makes the split: the
    /// pinned files of shard `i` -- the same export a move takes -- become
    /// the new shard's directory (linked when the directory is in the
    /// clear, re-sealed under the new name when it is encrypted), each
    /// shard's range is what makes it answer only its own keys, and the
    /// rows outside a range stay on disk, invisible, until a compaction
    /// drops them; so a split moves no row and takes what a hard link
    /// takes. Issued elsewhere, the statement goes to the holder as
    /// `LOCAL SPLIT SHARD`, and the holder carries the new map to every
    /// peer the same way, which is what `LOCAL` means here: the holder's
    /// word, the map alone.
    pub fn split_shard(
        &mut self,
        collection: &str,
        shard: usize,
        at: Option<&str>,
        local: bool,
    ) -> Result<Outcome> {
        let _deadline = self.arm_default_deadline();
        if at.is_some_and(|k| k.is_empty() || k.contains('\n')) {
            return Err(Error::Schema(
                "a split key must be non-empty and must not contain a line break".into(),
            ));
        }
        let coll = self.catalog.get(collection)?.clone();
        let tablets = self.catalog.placement.get(collection).cloned().ok_or_else(|| {
            Error::Plan(format!("collection `{collection}` has no placement map"))
        })?;
        let Some(t) = tablets.get(shard).cloned() else {
            return Err(Error::Plan(format!(
                "`{collection}` has {} shard(s); there is no shard {shard}",
                tablets.len()
            )));
        };
        if t.is_merged() {
            return Err(Error::Plan(format!(
                "shard {shard} of `{collection}` was merged away and owns no key"
            )));
        }
        let holder = t.node.clone();
        let next = tablets.len();
        let hi_text = t.hi.clone().unwrap_or_default();
        if !self.is_self(&holder) {
            let Some(at) = at else {
                if local {
                    return Err(Error::Plan(format!(
                        "shard {shard} of `{collection}` is on {holder}, which picks the key"
                    )));
                }
                // The holder picks the median, and tells everyone.
                let conn = self.node_conn(&holder)?;
                let sql = format!("LOCAL SPLIT SHARD {shard} OF {collection}");
                let remaining = crate::deadline::remaining_ms();
                return Ok(Outcome::Deferred(Deferred::new(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    Ok(Outcome::Ack(conn.statement(&sql, &[])?))
                })));
            };
            Self::inside_range(collection, shard, &t, at)?;
            if local {
                let new = Self::split_map(&tablets, shard, at);
                self.catalog.placement.insert(collection.to_string(), new);
                self.persist_catalog()?;
                return Ok(Outcome::Ack(format!(
                    "shard {shard} of `{collection}` split at '{at}' on {holder}: shard {next} is \
                     [{at}, {hi_text}) there"
                )));
            }
            let conn = self.node_conn(&holder)?;
            let sql = format!(
                "LOCAL SPLIT SHARD {shard} OF {collection} AT '{}'",
                at.replace('\'', "''")
            );
            let remaining = crate::deadline::remaining_ms();
            return Ok(Outcome::Deferred(Deferred::new(move || {
                let _deadline = crate::deadline::arm(remaining);
                Ok(Outcome::Ack(conn.statement(&sql, &[])?))
            })));
        }
        // Held here: the key given, or the middle of the shard's keys.
        let at: String = match at {
            Some(k) => k.to_string(),
            None => {
                let ts = self.clock.peek().max(self.last_commit);
                self.shards
                    .get(collection)
                    .and_then(|v| v.iter().find(|s| s.index == shard))
                    .ok_or_else(|| {
                        Error::Plan(format!("shard {shard} of `{collection}` is not on this node"))
                    })?
                    .median_key(ts)
                    .ok_or_else(|| {
                        Error::Plan(format!(
                            "shard {shard} of `{collection}` holds fewer than two keys; nothing \
                             to split at"
                        ))
                    })?
            }
        };
        let at = at.as_str();
        Self::inside_range(collection, shard, &t, at)?;
        let new = Self::split_map(&tablets, shard, at);
        let quoted = at.replace('\'', "''");
        let dir = self
            .dir
            .clone()
            .ok_or_else(|| Error::Plan("a split needs a persistent database (--dir)".into()))?;
        if self.shards.get(collection).is_some_and(|v| v.iter().any(|s| s.index == next)) {
            return Err(Error::Plan(format!(
                "shard {next} of `{collection}` is already on this node"
            )));
        }
        self.absorb_shard_catalogs(collection)?;
        let ts = self.clock.peek().max(self.last_commit);
        let cdir = dir.join("collections").join(collection);
        let incoming = cdir.join(format!("shard-{next:04}.incoming"));
        let new_id = format!("shard-{next:04}");
        let files = {
            let s = self
                .shards
                .get(collection)
                .and_then(|v| v.iter().find(|s| s.index == shard))
                .ok_or_else(|| {
                Error::Plan(format!("shard {shard} of `{collection}` is not on this node"))
            })?;
            let ex = export_shard(&coll, s, ts, self.opts.build)?;
            let out = MoveOut::from_export(&holder, ex)?;
            let _ = fs::remove_dir_all(&incoming);
            fs::create_dir_all(incoming.join("segments"))?;
            fs::create_dir_all(incoming.join("deletes"))?;
            fs::create_dir_all(incoming.join("archive"))?;
            let mut n = 0;
            for (name, f) in out.files() {
                if name == "RANGE" {
                    continue;
                }
                let base = name.rsplit('/').next().unwrap_or(name);
                let dest = incoming.join(name);
                match (f, &self.cipher) {
                    // Immutable bytes as they lie: one more name for them.
                    (MoveFile::Path(p), None) => {
                        if fs::hard_link(p, &dest).is_err() {
                            fs::copy(p, &dest)?;
                        }
                    }
                    (MoveFile::Path(p), Some(_)) => {
                        let plain = crate::shard::read_content(&self.cipher, &s.file_id(base), p)?
                            .ok_or_else(|| {
                                Error::Storage(format!("{}: gone under the split", p.display()))
                            })?;
                        crate::shard::write_content(
                            &self.cipher,
                            &format!("{new_id}/{base}"),
                            &dest,
                            &plain,
                        )?;
                    }
                    (MoveFile::Bytes(b), None) => crate::shard::atomic_write(&dest, b)?,
                    (MoveFile::Bytes(b), Some(c)) => {
                        let plain = c.open_file(&s.file_id(base), b)?;
                        crate::shard::write_content(
                            &self.cipher,
                            &format!("{new_id}/{base}"),
                            &dest,
                            &plain,
                        )?;
                    }
                }
                n += 1;
            }
            n
        };
        crate::shard::write_content(
            &self.cipher,
            &format!("{new_id}/RANGE"),
            &incoming.join("RANGE"),
            format!("{at}\n{hi_text}").as_bytes(),
        )?;
        crate::shard::sync_dir(&incoming)?;
        self.adopt_shard(&coll, &new, next, &incoming)?;
        if let Some(sh) =
            self.shards.get_mut(collection).and_then(|v| v.iter_mut().find(|s| s.index == shard))
        {
            sh.set_key_range(t.lo.clone(), Some(at.to_string()));
        }
        crate::shard::write_content(
            &self.cipher,
            &format!("shard-{shard:04}/RANGE"),
            &cdir.join(format!("shard-{shard:04}")).join("RANGE"),
            format!("{}\n{at}", t.lo.clone().unwrap_or_default()).as_bytes(),
        )?;
        let peers = self.every_peer(&new)?;
        let switch =
            Switch { sql: format!("SPLIT SHARD {shard} OF {collection} AT '{quoted}'"), peers };
        let ack = format!(
            "shard {shard} of `{collection}` split at '{at}': shard {next} is [{at}, {hi_text}) on \
             this node, {files} file(s)"
        );
        Ok(Outcome::Deferred(Deferred::new(move || {
            let switched = Db::carry_switch(&switch);
            Ok(Outcome::Ack(format!("{ack}; {switched}")))
        })))
    }

    /// Whether `at` is strictly inside shard `shard`'s range.
    fn inside_range(collection: &str, shard: usize, t: &Tablet, at: &str) -> Result<()> {
        let inside = t.lo.as_deref().map_or(true, |lo| at > lo)
            && t.hi.as_deref().map_or(true, |hi| at < hi);
        if inside {
            Ok(())
        } else {
            Err(Error::Plan(format!(
                "shard {shard} of `{collection}` holds [{}, {}); a split key must be strictly \
                 inside that range",
                t.lo.clone().unwrap_or_default(),
                t.hi.clone().unwrap_or_default()
            )))
        }
    }

    /// The map after a split of `shard` at `at`: the shard's high bound
    /// lowered, and the new shard appended on the same node.
    fn split_map(tablets: &[Tablet], shard: usize, at: &str) -> Vec<Tablet> {
        let mut new = tablets.to_vec();
        let hi = new[shard].hi.take();
        new[shard].hi = Some(at.to_string());
        new.push(Tablet {
            node: new[shard].node.clone(),
            lo: Some(at.to_string()),
            hi,
            term: new[shard].term,
            followers: new[shard].followers.clone(),
        });
        new
    }

    /// `MERGE SHARDS a AND b OF c`: two adjacent shards on one node become
    /// one. Shard `b`'s rows are rebuilt into shard `a` as segments of its
    /// own (the memtable sealed first, then every live row of every
    /// segment, versions layered and deletes carried as a compaction
    /// carries them), shard `a`'s range becomes the union, and shard
    /// `b`'s directory goes; its entry stays in the map as a merged marker
    /// with an empty range, so no shard renumbers and nothing routes to
    /// it. A merge is row work -- `b`'s rows through the segment builder,
    /// under the lock, with those rows in memory meanwhile -- so name the
    /// larger shard first. Two shards on different nodes are refused with
    /// the move that brings them together.
    pub fn merge_shards(
        &mut self,
        collection: &str,
        a: usize,
        b: usize,
        local: bool,
    ) -> Result<Outcome> {
        let _deadline = self.arm_default_deadline();
        if a == b {
            return Err(Error::Plan("a merge takes two different shards".into()));
        }
        self.catalog.get(collection)?;
        let tablets = self.catalog.placement.get(collection).cloned().ok_or_else(|| {
            Error::Plan(format!("collection `{collection}` has no placement map"))
        })?;
        for i in [a, b] {
            let Some(t) = tablets.get(i) else {
                return Err(Error::Plan(format!(
                    "`{collection}` has {} shard(s); there is no shard {i}",
                    tablets.len()
                )));
            };
            if t.is_merged() {
                return Err(Error::Plan(format!(
                    "shard {i} of `{collection}` was merged away and owns no key"
                )));
            }
        }
        let (ta, tb) = (tablets[a].clone(), tablets[b].clone());
        let (lo, hi) = if ta.hi.is_some() && ta.hi == tb.lo {
            (ta.lo.clone(), tb.hi.clone())
        } else if tb.hi.is_some() && tb.hi == ta.lo {
            (tb.lo.clone(), ta.hi.clone())
        } else {
            return Err(Error::Plan(format!(
                "shards {a} and {b} of `{collection}` are not adjacent: [{}, {}) and [{}, {})",
                ta.lo.clone().unwrap_or_default(),
                ta.hi.clone().unwrap_or_default(),
                tb.lo.clone().unwrap_or_default(),
                tb.hi.clone().unwrap_or_default()
            )));
        };
        if ta.node != tb.node {
            return Err(Error::Plan(format!(
                "shards {a} and {b} of `{collection}` are on different nodes ({} and {}); bring \
                 them together first: MOVE SHARD {b} OF {collection} TO '{}'",
                ta.node, tb.node, ta.node
            )));
        }
        let holder = ta.node.clone();
        let mut new = tablets.clone();
        new[a] = Tablet {
            node: holder.clone(),
            lo: lo.clone(),
            hi: hi.clone(),
            term: ta.term,
            followers: ta.followers.clone(),
        };
        let mark = Some(tb.lo.clone().unwrap_or_default());
        new[b] = Tablet { node: holder.clone(), lo: mark.clone(), hi: mark, ..Default::default() };
        let (lo_text, hi_text) = (lo.clone().unwrap_or_default(), hi.clone().unwrap_or_default());
        if !self.is_self(&holder) {
            if local {
                self.catalog.placement.insert(collection.to_string(), new);
                self.persist_catalog()?;
                return Ok(Outcome::Ack(format!(
                    "shards {a} and {b} of `{collection}` merged on {holder}: shard {a} is \
                     [{lo_text}, {hi_text}) there, shard {b} owns no key"
                )));
            }
            let conn = self.node_conn(&holder)?;
            let sql = format!("LOCAL MERGE SHARDS {a} AND {b} OF {collection}");
            let remaining = crate::deadline::remaining_ms();
            return Ok(Outcome::Deferred(Deferred::new(move || {
                let _deadline = crate::deadline::arm(remaining);
                Ok(Outcome::Ack(conn.statement(&sql, &[])?))
            })));
        }
        let dir = self
            .dir
            .clone()
            .ok_or_else(|| Error::Plan("a merge needs a persistent database (--dir)".into()))?;
        for i in [a, b] {
            if !self.shards.get(collection).is_some_and(|v| v.iter().any(|s| s.index == i)) {
                return Err(Error::Plan(format!(
                    "shard {i} of `{collection}` is not on this node"
                )));
            }
        }
        self.absorb_shard_catalogs(collection)?;
        let now = self.clock.peek().max(self.last_commit);
        let copts = self.opts.compaction;
        let shards = self.shards.get_mut(collection).expect("checked above");
        // Every row of `b`, sealed first so one path -- the segments' --
        // carries every version and delete the way a compaction reads them.
        let (docs, carried) = {
            let g = shards.iter_mut().find(|s| s.index == b).expect("checked above");
            g.flush()?;
            let ids: Vec<u64> = g.segments.iter().map(|h| h.id()).collect();
            let retain = g.retain_from(now);
            crate::shard::collect_from_handles(&g.segments, &ids, retain)?
        };
        let rows = docs.len();
        {
            let k = shards.iter_mut().find(|s| s.index == a).expect("checked above");
            // What a split left in `a` outside its range is dropped for
            // good before the range widens over it again: sealed, then
            // every masked segment rewritten, as a compaction would have.
            k.flush()?;
            let masked: Vec<u64> =
                k.segments.iter().filter(|h| h.mask().is_some()).map(|h| h.id()).collect();
            for id in masked {
                let job =
                    compaction::Job::Rewrite { input: id, reason: compaction::Reason::DeadRatio };
                compaction::run(k, &job, &copts)?;
            }
            compaction::absorb(k, docs, &carried, &copts)?;
            k.set_key_range(lo.clone(), hi.clone());
            // The shipper is replaced (`refresh_shippers`, at the persist
            // below): a fresh one asks every follower where it stands, and
            // the absorbed rows -- with timestamps older than any
            // follower's stand -- reach them through the catch-up from
            // nothing the raised floor makes of it. Kept, it stayed live
            // and shipped nothing of the merge; the follower promoted after
            // one answered two rows of three.
            if let Some(sh) = k.shipper.take() {
                sh.stop();
            }
        }
        shards.retain(|s| s.index != b);
        let cdir = dir.join("collections").join(collection);
        crate::shard::write_content(
            &self.cipher,
            &format!("shard-{a:04}/RANGE"),
            &cdir.join(format!("shard-{a:04}")).join("RANGE"),
            format!("{lo_text}\n{hi_text}").as_bytes(),
        )?;
        let _ = fs::remove_dir_all(cdir.join(format!("shard-{b:04}")));
        self.catalog.placement.insert(collection.to_string(), new.clone());
        self.persist_catalog()?;
        let peers = self.every_peer(&new)?;
        let switch = Switch { sql: format!("MERGE SHARDS {a} AND {b} OF {collection}"), peers };
        let ack = format!(
            "shards {a} and {b} of `{collection}` merged: shard {a} is [{lo_text}, {hi_text}) on \
             this node, {rows} row(s) of shard {b} rebuilt into it, shard {b} owns no key"
        );
        Ok(Outcome::Deferred(Deferred::new(move || {
            let switched = Db::carry_switch(&switch);
            Ok(Outcome::Ack(format!("{ack}; {switched}")))
        })))
    }

    /// `ALTER COLLECTION c SET (replicas = n)`: the followers of every shard
    /// re-planned to `n - 1` after its holder in the data nodes' order;
    /// what was.
    fn set_replicas(&mut self, collection: &str, n: usize) -> Result<usize> {
        let coll = self.catalog.get(collection)?.clone();
        let was = if coll.replicas == 0 { DEFAULT_REPLICAS } else { coll.replicas as usize };
        let nodes = self.data_nodes();
        let mut tablets = self.catalog.placement.get(collection).cloned().unwrap_or_default();
        for t in tablets.iter_mut() {
            if t.is_merged() {
                continue;
            }
            let holder_at = nodes
                .iter()
                .position(|x| x == &t.node || (self.is_self(x) && self.is_self(&t.node)));
            t.followers = match holder_at {
                Some(h) => Self::followers_for(&nodes, h, n),
                None => Vec::new(),
            };
        }
        self.catalog.placement.insert(collection.to_string(), tablets);
        if let Some(c) = self.catalog.collections.get_mut(collection) {
            c.replicas = n as u8;
        }
        self.ensure_followed(collection)?;
        self.persist_catalog()?;
        Ok(was)
    }

    /// What the last write statement's acknowledgement waits for: each
    /// shard it wrote here, at the instant it wrote, on that shard's
    /// followers. Empty when nothing here has followers. Takes the writes
    /// noted so far.
    pub fn confirmation(&mut self) -> Confirmation {
        let budget = crate::deadline::remaining_ms();
        let mut waits: Vec<(Arc<crate::replication::Shipper>, Timestamp)> = Vec::new();
        for (c, idx, ts) in self.recent_writes.drain(..) {
            if let Some(sh) = self
                .shards
                .get(&c)
                .and_then(|v| v.iter().find(|s| s.index == idx))
                .and_then(|s| s.shipper.clone())
            {
                match waits.iter().position(|(s, _)| Arc::ptr_eq(s, &sh)) {
                    Some(p) => waits[p].1 = waits[p].1.max(ts),
                    None => waits.push((sh, ts)),
                }
            }
        }
        Confirmation { waits, budget }
    }

    /// The instant of the last commit here.
    pub fn last_commit_ts(&self) -> Timestamp {
        self.last_commit
    }

    /// The shippers of every held shard, as the map says: one per held
    /// shard with followers, replaced when its followers or term change,
    /// stopped when it has none.
    fn refresh_shippers(&mut self) {
        let sync = self.opts.replication_sync;
        let placement = self.catalog.placement.clone();
        let mut fresh: Vec<(String, usize, Arc<crate::replication::Shipper>)> = Vec::new();
        for (name, tablets) in &placement {
            let Some(shards) = self.shards.get(name) else { continue };
            for s in shards {
                let Some(t) = tablets.get(s.index) else { continue };
                let followers: Vec<String> =
                    t.followers.iter().filter(|f| !self.is_self(f)).cloned().collect();
                let same = s
                    .shipper
                    .as_ref()
                    .is_some_and(|sh| sh.followers() == followers && sh.term == t.term)
                    || (followers.is_empty() && s.shipper.is_none());
                if same {
                    continue;
                }
                if followers.is_empty() {
                    fresh.push((
                        name.clone(),
                        s.index,
                        Arc::new(crate::replication::Shipper::idle()),
                    ));
                    continue;
                }
                let mut conns = Vec::new();
                for f in &followers {
                    match self.wire_node(f) {
                        Ok(n) => conns.push((f.clone(), Arc::new(n))),
                        Err(e) => crate::log::warn(
                            "follower_not_dialled",
                            &[("node", f.clone()), ("error", e.to_string())],
                        ),
                    }
                }
                fresh.push((
                    name.clone(),
                    s.index,
                    crate::replication::Shipper::new(
                        name,
                        s.index,
                        t.term,
                        conns,
                        sync,
                        s.dir().map(|d| d.to_path_buf()),
                    ),
                ));
            }
        }
        for (name, idx, sh) in fresh {
            if let Some(s) =
                self.shards.get_mut(&name).and_then(|v| v.iter_mut().find(|s| s.index == idx))
            {
                if let Some(old) = s.shipper.take() {
                    old.stop();
                }
                s.shipper = if sh.followers().is_empty() { None } else { Some(sh) };
            }
        }
    }

    /// The catch-ups due: for every follower a shipper is waiting to catch
    /// up, the next chunk of what it lacks, cut from the held shard. What
    /// the console's maintenance thread and the wire's driver run; how
    /// many chunks were cut. A read: it cuts from the shard's sealed files
    /// and memtable and hands the chunk to the shipper, which keeps its
    /// own state. Under the exclusive lock, as it first was, a node under
    /// sustained reads never cut a chunk -- the try for the lock found a
    /// reader every time -- so its followers never caught up, every write
    /// waited for their confirmation until the deadline, and the
    /// statements behind those writes with it.
    pub fn replication_step(&self) -> usize {
        let now = self.clock.peek().max(self.last_commit);
        let mut cut = 0;
        let mut jobs: Vec<(
            String,
            usize,
            Arc<crate::replication::Shipper>,
            String,
            crate::replication::FollowerState,
        )> = Vec::new();
        for (name, shards) in &self.shards {
            for s in shards {
                if let Some(sh) = &s.shipper {
                    for (url, _) in sh.catchups_due() {
                        if let Some(state) = sh.fix_upto(&url, now) {
                            jobs.push((name.clone(), s.index, sh.clone(), url, state));
                        }
                    }
                }
            }
        }
        for (name, idx, sh, url, state) in jobs {
            let crate::replication::FollowerState::CatchingUp { from, upto, cursor, reset, .. } =
                state
            else {
                continue;
            };
            let Some(s) = self.shards.get(&name).and_then(|v| v.iter().find(|s| s.index == idx))
            else {
                continue;
            };
            let mut items = Vec::new();
            // A follower that stood before the newest delete a compaction
            // here has forgotten may have missed it: from nothing. (Off the
            // version floor, which every seal raises, this reset every
            // follower that had been away across a seal.)
            let reset = reset || from < s.catchup_floor;
            if cursor.is_none() {
                if reset {
                    items.push(crate::replication::ShipItem {
                        kind: crate::replication::SHIP_RESET,
                        key: String::new(),
                        ts: 0,
                        doc: None,
                    });
                } else {
                    items.extend(s.deletes_since(from));
                }
            }
            let from = if reset { 0 } else { from };
            match s.changes_since(from, cursor.as_deref(), upto, CATCHUP_CHUNK) {
                Ok((rows, next)) => {
                    items.extend(rows);
                    let done = next.is_none();
                    if done {
                        items.push(crate::replication::ShipItem {
                            kind: crate::replication::SHIP_CAUGHT_UP,
                            key: String::new(),
                            ts: upto,
                            doc: None,
                        });
                    }
                    sh.push_catchup(&url, items, next, done);
                    cut += 1;
                }
                Err(e) => crate::log::warn(
                    "catchup_not_cut",
                    &[
                        ("collection", name.clone()),
                        ("shard", idx.to_string()),
                        ("error", e.to_string()),
                    ],
                ),
            }
        }
        cut
    }

    /// The followed copies, shared with the wire.
    pub fn followed(&self) -> Followed {
        self.followed.clone()
    }

    /// `PROMOTE SHARD i OF c ON 'node'`: the follower named becomes the
    /// holder at the next term, and the holder it replaces a follower. Made
    /// on the node promoted -- the statement goes there from wherever it
    /// was issued -- which then carries the map at the new term to every
    /// peer: a peer takes the higher term, and the old holder, when it
    /// hears (now, or from a catalog at the next sweep), demotes its copy
    /// to a follower's and is caught up from the new holder, which drops
    /// whatever it took that was never confirmed. Writes the old holder
    /// takes meanwhile are never acknowledged, since its follower -- the
    /// node promoted -- answers its log with the new term.
    pub fn promote_shard(
        &mut self,
        collection: &str,
        shard: usize,
        node: &str,
        term: Option<u64>,
        local: bool,
    ) -> Result<Outcome> {
        let _deadline = self.arm_default_deadline();
        crate::wire::parse_url(node)?;
        self.catalog.get(collection)?;
        let tablets = self.catalog.placement.get(collection).cloned().ok_or_else(|| {
            Error::Plan(format!("collection `{collection}` has no placement map"))
        })?;
        let Some(t) = tablets.get(shard).cloned() else {
            return Err(Error::Plan(format!(
                "`{collection}` has {} shard(s); there is no shard {shard}",
                tablets.len()
            )));
        };
        if t.is_merged() {
            return Err(Error::Plan(format!(
                "shard {shard} of `{collection}` was merged away and owns no key"
            )));
        }
        if !local {
            if t.node == node || (self.is_self(&t.node) && self.is_self(node)) {
                return Err(Error::Plan(format!(
                    "shard {shard} of `{collection}` is already held by {node}"
                )));
            }
            if !t.followed_by(node) {
                return Err(Error::Plan(format!(
                    "{node} does not follow shard {shard} of `{collection}` (its followers: {}); \
                     only a follower can be promoted",
                    if t.followers.is_empty() {
                        "none".to_string()
                    } else {
                        t.followers.join(", ")
                    }
                )));
            }
        }
        let new_term = match term {
            Some(n) => n,
            None => t.term + 1,
        };
        if local && new_term <= t.term {
            return Ok(Outcome::Ack(format!(
                "shard {shard} of `{collection}` is at term {} already",
                t.term
            )));
        }
        if !self.is_self(node) {
            if local {
                // The promoted node's word: the map, and this node's copy
                // demoted if it held the shard.
                let held =
                    self.shards.get(collection).is_some_and(|v| v.iter().any(|s| s.index == shard));
                let mut new = tablets.clone();
                new[shard] = Self::promoted_tablet(&t, node, new_term);
                self.catalog.placement.insert(collection.to_string(), new);
                if held {
                    self.demote_here(collection, shard)?;
                }
                self.persist_catalog()?;
                return Ok(Outcome::Ack(format!(
                    "shard {shard} of `{collection}` is held by {node} at term {new_term}{}",
                    if held { "; this node's copy follows it" } else { "" }
                )));
            }
            let conn = self.node_conn(node)?;
            let sql =
                format!("LOCAL PROMOTE SHARD {shard} OF {collection} ON '{node}' TERM {new_term}");
            let remaining = crate::deadline::remaining_ms();
            return Ok(Outcome::Deferred(Deferred::new(move || {
                let _deadline = crate::deadline::arm(remaining);
                Ok(Outcome::Ack(conn.statement(&sql, &[])?))
            })));
        }
        // This node is promoted: its copy becomes the shard, then everyone
        // hears.
        self.promote_here(collection, shard, &t, new_term)?;
        let new = self.catalog.placement.get(collection).cloned().unwrap_or_default();
        let peers = self.every_peer(&new)?;
        let switch = Switch {
            sql: format!("PROMOTE SHARD {shard} OF {collection} ON '{node}' TERM {new_term}"),
            peers,
        };
        let ack = format!(
            "shard {shard} of `{collection}` promoted here at term {new_term} (was on {}); {} follow it",
            t.node,
            if new[shard].followers.is_empty() { "none".to_string() } else { new[shard].followers.join(", ") }
        );
        Ok(Outcome::Deferred(Deferred::new(move || {
            let switched = Db::carry_switch(&switch);
            Ok(Outcome::Ack(format!("{ack}; {switched}")))
        })))
    }

    /// The map entry after a promotion: the node promoted holds, the old
    /// holder follows, the term raised.
    fn promoted_tablet(t: &Tablet, node: &str, term: u64) -> Tablet {
        let mut followers: Vec<String> =
            t.followers.iter().filter(|f| *f != node).cloned().collect();
        if !t.node.is_empty() && t.node != node && !followers.contains(&t.node) {
            followers.push(t.node.clone());
        }
        Tablet { node: node.to_string(), lo: t.lo.clone(), hi: t.hi.clone(), term, followers }
    }

    /// This node's followed copy becomes the shard: the directory moves
    /// beside the held ones (the files keep their names, and so their
    /// keys), the shard opens from it, the map says so.
    fn promote_here(
        &mut self,
        collection: &str,
        shard: usize,
        t: &Tablet,
        term: u64,
    ) -> Result<()> {
        let dir = self
            .dir
            .clone()
            .ok_or_else(|| Error::Plan("a promotion needs a persistent database (--dir)".into()))?;
        let copy = self
            .followed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(collection.to_string(), shard));
        let Some(copy) = copy else {
            return Err(Error::Plan(format!(
                "this node has no copy of shard {shard} of `{collection}` to promote"
            )));
        };
        let (caught_up, at, copy_term) = (copy.shard.caught_up, copy.shard.ship_ts, copy.term);
        // The copy closes and its files stay: they are the shard now. (It
        // was retired here, which unlinks every sealed segment the
        // manifest names, and the first promotion of a copy with a sealed
        // segment -- on five real nodes, not in the tests' forty rows --
        // failed to open what it had just deleted, and the shard stayed
        // down with its holder.)
        drop(copy);
        let cdir = dir.join("collections").join(collection);
        let from = cdir.join("followed").join(format!("shard-{shard:04}"));
        let to = cdir.join(format!("shard-{shard:04}"));
        if to.exists() {
            fs::remove_dir_all(&to)?;
        }
        fs::rename(&from, &to)?;
        crate::shard::sync_dir(&cdir)?;
        let def = self.catalog.get(collection)?.clone();
        let opened = read_range(&self.cipher, &to, shard, collection).and_then(|(lo, hi)| {
            let mut sh = Shard::open(def, self.clock.clone(), self.shard_opts(), &to)?;
            sh.set_key_range(lo, hi);
            Ok(sh)
        });
        let mut sh = match opened {
            Ok(sh) => sh,
            Err(e) => {
                // Back where it was, as the copy it was: a promotion that
                // fails leaves the follower a follower, so the next sweep
                // finds a candidate and the operator a copy.
                let _ = fs::rename(&to, &from);
                let _ = crate::shard::sync_dir(&cdir);
                if let Ok((lo, hi)) = read_range(&self.cipher, &from, shard, collection) {
                    if let Ok(def) = self.catalog.get(collection).cloned() {
                        if let Ok(mut back) =
                            Shard::open(def, self.clock.clone(), self.shard_opts(), &from)
                        {
                            back.set_key_range(lo, hi);
                            back.index = shard;
                            self.followed.lock().unwrap_or_else(|p| p.into_inner()).insert(
                                (collection.to_string(), shard),
                                FollowedShard { shard: back, term: copy_term },
                            );
                        }
                    }
                }
                return Err(Error::Storage(format!(
                    "the copy of shard {shard} of `{collection}` did not open as the shard, \
                     and follows again: {e}"
                )));
            }
        };
        sh.index = shard;
        let v = self.shards.entry(collection.to_string()).or_default();
        v.push(sh);
        v.sort_by_key(|s| s.index);
        let me = self.opts.node.clone().unwrap_or_default();
        let mut new = self.catalog.placement.get(collection).cloned().unwrap_or_default();
        new[shard] = Self::promoted_tablet(t, &me, term);
        self.catalog.placement.insert(collection.to_string(), new);
        self.absorb_shard_catalogs(collection)?;
        crate::log::info(
            "promoted",
            &[
                ("collection", collection.to_string()),
                ("shard", shard.to_string()),
                ("term", term.to_string()),
                (
                    "copy",
                    if caught_up {
                        format!("caught up to ts {at}")
                    } else {
                        "not caught up".into()
                    },
                ),
            ],
        );
        self.persist_catalog()
    }

    /// This node's held shard becomes a followed copy: the directory moves
    /// under `followed/`, the copy opens from it not caught up, so the new
    /// holder starts it from nothing -- what this node took after the
    /// promotion was never confirmed, and goes.
    fn demote_here(&mut self, collection: &str, shard: usize) -> Result<()> {
        let Some(dir) = self.dir.clone() else { return Ok(()) };
        let mut gone = Vec::new();
        if let Some(v) = self.shards.get_mut(collection) {
            let mut keep = Vec::new();
            for s in v.drain(..) {
                if s.index == shard {
                    gone.push(s);
                } else {
                    keep.push(s);
                }
            }
            *v = keep;
        }
        if self.shards.get(collection).is_some_and(|v| v.is_empty()) {
            self.shards.remove(collection);
        }
        self.abort_move(collection, shard);
        // Where the new holder stood when it was promoted, as far as this
        // node knows: what its shipper heard the new holder confirm, or
        // the `CONFIRMED` the shipper had written before this process
        // ended. The copy is cut there: everything above it was taken by
        // this node alone after the promotion and nobody confirmed it.
        let new_holder = self
            .catalog
            .placement
            .get(collection)
            .and_then(|v| v.get(shard))
            .map(|t| t.node.clone())
            .unwrap_or_default();
        let mut confirmed = 0;
        for mut s in gone {
            if let Some(sh) = s.shipper.take() {
                if let Some(f) = sh.report().into_iter().find(|f| f.url == new_holder) {
                    confirmed = confirmed.max(f.acked);
                }
                sh.stop();
            }
            if let Some(d) = s.dir() {
                confirmed = confirmed.max(crate::replication::confirmed_in(d));
            }
            // Closed, not retired: the files are the copy's now.
            drop(s);
        }
        let cdir = dir.join("collections").join(collection);
        let from = cdir.join(format!("shard-{shard:04}"));
        let to = cdir.join("followed").join(format!("shard-{shard:04}"));
        let _ = fs::remove_file(from.join(crate::replication::CONFIRMED_FILE));
        fs::create_dir_all(cdir.join("followed"))?;
        if to.exists() {
            fs::remove_dir_all(&to)?;
        }
        if from.exists() {
            fs::rename(&from, &to)?;
        }
        crate::shard::sync_dir(&cdir)?;
        let term = self
            .catalog
            .placement
            .get(collection)
            .and_then(|v| v.get(shard))
            .map(|t| t.term)
            .unwrap_or(0);
        if to.exists() {
            let def = self.catalog.get(collection)?.clone();
            let (lo, hi) = read_range(&self.cipher, &to, shard, collection)?;
            let cut = if confirmed > 0 {
                match Shard::open_at_most(
                    def.clone(),
                    self.clock.clone(),
                    self.shard_opts(),
                    &to,
                    confirmed,
                ) {
                    Ok(sh) => Some(sh),
                    Err(e) => {
                        crate::log::warn(
                            "demoted_copy_from_nothing",
                            &[
                                ("collection", collection.to_string()),
                                ("shard", shard.to_string()),
                                ("error", e.to_string()),
                            ],
                        );
                        None
                    }
                }
            } else {
                None
            };
            let mut sh = match cut {
                Some(mut sh) => {
                    // As the new holder's copy stood at the promotion: caught
                    // up to there, the rest comes from it.
                    sh.caught_up = true;
                    sh.ship_ts = confirmed;
                    sh
                }
                None => {
                    let mut sh = Shard::open(def, self.clock.clone(), self.shard_opts(), &to)?;
                    sh.caught_up = false;
                    sh.ship_ts = 0;
                    sh
                }
            };
            sh.set_key_range(lo, hi);
            sh.index = shard;
            self.followed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert((collection.to_string(), shard), FollowedShard { shard: sh, term });
        }
        crate::log::info(
            "demoted",
            &[
                ("collection", collection.to_string()),
                ("shard", shard.to_string()),
                ("term", term.to_string()),
                (
                    "copy",
                    if confirmed > 0 {
                        format!("cut at ts {confirmed}, following from there")
                    } else {
                        "from nothing".into()
                    },
                ),
            ],
        );
        Ok(())
    }

    /// The steward: the node named, or the lowest address among the nodes
    /// this one knows and itself. None for a node with no address.
    pub fn steward(&self) -> Option<String> {
        if self.opts.stewards.is_some() {
            // Elected: whoever this node last took a lease from, or itself.
            return self.lease.lock().unwrap_or_else(|p| p.into_inner()).steward.clone();
        }
        if let Some(s) = &self.opts.steward {
            return Some(s.clone());
        }
        let me = self.opts.node.clone()?;
        let mut all: Vec<String> = self.catalog.nodes.to_vec();
        all.push(me);
        all.into_iter().min()
    }

    pub fn auto_failover(&self) -> bool {
        self.opts.auto_failover
    }

    pub fn lease_secs(&self) -> u64 {
        self.opts.lease_secs
    }

    /// The holder of a shard, as the map says.
    pub fn holder_of(&self, collection: &str, shard: usize) -> Option<String> {
        self.catalog.placement.get(collection).and_then(|v| v.get(shard)).map(|t| t.node.clone())
    }

    pub fn is_steward(&self) -> bool {
        if self.opts.stewards.is_some() {
            let g = self.lease.lock().unwrap_or_else(|p| p.into_inner());
            return g.election.as_ref().is_some_and(|e| e.role() == crate::steward::Role::Steward);
        }
        self.steward().is_some_and(|s| self.is_self(&s))
    }

    /// The group that elects the steward, this node included, or none
    /// when the steward is configured.
    pub fn steward_group(&self) -> Option<Vec<String>> {
        let mut g = self.opts.stewards.clone()?;
        if let Some(me) = &self.opts.node {
            if !g.contains(me) {
                g.push(me.clone());
            }
        }
        Some(g)
    }

    /// The lease, shared with the wire.
    pub fn lease(&self) -> Lease {
        self.lease.clone()
    }

    /// A write is refused under automatic failover once the steward's
    /// lease on this node ran out: a holder the steward cannot reach may
    /// be replaced, and must not take writes meanwhile.
    fn lease_check(&self) -> Result<()> {
        if !self.opts.auto_failover || self.is_steward() {
            return Ok(());
        }
        let Some(steward) = self.steward() else { return Ok(()) };
        let g = self.lease.lock().unwrap_or_else(|p| p.into_inner());
        let fresh = g
            .at
            .is_some_and(|at| at.elapsed() < std::time::Duration::from_secs(self.opts.lease_secs));
        if fresh {
            return Ok(());
        }
        Err(Error::Plan(format!(
            "this node's lease from the steward {steward} {}; writes are refused until it is \
             renewed (automatic failover is on; the steward may be promoting a follower)",
            match g.at {
                Some(at) => format!(
                    "ran out {} s ago",
                    at.elapsed().as_secs().saturating_sub(self.opts.lease_secs)
                ),
                None => "was never granted".to_string(),
            }
        )))
    }

    /// The steward's word about who it is, kept beside the lease so a
    /// renewal is checked without the lock.
    fn refresh_lease_steward(&mut self) {
        let s = self.steward();
        self.lease.lock().unwrap_or_else(|p| p.into_inner()).steward = s;
    }

    /// For the steward: the shards whose holder is not among `answered`,
    /// with the followers that are -- what an automatic failover promotes
    /// from.
    pub fn failover_plan(&self, answered: &[String]) -> Vec<(String, usize, u64, Vec<String>)> {
        let mut out = Vec::new();
        for (name, tablets) in &self.catalog.placement {
            for (i, t) in tablets.iter().enumerate() {
                if t.is_merged() || self.is_self(&t.node) || answered.contains(&t.node) {
                    continue;
                }
                let up: Vec<String> = t
                    .followers
                    .iter()
                    .filter(|f| answered.contains(f) || self.is_self(f))
                    .cloned()
                    .collect();
                if !up.is_empty() {
                    out.push((name.clone(), i, t.term, up));
                }
            }
        }
        out
    }

    /// Every node this one knows, other than itself, with a connection:
    /// the holders, the nodes and coordinators the catalog names, and the
    /// peers that attached.
    fn every_peer(&self, tablets: &[Tablet]) -> Result<Vec<(String, Arc<crate::wire::Node>)>> {
        let mut order: Vec<String> = Vec::new();
        let known = tablets
            .iter()
            .map(|t| t.node.clone())
            .chain(self.catalog.nodes.iter().cloned())
            .chain(self.catalog.coordinators.iter().cloned())
            .chain(self.attached.iter().cloned());
        for n in known {
            if !n.is_empty() && !self.is_self(&n) && !order.contains(&n) {
                order.push(n);
            }
        }
        order.into_iter().map(|n| Ok((n.clone(), self.node_conn(&n)?))).collect()
    }

    /// `PLACE SHARD i OF c ON 'node'`: this node's map records the shard
    /// there. A copy this node holds of a shard now placed elsewhere is
    /// retired and its directory removed; a shard placed here that this
    /// node does not hold is refused, since the pull has to come first.
    pub fn place_shard(&mut self, collection: &str, shard: usize, node: &str) -> Result<()> {
        // A node the collection never reached has no map to switch and
        // nothing to drop; the move told it in case it coordinates later.
        let Some(tablets) = self.catalog.placement.get_mut(collection) else {
            return Ok(());
        };
        let Some(t) = tablets.get_mut(shard) else {
            return Err(Error::Plan(format!("`{collection}` has no shard {shard}")));
        };
        // The new holder was a follower, or was not; the old holder follows
        // from here on, so a move keeps the copies where they are.
        let prev = std::mem::replace(&mut t.node, node.to_string());
        t.followers.retain(|f| f != node);
        if !prev.is_empty() && prev != node && !t.followers.contains(&prev) {
            t.followers.push(prev);
        }
        let held = self.shards.get(collection).is_some_and(|v| v.iter().any(|s| s.index == shard));
        if self.is_self(node) {
            if !held {
                return Err(Error::Plan(format!(
                    "shard {shard} of `{collection}` was placed here but this node has not \
                     received it; run MOVE SHARD again"
                )));
            }
        } else if held {
            let mut gone = Vec::new();
            if let Some(v) = self.shards.get_mut(collection) {
                let mut keep = Vec::new();
                for s in v.drain(..) {
                    if s.index == shard {
                        gone.push(s);
                    } else {
                        keep.push(s);
                    }
                }
                *v = keep;
            }
            if self.shards.get(collection).is_some_and(|v| v.is_empty()) {
                self.shards.remove(collection);
            }
            self.abort_move(collection, shard);
            for mut s in gone {
                s.retire_all();
                if let Some(d) = s.dir().map(|d| d.to_path_buf()) {
                    let _ = fs::remove_dir_all(&d);
                    if let Some(p) = d.parent() {
                        let _ = crate::shard::sync_dir(p);
                    }
                }
            }
        }
        self.ensure_followed(collection)?;
        self.persist_catalog()
    }

    /// `REBALANCE c`: the moves that put shard `i` on the `i`-th of this
    /// node and the attached ones, in attach order -- the placement a
    /// `CREATE COLLECTION` with no nodes named makes -- each a `MOVE
    /// SHARD`, stopping at the first that fails.
    pub fn rebalance(&mut self, collection: &str) -> Result<Outcome> {
        let tablets = self.catalog.placement.get(collection).cloned().ok_or_else(|| {
            Error::Plan(format!("collection `{collection}` has no placement map"))
        })?;
        let me = self.opts.node.clone().ok_or_else(|| {
            Error::Plan("this node has no address; nothing to rebalance over".into())
        })?;
        let _ = me;
        let nodes = self.data_nodes();
        if nodes.is_empty() {
            return Err(Error::Plan("no data node to rebalance over".into()));
        }
        // Every move pinned under the lock, then copied one after another
        // holding nothing: the pins overlap for the rebalance's length,
        // which is writes to those shards refused meanwhile.
        let mut plans = Vec::new();
        for (i, t) in tablets.iter().enumerate() {
            let target = &nodes[i % nodes.len()];
            if t.is_merged() || &t.node == target || (self.is_self(&t.node) && self.is_self(target))
            {
                continue;
            }
            plans.push(self.move_begin(collection, i, target)?);
        }
        if plans.is_empty() {
            return Ok(Outcome::Ack(format!("`{collection}` is balanced; nothing moved")));
        }
        Ok(Outcome::Deferred(Deferred::new(move || {
            let mut lines = Vec::new();
            for plan in plans {
                if let Outcome::Ack(m) = Db::move_run(plan)? {
                    lines.push(m);
                }
            }
            Ok(Outcome::Ack(lines.join("\n")))
        })))
    }

    pub fn add_index(&mut self, collection: &str, idx: IndexDef) -> Result<()> {
        // DDL is a control-plane transaction; data-plane nodes observe catalog
        // versions and never block on it (§10). Here that means the shards get
        // the new definition and the next flush picks it up.
        let name = idx.name.clone();
        self.catalog.add_index(collection, idx)?;
        // Creation time is what a `SINCE CREATION` rule measures, and an index
        // starts its idle clock now rather than at epoch — otherwise every
        // index is instantly overdue the moment a policy is written.
        let now = lifecycle::now_micros(&self.clock);
        self.catalog.activity.insert((collection.to_string(), name), IndexActivity::new(now));
        let coll = self.catalog.get(collection)?.clone();
        if let Some(shards) = self.shards.get_mut(collection) {
            for s in shards.iter_mut() {
                s.adopt_catalog(coll.clone())?;
            }
        }
        for f in followed_of(&self.followed, collection).iter_mut() {
            f.shard.adopt_catalog(coll.clone())?;
        }
        // A new index changes where this collection's segments belong: the
        // resolved tier of a segment is the coldest tier over the indexes it
        // holds, so adding a cached or active index to a collection whose
        // files were relocated into `archive/` means they have to come back.
        // `sync_archive` is the only thing that moves them, and `apply_tiers`
        // is the only caller of it -- without this the catalog says the new
        // index is active and the bytes are still in the archive, and nothing
        // else on any path reconciles the two.
        self.apply_tiers(collection)?;
        self.persist_catalog()?;
        Ok(())
    }

    /// The catalog as it is written: the definitions as they are, with each
    /// collection's statistics counting the documents sealed into segments and
    /// not the ones still in a memtable. The memtable's are in the WAL, and a
    /// reopen observes every replayed record, so a count that included them
    /// would count them again at every reopen. A side effect worth having: an
    /// insert no longer changes these bytes, so a persist after one is a skip
    /// rather than a rewrite of CATALOG.
    /// `plain` as the root file `name` holds it: framed under the cipher
    /// when there is one.
    fn seal_root(&self, name: &str, plain: &[u8]) -> Result<Vec<u8>> {
        match &self.cipher {
            Some(c) => c.seal_file(name, plain),
            None => Ok(plain.to_vec()),
        }
    }

    /// The wrapped data key, `KEY`, when the database is encrypted: what an
    /// export and a backup carry so they open where the master key is.
    fn key_bytes(&self) -> Result<Option<Vec<u8>>> {
        match (&self.cipher, &self.dir) {
            (Some(_), Some(dir)) => crate::shard::read_optional(&dir.join("KEY")),
            _ => Ok(None),
        }
    }

    fn persisted_catalog(&self) -> Catalog {
        let mut catalog = self.catalog.clone();
        for (name, shards) in &self.shards {
            let Some(coll) = catalog.collections.get_mut(name) else { continue };
            let mut tally = self.stats_baseline.get(name).cloned().unwrap_or_default();
            for s in shards {
                tally.merge(&s.sealed);
            }
            coll.doc_count = tally.docs;
            coll.paths = tally.paths;
        }
        catalog
    }

    fn persist_catalog(&mut self) -> Result<()> {
        self.refresh_lease_steward();
        let names: Vec<String> = self.catalog.placement.keys().cloned().collect();
        for n in names {
            self.ensure_followed(&n)?;
        }
        self.refresh_shippers();
        if let Some(dir) = &self.dir {
            let bytes = self.persisted_catalog().encode();
            let p = dir.join("CATALOG");
            // The same bytes as last time mean the same file on the disk, and
            // rewriting it durably costs two fsyncs and a rename to say
            // nothing. The cache is a record of what THIS `Db` published --
            // that is the invariant, not "this process": there is no lock file
            // anywhere in the tree, so two `Db` handles on one directory each
            // keep their own cache and neither sees the other's writes. Two
            // handles on one directory were already unsupported and are no more
            // supported now. What is defended is the file being taken away or
            // replaced underneath a single handle: `still_published` compares
            // the bytes on disk, and a save that can no longer be written has
            // to say so rather than skipping its way to success.
            // The cache holds what reached the file -- framed, under a
            // cipher -- so that is what the file is compared against; the
            // plaintext is compared by opening the cache.
            if let Some(w) = self.published_catalog.as_deref() {
                let same = match &self.cipher {
                    Some(c) => c.open_file("CATALOG", w).map(|p| p == bytes).unwrap_or(false),
                    None => w == bytes.as_slice(),
                };
                if same && crate::shard::still_published(&p, w) {
                    return Ok(());
                }
            }
            // Not `fs::write`: that truncates in place, so a crash partway
            // through leaves a catalog that will not decode and a database
            // that will not open, with every segment file intact.
            let written = crate::shard::write_content(&self.cipher, "CATALOG", &p, &bytes)?;
            self.published_catalog = Some(written);
        }
        Ok(())
    }

    /// Sync the per-shard catalog copies with the control plane's, so that
    /// inferred path statistics accumulated on the write path are visible to
    /// planning.
    /// The collection as planning wants it: the catalog's entry with the
    /// statistics the shards have accumulated since it was last absorbed,
    /// merged into a copy. What a read uses instead of
    /// [`absorb_shard_catalogs`](Self::absorb_shard_catalogs), which writes
    /// the merge back and so needs `&mut self`.
    pub(crate) fn planning_collection(&self, collection: &str) -> Result<Collection> {
        let mut merged = self.catalog.get(collection)?.clone();
        if let Some(shards) = self.shards.get(collection) {
            let mut tally = self.stats_baseline.get(collection).cloned().unwrap_or_default();
            for s in shards {
                tally.docs += s.coll.doc_count;
                for (p, st) in &s.coll.paths {
                    tally.paths.entry(p.clone()).or_default().merge(st);
                }
            }
            merged.doc_count = tally.docs;
            merged.paths = tally.paths;
        }
        Ok(merged)
    }

    fn absorb_shard_catalogs(&mut self, collection: &str) -> Result<()> {
        let mut merged = self.catalog.get(collection)?.clone();
        if let Some(shards) = self.shards.get(collection) {
            let mut tally = self.stats_baseline.get(collection).cloned().unwrap_or_default();
            for s in shards {
                tally.docs += s.coll.doc_count;
                for (p, st) in &s.coll.paths {
                    tally.paths.entry(p.clone()).or_default().merge(st);
                }
            }
            merged.doc_count = tally.docs;
            merged.paths = tally.paths;
        }
        *self.catalog.get_mut(collection)? = merged;
        Ok(())
    }

    // ------------------------------------------------------------- writes

    pub fn insert(&mut self, collection: &str, doc: Value) -> Result<Timestamp> {
        // A document whose shard is elsewhere is forwarded HERE, under
        // whatever lock the caller holds: this is the embedded API, one
        // process and its own shards. In a cluster, an `INSERT` statement
        // through `execute` is the way: its carry to the holder runs with
        // the lock let go, so two nodes each forwarding to the other under
        // their locks cannot wait for each other.
        let _deadline = self.arm_default_deadline();
        let coll = self.catalog.get(collection)?;
        let key = sort_key(coll, &doc)?;
        let owner = self.owner_of(collection, &key)?;
        let ts = match owner {
            None => self.insert_here(collection, doc)?,
            Some(url) => {
                // Counted where it lands, so that the sum of every holder's
                // counter is the collection's write count exactly once --
                // which is what the statistics epoch compares against, on
                // every node alike.
                let ts = self.node_conn(&url)?.insert(collection, &doc)?;
                self.writes += 1;
                self.last_commit = self.last_commit.max(ts);
                ts
            }
        };
        self.maybe_run_lifecycle()?;
        Ok(ts)
    }

    /// The documents of one statement: the ones this node's shards own go to
    /// each shard in chunks of `DbOpts::insert_batch` -- one WAL sync per
    /// chunk, not one per document -- and the ones another node owns are
    /// forwarded one at a time as [`insert`](Self::insert) forwards them. In
    /// statement order within a shard; the last timestamp is what the
    /// acknowledgement names.
    pub fn insert_many(&mut self, collection: &str, docs: Vec<Value>) -> Result<Timestamp> {
        self.insert_many_local(collection, docs)
    }

    /// The embedded write: durable here when it returns, the followers'
    /// confirmation left in [`confirmation`](Self::confirmation) for the
    /// caller to wait on with the lock let go -- a wait under the lock
    /// would hold the very driver that feeds the followers. A statement
    /// through `execute` waits for it as deferred work.
    fn insert_many_local(&mut self, collection: &str, docs: Vec<Value>) -> Result<Timestamp> {
        let _deadline = self.arm_default_deadline();
        let mut here: BTreeMap<usize, Vec<Value>> = BTreeMap::new();
        let mut last = self.last_commit;
        for doc in docs {
            let coll = self.catalog.get(collection)?;
            let key = sort_key(coll, &doc)?;
            match self.owner_of(collection, &key)? {
                Some(url) => {
                    let ts = self.node_conn(&url)?.insert(collection, &doc)?;
                    self.writes += 1;
                    self.last_commit = self.last_commit.max(ts);
                    last = last.max(ts);
                }
                None => {
                    self.refuse_if_moving(collection, &key)?;
                    let shards = self.shards.get(collection).ok_or_else(|| {
                        Error::Plan(format!("no shard of `{collection}` is on this node"))
                    })?;
                    let idx = shards
                        .iter()
                        .position(|s| s.owns(&key))
                        .ok_or_else(|| Error::Plan(format!("no shard owns key `{key}`")))?;
                    here.entry(idx).or_default().push(doc);
                }
            }
        }
        let chunk = self.opts.insert_batch.max(1);
        if !here.is_empty() {
            self.lease_check()?;
        }
        for (idx, mut batch) in here {
            self.wait_for_compaction(collection, idx);
            while !batch.is_empty() {
                let tail = batch.split_off(batch.len().min(chunk));
                let (stamps, index) = {
                    let shards = self.shards.get_mut(collection).expect("checked above");
                    (shards[idx].insert_many(batch)?, shards[idx].index)
                };
                let mut chunk_last = 0;
                for ts in stamps {
                    self.note_write(collection, ts);
                    last = last.max(ts);
                    chunk_last = chunk_last.max(ts);
                }
                if chunk_last > 0 {
                    self.recent_writes.push((collection.to_string(), index, chunk_last));
                }
                batch = tail;
            }
        }
        self.maybe_run_lifecycle()?;
        Ok(last)
    }

    /// The node holding the shard that owns `key`: `None` for this one.
    /// Backpressure before a write: while the shard holds more flat segments
    /// than compaction has caught up with, the writer waits, so a load
    /// cannot leave reads scanning dozens of unsorted segments. Counted for
    /// the metrics.
    /// The documents of this node's shards, and the rest grouped by the
    /// holder they route to with a connection to it.
    fn split_by_holder(
        &self,
        collection: &str,
        docs: Vec<Value>,
    ) -> Result<(Vec<Value>, Away<Value>)> {
        let mut here = Vec::new();
        let mut away: Away<Value> = BTreeMap::new();
        for doc in docs {
            let coll = self.catalog.get(collection)?;
            let key = sort_key(coll, &doc)?;
            match self.owner_of(collection, &key)? {
                Some(url) => {
                    let conn = self.node_conn(&url)?;
                    away.entry(url).or_insert_with(|| (conn, Vec::new())).1.push(doc);
                }
                None => here.push(doc),
            }
        }
        Ok((here, away))
    }

    fn wait_for_compaction(&mut self, collection: &str, idx: usize) {
        let wait = match self.shards.get(collection).and_then(|s| s.get(idx)) {
            Some(shard) => crate::compaction::backpressure(shard, &self.opts.compaction),
            None => return,
        };
        if !wait.is_zero() {
            self.backpressure_waits += 1;
            self.backpressure_micros += wait.as_micros() as u64;
            std::thread::sleep(wait);
        }
    }

    /// How often a write waited for compaction, and for how long in
    /// microseconds, since this database was opened.
    pub fn backpressure(&self) -> (u64, u64) {
        (self.backpressure_waits, self.backpressure_micros)
    }

    fn owner_of(&self, collection: &str, key: &str) -> Result<Option<String>> {
        let Some(tablets) = self.catalog.placement.get(collection) else {
            // No placement is a collection wholly here.
            return Ok(None);
        };
        let t = tablets
            .iter()
            .find(|t| t.owns(key))
            .ok_or_else(|| Error::Plan(format!("no shard owns key `{key}`")))?;
        Ok(if self.is_self(&t.node) { None } else { Some(t.node.clone()) })
    }

    fn note_write(&mut self, collection: &str, ts: Timestamp) {
        self.writes += 1;
        *self.collection_writes.entry(collection.to_string()).or_insert(0) += 1;
        self.last_commit = self.last_commit.max(ts);
    }

    /// Insert into the shard on THIS node that owns the key. What the wire
    /// calls on the owner; refused if the placement says the key is
    /// elsewhere, because a forwarded write that is forwarded again is a
    /// placement map that disagrees between nodes.
    pub fn insert_here(&mut self, collection: &str, doc: Value) -> Result<Timestamp> {
        let coll = self.catalog.get(collection)?;
        let key = sort_key(coll, &doc)?;
        if let Some(url) = self.owner_of(collection, &key)? {
            return Err(Error::Plan(format!(
                "key `{key}` of `{collection}` belongs to the shard on {url}, not to this node; \
                 the placement maps disagree"
            )));
        }
        self.refuse_if_moving(collection, &key)?;
        self.lease_check()?;
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no shard of `{collection}` is on this node")))?;
        let idx = shards
            .iter()
            .position(|s| s.owns(&key))
            .ok_or_else(|| Error::Plan(format!("no shard owns key `{key}`")))?;
        let ts = shards[idx].insert(doc)?;
        let index = shards[idx].index;
        self.note_write(collection, ts);
        self.recent_writes.push((collection.to_string(), index, ts));
        Ok(ts)
    }

    /// Inserts and deletes this node has applied or forwarded for a
    /// collection: what ages its statistics cache.
    pub fn writes_to(&self, collection: &str) -> u64 {
        self.collection_writes.get(collection).copied().unwrap_or(0)
    }

    /// Fire the lifecycle runner on a write interval, if one is configured.
    ///
    /// Off by default. Tiering moves gigabytes, and an operator who wants that
    /// on a schedule usually wants *their* schedule — `RUN LIFECYCLE` from a
    /// cron job — not one that speeds up when the database is busy.
    fn maybe_run_lifecycle(&mut self) -> Result<()> {
        let n = self.opts.lifecycle_interval_writes;
        if n == 0 || self.writes.saturating_sub(self.lifecycle_checked_at_writes) < n {
            return Ok(());
        }
        self.run_lifecycle(None)?;
        Ok(())
    }

    pub fn delete_key(&mut self, collection: &str, key: &str) -> Result<bool> {
        let _deadline = self.arm_default_deadline();
        self.catalog.get(collection)?;
        match self.owner_of(collection, key)? {
            None => self.delete_key_here(collection, key),
            Some(url) => {
                let deleted = self.node_conn(&url)?.delete(collection, key)?;
                if deleted {
                    self.writes += 1;
                }
                Ok(deleted)
            }
        }
    }

    /// Delete from the shard on THIS node that owns the key; see
    /// [`Db::insert_here`].
    pub fn delete_key_here(&mut self, collection: &str, key: &str) -> Result<bool> {
        self.catalog.get(collection)?;
        if let Some(url) = self.owner_of(collection, key)? {
            return Err(Error::Plan(format!(
                "key `{key}` of `{collection}` belongs to the shard on {url}, not to this node; \
                 the placement maps disagree"
            )));
        }
        self.refuse_if_moving(collection, key)?;
        self.lease_check()?;
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no shard of `{collection}` is on this node")))?;
        for s in shards.iter_mut() {
            if s.owns(key) {
                if let Some(ts) = s.delete(key)? {
                    let idx = s.index;
                    self.note_write(collection, ts);
                    self.recent_writes.push((collection.to_string(), idx, ts));
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn flush(&mut self, collection: &str) -> Result<usize> {
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no such collection `{collection}`")))?;
        let mut n = 0;
        for s in shards.iter_mut() {
            // Count shards that sealed, not segments written: a pinned horizon
            // can make one seal emit several, or — if the drain collected
            // every row — none at all, and the memtable was swapped out, the
            // manifest bumped and the WAL truncated in every one of those
            // cases. `FLUSH` reports work done, so it counts seals.
            if s.flush()?.is_some() {
                n += 1;
            }
        }
        self.absorb_shard_catalogs(collection)?;
        self.persist_catalog()?;
        Ok(n)
    }

    /// The next compaction some shard of this node wants, planned and
    /// pinned under the lock: the console's maintenance thread takes one,
    /// builds it with [`compaction_build`](Self::compaction_build) holding
    /// nothing, and installs it with
    /// [`compaction_install`](Self::compaction_install). `None` when every
    /// shard is quiet.
    pub fn compaction_reserve(&mut self) -> Option<CompactionTicket> {
        let opts = self.opts.compaction;
        for (name, shards) in self.shards.iter_mut() {
            for (i, s) in shards.iter_mut().enumerate() {
                if let Some(reserved) = compaction::reserve(s, &opts) {
                    return Some(CompactionTicket { collection: name.clone(), shard: i, reserved });
                }
            }
        }
        None
    }

    /// Build a reserved compaction: no `Db` is involved, so no lock is held
    /// while the segments are merged.
    pub fn compaction_build(ticket: &CompactionTicket) -> Result<Option<compaction::Built>> {
        compaction::build(&ticket.reserved)
    }

    /// Install a built compaction on the shard it was reserved on. `false`
    /// when the shard moved on meanwhile and the build is dropped.
    /// Whether a due seal freezes for the maintenance thread or builds
    /// inline; set on every shard held and every one built from here on.
    pub fn set_background_seal(&mut self, on: bool) {
        self.opts.background_seal = on;
        for shards in self.shards.values_mut() {
            for s in shards.iter_mut() {
                s.opts.background_seal = on;
            }
        }
    }

    /// The oldest frozen memtable of any shard waiting for its build,
    /// under the lock; [`seal_build`](Self::seal_build) holding nothing;
    /// [`seal_install`](Self::seal_install) under it again. Compaction's
    /// triple, for the seal.
    pub fn seal_reserve(&mut self) -> Option<SealJob> {
        for (name, shards) in self.shards.iter_mut() {
            for (i, s) in shards.iter_mut().enumerate() {
                if let Some(ticket) = s.seal_take() {
                    return Some(SealJob { collection: name.clone(), shard: i, ticket });
                }
            }
        }
        None
    }

    pub fn seal_build(job: &SealJob) -> Result<crate::shard::SealBuilt> {
        Shard::seal_build(&job.ticket)
    }

    /// Commit a built seal. `false` when the shard is gone.
    pub fn seal_install(&mut self, job: SealJob, built: crate::shard::SealBuilt) -> Result<bool> {
        let Some(shards) = self.shards.get_mut(&job.collection) else { return Ok(false) };
        let Some(shard) = shards.get_mut(job.shard) else { return Ok(false) };
        shard.seal_install(job.ticket, built)?;
        let name = job.collection.clone();
        self.absorb_shard_catalogs(&name)?;
        self.persist_catalog()?;
        Ok(true)
    }

    /// A build that failed: the ticket goes back to its shard, retried by
    /// the next reserve; the failure is counted on the shard.
    pub fn seal_requeue(&mut self, job: SealJob, err: &Error) {
        if let Some(shard) = self.shards.get_mut(&job.collection).and_then(|s| s.get_mut(job.shard))
        {
            shard.seal_requeue(job.ticket, err);
        }
    }

    pub fn compaction_install(
        &mut self,
        ticket: CompactionTicket,
        built: compaction::Built,
    ) -> Result<bool> {
        let Some(shards) = self.shards.get_mut(&ticket.collection) else { return Ok(false) };
        let Some(shard) = shards.get_mut(ticket.shard) else { return Ok(false) };
        let installed = compaction::install(shard, built)?;
        if installed {
            self.persist()?;
        }
        Ok(installed)
    }

    pub fn compact(&mut self, collection: &str) -> Result<usize> {
        let opts = self.opts.compaction;
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no such collection `{collection}`")))?;
        let mut n = 0;
        for s in shards.iter_mut() {
            n += compaction::run_to_quiescence(s, &opts, 64)?;
        }
        Ok(n)
    }

    // ------------------------------------------------------------ statistics

    /// Global term statistics for this query's terms (§8.2).
    ///
    /// Both arms answer the same *kind* of number: `num_docs`, the length sum
    /// and `doc_freq`, each masked by visibility at `ts`, so the triple is a
    /// function of the corpus live at that instant and not of how many
    /// physical versions or tombstones happen to be resident. `exact` performs
    /// the two-phase gather on this query — in a cluster a broadcast, here a
    /// loop. Otherwise the cache answers, which is the same gather run only at
    /// the refresh points a fixed write count sets (512 writes), and only for
    /// terms no earlier query in this epoch has already paid for. The
    /// difference between the two arms is staleness — the cached triple is a
    /// set of live sums at ONE instant, at most that many writes behind this
    /// query, never a mixture of instants — plus one difference of spelling:
    /// for a term no unit holds the exact arm omits the entry and the cached
    /// arm stores an explicit `0`, which `GlobalStats::idf` reads identically.
    ///
    /// Staleness alone does not put the shard count back into the answer,
    /// because what a score needs is a `df` and the `num_docs` it is divided by
    /// measured at ONE instant: a stale-but-coherent triple still describes one
    /// corpus, whatever its layout, while a fresh `df` beside a `num_docs` from
    /// a different instant does not describe any corpus at all.
    ///
    /// `ts` must be a timestamp this query pins, not one stored earlier — on
    /// the cached arm the triple gathered at it is written into epoch-lived
    /// state every later query in the epoch reads, so a historical `as_of`
    /// read would poison it for every one of them. A time-travel caller must
    /// pass `exact: true`, which writes nothing. The rule and the reason are
    /// on `fill_term_stats`.
    ///
    /// The term list per path is taken as a SET: a repeat is ignored rather
    /// than counted twice. That is a fix and not a convenience — the shard
    /// gather walks one posting cursor per element of the slice it is handed
    /// and accumulates into the same entry, so `["dup", "dup"]` came back at
    /// twice its real frequency: `df = 160` against `num_docs = 80` on a corpus
    /// all holding `dup`, which is a negative logarithm and the IDF clamp, the
    /// LOWEST weight there is, for a term the corpus is full of.
    ///
    /// # Panics
    ///
    /// With `exact: false`, `ts` must be at or above the last commit this
    /// engine took. That is a precondition, not a quality note, and a
    /// `debug_assert!` holds callers to it: a historical timestamp panics in a
    /// debug build. In release, where that assertion is not compiled, the
    /// timestamp is RAISED to the last commit instead, so the call answers
    /// something fresh rather than writing a past corpus into the cache. Both
    /// halves are stated because an assertion that fires only in debug is
    /// otherwise the worst of both worlds.
    ///
    /// What makes it a contract rather than a quality note is WHOSE answer it
    /// spoils. This arm does not merely answer the caller who passed the old
    /// `ts` inaccurately — it writes the triple it gathered into epoch-lived
    /// state that every later query in the epoch reads, so one historical read
    /// mis-scores queries that asked for nothing of the kind and cannot tell. A
    /// wrong answer confined to the caller would be a documentation matter; one
    /// that escapes to other callers is a contract.
    ///
    /// `exact: true` has no such precondition and is the supported way to read
    /// the past: it writes nothing, and at a `ts` below
    /// `Shard::retain_floor` it is best-effort in exactly the sense
    /// `Shard::term_stats` documents, which a pinned `gc_horizon` makes
    /// exact again.
    pub fn gather_stats(
        &self,
        collection: &str,
        want: &BTreeMap<String, Vec<String>>,
        ts: Timestamp,
        exact: bool,
    ) -> Result<BTreeMap<String, GlobalStats>> {
        let coll = self.catalog.get(collection)?.clone();
        let writes = self.writes_to(collection);
        let tablets = self.catalog.placement.get(collection).cloned().unwrap_or_default();
        let here = self.shards_here(collection);
        let services = services_for(here, &tablets, &BTreeMap::new(), collection, self.sim.clone());
        self.gather_stats_over(&services, &coll, want, ts, exact, false, &mut Vec::new(), writes)
    }

    /// The shards of `collection` on this node, or none: a coordinator
    /// holding no data has no entry and that is not an error.
    fn shards_here(&self, collection: &str) -> &[Shard] {
        self.shards.get(collection).map(Vec::as_slice).unwrap_or(&[])
    }

    /// [`gather_stats`](Self::gather_stats) over an explicit set of shard
    /// services. `unreachable` collects the shards that did not answer, under
    /// `partial`; without it a shard that does not answer is the statement's
    /// error. A fill that lost a shard is used for this statement and never
    /// written to the cache, which holds only sums measured over every shard.
    #[allow(clippy::too_many_arguments)]
    fn gather_stats_over(
        &self,
        services: &[Box<dyn ShardService + '_>],
        coll: &Collection,
        want: &BTreeMap<String, Vec<String>>,
        ts: Timestamp,
        exact: bool,
        partial: bool,
        unreachable: &mut Vec<usize>,
        writes: u64,
    ) -> Result<BTreeMap<String, GlobalStats>> {
        let mut out = BTreeMap::new();
        let collection = coll.name.as_str();
        let prefix_cap = coll.prefix_cap();
        for (path, terms) in want {
            // Once, here, for both arms: the shard gather walks a posting list
            // per element of the slice it is handed, so a term named twice is
            // counted twice. [`term_set`] has the consequences.
            let terms = term_set(terms);
            let terms: &[String] = &terms;
            if exact {
                let (TermStats { num_docs, total_doc_len: total_len, doc_freq: df }, _) =
                    sum_term_stats(services, path, terms, ts, partial, unreachable)?;
                out.insert(
                    path.clone(),
                    GlobalStats {
                        num_docs,
                        avg_doc_len: if num_docs > 0 {
                            total_len as f64 / num_docs as f64
                        } else {
                            1.0
                        },
                        doc_freq: df,
                        // Filled in by `run_select`, which is the only caller
                        // that knows the statement's prefixes.
                        expansions: Default::default(),
                        prefix_cap,
                        exact: true,
                    },
                );
            } else {
                // The rule on the doc comment, as a test failure rather than
                // a sentence: this arm writes what it gathers into state the
                // whole epoch reads, so the timestamp has to be one this query
                // pinned. `run_select` passes `clock.peek().max(last_commit)`.
                debug_assert!(
                    ts >= self.last_commit,
                    "a historical `as_of` must be read with `exact: true`: it would poison the \
                     cache for every later query in the epoch"
                );
                // And self-enforcing in release, where the assertion is not
                // compiled and this is a PUBLIC method one word of a doc
                // comment away from being called with a historical `ts`. Raised
                // rather than refused: `last_commit` is a valid pin by the
                // argument `fill_term_stats` makes — it is at or above every
                // commit issued so far, so the set it selects is "everything
                // committed" — which turns the misuse into a merely-fresh read
                // instead of an epoch every later query mis-scores from. A
                // conforming caller is already at or above it, so this is a
                // no-op for them.
                //
                // Below the `debug_assert!`, not above it: shadowing `ts` first
                // would delete both the debug panic and the test that pins it.
                let ts = ts.max(self.last_commit);
                // Reset first, then fill: a refresh point starts a new epoch
                // by emptying the entry, and filling into an entry that is
                // about to be emptied would pay for a masked walk and discard
                // it.
                self.reset_stats_if_stale(collection, path, writes);
                let fresh = self.fill_term_stats(
                    services,
                    collection,
                    path,
                    terms,
                    ts,
                    partial,
                    unreachable,
                    writes,
                )?;
                // The answer comes from the fill whenever the fill gathered
                // anything, and NOT from reading the cache back. Those are
                // different numbers: the entry cap evicts oldest first, and
                // `required_terms` hands over a sorted list, so a common term
                // early in the alphabet is filled first, sits at the front of
                // `fill_order`, and is evicted by its own query as soon as
                // that query carries [`STATS_TERM_CAP`] other terms — which
                // one query may, nothing bounds the count. Read back, it would
                // answer `df = 0`, the highest weight there is, for a term the
                // corpus is full of. The cap is a bound on what is retained.
                //
                // Read by reference in the other arm. It used to clone the
                // entry, back when the entry was the collection's whole
                // vocabulary, and that cost 2.4 ms per query at 50k documents
                // and 18.6 ms at 200k — more than the exact gather the cache
                // exists to avoid. [`STATS_TERM_CAP`] bounds it now, so the
                // clone would be smaller; it would still be copying up to four
                // thousand entries to read two or three.
                let key = cache_key(collection, path);
                // `None` means the fill gathered nothing, which it does only
                // when every term is already cached under globals measured in
                // this epoch — so the cache holds an entry for every one of
                // them and nothing was evicted, and both defaults below are
                // unreachable. They are spelled out rather than unwrapped so
                // that a future early return cannot turn into a panic on the
                // query path.
                let (num_docs, total_doc_len, df): StatsTriple = match fresh {
                    Some(t) => t,
                    None => match guard(&self.stats).get(&key) {
                        Some(c) => (
                            c.num_docs,
                            c.total_doc_len,
                            terms
                                .iter()
                                .map(|t| (t.clone(), c.doc_freq.get(t).copied().unwrap_or(0)))
                                .collect(),
                        ),
                        None => (0, 0, terms.iter().map(|t| (t.clone(), 0)).collect()),
                    },
                };
                out.insert(
                    path.clone(),
                    GlobalStats {
                        num_docs,
                        avg_doc_len: if num_docs > 0 {
                            total_doc_len as f64 / num_docs as f64
                        } else {
                            1.0
                        },
                        doc_freq: df,
                        expansions: Default::default(),
                        exact: false,
                        prefix_cap,
                    },
                );
            }
        }
        Ok(out)
    }

    /// Start a new statistics epoch if the engine has taken
    /// [`STATS_REFRESH_WRITES`] writes since the last one started.
    ///
    /// The gate is a write counter and nothing else — no shard's seal
    /// schedule, no segment count, no elapsed time. That is what makes a stale
    /// read shard-count independent rather than merely approximate: every
    /// shard count crosses the same thresholds after the same writes, so they
    /// all measure the corpus at the same points in its history.
    ///
    /// This gathers nothing, and that is the change it is worth being explicit
    /// about. It used to sum the globals here as well, and the sum was dead on
    /// every query that carries terms: the reset empties `doc_freq`, so the
    /// fill that follows always has something missing and always re-anchors
    /// the globals itself, over the same shards at the same timestamp.
    /// Measured at a real epoch boundary, 50k documents over two units,
    /// release: the two passes cost 410-508 us and 923-1041 us, and the one
    /// pass that replaces them costs 870-1215 us. So it is the whole of the
    /// first pass that goes, not a fraction of the second — the pass that
    /// remains does the posting walk either way, and the globals it needs it
    /// computed anyway. The prefix-only query, whose term list is empty and
    /// which was the one caller this pass was ever live for, now takes its
    /// globals from the fill's empty-slice gather, in one pass rather than
    /// two.
    fn reset_stats_if_stale(&self, collection: &str, path: &str, writes: u64) {
        let key = cache_key(collection, path);
        let mut stats = guard(&self.stats);
        let stale = match stats.get(&key) {
            None => true,
            Some(c) => writes.saturating_sub(c.refreshed_at_writes) >= STATS_REFRESH_WRITES,
        };
        if !stale {
            return;
        }
        // The empty `doc_freq` is the epoch boundary, and it is deliberate: a
        // frequency measured in the previous epoch must not be read against
        // globals measured in this one, so the reset drops every fill — and
        // `fill_order` with it, in the same statement, because the two are
        // only ever correct together.
        stats.insert(
            key,
            CachedStats {
                num_docs: 0,
                total_doc_len: 0,
                doc_freq: BTreeMap::new(),
                fill_order: VecDeque::new(),
                refreshed_at_writes: writes,
                measured_at_writes: 0,
                anchored: false,
            },
        );
    }

    /// Gather what the cache cannot coherently answer for `terms`, re-anchor
    /// the globals on the same gather, and return the triple this query is to
    /// be answered with — `None` when it gathered nothing, which means the
    /// cache already holds the whole answer.
    ///
    /// Returning the answer rather than leaving the caller to read the cache
    /// back is what decouples the answer from residency. The entry cap evicts
    /// oldest first and a single query may ask for more terms than the cap
    /// holds, so a term this very call filled can be gone by the time the call
    /// returns; it is still in the triple, because the triple is built before
    /// the eviction loop runs.
    ///
    /// What it gathers is decided by whether the cached frequencies and the
    /// globals about to be written would be from the same instant. `Db::writes`
    /// counts every insert and every delete, so `measured_at_writes ==
    /// self.writes` proves the live corpus has not moved since the cached
    /// entries were measured, and gathering only the missing terms leaves the
    /// entry coherent. Otherwise it gathers ALL of `terms` and drops every
    /// earlier frequency: they belong to an earlier corpus than the globals
    /// this call is about to write, and a `df` divided by an `n` it was never
    /// measured against is not a stale answer but an incoherent one — `df > n`
    /// and the IDF clamp are reachable from it, and the error is a function of
    /// `df/n`, so it is unbounded as the corpus is small. That costs at most
    /// one gather of exactly this query's terms, which is exactly what
    /// `WITH (exact_scoring)` would have cost: the default path never costs
    /// more than the exact path for the same query, and usually costs nothing.
    ///
    /// One drift survives and it is not a write: a lifecycle transition that
    /// makes a segment refuse reads changes what `Shard::term_stats` can see
    /// without touching `Db::writes`. That is a refusal rather than a drift,
    /// and it is the same on the exact path.
    ///
    /// THE RULE, and it is the one plausible-looking optimisation that
    /// silently undoes everything above: `ts` must be a timestamp pinned by
    /// the CURRENT query — `run_select` computes it as
    /// `clock.peek().max(last_commit)`. Never store the timestamp a refresh
    /// used and re-read at it later. `Shard::term_stats` is a pure function
    /// of the live corpus only at or above `Shard::retain_floor`, and
    /// `retain_from` returns `now` when no `gc_horizon` is pinned, so a seal
    /// or a compaction walks the floor up past any stored timestamp and the
    /// triple gathered at it becomes best-effort — worse, best-effort in a way
    /// that depends on when each shard happened to compact, which is precisely
    /// the dependence this cache was rebuilt to remove. A stored `as_of` would
    /// look like a free win and would put the bug straight back.
    ///
    /// That the numeric `ts` differs between shard counts is harmless: the
    /// clock advances only on insert and delete, seals and compactions read it
    /// with `peek`, and the pin is at or above every commit issued so far — so
    /// the set it selects is "everything committed" whatever the number is.
    #[allow(clippy::too_many_arguments)]
    fn fill_term_stats(
        &self,
        services: &[Box<dyn ShardService + '_>],
        collection: &str,
        path: &str,
        terms: &[String],
        ts: Timestamp,
        partial: bool,
        unreachable: &mut Vec<usize>,
        writes: u64,
    ) -> Result<Option<StatsTriple>> {
        let key = cache_key(collection, path);
        // Looked at under the lock and let go before the gather: the shard
        // calls below must not be made with the cache held.
        let (missing, anchored, same_instant) = match guard(&self.stats).get(&key) {
            Some(c) => (
                terms.iter().filter(|t| !c.doc_freq.contains_key(*t)).cloned().collect::<Vec<_>>(),
                c.anchored,
                c.anchored && c.measured_at_writes == writes,
            ),
            None => (terms.to_vec(), false, false),
        };
        if missing.is_empty() && anchored {
            // Everything asked for is already cached, under globals measured
            // in this epoch. Nothing is gathered, so nothing can be evicted,
            // so the cache is the answer and the caller may read it.
            return Ok(None);
        }
        // Either the corpus has not moved since the cached frequencies were
        // measured — in which case the globals this gather produces are the
        // ones already stored and only the missing terms need measuring — or
        // it has, and every cached frequency belongs to an older corpus than
        // the globals about to be written over it.
        let (gather, stale_generation) =
            if same_instant { (missing, false) } else { (terms.to_vec(), true) };
        // An empty `gather` is not a wasted call: `term_stats` then does
        // one `visibility` and one masked length sum per unit and enters
        // no posting cursor at all, so a statement with no terms to
        // measure still gets real `num_docs` and `avgdl` for one cheap
        // pass. A prefix-only query used to be the example, because
        // `TextQuery::leaf_terms` skipped `Prefix` and left the list
        // empty; `run_select` now merges the resolved expansion into
        // `want` before this runs, so what reaches here empty is a prefix
        // that matched no live term, or a path whose leaves are all
        // negated — TERMS and prefixes alike, since a negated expansion is
        // an exclusion set and an exclusion set is enumerated, not scored.
        let (TermStats { num_docs, total_doc_len, doc_freq: df }, complete) =
            sum_term_stats(services, path, &gather, ts, partial, unreachable)?;
        if !complete {
            // Measured over the shards that answered: right for this
            // statement, which reports them missing, and wrong for the cache,
            // which every later statement in the epoch would read as the
            // whole collection. Answer from it and write nothing.
            let answer: BTreeMap<String, u64> =
                terms.iter().map(|t| (t.clone(), df.get(t).copied().unwrap_or(0))).collect();
            return Ok(Some((num_docs, total_doc_len, answer)));
        }
        let mut stats = guard(&self.stats);
        let Some(c) = stats.get_mut(&key) else { return Ok(None) };
        if stale_generation {
            // Dropped, not read. Both together: see `CachedStats::fill_order`.
            c.doc_freq.clear();
            c.fill_order.clear();
        }
        // Overwriting the globals from this same call is load-bearing, not an
        // optimisation. It is what makes the frequency just measured coherent
        // with the `num_docs` it will be divided by; leave the old globals in
        // place and a burst of writes carrying a new term gives a `df`
        // gathered over the corpus as it is now against an `n` gathered over
        // the corpus as it was, which is how `df > n` and a negative IDF
        // become reachable.
        c.num_docs = num_docs;
        c.total_doc_len = total_doc_len;
        c.measured_at_writes = writes;
        c.anchored = true;
        for t in &gather {
            // An explicit `0` for a term no unit holds. Leaving it out would
            // mean every query for a term that is not in the corpus re-walks
            // every unit looking for it, forever.
            //
            // The `is_none` guard cannot fire as the code stands, and it is
            // kept deliberately rather than by oversight: `gather` is either
            // `missing`, which is by construction the terms NOT in the map, or
            // all of `terms` after the branch above cleared the map, and
            // [`term_set`] has already made `terms` distinct. It is the one
            // statement that keeps `fill_order` in step with `doc_freq`, they
            // are only ever correct together, and a desync is silent until the
            // eviction loop stops bounding the map. It costs a comparison.
            if c.doc_freq.insert(t.clone(), df.get(t).copied().unwrap_or(0)).is_none() {
                c.fill_order.push_back(t.clone());
            }
        }
        // The answer, built BEFORE the eviction below: every term asked for,
        // from the frequencies just gathered merged with the cached ones that
        // survived. After the eviction this would be a different map.
        let answer: BTreeMap<String, u64> =
            terms.iter().map(|t| (t.clone(), c.doc_freq.get(t).copied().unwrap_or(0))).collect();
        while c.doc_freq.len() > STATS_TERM_CAP {
            match c.fill_order.pop_front() {
                Some(t) => {
                    c.doc_freq.remove(&t);
                }
                None => break,
            }
        }
        // Deliberately NOT bumping `refreshed_at_writes`. The epoch clock has
        // to keep running: a query stream that keeps asking for fresh terms
        // would otherwise reset it on every query and pin the globals — and
        // every frequency filled under them — to an epoch that never ends.
        Ok(Some((num_docs, total_doc_len, answer)))
    }

    // ------------------------------------------------------------ execution

    pub fn execute(&mut self, sql: &str) -> Result<Outcome> {
        self.execute_with(sql, &[])
    }

    pub fn execute_with(&mut self, sql: &str, params: &[Value]) -> Result<Outcome> {
        let stmt = sql::parse(sql, params)?;
        // A directory that vanished under a running node -- unmounted,
        // removed, renamed -- must not be written to: the log's descriptor
        // still accepts bytes into a file no reopen can find, and that is a
        // write acknowledged into nothing. The LOCK this process holds is
        // the cheapest witness that the directory is still where it was.
        if !Db::is_read(&stmt) {
            if let Some(dir) = &self.dir {
                if !dir.join("LOCK").exists() {
                    return Err(Error::Storage(format!(
                        "the data directory {} is gone (its LOCK is not there); nothing was \
                         written, and this node should be stopped",
                        dir.display()
                    )));
                }
            }
        }
        // Every statement runs under the default deadline, not only a SELECT
        // (which re-arms with its own WITH). A forwarded write or a DDL that
        // reaches other nodes waits on them, and a wait with no bound is a
        // cluster that cannot be told from a hung one: two nodes each holding
        // their lock while waiting for the other would wait forever.
        let _deadline = self.arm_default_deadline();
        // What reads noted before this statement is applied first, so a
        // lifecycle run sees the accesses that preceded it.
        self.apply_touches()?;
        let out = self.run(stmt, sql, params, false, false);
        self.apply_touches()?;
        out
    }

    /// A delete by predicate: the keys it selects, then each deleted here
    /// or carried to its holder. Under the lock, as every statement.
    fn delete_where(&mut self, d: DeleteStmt, sql: &str, params: &[Value]) -> Result<Outcome> {
        let (keys, cut): (Vec<String>, Vec<String>) = match &d.predicate {
            None => {
                return Err(Error::Plan(
                    "DELETE without WHERE is refused; add a predicate or drop the \
                     collection"
                        .into(),
                ))
            }
            Some(p) => {
                let sel = exec::select_for_delete(&d.collection, p);
                // The whole result, not just its rows. A DELETE runs
                // the same `Select` every query does, so its predicate
                // can be CUT the same way — and this is the one
                // statement shape where a short answer does write
                // work.
                let r = self.run_select(&sel, sql, params, false)?;
                (r.rows.iter().map(|x| x.key.clone()).collect(), r.truncated_prefixes.clone())
            }
        };
        // REFUSED, and before a single `delete_key`. The choice is
        // between executing and saying so, and refusing, and it is
        // decided by what a cut predicate does in the shape that goes
        // WRONG rather than by the shape that is merely short.
        //
        // A cut SELECT is recoverable: the caller widens the prefix and
        // runs it again, and the rows are still there to be found. A
        // cut DELETE is not, and in the negated shape it is not even
        // short — it is LARGER than what was asked for. `DELETE ...
        // WHERE text_match(body, 'zed -a*')` over 1000 documents each
        // holding its own `a#####` term excludes every one of them, so
        // the correct answer is zero rows; the cut exclusion set covers
        // 512 of the terms, the other 488 documents are no longer
        // excluded, and executing it destroyed 488 rows THE PREDICATE
        // EXCLUDED. No acknowledgement fixes that, because by the time
        // it is read the rows are gone.
        //
        // Nor are the two told apart here and only the dangerous one
        // refused. The direction IS known — `required_prefixes` records
        // the effective polarity, SQL's own `NOT` included, which is
        // what lets the SELECT report name a consequence — but knowing
        // it buys nothing for a statement that cannot be taken back.
        // One statement may spell a prefix both ways and get one term
        // list, a positive leaf that is merely SHORT still deletes a
        // set nobody named, and the caller's remedy is the same either
        // way. So the refusal is on the CUT, not on the sign.
        //
        // So: refuse, which is what this repository does everywhere the
        // alternative is doing something irreversible quietly — the
        // prefix-leaf budget above refuses rather than trimming, the
        // server binds the loopback rather than guessing an interface.
        // The cost is a wide-prefix DELETE that has to be spelled as
        // several narrower ones, and that loop deletes exactly what
        // each piece names, which is the property a re-run loop over a
        // cut predicate never had.
        if !cut.is_empty() {
            let cap = self.catalog.get(&d.collection)?.prefix_cap();
            return Err(Error::Plan(format!(
                "DELETE refused and NOTHING was deleted: its predicate was CUT at \
                 {cap} expanded terms (this collection's prefix_expansion), so the \
                 documents it selects are not the documents it describes. A cut `a*` \
                 names only part of what it matches; a cut `-a*` is a short EXCLUSION \
                 set, so the statement would delete documents the predicate EXCLUDES \
                 — and a delete cannot be taken back either way. Narrow the prefix \
                 until it expands to at most {cap} terms and delete the pieces, raise \
                 the collection's prefix_expansion if its vocabulary fits under the \
                 ceiling, or delete by key; the same predicate as a SELECT shows what \
                 was cut — {}",
                cut.join("; ")
            )));
        }
        let mut n = 0;
        let mut away: Away<String> = BTreeMap::new();
        for k in keys {
            match self.owner_of(&d.collection, &k)? {
                None => {
                    if self.delete_key_here(&d.collection, &k)? {
                        n += 1;
                    }
                }
                Some(url) => {
                    let conn = self.node_conn(&url)?;
                    away.entry(url).or_insert_with(|| (conn, Vec::new())).1.push(k);
                }
            }
        }
        let confirm = self.confirmation();
        if away.is_empty() && confirm.is_empty() {
            return Ok(Outcome::Ack(format!("{n} document(s) deleted")));
        }
        let collection = d.collection.clone();
        let remaining = crate::deadline::remaining_ms();
        let dialer = self.dialer();
        Ok(Outcome::Deferred(Deferred::new(move || {
            let n = n + carry_deletes(&collection, away, remaining, &dialer)?;
            confirm.wait()?;
            Ok(Outcome::Ack(format!("{n} document(s) deleted")))
        })))
    }

    /// Whether `stmt` is answered by [`read`](Self::read): a `SELECT`, an
    /// `EXPLAIN` of one, or either behind `LOCAL`. Everything else changes
    /// something and takes `&mut self`.
    pub fn is_read(stmt: &Statement) -> bool {
        match stmt {
            Statement::Select(_) | Statement::ShowHealth => true,
            Statement::Explain { inner, .. } | Statement::Local(inner) => Self::is_read(inner),
            _ => false,
        }
    }

    /// A read statement under a shared reference: what the console and the
    /// wire run under a read lock, so reads proceed side by side and beside
    /// nothing but writes. Refuses a statement that is not one
    /// ([`is_read`](Self::is_read) says which); the caller takes the write
    /// lock and [`execute`](Self::execute) instead. What the read noted --
    /// index accesses -- waits in [`touches_pending`](Self::touches_pending)
    /// for a writer.
    pub fn read(&self, sql: &str) -> Result<Outcome> {
        self.read_with(sql, &[])
    }

    pub fn read_with(&self, sql: &str, params: &[Value]) -> Result<Outcome> {
        let stmt = sql::parse(sql, params)?;
        let _deadline = self.arm_default_deadline();
        self.run_read(stmt, sql, params)
    }

    fn run_read(&self, stmt: Statement, sql: &str, params: &[Value]) -> Result<Outcome> {
        match stmt {
            Statement::Select(sel) => Ok(Outcome::Rows(self.run_select(&sel, sql, params, false)?)),
            // Dials every peer: under the write lock that held every
            // reader on the node for as long as a peer took to answer.
            Statement::ShowHealth => Ok(Outcome::Ack(self.show_health())),
            Statement::Local(inner) => self.run_read(*inner, sql, params),
            Statement::Explain { analyze, inner } => match *inner {
                Statement::Select(sel) => {
                    Ok(Outcome::Explain(self.explain_select(&sel, sql, params, analyze)?))
                }
                other => self.run_read(
                    Statement::Explain { analyze, inner: Box::new(other) },
                    sql,
                    params,
                ),
            },
            other => Err(Error::Plan(format!(
                "`{}` is not a read; it runs under the write lock",
                statement_kind(&other)
            ))),
        }
    }

    /// The rendered plan of a select, run.
    fn explain_select(
        &self,
        sel: &Select,
        sql: &str,
        params: &[Value],
        analyze: bool,
    ) -> Result<String> {
        let r = self.run_select(sel, sql, params, true)?;
        let mut text = r.explain.as_ref().map(|e| e.render()).unwrap_or_else(|| "(no plan)".into());
        if !analyze {
            // Without ANALYZE the timings are still printed but the query
            // did run; say so rather than implying a cost-only estimate.
            text.push_str(
                "  note: this plan was executed; EXPLAIN without ANALYZE does not yet avoid execution\n",
            );
        }
        Ok(text)
    }

    /// The default statement budget, for the entry points a caller reaches
    /// without SQL; a SELECT's `WITH (deadline_ms)` nests inside it.
    fn arm_default_deadline(&self) -> crate::deadline::Armed {
        crate::deadline::arm(self.opts.statement_deadline_ms)
    }

    /// The other holders a statement has to reach after it ran here, or
    /// none: DDL and the operational statements are per collection and
    /// every holder carries the collection's control-plane state; a query,
    /// a write and the node-local statements fan out through other means or
    /// not at all.
    fn fan_out_of(&self, stmt: &Statement) -> Vec<String> {
        // A definition changes on every node that plans over the
        // collection, coordinators included; a seal, a compaction or a
        // lifecycle run happens where the shards are.
        let (collection, definition) = match stmt {
            Statement::CreateIndex(c) => (Some(c.collection.clone()), true),
            Statement::AlterIndexTier { collection, .. }
            | Statement::AlterCollection { collection, .. }
            | Statement::DropIndex { collection, .. } => (Some(collection.clone()), true),
            Statement::Flush { collection } | Statement::Compact { collection } => {
                (Some(collection.clone()), false)
            }
            Statement::DropCollection { name } => (Some(name.clone()), true),
            Statement::CreateLifecyclePolicy(d) => (Some(d.collection.clone()), true),
            Statement::DropLifecyclePolicy { name } => {
                (self.catalog.policies.get(name).map(|p| p.collection.clone()), true)
            }
            Statement::RunLifecycle { collection: Some(c) } => (Some(c.clone()), false),
            _ => (None, false),
        };
        match collection {
            Some(c) if definition => self.ddl_targets(&c),
            Some(c) => self.holders(&c),
            None => Vec::new(),
        }
    }

    fn run(
        &mut self,
        stmt: Statement,
        sql: &str,
        params: &[Value],
        analyze: bool,
        local: bool,
    ) -> Result<Outcome> {
        if let Statement::Local(inner) = stmt {
            return self.run(*inner, sql, params, analyze, true);
        }
        // A statement that arrived over the wire never fans out again,
        // whatever its text says: the holder that forwarded it is waiting
        // for this one under its own lock, and a fan-out from here that
        // reached it back would wait for that lock forever.
        let local = local || crate::wire::serving();
        let holders = if local { Vec::new() } else { self.fan_out_of(&stmt) };
        let reconciled = Db::reconciles(&stmt);
        // A split is the holder's to make and everyone's to learn: `LOCAL`
        // (or the wire) is how the holder's word arrives.
        if let Statement::SplitShard { collection, shard, at } = &stmt {
            return self.split_shard(collection, *shard, at.as_deref(), local);
        }
        if let Statement::MergeShards { collection, a, b } = &stmt {
            return self.merge_shards(collection, *a, *b, local);
        }
        if let Statement::PromoteShard { collection, shard, node, term } = &stmt {
            return self.promote_shard(collection, *shard, node, *term, local);
        }
        let out = self.run_one(stmt, sql, params, analyze)?;
        if holders.is_empty() {
            return Ok(out);
        }
        // Applied here under the lock; carried to the holders as deferred
        // work holding nothing, every holder at once. A holder that cannot
        // be reached costs a dial's timeout, and ten of them in turn under
        // the lock was a console that answered nothing for the better part
        // of a minute -- which a liveness probe reads as a dead node.
        let mut conns = Vec::with_capacity(holders.len());
        let mut failures = Vec::new();
        for url in &holders {
            match self.node_conn(url) {
                Ok(n) => conns.push((url.clone(), n)),
                Err(e) => failures.push(format!("{url}: {e}")),
            }
        }
        let local = format!("LOCAL {sql}");
        let params = params.to_vec();
        let sql = sql.to_string();
        let remaining = crate::deadline::remaining_ms();
        Ok(Outcome::Deferred(Deferred::new(move || {
            let (done, more) = carry_statement(&conns, &local, &params, remaining);
            failures.extend(more);
            if !failures.is_empty() && !reconciled {
                return Err(Db::not_propagated(&done, &failures, &sql));
            }
            let note = if failures.is_empty() {
                String::new()
            } else {
                format!(
                    "; not on {}: they adopt it when they reconnect ({RECONCILE_NOTE})",
                    failures.join("; ")
                )
            };
            Ok(match out {
                Outcome::Ack(m) if done.is_empty() => Outcome::Ack(format!("{m}{note}")),
                Outcome::Ack(m) => Outcome::Ack(format!("{m}; and on {}{note}", done.join(", "))),
                other => other,
            })
        })))
    }

    /// Whether a statement's effect is carried by `reconcile` to a node it
    /// did not reach: the definitions and drops the catalog keeps a time
    /// for. An `ALTER`, a placement or a policy's drop is not, and a node
    /// it did not reach has to be told.
    fn reconciles(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateIndex(_)
                | Statement::DropIndex { .. }
                | Statement::DropCollection { .. }
                | Statement::CreateLifecyclePolicy(_)
        )
    }

    /// Convenience: run a SELECT and return its rows.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)?.rows()
    }

    pub fn query_with(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.execute_with(sql, params)?.rows()
    }

    fn run_one(
        &mut self,
        stmt: Statement,
        sql: &str,
        params: &[Value],
        analyze: bool,
    ) -> Result<Outcome> {
        match stmt {
            Statement::Explain { analyze, inner } => match *inner {
                Statement::Select(sel) => {
                    Ok(Outcome::Explain(self.explain_select(&sel, sql, params, analyze)?))
                }
                other => self.run_one(other, sql, params, true),
            },
            Statement::CreateCollection(c) => {
                let pk = c
                    .columns
                    .iter()
                    .find(|x| x.primary_key)
                    .map(|x| x.path.clone())
                    .unwrap_or_else(|| "id".to_string());
                let mut coll = Collection::new(&c.name, &pk, c.partition_by.clone());
                if let Some(n) = c.prefix_expansion {
                    check_prefix_cap(n)?;
                    coll.prefix_expansion = Some(n);
                }
                if let Some(of) = &c.nodes_of {
                    self.check_nodes_of(&c.name, of)?;
                    coll.nodes_of = Some(of.clone());
                }
                coll.undirected = c.undirected;
                for col in &c.columns {
                    coll.declared.push(ColumnDef {
                        path: col.path.clone(),
                        ty: col.ty,
                        not_null: col.not_null,
                    });
                }
                coll.replicas = c.replicas.unwrap_or(DEFAULT_REPLICAS) as u8;
                let tablets = self.plan_tablets(&c.splits, &c.nodes, coll.replicas as usize)?;
                let n = tablets.len();
                let mut nodes: Vec<&str> = tablets.iter().map(|t| t.node.as_str()).collect();
                nodes.sort();
                nodes.dedup();
                let where_ = if nodes.len() == 1 && self.is_self(nodes[0]) {
                    String::new()
                } else {
                    format!(" on {}", nodes.join(", "))
                };
                let spread = self.create_spread(coll, tablets)?;
                let name = c.name.clone();
                if spread.conns.is_empty() {
                    // Nobody to carry it to: answered now.
                    let note = spread.carry();
                    return Ok(Outcome::Ack(format!(
                        "collection `{name}` created with {n} shard(s){where_}{note}"
                    )));
                }
                Ok(Outcome::Deferred(Deferred::new(move || {
                    let note = spread.carry();
                    Ok(Outcome::Ack(format!(
                        "collection `{name}` created with {n} shard(s){where_}{note}"
                    )))
                })))
            }
            Statement::CreateIndex(c) => {
                let kind = match c.spec {
                    IndexSpec::FullText { analyzer } => IndexKind::FullText { analyzer },
                    IndexSpec::Vector { dims, metric } => IndexKind::Vector { dims, metric },
                    IndexSpec::Secondary => IndexKind::Secondary,
                    IndexSpec::Adjacency { to } => {
                        self.check_adjacency(&c.collection, &c.path, &to)?;
                        IndexKind::Adjacency { to }
                    }
                };
                self.add_index(&c.collection, IndexDef::new(&c.name, &c.path, kind, c.tier))?;
                Ok(Outcome::Ack(format!(
                    "index `{}` created on the {} tier",
                    c.name,
                    c.tier.name()
                )))
            }
            Statement::Insert(i) => {
                let n = i.docs.len();
                // The documents of this node's shards written here under
                // the lock; the rest carried to their holders as deferred
                // work holding nothing, holder by holder at once. A holder
                // that does not answer held the console for a deadline.
                let (here, away) = self.split_by_holder(&i.collection, i.docs)?;
                let mut last = self.last_commit;
                if !here.is_empty() {
                    last = last.max(self.insert_many_local(&i.collection, here)?);
                }
                let confirm = self.confirmation();
                if away.is_empty() && confirm.is_empty() {
                    return Ok(Outcome::Ack(format!("{n} document(s) written at ts {last}")));
                }
                let collection = i.collection.clone();
                let remaining = crate::deadline::remaining_ms();
                let dialer = self.dialer();
                let here_n = n - away.values().map(|(_, d)| d.len()).sum::<usize>();
                Ok(Outcome::Deferred(Deferred::new(move || {
                    let last = carry_writes(&collection, away, last, here_n, remaining, &dialer)?;
                    confirm.wait()?;
                    Ok(Outcome::Ack(format!("{n} document(s) written at ts {last}")))
                })))
            }
            Statement::Delete(d) => {
                // A delete by predicate selects its keys first, from every
                // holder, so a holder that cannot be reached is a select
                // that waits for it -- and the select is under the lock.
                // So the holders are asked whether they answer with the
                // lock let go, and the keys are selected under it only
                // once they have; one that does not answer refuses the
                // delete before anything is deleted. A holder lost between
                // the two steps is waited for under the lock, as before.
                let holders = self.holders(&d.collection);
                if d.predicate.is_none() || holders.is_empty() || crate::wire::serving() {
                    return self.delete_where(d, sql, params);
                }
                let mut conns = Vec::new();
                for url in holders {
                    conns.push((url.clone(), self.node_conn(&url)?));
                }
                let collection = d.collection.clone();
                let remaining = crate::deadline::remaining_ms();
                let sql = sql.to_string();
                let params = params.to_vec();
                Ok(Outcome::Deferred(Deferred::then_under_lock(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    for (url, _, r) in fetch_counters(&conns, &collection, false) {
                        if let Err(Error::Deadline(e)) = r {
                            return Err(Error::Deadline(format!(
                                "DELETE refused and NOTHING was deleted: {url} holds shards of                                  `{collection}` and did not answer ({e}); a delete by predicate                                  selects its keys from every holder first"
                            )));
                        }
                        r?;
                    }
                    let left = crate::deadline::remaining_ms();
                    Ok(Resume::new(move |db: &mut Db| {
                        let _deadline = crate::deadline::arm(left);
                        db.delete_where(d, &sql, &params)
                    }))
                })))
            }
            Statement::Select(sel) => {
                Ok(Outcome::Rows(self.run_select(&sel, sql, params, analyze)?))
            }
            Statement::Flush { collection } => {
                let n = self.flush(&collection)?;
                Ok(Outcome::Ack(format!("{n} shard(s) flushed")))
            }
            Statement::Backup { to, keep, as_of, cluster } => {
                if cluster {
                    self.backup_cluster(&to, keep)
                } else {
                    self.backup(&to, keep, as_of)
                }
            }
            Statement::Restore { from, node, as_of } => self.restore(&from, node.as_deref(), as_of),
            Statement::VerifyBackup { from, node, as_of } => {
                self.verify_backup(&from, node.as_deref(), as_of)
            }
            Statement::Compact { collection } => {
                let n = self.compact(&collection)?;
                Ok(Outcome::Ack(format!("{n} compaction job(s) run")))
            }
            Statement::ShowSegments { collection } => {
                let ts = self.clock.peek();
                let mut out = String::from("shard  segment  level  docs      vectors   dead\n");
                for (i, s) in self.shards(&collection)?.iter().enumerate() {
                    for (id, level, docs, vecs, dead) in s.segment_summary(ts) {
                        out.push_str(&format!(
                            "{i:<6} {id:<8} {level:<6} {docs:<9} {vecs:<9} {:.1}%\n",
                            dead * 100.0
                        ));
                    }
                    out.push_str(&format!(
                        "{i:<6} memtable {:<6} {:<9} {:<9} -\n",
                        "-",
                        s.memtable.len(),
                        s.memtable.num_vectors()
                    ));
                }
                Ok(Outcome::Ack(out))
            }
            Statement::ShowHealth => Ok(Outcome::Ack(self.show_health())),
            Statement::ShowCatalog { collection } => {
                let names: Vec<String> = match collection {
                    Some(c) => vec![c],
                    None => self.catalog.collections.keys().cloned().collect(),
                };
                let mut out = String::new();
                for n in names {
                    self.absorb_shard_catalogs(&n)?;
                    let c = self.catalog.get(&n)?;
                    out.push_str(&format!(
                        "collection {} (pk={}, partition_by={:?}, docs={}, prefix_expansion={})\n",
                        c.name,
                        c.primary_key,
                        c.partition_key,
                        c.doc_count,
                        c.prefix_cap()
                    ));
                    if let Some(tablets) = self.catalog.placement.get(&n) {
                        for (i, t) in tablets.iter().enumerate() {
                            if t.is_merged() {
                                out.push_str(&format!("  shard {i} merged away (owns no key)\n"));
                                continue;
                            }
                            out.push_str(&format!(
                                "  shard {i} on {} [{}, {}){}{}\n",
                                if self.is_self(&t.node) { "this node" } else { t.node.as_str() },
                                t.lo.as_deref().unwrap_or(""),
                                t.hi.as_deref().unwrap_or(""),
                                if t.followers.is_empty() {
                                    String::new()
                                } else {
                                    format!(" followed by {}", t.followers.join(", "))
                                },
                                if t.term > 0 {
                                    format!(" term {}", t.term)
                                } else {
                                    String::new()
                                }
                            ));
                        }
                    }
                    for i in &c.indexes {
                        // The tier belongs here, not only in `SHOW RESIDENCY`:
                        // residency reports what is decoded right now, so an
                        // index nothing has queried is invisible there, and an
                        // operator has no other way to see what they declared.
                        let now = if i.tier == i.declared_tier {
                            String::new()
                        } else {
                            format!(" (declared {})", i.declared_tier.name())
                        };
                        out.push_str(&format!(
                            "  index {} on {} {:?} tier={}{}\n",
                            i.name,
                            i.path,
                            i.kind,
                            i.tier.name(),
                            now
                        ));
                    }
                    for (p, st) in &c.paths {
                        out.push_str(&format!(
                            "  path {:<24} {:?} present={} distinct≈{}\n",
                            p,
                            st.classify(c.doc_count),
                            st.present,
                            st.approx_cardinality()
                        ));
                    }
                }
                Ok(Outcome::Ack(out))
            }
            Statement::MeasureRecall { collection, k, samples } => {
                // The harness goes at the shards directly rather than through
                // `run_select`, so without this a recall cron job exercising an
                // index every hour would not stop an inactivity rule archiving
                // it out from under itself.
                let vec_paths: Vec<(String, IndexUse)> = self
                    .catalog
                    .get(&collection)?
                    .indexes
                    .iter()
                    .filter(|i| matches!(i.kind, IndexKind::Vector { .. }))
                    .map(|i| (i.path.clone(), IndexUse::Vector))
                    .collect();
                self.touch_indexes(&collection, &vec_paths)?;
                let r = crate::harness::measure_recall(self, &collection, k, samples)?;
                Ok(Outcome::Recall(r))
            }
            Statement::AlterIndexTier { collection, index, tier } => {
                let from = self.set_index_tier(&collection, &index, tier)?;
                Ok(Outcome::Ack(format!(
                    "index `{index}` moved {} -> {}",
                    from.name(),
                    tier.name()
                )))
            }
            Statement::AlterCollection { collection, prefix_expansion, replicas, nodes_of } => {
                let mut acks = Vec::new();
                if let Some(n) = replicas {
                    let placed = self.set_replicas(&collection, n)?;
                    acks.push(format!("replicas {placed} -> {n}"));
                }
                if let Some(cap) = prefix_expansion {
                    let from = self.set_prefix_expansion(&collection, cap)?;
                    acks.push(format!(
                        "prefix_expansion {from} -> {cap}; its statements may now name {} \
                         distinct prefix(es)",
                        prefix_leaves_limit(cap)
                    ));
                }
                if let Some(of) = nodes_of {
                    self.set_nodes_of(&collection, &of)?;
                    acks.push(format!("its edges point into `{of}`"));
                }
                Ok(Outcome::Ack(format!("collection `{collection}` {}", acks.join("; "))))
            }
            Statement::CreateLifecyclePolicy(d) => {
                let name = d.name.clone();
                self.create_policy(LifecyclePolicy {
                    name: d.name,
                    collection: d.collection,
                    indexes: d.indexes,
                    rules: d.rules,
                })?;
                Ok(Outcome::Ack(format!("lifecycle policy `{name}` created")))
            }
            Statement::DropLifecyclePolicy { name } => {
                self.drop_policy(&name)?;
                Ok(Outcome::Ack(format!("lifecycle policy `{name}` dropped")))
            }
            Statement::DropCollection { name } => {
                self.drop_collection(&name)?;
                Ok(Outcome::Ack(format!("collection `{name}` dropped")))
            }
            Statement::AttachNode { url } => {
                self.attach_node(&url)?;
                Ok(Outcome::Ack(format!("node {url} attached")))
            }
            Statement::DetachNode { url } => {
                self.detach_node(&url)?;
                Ok(Outcome::Ack(format!("node {url} detached")))
            }
            Statement::MoveShard { collection, shard, to } => {
                self.move_shard(&collection, shard, &to)
            }
            Statement::Rebalance { collection } => self.rebalance(&collection),
            Statement::PlaceShard { collection, shard, node } => {
                self.place_shard(&collection, shard, &node)?;
                Ok(Outcome::Ack(format!("shard {shard} of `{collection}` placed on {node}")))
            }
            Statement::Local(inner) => match *inner {
                Statement::SplitShard { collection, shard, at } => {
                    self.split_shard(&collection, shard, at.as_deref(), true)
                }
                Statement::MergeShards { collection, a, b } => {
                    self.merge_shards(&collection, a, b, true)
                }
                Statement::PromoteShard { collection, shard, node, term } => {
                    self.promote_shard(&collection, shard, &node, term, true)
                }
                other => self.run_one(other, sql, params, analyze),
            },
            Statement::SplitShard { collection, shard, at } => {
                self.split_shard(&collection, shard, at.as_deref(), false)
            }
            Statement::MergeShards { collection, a, b } => {
                self.merge_shards(&collection, a, b, false)
            }
            Statement::PromoteShard { collection, shard, node, term } => {
                self.promote_shard(&collection, shard, &node, term, false)
            }
            Statement::DropIndex { collection, index } => {
                self.drop_index(&collection, &index)?;
                Ok(Outcome::Ack(format!("index `{index}` dropped from `{collection}`")))
            }
            Statement::RunLifecycle { collection } => {
                let run = self.run_lifecycle(collection.as_deref())?;
                let mut out = String::new();
                let mut note_minimal = false;
                for t in &run.moves {
                    out.push_str(&format!("{t}\n"));
                    note_minimal |= t.to == Tier::Minimal;
                }
                if note_minimal {
                    // Worth saying, because the same transition has opposite
                    // effects per node and neither is wrong: `minimal` reclaims
                    // memory everywhere except on the one node that keeps the
                    // copy, which is the entire point of the tier.
                    out.push_str(
                        "note: a move to `minimal` frees nothing on the node designated to hold \
                         the index; it frees the copy on every other replica\n",
                    );
                }
                if run.moves.is_empty() && run.failures.is_empty() {
                    out.push_str("no index is due to move\n");
                } else {
                    out.push_str(&format!("{} transition(s)\n", run.moves.len()));
                }
                for (c, e) in &run.failures {
                    out.push_str(&format!("FAILED on `{c}`: {e}\n"));
                }
                Ok(Outcome::Ack(out))
            }
            Statement::UnloadIdle { collection } => {
                let (idle, evicted) = self.unload_idle(collection.as_deref())?;
                Ok(Outcome::Ack(format!(
                    "released {} idle, {} over-budget; {} resident of {} budget\n",
                    bytes(idle),
                    bytes(evicted),
                    bytes(self.residency.resident_bytes()),
                    bytes(self.residency.opts().budget_bytes),
                )))
            }
            Statement::ShowResidency { collection } => {
                Ok(Outcome::Ack(self.render_residency(collection.as_deref())?))
            }
            Statement::ShowLifecycle => Ok(Outcome::Ack(self.render_lifecycle())),
        }
    }

    // ------------------------------------------------------- tiers, lifecycle

    /// Move one index to a tier by operator command. This restates the
    /// baseline, so a later access will not undo it.
    /// Set how many dictionary terms a prefix on `collection` expands to, and
    /// persist it. Refused outside `1..=PREFIX_EXPANSION_CEILING`; the leaf
    /// budget its statements are held to follows from it, see
    /// `prefix_leaves_limit`. Returns the cap that was in force. Takes effect
    /// for the next statement: an expansion is resolved per statement and
    /// never cached, and the budget is derived when the statement runs.
    pub fn set_prefix_expansion(&mut self, collection: &str, cap: usize) -> Result<usize> {
        check_prefix_cap(cap)?;
        let c = self.catalog.get_mut(collection)?;
        let before = c.prefix_expansion;
        let from = c.prefix_cap();
        c.prefix_expansion = Some(cap);
        let coll = c.clone();
        if let Some(shards) = self.shards.get_mut(collection) {
            for s in shards.iter_mut() {
                s.adopt_definition(coll.clone());
            }
        }
        // As `set_index_tier`: a setting the disk did not take is not a
        // setting, and leaving it in memory would let the next unrelated
        // persist write it.
        if let Err(e) = self.persist_catalog() {
            if let Ok(c) = self.catalog.get_mut(collection) {
                c.prefix_expansion = before;
                let coll = c.clone();
                if let Some(shards) = self.shards.get_mut(collection) {
                    for s in shards.iter_mut() {
                        s.adopt_definition(coll.clone());
                    }
                }
            }
            return Err(e);
        }
        Ok(from)
    }

    /// An edge collection points into a node collection that exists and is
    /// not itself; the columns a walk reads are declared when the adjacency
    /// index is, not here.
    fn check_nodes_of(&self, collection: &str, of: &str) -> Result<()> {
        if of == collection {
            return Err(Error::Schema(format!(
                "`{collection}` cannot be its own node collection; an edge collection's \
                 nodes_of names the collection its src and dst are primary keys of"
            )));
        }
        let target = self.catalog.get(of).map_err(|_| {
            Error::Schema(format!(
                "nodes_of names `{of}`, which does not exist; create the node collection first"
            ))
        })?;
        if target.nodes_of.is_some() {
            return Err(Error::Schema(format!(
                "nodes_of names `{of}`, which is itself an edge collection (its edges point \
                 into `{}`); edges point at nodes, not at edges",
                target.nodes_of.as_deref().unwrap_or("")
            )));
        }
        Ok(())
    }

    /// An adjacency index needs an edge collection and two declared text
    /// columns: the walk reads them as keys, and a column that could hold
    /// anything else is a walk that silently skips edges.
    fn check_adjacency(&self, collection: &str, from: &str, to: &str) -> Result<()> {
        let c = self.catalog.get(collection)?;
        if c.nodes_of.is_none() {
            return Err(Error::Schema(format!(
                "`{collection}` is not an edge collection; an adjacency index needs one: \
                 CREATE COLLECTION ... WITH (nodes_of = '<node collection>'), or ALTER \
                 COLLECTION {collection} SET (nodes_of = ...)"
            )));
        }
        if let Some(a) = c.adjacency_index() {
            return Err(Error::Schema(format!(
                "`{collection}` already has an adjacency index, `{}`; a walk uses one",
                a.name
            )));
        }
        if from == to {
            return Err(Error::Schema(format!(
                "an adjacency index probes one column and reads another; both are `{from}`"
            )));
        }
        for col in [from, to] {
            match c.declared.iter().find(|d| d.path == col) {
                Some(d) if d.ty == ValueType::Str => {}
                Some(d) => {
                    return Err(Error::Schema(format!(
                        "`{col}` is declared {} and an adjacency index reads keys: declare \
                         it TEXT",
                        d.ty.name()
                    )))
                }
                None => {
                    return Err(Error::Schema(format!(
                        "`{col}` is not a declared column of `{collection}`; an adjacency \
                         index reads declared TEXT columns"
                    )))
                }
            }
        }
        Ok(())
    }

    /// `ALTER COLLECTION ... SET (nodes_of = ...)`: the same setting the
    /// `CREATE` takes, for a collection loaded before it had a walk.
    pub fn set_nodes_of(&mut self, collection: &str, of: &str) -> Result<()> {
        self.check_nodes_of(collection, of)?;
        let c = self.catalog.get_mut(collection)?;
        let before = c.nodes_of.clone();
        c.nodes_of = Some(of.to_string());
        let coll = c.clone();
        if let Some(shards) = self.shards.get_mut(collection) {
            for s in shards.iter_mut() {
                s.adopt_definition(coll.clone());
            }
        }
        if let Err(e) = self.persist_catalog() {
            if let Ok(c) = self.catalog.get_mut(collection) {
                c.nodes_of = before;
                let coll = c.clone();
                if let Some(shards) = self.shards.get_mut(collection) {
                    for s in shards.iter_mut() {
                        s.adopt_definition(coll.clone());
                    }
                }
            }
            return Err(e);
        }
        Ok(())
    }

    pub fn set_index_tier(&mut self, collection: &str, index: &str, tier: Tier) -> Result<Tier> {
        let c = self.catalog.get_mut(collection)?;
        let Some(def) = c.indexes.iter_mut().find(|i| i.name == index) else {
            return Err(Error::Plan(format!("no index `{index}` on collection `{collection}`")));
        };
        let from = def.tier;
        let declared_before = def.declared_tier;
        // The baseline restates the intent, so a later access will not undo it;
        // the effective tier moves only if the files can follow it.
        def.declared_tier = tier;
        let key = (collection.to_string(), index.to_string());
        let demoted_before = self.catalog.activity.get(&key).and_then(|a| a.demoted_by);
        if let Some(a) = self.catalog.activity.get_mut(&key) {
            a.demoted_by = None;
        }
        if let Err(e) = self.commit_tiers(collection, &[(index.to_string(), tier)]) {
            // `commit_tiers` restores the effective tier and nothing else. Without
            // these two the caller is told the move failed while the catalog keeps
            // a baseline the files never took and a retention pin that was dropped
            // — and the next unrelated successful persist writes both to disk.
            if let Ok(c) = self.catalog.get_mut(collection) {
                if let Some(d) = c.indexes.iter_mut().find(|i| i.name == index) {
                    d.declared_tier = declared_before;
                }
            }
            if let Some(a) = self.catalog.activity.get_mut(&key) {
                a.demoted_by = demoted_before;
            }
            return Err(e);
        }
        Ok(from)
    }

    /// Push the catalog's tiers down to every segment of a collection, and put
    /// the segment files where those tiers say they belong.
    fn apply_tiers(&mut self, collection: &str) -> Result<()> {
        let coll = self.catalog.get(collection)?.clone();
        // Resolved once for the collection, not once per segment: the answer is
        // the same for every segment of every shard, and computing it per
        // segment means re-sorting the replica list and re-hashing every
        // component for each one.
        let resolved = self.opts.placement.resolve_tiers(&coll);
        let Some(shards) = self.shards.get_mut(collection) else { return Ok(()) };
        for s in shards.iter_mut() {
            s.adopt_definition(coll.clone());
            for h in &s.segments {
                h.segment.set_tiers(resolved.clone());
                // Straight into the ledger, not at the next access: an index
                // demoted because nobody queries it would otherwise keep its
                // old eviction priority until somebody does.
                h.segment.refresh_ledger_tiers();
            }
            s.sync_archive()?;
        }
        Ok(())
    }

    /// Apply a set of tier changes to one collection, or leave it as it was.
    ///
    /// `apply_tiers` moves files, so it can fail. A failure that leaves the
    /// catalog claiming a tier the files do not have is worse than not moving
    /// at all, because the next unrelated write persists the lie and nothing
    /// afterwards reconciles it.
    fn commit_tiers(&mut self, collection: &str, changes: &[(String, Tier)]) -> Result<()> {
        let before: Vec<(String, Tier)> = self
            .catalog
            .get(collection)?
            .indexes
            .iter()
            .map(|i| (i.name.clone(), i.tier))
            .collect();
        {
            let c = self.catalog.get_mut(collection)?;
            for (name, tier) in changes {
                if let Some(d) = c.indexes.iter_mut().find(|i| &i.name == name) {
                    d.tier = *tier;
                }
            }
        }
        if let Err(e) = self.apply_tiers(collection) {
            let c = self.catalog.get_mut(collection)?;
            for (name, tier) in before {
                if let Some(d) = c.indexes.iter_mut().find(|i| i.name == name) {
                    d.tier = tier;
                }
            }
            let _ = self.apply_tiers(collection);
            return Err(e);
        }
        self.catalog.version += 1;
        self.persist_catalog()
    }

    pub fn create_policy(&mut self, p: LifecyclePolicy) -> Result<()> {
        // Refuse a policy for a collection that does not exist: a typo here is
        // silent for days otherwise, and only shows up as data that never moved.
        self.catalog.get(&p.collection)?;
        let coll = self.catalog.get(&p.collection)?;
        for i in &p.indexes {
            if coll.index_by_name(i).is_none() {
                return Err(Error::Plan(format!(
                    "policy `{}` names index `{i}`, which does not exist on `{}`",
                    p.name, p.collection
                )));
            }
        }
        if p.rules.is_empty() {
            return Err(Error::Plan(format!("policy `{}` has no rules", p.name)));
        }
        // A policy name is global, so an insert would silently delete a policy
        // of the same name on another collection. `CREATE COLLECTION` and
        // `CREATE INDEX` both refuse a duplicate; so does this.
        if let Some(old) = self.catalog.policies.get(&p.name) {
            return Err(Error::Plan(format!(
                "lifecycle policy `{}` already exists on `{}`; DROP it first",
                p.name, old.collection
            )));
        }
        // A rule can only move an index further from RAM. Promotion is what an
        // access does; a rule that promotes would fight every query.
        if let Some(r) = p.rules.iter().find(|r| r.to == Tier::Active) {
            return Err(Error::Plan(format!(
                "`MOVE TO active AFTER {}` would never fire: a policy only demotes, and an \
                 index returns to its declared tier when it is used",
                r.after
            )));
        }
        self.catalog.policies.insert(p.name.clone(), p);
        self.catalog.version += 1;
        self.persist_catalog()
    }

    pub fn drop_policy(&mut self, name: &str) -> Result<()> {
        let Some(gone) = self.catalog.policies.remove(name) else {
            return Err(Error::Plan(format!("no lifecycle policy `{name}`")));
        };
        // A `demoted_by` pin exists only so that a retention rule and
        // access-promotion do not fight: it says "a rule put this here, and a
        // query is not evidence against it". With the rule gone, and no other
        // policy left covering the index, there is nothing to fight and the
        // pin is simply permanent -- the index can never be promoted back by
        // an access, and nothing else in the system clears it.
        let policies = &self.catalog.policies;
        for ((c, i), act) in self.catalog.activity.iter_mut() {
            if act.demoted_by.is_none() || *c != gone.collection || !gone.covers(i) {
                continue;
            }
            if !policies.values().any(|p| p.collection == *c && p.covers(i)) {
                act.demoted_by = None;
            }
        }
        self.catalog.version += 1;
        self.persist_catalog()
    }

    /// Evaluate the policies and carry out what they call for.
    ///
    /// Explicit rather than background, for the same reason compaction is
    /// (§12.1): a tiering decision that moves gigabytes should be schedulable
    /// and visible, not a surprise.
    pub fn run_lifecycle(&mut self, collection: Option<&str>) -> Result<LifecycleRun> {
        let now = lifecycle::now_micros(&self.clock);
        let names: Vec<String> = match collection {
            Some(c) => {
                self.catalog.get(c)?;
                vec![c.to_string()]
            }
            None => self.catalog.collections.keys().cloned().collect(),
        };
        let mut all = Vec::new();
        let mut failures: Vec<(String, String)> = Vec::new();
        for name in names {
            let indexes: Vec<(String, Tier)> =
                self.catalog.get(&name)?.indexes.iter().map(|i| (i.name.clone(), i.tier)).collect();
            let activity: BTreeMap<String, IndexActivity> = self
                .catalog
                .activity
                .iter()
                .filter(|((c, _), _)| *c == name)
                .map(|((_, i), a)| (i.clone(), *a))
                .collect();
            let moves = lifecycle::plan(&self.catalog.policies, &name, &indexes, &activity, now);
            if moves.is_empty() {
                continue;
            }
            let changes: Vec<(String, Tier)> =
                moves.iter().map(|t| (t.index.clone(), t.to)).collect();
            if let Err(e) = self.commit_tiers(&name, &changes) {
                // One collection whose files will not move must not hide the
                // collections that already moved. Report it and carry on.
                failures.push((name.clone(), e.to_string()));
                continue;
            }
            for t in &moves {
                let e = self
                    .catalog
                    .activity
                    .entry((name.clone(), t.index.clone()))
                    .or_insert_with(|| IndexActivity::new(now));
                e.demoted_by = Some(t.trigger);
            }
            self.persist_catalog()?;
            all.extend(moves);
        }
        self.lifecycle_checked_at_writes = self.writes;
        Ok(LifecycleRun { moves: all, failures })
    }

    /// Release idle components, then evict down to the node budget. Returns
    /// `(idle bytes, over-budget bytes)`.
    pub fn unload_idle(&mut self, collection: Option<&str>) -> Result<(usize, usize)> {
        // The wall clock, not the HLC's physical component, because the stamps
        // this is about to be subtracted from are wall-clock stamps written by
        // `Segment::acquire`. The HLC is monotone and absorbs remote
        // timestamps, so it can only ever run *ahead* of wall time: mixing the
        // two makes every component look idle by however far ahead it is, and
        // the sweeper unloads components that were touched a moment ago.
        let now = crate::time::now_micros() as u64;
        let names: Vec<String> = match collection {
            Some(c) => {
                self.catalog.get(c)?;
                vec![c.to_string()]
            }
            None => self.shards.keys().cloned().collect(),
        };
        let mut idle = 0;
        for n in &names {
            if let Some(shards) = self.shards.get(n) {
                for s in shards {
                    idle += s.unload_idle(now);
                }
            }
        }
        Ok((idle, self.sweep_residency()))
    }

    /// Release every decoded component, whatever its tier or idle time.
    ///
    /// What a node does when it is about to be idle for a while, and what a
    /// test does to prove that residency is not load-bearing. Returns the bytes
    /// released.
    pub fn sweep_all(&mut self) -> Result<usize> {
        let mut freed = 0;
        for shards in self.shards.values() {
            for s in shards {
                for h in &s.segments {
                    freed += h.segment.unload_all();
                }
            }
        }
        Ok(freed)
    }

    /// Evict, coldest tier and stalest first, until the node is back inside
    /// its budget. Returns the bytes released.
    pub fn sweep_residency(&mut self) -> usize {
        let budget = self.residency.opts().budget_bytes;
        let resident = self.residency.resident_bytes();
        if resident <= budget {
            return 0;
        }
        let victims = self.residency.plan_evictions(resident - budget);
        let mut freed = 0;
        // Matched by residency uid, not segment id: ids are assigned per shard,
        // so two tablets both have a segment 1 and an id match would evict from
        // whichever one it walked into.
        for (uid, component) in victims {
            for shards in self.shards.values() {
                for s in shards {
                    for h in &s.segments {
                        if h.segment.uid() == uid {
                            freed += h.segment.unload_component(&component);
                        }
                    }
                }
            }
        }
        freed
    }

    /// Record that a query touched these indexes. This is what "last accessed"
    /// means for an inactivity rule, and it is also what promotes an index back
    /// toward its declared tier.
    /// A read's record of what it used: applied by [`apply_touches`](Self::apply_touches).
    fn note_touches(&self, collection: &str, used: &[(String, IndexUse)]) {
        if !used.is_empty() {
            guard(&self.touches).push((collection.to_string(), used.to_vec()));
        }
    }

    /// Whether a read has left touches for a writer to apply.
    pub fn touches_pending(&self) -> bool {
        !guard(&self.touches).is_empty()
    }

    /// Apply what reads noted since the last time: the access clocks, a
    /// promotion for a demoted index that was used, the clocks persisted
    /// if stale. Called by every statement that holds `&mut self`, and by
    /// the console right after a read that noted something, so a fault-in
    /// still promotes within the request that caused it.
    pub fn apply_touches(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut *guard(&self.touches));
        for (collection, used) in pending {
            self.touch_indexes(&collection, &used)?;
        }
        Ok(())
    }

    fn touch_indexes(&mut self, collection: &str, used: &[(String, IndexUse)]) -> Result<()> {
        if used.is_empty() {
            return Ok(());
        }
        let now = lifecycle::now_micros(&self.clock);
        let touched: Vec<(String, bool)> = {
            let Ok(coll) = self.catalog.get(collection) else { return Ok(()) };
            coll.indexes
                .iter()
                .filter(|i| used.iter().any(|(p, u)| p == &i.path && u.matches(&i.kind)))
                .map(|i| (i.name.clone(), i.tier.is_colder_than(i.declared_tier)))
                .collect()
        };
        let mut promote: Vec<String> = Vec::new();
        for (name, demoted) in touched {
            let e = self
                .catalog
                .activity
                .entry((collection.to_string(), name.clone()))
                .or_insert_with(|| IndexActivity::new(now));
            e.last_access_micros = now;
            if demoted && e.promotable() {
                e.demoted_by = None;
                promote.push(name);
            }
        }
        if !promote.is_empty() {
            let changes: Vec<(String, Tier)> = {
                let c = self.catalog.get(collection)?;
                promote
                    .iter()
                    .filter_map(|n| c.index_by_name(n).map(|d| (d.name.clone(), d.declared_tier)))
                    .collect()
            };
            // Through `commit_tiers`, not straight into the catalog: a
            // promotion un-archives segment files, and that rename can fail.
            // Persisted immediately once it lands, because a restart that finds
            // the catalog saying `archived` while every byte is local has no
            // way to notice.
            return self.commit_tiers(collection, &changes);
        }
        self.persist_activity_if_stale(now)
    }

    /// Persist the access clocks, at most once per [`ACTIVITY_PERSIST_MICROS`].
    ///
    /// The clocks have to survive a restart — "idle for seven days" that resets
    /// on every deploy is not a policy — but writing the catalog on every read
    /// would make a read a write. The lifecycle DSL's finest unit is a minute,
    /// so a clock persisted to within a minute is exact at the resolution
    /// anybody can express.
    fn persist_activity_if_stale(&mut self, now: u64) -> Result<()> {
        if self.dir.is_none() {
            return Ok(());
        }
        if now.saturating_sub(self.activity_persisted_micros) < ACTIVITY_PERSIST_MICROS {
            return Ok(());
        }
        self.activity_persisted_micros = now;
        self.persist_catalog()
    }

    fn render_residency(&self, collection: Option<&str>) -> Result<String> {
        let names: Vec<String> = match collection {
            Some(c) => {
                self.catalog.get(c)?;
                vec![c.to_string()]
            }
            None => self.shards.keys().cloned().collect(),
        };
        // Wall time, for the same reason as `unload_idle`: `last` below is a
        // wall-clock stamp, and the idle column is their difference.
        let now = crate::time::now_micros() as u64;
        let mut out = String::from(
            "collection            shard  segment  component        tier      bytes      idle\n",
        );
        let mut total = 0usize;
        for n in &names {
            let Some(shards) = self.shards.get(n) else { continue };
            for (i, s) in shards.iter().enumerate() {
                let mut rows = s.residency_rows();
                rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                for (id, comp, tier, last, b) in rows {
                    total += b;
                    out.push_str(&format!(
                        "{:<21} {:<6} {:<8} {:<16} {:<9} {:<10} {}\n",
                        n,
                        i,
                        id,
                        comp,
                        tier.name(),
                        bytes(b),
                        lifecycle::render_micros(now.saturating_sub(last)),
                    ));
                }
            }
        }
        let o = self.residency.opts();
        out.push_str(&format!(
            "\nresident {} of {} budget (peak {}), {} load(s), {} unload(s), {} fault(s) from \
             archive\n",
            bytes(total),
            bytes(o.budget_bytes),
            bytes(self.residency.peak_bytes()),
            self.residency.loads(),
            self.residency.unloads(),
            self.residency.faults(),
        ));
        let window = |d: Option<std::time::Duration>| match d {
            Some(d) => format!("{}s", d.as_secs()),
            None => "never".into(),
        };
        out.push_str(&format!(
            "idle unload: active {}, minimal {}, cached {}s, archived {}s\n",
            window(o.active_idle_unload),
            window(o.minimal_idle_unload),
            o.cached_idle_unload.as_secs(),
            o.archived_idle_unload.as_secs(),
        ));
        let p = self.placement();
        if p.replicas.len() > 1 {
            out.push_str(&format!(
                "placement: node `{}` of {} replica(s); `minimal` indexes it does not hold \
                 resolve to cached\n",
                p.node_id,
                p.replicas.len()
            ));
        }
        Ok(out)
    }

    fn render_lifecycle(&self) -> String {
        if self.catalog.policies.is_empty() {
            return "no lifecycle policies\n".into();
        }
        let now = lifecycle::now_micros(&self.clock);
        let mut out = String::new();
        for p in self.catalog.policies.values() {
            out.push_str(&p.render());
            out.push('\n');
            let Ok(c) = self.catalog.get(&p.collection) else { continue };
            for i in c.indexes.iter().filter(|i| p.covers(&i.name)) {
                let key = (p.collection.clone(), i.name.clone());
                let (idle, age) = match self.catalog.activity.get(&key) {
                    Some(a) => (
                        lifecycle::render_micros(now.saturating_sub(a.last_access_micros)),
                        lifecycle::render_micros(now.saturating_sub(a.created_micros)),
                    ),
                    None => ("never used".into(), "unknown".into()),
                };
                let pinned =
                    self.catalog.activity.get(&key).map(|a| !a.promotable()).unwrap_or(false);
                out.push_str(&format!(
                    "    {:<20} tier={:<9} declared={:<9} idle={:<14} age={:<14}{}\n",
                    i.name,
                    i.tier.name(),
                    i.declared_tier.name(),
                    idle,
                    age,
                    // Why traffic will not bring this one back.
                    if pinned { "(pinned by a retention rule)" } else { "" }
                ));
            }
        }
        out
    }

    pub fn run_select(
        &self,
        sel: &Select,
        sql: &str,
        params: &[Value],
        analyze: bool,
    ) -> Result<QueryResult> {
        // The statement's budget: none if it said `no_deadline`, its own if
        // it named one, else the `Db`'s.
        let budget = if sel.with.no_deadline {
            None
        } else {
            sel.with.deadline_ms.or(self.opts.statement_deadline_ms)
        };
        let _deadline = crate::deadline::arm(budget);
        // Before the query, not after: an index the query is about to fault in
        // from cold storage counts as used even if the query then fails.
        self.note_touches(&sel.collection, &index_uses(sel));
        // Read-your-writes: pin at least the last commit timestamp this client
        // observed (§6).
        let ts = self.clock.peek().max(self.last_commit);
        let coll = self.planning_collection(&sel.collection)?;
        if let Some(path) = exec::undeclared_text_path(&coll, sel) {
            return Err(Error::Plan(format!(
                "no full-text index on `{path}`; CREATE INDEX ... USING fulltext ({path})"
            )));
        }
        self.log_vector_queries(sel);
        let partial = sel.with.partial_results;
        // The snapshot and the epoch: this node's clock and write count,
        // raised by every other holder's, so that a write that landed on
        // another node is read and ages the statistics here.
        self.refuse_if_fenced(&sel.collection)?;
        let tablets = self.catalog.placement.get(&sel.collection).cloned().unwrap_or_default();
        let mut ts = ts;
        let mut writes = self.writes_to(&sel.collection);
        let mut unreachable: Vec<usize> = Vec::new();
        let mut remotes: BTreeMap<String, Arc<crate::wire::Node>> = BTreeMap::new();
        // Every holder's counters, for the read-your-writes instant and the
        // statistics epoch. A holder that does not answer here is not yet
        // the statement's failure: under `partial_results` its shards are
        // missing from here on; without it the statement goes on, and fails
        // at the first shard call it makes to that node -- so a statement
        // that never asks the lost node's shards answers (a key the
        // predicate pins to a live shard, a partition on a live shard).
        // Until 0.34.0 the counters call itself failed every statement.
        // Every holder is asked at once, and under `partial_results` for
        // half the budget: a holder behind a partition answers nothing
        // until the deadline, and five of them one after another spent a
        // partial statement's whole budget before the near shards were
        // asked, which then counted as missing too. Half to learn who is
        // there, half to read from those who are.
        let mut conns = Vec::new();
        for url in self.holders(&sel.collection) {
            conns.push((url.clone(), self.node_conn(&url)?));
        }
        for (url, node, r) in fetch_counters(&conns, &sel.collection, partial) {
            match r {
                Ok((t, w)) => {
                    ts = ts.max(t);
                    writes += w;
                }
                Err(Error::Deadline(_)) if partial => {
                    unreachable.extend(
                        tablets
                            .iter()
                            .enumerate()
                            .filter(|(_, t)| t.node == url && !t.is_merged())
                            .map(|(i, _)| i),
                    );
                }
                Err(Error::Deadline(_)) => {}
                Err(e) => return Err(e),
            }
            remotes.insert(url, node);
        }
        let here = self.shards_here(&sel.collection);
        let services = services_for(here, &tablets, &remotes, &sel.collection, self.sim.clone());
        self.run_over(&coll, &services, sel, sql, params, analyze, ts, writes, partial, unreachable)
    }

    /// The statement over an explicit set of shard services: prefix
    /// expansion, the statistics, then the executor. What `run_select` calls
    /// after choosing the services and the instant.
    #[allow(clippy::too_many_arguments)]
    fn run_over(
        &self,
        coll: &Collection,
        services: &[Box<dyn ShardService + '_>],
        sel: &Select,
        sql: &str,
        params: &[Value],
        analyze: bool,
        ts: Timestamp,
        writes: u64,
        partial: bool,
        mut unreachable: Vec<usize>,
    ) -> Result<QueryResult> {
        // Every walk first: `WITHIN k HOPS OF` is resolved to a key set here,
        // at the same pinned instant as everything after it, and bound into
        // the statement as an `IN` before the prefixes, the statistics and
        // the scatter see it. Nothing below knows a walk happened, which is
        // the point: the hybrid intersection is untouched.
        let mut ts = ts;
        let mut frontiers: Vec<Vec<String>> = Vec::new();
        let mut hop_sources: Vec<Vec<Candidate>> = Vec::new();
        let mut walks = Vec::new();
        let mut cut_walks = Vec::new();
        let mut walk_missing = Vec::new();
        // The predicate's walks first, then the `hops(...)` sources of the
        // ORDER BY: the former bind as `IN` lists, the latter become
        // candidate lists scored by hop, and a shard on another node numbers
        // them the same way (`walk::walks_of`).
        let hops: Vec<Expr> = walk::walks_of(sel);
        let predicate_walks = sel.predicate.as_ref().map(|p| walk::hops_in(p).len()).unwrap_or(0);
        for (wi, h) in hops.iter().enumerate() {
            let Expr::Hops { path, k, start, via, reverse, filters } = h else { unreachable!() };
            let (edges, index) = self.check_walk(coll, path, *k, via, filters)?;
            self.note_touches(via, &[(index.path.clone(), IndexUse::Walk)]);
            // The edge collection's holders, for its instant and its shards.
            self.refuse_if_fenced(via)?;
            let tablets = self.catalog.placement.get(via).cloned().unwrap_or_default();
            let mut edge_unreachable: Vec<usize> = Vec::new();
            let mut remotes: BTreeMap<String, Arc<crate::wire::Node>> = BTreeMap::new();
            let mut conns = Vec::new();
            for url in self.holders(via) {
                conns.push((url.clone(), self.node_conn(&url)?));
            }
            for (url, node, r) in fetch_counters(&conns, via, partial) {
                match r {
                    Ok((t, _)) => ts = ts.max(t),
                    Err(Error::Deadline(_)) if !partial => {}
                    Err(Error::Deadline(_)) => {
                        for (i, t) in tablets.iter().enumerate() {
                            if t.node == url && !t.is_merged() {
                                edge_unreachable.push(i);
                                walk_missing.push(format!("{via} shard {i}"));
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
                remotes.insert(url, node);
            }
            let clause = format!(
                "WITHIN {k} HOPS OF '{start}' VIA {via}{}",
                if *reverse { " REVERSE" } else { "" }
            );
            let label = if wi < predicate_walks { clause } else { format!("hops({clause})") };
            let spec = WalkSpec {
                label,
                k: *k,
                start,
                reverse: *reverse,
                filters,
                edges: &edges,
                index: &index,
                nodes: coll,
                walk: wi,
                statement: sql,
                params,
                max_frontier: sel.with.max_frontier,
                max_fanout: sel.with.max_fanout,
            };
            let out = {
                let edge_services =
                    services_for(self.shards_here(via), &tablets, &remotes, via, self.sim.clone());
                walk::walk(
                    &spec,
                    &edge_services,
                    &mut edge_unreachable,
                    services,
                    &mut unreachable,
                    ts,
                    partial,
                )?
            };
            if wi < predicate_walks {
                frontiers.push(out.keys);
            } else {
                // Scored by the hop a key was first reached at: nearer is
                // better, and fusion ranks it beside the other sources.
                let mut list = Vec::new();
                for (i, keys) in out.by_hop.iter().enumerate() {
                    for key in keys {
                        list.push(Candidate { key: key.clone(), raw_score: (i + 1) as f32 });
                    }
                }
                hop_sources.push(list);
            }
            walks.push(out.explain);
            cut_walks.extend(out.cuts);
            walk_missing.extend(out.missing);
        }
        let bound;
        let sel = if frontiers.is_empty() {
            sel
        } else {
            bound = walk::bind_hops(sel, &frontiers);
            &bound
        };
        let mut want = exec::required_terms(coll, sel);
        // Resolve every prefix in the statement HERE, once, before the gather.
        //
        // The cap is applied to the union over every unit of every shard, not
        // per unit, and that is the whole mechanism: a term is in the answer if
        // it is among the lexicographically first `PREFIX_EXPANSION_LIMIT` of
        // the collection's LIVE matching vocabulary, which is a property of the
        // collection at this instant and not of how it happens to be laid out.
        // Asking each unit for its own first `limit` live terms is sufficient —
        // `Shard::prefix_terms` proves it — so the work is bounded by the cap
        // plus whatever garbage it steps over, never by the vocabulary.
        //
        // This is a SECOND coordinator read at the same pinned `ts`, and the
        // two reads have to agree, which is why both mask at `ts`: this one
        // says which terms the query names, `term_stats` then measures how many
        // live documents hold each of them. A term whose last posting died
        // between them comes back with `df == 0` and matches nothing, which is
        // the same answer it would get had it never been enumerated.
        //
        // Deliberately NOT cached across queries, and the reason changed with
        // the masking, so it is worth stating precisely rather than leaving the
        // old one standing. It used to be a correctness argument: the list was
        // a function of the PHYSICAL dictionary, FLUSH and COMPACT rebuild that
        // without moving `Db::writes` — the counter `refreshed_at_writes` is
        // compared against — and a list cached against it survived a compaction
        // that changed it. Measured: 212 rows, then 212 again after a `COMPACT`
        // that should have made it 300.
        //
        // That is no longer true, because the list is now a function of the
        // live corpus at `ts` and FLUSH and COMPACT do not change that. What is
        // left is weaker and still sufficient: a cache key here would have to
        // be a complete clock for WHICH DOCUMENTS ARE LIVE, and `writes` is
        // only that by coincidence of counting exactly inserts and deletes
        // today. The list is cheap — bounded by the cap plus the garbage in
        // front of it — and it is read once per statement. Buying a
        // microsecond with a second thing that has to stay in step with
        // liveness forever is not a trade worth making.
        let prefixes_by_path = exec::required_prefixes(coll, sel);
        // One statement, one budget. Each DISTINCT prefix costs a dictionary
        // walk in every unit of every shard plus up to the collection's
        // `prefix_expansion` gathered frequencies, and nothing in the grammar bounds how many of
        // them a `text_match` string may name — measured at 24 in one
        // statement for 1.0 s a query, with the aggregate expansion (12288
        // terms) so far over `STATS_TERM_CAP` that the statement evicts its own
        // entries and never warms.
        //
        // A refusal, not a silent cut: the cut is the thing this whole change
        // exists to stop doing quietly, and a statement that names more than
        // the statistics cache can hold is a statement the operator wants to
        // see. The bound is exactly the number of full-cap expansions that fit
        // in `STATS_TERM_CAP`, which is a per-PATH cache — so the number is
        // where it is for that reason, while what it really bounds is the
        // aggregate expansion cost below, which is path-independent.
        //
        // What is counted is distinct `(path, prefix)` PAIRS, summed over the
        // statement's paths, because that is what
        // is paid for: the loop below resolves each `(path, prefix)` pair once
        // and every clause that spells it reads the same expansion. The
        // message says so. It used to say "this statement has N prefix
        // leaves", which is a different quantity from the one the bound is
        // taken over — a statement spelling `a*` twelve times has twelve
        // leaves, costs one expansion, and is admitted — so an operator
        // counting the leaves in their own statement could not predict, or
        // meet, this refusal.
        let cap = coll.prefix_cap();
        let leaves = prefix_leaves_limit(cap);
        let distinct: usize = prefixes_by_path.values().map(|v| v.len()).sum();
        if distinct > leaves {
            return Err(Error::Plan(format!(
                "this statement names {distinct} distinct prefixes across its indexed paths \
                 and the limit is {leaves}; each DISTINCT one expands against every unit of \
                 every shard and gathers up to {cap} document frequencies (the collection's \
                 prefix_expansion), which is the cost this bounds — {leaves} is how many \
                 full-cap expansions fit the {STATS_TERM_CAP}-term statistics cache each \
                 indexed path keeps, so the budget is {STATS_TERM_CAP} / prefix_expansion. \
                 Repeats of one prefix on one path are expanded and gathered once, which is \
                 what this limit counts; each occurrence is still evaluated separately in \
                 every unit. Split the statement, narrow the prefixes, or lower the \
                 collection's prefix_expansion."
            )));
        }
        // The same prune execution applies, applied to the EXPANSION too. A
        // prefix is resolved once for the whole statement, so resolving it over
        // the whole collection spends a per-statement cap on terms belonging to
        // partitions the statement cannot return a row from: with 600 terms per
        // tenant and a 512-term cap, a query scoped to the second tenant was
        // expanded entirely out of its own partition and answered zero rows.
        //
        // Safe because this prefix restricts EVERY row the statement can
        // return — `partition_constraint` derives it only from a top-level
        // conjunctive equality, never from a disjunct or a negation — and
        // `run_select` already ANDs the same `key_prefix` into the filter of
        // every unit. A term no document in the partition holds could only have
        // displaced one that is.
        let part = exec::partition_constraint(coll, sel.predicate.as_ref());
        let mut expansions: BTreeMap<String, BTreeMap<String, Expansion>> = BTreeMap::new();
        for (path, prefixes) in prefixes_by_path {
            for (p, used) in prefixes {
                // One MORE than the cap, from every unit, for the one thing
                // that cannot be recovered afterwards: whether the cap cut
                // anything. A union of exactly `cap` terms is what both "the
                // collection holds exactly this many" and "there were more"
                // look like, and the caller must not have to guess. Asking for
                // `cap + 1` per unit is enough to decide it globally by the
                // same sorted-order argument `Shard::prefix_terms` makes — if
                // the collection matches more than `cap` terms, each of the
                // `cap + 1` smallest is among the `cap + 1` smallest of
                // whichever unit holds it, so all of them reach the union.
                //
                // What it does NOT buy is how many were dropped: the
                // enumeration is bounded, so the honest report is "there are
                // more", not a count. Counting them means enumerating the whole
                // matching vocabulary, whose cost is exactly what the cap
                // exists to refuse.
                let mut union: BTreeSet<String> = BTreeSet::new();
                for s in services {
                    if unreachable.contains(&s.index()) {
                        continue;
                    }
                    match s.prefix_terms(&path, &p, ts, cap + 1, part.as_deref()) {
                        Ok(terms) => union.extend(terms),
                        Err(Error::Deadline(e)) => {
                            if !partial {
                                return Err(Error::Deadline(e));
                            }
                            unreachable.push(s.index());
                        }
                        Err(e) => return Err(e),
                    }
                }
                // Over LIVE terms, so it says the honest thing: the
                // collection really does hold more than `cap` matching terms a
                // visible document carries, and the answer really is short.
                // Decided over the physical dictionary it fired on complete
                // answers, which is a warning nobody could act on.
                let truncated = union.len() > cap;
                let terms: Vec<String> = union.into_iter().take(cap).collect();
                // The expanded terms join the statement's own terms, so the
                // gather is still one pass per path over one deduplicated list.
                //
                // A path whose every prefix is negated adds nothing here and
                // still has to reach `gather_stats`: without an entry in
                // `want` the loop below has nowhere to attach the resolved
                // `expansions`, every unit falls onto `scorer::build`'s
                // no-coordinator arm, and the per-unit expansion this whole
                // path exists to replace comes back. That entry is
                // `required_terms`' job and it already does it: the two
                // functions walk the same two sites behind the same
                // `fulltext_index` guard and the same `TextQuery::parse`, so
                // every path `required_prefixes` names is a path
                // `required_terms` named first — with an EMPTY term list when
                // nothing is scored, which is exactly the entry needed.
                // `prefixes_named_here_are_paths_required_terms_already_created`
                // pins that; creating the entry a second time here does not,
                // because a redundant write cannot fail when the invariant it
                // stands in for breaks.
                //
                // A leaf written `-a*` is compiled under `TextQuery::Not`,
                // whose scorer arm keeps the document ids and drops the scores,
                // so a `df` gathered for its terms is never read — the same
                // rule `leaf_terms` follows for a negated TERM, applied where a
                // prefix expansion joins the gather. Measured at 512 entries
                // per negation-only statement, which is an eighth of the cache
                // filled with terms nothing can score. The terms still go into
                // `Expansion` below either way: that list IS the exclusion set.
                //
                // Safe because a term absent from `doc_freq` scores
                // `idf_for_df(0)` — the largest weight there is, and positive —
                // so the disjunction's pivot still reaches every posting and
                // the exclusion bitmap is identical.
                if used.positive {
                    want.entry(path.clone()).or_default().extend(terms.iter().cloned());
                }
                expansions
                    .entry(path.clone())
                    .or_default()
                    .insert(p, Expansion { terms, truncated, used });
            }
            if let Some(v) = want.get_mut(&path) {
                v.sort();
                v.dedup();
            }
        }
        let mut stats = self.gather_stats_over(
            services,
            coll,
            &want,
            ts,
            sel.with.exact_scoring || sel.with.exact,
            partial,
            &mut unreachable,
            writes,
        )?;
        // Every path named here is a path `want` carries, so `gather_stats`
        // produced an entry for it.
        for (path, e) in expansions {
            if let Some(g) = stats.get_mut(&path) {
                g.expansions = e;
            }
        }
        exec::run_select(ExecInput {
            shards: services,
            unreachable: &unreachable,
            coll,
            params,
            select: sel,
            ts,
            stats: &stats,
            analyze,
            statement: sql.to_string(),
            frontiers: &frontiers,
            hop_sources,
            walks,
            cut_walks,
            walk_missing,
        })
    }

    /// Whether a walk can run: it selects by the node collection's primary
    /// key, over an edge collection that points into it, through an
    /// adjacency index warm enough to read at every hop, with a structured
    /// edge filter. The edge collection and the index, cloned, for the walk.
    fn check_walk(
        &self,
        coll: &Collection,
        path: &str,
        k: usize,
        via: &str,
        filters: &[Expr],
    ) -> Result<(Collection, IndexDef)> {
        if path != coll.primary_key {
            return Err(Error::Plan(format!(
                "a walk selects by primary key: `{path}` is not the primary key of `{}`, \
                 which is `{}`",
                coll.name, coll.primary_key
            )));
        }
        if k == 0 {
            return Err(Error::Plan(
                "WITHIN 0 HOPS OF selects nothing; a walk is at least one hop".into(),
            ));
        }
        let edges = self
            .catalog
            .get(via)
            .map_err(|_| Error::Plan(format!("no collection `{via}` to walk over")))?;
        if edges.nodes_of.as_deref() != Some(coll.name.as_str()) {
            return Err(Error::Plan(match &edges.nodes_of {
                Some(of) => format!(
                    "`{via}` is an edge collection of `{of}`, not of `{}`; a walk follows \
                     edges whose src and dst are keys of the collection it selects from",
                    coll.name
                ),
                None => format!(
                    "`{via}` is not an edge collection; CREATE COLLECTION ... WITH (nodes_of = \
                     '{}') or ALTER COLLECTION {via} SET (nodes_of = '{}'), then CREATE INDEX \
                     ... ON {via} USING adjacency (src, dst)",
                    coll.name, coll.name
                ),
            }));
        }
        let Some(index) = edges.adjacency_index() else {
            return Err(Error::Plan(format!(
                "no adjacency index on `{via}`; CREATE INDEX ... ON {via} USING adjacency \
                 (src, dst)"
            )));
        };
        // A walk reads the index at every hop, so an index below `cached`
        // would pay a chain of fault-ins per hop; that is refused, not paid.
        if index.tier.is_colder_than(Tier::Cached) {
            return Err(Error::Plan(format!(
                "the walk over `{via}` is refused: its adjacency index `{}` is on the {} tier \
                 and a walk reads it at every hop; ALTER INDEX {} ON {via} SET TIER 'cached' \
                 (or warmer), or narrow the walk",
                index.name,
                index.tier.name(),
                index.name
            )));
        }
        if filters.len() > 1 && filters.len() != k {
            return Err(Error::Plan(format!(
                "the walk over `{via}` has {k} hop(s) and {} edge filters; give one filter for \
                 every hop, or one per hop joined by THEN WHERE",
                filters.len()
            )));
        }
        for f in filters {
            walk::check_edge_filter(edges, f)?;
        }
        Ok((edges.clone(), index.clone()))
    }

    /// Sample production vector queries for the recall harness (§12.1). The
    /// point of sampling *real* queries rather than synthetic ones is that
    /// recall regressions are workload-shaped: they show up on the filters and
    /// query distributions users actually have.
    fn log_vector_queries(&self, sel: &Select) {
        let push = |path: &str, q: &Vec<f32>| {
            let mut log = guard(&self.recall);
            log.queries_seen += 1;
            if self.opts.recall_sample_rate == 0
                || log.queries_seen % self.opts.recall_sample_rate != 0
            {
                return;
            }
            log.query_log.push(LoggedVectorQuery {
                collection: sel.collection.clone(),
                path: path.to_string(),
                query: q.clone(),
                k: sel.limit.unwrap_or(10),
                filter_sql: None,
            });
            if log.query_log.len() > 4096 {
                log.query_log.remove(0);
            }
        };
        match &sel.order {
            Some(OrderBy::Distance { path, query, .. }) => push(path, query),
            Some(OrderBy::Hybrid(h)) => {
                for s in &h.sources {
                    if let HybridSource::Vector { path, query, .. } = s {
                        push(path, query);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn logged_queries(&self, collection: &str) -> Vec<LoggedVectorQuery> {
        guard(&self.recall)
            .query_log
            .iter()
            .filter(|q| q.collection == collection)
            .cloned()
            .collect()
    }

    pub fn persist(&mut self) -> Result<()> {
        self.persist_catalog()?;
        for shards in self.shards.values_mut() {
            for s in shards.iter_mut() {
                s.persist_manifest()?;
            }
        }
        Ok(())
    }
}

/// The caller's term list as a set, borrowed when it already is one.
///
/// [`Db::gather_stats`] is public, on a published crate, and takes a
/// `Vec<String>` per path — so `["dup", "dup"]` is expressible, and before
/// this it was double counted on BOTH arms. `Shard::term_stats` walks one
/// posting cursor per element of the slice it is handed and accumulates into
/// the same `df` entry, so a term named twice came back at twice its real
/// frequency: on a corpus of 80 documents all holding `dup`, `df = 160`
/// against `num_docs = 80`. That is not a stale answer, it is `df > num_docs`,
/// a negative logarithm and the IDF clamp — the lowest weight there is — for a
/// term the corpus is full of.
///
/// Deduplicating at the boundary fixes both arms in one place and is the only
/// place that has to know. No SQL query could reach it — `required_terms` ends
/// `sort(); dedup();` — so this is the public API's defect alone, which is
/// exactly why it needed fixing rather than documenting: a direct caller has
/// no reason to suspect the list is not a list.
///
/// Sorted and distinct is the SQL shape, and it borrows: the query path pays
/// one comparison per term and no allocation. The other shape keeps
/// first-occurrence order instead of sorting, because order decides which fill
/// [`STATS_TERM_CAP`] evicts first and that is the caller's to choose.
fn term_set(terms: &[String]) -> Cow<'_, [String]> {
    if terms.windows(2).all(|w| w[0] < w[1]) {
        return Cow::Borrowed(terms);
    }
    let mut seen: BTreeSet<&String> = BTreeSet::new();
    Cow::Owned(terms.iter().filter(|t| seen.insert(t)).cloned().collect())
}

fn cache_key(collection: &str, path: &str) -> String {
    format!("{collection}/{path}")
}

/// A shard's tablet map from its RANGE file. Two lines, a low bound and a
/// high one, and neither the file nor either line is optional.
/// `create_collection` refuses a split key that could write a third line, so
/// a third is a damaged tablet map -- and a tablet map read wrong is a shard
/// that silently owns the wrong keys, which no later check catches because
/// every shard agrees with itself. A missing or empty RANGE is the same
/// defect wearing the opposite disguise, and it used to be read as success:
/// an `unwrap_or_default()` turned it into `""`, which splits into ONE empty
/// part, and a shard with no bounds owns every key. It has to fail here.
/// The database's cipher, from `KEY` and the options: opened under the
/// master key when `KEY` is there; made -- or adopted from `key_file` --
/// when the directory is empty and a master key is given; refused with the
/// reason when an encrypted database has no master key, or a plain database
/// with data is asked to be encrypted (that is a rewrite: export, and import
/// into a fresh directory opened with the key).
fn open_key(dir: &Path, opts: &DbOpts) -> Result<crate::cipher::Shared> {
    let key_path = dir.join("KEY");
    let key_file = crate::shard::read_optional(&key_path)?;
    match (key_file, &opts.master_key) {
        (Some(wrapped), Some(master)) => {
            Ok(Some(Arc::new(crate::cipher::Cipher::unwrap(&wrapped, master)?)))
        }
        (Some(_), None) => Err(Error::Storage(format!(
            "{} is encrypted; set CELASTRO_MASTER_KEY_FILE (or CELASTRO_MASTER_KEY) to open it",
            dir.display()
        ))),
        (None, None) => Ok(None),
        (None, Some(master)) => {
            let has_data = dir.join("CATALOG").exists() || dir.join("collections").exists();
            if has_data {
                return Err(Error::Storage(format!(
                    "{} holds plain data and a master key was given; encrypting existing data is \
                     an export, and an import into a fresh directory opened with the key",
                    dir.display()
                )));
            }
            let wrapped = match &opts.key_file {
                Some(p) => fs::read(p)
                    .map_err(|e| Error::Storage(format!("key file {}: {e}", p.display())))?,
                None => crate::cipher::Cipher::generate()?.wrap(master)?,
            };
            let cipher = crate::cipher::Cipher::unwrap(&wrapped, master)?;
            crate::shard::atomic_write(&key_path, &wrapped)?;
            Ok(Some(Arc::new(cipher)))
        }
    }
}

/// The tablet index a shard directory's name carries: `shard-0003` is 3.
fn shard_index(dir: Option<&Path>, name: &str) -> Result<usize> {
    dir.and_then(|d| d.file_name())
        .and_then(|f| f.to_str())
        .and_then(|f| f.strip_prefix("shard-"))
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| Error::Storage(format!("a shard of `{name}` has no directory to name it")))
}

fn read_range(
    cipher: &crate::cipher::Shared,
    sdir: &Path,
    i: usize,
    name: &str,
) -> Result<(Option<String>, Option<String>)> {
    read_range_at(cipher, sdir, &format!("shard-{i:04}"), name)
}

/// `RANGE` of the shard directory named `dirname` (`shard-NNNN` or
/// `follow-NNNN`), sealed under that name.
fn read_range_at(
    cipher: &crate::cipher::Shared,
    sdir: &Path,
    dirname: &str,
    name: &str,
) -> Result<(Option<String>, Option<String>)> {
    let i = dirname;
    let bytes =
        crate::shard::read_content(cipher, &format!("{dirname}/RANGE"), &sdir.join("RANGE"))
            .map_err(|e| Error::Storage(format!("shard-{i:04} of `{name}`: RANGE: {e}")))?
            .ok_or_else(|| Error::Storage(format!("shard-{i:04} of `{name}`: RANGE is missing")))?;
    let ranges = String::from_utf8_lossy(&bytes).to_string();
    let parts: Vec<&str> = ranges.split('\n').collect();
    if parts.len() != 2 {
        return Err(Error::Storage(format!(
            "shard-{i:04} of `{name}`: RANGE holds {} lines, expected 2",
            parts.len()
        )));
    }
    let lo = parts.first().filter(|s| !s.is_empty()).map(|s| s.to_string());
    let hi = parts.get(1).filter(|s| !s.is_empty()).map(|s| s.to_string());
    Ok((lo, hi))
}

/// A collection's shards as the coordinator calls them: `Local` by direct
/// call, or through the fault schedule when one is installed, in the order
/// it chooses.
fn services_for<'a>(
    shards: &'a [Shard],
    tablets: &[Tablet],
    remotes: &BTreeMap<String, Arc<crate::wire::Node>>,
    collection: &str,
    sim: Option<Arc<crate::sim::Sim>>,
) -> Vec<Box<dyn ShardService + 'a>> {
    // This node's shards by direct call, or through the fault schedule when
    // one is installed; every other tablet through its node. A tablet
    // whose node did not answer the counters call is not here at all: the
    // caller already marked it unreachable.
    let mut out: Vec<Box<dyn ShardService + 'a>> = Vec::new();
    for s in shards {
        out.push(match &sim {
            None => Box::new(Local { shard: s, index: s.index }),
            Some(sim) => Box::new(crate::sim::SimShard::new(sim.clone(), s.index, s)),
        });
    }
    for (i, t) in tablets.iter().enumerate() {
        if t.is_merged() || shards.iter().any(|s| s.index == i) {
            continue;
        }
        if let Some(node) = remotes.get(&t.node) {
            out.push(Box::new(crate::wire::Remote::new(node.clone(), collection, i, t)));
        }
    }
    if let Some(sim) = &sim {
        let order = sim.order(out.len());
        let mut by_pos: Vec<Option<Box<dyn ShardService + 'a>>> =
            out.into_iter().map(Some).collect();
        out = order.into_iter().map(|p| by_pos[p].take().expect("each position once")).collect();
    }
    out
}

/// One path's statistics summed over every shard that answered. `complete`
/// is false when, under `partial`, a shard did not: its index goes to
/// `unreachable` and the sum is over the rest. Without `partial` the first
/// shard that does not answer is the error.
fn sum_term_stats(
    services: &[Box<dyn ShardService + '_>],
    path: &str,
    terms: &[String],
    ts: Timestamp,
    partial: bool,
    unreachable: &mut Vec<usize>,
) -> Result<(TermStats, bool)> {
    let mut sum = TermStats::default();
    let mut complete = true;
    for s in services {
        if unreachable.contains(&s.index()) {
            complete = false;
            continue;
        }
        match s.term_stats(path, terms, ts) {
            Ok(t) => {
                sum.num_docs += t.num_docs;
                sum.total_doc_len += t.total_doc_len;
                for (term, c) in t.doc_freq {
                    *sum.doc_freq.entry(term).or_insert(0) += c;
                }
            }
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                unreachable.push(s.index());
                complete = false;
            }
            Err(e) => return Err(e),
        }
    }
    Ok((sum, complete))
}

/// A seal frozen on one shard: what the console's maintenance thread
/// carries between the lock it took to freeze and the lock it takes to
/// install, with the build in between holding nothing.
pub struct SealJob {
    collection: String,
    shard: usize,
    ticket: crate::shard::SealTicket,
}

impl SealJob {
    pub fn describe(&self) -> String {
        format!("seal of shard {} of `{}`: {:?}", self.shard, self.collection, self.ticket)
    }
}

/// Copies of every shard unless a collection says otherwise: the holder
/// and one follower.
pub const DEFAULT_REPLICAS: usize = 2;

/// Rows per catch-up chunk.
// 500, not 2000: a chunk is decoded and shipped by the holder while it
// serves the shard, and a node back from away found its statements behind
// the chunks; smaller ones interleave with the statements.
const CATCHUP_CHUNK: usize = 500;

/// The steward's lease on this node: when it was last renewed and by
/// whom it may be, shared with the wire.
pub struct LeaseState {
    pub at: Option<std::time::Instant>,
    pub steward: Option<String>,
    /// The steward's term this node last accepted a lease at; zero with
    /// a configured steward.
    pub term: u64,
    /// The election, when the steward is elected (`stewards` set): the
    /// wire feeds it votes and heartbeats, the elector thread its clock.
    pub election: Option<crate::steward::Election>,
    /// Where `STEWARD` (the persisted term and vote) lives.
    pub dir: Option<PathBuf>,
}

pub type Lease = Arc<Mutex<LeaseState>>;

/// The file the election's term and vote are kept in.
pub const STEWARD_FILE: &str = "STEWARD";

/// The persisted term and vote, or zero and none.
pub fn read_steward_file(dir: &Path) -> (u64, Option<String>) {
    let Ok(s) = fs::read_to_string(dir.join(STEWARD_FILE)) else { return (0, None) };
    let mut lines = s.lines();
    let term = lines.next().and_then(|l| l.trim().parse().ok()).unwrap_or(0);
    let voted = lines.next().map(str::trim).filter(|v| !v.is_empty()).map(String::from);
    (term, voted)
}

/// The election's actions that concern this node: the persisted term
/// and vote written before anything else, a steward's rise or fall
/// noted in the cell. The sends are returned for the caller to carry
/// -- a reply, for the wire; a round, for the elector.
pub fn apply_election_actions(
    g: &mut LeaseState,
    actions: Vec<crate::steward::Action>,
) -> Vec<(String, crate::steward::Msg)> {
    use crate::steward::Action;
    let mut sends = Vec::new();
    for a in actions {
        match a {
            Action::Persist { term, voted_for } => {
                if let Some(d) = &g.dir {
                    let body = format!("{term}\n{}\n", voted_for.as_deref().unwrap_or(""));
                    if let Err(e) =
                        crate::shard::atomic_write(&d.join(STEWARD_FILE), body.as_bytes())
                    {
                        crate::log::warn("steward_not_persisted", &[("error", e.to_string())]);
                    }
                }
            }
            Action::BecameSteward { term } => {
                g.term = term;
                g.at = Some(std::time::Instant::now());
                if let Some(e) = &g.election {
                    g.steward = e.steward().map(String::from);
                }
                crate::log::info("steward_elected", &[("term", term.to_string())]);
            }
            Action::SteppedDown { term } => {
                crate::log::warn("steward_stepped_down", &[("term", term.to_string())]);
            }
            Action::Send { to, msg } => sends.push((to, msg)),
        }
    }
    sends
}

/// A renewal from `from` at the steward's `term`: taken when `from` is
/// the steward this node expects -- the configured one, or, with an
/// election, one at a term no lower than this node has seen, which
/// then is the steward. Answers this node's term, for a steward that
/// is behind to step down by.
pub fn renew_lease(lease: &Lease, from: &str, term: u64) -> Result<u64> {
    let mut g = lease.lock().unwrap_or_else(|p| p.into_inner());
    if g.election.is_some() {
        let now = std::time::Instant::now();
        let actions = g.election.as_mut().expect("checked").on_message(
            now,
            from,
            crate::steward::Msg::Heartbeat { term },
        );
        let sends = apply_election_actions(&mut g, actions);
        let accepted = sends
            .iter()
            .any(|(_, m)| matches!(m, crate::steward::Msg::HeartbeatAnswer { accepted: true, .. }));
        let mine = g.election.as_ref().map_or(0, |e| e.term());
        if accepted {
            g.at = Some(now);
            g.steward = Some(from.to_string());
            g.term = mine;
            return Ok(mine);
        }
        return Err(Error::Plan(format!(
            "{from} renews at term {term}; this node has seen term {mine} and takes no lease \
             from a lower one"
        )));
    }
    match &g.steward {
        Some(s) if s == from => {
            g.at = Some(std::time::Instant::now());
            Ok(0)
        }
        Some(s) => Err(Error::Plan(format!("{from} is not the steward this node expects ({s})"))),
        None => Err(Error::Plan("this node has no steward".into())),
    }
}

/// A vote asked by `from` for `candidate` at `term`: granted or not, and
/// this node's term.
pub fn vote(lease: &Lease, from: &str, term: u64, candidate: &str, pre: bool) -> (bool, u64) {
    let mut g = lease.lock().unwrap_or_else(|p| p.into_inner());
    let Some(e) = g.election.as_mut() else { return (false, 0) };
    let msg = if pre {
        crate::steward::Msg::PreVote { term, candidate: candidate.to_string() }
    } else {
        crate::steward::Msg::Vote { term, candidate: candidate.to_string() }
    };
    let actions = e.on_message(std::time::Instant::now(), from, msg);
    let sends = apply_election_actions(&mut g, actions);
    for (_, m) in sends {
        match m {
            crate::steward::Msg::VoteAnswer { term, granted }
            | crate::steward::Msg::PreVoteAnswer { term, granted } => return (granted, term),
            _ => {}
        }
    }
    (false, g.election.as_ref().map_or(0, |e| e.term()))
}

/// A copy this node follows, and the term it follows at.
pub struct FollowedShard {
    pub shard: Shard,
    pub term: u64,
}

/// The followed copies by `(collection, shard)`, behind a lock of their
/// own: the wire applies a holder's batch under this lock and no other.
pub type Followed = Arc<Mutex<BTreeMap<(String, usize), FollowedShard>>>;

/// The followed copies of one collection, for a definition that reaches
/// them; a guard with the lock.
fn followed_of<'a>(f: &'a Followed, collection: &str) -> FollowedOf<'a> {
    let guard = f.lock().unwrap_or_else(|p| p.into_inner());
    let keys: Vec<(String, usize)> =
        guard.keys().filter(|(c, _)| c == collection).cloned().collect();
    FollowedOf { guard, keys }
}

struct FollowedOf<'a> {
    guard: MutexGuard<'a, BTreeMap<(String, usize), FollowedShard>>,
    keys: Vec<(String, usize)>,
}

impl FollowedOf<'_> {
    fn iter_mut(&mut self) -> Vec<&mut FollowedShard> {
        let keys = self.keys.clone();
        let mut out = Vec::new();
        // Distinct keys, so the mutable borrows are disjoint.
        let map: *mut BTreeMap<(String, usize), FollowedShard> = &mut *self.guard;
        for k in keys {
            // SAFETY: each key is looked up once and the keys are distinct,
            // so no two references alias.
            if let Some(f) = unsafe { (*map).get_mut(&k) } {
                out.push(f);
            }
        }
        out
    }
}

/// A batch of a holder's log into a copy this node follows, at the
/// holder's term, under the followed copies' lock alone; where the copy
/// stands. The first item may say to start from nothing.
pub fn apply_shipped(
    followed: &Followed,
    collection: &str,
    shard: usize,
    term: u64,
    items: &[crate::replication::ShipItem],
) -> Result<(bool, Timestamp)> {
    let mut g = followed.lock().unwrap_or_else(|p| p.into_inner());
    let f = g.get_mut(&(collection.to_string(), shard)).ok_or_else(|| {
        Error::Plan(format!("this node does not follow shard {shard} of `{collection}`"))
    })?;
    if f.term != term {
        return Err(Error::Plan(format!(
            "shard {shard} of `{collection}`: this node follows term {}, the shipper is term {term}",
            f.term
        )));
    }
    let rest = match items.first() {
        Some(it) if it.kind == crate::replication::SHIP_RESET => {
            f.shard.reset_copy()?;
            &items[1..]
        }
        _ => items,
    };
    f.shard.apply_shipped(rest)
}

/// Where a copy this node follows stands.
pub fn follower_status(
    followed: &Followed,
    collection: &str,
    shard: usize,
    term: u64,
) -> Result<(bool, Timestamp)> {
    let g = followed.lock().unwrap_or_else(|p| p.into_inner());
    let f = g.get(&(collection.to_string(), shard)).ok_or_else(|| {
        Error::Plan(format!("this node does not follow shard {shard} of `{collection}`"))
    })?;
    if f.term != term {
        return Err(Error::Plan(format!(
            "shard {shard} of `{collection}`: this node follows term {}, the shipper is term {term}",
            f.term
        )));
    }
    Ok((f.shard.caught_up, f.shard.ship_ts))
}

/// What a write statement waits for before it is acknowledged: each shard
/// it wrote, at the instant it wrote, confirmed by that shard's followers.
pub struct Confirmation {
    waits: Vec<(Arc<crate::replication::Shipper>, Timestamp)>,
    budget: Option<u64>,
}

impl Confirmation {
    pub fn is_empty(&self) -> bool {
        self.waits.is_empty()
    }

    /// Wait for every confirmation, within the statement's budget.
    pub fn wait(&self) -> Result<()> {
        for (sh, ts) in &self.waits {
            sh.wait(*ts, self.budget)?;
        }
        Ok(())
    }
}

/// What routes elsewhere, by holder: the connection and the items for it.
type Away<T> = BTreeMap<String, (Arc<crate::wire::Node>, Vec<T>)>;

/// Every holder's clock and write counter for a collection, asked at once
/// in the holders' order, each thread armed with the caller's remaining
/// budget -- half of it under `partial`, so that a holder that never
/// answers leaves the other half for the shards that do.
#[allow(clippy::type_complexity)]
fn fetch_counters(
    conns: &[(String, Arc<crate::wire::Node>)],
    collection: &str,
    partial: bool,
) -> Vec<(String, Arc<crate::wire::Node>, Result<(Timestamp, u64)>)> {
    let remaining = crate::deadline::remaining_ms().map(|ms| if partial { ms / 2 } else { ms });
    if conns.len() < 2 {
        let _deadline = crate::deadline::arm(remaining);
        return conns
            .iter()
            .map(|(url, n)| (url.clone(), n.clone(), n.counters(collection)))
            .collect();
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = conns
            .iter()
            .map(|(url, n)| {
                let (url, n) = (url.clone(), n.clone());
                scope.spawn(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    let r = n.counters(collection);
                    (url, n, r)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("a counters thread panicked")).collect()
    })
}

/// A collection made here, to be carried to its holders and the
/// coordinators as deferred work holding nothing.
pub struct Spread {
    coll: Collection,
    tablets: Vec<Tablet>,
    conns: Vec<(String, Arc<crate::wire::Node>)>,
    failures: Vec<String>,
    remaining: Option<u64>,
}

impl Spread {
    /// Every target at once; the note naming the ones not reached.
    fn carry(mut self) -> String {
        let answers: Vec<(String, Result<()>)> = std::thread::scope(|scope| {
            let handles: Vec<_> = self
                .conns
                .iter()
                .map(|(url, n)| {
                    let (url, n, coll, tablets, remaining) =
                        (url.clone(), n.clone(), &self.coll, &self.tablets, self.remaining);
                    scope.spawn(move || {
                        let _deadline = crate::deadline::arm(remaining);
                        (url, n.create_collection(coll, tablets))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().expect("a creation thread panicked")).collect()
        });
        for (url, r) in answers {
            if let Err(e) = r {
                self.failures.push(format!("{url}: {e}"));
            }
        }
        if self.failures.is_empty() {
            String::new()
        } else {
            format!(
                "; not on {}: they adopt it when they reconnect ({})",
                self.failures.join("; "),
                RECONCILE_NOTE
            )
        }
    }
}

/// Run a `LOCAL` statement on every peer at once, holding nothing: the
/// peers that took it, and what went wrong on the ones that did not. The
/// caller's deadline is armed in each thread, since it is thread-local.
fn carry_statement(
    conns: &[(String, Arc<crate::wire::Node>)],
    local: &str,
    params: &[Value],
    remaining: Option<u64>,
) -> (Vec<String>, Vec<String>) {
    let answers: Vec<(String, Result<String>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = conns
            .iter()
            .map(|(url, n)| {
                let (url, n) = (url.clone(), n.clone());
                scope.spawn(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    (url, n.statement(local, params))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("a carrier thread panicked")).collect()
    });
    let mut done = Vec::new();
    let mut failures = Vec::new();
    for (url, r) in answers {
        match r {
            Ok(_) => done.push(url),
            Err(e) => failures.push(format!("{url}: {e}")),
        }
    }
    (done, failures)
}

/// Write documents to their holders, holder by holder at once, holding
/// nothing; the latest commit instant. A holder that refuses is the
/// statement's failure, after every holder was tried, naming it.
fn carry_writes(
    collection: &str,
    away: Away<Value>,
    last: Timestamp,
    here_n: usize,
    remaining: Option<u64>,
    dialer: &Dialer,
) -> Result<Timestamp> {
    // Per holder: its url, how many rows it took, and the ts or the error.
    // A statement over several holders is per holder, not all or nothing;
    // when one refuses, the rows the others took have landed, and the error
    // says so -- a client that retries the statement (inserts are
    // idempotent by key) or the refused rows alone knows which.
    let answers: Vec<(String, usize, usize, Result<Timestamp>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = away
            .iter()
            .map(|(url, (n, docs))| {
                let n = n.clone();
                let url = url.clone();
                scope.spawn(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    let mut last = 0;
                    let mut moved = Moved::default();
                    let mut taken = 0usize;
                    for doc in docs {
                        let ts = match n.insert(collection, doc) {
                            Ok(ts) => ts,
                            Err(e) => match moved_to(&e) {
                                Some(to) => match moved.dial(dialer, &to) {
                                    Ok(c) => match c.insert(collection, doc) {
                                        Ok(ts) => ts,
                                        Err(e) => return (url, taken, docs.len(), Err(e)),
                                    },
                                    Err(e) => return (url, taken, docs.len(), Err(e)),
                                },
                                None => return (url, taken, docs.len(), Err(e)),
                            },
                        };
                        taken += 1;
                        last = last.max(ts);
                    }
                    (url, taken, docs.len(), Ok(last))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("a carrier thread panicked")).collect()
    });
    let mut latest = last;
    let mut landed: Vec<String> = Vec::new();
    let mut refused: Vec<String> = Vec::new();
    let mut deadline_only = true;
    if here_n > 0 {
        landed.push(format!("{here_n} here"));
    }
    for (url, taken, of, r) in answers {
        match r {
            Ok(ts) => {
                latest = latest.max(ts);
                landed.push(format!("{of} on {url}"));
            }
            Err(e) => {
                if !matches!(e, Error::Deadline(_)) {
                    deadline_only = false;
                }
                if taken > 0 {
                    landed.push(format!("{taken} on {url}"));
                }
                refused.push(format!("{} on {url} ({e})", of - taken));
            }
        }
    }
    if refused.is_empty() {
        return Ok(latest);
    }
    let msg = format!(
        "NOT written: {}; written: {}. A statement over several holders is per holder: run \
         it again once the holder answers (inserts are idempotent by key), or the refused \
         rows alone",
        refused.join("; "),
        if landed.is_empty() { "nothing".to_string() } else { landed.join(", ") }
    );
    Err(if deadline_only { Error::Deadline(msg) } else { Error::Plan(msg) })
}

/// Delete keys on their holders, holder by holder at once, holding
/// nothing; how many were there.
fn carry_deletes(
    collection: &str,
    away: Away<String>,
    remaining: Option<u64>,
    dialer: &Dialer,
) -> Result<usize> {
    let answers: Vec<Result<usize>> = std::thread::scope(|scope| {
        let handles: Vec<_> = away
            .values()
            .map(|(n, keys)| {
                let n = n.clone();
                scope.spawn(move || {
                    let _deadline = crate::deadline::arm(remaining);
                    let mut n_deleted = 0;
                    let mut moved = Moved::default();
                    for k in keys {
                        let gone = match n.delete(collection, k) {
                            Ok(gone) => gone,
                            Err(e) => match moved_to(&e) {
                                Some(url) => moved.dial(dialer, &url)?.delete(collection, k)?,
                                None => return Err(e),
                            },
                        };
                        if gone {
                            n_deleted += 1;
                        }
                    }
                    Ok(n_deleted)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("a carrier thread panicked")).collect()
    });
    let mut n = 0;
    for r in answers {
        n += r?;
    }
    Ok(n)
}

/// What dials a holder outside the lock: the token, the TLS and this node's
/// identity, as `Db::wire_node` uses them under it.
pub(crate) struct Dialer {
    token: Option<String>,
    tls: Option<Arc<crate::tls::Tls>>,
    me: Option<String>,
    epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl Dialer {
    fn dial(&self, url: &str) -> Result<crate::wire::Node> {
        let n = crate::wire::Node::new(url, self.token.as_deref(), self.tls.clone())?;
        Ok(match &self.me {
            Some(me) => n.with_identity(me, self.epoch.clone()),
            None => n,
        })
    }
}

/// The holders a carry was sent on to, one connection each: a holder
/// that refuses a key as another's is the map having moved under the
/// statement -- a shard switched to its new node between the plan and
/// the carry -- and the refusal names where, so the key goes there once.
#[derive(Default)]
struct Moved(BTreeMap<String, crate::wire::Node>);

impl Moved {
    fn dial(&mut self, dialer: &Dialer, url: &str) -> Result<&crate::wire::Node> {
        if !self.0.contains_key(url) {
            self.0.insert(url.to_string(), dialer.dial(url)?);
        }
        Ok(&self.0[url])
    }
}

/// What a read of a fenced shard is refused with, here and over the wire.
pub fn fenced_message(collection: &str, shard: usize, to: &str) -> String {
    format!(
        "shard {shard} of `{collection}` is moving to {to} and its map is switching; a read of \
         it here would miss the writes landing there -- retry"
    )
}

/// The node a holder's refusal names as the key's, when it names one.
fn moved_to(e: &Error) -> Option<String> {
    let Error::Plan(m) = e else { return None };
    let rest = m.split_once("belongs to the shard on ")?.1;
    Some(rest.split(',').next()?.trim().to_string())
}

/// A map switch to carry to the peers, holding nothing: the statement and
/// the peers in order, the source last.
pub struct Switch {
    sql: String,
    peers: Vec<(String, Arc<crate::wire::Node>)>,
}

/// A move pinned under the lock: what the deferred copy carries.
pub struct MovePlan {
    collection: String,
    shard: usize,
    from: String,
    to: String,
    coll: Collection,
    new: Vec<Tablet>,
    /// The pinned files, when this node is the source and pinned under the
    /// lock; `None` for a source elsewhere, pinned by the deferred work.
    files: Option<Vec<(String, u64)>>,
    source: Arc<crate::wire::Node>,
    target: Arc<crate::wire::Node>,
    /// What was left of the statement's budget at the plan, for the pin.
    deadline_ms: Option<u64>,
}

/// A compaction reserved on one shard: what the console's maintenance
/// thread carries between the lock it took to plan and the lock it takes
/// to install.
pub struct CompactionTicket {
    collection: String,
    shard: usize,
    reserved: compaction::Reserved,
}

impl CompactionTicket {
    /// `<collection> shard <i>: <n> segment(s) -> level <l>`, for a log line.
    pub fn describe(&self) -> String {
        format!(
            "`{}` shard {}: {} segment(s) into level {}",
            self.collection,
            self.shard,
            self.reserved.inputs.len(),
            self.reserved.level
        )
    }
}

/// The first word or two of a statement, for a message that names it.
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Delete(_) => "DELETE",
        Statement::CreateCollection(_) => "CREATE COLLECTION",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::Flush { .. } => "FLUSH",
        Statement::Compact { .. } => "COMPACT",
        Statement::Backup { .. } => "BACKUP",
        Statement::Restore { .. } => "RESTORE",
        Statement::SplitShard { .. } => "SPLIT SHARD",
        Statement::MergeShards { .. } => "MERGE SHARDS",
        Statement::PromoteShard { .. } => "PROMOTE SHARD",
        Statement::VerifyBackup { .. } => "VERIFY BACKUP",
        _ => "this statement",
    }
}

/// What a collection's directory is renamed to at the start of a drop.
const DROPPING_SUFFIX: &str = ".dropping";

fn dropping_dir(dir: &Path, name: &str) -> PathBuf {
    dir.join("collections").join(format!("{name}{DROPPING_SUFFIX}"))
}

/// Human-readable byte counts for the residency reports. Operators reason
/// about "3.2 GiB", not about 3435973836.
pub fn bytes(n: usize) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < U.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// What a select does with a path, which decides *which* index it uses.
///
/// A path is not an index. `ORDER BY body ASC` is a lexicographic sort that
/// reads no full-text index at all, and counting it as a use of `items_body`
/// would keep a full-text index permanently hot on a workload that never
/// searches it — which is the inactivity rule failing to notice inactivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndexUse {
    Text,
    Vector,
    Scalar,
    /// A walk over an edge collection's adjacency index.
    Walk,
}

impl IndexUse {
    fn matches(self, kind: &IndexKind) -> bool {
        matches!(
            (self, kind),
            (IndexUse::Text, IndexKind::FullText { .. })
                | (IndexUse::Vector, IndexKind::Vector { .. })
                | (IndexUse::Scalar, IndexKind::Secondary)
                | (IndexUse::Walk, IndexKind::Adjacency { .. })
        )
    }
}

/// Every index a select actually reads, as `(path, use)`.
fn index_uses(sel: &Select) -> Vec<(String, IndexUse)> {
    let mut out: Vec<(String, IndexUse)> = Vec::new();
    fn walk(e: &Expr, out: &mut Vec<(String, IndexUse)>) {
        match e {
            Expr::Compare { path, .. } => out.push((path.clone(), IndexUse::Scalar)),
            Expr::TextMatch { path, .. } => out.push((path.clone(), IndexUse::Text)),
            Expr::VectorDistance { path, .. } => out.push((path.clone(), IndexUse::Vector)),
            // The walk's index is on ANOTHER collection; `run_over` touches
            // it there. The primary key it selects by is no index here.
            Expr::Hops { .. } => {}
            Expr::And(v) | Expr::Or(v) => v.iter().for_each(|x| walk(x, out)),
            Expr::Not(b) => walk(b, out),
            Expr::True => {}
        }
    }
    if let Some(e) = &sel.predicate {
        walk(e, &mut out);
    }
    match &sel.order {
        Some(OrderBy::Distance { path, .. }) => out.push((path.clone(), IndexUse::Vector)),
        Some(OrderBy::Fields(v)) => {
            out.extend(v.iter().map(|(p, _)| (p.clone(), IndexUse::Scalar)))
        }
        Some(OrderBy::Hybrid(h)) => {
            for s in &h.sources {
                match s {
                    HybridSource::Text { path, .. } => out.push((path.clone(), IndexUse::Text)),
                    HybridSource::Vector { path, .. } => out.push((path.clone(), IndexUse::Vector)),
                    // The walk touches its adjacency index itself, on the
                    // edge collection, as a filter walk does.
                    HybridSource::Hops { .. } => {}
                }
            }
        }
        None => {}
    }
    if let Some(c) = &sel.collapse {
        out.push((c.clone(), IndexUse::Scalar));
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {

    /// A shard past the flat-segment debt makes a write wait, and the wait
    /// is counted; within the debt nothing waits.
    #[test]
    fn a_write_waits_when_flat_segments_run_ahead_of_compaction() {
        let mut opts = DbOpts::default();
        opts.thresholds.max_bytes = 1;
        opts.compaction.tier_fanout = 1000;
        opts.compaction.debt_segments = 2;
        opts.compaction.debt_wait_ms = 30;
        let mut db = Db::with_opts(opts);
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        let doc = |i: usize| Value::obj(vec![("id".into(), Value::Str(format!("n{i}")))]);
        for i in 0..2 {
            db.insert_many("notes", vec![doc(i)]).unwrap();
        }
        assert_eq!(db.backpressure().0, 0, "two flat segments: within the debt");
        db.insert_many("notes", vec![doc(2)]).unwrap();
        let t0 = std::time::Instant::now();
        db.insert_many("notes", vec![doc(3)]).unwrap();
        assert!(t0.elapsed() >= std::time::Duration::from_millis(30), "{:?}", t0.elapsed());
        assert_eq!(db.backpressure().0, 1);
        assert!(db.backpressure().1 >= 30_000);
    }

    /// A directory that vanishes under a running node: writes are refused
    /// naming it, reads still answer from memory, and the node says it is
    /// not well.
    #[test]
    fn a_vanished_directory_refuses_writes_and_is_not_well() {
        let dir = tmp("vanished");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.insert("notes", Value::obj(vec![("id".into(), Value::Str("a".into()))])).unwrap();
        assert!(db.directory_present());
        let aside = dir.with_extension("aside");
        let _ = fs::remove_dir_all(&aside);
        fs::rename(&dir, &aside).unwrap();
        assert!(!db.directory_present());
        let e = db
            .execute("INSERT INTO notes VALUES ('{\"id\":\"b\"}')")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(e.contains("is gone") && e.contains("nothing was written"), "{e}");
        assert_eq!(db.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 1);
        drop(db);
        let _ = fs::remove_dir_all(&aside);
    }

    #[test]
    fn fuzz_catalog_decoding_never_panics() {
        let mut db = Db::in_memory();
        for sql in [
            "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) PARTITION BY (tenant) WITH (splits = ['m'])",
            "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
            "CREATE INDEX items_emb ON items USING vector (embedding) WITH (dims = 4, metric = 'cosine', tier = 'cached')",
            "CREATE INDEX items_n ON items USING secondary (n)",
            "CREATE COLLECTION edges (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) WITH (nodes_of = 'items')",
            "CREATE INDEX edges_adj ON edges USING adjacency (src, dst)",
        ] {
            db.execute(sql).unwrap();
        }
        let sample = db.catalog.encode();
        crate::fuzz::sweep(51, &[sample], 6000, |b| {
            let _ = Catalog::decode(b);
        });
    }
    use super::*;

    use crate::shard::durability_probe::{self, Op};
    use crate::text::scorer::PREFIX_EXPANSION_LIMIT;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("celastro-engine-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn a_collection_whose_shards_cannot_be_built_is_not_left_in_the_catalog() {
        let dir = tmp("create-rollback");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        // A regular file where the collection's shard directories have to go,
        // so the first `create_dir_all` inside `create_collection` fails.
        fs::create_dir_all(dir.join("collections")).unwrap();
        fs::write(dir.join("collections").join("notes"), b"not a directory").unwrap();

        assert!(db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").is_err());
        // Without the rollback the catalog keeps an entry with no shards, and
        // the database answers `already exists` and `no such collection` to the
        // same collection until it is restarted.
        assert!(db.catalog.get("notes").is_err());
        assert!(db.shards("notes").is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    /// Files in `collections/<c>/shard-NNNN/<sub>`.
    fn count(dir: &Path, collection: &str, sub: &str) -> usize {
        let p = dir.join("collections").join(collection).join("shard-0000").join(sub);
        fs::read_dir(p).map(|d| d.filter_map(|e| e.ok()).count()).unwrap_or(0)
    }

    fn note(id: &str) -> Value {
        Value::obj(vec![
            ("id".into(), Value::Str(id.into())),
            ("body".into(), Value::Str("segments and postings".into())),
            ("title".into(), Value::Str("a title".into())),
        ])
    }

    /// A collection is archived when *every* index says so, so creating one
    /// that does not is a placement decision, not just a catalog edit. Without
    /// re-running placement the new index is `active` in the catalog while its
    /// segments are still in `archive/`, and nothing afterwards reconciles the
    /// two: `sync_archive` is the only thing that moves a file back, and no
    /// other path calls it.
    #[test]
    fn creating_an_index_brings_the_segments_back_out_of_the_archive() {
        let dir = tmp("add-index-placement");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert("notes", note("a")).unwrap();
        db.execute("FLUSH notes").unwrap();
        db.execute("ALTER INDEX notes_body ON notes SET TIER 'archived'").unwrap();
        assert_eq!(count(&dir, "notes", "segments"), 0, "everything archived, so the file moved");
        assert!(count(&dir, "notes", "archive") > 0);

        db.execute(
            "CREATE INDEX notes_title ON notes USING fulltext (title) WITH (analyzer = 'english')",
        )
        .unwrap();
        assert_eq!(
            count(&dir, "notes", "archive"),
            0,
            "an index that is not archived means the collection is not archived either"
        );
        assert!(count(&dir, "notes", "segments") > 0, "the segment file has to come back");

        // And reading it is no longer an archive round trip, because it is no
        // longer in the archive.
        let faults = db.residency().faults();
        let r = db.query("SELECT * FROM notes WHERE text_match(body, 'postings')").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(db.residency().faults(), faults, "nothing left to fault in");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The pin is a truce between a retention rule and access-promotion. Drop
    /// the rule and there is nothing left to keep the truce with, but the pin
    /// is persisted and permanent: the index can never be promoted back by a
    /// query, and no other path clears it.
    #[test]
    fn dropping_the_last_policy_covering_an_index_releases_its_retention_pin() {
        let dir = tmp("drop-policy-pin");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert("notes", note("a")).unwrap();
        db.execute("FLUSH notes").unwrap();
        db.execute(
            "CREATE LIFECYCLE POLICY old ON notes MOVE TO cached AFTER 1 day SINCE CREATION",
        )
        .unwrap();

        let key = ("notes".to_string(), "notes_body".to_string());
        let now = lifecycle::now_micros(&db.clock);
        db.catalog.activity.get_mut(&key).unwrap().created_micros = now - 2 * 86_400_000_000;
        assert_eq!(db.run_lifecycle(Some("notes")).unwrap().moves.len(), 1);
        assert_eq!(
            db.catalog.activity[&key].demoted_by,
            Some(lifecycle::Trigger::SinceCreation),
            "an age demotion pins the index against promotion"
        );

        db.drop_policy("old").unwrap();
        assert_eq!(
            db.catalog.activity[&key].demoted_by, None,
            "with the rule gone the pin has nothing left to protect"
        );
        assert!(db.catalog.activity[&key].promotable());
        // And it survives the write, rather than being a live-only repair.
        // One handle per directory: the first is let go before the reopen.
        drop(db);
        let re = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(re.catalog.activity[&key].demoted_by, None);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A policy that still covers the index keeps its pin: the truce is with
    /// whatever rule can still fire, not with the one that happened to fire.
    #[test]
    fn a_pin_survives_dropping_a_policy_that_is_not_the_last_one() {
        let dir = tmp("drop-policy-pin-kept");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert("notes", note("a")).unwrap();
        db.execute("FLUSH notes").unwrap();
        db.execute(
            "CREATE LIFECYCLE POLICY old ON notes MOVE TO cached AFTER 1 day SINCE CREATION",
        )
        .unwrap();
        db.execute(
            "CREATE LIFECYCLE POLICY older ON notes MOVE TO archived AFTER 9 days SINCE CREATION",
        )
        .unwrap();

        let key = ("notes".to_string(), "notes_body".to_string());
        let now = lifecycle::now_micros(&db.clock);
        db.catalog.activity.get_mut(&key).unwrap().created_micros = now - 2 * 86_400_000_000;
        assert_eq!(db.run_lifecycle(Some("notes")).unwrap().moves.len(), 1);

        db.drop_policy("old").unwrap();
        assert_eq!(
            db.catalog.activity[&key].demoted_by,
            Some(lifecycle::Trigger::SinceCreation),
            "`older` still covers this index, and its rule would demote it again"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// `last_access` is stamped from the wall clock by `Segment::acquire`, so
    /// the sweeper has to subtract it from the wall clock. The HLC absorbs
    /// timestamps from elsewhere and is monotone, so it runs ahead of wall
    /// time by however far ahead the furthest coordinator was -- and every
    /// component then looks that much idler than it is.
    #[test]
    fn idleness_is_measured_against_the_clock_that_stamped_it() {
        let dir = tmp("idle-clock");
        let mut o = DbOpts::default();
        o.residency.active_idle_unload = Some(std::time::Duration::from_secs(600));
        let mut db = Db::open(&dir, o).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert("notes", note("a")).unwrap();
        db.execute("FLUSH notes").unwrap();
        db.query("SELECT * FROM notes WHERE text_match(body, 'postings')").unwrap();
        let resident = db.residency().resident_bytes();
        assert!(resident > 0, "the query has to leave something decoded to unload");

        // A read-your-writes token from a coordinator an hour ahead. Absorbing
        // it is what the clock is for (§6); it says nothing about how long ago
        // this node touched its own segments.
        db.clock.observe(crate::time::from_micros(crate::time::now_micros() + 3_600_000_000));

        let (idle, _) = db.unload_idle(None).unwrap();
        assert_eq!(idle, 0, "nothing here has been idle for ten minutes");
        assert_eq!(db.residency().resident_bytes(), resident);
        assert_eq!(db.residency().unloads(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The tablet map is two lines in a file, so a split key that holds a line
    /// break is a shard boundary the reopen cannot read back -- and a boundary
    /// read back wrong is a shard that owns keys nobody routed to it.
    #[test]
    fn a_split_key_the_tablet_map_cannot_hold_is_refused() {
        let dir = tmp("range-guard");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        let coll = Collection::new("notes", "id", None);
        let e = db.create_collection(coll.clone(), &["m\nz".to_string()]).unwrap_err();
        assert!(matches!(e, Error::Schema(_)), "{e}");
        assert!(db.create_collection(coll.clone(), &[String::new()]).is_err());
        assert!(
            db.catalog.collections.is_empty(),
            "a refused split list must not leave the collection behind"
        );
        db.create_collection(coll, &["m".to_string()]).unwrap();
        assert_eq!(db.shards("notes").unwrap().len(), 2);

        // And a tablet map that already holds one -- written by a build
        // without the guard -- is reported at open rather than silently
        // reinterpreted as a different range.
        let range = dir.join("collections").join("notes").join("shard-0000").join("RANGE");
        fs::write(&range, b"\nm\nz").unwrap();
        assert!(
            matches!(Db::open(&dir, DbOpts::default()), Err(Error::Storage(_))),
            "a three-line RANGE has to be reported, not reinterpreted"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_tier_change_rolls_back_the_declared_tier_and_the_retention_pin() {
        let dir = tmp("tier-rollback");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert(
            "notes",
            Value::obj(vec![
                ("id".into(), Value::Str("a".into())),
                ("body".into(), Value::Str("segments and postings".into())),
            ]),
        )
        .unwrap();
        db.execute("FLUSH notes").unwrap();

        // Take away the directory the sealed segment would be archived into,
        // so the file move that `SET TIER 'archived'` asks for fails.
        let shard = dir.join("collections").join("notes").join("shard-0000");
        fs::remove_dir_all(shard.join("archive")).unwrap();

        let segments = fs::read_dir(shard.join("segments")).unwrap().count();
        assert!(segments > 0, "the flush has to leave a sealed segment for the move to fail on");

        let key = ("notes".to_string(), "notes_body".to_string());
        db.catalog.activity.get_mut(&key).unwrap().demoted_by =
            Some(lifecycle::Trigger::SinceCreation);
        let def = db.catalog.get("notes").unwrap().index_by_name("notes_body").unwrap();
        let (tier_before, declared_before) = (def.tier, def.declared_tier);

        assert!(db.set_index_tier("notes", "notes_body", Tier::Archived).is_err());

        let after = db.catalog.get("notes").unwrap().index_by_name("notes_body").unwrap();
        assert_eq!(after.tier, tier_before);
        assert_eq!(after.declared_tier, declared_before);
        assert_eq!(
            db.catalog.activity[&key].demoted_by,
            Some(lifecycle::Trigger::SinceCreation),
            "an age demotion is a retention pin; a failed ALTER must not drop it"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_flush_counts_the_shards_that_sealed_not_the_segments_they_wrote() {
        // `FLUSH` reports work done, and a seal is the unit of that work: the
        // memtable was swapped out, the manifest bumped and the WAL truncated,
        // once, however many segments came out of it. Two shards here, only
        // one of which has anything to seal, and it seals under a pin that
        // makes it emit *two* segments — because the update superseded a
        // version a reader at the horizon still needs, and one segment holds
        // one version per key. So 1 is the only honest answer: counting the
        // segments written says 2, and counting every shard walked says 2 as
        // well, and that second one would report a shard as flushed whose
        // memtable was empty.
        let dir = tmp("flush-counts-seals");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['m'])").unwrap();
        assert_eq!(db.shards("notes").unwrap().len(), 2, "the split makes two shards");

        let note = |body: &str| {
            Value::obj(vec![
                ("id".into(), Value::Str("a".into())),
                ("body".into(), Value::Str(body.into())),
            ])
        };
        // Both writes sort below the split point, so the second shard never
        // takes a row.
        db.insert("notes", note("first")).unwrap();
        let horizon = db.clock.peek();
        db.shards.get_mut("notes").unwrap()[0].opts.gc_horizon = horizon;
        db.insert("notes", note("second")).unwrap();

        let flushed = db.flush("notes").unwrap();
        let shards = db.shards("notes").unwrap();
        assert_eq!(shards[0].segments.len(), 2, "the pinned seal has to emit two segments");
        assert!(shards[1].memtable.is_empty(), "the second shard had nothing to seal");
        assert!(shards[1].segments.is_empty());
        assert_eq!(flushed, 1, "one shard sealed: not two segments, and not two shards");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_cached_statistics_are_live_sums_over_every_unit_of_every_shard() {
        // The only test that pins the cached triple to absolute numbers, and
        // the only one that constrains the accumulation per unit AND per
        // shard: three shards, and two non-empty units in each (a sealed
        // segment and a live memtable). With one shard and one contributing
        // unit — what the path used to be tested with — `+=` and `=` are
        // indistinguishable, and so is "read the first shard and stop". Its
        // siblings below cover the epoch, the fill, the cap and coherence, but
        // every one of them compares against a second gather or a bound.
        //
        // The numbers are absolute, not compared against a second gather.
        // Comparing the cached triple against the exact one pins only that
        // they agree, and a mutation that moves both is invisible to it.
        //
        // These numbers used to be over PHYSICAL rows — 1200 documents, and
        // `doc_freq["alpha"] == 1200` — and that WAS the bug this path had.
        // A physical count includes the versions a seal superseded and the
        // rows a compaction has not yet collected, and how many of those exist
        // is each shard's own decision, taken at its own thresholds: the
        // statistic therefore moved with the shard count, and the default
        // path's scores moved with it. Every number below is now masked by
        // visibility at the query's timestamp, so it counts the 1118 documents
        // that are actually there.
        let dir = tmp("cached-stats-sum");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute(
            "CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['n0300', 'n0600'])",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        // `alpha` and `zeta` are terms the english analyzer leaves alone, so
        // the key the dictionary holds is the key the gather asks for.
        let note = |id: String, body: String| {
            Value::obj(vec![("id".into(), Value::Str(id)), ("body".into(), Value::Str(body))])
        };
        // 900 rows spread evenly over the three shards by the split points,
        // then sealed, so every shard owns exactly one segment.
        for i in 0..900usize {
            let body = if i % 5 == 0 { "alpha zeta" } else { "alpha" };
            db.insert("notes", note(format!("n{i:04}"), body.into())).unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        // 300 more, keyed to interleave with the first batch rather than to
        // sort past its end, so all three memtables take rows. No flush: this
        // is the second non-empty unit each shard needs.
        for i in (0..900usize).step_by(3) {
            db.insert("notes", note(format!("n{i:04}x"), "alpha".into())).unwrap();
        }
        // And tombstones, so the cache is exercised over a corpus whose
        // physical rows and live rows have parted company — which is the whole
        // difference this test is here to see.
        for i in (0..900usize).step_by(11) {
            db.delete_key("notes", &format!("n{i:04}")).unwrap();
        }

        let shards = db.shards("notes").unwrap();
        assert_eq!(shards.len(), 3, "the two split points make three shards");
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.segments.len(), 1, "shard {i} sealed exactly one segment");
            assert!(!s.memtable.is_empty(), "shard {i} also carries live memtable rows");
        }

        let want =
            BTreeMap::from([("body".to_string(), vec!["alpha".to_string(), "zeta".to_string()])]);
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want, ts, false).unwrap();
        let g = &g["body"];
        assert!(!g.exact, "this is the cached path");

        // Absolute, and every one of them is a sum the gather has to get right
        // across six units and three shards.
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            c.num_docs, 1118,
            "900 sealed rows and 300 memtable rows, less the 82 keys deleted; the deletes are \
             the point — a physical count answers 1200 here, because a tombstone hides a row \
             without removing it"
        );
        assert_eq!(
            c.total_doc_len, 1281,
            "1080 in the segments (180 two-term bodies and 720 one-term ones) and 300 in the \
             memtables, less the 17 two-term and 65 one-term bodies deleted"
        );
        assert_eq!(c.doc_freq["alpha"], 1118, "every live row holds it");
        assert_eq!(c.doc_freq["zeta"], 163, "one row in five of the sealed batch, less 17 deleted");
        assert_eq!(
            c.doc_freq.len(),
            2,
            "and the cache holds the two terms the query asked for, not the vocabulary"
        );

        assert_eq!(g.num_docs, 1118);
        assert_eq!(g.avg_doc_len.to_bits(), (1281.0f64 / 1118.0).to_bits());
        // The invariant that survives the change, bought differently: it used
        // to hold because `doc_freq` and `num_docs` counted the same physical
        // rows, and it now holds because they are masked at the same instant
        // by the same `Shard::term_stats` call. Either way `doc_freq` cannot
        // exceed `num_docs`, so IDF's logarithm never takes an argument below
        // one — and a negative IDF does not blur a ranking, it reverses it.
        for (t, df) in &g.doc_freq {
            assert!(*df <= g.num_docs, "df({t}) = {df} exceeds num_docs = {}", g.num_docs);
            assert!(g.idf(t) > 0.0, "idf({t}) = {} is not positive", g.idf(t));
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_freshly_refreshed_cache_answers_exactly_what_the_exact_gather_answers() {
        // The strongest single assertion available on this path, and the
        // cheapest: with no writes between the refresh and the read, staleness
        // is zero, so the two arms of `gather_stats` are gathering the same
        // quantity from the same corpus and must agree bit for bit. Anything
        // that makes the cached arm count something else — a physical row, a
        // dictionary entry, a sum taken at a different instant — shows up
        // here without needing a second shard count to compare against.
        let dir = tmp("stats-fresh-equals-exact");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['n0100'])")
            .unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let note = |id: String, body: String| {
            Value::obj(vec![("id".into(), Value::Str(id)), ("body".into(), Value::Str(body))])
        };
        for i in 0..200usize {
            let body = if i % 3 == 0 { "alpha zeta zeta" } else { "alpha" };
            db.insert("notes", note(format!("n{i:04}"), body.into())).unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        for i in (0..200usize).step_by(9) {
            db.delete_key("notes", &format!("n{i:04}")).unwrap();
        }

        let want =
            BTreeMap::from([("body".to_string(), vec!["alpha".to_string(), "zeta".to_string()])]);
        let ts = db.clock.peek();
        let cached = db.gather_stats("notes", &want, ts, false).unwrap();
        let exact = db.gather_stats("notes", &want, ts, true).unwrap();
        let (c, e) = (&cached["body"], &exact["body"]);
        assert_eq!(c.num_docs, e.num_docs);
        assert_eq!(c.avg_doc_len.to_bits(), e.avg_doc_len.to_bits(), "by bits: it is an average");
        assert_eq!(c.doc_freq, e.doc_freq);
        assert!(c.num_docs > 0 && c.doc_freq["alpha"] > 0, "not agreeing by both being empty");

        // A term nothing holds is the one place the two arms are spelled
        // differently, and the difference is deliberate rather than a
        // divergence: the exact arm leaves it out, the cached arm stores an
        // explicit zero, because without it every query for a term outside the
        // corpus would re-walk every unit looking for it. `GlobalStats::idf`
        // reads a missing entry as zero, so the weight both arms produce is
        // the same.
        let want = BTreeMap::from([("body".to_string(), vec!["quokka".to_string()])]);
        let cached = db.gather_stats("notes", &want, ts, false).unwrap();
        let exact = db.gather_stats("notes", &want, ts, true).unwrap();
        assert_eq!(cached["body"].doc_freq["quokka"], 0);
        assert!(exact["body"].doc_freq.is_empty());
        assert_eq!(cached["body"].idf("quokka"), exact["body"].idf("quokka"));
        assert_eq!(
            guard(&db.stats).get(&cache_key("notes", "body")).unwrap().doc_freq["quokka"],
            0,
            "and the zero is in the cache, so the next query does not walk for it again"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_term_filled_mid_epoch_is_measured_against_the_document_count_it_will_be_divided_by() {
        // The coherence pin, and the reason `fill_term_stats` overwrites the
        // cached globals from the same `Shard::term_stats` call that produced
        // its frequencies. Drop that one step and this test fails: the burst
        // below stays inside a single refresh window, so `num_docs` would be
        // left at the 20 documents the epoch was anchored on while
        // `doc_freq["beta"]` was measured over all 420. `df > n` is then
        // reachable, IDF's clamp fires, and the two terms — one in 20
        // documents, one in 400 — collapse onto the same weight. That
        // flattening is the failure mode, not a rounding error.
        //
        // The direction here is inserts, which is the direction the re-anchor
        // alone repairs. The sibling
        // `a_frequency_and_the_count_it_is_divided_by_are_never_from_different_instants`
        // covers deletes, where re-anchoring the globals and keeping the old
        // frequencies is worse than doing neither.
        let dir = tmp("stats-coherent-fill");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let note = |id: String, body: &str| {
            Value::obj(vec![
                ("id".into(), Value::Str(id)),
                ("body".into(), Value::Str(body.into())),
            ])
        };
        for i in 0..20usize {
            db.insert("notes", note(format!("n{i:04}"), "alpha")).unwrap();
        }
        let want = |terms: Vec<&str>| {
            BTreeMap::from([(
                "body".to_string(),
                terms.into_iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            )])
        };
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(vec!["alpha"]), ts, false).unwrap();

        // 400 documents carrying a term the cache has never seen. 420 writes
        // in total is short of `STATS_REFRESH_WRITES`, so no refresh point
        // passes and the globals are whatever the fill leaves behind.
        for i in 0..400usize {
            db.insert("notes", note(format!("m{i:04}"), "beta")).unwrap();
        }
        assert!(db.writes < STATS_REFRESH_WRITES, "the burst has to fit inside one epoch");
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want(vec!["alpha", "beta"]), ts, false).unwrap();
        let g = &g["body"];

        assert_eq!(g.num_docs, 420, "the fill re-anchored the count on the corpus it measured");
        assert_eq!(g.doc_freq["beta"], 400);
        assert_eq!(
            g.doc_freq["alpha"], 20,
            "and `alpha` was re-measured in the same call rather than carried across the \
             re-anchor, so the whole triple is one instant. 400 inserts and no deletes, so the \
             value is the one the first fill saw — what would differ is which `num_docs` it is \
             coherent with"
        );
        assert!(
            g.idf("alpha") > g.idf("beta"),
            "a term in 20 of 420 documents has to outweigh one in 400 of them: idf(alpha) = {}, \
             idf(beta) = {}",
            g.idf("alpha"),
            g.idf("beta")
        );
        assert!(g.idf("beta") > 0.0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_frequency_and_the_count_it_is_divided_by_are_never_from_different_instants() {
        // The guarantee the default path offers, stated as the thing that can
        // break it: every triple this path returns is a set of live sums at
        // ONE instant. Stale by up to [`STATS_REFRESH_WRITES`] writes, never
        // mixed. A `df` measured over one corpus divided by an `n` measured
        // over another is not a stale answer, it is an answer to no question,
        // and it is arbitrarily wrong rather than boundedly wrong: the error
        // is a function of `df/n`, so a drift bounded at 511 DOCUMENTS is
        // negligible at a million documents and total at five hundred.
        //
        // The shape below is the one that cannot be repaired by re-gathering
        // when a term is missing, because nothing is missing: the query that
        // mixes the instants (`newterm`) does not ask about `alpha` at all,
        // and the query that reads `alpha` back runs no gather under a cache
        // that keeps frequencies across a re-anchor. Measured against that
        // cache: `df(alpha) = 300` against `num_docs = 250` — `df > n`, which
        // is what `idf_for_df`'s clamp exists to survive — and `idf(alpha) =
        // 0.00199` against an exact 1.6035, an 804x under-weight, where doing
        // nothing at all would have been 3.1x. `fill_term_stats` drops every
        // frequency it did not measure under the globals it is about to write,
        // which costs the drop's re-gather and buys the sentence above.
        let dir = tmp("stats-one-instant");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let note = |id: String, body: &str| {
            Value::obj(vec![
                ("id".into(), Value::Str(id)),
                ("body".into(), Value::Str(body.into())),
            ])
        };
        for i in 0..300usize {
            db.insert("notes", note(format!("a{i:04}"), "alpha zeta")).unwrap();
        }
        for i in 0..200usize {
            db.insert("notes", note(format!("z{i:04}"), "zeta")).unwrap();
        }
        let want = |terms: Vec<&str>| {
            BTreeMap::from([(
                "body".to_string(),
                terms.into_iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            )])
        };

        // `alpha` measured over 500 documents: 300 of them hold it.
        let ts = db.clock.peek();
        let first = db.gather_stats("notes", &want(vec!["alpha"]), ts, false).unwrap();
        assert_eq!((first["body"].num_docs, first["body"].doc_freq["alpha"]), (500, 300));

        // Then 250 of the alpha-bearing documents go away. Deletes only, so
        // this is the direction staleness alone is harmless in and a mixed
        // instant is not.
        for i in 0..250usize {
            db.delete_key("notes", &format!("a{i:04}")).unwrap();
        }
        let at = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
        assert!(
            db.writes - at < STATS_REFRESH_WRITES,
            "no refresh point may pass: the whole point is that this is INSIDE one epoch, where \
             the cache is entitled to be stale"
        );

        // An unrelated query re-anchors the globals on the corpus as it is
        // now. It never mentions `alpha`, which is what makes this case
        // unreachable for any repair keyed on the current query's terms.
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(vec!["newterm"]), ts, false).unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(c.num_docs, 250, "the globals moved with the deletes");
        assert!(
            !c.doc_freq.contains_key("alpha"),
            "and the frequency measured under the old globals went with them, rather than \
             staying to be divided by a count it was never measured against"
        );
        assert_eq!(
            c.doc_freq.len(),
            c.fill_order.len(),
            "the map and the eviction queue are dropped together, or the cap stops holding"
        );

        // The read that follows. Whether it re-gathers is an implementation
        // detail; that it is coherent is not.
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want(vec!["alpha"]), ts, false).unwrap();
        let e = db.gather_stats("notes", &want(vec!["alpha"]), ts, true).unwrap();
        let (g, e) = (&g["body"], &e["body"]);
        assert!(
            g.doc_freq["alpha"] <= g.num_docs,
            "df(alpha) = {} exceeds num_docs = {}",
            g.doc_freq["alpha"],
            g.num_docs
        );
        assert_eq!((g.num_docs, g.doc_freq["alpha"]), (250, 50));
        assert_eq!(g.num_docs, e.num_docs);
        assert_eq!(g.avg_doc_len.to_bits(), e.avg_doc_len.to_bits(), "by bits: it is an average");
        assert_eq!(g.doc_freq, e.doc_freq);
        assert_eq!(g.idf("alpha").to_bits(), e.idf("alpha").to_bits());

        // And again with nothing missing and nothing to gather, which is the
        // arm that reads the cache directly.
        let g2 = db.gather_stats("notes", &want(vec!["alpha"]), ts, false).unwrap();
        assert_eq!(g2["body"].num_docs, g.num_docs);
        assert_eq!(g2["body"].doc_freq, g.doc_freq);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_statistics_refresh_at_the_same_write_counts_whatever_the_shard_count() {
        // Why a stale statistic is not a shard-dependent one, as an assertion
        // rather than as a paragraph of the `CachedStats` comment. The gate is
        // a write counter — engine-wide, as it happens, but what matters is
        // that no shard's seal or compaction schedule touches it — so one
        // shard and six cross it after the same writes and reset against the
        // same corpus; a stale read is
        // then the same live quantity taken at the same earlier instant, not a
        // different quantity. Compare with the counts this path used to make,
        // which were rebuilt at these same moments and still disagreed,
        // because what they measured depended on when each shard had sealed.
        let run = |splits: &str, tag: &str| {
            let dir = tmp(tag);
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            db.execute(&format!("CREATE COLLECTION notes (id TEXT PRIMARY KEY){splits}")).unwrap();
            db.execute(
                "CREATE INDEX notes_body ON notes USING fulltext (body) \
                 WITH (analyzer = 'english')",
            )
            .unwrap();
            let want = BTreeMap::from([("body".to_string(), vec!["alpha".to_string()])]);
            let mut points = Vec::new();
            for i in 0..2000usize {
                let body = if i % 4 == 0 { "alpha zeta" } else { "alpha" };
                db.insert(
                    "notes",
                    Value::obj(vec![
                        ("id".into(), Value::Str(format!("n{i:04}"))),
                        ("body".into(), Value::Str(body.into())),
                    ]),
                )
                .unwrap();
                if i % 13 == 0 {
                    db.delete_key("notes", &format!("n{:04}", i / 2)).unwrap();
                }
                if i % 500 == 0 {
                    db.execute("FLUSH notes").unwrap();
                }
                let ts = db.clock.peek();
                db.gather_stats("notes", &want, ts, false).unwrap();
                let at =
                    guard(&db.stats).get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
                if points.last() != Some(&at) {
                    points.push(at);
                }
            }
            let _ = fs::remove_dir_all(&dir);
            points
        };

        let one = run("", "refresh-points-1");
        let three = run(" WITH (splits = ['n0700', 'n1400'])", "refresh-points-3");
        let six = run(
            " WITH (splits = ['n0350', 'n0700', 'n1050', 'n1400', 'n1750'])",
            "refresh-points-6",
        );
        assert!(points_are_sane(&one), "the workload has to cross several refresh points: {one:?}");
        assert_eq!(one, three, "1 shard vs 3 shards");
        assert_eq!(one, six, "1 shard vs 6 shards");
    }

    #[test]
    fn the_epoch_clock_keeps_running_when_every_query_fills_a_new_term() {
        // `fill_term_stats` deliberately does not touch `refreshed_at_writes`,
        // and the test above cannot see it: that one asks for the same term
        // every iteration, so the only fill per epoch happens at the instant
        // the reset has just written the same value, and bumping it there is a
        // no-op. A query stream with a long tail of distinct terms fills on
        // EVERY query, and a fill that reset the clock would push the refresh
        // point forward every time — pinning the globals, and every frequency
        // measured under them, to an epoch that never ends.
        let dir = tmp("stats-epoch-clock");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let mut points = Vec::new();
        for i in 0..2000usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha".into())),
                ]),
            )
            .unwrap();
            // A term no earlier query asked for, so every one of these fills.
            let want = BTreeMap::from([("body".to_string(), vec![format!("q{i:06}")])]);
            let ts = db.clock.peek();
            db.gather_stats("notes", &want, ts, false).unwrap();
            let at = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
            if points.last() != Some(&at) {
                points.push(at);
            }
        }
        assert!(
            points_are_sane(&points),
            "the epoch clock has to keep running under a fill on every query: {points:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_entry_cap_survives_an_epoch_rollover() {
        // The cap and the epoch boundary, which are only ever tested apart.
        // A reset starts the epoch by dropping `doc_freq` AND `fill_order`,
        // and dropping one without the other is silent in every other test:
        // the queue is drained only by over-cap eviction, so it would grow
        // across epochs without bound — an unbounded `String` leak — and once
        // it is longer than the map the eviction loop pops names the map no
        // longer holds, removing nothing while draining the queue, so the map
        // stops being bounded by [`STATS_TERM_CAP`] at all.
        let dir = tmp("stats-cap-rollover");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let note = |i: usize| {
            Value::obj(vec![
                ("id".into(), Value::Str(format!("n{i:06}"))),
                ("body".into(), Value::Str("alpha".into())),
            ])
        };
        for i in 0..40usize {
            db.insert("notes", note(i)).unwrap();
        }
        let want = |terms: Vec<String>| BTreeMap::from([("body".to_string(), terms)]);

        // Epoch one: a few terms.
        let old: Vec<String> = (0..3).map(|i| format!("old{i:04}")).collect();
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(old.clone()), ts, false).unwrap();
        assert_eq!(guard(&db.stats).get(&cache_key("notes", "body")).unwrap().fill_order.len(), 3);

        // Over a refresh point, into epoch two.
        for i in 0..STATS_REFRESH_WRITES as usize {
            db.insert("notes", note(1000 + i)).unwrap();
        }
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(vec!["new0000".to_string()]), ts, false).unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            c.doc_freq.keys().cloned().collect::<Vec<_>>(),
            vec!["new0000".to_string()],
            "a refresh point starts the epoch empty: a frequency measured in the last epoch must \
             not be read against globals measured in this one"
        );
        assert_eq!(
            c.fill_order.iter().cloned().collect::<Vec<_>>(),
            vec!["new0000".to_string()],
            "and the eviction queue went with it, or it never shrinks again"
        );

        // And the cap still bounds the map on the far side of the rollover.
        let n = STATS_TERM_CAP + 7;
        let terms: Vec<String> = (0..n).map(|i| format!("r{i:06}")).collect();
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(terms), ts, false).unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(c.doc_freq.len(), STATS_TERM_CAP, "the cap is a cap in the second epoch too");
        assert_eq!(c.fill_order.len(), c.doc_freq.len(), "and the queue is still in step with it");
        assert!(
            old.iter().all(|t| !c.doc_freq.contains_key(t)),
            "nothing from the first epoch survived into the second"
        );

        // The other way the two desync, and the cheapest: a term list is not a
        // set — `gather_stats` takes whatever the caller hands it — and a
        // repeat must not push a second copy of the name into the queue, or
        // the eviction loop drains a slot further than it evicts.
        //
        // The term repeated is `alpha`, which every document in this fixture
        // holds, and the VALUE is asserted. It used to be `dup`, which nothing
        // holds: `df` was zero, and zero counted twice is still zero, so the
        // leg read as coverage of the repeated-term case while saying nothing
        // about the number it produces. The number was wrong — twice the real
        // frequency, on both arms. See [`term_set`].
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want(vec!["alpha".to_string(); 2]), ts, false).unwrap();
        let g = &g["body"];
        let live = 40 + STATS_REFRESH_WRITES;
        assert_eq!(g.num_docs, live);
        assert_eq!(
            g.doc_freq["alpha"], live,
            "every document holds it once; naming the term twice does not put it in them twice"
        );
        assert!(
            g.doc_freq["alpha"] <= g.num_docs,
            "the double count made this `df > num_docs`, which is a negative logarithm"
        );
        assert!(g.idf("alpha") > 0.0, "and the IDF clamp fired on a term the corpus is full of");
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(c.doc_freq.len(), STATS_TERM_CAP, "the cap still holds");
        assert_eq!(
            c.fill_order.len(),
            c.doc_freq.len(),
            "a repeated term is one map entry and one queue slot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_query_at_the_same_instant_still_gets_the_first_query_s_frequency() {
        // The merge, which is the whole reason `fill_term_stats` returns a
        // triple instead of letting the caller read the cache back — and which
        // nothing in the tree pinned, because every other multi-term fixture
        // writes between its two gathers and so takes the
        // re-gather-everything branch, where the answer happens to be exactly
        // what this call measured.
        //
        // Two gathers at the SAME instant, with overlapping but not identical
        // term lists and NO write between them. That is the shape that takes
        // the fast path: the second call gathers only `beta`, because `alpha`
        // is already cached under globals measured at this same instant. So
        // `alpha`'s frequency has to come out of the cache and be merged into
        // the answer. Build the answer from the freshly gathered map alone and
        // `alpha` comes back `df = 0` — the highest weight IDF has — for a
        // term every document in the corpus holds.
        let dir = tmp("stats-same-instant-merge");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['n0030'])")
            .unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..90usize {
            let body = if i % 3 == 0 { "alpha beta" } else { "alpha" };
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str(body.into())),
                ]),
            )
            .unwrap();
        }
        let want = |terms: Vec<String>| BTreeMap::from([("body".to_string(), terms)]);

        let ts = db.clock.peek();
        let first = db.gather_stats("notes", &want(vec!["alpha".to_string()]), ts, false).unwrap();
        assert_eq!(first["body"].doc_freq["alpha"], 90, "every document holds it");

        // No write, so `measured_at_writes == self.writes` and the fast path
        // holds. Assert that it does, or the test could go green by taking the
        // slow branch and prove nothing about the merge.
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(c.measured_at_writes, db.writes, "the fast path is what this test is about");
        assert!(c.anchored && c.doc_freq.contains_key("alpha"));

        let ts = db.clock.peek();
        let g = db
            .gather_stats("notes", &want(vec!["alpha".to_string(), "beta".to_string()]), ts, false)
            .unwrap();
        let g = &g["body"];
        assert_eq!(g.num_docs, 90);
        assert_eq!(
            g.doc_freq["alpha"], 90,
            "`alpha` was filled by the first query and this one did not re-gather it, so it comes \
             from the cache — dropping the merge answers 0 here, the maximum IDF, for the most \
             common term in the corpus"
        );
        assert_eq!(g.doc_freq["beta"], 30, "and `beta` is what this call did gather");
        assert!(
            g.idf("alpha") < g.idf("beta"),
            "a term in every document weighs less than one in \
             a third of them; the unmerged answer reverses that"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_average_document_length_falls_back_only_when_there_is_nothing_to_average() {
        // The one arithmetic guard on the default path, from both sides. It is
        // a division, and the fallback exists because the denominator can be
        // zero — so the test has to hold the boundary at zero AND at one, or
        // it holds nothing: `num_docs > 0` widened to `>= 0` divides 0 by 0,
        // and `avgdl` is a denominator inside every BM25 term, so one NaN
        // makes every score on the query NaN and every comparison between two
        // of them false. Narrowed to `> 1` it is quieter and no more correct:
        // a one-document collection gets the literal 1.0 instead of its own
        // average, so a document of forty terms is scored as if it were one.
        let dir = tmp("stats-avgdl-boundary");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        for c in ["empty", "single"] {
            db.execute(&format!("CREATE COLLECTION {c} (id TEXT PRIMARY KEY)")).unwrap();
            db.execute(&format!(
                "CREATE INDEX {c}_body ON {c} USING fulltext (body) WITH (analyzer = 'english')"
            ))
            .unwrap();
        }
        let want = BTreeMap::from([("body".to_string(), vec!["alpha".to_string()])]);

        // Zero documents: nothing to average, so the fallback is the answer.
        let ts = db.clock.peek();
        let g = db.gather_stats("empty", &want, ts, false).unwrap();
        let g = &g["body"];
        assert_eq!(g.num_docs, 0, "the fixture is an indexed collection with no documents in it");
        assert!(
            g.avg_doc_len.is_finite(),
            "0/0 is NaN and NaN propagates: every BM25 score on the query becomes NaN, every \
             comparison between two of them is false, and the ranking is whatever order the \
             sort happened to start in"
        );
        assert_eq!(g.avg_doc_len.to_bits(), 1.0f64.to_bits(), "by bits: it is an average");

        // One document: there IS something to average, and the average is its
        // own length, not the fallback.
        db.insert(
            "single",
            Value::obj(vec![
                ("id".into(), Value::Str("n0".into())),
                ("body".into(), Value::Str("alpha beta gamma delta".into())),
            ]),
        )
        .unwrap();
        let ts = db.clock.peek();
        let g = db.gather_stats("single", &want, ts, false).unwrap();
        let e = db.gather_stats("single", &want, ts, true).unwrap();
        let (g, e) = (&g["body"], &e["body"]);
        assert_eq!(g.num_docs, 1);
        assert!(
            g.avg_doc_len > 1.0,
            "one document still has an average, and it is that document's own length — the \
             fallback is for having nothing to average, not for having little"
        );
        assert_eq!(g.avg_doc_len.to_bits(), 4.0f64.to_bits(), "one document of four terms");
        assert_eq!(
            g.avg_doc_len.to_bits(),
            e.avg_doc_len.to_bits(),
            "and the two arms agree at the boundary, as they do everywhere else"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn a_historical_timestamp_is_raised_to_the_last_commit_where_the_assertion_is_gone() {
        // The other half of the test below, in the build downstream users
        // actually ship. `debug_assert!` compiles away in release, so the
        // misuse it describes used to run to completion there: the gather at
        // the historical `ts` was written into the epoch entry with
        // `measured_at_writes` set to the LIVE write counter, so the next
        // ordinary query's coherence check said the triple was current and kept
        // the historical `df` beside freshly gathered globals — `df = 0` for a
        // term 57 of 60 documents hold, which is the incoherent `df`/`n`
        // pairing this whole arm exists to make unreachable, and it inverted a
        // ranking for queries that asked for nothing historical.
        let dir = tmp("stats-historical-ts-release");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..60usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    // Disjoint groups: `rare` is the term whose ranking a
                    // poisoned `df` for `alpha` would invert.
                    ("body".into(), Value::Str(if i < 3 { "rare" } else { "alpha" }.into())),
                ]),
            )
            .unwrap();
        }
        let want = BTreeMap::from([("body".to_string(), vec!["alpha".to_string()])]);
        let historical = db.last_commit - 1;
        assert!(historical > 0);

        let g = db.gather_stats("notes", &want, historical, false).unwrap();
        let g = &g["body"];
        assert_eq!(g.num_docs, 60, "raised to the last commit, not answered at the past");
        assert_eq!(g.doc_freq.get("alpha"), Some(&57));

        // And the epoch entry it wrote is the live corpus, so an ordinary query
        // that follows reads a coherent triple.
        let later = db.gather_stats("notes", &want, db.last_commit, false).unwrap();
        assert_eq!(later["body"].num_docs, 60);
        assert_eq!(later["body"].doc_freq.get("alpha"), Some(&57));

        // The ranking that inverted: three `rare` documents must outrank the
        // fifty-seven `alpha` ones.
        let r = db
            .query(
                "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'alpha rare'), \
                 method => 'linear', k => 100000) LIMIT 3",
            )
            .unwrap();
        let got: Vec<&str> = r.rows.iter().map(|x| x.key.as_str()).collect();
        assert_eq!(got, vec!["n0000", "n0001", "n0002"], "the rarer term wins");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a historical `as_of` must be read with `exact: true`")]
    fn a_historical_timestamp_on_the_default_arm_is_a_caller_error_and_not_an_approximation() {
        // `gather_stats` is public on a published crate, so a `debug_assert`
        // in it is a downstream-visible change and has to earn its place. It
        // does, and the line between the two arms is what earns it: the exact
        // call below is a legitimate read of the past and returns; the default
        // call is not an inaccurate answer to this caller but a write into
        // epoch-lived state that every LATER query in the epoch reads, which
        // is a contract rather than a quality note. `run_select` pins
        // `clock.peek().max(last_commit)` and the crate has no `AS OF` syntax,
        // so no SQL path can reach this — only a direct caller, who is exactly
        // who the panic is for.
        let dir = tmp("stats-historical-ts");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..20usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha".into())),
                ]),
            )
            .unwrap();
        }
        let want = BTreeMap::from([("body".to_string(), vec!["alpha".to_string()])]);
        let historical = db.last_commit - 1;
        assert!(historical > 0, "the fixture has to have committed something to have a past");

        // Supported, and it must not panic: it writes nothing, so there is
        // nothing to poison.
        db.gather_stats("notes", &want, historical, true).unwrap();

        // Not supported.
        let _ = db.gather_stats("notes", &want, historical, false);
        unreachable!("a historical timestamp on the default arm has to be refused");
    }

    #[test]
    fn a_term_named_twice_is_counted_once_on_both_arms() {
        // A term list is a `Vec<String>` on a public method, so `["dup",
        // "dup"]` is expressible, and the shard gather walks one posting
        // cursor per element: the term came back at twice its real frequency.
        // Not stale — `df > num_docs`, a negative logarithm, and the IDF clamp
        // on a term every document holds. No SQL query can reach it, because
        // `required_terms` sorts and dedups, which is precisely why nothing
        // caught it.
        let dir = tmp("stats-duplicate-terms");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['n0040'])")
            .unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..80usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("dup zeta".into())),
                ]),
            )
            .unwrap();
        }
        let want = BTreeMap::from([("body".to_string(), vec!["dup".to_string(); 2])]);

        // Both arms, because both summed the same double-counted gather.
        let ts = db.clock.peek();
        for exact in [true, false] {
            let g = db.gather_stats("notes", &want, ts, exact).unwrap();
            let g = &g["body"];
            assert_eq!(g.num_docs, 80);
            assert_eq!(
                g.doc_freq["dup"], 80,
                "exact = {exact}: 80 documents hold `dup` once each, so naming it twice in the \
                 term list cannot make it 160"
            );
            assert!(
                g.doc_freq["dup"] <= g.num_docs,
                "exact = {exact}: `df > num_docs` is a negative logarithm, not a stale number"
            );
            assert!(g.idf("dup") > 0.0, "exact = {exact}: and it drove IDF onto its clamp");
        }

        // The other shape a direct caller can hand over, and the one that
        // costs an allocation: not sorted, and repeating. First-occurrence
        // order is kept rather than sorted, because the order of the fill is
        // the order the entry cap evicts in and that is the caller's choice.
        let want = BTreeMap::from([(
            "body".to_string(),
            vec!["zeta".to_string(), "dup".to_string(), "zeta".to_string()],
        )]);
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want, ts, false).unwrap();
        assert_eq!(g["body"].doc_freq["zeta"], 80);
        assert_eq!(g["body"].doc_freq["dup"], 80, "already cached, and still counted once");
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            c.fill_order.iter().cloned().collect::<Vec<_>>(),
            vec!["dup".to_string(), "zeta".to_string()],
            "`dup` was filled by the first query and `zeta` by the second; one slot each"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_only_query_still_gets_real_globals() {
        // Two things, and they used to be one. `required_terms` creates a
        // path entry for a text query whether or not it names a term, so
        // `terms` can arrive EMPTY — and the empty slice still has to yield a
        // real `num_docs` and `avgdl`, which the fill's own gather provides in
        // one pass. Nothing else in the tree asserts on that, and the scorer
        // divides every length norm by the second of them.
        //
        // The second half is what changed: `run_select` now resolves a
        // prefix against every unit of every shard before the gather and hands
        // the resolved terms to it, so a prefix query does NOT ask for nothing
        // any more. The final assertion used to read `doc_freq.is_empty()`,
        // "a prefix query asks the gather for no terms", and that sentence was
        // the defect written down as a guarantee: it is exactly why an
        // expanded term was weighted by whichever segment happened to score
        // it. It is inverted below.
        let dir = tmp("stats-prefix-globals");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..200usize {
            let body = if i % 4 == 0 { "alphabet zeta zeta" } else { "alphabet" };
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str(body.into())),
                ]),
            )
            .unwrap();
        }

        let want = BTreeMap::from([("body".to_string(), Vec::<String>::new())]);
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want, ts, false).unwrap();
        let e = db.gather_stats("notes", &want, ts, true).unwrap();
        let (g, e) = (&g["body"], &e["body"]);
        assert!(!g.exact, "this leg has to be measuring the cached path");
        assert_eq!(g.num_docs, 200);
        assert_eq!(g.num_docs, e.num_docs);
        assert_eq!(g.avg_doc_len.to_bits(), e.avg_doc_len.to_bits(), "by bits: it is an average");
        assert!(g.avg_doc_len > 1.0, "a real average, not the `num_docs == 0` fallback of 1.0");
        assert!(g.doc_freq.is_empty(), "no term was asked about, so none is answered");

        // Now through a real query. `alph*` resolves to `alphabet` — every
        // document has it — so the gather is asked for it and answers its
        // real collection-wide count, which is what makes the weight the same
        // in every unit that scores it.
        db.query("SELECT id FROM notes WHERE text_match(body, 'alph*') LIMIT 5").unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert!(c.anchored, "the globals were measured, and `num_docs == 0` cannot say so");
        assert_eq!(c.num_docs, 200);
        assert_eq!(
            c.doc_freq.get("alphabet").copied(),
            Some(200),
            "the coordinator resolved `alph*` and gathered a global df for what it resolved to"
        );
        assert_eq!(c.doc_freq.get("zeta"), None, "and only for what it resolved to");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_resolves_to_the_same_terms_at_every_shard_count() {
        // The statistic-level pin under the two end-to-end tests in
        // `tests/integration.rs`, and the one place the CAP itself is
        // asserted. Two claims, and they are different claims:
        //
        //   * the resolved list is the lexicographically first
        //     `PREFIX_EXPANSION_LIMIT` of the UNION over every unit of every
        //     shard. Taking the cap per unit — which is what `scorer::build`
        //     did, and still does when it is called without a coordinator —
        //     gives a union of per-unit cuts, and how many terms that is
        //     depends on how many units there are;
        //   * the triple gathered for that list is identical at every shard
        //     count, bit for bit. Bits are safe HERE and nowhere near a score:
        //     this is a sum of counts and an average of lengths, not an f32
        //     accumulation whose order depends on cursor positions.
        //
        // 600 distinct terms against a cap of 512, deliberately: the cap has
        // to bind or neither claim is being tested. The mid-way FLUSH is
        // load-bearing too — with one unit per shard the per-unit cut and the
        // union cut coincide, and the old code passes.
        let run = |splits: &str, tag: &str| {
            let dir = tmp(tag);
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            db.execute(&format!("CREATE COLLECTION notes (id TEXT PRIMARY KEY){splits}")).unwrap();
            db.execute(
                "CREATE INDEX notes_body ON notes USING fulltext (body)                  WITH (analyzer = 'english')",
            )
            .unwrap();
            for i in 0..600usize {
                if i == 300 {
                    db.execute("FLUSH notes").unwrap();
                }
                db.insert(
                    "notes",
                    Value::obj(vec![
                        ("id".into(), Value::Str(format!("n{i:04}"))),
                        // Length variance so `avgdl` is a real average that a
                        // bit comparison can catch moving.
                        ("body".into(), Value::Str(format!("a{i:05}{}", " pad".repeat(i % 5)))),
                    ]),
                )
                .unwrap();
            }
            db.execute("FLUSH notes").unwrap();
            // The ANSWER as well as the cache. Asserting only on `db.stats`
            // couples this test to `STATS_TERM_CAP`, a tuning constant it does
            // not mention: dropping that to 256 fails the assertions below
            // while every query still answers correctly, and — the direction
            // that matters — a defect in the answer path can be papered over by
            // the cache happening to hold the right numbers.
            let mut rows: Vec<String> = db
                .query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 100000")
                .unwrap()
                .rows
                .iter()
                .map(|r| r.key.clone())
                .collect();
            rows.sort();
            let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
            let got = (c.num_docs, c.total_doc_len, c.doc_freq.clone(), rows);
            let _ = fs::remove_dir_all(&dir);
            got
        };

        let one = run("", "prefix-pinned-1");
        let three = run(" WITH (splits = ['n0200', 'n0400'])", "prefix-pinned-3");
        let six = run(
            " WITH (splits = ['n0100', 'n0200', 'n0300', 'n0400', 'n0500'])",
            "prefix-pinned-6",
        );

        // `pad` is in the dictionary and matches no prefix, so its absence
        // also says the gather was asked for the expansion and not for the
        // vocabulary.
        let want: BTreeMap<String, u64> =
            (0..PREFIX_EXPANSION_LIMIT).map(|i| (format!("a{i:05}"), 1u64)).collect();
        assert_eq!(one.2, want, "the lexicographically first 512 of 600, each in one document");
        assert_eq!(one.0, 600, "and every document counts, including those the cut left out");
        let want_rows: Vec<String> =
            (0..PREFIX_EXPANSION_LIMIT).map(|i| format!("n{i:04}")).collect();
        assert_eq!(one.3, want_rows, "and the rows the query returned, not just the cache");
        assert_eq!(one, three, "1 shard vs 3 shards");
        assert_eq!(one, six, "1 shard vs 6 shards");
    }

    /// A collection of `n` notes `n00000..`, each with its own `a#####` term,
    /// of which the first `dead` are rewritten to a `b` term after a FLUSH — so
    /// a run of terms sorting before every survivor is in the dictionary and
    /// held by no live document.
    fn dead_run(dir: &std::path::Path, splits: &str, n: usize, dead: usize) -> Db {
        let mut db = Db::open(dir, DbOpts::default()).unwrap();
        db.execute(&format!("CREATE COLLECTION notes (id TEXT PRIMARY KEY){splits}")).unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let put = |db: &mut Db, i: usize, t: char| {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:05}"))),
                    ("body".into(), Value::Str(format!("{t}{i:05}"))),
                ]),
            )
            .unwrap();
        };
        for i in 0..n {
            put(&mut db, i, 'a');
        }
        db.execute("FLUSH notes").unwrap();
        for i in 0..dead {
            put(&mut db, i, 'b');
        }
        db.execute("FLUSH notes").unwrap();
        db
    }

    #[test]
    fn no_term_in_a_resolved_expansion_comes_back_with_a_zero_frequency() {
        // The statistics-level statement of the same invariant the integration
        // test asserts on rows, and it is cheap enough to be worth having
        // twice: in a quiescent fixture a resolved term CANNOT legitimately
        // have `df == 0`. A term only reaches zero by dying between the
        // dictionary read and the gather, and nothing writes while this
        // assertion runs — so a zero here means a PHYSICAL term got into the
        // expansion, whatever path put it there.
        //
        // That generality is the point. This fails the moment a dead term
        // re-enters the pinned list, including through some future route that
        // has nothing to do with prefixes, and it does so without needing a
        // fixture tuned to make the cap bind.
        let dir = tmp("expansion-zero-df");
        let mut db = dead_run(&dir, "", 1000, 600);
        db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 5").unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        let dead: Vec<&String> =
            c.doc_freq.iter().filter(|(_, &d)| d == 0).map(|(t, _)| t).collect();
        assert!(dead.is_empty(), "terms no live document holds, in a pinned expansion: {dead:?}");
        assert_eq!(c.doc_freq.len(), 400, "the live matching vocabulary, all of it");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_expansion_over_an_unflushed_memtable_takes_the_first_terms() {
        // The memtable arm of the expansion, with nothing sealed. It is the arm
        // with the least cover in the crate — every other prefix fixture
        // flushes — and two independent defects live in it: returning the
        // matching terms in some order other than sorted (the coordinator's
        // union cap is correct ONLY because each unit answers its own first
        // `limit`), and applying the visibility filter AFTER `take(limit)`
        // instead of before it.
        let dir = tmp("prefix-memtable-order");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..600usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:05}"))),
                    ("body".into(), Value::Str(format!("a{i:05}"))),
                ]),
            )
            .unwrap();
        }
        // Deliberately NO flush: this has to be the memtable's own `BTreeMap`
        // range walk, not a dictionary block walk.
        let run = |db: &mut Db| {
            let r =
                db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 100000").unwrap();
            let mut ks: Vec<String> = r.rows.iter().map(|x| x.key.clone()).collect();
            ks.sort();
            (ks, r.truncated_prefixes)
        };
        let (got, cut) = run(&mut db);
        assert_eq!(got.len(), PREFIX_EXPANSION_LIMIT);
        assert_eq!(got.first().map(String::as_str), Some("n00000"), "the FIRST 512 of 600");
        assert_eq!(got.last().map(String::as_str), Some("n00511"), "not the last 512");
        assert_eq!(cut.len(), 1, "600 live terms against a cap of 512: {cut:?}");

        // And with one document deleted, which takes the masked arm rather
        // than the every-document-is-visible fast path: `a00000` is now held by
        // nobody, so it must not spend a slot — the answer shifts by exactly
        // one term at BOTH ends.
        assert!(db.delete_key("notes", "n00000").unwrap());
        let (got, cut) = run(&mut db);
        assert_eq!(got.len(), PREFIX_EXPANSION_LIMIT);
        assert_eq!(got.first().map(String::as_str), Some("n00001"));
        assert_eq!(got.last().map(String::as_str), Some("n00512"), "the dead term freed a slot");
        // The VERDICT, which is the only thing this leg can say that the leg
        // above cannot. The rows alone cannot separate filter-before-take from
        // filter-after-take here, because the coordinator asks each unit for
        // `cap + 1` and that spare slot absorbs exactly one displaced dead
        // term: both orders return the same 512 keys, and only the truncation
        // flag differs — 599 live terms reach the union under one and 512 under
        // the other.
        assert_eq!(cut.len(), 1, "599 live terms is still more than the cap: {cut:?}");

        // A dead RUN longer than the cap, which is where the two orders part
        // company on the ROWS as well. 520 dead terms sort before every live
        // one, so filter-after-take spends the whole budget stepping through
        // corpses and answers with nothing — a silently EMPTY result for a
        // query whose honest answer is 80 rows and no warning at all.
        for i in 1..520usize {
            assert!(db.delete_key("notes", &format!("n{i:05}")).unwrap());
        }
        let (got, cut) = run(&mut db);
        assert_eq!(got.len(), 80, "the live terms, all of them: `a00520`..`a00599`");
        assert_eq!(got.first().map(String::as_str), Some("n00520"));
        assert!(cut.is_empty(), "80 live terms is under the cap, so nothing was cut: {cut:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_statement_carrying_more_prefix_leaves_than_the_budget_is_refused() {
        // The per-statement bound. One leaf is bounded by the expansion cap;
        // nothing bounded how many leaves a `text_match` string may hold, and
        // the multiplier was therefore chosen by whoever wrote the query — 24
        // leaves measured at 1.2 s, gathering 12288 terms into a 4096-entry
        // cache that then evicts its own entries, so the identical statement
        // never warms.
        //
        // Refused rather than silently cut, which is the same choice the rest
        // of this change makes: a query that cannot be answered in full should
        // say so rather than answer less.
        let dir = tmp("prefix-leaf-budget");
        let mut db = cap_fixture(&dir, 64);
        let budget = prefix_leaves_limit(PREFIX_EXPANSION_LIMIT);
        let q = |n: usize| {
            let leaves = (0..n).map(|i| format!("a{i:05}*")).collect::<Vec<_>>().join(" ");
            format!("SELECT id FROM notes WHERE text_match(body, '{leaves}') LIMIT 5")
        };
        assert!(db.query(&q(budget)).is_ok(), "the budget itself is admitted");
        let e = db.query(&q(budget + 1)).unwrap_err().to_string();
        assert!(e.contains(&budget.to_string()), "the bound, so it can be met: {e}");

        // What the bound COUNTS, and the message saying the same thing. The
        // budget is taken over distinct `(path, prefix)` pairs, because the
        // coordinator resolves each pair once; a statement spelling one prefix
        // a dozen times pays for one expansion and is admitted. That is the
        // right cost model and this leg pins it — but the refusal used to
        // announce "this statement has N prefix leaves", a quantity the
        // statement above has twelve of and is not refused for, so an operator
        // counting leaves could neither predict the refusal nor meet it.
        assert!(e.contains("distinct prefixes"), "the message names what it counts: {e}");
        let repeated = (0..12).map(|_| "a00000*").collect::<Vec<_>>().join(" ");
        assert!(
            db.query(&format!("SELECT id FROM notes WHERE text_match(body, '{repeated}') LIMIT 5"))
                .is_ok(),
            "one prefix spelled twelve times is one expansion and is admitted"
        );

        // And SUMMED across paths, which nothing pinned: the check is
        // `.values().map(len).sum()`, while the constant's headline and README
        // both said "per indexed path" — a rule under which four-plus-four is
        // the same as five-plus-five, and an operator who split their statement
        // by it was refused again with no way to work out why.
        let dir2 = tmp("prefix-leaf-budget-two-paths");
        let mut db2 = Db::open(&dir2, DbOpts::default()).unwrap();
        db2.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        for (name, path) in [("notes_body", "body"), ("notes_title", "title")] {
            db2.execute(&format!(
                "CREATE INDEX {name} ON notes USING fulltext ({path}) \
                 WITH (analyzer = 'english')"
            ))
            .unwrap();
        }
        db2.insert(
            "notes",
            Value::obj(vec![
                ("id".into(), Value::Str("n0".into())),
                ("body".into(), Value::Str("zed".into())),
                ("title".into(), Value::Str("zed".into())),
            ]),
        )
        .unwrap();
        let two = |n: usize| {
            let leaves = (0..n).map(|i| format!("a{i:05}*")).collect::<Vec<_>>().join(" ");
            format!(
                "SELECT id FROM notes WHERE text_match(body, '{leaves}') \
                 AND text_match(title, '{leaves}') LIMIT 5"
            )
        };
        let half = budget / 2;
        assert!(db2.query(&two(half)).is_ok(), "{half} on each of two paths is the budget");
        let e = db2.query(&two(half + 1)).unwrap_err().to_string();
        assert!(e.contains(&(2 * (half + 1)).to_string()), "the summed count: {e}");
        assert!(e.contains("across its indexed paths"), "and it says the count is summed: {e}");

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dir2);
    }

    /// The expansion cap is a per-collection setting, and the leaf budget is
    /// derived from it. Each leg names the mutation that fails it: the union
    /// cut still reading the constant (600 terms at a cap of 1024 come back as
    /// 512); the budget still reading the constant (five prefixes admitted at
    /// 1024, where four is the budget); the ceiling unchecked (4097 accepted);
    /// the setting not persisted (a reopen answers 512 again); the export not
    /// carrying it (the copy answers 512); the DELETE refusal naming the
    /// constant rather than the cap; and CREATE ignoring its option.
    #[test]
    fn a_collection_can_raise_its_prefix_expansion_and_pays_with_its_leaf_budget() {
        fn ack(db: &mut Db, sql: &str) -> String {
            match db.execute(sql).unwrap() {
                Outcome::Ack(m) => m,
                other => panic!("{sql}: {other:?}"),
            }
        }
        let dir = tmp("prefix-cap-dial");
        // ONE unit, so that the per-unit ask is observable: spread over three,
        // each unit holds 200 terms and answers all of them whether it was
        // asked for `cap + 1` or for `512 + 1`, and a coordinator still asking
        // every unit for the default would pass. In one unit the raised cap
        // has to reach the ask, or 600 terms come back as 513.
        let mut db = cap_fixture_in(&dir, 600, 1);
        let wide = "SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 1000";
        let r = db.query(wide).unwrap();
        assert_eq!(r.rows.len(), PREFIX_EXPANSION_LIMIT, "the default cap cuts 600 to 512");
        assert!(!r.truncated_prefixes.is_empty(), "and says so");
        assert!(ack(&mut db, "SHOW CATALOG notes").contains("prefix_expansion=512"));

        let m = ack(&mut db, "ALTER COLLECTION notes SET (prefix_expansion = 1024)");
        assert!(m.contains("512 -> 1024") && m.contains("4 distinct"), "{m}");
        let r = db.query(wide).unwrap();
        assert_eq!(r.rows.len(), 600, "the whole vocabulary fits the raised cap");
        assert!(r.truncated_prefixes.is_empty(), "so nothing was cut: {:?}", r.truncated_prefixes);

        // The budget followed the cap down: 4096 / 1024.
        let q = |n: usize| {
            let leaves = (0..n).map(|i| format!("a{i:05}*")).collect::<Vec<_>>().join(" ");
            format!("SELECT id FROM notes WHERE text_match(body, '{leaves}') LIMIT 5")
        };
        assert!(db.query(&q(4)).is_ok(), "four is the budget at 1024");
        let e = db.query(&q(5)).unwrap_err().to_string();
        assert!(e.contains("limit is 4") && e.contains("1024"), "the budget and the cap: {e}");

        // The ceiling is the cache, and a refusal changes nothing.
        for bad in [0usize, PREFIX_EXPANSION_CEILING + 1] {
            let e = db
                .execute(&format!("ALTER COLLECTION notes SET (prefix_expansion = {bad})"))
                .unwrap_err()
                .to_string();
            assert!(e.contains(&PREFIX_EXPANSION_CEILING.to_string()), "{bad}: {e}");
        }
        assert!(ack(&mut db, "SHOW CATALOG notes").contains("prefix_expansion=1024"));
        ack(&mut db, "ALTER COLLECTION notes SET (prefix_expansion = 4096)");
        assert!(db.query(&q(1)).is_ok(), "one full expansion is exactly the cache");
        assert!(db.query(&q(2)).is_err(), "and two do not fit it");

        // A DELETE's refusal names the cap in force, not the constant.
        ack(&mut db, "ALTER COLLECTION notes SET (prefix_expansion = 256)");
        let e =
            db.execute("DELETE FROM notes WHERE text_match(body, 'a*')").unwrap_err().to_string();
        assert!(e.contains("CUT at 256"), "{e}");
        assert_eq!(db.query(wide).unwrap().rows.len(), 256, "a lowered cap cuts where it says");

        // Persisted: the setting is what the next open reads.
        drop(db);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert!(ack(&mut db, "SHOW CATALOG notes").contains("prefix_expansion=256"));
        assert_eq!(db.query(wide).unwrap().rows.len(), 256);

        // Carried by an export, so the copy answers as its source does.
        let exported = tmp("prefix-cap-export");
        db.export_collection("notes").unwrap().write_to(&exported).unwrap();
        let dst_dir = tmp("prefix-cap-import");
        let mut dst = Db::open(&dst_dir, DbOpts::default()).unwrap();
        dst.import_collection(&exported).unwrap();
        assert!(ack(&mut dst, "SHOW CATALOG notes").contains("prefix_expansion=256"));
        assert_eq!(dst.query(wide).unwrap().rows.len(), 256);
        // An export claiming a cap this build's cache cannot hold is refused
        // at import, before anything is adopted. Written by hand, because no
        // build with this ceiling can write one.
        let mut over = db.export_collection("notes").unwrap();
        let mut catalog = Catalog::decode(&over.catalog_bytes).unwrap();
        catalog.collections.get_mut("notes").unwrap().prefix_expansion = Some(5000);
        over.catalog_bytes = catalog.encode();
        let over_dir = tmp("prefix-cap-export-over");
        over.write_to(&over_dir).unwrap();
        let mut third = Db::open(&tmp("prefix-cap-import-over"), DbOpts::default()).unwrap();
        let e = third.import_collection(&over_dir).unwrap_err().to_string();
        assert!(e.contains("4096") && e.contains("5000"), "{e}");
        assert!(third.execute("SHOW CATALOG notes").is_err(), "and nothing was adopted");

        // And at creation, with the same check.
        let m = ack(
            &mut dst,
            "CREATE COLLECTION wide (id TEXT PRIMARY KEY) WITH (prefix_expansion = 2048)",
        );
        assert!(m.contains("created"), "{m}");
        assert!(ack(&mut dst, "SHOW CATALOG wide").contains("prefix_expansion=2048"));
        let e = dst
            .execute("CREATE COLLECTION wider (id TEXT PRIMARY KEY) WITH (prefix_expansion = 5000)")
            .unwrap_err()
            .to_string();
        assert!(e.contains("4096"), "{e}");
        assert!(dst.execute("SHOW CATALOG wider").is_err(), "a refused CREATE created nothing");

        for d in [&dir, &exported, &dst_dir, &over_dir, &tmp("prefix-cap-import-over")] {
            let _ = fs::remove_dir_all(d);
        }
    }

    /// DROP COLLECTION takes the entry, the shards, the files and everything
    /// recorded against the name -- and the last of those is the point: the
    /// statistics cache is keyed by name alone, so a collection recreated
    /// under the same name was answered from its predecessor's frequencies.
    /// Each leg names the mutation that fails it: the files left behind; the
    /// cache not pruned (the recreated collection gathers 10 documents where
    /// it has 3); the policy refusal skipped; the interrupted drop not
    /// completed at open, and the directory left aside not swept.
    #[test]
    fn dropping_a_collection_removes_it_and_everything_recorded_against_its_name() {
        fn ack(db: &mut Db, sql: &str) -> String {
            match db.execute(sql).unwrap() {
                Outcome::Ack(m) => m,
                other => panic!("{sql}: {other:?}"),
            }
        }
        let dir = tmp("drop-collection");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..10 {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i}"))),
                    ("body".into(), Value::Str("graph search".into())),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        // Warm the cache with the ten-document corpus.
        db.query("SELECT id FROM notes WHERE text_match(body, 'graph') LIMIT 5").unwrap();
        assert!(
            guard(&db.stats).contains_key(&cache_key("notes", "body")),
            "the fixture warmed nothing"
        );
        db.execute(
            "CREATE LIFECYCLE POLICY cool ON notes FOR (notes_body) \
             MOVE TO cached AFTER 1 hour OF INACTIVITY",
        )
        .unwrap();
        let e = db.execute("DROP COLLECTION notes").unwrap_err().to_string();
        assert!(e.contains("policy `cool`"), "refused while a policy names it: {e}");
        assert!(db.execute("SHOW CATALOG notes").is_ok(), "and nothing was dropped");
        db.execute("DROP LIFECYCLE POLICY cool").unwrap();

        assert_eq!(ack(&mut db, "DROP COLLECTION notes"), "collection `notes` dropped");
        assert!(db.execute("SHOW CATALOG notes").is_err());
        assert!(!dir.join("collections").join("notes").exists(), "the files were left behind");
        assert!(!dropping_dir(&dir, "notes").exists(), "the directory was left aside");
        assert!(
            guard(&db.stats).keys().all(|k| !k.starts_with("notes/")),
            "the statistics were kept"
        );
        assert!(db.catalog.activity.keys().all(|(c, _)| c != "notes"), "the clocks were kept");
        let e = db.execute("DROP COLLECTION notes").unwrap_err().to_string();
        assert!(e.contains("no such collection"), "{e}");

        // Recreated under the same name, with a corpus of three: the gather
        // must measure three, not answer ten out of the old cache.
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..3 {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("m{i}"))),
                    ("body".into(), Value::Str("graph search".into())),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        let ts = db.last_commit;
        let want = BTreeMap::from([("body".to_string(), vec!["graph".to_string()])]);
        let st = db.gather_stats("notes", &want, ts, false).unwrap();
        assert_eq!(st["body"].num_docs, 3, "answered from the dropped collection's cache");
        assert_eq!(st["body"].doc_freq["graph"], 3);
        assert_eq!(
            db.query("SELECT id FROM notes WHERE text_match(body, 'graph') LIMIT 10")
                .unwrap()
                .rows
                .len(),
            3
        );

        // Persisted: a reopen finds the recreated collection and no trace of
        // the dropped one.
        drop(db);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(db.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 3);

        // An interrupted drop: the directory is aside and the catalog still
        // names the collection, which is exactly the state a crash between the
        // rename and the catalog's publication leaves. The open completes it.
        db.persist().unwrap();
        drop(db);
        fs::rename(dir.join("collections").join("notes"), dropping_dir(&dir, "notes")).unwrap();
        fs::create_dir_all(dropping_dir(&dir, "orphan")).unwrap();
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert!(
            db.execute("SHOW CATALOG notes").is_err(),
            "the interrupted drop was not completed"
        );
        assert!(!dropping_dir(&dir, "notes").exists(), "the directory aside was not removed");
        assert!(!dropping_dir(&dir, "orphan").exists(), "a directory aside with no entry was kept");
        assert_eq!(db.collection_count(), 0);
        drop(db);
        let db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(db.collection_count(), 0, "and the completed drop was published");
        let _ = fs::remove_dir_all(&dir);
    }

    /// DROP INDEX withdraws the declaration: the planner refuses the path,
    /// the clock and the statistics go, a reopen agrees, and the index can be
    /// declared again. The sealed regions are left for compaction, as CREATE
    /// INDEX leaves them for the next seal. The policy refusal covers only a
    /// policy that names the index; one covering the collection covers one
    /// fewer.
    #[test]
    fn dropping_an_index_withdraws_the_declaration_and_what_was_recorded_against_it() {
        let dir = tmp("drop-index");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX notes_emb ON notes USING vector (embedding) \
             WITH (dims = 2, metric = 'cosine')",
        )
        .unwrap();
        for i in 0..4 {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i}"))),
                    ("body".into(), Value::Str("graph search".into())),
                    (
                        "embedding".into(),
                        Value::Array(vec![Value::Float(i as f64), Value::Float(1.0)]),
                    ),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        let text = "SELECT id FROM notes WHERE text_match(body, 'graph') LIMIT 10";
        assert_eq!(db.query(text).unwrap().rows.len(), 4);
        assert!(guard(&db.stats).contains_key(&cache_key("notes", "body")));
        db.execute(
            "CREATE LIFECYCLE POLICY cool ON notes FOR (notes_body) \
             MOVE TO cached AFTER 1 hour OF INACTIVITY",
        )
        .unwrap();
        db.execute(
            "CREATE LIFECYCLE POLICY all_of_it ON notes MOVE TO cached AFTER 1 hour OF INACTIVITY",
        )
        .unwrap();
        let e = db.execute("DROP INDEX notes_body ON notes").unwrap_err().to_string();
        assert!(e.contains("policy `cool`"), "{e}");
        db.execute("DROP LIFECYCLE POLICY cool").unwrap();
        let e = db.execute("DROP INDEX nope ON notes").unwrap_err().to_string();
        assert!(e.contains("no index `nope`"), "{e}");

        match db.execute("DROP INDEX notes_body ON notes").unwrap() {
            Outcome::Ack(m) => assert_eq!(m, "index `notes_body` dropped from `notes`"),
            other => panic!("{other:?}"),
        }
        let e = db.query(text).unwrap_err().to_string();
        assert!(e.contains("no full-text index on"), "the planner still used it: {e}");
        assert!(
            !guard(&db.stats).contains_key(&cache_key("notes", "body")),
            "the statistics were kept"
        );
        assert!(!db.catalog.activity.contains_key(&("notes".into(), "notes_body".into())));
        assert!(db.catalog.get("notes").unwrap().index_by_name("notes_body").is_none());
        let emb = "SELECT id FROM notes ORDER BY embedding <=> [1.0, 1.0] LIMIT 2";
        assert_eq!(db.query(emb).unwrap().rows.len(), 2, "the other index is untouched");
        // A write after the drop lands in a memtable rebuilt without the index.
        db.insert(
            "notes",
            Value::obj(vec![
                ("id".into(), Value::Str("n9".into())),
                ("body".into(), Value::Str("graph".into())),
                ("embedding".into(), Value::Array(vec![Value::Float(9.0), Value::Float(1.0)])),
            ]),
        )
        .unwrap();

        drop(db);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert!(
            db.catalog.get("notes").unwrap().index_by_name("notes_body").is_none(),
            "reopened with it"
        );
        assert!(db.query(text).is_err());
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        // The sealed regions are still there for the declaration to find, and
        // the write after the drop was sealed by the re-declaration itself.
        assert_eq!(db.query(text).unwrap().rows.len(), 5, "declared again");
        db.execute("DROP LIFECYCLE POLICY all_of_it").unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_query_in_a_fresh_epoch_ranks_like_exact_scoring() {
        // The parity the `CachedStats` doc comment now claims for Prefix as
        // well as Term and Phrase, as an assertion. The two arms are different
        // machinery — `exact_scoring` gathers per query, the default reads an
        // epoch-scoped cache — and they agree only because the expansion is
        // resolved before either of them runs and both are then asked for the
        // same term list. Before that, `exact_scoring` bought a prefix query
        // nothing at all: no expanded term ever reached either gather.
        //
        // Fresh epoch, so staleness is off the table and what is left is the
        // choice of `df`. The order is asserted, not the score bits: a prefix
        // compiles to a many-child disjunction and `DisjunctionScorer::score`
        // sums f32 in cursor order.
        let dir = tmp("prefix-exact-parity");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['n0300'])")
            .unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..600usize {
            let term = if i % 7 == 0 { "alphabet" } else { "alpha" };
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str(format!("{term}{}", " pad".repeat(i % 5)))),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();

        let ranked = |db: &mut Db, with: &str| {
            db.query(&format!(
                "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'alph*'),                  method => 'linear', k => 100000) LIMIT 10{with}"
            ))
            .unwrap()
            .rows
            .iter()
            .map(|r| r.key.clone())
            .collect::<Vec<_>>()
        };
        let default = ranked(&mut db, "");
        let exact = ranked(&mut db, " WITH (exact_scoring)");
        assert_eq!(default.len(), 10);
        assert_eq!(default, exact, "default vs `WITH (exact_scoring)` in a fresh epoch");

        // And the cached entry is the exact gather's own numbers, for the
        // expansion's terms rather than for nothing.
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            c.doc_freq,
            BTreeMap::from([("alpha".to_string(), 514u64), ("alphabet".to_string(), 86)]),
            "600 documents, one in seven holding the rarer term"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_term_living_in_one_shard_is_weighted_by_the_whole_collection() {
        // The shape the old code could not get right even without a cap in
        // play: `alphabet` lives almost entirely in the second shard, so the
        // first shard's own dictionary says it is a two-document rarity while
        // the collection says it is in 102 of 600. A segment-local df hands
        // those two documents the weight of the rarest term there is.
        //
        // Three things at once, and each is a separate way to get this wrong:
        // the union must CONTAIN a term no single shard could enumerate on its
        // own; the df must be the collection's, so a document scores the same
        // whichever shard holds it; and the shard that holds no `alpha` at all
        // must contribute nothing to `df(alpha)` while its documents still
        // count in `num_docs` — an IDF divided by a corpus that shrank to the
        // shard is wrong in the other direction.
        let run = |splits: &str, tag: &str| {
            let dir = tmp(tag);
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            db.execute(&format!("CREATE COLLECTION notes (id TEXT PRIMARY KEY){splits}")).unwrap();
            db.execute(
                "CREATE INDEX notes_body ON notes USING fulltext (body)                  WITH (analyzer = 'english')",
            )
            .unwrap();
            for i in 0..600usize {
                let term = if (2..500).contains(&i) { "alpha" } else { "alphabet" };
                db.insert(
                    "notes",
                    Value::obj(vec![
                        ("id".into(), Value::Str(format!("n{i:04}"))),
                        ("body".into(), Value::Str(format!("{term}{}", " pad".repeat(i % 5)))),
                    ]),
                )
                .unwrap();
            }
            db.execute("FLUSH notes").unwrap();
            let ranked = db
                .query(
                    "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'alph*'),                      method => 'linear', k => 100000) LIMIT 10",
                )
                .unwrap()
                .rows
                .iter()
                .map(|r| r.key.clone())
                .collect::<Vec<_>>();
            let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
            let got = (c.num_docs, c.doc_freq.clone(), ranked);
            let _ = fs::remove_dir_all(&dir);
            got
        };

        let one = run("", "prefix-one-shard-1");
        let two = run(" WITH (splits = ['n0500'])", "prefix-one-shard-2");

        assert_eq!(
            one.1,
            BTreeMap::from([("alpha".to_string(), 498u64), ("alphabet".to_string(), 102)]),
            "both expanded terms, at their collection-wide frequencies"
        );
        assert_eq!(one.0, 600, "including the documents of the shard that holds no `alpha`");
        assert_eq!(one.1, two.1, "df, 1 shard vs 2 shards");
        assert_eq!(one.0, two.0, "num_docs, 1 shard vs 2 shards");
        assert_eq!(one.2.len(), 10);
        assert_eq!(one.2, two.2, "the ranking, 1 shard vs 2 shards");
    }

    /// A collection holding exactly `terms` distinct `a#####` terms, one per
    /// document, split across three units so the coordinator's union is a real
    /// union rather than one unit's dictionary read twice.
    fn cap_fixture(dir: &std::path::Path, terms: usize) -> Db {
        cap_fixture_in(dir, terms, 3)
    }

    /// The same collection laid out in exactly `units` sealed segments.
    ///
    /// The unit count is a parameter because the per-unit enumeration limit is
    /// only observable when ONE unit holds more than the cap: a vocabulary
    /// spread over three units is asked for `cap + 1` terms per unit and each
    /// of them answers with everything it has, so asking for `cap` instead
    /// would return the same union and the boundary would go unpinned. See
    /// `an_expansion_of_exactly_the_cap_dropped_nothing_and_must_not_say_it_did`.
    ///
    /// Every document also holds `zed`, so a negated shape — `zed -a*`, where
    /// the expansion is the EXCLUSION set — has a positive clause that admits
    /// everything and therefore measures nothing but the exclusion. It is in
    /// the fixture rather than in the one test that needs it because a term
    /// every document holds shifts no relative frequency and no ranking.
    fn cap_fixture_in(dir: &std::path::Path, terms: usize, units: usize) -> Db {
        let mut db = Db::open(dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..terms {
            if units > 1 && i > 0 && i % (terms / units).max(1) == 0 {
                db.execute("FLUSH notes").unwrap();
            }
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:05}"))),
                    ("body".into(), Value::Str(format!("zed a{i:05}"))),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        db
    }

    #[test]
    fn an_expansion_of_exactly_the_cap_dropped_nothing_and_must_not_say_it_did() {
        // The off-by-one, in the shape that reproduced it. The test used to be
        // `terms.len() >= PREFIX_EXPANSION_LIMIT` applied to a list that had
        // just been `take(cap)`-ed, so it could never mean anything else: a
        // dictionary the cap had cut and a dictionary holding exactly `cap`
        // matching terms produce lists of identical length, and the shorter
        // one is unreachable. Every prefix that reached the cap was reported
        // truncated, including the ones that lost nothing — a design pass
        // measured a 512-term vocabulary reporting 3 of 9 units "truncating"
        // with zero terms dropped.
        //
        // The fix is not a comparison operator, it is asking for one more term
        // than the cap and seeing whether it comes back. That extra term is the
        // only evidence that exists, at the coordinator or in `scorer::build`;
        // the length of the answer carries none.
        //
        // A warning nobody can act on is worse than silence, because the next
        // wide prefix — the one that really did drop 3000 terms — says exactly
        // the same thing.
        let dir = tmp("prefix-cap-exact");
        let mut db = cap_fixture(&dir, PREFIX_EXPANSION_LIMIT);
        let r = db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 100000").unwrap();
        assert_eq!(r.rows.len(), PREFIX_EXPANSION_LIMIT, "every document matches and none is cut");
        assert!(
            r.truncated_prefixes.is_empty(),
            "exactly the cap, nothing dropped: {:?}",
            r.truncated_prefixes
        );
        let _ = fs::remove_dir_all(&dir);

        // One more term, and one document is now unreachable by this query.
        // Same assertion machinery, opposite verdict: if the boundary moved
        // the wrong way, this leg is what catches it.
        let dir = tmp("prefix-cap-over");
        let mut db = cap_fixture(&dir, PREFIX_EXPANSION_LIMIT + 1);
        let r = db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 100000").unwrap();
        assert_eq!(r.rows.len(), PREFIX_EXPANSION_LIMIT, "the last document is behind the cut");
        assert_eq!(r.truncated_prefixes.len(), 1, "{:?}", r.truncated_prefixes);
        let msg = &r.truncated_prefixes[0];
        assert!(msg.contains("body") && msg.contains("'a*'"), "{msg}");
        assert!(msg.contains("512"), "the number of terms kept, which is the dial: {msg}");
        let _ = fs::remove_dir_all(&dir);

        // And the same `cap + 1` terms in ONE unit, which is the only layout
        // that can see the `cap + 1` the coordinator asks each unit for. The
        // two legs above hold their vocabulary in three units of ~171 terms,
        // so a unit asked for `cap` and a unit asked for `cap + 1` hand back
        // the identical dictionary and the union is the same either way: they
        // pin the VERDICT, and this leg pins the ENUMERATION it is read off.
        //
        // Drop the `+ 1` in `Db::run_select` and this query sees a union of
        // exactly 512 terms, cannot tell "the collection holds 512" from "the
        // collection holds more", reports no truncation, and drops `n00512`
        // in silence — the original defect, one level down from where the
        // legs above look. It was an integration test's job before this leg
        // existed, so the boundary went unpinned where it is implemented.
        let dir = tmp("prefix-cap-over-one-unit");
        let mut db = cap_fixture_in(&dir, PREFIX_EXPANSION_LIMIT + 1, 1);
        let r = db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 100000").unwrap();
        assert_eq!(r.rows.len(), PREFIX_EXPANSION_LIMIT, "one unit, one cut");
        assert_eq!(
            r.truncated_prefixes.len(),
            1,
            "a single unit holding `cap + 1` terms is truncated, and the only evidence of \
             that is the term the coordinator asks for beyond the cap: {:?}",
            r.truncated_prefixes
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_truncated_prefix_says_so_on_a_plain_query_of_either_shape() {
        // Where the signal has to arrive. It used to reach `EXPLAIN ANALYZE`
        // and nowhere else — `QueryResult::explain` is `None` unless `analyze`
        // — so an ordinary ranked query truncated in silence, and the filter
        // path dropped the flag on the floor before it could reach even that.
        //
        // `WHERE text_match(body, 'a*')` is the commonest shape a wide prefix
        // takes and the one that returned a fraction of the matching documents
        // with no signal on any path at all.
        let dir = tmp("prefix-cut-reported");
        let mut db = cap_fixture(&dir, PREFIX_EXPANSION_LIMIT + 200);

        let filtered =
            db.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 5").unwrap();
        assert!(filtered.explain.is_none(), "no ANALYZE: the point is that it still says so");
        assert_eq!(filtered.truncated_prefixes.len(), 1, "{:?}", filtered.truncated_prefixes);

        let ranked = db
            .query(
                "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'a*'), \
                 method => 'linear', k => 100000) LIMIT 5",
            )
            .unwrap();
        assert!(ranked.explain.is_none());
        assert_eq!(ranked.truncated_prefixes, filtered.truncated_prefixes, "same query, same cut");

        // A prefix that fits says nothing, on the same database: the report is
        // a property of the query, not a banner the collection wears.
        let narrow =
            db.query("SELECT id FROM notes WHERE text_match(body, 'a0000*') LIMIT 100").unwrap();
        assert_eq!(narrow.rows.len(), 10, "`a00000`..`a00009`");
        assert!(narrow.truncated_prefixes.is_empty(), "{:?}", narrow.truncated_prefixes);

        // And the per-unit line, which is the only thing that can report the
        // no-coordinator fallback arm of `scorer::build`. The filter path
        // pushed no text line at all before this, so neither the flag nor the
        // survivor count reached the plan.
        let text = match db
            .execute("EXPLAIN ANALYZE SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 5")
            .unwrap()
        {
            Outcome::Explain(t) => t,
            _ => panic!("expected a plan"),
        };
        assert!(text.contains("[filter]"), "the filter path reports a text line:\n{text}");
        assert!(text.contains("PREFIX EXPANSION TRUNCATED"), "{text}");
        // And it describes the machinery it actually ran. This line used to go
        // through the ranked path's formatter, so it announced "block-max WAND"
        // for a call to `scorer::evaluate_to_bitmap` — a bare `advance` loop
        // with no scoring, no pivot and no threshold — and labelled its
        // survivor count "candidates", with `terms=[]` beside it. A plan is the
        // document someone debugging a slow query reasons from; one that names
        // an algorithm the query never entered is worse than no plan.
        assert!(
            text.contains("[filter]]: bitmap evaluation, survivors="),
            "the filter line describes the filter path:\n{text}"
        );
        assert!(!text.contains("[filter]]: block-max WAND"), "{text}");
        // The coordinator's line too, once, rather than once per unit.
        assert_eq!(
            text.matches("documents are missing from this answer").count(),
            1,
            "the query-level statement belongs in the plan once:\n{text}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_query_on_an_unindexed_path_leaves_nothing_behind_in_the_statistics_map() {
        // `Db::stats` is keyed `collection/path` and nothing ever removes an
        // entry from the outer map — `STATS_TERM_CAP` bounds the terms INSIDE
        // one entry, not how many entries there are. `analyzer_for` falls back
        // to "standard" for any path, so a `text_match` naming a path with no
        // index used to parse, reach `gather_stats`, and have an entry created
        // for it a moment before `eval_expr` failed the statement. One
        // statement can name as many such paths as it has bytes for, and each
        // one is retained for the life of the process.
        let dir = tmp("unindexed-path-stats");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.insert(
            "notes",
            Value::obj(vec![
                ("id".into(), Value::Str("n1".into())),
                ("body".into(), Value::Str("zed".into())),
            ]),
        )
        .unwrap();

        // One statement, several fresh paths, one error to the caller.
        let clauses: Vec<String> =
            (0..8).map(|i| format!("text_match(ghost{i:04}{}, 'x')", "z".repeat(64))).collect();
        let sql = format!("SELECT id FROM notes WHERE {} LIMIT 5", clauses.join(" OR "));
        let e = db.query(&sql).unwrap_err().to_string();
        // The message stays exactly where it was: this declines to NAME the
        // path, it does not move the refusal to the coordinator.
        assert!(e.contains("no full-text index on"), "{e}");
        assert!(
            guard(&db.stats).is_empty(),
            "a failed query retained {} entries",
            guard(&db.stats).len()
        );

        // A prefix on an unindexed path is out of the budget too, since the
        // budget counts expansions that get paid for.
        let many: Vec<String> =
            (0..12).map(|i| format!("text_match(ghost{i:04}, 'a* b*')")).collect();
        let sql = format!("SELECT id FROM notes WHERE {} LIMIT 5", many.join(" OR "));
        let e = db.query(&sql).unwrap_err().to_string();
        assert!(e.contains("no full-text index on"), "not the prefix budget: {e}");
        assert!(guard(&db.stats).is_empty(), "{} entries", guard(&db.stats).len());

        // The indexed path still works and still caches, which is the control.
        let r = db.query("SELECT id FROM notes WHERE text_match(body, 'zed') LIMIT 5").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(guard(&db.stats).len(), 1, "the one path that has an index");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_negated_expansion_is_resolved_but_not_gathered() {
        // A negated prefix's expansion is the EXCLUSION set: it decides which
        // documents are dropped, so it has to be resolved at the coordinator
        // like any other — but nothing SCORES it, so its document frequencies
        // are never read. Gathering them anyway cost a masked posting walk per
        // term per unit and filled 512 of the path's 4096 cache slots with
        // terms that cannot be looked up, so one admitted negation-only
        // statement displaced the whole cache and the next ranked query paid a
        // fresh gather for its own terms.
        let dir = tmp("negated-expansion-gather");
        let mut db = cap_fixture(&dir, 600);
        let key = cache_key("notes", "body");

        let r = db
            .query("SELECT id FROM notes WHERE text_match(body, 'zed -a*') LIMIT 100000")
            .unwrap();
        // 600 terms against a cap of 512: the exclusion set is short, so the 88
        // documents it failed to exclude are still here. Unchanged by this —
        // the expansion is still resolved globally, only the gather is skipped.
        assert_eq!(r.rows.len(), 88, "the coordinator's global cut, not a per-unit one");
        assert_eq!(r.truncated_prefixes.len(), 1, "{:?}", r.truncated_prefixes);
        let c = guard(&db.stats).get(&key).unwrap().clone();
        assert_eq!(c.doc_freq.get("zed"), Some(&600), "the positive clause is still gathered");
        let a: Vec<&String> = c.doc_freq.keys().filter(|t| t.starts_with('a')).collect();
        assert!(a.is_empty(), "frequencies nothing can read: {} of them", a.len());

        // A PURE negation keeps its path in `want` even with nothing to gather,
        // or `gather_stats` produces no entry for it, the resolved expansion
        // has nowhere to attach, and every unit re-expands the prefix against
        // its own dictionary — which would answer 0 rows here, since each
        // unit's 200 terms fit under the cap.
        let dir2 = tmp("negated-expansion-pure");
        let mut db2 = cap_fixture(&dir2, 600);
        let r =
            db2.query("SELECT id FROM notes WHERE text_match(body, '-a*') LIMIT 100000").unwrap();
        assert_eq!(r.rows.len(), 88, "the global cut, not each unit's own");
        assert!(guard(&db2.stats).get(&cache_key("notes", "body")).unwrap().doc_freq.is_empty());

        // And the positive spelling still gathers, which is the control: this
        // turns on the leaf's sign, not on prefixes as a class.
        let dir3 = tmp("negated-expansion-control");
        let mut db3 = cap_fixture(&dir3, 600);
        db3.query("SELECT id FROM notes WHERE text_match(body, 'a*') LIMIT 5").unwrap();
        let c = guard(&db3.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            c.doc_freq.keys().filter(|t| t.starts_with('a')).count(),
            PREFIX_EXPANSION_LIMIT,
            "a matching expansion IS scored"
        );

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dir2);
        let _ = fs::remove_dir_all(&dir3);
    }

    #[test]
    fn a_cut_prefix_under_a_sql_not_reports_the_consequence_that_shape_has() {
        // The half the mini-language's `-a*` arm was built to prevent, reached
        // by the OTHER spelling. `Expr::Not` is evaluated as `eval_defined(..)
        // andnot matched`, so a SHORT `matched` yields MORE rows — `NOT
        // text_match(body, 'a*')` over documents that ALL hold an `a*` term
        // must return nothing, and a cut expansion leaves the surplus behind.
        // The report used to read the consequence off the sign inside the
        // query string, where there is none, so it said "documents are
        // missing" about a statement holding extra ones. A caller acting on
        // that widens the prefix and keeps more.
        let dir = tmp("prefix-cut-under-sql-not");
        let mut db = cap_fixture(&dir, PREFIX_EXPANSION_LIMIT + 200);

        let neg =
            db.query("SELECT id FROM notes WHERE NOT text_match(body, 'a*') LIMIT 2000").unwrap();
        // Every document holds its own `a#####`, so the honest answer is zero
        // rows. The 200 that are here are the cut's surplus, and they are the
        // reason the message has to name this direction.
        assert_eq!(neg.rows.len(), 200, "the cut exclusion set left rows behind");
        assert_eq!(neg.truncated_prefixes.len(), 1, "{:?}", neg.truncated_prefixes);
        let m = &neg.truncated_prefixes[0];
        assert!(
            m.contains("should have excluded are still in this answer"),
            "the consequence this shape had: {m}"
        );
        assert!(m.contains("text_match(body, 'a*')"), "the caller's own spelling: {m}");
        assert!(!m.contains("'-a*'"), "a spelling this statement never wrote: {m}");

        // Same rows through the mini-language, and — now — the same message.
        // Which of the two spellings a caller reaches for cannot decide what
        // they are told happened.
        let mini =
            db.query("SELECT id FROM notes WHERE text_match(body, 'zed -a*') LIMIT 2000").unwrap();
        assert_eq!(mini.rows.len(), neg.rows.len());
        assert!(
            mini.truncated_prefixes[0].contains("should have excluded are still in this answer"),
            "{:?}",
            mini.truncated_prefixes
        );

        // A double negation matches again, so the cut is short rather than
        // surplus. This is why the sign is FLIPPED and not OR-ed: OR-ing the
        // SQL `NOT` into an already-negated leaf leaves it negated, and the
        // message stays inverted in the rarer shape.
        let dbl = db
            .query("SELECT id FROM notes WHERE NOT text_match(body, 'zed -a*') LIMIT 2000")
            .unwrap();
        assert_eq!(dbl.rows.len(), PREFIX_EXPANSION_LIMIT, "512 of 712: rows are MISSING");
        assert!(
            dbl.truncated_prefixes[0].contains("documents are missing from this answer"),
            "{:?}",
            dbl.truncated_prefixes
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_delete_whose_predicate_was_cut_is_refused_rather_than_deleting_what_it_did_not_name() {
        // The one statement shape where a short answer does WRITE work, and
        // the only one where the write cannot be taken back.
        //
        // The arm used to take `.rows` and throw the rest of the result away:
        // the caller saw `512 document(s) deleted` and 200 matching documents
        // still sitting there with nothing to say why. Reporting it instead of
        // dropping it was the first fix and it was not enough, because the
        // report was written for the positive shape and the negated one does
        // the opposite thing. MEASURED on 1000 documents each holding `zed
        // a#####`, every one of which `zed -a*` EXCLUDES, so the correct
        // answer is zero deletions: it deleted 488 and left 512, then said
        // that documents it matches remain and re-running would delete more.
        // Re-running deleted nothing. The message was false in the direction
        // that matters, and the sentence the operator needed — that 488 rows
        // their predicate excluded are gone — was not in it.
        //
        // So the statement is REFUSED, before any `delete_key`, in both shapes
        // and for the reasons `Db::execute` states: a cut SELECT is
        // recoverable and a cut DELETE is not, and which of the two kinds of
        // damage a given statement would do cannot be read off the leaf,
        // because SQL's own `NOT` inverts it and one statement may spell both.
        let dir = tmp("delete-cut");
        let mut db = cap_fixture(&dir, PREFIX_EXPANSION_LIMIT + 200);
        let all = |db: &mut Db| {
            db.query("SELECT id FROM notes WHERE text_match(body, 'zed') LIMIT 100000")
                .unwrap()
                .rows
                .len()
        };
        assert_eq!(all(&mut db), PREFIX_EXPANSION_LIMIT + 200, "the fixture, before anything");

        // The POSITIVE shape: the predicate names 512 of the 712 documents it
        // describes, so executing it would delete a subset and leave the rest
        // with no way to see which.
        let e = db.execute("DELETE FROM notes WHERE text_match(body, 'a*')").unwrap_err();
        let e = e.to_string();
        assert!(e.contains("refused") && e.contains("NOTHING was deleted"), "{e}");
        assert!(e.contains("'a*'"), "and which leaf was cut: {e}");
        assert_eq!(all(&mut db), PREFIX_EXPANSION_LIMIT + 200, "refused means nothing was written");

        // The NEGATED shape, which the old test did not cover and the old
        // message described backwards. Every document holds `zed` and its own
        // `a#####`, so `zed -a*` excludes all 712 and the honest answer is
        // zero rows; the cut exclusion set covers 512 of them, which used to
        // make the other 200 — documents the predicate EXCLUDES — deletable.
        let e = db.execute("DELETE FROM notes WHERE text_match(body, 'zed -a*')").unwrap_err();
        let e = e.to_string();
        assert!(e.contains("refused") && e.contains("NOTHING was deleted"), "{e}");
        // The leaf as WRITTEN, sign and all: `-a*` and `a*` are cut for
        // different reasons and a reader has to find the clause.
        assert!(e.contains("'-a*'"), "the negated leaf, as it was written: {e}");
        // And the sentence that is true of this shape and was missing: a cut
        // exclusion deletes rows the predicate excluded.
        assert!(
            e.contains("delete documents the predicate EXCLUDES"),
            "the consequence of a cut exclusion set: {e}"
        );
        assert!(
            !e.contains("re-running"),
            "the old message's claim, which was false here — re-running deleted 0: {e}"
        );
        assert_eq!(all(&mut db), PREFIX_EXPANSION_LIMIT + 200, "and 200 excluded rows survive");

        // The refusal is not a ban on deleting by prefix. A prefix whose
        // expansion fits is not cut, so it deletes and reports a plain count —
        // which is also the loop the refusal points the operator at: `a0000*`,
        // `a0001*`, and so on, each one deleting exactly what it names.
        let msg = match db.execute("DELETE FROM notes WHERE text_match(body, 'a0000*')").unwrap() {
            Outcome::Ack(m) => m,
            other => panic!("expected an ack, got {other:?}"),
        };
        assert_eq!(msg, "10 document(s) deleted", "{msg}");
        assert_eq!(all(&mut db), PREFIX_EXPANSION_LIMIT + 190, "`a00000`..`a00009`, and no more");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn two_text_paths_in_one_collection_do_not_share_one_statistics_entry() {
        // `cache_key` is `collection/path`, and the path half is load-bearing.
        // Collapse it and two fields share one [`STATS_TERM_CAP`]-entry map
        // and one `avgdl`: they evict each other, every length norm is divided
        // by the average of both fields, and a term that appears in both is
        // answered for whichever field asked first. Nothing else in the suite
        // can notice — every fulltext index in the tree is on `body`, so no
        // collection anywhere else has a second text path.
        let dir = tmp("stats-two-paths");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX notes_title ON notes USING fulltext (title) WITH (analyzer = 'english')",
        )
        .unwrap();
        // `alpha` is in every body and in one title in ten, and the two fields
        // are of very different lengths.
        for i in 0..100usize {
            let title = if i % 10 == 0 { "alpha" } else { "zeta" };
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha beta gamma delta".into())),
                    ("title".into(), Value::Str(title.into())),
                ]),
            )
            .unwrap();
        }

        let one = |path: &str| BTreeMap::from([(path.to_string(), vec!["alpha".to_string()])]);
        let ts = db.clock.peek();
        // Body first, so a shared entry would already hold `alpha` when the
        // title asks — and would answer the title's query with the body's
        // frequency without gathering anything.
        let b = db.gather_stats("notes", &one("body"), ts, false).unwrap();
        let t = db.gather_stats("notes", &one("title"), ts, false).unwrap();
        let e = db.gather_stats("notes", &one("title"), ts, true).unwrap();
        assert_eq!(b["body"].doc_freq["alpha"], 100);
        assert_eq!(t["title"].doc_freq["alpha"], 10, "the title's own frequency, not the body's");
        assert_eq!(t["title"].doc_freq, e["title"].doc_freq);
        assert_eq!(t["title"].avg_doc_len.to_bits(), e["title"].avg_doc_len.to_bits());
        assert_eq!(b["body"].avg_doc_len, 4.0, "four words of body");
        assert_eq!(t["title"].avg_doc_len, 1.0, "one of title — and not the average of both");

        assert_eq!(
            guard(&db.stats).get(&cache_key("notes", "body")).unwrap().doc_freq["alpha"],
            100
        );
        assert_eq!(
            guard(&db.stats).get(&cache_key("notes", "title")).unwrap().doc_freq["alpha"],
            10
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_fill_later_in_the_epoch_gathers_at_the_query_timestamp_and_not_a_stored_one() {
        // The detector for the rule written on `fill_term_stats`. The test
        // below it demonstrates WHY the rule exists, at the shard level; this
        // one fails if the rule is broken, which is the other half and the one
        // a future author needs. Storing the timestamp the epoch was anchored
        // at and re-gathering at it looks free and is not: below a shard's
        // retain floor `Shard::term_stats` is best-effort, and a seal or a
        // compaction walks that floor up past any timestamp held from earlier,
        // so what survives to be counted becomes a per-shard compaction
        // decision — the exact dependence this cache was rebuilt to remove.
        //
        // So: anchor the epoch, then write, delete and compact under it, then
        // fill a new term in the SAME epoch and demand the live answer. Both
        // halves of the hazard are in that sequence. A stored timestamp cannot
        // see the writes above it — 100 documents here — and below the retain
        // floor the compaction walked past it, what it still sees of the 200
        // deleted ones is that shard's own collection decision.
        let dir = tmp("stats-fill-ts");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..600usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha".into())),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        let want = |terms: Vec<&str>| {
            BTreeMap::from([(
                "body".to_string(),
                terms.into_iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            )])
        };
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want(vec!["alpha"]), ts, false).unwrap();
        assert_eq!((g["body"].num_docs, g["body"].doc_freq["alpha"]), (600, 600));

        for i in (0..600usize).step_by(3) {
            db.delete_key("notes", &format!("n{i:04}")).unwrap();
        }
        for i in 0..100usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("m{i:04}"))),
                    ("body".into(), Value::Str("gamma".into())),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        db.execute("COMPACT notes").unwrap();
        let at = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
        assert!(
            db.writes - at < STATS_REFRESH_WRITES,
            "the second gather has to be a FILL inside the epoch the first one anchored, not a \
             fresh epoch that would take a new timestamp anyway"
        );

        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want(vec!["alpha", "beta"]), ts, false).unwrap();
        let e = db.gather_stats("notes", &want(vec!["alpha", "beta"]), ts, true).unwrap();
        let (g, e) = (&g["body"], &e["body"]);
        assert_eq!(g.num_docs, 500, "600 written, one in three deleted, 100 added");
        assert_eq!(g.doc_freq["alpha"], 400, "and the 100 added carry `gamma`, not `alpha`");
        assert_eq!(g.num_docs, e.num_docs, "the fill measured the corpus this query sees");
        assert_eq!(g.avg_doc_len.to_bits(), e.avg_doc_len.to_bits(), "by bits: it is an average");
        for (t, df) in &g.doc_freq {
            assert_eq!(*df, e.doc_freq.get(t).copied().unwrap_or(0), "df({t})");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    fn points_are_sane(points: &[u64]) -> bool {
        points.len() >= 4 && points.windows(2).all(|w| w[1] - w[0] >= STATS_REFRESH_WRITES)
    }

    #[test]
    fn a_statistic_gathered_at_a_pinned_timestamp_does_not_stay_true_at_that_timestamp() {
        // The demonstration behind the rule written on `fill_term_stats`, and
        // the reason it is a rule rather than a preference. It shows the
        // hazard; it does not detect it, because nothing here goes through
        // `Db`'s statistics path at all — a stored `as_of` can be added to
        // `fill_term_stats` and every assertion below still passes. The
        // detector is
        // `a_fill_later_in_the_epoch_gathers_at_the_query_timestamp_and_not_a_stored_one`
        // above, and the two are worth having separately: one says what goes
        // wrong, the other says that it has not. Caching the
        // timestamp a refresh used and re-gathering at it later looks free —
        // it would spare the fill nothing but a clock read — and it silently
        // undoes the whole change: below a shard's retain floor
        // `Shard::term_stats` is best-effort, `Shard::retain_from` returns
        // `now` when no `gc_horizon` is pinned, and a seal or a compaction
        // walks the floor up past any timestamp held from earlier. What
        // survives to be counted is then a per-shard compaction decision,
        // which is exactly the dependence the live sums removed.
        //
        // So: pin a timestamp, read the triple at it, compact, read it at the
        // SAME timestamp again, and watch it change.
        let dir = tmp("stats-stale-ts");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..600usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha".into())),
                ]),
            )
            .unwrap();
        }
        db.execute("FLUSH notes").unwrap();

        // The timestamp a refresh point might have stored, taken while all 600
        // documents are live.
        let terms = vec!["alpha".to_string()];
        let pinned = db.clock.peek();
        let before = db.shards("notes").unwrap()[0].term_stats("body", &terms, pinned).unwrap();
        assert_eq!(before, (600, 600, BTreeMap::from([("alpha".to_string(), 600)])));

        // Writes the pinned timestamp is below, and then the collection that
        // drops what they superseded.
        for i in (0..600usize).step_by(3) {
            db.delete_key("notes", &format!("n{i:04}")).unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        db.execute("COMPACT notes").unwrap();

        let after = db.shards("notes").unwrap()[0].term_stats("body", &terms, pinned).unwrap();
        assert_ne!(
            before, after,
            "the same timestamp answered {before:?} twice running — a stored `as_of` is not a \
             stable thing to gather at"
        );
        assert!(after.0 < before.0, "and what it lost is rows: {before:?} then {after:?}");
        // The live read is the honest one, and it is the reason the fill takes
        // the query's timestamp: 400 documents survive, and that is a fact
        // about the corpus rather than about when this shard compacted.
        let live =
            db.shards("notes").unwrap()[0].term_stats("body", &terms, db.clock.peek()).unwrap();
        assert_eq!(live.0, 400, "600 written, one in three deleted");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_per_term_statistics_stay_bounded_at_the_entry_cap() {
        // Two properties, and the second one is the one that is easy to lose.
        //
        // `doc_freq` is filled by whatever queries ask for, so without the cap
        // a workload with a long tail of distinct terms grows it back into the
        // corpus-wide vocabulary this path stopped holding. Evicting oldest
        // first is what the cap costs: a term dropped here is re-gathered by
        // the next query that wants it, at one masked walk. That is a bound on
        // what the cache RETAINS.
        //
        // It must never become a bound on what a query is ANSWERED, and the
        // two are one keystroke apart: build the answer by re-reading the
        // cache after the eviction loop has run and a term the query itself
        // asked for can be evicted by its own query and read back as `df = 0`
        // — the highest weight BM25 gives — for a term the whole corpus holds.
        // `alpha` is that term here. Every document has it, `required_terms`
        // sorts, so `alpha` sorts ahead of `t000000`, is filled first, sits at
        // the front of `fill_order` and is the first entry this very query
        // evicts. `fill_term_stats` returns the triple it gathered instead of
        // leaving `gather_stats` to re-read residency, so the two are
        // independent: the assertions below hold `alpha` correct in the answer
        // and absent from the cache at the same time.
        let dir = tmp("stats-term-cap");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for i in 0..40usize {
            db.insert(
                "notes",
                Value::obj(vec![
                    ("id".into(), Value::Str(format!("n{i:04}"))),
                    ("body".into(), Value::Str("alpha".into())),
                ]),
            )
            .unwrap();
        }

        // Well past the cap, and inside one epoch, so nothing here is a
        // refresh discarding the map rather than the cap bounding it. Nothing
        // bounds how many terms one query may ask for: `TextQuery::parse`
        // builds `Any`/`All` from flat loops with no width limit and
        // `required_terms` unions every `text_match` in the statement, so
        // `STATS_TERM_CAP + 500` sorted terms in a single request is a shape
        // SQL can really produce.
        let n = STATS_TERM_CAP + 500;
        let mut terms: Vec<String> = (0..n).map(|i| format!("t{i:06}")).collect();
        terms.insert(0, "alpha".to_string());
        let want = BTreeMap::from([("body".to_string(), terms.clone())]);
        let ts = db.clock.peek();
        let g = db.gather_stats("notes", &want, ts, false).unwrap();
        let e = db.gather_stats("notes", &want, ts, true).unwrap();
        assert!(db.writes < STATS_REFRESH_WRITES, "no refresh point may pass during this");
        let (g, e) = (&g["body"], &e["body"]);
        assert_eq!(g.doc_freq.len(), terms.len(), "the answer covers every term asked for");

        // The answer, against the exact gather of the same terms at the same
        // instant. The exact arm omits a term no unit holds where the cached
        // arm stores an explicit zero (see
        // `a_freshly_refreshed_cache_answers_exactly_what_the_exact_gather_answers`),
        // so absence on the exact side reads as the zero it means.
        assert_eq!(g.doc_freq["alpha"], 40, "a term every document holds, answered over the cap");
        assert_eq!(g.num_docs, e.num_docs);
        assert_eq!(g.avg_doc_len.to_bits(), e.avg_doc_len.to_bits(), "by bits: it is an average");
        for (t, df) in &g.doc_freq {
            assert_eq!(
                *df,
                e.doc_freq.get(t).copied().unwrap_or(0),
                "the cap bounds what is retained, never what is answered: df({t})"
            );
        }
        assert_eq!(g.idf("alpha").to_bits(), e.idf("alpha").to_bits(), "and so the weight agrees");

        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(c.doc_freq.len(), STATS_TERM_CAP, "the cache is capped");
        assert_eq!(c.fill_order.len(), STATS_TERM_CAP, "and the eviction order with it");
        assert_eq!(c.num_docs, 40, "the globals are untouched by the eviction");
        assert_eq!(c.total_doc_len, 40);
        assert!(
            !c.doc_freq.contains_key("alpha"),
            "`alpha` was evicted by its own query, and the assertions above still hold: that \
             sentence is the whole design, so it is an assertion and not a comment"
        );
        assert!(
            !c.doc_freq.contains_key(&terms[1]) && c.doc_freq.contains_key(&terms[n]),
            "oldest first, as a retention policy: the first term filled is gone and the last \
             one is still there"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// CATALOG and MANIFEST are published by a rename, and a rename is a change
    /// to a directory: until that directory is fsynced the bytes can be durable
    /// while the name that reaches them is not. Asserted at the callers rather
    /// than on `atomic_write` itself, so that deleting the sync turns this red
    /// from the path the database actually takes -- and asserted as an ORDER,
    /// because a directory synced before the rename is a directory synced for
    /// nothing.
    #[test]
    fn the_catalog_and_manifest_renames_are_made_durable_at_their_callers() {
        let dir = tmp("dirsync");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();

        durability_probe::start();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        let ev = durability_probe::take();
        let catalog =
            ev.at(Op::Rename, &dir.join("CATALOG")).expect("CATALOG was not published by a rename");
        assert!(
            ev.at_after(Op::DirSync, &dir, catalog).is_some(),
            "CATALOG was renamed into {dir:?} and that directory was not synced after it: {ev:?}"
        );

        let sdir = dir.join("collections").join("notes").join("shard-0000");
        db.insert("notes", note("a")).unwrap();
        durability_probe::start();
        db.flush("notes").unwrap();
        db.persist().unwrap();
        let ev = durability_probe::take();
        let manifest = ev
            .at(Op::Rename, &sdir.join("MANIFEST"))
            .expect("MANIFEST was not published by a rename");
        assert!(
            ev.at_after(Op::DirSync, &sdir, manifest).is_some(),
            "MANIFEST was renamed into {sdir:?} and that directory was not synced after it: {ev:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A `CREATE COLLECTION` is acknowledged, and an `INSERT` after it is
    /// acknowledged on the strength of an fdatasync'd WAL record. fdatasync
    /// flushes the file's data and cannot create the directory entry that names
    /// the file, so unless the directories created along the way are fsynced
    /// too, the acknowledged insert lives in a file a crash can take away
    /// wholesale -- and `Db::open`'s shard scan stops at the missing directory,
    /// leaving a collection that is in the catalog with no shards, answering
    /// `already exists` to CREATE and `no such collection` to everything else.
    ///
    /// The chain is the assertion: every directory from the root down to each
    /// shard, and all of it before the first record is synced into any of them.
    ///
    /// Two shards, and the collection directory's own fsync stated against the
    /// LAST of them. A collection with one shard cannot tell a chain walk that
    /// runs once for the collection from one that runs for the first shard and
    /// stops: both fsync `collections/notes` exactly once, at a moment that
    /// looks the same. They are not the same. The second leaves `shard-0001`'s
    /// entry in `collections/notes` as dirty metadata a crash takes, and
    /// `Db::open`'s scan stops at the first shard directory that is not there
    /// -- so the collection comes back with half its key space missing and
    /// nothing anywhere returns an error.
    /// `insert_batch` is the number of documents per sync within one
    /// statement: 2,500 documents at 1,000 a chunk are three syncs of the
    /// shard's log, and every document is there afterwards.
    #[test]
    fn a_statement_of_many_documents_syncs_once_per_insert_batch() {
        let dir =
            std::env::temp_dir().join(format!("celastro-insert-batch-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut opts = DbOpts::default();
        opts.insert_batch = 1000;
        let mut db = Db::open(&dir, opts).unwrap();
        db.execute("CREATE COLLECTION items (id TEXT PRIMARY KEY)").unwrap();
        let log = dir.join("collections").join("items").join("shard-0000").join("wal.log");
        let docs: Vec<Value> = (0..2500)
            .map(|i| Value::obj(vec![("id".to_string(), Value::Str(format!("d{i:05}")))]))
            .collect();
        durability_probe::start();
        db.insert_many("items", docs).unwrap();
        let ev = durability_probe::take();
        assert_eq!(ev.count(Op::WalSync, &log), 3, "{}", ev.count(Op::WalAppend, &log));
        assert_eq!(ev.count(Op::WalAppend, &log), 2500);
        assert_eq!(db.query("SELECT id FROM items LIMIT 10000").unwrap().rows.len(), 2500);
        let _ = fs::remove_dir_all(&dir);
    }

    /// One process -- one `Db` -- per directory: a second open while the
    /// first is alive is refused naming the directory and the holder, and
    /// succeeds once the first is dropped. The lock is `flock`, so a crash
    /// releases it without anyone cleaning up.
    #[test]
    fn a_directory_is_opened_by_one_db_at_a_time() {
        let dir = std::env::temp_dir().join(format!("celastro-dirlock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let first = Db::open(&dir, DbOpts::default()).unwrap();
        let e = match Db::open(&dir, DbOpts::default()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a second open of a held directory succeeded"),
        };
        assert!(e.contains("open in another process"), "{e}");
        assert!(e.contains(&format!("pid {}", std::process::id())), "{e}");
        assert!(dir.join("LOCK").exists());
        drop(first);
        let again = Db::open(&dir, DbOpts::default());
        assert!(again.is_ok(), "{:?}", again.err());
        drop(again);
        let _ = fs::remove_dir_all(&dir);
    }

    /// On a collection split by key with no partition key, an equality on
    /// the primary key asks the one shard whose range holds it; EXPLAIN
    /// shows the others pruned.
    #[test]
    fn a_primary_key_equality_prunes_to_the_owning_shard() {
        let mut db = Db::in_memory();
        db.execute(
            "CREATE COLLECTION items (id TEXT PRIMARY KEY, n INT) WITH (splits = ['g', 'p'])",
        )
        .unwrap();
        for (i, k) in ["a1", "h1", "t1"].iter().enumerate() {
            db.insert(
                "items",
                Value::obj(vec![
                    ("id".into(), Value::Str(k.to_string())),
                    ("n".into(), Value::Int(i as i64)),
                ]),
            )
            .unwrap();
        }
        let r = db.query("SELECT id FROM items WHERE id = 'h1' LIMIT 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        let plan = match db
            .execute("EXPLAIN ANALYZE SELECT id FROM items WHERE id = 'h1' LIMIT 1")
            .unwrap()
        {
            Outcome::Explain(t) => t,
            other => panic!("{other:?}"),
        };
        assert_eq!(plan.matches("PRUNED").count(), 2, "{plan}");
        let r = db.query("SELECT id FROM items WHERE n = 1 LIMIT 5").unwrap();
        assert_eq!(r.rows.len(), 1, "a predicate that is not on the key still fans out");
    }

    #[test]
    fn creating_a_collection_makes_the_directories_that_hold_it_durable() {
        let dir = tmp("dircreate");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        let cdir = dir.join("collections").join("notes");
        let sdirs: Vec<PathBuf> = (0..2).map(|i| cdir.join(format!("shard-{i:04}"))).collect();

        durability_probe::start();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['m'])").unwrap();
        // One document on each side of the split, so every WAL in the
        // collection is one a crash could be asked about.
        db.insert("notes", note("a")).unwrap();
        db.insert("notes", note("z")).unwrap();
        let ev = durability_probe::take();

        for d in [&dir, &dir.join("collections"), &cdir].into_iter().chain(sdirs.iter()) {
            assert!(
                ev.at(Op::DirSync, d).is_some(),
                "{d:?} names something this database needs and was never made durable: {ev:?}"
            );
        }

        for sdir in &sdirs {
            let log = sdir.join("wal.log");
            // The shard's own directory, in the order the claim needs: `sdir`
            // does not name `wal.log` until `Wal::open` has created it, so an
            // fsync of `sdir` above that call syncs a directory the log is not
            // in yet and its name is never made durable at all.
            let created = ev
                .at(Op::WalCreate, &log)
                .unwrap_or_else(|| panic!("{log:?} was never created: {ev:?}"));
            assert!(
                ev.at_after(Op::DirSync, sdir, created).is_some(),
                "{sdir:?} was fsynced before it named {log:?}, so the log every later insert is \
                 acknowledged against is a file a crash can still take: {ev:?}"
            );
            assert!(
                ev.ordered((Op::DirSync, sdir), (Op::WalSync, &log)),
                "the insert's record was fsynced into a WAL whose own directory entry was still \
                 only in the page cache: {ev:?}"
            );
            assert!(
                ev.ordered((Op::DirSync, &cdir), (Op::WalSync, &log)),
                "the shard directory's own name was not durable when the insert was \
                 acknowledged, so the whole shard can be gone: {ev:?}"
            );
        }

        let last = ev.at(Op::WalCreate, &sdirs[1].join("wal.log")).unwrap();
        assert!(
            ev.at_after(Op::DirSync, &cdir, last).is_some(),
            "{cdir:?} was made durable before the last shard directory in it existed, so the \
             shards after the first are names a crash takes back: {ev:?}"
        );

        // Nothing about this is per-write: the second insert syncs no directory
        // at all.
        durability_probe::start();
        db.insert("notes", note("b")).unwrap();
        let ev = durability_probe::take();
        assert!(ev.paths(Op::DirSync).is_empty(), "a write paid for a directory fsync: {ev:?}");

        let _ = fs::remove_dir_all(&dir);
    }

    /// RANGE decides which keys a shard owns and the statement that writes it
    /// is acknowledged as durable, so it is published like every other file
    /// this database cannot afford to find half-written.
    ///
    /// The second half is what a bare `fs::write` used to leave behind: a
    /// zero-length RANGE, whose dirent committed while its data was still in
    /// the page cache. That used to be read as a shard with no bounds -- which
    /// owns every key, so every shard in the collection owns every key, writes
    /// land wherever the router looked first and the same key is readable from
    /// two shards. There is no later check for it, because every shard agrees
    /// with itself.
    #[test]
    fn the_tablet_map_is_published_durably_and_a_damaged_one_is_refused() {
        let dir = tmp("range-durable");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();

        durability_probe::start();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['m'])").unwrap();
        let ev = durability_probe::take();

        let cdir = dir.join("collections").join("notes");
        for i in 0..2 {
            let sdir = cdir.join(format!("shard-{i:04}"));
            let range = sdir.join("RANGE");
            let renamed = ev.at(Op::Rename, &range).unwrap_or_else(|| {
                panic!("{range:?} was not published by a rename, so a crash can find it half written: {ev:?}")
            });
            assert!(
                ev.at_after(Op::DirSync, &sdir, renamed).is_some(),
                "{range:?} was renamed into a directory that was never synced after it: {ev:?}"
            );
        }
        drop(db);

        // The read side of the same two-line file, which is the other half of
        // what it means. An empty line is how RANGE spells an unbounded end,
        // and reading it as the bound `""` instead of as `None` is not a
        // decoding detail: `""` sorts below every key, so the top shard's `hi`
        // becomes `Some("")` and it owns nothing at all -- every insert above
        // the split is refused with `no shard owns key`, and no later check
        // notices, because every shard still agrees with itself.
        let mut re = Db::open(&dir, DbOpts::default()).unwrap();
        let ranges: Vec<_> =
            re.shards("notes").unwrap().iter().map(|s| s.key_range.clone()).collect();
        assert_eq!(
            ranges,
            vec![Some((None, Some("m".to_string()))), Some((Some("m".to_string()), None))],
            "an empty RANGE line is an unbounded end, not a bound: {ranges:?}"
        );
        // And the consequence, from the outside: both ends of the key space
        // still route somewhere.
        re.insert("notes", note("zzz")).unwrap();
        re.insert("notes", note("aaa")).unwrap();
        drop(re);

        // What a crash between the dirent and the data used to leave.
        let range = cdir.join("shard-0001").join("RANGE");
        fs::write(&range, b"").unwrap();
        match Db::open(&dir, DbOpts::default()) {
            Err(Error::Storage(m)) => assert!(m.contains("RANGE"), "{m}"),
            Err(e) => panic!("the wrong failure: {e}"),
            Ok(_) => panic!("an empty tablet map was read as a shard that owns every key"),
        }
        fs::remove_file(&range).unwrap();
        assert!(
            matches!(Db::open(&dir, DbOpts::default()), Err(Error::Storage(_))),
            "a missing tablet map was read as a shard that owns every key"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The staleness gate counts writes to the collection whose statistics it
    /// guards. It used to count writes to the whole engine, so a burst on an
    /// unrelated collection ended this one's epoch and unanchored its
    /// globals. Both halves are pinned: a refresh interval of writes to B
    /// leaves A's epoch and anchor where they were, and the same writes to A
    /// end it.
    #[test]
    fn writes_to_another_collection_do_not_age_this_ones_statistics() {
        let mut db = Db::with_opts(DbOpts::default());
        for name in ["notes", "other"] {
            db.execute(&format!("CREATE COLLECTION {name} (id TEXT PRIMARY KEY)")).unwrap();
            db.execute(&format!(
                "CREATE INDEX {name}_body ON {name} USING fulltext (body) WITH (analyzer = 'english')"
            ))
            .unwrap();
        }
        for i in 0..40 {
            db.insert("notes", note(&format!("n{i:02}"))).unwrap();
        }
        let want = |terms: &[&str]| {
            BTreeMap::from([(
                "body".to_string(),
                terms.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            )])
        };
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(&["segments"]), ts, false).unwrap();
        let before = {
            let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
            (c.refreshed_at_writes, c.measured_at_writes, c.num_docs, c.anchored)
        };
        assert!(before.3, "the first gather did not anchor");

        for i in 0..STATS_REFRESH_WRITES as usize {
            db.insert("other", note(&format!("o{i:04}"))).unwrap();
        }
        let ts = db.clock.peek();
        let gathered_by_notes = |db: &Db| -> u64 {
            db.shards("notes")
                .unwrap()
                .iter()
                .map(|s| s.terms_gathered.load(std::sync::atomic::Ordering::Relaxed))
                .sum()
        };
        let gathered_before = gathered_by_notes(&db);
        db.gather_stats("notes", &want(&["segments", "postings"]), ts, false).unwrap();
        let gathered = gathered_by_notes(&db) - gathered_before;
        // One shard, one missing term: an anchor that compared the engine-wide
        // count would find it moved and re-measure both.
        assert_eq!(gathered, 1, "writes to `other` made `notes` re-gather {gathered} terms");
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert_eq!(
            (c.refreshed_at_writes, c.num_docs, c.anchored),
            (before.0, before.2, true),
            "writes to `other` aged `notes`"
        );
        assert_eq!(c.measured_at_writes, before.1, "writes to `other` unanchored `notes`");
        assert_ne!(c.measured_at_writes, db.writes, "the anchor is the engine-wide count");
        assert!(c.doc_freq.contains_key("postings"), "the new term was not filled");

        for i in 0..STATS_REFRESH_WRITES as usize {
            db.insert("notes", note(&format!("m{i:04}"))).unwrap();
        }
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(&["segments"]), ts, false).unwrap();
        let c = guard(&db.stats).get(&cache_key("notes", "body")).unwrap().clone();
        assert!(c.refreshed_at_writes > before.0, "writes to `notes` did not end its epoch");
    }

    /// A copy is the source at the instant it was pinned, whatever the source
    /// does afterwards. Three shards, sealed rows and memtable rows, deletes
    /// on both; then, between the pin and the write, inserts, deletes of rows
    /// the copy holds, updates, a flush and a compaction. The copy opens as a
    /// database of its own and answers exactly what the source answered at
    /// the pin, document by document and byte for byte -- and the source no
    /// longer does, which is what proves the interleaving reached it and not
    /// the copy.
    #[test]
    fn a_copy_is_the_source_at_its_pinned_instant_whatever_happens_after() {
        let dir = tmp("export-src");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute(
            "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, n INT) \
             PARTITION BY (tenant_id) WITH (splits = ['t1', 't2'])",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let doc = |i: usize, v: usize| {
            crate::json::parse(&format!(
                r#"{{"id":"d{i:04}","tenant_id":"t{}","n":{v},"body":"item {i} version {v}"}}"#,
                i % 3
            ))
            .unwrap()
        };
        let key = |i: usize| format!("t{}\u{1}d{i:04}", i % 3);
        for i in 0..200 {
            db.insert("items", doc(i, 0)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        for i in 200..300 {
            db.insert("items", doc(i, 0)).unwrap();
        }
        for i in (0..300).step_by(10) {
            assert!(db.delete_key("items", &key(i)).unwrap(), "{}", key(i));
        }
        let rows = |db: &mut Db| -> Vec<(String, Vec<u8>)> {
            let mut r: Vec<(String, Vec<u8>)> = db
                .query("SELECT * FROM items LIMIT 100000")
                .unwrap()
                .rows
                .into_iter()
                .map(|x| (x.key, crate::variant::encode_to_vec(&x.doc)))
                .collect();
            r.sort();
            r
        };
        let expected = rows(&mut db);
        assert_eq!(expected.len(), 270);

        let export = db.export_collection("items").unwrap();

        // Everything a source does while a copy is in flight.
        for i in 300..350 {
            db.insert("items", doc(i, 0)).unwrap();
        }
        for i in (5..300).step_by(10) {
            assert!(db.delete_key("items", &key(i)).unwrap());
        }
        for i in (1..300).step_by(7) {
            db.insert("items", doc(i, 1)).unwrap();
        }
        db.execute("FLUSH items").unwrap();
        db.execute("COMPACT items").unwrap();
        assert_ne!(rows(&mut db), expected, "the interleaved writes changed nothing");

        let dest = tmp("export-dst");
        export.write_to(&dest).unwrap();
        drop(export);
        assert!(!dest.with_extension("tmp").exists(), "the temporary directory was left behind");
        let mut copy = Db::open(&dest, DbOpts::default()).unwrap();
        assert_eq!(rows(&mut copy), expected, "the copy is not the source at the pinned instant");
        assert_eq!(copy.shards("items").unwrap().len(), 3);
        // And it is a database, not a snapshot: it takes writes of its own.
        copy.insert("items", doc(900, 0)).unwrap();
        assert_eq!(rows(&mut copy).len(), 271);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dest);
    }

    /// A copy that fails partway leaves the destination absent or complete,
    /// never in between. Two failures, either side of the one rename: the
    /// fsync of the temporary tree, after every file has been written under
    /// it, which is the moment a half-populated destination would look most
    /// complete -- and the destination is absent; and the fsync of the parent
    /// after the rename -- and the destination is there, whole, and opens.
    #[test]
    fn an_interrupted_copy_leaves_no_destination_to_open_by_mistake() {
        let dir = tmp("export-fail-src");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        for id in ["a", "b", "c"] {
            db.insert("notes", note(id)).unwrap();
        }
        db.execute("FLUSH notes").unwrap();
        db.insert("notes", note("d")).unwrap();
        let export = db.export_collection("notes").unwrap();
        let dest = tmp("export-fail-dst");
        durability_probe::fail_next(Op::DirSync, &dest.with_extension("tmp"));
        let e = export.write_to(&dest).expect_err("the injected failure was not reported");
        assert!(matches!(e, Error::Io(_)), "{e}");
        assert!(!dest.exists(), "a failed copy left a destination");
        assert!(!dest.with_extension("tmp").exists(), "a failed copy left its temporary tree");
        // After the rename only the parent's fsync is left; a failure there
        // is reported, and what it leaves is complete.
        durability_probe::fail_next(Op::DirSync, dest.parent().unwrap());
        let e = export.write_to(&dest).expect_err("the injected failure was not reported");
        assert!(matches!(e, Error::Io(_)), "{e}");
        let mut copy = Db::open(&dest, DbOpts::default()).unwrap();
        assert_eq!(copy.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 4);
        drop(copy);
        let _ = fs::remove_dir_all(&dest);
        // And with nothing armed, the same export writes fine.
        export.write_to(&dest).unwrap();
        let mut copy = Db::open(&dest, DbOpts::default()).unwrap();
        assert_eq!(copy.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 4);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dest);
    }

    /// A copy is adopted by another instance beside what it already holds,
    /// refused where the name is taken, and still there after a reopen.
    #[test]
    fn an_import_adds_the_collection_to_another_instance() {
        let src_dir = tmp("import-src");
        let mut src = Db::open(&src_dir, DbOpts::default()).unwrap();
        src.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        for id in ["a", "b", "c"] {
            src.insert("notes", note(id)).unwrap();
        }
        src.execute("FLUSH notes").unwrap();
        src.insert("notes", note("d")).unwrap();
        let export = src.export_collection("notes").unwrap();
        let exported = tmp("import-exported");
        export.write_to(&exported).unwrap();

        let dst_dir = tmp("import-dst");
        let mut dst = Db::open(&dst_dir, DbOpts::default()).unwrap();
        dst.execute("CREATE COLLECTION other (id TEXT PRIMARY KEY)").unwrap();
        dst.insert("other", note("x")).unwrap();
        assert_eq!(dst.import_collection(&exported).unwrap(), "notes");
        assert_eq!(dst.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 4);
        assert_eq!(dst.query("SELECT id FROM other LIMIT 10").unwrap().rows.len(), 1);
        assert!(matches!(dst.import_collection(&exported), Err(Error::Plan(_))));
        drop(dst);
        let mut dst = Db::open(&dst_dir, DbOpts::default()).unwrap();
        assert_eq!(dst.query("SELECT id FROM notes LIMIT 10").unwrap().rows.len(), 4);
        assert_eq!(dst.collection_count(), 2);
        let mut mem = Db::in_memory();
        assert!(matches!(mem.import_collection(&exported), Err(Error::Plan(_))));
        for d in [src_dir, exported, dst_dir] {
            let _ = fs::remove_dir_all(&d);
        }
    }

    /// A tier move is a publication. `sync_archive` relocates a segment
    /// between `segments/` and `archive/`, and the manifest finds it by id in
    /// whichever holds it -- so a rename whose directory entries a crash took
    /// back was a segment the manifest named and neither directory held. It
    /// was the one rename outside the publication path, invisible to the
    /// probe and to the invariant over the whole event log. Both directions
    /// are pinned, each followed by a reopen that has to find the segment.
    #[test]
    fn an_archive_move_fsyncs_both_directories_and_survives_a_reopen() {
        let dir = tmp("archive-move");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        for id in ["a", "b", "c"] {
            db.insert("notes", note(id)).unwrap();
        }
        db.flush("notes").unwrap();
        let sdir = dir.join("collections").join("notes").join("shard-0000");
        for (tier, sub) in [("archived", "archive"), ("active", "segments")] {
            durability_probe::start();
            db.execute(&format!("ALTER INDEX notes_body ON notes SET TIER '{tier}'")).unwrap();
            let ev = durability_probe::take();
            let moved: Vec<PathBuf> = ev
                .paths(Op::Rename)
                .into_iter()
                .filter(|p| p.extension().is_some_and(|e| e == "seg"))
                .collect();
            assert!(!moved.is_empty(), "{tier}: no segment was moved, so this says nothing");
            assert!(
                moved.iter().all(|p| p.starts_with(sdir.join(sub))),
                "{tier}: a segment was moved somewhere other than {sub}/: {moved:?}"
            );
            let unpublished = ev.unpublished_renames();
            assert!(unpublished.is_empty(), "{tier}: moved and not fsynced: {unpublished:?}");
            assert!(
                ev.at(Op::DirSync, &sdir.join("segments")).is_some()
                    && ev.at(Op::DirSync, &sdir.join("archive")).is_some(),
                "{tier}: only one of the two directories was synced: {ev:?}"
            );
            drop(db);
            db = Db::open(&dir, DbOpts::default()).unwrap();
            assert_eq!(
                db.query("SELECT id FROM notes WHERE text_match(body, 'segments') LIMIT 10")
                    .unwrap()
                    .rows
                    .len(),
                3,
                "{tier}: the moved segment was not found at the reopen"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A statement that cannot finish within its deadline is refused with the
    /// budget named and the two ways to change it, on every query shape, and
    /// by DEFAULT: this `Db` was given a budget of nothing and the statements
    /// say nothing. `WITH (partial_results)` turns the refusal into an answer
    /// that says which shards are missing, and `WITH (no_deadline)` lifts
    /// the budget for one statement. Writes, DDL and maintenance carry no
    /// budget: the same `Db` creates, inserts and flushes without complaint.
    #[test]
    fn a_statement_past_its_deadline_is_refused_by_default_and_the_budget_is_named() {
        let dir = tmp("deadline");
        let opts = DbOpts { statement_deadline_ms: Some(0), ..Default::default() };
        let mut db = Db::open(&dir, opts).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX notes_emb ON notes USING vector (embedding) \
             WITH (dims = 4, metric = 'cosine')",
        )
        .unwrap();
        for i in 0..50 {
            let d = crate::json::parse(&format!(
                r#"{{"id":"n{i:02}","topic":"t","body":"segments and postings {i}","embedding":[{},1.0,0.5,0.25]}}"#,
                i as f32 / 50.0
            ))
            .unwrap();
            db.insert("notes", d).unwrap();
        }
        let shapes = [
            "SELECT id FROM notes LIMIT 5",
            "SELECT id FROM notes WHERE text_match(body, 'segments') LIMIT 5",
            "SELECT id FROM notes ORDER BY embedding <=> [1,0,0,0] LIMIT 5",
            "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'segments'), \
             embedding <=> [1,0,0,0]) LIMIT 5",
            "SELECT id FROM notes WHERE embedding <=> [1,0,0,0] < 0.5 LIMIT 5",
        ];
        for sql in shapes {
            match db.query(sql) {
                Err(Error::Deadline(m)) => {
                    assert!(m.contains("0 ms") && m.contains("deadline_ms"), "{sql}: {m}")
                }
                other => panic!("{sql}: {other:?}"),
            }
            let r = db.query(&format!("{sql} WITH (partial_results)")).unwrap();
            assert!(!r.missing.is_empty(), "{sql}: nothing was reported missing");
            let r = db.query(&format!("{sql} WITH (no_deadline)")).unwrap();
            assert_eq!(r.rows.len(), 5, "{sql}: lifting the deadline");
            assert!(r.missing.is_empty());
        }
        db.execute("FLUSH notes").unwrap();
        assert_eq!(
            db.query("SELECT id FROM notes LIMIT 5 WITH (deadline_ms = 60000)").unwrap().rows.len(),
            5
        );
        drop(db);

        // And the default IS a budget: a `Db` given no opinion shows one in
        // the plan, and only `no_deadline` takes it away.
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        let plan = |db: &mut Db, with: &str| match db
            .execute(&format!("EXPLAIN ANALYZE SELECT id FROM notes LIMIT 5{with}"))
            .unwrap()
        {
            Outcome::Explain(t) => t,
            _ => panic!("expected a plan"),
        };
        let text = plan(&mut db, "");
        assert!(
            text.contains(&format!("deadline={DEFAULT_STATEMENT_DEADLINE_MS} ms")),
            "a default Db has no budget: {text}"
        );
        assert!(plan(&mut db, " WITH (deadline_ms = 7)").contains("deadline=7 ms"));
        assert!(plan(&mut db, " WITH (no_deadline)").contains("deadline=none"));
        let _ = fs::remove_dir_all(&dir);
    }

    /// A distance threshold in WHERE is a filter that agrees with the
    /// `distance` column of the nearest-neighbour path for the same vector
    /// and operator, at every threshold and for both metrics; composes with
    /// a structured predicate; is three-valued under NOT, so a document with
    /// no vector is on neither side; and reports its strategy. Exact match
    /// is `<= 0` for L2, where identical vectors are at exactly 0, and a
    /// small threshold for cosine, where normalisation leaves an identical
    /// vector within floating-point rounding of 0 and not reliably at it.
    /// Each claim is the mutation that passes the others: comparing
    /// the raw metric value rather than the presented one moves every L2
    /// threshold; a leaf that answered "every vector" or "none" fails the
    /// agreement at the first threshold; a two-valued NOT puts the
    /// vectorless document on the outside.
    #[test]
    fn a_distance_threshold_in_where_agrees_with_the_distance_column() {
        for metric in ["cosine", "l2"] {
            let dir = tmp(&format!("threshold-{metric}"));
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            db.execute("CREATE COLLECTION pts (id TEXT PRIMARY KEY, kind TEXT)").unwrap();
            db.execute(&format!(
                "CREATE INDEX pts_v ON pts USING vector (v) WITH (dims = 4, metric = '{metric}')"
            ))
            .unwrap();
            let mut seed = 0x2545f4914f6cdd1du64;
            let mut next = move || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
            };
            for i in 0..120 {
                let v: Vec<String> = (0..4).map(|_| format!("{:.6}", next())).collect();
                let doc = format!(
                    r#"{{"id":"p{i:03}","kind":"{}","v":[{}]}}"#,
                    if i % 3 == 0 { "a" } else { "b" },
                    v.join(",")
                );
                db.insert("pts", crate::json::parse(&doc).unwrap()).unwrap();
                if i == 59 {
                    db.execute("FLUSH pts").unwrap();
                }
            }
            db.insert("pts", crate::json::parse(r#"{"id":"p999","kind":"a"}"#).unwrap()).unwrap();
            let sorted = |r: QueryResult| {
                let mut k: Vec<String> = r.rows.into_iter().map(|x| x.key).collect();
                k.sort();
                k
            };
            let op = if metric == "cosine" { "<=>" } else { "<->" };
            let q = "[0.5,0.25,-0.25,0.1]";
            let ranked =
                db.query(&format!("SELECT id FROM pts ORDER BY v {op} {q} LIMIT 1000")).unwrap();
            assert_eq!(ranked.rows.len(), 120);
            let shown = |keep: &dyn Fn(f64) -> bool| {
                let mut k: Vec<String> = ranked
                    .rows
                    .iter()
                    .filter(|r| keep(r.distance.unwrap() as f64))
                    .map(|r| r.key.clone())
                    .collect();
                k.sort();
                k
            };
            for t in [0.0, 0.05, 0.3, 0.8, 1.5] {
                let got = sorted(
                    db.query(&format!("SELECT id FROM pts WHERE v {op} {q} < {t} LIMIT 1000"))
                        .unwrap(),
                );
                assert_eq!(got, shown(&|d| d < t), "{metric} < {t}");
                let got = sorted(
                    db.query(&format!("SELECT id FROM pts WHERE v {op} {q} >= {t} LIMIT 1000"))
                        .unwrap(),
                );
                assert_eq!(got, shown(&|d| d >= t), "{metric} >= {t}");
            }
            let got = sorted(
                db.query(&format!(
                    "SELECT id FROM pts WHERE kind = 'a' AND v {op} {q} < 0.8 LIMIT 1000"
                ))
                .unwrap(),
            );
            let want: Vec<String> = shown(&|d| d < 0.8)
                .into_iter()
                .filter(|k| k[1..].parse::<usize>().unwrap() % 3 == 0)
                .collect();
            assert!(!want.is_empty());
            assert_eq!(got, want, "{metric}: the threshold did not compose with kind = 'a'");

            let inside = sorted(
                db.query(&format!("SELECT id FROM pts WHERE v {op} {q} < 0.8 LIMIT 1000")).unwrap(),
            );
            let outside = sorted(
                db.query(&format!("SELECT id FROM pts WHERE NOT (v {op} {q} < 0.8) LIMIT 1000"))
                    .unwrap(),
            );
            assert_eq!(
                inside.len() + outside.len(),
                120,
                "{metric}: NOT is not the complement over vectors"
            );
            assert!(
                !outside.contains(&"p999".to_string()),
                "{metric}: a document with no vector is outside"
            );

            let stored = db.query("SELECT v FROM pts WHERE id = 'p007' LIMIT 1").unwrap();
            let lit = crate::json::to_string(stored.rows[0].doc.get("v").unwrap());
            let zero = if metric == "l2" { "<= 0" } else { "< 0.000001" };
            let exact = sorted(
                db.query(&format!("SELECT id FROM pts WHERE v {op} {lit} {zero} LIMIT 10"))
                    .unwrap(),
            );
            assert_eq!(exact, vec!["p007".to_string()], "{metric}: exact match");

            let text = match db
                .execute(&format!(
                    "EXPLAIN ANALYZE SELECT id FROM pts WHERE v {op} {q} < 0.3 LIMIT 5"
                ))
                .unwrap()
            {
                Outcome::Explain(t) => t,
                _ => panic!("expected a plan"),
            };
            assert!(text.contains("strategy=brute_force"), "{text}");
            assert!(text.contains(&format!("v {op} [4] < 0.3 [filter]")), "{text}");

            let wrong = if metric == "cosine" { "<->" } else { "<=>" };
            assert!(matches!(
                db.query(&format!("SELECT id FROM pts WHERE v {wrong} {q} < 0.3")),
                Err(Error::Plan(_))
            ));
            assert!(matches!(
                db.query("SELECT id FROM pts WHERE kind <=> [1,2,3,4] < 0.3"),
                Err(Error::Plan(_))
            ));
            assert!(matches!(
                db.query(&format!("SELECT id FROM pts WHERE v {op} [1,2,3] < 0.3")),
                Err(Error::Plan(_))
            ));
            let _ = fs::remove_dir_all(&dir);
        }
    }

    /// A scan under a small `LIMIT` decodes the page and not the collection.
    /// It used to decode and buffer every matching document before taking
    /// `offset + k` of them. Three shapes against the same 600 documents,
    /// half sealed and half in memtables, each compared with the answer of a
    /// scan that sees everything: key order decodes exactly the rows it
    /// returns, even past an offset and a cursor; an `ORDER BY` on a field
    /// has to decode every survivor to place it, and still answers the same;
    /// and `COLLAPSE BY` returns the best row of each of the first parents,
    /// not `k` children of the first one.
    #[test]
    fn a_scan_under_a_small_limit_decodes_the_page_and_answers_like_a_full_one() {
        use crate::shard::documents_decoded;
        let dir = tmp("bounded-scan");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute(
            "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL) \
             PARTITION BY (tenant_id) WITH (splits = ['t1', 't2'])",
        )
        .unwrap();
        for i in 0..600 {
            let d = crate::json::parse(&format!(
                r#"{{"id":"d{i:04}","tenant_id":"t{}","n":{},"parent":"p{:02}"}}"#,
                i % 3,
                (i * 7919) % 600,
                i % 50
            ))
            .unwrap();
            db.insert("items", d).unwrap();
            if i == 299 {
                db.execute("FLUSH items").unwrap();
            }
        }
        let keys = |r: &QueryResult| r.rows.iter().map(|x| x.key.clone()).collect::<Vec<_>>();
        let full = db.query("SELECT id FROM items LIMIT 100000").unwrap();
        assert_eq!(full.rows.len(), 600);

        // Key order: decodes exactly the page, wherever the page is.
        let decoded = |db: &mut Db, sql: &str| {
            let before = documents_decoded();
            let r = db.query(sql).unwrap();
            (r, documents_decoded() - before)
        };
        let (page, n) = decoded(&mut db, "SELECT * FROM items LIMIT 5");
        assert_eq!(keys(&page), keys(&full)[..5].to_vec());
        assert_eq!(n, 5, "a LIMIT 5 scan decoded {n} documents");
        let (page, n) = decoded(&mut db, "SELECT * FROM items LIMIT 5 OFFSET 200");
        assert_eq!(keys(&page), keys(&full)[200..205].to_vec());
        assert_eq!(n, 5, "an OFFSET 200 scan decoded {n} documents");
        let cursor = page.next_cursor.clone();
        assert!(cursor.is_none(), "an unranked scan hands out no cursor");
        let (page, n) =
            decoded(&mut db, &format!("SELECT * FROM items LIMIT 3 AFTER '{}'", keys(&full)[100]));
        assert_eq!(keys(&page), keys(&full)[101..104].to_vec(), "the cursor resumed elsewhere");
        assert_eq!(n, 3, "a cursor scan decoded {n} documents");

        // A field order has to read every survivor, and answers the same.
        let all = db.query("SELECT id, n FROM items ORDER BY n DESC LIMIT 100000").unwrap();
        let (page, n) =
            decoded(&mut db, "SELECT id, n FROM items ORDER BY n DESC LIMIT 4 OFFSET 7");
        assert_eq!(keys(&page), keys(&all)[7..11].to_vec());
        assert_eq!(n, 600, "an ORDER BY scan decoded {n} documents where the order needs all 600");

        // COLLAPSE BY: the best row of each of the first parents.
        let all = db.query("SELECT id, parent FROM items LIMIT 100000 COLLAPSE BY parent").unwrap();
        assert_eq!(all.rows.len(), 50);
        let page =
            db.query("SELECT id, parent FROM items LIMIT 6 OFFSET 2 COLLAPSE BY parent").unwrap();
        assert_eq!(keys(&page), keys(&all)[2..8].to_vec());
        let parents: std::collections::BTreeSet<String> = page
            .rows
            .iter()
            .map(|r| r.doc.get("parent").unwrap().as_str().unwrap().to_string())
            .collect();
        assert_eq!(parents.len(), 6, "collapsed rows share a parent: {parents:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The SELECT list narrows what comes back. It used to be parsed and read
    /// nowhere, so `SELECT id FROM notes` returned the whole document on
    /// every surface and the shells printed every field. Each claim below is
    /// the mutation that would pass the others: a named path is kept and an
    /// unnamed one is not; an alias renames; a nested path is keyed as
    /// written; a path the document lacks is `Null` rather than absent, so the
    /// rows share a shape; `*` keeps everything; and a ranked query's `score`
    /// is on the row whether or not the list names it.
    #[test]
    fn the_select_list_decides_what_a_row_carries() {
        let dir = tmp("select-list");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        let mut n1 = note("n1");
        n1.set_path("meta.tag", Value::Str("kept".into())).unwrap();
        db.insert("notes", n1).unwrap();
        db.insert("notes", note("n2")).unwrap();

        let r = db.query("SELECT id, title AS t, meta.tag FROM notes LIMIT 10").unwrap();
        assert_eq!(r.rows.len(), 2);
        for row in &r.rows {
            let Value::Object(fields) = &row.doc else { panic!("{:?}", row.doc) };
            let names: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(names, vec!["id", "meta.tag", "t"], "key {}", row.key);
            assert!(row.doc.get("body").is_none(), "an unnamed field came back: {:?}", row.doc);
            assert_eq!(row.doc.get("t").and_then(|v| v.as_str()), Some("a title"));
        }
        let by_key = |k: &str| r.rows.iter().find(|row| row.key == k).unwrap();
        assert_eq!(by_key("n1").doc.get("meta.tag").and_then(|v| v.as_str()), Some("kept"));
        assert_eq!(by_key("n2").doc.get("meta.tag"), Some(&Value::Null));

        let whole = db.query("SELECT * FROM notes WHERE id = 'n2' LIMIT 1").unwrap();
        assert_eq!(whole.rows[0].doc, note("n2"));

        let ranked = db
            .query("SELECT id FROM notes ORDER BY hybrid(text_match(body, 'segments')) LIMIT 2")
            .unwrap();
        assert_eq!(ranked.rows.len(), 2);
        for row in &ranked.rows {
            assert!(row.score.is_some(), "the score left the row with the projection");
            assert_eq!(row.doc, Value::obj(vec![("id".into(), Value::Str(row.key.clone()))]));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Every document is counted once, however many times the directory is
    /// reopened. The persisted catalog used to count the memtable's documents,
    /// which the WAL also holds, so every reopen replayed and re-observed
    /// them on top of a baseline that already had them: 4, 8, 12, 16. Three
    /// claims, each the mutation that would pass the others: the count is
    /// right immediately after an open with nothing asked yet (open absorbs);
    /// it is unchanged across reopens with an unflushed WAL (the persisted
    /// catalog counts sealed documents only); and a record the WAL holds that
    /// no persist ever saw is still counted, once (replay observes).
    #[test]
    fn a_reopen_with_an_unflushed_wal_counts_its_documents_once() {
        let dir = tmp("count-once");
        let stats = |db: &Db| {
            let c = db.catalog.get("notes").unwrap();
            (c.doc_count, c.paths.get("title").map(|p| p.present).unwrap_or(0))
        };
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT)").unwrap();
        for i in 0..4 {
            db.insert("notes", note(&format!("n{i}"))).unwrap();
        }
        db.execute("SHOW CATALOG notes").unwrap();
        assert_eq!(stats(&db), (4, 4));
        db.persist().unwrap();
        drop(db);

        for round in 1..=3 {
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            assert_eq!(stats(&db), (4, 4), "reopen {round}, before any statement");
            db.execute("SHOW CATALOG notes").unwrap();
            assert_eq!(stats(&db), (4, 4), "reopen {round}, after SHOW CATALOG");
            db.persist().unwrap();
        }

        // A record that reached the WAL after the last persist: nobody wrote
        // the catalog after it, so only the replay can count it.
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.insert("notes", note("n4")).unwrap();
        drop(db);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(stats(&db), (5, 5), "a record the catalog never saw");
        db.execute("FLUSH notes").unwrap();
        db.persist().unwrap();
        drop(db);
        for round in 1..=2 {
            let db = Db::open(&dir, DbOpts::default()).unwrap();
            assert_eq!(stats(&db), (5, 5), "reopen {round} after the flush");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// An unreadable CATALOG is not an absent one. `Db::open` read it with
    /// `if let Ok(b)`, so EIO or EACCES opened a database with no collections
    /// and the next DDL published that catalog over the real one. A directory
    /// where the file goes is the injection -- `fs::read` on it fails with
    /// EISDIR -- and the fresh open first is the other half: a genuinely
    /// absent catalog is a new database, and a fix that refused it would pass
    /// the second assertion alone.
    #[test]
    fn a_catalog_that_cannot_be_read_fails_the_open_rather_than_opening_empty() {
        let dir = tmp("catalog-unreadable");
        let db = Db::open(&dir, DbOpts::default()).expect("an absent catalog is a new database");
        assert!(db.catalog.collections.is_empty());
        drop(db);

        let catalog = dir.join("CATALOG");
        fs::create_dir(&catalog).unwrap();
        match Db::open(&dir, DbOpts::default()) {
            Err(Error::Storage(m)) => assert!(m.contains("CATALOG"), "{m}"),
            Err(e) => panic!("the wrong failure: {e}"),
            Ok(db) => panic!(
                "a catalog that could not be read opened a database with {} collections",
                db.catalog.collections.len()
            ),
        }
        assert!(catalog.is_dir(), "something replaced the catalog the open could not read");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The catalog's half of the skip guard. `Shard::persist_manifest` has one
    /// of these and the two are separate call sites: removing
    /// `still_published` from this one leaves a `Db` that has published CATALOG
    /// once agreeing, for the rest of its life, that a catalog which is no
    /// longer there is still published -- and every mutating statement in the
    /// shells acknowledging durability against it.
    #[test]
    fn a_catalog_that_disappeared_is_republished_rather_than_skipped() {
        let dir = tmp("catalog-regone");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        let published = fs::read(dir.join("CATALOG")).unwrap();
        fs::remove_file(dir.join("CATALOG")).unwrap();

        durability_probe::start();
        db.persist().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &dir.join("CATALOG")).is_some(),
            "the catalog is gone and a persist reported success without rewriting it: {ev:?}"
        );
        assert_eq!(fs::read(dir.join("CATALOG")).unwrap(), published);

        // And the same for a file of the same length that this `Db` did not
        // write: the comparison is the bytes, not the size.
        let mut other = published.clone();
        let last = other.len() - 1;
        other[last] ^= 0xff;
        fs::write(dir.join("CATALOG"), &other).unwrap();
        durability_probe::start();
        db.persist().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &dir.join("CATALOG")).is_some(),
            "a different file of the same length was accepted as the published catalog: {ev:?}"
        );
        assert_eq!(fs::read(dir.join("CATALOG")).unwrap(), published);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Every publication in the database, whatever wrote it: a rename, and then
    /// an fsync of the directory the new name landed in.
    ///
    /// The second half is the half that keeps being left out, and it is left
    /// out one call site at a time. CATALOG, MANIFEST and RANGE each got a test
    /// of their own as they were written; `Shard::persist_segment` was added
    /// after them and got none, and a `.seg` published without the directory
    /// fsync is a segment the durable MANIFEST names and a crash can unname --
    /// the reopen fails with `segment ... named by the manifest is missing`,
    /// with every byte of the segment on the disk.
    ///
    /// So this is deliberately not a fourth per-call-site test. It is the
    /// invariant read off the whole event log, and the workload is chosen to
    /// drive one of everything the database publishes: the catalog, a tablet
    /// map per shard, a segment, a manifest and a delete log. The call site
    /// written next is covered by it on the day it is written, which is the
    /// property the per-call-site tests kept failing to have.
    #[test]
    fn every_publication_fsyncs_the_directory_it_renamed_into() {
        let dir = tmp("published");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();

        durability_probe::start();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY) WITH (splits = ['m'])").unwrap();
        for id in ["a", "b", "y", "z"] {
            db.insert("notes", note(id)).unwrap();
        }
        db.flush("notes").unwrap();
        assert!(db.delete_key("notes", "a").unwrap(), "nothing was deleted");
        db.flush("notes").unwrap();
        db.persist().unwrap();
        let ev = durability_probe::take();

        // Proving nothing is how an invariant test fails, so first: the
        // workload really did publish one of each kind.
        let renamed = ev.paths(Op::Rename);
        let named = |what: &str| renamed.iter().any(|p| p.file_name().is_some_and(|n| n == what));
        let with_ext = |e: &str| renamed.iter().any(|p| p.extension().is_some_and(|x| x == e));
        for what in ["CATALOG", "RANGE", "MANIFEST"] {
            assert!(named(what), "{what} was never published, so this says nothing about it");
        }
        assert!(with_ext("seg"), "no segment was published: {renamed:?}");
        assert!(with_ext("dlog"), "no delete log was published: {renamed:?}");

        let unpublished = ev.unpublished_renames();
        assert!(
            unpublished.is_empty(),
            "renamed into place and then left in a directory nobody fsynced: the bytes are \
             durable and the NAME is not, so a crash takes the file back and whatever names it \
             -- a manifest, a catalog -- names nothing: {unpublished:?}"
        );

        // And the consequence the invariant is standing in for.
        drop(db);
        let re = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(re.shards("notes").unwrap().len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The catalog's half of "a publication that failed AFTER the rename is
    /// retried rather than believed". `Shard::persist_manifest` has a test for
    /// that and this call site had none, so a cache assigned a line too early
    /// was caught for MANIFEST and invisible for CATALOG: the same
    /// second-call-site weakness, one level up.
    ///
    /// Only an injected failure reaches the window, and the window is the whole
    /// point: the rename has already happened, so the bytes on disk ARE the
    /// bytes the cache holds and `still_published` agrees with it. A cache
    /// written before `atomic_write` returned therefore skips every later
    /// publication of a catalog whose name was never made durable -- for the
    /// life of the process, with every mutating statement in the shells
    /// acknowledged against it.
    #[test]
    fn a_catalog_publication_that_failed_after_the_rename_is_retried() {
        let dir = tmp("catalog-retry");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        let published = fs::read(dir.join("CATALOG")).unwrap();

        // A statement that changes the catalog and creates no directories, so
        // the arming lands on CATALOG's own publication rather than on the
        // directory chain a CREATE COLLECTION walks first.
        durability_probe::fail_next(Op::DirSync, &dir);
        let e = db.execute("CREATE INDEX notes_body ON notes USING fulltext (body)").unwrap_err();
        assert!(matches!(e, Error::Io(_)), "a directory fsync that failed was swallowed: {e}");
        assert_ne!(
            fs::read(dir.join("CATALOG")).unwrap(),
            published,
            "the rename happened: the bytes on disk are the ones a cache written too early \
             would be believed against"
        );

        durability_probe::start();
        db.persist().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.at(Op::Rename, &dir.join("CATALOG")).is_some(),
            "the catalog was believed published by a call that returned `Err`, so the name that \
             reaches those bytes is never made durable: {ev:?}"
        );
        assert!(
            ev.at(Op::DirSync, &dir).is_some(),
            "the catalog was republished and its directory was not synced: {ev:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The other half of the same change, and the reason the first half is
    /// affordable: an acknowledged statement that changed no published file
    /// writes nothing at all. Only the segment set and the catalog live in
    /// those files, and an insert changes neither.
    #[test]
    fn a_persist_after_a_statement_that_changed_no_published_file_writes_nothing() {
        let dir = tmp("persist-noop");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION notes (id TEXT PRIMARY KEY)").unwrap();
        db.insert("notes", note("a")).unwrap();
        db.persist().unwrap();

        durability_probe::start();
        db.insert("notes", note("b")).unwrap();
        db.persist().unwrap();
        let ev = durability_probe::take();
        assert!(
            ev.paths(Op::Rename).is_empty() && ev.paths(Op::DirSync).is_empty(),
            "the catalog and the manifest are byte for byte what they already were: {ev:?}"
        );

        // The documents are durable anyway -- that is the WAL sync's job, not
        // this one's -- so declining to rewrite the manifest loses nothing.
        drop(db);
        let re = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(re.shards("notes").unwrap()[0].num_docs(crate::time::MAX_TS), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    // ----------------------------------------------------------- walks (G1)

    /// The citation graph every walk test starts from. Nine papers and the
    /// edges between them, in an edge collection pointed at `papers` with an
    /// adjacency index over `(src, dst)`:
    ///
    ///   p1 -> p2, p3, p8(kind weak)     p2 -> p4, pX(dangling)
    ///   p3 -> p4, p5                    p4 -> p6
    ///   p5 -> p1 (back to the start)    p7 -> p1 (into the start)
    ///   p8 -> p9
    fn graph(dir: &Path) -> Db {
        let mut db = Db::open(dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION papers (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE INDEX papers_body ON papers USING fulltext (body) WITH (analyzer = 'standard')",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX papers_emb ON papers USING vector (embedding) WITH (dims = 2, metric = 'l2')",
        )
        .unwrap();
        db.execute(
            "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) \
             WITH (nodes_of = 'papers')",
        )
        .unwrap();
        db.execute("CREATE INDEX cites_adj ON cites USING adjacency (src, dst)").unwrap();
        let bodies = [
            ("p1", "graph search"),
            ("p2", "graph index"),
            ("p3", "vector index"),
            ("p4", "graph vector"),
            ("p5", "segment"),
            ("p6", "graph"),
            ("p7", "graph"),
            ("p8", "graph"),
            ("p9", "graph"),
        ];
        for (i, (id, body)) in bodies.iter().enumerate() {
            db.execute(&format!(
                r#"INSERT INTO papers VALUES ('{{"id":"{id}","body":"{body}","embedding":[{i}.0, 0.0]}}')"#
            ))
            .unwrap();
        }
        let edges = [
            ("e01", "p1", "p2", "cites"),
            ("e02", "p1", "p3", "cites"),
            ("e03", "p1", "p8", "weak"),
            ("e04", "p2", "p4", "cites"),
            ("e05", "p2", "pX", "cites"),
            ("e06", "p3", "p4", "cites"),
            ("e07", "p3", "p5", "cites"),
            ("e08", "p4", "p6", "cites"),
            ("e09", "p5", "p1", "cites"),
            ("e10", "p7", "p1", "cites"),
            ("e11", "p8", "p9", "cites"),
        ];
        for (id, src, dst, kind) in edges {
            db.execute(&format!(
                r#"INSERT INTO cites VALUES ('{{"id":"{id}","src":"{src}","dst":"{dst}","kind":"{kind}"}}')"#
            ))
            .unwrap();
        }
        // Sealed, so a walk probes the adjacency region; the memtable path
        // is exercised by what each test inserts afterwards and by
        // `a_hop_probes_the_region_and_scans_only_units_sealed_before_it`.
        db.execute("FLUSH papers").unwrap();
        db.execute("FLUSH cites").unwrap();
        db
    }

    /// A segment sealed with the adjacency index is probed; a memtable, and
    /// a segment sealed before the index was declared, is scanned -- the
    /// plan counts those units per hop -- and a compaction rewrites the old
    /// segment with the region. The answer is the same on every path.
    #[test]
    fn a_hop_probes_the_region_and_scans_only_units_sealed_before_it() {
        let dir = tmp("hops-probe");
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        db.execute("CREATE COLLECTION papers (id TEXT PRIMARY KEY)").unwrap();
        db.execute(
            "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) \
             WITH (nodes_of = 'papers')",
        )
        .unwrap();
        for p in ["p1", "p2", "p3", "p4", "p5"] {
            db.execute(&format!(r#"INSERT INTO papers VALUES ('{{"id":"{p}"}}')"#)).unwrap();
        }
        // Sealed before the index exists: no region, scanned.
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e1","src":"p1","dst":"p2"}')"#).unwrap();
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e2","src":"p1","dst":"p3"}')"#).unwrap();
        db.execute("FLUSH cites").unwrap();
        db.execute("CREATE INDEX cites_adj ON cites USING adjacency (src, dst)").unwrap();
        // Sealed after: probed.
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e3","src":"p2","dst":"p4"}')"#).unwrap();
        db.execute("FLUSH cites").unwrap();
        // Still in the memtable: scanned.
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e4","src":"p3","dst":"p5"}')"#).unwrap();
        let sql = "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites LIMIT 100";
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p4", "p5"]);
        let plan = plan_of(&mut db, sql);
        assert!(
            plan.contains(
                "hop 1: 1 key(s) expanded over 2 edge(s): 2 new, 0 dangling, frontier 2 (expand"
            ),
            "{plan}"
        );
        assert!(
            plan.contains("2 unit(s) scanned: no adjacency region"),
            "the old segment and the memtable:\n{plan}"
        );
        assert_eq!(plan.matches("unit(s) scanned").count(), 2, "at both hops:\n{plan}");
        // A reverse walk probes the other column's map of the same region.
        assert_eq!(key_set(&db.query("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p4' VIA cites REVERSE LIMIT 10").unwrap()), ["p2"]);
        // An edge deleted after its segment was sealed is hidden from the
        // probe by the same visibility the scan applies.
        assert!(db.delete_key("cites", "e3").unwrap());
        assert_eq!(
            key_set(&db.query(sql).unwrap()),
            ["p2", "p3", "p5"],
            "p4 was reached only through e3"
        );
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e5","src":"p2","dst":"p4"}')"#).unwrap();
        // Sealing the memtable leaves the old segment as the only scan;
        // compacting -- four level-0 segments, the tier fan-out -- rewrites
        // it with the region, and nothing is scanned.
        db.execute("FLUSH cites").unwrap();
        db.execute(r#"INSERT INTO cites VALUES ('{"id":"e6","src":"p5","dst":"p1"}')"#).unwrap();
        db.execute("FLUSH cites").unwrap();
        let plan = plan_of(&mut db, sql);
        assert!(plan.contains("1 unit(s) scanned: no adjacency region"), "{plan}");
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p4", "p5"]);
        db.execute("COMPACT cites").unwrap();
        let plan = plan_of(&mut db, sql);
        assert!(
            !plan.contains("unit(s) scanned"),
            "every unit has the region after the rewrite:\n{plan}"
        );
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p4", "p5"]);
        // The edge filter still applies on the probed path.
        assert_eq!(
            key_set(&db.query("SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites WHERE id <> 'e2' LIMIT 100").unwrap()),
            ["p2", "p4"]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    fn key_set(r: &QueryResult) -> Vec<String> {
        let mut v: Vec<String> = r.rows.iter().map(|r| r.key.clone()).collect();
        v.sort();
        v
    }

    fn plan_of(db: &mut Db, sql: &str) -> String {
        match db.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
            Outcome::Explain(t) => t,
            other => panic!("{other:?}"),
        }
    }

    /// `WITHIN k HOPS OF` is the neighbourhood: every node in 1..k hops, the
    /// start excluded, whichever way the graph loops back; the edge filter
    /// applies at every hop; `REVERSE` follows the index backwards; `OR id =
    /// 'x'` puts the start back and `NOT` takes the complement. Fused with a
    /// text match and a distance it is one plan, and the plan shows every
    /// hop. What cannot walk is refused naming why.
    #[test]
    fn a_hop_filter_selects_the_neighbourhood_and_nothing_else() {
        let dir = tmp("hops");
        let mut db = graph(&dir);
        let hop = |db: &mut Db, clause: &str| {
            key_set(&db.query(&format!("SELECT id FROM papers WHERE {clause} LIMIT 100")).unwrap())
        };
        assert_eq!(hop(&mut db, "id WITHIN 1 HOP OF 'p1' VIA cites"), ["p2", "p3", "p8"]);
        assert_eq!(
            hop(&mut db, "id WITHIN 2 HOPS OF 'p1' VIA cites"),
            ["p2", "p3", "p4", "p5", "p8", "p9"],
            "the union over both hops; pX is dangling and p1 is the start"
        );
        assert_eq!(
            hop(&mut db, "id WITHIN 3 HOPS OF 'p1' VIA cites"),
            ["p2", "p3", "p4", "p5", "p6", "p8", "p9"],
            "p5 -> p1 loops back to the start, which stays excluded"
        );
        assert_eq!(
            hop(&mut db, "id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'cites'"),
            ["p2", "p3", "p4", "p5"],
            "the weak edge is filtered at hop 1, so p8 and p9 are never reached"
        );
        assert_eq!(hop(&mut db, "id WITHIN 1 HOP OF 'p1' VIA cites REVERSE"), ["p5", "p7"]);
        assert_eq!(
            hop(&mut db, "id WITHIN 1 HOP OF 'p1' VIA cites OR id = 'p1'"),
            ["p1", "p2", "p3", "p8"]
        );
        assert_eq!(
            hop(&mut db, "NOT (id WITHIN 1 HOP OF 'p1' VIA cites)"),
            ["p1", "p4", "p5", "p6", "p7", "p9"]
        );
        assert_eq!(hop(&mut db, "id WITHIN 2 HOPS OF 'p9' VIA cites"), Vec::<String>::new());
        assert_eq!(hop(&mut db, "id WITHIN 2 HOPS OF 'nobody' VIA cites"), Vec::<String>::new());

        // One plan: the walk, the text filter and the distance order.
        let fused = "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites \
                     AND text_match(body, 'graph') ORDER BY embedding <-> [9.0, 0.0] LIMIT 2";
        let r = db.query(fused).unwrap();
        let keys: Vec<&str> = r.rows.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(
            keys,
            ["p9", "p8"],
            "nearest first, within the neighbourhood, matching the text"
        );
        assert!(r.cut_walks.is_empty() && r.missing.is_empty());
        let plan = plan_of(&mut db, fused);
        assert!(
            plan.contains("walk: WITHIN 2 HOPS OF 'p1' VIA cites (index cites_adj, outgoing)"),
            "{plan}"
        );
        assert!(
            plan.contains("hop 1: 1 key(s) expanded over 3 edge(s): 3 new, 0 dangling, frontier 3"),
            "{plan}"
        );
        assert!(
            plan.contains("hop 2: 3 key(s) expanded over 5 edge(s): 4 new, 1 dangling, frontier 3"),
            "{plan}"
        );
        assert!(plan.contains("6 key(s) in 1..2 hop(s)"), "{plan}");
        assert!(plan.contains("id IN ["), "the units saw an IN:\n{plan}");
        let hybrid = "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites ORDER BY \
                      hybrid(text_match(body, 'graph index'), embedding <-> [2.0, 0.0], method => 'linear') LIMIT 3";
        let r = db.query(hybrid).unwrap();
        for row in &r.rows {
            assert!(
                ["p2", "p3", "p4", "p5", "p8", "p9"].contains(&row.key.as_str()),
                "{}",
                row.key
            );
        }
        assert_eq!(r.rows[0].key, "p2", "the best fused candidate inside the neighbourhood");

        // Refusals, each naming what is missing.
        for (sql, why) in [
            ("SELECT id FROM papers WHERE body WITHIN 1 HOP OF 'p1' VIA cites", "not the primary key"),
            ("SELECT id FROM papers WHERE id WITHIN 0 HOPS OF 'p1' VIA cites", "at least one hop"),
            ("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA nothing", "no collection `nothing`"),
            ("SELECT id FROM cites WHERE id WITHIN 1 HOP OF 'e01' VIA cites", "is an edge collection of `papers`"),
            ("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA papers", "not an edge collection"),
            (
                "SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA cites WHERE text_match(kind, 'x')",
                "edge filter on `cites` is structured",
            ),
        ] {
            let e = db.query(sql).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
        db.execute(
            "CREATE COLLECTION links (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) \
             WITH (nodes_of = 'papers')",
        )
        .unwrap();
        let e = db
            .query("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA links")
            .unwrap_err()
            .to_string();
        assert!(e.contains("no adjacency index on `links`"), "{e}");
        for (sql, why) in [
            (
                "CREATE INDEX links_adj ON links USING adjacency (src, weight)",
                "not a declared column",
            ),
            ("CREATE INDEX links_adj ON links USING adjacency (src, src)", "both are `src`"),
            (
                "CREATE INDEX papers_adj ON papers USING adjacency (src, dst)",
                "not an edge collection",
            ),
            (
                "CREATE INDEX cites_adj2 ON cites USING adjacency (dst, src)",
                "already has an adjacency index",
            ),
            (
                "CREATE COLLECTION loops (id TEXT PRIMARY KEY) WITH (nodes_of = 'loops')",
                "its own node collection",
            ),
            (
                "CREATE COLLECTION meta (id TEXT PRIMARY KEY) WITH (nodes_of = 'cites')",
                "itself an edge collection",
            ),
            (
                "CREATE COLLECTION orphan (id TEXT PRIMARY KEY) WITH (nodes_of = 'nowhere')",
                "does not exist",
            ),
        ] {
            let e = db.execute(sql).unwrap_err().to_string();
            assert!(e.contains(why), "{sql}: {e}");
        }
        // A collection loaded before it was an edge collection can become one.
        db.execute(
            "CREATE COLLECTION later (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL)",
        )
        .unwrap();
        db.execute("ALTER COLLECTION later SET (nodes_of = 'papers')").unwrap();
        db.execute("CREATE INDEX later_adj ON later USING adjacency (src, dst)").unwrap();
        db.execute(r#"INSERT INTO later VALUES ('{"id":"l1","src":"p9","dst":"p1"}')"#).unwrap();
        assert_eq!(hop(&mut db, "id WITHIN 1 HOP OF 'p9' VIA later"), ["p1"]);
        // And all of it survives a reopen.
        drop(db);
        let mut db = Db::open(&dir, DbOpts::default()).unwrap();
        assert_eq!(
            hop(&mut db, "id WITHIN 2 HOPS OF 'p1' VIA cites"),
            ["p2", "p3", "p4", "p5", "p8", "p9"]
        );
        assert_eq!(hop(&mut db, "id WITHIN 1 HOP OF 'p9' VIA later"), ["p1"]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The same graph laid out over one, three and six shards of each
    /// collection -- with a flush half way so both memtables and segments
    /// are in play -- answers a hop statement, fused with text and a vector,
    /// bit for bit the same. The walk is a function of the live graph at the
    /// instant, not of where its edges happen to be.
    /// `hops(...)` inside `hybrid(...)` ranks by the hop a node was first
    /// reached at, nearer first, beside the other sources under either
    /// method; alone it is refused, since alone it is the filter. `THEN
    /// WHERE` gives each hop its own edge filter, the plan says which, and
    /// a count that fits neither one-for-all nor one-per-hop is refused.
    #[test]
    fn a_hop_source_ranks_nearer_nodes_higher_and_per_hop_filters_apply_in_order() {
        let dir = tmp("hop-source");
        let mut db = graph(&dir);
        // From p1: hop 1 = p2 p3 p8, hop 2 = p4 p5 p9, hop 3 = p6. Text
        // 'graph' matches p1 p2 p4 p6 p7 p8 p9 -- p6 p7 p8 p9 with one and
        // the same score (the body is the one word), p2 and p4 with another
        // (two words each). Where the text says the same, the hop decides.
        for method in ["rrf", "linear"] {
            let sql = format!(
                "SELECT id FROM papers ORDER BY hybrid(text_match(body, 'graph'), \
                 hops(id WITHIN 3 HOPS OF 'p1' VIA cites), method => '{method}') LIMIT 10"
            );
            let r = db.query(&sql).unwrap();
            let keys: Vec<&str> = r.rows.iter().map(|r| r.key.as_str()).collect();
            let pos = |k: &str| keys.iter().position(|x| *x == k).unwrap_or(usize::MAX);
            // Under either method a hop-1 node beats the same text score at
            // hop 2, 3 or unreached. Under linear fusion, where equal text
            // scores normalise equal, the hop alone orders the rest; under
            // RRF a text tie is broken by key before the ranks fuse, so p6's
            // better text rank can lift it over p9, and only the first place
            // is the hop's.
            assert!(
                pos("p8") < pos("p9") && pos("p8") < pos("p6") && pos("p8") < pos("p7"),
                "{method}: hop 1 first among equal text scores: {keys:?}"
            );
            assert!(pos("p2") < pos("p4"), "{method}: hop 1 before hop 2 at equal text: {keys:?}");
            if method == "linear" {
                assert!(
                    pos("p9") < pos("p6") && pos("p6") < pos("p7"),
                    "linear: hop 2, hop 3, unreached: {keys:?}"
                );
            }
            assert!(
                keys.contains(&"p3"),
                "{method}: reached but no text match is still a candidate"
            );
            assert!(
                keys.contains(&"p7"),
                "{method}: text match but never reached is still a candidate"
            );
            assert!(keys.contains(&"p1"), "{method}: the start, matching the text, is a candidate of the text source: {keys:?}");
            assert!(
                keys.contains(&"p5"),
                "{method}: reached at hop 2 with no text match: {keys:?}"
            );
            let plan = plan_of(&mut db, &sql);
            assert!(plan.contains("walk: hops(WITHIN 3 HOPS OF 'p1' VIA cites)"), "{plan}");
            assert!(plan.contains("sources=[\"text(body)\", \"hops(cites)\"]"), "{plan}");
        }
        let alone = db.query(
            "SELECT id FROM papers ORDER BY hybrid(hops(id WITHIN 2 HOPS OF 'p1' VIA cites)) LIMIT 5",
        );
        assert!(alone.unwrap_err().to_string().contains("ranks beside another source"));

        let hop = |db: &mut Db, clause: &str| {
            key_set(&db.query(&format!("SELECT id FROM papers WHERE {clause} LIMIT 100")).unwrap())
        };
        assert_eq!(
            hop(
                &mut db,
                "id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'weak' THEN WHERE kind = 'cites'"
            ),
            ["p8", "p9"],
            "hop 1 follows the weak edge only, hop 2 the strong ones from there"
        );
        assert_eq!(
            hop(
                &mut db,
                "id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'cites' THEN WHERE kind = 'weak'"
            ),
            ["p2", "p3"],
            "no weak edge leaves hop 1's frontier"
        );
        let plan = plan_of(
            &mut db,
            "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'weak' \
             THEN WHERE kind = 'cites' LIMIT 100",
        );
        assert!(plan.contains("edge filter 1 of 2"), "{plan}");
        assert!(plan.contains("edge filter 2 of 2"), "{plan}");
        let mismatch = db.query(
            "SELECT id FROM papers WHERE id WITHIN 3 HOPS OF 'p1' VIA cites WHERE kind = 'a' \
             THEN WHERE kind = 'b' LIMIT 5",
        );
        let msg = mismatch.unwrap_err().to_string();
        assert!(msg.contains("3 hop(s) and 2 edge filters"), "{msg}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_hop_statement_is_bit_identical_across_shard_counts() {
        let layouts: [(&str, &str); 3] = [
            ("", ""),
            (" WITH (splits = ['n020', 'n040'])", " WITH (nodes_of = 'nodes', splits = ['e0100', 'e0200'])"),
            (
                " WITH (splits = ['n010', 'n020', 'n030', 'n040', 'n050'])",
                " WITH (nodes_of = 'nodes', splits = ['e0050', 'e0100', 'e0150', 'e0200', 'e0250'])",
            ),
        ];
        let words = ["graph", "search", "vector", "index", "segment", "fusion", "rank"];
        let statements = [
            "SELECT id FROM nodes WHERE id WITHIN 2 HOPS OF 'n007' VIA edges AND text_match(body, 'graph') \
             ORDER BY embedding <=> [0.5, 0.5, 0.5, 1.0] LIMIT 10 WITH (exact)",
            "SELECT id FROM nodes WHERE id WITHIN 3 HOPS OF 'n001' VIA edges WHERE w > 2 ORDER BY \
             hybrid(text_match(body, 'vector index'), embedding <=> [0.2, 0.2, 0.9, 1.0], method => 'linear') \
             LIMIT 8 WITH (exact)",
            "SELECT id FROM nodes WHERE id WITHIN 2 HOPS OF 'n030' VIA edges REVERSE LIMIT 100",
            "SELECT id FROM nodes WHERE id WITHIN 2 HOPS OF 'n003' VIA edges LIMIT 100 WITH (max_frontier = 7, max_fanout = 3)",
            "SELECT id FROM nodes ORDER BY hybrid(text_match(body, 'graph search'), \
             hops(id WITHIN 3 HOPS OF 'n005' VIA edges WHERE w > 1)) LIMIT 12 WITH (exact)",
            "SELECT id FROM nodes WHERE id WITHIN 3 HOPS OF 'n011' VIA edges WHERE w > 3 THEN WHERE w < 2 \
             THEN WHERE w = 2 ORDER BY hybrid(embedding <=> [0.9, 0.1, 0.1, 1.0], \
             hops(id WITHIN 2 HOPS OF 'n011' VIA edges REVERSE), method => 'linear') LIMIT 10 WITH (exact)",
        ];
        let mut answers: Vec<Vec<String>> = Vec::new();
        for (li, (nsplit, esplit)) in layouts.iter().enumerate() {
            let dir = tmp(&format!("hops-layout-{li}"));
            let mut db = Db::open(&dir, DbOpts::default()).unwrap();
            db.execute(&format!("CREATE COLLECTION nodes (id TEXT PRIMARY KEY){nsplit}")).unwrap();
            db.execute("CREATE INDEX nodes_body ON nodes USING fulltext (body) WITH (analyzer = 'english')").unwrap();
            db.execute("CREATE INDEX nodes_emb ON nodes USING vector (embedding) WITH (dims = 4, metric = 'cosine')").unwrap();
            let esplit = if esplit.is_empty() { " WITH (nodes_of = 'nodes')" } else { esplit };
            db.execute(&format!(
                "CREATE COLLECTION edges (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL){esplit}"
            ))
            .unwrap();
            db.execute("CREATE INDEX edges_adj ON edges USING adjacency (src, dst)").unwrap();
            for i in 0..60usize {
                if i == 40 {
                    db.execute("FLUSH nodes").unwrap();
                    db.execute("FLUSH edges").unwrap();
                }
                let body =
                    format!("{} {} {}", words[i % 7], words[(i * 3) % 7], words[(i * 5) % 7]);
                db.execute(&format!(
                    r#"INSERT INTO nodes VALUES ('{{"id":"n{i:03}","body":"{body}","embedding":[{},{},{},1.0]}}')"#,
                    (i % 7) as f32 / 7.0,
                    (i % 5) as f32 / 5.0,
                    (i % 3) as f32 / 3.0,
                ))
                .unwrap();
                // Five edges each, forward by 1, 3, 7, 11 and 19, wrapping.
                for (j, step) in [1usize, 3, 7, 11, 19].iter().enumerate() {
                    let dst = (i + step) % 60;
                    db.execute(&format!(
                        r#"INSERT INTO edges VALUES ('{{"id":"e{:04}","src":"n{i:03}","dst":"n{dst:03}","w":{}}}')"#,
                        i * 5 + j,
                        (i + j) % 5
                    ))
                    .unwrap();
                }
            }
            db.execute(
                r#"INSERT INTO edges VALUES ('{"id":"e9999","src":"n007","dst":"gone","w":4}')"#,
            )
            .unwrap();
            db.delete_key("nodes", "n008").unwrap();
            let mut got = Vec::new();
            for sql in statements {
                let r = db.query(sql).unwrap();
                assert!(!r.rows.is_empty(), "{sql}");
                got.push(format!(
                    "{:?} cut={:?}",
                    r.rows
                        .iter()
                        .map(|row| (
                            row.key.clone(),
                            row.score.map(f32::to_bits),
                            row.distance.map(f32::to_bits)
                        ))
                        .collect::<Vec<_>>(),
                    r.cut_walks
                ));
            }
            let plan = plan_of(&mut db, statements[0]);
            // n007 -> n008 (deleted) and n007 -> gone (never a node).
            assert!(
                plan.contains("6 new, 2 dangling, frontier 4"),
                "layout {li}: both dangling edges are counted:\n{plan}"
            );
            answers.push(got);
            let _ = fs::remove_dir_all(&dir);
        }
        for (li, a) in answers.iter().enumerate().skip(1) {
            assert_eq!(a, &answers[0], "layout {li} answered differently from one shard");
        }
    }

    /// A cap that binds says so on the response, in the plan, and per hop:
    /// which cap, at which hop, how much it kept. What it keeps is the
    /// lexicographically first, so a cut answer is the same cut answer at
    /// every layout. A statement no cap bound carries no such line.
    #[test]
    fn a_cut_walk_says_which_cap_bound_it() {
        let dir = tmp("hops-cut");
        let mut db = graph(&dir);
        // A hub: p1 gains twenty more targets.
        for i in 0..20 {
            db.execute(&format!(r#"INSERT INTO papers VALUES ('{{"id":"q{i:02}","body":"graph","embedding":[0.0,{i}.0]}}')"#)).unwrap();
            db.execute(&format!(r#"INSERT INTO cites VALUES ('{{"id":"h{i:02}","src":"p1","dst":"q{i:02}","kind":"cites"}}')"#)).unwrap();
        }
        let r = db
            .query("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA cites LIMIT 100")
            .unwrap();
        assert_eq!(r.rows.len(), 23);
        assert!(r.cut_walks.is_empty(), "nothing bound: {:?}", r.cut_walks);

        let r = db
            .query("SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA cites LIMIT 100 WITH (max_fanout = 4)")
            .unwrap();
        assert_eq!(key_set(&r), ["p2", "p3", "p8", "q00"], "the first four by key");
        assert_eq!(r.cut_walks.len(), 1, "{:?}", r.cut_walks);
        assert_eq!(
            r.cut_walks[0],
            "WITHIN 1 HOPS OF 'p1' VIA cites was cut at hop 1: max_fanout = 4 bound 1 node(s), \
             whose remaining edges were not followed"
        );
        let plan = plan_of(&mut db, "SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA cites LIMIT 100 WITH (max_fanout = 4)");
        assert!(plan.contains("frontier 4; CUT: max_fanout = 4 bound 1 node(s)"), "{plan}");
        assert!(plan.contains("note: WITHIN 1 HOPS OF 'p1' VIA cites was cut at hop 1"), "{plan}");

        let r = db
            .query("SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites LIMIT 100 WITH (max_frontier = 2)")
            .unwrap();
        // Hop 1 keeps p2 and p3 of 23; hop 2 from them finds p4, p5 and pX
        // and keeps the first two. The cap is applied to what the hop found,
        // before the liveness check, so the work it bounds is bounded; pX
        // was cut, not found dangling.
        assert_eq!(key_set(&r), ["p2", "p3", "p4", "p5"]);
        assert_eq!(
            r.cut_walks,
            [
                "WITHIN 2 HOPS OF 'p1' VIA cites was cut at hop 1: max_frontier = 2 kept 2 of 23 keys",
                "WITHIN 2 HOPS OF 'p1' VIA cites was cut at hop 2: max_frontier = 2 kept 2 of 3 keys",
            ]
        );
        let plan = plan_of(&mut db, "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites LIMIT 100 WITH (max_frontier = 2)");
        assert!(plan.contains("hop 1: 1 key(s) expanded over 23 edge(s): 23 new, 0 dangling, frontier 2; CUT: max_frontier = 2 kept 2 of 23"), "{plan}");
        assert!(plan.contains("hop 2: 2 key(s) expanded over 4 edge(s): 3 new, 0 dangling, frontier 2; CUT: max_frontier = 2 kept 2 of 3"), "{plan}");
        // The console and any client see the same line the rows came with.
        let text = crate::serve::rows_json(&r, 0);
        assert!(
            text.contains(r#""cut_walks":["WITHIN 2 HOPS OF 'p1' VIA cites was cut at hop 1"#),
            "{text}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// An adjacency index is tiered like any index, and a walk that finds it
    /// below `cached` is refused naming the tier and the index, rather than
    /// paying a chain of archive fault-ins per hop. Raising the tier makes
    /// the same statement answer again.
    #[test]
    fn a_walk_over_a_cold_adjacency_index_is_refused_naming_the_tier() {
        let dir = tmp("hops-cold");
        let mut db = graph(&dir);
        db.execute("FLUSH cites").unwrap();
        let sql = "SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p1' VIA cites LIMIT 100";
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p8"]);
        db.execute("ALTER INDEX cites_adj ON cites SET TIER 'archived'").unwrap();
        let e = db.query(sql).unwrap_err().to_string();
        assert!(
            e.contains("archived") && e.contains("cites_adj") && e.contains("SET TIER 'cached'"),
            "{e}"
        );
        db.execute("ALTER INDEX cites_adj ON cites SET TIER 'cached'").unwrap();
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p8"]);
        // `minimal` and `active` are warmer than `cached`, so they walk too.
        db.execute("ALTER INDEX cites_adj ON cites SET TIER 'minimal'").unwrap();
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p8"]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// An edge to a node that is deleted at the statement's instant, or that
    /// never existed, is followed and resolves to nothing: the key is not in
    /// the answer, nothing beyond it is walked, and the plan counts it at
    /// the hop that found it. A read at an instant before the delete still
    /// walks through the node, because MVCC hides it only after.
    #[test]
    fn a_dangling_edge_is_skipped_and_counted() {
        let dir = tmp("hops-dangling");
        let mut db = graph(&dir);
        let sql = "SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites WHERE kind = 'cites' LIMIT 100";
        assert_eq!(key_set(&db.query(sql).unwrap()), ["p2", "p3", "p4", "p5"]);
        let plan = plan_of(&mut db, sql);
        assert!(
            plan.contains("hop 1: 1 key(s) expanded over 2 edge(s): 2 new, 0 dangling, frontier 2"),
            "{plan}"
        );
        assert!(
            plan.contains("hop 2: 2 key(s) expanded over 4 edge(s): 3 new, 1 dangling, frontier 2"),
            "{plan}"
        );
        db.delete_key("papers", "p3").unwrap();
        assert_eq!(
            key_set(&db.query(sql).unwrap()),
            ["p2", "p4"],
            "p3 is gone, and p5 -- reachable only through it -- with it"
        );
        let plan = plan_of(&mut db, sql);
        assert!(
            plan.contains("hop 1: 1 key(s) expanded over 2 edge(s): 2 new, 1 dangling, frontier 1"),
            "{plan}"
        );
        assert!(
            plan.contains("hop 2: 1 key(s) expanded over 2 edge(s): 2 new, 1 dangling, frontier 1"),
            "{plan}"
        );
        assert!(plan.contains("2 key(s) in 1..2 hop(s)"), "{plan}");
        // The edge rows themselves are untouched: the walk hides the node,
        // not the edge.
        assert_eq!(
            db.query("SELECT id FROM cites WHERE src = 'p3' LIMIT 10").unwrap().rows.len(),
            2
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
