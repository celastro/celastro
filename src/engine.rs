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
use std::sync::Arc;

use crate::catalog::{Catalog, Collection, ColumnDef, IndexDef, IndexKind};
use crate::compaction::{self, CompactionOpts};
use crate::error::{Error, Result};
use crate::lifecycle::{self, IndexActivity, LifecyclePolicy};
use crate::memtable::{FlushThresholds, MemtableBudget};
use crate::plan::exec::{self, ExecInput, QueryResult};
use crate::residency::{Placement, ResidencyManager, ResidencyOpts, Tier};
use crate::segment::BuildOpts;
use crate::shard::{sort_key, Shard, ShardOpts};
use crate::sql::{self, ast::*};
use crate::text::scorer::GlobalStats;
use crate::time::{Hlc, Timestamp};
use crate::value::Value;

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

/// How coarsely the per-index access clocks are written to disk. The lifecycle
/// DSL's finest unit is a minute, so persisting to within a minute is exact at
/// the resolution anyone can express — and it keeps a read from becoming a
/// catalog write.
const ACTIVITY_PERSIST_MICROS: u64 = 60_000_000;

#[derive(Debug, Clone)]
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
    /// Who this node is and which nodes share its tablets. Only the `minimal`
    /// tier consults it, and only to decide whether this node is the one
    /// keeping a given index decoded.
    pub placement: Placement,
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
        }
    }
}

/// The periodically refreshed global statistics of §8.2. All three numbers —
/// `num_docs`, `total_doc_len` and `doc_freq` — are masked sums at one
/// instant: exactly the triple [`Shard::term_stats`] answers on the exact
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
/// Every fill rewrites the two globals from the same [`Shard::term_stats`]
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
/// Two approximations are *not* addressed here, and both are shared with
/// `WITH (exact_scoring)` rather than particular to this cache. Prefix
/// expansion is not covered: `TextQuery::leaf_terms` deliberately skips it, so
/// an expanded term is never asked for on either path and `TextScorer::compile`
/// falls back to the segment's own physical dictionary count. And `avgdl` is
/// diluted by documents that carry no text on the path, which
/// [`Shard::term_stats`] documents where the dilution happens. So what this
/// buys is parity with `WITH (exact_scoring)` for Term and Phrase queries, not
/// blanket invariance.
///
/// A note on the cadence, now that staleness is the only residual and the
/// cadence is the only knob left: `refreshed_at_writes` is compared against
/// `Db::writes`, which counts writes to the whole ENGINE rather than to this
/// collection, so traffic on an unrelated collection ages this entry. The
/// load-bearing property is that the counter is shard-count independent, not
/// that it is per collection: an engine-wide counter only makes an epoch end
/// sooner, and it ends sooner by the same amount at every shard count. But it
/// does mean the interval is an upper bound on how fresh these numbers can be,
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

#[derive(Debug)]
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
    /// `Db::writes` at the last epoch reset. The refresh gate.
    refreshed_at_writes: u64,
    /// `Db::writes` when the numbers below were measured, meaningful only when
    /// `anchored`. Equal to `Db::writes` means no insert and no delete has
    /// landed since, in any collection, so the live corpus has not moved and a
    /// further gather measures the same one.
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
}

impl Outcome {
    pub fn rows(self) -> Result<QueryResult> {
        match self {
            Outcome::Rows(r) => Ok(r),
            Outcome::Ack(m) => Err(Error::Plan(format!("statement returned no rows: {m}"))),
            Outcome::Explain(_) => Err(Error::Plan("statement returned a plan, not rows".into())),
            Outcome::Recall(_) => Err(Error::Plan("statement returned a recall report".into())),
        }
    }
}

pub struct Db {
    pub catalog: Catalog,
    shards: BTreeMap<String, Vec<Shard>>,
    pub clock: Arc<Hlc>,
    pub opts: DbOpts,
    dir: Option<PathBuf>,
    budget: Arc<MemtableBudget>,
    residency: Arc<ResidencyManager>,
    stats: BTreeMap<String, CachedStats>,
    /// Inferred path statistics as of the last reopen, per collection.
    ///
    /// A shard accumulates statistics for the documents *it* observes, and
    /// `absorb_shard_catalogs` sums the shards. On reopen each shard is handed a
    /// copy of the catalog's collection, which already holds the aggregate — so
    /// summing three shards trebles it, and the next persist writes the trebled
    /// number down to be trebled again. The shards' copies are cleared at open
    /// and this holds what they no longer carry.
    stats_baseline: BTreeMap<String, (u64, BTreeMap<String, crate::catalog::PathStats>)>,
    writes: u64,
    lifecycle_checked_at_writes: u64,
    activity_persisted_micros: u64,
    query_log: Vec<LoggedVectorQuery>,
    queries_seen: u64,
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
            clock: Arc::new(Hlc::new()),
            opts,
            dir: None,
            budget,
            residency,
            stats: BTreeMap::new(),
            stats_baseline: BTreeMap::new(),
            writes: 0,
            lifecycle_checked_at_writes: 0,
            activity_persisted_micros: 0,
            query_log: Vec::new(),
            queries_seen: 0,
            last_commit: 0,
        }
    }

    /// Open a database rooted at `dir`, installing the catalog and every
    /// shard's manifest, then replaying each WAL.
    pub fn open(dir: &Path, opts: DbOpts) -> Result<Db> {
        opts.placement.validate()?;
        let mut db = Db::with_opts(opts);
        fs::create_dir_all(dir)?;
        db.dir = Some(dir.to_path_buf());
        if let Ok(b) = fs::read(dir.join("CATALOG")) {
            db.catalog = Catalog::decode(&b)?;
        }
        let names: Vec<String> = db.catalog.collections.keys().cloned().collect();
        for name in names {
            let mut coll = db.catalog.get(&name)?.clone();
            // What is already counted stays in the baseline; the shards start
            // from zero and count only what they see from here.
            db.stats_baseline
                .insert(name.clone(), (coll.doc_count, std::mem::take(&mut coll.paths)));
            coll.doc_count = 0;
            let cdir = dir.join("collections").join(&name);
            let mut shards = Vec::new();
            let mut i = 0usize;
            loop {
                let sdir = cdir.join(format!("shard-{i:04}"));
                if !sdir.exists() {
                    break;
                }
                let ranges = fs::read_to_string(sdir.join("RANGE")).unwrap_or_default();
                let mut sh = Shard::open(coll.clone(), db.clock.clone(), db.shard_opts(), &sdir)?;
                let parts: Vec<&str> = ranges.split('\n').collect();
                let lo = parts.first().filter(|s| !s.is_empty()).map(|s| s.to_string());
                let hi = parts.get(1).filter(|s| !s.is_empty()).map(|s| s.to_string());
                sh.key_range = Some((lo, hi));
                shards.push(sh);
                i += 1;
            }
            if !shards.is_empty() {
                db.shards.insert(name, shards);
            }
        }
        Ok(db)
    }

    fn shard_opts(&self) -> ShardOpts {
        ShardOpts {
            thresholds: self.opts.thresholds,
            build: self.opts.build,
            budget: Some(self.budget.clone()),
            gc_horizon: 0,
            residency: Some(self.residency.clone()),
            placement: self.opts.placement.clone(),
        }
    }

    /// Node-level residency accounting: what is decoded, what it cost, and how
    /// often a read had to fault a component back in.
    pub fn residency(&self) -> &Arc<ResidencyManager> {
        &self.residency
    }

    pub fn placement(&self) -> &Placement {
        &self.opts.placement
    }

    pub fn shards(&self, collection: &str) -> Result<&[Shard]> {
        self.shards
            .get(collection)
            .map(|v| v.as_slice())
            .ok_or_else(|| Error::Plan(format!("no such collection `{collection}`")))
    }

    pub fn memtable_budget(&self) -> &MemtableBudget {
        &self.budget
    }

    // ------------------------------------------------------------- control

    /// Create a collection. `splits` are the boundary keys of the tablet map:
    /// `n` split points make `n+1` shards, range-partitioned on the composite
    /// `(partition_key, primary_key)`.
    pub fn create_collection(&mut self, coll: Collection, splits: &[String]) -> Result<()> {
        let name = coll.name.clone();
        // The catalog entry has to go in first, so that a duplicate name is
        // refused before anything is written to disk — which means every
        // failure below has to take it back out. A collection that is in the
        // catalog but has no shards answers `already exists` to CREATE and
        // `no such collection` to every read and write, until a restart.
        self.catalog.create(coll.clone())?;
        let shards = match self.build_shards(&coll, splits) {
            Ok(s) => s,
            Err(e) => {
                self.catalog.collections.remove(&name);
                return Err(e);
            }
        };
        self.shards.insert(name, shards);
        self.persist_catalog()?;
        Ok(())
    }

    /// Build the shard set for a new collection: `n` split points make `n+1`
    /// shards. Separate from [`Db::create_collection`] so that a failure part
    /// way through has one place to unwind from.
    fn build_shards(&self, coll: &Collection, splits: &[String]) -> Result<Vec<Shard>> {
        let mut shards = Vec::with_capacity(splits.len() + 1);
        for i in 0..=splits.len() {
            let lo = if i == 0 { None } else { Some(splits[i - 1].clone()) };
            let hi = splits.get(i).cloned();
            let mut sh = Shard::new(coll.clone(), self.clock.clone(), self.shard_opts())
                .with_key_range(lo.clone(), hi.clone());
            if let Some(dir) = &self.dir {
                let sdir = dir.join("collections").join(&coll.name).join(format!("shard-{i:04}"));
                fs::create_dir_all(&sdir)?;
                fs::write(
                    sdir.join("RANGE"),
                    format!("{}\n{}", lo.unwrap_or_default(), hi.unwrap_or_default()),
                )?;
                sh.attach_dir(&sdir)?;
            }
            shards.push(sh);
        }
        Ok(shards)
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
        self.persist_catalog()?;
        Ok(())
    }

    fn persist_catalog(&self) -> Result<()> {
        if let Some(dir) = &self.dir {
            // Not `fs::write`: that truncates in place, so a crash partway
            // through leaves a catalog that will not decode and a database
            // that will not open, with every segment file intact.
            crate::shard::atomic_write(&dir.join("CATALOG"), &self.catalog.encode())?;
        }
        Ok(())
    }

    /// Sync the per-shard catalog copies with the control plane's, so that
    /// inferred path statistics accumulated on the write path are visible to
    /// planning.
    fn absorb_shard_catalogs(&mut self, collection: &str) -> Result<()> {
        let mut merged = self.catalog.get(collection)?.clone();
        if let Some(shards) = self.shards.get(collection) {
            merged.paths.clear();
            merged.doc_count = 0;
            if let Some((docs, paths)) = self.stats_baseline.get(collection) {
                merged.doc_count = *docs;
                for (p, st) in paths {
                    merged.paths.entry(p.clone()).or_default().merge(st);
                }
            }
            for s in shards {
                merged.doc_count += s.coll.doc_count;
                for (p, st) in &s.coll.paths {
                    merged.paths.entry(p.clone()).or_default().merge(st);
                }
            }
        }
        *self.catalog.get_mut(collection)? = merged;
        Ok(())
    }

    // ------------------------------------------------------------- writes

    pub fn insert(&mut self, collection: &str, doc: Value) -> Result<Timestamp> {
        let coll = self.catalog.get(collection)?;
        let key = sort_key(coll, &doc)?;
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no such collection `{collection}`")))?;
        let idx = shards
            .iter()
            .position(|s| s.owns(&key))
            .ok_or_else(|| Error::Plan(format!("no shard owns key `{key}`")))?;
        let ts = shards[idx].insert(doc)?;
        self.writes += 1;
        self.last_commit = self.last_commit.max(ts);
        self.maybe_run_lifecycle()?;
        Ok(ts)
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
        let shards = self
            .shards
            .get_mut(collection)
            .ok_or_else(|| Error::Plan(format!("no such collection `{collection}`")))?;
        for s in shards.iter_mut() {
            if s.owns(key) {
                if let Some(ts) = s.delete(key)? {
                    self.writes += 1;
                    self.last_commit = self.last_commit.max(ts);
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
    /// the refresh points [`STATS_REFRESH_WRITES`] sets, and only for terms no
    /// earlier query in this epoch has already paid for. The difference
    /// between the two arms is staleness — the cached triple is a set of live
    /// sums at ONE instant, at most [`STATS_REFRESH_WRITES`] writes behind
    /// this query, never a mixture of instants — plus one difference of
    /// spelling: for a term no unit holds the exact arm omits the entry and
    /// the cached arm stores an explicit `0`, which `GlobalStats::idf` reads
    /// identically. See [`CachedStats`] for why staleness alone does not put
    /// the shard count back into the answer.
    ///
    /// `ts` must be a timestamp this query pins, not one stored earlier — on
    /// the cached arm the triple gathered at it is written into epoch-lived
    /// state every later query in the epoch reads, so a historical `as_of`
    /// read would poison it for every one of them. A time-travel caller must
    /// pass `exact: true`, which writes nothing. The rule and the reason are
    /// on `fill_term_stats`.
    ///
    /// The term list per path is taken as a SET: a repeat is ignored rather
    /// than counted twice. See [`term_set`] for why that is a fix and not a
    /// convenience.
    ///
    /// # Panics
    ///
    /// With `exact: false`, `ts` must be at or above the last commit this
    /// engine took. That is a precondition, not a quality note, and a
    /// `debug_assert!` holds callers to it: a historical timestamp panics in a
    /// debug build. The distinction worth being explicit about, because an
    /// assertion that fires only in debug is otherwise the worst of both
    /// worlds, is WHOSE answer it spoils. This arm does not merely answer the
    /// caller who passed the old `ts` inaccurately — it writes the triple it
    /// gathered into epoch-lived state that every later query in the epoch
    /// reads, so one historical read mis-scores queries that asked for nothing
    /// of the kind and cannot tell. A wrong answer confined to the caller
    /// would be a documentation matter; one that escapes to other callers is a
    /// contract.
    ///
    /// `exact: true` has no such precondition and is the supported way to read
    /// the past: it writes nothing, and at a `ts` below
    /// [`Shard::retain_floor`] it is best-effort in exactly the sense
    /// [`Shard::term_stats`] documents, which a pinned `gc_horizon` makes
    /// exact again.
    pub fn gather_stats(
        &mut self,
        collection: &str,
        want: &BTreeMap<String, Vec<String>>,
        ts: Timestamp,
        exact: bool,
    ) -> Result<BTreeMap<String, GlobalStats>> {
        let mut out = BTreeMap::new();
        for (path, terms) in want {
            // Once, here, for both arms: the shard gather walks a posting list
            // per element of the slice it is handed, so a term named twice is
            // counted twice. [`term_set`] has the consequences.
            let terms = term_set(terms);
            let terms: &[String] = &terms;
            if exact {
                let mut num_docs = 0u64;
                let mut total_len = 0u64;
                let mut df: BTreeMap<String, u64> = BTreeMap::new();
                for s in self.shards(collection)? {
                    let (n, tl, d) = s.term_stats(path, terms, ts)?;
                    num_docs += n;
                    total_len += tl;
                    for (t, c) in d {
                        *df.entry(t).or_insert(0) += c;
                    }
                }
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
                // Reset first, then fill: a refresh point starts a new epoch
                // by emptying the entry, and filling into an entry that is
                // about to be emptied would pay for a masked walk and discard
                // it.
                self.reset_stats_if_stale(collection, path);
                let fresh = self.fill_term_stats(collection, path, terms, ts)?;
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
                    None => match self.stats.get(&key) {
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
                        exact: false,
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
    fn reset_stats_if_stale(&mut self, collection: &str, path: &str) {
        let key = cache_key(collection, path);
        let stale = match self.stats.get(&key) {
            None => true,
            Some(c) => self.writes.saturating_sub(c.refreshed_at_writes) >= STATS_REFRESH_WRITES,
        };
        if !stale {
            return;
        }
        // The empty `doc_freq` is the epoch boundary, and it is deliberate: a
        // frequency measured in the previous epoch must not be read against
        // globals measured in this one, so the reset drops every fill — and
        // `fill_order` with it, in the same statement, because the two are
        // only ever correct together.
        self.stats.insert(
            key,
            CachedStats {
                num_docs: 0,
                total_doc_len: 0,
                doc_freq: BTreeMap::new(),
                fill_order: VecDeque::new(),
                refreshed_at_writes: self.writes,
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
    /// makes a segment refuse reads changes what [`Shard::term_stats`] can see
    /// without touching `Db::writes`. That is a refusal rather than a drift,
    /// and it is the same on the exact path.
    ///
    /// THE RULE, and it is the one plausible-looking optimisation that
    /// silently undoes everything above: `ts` must be a timestamp pinned by
    /// the CURRENT query — `run_select` computes it as
    /// `clock.peek().max(last_commit)`. Never store the timestamp a refresh
    /// used and re-read at it later. [`Shard::term_stats`] is a pure function
    /// of the live corpus only at or above [`Shard::retain_floor`], and
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
    fn fill_term_stats(
        &mut self,
        collection: &str,
        path: &str,
        terms: &[String],
        ts: Timestamp,
    ) -> Result<Option<StatsTriple>> {
        let key = cache_key(collection, path);
        let (missing, anchored, same_instant) = match self.stats.get(&key) {
            Some(c) => (
                terms.iter().filter(|t| !c.doc_freq.contains_key(*t)).cloned().collect::<Vec<_>>(),
                c.anchored,
                c.anchored && c.measured_at_writes == self.writes,
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
        let writes = self.writes;
        let mut num_docs = 0u64;
        let mut total_doc_len = 0u64;
        let mut df: BTreeMap<String, u64> = BTreeMap::new();
        for s in self.shards(collection)? {
            // An empty `gather` is not a wasted call: `term_stats` then does
            // one `visibility` and one masked length sum per unit and enters
            // no posting cursor at all, which is how a prefix-only query —
            // `TextQuery::leaf_terms` skips `Prefix`, so its term list is
            // empty — gets real globals for one cheap pass.
            let (n, tl, d) = s.term_stats(path, &gather, ts)?;
            num_docs += n;
            total_doc_len += tl;
            for (t, c) in d {
                *df.entry(t).or_insert(0) += c;
            }
        }
        let Some(c) = self.stats.get_mut(&key) else { return Ok(None) };
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
        self.run(stmt, sql, false)
    }

    /// Convenience: run a SELECT and return its rows.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)?.rows()
    }

    pub fn query_with(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.execute_with(sql, params)?.rows()
    }

    fn run(&mut self, stmt: Statement, sql: &str, analyze: bool) -> Result<Outcome> {
        match stmt {
            Statement::Explain { analyze, inner } => {
                let inner_sql = sql.to_string();
                match *inner {
                    Statement::Select(sel) => {
                        let r = self.run_select(&sel, &inner_sql, true)?;
                        let text = r
                            .explain
                            .as_ref()
                            .map(|e| e.render())
                            .unwrap_or_else(|| "(no plan)".into());
                        let mut text = text;
                        if !analyze {
                            // Without ANALYZE the timings are still printed but
                            // the query did run; say so rather than implying a
                            // cost-only estimate.
                            text.push_str(
                                "  note: this plan was executed; EXPLAIN without ANALYZE \
                                 does not yet avoid execution\n",
                            );
                        }
                        Ok(Outcome::Explain(text))
                    }
                    other => self.run(other, sql, true),
                }
            }
            Statement::CreateCollection(c) => {
                let pk = c
                    .columns
                    .iter()
                    .find(|x| x.primary_key)
                    .map(|x| x.path.clone())
                    .unwrap_or_else(|| "id".to_string());
                let mut coll = Collection::new(&c.name, &pk, c.partition_by.clone());
                for col in &c.columns {
                    coll.declared.push(ColumnDef {
                        path: col.path.clone(),
                        ty: col.ty,
                        not_null: col.not_null,
                    });
                }
                let splits = c.splits.clone();
                let n = splits.len() + 1;
                self.create_collection(coll, &splits)?;
                Ok(Outcome::Ack(format!("collection `{}` created with {} shard(s)", c.name, n)))
            }
            Statement::CreateIndex(c) => {
                let kind = match c.spec {
                    IndexSpec::FullText { analyzer } => IndexKind::FullText { analyzer },
                    IndexSpec::Vector { dims, metric } => IndexKind::Vector { dims, metric },
                    IndexSpec::Secondary => IndexKind::Secondary,
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
                for d in i.docs {
                    self.insert(&i.collection, d)?;
                }
                Ok(Outcome::Ack(format!("{n} document(s) written at ts {}", self.last_commit)))
            }
            Statement::Delete(d) => {
                let keys: Vec<String> = match &d.predicate {
                    None => {
                        return Err(Error::Plan(
                            "DELETE without WHERE is refused; add a predicate or drop the \
                             collection"
                                .into(),
                        ))
                    }
                    Some(p) => {
                        let sel = Select {
                            projections: vec![Projection::All],
                            collection: d.collection.clone(),
                            predicate: Some(p.clone()),
                            order: None,
                            limit: Some(usize::MAX),
                            offset: 0,
                            cursor: None,
                            collapse: None,
                            with: WithOpts::default(),
                        };
                        self.run_select(&sel, "", false)?
                            .rows
                            .iter()
                            .map(|r| r.key.clone())
                            .collect()
                    }
                };
                let mut n = 0;
                for k in keys {
                    if self.delete_key(&d.collection, &k)? {
                        n += 1;
                    }
                }
                Ok(Outcome::Ack(format!("{n} document(s) deleted")))
            }
            Statement::Select(sel) => Ok(Outcome::Rows(self.run_select(&sel, sql, analyze)?)),
            Statement::Flush { collection } => {
                let n = self.flush(&collection)?;
                Ok(Outcome::Ack(format!("{n} shard(s) flushed")))
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
                        "collection {} (pk={}, partition_by={:?}, docs={})\n",
                        c.name, c.primary_key, c.partition_key, c.doc_count
                    ));
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
        if self.catalog.policies.remove(name).is_none() {
            return Err(Error::Plan(format!("no lifecycle policy `{name}`")));
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
        let now = lifecycle::now_micros(&self.clock);
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
        let now = lifecycle::now_micros(&self.clock);
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

    pub fn run_select(&mut self, sel: &Select, sql: &str, analyze: bool) -> Result<QueryResult> {
        self.absorb_shard_catalogs(&sel.collection)?;
        // Before the query, not after: an index the query is about to fault in
        // from cold storage counts as used even if the query then fails.
        self.touch_indexes(&sel.collection, &index_uses(sel))?;
        // Read-your-writes: pin at least the last commit timestamp this client
        // observed (§6).
        let ts = self.clock.peek().max(self.last_commit);
        let coll = self.catalog.get(&sel.collection)?.clone();
        let want = exec::required_terms(&coll, sel);
        let stats = self.gather_stats(
            &sel.collection,
            &want,
            ts,
            sel.with.exact_scoring || sel.with.exact,
        )?;
        self.log_vector_queries(sel);
        let shards = self.shards(&sel.collection)?;
        exec::run_select(ExecInput {
            shards,
            coll: &coll,
            select: sel,
            ts,
            stats: &stats,
            analyze,
            statement: sql.to_string(),
        })
    }

    /// Sample production vector queries for the recall harness (§12.1). The
    /// point of sampling *real* queries rather than synthetic ones is that
    /// recall regressions are workload-shaped: they show up on the filters and
    /// query distributions users actually have.
    fn log_vector_queries(&mut self, sel: &Select) {
        let mut push = |path: &str, q: &Vec<f32>| {
            self.queries_seen += 1;
            if self.opts.recall_sample_rate == 0
                || self.queries_seen % self.opts.recall_sample_rate != 0
            {
                return;
            }
            self.query_log.push(LoggedVectorQuery {
                collection: sel.collection.clone(),
                path: path.to_string(),
                query: q.clone(),
                k: sel.limit.unwrap_or(10),
                filter_sql: None,
            });
            if self.query_log.len() > 4096 {
                self.query_log.remove(0);
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
        self.query_log.iter().filter(|q| q.collection == collection).cloned().collect()
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
/// this it was double counted on BOTH arms. [`Shard::term_stats`] walks one
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
}

impl IndexUse {
    fn matches(self, kind: &IndexKind) -> bool {
        matches!(
            (self, kind),
            (IndexUse::Text, IndexKind::FullText { .. })
                | (IndexUse::Vector, IndexKind::Vector { .. })
                | (IndexUse::Scalar, IndexKind::Secondary)
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
    use super::*;

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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
            db.stats.get(&cache_key("notes", "body")).unwrap().doc_freq["quokka"],
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
        let at = db.stats.get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
                let at = db.stats.get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
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
            let at = db.stats.get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
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
        assert_eq!(db.stats.get(&cache_key("notes", "body")).unwrap().fill_order.len(), 3);

        // Over a refresh point, into epoch two.
        for i in 0..STATS_REFRESH_WRITES as usize {
            db.insert("notes", note(1000 + i)).unwrap();
        }
        let ts = db.clock.peek();
        db.gather_stats("notes", &want(vec!["new0000".to_string()]), ts, false).unwrap();
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
        assert_eq!(
            c.fill_order.iter().cloned().collect::<Vec<_>>(),
            vec!["dup".to_string(), "zeta".to_string()],
            "`dup` was filled by the first query and `zeta` by the second; one slot each"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_prefix_only_query_still_gets_real_globals() {
        // The one query shape that asks for no terms at all: `leaf_terms`
        // skips `Prefix` — an expanded term's frequency comes from the
        // segment's own dictionary on both paths — while `required_terms`
        // still creates the path entry, so `terms` arrives empty. It used to
        // be the only caller the reset's own gather was live for. Now the
        // fill's empty-slice gather is the authoritative one, which is one
        // pass instead of two and previously untested ground: nothing in the
        // tree asserted on a prefix query's `num_docs` or `avgdl`, and the
        // scorer divides every length norm by the second of them.
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

        // The same, through the query that actually produces this shape.
        db.query("SELECT id FROM notes WHERE text_match(body, 'alph*') LIMIT 5").unwrap();
        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
        assert!(c.anchored, "the globals were measured, and `num_docs == 0` cannot say so");
        assert_eq!(c.num_docs, 200);
        assert!(c.doc_freq.is_empty(), "a prefix query asks the gather for no terms");

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

        assert_eq!(db.stats.get(&cache_key("notes", "body")).unwrap().doc_freq["alpha"], 100);
        assert_eq!(db.stats.get(&cache_key("notes", "title")).unwrap().doc_freq["alpha"], 10);

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
        let at = db.stats.get(&cache_key("notes", "body")).unwrap().refreshed_at_writes;
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

        let c = db.stats.get(&cache_key("notes", "body")).unwrap();
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
}
