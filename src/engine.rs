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

use std::collections::BTreeMap;
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

#[derive(Debug, Clone, Default)]
struct CachedStats {
    num_docs: u64,
    total_doc_len: u64,
    doc_freq: BTreeMap<String, u64>,
    refreshed_at_writes: u64,
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
    /// `exact` performs the two-phase gather: ask every shard for the document
    /// frequency of exactly these terms, counting only postings visible at the
    /// snapshot. Otherwise the cached, periodically refreshed numbers are used —
    /// stale, and counting tombstones, which is precisely the approximation the
    /// design accepts by default.
    pub fn gather_stats(
        &mut self,
        collection: &str,
        want: &BTreeMap<String, Vec<String>>,
        ts: Timestamp,
        exact: bool,
    ) -> Result<BTreeMap<String, GlobalStats>> {
        let mut out = BTreeMap::new();
        for (path, terms) in want {
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
                self.refresh_stats_if_stale(collection, path)?;
                let c = self.stats.get(&cache_key(collection, path)).cloned().unwrap_or_default();
                let mut df = BTreeMap::new();
                for t in terms {
                    df.insert(t.clone(), c.doc_freq.get(t).copied().unwrap_or(0));
                }
                out.insert(
                    path.clone(),
                    GlobalStats {
                        num_docs: c.num_docs,
                        avg_doc_len: if c.num_docs > 0 {
                            c.total_doc_len as f64 / c.num_docs as f64
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

    fn refresh_stats_if_stale(&mut self, collection: &str, path: &str) -> Result<()> {
        let key = cache_key(collection, path);
        let stale = match self.stats.get(&key) {
            None => true,
            Some(c) => self.writes.saturating_sub(c.refreshed_at_writes) >= STATS_REFRESH_WRITES,
        };
        if !stale {
            return Ok(());
        }
        let mut c = CachedStats { refreshed_at_writes: self.writes, ..Default::default() };
        let ts = self.clock.peek();
        for s in self.shards(collection)? {
            let snap = s.snapshot_at(ts);
            for unit in s.sources(&snap) {
                c.num_docs += unit.num_docs() as u64;
                let handle = unit.text_handle(path)?;
                if let Some(src) = handle.as_ref().and_then(|h| h.source(path)) {
                    c.total_doc_len += src.total_doc_len();
                    for (t, df) in src.all_terms() {
                        *c.doc_freq.entry(t).or_insert(0) += df as u64;
                    }
                }
            }
        }
        self.stats.insert(key, c);
        Ok(())
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
}
