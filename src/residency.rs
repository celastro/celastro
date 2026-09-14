//! Storage tiers, placement and memory residency.
//!
//! The design states a residency *requirement* — "a tablet is not ready to
//! serve until codes, graphs, postings and document blobs are NVMe-resident;
//! full-precision vectors are the only component allowed cold" (§8.4) — and a
//! storage *layout* — "sealed segments: object storage, cached on local NVMe"
//! (§4.5). What it does not have is a mechanism: something that decides what is
//! in memory right now, notices when that stops being worth it, and lets the
//! bytes go.
//!
//! This module is that mechanism. Four ideas, kept separate on purpose:
//!
//! * **Tier** is a *declaration* on an index: how ready its owner wants it to
//!   be.
//! * **Placement** ([`Placement`]) turns a declaration into a per-node
//!   decision: which of the nodes holding a tablet is responsible for keeping a
//!   given index decoded.
//! * **Residency** is a *fact* about this node right now: what is decoded in
//!   RAM, how big it is, and when it was last touched.
//! * **Lifecycle** ([`crate::lifecycle`]) is a *policy* that moves an index
//!   from one tier to another as it ages or goes idle.
//!
//! The separation matters because the four are answerable by different people.
//! A tier is a capacity-planning decision. Placement is a cluster-topology
//! decision. Residency is an operational fact that changes minute to minute. A
//! lifecycle policy is a written-down rule that connects them without anyone
//! having to watch.
//!
//! ## The ladder
//!
//! Each name states its own guarantee rather than a temperature, because the
//! tiers do not differ along one axis. `active` and `minimal` are both "decoded
//! in RAM"; they differ in *how many nodes*. `cached` and `archived` are both
//! "not kept decoded"; they differ in *where the bytes are*. A hot/warm/cold
//! ladder hides that, and hiding it is how an operator ends up believing
//! `warm` is a slower `hot` when it is in fact the same speed on one node and
//! `cached` everywhere else.
//!
//! | tier | decoded on | bytes live | first query after idle |
//! |---|---|---|---|
//! | `active` | every node holding the tablet | local NVMe | already there |
//! | `minimal` | exactly one node, whatever the replica count | local NVMe | already there on that node; one segment read elsewhere |
//! | `cached` | no node between queries | local NVMe | one segment read |
//! | `archived` | never | archive store | one archive round trip, or refused |
//!
//! `minimal` is the tier that ignores the replication factor. `active` scales
//! its memory cost with the number of replicas — three replicas means three
//! copies decoded — which is what you want for an index on the critical path
//! and ruinous for one that is merely required to be *available*. `minimal`
//! says: somebody keeps this ready, exactly one somebody, and I do not care
//! how many nodes the tablet lives on. The nodes that are not the designated
//! holder treat it as `cached`, so it still answers; it just pays a segment
//! read to do it.
//!
//! Being honest about the archive tier is the point of the bottom of the
//! ladder, because §1's p99 target and §8.4's cache-residency precondition are
//! in direct tension with putting an index on object storage. A graph traversal
//! is a chain of *dependent* reads, so an archived vector index cannot serve a
//! query at anything like the latency target — §8.4 budgets hundreds of
//! milliseconds for it. An archived index is therefore not a slower served
//! index; it is an index you have decided to stop serving, which is why
//! [`ArchivedAccess::Refuse`] exists and why the default fault-in is reported
//! by `EXPLAIN` rather than hidden.
//!
//! ## What a tier is not
//!
//! A tier is a *priority*, not a guarantee. The node budget outranks every
//! declaration: a node that is over its budget evicts, and it evicts the
//! furthest-down-the-ladder, least-recently-used component first — but if only
//! `active` components are left, `active` components go. A budget that yields
//! to a declaration is not a budget.
//!
//! Nor is a tier a correctness boundary. Every component can be rebuilt from
//! the segment file, so unloading, evicting, archiving and faulting back in
//! change latency and memory and nothing else. The test suite states this as
//! an invariant rather than an aspiration: each residency test runs a query,
//! disturbs residency, and runs the same query again expecting the same rows.
//! It is what makes `minimal` implementable at all: a node that is not the
//! designated holder does not fail, it pays.
//!
//! ## Known gap
//!
//! §8.4 says full-precision vectors are the one component allowed to stay
//! cold. They are not, here: `vectors.full` is loaded as part of the `vec:`
//! component and is resident whenever the ANN index is, so a cold vector index
//! costs its full-precision copy as well as its codes and graph. Splitting it
//! into its own component is the right fix and is not done — `VectorStore`
//! holds `full` inline and reranking indexes into it directly, so the split is
//! a change to the vector module rather than to this one. Until then, the
//! practical consequence is that vector residency is larger than the design
//! calls for, by roughly `4 * dims` bytes per vector.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::error::{Error, Result};

/// How ready an index's owner wants it to be.
///
/// Ordered from most ready to least, so `a > b` means `a` is further down the
/// ladder — the direction a lifecycle policy moves an index and the order the
/// budget sheds it in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Tier {
    /// Decoded in RAM on every node holding the tablet. Evicted only under real
    /// memory pressure. Memory cost scales with the replication factor.
    #[default]
    Active,
    /// Decoded in RAM on exactly one node, whatever the replication factor
    /// says. The designated holder treats it as [`Tier::Active`]; every other
    /// node treats it as [`Tier::Cached`] and pays a segment read.
    ///
    /// This is the tier for an index that must be *ready* but need not be ready
    /// three times over — the memory cost is one copy per cluster rather than
    /// one per replica.
    Minimal,
    /// Local disk. Decoded on demand and released when idle; the bytes are
    /// always one local read away.
    Cached,
    /// Archive store (object storage). Never decoded between queries, and
    /// reaching it costs an archive round trip — or is refused outright.
    Archived,
}

impl Tier {
    pub fn parse(s: &str) -> Result<Tier> {
        // One name per tier, and it is the name the reports print. There
        // used to be temperature words beside them -- `hot`, `cold`, `ram`,
        // `s3` and a dozen more -- and every one was a word a reader had to
        // map back to a rung whose whole point is that it is not a
        // temperature: `minimal` is not "a bit less hot" than `active`, it is
        // the same speed on the node that holds it and `cached` on every
        // other. A tier has one name.
        match s.to_ascii_lowercase().as_str() {
            "active" => Ok(Tier::Active),
            "minimal" => Ok(Tier::Minimal),
            "cached" => Ok(Tier::Cached),
            "archived" => Ok(Tier::Archived),
            other => Err(Error::Schema(format!(
                "unknown tier `{other}`; expected active, minimal, cached or archived"
            ))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Tier::Active => "active",
            Tier::Minimal => "minimal",
            Tier::Cached => "cached",
            Tier::Archived => "archived",
        }
    }

    /// One line an operator can act on, for the reports.
    pub fn describes(self) -> &'static str {
        match self {
            Tier::Active => "decoded on every node holding the tablet",
            Tier::Minimal => "decoded on exactly one node",
            Tier::Cached => "decoded on demand, released when idle",
            Tier::Archived => "in the archive; faulted in or refused",
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Tier::Active => 0,
            Tier::Minimal => 1,
            Tier::Cached => 2,
            Tier::Archived => 3,
        }
    }

    pub fn from_u8(b: u8) -> Tier {
        match b {
            0 => Tier::Active,
            1 => Tier::Minimal,
            2 => Tier::Cached,
            _ => Tier::Archived,
        }
    }

    /// The same mapping for bytes that came off disk, where an unknown byte is
    /// damage rather than a tier.
    ///
    /// `from_u8` saturates, which is the wrong shape for a decoder: it turns
    /// every unrecognised byte into `archived`, the one tier that relocates
    /// files. A byte this build does not know is a record it should not be
    /// interpreting at all.
    pub(crate) fn try_from_u8(b: u8) -> Option<Tier> {
        (b <= 3).then(|| Tier::from_u8(b))
    }

    /// Is this a demotion — further down the ladder?
    pub fn is_colder_than(self, other: Tier) -> bool {
        self > other
    }
}

/// Which nodes hold this tablet, and which one this is.
///
/// The cluster itself is not built; what exists is the *decision function*, because
/// it is the part [`Tier::Minimal`] actually needs and the part that has to be
/// agreed without coordination. Every node computes the same answer from the
/// same replica list, so nothing has to be told who the holder is — which is
/// what makes the tier implementable at all in a design where the control
/// plane publishes a tablet map and the data plane never blocks on it (§10).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Placement {
    /// This node's identity in the replica list.
    pub node_id: String,
    /// Every node holding this tablet, this one included. Empty or
    /// single-element means a single-node deployment, where this node is
    /// necessarily the holder of everything.
    pub replicas: Vec<String>,
}

impl Default for Placement {
    fn default() -> Self {
        Placement { node_id: "node-0".to_string(), replicas: Vec::new() }
    }
}

impl Placement {
    /// This node's identity and the replica list it is part of. The one
    /// constructor: the struct is `#[non_exhaustive]`, so a caller builds it
    /// here or from `Default` and sets fields.
    pub fn new(node_id: impl Into<String>, replicas: Vec<String>) -> Placement {
        Placement { node_id: node_id.into(), replicas }
    }

    pub fn single(node_id: &str) -> Placement {
        Placement { node_id: node_id.to_string(), replicas: vec![node_id.to_string()] }
    }

    /// The node responsible for keeping `key` decoded.
    ///
    /// Deterministic from the replica list alone, and stable under everything
    /// except a change to that list — so an index does not migrate its holder
    /// because a query ran, a segment compacted, or a node restarted. Sorting
    /// first means the answer does not depend on the order the tablet map
    /// happened to list the replicas in.
    pub fn holder_for(&self, key: &str) -> Option<String> {
        if self.replicas.is_empty() {
            return None;
        }
        let mut r = self.replicas.clone();
        r.sort();
        r.dedup();
        let h = crate::codec::mix64(crate::codec::fnv1a(key.as_bytes()));
        Some(r[(h % r.len() as u64) as usize].clone())
    }

    /// Is this node the one that keeps `key` decoded?
    ///
    /// An empty replica list means a single-node deployment, which holds
    /// everything. A non-empty list that does not contain `node_id` is a
    /// misconfiguration in which *no* node holds anything — see
    /// [`Placement::validate`], which refuses it at startup rather than letting
    /// the tier silently guarantee nothing.
    pub fn holds(&self, key: &str) -> bool {
        match self.holder_for(key) {
            None => true,
            Some(h) => h == self.node_id,
        }
    }

    /// Refuse a placement whose guarantee cannot hold.
    ///
    /// A non-empty replica list that omits this node's id makes `holds` false
    /// for every key, so every `minimal` index resolves to `cached` on every
    /// node and the tier's whole promise — one decoded copy — silently becomes
    /// zero. It is also the likeliest mistake: set `replicas`, forget to set
    /// `node_id` off its default. The failure is invisible in every report, so
    /// it has to be caught where it is made.
    pub fn validate(&self) -> Result<()> {
        if self.replicas.is_empty() {
            return Ok(());
        }
        if self.node_id.is_empty() {
            return Err(Error::Schema(
                "placement: node_id is empty but a replica list was given".into(),
            ));
        }
        if !self.replicas.contains(&self.node_id) {
            return Err(Error::Schema(format!(
                "placement: this node is `{}` but the replica list is {:?}, which does not \
                 include it; no node would hold any `minimal` index",
                self.node_id, self.replicas
            )));
        }
        Ok(())
    }

    /// Every component's declared tier, resolved for this node.
    ///
    /// Keyed on `collection/component` rather than including the shard: the
    /// holder of an index should be one node, not one node per tablet — a
    /// collection with sixty shards would otherwise put a `minimal` copy on
    /// every node in the cluster, which is exactly what the tier exists to
    /// avoid.
    pub fn resolve_tiers(&self, coll: &crate::catalog::Collection) -> BTreeMap<String, Tier> {
        coll.index_tiers()
            .into_iter()
            .map(|(component, declared)| {
                let key = format!("{}/{}", coll.name, component);
                let t = self.resolve(declared, &key);
                (component, t)
            })
            .collect()
    }

    /// Resolve a declaration into what *this* node will do about it.
    ///
    /// The only tier that resolves to something different is [`Tier::Minimal`],
    /// and it resolves to [`Tier::Cached`] on every node but one. Residency is
    /// a fact about a node, so this is the tier the ledger, the idle sweeper
    /// and `SHOW RESIDENCY` all speak; the catalog keeps the declaration.
    pub fn resolve(&self, declared: Tier, key: &str) -> Tier {
        match declared {
            Tier::Minimal if !self.holds(key) => Tier::Cached,
            other => other,
        }
    }
}

/// What happens when a query needs an archived index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedAccess {
    /// Read it back from the archive and serve the query. Correct, and slow
    /// enough that `EXPLAIN` reports it as a distinct line.
    FaultIn,
    /// Fail the query, naming the index and its tier. For deployments where a
    /// silent hundred-millisecond stall is worse than an error.
    Refuse,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ResidencyOpts {
    /// Node-level ceiling on decoded index structures. Like the memtable
    /// budget (§4.3), this is a *node* number: a node hosts hundreds of
    /// tablets and per-index limits do not add up to anything enforceable.
    pub budget_bytes: usize,
    /// Unload an `active` index that has not been touched for this long.
    /// `None` — the default — keeps it resident until the budget forces a
    /// choice, which is what `active` means.
    pub active_idle_unload: Option<std::time::Duration>,
    /// Unload a `minimal` index *on the node designated to hold it*. `None` by
    /// default, for the same reason: on that node the tier is a promise to keep
    /// it ready. Every other node resolves `minimal` to `cached` and uses the
    /// window below, so this setting only ever governs one node per index.
    pub minimal_idle_unload: Option<std::time::Duration>,
    /// Unload a `cached` index this long after its last use. Small by design:
    /// the point of `cached` is that it is not holding memory between queries.
    pub cached_idle_unload: std::time::Duration,
    /// Unload an archived index this long after a fault-in, so a burst of
    /// queries shares one round trip.
    pub archived_idle_unload: std::time::Duration,
    pub archived_access: ArchivedAccess,
}

impl Default for ResidencyOpts {
    fn default() -> Self {
        ResidencyOpts {
            budget_bytes: 4 << 30,
            active_idle_unload: None,
            minimal_idle_unload: None,
            cached_idle_unload: std::time::Duration::from_secs(60),
            archived_idle_unload: std::time::Duration::from_secs(300),
            archived_access: ArchivedAccess::FaultIn,
        }
    }
}

/// Identifies one loadable piece of one open segment.
///
/// The first element is the segment's *residency uid*, not its segment id.
/// Segment ids are assigned per shard, so a node hosting two tablets has two
/// segment 1s; keying the ledger on the id merges their entries, and then an
/// unload in one shard marks the other's component evictable while it is still
/// in RAM. The uid is unique per open segment on the node.
pub type ComponentKey = (u64, String);

/// Hands out residency uids. Wraps at 2^64, which is a number of segment opens
/// no process reaches.
static NEXT_UID: AtomicU64 = AtomicU64::new(1);

pub fn next_uid() -> u64 {
    NEXT_UID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct ComponentStat {
    /// Ledger identity: unique per open segment on this node.
    pub uid: u64,
    /// The segment id an operator would recognise, for reports.
    pub segment: u64,
    pub component: String,
    pub tier: Tier,
    pub bytes: usize,
    pub loaded: bool,
    pub last_access_micros: u64,
    pub loads: u64,
    pub unloads: u64,
    /// Loads that had to go to the archive store rather than the local file.
    pub faults: u64,
}

/// Node-level residency accounting.
///
/// Deliberately does not own the segments. It records what is resident and
/// decides what should stop being resident; the shard walks its segments and
/// carries the decision out. Keeping the ledger and the eviction separate is
/// what lets the same accounting cover a hundred tablets without the manager
/// holding a reference to any of them.
///
/// The resident-byte total is derived from the ledger and only ever changed
/// while the ledger lock is held, with the delta computed from the entry's own
/// recorded state rather than from what the caller believes. That is what makes
/// the accounting self-correcting: a duplicate load, a re-load, or an unload of
/// something already unloaded moves the total by exactly the right amount,
/// including zero. A read-modify-write on the atomic outside the lock does not
/// have that property, and loses updates under concurrency.
#[derive(Debug, Default)]
pub struct ResidencyManager {
    opts: Mutex<ResidencyOpts>,
    resident: AtomicUsize,
    peak: AtomicUsize,
    total_loads: AtomicU64,
    total_unloads: AtomicU64,
    total_faults: AtomicU64,
    entries: Mutex<BTreeMap<ComponentKey, ComponentStat>>,
}

impl ResidencyManager {
    pub fn new(opts: ResidencyOpts) -> ResidencyManager {
        ResidencyManager { opts: Mutex::new(opts), ..Default::default() }
    }

    pub fn opts(&self) -> ResidencyOpts {
        *self.opts.lock().unwrap()
    }

    pub fn set_opts(&self, o: ResidencyOpts) {
        *self.opts.lock().unwrap() = o;
    }

    pub fn resident_bytes(&self) -> usize {
        self.resident.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn loads(&self) -> u64 {
        self.total_loads.load(Ordering::Relaxed)
    }

    pub fn unloads(&self) -> u64 {
        self.total_unloads.load(Ordering::Relaxed)
    }

    pub fn faults(&self) -> u64 {
        self.total_faults.load(Ordering::Relaxed)
    }

    pub fn over_budget(&self) -> bool {
        self.resident_bytes() > self.opts().budget_bytes
    }

    /// Apply a signed change to the resident total. Called only with the
    /// ledger lock held, so the two cannot drift.
    fn charge(&self, before: usize, after: usize) {
        if after >= before {
            let r = self.resident.fetch_add(after - before, Ordering::Relaxed) + (after - before);
            self.peak.fetch_max(r, Ordering::Relaxed);
        } else {
            let d = before - after;
            let _ = self
                .resident
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(d)));
        }
    }

    pub fn note_load(
        &self,
        key: ComponentKey,
        segment: u64,
        tier: Tier,
        bytes: usize,
        from_archive: bool,
        now: u64,
    ) {
        self.total_loads.fetch_add(1, Ordering::Relaxed);
        if from_archive {
            self.total_faults.fetch_add(1, Ordering::Relaxed);
        }
        let mut e = self.entries.lock().unwrap();
        let stat = e.entry(key.clone()).or_insert_with(|| ComponentStat {
            uid: key.0,
            segment,
            component: key.1.clone(),
            tier,
            bytes: 0,
            loaded: false,
            last_access_micros: now,
            loads: 0,
            unloads: 0,
            faults: 0,
        });
        // `was` is what this component is currently charged, which is 0 unless
        // a racing loader got here first. Charging the difference means two
        // threads that decode the same component bill it once, not twice.
        let was = if stat.loaded { stat.bytes } else { 0 };
        stat.segment = segment;
        stat.tier = tier;
        stat.bytes = bytes;
        stat.loaded = true;
        stat.last_access_micros = now;
        stat.loads += 1;
        if from_archive {
            stat.faults += 1;
        }
        self.charge(was, bytes);
    }

    /// Record a hit on a resident component, refreshing the tier as well as the
    /// clock: a tier change while a component is resident has to reach the
    /// eviction order, or `ALTER INDEX ... SET TIER archived` would leave the
    /// index it just demoted sitting at the *back* of the eviction queue.
    pub fn note_access(&self, key: &ComponentKey, tier: Tier, now: u64) {
        if let Some(s) = self.entries.lock().unwrap().get_mut(key) {
            s.last_access_micros = now;
            s.tier = tier;
        }
    }

    /// Update a resident component's tier without touching its clock.
    ///
    /// `ALTER INDEX ... SET TIER` and a lifecycle demotion both change where an
    /// index belongs *now*. Waiting for the next access to carry that into the
    /// ledger leaves the index the operator just demoted at the back of the
    /// eviction queue until somebody queries it — which, for an index being
    /// demoted precisely because nobody queries it, may be never.
    pub fn note_tier(&self, key: &ComponentKey, tier: Tier) {
        if let Some(s) = self.entries.lock().unwrap().get_mut(key) {
            s.tier = tier;
        }
    }

    pub fn note_unload(&self, key: &ComponentKey) {
        self.total_unloads.fetch_add(1, Ordering::Relaxed);
        let mut e = self.entries.lock().unwrap();
        if let Some(s) = e.get_mut(key) {
            let was = if s.loaded { s.bytes } else { 0 };
            s.loaded = false;
            s.bytes = 0;
            s.unloads += 1;
            self.charge(was, 0);
        }
    }

    /// Drop every entry for a segment that is going away.
    ///
    /// Without this the ledger grows by a segment's worth of entries at every
    /// compaction, all still flagged resident, and the node concludes it is
    /// permanently over a budget it cannot get under — evicting live
    /// components on every sweep to reclaim memory that was freed long ago.
    pub fn forget_segment(&self, uid: u64) {
        let mut e = self.entries.lock().unwrap();
        let keys: Vec<ComponentKey> = e
            .range((uid, String::new())..)
            .take_while(|(k, _)| k.0 == uid)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(s) = e.remove(&k) {
                if s.loaded {
                    self.charge(s.bytes, 0);
                }
            }
        }
    }

    /// Has this component been idle long enough for its tier to release it?
    pub fn idle_expired(&self, tier: Tier, last_access: u64, now: u64) -> bool {
        let o = self.opts();
        let idle = now.saturating_sub(last_access);
        // Saturating, not `as`: a window expressed as a very large number of
        // seconds — the natural way to write "effectively never" — truncates to
        // a fraction of a second under a plain cast, and unloads everything.
        let micros = |d: std::time::Duration| u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        // `tier` here is the *resolved* tier, so `Minimal` only reaches this on
        // the node designated to hold it; everywhere else placement has already
        // turned it into `Cached`.
        let limit = match tier {
            Tier::Active => match o.active_idle_unload {
                Some(d) => micros(d),
                None => return false,
            },
            Tier::Minimal => match o.minimal_idle_unload {
                Some(d) => micros(d),
                None => return false,
            },
            Tier::Cached => micros(o.cached_idle_unload),
            Tier::Archived => micros(o.archived_idle_unload),
        };
        idle >= limit
    }

    /// Components to evict, worst first, until the budget is met.
    ///
    /// Furthest down the ladder first, then least recently used. An `active`
    /// index is evicted only when nothing below it is left — which is the whole
    /// meaning of declaring it active, and also why the tier is a priority
    /// rather than a guarantee: a budget that yields to a declaration is not a
    /// budget.
    ///
    /// Entries recorded resident at zero bytes are skipped rather than
    /// nominated: they free nothing, so nominating them burns an eviction slot
    /// on every sweep and never makes progress.
    pub fn plan_evictions(&self, mut need: usize) -> Vec<ComponentKey> {
        if need == 0 {
            return Vec::new();
        }
        let e = self.entries.lock().unwrap();
        let mut live: Vec<&ComponentStat> =
            e.values().filter(|s| s.loaded && s.bytes > 0).collect();
        live.sort_by(|a, b| {
            b.tier
                .cmp(&a.tier)
                .then(a.last_access_micros.cmp(&b.last_access_micros))
                .then(a.segment.cmp(&b.segment))
                .then(a.uid.cmp(&b.uid))
                .then(a.component.cmp(&b.component))
        });
        let mut out = Vec::new();
        for s in live {
            if need == 0 {
                break;
            }
            out.push((s.uid, s.component.clone()));
            need = need.saturating_sub(s.bytes);
        }
        out
    }

    pub fn snapshot(&self) -> Vec<ComponentStat> {
        self.entries.lock().unwrap().values().cloned().collect()
    }
}

/// A lazily decoded segment component.
///
/// The `RwLock<Option<Arc<T>>>` is the whole trick: readers take the `Arc` and
/// let go of the lock, so an eviction that happens mid-query cannot pull the
/// bytes out from under a reader — it only drops the segment's own reference,
/// and the memory goes when the last reader finishes. This is the same
/// discipline the manifest uses for retired segment files, for the same reason.
#[derive(Debug)]
pub struct Lazy<T> {
    cell: std::sync::RwLock<Option<std::sync::Arc<T>>>,
    last_access: AtomicU64,
    bytes: AtomicUsize,
}

impl<T> Default for Lazy<T> {
    fn default() -> Self {
        Lazy {
            cell: std::sync::RwLock::new(None),
            last_access: AtomicU64::new(0),
            bytes: AtomicUsize::new(0),
        }
    }
}

impl<T> Lazy<T> {
    pub fn loaded(value: T, bytes: usize, now: u64) -> Lazy<T> {
        Lazy {
            cell: std::sync::RwLock::new(Some(std::sync::Arc::new(value))),
            last_access: AtomicU64::new(now),
            bytes: AtomicUsize::new(bytes),
        }
    }

    pub fn peek(&self) -> Option<std::sync::Arc<T>> {
        self.cell.read().unwrap().clone()
    }

    pub fn is_loaded(&self) -> bool {
        self.cell.read().unwrap().is_some()
    }

    pub fn last_access(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn touch(&self, now: u64) {
        self.last_access.store(now, Ordering::Relaxed);
    }

    pub fn store(&self, value: T, bytes: usize, now: u64) -> std::sync::Arc<T> {
        let a = std::sync::Arc::new(value);
        *self.cell.write().unwrap() = Some(a.clone());
        self.bytes.store(bytes, Ordering::Relaxed);
        self.last_access.store(now, Ordering::Relaxed);
        a
    }

    /// Populate the cell under the write lock, running `build` only if it is
    /// still empty, and reporting the result to `note` before the lock is
    /// released.
    ///
    /// Both halves matter. Re-checking under the lock is what stops two threads
    /// that miss together from decoding the same component twice. Reporting
    /// under the lock is what stops an eviction landing between the store and
    /// the bookkeeping — which would credit an unload for bytes that had not yet
    /// been charged, and then charge them against a cell that is already empty.
    ///
    /// `build` and `note` must not touch this `Lazy` again; nothing else is
    /// held, so a load may take the segment's source lock and the manager's
    /// ledger lock freely.
    pub fn load_with<E>(
        &self,
        now: u64,
        build: impl FnOnce() -> std::result::Result<(T, usize), E>,
        note: impl FnOnce(usize),
    ) -> std::result::Result<(std::sync::Arc<T>, bool), E> {
        let mut g = self.cell.write().unwrap();
        if let Some(a) = g.as_ref() {
            self.last_access.store(now, Ordering::Relaxed);
            return Ok((a.clone(), false));
        }
        let (value, bytes) = build()?;
        let a = std::sync::Arc::new(value);
        *g = Some(a.clone());
        self.bytes.store(bytes, Ordering::Relaxed);
        self.last_access.store(now, Ordering::Relaxed);
        note(bytes);
        Ok((a, true))
    }

    /// Drop this node's reference and tell the ledger before the lock is
    /// released. `Some(bytes)` when this call is the one that took the value —
    /// including `Some(0)` for a component that decoded to nothing, so the
    /// ledger is told even when there is nothing to reclaim. `None` when it was
    /// already unloaded, in which case `note` does not run.
    ///
    /// Reporting under the lock is the mirror of [`Lazy::load_with`], and for
    /// the same reason. Release the lock first and a loader can complete an
    /// entire reload inside the gap; this call's `note_unload` then erases the
    /// charge for bytes that are, right now, in RAM — and because eviction
    /// planning only looks at entries flagged resident, those bytes become
    /// invisible to the sweeper until something unloads and reloads them again.
    pub fn unload_with(&self, note: impl FnOnce(usize)) -> Option<usize> {
        let mut g = self.cell.write().unwrap();
        if g.take().is_some() {
            let b = self.bytes.swap(0, Ordering::Relaxed);
            note(b);
            Some(b)
        } else {
            None
        }
    }

    /// [`Lazy::unload_with`] with no bookkeeping, for cells the ledger does not
    /// track.
    pub fn unload(&self) -> Option<usize> {
        self.unload_with(|_| {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both `load_with` and `unload_with` must report to the ledger *before*
    /// they release the cell, or a load racing an unload leaves the two
    /// disagreeing about what is in RAM.
    ///
    /// Tested by construction rather than by hoping to hit the interleaving:
    /// the callback signals that it has started and then dawdles, and a second
    /// thread's attempt on the same cell must not get through while it does.
    #[test]
    fn the_ledger_is_told_before_the_cell_is_released() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc as A;

        // The `note` from an unload must exclude a concurrent load.
        let cell: A<Lazy<Vec<u8>>> = A::new(Lazy::loaded(vec![0u8; 8], 8, 0));
        let in_note = A::new(AtomicBool::new(false));
        let other_got_through = A::new(AtomicBool::new(false));
        let unloader = {
            let (cell, in_note) = (cell.clone(), in_note.clone());
            std::thread::spawn(move || {
                cell.unload_with(|_| {
                    in_note.store(true, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(150));
                });
            })
        };
        while !in_note.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        let loader = {
            let (cell, flag) = (cell.clone(), other_got_through.clone());
            std::thread::spawn(move || {
                let _ = cell.load_with(1, || Ok::<_, ()>((vec![0u8; 8], 8)), |_| {});
                flag.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(40));
        assert!(
            !other_got_through.load(Ordering::SeqCst),
            "a load completed while an unload was still reporting: its charge is about to be \
             erased for bytes that are in RAM"
        );
        unloader.join().unwrap();
        loader.join().unwrap();

        // And symmetrically, the `note` from a load must exclude a concurrent
        // unload.
        let cell: A<Lazy<Vec<u8>>> = A::new(Lazy::default());
        let in_note = A::new(AtomicBool::new(false));
        let other_got_through = A::new(AtomicBool::new(false));
        let loader = {
            let (cell, in_note) = (cell.clone(), in_note.clone());
            std::thread::spawn(move || {
                let _ = cell.load_with(
                    1,
                    || Ok::<_, ()>((vec![0u8; 8], 8)),
                    |_| {
                        in_note.store(true, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(150));
                    },
                );
            })
        };
        while !in_note.load(Ordering::SeqCst) {
            std::hint::spin_loop();
        }
        let unloader = {
            let (cell, flag) = (cell.clone(), other_got_through.clone());
            std::thread::spawn(move || {
                cell.unload_with(|_| {});
                flag.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(40));
        assert!(
            !other_got_through.load(Ordering::SeqCst),
            "an unload completed while a load was still reporting"
        );
        loader.join().unwrap();
        unloader.join().unwrap();
    }

    /// Two threads that miss on the same component decode it once, and charge
    /// it once.
    #[test]
    fn concurrent_first_touches_decode_and_charge_exactly_once() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc as A;

        for _ in 0..12 {
            let m = A::new(ResidencyManager::new(ResidencyOpts::default()));
            let cell: A<Lazy<Vec<u8>>> = A::new(Lazy::default());
            let builds = A::new(AtomicUsize::new(0));
            let start = A::new(std::sync::Barrier::new(8));
            let mut hs = Vec::new();
            for _ in 0..8 {
                let (m, cell, builds, start) =
                    (m.clone(), cell.clone(), builds.clone(), start.clone());
                hs.push(std::thread::spawn(move || {
                    start.wait();
                    let _ = cell.load_with(
                        1,
                        || {
                            builds.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, ()>((vec![0u8; 512], 512))
                        },
                        |b| m.note_load((1, "x".into()), 1, Tier::Active, b, false, 1),
                    );
                }));
            }
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(builds.load(Ordering::SeqCst), 1, "the component decoded more than once");
            assert_eq!(m.resident_bytes(), 512, "and was charged more than once");
        }
    }

    #[test]
    fn tier_names_round_trip() {
        for t in [Tier::Active, Tier::Minimal, Tier::Cached, Tier::Archived] {
            assert_eq!(Tier::parse(t.name()).unwrap(), t);
            assert_eq!(Tier::from_u8(t.as_u8()), t);
        }
        // Case is not a second spelling; a temperature word is, and is
        // refused naming the four tiers.
        assert_eq!(Tier::parse("ACTIVE").unwrap(), Tier::Active);
        for alias in [
            "hot",
            "ram",
            "memory",
            "resident",
            "warm",
            "pinned",
            "single",
            "one_copy",
            "cold",
            "disk",
            "ssd",
            "nvme",
            "archive",
            "s3",
            "object",
            "object_store",
            "lukewarm",
        ] {
            let e = Tier::parse(alias).unwrap_err().to_string();
            assert!(e.contains("active, minimal, cached or archived"), "{alias}: {e}");
        }
    }

    #[test]
    fn eviction_takes_the_coldest_and_stalest_first() {
        let m = ResidencyManager::new(ResidencyOpts { budget_bytes: 100, ..Default::default() });
        // Every clock is identical, so nothing here can be explained by
        // recency: the order is the ladder, and only the ladder.
        let t = 500u64;
        m.note_load((1, "active".into()), 1, Tier::Active, 40, false, t);
        m.note_load((2, "minimal".into()), 2, Tier::Minimal, 40, false, t);
        m.note_load((3, "cached".into()), 3, Tier::Cached, 40, false, t);
        m.note_load((4, "archived".into()), 4, Tier::Archived, 40, true, t);
        assert_eq!(m.resident_bytes(), 160);
        assert!(m.over_budget());
        assert_eq!(m.faults(), 1);
        assert_eq!(
            m.plan_evictions(1000),
            vec![
                (4, "archived".to_string()),
                (3, "cached".to_string()),
                (2, "minimal".to_string()),
                (1, "active".to_string()),
            ],
            "eviction walks the ladder from the bottom"
        );
        // Freeing 60 bytes stops as soon as it has enough.
        assert_eq!(
            m.plan_evictions(60),
            vec![(4, "archived".to_string()), (3, "cached".to_string())]
        );

        // Within one rung, recency decides — and never across rungs.
        let m = ResidencyManager::new(ResidencyOpts { budget_bytes: 100, ..Default::default() });
        m.note_load((1, "active-fresh".into()), 1, Tier::Active, 40, false, 1_000);
        m.note_load((1, "active-stale".into()), 1, Tier::Active, 40, false, 10);
        m.note_load((2, "cached-fresh".into()), 2, Tier::Cached, 40, false, 999);
        assert_eq!(
            m.plan_evictions(1000),
            vec![
                (2, "cached-fresh".to_string()),
                (1, "active-stale".to_string()),
                (1, "active-fresh".to_string()),
            ],
            "the freshly used cached index still goes before the stalest active one"
        );
    }

    #[test]
    fn idle_rules_follow_the_tier() {
        let m = ResidencyManager::new(ResidencyOpts {
            active_idle_unload: None,
            cached_idle_unload: std::time::Duration::from_secs(60),
            archived_idle_unload: std::time::Duration::from_secs(300),
            ..Default::default()
        });
        let minute = 60_000_000u64;
        let now = 10_000 * minute;
        // An `active` index is never released on idle alone, and neither is a
        // `minimal` one — but only the designated holder ever asks, because
        // placement has already turned it into `cached` everywhere else.
        assert!(!m.idle_expired(Tier::Active, now - 100 * minute, now));
        assert!(!m.idle_expired(Tier::Minimal, now - 100 * minute, now));
        assert!(!m.idle_expired(Tier::Cached, now - minute / 2, now));
        assert!(m.idle_expired(Tier::Cached, now - 2 * minute, now));
        assert!(!m.idle_expired(Tier::Archived, now - 2 * minute, now));
        assert!(m.idle_expired(Tier::Archived, now - 10 * minute, now));

        // With an idle limit configured for either resident tier, it applies.
        m.set_opts(ResidencyOpts {
            active_idle_unload: Some(std::time::Duration::from_secs(600)),
            minimal_idle_unload: Some(std::time::Duration::from_secs(900)),
            ..m.opts()
        });
        assert!(!m.idle_expired(Tier::Minimal, now - 10 * minute, now));
        assert!(m.idle_expired(Tier::Minimal, now - 20 * minute, now));
        assert!(m.idle_expired(Tier::Active, now - 20 * minute, now));
        assert!(!m.idle_expired(Tier::Active, now - 5 * minute, now));
    }

    #[test]
    fn accounting_survives_load_unload_cycles() {
        let m = ResidencyManager::new(ResidencyOpts::default());
        for i in 0..10u64 {
            m.note_load((i, "x".into()), i, Tier::Cached, 100, false, i);
        }
        assert_eq!(m.resident_bytes(), 1000);
        assert_eq!(m.peak_bytes(), 1000);
        for i in 0..10u64 {
            m.note_unload(&(i, "x".into()));
        }
        assert_eq!(m.resident_bytes(), 0);
        assert_eq!(m.peak_bytes(), 1000, "peak is a high-water mark");
        assert_eq!(m.loads(), 10);
        assert_eq!(m.unloads(), 10);
        // A retired segment leaves no ghost entries behind.
        m.note_load((7, "x".into()), 7, Tier::Active, 50, false, 1);
        m.forget_segment(7);
        assert_eq!(m.resident_bytes(), 0);
        assert!(m.snapshot().iter().all(|s| s.segment != 7));
    }

    #[test]
    fn a_lazy_cell_hands_out_a_reference_that_outlives_eviction() {
        let l = Lazy::loaded(vec![1u8, 2, 3], 3, 0);
        let held = l.peek().unwrap();
        assert_eq!(l.unload(), Some(3));
        assert!(!l.is_loaded());
        // The reader still has its data; the memory goes when it lets go.
        assert_eq!(*held, vec![1u8, 2, 3]);
        assert_eq!(l.unload(), None, "unloading twice must not double-count");
    }
}
