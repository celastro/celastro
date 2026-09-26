//! A deterministic simulator for the coordinator-to-shard boundary.
//!
//! celastro is one process, and the boundary a network will one day sit
//! under is [`crate::plan::service::ShardService`]. This module puts a
//! seeded fault schedule there now, so that the property a transport has to
//! keep is stated and checked before the transport exists — building the
//! transport first and the simulator second means debugging a distributed
//! system with print statements.
//!
//! The property: **a fault can shorten an answer only by saying so.** Under
//! any schedule, a statement either answers exactly what it answers with no
//! faults, or is refused naming the shard, or — only under
//! `WITH (partial_results)` — answers from the shards that replied and names
//! the rest in `missing`. It never answers something else quietly.
//!
//! Faults, decided per call from one seeded generator so a run reproduces
//! exactly from its seed:
//!
//! - **drop** — the call is never answered. The coordinator sees
//!   [`Error::Deadline`], the same thing a shard that ran out of time in
//!   this process produces, so a partition and a slow shard are one case.
//! - **crash and restart** — the shard's process dies before the call and a
//!   replacement opens the same directory: the WAL is replayed, the manifest
//!   read, and the call is answered by the replacement, which serves every
//!   later call of the statement too. This is the durability work made
//!   observable: whatever was acknowledged is on the disk, or the replacement
//!   answers differently and the property fails.
//! - **reorder** — the shards are handed to the coordinator in a permuted
//!   order, so nothing may depend on the order answers arrive in.
//!
//! Delay is not simulated separately: the coordinator is synchronous, so a
//! reply that arrives inside the deadline is a delivered reply and one that
//! does not is a drop. What arrival order could change is covered by reorder.
//!
//! A [`Sim`] is installed on a [`crate::Db`] with [`crate::Db::install_sim`]
//! and records every decision in a trace.

use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::plan::service::{
    CandidatesRequest, Local, ScanRequest, ShardCandidates, ShardScan, ShardService, TermStats,
};
use crate::plan::walk::{ExpandRequest, HopExpansion};
use crate::shard::Shard;
use crate::time::Timestamp;
use crate::value::Value;

/// Fault probabilities, per call, in parts per thousand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Faults {
    /// A call is never answered.
    pub drop: u32,
    /// The shard crashes before the call and a replacement, opened from the
    /// shard's directory, answers it and every later call of the statement.
    /// Needs a persistent database; a crash of an in-memory shard is a drop.
    pub crash: u32,
    /// Hand the shards to the coordinator in a random order.
    pub reorder: bool,
}

impl Faults {
    pub fn new(drop: u32, crash: u32, reorder: bool) -> Faults {
        Faults { drop, crash, reorder }
    }
}

/// What the schedule decided for one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    Delivered,
    Dropped,
    /// The shard restarted, then answered.
    Restarted,
}

/// One decision, in the order it was taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub shard: usize,
    pub call: &'static str,
    pub fate: Fate,
}

struct State {
    rng: u64,
    trace: Vec<Event>,
    /// Replacements opened from a directory, counted after the open succeeded.
    restarts: u64,
}

/// The seeded schedule. Shared by every service it wraps.
pub struct Sim {
    seed: u64,
    faults: Faults,
    state: Mutex<State>,
}

impl std::fmt::Debug for Sim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sim(seed={}, {:?})", self.seed, self.faults)
    }
}

/// splitmix64, to turn any seed (including 0) into a non-zero generator
/// state; then xorshift64* for the draws.
fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) | 1
}

impl Sim {
    pub fn new(seed: u64, faults: Faults) -> Arc<Sim> {
        Arc::new(Sim {
            seed,
            faults,
            state: Mutex::new(State { rng: mix(seed), trace: Vec::new(), restarts: 0 }),
        })
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn faults(&self) -> Faults {
        self.faults
    }

    /// Back to the start of the schedule, with the trace cleared: the same
    /// statements in the same order now meet the same decisions.
    pub fn reset(&self) {
        let mut s = self.state.lock().unwrap();
        s.rng = mix(self.seed);
        s.trace.clear();
        s.restarts = 0;
    }

    /// How many replacements were opened from a directory: a restart that
    /// did not read the disk is not a restart, and this counts only those
    /// that did.
    pub fn restarts(&self) -> u64 {
        self.state.lock().unwrap().restarts
    }

    /// Every decision so far, in order.
    pub fn trace(&self) -> Vec<Event> {
        self.state.lock().unwrap().trace.clone()
    }

    fn next(state: &mut State) -> u64 {
        let mut x = state.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn decide(&self, shard: usize, call: &'static str) -> Fate {
        let mut s = self.state.lock().unwrap();
        let fate = if (Sim::next(&mut s) % 1000) < self.faults.drop as u64 {
            Fate::Dropped
        } else if (Sim::next(&mut s) % 1000) < self.faults.crash as u64 {
            Fate::Restarted
        } else {
            Fate::Delivered
        };
        s.trace.push(Event { shard, call, fate });
        fate
    }

    /// The order to hand `n` shards to the coordinator in.
    pub fn order(&self, n: usize) -> Vec<usize> {
        let mut order: Vec<usize> = (0..n).collect();
        if self.faults.reorder {
            let mut s = self.state.lock().unwrap();
            for i in (1..n).rev() {
                let j = (Sim::next(&mut s) % (i as u64 + 1)) as usize;
                order.swap(i, j);
            }
        }
        order
    }

    fn unreachable(shard: usize, call: &str) -> Error {
        Error::Deadline(format!(
            "shard {shard} did not answer `{call}` within the statement deadline (simulated \
             partition); use WITH (partial_results) to opt in to incomplete answers"
        ))
    }
}

/// One shard as the simulator presents it to the coordinator.
pub struct SimShard<'a> {
    sim: Arc<Sim>,
    index: usize,
    live: &'a Shard,
    /// The replacement after a crash: opened from the shard's directory,
    /// serving every later call of the statement. Dropped with the service,
    /// which is the end of the statement.
    replacement: Mutex<Option<Shard>>,
}

impl<'a> SimShard<'a> {
    pub(crate) fn new(sim: Arc<Sim>, index: usize, live: &'a Shard) -> SimShard<'a> {
        SimShard { sim, index, live, replacement: Mutex::new(None) }
    }

    fn restart(&self) -> Result<()> {
        let Some(dir) = self.live.dir() else {
            // Nothing on a disk to come back from: a crash of an in-memory
            // shard loses it, which to the coordinator is a drop.
            return Err(Sim::unreachable(self.index, "restart"));
        };
        let mut fresh = Shard::open(
            self.live.coll.clone(),
            self.live.clock.clone(),
            self.live.opts.clone(),
            dir,
        )?;
        fresh.key_range = self.live.key_range.clone();
        *self.replacement.lock().unwrap() = Some(fresh);
        self.sim.state.lock().unwrap().restarts += 1;
        Ok(())
    }

    /// Run `f` against whichever incarnation of the shard is serving, after
    /// the schedule has had its say about this call.
    fn deliver<R>(&self, call: &'static str, f: impl FnOnce(&Local<'_>) -> Result<R>) -> Result<R> {
        match self.sim.decide(self.index, call) {
            Fate::Dropped => return Err(Sim::unreachable(self.index, call)),
            Fate::Restarted => self.restart()?,
            Fate::Delivered => {}
        }
        let replacement = self.replacement.lock().unwrap();
        let shard: &Shard = replacement.as_ref().unwrap_or(self.live);
        f(&Local { shard, index: self.index })
    }
}

impl ShardService for SimShard<'_> {
    fn index(&self) -> usize {
        self.index
    }

    /// One call at a time, so the schedule's decisions fall in one order
    /// and a seed reproduces its trace.
    fn concurrent(&self) -> bool {
        false
    }

    fn manifest_version(&self) -> u64 {
        self.live.manifest_version
    }

    fn may_hold(&self, prefix: &str) -> bool {
        Local { shard: self.live, index: self.index }.may_hold(prefix)
    }

    fn term_stats(&self, path: &str, terms: &[String], ts: Timestamp) -> Result<TermStats> {
        self.deliver("term_stats", |s| s.term_stats(path, terms, ts))
    }

    fn prefix_terms(
        &self,
        path: &str,
        prefix: &str,
        ts: Timestamp,
        limit: usize,
        key_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        self.deliver("prefix_terms", |s| s.prefix_terms(path, prefix, ts, limit, key_prefix))
    }

    fn candidates(&self, req: &CandidatesRequest<'_>) -> Result<ShardCandidates> {
        self.deliver("candidates", |s| s.candidates(req))
    }

    fn scan(&self, req: &ScanRequest<'_>) -> Result<ShardScan> {
        self.deliver("scan", |s| s.scan(req))
    }

    fn documents(
        &self,
        manifest_version: u64,
        ts: Timestamp,
        handles: &[(usize, u32)],
    ) -> Result<Vec<Value>> {
        self.deliver("documents", |s| s.documents(manifest_version, ts, handles))
    }

    fn get(&self, key: &str, ts: Timestamp) -> Result<Option<Value>> {
        self.deliver("get", |s| s.get(key, ts))
    }

    fn count(&self, ts: Timestamp) -> Result<Option<u64>> {
        self.deliver("count", |s| s.count(ts))
    }

    fn expand(&self, req: &ExpandRequest<'_>) -> Result<HopExpansion> {
        self.deliver("expand", |s| s.expand(req))
    }

    fn present(&self, keys: &[String], ts: Timestamp) -> Result<Vec<String>> {
        self.deliver("present", |s| s.present(keys, ts))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::engine::{Db, DbOpts};
    use crate::plan::exec::QueryResult;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("celastro-sim-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    /// Three shards by tenant, text and vector indexes, sixty sealed rows and
    /// thirty in the memtable, a few deletes on both sides of the seal: the
    /// shapes a restart has to reproduce from the disk.
    fn fixture(dir: &Path) -> Db {
        let mut db = Db::open(dir, DbOpts::default()).unwrap();
        db.execute(
            "CREATE COLLECTION items (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, n INT) \
             PARTITION BY (tenant) WITH (splits = ['t1', 't2'])",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX items_body ON items USING fulltext (body) WITH (analyzer = 'english')",
        )
        .unwrap();
        db.execute(
            "CREATE INDEX items_emb ON items USING vector (embedding) \
             WITH (dims = 4, metric = 'cosine')",
        )
        .unwrap();
        let words = ["graph", "search", "vector", "index", "segment", "fusion", "rank"];
        for i in 0..90usize {
            if i == 60 {
                db.execute("FLUSH items").unwrap();
                db.delete_key("items", "t0\u{1}doc-003").unwrap();
                db.delete_key("items", "t1\u{1}doc-031").unwrap();
            }
            let body = format!("{} {} {}", words[i % 7], words[(i * 3) % 7], words[(i * 5) % 7]);
            db.insert(
                "items",
                crate::json::parse(&format!(
                    r#"{{"id":"doc-{i:03}","tenant":"t{}","n":{i},"body":"{body}","embedding":[{},{},{},1.0]}}"#,
                    i % 3,
                    (i % 7) as f32 / 7.0,
                    (i % 5) as f32 / 5.0,
                    (i % 3) as f32 / 3.0,
                ))
                .unwrap(),
            )
            .unwrap();
        }
        db.delete_key("items", "t2\u{1}doc-071").unwrap();
        db
    }

    /// The fixture plus an edge collection over it, in three shards of its
    /// own: each item cites the next, the third after and the eleventh
    /// after, wrapping, so a walk crosses every shard of both collections.
    fn graph_fixture(dir: &Path) -> Db {
        let mut db = fixture(dir);
        db.execute(
            "CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL) \
             WITH (nodes_of = 'items', splits = ['e090', 'e180'])",
        )
        .unwrap();
        db.execute("CREATE INDEX cites_adj ON cites USING adjacency (src, dst)").unwrap();
        for i in 0..90usize {
            if i == 60 {
                db.execute("FLUSH cites").unwrap();
            }
            for (j, step) in [1usize, 3, 11].iter().enumerate() {
                db.insert(
                    "cites",
                    crate::json::parse(&format!(
                        r#"{{"id":"e{:03}","src":"doc-{i:03}","dst":"doc-{:03}","w":{j}}}"#,
                        i * 3 + j,
                        (i + step) % 90
                    ))
                    .unwrap(),
                )
                .unwrap();
            }
        }
        db
    }

    /// Walk clauses and the rest of the statement each is fused with.
    const WALKS: &[(&str, &str)] = &[
        (
            "id WITHIN 2 HOPS OF 'doc-010' VIA cites",
            "AND text_match(body, 'graph') ORDER BY embedding <=> [0.5, 0.5, 0.5, 1.0] LIMIT 10",
        ),
        (
            "id WITHIN 3 HOPS OF 'doc-002' VIA cites WHERE w > 0",
            "ORDER BY hybrid(text_match(body, 'vector index'), embedding <=> [0.2, 0.2, 0.9, 1.0], \
             method => 'linear') LIMIT 6",
        ),
        ("id WITHIN 2 HOPS OF 'doc-050' VIA cites REVERSE", "LIMIT 100"),
        (
            "id WITHIN 2 HOPS OF 'doc-030' VIA cites",
            "LIMIT 100 WITH (max_frontier = 5, max_fanout = 2)",
        ),
    ];

    const QUERIES: &[&str] = &[
        "SELECT id FROM items ORDER BY hybrid(text_match(body, 'graph search'), embedding <=> \
         [0.5, 0.5, 0.5, 1.0], method => 'linear') LIMIT 10",
        "SELECT id FROM items ORDER BY embedding <=> [0.1, 0.9, 0.2, 1.0] LIMIT 7",
        "SELECT id FROM items WHERE text_match(body, 'seg*') LIMIT 100",
        "SELECT id, n FROM items ORDER BY n DESC LIMIT 12 OFFSET 3",
        "SELECT id FROM items LIMIT 20",
        "SELECT id FROM items WHERE n > 40 AND text_match(body, 'vector') ORDER BY \
         hybrid(text_match(body, 'vector index'), embedding <=> [0.2, 0.2, 0.9, 1.0], \
         method => 'linear') LIMIT 5",
        "SELECT id FROM items WHERE text_match(body, 'seg* fus*') LIMIT 100",
        // Every row matches and `k'` is under a shard's share of them, so
        // the shards fill the depth they are asked for and some are asked
        // again: the second round under the schedule's faults too.
        "SELECT id FROM items ORDER BY hybrid(text_match(body, 'graph search vector index \
         segment fusion rank'), embedding <=> [0.5, 0.5, 0.5, 1.0], method => 'rrf', k => 30) \
         LIMIT 10",
        "SELECT id FROM items ORDER BY hybrid(text_match(body, 'rank segment'), method => \
         'linear') LIMIT 8",
    ];

    /// Everything a caller can see, with floats compared bit for bit.
    fn shape(r: &QueryResult) -> Vec<(String, String, Option<u32>, Option<u32>)> {
        r.rows
            .iter()
            .map(|row| {
                (
                    row.key.clone(),
                    crate::json::to_string(&row.doc),
                    row.score.map(f32::to_bits),
                    row.distance.map(f32::to_bits),
                )
            })
            .collect()
    }

    fn owner(key: &str) -> usize {
        // `t<n>\u{1}...`: the tenant number is the shard, by the fixture's splits.
        key[1..2].parse().unwrap()
    }

    #[test]
    fn a_seeded_run_reproduces_its_trace_and_its_answers() {
        let dir = tmp("seeded");
        let mut db = fixture(&dir);
        let sim = Sim::new(7, Faults::new(300, 200, true));
        db.install_sim(sim.clone());
        let run = |db: &mut Db| -> (Vec<String>, Vec<Event>) {
            let mut out = Vec::new();
            for q in QUERIES {
                let r = db.query(&format!("{q} WITH (partial_results)"));
                out.push(match r {
                    Ok(r) => format!("{:?} missing={:?}", shape(&r), r.missing),
                    Err(e) => format!("ERR {e}"),
                });
            }
            (out, sim.trace())
        };
        let first = run(&mut db);
        assert!(first.1.iter().any(|e| e.fate == Fate::Dropped), "the schedule dropped nothing");
        assert!(
            first.1.iter().any(|e| e.fate == Fate::Restarted),
            "the schedule restarted nothing"
        );
        sim.reset();
        let second = run(&mut db);
        assert_eq!(first.1, second.1, "the same seed took different decisions");
        assert_eq!(first.0, second.0, "the same decisions gave different answers");

        let other = Sim::new(8, Faults::new(300, 200, true));
        db.install_sim(other.clone());
        for q in QUERIES {
            let _ = db.query(&format!("{q} WITH (partial_results)"));
        }
        assert_ne!(first.1, other.trace(), "a different seed took the same decisions");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Without `partial_results`, a faulted statement is refused or answered
    /// exactly as with no faults — never answered differently. Twenty seeds
    /// over every query shape, with drops and restarts both in the schedule,
    /// and the test says how many of each outcome it saw so that a schedule
    /// that faulted nothing cannot pass it.
    #[test]
    fn a_fault_cannot_change_an_answer_without_saying_so() {
        let dir = tmp("refuse-or-agree");
        let mut db = fixture(&dir);
        let want: Vec<_> = QUERIES.iter().map(|q| shape(&db.query(q).unwrap())).collect();
        let (mut refused, mut agreed) = (0, 0);
        for seed in 1..=20u64 {
            let sim = Sim::new(seed, Faults::new(250, 250, true));
            db.install_sim(sim);
            for (q, want) in QUERIES.iter().zip(&want) {
                match db.query(q) {
                    Ok(r) => {
                        assert_eq!(&shape(&r), want, "seed {seed}: `{q}` answered differently");
                        assert!(r.missing.is_empty(), "nothing is missing without partial_results");
                        agreed += 1;
                    }
                    Err(Error::Deadline(_)) => refused += 1,
                    Err(e) => panic!("seed {seed}: `{q}`: {e}"),
                }
            }
        }
        assert!(refused > 0 && agreed > 0, "refused {refused}, agreed {agreed}: nothing measured");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With `partial_results`, `missing` names exactly the shards the schedule
    /// dropped a call from, and every row is a real one. "No row from a
    /// missing shard" is deliberately NOT the claim: a shard can answer the
    /// candidate call and then stop answering fetches, and the rows it
    /// already contributed are correct rows -- the contract of the option is
    /// that some of a missing shard's rows may be absent, not that all are.
    #[test]
    fn a_partial_answer_names_every_shard_that_did_not_answer_and_carries_only_real_rows() {
        let dir = tmp("partial");
        let mut db = fixture(&dir);
        let want: Vec<_> = QUERIES.iter().map(|q| shape(&db.query(q).unwrap())).collect();
        let corpus: BTreeSet<String> = db
            .query("SELECT id FROM items LIMIT 1000")
            .unwrap()
            .rows
            .iter()
            .map(|r| r.key.clone())
            .collect();
        assert_eq!(corpus.len(), 87, "ninety documents, three deleted");
        let mut seen_partial = 0;
        for seed in 1..=20u64 {
            let sim = Sim::new(seed, Faults::new(250, 100, true));
            db.install_sim(sim.clone());
            // A term no statement has named yet, so the statistics cache
            // has to gather it and the gather can lose a shard; the cached
            // ones never reach the boundary again once they are warm.
            let cold = format!(
                "SELECT id FROM items ORDER BY hybrid(text_match(body, 'graph novel{seed}'), \
                 method => 'linear') LIMIT 10"
            );
            for q in QUERIES.iter().copied().chain(std::iter::once(cold.as_str())) {
                let before = sim.trace().len();
                let r = db.query(&format!("{q} WITH (partial_results)")).unwrap();
                let dropped: BTreeSet<usize> = sim.trace()[before..]
                    .iter()
                    .filter(|e| e.fate == Fate::Dropped)
                    .map(|e| e.shard)
                    .collect();
                let missing: BTreeSet<usize> = r
                    .missing
                    .iter()
                    .map(|m| m.trim_start_matches("shard ").parse().unwrap())
                    .collect();
                assert_eq!(missing, dropped, "seed {seed}: `{q}`");
                // A shard the statement gave up on is not asked again by
                // any later stage of the same statement.
                let events = &sim.trace()[before..];
                for (i, e) in events.iter().enumerate() {
                    if e.fate == Fate::Dropped {
                        assert!(
                            events[i + 1..].iter().all(|later| later.shard != e.shard),
                            "seed {seed}: `{q}` called shard {} again after giving up on it: {events:?}",
                            e.shard
                        );
                    }
                }
                let keys: Vec<&str> = r.rows.iter().map(|row| row.key.as_str()).collect();
                let distinct: BTreeSet<&str> = keys.iter().copied().collect();
                assert_eq!(keys.len(), distinct.len(), "seed {seed}: `{q}` repeated a row");
                for k in &keys {
                    assert!(
                        corpus.contains(*k),
                        "seed {seed}: `{q}` answered a row that does not exist: {k}"
                    );
                    assert!(owner(k) < 3);
                }
                if !missing.is_empty() {
                    seen_partial += 1;
                }
                if q == cold.as_str() && !missing.is_empty() {
                    // The gather that just lost a shard must not have
                    // written its partial sum: with the faults gone, a
                    // statement whose terms are all cached reads the
                    // corpus size from the cache, and a text-only linear
                    // score is that number bit for bit.
                    db.remove_sim();
                    let text_only = QUERIES.len() - 1;
                    assert_eq!(
                        &shape(&db.query(QUERIES[text_only]).unwrap()),
                        &want[text_only],
                        "seed {seed}: the cache holds a partial sum"
                    );
                    db.install_sim(sim.clone());
                }
            }
        }
        assert!(seen_partial > 0, "no statement was partial, so nothing was measured");
        // Everything dropped: each shard is asked exactly once per statement,
        // at the first stage that reaches it, and never again by a later
        // one -- the statistics under `exact_scoring`, or a prefix, or the
        // scan. Then the answer is empty and names every shard.
        let all = Sim::new(99, Faults::new(1000, 0, false));
        db.install_sim(all.clone());
        for q in QUERIES {
            let before = all.trace().len();
            let r = db.query(&format!("{q} WITH (partial_results, exact_scoring)")).unwrap();
            let events = &all.trace()[before..];
            let shards: BTreeSet<usize> = events.iter().map(|e| e.shard).collect();
            assert_eq!(events.len(), 3, "`{q}`: a shard was asked again after a drop: {events:?}");
            assert_eq!(shards.len(), 3, "{events:?}");
            assert!(r.rows.is_empty(), "`{q}` answered rows with every shard gone");
            assert_eq!(r.missing, vec!["shard 0", "shard 1", "shard 2"], "`{q}`");
        }
        // A statistics fill that lost a shard served its own statement and
        // was never written to the cache: with the faults gone, every
        // statement answers exactly what it answered before any fault.
        db.remove_sim();
        for (q, want) in QUERIES.iter().zip(&want) {
            assert_eq!(&shape(&db.query(q).unwrap()), want, "`{q}`: the cache holds a partial sum");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every call crashes the shard first, so every answer comes from a
    /// replacement that opened the directory: sealed rows through the
    /// manifest, unsealed ones through the WAL, deletes on both sides. The
    /// answers are bit-identical to the live shards'.
    /// The walk's two calls, `expand` and `present`, are faulted like every
    /// other. Without `partial_results` a faulted walk is refused or answers
    /// exactly as with no faults; with it, every row is inside the unfaulted
    /// neighbourhood -- a dropped `expand` loses edges and can never invent
    /// one -- and a short answer names a shard, of either collection, that
    /// did not answer. The schedule has to have dropped a walk call for the
    /// test to mean anything, and it says so.
    #[test]
    fn a_faulted_walk_refuses_or_agrees_and_a_partial_one_says_so() {
        let dir = tmp("walk-faults");
        let mut db = graph_fixture(&dir);
        let statements: Vec<String> = WALKS
            .iter()
            .map(|(w, rest)| format!("SELECT id FROM items WHERE {w} {rest}"))
            .collect();
        let want: Vec<_> = statements.iter().map(|q| shape(&db.query(q).unwrap())).collect();
        let hoods: Vec<BTreeSet<String>> = WALKS
            .iter()
            .map(|(w, _)| {
                db.query(&format!("SELECT id FROM items WHERE {w} LIMIT 1000"))
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|r| r.key)
                    .collect()
            })
            .collect();
        for (w, h) in want.iter().zip(&hoods) {
            assert!(!w.is_empty() && h.len() >= w.len(), "a walk with no answer measures nothing");
        }
        let (mut refused, mut agreed, mut short) = (0, 0, 0);
        let (mut dropped_expand, mut dropped_present) = (0, 0);
        // Two hundred seeds, not twenty: the fault this has to reach is a
        // dropped `present` on exactly the shard holding a deleted node at
        // the hop that reaches it -- the one that, kept "unverified", walks
        // through the dead node and lengthens the answer -- and at a rate
        // low enough for "agreed" to be reachable that is a few seeds in a
        // hundred. The schedule is a function of the seed, so once a seed
        // reaches it, it always does.
        for seed in 1..=200u64 {
            // A walk makes a call per shard of both collections per hop
            // before the scatter even starts, so at the rate the other
            // properties use every statement is refused and "agreed" is
            // never measured; a lower rate leaves both outcomes reachable.
            let sim = Sim::new(seed, Faults::new(30, 30, true));
            db.install_sim(sim.clone());
            for ((q, want), hood) in statements.iter().zip(&want).zip(&hoods) {
                match db.query(q) {
                    Ok(r) => {
                        assert_eq!(&shape(&r), want, "seed {seed}: `{q}` answered differently");
                        assert!(r.missing.is_empty());
                        agreed += 1;
                    }
                    Err(Error::Deadline(_)) => refused += 1,
                    Err(e) => panic!("seed {seed}: `{q}`: {e}"),
                }
                let partial = match q.strip_suffix(')') {
                    Some(head) if q.contains(" WITH (") => format!("{head}, partial_results)"),
                    _ => format!("{q} WITH (partial_results)"),
                };
                let r = db.query(&partial).unwrap();
                for row in &r.rows {
                    assert!(hood.contains(&row.key), "seed {seed}: `{q}` invented {}", row.key);
                }
                if r.rows.len() < want.len() {
                    assert!(!r.missing.is_empty(), "seed {seed}: `{q}` is short and says nothing");
                    short += 1;
                }
            }
            let trace = sim.trace();
            dropped_expand +=
                trace.iter().filter(|e| e.fate == Fate::Dropped && e.call == "expand").count();
            dropped_present +=
                trace.iter().filter(|e| e.fate == Fate::Dropped && e.call == "present").count();
        }
        assert!(
            refused > 0 && agreed > 0 && short > 0 && dropped_expand > 0 && dropped_present > 0,
            "refused {refused}, agreed {agreed}, short {short}, dropped expand {dropped_expand}, \
             dropped present {dropped_present}: nothing measured"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_shard_that_restarted_answers_exactly_what_it_did_before() {
        let dir = tmp("restart");
        let mut db = fixture(&dir);
        let want: Vec<_> = QUERIES.iter().map(|q| shape(&db.query(q).unwrap())).collect();
        assert!(want.iter().all(|w| !w.is_empty()));
        let sim = Sim::new(3, Faults::new(0, 1000, false));
        db.install_sim(sim.clone());
        let loads_before = db.residency().loads();
        for (q, want) in QUERIES.iter().zip(&want) {
            let r = db.query(q).unwrap();
            assert_eq!(&shape(&r), want, "`{q}` answered differently after a restart");
            assert!(r.missing.is_empty());
        }
        let trace = sim.trace();
        assert!(trace.iter().all(|e| e.fate == Fate::Restarted), "{trace:?}");
        // And the replacements did the answering: they open cold, so their
        // components are loaded to answer, where the live shards' were warm
        // from the reference run and would load nothing.
        assert!(
            db.residency().loads() > loads_before,
            "no component was loaded, so the live shards answered, not the replacements"
        );
        // The restarts were real: each opened a shard from the directory,
        // rather than the live shard answering under another name.
        assert_eq!(sim.restarts(), trace.len() as u64, "{} restarts opened nothing", trace.len());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The order shards answer in changes nothing a caller sees, including
    /// the plan, which lists shards by their index whatever order they were
    /// asked in.
    #[test]
    fn the_order_shards_answer_in_does_not_change_the_answer() {
        let dir = tmp("reorder");
        let mut db = fixture(&dir);
        let want: Vec<_> = QUERIES.iter().map(|q| shape(&db.query(q).unwrap())).collect();
        let sim = Sim::new(11, Faults::new(0, 0, true));
        db.install_sim(sim.clone());
        let mut permuted = false;
        for _ in 0..8 {
            for (q, want) in QUERIES.iter().zip(&want) {
                let r = db.query(q).unwrap();
                assert_eq!(&shape(&r), want, "`{q}` answered differently in another order");
                let plan = match db.execute(&format!("EXPLAIN ANALYZE {q}")).unwrap() {
                    crate::engine::Outcome::Explain(text) => text,
                    other => panic!("{other:?}"),
                };
                let idx: Vec<usize> = plan
                    .lines()
                    .filter_map(|l| l.strip_prefix("  shard "))
                    .map(|l| {
                        l.split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
                    })
                    .collect();
                assert_eq!(idx.len(), 3, "{plan}");
                let mut sorted = idx.clone();
                sorted.sort();
                assert_eq!(idx, sorted, "the plan lists shards out of order:\n{plan}");
            }
            permuted |= sim.order(3) != vec![0, 1, 2];
        }
        assert!(permuted, "the schedule never permuted anything");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
