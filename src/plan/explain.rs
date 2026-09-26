//! `EXPLAIN ANALYZE` output.
//!
//! The runtime decisions in §5.3 (which filtered-vector strategy), §6 (how much
//! `k` was amplified and whether a re-probe fired) and §8.4 (whether a hedge
//! fired) are **invisible without this** (§12.1). A query that got slow or lost
//! recall did so because of one of them, so every one is recorded at the point
//! it is made and printed here.

use crate::plan::fusion::FusionExplain;
use crate::vector::VectorReport;

/// Which machinery the unit actually ran for one text leaf.
///
/// A discriminant rather than a string built at the push site, because the
/// renderer is the thing that was wrong: it formatted every text line as
/// "block-max WAND, terms=…, candidates=…", including the filter path, which
/// runs [`scorer::evaluate_to_bitmap`](crate::text::scorer::evaluate_to_bitmap)
/// — a bare `advance` loop with no scoring, no max-score pivot and no
/// threshold. A plan that names an algorithm the query did not enter is worse
/// than no plan, because it is the document someone debugging a slow query
/// reasons from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TextStrategy {
    /// Ranked: block-max WAND over the scored terms, keeping the top `k'`.
    /// `matched` is the candidates it kept.
    #[default]
    Wand,
    /// A `text_match` predicate: a full bitmap evaluation, no pruning and no
    /// scores. It has no scored terms to list, and `matched` is survivors.
    Filter,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TextExplain {
    pub source: String,
    pub strategy: TextStrategy,
    pub terms: Vec<String>,
    /// Candidates kept on [`TextStrategy::Wand`], survivors on
    /// [`TextStrategy::Filter`]. One number, two honest names, and the
    /// renderer prints whichever one this unit earned.
    pub matched: usize,
    /// A `foo*` that hit the expansion cap, as seen by THIS unit. Normally the
    /// coordinator's verdict on the whole collection, repeated by every unit
    /// that compiled the query; on the no-coordinator path it is the unit's own
    /// dictionary being cut, which is the only place that fact is visible.
    ///
    /// The query-level statement lives on
    /// [`QueryResult::truncated_prefixes`](crate::plan::exec::QueryResult),
    /// which an ordinary query sees without asking for a plan. Silently
    /// returning fewer results is worse than saying so, and saying so only
    /// under `EXPLAIN ANALYZE` was most of the way to silence.
    pub prefix_truncated: bool,
    pub stats_exact: bool,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct UnitExplain {
    pub label: String,
    pub docs: usize,
    pub visible: usize,
    pub survivors: usize,
    pub selectivity: f64,
    /// Which access path each predicate took, in the order they ran.
    pub access_paths: Vec<String>,
    pub text: Vec<TextExplain>,
    pub vector: Vec<(String, VectorReport)>,
    pub micros: u128,
    /// Components this unit had to decode, and how many of those came from the
    /// archive. Zero on a warm unit; non-zero is where a p99 outlier lives.
    pub loads: u64,
    pub faults: u64,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ShardExplain {
    pub index: usize,
    pub pruned: bool,
    pub prune_reason: Option<String>,
    /// Answered from the shard's live count, not scanned: a plain
    /// `count(*)`.
    pub counted: bool,
    pub manifest_version: u64,
    pub units: Vec<UnitExplain>,
    pub micros: u128,
    pub timed_out: bool,
    /// The block as the holder rendered it, for a shard on another node:
    /// the same text `render_shard` produces here, carried across the wire
    /// instead of the units behind it.
    pub rendered: Option<String>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Explain {
    pub statement: String,
    pub snapshot_ts: u64,
    /// Each source's depth over the collection: what its merged list is
    /// cut to before the fusion.
    pub k_prime: usize,
    /// The depth each shard was asked for per source in the first round
    /// (`exec::shard_depth`); `k_prime` when nothing was shaped.
    pub shard_depth: usize,
    /// The shards asked again at the full depth, because their first
    /// answer could not vouch for the top (`exec::uncertified`).
    pub reasked: Vec<usize>,
    pub limit: usize,
    pub offset: usize,
    pub exact_mode: bool,
    /// The statement's budget, or `None` under `WITH (no_deadline)` or a
    /// `Db` with none. In the plan because a refusal for running out is
    /// otherwise the first a reader hears of it.
    pub deadline_ms: Option<u64>,
    pub stats_exact: bool,
    pub shards: Vec<ShardExplain>,
    pub fusion: Option<FusionExplain>,
    pub collapse: Option<(String, usize)>,
    pub fetched_payloads: usize,
    pub total_micros: u128,
    pub fetch_micros: u128,
    pub missing: Vec<String>,
    pub notes: Vec<String>,
    /// The statement went whole to one other node, as one call, and this
    /// is that node and the plan as it rendered it there: every shard
    /// the statement reached was on it and none here.
    pub forwarded: Option<(String, String)>,
    /// Each `WITHIN k HOPS OF` of the statement, as the coordinator walked
    /// it before the scatter: the frontier after every hop and what cut it.
    pub walks: Vec<WalkExplain>,
}

/// One walk of the statement, resolved before the scatter.
#[derive(Debug, Clone, Default)]
pub struct WalkExplain {
    /// The clause as written.
    pub label: String,
    /// The adjacency index it read.
    pub index: String,
    /// `outgoing`, `reverse` or `both directions`.
    pub direction: String,
    pub hops: Vec<HopExplain>,
    /// Keys in the final set: every node in `1..k` hops, the start excluded.
    pub keys: usize,
    pub micros: u128,
}

/// One hop of a walk.
#[derive(Debug, Clone, Default)]
pub struct HopExplain {
    pub hop: usize,
    /// Keys expanded from.
    pub expanded: usize,
    /// Edges followed, after the fan-out cap.
    pub edges: usize,
    /// Keys reached that no earlier hop had, before the frontier cap.
    pub found: usize,
    /// Keys reached that name no live document: skipped, and counted here.
    pub dangling: usize,
    /// Keys the next hop expands from: found, capped, live.
    pub frontier: usize,
    /// Which cap bound this hop, one line each; empty for none.
    pub cut: Vec<String>,
    /// Time in the edge shards' `expand` calls and the coordinator's merge.
    pub expand_micros: u128,
    /// Time in the node shards' `present` calls.
    pub check_micros: u128,
    /// Units that had no adjacency region -- a memtable, or a segment sealed
    /// before the index was declared -- and were scanned instead of probed.
    pub scanned: usize,
    /// `(i, n)` when the walk has one edge filter per hop and this hop used
    /// the i-th of n; `None` for no filter or one for every hop.
    pub filter: Option<(usize, usize)>,
}

impl Explain {
    pub fn render(&self) -> String {
        if let Some((url, plan)) = &self.forwarded {
            let mut o = format!(
                "Query plan  (forwarded whole to {url} as one call, {:.2} ms: every shard it \
                 reaches is there)\n",
                self.total_micros as f64 / 1000.0
            );
            for line in plan.lines() {
                o.push_str("  ");
                o.push_str(line);
                o.push('\n');
            }
            return o;
        }
        let mut o = String::new();
        let shards_counted = self.shards.iter().filter(|s| s.counted).count();
        let shards_scanned = self.shards.iter().filter(|s| !s.pruned && !s.counted).count();
        o.push_str(&format!(
            "Query plan  (snapshot ts={}, limit={}, k'={}, deadline={}{})\n",
            self.snapshot_ts,
            self.limit,
            self.k_prime,
            match self.deadline_ms {
                Some(ms) => format!("{ms} ms"),
                None => "none".to_string(),
            },
            if self.exact_mode { ", EXACT MODE" } else { "" }
        ));
        o.push_str(&format!(
            "  scatter: {} of {} shard(s) scanned, {} pruned by partition key{}\n",
            shards_scanned,
            self.shards.len(),
            self.shards.len() - shards_scanned - shards_counted,
            if shards_counted > 0 {
                format!(", {shards_counted} counted, not scanned")
            } else {
                String::new()
            }
        ));
        if self.shard_depth > 0 && self.shard_depth < self.k_prime {
            o.push_str(&format!(
                "  candidates: k'={} per shard for each source's top {} over the collection{}\n",
                self.shard_depth,
                self.k_prime,
                if self.reasked.is_empty() {
                    String::new()
                } else {
                    format!(
                        "; asked again at {}: {}",
                        self.k_prime,
                        self.reasked
                            .iter()
                            .map(|s| format!("shard {s}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            ));
        }
        o.push_str(&format!(
            "  term statistics: {}\n",
            if self.stats_exact { "exact (two-phase)" } else { "cached approximate" }
        ));
        for w in &self.walks {
            o.push_str(&format!("  walk: {} (index {}, {})\n", w.label, w.index, w.direction));
            for h in &w.hops {
                let mut notes = String::new();
                if let Some((i, n)) = h.filter {
                    notes.push_str(&format!("; edge filter {i} of {n}"));
                }
                if h.scanned > 0 {
                    notes
                        .push_str(&format!("; {} unit(s) scanned: no adjacency region", h.scanned));
                }
                o.push_str(&format!(
                    "    hop {}: {} key(s) expanded over {} edge(s): {} new, {} dangling, \
                     frontier {}{} (expand {:.2} ms, check {:.2} ms{})\n",
                    h.hop,
                    h.expanded,
                    h.edges,
                    h.found,
                    h.dangling,
                    h.frontier,
                    if h.cut.is_empty() {
                        String::new()
                    } else {
                        format!("; CUT: {}", h.cut.join(", "))
                    },
                    h.expand_micros as f64 / 1000.0,
                    h.check_micros as f64 / 1000.0,
                    notes
                ));
            }
            o.push_str(&format!(
                "    {} key(s) in 1..{} hop(s), {:.2} ms\n",
                w.keys,
                w.hops.len(),
                w.micros as f64 / 1000.0
            ));
        }
        for s in &self.shards {
            o.push_str(&render_shard(s));
        }
        if let Some(f) = &self.fusion {
            o.push_str(&format!(
                "  fusion at coordinator: method={} sources={:?} candidates={:?} union={}\n",
                f.method, f.sources, f.candidates_per_source, f.union_size
            ));
            if !f.normalisation.is_empty() {
                o.push_str(&format!(
                    "    normalisation (once, over the merged set): {:?}\n",
                    f.normalisation
                ));
            }
        }
        if let Some((path, amp)) = &self.collapse {
            o.push_str(&format!("  collapse by {path}: k amplified {amp}x\n"));
        }
        if self.offset > 0 {
            o.push_str(&format!(
                "  OFFSET {}: costs k+n = {} per shard; prefer a cursor for deep pagination\n",
                self.offset,
                self.limit + self.offset
            ));
        }
        o.push_str(&format!(
            "  fetch: {} payload(s) from winning shards only, {:.2} ms\n",
            self.fetched_payloads,
            self.fetch_micros as f64 / 1000.0
        ));
        if !self.missing.is_empty() {
            o.push_str(&format!("  PARTIAL RESULTS: missing {:?}\n", self.missing));
        }
        for n in &self.notes {
            o.push_str(&format!("  note: {n}\n"));
        }
        o.push_str(&format!("  total: {:.2} ms\n", self.total_micros as f64 / 1000.0));
        o
    }
}

/// One shard's block of the plan. Public because a holder renders its own
/// shard's block with it and sends the text: the coordinator then prints
/// what it would have printed had the shard been local.
pub fn render_shard(s: &ShardExplain) -> String {
    if let Some(text) = &s.rendered {
        return text.clone();
    }
    let mut o = String::new();
    if s.pruned {
        o.push_str(&format!(
            "  shard {}: PRUNED ({})\n",
            s.index,
            s.prune_reason.as_deref().unwrap_or("out of key range")
        ));
        return o;
    }
    if s.counted {
        o.push_str(&format!(
            "  shard {} (manifest v{}): counted, not scanned\n",
            s.index, s.manifest_version
        ));
        return o;
    }
    o.push_str(&format!(
        "  shard {} (manifest v{}, {:.2} ms{}):\n",
        s.index,
        s.manifest_version,
        s.micros as f64 / 1000.0,
        if s.timed_out { ", DEADLINE EXCEEDED" } else { "" }
    ));
    for u in &s.units {
        o.push_str(&format!(
            "    {:<14} docs={:<7} visible={:<7} survivors={:<7} s={:.4}  {:.2} ms\n",
            u.label,
            u.docs,
            u.visible,
            u.survivors,
            u.selectivity,
            u.micros as f64 / 1000.0
        ));
        if !u.access_paths.is_empty() {
            o.push_str(&format!("      filter: {}\n", u.access_paths.join(" -> ")));
        }
        if u.loads > 0 {
            o.push_str(&format!(
                "      residency: {} component(s) decoded on demand{}\n",
                u.loads,
                if u.faults > 0 {
                    format!(", {} faulted in from the archive", u.faults)
                } else {
                    String::new()
                }
            ));
        }
        for t in &u.text {
            let cut = if t.prefix_truncated { ", PREFIX EXPANSION TRUNCATED" } else { "" };
            o.push_str(&match t.strategy {
                TextStrategy::Wand => format!(
                    "      text[{}]: block-max WAND, terms={:?}, candidates={}{}\n",
                    t.source, t.terms, t.matched, cut
                ),
                TextStrategy::Filter => format!(
                    "      text[{}]: bitmap evaluation, survivors={}{}\n",
                    t.source, t.matched, cut
                ),
            });
        }
        for (name, v) in &u.vector {
            o.push_str(&format!(
                    "      vector[{}]: strategy={} tier={} s={:.4} survivors={} ef={} amp={:.1}x visits={}{} reranked={} reprobes={}\n",
                    name,
                    v.strategy.map(|s| s.name()).unwrap_or("none"),
                    v.tier.map(|t| format!("{t:?}")).unwrap_or_else(|| "-".into()),
                    v.selectivity,
                    v.survivors,
                    v.ef_used,
                    v.amplification,
                    v.visits,
                    v.budget.map(|b| format!(" budget={b}")).unwrap_or_default(),
                    v.reranked,
                    v.reprobes
                ));
        }
    }
    o
}
