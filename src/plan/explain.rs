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
    pub manifest_version: u64,
    pub units: Vec<UnitExplain>,
    pub micros: u128,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Explain {
    pub statement: String,
    pub snapshot_ts: u64,
    pub k_prime: usize,
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
}

impl Explain {
    pub fn render(&self) -> String {
        let mut o = String::new();
        let shards_scanned = self.shards.iter().filter(|s| !s.pruned).count();
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
            "  scatter: {} of {} shard(s) scanned, {} pruned by partition key\n",
            shards_scanned,
            self.shards.len(),
            self.shards.len() - shards_scanned
        ));
        o.push_str(&format!(
            "  term statistics: {}\n",
            if self.stats_exact { "exact (two-phase)" } else { "cached approximate" }
        ));
        for s in &self.shards {
            if s.pruned {
                o.push_str(&format!(
                    "  shard {}: PRUNED ({})\n",
                    s.index,
                    s.prune_reason.as_deref().unwrap_or("out of key range")
                ));
                continue;
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
