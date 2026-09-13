//! BM25 scoring and block-max WAND.
//!
//! Two properties this file exists to guarantee:
//!
//! 1. **The structured filter is an admission predicate, not a post-filter.**
//!    Filtering and scoring interleave (§5.1): a candidate outside the bitmap
//!    is never scored, and the scorer jumps directly to the next admitted
//!    ordinal rather than walking the gap.
//! 2. **`idf` comes from global statistics supplied per query** (§8.2), never
//!    from the segment. Scores are only mergeable across shards if every shard
//!    scored against the same numbers, and the block-max upper bounds are
//!    therefore computed at query time from stored `(max_tf, min_dl)` rather
//!    than baked in at write time.

use std::collections::BTreeMap;

use crate::bitmap::Bitmap;
use crate::error::{Error, Result};
use crate::text::postings::EXHAUSTED;
use crate::text::query::TextQuery;
use crate::text::{PostingsRef, TextSource};

#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Bm25Params { k1: 1.2, b: 0.75 }
    }
}

/// What one `foo*` leaf resolved to at the coordinator: the terms, and whether
/// the collection had more of them than [`PREFIX_EXPANSION_LIMIT`] allows.
///
/// The two travel together because they are one fact. The terms alone cannot
/// say whether anything was dropped — a list exactly `PREFIX_EXPANSION_LIMIT`
/// long is what both "the cap cut this" and "the collection happens to hold
/// exactly that many" look like, and reading the cut off the length is the
/// off-by-one this type exists to make unrepresentable. Only the coordinator
/// can tell them apart, because only the coordinator saw the union.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Expansion {
    /// The terms the query names: the lexicographically first, capped, LIVE
    /// matching terms of the collection at the query's snapshot. A term no
    /// visible document holds is not here, and — the part that took a
    /// measurement to get right — it did not occupy a slot either.
    pub terms: Vec<String>,
    /// The collection matched more LIVE terms than `terms` carries, so the
    /// answer really is short. Reported on
    /// [`QueryResult`](crate::plan::exec::QueryResult).
    ///
    /// Live, not physical, and that is what makes the report actionable: a
    /// verdict taken over the physical dictionary fires when a collection with
    /// a few hundred live terms happens to be carrying dead ones, and tells a
    /// caller holding a COMPLETE answer that it is incomplete. A warning
    /// nobody can act on is worse than silence.
    pub truncated: bool,
    /// How the statement used this prefix, which decides what a cut COST.
    pub used: PrefixUse,
}

/// The polarity (or polarities) one prefix leaf was written in, and what a cut
/// of its expansion therefore COSTS.
///
/// Two pairs, not one, because spelling and consequence are different facts and
/// a SQL `NOT` separates them. `NOT text_match(body, 'a*')` is spelled `a*` —
/// that is the only string the caller can find in their own statement — but it
/// behaves as an exclusion, so cutting its expansion keeps rows rather than
/// losing them. Deriving the consequence from the spelling reports the inverse
/// of what happened for exactly that statement.
///
/// Either pair may have both flags set: one statement may spell `a*` in one
/// clause and `-a*` in another, they resolve to the same term list, and
/// truncating it then does both kinds of damage at once.
///
/// `#[non_exhaustive]` because the consequence pair was added after the
/// spelling pair, and the next distinction should not need a third release.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PrefixUse {
    /// Written WITHOUT a leading `-` inside the `text_match` string. Says how
    /// to render the leaf, not what a cut costs — see `loses_rows`.
    pub positive: bool,
    /// Written WITH a leading `-` inside the `text_match` string.
    pub negated: bool,
    /// The leaf matches, so cutting its expansion LOSES rows: the answer is
    /// short. True when the spelling and the enclosing SQL agree in sign — a
    /// plain `a*`, or `NOT text_match(body, '-a*')`.
    pub loses_rows: bool,
    /// The leaf excludes, so cutting its expansion KEEPS rows the query asked
    /// to drop — the opposite consequence, which is why the two cannot share
    /// one message. True for `-a*` and equally for `NOT text_match(body,
    /// 'a*')`.
    pub keeps_rows: bool,
}

/// Collection-wide term statistics, gathered by the coordinator and attached
/// to the query for its own terms only (§8.1).
///
/// `#[non_exhaustive]` because this grew a field in a breaking release and the
/// next statistic should not need another one.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct GlobalStats {
    pub num_docs: u64,
    pub avg_doc_len: f64,
    pub doc_freq: BTreeMap<String, u64>,
    /// Prefix leaf -> what the coordinator resolved it to, pinned once for the
    /// whole query (§8.1). Present for every prefix in the statement when the
    /// query came through `Db::run_select`; empty when `compile` was called
    /// directly, which is the fallback `build` documents.
    ///
    /// This is what makes a prefix query mean the same thing at every shard
    /// count. Expanding per searchable unit applies the expansion cap per unit,
    /// so each one truncates at its own lexicographic cut and the union — the
    /// set of terms the query actually names — grows and shrinks with the
    /// flush and compaction schedule. Resolving it here is necessary but not
    /// sufficient: the enumeration must also be masked by the query's own
    /// snapshot, or the cut is over the PHYSICAL dictionary and the schedule
    /// gets back in through the dead terms it leaves in front of the live ones.
    pub expansions: BTreeMap<String, Expansion>,
    /// True when gathered exactly for this query (`WITH exact_scoring`), false
    /// when read from the periodically refreshed cache. Reported by
    /// `EXPLAIN ANALYZE` so a scoring anomaly can be attributed.
    pub exact: bool,
}

impl GlobalStats {
    /// IDF for a term the coordinator gathered a global `df` for. A term it
    /// never saw has `df == 0`, the most informative a term can be, which is
    /// the right answer for a term that occurs nowhere and is scored nowhere.
    pub fn idf(&self, term: &str) -> f32 {
        self.idf_for_df(self.doc_freq.get(term).copied().unwrap_or(0))
    }

    /// BM25's IDF against an externally supplied `df`, for the one caller that
    /// has a document frequency the coordinator did not gather (prefix
    /// expansion, see `build`).
    ///
    /// `num_docs` is taken from `self` rather than passed alongside `doc_freq`,
    /// and that is the entire point of the signature. Two adjacent `u64`
    /// parameters that nothing type-checks against each other are a trap: the
    /// arguments are interchangeable to the compiler, and exchanging them is
    /// silent — the result stays finite and positive, and even keeps rare
    /// terms above common ones, so it fails no smoke test while collapsing the
    /// weight of a term in 1 of 1000 documents from 6.50 to 0.29. One
    /// parameter has no order to get wrong. It also states the invariant this
    /// file exists to hold (§8.2): the document count always comes from the
    /// query's global statistics, never from a segment.
    ///
    /// The `df` is clamped to `num_docs`, so the log's argument never falls
    /// below one and the result is never negative.
    ///
    /// The clamp is not a guard against a caller's mistake, it is what the
    /// formula means at the boundary. IDF measures how much seeing a term
    /// narrows the collection down; a term in every document narrows it down
    /// by nothing, so `df == n` is the least informative a term can be and
    /// `ln(1 + 0.5/(n+0.5))` is the smallest value the formula can
    /// legitimately produce. "More documents hold the term than exist" is not
    /// more extreme than that, it is not a quantity at all — and the
    /// unclamped formula answers it by turning the score negative, which does
    /// not make the term merely uninformative, it makes every document that
    /// contains it rank *below* every document that does not.
    ///
    /// `df > n` reaches here whenever the two numbers come from different
    /// corpora: a prefix expansion has no global `df` and mixes the segment's
    /// own against the collection's `num_docs`, and any statistics source that
    /// counts postings and documents at different instants can do the same.
    pub fn idf_for_df(&self, doc_freq: u64) -> f32 {
        let n = self.num_docs.max(1);
        let df = doc_freq.min(n) as f64;
        let n = n as f64;
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln() as f32
    }
}

#[inline]
fn bm25(idf: f32, tf: f32, dl: f32, avgdl: f32, p: Bm25Params) -> f32 {
    let norm = 1.0 - p.b + p.b * (dl / avgdl.max(1e-6));
    idf * (tf * (p.k1 + 1.0)) / (tf + p.k1 * norm)
}

pub trait Scorer {
    fn doc(&self) -> u32;
    /// Move to the first matching ordinal >= `target`.
    fn advance(&mut self, target: u32) -> u32;
    fn score(&mut self) -> f32;
    /// Upper bound over the whole posting list.
    fn max_score(&self) -> f32;
    /// Upper bound valid up to [`block_last`](Self::block_last).
    fn block_max_score(&self) -> f32;
    fn block_last(&self) -> u32;
    /// Threshold propagation for WAND. A no-op for everything except a
    /// disjunction.
    fn set_threshold(&mut self, _t: f32) {}
}

pub struct TermScorer<'a> {
    cur: PostingsRef<'a>,
    idf: f32,
    doc_lens: &'a [u32],
    avgdl: f32,
    params: Bm25Params,
    max: f32,
}

impl<'a> TermScorer<'a> {
    pub fn new(
        cur: PostingsRef<'a>,
        idf: f32,
        doc_lens: &'a [u32],
        avgdl: f32,
        params: Bm25Params,
    ) -> Self {
        let max =
            bm25(idf, cur.global_max_tf() as f32, cur.global_min_dl().max(1) as f32, avgdl, params);
        TermScorer { cur, idf, doc_lens, avgdl, params, max }
    }

    fn dl(&self, ord: u32) -> f32 {
        self.doc_lens.get(ord as usize).copied().unwrap_or(1).max(1) as f32
    }
}

impl<'a> Scorer for TermScorer<'a> {
    fn doc(&self) -> u32 {
        self.cur.doc()
    }
    fn advance(&mut self, target: u32) -> u32 {
        self.cur.advance(target)
    }
    fn score(&mut self) -> f32 {
        let d = self.cur.doc();
        if d == EXHAUSTED {
            return 0.0;
        }
        bm25(self.idf, self.cur.tf() as f32, self.dl(d), self.avgdl, self.params)
    }
    fn max_score(&self) -> f32 {
        self.max
    }
    fn block_max_score(&self) -> f32 {
        bm25(
            self.idf,
            self.cur.block_max_tf() as f32,
            self.cur.block_min_dl().max(1) as f32,
            self.avgdl,
            self.params,
        )
    }
    fn block_last(&self) -> u32 {
        self.cur.block_last_ord()
    }
}

/// Disjunction with block-max WAND.
///
/// The pivot rule: sort children by current ordinal, walk the prefix summing
/// global upper bounds, and the first ordinal at which that running sum can
/// exceed the threshold is the pivot. Nothing before it can make the heap, so
/// everything before it is skipped rather than scored. The block-max refinement
/// then re-checks with per-block bounds, which are far tighter, and skips again
/// when they too fall short.
pub struct DisjunctionScorer<'a> {
    children: Vec<Box<dyn Scorer + 'a>>,
    cur: u32,
    threshold: f32,
    max: f32,
    started: bool,
}

impl<'a> DisjunctionScorer<'a> {
    pub fn new(children: Vec<Box<dyn Scorer + 'a>>) -> Self {
        let max = children.iter().map(|c| c.max_score()).sum();
        DisjunctionScorer { children, cur: 0, threshold: 0.0, max, started: false }
    }

    fn sort_by_doc(&mut self) {
        self.children.sort_by_key(|c| c.doc());
    }
}

impl<'a> Scorer for DisjunctionScorer<'a> {
    fn doc(&self) -> u32 {
        self.cur
    }

    fn advance(&mut self, target: u32) -> u32 {
        let mut target = target;
        loop {
            if !self.started {
                // Fresh cursors report EXHAUSTED until first positioned, which
                // is indistinguishable from a spent one — so the first move is
                // unconditional and every later one is not.
                for c in self.children.iter_mut() {
                    c.advance(target);
                }
                self.started = true;
            } else {
                for c in self.children.iter_mut() {
                    if c.doc() < target {
                        c.advance(target);
                    }
                }
            }
            self.sort_by_doc();
            if self.children.is_empty() || self.children[0].doc() == EXHAUSTED {
                self.cur = EXHAUSTED;
                return EXHAUSTED;
            }

            // Find the pivot: the shallowest prefix whose combined global bound
            // can beat the threshold.
            let mut sum = 0.0f32;
            let mut pivot = usize::MAX;
            for (i, c) in self.children.iter().enumerate() {
                if c.doc() == EXHAUSTED {
                    break;
                }
                sum += c.max_score();
                if sum > self.threshold {
                    pivot = i;
                    break;
                }
            }
            if pivot == usize::MAX {
                // No remaining combination can clear the threshold.
                self.cur = EXHAUSTED;
                return EXHAUSTED;
            }
            let pivot_doc = self.children[pivot].doc();
            // Extend the pivot over every cursor already sitting on pivot_doc.
            // They contribute to its real score, so leaving them out of the
            // block-max sum can prune a document that would have won: a rare,
            // heavily-weighted term beyond the pivot is exactly the case where
            // that matters, and exactly the case the index is there to serve.
            while pivot + 1 < self.children.len() && self.children[pivot + 1].doc() == pivot_doc {
                pivot += 1;
            }

            if self.children[0].doc() == pivot_doc {
                // All of 0..=pivot are aligned on pivot_doc. Re-check with the
                // tighter per-block bounds before committing to a full score.
                let bsum: f32 = self.children[..=pivot].iter().map(|c| c.block_max_score()).sum();
                if bsum > self.threshold {
                    self.cur = pivot_doc;
                    return pivot_doc;
                }
                let mut horizon = self.children[..=pivot]
                    .iter()
                    .map(|c| c.block_last())
                    .min()
                    .unwrap_or(pivot_doc);
                // The bound that justified this skip covers only the pivot
                // prefix. Past the next cursor's current document the other
                // lists start contributing, so the skip must stop there — or it
                // flies over documents where they do.
                if let Some(next) = self.children.get(pivot + 1) {
                    let d = next.doc();
                    if d != EXHAUSTED && d > 0 {
                        horizon = horizon.min(d - 1);
                    }
                }
                target = horizon.saturating_add(1).max(pivot_doc + 1);
            } else {
                target = pivot_doc;
            }
        }
    }

    fn score(&mut self) -> f32 {
        let d = self.cur;
        let mut s = 0.0;
        for c in self.children.iter_mut() {
            if c.doc() == d {
                s += c.score();
            }
        }
        s
    }

    fn max_score(&self) -> f32 {
        self.max
    }

    fn block_max_score(&self) -> f32 {
        self.children.iter().filter(|c| c.doc() != EXHAUSTED).map(|c| c.block_max_score()).sum()
    }

    fn block_last(&self) -> u32 {
        self.children
            .iter()
            .filter(|c| c.doc() != EXHAUSTED)
            .map(|c| c.block_last())
            .min()
            .unwrap_or(EXHAUSTED)
    }

    fn set_threshold(&mut self, t: f32) {
        self.threshold = t;
    }
}

/// Conjunction: leapfrog over children, score is the sum.
pub struct ConjunctionScorer<'a> {
    children: Vec<Box<dyn Scorer + 'a>>,
    cur: u32,
    /// Compound scorers report `doc() == 0` before they are first positioned,
    /// which is indistinguishable from "matches ordinal 0". Without this flag
    /// a conjunction accepts ordinal 0 from an unpositioned child and returns
    /// documents that match none of its terms.
    started: bool,
}

impl<'a> ConjunctionScorer<'a> {
    pub fn new(children: Vec<Box<dyn Scorer + 'a>>) -> Self {
        ConjunctionScorer { children, cur: 0, started: false }
    }
}

impl<'a> Scorer for ConjunctionScorer<'a> {
    fn doc(&self) -> u32 {
        self.cur
    }

    fn advance(&mut self, target: u32) -> u32 {
        if self.children.is_empty() {
            self.cur = EXHAUSTED;
            return EXHAUSTED;
        }
        let mut t = target;
        let mut first = !self.started;
        self.started = true;
        'outer: loop {
            for c in self.children.iter_mut() {
                let positioned = !first && c.doc() >= t && c.doc() != EXHAUSTED;
                let d = if positioned { c.doc() } else { c.advance(t) };
                if d == EXHAUSTED {
                    self.cur = EXHAUSTED;
                    return EXHAUSTED;
                }
                if d > t {
                    t = d;
                    // Every child has now been advanced at least once.
                    first = false;
                    continue 'outer;
                }
            }
            self.cur = t;
            return t;
        }
    }

    fn score(&mut self) -> f32 {
        self.children.iter_mut().map(|c| c.score()).sum()
    }
    fn max_score(&self) -> f32 {
        self.children.iter().map(|c| c.max_score()).sum()
    }
    fn block_max_score(&self) -> f32 {
        self.children.iter().map(|c| c.block_max_score()).sum()
    }
    fn block_last(&self) -> u32 {
        self.children.iter().map(|c| c.block_last()).min().unwrap_or(EXHAUSTED)
    }
}

/// Phrase matching over positions. Terms must appear at consecutive positions
/// in order. The [`ARRAY_POSITION_GAP`](crate::text::analyzer::ARRAY_POSITION_GAP)
/// inserted between array elements is what stops a phrase from matching across
/// element boundaries (§2.2).
pub struct PhraseScorer<'a> {
    cursors: Vec<PostingsRef<'a>>,
    idf: f32,
    doc_lens: &'a [u32],
    avgdl: f32,
    params: Bm25Params,
    cur: u32,
    cur_tf: u32,
    max: f32,
    started: bool,
}

impl<'a> PhraseScorer<'a> {
    pub fn new(
        cursors: Vec<PostingsRef<'a>>,
        idf: f32,
        doc_lens: &'a [u32],
        avgdl: f32,
        params: Bm25Params,
    ) -> Self {
        let max_tf = cursors.iter().map(|c| c.global_max_tf()).min().unwrap_or(1);
        let min_dl = cursors.iter().map(|c| c.global_min_dl()).min().unwrap_or(1);
        let max = bm25(idf, max_tf as f32, min_dl.max(1) as f32, avgdl, params);
        PhraseScorer {
            cursors,
            idf,
            doc_lens,
            avgdl,
            params,
            cur: 0,
            cur_tf: 0,
            max,
            started: false,
        }
    }

    fn phrase_freq(&self) -> u32 {
        let first = self.cursors[0].positions();
        if first.is_empty() {
            return 0;
        }
        let rest: Vec<Vec<u32>> = self.cursors[1..].iter().map(|c| c.positions()).collect();
        let mut count = 0;
        for &p in &first {
            let mut ok = true;
            for (i, positions) in rest.iter().enumerate() {
                let want = p + i as u32 + 1;
                if positions.binary_search(&want).is_err() {
                    ok = false;
                    break;
                }
            }
            if ok {
                count += 1;
            }
        }
        count
    }
}

impl<'a> Scorer for PhraseScorer<'a> {
    fn doc(&self) -> u32 {
        self.cur
    }

    fn advance(&mut self, target: u32) -> u32 {
        let mut t = target;
        let mut first = !self.started;
        self.started = true;
        'outer: loop {
            for c in self.cursors.iter_mut() {
                let positioned = !first && c.doc() >= t && c.doc() != EXHAUSTED;
                let d = if positioned { c.doc() } else { c.advance(t) };
                if d == EXHAUSTED {
                    self.cur = EXHAUSTED;
                    return EXHAUSTED;
                }
                if d > t {
                    t = d;
                    first = false;
                    continue 'outer;
                }
            }
            first = false;
            // All cursors aligned on t; now the positional check.
            let f = self.phrase_freq();
            if f > 0 {
                self.cur = t;
                self.cur_tf = f;
                return t;
            }
            t += 1;
        }
    }

    fn score(&mut self) -> f32 {
        if self.cur == EXHAUSTED {
            return 0.0;
        }
        let dl = self.doc_lens.get(self.cur as usize).copied().unwrap_or(1).max(1) as f32;
        bm25(self.idf, self.cur_tf as f32, dl, self.avgdl, self.params)
    }
    fn max_score(&self) -> f32 {
        self.max
    }
    fn block_max_score(&self) -> f32 {
        self.max
    }
    fn block_last(&self) -> u32 {
        self.cursors.iter().map(|c| c.block_last_ord()).min().unwrap_or(EXHAUSTED)
    }
}

/// A scorer over an explicit ordinal set with a constant contribution. Used for
/// nothing in v1's default path; kept because learned sparse vectors ride on
/// this index with weighted terms (§5.4) and land here.
pub struct BitmapScorer {
    ords: Vec<u32>,
    idx: usize,
    weight: f32,
}

impl BitmapScorer {
    pub fn new(bm: &Bitmap, weight: f32) -> Self {
        BitmapScorer { ords: bm.to_vec(), idx: 0, weight }
    }
}

impl Scorer for BitmapScorer {
    fn doc(&self) -> u32 {
        self.ords.get(self.idx).copied().unwrap_or(EXHAUSTED)
    }
    fn advance(&mut self, target: u32) -> u32 {
        let start = self.idx.min(self.ords.len());
        self.idx = start + self.ords[start..].partition_point(|&o| o < target);
        self.doc()
    }
    fn score(&mut self) -> f32 {
        self.weight
    }
    fn max_score(&self) -> f32 {
        self.weight
    }
    fn block_max_score(&self) -> f32 {
        self.weight
    }
    fn block_last(&self) -> u32 {
        self.ords.last().copied().unwrap_or(EXHAUSTED)
    }
}

/// Maximum number of dictionary terms a single `foo*` expands to. An unbounded
/// prefix on a large dictionary is a denial of service.
///
/// Applied by the coordinator to the UNION over every unit of every shard
/// (`Db::run_select`), so the terms a prefix names are the lexicographically
/// first `PREFIX_EXPANSION_LIMIT` of the LIVE matching vocabulary at the
/// query's snapshot — a property of the DATA, not of its layout. Applied per
/// unit, which is what the fallback arm of `build` still does, the union of
/// the per-unit cuts grows with the number of units and the query means
/// something different after a flush.
///
/// The vocabulary is the collection's, or — for a statement that names a
/// partition key equality — that PARTITION's. Expanding over the whole
/// collection for a partition-scoped statement spends the cap on terms no row
/// the statement can return holds, which expands a tenant out of its own
/// query; scoping it is still layout-independent, so everything below carries
/// over unchanged over the smaller live set.
///
/// "Live" is the second half of the same property and it is not decoration.
/// Each unit enumerates only terms a visible document still holds, and it
/// enumerates PAST the dead ones rather than counting them against the cap: a
/// dead term contributes no rows either way, but under a cap it would otherwise
/// displace a live term, which puts the compaction schedule back in the answer.
/// See [`crate::shard::Shard::prefix_terms`].
///
/// A wide prefix is therefore a PARTIAL answer, and the honest bound is
/// recall, not correctness: `a*` keeps the terms nearest the start of the
/// alphabet, which is arbitrary but stable. Silently returning fewer results
/// is worse than saying so, so the cut is reported on
/// [`QueryResult::truncated_prefixes`](crate::plan::exec::QueryResult) for
/// every query — ranked or filtered, `EXPLAIN ANALYZE` or not — and repeated
/// per unit in `EXPLAIN ANALYZE`.
///
/// Raising it is a recall dial with a real price. The rows a wide prefix
/// returns go roughly as `n · (1 − (1 − cap/V)^t)` for a collection of `n`
/// documents over a matching vocabulary of `V` terms with `t` of them per
/// document, so recall improves sub-linearly in the cap while the work — one
/// cursor per resolved term, in every unit — grows with it directly. The
/// "A wide `foo*` is a partial answer" note in `docs/design.md` carries the
/// measured numbers.
pub const PREFIX_EXPANSION_LIMIT: usize = 512;

pub struct Compiled<'a> {
    pub scorer: Option<Box<dyn Scorer + 'a>>,
    /// Ordinals excluded by negations that apply to the query as a whole.
    /// Negations nested inside a disjunct are applied to that disjunct instead
    /// — see [`ExcludingScorer`].
    pub excluded: Option<Bitmap>,
    pub prefix_truncated: bool,
    /// True when the query is nothing but negations, so the match set is
    /// "everything except". Meaningful as a filter, useless as a ranking
    /// source, and the two consumers differ accordingly.
    pub pure_negation: bool,
}

/// Wraps a scorer with an exclusion set scoped to it.
///
/// A `NOT` inside one branch of an `OR` must not filter the other branches. The
/// natural implementation — one exclusion bitmap threaded through the whole
/// compile — silently does exactly that, so `(alpha -beta) OR gamma` drops the
/// `gamma` documents that happen to contain `beta`.
pub struct ExcludingScorer<'a> {
    inner: Box<dyn Scorer + 'a>,
    excluded: Bitmap,
}

impl<'a> ExcludingScorer<'a> {
    pub fn new(inner: Box<dyn Scorer + 'a>, excluded: Bitmap) -> Self {
        ExcludingScorer { inner, excluded }
    }
}

impl<'a> Scorer for ExcludingScorer<'a> {
    fn doc(&self) -> u32 {
        self.inner.doc()
    }
    fn advance(&mut self, target: u32) -> u32 {
        let mut d = self.inner.advance(target);
        while d != EXHAUSTED && self.excluded.get(d as usize) {
            d = self.inner.advance(d + 1);
        }
        d
    }
    fn score(&mut self) -> f32 {
        self.inner.score()
    }
    fn max_score(&self) -> f32 {
        self.inner.max_score()
    }
    fn block_max_score(&self) -> f32 {
        self.inner.block_max_score()
    }
    fn block_last(&self) -> u32 {
        self.inner.block_last()
    }
    fn set_threshold(&mut self, t: f32) {
        self.inner.set_threshold(t)
    }
}

struct Built<'a> {
    scorer: Option<Box<dyn Scorer + 'a>>,
    /// Exclusions produced by this subtree, to be applied by whoever owns it.
    excluded: Bitmap,
    /// This subtree is a bare negation: it filters but generates nothing.
    negation_only: bool,
}

/// `vis` is the unit's visibility bitmap at the query's pinned `t`, and it is
/// required rather than optional because the one place it is read — the
/// no-coordinator prefix arm below — must use the SAME liveness
/// predicate, at the same instant, as `Shard::prefix_terms` and
/// `Shard::term_stats`. One liveness rule in the crate, not two.
pub fn compile<'a>(
    q: &TextQuery,
    src: &'a TextSource<'a>,
    vis: &Bitmap,
    stats: &GlobalStats,
    params: Bm25Params,
) -> Result<Compiled<'a>> {
    let avgdl = if stats.avg_doc_len > 0.0 { stats.avg_doc_len as f32 } else { 1.0 };
    let mut truncated = false;
    let b = build(q, src, vis, stats, params, avgdl, &mut truncated)?;
    Ok(Compiled {
        scorer: b.scorer,
        excluded: if b.excluded.is_empty() { None } else { Some(b.excluded) },
        prefix_truncated: truncated,
        pure_negation: b.negation_only,
    })
}

fn build<'a>(
    q: &TextQuery,
    src: &'a TextSource<'a>,
    vis: &Bitmap,
    stats: &GlobalStats,
    params: Bm25Params,
    avgdl: f32,
    truncated: &mut bool,
) -> Result<Built<'a>> {
    let n = src.num_docs() as usize;
    let empty = || Built { scorer: None, excluded: Bitmap::new(n), negation_only: false };
    Ok(match q {
        TextQuery::Empty => empty(),
        TextQuery::Term(t) => match src.try_cursor(t)? {
            None => empty(),
            Some(cur) => Built {
                scorer: Some(Box::new(TermScorer::new(
                    cur,
                    stats.idf(t),
                    src.doc_lens(),
                    avgdl,
                    params,
                ))),
                excluded: Bitmap::new(n),
                negation_only: false,
            },
        },
        TextQuery::Prefix(p) => {
            // The coordinator resolved this prefix against every unit of every
            // shard and pinned the first `PREFIX_EXPANSION_LIMIT` of the union,
            // so the term list — and therefore what the query MEANS — is the
            // same in every unit that compiles it. Expanding here instead
            // applies the cap per unit, and the union of per-unit cuts is a
            // function of the flush and compaction schedule.
            let resolved = stats.expansions.get(p);
            let owned;
            let terms: &[String] = match resolved {
                Some(e) => {
                    // The coordinator already decided whether anything was
                    // dropped, because it is the only place that saw the union,
                    // and every unit repeats its verdict — so for the first
                    // time this flag says something about the QUERY rather
                    // than about one segment. Note that it is NOT read off
                    // `e.terms.len()`: a full list and a cut list are the same
                    // length.
                    *truncated |= e.truncated;
                    &e.terms
                }
                None => {
                    // `compile` called without a coordinator: the unit is the
                    // whole world, so expanding here is the best available
                    // answer rather than a second-choice one.
                    //
                    // Masked by `vis`, exactly as `Shard::prefix_terms` masks
                    // the coordinator's read, and for the same reason: a term
                    // no live document holds must not spend a slot under the
                    // cap and displace one that a document does hold. A
                    // unit-local expansion is a smaller world than the
                    // collection, but it should still not depend on that
                    // unit's garbage.
                    //
                    // One MORE than the cap, then cut back. Asking for exactly
                    // the cap and testing `len() == cap` cannot tell a
                    // dictionary the cap truncated from a dictionary that holds
                    // exactly `cap` matching terms and lost nothing, and it
                    // called both of them truncation. The extra term is the
                    // whole difference: if it comes back, something was left
                    // behind.
                    owned = src.live_terms_with_prefix(p, PREFIX_EXPANSION_LIMIT + 1, vis);
                    if owned.len() > PREFIX_EXPANSION_LIMIT {
                        *truncated = true;
                        &owned[..PREFIX_EXPANSION_LIMIT]
                    } else {
                        &owned
                    }
                }
            };
            let mut kids: Vec<Box<dyn Scorer + 'a>> = Vec::with_capacity(terms.len());
            for t in terms {
                let Some(cur) = src.try_cursor(t)? else { continue };
                let idf = if resolved.is_some() {
                    // UNCONDITIONALLY global once the expansion was resolved,
                    // with no per-term fallback. Every term in the pinned list
                    // was handed to the same gather that produced `doc_freq`,
                    // so a miss means no live document holds it and `df == 0`
                    // — the highest weight there is — is the right answer, the
                    // same one a Term leaf gets in the same situation. Falling
                    // back to `src.doc_freq(t)` for the misses would reinstate
                    // the segment-local weight for exactly the terms the
                    // coordinator disagrees with the segment about, which is
                    // the defect, restricted to its worst case.
                    stats.idf(t)
                } else {
                    // No coordinator, so the only df available is the
                    // segment's own PHYSICAL dictionary count, which counts
                    // superseded versions and tombstoned rows. Internally
                    // consistent within one unit and not comparable between
                    // units — which is why the resolved path above exists and
                    // why this arm is reached only by direct `compile` calls.
                    //
                    // The clamp inside `idf` is load-bearing here, not
                    // cosmetic. A `num_docs` smaller than the local df — a
                    // stale statistics cache, or a segment newer than the
                    // gather — would drive the log negative, and with a
                    // non-positive `max_score` the pivot loop in
                    // `DisjunctionScorer::advance` (which needs `sum >
                    // threshold`, and the threshold starts at zero) never finds
                    // a pivot at all: the prefix query returns nothing instead of
                    // returning its matches cheaply ranked.
                    stats.idf_for_df(src.doc_freq(t) as u64)
                };
                kids.push(Box::new(TermScorer::new(cur, idf, src.doc_lens(), avgdl, params)));
            }
            if kids.is_empty() {
                empty()
            } else {
                Built {
                    scorer: Some(Box::new(DisjunctionScorer::new(kids))),
                    excluded: Bitmap::new(n),
                    negation_only: false,
                }
            }
        }
        TextQuery::Phrase(terms) => {
            let mut cursors = Vec::with_capacity(terms.len());
            for t in terms {
                match src.try_cursor(t)? {
                    Some(c) => cursors.push(c),
                    None => return Ok(empty()),
                }
            }
            if cursors.len() == 1 {
                let t = &terms[0];
                return Ok(Built {
                    scorer: Some(Box::new(TermScorer::new(
                        cursors.pop().unwrap(),
                        stats.idf(t),
                        src.doc_lens(),
                        avgdl,
                        params,
                    ))),
                    excluded: Bitmap::new(n),
                    negation_only: false,
                });
            }
            // A phrase is at most as frequent as its rarest term, so its idf is
            // at least the maximum of its terms'. The `0.0` seed was a second
            // hand-rolled floor against a negative term idf; `idf`'s clamp
            // makes it inert, and it stays only as the identity of `max`.
            let idf = terms.iter().map(|t| stats.idf(t)).fold(0.0f32, f32::max);
            Built {
                scorer: Some(Box::new(PhraseScorer::new(
                    cursors,
                    idf,
                    src.doc_lens(),
                    avgdl,
                    params,
                ))),
                excluded: Bitmap::new(n),
                negation_only: false,
            }
        }
        TextQuery::Not(inner) => {
            let b = build(inner, src, vis, stats, params, avgdl, truncated)?;
            let mut ex = Bitmap::new(n);
            if let Some(s) = b.scorer {
                let mut s = if b.excluded.is_empty() {
                    s
                } else {
                    Box::new(ExcludingScorer::new(s, b.excluded)) as Box<dyn Scorer + 'a>
                };
                let mut d = s.advance(0);
                while d != EXHAUSTED {
                    if (d as usize) < n {
                        ex.set(d as usize);
                    }
                    d = s.advance(d + 1);
                }
            }
            Built { scorer: None, excluded: ex, negation_only: true }
        }
        TextQuery::Any(parts) => {
            let mut kids: Vec<Box<dyn Scorer + 'a>> = Vec::new();
            let mut union_excluded = Bitmap::new(n);
            // Whether a branch *was* a negation, not whether it happened to
            // exclude anything. A negated term that matches nothing yields an
            // empty exclusion bitmap, and keying off the bitmap's contents
            // there skips the refusal below and quietly answers the positive
            // branch alone — the exact mis-answer the refusal exists to avoid.
            let mut saw_negation = false;
            for p in parts {
                let b = build(p, src, vis, stats, params, avgdl, truncated)?;
                match b.scorer {
                    Some(s) => {
                        // Scoped here, to this disjunct only.
                        kids.push(if b.excluded.is_empty() {
                            s
                        } else {
                            Box::new(ExcludingScorer::new(s, b.excluded))
                        });
                    }
                    None if b.negation_only => {
                        saw_negation = true;
                        union_excluded.or_inplace(&b.excluded);
                    }
                    None => {}
                }
            }
            if kids.is_empty() {
                // Every positive branch matched nothing, so what is left *is*
                // the negation: `nothing OR NOT y` is `NOT y`.
                return Ok(Built {
                    scorer: None,
                    excluded: union_excluded,
                    negation_only: saw_negation,
                });
            }
            if saw_negation {
                // `a OR NOT b` needs the complement of `b`, which no posting
                // list can generate. Refusing is better than quietly
                // answering `a`.
                return Err(Error::Sql(
                    "a negation cannot be one side of an OR: `a OR NOT b` would have to \
                     enumerate every document that is not `b`. Put the negation outside the \
                     OR, or add a positive term to that branch."
                        .into(),
                ));
            }
            Built {
                scorer: Some(if kids.len() == 1 {
                    kids.into_iter().next().unwrap()
                } else {
                    Box::new(DisjunctionScorer::new(kids))
                }),
                excluded: Bitmap::new(n),
                negation_only: false,
            }
        }
        TextQuery::All(parts) => {
            let mut kids: Vec<Box<dyn Scorer + 'a>> = Vec::new();
            let mut excluded = Bitmap::new(n);
            let mut saw_negation = false;
            for p in parts {
                // A conjunct that analysed away to nothing is not a constraint.
                // `the AND fox` parses to `All([Empty, Term(fox)])`, and letting
                // that `Empty` empty the conjunction makes the broader query
                // return fewer rows than `fox` alone. `Any` already drops such
                // branches, and `TextQuery::is_empty` already calls an `All`
                // empty only when *every* part is.
                if p.is_empty() {
                    continue;
                }
                let b = build(p, src, vis, stats, params, avgdl, truncated)?;
                excluded.or_inplace(&b.excluded);
                match b.scorer {
                    Some(s) => kids.push(s),
                    // A conjunct that matches nothing empties the conjunction —
                    // unless it was a negation, which only filters.
                    None if b.negation_only => saw_negation = true,
                    None => return Ok(empty()),
                }
            }
            match kids.len() {
                // Only a bare negation reaches here as "everything except";
                // a conjunction of nothing at all matches nothing.
                0 => Built { scorer: None, excluded, negation_only: saw_negation },
                1 => Built {
                    scorer: Some(kids.into_iter().next().unwrap()),
                    excluded,
                    negation_only: false,
                },
                _ => Built {
                    scorer: Some(Box::new(ConjunctionScorer::new(kids))),
                    excluded,
                    negation_only: false,
                },
            }
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hit {
    pub ord: u32,
    pub score: f32,
}

/// Collect the top `k` by score, admitting only ordinals in `filter`.
///
/// The WAND threshold is the k-th best score so far, pushed into the scorer
/// tree after every heap replacement. Rejected candidates cost one `advance`,
/// and a candidate outside the filter costs one jump to the next admitted
/// ordinal — never a scan of the gap.
pub fn collect_top_k(
    mut scorer: Box<dyn Scorer + '_>,
    filter: &Bitmap,
    excluded: Option<&Bitmap>,
    k: usize,
) -> Vec<Hit> {
    if k == 0 {
        return Vec::new();
    }
    let mut heap: Vec<Hit> = Vec::with_capacity(k.min(4096) + 1);
    let mut d = scorer.advance(0);
    while d != EXHAUSTED {
        if crate::deadline::expired() {
            break;
        }
        let admitted =
            filter.get(d as usize) && !excluded.map(|e| e.get(d as usize)).unwrap_or(false);
        if !admitted {
            // Jump straight to the next admitted ordinal.
            match filter.next_set(d as usize + 1) {
                Some(next) => {
                    d = scorer.advance(next as u32);
                    continue;
                }
                None => break,
            }
        }
        let s = scorer.score();
        if heap.len() < k {
            heap.push(Hit { ord: d, score: s });
            if heap.len() == k {
                heap.sort_by(cmp_hit);
                // The heap is full: from here the k-th best score is the bar
                // every later candidate has to clear, and WAND prunes to it.
                scorer.set_threshold(heap[0].score);
            }
        } else if better(s, d, heap[0].score, heap[0].ord) {
            heap[0] = Hit { ord: d, score: s };
            heap.sort_by(cmp_hit);
            scorer.set_threshold(heap[0].score);
        }
        d = scorer.advance(d + 1);
    }
    // Descending by score, then ascending by ordinal. The ordinal tie-break is
    // a local stand-in for the primary-key tie-break the coordinator applies
    // (§7.2). The two agree inside a sealed segment, which is primary-key
    // sorted. They do NOT agree inside the memtable, whose ordinals are push
    // order, so a memtable score tie is broken by insertion order and the
    // wrong one of two tied documents can take the last slot.
    //
    // That is known and not fixed here, because it is not a local change. The
    // k-th score is also the WAND threshold, and `DisjunctionScorer::advance`
    // prunes on `sum > threshold`, so a document that can only *tie* the k-th
    // score is skipped before it is ever scored — a key-aware tie-break here
    // would never see it. Admitting it means pushing a threshold below the
    // k-th score, which un-prunes every tie in the collection and is paid on
    // every multi-term query. The tie-break is not worth that price.
    heap.sort_by(|a, b| {
        b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal).then(a.ord.cmp(&b.ord))
    });
    heap
}

fn cmp_hit(a: &Hit, b: &Hit) -> std::cmp::Ordering {
    a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal).then(b.ord.cmp(&a.ord))
}

/// Strictly greater, deliberately. Candidates are visited in ascending ordinal
/// order, so a later document with an equal score always has the larger ordinal
/// and must lose the tie — which is exactly what WAND's `> threshold` pivot
/// assumes. Anything looser here and the pruned plan stops agreeing with the
/// brute-force one.
fn better(s: f32, _ord: u32, worst_s: f32, _worst_ord: u32) -> bool {
    s > worst_s
}

/// Evaluate to a plain matching set. This is `text_match(...)` in `WHERE`,
/// where it is a **must**: it filters and contributes no rank (§2.4).
pub fn evaluate_to_bitmap(c: Compiled<'_>, len: usize) -> Bitmap {
    let Compiled { scorer, excluded, pure_negation, .. } = c;
    let excluded = excluded.as_ref();
    // "Everything except" is a perfectly good filter, even though it is not a
    // candidate source.
    let mut out =
        if pure_negation && scorer.is_none() { Bitmap::all(len) } else { Bitmap::new(len) };
    if let Some(mut s) = scorer {
        let mut d = s.advance(0);
        while d != EXHAUSTED {
            if crate::deadline::expired() {
                break;
            }
            if (d as usize) < len {
                out.set(d as usize);
            }
            d = s.advance(d + 1);
        }
    }
    if let Some(e) = excluded {
        out.andnot_inplace(e);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::analyzer::Analyzer;
    use crate::text::postings::InvertedBuilder;
    use crate::text::TextSource;

    const DOCS: &[&str] = &[
        "the quick brown fox jumps over the lazy dog",
        "vector search with hierarchical navigable small world graphs",
        "brown bears hibernate in winter and dream of vector graphs",
        "a lazy afternoon with a quick coffee",
        "hybrid retrieval fuses lexical and vector signals",
        "the fox and the dog and the bear",
        "graph traversal over quantized codes then rerank",
        "quick quick quick repetition of one term",
        "navigable small world is a graph family",
        "nothing here matches anything interesting at all",
    ];

    fn build() -> (crate::text::postings::DictParts, Vec<u8>, Vec<u32>) {
        let mut b = InvertedBuilder::new();
        for (i, d) in DOCS.iter().enumerate() {
            let mut toks = Vec::new();
            Analyzer::English.analyze(d, 0, &mut toks);
            b.add_doc(i as u32, &toks);
        }
        let (dict, post, _) = b.finish();
        (crate::text::postings::DictParts::parse(&dict).unwrap(), post, b.doc_lens.clone())
    }

    /// Every document visible. These fixtures build a source directly, with no
    /// delete log and no superseded versions, so a full bitmap is not a
    /// convenience stand-in for visibility — it IS this source's visibility.
    fn all_live(src: &TextSource<'_>) -> Bitmap {
        Bitmap::all(src.num_docs() as usize)
    }

    fn stats(src: &TextSource<'_>) -> GlobalStats {
        let mut s = GlobalStats {
            num_docs: src.num_docs() as u64,
            avg_doc_len: src.total_doc_len() as f64 / src.num_docs().max(1) as f64,
            doc_freq: Default::default(),
            expansions: Default::default(),
            exact: true,
        };
        for (t, df) in src.all_terms() {
            s.doc_freq.insert(t, df as u64);
        }
        s
    }

    /// Score every document with no pruning at all. If block-max WAND ever
    /// disagrees with this, the pruning is wrong — which is the entire failure
    /// mode WAND has.
    fn brute_force(
        src: &TextSource<'_>,
        st: &GlobalStats,
        q: &TextQuery,
        filter: &Bitmap,
        k: usize,
    ) -> Vec<Hit> {
        let c = compile(q, src, &all_live(src), st, Bm25Params::default()).unwrap();
        let excluded = c.excluded.clone();
        let mut out = Vec::new();
        if let Some(mut s) = c.scorer {
            let mut d = s.advance(0);
            while d != EXHAUSTED {
                if filter.get(d as usize)
                    && !excluded.as_ref().map(|e| e.get(d as usize)).unwrap_or(false)
                {
                    out.push(Hit { ord: d, score: s.score() });
                }
                d = s.advance(d + 1);
            }
        }
        out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap().then(a.ord.cmp(&b.ord)));
        out.truncate(k);
        out
    }

    #[test]
    fn wand_agrees_with_brute_force() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let all = Bitmap::all(DOCS.len());
        for qs in [
            "quick",
            "quick brown fox",
            "vector graphs",
            "vector AND graphs",
            "\"small world\"",
            "quick -lazy",
            "graph*",
            "(vector OR lexical) AND retrieval",
            "nonexistentterm",
        ] {
            let q = TextQuery::parse(qs, Analyzer::English).unwrap();
            for k in [1usize, 3, 10] {
                let want = brute_force(&src, &st, &q, &all, k);
                let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
                let got = match c.scorer {
                    Some(s) => collect_top_k(s, &all, c.excluded.as_ref(), k),
                    None => Vec::new(),
                };
                assert_eq!(got.len(), want.len(), "query `{qs}` k={k}");
                for (g, w) in got.iter().zip(want.iter()) {
                    assert_eq!(g.ord, w.ord, "query `{qs}` k={k}: {got:?} vs {want:?}");
                    assert!((g.score - w.score).abs() < 1e-4, "query `{qs}` k={k}");
                }
            }
        }
    }

    #[test]
    fn filter_is_an_admission_predicate() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        // Only odd ordinals admitted.
        let mut f = Bitmap::new(DOCS.len());
        for i in (1..DOCS.len()).step_by(2) {
            f.set(i);
        }
        let q = TextQuery::parse("quick brown fox vector graph", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let got = collect_top_k(c.scorer.unwrap(), &f, c.excluded.as_ref(), 10);
        assert!(!got.is_empty());
        assert!(got.iter().all(|h| h.ord % 2 == 1), "{got:?}");
        let want = brute_force(&src, &st, &q, &f, 10);
        assert_eq!(
            got.iter().map(|h| h.ord).collect::<Vec<_>>(),
            want.iter().map(|h| h.ord).collect::<Vec<_>>()
        );
    }

    /// Scoring and filtering both stop when the statement's deadline has
    /// passed: the top-k loop returns nothing and the filter walk sets no
    /// bit, so the executor above refuses the statement rather than the loop
    /// answering with a fraction. With the deadline lifted the same query is
    /// complete.
    #[test]
    fn scoring_stops_when_the_deadline_has_passed() {
        let mut b = InvertedBuilder::new();
        for i in 0..40u32 {
            let mut toks = Vec::new();
            Analyzer::English.analyze(&format!("small world {i}"), 0, &mut toks);
            b.add_doc(i, &toks);
        }
        let (dict, post, _) = b.finish();
        let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("small world", Analyzer::English).unwrap();
        {
            let _expired = crate::deadline::arm(Some(0));
            let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
            let got = collect_top_k(c.scorer.unwrap(), &Bitmap::all(40), c.excluded.as_ref(), 10);
            assert!(got.is_empty(), "scoring ran past the deadline: {} hits", got.len());
            let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
            assert_eq!(evaluate_to_bitmap(c, 40).popcount(), 0, "the filter ran past the deadline");
        }
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert_eq!(
            collect_top_k(c.scorer.unwrap(), &Bitmap::all(40), c.excluded.as_ref(), 10).len(),
            10
        );
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert_eq!(evaluate_to_bitmap(c, 40).popcount(), 40);
    }

    #[test]
    fn phrases_do_not_match_across_the_array_gap() {
        use crate::text::analyzer::ARRAY_POSITION_GAP;
        let mut b = InvertedBuilder::new();
        let mut toks = Vec::new();
        // Two array elements: ["... small"], ["world ..."]. Adjacent only if
        // the gap is ignored.
        Analyzer::English.analyze("tiny small", 0, &mut toks);
        Analyzer::English.analyze("world large", ARRAY_POSITION_GAP, &mut toks);
        b.add_doc(0, &toks);
        let mut t2 = Vec::new();
        Analyzer::English.analyze("small world", 0, &mut t2);
        b.add_doc(1, &t2);
        let (dict, post, _) = b.finish();
        let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("\"small world\"", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let got = collect_top_k(c.scorer.unwrap(), &Bitmap::all(2), c.excluded.as_ref(), 10);
        assert_eq!(got.iter().map(|h| h.ord).collect::<Vec<_>>(), vec![1]);
    }

    /// A negation inside one branch of an OR must not filter the other
    /// branches. Threading a single exclusion bitmap through the whole compile
    /// — the obvious implementation — does exactly that.
    #[test]
    fn a_negation_is_scoped_to_its_own_branch() {
        let mut b = InvertedBuilder::new();
        for (i, text) in ["alpha beta", "alpha", "gamma beta", "gamma"].iter().enumerate() {
            let mut toks = Vec::new();
            Analyzer::English.analyze(text, 0, &mut toks);
            b.add_doc(i as u32, &toks);
        }
        let (dict, post, _) = b.finish();
        let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);

        let q = TextQuery::parse("(alpha -beta) OR gamma", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        // doc 2 is `gamma beta`: it matches the gamma branch, and the `-beta`
        // in the *other* branch has no business removing it.
        assert_eq!(evaluate_to_bitmap(c, 4).to_vec(), vec![1, 2, 3]);

        // A negation that genuinely applies to the whole query still does.
        let q = TextQuery::parse("(alpha OR gamma) AND -beta", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert_eq!(evaluate_to_bitmap(c, 4).to_vec(), vec![1, 3]);

        // And a negation as an OR branch is refused rather than mis-answered.
        let q = TextQuery::parse("alpha OR -beta", Analyzer::English).unwrap();
        let e = match compile(&q, &src, &all_live(&src), &st, Bm25Params::default()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a refusal"),
        };
        assert!(e.contains("negation cannot be one side of an OR"), "{e}");
    }

    /// "Everything except" is a usable filter even though it generates no
    /// candidates.
    #[test]
    fn a_pure_negation_filters() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("-quick", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert!(c.pure_negation);
        let bm = evaluate_to_bitmap(c, DOCS.len());
        assert!(!bm.get(0) && !bm.get(3) && !bm.get(7));
        assert_eq!(bm.popcount(), DOCS.len() - 3);
    }

    /// A stopword conjunct analyses to `Empty`, and letting `Empty` empty the
    /// whole conjunction makes `the AND fox` return nothing at all while `fox`
    /// alone returns matches — a strictly broader query answering with strictly
    /// fewer rows.
    #[test]
    fn a_stopword_conjunct_does_not_annihilate_the_conjunction() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("the AND fox", Analyzer::English).unwrap();
        assert_eq!(q, TextQuery::All(vec![TextQuery::Empty, TextQuery::Term("fox".into())]));
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let got = evaluate_to_bitmap(c, DOCS.len());
        assert_eq!(got.to_vec(), vec![0, 5]);

        let bare = TextQuery::parse("fox", Analyzer::English).unwrap();
        let c = compile(&bare, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert_eq!(got.to_vec(), evaluate_to_bitmap(c, DOCS.len()).to_vec());
    }

    /// The refusal of `a OR NOT b` used to be keyed off the exclusion bitmap
    /// being non-empty. A negated term that matches nothing excludes nothing,
    /// so the refusal was skipped and the query quietly answered `a` — neither
    /// the promised refusal nor the complement it asked for.
    #[test]
    fn an_or_branch_negation_is_refused_even_when_it_excludes_nothing() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("quick OR -nonexistentterm", Analyzer::English).unwrap();
        let e = match compile(&q, &src, &all_live(&src), &st, Bm25Params::default()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a refusal"),
        };
        assert!(e.contains("negation cannot be one side of an OR"), "{e}");
    }

    /// The expansion cap's boundary, on the arm that has to find it for
    /// itself. `compile` here is called without a coordinator, so `build`
    /// expands the prefix against this source's own dictionary — and that arm
    /// used to ask for exactly `PREFIX_EXPANSION_LIMIT` terms and then report
    /// truncation when it got them. After a `take(limit)` the answer can never
    /// be longer than the limit, so the report was not a measurement: a
    /// dictionary with exactly `cap` matching terms, which dropped nothing, is
    /// indistinguishable by length from one the cap cut in half.
    ///
    /// Both legs are here because either alone passes under a plausible wrong
    /// fix: reporting nothing ever passes the first, and the old code passes
    /// the second.
    #[test]
    fn an_expansion_reports_truncation_only_when_a_term_was_actually_dropped() {
        let expand = |vocab: usize| {
            let mut b = InvertedBuilder::new();
            for i in 0..vocab {
                let mut toks = Vec::new();
                Analyzer::English.analyze(&format!("a{i:05}"), 0, &mut toks);
                b.add_doc(i as u32, &toks);
            }
            let (dict, post, _) = b.finish();
            let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
            let lens = b.doc_lens.clone();
            let src = TextSource::sealed(&dict, &post, &lens);
            let st = GlobalStats { num_docs: vocab as u64, avg_doc_len: 1.0, ..Default::default() };
            let q = TextQuery::parse("a*", Analyzer::English).unwrap();
            let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
            let truncated = c.prefix_truncated;
            (truncated, evaluate_to_bitmap(c, vocab).popcount())
        };

        assert_eq!(
            expand(PREFIX_EXPANSION_LIMIT),
            (false, PREFIX_EXPANSION_LIMIT),
            "exactly the cap: every term is in the answer, so there is nothing to warn about"
        );
        assert_eq!(
            expand(PREFIX_EXPANSION_LIMIT + 1),
            (true, PREFIX_EXPANSION_LIMIT),
            "one term over: one document is unreachable, and the answer has to say so"
        );
    }

    #[test]
    fn a_dead_run_does_not_spend_the_prefix_cap_in_the_no_coordinator_arm() {
        // `vis` is a REQUIRED argument on `compile`/`build`, and it exists for
        // exactly one line: the no-coordinator prefix arm, which masks its own
        // dictionary enumeration by visibility. Every other fixture in this
        // module supplies `all_live`, which is honest about those sources —
        // they have no deletes — and therefore cannot exercise the mask at all.
        // Deleting the masking left the whole suite green.
        //
        // 520 dead terms sorting before 80 live ones, against a cap of 512: an
        // unmasked walk spends the entire budget on terms no visible document
        // holds, returns nothing, and reports truncation while doing it — an
        // empty answer plus a warning that is false in both halves.
        let vocab = 600usize;
        let mut b = InvertedBuilder::new();
        for i in 0..vocab {
            let mut toks = Vec::new();
            Analyzer::English.analyze(&format!("a{i:05}"), 0, &mut toks);
            b.add_doc(i as u32, &toks);
        }
        let (dict, post, _) = b.finish();
        let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();
        let mut vis = Bitmap::new(vocab);
        for o in 520..vocab {
            vis.set(o);
        }
        // 80 live documents, not 600: the statistics describe the live corpus,
        // which is what makes `num_docs` and the mask one consistent snapshot.
        let st = GlobalStats { num_docs: 80, avg_doc_len: 1.0, ..Default::default() };
        let q = TextQuery::parse("a*", Analyzer::English).unwrap();

        // Both walks, because `live_terms_with_prefix` has two independent
        // implementations and a sealed-only fixture pins one of them.
        let sealed = TextSource::sealed(&dict, &post, &lens);
        let mem = TextSource::Memory { terms: &b.terms, doc_lens: &b.doc_lens };
        for (name, src) in [("sealed", sealed), ("memtable", mem)] {
            let c = compile(&q, &src, &vis, &st, Bm25Params::default()).unwrap();
            assert!(!c.prefix_truncated, "{name}: 80 live terms is under the cap");
            // The exact hit set, not a count: the right NUMBER of wrong terms
            // would pass a popcount, since the caller masks by `vis` anyway.
            assert_eq!(
                evaluate_to_bitmap(c, vocab).to_vec(),
                (520..vocab as u32).collect::<Vec<u32>>(),
                "{name}: the live terms, all of them and only them"
            );
        }
    }

    /// With no coordinator to ask, a prefix expansion has no global df, so it
    /// scores against the unit's own. When `num_docs` is smaller than that df —
    /// a stale statistics cache, or a segment newer than the gather — the idf
    /// goes negative, and a non-positive `max_score` gives WAND's pivot loop
    /// nothing to pivot on: the query returns no rows rather than its matches.
    /// The clamp inside `idf` is what stops that, and this is the test that
    /// says so.
    #[test]
    fn a_stale_global_doc_count_does_not_make_a_prefix_query_return_nothing() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        // Deliberately inconsistent with the segment: four documents hold
        // `graph`, and the coordinator thinks the collection has one document.
        let st = GlobalStats {
            num_docs: 1,
            avg_doc_len: src.total_doc_len() as f64 / src.num_docs().max(1) as f64,
            doc_freq: Default::default(),
            expansions: Default::default(),
            exact: false,
        };
        let q = TextQuery::parse("graph*", Analyzer::English).unwrap();
        assert_eq!(q, TextQuery::Prefix("graph".into()));
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        assert_eq!(evaluate_to_bitmap(c, DOCS.len()).to_vec(), vec![1, 2, 6, 8]);

        // And the same query through the pruning collector, which is where the
        // threshold the pivot is compared against actually comes from.
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let got = collect_top_k(c.scorer.unwrap(), &Bitmap::all(DOCS.len()), None, 4);
        assert_eq!(got.len(), 4);
        assert!(got.iter().all(|h| h.score > 0.0), "{got:?}");
    }

    /// `df > num_docs` is not a hypothetical, and the reason is no longer the
    /// one it used to be: the statistics cache counted tombstoned postings,
    /// and now it does not — `Db::gather_stats` sums masked, live frequencies
    /// on both arms. What keeps this total rather than merely tidy is that
    /// `num_docs` and `doc_freq` reach a scorer through several routes and
    /// only one of them is that gather. A prefix expansion supplies its own
    /// `df` from a segment's physical dictionary — tombstones included —
    /// against a `num_docs` the coordinator gathered live and possibly
    /// earlier, which is the route `build` documents in place and
    /// `a_stale_global_doc_count_does_not_make_a_prefix_query_return_nothing`
    /// pins; and `GlobalStats` is a public struct with public fields, so a
    /// caller may hand the scorer any pair of numbers at all. Unclamped, the
    /// log's argument drops below one and every
    /// document holding the term ranks *below* every document that does not —
    /// BM25 run backwards. Clamped, the term lands where a term in every
    /// document belongs: informative of nothing, but still worth more than
    /// absence.
    #[test]
    fn idf_is_never_negative_however_incoherent_the_statistics() {
        let floor = |n: u64| (1.0f64 + 0.5 / (n.max(1) as f64 + 0.5)).ln() as f32;
        let idf =
            |n: u64, df: u64| GlobalStats { num_docs: n, ..Default::default() }.idf_for_df(df);
        for n in [0u64, 1, 2, 40, 1_000, u32::MAX as u64] {
            for df in [0u64, 1, n / 2, n, n + 1, n * 3 + 7, u64::MAX] {
                let v = idf(n, df);
                assert!(v.is_finite() && v > 0.0, "idf({n}, {df}) = {v}");
                assert!(v >= floor(n), "idf({n}, {df}) = {v} is below the every-document floor");
            }
            // And the clamp is exactly the floor the prefix branch used to
            // compute by hand, to the bit, so it changes no score that was
            // already well defined.
            assert_eq!(idf(n, n + 1).to_bits(), floor(n).to_bits());
        }
    }

    /// What the formula's shape does not pin is which count is which.
    /// `num_docs` and `doc_freq` are both document counts, so an implementation
    /// that exchanges them still returns something finite, positive, and even
    /// still ordered: the clamp turns every exchanged call into the
    /// every-document floor of `df`, which falls as `df` rises, so rare terms
    /// keep beating common ones and "a rarer term scores higher" catches
    /// nothing. The collection axis is what an exchange destroys — exchanged,
    /// `num_docs` reaches the formula only through the clamp, so widening the
    /// collection around a fixed `df` stops changing the answer. That is the
    /// second assertion, and it is the one that fails.
    #[test]
    fn idf_rises_with_the_collection_and_falls_with_the_term() {
        let st = |n: u64| GlobalStats { num_docs: n, ..Default::default() };
        // A term is worth less the more of the collection holds it.
        assert!(st(1_000).idf_for_df(1) > st(1_000).idf_for_df(900));
        // And worth more the more documents could have held it and did not.
        assert!(
            st(1_000).idf_for_df(1) > st(10).idf_for_df(1),
            "1 of 1000 documents ({}) must outweigh 1 of 10 ({})",
            st(1_000).idf_for_df(1),
            st(10).idf_for_df(1),
        );
    }

    /// With no coordinator to ask, a prefix expansion scores each expanded term
    /// against the *segment's* own df — and reaching for the global map anyway
    /// is not a smaller mistake than it sounds: with nothing gathered, every
    /// expanded term misses, every miss is `df == 0`, and `df == 0` is the
    /// maximum idf, so the whole expansion is weighted identically and the
    /// ranking within it collapses to term frequency. (Through `Db::run_select`
    /// the map is populated, every term is in it, and the global branch is the
    /// right one — see `build`.)
    ///
    /// Here `alphabet` holds 1 document of 10 and `alpha` holds 8, so the one
    /// document with the rare term must beat the document that repeats the
    /// common one three times. Against a global df it loses to it instead.
    ///
    /// This pins the NO-COORDINATOR fallback path specifically. `compile` is
    /// called here with an empty `GlobalStats`, which is what a direct caller
    /// gets and what `build` documents as the fallback arm; a query through
    /// `Db::run_select` arrives with `stats.expansions` already resolved and a
    /// global `df` for each of its terms. Either way the *choice* is the same
    /// and is what this test defends: each expanded term is weighted by its own
    /// frequency, not collapsed into one synthetic term. Any design that scored
    /// a whole expansion as a single term contradicts this test, and should be
    /// rejected on its authority rather than by editing it.
    #[test]
    fn a_prefix_expansion_ranks_by_each_terms_own_document_frequency() {
        const DOCS: &[&str] = &[
            "alpha alpha alpha",
            "alpha",
            "alpha",
            "alpha",
            "alpha",
            "alpha",
            "alpha",
            "alpha",
            "alphabet",
            "unrelated",
        ];
        let mut b = InvertedBuilder::new();
        for (i, d) in DOCS.iter().enumerate() {
            let mut toks = Vec::new();
            Analyzer::English.analyze(d, 0, &mut toks);
            b.add_doc(i as u32, &toks);
        }
        let (dict, post, _) = b.finish();
        let dict = crate::text::postings::DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();
        let src = TextSource::sealed(&dict, &post, &lens);
        assert_eq!((src.doc_freq("alpha"), src.doc_freq("alphabet")), (8, 1));

        // Empty `doc_freq` and empty `expansions` together are what a DIRECT
        // `compile` caller gets, and are what put this test on the fallback arm
        // — see the doc comment. Through `Db::run_select` both arrive
        // populated: the coordinator resolves every prefix and merges its terms
        // into the gather, so "a prefix query's terms are never gathered",
        // which this comment used to assert as a present-tense fact, describes
        // the defect that change removed.
        let st = GlobalStats {
            num_docs: DOCS.len() as u64,
            avg_doc_len: src.total_doc_len() as f64 / DOCS.len() as f64,
            doc_freq: Default::default(),
            expansions: Default::default(),
            exact: false,
        };
        let q = TextQuery::parse("alpha*", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let got = collect_top_k(c.scorer.unwrap(), &Bitmap::all(DOCS.len()), None, 2);
        assert_eq!(got[0].ord, 8, "expected the rare expanded term to win: {got:?}");
        assert_eq!(got[1].ord, 0, "{got:?}");
    }

    #[test]
    fn negation_filters_and_where_semantics_are_a_set() {
        let (dict, post, lens) = build();
        let src = TextSource::sealed(&dict, &post, &lens);
        let st = stats(&src);
        let q = TextQuery::parse("quick AND -lazy", Analyzer::English).unwrap();
        let c = compile(&q, &src, &all_live(&src), &st, Bm25Params::default()).unwrap();
        let bm = evaluate_to_bitmap(c, DOCS.len());
        // docs 0 and 3 have both quick and lazy; 7 has quick alone.
        assert_eq!(bm.to_vec(), vec![7]);
    }
}
