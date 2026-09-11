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

/// Collection-wide term statistics, gathered by the coordinator and attached
/// to the query for its own terms only (§8.1).
#[derive(Debug, Clone, Default)]
pub struct GlobalStats {
    pub num_docs: u64,
    pub avg_doc_len: f64,
    pub doc_freq: BTreeMap<String, u64>,
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
/// prefix on a large dictionary is a denial of service, and silently returning
/// fewer results is worse than saying so — the cap is reported in
/// `EXPLAIN ANALYZE`.
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

pub fn compile<'a>(
    q: &TextQuery,
    src: &'a TextSource<'a>,
    stats: &GlobalStats,
    params: Bm25Params,
) -> Result<Compiled<'a>> {
    let avgdl = if stats.avg_doc_len > 0.0 { stats.avg_doc_len as f32 } else { 1.0 };
    let mut truncated = false;
    let b = build(q, src, stats, params, avgdl, &mut truncated)?;
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
            let terms = src.terms_with_prefix(p, PREFIX_EXPANSION_LIMIT);
            if terms.len() >= PREFIX_EXPANSION_LIMIT {
                *truncated = true;
            }
            let mut kids: Vec<Box<dyn Scorer + 'a>> = Vec::with_capacity(terms.len());
            for t in &terms {
                let Some(cur) = src.try_cursor(t)? else { continue };
                let idf = if stats.doc_freq.contains_key(t) {
                    stats.idf(t)
                } else {
                    // A prefix term the coordinator never saw has no global df,
                    // because `TextQuery::leaf_terms` deliberately skips
                    // `Prefix`, so this mixes the segment's own df with the
                    // global document count. It is internally consistent within
                    // one segment, but scores from a prefix expansion are still
                    // NOT comparable across shards (§8.2) — making them so needs
                    // the coordinator to gather df for the expanded terms, which
                    // cannot be decided here.
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
            let b = build(inner, src, stats, params, avgdl, truncated)?;
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
                let b = build(p, src, stats, params, avgdl, truncated)?;
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
                let b = build(p, src, stats, params, avgdl, truncated)?;
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
    // (§7.2); within a segment the two agree, because segments are primary-key
    // sorted.
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

    fn stats(src: &TextSource<'_>) -> GlobalStats {
        let mut s = GlobalStats {
            num_docs: src.num_docs() as u64,
            avg_doc_len: src.total_doc_len() as f64 / src.num_docs().max(1) as f64,
            doc_freq: Default::default(),
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
        let c = compile(q, src, st, Bm25Params::default()).unwrap();
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
                let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        let got = collect_top_k(c.scorer.unwrap(), &f, c.excluded.as_ref(), 10);
        assert!(!got.is_empty());
        assert!(got.iter().all(|h| h.ord % 2 == 1), "{got:?}");
        let want = brute_force(&src, &st, &q, &f, 10);
        assert_eq!(
            got.iter().map(|h| h.ord).collect::<Vec<_>>(),
            want.iter().map(|h| h.ord).collect::<Vec<_>>()
        );
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        // doc 2 is `gamma beta`: it matches the gamma branch, and the `-beta`
        // in the *other* branch has no business removing it.
        assert_eq!(evaluate_to_bitmap(c, 4).to_vec(), vec![1, 2, 3]);

        // A negation that genuinely applies to the whole query still does.
        let q = TextQuery::parse("(alpha OR gamma) AND -beta", Analyzer::English).unwrap();
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        assert_eq!(evaluate_to_bitmap(c, 4).to_vec(), vec![1, 3]);

        // And a negation as an OR branch is refused rather than mis-answered.
        let q = TextQuery::parse("alpha OR -beta", Analyzer::English).unwrap();
        let e = match compile(&q, &src, &st, Bm25Params::default()) {
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        let got = evaluate_to_bitmap(c, DOCS.len());
        assert_eq!(got.to_vec(), vec![0, 5]);

        let bare = TextQuery::parse("fox", Analyzer::English).unwrap();
        let c = compile(&bare, &src, &st, Bm25Params::default()).unwrap();
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
        let e = match compile(&q, &src, &st, Bm25Params::default()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a refusal"),
        };
        assert!(e.contains("negation cannot be one side of an OR"), "{e}");
    }

    /// A prefix expansion has no global df, so it scores against the local one.
    /// When `num_docs` is smaller than that df — a stale statistics cache — the
    /// idf goes negative, and a non-positive `max_score` gives WAND's pivot loop
    /// nothing to pivot on: the query returns no rows rather than its matches.
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
            exact: false,
        };
        let q = TextQuery::parse("graph*", Analyzer::English).unwrap();
        assert_eq!(q, TextQuery::Prefix("graph".into()));
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        assert_eq!(evaluate_to_bitmap(c, DOCS.len()).to_vec(), vec![1, 2, 6, 8]);

        // And the same query through the pruning collector, which is where the
        // threshold the pivot is compared against actually comes from.
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
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

    /// A prefix expansion scores each expanded term against the *segment's*
    /// df, because `TextQuery::leaf_terms` skips `Prefix` and the coordinator
    /// therefore gathers no global df for any of them (§8.2). Reaching for the
    /// global map anyway is not a smaller mistake than it sounds: every
    /// expanded term misses, every miss is `df == 0`, and `df == 0` is the
    /// maximum idf — so the whole expansion is weighted identically and the
    /// ranking within it collapses to term frequency.
    ///
    /// Here `alphabet` holds 1 document of 10 and `alpha` holds 8, so the one
    /// document with the rare term must beat the document that repeats the
    /// common one three times. Against a global df it loses to it instead.
    ///
    /// This pins the *choice of df*, not the cross-shard consequence: prefix
    /// scores still are not comparable between shards, and making them so
    /// needs the coordinator to gather df for expanded terms — a separate
    /// backlog item, not something this file can decide.
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

        // An empty `doc_freq` is not a contrivance: it is exactly what a
        // prefix query gets, since its terms are never gathered.
        let st = GlobalStats {
            num_docs: DOCS.len() as u64,
            avg_doc_len: src.total_doc_len() as f64 / DOCS.len() as f64,
            doc_freq: Default::default(),
            exact: false,
        };
        let q = TextQuery::parse("alpha*", Analyzer::English).unwrap();
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
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
        let c = compile(&q, &src, &st, Bm25Params::default()).unwrap();
        let bm = evaluate_to_bitmap(c, DOCS.len());
        // docs 0 and 3 have both quick and lazy; 7 has quick alone.
        assert_eq!(bm.to_vec(), vec![7]);
    }
}
