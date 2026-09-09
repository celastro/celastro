//! The vector subsystem: storage, tiering, and the runtime filtered-search
//! decision.
//!
//! §5.3 calls filtered vector search the hardest problem in the system, and the
//! reason is that neither pre-filter nor post-filter works across the
//! selectivity range. The response here is not a better algorithm but a
//! *runtime* choice: because structured predicates are evaluated first and
//! materialised as a bitmap, selectivity is **measured, not estimated**, and
//! the strategy is picked per segment per query from that measurement. A
//! planner that picks one strategy statically will be badly wrong on some
//! fraction of production queries; that fraction is invisible in a benchmark
//! and expensive in production.

pub mod distance;
pub mod hnsw;
pub mod quant;

use crate::bitmap::Bitmap;
use crate::catalog::Metric;
use crate::codec::*;
use crate::error::{Error, Result};
use hnsw::{Hnsw, HnswParams};
use quant::{Codes, Quantizer};

/// Which index a vector set carries, chosen by size (§5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Memtable and tiny segments: flat, exact, always fresh.
    Flat,
    /// Small sealed segments: in-memory HNSW.
    Hnsw,
}

/// Below this many vectors a graph costs more than it saves, and an exact scan
/// is both faster and — more usefully — exact.
pub const FLAT_TIER_MAX: usize = 4_096;

#[derive(Debug, Clone, Copy)]
pub struct SearchOpts {
    /// Candidate breadth for graph traversal.
    pub ef_search: usize,
    /// How many candidates to rerank against full precision, as a multiple of
    /// `k`.
    pub rerank_multiplier: usize,
    /// `WITH exact`: brute force over every vector, no approximation (§14).
    pub exact: bool,
    /// Upper bound on `1/s` amplification, so a pathological filter cannot turn
    /// one query into a full scan through the back door.
    pub max_amplification: usize,
}

impl Default for SearchOpts {
    fn default() -> Self {
        SearchOpts { ef_search: 128, rerank_multiplier: 3, exact: false, max_amplification: 64 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Exact distance over the filter bitmap. Cheaper *and* exact when few
    /// survive.
    BruteForce,
    /// Standard ANN traversal then post-filter, with `k` amplified by ~`1/s`.
    PostFilter,
    /// Expand through non-matching neighbours, admit only matching ones
    /// (ACORN-style).
    FilterAware,
    /// Forced by `WITH exact`.
    Exact,
}

impl Strategy {
    pub fn name(self) -> &'static str {
        match self {
            Strategy::BruteForce => "brute_force",
            Strategy::PostFilter => "ann_post_filter",
            Strategy::FilterAware => "ann_filter_aware",
            Strategy::Exact => "exact",
        }
    }
}

/// What the runtime actually did. Surfaced by `EXPLAIN ANALYZE` (§12.1),
/// because these decisions are invisible without it and they are the decisions
/// that explain a latency or recall surprise.
#[derive(Debug, Clone, Default)]
pub struct VectorReport {
    pub strategy: Option<Strategy>,
    pub selectivity: f64,
    pub survivors: usize,
    pub ef_used: usize,
    pub amplification: f64,
    pub reranked: usize,
    pub reprobes: usize,
    pub tier: Option<Tier>,
}

/// A segment's (or memtable's) vectors for one vector field.
pub struct VectorStore {
    pub dims: usize,
    pub metric: Metric,
    /// Full precision, vector-ordinal major. The only component allowed to be
    /// cold (§8.4); rerank reads it in one batched range per segment.
    pub full: Vec<f32>,
    /// Resident quantized codes.
    pub codes: Codes,
    /// `vec_ordinals.map`: vector ordinal → document ordinal. In v1 there is
    /// one vector per (document, vector field), so this is usually the
    /// identity — but the indirection is the primitive native multi-vector
    /// fields need in v2 (§5.4), and it is free to carry now.
    pub vec_to_doc: Vec<u32>,
    pub graph: Option<Hnsw>,
}

impl VectorStore {
    pub fn new(dims: usize, metric: Metric) -> VectorStore {
        VectorStore {
            dims,
            metric,
            full: Vec::new(),
            codes: Codes::empty(Quantizer::None, dims),
            vec_to_doc: Vec::new(),
            graph: None,
        }
    }

    pub fn len(&self) -> usize {
        self.vec_to_doc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vec_to_doc.is_empty()
    }

    pub fn tier(&self) -> Tier {
        if self.graph.is_some() {
            Tier::Hnsw
        } else {
            Tier::Flat
        }
    }

    pub fn vector(&self, vec_ord: usize) -> &[f32] {
        &self.full[vec_ord * self.dims..(vec_ord + 1) * self.dims]
    }

    /// Append one vector, already prepared for the metric.
    pub fn push(&mut self, doc_ord: u32, v: &[f32]) -> Result<()> {
        if v.len() != self.dims {
            return Err(Error::Schema(format!(
                "vector has {} dimensions but the index declares {}",
                v.len(),
                self.dims
            )));
        }
        if let Some(bad) = v.iter().find(|x| !x.is_finite()) {
            // One infinity gives a dimension an infinite range, which makes
            // every SQ8 code in the segment dequantize to NaN — not just this
            // vector's. A single bad document would take the whole segment's
            // vector index with it.
            return Err(Error::Schema(format!("vector component {bad} is not finite")));
        }
        self.full.extend_from_slice(v);
        self.vec_to_doc.push(doc_ord);
        Ok(())
    }

    /// Quantize and build the graph. Called once at seal, never incrementally:
    /// segments are immutable, and that is what lets the graph be built by
    /// whoever has spare CPU rather than by whoever owns the write path (§4.5).
    pub fn seal(&mut self, quantizer: Quantizer, params: HnswParams, flat_tier_max: usize) {
        let n = self.len();
        if n == 0 {
            return;
        }
        self.codes = Codes::build(quantizer, self.dims, &self.full);
        if n > flat_tier_max {
            let dims = self.dims;
            let metric = self.metric;
            let full = &self.full;
            let dist = |a: u32, b: u32| {
                distance::distance(
                    metric,
                    &full[a as usize * dims..(a as usize + 1) * dims],
                    &full[b as usize * dims..(b as usize + 1) * dims],
                )
            };
            self.graph = Some(Hnsw::build(n, params, &dist));
        }
    }

    pub fn memory_bytes(&self) -> usize {
        self.codes.memory_bytes()
            + self.graph.as_ref().map(|g| g.memory_bytes()).unwrap_or(0)
            + self.vec_to_doc.len() * 4
    }

    /// Project a document-ordinal filter into vector space.
    fn admit_bitmap(&self, doc_filter: &Bitmap) -> Bitmap {
        let n = self.len();
        let mut bm = Bitmap::new(n);
        for (v, &d) in self.vec_to_doc.iter().enumerate() {
            if doc_filter.get(d as usize) {
                bm.set(v);
            }
        }
        bm
    }

    /// Search, returning `(document ordinal, distance)` ascending.
    ///
    /// `doc_filter` must already have visibility ANDed in. That is not an
    /// implementation detail: it is what makes the visible ratio part of the
    /// measured selectivity, so the `1/s` amplification below *is* the adaptive
    /// `k` amplification of §6, and recall does not silently decay as a
    /// collection accumulates deletes.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        doc_filter: &Bitmap,
        opts: &SearchOpts,
    ) -> (Vec<(u32, f32)>, VectorReport) {
        let mut report = VectorReport { tier: Some(self.tier()), ..Default::default() };
        let n = self.len();
        if n == 0 || k == 0 {
            return (Vec::new(), report);
        }
        let admit = self.admit_bitmap(doc_filter);
        let survivors = admit.popcount();
        let s = survivors as f64 / n as f64;
        report.selectivity = s;
        report.survivors = survivors;
        if survivors == 0 {
            report.strategy = Some(Strategy::BruteForce);
            return (Vec::new(), report);
        }

        let strategy = self.choose(survivors, s, k, opts);
        report.strategy = Some(strategy);

        match strategy {
            Strategy::BruteForce | Strategy::Exact => {
                report.reranked = survivors;
                (self.brute_force(query, k, &admit), report)
            }
            Strategy::PostFilter => {
                let amp = (1.0 / s.max(1e-9)).min(opts.max_amplification as f64);
                report.amplification = amp;
                let mut ef = ((k as f64 * amp).ceil() as usize).max(opts.ef_search);
                for attempt in 0..3 {
                    report.ef_used = ef;
                    let out = self.graph_search(query, k, ef, None, &admit, opts, &mut report);
                    // Re-probe if the heap came up short — the post-filter
                    // failure mode is returning 6 of 10 and calling it a day.
                    if out.len() >= k || attempt == 2 || ef >= n {
                        return (out, report);
                    }
                    ef = (ef * 4).min(n);
                    report.reprobes += 1;
                }
                unreachable!()
            }
            Strategy::FilterAware => {
                let ef = opts.ef_search.max(k * 4);
                report.ef_used = ef;
                let out = self.graph_search(query, k, ef, Some(&admit), &admit, opts, &mut report);
                (out, report)
            }
        }
    }

    /// The cost model. Brute force is `survivors × dim`; a post-filtered ANN is
    /// roughly `ANN(k/s)`, which is `ef` traversal steps each touching `m0`
    /// neighbours. Codes are bytes rather than floats, so a traversal step is
    /// cheaper per dimension than an exact one — hence `CODE_COST`.
    fn choose(&self, survivors: usize, s: f64, k: usize, opts: &SearchOpts) -> Strategy {
        if opts.exact {
            return Strategy::Exact;
        }
        let Some(g) = self.graph.as_ref() else {
            return Strategy::BruteForce;
        };
        const CODE_COST: f64 = 0.35;
        let dims = self.dims as f64;
        let cost_brute = survivors as f64 * dims;
        let amp = (1.0 / s.max(1e-9)).min(opts.max_amplification as f64);
        let ef = ((k as f64 * amp).ceil()).max(opts.ef_search as f64);
        let cost_ann = ef * g.params.m0 as f64 * dims * CODE_COST;
        if cost_brute <= cost_ann {
            // Few enough survive that scanning them is cheaper — and exact.
            return Strategy::BruteForce;
        }
        // Above this selectivity, amplified post-filtering keeps recall; below
        // it, a post-filter is mostly discarding what it just spent work to
        // find, and filter-aware traversal wins.
        const POST_FILTER_FLOOR: f64 = 0.15;
        if s >= POST_FILTER_FLOOR {
            Strategy::PostFilter
        } else {
            Strategy::FilterAware
        }
    }

    fn brute_force(&self, query: &[f32], k: usize, admit: &Bitmap) -> Vec<(u32, f32)> {
        let mut best: Vec<(u32, f32)> = Vec::with_capacity(k + 1);
        let mut worst = f32::INFINITY;
        for v in admit.iter() {
            let d = distance::distance(self.metric, query, self.vector(v as usize));
            if best.len() < k {
                best.push((self.vec_to_doc[v as usize], d));
                if best.len() == k {
                    best.sort_by(cmp_dist);
                    worst = best[k - 1].1;
                }
            } else if cmp_dist(&(0, d), &(0, worst)) == std::cmp::Ordering::Less {
                best[k - 1] = (self.vec_to_doc[v as usize], d);
                best.sort_by(cmp_dist);
                worst = best[k - 1].1;
            }
        }
        best.sort_by(cmp_dist);
        best
    }

    /// Two-stage: traverse over quantized codes, then rerank the candidate set
    /// against full precision (§5.2). The rerank is what makes aggressive
    /// quantization safe — 1-bit codes are only viable because nothing is
    /// returned on a code distance alone.
    #[allow(clippy::too_many_arguments)]
    fn graph_search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        admit_during: Option<&Bitmap>,
        admit_after: &Bitmap,
        opts: &SearchOpts,
        report: &mut VectorReport,
    ) -> Vec<(u32, f32)> {
        let Some(g) = self.graph.as_ref() else {
            return self.brute_force(query, k, admit_after);
        };
        let use_codes = self.codes.quantizer != Quantizer::None;
        let d_to = |i: u32| {
            if use_codes {
                self.codes.distance(self.metric, query, i as usize)
            } else {
                distance::distance(self.metric, query, self.vector(i as usize))
            }
        };
        let want = k.saturating_mul(opts.rerank_multiplier).max(k);
        // When the filter is applied *during* traversal the heap already holds
        // only admitted vectors, so rerank depth is all that is needed. When it
        // is applied afterwards the heap is unfiltered, and taking only a
        // rerank-sized slice of it throws away the very candidates the `1/s`
        // amplification just paid `ef` to find — the amplification widens the
        // traversal, and then the result list has to be wide enough to carry it
        // out. Capping here is what made post-filtered queries return six rows
        // when ten were available.
        let take = if admit_during.is_some() { want.min(ef) } else { want.max(ef) };
        let raw = g.search(ef, take, admit_during, &d_to);

        let mut reranked: Vec<(u32, f32)> = raw
            .into_iter()
            .filter(|(v, _)| admit_after.get(*v as usize))
            .map(|(v, _)| {
                (
                    self.vec_to_doc[v as usize],
                    distance::distance(self.metric, query, self.vector(v as usize)),
                )
            })
            .collect();
        report.reranked += reranked.len();
        reranked.sort_by(cmp_dist);
        reranked.truncate(k);
        reranked
    }

    // --- Serialisation. Three regions, matching the segment layout: codes and
    // graph are resident, `full` is the cold tier.

    pub fn encode_full(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.full.len() * 4);
        for x in &self.full {
            put_f32(&mut out, *x);
        }
        out
    }

    pub fn encode_map(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.vec_to_doc.len() * 4);
        for d in &self.vec_to_doc {
            put_u32(&mut out, *d);
        }
        out
    }

    /// Every region of a sealed vector index, passed separately because each is
    /// a distinct residency component with its own lifetime.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        dims: usize,
        metric: Metric,
        full: &[u8],
        codes: &[u8],
        graph: &[u8],
        map: &[u8],
    ) -> Result<VectorStore> {
        let bad = || Error::Storage("vectors: truncated".into());
        let mut i = 0usize;
        let mut fullv = Vec::with_capacity(full.len() / 4);
        while i < full.len() {
            fullv.push(get_f32(full, &mut i).ok_or_else(bad)?);
        }
        let mut j = 0usize;
        let mut map_v = Vec::with_capacity(map.len() / 4);
        while j < map.len() {
            map_v.push(get_u32(map, &mut j).ok_or_else(bad)?);
        }
        let codes = if codes.is_empty() {
            Codes::empty(Quantizer::None, dims)
        } else {
            Codes::decode_bytes(codes)?
        };
        let graph = if graph.is_empty() { None } else { Some(Hnsw::decode(graph)?) };
        Ok(VectorStore { dims, metric, full: fullv, codes, vec_to_doc: map_v, graph })
    }
}

/// Total order over `(ordinal, distance)`.
///
/// NaN sorts last rather than comparing Equal to everything. A comparator that
/// treats NaN as Equal is not a total order: `sort_by` is entitled to panic on
/// it, and the `worst`-threshold loop in `brute_force` silently stops accepting
/// candidates the moment NaN reaches the heap — turning "exact" search into
/// "the first k in ordinal order".
fn cmp_dist(a: &(u32, f32), b: &(u32, f32)) -> std::cmp::Ordering {
    let x = if a.1.is_nan() { f32::INFINITY } else { a.1 };
    let y = if b.1.is_nan() { f32::INFINITY } else { b.1 };
    x.partial_cmp(&y)
        .unwrap_or(std::cmp::Ordering::Equal)
        // Ordinal tie-break keeps equal distances deterministic; the
        // coordinator re-breaks on primary key (§7.2).
        .then(a.0.cmp(&b.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(n: usize, dims: usize, seal: bool) -> VectorStore {
        let mut rng = Rng::new(31);
        let mut vs = VectorStore::new(dims, Metric::Cosine);
        for i in 0..n {
            let mut v: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut v);
            vs.push(i as u32, &v).unwrap();
        }
        if seal {
            vs.seal(Quantizer::Sq8, HnswParams::default(), FLAT_TIER_MAX);
        }
        vs
    }

    fn truth(vs: &VectorStore, q: &[f32], k: usize, f: &Bitmap) -> Vec<u32> {
        let mut all: Vec<(u32, f32)> = (0..vs.len())
            .filter(|i| f.get(vs.vec_to_doc[*i] as usize))
            .map(|i| (vs.vec_to_doc[i], distance::distance(vs.metric, q, vs.vector(i))))
            .collect();
        all.sort_by(cmp_dist);
        all.into_iter().take(k).map(|(d, _)| d).collect()
    }

    #[test]
    fn few_survivors_pick_brute_force_and_are_exact() {
        let vs = store(6000, 32, true);
        let mut f = Bitmap::new(6000);
        for i in (0..6000).step_by(97) {
            f.set(i);
        }
        let q = vs.vector(0).to_vec();
        let (got, rep) = vs.search(&q, 10, &f, &SearchOpts::default());
        assert_eq!(rep.strategy, Some(Strategy::BruteForce));
        assert_eq!(got.iter().map(|(d, _)| *d).collect::<Vec<_>>(), truth(&vs, &q, 10, &f));
    }

    #[test]
    fn high_selectivity_picks_post_filter() {
        let vs = store(6000, 32, true);
        let f = Bitmap::all(6000);
        let q = vs.vector(1).to_vec();
        let (got, rep) = vs.search(&q, 10, &f, &SearchOpts::default());
        assert_eq!(rep.strategy, Some(Strategy::PostFilter));
        assert_eq!(got.len(), 10);
        let t = truth(&vs, &q, 10, &f);
        let hits = got.iter().filter(|(d, _)| t.contains(d)).count();
        assert!(hits >= 9, "recall {hits}/10");
    }

    #[test]
    fn middling_selectivity_picks_filter_aware() {
        let vs = store(20000, 24, true);
        // 10%: 2000 survivors is too many to scan exactly, but a post-filtered
        // traversal would throw away nine of every ten candidates it found.
        let mut f = Bitmap::new(20000);
        for i in (0..20000).step_by(10) {
            f.set(i);
        }
        let q = vs.vector(3).to_vec();
        let (got, rep) = vs.search(&q, 10, &f, &SearchOpts::default());
        assert_eq!(rep.strategy, Some(Strategy::FilterAware));
        assert_eq!(got.len(), 10);
        assert!(got.iter().all(|(d, _)| f.get(*d as usize)));
        let t = truth(&vs, &q, 10, &f);
        let hits = got.iter().filter(|(d, _)| t.contains(d)).count();
        assert!(hits >= 8, "filter-aware recall {hits}/10");
    }

    #[test]
    fn exact_mode_ignores_the_graph_entirely() {
        let vs = store(6000, 32, true);
        let f = Bitmap::all(6000);
        let q = vs.vector(5).to_vec();
        let opts = SearchOpts { exact: true, ..Default::default() };
        let (got, rep) = vs.search(&q, 10, &f, &opts);
        assert_eq!(rep.strategy, Some(Strategy::Exact));
        assert_eq!(got.iter().map(|(d, _)| *d).collect::<Vec<_>>(), truth(&vs, &q, 10, &f));
    }

    #[test]
    fn flat_tier_is_exact_by_construction() {
        let vs = store(500, 32, true);
        assert_eq!(vs.tier(), Tier::Flat);
        let f = Bitmap::all(500);
        let q = vs.vector(7).to_vec();
        let (got, _) = vs.search(&q, 10, &f, &SearchOpts::default());
        assert_eq!(got.iter().map(|(d, _)| *d).collect::<Vec<_>>(), truth(&vs, &q, 10, &f));
    }

    #[test]
    fn store_round_trips() {
        let vs = store(5000, 16, true);
        let back = VectorStore::open(
            16,
            Metric::Cosine,
            &vs.encode_full(),
            &vs.codes.encode_bytes(),
            &vs.graph.as_ref().unwrap().encode(),
            &vs.encode_map(),
        )
        .unwrap();
        let f = Bitmap::all(5000);
        let q = vs.vector(2).to_vec();
        let a = vs.search(&q, 10, &f, &SearchOpts::default()).0;
        let b = back.search(&q, 10, &f, &SearchOpts::default()).0;
        assert_eq!(a, b);
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use crate::vector::hnsw::HnswParams;
    use crate::vector::quant::Quantizer;

    fn store(n: usize, dims: usize, flat_max: usize) -> VectorStore {
        let mut rng = Rng::new(4242);
        let mut vs = VectorStore::new(dims, Metric::Cosine);
        for i in 0..n {
            let mut v: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut v);
            vs.push(i as u32, &v).unwrap();
        }
        vs.seal(Quantizer::Sq8, HnswParams::default(), flat_max);
        vs
    }

    /// A post-filter must still return `k` when `k` are admitted. Amplifying
    /// `ef` by `1/s` and then truncating the result to a rerank-sized slice
    /// throws the amplification away.
    #[test]
    fn a_post_filter_returns_k_results_when_k_are_admitted() {
        let (n, dims, k) = (20_000usize, 32usize, 10usize);
        let vs = store(n, dims, 512);
        // 20% selectivity: comfortably inside the post-filter band.
        let mut f = Bitmap::new(n);
        for i in (0..n).step_by(5) {
            f.set(i);
        }
        let mut rng = Rng::new(7);
        for _ in 0..8 {
            let mut q: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut q);
            let (got, rep) = vs.search(&q, k, &f, &SearchOpts::default());
            assert_eq!(rep.strategy, Some(Strategy::PostFilter));
            assert_eq!(got.len(), k, "post-filter returned {} of {k}", got.len());
            assert!(got.iter().all(|(d, _)| f.get(*d as usize)));
        }
    }

    /// A NaN distance used to pin the brute-force threshold, after which no
    /// further candidate could ever beat it.
    #[test]
    fn a_nan_distance_cannot_stop_the_exact_scan() {
        let mut vs = VectorStore::new(2, Metric::InnerProduct);
        for (i, v) in [[1.0f32, 0.0], [0.0, 5.0], [0.0, 4.0], [0.0, 3.0]].iter().enumerate() {
            vs.push(i as u32, v).unwrap();
        }
        // The query itself carries the NaN — nothing stored is malformed.
        let q = vec![f32::NAN, 1.0];
        let (got, _) = vs.search(&q, 2, &Bitmap::all(4), &SearchOpts::default());
        assert_eq!(got.len(), 2);
        // Every distance is NaN, so the answer is arbitrary — but it must be a
        // deterministic, total ordering rather than a scan that gave up.
        // (Compared by ordinal: NaN is not equal to itself.)
        let (again, _) = vs.search(&q, 2, &Bitmap::all(4), &SearchOpts::default());
        let ords = |v: &Vec<(u32, f32)>| v.iter().map(|(d, _)| *d).collect::<Vec<_>>();
        assert_eq!(ords(&got), ords(&again));

        // And a NaN reaching the heap must not stop the scan: with one bad
        // component in the query only, the finite comparisons still win.
        let q2 = vec![0.0f32, 1.0];
        let (good, _) = vs.search(&q2, 2, &Bitmap::all(4), &SearchOpts::default());
        assert_eq!(ords(&good), vec![1, 2], "nearest by inner product");
    }

    #[test]
    fn a_non_finite_vector_is_refused_before_it_reaches_a_code_table() {
        let mut vs = VectorStore::new(3, Metric::L2);
        assert!(vs.push(0, &[1.0, 2.0, 3.0]).is_ok());
        let e = vs.push(1, &[1.0, f32::INFINITY, 3.0]).unwrap_err().to_string();
        assert!(e.contains("finite"), "{e}");
        let e = vs.push(1, &[1.0, f32::NAN, 3.0]).unwrap_err().to_string();
        assert!(e.contains("finite"), "{e}");
    }

    /// `m0 = 256` filled a `u8` degree counter to zero, making every full node
    /// a sink.
    #[test]
    fn a_wide_graph_does_not_truncate_its_degrees() {
        let (n, dims) = (1200usize, 8usize);
        let mut rng = Rng::new(9);
        let data: Vec<f32> = (0..n * dims).map(|_| rng.next_normal()).collect();
        let dist = |a: u32, b: u32| {
            distance::l2_squared(
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let params = HnswParams { m: 128, m0: 256, ef_construction: 200, seed: 0x5eed };
        let g = crate::vector::hnsw::Hnsw::build(n, params, &dist);
        let mut missed = 0;
        for i in 0..n {
            let q = &data[i * dims..(i + 1) * dims];
            let d_to =
                |j: u32| distance::l2_squared(q, &data[j as usize * dims..(j as usize + 1) * dims]);
            if g.search(64, 1, None, &d_to).first().map(|(id, _)| *id) != Some(i as u32) {
                missed += 1;
            }
        }
        assert_eq!(missed, 0, "{missed} of {n} nodes were unreachable");
        // And it still round-trips with the wider counter.
        let back = crate::vector::hnsw::Hnsw::decode(&g.encode()).unwrap();
        assert_eq!(back.encode(), g.encode());
    }

    /// The 1-bit L2 estimator must agree with decoding the code and measuring.
    #[test]
    fn one_bit_l2_estimate_matches_the_decoded_vector() {
        let (n, dims) = (200usize, 16usize);
        let mut rng = Rng::new(11);
        let data: Vec<f32> = (0..n * dims).map(|_| rng.next_normal()).collect();
        let codes = crate::vector::quant::Codes::build(Quantizer::OneBit, dims, &data);
        let q: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
        let mut worst = 0.0f32;
        let mut dec = vec![0.0f32; dims];
        for i in 0..n {
            codes.decode(i, &mut dec);
            let exact = distance::distance(Metric::L2, &q, &dec);
            let est = codes.distance(Metric::L2, &q, i);
            worst = worst.max((exact - est).abs());
        }
        assert!(worst < 1e-3, "worst 1-bit L2 estimation error {worst}");
    }
}
