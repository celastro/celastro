//! HNSW: hierarchical navigable small world graph.
//!
//! The tier for small sealed segments (§5.2), where memory cost is acceptable
//! and latency matters most. Three things here are not textbook and are worth
//! naming:
//!
//! * **Build uses full precision, search uses codes.** Graph quality is fixed
//!   once at build time and paid back on every query; traversal is the part
//!   that must fit in memory. Building over quantized codes would bake the
//!   quantization error into the graph structure itself.
//! * **Deleted vectors stay in the graph as routing nodes** until compaction
//!   rewrites the segment (§5.2). They are excluded at admission by the
//!   visibility bitmap and never unlinked, because unlinking degrades
//!   connectivity — the neighbours that reached each other only through a
//!   deleted hub stop reaching each other at all.
//! * **Filter-aware traversal is a mode of the same search**, not a separate
//!   algorithm: expansion follows non-matching neighbours, admission to the
//!   result heap requires a match (ACORN-style, §5.3).

use std::collections::{BTreeMap, BinaryHeap};

use crate::bitmap::Bitmap;
use crate::codec::*;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub struct HnswParams {
    /// Neighbours per node above level 0.
    pub m: usize,
    /// Neighbours per node at level 0 — conventionally `2m`.
    pub m0: usize,
    pub ef_construction: usize,
    /// Seeded so that a rebuild of the same input produces the same graph.
    /// Approximate results legitimately depend on segment layout (§14), so the
    /// layout at least has to be reproducible.
    pub seed: u64,
}

impl Default for HnswParams {
    fn default() -> Self {
        HnswParams { m: 16, m0: 32, ef_construction: 200, seed: 0x5eed }
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Cand {
    d: f32,
    id: u32,
}

impl Eq for Cand {}
impl Ord for Cand {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        let a = if self.d.is_nan() { f32::INFINITY } else { self.d };
        let b = if o.d.is_nan() { f32::INFINITY } else { o.d };
        a.partial_cmp(&b).unwrap().then(self.id.cmp(&o.id))
    }
}
impl PartialOrd for Cand {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

/// Min-heap ordering by distance.
#[derive(Clone, Copy, PartialEq)]
struct RevCand(Cand);
impl Eq for RevCand {}
impl Ord for RevCand {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.cmp(&self.0)
    }
}
impl PartialOrd for RevCand {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

pub struct Hnsw {
    pub params: HnswParams,
    pub count: usize,
    entry: u32,
    max_level: usize,
    /// Level 0 adjacency, flat: `m0` slots per node, `deg0` says how many are
    /// live. One allocation instead of N vectors.
    link0: Vec<u32>,
    /// `u16`, not `u8`: `m0` is configurable, and at `m0 = 256` a full
    /// neighbour list wraps to degree zero, turning the node into a sink no
    /// traversal can leave.
    deg0: Vec<u16>,
    /// Levels above 0 are sparse — a node is present with probability
    /// `1/e^level`, so a map beats a dense array by orders of magnitude.
    upper: Vec<BTreeMap<u32, Vec<u32>>>,
    node_level: Vec<u8>,
}

impl Hnsw {
    pub fn empty(params: HnswParams) -> Hnsw {
        Hnsw {
            params,
            count: 0,
            entry: u32::MAX,
            max_level: 0,
            link0: Vec::new(),
            deg0: Vec::new(),
            upper: Vec::new(),
            node_level: Vec::new(),
        }
    }

    pub fn memory_bytes(&self) -> usize {
        self.link0.len() * 4
            + self.deg0.len()
            + self.node_level.len()
            + self
                .upper
                .iter()
                .map(|m| m.values().map(|v| v.len() * 4 + 32).sum::<usize>())
                .sum::<usize>()
    }

    fn neighbors(&self, level: usize, id: u32) -> &[u32] {
        if level == 0 {
            let base = id as usize * self.params.m0;
            &self.link0[base..base + self.deg0[id as usize] as usize]
        } else {
            self.upper.get(level - 1).and_then(|m| m.get(&id)).map(|v| v.as_slice()).unwrap_or(&[])
        }
    }

    fn set_neighbors(&mut self, level: usize, id: u32, ns: &[u32]) {
        if level == 0 {
            let m0 = self.params.m0;
            let base = id as usize * m0;
            let n = ns.len().min(m0);
            self.link0[base..base + n].copy_from_slice(&ns[..n]);
            self.deg0[id as usize] = n as u16;
        } else {
            while self.upper.len() < level {
                self.upper.push(BTreeMap::new());
            }
            let m = self.params.m;
            let mut v = ns.to_vec();
            v.truncate(m);
            self.upper[level - 1].insert(id, v);
        }
    }

    /// Build over full-precision vectors. `dist(a, b)` is the metric distance
    /// between two stored vectors.
    pub fn build(count: usize, params: HnswParams, dist: &dyn Fn(u32, u32) -> f32) -> Hnsw {
        let mut g = Hnsw::empty(params);
        g.count = count;
        g.link0 = vec![0u32; count * params.m0];
        g.deg0 = vec![0u16; count];
        g.node_level = vec![0u8; count];

        let mut rng = Rng::new(params.seed);
        let ml = 1.0 / (params.m as f64).ln();
        for i in 0..count {
            let l = ((-rng.next_f64().max(1e-12).ln()) * ml).floor() as usize;
            g.node_level[i] = l.min(15) as u8;
        }

        for i in 0..count {
            let id = i as u32;
            let level = g.node_level[i] as usize;
            if g.entry == u32::MAX {
                g.entry = id;
                g.max_level = level;
                for l in 1..=level {
                    g.set_neighbors(l, id, &[]);
                }
                continue;
            }
            let d_to = |a: u32| dist(id, a);
            let mut ep = g.entry;
            // Descend the express lanes greedily.
            let mut l = g.max_level;
            while l > level {
                ep = g.greedy_descend(ep, l, &d_to);
                if l == 0 {
                    break;
                }
                l -= 1;
            }
            let mut cur_ep = vec![ep];
            for l in (0..=level.min(g.max_level)).rev() {
                let found = g.search_layer_build(&cur_ep, params.ef_construction, l, &d_to);
                let m = if l == 0 { params.m0 } else { params.m };
                let selected = g.select_neighbors(&found, m, dist);
                g.set_neighbors(l, id, &selected);
                for &n in &selected {
                    let mut ns = g.neighbors(l, n).to_vec();
                    if !ns.contains(&id) {
                        ns.push(id);
                    }
                    if ns.len() > m {
                        // Re-select rather than truncate: dropping the farthest
                        // neighbour blindly is what turns a navigable graph into
                        // a set of disconnected clusters.
                        let cands: Vec<Cand> =
                            ns.iter().map(|&x| Cand { d: dist(n, x), id: x }).collect();
                        ns = g.select_neighbors(&cands, m, dist);
                    }
                    g.set_neighbors(l, n, &ns);
                }
                cur_ep = found.iter().map(|c| c.id).collect();
                if cur_ep.is_empty() {
                    cur_ep = vec![ep];
                }
            }
            if level > g.max_level {
                g.max_level = level;
                g.entry = id;
            }
        }
        g
    }

    fn greedy_descend(&self, mut ep: u32, level: usize, d_to: &dyn Fn(u32) -> f32) -> u32 {
        let mut best = d_to(ep);
        loop {
            let mut improved = false;
            for &n in self.neighbors(level, ep) {
                let d = d_to(n);
                if d < best {
                    best = d;
                    ep = n;
                    improved = true;
                }
            }
            if !improved {
                return ep;
            }
        }
    }

    fn search_layer_build(
        &self,
        eps: &[u32],
        ef: usize,
        level: usize,
        d_to: &dyn Fn(u32) -> f32,
    ) -> Vec<Cand> {
        let mut visited = vec![false; self.count];
        let mut cands: BinaryHeap<RevCand> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        for &e in eps {
            if e as usize >= self.count || visited[e as usize] {
                continue;
            }
            visited[e as usize] = true;
            let c = Cand { d: d_to(e), id: e };
            cands.push(RevCand(c));
            results.push(c);
        }
        while let Some(RevCand(c)) = cands.pop() {
            if let Some(worst) = results.peek() {
                if c.d > worst.d && results.len() >= ef {
                    break;
                }
            }
            for &n in self.neighbors(level, c.id) {
                if visited[n as usize] {
                    continue;
                }
                visited[n as usize] = true;
                let d = d_to(n);
                let worst = results.peek().map(|w| w.d).unwrap_or(f32::INFINITY);
                if results.len() < ef || d < worst {
                    let nc = Cand { d, id: n };
                    cands.push(RevCand(nc));
                    results.push(nc);
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut v: Vec<Cand> = results.into_vec();
        v.sort();
        v
    }

    /// Heuristic neighbour selection: keep a candidate only if it is closer to
    /// the new node than to any already-kept neighbour. This is what produces
    /// long-range links instead of a clique of mutual nearest neighbours.
    fn select_neighbors(
        &self,
        cands: &[Cand],
        m: usize,
        dist: &dyn Fn(u32, u32) -> f32,
    ) -> Vec<u32> {
        let mut sorted = cands.to_vec();
        sorted.sort();
        let mut kept: Vec<u32> = Vec::with_capacity(m);
        for c in sorted {
            if kept.len() >= m {
                break;
            }
            let good = kept.iter().all(|&k| dist(c.id, k) > c.d);
            if good {
                kept.push(c.id);
            }
        }
        // Backfill with the nearest rejected candidates rather than return a
        // short list; an under-connected node is a recall hole.
        if kept.len() < m {
            let mut sorted2 = cands.to_vec();
            sorted2.sort();
            for c in sorted2 {
                if kept.len() >= m {
                    break;
                }
                if !kept.contains(&c.id) {
                    kept.push(c.id);
                }
            }
        }
        kept
    }

    /// Search with an optional admission filter over **vector ordinals**.
    ///
    /// `admit = None` is plain ANN. `admit = Some(bm)` is the filter-aware
    /// traversal: every neighbour is followed, only members of `bm` are
    /// admitted to the result heap.
    ///
    /// Returns at most `min(k, ef)` results: `ef` is the traversal budget and
    /// `k` only truncates.
    pub fn search(
        &self,
        ef: usize,
        k: usize,
        admit: Option<&Bitmap>,
        d_to: &dyn Fn(u32) -> f32,
    ) -> Vec<(u32, f32)> {
        if self.count == 0 || self.entry == u32::MAX {
            return Vec::new();
        }
        let mut ep = self.entry;
        let mut l = self.max_level;
        while l > 0 {
            ep = self.greedy_descend(ep, l, d_to);
            l -= 1;
        }
        let admitted = |id: u32| admit.map(|b| b.get(id as usize)).unwrap_or(true);

        let mut visited = vec![false; self.count];
        let mut cands: BinaryHeap<RevCand> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        visited[ep as usize] = true;
        let c0 = Cand { d: d_to(ep), id: ep };
        cands.push(RevCand(c0));
        if admitted(ep) {
            results.push(c0);
        }
        // `ef` alone governs traversal breadth. Raising it to `k` would let a
        // caller that merely wants a wide *result* list silently widen the
        // search, which makes `ef_search` mean nothing and hides the cost of
        // asking for more candidates. A caller cannot receive more than the
        // heap held, and that is the honest answer.
        let ef = ef.max(1);
        while let Some(RevCand(c)) = cands.pop() {
            let worst = results.peek().map(|w| w.d).unwrap_or(f32::INFINITY);
            if results.len() >= ef && c.d > worst {
                break;
            }
            for &n in self.neighbors(0, c.id) {
                if visited[n as usize] {
                    continue;
                }
                visited[n as usize] = true;
                let d = d_to(n);
                let worst = results.peek().map(|w| w.d).unwrap_or(f32::INFINITY);
                // Expansion is unconditional; admission is not. This is the
                // whole of the ACORN idea.
                if results.len() < ef || d < worst {
                    cands.push(RevCand(Cand { d, id: n }));
                }
                if admitted(n) && (results.len() < ef || d < worst) {
                    results.push(Cand { d, id: n });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut v: Vec<Cand> = results.into_vec();
        v.sort();
        v.truncate(k);
        v.into_iter().map(|c| (c.id, c.d)).collect()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.count as u64);
        put_uvarint(&mut out, self.params.m as u64);
        put_uvarint(&mut out, self.params.m0 as u64);
        put_uvarint(&mut out, self.params.ef_construction as u64);
        put_u64(&mut out, self.params.seed);
        put_u32(&mut out, self.entry);
        put_uvarint(&mut out, self.max_level as u64);
        out.extend_from_slice(&self.node_level);
        for d in &self.deg0 {
            out.extend_from_slice(&d.to_le_bytes());
        }
        for i in 0..self.count {
            let d = self.deg0[i] as usize;
            let base = i * self.params.m0;
            for j in 0..d {
                put_u32(&mut out, self.link0[base + j]);
            }
        }
        put_uvarint(&mut out, self.upper.len() as u64);
        for layer in &self.upper {
            put_uvarint(&mut out, layer.len() as u64);
            for (id, ns) in layer {
                put_u32(&mut out, *id);
                put_uvarint(&mut out, ns.len() as u64);
                for n in ns {
                    put_u32(&mut out, *n);
                }
            }
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<Hnsw> {
        let bad = || Error::Storage("hnsw: truncated".into());
        let mut i = 0usize;
        let count = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let m = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let m0 = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let efc = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let seed = get_u64(b, &mut i).ok_or_else(bad)?;
        let entry = get_u32(b, &mut i).ok_or_else(bad)?;
        let max_level = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let node_level = b.get(i..i + count).ok_or_else(bad)?.to_vec();
        i += count;
        let mut deg0 = Vec::with_capacity(count);
        for _ in 0..count {
            let d = b.get(i..i + 2).ok_or_else(bad)?;
            deg0.push(u16::from_le_bytes(d.try_into().unwrap()));
            i += 2;
        }
        let params = HnswParams { m, m0, ef_construction: efc, seed };
        let mut link0 = vec![0u32; count * m0];
        for node in 0..count {
            let d = deg0[node] as usize;
            for j in 0..d {
                link0[node * m0 + j] = get_u32(b, &mut i).ok_or_else(bad)?;
            }
        }
        let nlayers = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let mut upper = Vec::with_capacity(nlayers);
        for _ in 0..nlayers {
            let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            let mut layer = BTreeMap::new();
            for _ in 0..n {
                let id = get_u32(b, &mut i).ok_or_else(bad)?;
                let k = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut ns = Vec::with_capacity(k);
                for _ in 0..k {
                    ns.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                layer.insert(id, ns);
            }
            upper.push(layer);
        }
        Ok(Hnsw { params, count, entry, max_level, link0, deg0, upper, node_level })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Metric;
    use crate::vector::distance;

    fn corpus(n: usize, dims: usize) -> Vec<f32> {
        let mut rng = Rng::new(11);
        let mut v = Vec::with_capacity(n * dims);
        for _ in 0..n {
            let mut x: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut x);
            v.extend_from_slice(&x);
        }
        v
    }

    fn exact_top(
        data: &[f32],
        dims: usize,
        q: &[f32],
        k: usize,
        admit: Option<&Bitmap>,
    ) -> Vec<u32> {
        let n = data.len() / dims;
        let mut all: Vec<(u32, f32)> = (0..n)
            .filter(|i| admit.map(|b| b.get(*i)).unwrap_or(true))
            .map(|i| {
                (i as u32, distance::distance(Metric::Cosine, q, &data[i * dims..(i + 1) * dims]))
            })
            .collect();
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        all.into_iter().take(k).map(|(i, _)| i).collect()
    }

    #[test]
    fn recall_at_10_meets_the_target() {
        let (n, dims, k) = (3000usize, 48usize, 10usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        let mut rng = Rng::new(2024);
        let (mut hits, trials) = (0usize, 30);
        for _ in 0..trials {
            let mut q: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut q);
            let d_to = |i: u32| {
                distance::distance(
                    Metric::Cosine,
                    &q,
                    &data[i as usize * dims..(i as usize + 1) * dims],
                )
            };
            let got = g.search(64, k, None, &d_to);
            let truth = exact_top(&data, dims, &q, k, None);
            hits += got.iter().filter(|(i, _)| truth.contains(i)).count();
        }
        let recall = hits as f64 / (trials * k) as f64;
        // §1 targets recall@10 >= 0.95 at default settings.
        assert!(recall >= 0.95, "recall@10 = {recall}");
    }

    #[test]
    fn filter_aware_traversal_beats_post_filtering_at_low_selectivity() {
        let (n, dims, k) = (3000usize, 48usize, 10usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        // 2% selectivity: post-filtering a top-64 would return almost nothing.
        let mut admit = Bitmap::new(n);
        for i in (0..n).step_by(50) {
            admit.set(i);
        }
        let mut rng = Rng::new(5);
        let (mut aware_hits, mut post_hits, trials) = (0usize, 0usize, 20);
        for _ in 0..trials {
            let mut q: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(Metric::Cosine, &mut q);
            let d_to = |i: u32| {
                distance::distance(
                    Metric::Cosine,
                    &q,
                    &data[i as usize * dims..(i as usize + 1) * dims],
                )
            };
            let truth = exact_top(&data, dims, &q, k, Some(&admit));
            let aware = g.search(128, k, Some(&admit), &d_to);
            aware_hits += aware.iter().filter(|(i, _)| truth.contains(i)).count();
            let post: Vec<(u32, f32)> = g
                .search(128, 128, None, &d_to)
                .into_iter()
                .filter(|(i, _)| admit.get(*i as usize))
                .take(k)
                .collect();
            post_hits += post.iter().filter(|(i, _)| truth.contains(i)).count();
        }
        let aware_recall = aware_hits as f64 / (trials * k) as f64;
        let post_recall = post_hits as f64 / (trials * k) as f64;
        assert!(
            aware_recall > post_recall,
            "filter-aware {aware_recall} should beat post-filter {post_recall}"
        );
        assert!(aware_recall >= 0.9, "filter-aware recall@10 = {aware_recall}");
    }

    #[test]
    fn graph_round_trips_through_bytes() {
        let (n, dims) = (500usize, 16usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        let back = Hnsw::decode(&g.encode()).unwrap();
        let q = &data[0..dims];
        let d_to = |i: u32| {
            distance::distance(Metric::Cosine, q, &data[i as usize * dims..(i as usize + 1) * dims])
        };
        assert_eq!(g.search(32, 5, None, &d_to), back.search(32, 5, None, &d_to));
    }

    #[test]
    fn build_is_deterministic_for_a_fixed_seed() {
        let (n, dims) = (400usize, 16usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let a = Hnsw::build(n, HnswParams::default(), &dist);
        let b = Hnsw::build(n, HnswParams::default(), &dist);
        assert_eq!(a.encode(), b.encode());
    }
}
