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
#[non_exhaustive]
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

/// A distance as `Cand::cmp` orders it.
///
/// Every threshold read out of a result heap has to be read through this. The
/// heap's order is total because `Cand::cmp` maps NaN to `+INFINITY`, but the
/// thresholds used to be the raw `w.d`: once a NaN-distance candidate reached
/// the top of the heap, `d < worst` was false for every later candidate, so
/// admission stopped, and `c.d > worst` was false too, so the early stop never
/// fired and the traversal ran to exhaustion. A NaN is the worst possible
/// distance in the heap and has to be the worst possible threshold as well.
#[inline]
fn ord_d(d: f32) -> f32 {
    if d.is_nan() {
        f32::INFINITY
    } else {
        d
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

/// The visited set for one traversal.
///
/// A `vec![false; count]` is allocated and zeroed on every search and every
/// build-time layer search, even though a traversal touches only about
/// `ef * m0` nodes: on a million-vector segment that is a megabyte of
/// zeroing per query per segment, and on the build path it makes sealing
/// quadratic in the number of vectors. This is an open-addressed set sized to
/// the traversal instead, so the cost follows the nodes actually visited.
///
/// `u32::MAX` marks an empty slot, which is safe because it is already the
/// graph's "no such node" sentinel (`entry`) and `decode` rejects any node id
/// that is not below `count`.
struct Visited {
    slots: Vec<u32>,
    mask: usize,
    len: usize,
}

impl Visited {
    const EMPTY: u32 = u32::MAX;

    fn with_capacity(hint: usize) -> Visited {
        // Linear probing degrades sharply past half full, so reserve twice the
        // hint. The ceiling keeps a caller that asks for a very wide `ef` from
        // reserving up front for a traversal it will probably not run;
        // `insert` grows from there, and doubling makes that amortized cheap.
        let mut cap = 64usize;
        while cap < hint.saturating_mul(2) && cap < (1 << 14) {
            cap *= 2;
        }
        Visited { slots: vec![Visited::EMPTY; cap], mask: cap - 1, len: 0 }
    }

    /// Index of `id`, or of the empty slot it belongs in. Terminates because
    /// the table is never more than half full.
    fn slot(slots: &[u32], mask: usize, id: u32) -> usize {
        // Fibonacci hashing: node ids are dense and consecutive, so the low
        // bits alone would pile every neighbour list into one probe cluster.
        let mut idx = ((id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask;
        while slots[idx] != Visited::EMPTY && slots[idx] != id {
            idx = (idx + 1) & mask;
        }
        idx
    }

    /// Marks `id` visited, returning true if it had not been seen before.
    fn insert(&mut self, id: u32) -> bool {
        let idx = Visited::slot(&self.slots, self.mask, id);
        if self.slots[idx] == id {
            return false;
        }
        self.slots[idx] = id;
        self.len += 1;
        if self.len * 2 >= self.slots.len() {
            self.grow();
        }
        true
    }

    fn grow(&mut self) {
        let cap = self.slots.len() * 2;
        let mask = cap - 1;
        let mut slots = vec![Visited::EMPTY; cap];
        for &id in &self.slots {
            if id != Visited::EMPTY {
                let idx = Visited::slot(&slots, mask, id);
                slots[idx] = id;
            }
        }
        self.slots = slots;
        self.mask = mask;
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
    /// The distance beside each link, kept only while building: when a
    /// neighbour's list overflows and is pruned, the candidates' distances
    /// are here rather than measured again (one per link, per insertion,
    /// before 0.35.0). Empty once the build returns and never serialised.
    dist0: Vec<f32>,
    upper_d: Vec<BTreeMap<u32, Vec<f32>>>,
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
            dist0: Vec::new(),
            upper_d: Vec::new(),
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

    /// The links of `id` at `level` with the distances the build stored.
    fn neighbors_with(&self, level: usize, id: u32) -> Vec<Cand> {
        let ids = self.neighbors(level, id);
        let ds: &[f32] = if level == 0 {
            let base = id as usize * self.params.m0;
            &self.dist0[base..base + ids.len()]
        } else {
            self.upper_d
                .get(level - 1)
                .and_then(|m| m.get(&id))
                .map(|v| v.as_slice())
                .unwrap_or(&[])
        };
        ids.iter().zip(ds).map(|(&n, &d)| Cand { d, id: n }).collect()
    }

    /// `set_neighbors`, with each link's distance kept beside it.
    fn set_neighbors_with(&mut self, level: usize, id: u32, ns: &[Cand]) {
        let ids: Vec<u32> = ns.iter().map(|c| c.id).collect();
        self.set_neighbors(level, id, &ids);
        if level == 0 {
            let m0 = self.params.m0;
            let base = id as usize * m0;
            let n = ns.len().min(m0);
            for (k, c) in ns.iter().take(n).enumerate() {
                self.dist0[base + k] = c.d;
            }
        } else {
            while self.upper_d.len() < level {
                self.upper_d.push(BTreeMap::new());
            }
            let mut v: Vec<f32> = ns.iter().map(|c| c.d).collect();
            v.truncate(self.params.m);
            self.upper_d[level - 1].insert(id, v);
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
        Hnsw::build_unless(count, params, dist, &|| false).expect("nothing stops it")
    }

    /// `build`, giving up when `stop` says so: `None`, and nothing kept.
    /// Asked once every 64 nodes -- a node's insertion is the unit of the
    /// work, and a graph of a hundred thousand is minutes -- so a console
    /// asked to stop during a merge's build stops within a moment of it
    /// rather than at its end.
    pub fn build_unless(
        count: usize,
        params: HnswParams,
        dist: &dyn Fn(u32, u32) -> f32,
        stop: &dyn Fn() -> bool,
    ) -> Option<Hnsw> {
        let mut g = Hnsw::empty(params);
        g.count = count;
        g.link0 = vec![0u32; count * params.m0];
        g.dist0 = vec![0.0f32; count * params.m0];
        g.deg0 = vec![0u16; count];
        g.node_level = vec![0u8; count];

        let mut rng = Rng::new(params.seed);
        let ml = 1.0 / (params.m as f64).ln();
        for i in 0..count {
            let l = ((-rng.next_f64().max(1e-12).ln()) * ml).floor() as usize;
            g.node_level[i] = l.min(15) as u8;
        }

        for i in 0..count {
            if i % 64 == 0 && stop() {
                return None;
            }
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
                g.set_neighbors_with(l, id, &selected);
                for c in &selected {
                    // The reverse link, with the distance already measured:
                    // the metric is symmetric, and the build keeps every
                    // link's distance beside it.
                    let n = c.id;
                    let mut ns = g.neighbors_with(l, n);
                    if !ns.iter().any(|x| x.id == id) {
                        ns.push(Cand { d: c.d, id });
                    }
                    if ns.len() > m {
                        // Re-select rather than truncate: dropping the farthest
                        // neighbour blindly is what turns a navigable graph into
                        // a set of disconnected clusters.
                        ns = g.select_neighbors(&ns, m, dist);
                    }
                    g.set_neighbors_with(l, n, &ns);
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
        g.dist0 = Vec::new();
        g.upper_d = Vec::new();
        Some(g)
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
        let mut visited = Visited::with_capacity(ef.saturating_mul(2).min(self.count));
        let mut cands: BinaryHeap<RevCand> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        for &e in eps {
            if e as usize >= self.count || !visited.insert(e) {
                continue;
            }
            let c = Cand { d: d_to(e), id: e };
            cands.push(RevCand(c));
            results.push(c);
        }
        while let Some(RevCand(c)) = cands.pop() {
            if let Some(worst) = results.peek() {
                if ord_d(c.d) > ord_d(worst.d) && results.len() >= ef {
                    break;
                }
            }
            for &n in self.neighbors(level, c.id) {
                if !visited.insert(n) {
                    continue;
                }
                let d = d_to(n);
                let worst = results.peek().map(|w| ord_d(w.d)).unwrap_or(f32::INFINITY);
                if results.len() < ef || ord_d(d) < worst {
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
    ) -> Vec<Cand> {
        let mut sorted = cands.to_vec();
        sorted.sort();
        let mut kept: Vec<Cand> = Vec::with_capacity(m);
        for c in &sorted {
            if kept.len() >= m {
                break;
            }
            let good = kept.iter().all(|k| dist(c.id, k.id) > c.d);
            if good {
                kept.push(*c);
            }
        }
        // Backfill with the nearest rejected candidates rather than return a
        // short list; an under-connected node is a recall hole.
        if kept.len() < m {
            for c in &sorted {
                if kept.len() >= m {
                    break;
                }
                if !kept.iter().any(|k| k.id == c.id) {
                    kept.push(*c);
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
        self.search_budgeted(ef, k, admit, usize::MAX, d_to).0
    }

    /// [`Hnsw::search`] with a hard ceiling on how many nodes level 0 may
    /// visit.
    ///
    /// Filter-aware traversal cannot stop until the result heap holds `ef`
    /// *admitted* nodes, so at selectivity `s` it visits about `ef / s` nodes,
    /// and as `s` approaches zero that is a full scan of the segment. This
    /// budget caps that, and when it binds the answer degrades to the best the
    /// budget found.
    ///
    /// `SearchOpts::max_visits` is how a caller passes one, knowingly: a
    /// budget that binds returns fewer or worse documents, so it is never a
    /// silent default, and the number of nodes visited is reported beside
    /// it so a plan shows whether it bound. Returns the hits and that count.
    pub fn search_budgeted(
        &self,
        ef: usize,
        k: usize,
        admit: Option<&Bitmap>,
        max_visits: usize,
        d_to: &dyn Fn(u32) -> f32,
    ) -> (Vec<(u32, f32)>, usize) {
        if self.count == 0 || self.entry == u32::MAX {
            return (Vec::new(), 0);
        }
        let mut ep = self.entry;
        let mut l = self.max_level;
        while l > 0 {
            ep = self.greedy_descend(ep, l, d_to);
            l -= 1;
        }
        let admitted = |id: u32| admit.map(|b| b.get(id as usize)).unwrap_or(true);

        let mut visited = Visited::with_capacity(ef.saturating_mul(2).min(self.count));
        let mut cands: BinaryHeap<RevCand> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        visited.insert(ep);
        let mut visits = 1usize;
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
        'traverse: while let Some(RevCand(c)) = cands.pop() {
            if crate::deadline::expired() {
                break;
            }
            let worst = results.peek().map(|w| ord_d(w.d)).unwrap_or(f32::INFINITY);
            if results.len() >= ef && ord_d(c.d) > worst {
                break;
            }
            for &n in self.neighbors(0, c.id) {
                if !visited.insert(n) {
                    continue;
                }
                if visits >= max_visits {
                    break 'traverse;
                }
                visits += 1;
                let d = d_to(n);
                let worst = results.peek().map(|w| ord_d(w.d)).unwrap_or(f32::INFINITY);
                // Expansion is unconditional; admission is not. This is the
                // whole of the ACORN idea.
                if results.len() < ef || ord_d(d) < worst {
                    cands.push(RevCand(Cand { d, id: n }));
                }
                if admitted(n) && (results.len() < ef || ord_d(d) < worst) {
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
        (v.into_iter().map(|c| (c.id, c.d)).collect(), visits)
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
        let corrupt = |what: &str| Error::Storage(format!("hnsw: {what}"));
        let mut i = 0usize;
        let count = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        // Every node costs at least a level byte and a two-byte degree, so a
        // count larger than the whole input cannot describe a real graph — and
        // `i + count` on an unchecked value wraps before any slice bound is
        // ever consulted.
        if count > b.len() {
            return Err(bad());
        }
        let m = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let m0 = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        // `count * m0` sizes the level-0 link table. Degrees are stored as
        // `u16`, so no honest graph has more slots per node than that; without
        // the bound a stream can ask for a terabyte-scale allocation, which
        // aborts the process instead of returning an error.
        if count > 0 && (m0 == 0 || m0 > u16::MAX as usize) {
            return Err(corrupt("implausible m0"));
        }
        let efc = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let seed = get_u64(b, &mut i).ok_or_else(bad)?;
        let entry = get_u32(b, &mut i).ok_or_else(bad)?;
        // `search` indexes the graph by `entry` without checking it. The empty
        // graph legitimately carries the `u32::MAX` sentinel.
        if entry != u32::MAX && entry as usize >= count {
            return Err(corrupt("entry out of range"));
        }
        let max_level = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        // Per-node levels are `u8`, so nothing this encoder writes can sit
        // above 255. `search` walks down from `max_level` one level at a time,
        // so an unchecked value spins for billions of empty iterations.
        if max_level > u8::MAX as usize {
            return Err(corrupt("implausible max_level"));
        }
        let node_level = b.get(i..i + count).ok_or_else(bad)?.to_vec();
        i += count;
        let mut deg0 = Vec::with_capacity(count);
        for _ in 0..count {
            let d = b.get(i..i + 2).ok_or_else(bad)?;
            deg0.push(u16::from_le_bytes(d.try_into().unwrap()));
            i += 2;
        }
        let params = HnswParams { m, m0, ef_construction: efc, seed };
        let slots = count.checked_mul(m0).ok_or_else(|| corrupt("link table overflows"))?;
        // `try_reserve` rather than `vec![_; slots]`: a corrupt header should
        // come back as a storage error, not as an allocation failure that
        // aborts the process.
        let mut link0: Vec<u32> = Vec::new();
        link0.try_reserve_exact(slots).map_err(|_| corrupt("link table too large"))?;
        link0.resize(slots, 0u32);
        for node in 0..count {
            let d = deg0[node] as usize;
            // A degree above `m0` writes into the *next* node's slots, and off
            // the end of the table entirely for the last node.
            if d > m0 {
                return Err(corrupt("degree exceeds m0"));
            }
            for j in 0..d {
                let n = get_u32(b, &mut i).ok_or_else(bad)?;
                if n as usize >= count {
                    return Err(corrupt("link out of range"));
                }
                link0[node * m0 + j] = n;
            }
        }
        let nlayers = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        // Each layer is at least one varint, each node at least five bytes and
        // each link four, so the remaining input bounds every stream-supplied
        // repeat count. Reserving on the raw value instead lets a few bytes
        // request an unbounded allocation.
        if nlayers > b.len() - i {
            return Err(bad());
        }
        let mut upper = Vec::with_capacity(nlayers);
        for _ in 0..nlayers {
            let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            if n > (b.len() - i) / 5 {
                return Err(bad());
            }
            let mut layer = BTreeMap::new();
            for _ in 0..n {
                let id = get_u32(b, &mut i).ok_or_else(bad)?;
                if id as usize >= count {
                    return Err(corrupt("upper node out of range"));
                }
                let k = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                if k > (b.len() - i) / 4 {
                    return Err(bad());
                }
                let mut ns = Vec::with_capacity(k);
                for _ in 0..k {
                    let x = get_u32(b, &mut i).ok_or_else(bad)?;
                    if x as usize >= count {
                        return Err(corrupt("upper link out of range"));
                    }
                    ns.push(x);
                }
                layer.insert(id, ns);
            }
            upper.push(layer);
        }
        Ok(Hnsw {
            params,
            count,
            entry,
            max_level,
            link0,
            deg0,
            upper,
            node_level,
            dist0: Vec::new(),
            upper_d: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_graph_decoding_never_panics() {
        let (n, dims) = (300usize, 8usize);
        let mut rng = Rng::new(4);
        let data: Vec<f32> = (0..n * dims).map(|_| rng.next_normal()).collect();
        let dist = |a: u32, b: u32| {
            crate::vector::distance::l2_squared(
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        crate::fuzz::sweep(71, &[g.encode()], 3000, |b| {
            let _ = Hnsw::decode(b);
        });
    }
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

    /// Hand-rolled encoder. `Hnsw::encode` can only produce well-formed
    /// graphs, and every check below is about a stream that is not.
    fn hand_encoded(count: u64, m0: u64, entry: u32, deg0: &[u16], links: &[u32]) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, count);
        put_uvarint(&mut out, 16);
        put_uvarint(&mut out, m0);
        put_uvarint(&mut out, 200);
        put_u64(&mut out, 1);
        put_u32(&mut out, entry);
        put_uvarint(&mut out, 0);
        out.resize(out.len() + count as usize, 0u8);
        for d in deg0 {
            out.extend_from_slice(&d.to_le_bytes());
        }
        for l in links {
            put_u32(&mut out, *l);
        }
        put_uvarint(&mut out, 0);
        out
    }

    #[test]
    fn an_unbounded_m0_is_rejected_rather_than_requesting_a_terabyte_allocation() {
        let b = hand_encoded(1, 1 << 40, 0, &[0], &[]);
        assert!(Hnsw::decode(&b).is_err());
    }

    #[test]
    fn a_count_larger_than_the_input_is_rejected_before_the_offset_overflows() {
        let mut b = Vec::new();
        put_uvarint(&mut b, u64::MAX / 2);
        put_uvarint(&mut b, 16);
        put_uvarint(&mut b, 32);
        put_uvarint(&mut b, 200);
        put_u64(&mut b, 1);
        put_u32(&mut b, 0);
        put_uvarint(&mut b, 0);
        assert!(Hnsw::decode(&b).is_err());
    }

    #[test]
    fn a_degree_larger_than_m0_is_rejected_rather_than_spilling_into_the_next_node() {
        // Three links in one slot: the third would land past the end of the
        // whole table, and with more nodes it would silently rewrite the next
        // node's neighbours.
        let b = hand_encoded(2, 1, 0, &[3, 0], &[0, 1, 0]);
        assert!(Hnsw::decode(&b).is_err());
    }

    #[test]
    fn an_out_of_range_entry_point_is_rejected_rather_than_panicking_during_search() {
        let b = hand_encoded(2, 4, 7, &[0, 0], &[]);
        assert!(Hnsw::decode(&b).is_err());
    }

    #[test]
    fn an_out_of_range_link_is_rejected_rather_than_steering_search_off_the_end() {
        assert!(Hnsw::decode(&hand_encoded(2, 4, 0, &[1, 0], &[9])).is_err());
        // The same graph with an in-range link decodes, so the rejections
        // above are the corruption and not the hand-rolled framing.
        assert!(Hnsw::decode(&hand_encoded(2, 4, 0, &[1, 0], &[1])).is_ok());
    }

    #[test]
    fn a_node_reinserted_after_the_visited_set_grows_is_still_reported_as_visited() {
        let mut v = Visited::with_capacity(4);
        let n = 5000u32;
        for i in 0..n {
            assert!(v.insert(i), "{i} had not been visited yet");
        }
        for i in 0..n {
            assert!(!v.insert(i), "{i} was visited before the table grew");
        }
        assert_eq!(v.len, n as usize);
    }

    #[test]
    fn a_search_never_returns_the_same_node_twice() {
        let (n, dims) = (800usize, 16usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        let q = &data[0..dims];
        let d_to = |i: u32| {
            distance::distance(Metric::Cosine, q, &data[i as usize * dims..(i as usize + 1) * dims])
        };
        let got = g.search(200, 200, None, &d_to);
        let mut ids: Vec<u32> = got.iter().map(|(i, _)| *i).collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), before, "the visited set let a node be expanded twice");
    }

    #[test]
    fn a_low_selectivity_filter_stops_at_the_visit_budget_instead_of_scanning_the_segment() {
        let (n, dims) = (1000usize, 16usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        // Two admitted out of a thousand: the result heap can never reach
        // `ef`, so the distance-based stop never fires and the traversal runs
        // until the graph is exhausted.
        let mut admit = Bitmap::new(n);
        admit.set(3);
        admit.set(700);
        let q = &data[0..dims];
        let seen = std::cell::Cell::new(0usize);
        let d_to = |i: u32| {
            seen.set(seen.get() + 1);
            distance::distance(Metric::Cosine, q, &data[i as usize * dims..(i as usize + 1) * dims])
        };
        let full = g.search(64, 10, Some(&admit), &d_to);
        let unbudgeted = seen.get();
        seen.set(0);
        let capped = g.search_budgeted(64, 10, Some(&admit), 32, &d_to).0;
        let budgeted = seen.get();
        assert!(unbudgeted > n / 2, "unbudgeted traversal should scan the segment: {unbudgeted}");
        assert!(budgeted < n / 4, "the budget should have cut the traversal short: {budgeted}");
        assert!(capped.len() <= full.len());
    }

    /// A NaN distance used to freeze the result heap: `Cand::cmp` sorts it to
    /// the top as the worst candidate, and the raw `worst` read off that top
    /// made both `d < worst` and `c.d > worst` false, so nothing was admitted
    /// after it and the early stop never fired.
    #[test]
    fn a_nan_distance_neither_freezes_admission_nor_disables_the_early_stop() {
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
        let q = &data[0..dims];
        let seen = std::cell::Cell::new(0usize);
        let clean = |i: u32| {
            seen.set(seen.get() + 1);
            distance::distance(Metric::Cosine, q, &data[i as usize * dims..(i as usize + 1) * dims])
        };
        // The entry point is the one node guaranteed to be admitted before the
        // heap is full, so poisoning it puts the NaN at the top of the heap
        // from the first iteration.
        let bad = g.entry;
        let poisoned = |i: u32| if i == bad { f32::NAN } else { clean(i) };

        let got = g.search(10, 10, None, &poisoned);
        let poisoned_visits = seen.get();
        seen.set(0);
        let _ = g.search(10, 10, None, &clean);
        let clean_visits = seen.get();

        assert!(
            got.iter().all(|(_, d)| !d.is_nan()),
            "a NaN candidate was never displaced: {got:?}"
        );
        assert!(
            poisoned_visits < clean_visits * 4,
            "the early stop never fired: {poisoned_visits} visits against {clean_visits}"
        );
    }

    /// The same defect on the build path, where a frozen layer search silently
    /// degrades the graph instead of the answer.
    #[test]
    fn a_nan_distance_does_not_freeze_the_build_time_layer_search() {
        let (n, dims) = (200usize, 8usize);
        let data = corpus(n, dims);
        let dist = |a: u32, b: u32| {
            distance::distance(
                Metric::Cosine,
                &data[a as usize * dims..(a as usize + 1) * dims],
                &data[b as usize * dims..(b as usize + 1) * dims],
            )
        };
        let g = Hnsw::build(n, HnswParams::default(), &dist);
        let q = &data[0..dims];
        let bad = g.entry;
        let d_to = |i: u32| {
            if i == bad {
                f32::NAN
            } else {
                distance::distance(
                    Metric::Cosine,
                    q,
                    &data[i as usize * dims..(i as usize + 1) * dims],
                )
            }
        };
        let out = g.search_layer_build(&[bad], 8, 0, &d_to);
        assert_eq!(out.len(), 8);
        assert!(out.iter().all(|c| !c.d.is_nan()), "a NaN candidate held a slot in the layer");
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
