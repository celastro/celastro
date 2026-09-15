//! A bounded walk over an edge collection, resolved by the coordinator into
//! a set of primary keys before the scatter.
//!
//! `WHERE id WITHIN k HOPS OF 'x' VIA cites` is a filter like `text_match`:
//! it selects and contributes no rank. What makes it different is that no
//! unit can evaluate it alone -- an edge lives in another collection, on
//! whatever shard its own key put it -- so the coordinator walks first. Each
//! hop asks every edge shard for the edges leaving the frontier
//! ([`ShardService::expand`]), keeps the keys that are live nodes at the
//! statement's instant ([`ShardService::present`]), and the union over
//! `1..k` hops, the start excluded, is handed to the executor as the key set
//! of an `id IN (...)`. Every unit then turns it into an ordinal bitmap
//! exactly as it does for an `IN`, and the hybrid intersection is untouched.
//!
//! The walk is deterministic and layout-independent: pairs are sorted,
//! frontiers are sorted and distinct, and both caps cut lexicographically,
//! so the answer at one shard is the answer at six. A cut is reported on
//! the response and in the plan, per hop, never applied quietly.
//!
//! Every set the coordinator keeps -- the frontier, the keys seen, the keys
//! present, the answer -- is a sorted vector, and every operation on them
//! is one merge pass: a hub's second hop carries tens of thousands of keys,
//! and an ordered set of owned strings spent more on them than the shards
//! did on the hop.

use std::cmp::Ordering;
use std::time::Instant;

use crate::catalog::{Collection, IndexDef, IndexKind};
use crate::column::CmpOp;
use crate::deadline;
use crate::error::{Error, Result};
use crate::plan::exec;
use crate::plan::explain::{HopExplain, UnitExplain, WalkExplain};
use crate::plan::service::ShardService;
use crate::shard::{Searchable, Shard};
use crate::sql::ast::{Expr, HybridSource, OrderBy, Select};
use crate::time::Timestamp;
use crate::value::Value;

/// What an edge shard needs to expand one hop.
pub struct ExpandRequest<'a> {
    /// The edge collection.
    pub coll: &'a Collection,
    /// The keys to expand from.
    pub frontier: &'a [String],
    pub ts: Timestamp,
    /// Keep at most this many pairs per `from` key, the lexicographically
    /// first by `to`. The coordinator asks for one more than its cap, which
    /// is how it tells a full expansion from a cut one; the global first
    /// `n` are among the union of each shard's first `n + 1`.
    pub limit: Option<usize>,
    /// Follow the adjacency index against its declared order.
    pub reverse: bool,
    /// The structured predicate on the edge collection for this hop, if
    /// the walk has one for it ([`filter_for`]).
    pub filter: Option<&'a Expr>,
    /// The statement's text and parameters, for a shard on another node,
    /// which re-parses them and finds the filter as the `walk`-th walk of
    /// the statement at hop `hop`.
    pub statement: &'a str,
    pub params: &'a [Value],
    pub walk: usize,
    pub hop: usize,
}

/// The edge filter for hop `hop` (from 1) of a walk with these filters:
/// none, the one for every hop, or the `hop`-th of one per hop. A count
/// that is neither one nor the walk's `k` was refused before the walk.
pub fn filter_for(filters: &[Expr], hop: usize) -> Option<&Expr> {
    match filters.len() {
        0 => None,
        1 => filters.first(),
        _ => filters.get(hop - 1),
    }
}

/// A shard's answer to one hop.
pub struct HopExpansion {
    /// `(from, to)` pairs, sorted and distinct.
    pub pairs: Vec<(String, String)>,
    /// Units scanned for want of an adjacency region: the memtable, and any
    /// segment sealed before the index was declared.
    pub scanned: usize,
}

/// The shard's half of a hop: every live edge whose probed column is in the
/// frontier and which the filter admits, as `(from, to)` pairs, sorted and
/// distinct, at most `limit` per `from`. An undirected collection yields
/// both orders.
///
/// A segment with an adjacency region is probed once per frontier key --
/// a binary search and a run of ordinals -- and nothing else in it is
/// read but the ordinals' `to` strings and, when there is an edge filter,
/// the filter's own columns. A unit without one (the memtable; a segment
/// sealed before `CREATE INDEX`, until compaction rewrites it) is scanned
/// against the frontier as a set, as every unit was before the region
/// existed, and counted so the plan can say so.
pub(crate) fn expand_on(shard: &Shard, req: &ExpandRequest<'_>) -> Result<HopExpansion> {
    let idx = req
        .coll
        .adjacency_index()
        .ok_or_else(|| Error::Plan(format!("no adjacency index on `{}`", req.coll.name)))?;
    let IndexKind::Adjacency { to } = &idx.kind else { unreachable!("adjacency_index") };
    let (from, to) = (idx.path.as_str(), to.as_str());
    let orders: Vec<(&str, &str)> = if req.coll.undirected {
        vec![(from, to), (to, from)]
    } else if req.reverse {
        vec![(to, from)]
    } else {
        vec![(from, to)]
    };
    let lit = Value::Array(req.frontier.iter().map(|k| Value::Str(k.clone())).collect());
    let snap = shard.snapshot_at(req.ts);
    let units = shard.sources(&snap);
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut scanned = 0usize;
    let mut ux = UnitExplain::default();
    for unit in &units {
        if unit.num_docs() == 0 {
            continue;
        }
        let vis = unit.visibility(req.ts);
        for (probe, read) in &orders {
            let region = match unit {
                Searchable::Seg(h) => h.segment.adjacency(probe)?,
                Searchable::Mem(_) => None,
            };
            if let Some(adj) = region {
                // The filter once per unit, over the visible set, only when
                // there is one: its columns are the only scan left here.
                let admitted = match req.filter {
                    Some(f) => Some(exec::eval_structured(unit, f, &vis, &vis, &mut ux)?),
                    None => None,
                };
                let tos = unit.strings(read)?;
                for key in req.frontier {
                    for &ord in adj.probe(key) {
                        let ok = vis.get(ord as usize)
                            && admitted.as_ref().map_or(true, |a| a.get(ord as usize));
                        if !ok {
                            continue;
                        }
                        if let Some(b) = tos.at(ord)? {
                            pairs.push((key.clone(), b));
                        }
                    }
                }
                continue;
            }
            scanned += 1;
            let mut bm = unit.filter(probe, CmpOp::In, &lit, &vis)?.0;
            bm.and_inplace(&vis);
            if let Some(f) = req.filter {
                let fb = exec::eval_structured(unit, f, &vis, &bm, &mut ux)?;
                bm.and_inplace(&fb);
            }
            if bm.is_empty() {
                continue;
            }
            let (froms, tos) = (unit.strings(probe)?, unit.strings(read)?);
            for ord in bm.iter() {
                if let (Some(a), Some(b)) = (froms.at(ord)?, tos.at(ord)?) {
                    pairs.push((a, b));
                }
            }
        }
    }
    pairs.sort();
    pairs.dedup();
    if let Some(n) = req.limit {
        pairs = cut_fanout(pairs, n).0;
    }
    Ok(HopExpansion { pairs, scanned })
}

/// The shard's half of the liveness check: which of `keys` are primary
/// keys of a document visible at `ts`, sorted and distinct.
///
/// An unpartitioned collection's sort key is its primary key, so the keys,
/// sorted, are merged against each segment's sorted keys in one pass
/// ([`Shard::present_sorted`]) -- the shard's own `get` without the decode,
/// and without a search per key. A partitioned collection sorts by
/// `(partition, key)` and a walk carries bare keys, so it is a scan of the
/// key column against the set.
pub(crate) fn present_on(
    shard: &Shard,
    coll: &Collection,
    keys: &[String],
    ts: Timestamp,
) -> Result<Vec<String>> {
    if coll.partition_key.is_none() {
        // The coordinator sends them sorted; a caller that did not pays a
        // sort here rather than a wrong answer.
        let mut sorted = keys.to_vec();
        if !sorted.windows(2).all(|w| w[0] < w[1]) {
            sorted.sort();
            sorted.dedup();
        }
        let flags = shard.present_sorted(&sorted, ts);
        return Ok(sorted.into_iter().zip(flags).filter(|(_, p)| *p).map(|(k, _)| k).collect());
    }
    let lit = Value::Array(keys.iter().map(|k| Value::Str(k.clone())).collect());
    let snap = shard.snapshot_at(ts);
    let units = shard.sources(&snap);
    let mut out = Vec::new();
    for unit in &units {
        if unit.num_docs() == 0 {
            continue;
        }
        let vis = unit.visibility(ts);
        let mut bm = unit.filter(&coll.primary_key, CmpOp::In, &lit, &vis)?.0;
        bm.and_inplace(&vis);
        if bm.is_empty() {
            continue;
        }
        let keys = unit.strings(&coll.primary_key)?;
        for ord in bm.iter() {
            if let Some(k) = keys.at(ord)? {
                out.push(k);
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Keep the first `n` pairs per `from` of a sorted list; the count of `from`
/// keys that had more comes back with them.
fn cut_fanout(pairs: Vec<(String, String)>, n: usize) -> (Vec<(String, String)>, usize) {
    let mut out = Vec::with_capacity(pairs.len());
    let mut bound = 0;
    let mut run = 0usize;
    let mut last: Option<&str> = None;
    for p in &pairs {
        if last != Some(p.0.as_str()) {
            last = Some(p.0.as_str());
            run = 0;
        }
        run += 1;
        if run <= n {
            out.push(p.clone());
        } else if run == n + 1 {
            bound += 1;
        }
    }
    (out, bound)
}

/// One walk of a statement, as the coordinator runs it.
pub struct WalkSpec<'a> {
    /// The clause as written, for the plan and the cut report.
    pub label: String,
    pub k: usize,
    pub start: &'a str,
    pub reverse: bool,
    /// None, one for every hop, or one per hop; see [`filter_for`].
    pub filters: &'a [Expr],
    /// The edge collection and its adjacency index.
    pub edges: &'a Collection,
    pub index: &'a IndexDef,
    /// The node collection the statement is over.
    pub nodes: &'a Collection,
    pub walk: usize,
    pub statement: &'a str,
    pub params: &'a [Value],
    pub max_frontier: Option<usize>,
    pub max_fanout: Option<usize>,
}

/// What a walk produced: the keys, sorted and distinct; the plan; the cut
/// lines for the response; and the edge shards that did not answer, under
/// `partial_results`, named for `missing`.
pub struct WalkOutcome {
    pub keys: Vec<String>,
    /// The live keys each hop reached first, sorted, hop 1 first: the
    /// answer split by distance, for a walk that ranks rather than filters.
    pub by_hop: Vec<Vec<String>>,
    pub explain: WalkExplain,
    pub cuts: Vec<String>,
    pub missing: Vec<String>,
}

/// The walk: `k` rounds of expand-then-check over the edge and node shards,
/// each round under the statement's deadline.
///
/// A shard that does not answer is the statement's error, or under
/// `partial` is skipped, named, and never asked again: an edge shard's
/// absence loses the edges it held, and a node shard's absence loses the
/// keys only it could confirm -- they are neither answered nor walked
/// through. Not "kept unverified": a key that shard holds could not reach
/// the answer anyway, since the scatter skips the shard too, and a deleted
/// node it would have reported dead must not be walked through to nodes
/// the true answer does not hold. A fault shortens, and `missing` says so;
/// it never lengthens.
#[allow(clippy::too_many_arguments)]
pub fn walk(
    spec: &WalkSpec<'_>,
    edge_services: &[Box<dyn ShardService + '_>],
    edge_unreachable: &mut Vec<usize>,
    node_services: &[Box<dyn ShardService + '_>],
    node_unreachable: &mut Vec<usize>,
    ts: Timestamp,
    partial: bool,
) -> Result<WalkOutcome> {
    let t0 = Instant::now();
    let mut missing = Vec::new();
    let mut cuts = Vec::new();
    let mut hops = Vec::new();
    let mut by_hop = Vec::new();
    // Sorted and distinct, every one of them, so that each step below is a
    // merge and the frontier reaches the shards in key order.
    let mut seen: Vec<String> = vec![spec.start.to_string()];
    let mut answer: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = vec![spec.start.to_string()];
    // A partitioned node collection is keyed `(partition, key)`, and a walk
    // carries bare keys, so its tablet map cannot prune a check; every shard
    // is asked. Unpartitioned, the key is the composite and the map can.
    let prune = spec.nodes.partition_key.is_none();
    for hop in 1..=spec.k {
        if frontier.is_empty() {
            break;
        }
        deadline::check()?;
        let t_expand = Instant::now();
        let mut pairs: Vec<(String, String)> = Vec::new();
        let req = ExpandRequest {
            coll: spec.edges,
            frontier: &frontier,
            ts,
            limit: spec.max_fanout.map(|n| n + 1),
            reverse: spec.reverse,
            filter: filter_for(spec.filters, hop),
            statement: spec.statement,
            params: spec.params,
            walk: spec.walk,
            hop,
        };
        let mut scanned = 0usize;
        for s in edge_services {
            if edge_unreachable.contains(&s.index()) {
                continue;
            }
            match s.expand(&req) {
                Ok(x) => {
                    pairs.extend(x.pairs);
                    scanned += x.scanned;
                }
                Err(Error::Deadline(e)) => {
                    if !partial {
                        return Err(Error::Deadline(e));
                    }
                    edge_unreachable.push(s.index());
                    missing.push(format!("{} shard {} (hop {hop})", spec.edges.name, s.index()));
                }
                Err(e) => return Err(e),
            }
        }
        pairs.sort();
        pairs.dedup();
        let expand_micros = t_expand.elapsed().as_micros();
        let expanded = frontier.len();
        let mut cut = Vec::new();
        if let Some(n) = spec.max_fanout {
            let (kept, bound) = cut_fanout(pairs, n);
            pairs = kept;
            if bound > 0 {
                cut.push(format!("max_fanout = {n} bound {bound} node(s)"));
                cuts.push(format!(
                    "{} was cut at hop {hop}: max_fanout = {n} bound {bound} node(s), whose \
                     remaining edges were not followed",
                    spec.label
                ));
            }
        }
        let edges = pairs.len();
        // The pairs are sorted by `(from, to)`, so the `to`s are sorted runs,
        // one per `from`, which the sort below finds and merges.
        let mut new: Vec<String> = pairs.into_iter().map(|(_, to)| to).collect();
        new.sort();
        new.dedup();
        let mut new = difference(new, &seen);
        let found = new.len();
        if let Some(n) = spec.max_frontier {
            if new.len() > n {
                new.truncate(n);
                cut.push(format!("max_frontier = {n} kept {n} of {found}"));
                cuts.push(format!(
                    "{} was cut at hop {hop}: max_frontier = {n} kept {n} of {found} keys",
                    spec.label
                ));
            }
        }
        // Which of the new keys are live nodes at `ts`. A key nothing
        // reachable could confirm is absent.
        let t_check = Instant::now();
        let mut present: Vec<String> = Vec::new();
        for s in node_services {
            let mine: Vec<String> = if prune {
                new.iter().filter(|k| s.may_hold(k)).cloned().collect()
            } else {
                new.clone()
            };
            if mine.is_empty() {
                continue;
            }
            if node_unreachable.contains(&s.index()) {
                continue;
            }
            match s.present(&mine, ts) {
                Ok(p) => present = union(present, p),
                Err(Error::Deadline(e)) => {
                    if !partial {
                        return Err(Error::Deadline(e));
                    }
                    node_unreachable.push(s.index());
                    missing.push(format!("{} shard {} (hop {hop})", spec.nodes.name, s.index()));
                }
                Err(e) => return Err(e),
            }
        }
        let live = intersection(&new, &present);
        let dangling = new.len() - live.len();
        seen = union(seen, new);
        answer = union(answer, live.clone());
        by_hop.push(live.clone());
        frontier = live;
        hops.push(HopExplain {
            hop,
            expanded,
            edges,
            found,
            dangling,
            frontier: frontier.len(),
            cut,
            expand_micros,
            check_micros: t_check.elapsed().as_micros(),
            scanned,
            filter: match spec.filters.len() {
                0 | 1 => None,
                n => Some((hop, n)),
            },
        });
    }
    let keys = answer;
    let explain = WalkExplain {
        label: spec.label.clone(),
        index: spec.index.name.clone(),
        direction: if spec.edges.undirected {
            "both directions"
        } else if spec.reverse {
            "reverse"
        } else {
            "outgoing"
        }
        .to_string(),
        hops,
        keys: keys.len(),
        micros: t0.elapsed().as_micros(),
    };
    Ok(WalkOutcome { keys, by_hop, explain, cuts, missing })
}

/// Every walk of a statement, as the coordinator runs them and as a shard
/// on another node numbers them: the predicate's first, in the order
/// `bind_hops` replaces them, then the `hops(...)` sources of an `ORDER BY
/// hybrid(...)`, each as the `Expr::Hops` it would be as a filter.
pub fn walks_of(sel: &Select) -> Vec<Expr> {
    let mut out: Vec<Expr> = sel
        .predicate
        .as_ref()
        .map(|p| hops_in(p).into_iter().cloned().collect())
        .unwrap_or_default();
    if let Some(OrderBy::Hybrid(h)) = &sel.order {
        for s in &h.sources {
            if let HybridSource::Hops { path, k, start, via, reverse, filters } = s {
                out.push(Expr::Hops {
                    path: path.clone(),
                    k: *k,
                    start: start.clone(),
                    via: via.clone(),
                    reverse: *reverse,
                    filters: filters.clone(),
                });
            }
        }
    }
    out
}

/// Every walk in a predicate, in the order `bind_hops` replaces them.
pub fn hops_in(e: &Expr) -> Vec<&Expr> {
    fn go<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::Hops { .. } => out.push(e),
            Expr::And(v) | Expr::Or(v) => v.iter().for_each(|x| go(x, out)),
            Expr::Not(x) => go(x, out),
            _ => {}
        }
    }
    let mut out = Vec::new();
    go(e, &mut out);
    out
}

/// The statement with each walk replaced by `key IN (frontier)`, in the
/// order `hops_in` lists them. A shard on another node re-parses the
/// statement and applies the same replacement to the same frontiers, so
/// every shard evaluates the same predicate.
pub fn bind_hops(sel: &Select, frontiers: &[Vec<String>]) -> Select {
    fn go(e: &mut Expr, frontiers: &[Vec<String>], next: &mut usize) {
        match e {
            Expr::Hops { path, .. } => {
                let keys = frontiers.get(*next).map(|v| v.as_slice()).unwrap_or(&[]);
                *next += 1;
                *e = Expr::Compare {
                    path: path.clone(),
                    op: CmpOp::In,
                    lit: Value::Array(keys.iter().map(|k| Value::Str(k.clone())).collect()),
                };
            }
            Expr::And(v) | Expr::Or(v) => v.iter_mut().for_each(|x| go(x, frontiers, next)),
            Expr::Not(x) => go(x, frontiers, next),
            _ => {}
        }
    }
    let mut out = sel.clone();
    if let Some(p) = &mut out.predicate {
        go(p, frontiers, &mut 0);
    }
    out
}

/// An edge filter is structured: comparisons on the edge collection's
/// fields. A text match or a distance would need that collection's
/// statistics and indexes at every hop, and a nested walk has no start.
pub fn check_edge_filter(edges: &Collection, e: &Expr) -> Result<()> {
    match e {
        Expr::Compare { .. } | Expr::True => Ok(()),
        Expr::And(v) | Expr::Or(v) => v.iter().try_for_each(|x| check_edge_filter(edges, x)),
        Expr::Not(x) => check_edge_filter(edges, x),
        Expr::TextMatch { .. } | Expr::VectorDistance { .. } | Expr::Hops { .. } => {
            Err(Error::Plan(format!(
                "the edge filter on `{}` is structured: comparisons, IN, LIKE, IS NULL and \
                 their AND/OR/NOT; text_match, a distance and a nested walk are not",
                edges.name
            )))
        }
    }
}

/// `a ∪ b` for sorted, distinct inputs, one pass; a key in both appears
/// once.
fn union(a: Vec<String>, b: Vec<String>) -> Vec<String> {
    if b.is_empty() {
        return a;
    }
    if a.is_empty() {
        return b;
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
    while let (Some(x), Some(y)) = (a.peek(), b.peek()) {
        match x.cmp(y) {
            Ordering::Less => out.push(a.next().expect("peeked")),
            Ordering::Greater => out.push(b.next().expect("peeked")),
            Ordering::Equal => {
                out.push(a.next().expect("peeked"));
                b.next();
            }
        }
    }
    out.extend(a);
    out.extend(b);
    out
}

/// `a \ b` for sorted, distinct inputs, one pass.
fn difference(a: Vec<String>, b: &[String]) -> Vec<String> {
    let mut j = 0;
    a.into_iter()
        .filter(|x| {
            while j < b.len() && b[j].as_str() < x.as_str() {
                j += 1;
            }
            !(j < b.len() && b[j] == *x)
        })
        .collect()
}

/// `a ∩ b` for sorted, distinct inputs, one pass, as owned keys.
fn intersection(a: &[String], b: &[String]) -> Vec<String> {
    let mut j = 0;
    a.iter()
        .filter(|x| {
            while j < b.len() && b[j].as_str() < x.as_str() {
                j += 1;
            }
            j < b.len() && b[j] == **x
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn keys(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_string).collect()
    }

    /// The merges replace ordered sets; a merge that lost its place after a
    /// miss, dropped a tail, or kept a duplicate would change a frontier.
    #[test]
    fn the_sorted_set_merges_agree_with_ordered_sets() {
        let cases = [
            ("", ""),
            ("a", ""),
            ("", "a"),
            ("a b c", "a b c"),
            ("a c e g", "b d f h"),
            ("a b c d", "c d e f"),
            ("b", "a c"),
            ("a c", "b"),
            ("p000001 p000010 p000100", "p000010 p000011 p000100 p000101"),
        ];
        for (x, y) in cases {
            let (a, b) = (keys(x), keys(y));
            let sa: BTreeSet<String> = a.iter().cloned().collect();
            let sb: BTreeSet<String> = b.iter().cloned().collect();
            let want = |it: BTreeSet<&String>| it.into_iter().cloned().collect::<Vec<_>>();
            assert_eq!(union(a.clone(), b.clone()), want(sa.union(&sb).collect()), "{x:?} ∪ {y:?}");
            assert_eq!(
                difference(a.clone(), &b),
                want(sa.difference(&sb).collect()),
                "{x:?} \\ {y:?}"
            );
            assert_eq!(intersection(&a, &b), want(sa.intersection(&sb).collect()), "{x:?} ∩ {y:?}");
        }
    }
}
