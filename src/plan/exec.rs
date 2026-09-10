//! Distributed query execution — the coordinator and the per-shard, per-segment
//! work beneath it.
//!
//! The plan for a hybrid query (§8.1):
//!
//! ```text
//! coordinator          shard (per tablet)             segment
//! -----------          ------------------             -------
//! parse, plan          for each segment:              filter bitmap ∧ visible(T)
//! pin T + manifest →   ────────────────────────────→  s = measured selectivity
//! prune tablets        text: block-max WAND            choose vector strategy
//! term stats           vector: ANN | brute | ACORN     two per-source heaps (k')
//!                      merge per-source across
//!              ←────── segments; return (pk, source, raw score)
//! merge per source
//! global ranks, fuse
//! top-k
//! fetch payloads   →   winning shards only
//! ```
//!
//! **One scatter-gather plus one targeted fetch.** Network cost scales with
//! `k' × shards × sources` in small tuples, not with document size — which is
//! why step 3 returns identifiers and step 4 fetches payloads, rather than
//! shards returning documents.
//!
//! This build runs the shards in one process, so the "network" is a function
//! call. The boundary is still real: a shard never sees another shard's
//! candidates, never computes a rank, and returns nothing but
//! `(primary_key, source, raw_score)`.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::bitmap::Bitmap;
use crate::catalog::{Collection, Metric};
use crate::column::CmpOp;
use crate::error::{Error, Result};
use crate::plan::explain::{Explain, ShardExplain, TextExplain, UnitExplain};
use crate::plan::fusion::{fuse, Candidate, Direction, Fused, SourceList};
use crate::shard::{partition_prefix, Searchable, Shard};
use crate::sql::ast::*;
use crate::text::analyzer::Analyzer;
use crate::text::query::TextQuery;
use crate::text::scorer::{self, Bm25Params, GlobalStats};
use crate::time::Timestamp;
use crate::value::{compare_total, Value};
use crate::vector::{distance, SearchOpts, Strategy};

/// Per-source, per-shard candidate depth. Fusion quality depends on candidate
/// depth (§7.1), so the floor is generous and the knob is exposed.
pub fn default_k_prime(k: usize) -> usize {
    k.saturating_mul(4).max(100)
}

/// Hard ceiling on candidate depth. `k'` is user-settable and feeds a `Vec`
/// allocation, so an unbounded value is an out-of-memory abort with a SQL
/// statement as its trigger.
pub const MAX_K_PRIME: usize = 1 << 20;

/// How much to over-fetch before collapsing, so that a parent whose best child
/// sits just outside `k` is not lost (§5.4).
const COLLAPSE_AMPLIFICATION: usize = 5;

/// How deep each shard has to go per source, given the planned floor `base`.
///
/// Every stage below the gather truncates to this number, so it has to cover
/// everything the gather then throws away: the `offset` rows skipped before the
/// page starts, the collapse over-fetch, and the depth a cursor has already
/// consumed. A depth derived from LIMIT alone makes `LIMIT 10 OFFSET 10` return
/// zero rows — the shards produce exactly ten candidates and `skip(10)` empties
/// them. The `MAX_K_PRIME` clamp stays on the outside: `k'` feeds a `Vec`
/// allocation, so it must not become an out-of-memory lever.
fn candidate_depth(
    base: usize,
    k: usize,
    offset: usize,
    collapse_room: usize,
    cursor_depth: usize,
) -> usize {
    let page = k.saturating_add(offset).saturating_mul(collapse_room);
    let mut depth = base.saturating_mul(collapse_room).max(page);
    if cursor_depth > 0 {
        depth = depth.max(cursor_depth.saturating_add(k.saturating_mul(collapse_room)));
    }
    depth.min(MAX_K_PRIME)
}

#[derive(Debug, Clone)]
pub struct Row {
    pub key: String,
    pub doc: Value,
    /// Fused score, for ranked queries.
    pub score: Option<f32>,
    /// Distance, for a single-source vector query — in the metric's natural
    /// units (§ `distance::present`).
    pub distance: Option<f32>,
}

#[derive(Debug, Default)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub explain: Option<Explain>,
    /// Tablets that did not answer. Non-empty only under `WITH partial_results`
    /// (§8.4).
    pub missing: Vec<String>,
    pub next_cursor: Option<String>,
}

/// A candidate-generating source, resolved against the catalog.
enum SourcePlan {
    Text { path: String, query: TextQuery, terms: Vec<String>, name: String },
    Vector { path: String, query: Vec<f32>, metric: Metric, name: String },
}

impl SourcePlan {
    fn name(&self) -> &str {
        match self {
            SourcePlan::Text { name, .. } | SourcePlan::Vector { name, .. } => name,
        }
    }
    fn direction(&self) -> Direction {
        match self {
            SourcePlan::Text { .. } => Direction::HigherIsBetter,
            SourcePlan::Vector { .. } => Direction::LowerIsBetter,
        }
    }
}

pub struct ExecInput<'a> {
    pub shards: &'a [Shard],
    pub coll: &'a Collection,
    pub select: &'a Select,
    pub ts: Timestamp,
    /// Global term statistics for this query's terms, per text path (§8.2).
    pub stats: &'a BTreeMap<String, GlobalStats>,
    pub analyze: bool,
    pub statement: String,
}

/// Terms this statement needs global statistics for, per path. The coordinator
/// looks up only these and attaches only these (§8.1).
pub fn required_terms(coll: &Collection, sel: &Select) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut add = |path: &str, q: &str| {
        let an = Analyzer::parse(coll.analyzer_for(path));
        if let Ok(tq) = TextQuery::parse(q, an) {
            let mut terms = Vec::new();
            tq.leaf_terms(&mut terms);
            out.entry(path.to_string()).or_default().extend(terms);
        }
    };
    if let Some(e) = &sel.predicate {
        walk_text_match(e, &mut |p, q| add(p, q));
    }
    if let Some(OrderBy::Hybrid(h)) = &sel.order {
        for s in &h.sources {
            if let HybridSource::Text { path, query } = s {
                add(path, query);
            }
        }
    }
    for v in out.values_mut() {
        v.sort();
        v.dedup();
    }
    out
}

fn walk_text_match(e: &Expr, f: &mut impl FnMut(&str, &str)) {
    match e {
        Expr::TextMatch { path, query } => f(path, query),
        Expr::And(v) | Expr::Or(v) => {
            for x in v {
                walk_text_match(x, f);
            }
        }
        Expr::Not(x) => walk_text_match(x, f),
        _ => {}
    }
}

/// The partition-key equality this query is constrained on, if any. Partition
/// pruning is the primary defence against fan-out (§8.4), so it is looked for
/// before anything else runs.
fn partition_constraint(coll: &Collection, e: Option<&Expr>) -> Option<String> {
    let pk = coll.partition_key.as_ref()?;
    fn find(e: &Expr, pk: &str) -> Option<Value> {
        match e {
            Expr::Compare { path, op: CmpOp::Eq, lit } if path == pk => Some(lit.clone()),
            // Only a conjunction prunes: a disjunct can reach outside the range.
            Expr::And(v) => v.iter().find_map(|x| find(x, pk)),
            _ => None,
        }
    }
    // A value that cannot be rendered as a key cannot match any document, but
    // declining to prune is always safe, so a rendering failure just means the
    // query fans out.
    find(e?, pk).and_then(|v| partition_prefix(&v).ok())
}

pub fn run_select(input: ExecInput<'_>) -> Result<QueryResult> {
    let t0 = Instant::now();
    let sel = input.select;
    let k = sel.limit.unwrap_or(10);
    let deadline = sel.with.deadline_ms.map(std::time::Duration::from_millis);

    let mut ex = Explain {
        statement: input.statement.clone(),
        snapshot_ts: input.ts,
        limit: k,
        offset: sel.offset,
        exact_mode: sel.with.exact,
        stats_exact: input.stats.values().next().map(|s| s.exact).unwrap_or(sel.with.exact_scoring),
        ..Default::default()
    };

    if sel.cursor.is_some() {
        if sel.collapse.is_some() {
            return Err(Error::Plan(
                "AFTER cannot be combined with COLLAPSE BY: resuming a collapsed result needs \
                 the set of parents already emitted, which the cursor does not carry. Use \
                 OFFSET, or collapse in the client."
                    .into(),
            ));
        }
        if matches!(sel.order, Some(OrderBy::Fields(_))) {
            return Err(Error::Plan(
                "AFTER is not supported with ORDER BY <field>: the cursor is anchored to the \
                 sort tuple, and only ranked and primary-key orderings carry one. Use OFFSET."
                    .into(),
            ));
        }
    }

    let prefix = partition_constraint(input.coll, sel.predicate.as_ref());
    if prefix.is_none() && input.coll.partition_key.is_some() && input.shards.len() > 1 {
        ex.notes.push("no partition-key equality: the query fans out to every shard".to_string());
    }

    // Resolve the ORDER BY into candidate-generating sources.
    let SourcePlanning { sources, method, weights, rrf_c, k_prime } =
        plan_sources(input.coll, sel, k)?;
    // `COLLAPSE BY` throws away every child but the best per parent, so the
    // candidate set has to be deep enough that `k` parents survive (§5.4).
    let collapse_room = if sel.collapse.is_some() { COLLAPSE_AMPLIFICATION } else { 1 };
    // `search_after` over an approximate index has no cheap resume: a graph
    // heap cannot be handed "the next k after this distance" without traversing
    // to that depth first. So the cursor carries how deep the reader already
    // is, and candidate generation is widened to match — the same `depth + k`
    // per shard that OFFSET costs. The cursor's advantage over OFFSET is not
    // cost, it is stability: it survives concurrent writes, and it is anchored
    // to the sort tuple rather than to a position.
    let cursor_depth = sel.cursor.as_ref().map(|c| decode_cursor(c).1).unwrap_or(0);
    let k_prime = candidate_depth(k_prime, k, sel.offset, collapse_room, cursor_depth);
    if cursor_depth > 0 {
        ex.notes.push(format!(
            "cursor at depth {cursor_depth}: candidate generation widened to k'={k_prime} per shard"
        ));
    }
    ex.k_prime = k_prime;

    // --- Non-ranked queries: no candidate generation, just a filtered scan.
    if sources.is_empty() {
        let out = scan(&input, &prefix, k, &mut ex, deadline)?;
        ex.total_micros = t0.elapsed().as_micros();
        return Ok(QueryResult {
            rows: out.0,
            explain: if input.analyze { Some(ex) } else { None },
            missing: out.1,
            next_cursor: None,
        });
    }

    // --- Scatter: one round trip, every source evaluated in the same pass.
    let mut per_source: Vec<Vec<Candidate>> = vec![Vec::new(); sources.len()];
    let mut missing: Vec<String> = Vec::new();

    for (si, shard) in input.shards.iter().enumerate() {
        let mut sx = ShardExplain {
            index: si,
            manifest_version: shard.manifest_version,
            ..Default::default()
        };
        let ts = Instant::now();
        if let Some(p) = &prefix {
            if !shard_may_hold(shard, p) {
                sx.pruned = true;
                sx.prune_reason = Some(format!("key prefix `{}` outside range", show_key(p)));
                ex.shards.push(sx);
                continue;
            }
        }
        if let Some(d) = deadline {
            if t0.elapsed() > d {
                sx.timed_out = true;
                ex.shards.push(sx);
                missing.push(format!("shard {si}"));
                if sel.with.partial_results {
                    continue;
                }
                return Err(Error::Deadline(format!(
                    "shard {si} not reached within {} ms; use WITH partial_results to opt in to \
                     incomplete answers",
                    d.as_millis()
                )));
            }
        }

        let snap = shard.snapshot_at(input.ts);
        let units = shard.sources(&snap);
        // Per-source heaps, merged across every unit of this shard (step 3).
        let mut shard_heaps: Vec<Vec<Candidate>> = vec![Vec::new(); sources.len()];

        for unit in &units {
            let ut = Instant::now();
            let n = unit.num_docs();
            if n == 0 {
                continue;
            }
            let mut ux = UnitExplain { label: unit.label(), docs: n, ..Default::default() };
            let io0 = unit.io_counters();

            let vis = unit.visibility(input.ts);
            ux.visible = vis.popcount();
            let mut filter = match &prefix {
                Some(p) => {
                    ux.access_paths.push(format!("partition range `{}`", show_key(p)));
                    unit.key_prefix(p)
                }
                None => Bitmap::all(n),
            };
            filter.and_inplace(&vis);
            if let Some(e) = &sel.predicate {
                let bm = eval_expr(unit, e, &vis, &filter, input.stats, &mut ux)?;
                filter.and_inplace(&bm);
            }
            ux.survivors = filter.popcount();
            ux.selectivity = ux.survivors as f64 / n as f64;

            if ux.survivors > 0 {
                for (i, sp) in sources.iter().enumerate() {
                    let got = run_source(unit, sp, &filter, k_prime, sel, input.stats, &mut ux)?;
                    for (ord, raw) in got {
                        if let Some(key) = unit.key(ord) {
                            shard_heaps[i].push(Candidate { key: key.to_string(), raw_score: raw });
                        }
                    }
                }
            }
            if let (Some((l0, f0)), Some((l1, f1))) = (io0, unit.io_counters()) {
                ux.loads = l1.saturating_sub(l0);
                ux.faults = f1.saturating_sub(f0);
            }
            ux.micros = ut.elapsed().as_micros();
            sx.units.push(ux);
        }

        // Merge this shard's per-source heaps and truncate to k'. Identifiers
        // and raw scores only — no ranks, no documents.
        for (i, sp) in sources.iter().enumerate() {
            let dir = sp.direction();
            shard_heaps[i]
                .sort_by(|a, b| cmp_dir(dir, a.raw_score, b.raw_score).then(a.key.cmp(&b.key)));
            shard_heaps[i].dedup_by(|a, b| a.key == b.key);
            shard_heaps[i].truncate(k_prime);
            per_source[i].extend(std::mem::take(&mut shard_heaps[i]));
        }
        sx.micros = ts.elapsed().as_micros();
        ex.shards.push(sx);
    }

    // --- Gather: merge per source, rank globally, fuse (§7.2).
    let want = k.saturating_add(sel.offset);
    let deep = want.saturating_mul(collapse_room).min(MAX_K_PRIME);
    let ranked: Vec<(String, f32, Option<f32>)> = if sources.len() == 1 {
        // A single source needs no fusion, and forcing it through RRF would
        // throw away the distance the user asked to order by.
        let dir = sources[0].direction();
        let mut all = std::mem::take(&mut per_source[0]);
        all.sort_by(|a, b| cmp_dir(dir, a.raw_score, b.raw_score).then(a.key.cmp(&b.key)));
        all.dedup_by(|a, b| a.key == b.key);
        all.into_iter()
            .map(|c| match &sources[0] {
                // The sort value is always "higher is better", so one cursor
                // rule serves every ordering. A distance is reported separately
                // and in the metric's own units; negating it for the sort does
                // not leak into what the user sees.
                SourcePlan::Vector { metric, .. } => {
                    (c.key, -c.raw_score, Some(distance::present(*metric, c.raw_score)))
                }
                _ => (c.key, c.raw_score, None),
            })
            .collect()
    } else {
        let lists: Vec<SourceList> = sources
            .iter()
            .enumerate()
            .map(|(i, sp)| SourceList {
                name: sp.name().to_string(),
                direction: sp.direction(),
                weight: weights.get(i).copied().unwrap_or(1.0),
                candidates: std::mem::take(&mut per_source[i]),
            })
            .collect();
        // Fuse past the cursor before the cursor filters. Truncating to `deep`
        // first would hand `search_after` exactly the page it is meant to skip,
        // and every page after the first would come back empty.
        let depth = cursor_depth.saturating_add(deep).min(MAX_K_PRIME);
        let (fused, fx) = fuse(lists, method, rrf_c, depth);
        ex.fusion = Some(fx);
        fused.into_iter().map(|f: Fused| (f.key, f.score, None)).collect()
    };

    // Cursor: `search_after` over the (score, key) sort tuple.
    let mut ranked = ranked;
    if let Some(cur) = &sel.cursor {
        let after = decode_cursor(cur);
        ranked.retain(|(key, score, _)| after_cursor(&after, *score, key));
    }

    // COLLAPSE BY: keep the best-scoring child per parent (§5.4).
    let mut fetch_t = Instant::now();
    let mut rows: Vec<Row> = Vec::new();
    if let Some(parent_path) = &sel.collapse {
        ex.collapse = Some((parent_path.clone(), COLLAPSE_AMPLIFICATION));
        let mut seen: Vec<Value> = Vec::new();
        for (key, score, dist) in ranked.iter() {
            if rows.len() >= want {
                break;
            }
            let Some(doc) = fetch(input.shards, key, input.ts)? else { continue };
            let parent = doc.path(parent_path).cloned().unwrap_or(Value::Null);
            if !parent.is_null() && seen.contains(&parent) {
                continue;
            }
            seen.push(parent);
            rows.push(Row {
                key: key.clone(),
                doc,
                score: if dist.is_none() { Some(*score) } else { None },
                distance: *dist,
            });
        }
    } else {
        // Fetch payloads from the winning shards only.
        fetch_t = Instant::now();
        for (key, score, dist) in ranked.iter().take(want) {
            let Some(doc) = fetch(input.shards, key, input.ts)? else { continue };
            rows.push(Row {
                key: key.clone(),
                doc,
                score: if dist.is_none() { Some(*score) } else { None },
                distance: *dist,
            });
        }
    }
    ex.fetch_micros = fetch_t.elapsed().as_micros();
    ex.fetched_payloads = rows.len();

    if sel.offset > 0 {
        rows = rows.into_iter().skip(sel.offset).collect();
    }
    rows.truncate(k);
    // The cursor carries the last row's *sort* value, which is what
    // `search_after` compares against — not the distance the user sees.
    let last_sort =
        rows.last().and_then(|r| ranked.iter().find(|(k, _, _)| *k == r.key).map(|(_, s, _)| *s));
    let next_cursor = rows
        .last()
        .map(|r| encode_cursor(last_sort.unwrap_or(0.0), cursor_depth + rows.len(), &r.key));
    ex.missing = missing.clone();
    ex.total_micros = t0.elapsed().as_micros();
    Ok(QueryResult {
        rows,
        explain: if input.analyze { Some(ex) } else { None },
        missing,
        next_cursor,
    })
}

fn cmp_dir(d: Direction, a: f32, b: f32) -> std::cmp::Ordering {
    match d {
        Direction::HigherIsBetter => b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal),
        Direction::LowerIsBetter => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn show_key(k: &str) -> String {
    k.replace('\u{1}', "/")
}

/// What `plan_sources` decides: the per-source access paths, how their
/// candidate lists will be fused, and how deep each source has to go.
struct SourcePlanning {
    sources: Vec<SourcePlan>,
    method: FusionMethod,
    weights: Vec<f32>,
    rrf_c: f32,
    k_prime: usize,
}

fn plan_sources(coll: &Collection, sel: &Select, k: usize) -> Result<SourcePlanning> {
    let mut sources = Vec::new();
    let mut method = FusionMethod::Rrf;
    let mut weights = Vec::new();
    let mut rrf_c = 60.0f32;
    let mut k_prime = default_k_prime(k);

    match &sel.order {
        None => {}
        Some(OrderBy::Fields(_)) => {}
        Some(OrderBy::Distance { path, op, query }) => {
            check_vector(coll, path, *op, query)?;
            let metric = coll.vector_metric(path).unwrap_or(op.metric());
            sources.push(SourcePlan::Vector {
                path: path.clone(),
                query: prepare_query(metric, query),
                metric,
                name: format!("vector({path})"),
            });
            weights.push(1.0);
            // A pure ANN query does not need fusion depth; `k` plus headroom
            // for visibility amplification is enough.
            k_prime = k.max(10);
        }
        Some(OrderBy::Hybrid(h)) => {
            method = h.method;
            weights = h.weights.clone();
            rrf_c = h.rrf_c;
            if let Some(kp) = h.k_prime {
                k_prime = kp;
            }
            for s in &h.sources {
                match s {
                    HybridSource::Text { path, query } => {
                        if coll.fulltext_index(path).is_none() {
                            return Err(Error::Plan(format!(
                                "no full-text index on `{path}`; \
                                 CREATE INDEX ... USING fulltext ({path})"
                            )));
                        }
                        let tq = TextQuery::parse(query, Analyzer::parse(coll.analyzer_for(path)))?;
                        let mut terms = Vec::new();
                        tq.leaf_terms(&mut terms);
                        sources.push(SourcePlan::Text {
                            path: path.clone(),
                            query: tq,
                            terms,
                            name: format!("text({path})"),
                        });
                    }
                    HybridSource::Vector { path, op, query } => {
                        check_vector(coll, path, *op, query)?;
                        let metric = coll.vector_metric(path).unwrap_or(op.metric());
                        sources.push(SourcePlan::Vector {
                            path: path.clone(),
                            query: prepare_query(metric, query),
                            metric,
                            name: format!("vector({path})"),
                        });
                    }
                }
            }
        }
    }
    Ok(SourcePlanning { sources, method, weights, rrf_c, k_prime })
}

/// Prepare the query vector the same way stored vectors were prepared. For
/// cosine that means normalising it: stored vectors are unit-length, so a
/// query that is not produces a "distance" outside `[0, 2]` — ordering still
/// works, but every distance the user sees is wrong, and a threshold on one is
/// meaningless.
fn prepare_query(metric: Metric, q: &[f32]) -> Vec<f32> {
    let mut v = q.to_vec();
    distance::prepare(metric, &mut v);
    v
}

fn check_vector(coll: &Collection, path: &str, op: DistOp, query: &[f32]) -> Result<()> {
    let Some(dims) = coll.vector_dims(path) else {
        return Err(Error::Plan(format!(
            "no vector index on `{path}`; CREATE INDEX ... USING vector ({path})"
        )));
    };
    if query.len() != dims {
        return Err(Error::Plan(format!(
            "query vector has {} dimensions but `{path}` declares {dims}",
            query.len()
        )));
    }
    let declared = coll.vector_metric(path).unwrap();
    if declared != op.metric() {
        return Err(Error::Plan(format!(
            "`{path}` is indexed for {} but the query uses `{}` ({}); \
             the operator must match the index metric",
            declared.name(),
            op.symbol(),
            op.metric().name()
        )));
    }
    Ok(())
}

/// Evaluate a predicate tree into the unit's ordinal space.
fn eval_expr(
    unit: &Searchable<'_>,
    e: &Expr,
    vis: &Bitmap,
    candidates: &Bitmap,
    stats: &BTreeMap<String, GlobalStats>,
    ux: &mut UnitExplain,
) -> Result<Bitmap> {
    let n = unit.num_docs();
    Ok(match e {
        Expr::True => Bitmap::all(n),
        Expr::And(parts) => {
            // Cheap predicates first; an expensive one (a variant decode over a
            // path this segment did not shred) is applied only to what survives
            // (§5.3).
            let mut ordered: Vec<&Expr> = parts.iter().collect();
            ordered.sort_by_key(|p| predicate_cost(unit, p));
            let mut acc = candidates.clone();
            for p in ordered {
                if acc.is_empty() {
                    break;
                }
                let bm = eval_expr(unit, p, vis, &acc, stats, ux)?;
                acc.and_inplace(&bm);
            }
            acc
        }
        Expr::Or(parts) => {
            let mut acc = Bitmap::new(n);
            for p in parts {
                acc.or_inplace(&eval_expr(unit, p, vis, candidates, stats, ux)?);
            }
            acc
        }
        Expr::Not(inner) => {
            // `NOT p` selects rows where `p` is FALSE — not rows where it is
            // "not true". A row whose value cannot be compared with the literal
            // makes `p` NULL, and NULL is selected by neither `p` nor `NOT p`.
            // Complementing over visibility instead would select exactly those
            // rows, and would make `NOT (a = b)` disagree with `a <> b`.
            let all = Bitmap::all(n);
            let t = eval_expr(unit, inner, vis, &all, stats, ux)?;
            let mut acc = eval_defined(unit, inner, vis, &all, stats, ux)?;
            acc.andnot_inplace(&t);
            acc
        }
        Expr::Compare { path, op, lit } => {
            let (bm, used_column) = unit.filter(path, *op, lit, candidates)?;
            ux.access_paths.push(format!(
                "{} {} {}{}",
                path,
                op.name(),
                short(lit),
                if used_column { " [column]" } else { " [variant decode]" }
            ));
            bm
        }
        Expr::TextMatch { path, query } => {
            // A **must**: it filters and contributes no rank (§2.4).
            let Some(handle) = unit.text_handle(path)? else {
                return Err(Error::Plan(format!("no full-text index on `{path}`")));
            };
            let Some(src) = handle.source(path) else {
                return Err(Error::Plan(format!("no full-text index on `{path}`")));
            };
            let an = unit.analyzer(path)?.unwrap_or(Analyzer::Standard);
            let tq = TextQuery::parse(query, an)?;
            let empty = GlobalStats::default();
            let st = stats.get(path).unwrap_or(&empty);
            let c = scorer::compile(&tq, &src, st, Bm25Params::default())?;
            ux.access_paths.push(format!("text_match({path}, …) [filter]"));
            scorer::evaluate_to_bitmap(c, n)
        }
    })
}

/// Ordinals where `e` is *defined* (not NULL), for three-valued negation.
///
/// The De Morgan shape matters: `NOT (a AND b)` is defined where either
/// conjunct is defined and false, or both are defined — so the safe, and
/// standard, rule is that a compound is defined where all of its parts are.
///
/// `vis`, `stats` and `ux` are threaded through unchanged today because the
/// leaf cases that will use them — a text leaf that needs global statistics, a
/// per-leaf explain line — are the natural next thing to add here, and a
/// signature that differs from `eval_expr` invites the two walkers to drift.
#[allow(clippy::only_used_in_recursion)]
fn eval_defined(
    unit: &Searchable<'_>,
    e: &Expr,
    vis: &Bitmap,
    candidates: &Bitmap,
    stats: &BTreeMap<String, GlobalStats>,
    ux: &mut UnitExplain,
) -> Result<Bitmap> {
    let n = unit.num_docs();
    Ok(match e {
        Expr::True => Bitmap::all(n),
        Expr::And(parts) | Expr::Or(parts) => {
            let mut acc = Bitmap::all(n);
            for p in parts {
                acc.and_inplace(&eval_defined(unit, p, vis, candidates, stats, ux)?);
            }
            acc
        }
        Expr::Not(inner) => eval_defined(unit, inner, vis, candidates, stats, ux)?,
        Expr::Compare { path, op, lit } => unit.comparable(path, *op, lit, candidates)?,
        // A text match is two-valued: a document either matches or it does not.
        Expr::TextMatch { .. } => Bitmap::all(n),
    })
}

/// Rough cost, used only to order conjuncts. A shredded column is cheap; a
/// variant decode is not.
fn predicate_cost(unit: &Searchable<'_>, e: &Expr) -> u32 {
    match e {
        Expr::Compare { path, .. } => match unit {
            Searchable::Seg(h) if h.segment.is_shredded(path) => 0,
            Searchable::Mem(_) => 1,
            _ => 10,
        },
        Expr::TextMatch { .. } => 5,
        Expr::And(v) | Expr::Or(v) => v.iter().map(|x| predicate_cost(unit, x)).max().unwrap_or(1),
        Expr::Not(x) => predicate_cost(unit, x) + 1,
        Expr::True => 0,
    }
}

fn short(v: &Value) -> String {
    let s = crate::json::to_string(v);
    if s.len() > 24 {
        // By chars, not bytes: this runs on every query, and slicing a literal
        // like '日本語日本語日本語' at byte 24 lands inside a character.
        format!("{}…", s.chars().take(24).collect::<String>())
    } else {
        s
    }
}

fn run_source(
    unit: &Searchable<'_>,
    sp: &SourcePlan,
    filter: &Bitmap,
    k_prime: usize,
    sel: &Select,
    stats: &BTreeMap<String, GlobalStats>,
    ux: &mut UnitExplain,
) -> Result<Vec<(u32, f32)>> {
    match sp {
        SourcePlan::Text { path, query, terms, name, .. } => {
            let Some(handle) = unit.text_handle(path)? else { return Ok(Vec::new()) };
            let Some(src) = handle.source(path) else { return Ok(Vec::new()) };
            let empty = GlobalStats::default();
            let st = stats.get(path).unwrap_or(&empty);
            let c = scorer::compile(query, &src, st, Bm25Params::default())?;
            let hits = match c.scorer {
                Some(s) => scorer::collect_top_k(s, filter, c.excluded.as_ref(), k_prime),
                None => Vec::new(),
            };
            ux.text.push(TextExplain {
                source: name.clone(),
                terms: terms.clone(),
                candidates: hits.len(),
                prefix_truncated: c.prefix_truncated,
                stats_exact: st.exact,
            });
            Ok(hits.into_iter().map(|h| (h.ord, h.score)).collect())
        }
        SourcePlan::Vector { path, query, name, .. } => {
            let Some(vs) = unit.vector_handle(path)? else { return Ok(Vec::new()) };
            let mut opts = SearchOpts { exact: sel.with.exact, ..Default::default() };
            if let Some(ef) = sel.with.ef_search {
                opts.ef_search = ef;
            }
            let (hits, mut report) = vs.search(query, k_prime, filter, &opts);
            if sel.with.exact {
                report.strategy = Some(Strategy::Exact);
            }
            ux.vector.push((name.clone(), report));
            Ok(hits)
        }
    }
}

/// A non-ranked query: filter, then order by key or by explicit fields.
fn scan(
    input: &ExecInput<'_>,
    prefix: &Option<String>,
    k: usize,
    ex: &mut Explain,
    deadline: Option<std::time::Duration>,
) -> Result<(Vec<Row>, Vec<String>)> {
    let t0 = Instant::now();
    let sel = input.select;
    let mut out: Vec<(String, Value)> = Vec::new();
    let mut missing = Vec::new();
    for (si, shard) in input.shards.iter().enumerate() {
        let mut sx = ShardExplain {
            index: si,
            manifest_version: shard.manifest_version,
            ..Default::default()
        };
        if let Some(p) = prefix {
            if !shard_may_hold(shard, p) {
                sx.pruned = true;
                sx.prune_reason = Some("out of key range".into());
                ex.shards.push(sx);
                continue;
            }
        }
        if let Some(d) = deadline {
            if t0.elapsed() > d {
                sx.timed_out = true;
                ex.shards.push(sx);
                missing.push(format!("shard {si}"));
                if sel.with.partial_results {
                    continue;
                }
                return Err(Error::Deadline(format!(
                    "shard {si} not reached within {} ms; use WITH partial_results to opt in to \
                     incomplete answers",
                    d.as_millis()
                )));
            }
        }
        let snap = shard.snapshot_at(input.ts);
        for unit in &shard.sources(&snap) {
            let n = unit.num_docs();
            if n == 0 {
                continue;
            }
            let ut = Instant::now();
            let mut ux = UnitExplain { label: unit.label(), docs: n, ..Default::default() };
            let io0 = unit.io_counters();
            let vis = unit.visibility(input.ts);
            ux.visible = vis.popcount();
            let mut filter = match prefix {
                Some(p) => unit.key_prefix(p),
                None => Bitmap::all(n),
            };
            filter.and_inplace(&vis);
            if let Some(e) = &sel.predicate {
                let bm = eval_expr(unit, e, &vis, &filter, input.stats, &mut ux)?;
                filter.and_inplace(&bm);
            }
            ux.survivors = filter.popcount();
            ux.selectivity = ux.survivors as f64 / n as f64;
            for ord in filter.iter() {
                out.push((unit.key(ord).unwrap_or("").to_string(), unit.document(ord)?));
            }
            if let (Some((l0, f0)), Some((l1, f1))) = (io0, unit.io_counters()) {
                ux.loads = l1.saturating_sub(l0);
                ux.faults = f1.saturating_sub(f0);
            }
            ux.micros = ut.elapsed().as_micros();
            sx.units.push(ux);
        }
        ex.shards.push(sx);
    }

    match &sel.order {
        Some(OrderBy::Fields(fields)) => {
            out.sort_by(|a, b| {
                for (path, asc) in fields {
                    let av = a.1.path(path).cloned().unwrap_or(Value::Null);
                    let bv = b.1.path(path).cloned().unwrap_or(Value::Null);
                    let o = compare_total(&av, &bv);
                    let o = if *asc { o } else { o.reverse() };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                // Primary key is the final tie-break, always (§7.2).
                a.0.cmp(&b.0)
            });
        }
        _ => out.sort_by(|a, b| a.0.cmp(&b.0)),
    }
    if let Some(cur) = &sel.cursor {
        // A key-ordered scan resumes on the primary key. A full cursor token
        // from a ranked query still works: its last field is that key.
        let after = decode_cursor(cur).2;
        out.retain(|(key, _)| key.as_str() > after.as_str());
    }
    // `COLLAPSE BY` applies to unranked queries too. Skipping it here would
    // silently return `k` children of one parent for a statement that asked for
    // `k` distinct parents — a wrong answer with no error to point at.
    if let Some(parent_path) = &sel.collapse {
        // A scan materialises every survivor before ordering, so unlike the
        // ranked path there is nothing to over-fetch: no amplification.
        ex.collapse = Some((parent_path.clone(), 1));
        collapse_by_parent(&mut out, parent_path);
    }
    let rows: Vec<Row> = out
        .into_iter()
        .skip(sel.offset)
        .take(k)
        .map(|(key, doc)| Row { key, doc, score: None, distance: None })
        .collect();
    ex.fetched_payloads = rows.len();
    Ok((rows, missing))
}

/// Keep the first row per distinct parent, preserving the order already
/// established. A row whose parent path is absent or NULL belongs to no group,
/// so it survives — collapsing them together would fold every unrelated
/// document into one, which is the same rule the ranked path applies.
fn collapse_by_parent(rows: &mut Vec<(String, Value)>, parent_path: &str) {
    let mut seen: Vec<Value> = Vec::new();
    rows.retain(|(_, doc)| match doc.path(parent_path) {
        Some(parent) if !parent.is_null() => {
            if seen.contains(parent) {
                false
            } else {
                seen.push(parent.clone());
                true
            }
        }
        _ => true,
    });
}

/// Whether a shard's key range can hold anything under `prefix`.
fn shard_may_hold(shard: &Shard, prefix: &str) -> bool {
    if let Some(r) = &shard.key_range {
        // A prefix overlaps the range if it is not entirely below `lo` and
        // not at or above `hi`. `lo.starts_with(prefix)` covers the case where
        // the prefix is a strict prefix of the boundary itself.
        let lo_ok =
            r.0.as_ref().map(|lo| prefix >= lo.as_str() || lo.starts_with(prefix)).unwrap_or(true);
        let hi_ok = r.1.as_ref().map(|hi| prefix < hi.as_str()).unwrap_or(true);
        return lo_ok && hi_ok;
    }
    true
}

fn fetch(shards: &[Shard], key: &str, ts: Timestamp) -> Result<Option<Value>> {
    for s in shards {
        if let Some(d) = s.get(key, ts)? {
            return Ok(Some(d));
        }
    }
    Ok(None)
}

/// `<sort value>|<depth>|<primary key>`. The depth is what lets the next page
/// widen candidate generation instead of silently returning the same page.
///
/// The sort value is the f32's bit pattern, written as `#` plus eight hex
/// digits, because `after_cursor` compares it with `<`: a rendering that does
/// not round-trip exactly is off by an ULP in one direction or the other, which
/// either re-emits the anchor row on the next page or drops its whole tie
/// group. A decimal `{:.9}` is nine decimal *places*, and RRF scores sit near
/// 0.01 where the f32 grid is far finer than 1e-9. The token stays opaque to
/// callers either way.
fn encode_cursor(score: f32, depth: usize, key: &str) -> String {
    format!("#{:08x}|{depth}|{key}", score.to_bits())
}

/// The `#`-prefixed bit pattern [`encode_cursor`] writes; a plain decimal is
/// still read, so a cursor written by hand keeps working.
fn decode_score(s: &str) -> Option<f32> {
    match s.strip_prefix('#') {
        Some(bits) => u32::from_str_radix(bits, 16).ok().map(f32::from_bits),
        None => s.parse::<f32>().ok(),
    }
}

fn decode_cursor(c: &str) -> (Option<f32>, usize, String) {
    let mut it = c.splitn(3, '|');
    match (it.next(), it.next(), it.next()) {
        (Some(s), Some(d), Some(k)) => {
            (decode_score(s), d.parse::<usize>().unwrap_or(0), k.to_string())
        }
        // A bare primary key is still accepted: it is what a key-ordered scan
        // hands back, and what a human types.
        _ => (None, 0, c.to_string()),
    }
}

/// `search_after` over the `(score desc, key asc)` sort tuple — the same tuple
/// the result is ordered by, which is what makes deep pagination stable.
fn after_cursor(after: &(Option<f32>, usize, String), score: f32, key: &str) -> bool {
    match after.0 {
        None => key > after.2.as_str(),
        Some(s) => score < s || (score == s && key > after.2.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(key: &str, json: &str) -> (String, Value) {
        (key.to_string(), crate::json::parse(json).unwrap())
    }

    /// `k'` used to come from LIMIT alone, so every stage below the gather
    /// truncated to ten candidates for `LIMIT 10 OFFSET 10` and the `skip(10)`
    /// that follows emptied them: page two came back with zero rows.
    #[test]
    fn offset_is_part_of_candidate_depth_so_page_two_is_not_empty() {
        let k = 10;
        // A pure vector query plans the shallowest floor there is, `k`.
        assert!(candidate_depth(k, k, 0, 1, 0) >= k);
        assert!(candidate_depth(k, k, 10, 1, 0) >= 20, "OFFSET 10 needs 20 candidates");
        assert!(candidate_depth(k, k, 1, 1, 0) >= 11, "even OFFSET 1 costs one more row");
        // A deliberately shallow user-set `k'` still has to cover the page.
        assert!(candidate_depth(1, k, 90, 1, 0) >= 100);
        // COLLAPSE BY multiplies what the gather discards, so it multiplies depth.
        assert_eq!(candidate_depth(1, k, 10, COLLAPSE_AMPLIFICATION, 0), 100);
        // A cursor still widens by the depth already consumed.
        assert_eq!(candidate_depth(100, k, 0, 1, 250), 260);
        // And the clamp survives all of it: `k'` feeds a `Vec` allocation, so an
        // absurd OFFSET must not become an out-of-memory lever.
        assert_eq!(
            candidate_depth(MAX_K_PRIME, k, usize::MAX, COLLAPSE_AMPLIFICATION, 0),
            MAX_K_PRIME
        );
    }

    /// The cursor's score used to be written with `{:.9}` — nine decimal
    /// *places*, where an f32 needs nine significant digits. RRF scores sit near
    /// 0.01, on a grid far finer than 1e-9, so the value read back was an ULP
    /// off: too high and `after_cursor` re-emits the anchor row on the next
    /// page, too low and it drops the anchor's whole tie group.
    #[test]
    fn a_rounded_cursor_score_does_not_re_emit_or_drop_the_anchor_row() {
        for rank in 1..=3000u32 {
            let score = 1.0f32 / (60.0 + rank as f32);
            let after = decode_cursor(&encode_cursor(score, 10, "doc-0042"));
            assert_eq!(
                after.0.map(f32::to_bits),
                Some(score.to_bits()),
                "rank {rank} did not round-trip"
            );
            assert!(!after_cursor(&after, score, "doc-0042"), "rank {rank}: anchor re-emitted");
            assert!(!after_cursor(&after, score, "doc-0041"), "rank {rank}: tie ahead re-emitted");
            assert!(after_cursor(&after, score, "doc-0043"), "rank {rank}: tie group dropped");
        }
    }

    /// A negated distance is the sort value of a single-source vector query, so
    /// the cursor has to survive the sign as well.
    #[test]
    fn a_negative_sort_value_round_trips_through_the_cursor() {
        let score = -(1.0f32 / 3.0);
        let after = decode_cursor(&encode_cursor(score, 3, "tenant\u{1}doc-7"));
        assert_eq!(after.0.map(f32::to_bits), Some(score.to_bits()));
        assert_eq!(after.1, 3);
        assert_eq!(after.2, "tenant\u{1}doc-7");
        // A bare primary key is still a valid cursor, and so is a hand-written
        // decimal score.
        assert_eq!(decode_cursor("doc-7").2, "doc-7");
        assert_eq!(decode_cursor("0.5|2|doc-7").0, Some(0.5));
    }

    /// COLLAPSE BY used to be dropped on the floor for any query without an
    /// ORDER BY that generates candidates: `COLLAPSE BY parent_id LIMIT 5`
    /// happily returned five chunks of the same parent.
    #[test]
    fn collapse_by_on_an_unranked_scan_is_honoured_rather_than_ignored() {
        let mut rows = vec![
            doc("c1", r#"{"parent_id": "p1"}"#),
            doc("c2", r#"{"parent_id": "p1"}"#),
            doc("c3", r#"{"parent_id": "p2"}"#),
            doc("c4", r#"{"parent_id": "p1"}"#),
            doc("c5", r#"{"parent_id": "p3"}"#),
        ];
        collapse_by_parent(&mut rows, "parent_id");
        assert_eq!(
            rows.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["c1", "c3", "c5"]
        );
    }

    /// Documents with no parent are not one giant group; collapsing them
    /// together would delete unrelated rows from the answer.
    #[test]
    fn a_missing_or_null_parent_does_not_collapse_unrelated_rows_together() {
        let mut rows = vec![
            doc("a", r#"{"other": 1}"#),
            doc("b", r#"{"parent_id": null}"#),
            doc("c", r#"{"other": 2}"#),
            doc("d", r#"{"parent_id": "p"}"#),
            doc("e", r#"{"parent_id": "p"}"#),
        ];
        collapse_by_parent(&mut rows, "parent_id");
        assert_eq!(
            rows.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
    }
}
