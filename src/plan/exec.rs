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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use crate::bitmap::Bitmap;
use crate::catalog::{Collection, Metric};
use crate::column::CmpOp;
use crate::deadline;
use crate::error::{Error, Result};
use crate::plan::explain::{
    Explain, ShardExplain, TextExplain, TextStrategy, UnitExplain, WalkExplain,
};
use crate::plan::fusion::{fuse, Candidate, Direction, Fused, SourceList};
use crate::plan::service::{
    CandidatesRequest, ScanHit, ScanRequest, ShardCandidates, ShardScan, ShardService,
};
use crate::shard::{partition_prefix, Searchable, Shard};
use crate::sql::ast::*;
use crate::text::analyzer::Analyzer;
use crate::text::query::TextQuery;
use crate::text::scorer::{self, Bm25Params, GlobalStats, PrefixUse};
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

/// `#[non_exhaustive]` because this grew `truncated_prefixes` in a breaking
/// release, and the next thing a query needs to tell its caller should not
/// need another one.
/// `FACET`'s answer: per path, its top values by count over the rows the
/// predicate admits, the most first.
pub type Facets = Vec<(String, Vec<(Value, u64)>)>;

#[derive(Debug, Default)]
#[non_exhaustive]
pub struct QueryResult {
    pub rows: Vec<Row>,
    /// `FACET`: per path, its top values by count over the rows the
    /// predicate admits, the most first.
    pub facets: Facets,
    pub explain: Option<Explain>,
    /// Tablets that did not answer. Non-empty only under `WITH partial_results`
    /// (§8.4).
    pub missing: Vec<String>,
    /// Prefix leaves whose expansion hit
    /// [`PREFIX_EXPANSION_LIMIT`](crate::text::scorer::PREFIX_EXPANSION_LIMIT),
    /// one line each, naming the path and the prefix.
    ///
    /// The sibling of `missing`, and here for the same reason: both say the
    /// answer is short, and an answer that is short without saying so is the
    /// failure worth engineering against. `missing` is opt-in — a query only
    /// gets a partial answer from a tablet if it asked for one — while this is
    /// not, because nothing about `foo*` asks for a cap; it simply arrives.
    ///
    /// It is deliberately NOT confined to `EXPLAIN ANALYZE`, which is where
    /// truncation used to be reported and only on the ranking path. A
    /// `WHERE text_match(body, 'a*')` predicate is the commonest shape a wide
    /// prefix takes, it returns whatever fraction of the matching documents the
    /// cap left, and before this it said nothing on any path.
    pub truncated_prefixes: Vec<String>,
    /// Walks a cap bound, one line each naming the clause, the hop and the
    /// cap. The third sibling of `missing`: a `WITHIN k HOPS OF` under
    /// `max_frontier` or `max_fanout` returns the part of the neighbourhood
    /// the cap left, and says so here on every path, not only in the plan.
    pub cut_walks: Vec<String>,
    pub next_cursor: Option<String>,
}

impl QueryResult {
    /// A result of these rows and nothing else: no plan, nothing missing or
    /// cut, no cursor. What a client rebuilds from a console's answer before
    /// filling in what the answer carried.
    pub fn of_rows(rows: Vec<Row>) -> QueryResult {
        QueryResult {
            rows,
            facets: Vec::new(),
            explain: None,
            missing: Vec::new(),
            truncated_prefixes: Vec::new(),
            cut_walks: Vec::new(),
            next_cursor: None,
        }
    }
}

/// The prefixes this query's statistics say were cut, rendered for a human.
///
/// Read back off `GlobalStats` rather than threaded down from the coordinator
/// as a separate argument: the expansion and the verdict on it are one fact
/// (see [`Expansion`](crate::text::scorer::Expansion)), the stats map already
/// carries exactly this statement's paths and prefixes, and a second channel
/// would be a second thing to keep in step with the first.
///
/// The count is the terms KEPT, not the terms dropped. How many were dropped is
/// not known and deliberately not measured — see `Db::run_select`, where the
/// enumeration is bounded on purpose.
fn truncated_prefixes(stats: &BTreeMap<String, GlobalStats>) -> Vec<String> {
    let mut out = Vec::new();
    for (path, g) in stats {
        for (p, e) in &g.expansions {
            if !e.truncated {
                continue;
            }
            // Rendered as it was WRITTEN, sign and all. A report that prints
            // `a*` for a leaf the caller spelled `-a*` cannot be acted on:
            // both spellings can appear in one statement, and they are cut for
            // different reasons and at different costs.
            //
            // EVERY spelling, which is why this is a match and not a boolean.
            // One term list serves both polarities, so a statement that writes
            // `a*` in one clause and `-a*` in another gets ONE line here, and
            // that line has to name both — printing only `'a*'` beside the
            // combined consequence below left the reader with a message that
            // says rows were wrongly KEPT by a leaf whose spelling cannot keep
            // any, and no way to find the clause that did.
            let leaf = match (e.used.positive, e.used.negated) {
                (true, true) => format!("'{p}*' and '-{p}*'"),
                (false, true) => format!("'-{p}*'"),
                // `(false, false)` is unreachable — `required_prefixes` sets
                // one flag per occurrence and drops empty entries — and it
                // renders as the positive spelling, which is what a leaf with
                // no recorded polarity was written as.
                _ => format!("'{p}*'"),
            };
            // And the CONSEQUENCE, which is the half that was inverted.
            // Truncating an exclusion set does not lose rows — it fails to
            // remove them, so the answer has extra ones. Saying "documents are
            // missing" there is not a vague warning, it is the opposite of
            // what happened.
            //
            // Read off the CONSEQUENCE pair, not the spelling pair. The two
            // differ for any leaf under SQL's own `NOT`, which wraps the whole
            // `text_match` call and so cannot be seen from inside the query
            // string: `NOT text_match(body, 'a*')` is spelled positively and
            // excludes. Deriving this from `positive`/`negated` printed
            // "documents are missing" for a statement that had kept extra
            // ones, and a caller acting on that widens the prefix and keeps
            // more.
            let consequence = match (e.used.loses_rows, e.used.keeps_rows) {
                (_, false) => "so documents are missing from this answer",
                (false, true) => {
                    "so documents this query should have excluded are still in this answer"
                }
                (true, true) => {
                    "so documents are missing from this answer AND documents it should have \
                     excluded are still in it"
                }
            };
            // "more terms match", not "the collection matches more": the
            // expansion is resolved over the partition the statement named
            // when it names one, so the set this verdict was taken over is not
            // always the whole collection.
            out.push(format!(
                "text_match({path}, {leaf}) expanded to {} terms and was cut there: more \
                 terms than that match what this statement can see, {consequence}",
                e.terms.len()
            ));
        }
    }
    out
}

/// A candidate-generating source, resolved against the catalog.
pub enum SourcePlan {
    Text {
        path: String,
        query: TextQuery,
        terms: Vec<String>,
        name: String,
    },
    Vector {
        path: String,
        query: Vec<f32>,
        metric: Metric,
        name: String,
    },
    /// Hop distance, from the coordinator's walk: `walk` is its index among
    /// the statement's `hops(...)` sources, whose candidates the executor
    /// is handed already scored (`ExecInput::hop_sources`). A shard has
    /// nothing to add and answers it with an empty list.
    Hops {
        name: String,
        walk: usize,
    },
}

impl SourcePlan {
    fn name(&self) -> &str {
        match self {
            SourcePlan::Text { name, .. }
            | SourcePlan::Vector { name, .. }
            | SourcePlan::Hops { name, .. } => name,
        }
    }
    fn direction(&self) -> Direction {
        match self {
            SourcePlan::Text { .. } => Direction::HigherIsBetter,
            SourcePlan::Vector { .. } | SourcePlan::Hops { .. } => Direction::LowerIsBetter,
        }
    }
}

pub struct ExecInput<'a> {
    /// The collection's shards, through the boundary a network will one day
    /// sit under, in any order: every merge below attributes by
    /// `ShardService::index`, never by position.
    pub shards: &'a [Box<dyn ShardService + 'a>],
    /// Shards that did not answer before the executor was reached — at the
    /// statistics stage — under `partial_results`. Skipped here and reported
    /// in `missing`, so one statement names a missing shard once.
    pub unreachable: &'a [usize],
    pub coll: &'a Collection,
    /// The statement's parameters, carried to shards on other nodes with
    /// its text.
    pub params: &'a [Value],
    pub select: &'a Select,
    pub ts: Timestamp,
    /// Global term statistics for this query's terms, per text path (§8.2).
    pub stats: &'a BTreeMap<String, GlobalStats>,
    pub analyze: bool,
    pub statement: String,
    /// The key set each walk of the statement resolved to, in predicate
    /// order; `select` already has them bound as `IN` lists, and these are
    /// for the shards on other nodes that re-parse the statement.
    pub frontiers: &'a [Vec<String>],
    /// One candidate list per `hops(...)` source of the ORDER BY, in order,
    /// each key scored by the hop it was first reached at.
    pub hop_sources: Vec<Vec<Candidate>>,
    /// The walks as the coordinator ran them, for the plan.
    pub walks: Vec<WalkExplain>,
    /// Cut lines from the walks, for the response.
    pub cut_walks: Vec<String>,
    /// Edge shards that did not answer during a walk, under
    /// `partial_results`, for `missing`.
    pub walk_missing: Vec<String>,
    /// The facet this input computes, when it is one facet's aggregate
    /// rather than the statement: what its scan requests carry.
    pub facet: Option<String>,
}

/// Terms this statement needs global statistics for, per path. The coordinator
/// looks up only these and attaches only these (§8.1).
pub fn required_terms(coll: &Collection, sel: &Select) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut add = |path: &str, q: &str| {
        // A path with no full-text index has nothing to gather, and naming one
        // is not merely wasted work. `analyzer_for` falls back to "standard"
        // rather than reporting an unindexed path, so any string a caller puts
        // in a `text_match` parses and enters this map; `Db::gather_stats` then
        // creates a `CachedStats` entry keyed on it, in a map nothing evicts
        // from. One 1 MiB statement naming a thousand fresh paths retained a
        // thousand entries and returned one error. `Db::run_select` refuses an
        // undeclared path before it gets here, through [`undeclared_text_path`];
        // this guard stays so that the map never names one whatever the caller.
        if coll.fulltext_index(path).is_none() {
            return;
        }
        let an = Analyzer::parse(coll.analyzer_for(path));
        if let Ok(tq) = TextQuery::parse(q, an) {
            let mut terms = Vec::new();
            tq.leaf_terms(&mut terms);
            out.entry(path.to_string()).or_default().extend(terms);
        }
    };
    if let Some(e) = &sel.predicate {
        // The SQL sign is ignored here and used in `required_prefixes`: this
        // one gathers DOCUMENT FREQUENCIES, and `leaf_terms` already declines
        // to descend into a negation because a negated term is never scored.
        walk_text_match(e, false, &mut |p, q, _| add(p, q));
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

/// The first path a `text_match` in this statement names that the CATALOG
/// declares no full-text index on, in either site.
///
/// The catalog, not the units: a sealed segment keeps the region of an index
/// that was since dropped until compaction rewrites it, and a memtable rebuilt
/// after the drop has none, so a unit-by-unit check answered a query from the
/// old segments and refused it from the memtable -- the answer depended on
/// which units happened to exist. A declaration withdrawn is withdrawn for the
/// whole statement.
pub fn undeclared_text_path(coll: &Collection, sel: &Select) -> Option<String> {
    let mut found = None;
    let mut see = |path: &str| {
        if found.is_none() && coll.fulltext_index(path).is_none() {
            found = Some(path.to_string());
        }
    };
    if let Some(e) = &sel.predicate {
        walk_text_match(e, false, &mut |p, _, _| see(p));
    }
    if let Some(OrderBy::Hybrid(h)) = &sel.order {
        for s in &h.sources {
            if let HybridSource::Text { path, .. } = s {
                see(path);
            }
        }
    }
    found
}

/// Prefixes this statement needs the coordinator to expand, per path.
///
/// The sibling of [`required_terms`], walking the same two sites, and it exists
/// for the same reason: a prefix resolved per searchable unit is resolved
/// differently in each of them. `TextQuery::leaf_prefixes` explains why its
/// `Not` arm recurses where `leaf_terms`' does not.
///
/// Empty entries are dropped so that a statement with no prefix in it does no
/// expansion work at all — the coordinator's loop over this map is then simply
/// not entered.
///
/// Each prefix is carried once per path with a [`PrefixUse`] recording every
/// polarity the statement used it in. One term list serves both — `a*` and
/// `-a*` name the same terms — but they differ in what a CUT costs, so the
/// polarity has to survive the deduplication that merges them. Polarity here is
/// the mini-language's sign XOR the sign of the SQL around the call, which is
/// why the walk carries one: `NOT text_match(body, 'a*')` is spelled
/// positively and excludes, and both facts have to be recorded separately.
pub fn required_prefixes(
    coll: &Collection,
    sel: &Select,
) -> BTreeMap<String, BTreeMap<String, PrefixUse>> {
    let mut out: BTreeMap<String, BTreeMap<String, PrefixUse>> = BTreeMap::new();
    let mut add = |path: &str, q: &str, sql_negated: bool| {
        // As in [`required_terms`], and for the same reason. It also takes an
        // unindexed path out of the prefix-leaf budget's count, which is
        // right: that bound counts expansions that will be PAID for, and a
        // prefix on a path with no dictionary to walk costs nothing before the
        // statement fails.
        if coll.fulltext_index(path).is_none() {
            return;
        }
        let an = Analyzer::parse(coll.analyzer_for(path));
        if let Ok(tq) = TextQuery::parse(q, an) {
            let mut ps = Vec::new();
            // Seeded with the sign of the SQL around the call, so `effective`
            // is the leaf's real polarity: the mini-language's `-` XOR the
            // enclosing `NOT`.
            tq.prefixes_under(sql_negated, &mut ps);
            let e = out.entry(path.to_string()).or_default();
            for (p, effective) in ps {
                let u = e.entry(p).or_default();
                // How it was WRITTEN, which is the seed taken back out. This
                // is the only string a caller can find in their own statement,
                // so it is what the report renders.
                if effective != sql_negated {
                    u.negated = true;
                } else {
                    u.positive = true;
                }
                // What a CUT costs, which is the effective sign and nothing
                // else. `NOT text_match(body, 'a*')` is spelled positively and
                // excludes; deriving one from the other reports the inverse.
                if effective {
                    u.keeps_rows = true;
                } else {
                    u.loses_rows = true;
                }
            }
        }
    };
    if let Some(e) = &sel.predicate {
        walk_text_match(e, false, &mut |p, q, n| add(p, q, n));
    }
    if let Some(OrderBy::Hybrid(h)) = &sel.order {
        for s in &h.sources {
            if let HybridSource::Text { path, query } = s {
                // A ranking source is never under a `NOT`: it is named by
                // ORDER BY, not by the predicate.
                add(path, query, false);
            }
        }
    }
    out.retain(|_, v| !v.is_empty());
    out
}

/// Every `text_match` in a predicate, with the sign of the SQL around it.
///
/// The sign is not decoration. `Expr::Not` is evaluated as `eval_defined(inner)
/// andnot matched`, so a SHORT `matched` yields MORE rows, not fewer — a cut
/// prefix under a SQL `NOT` has the consequence of an exclusion however it was
/// spelled inside the `text_match` string. Without this flag the only polarity
/// anyone downstream can see is the one the mini-language records, which is
/// half the answer.
///
/// `And` and `Or` pass the sign through unchanged: under a `Not` they are De
/// Morgan, and De Morgan preserves each LEAF's effective sign.
fn walk_text_match(e: &Expr, negated: bool, f: &mut impl FnMut(&str, &str, bool)) {
    match e {
        Expr::TextMatch { path, query } => f(path, query, negated),
        Expr::And(v) | Expr::Or(v) => {
            for x in v {
                walk_text_match(x, negated, f);
            }
        }
        Expr::Not(x) => walk_text_match(x, !negated, f),
        _ => {}
    }
}

/// The partition-key equality this query is constrained on, if any. Partition
/// pruning is the primary defence against fan-out (§8.4), so it is looked for
/// before anything else runs.
///
/// `pub(crate)` because the coordinator needs the same answer BEFORE execution
/// starts: a prefix is expanded once for the whole statement, and expanding it
/// over the whole collection spends the cap on terms no document the statement
/// can return holds.
pub(crate) fn partition_constraint(coll: &Collection, e: Option<&Expr>) -> Option<String> {
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
    match &coll.partition_key {
        Some(pk) => find(e?, pk).and_then(|v| partition_prefix(&v).ok()),
        // No partition key: the sort key is the primary key itself, so an
        // equality on it pins the statement to the one shard whose range
        // holds that key (0.34.0). A whole key is its own prefix.
        None => find(e?, &coll.primary_key).and_then(|v| crate::shard::key_component(&v).ok()),
    }
}

pub fn run_select(input: ExecInput<'_>) -> Result<QueryResult> {
    // A distance threshold is checked against the catalog once, here, with
    // the same checks and the same messages as an `ORDER BY` distance: the
    // index exists, the dimensions agree, the operator matches the metric.
    // Inside a unit those are already true or the unit has no vectors.
    fn check_thresholds(coll: &Collection, e: &Expr) -> Result<()> {
        match e {
            Expr::VectorDistance { path, op, query, .. } => check_vector(coll, path, *op, query),
            Expr::And(v) | Expr::Or(v) => v.iter().try_for_each(|x| check_thresholds(coll, x)),
            Expr::Not(x) => check_thresholds(coll, x),
            // The coordinator binds every walk to a key set before the
            // executor sees the statement; one that reached here was not.
            Expr::Hops { via, .. } => Err(Error::Plan(format!(
                "a walk over `{via}` reached the executor unresolved; only the coordinator \
                 can walk, through Db::run_select"
            ))),
            Expr::Compare { .. } | Expr::TextMatch { .. } | Expr::True => Ok(()),
        }
    }
    if let Some(e) = &input.select.predicate {
        check_thresholds(input.coll, e)?;
    }
    let t0 = Instant::now();
    let sel = input.select;
    let k = sel.limit.unwrap_or(10);

    let mut ex = Explain {
        statement: input.statement.clone(),
        snapshot_ts: input.ts,
        limit: k,
        offset: sel.offset,
        exact_mode: sel.with.exact,
        deadline_ms: deadline::limit_ms(),
        stats_exact: input.stats.values().next().map(|s| s.exact).unwrap_or(sel.with.exact_scoring),
        walks: input.walks.clone(),
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

    // --- Aggregates: every shard folds its matching rows into one partial
    // per group, the coordinator merges the partials. No candidate
    // generation, no fetch.
    if sel.aggregates() {
        return aggregate_select(&input, &prefix, t0, ex);
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
        let out = scan(&input, &prefix, k, &mut ex)?;
        ex.total_micros = t0.elapsed().as_micros();
        let cut = truncated_prefixes(input.stats);
        ex.notes.extend(cut.iter().cloned());
        ex.notes.extend(input.cut_walks.iter().cloned());
        let mut rows = out.0;
        project(&input, &mut rows);
        if !sel.with.partial_results {
            deadline::check()?;
        }
        let facets = facets_for(&input, &prefix, t0)?;
        let mut missing = input.walk_missing.clone();
        missing.extend(out.1);
        ex.missing = missing.clone();
        return Ok(QueryResult {
            rows,
            facets,
            explain: if input.analyze { Some(ex) } else { None },
            missing,
            truncated_prefixes: cut,
            cut_walks: input.cut_walks.clone(),
            next_cursor: None,
        });
    }

    // --- Scatter: one round trip, every source evaluated in the same pass.
    // Each shard answers through `ShardService::candidates` with identifiers
    // and raw scores only -- no ranks, no documents -- in whatever order the
    // services were handed over, since every merge below sorts. A shard that
    // did not answer within the deadline, in this process or across the
    // boundary, is refused or reported by one rule.
    let partial = sel.with.partial_results;
    let mut per_source: Vec<Vec<Candidate>> = vec![Vec::new(); sources.len()];
    // A hop source's candidates come from the coordinator's walk, not the
    // shards; the walk ran before the scatter, so they are ready now.
    for (i, sp) in sources.iter().enumerate() {
        if let SourcePlan::Hops { walk, .. } = sp {
            per_source[i] = input.hop_sources.get(*walk).cloned().unwrap_or_default();
        }
    }
    let mut missing: Vec<String> = Vec::new();
    for si in input.unreachable {
        ex.shards.push(ShardExplain { index: *si, timed_out: true, ..Default::default() });
        missing.push(shard_name(*si));
    }
    for shard in input.shards {
        let si = shard.index();
        if input.unreachable.contains(&si) {
            continue;
        }
        if let Some(p) = &prefix {
            if !shard.may_hold(p) {
                ex.shards.push(ShardExplain {
                    index: si,
                    pruned: true,
                    prune_reason: Some(format!("key prefix `{}` outside range", show_key(p))),
                    manifest_version: shard.manifest_version(),
                    ..Default::default()
                });
                continue;
            }
        }
        if let Some(ms) = deadline::passed() {
            ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
            missing.push(shard_name(si));
            if partial {
                continue;
            }
            return Err(shard_deadline(si, ms));
        }
        let req = CandidatesRequest {
            coll: input.coll,
            select: sel,
            ts: input.ts,
            prefix: prefix.as_deref(),
            sources: &sources,
            k_prime,
            stats: input.stats,
            analyze: input.analyze,
            statement: &input.statement,
            params: input.params,
            frontiers: input.frontiers,
        };
        match shard.candidates(&req) {
            Ok(a) => {
                if a.timed_out {
                    missing.push(shard_name(si));
                }
                ex.shards.push(a.explain);
                for (i, list) in a.per_source.into_iter().enumerate() {
                    if let Some(p) = per_source.get_mut(i) {
                        p.extend(list);
                    }
                }
            }
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
                missing.push(shard_name(si));
            }
            Err(e) => return Err(e),
        }
    }
    ex.shards.sort_by_key(|s| s.index);

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
                SourcePlan::Hops { .. } => (c.key, -c.raw_score, None),
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
            let Some(doc) = fetch(input.shards, key, input.ts, partial, &mut missing)? else {
                continue;
            };
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
            let Some(doc) = fetch(input.shards, key, input.ts, partial, &mut missing)? else {
                continue;
            };
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
    missing.sort();
    missing.dedup();
    let mut missing = {
        let mut m = input.walk_missing.clone();
        m.extend(missing);
        m
    };
    missing.dedup();
    ex.missing = missing.clone();
    ex.total_micros = t0.elapsed().as_micros();
    let cut = truncated_prefixes(input.stats);
    ex.notes.extend(cut.iter().cloned());
    ex.notes.extend(input.cut_walks.iter().cloned());
    project(&input, &mut rows);
    // Strict at the end: a deadline that passed during the last unit's work
    // is a statement that did not finish in time, whatever the loop managed
    // to return. Under `partial_results` the shards that ran out are already
    // in `missing`, which is the contract that option buys.
    if !sel.with.partial_results {
        deadline::check()?;
    }
    let facets = facets_for(&input, &prefix, t0)?;
    Ok(QueryResult {
        rows,
        facets,
        explain: if input.analyze { Some(ex) } else { None },
        missing,
        truncated_prefixes: cut,
        cut_walks: input.cut_walks.clone(),
        next_cursor,
    })
}

/// The statement's facets, each one aggregate over the same shards: the
/// path's values counted over every row the predicate admits -- the
/// candidate set, not the page -- merged across shards and nodes as a
/// `GROUP BY` is, the most first, the top `n`. Nothing for a statement
/// without a `FACET`.
fn facets_for(input: &ExecInput<'_>, prefix: &Option<String>, t0: Instant) -> Result<Facets> {
    let sel = input.select;
    let mut out = Vec::with_capacity(sel.facets.len());
    for path in &sel.facets {
        let fsel = facet_select(sel, path, sel.facet_top);
        let finput = ExecInput {
            shards: input.shards,
            unreachable: input.unreachable,
            coll: input.coll,
            params: input.params,
            select: &fsel,
            ts: input.ts,
            stats: input.stats,
            analyze: false,
            statement: input.statement.clone(),
            frontiers: input.frontiers,
            hop_sources: Vec::new(),
            walks: Vec::new(),
            cut_walks: Vec::new(),
            walk_missing: Vec::new(),
            facet: Some(path.clone()),
        };
        let r = aggregate_select(&finput, prefix, t0, Explain::default())?;
        let values = r
            .rows
            .iter()
            .map(|row| {
                let v = row.doc.path("value").cloned().unwrap_or(Value::Null);
                let n = row.doc.path("n").and_then(|x| x.as_i64()).unwrap_or(0).max(0) as u64;
                (v, n)
            })
            .collect();
        out.push((path.clone(), values));
    }
    Ok(out)
}

/// Apply the SELECT list to the rows, in place, as the last step of a query.
///
/// `*` anywhere in the list keeps the whole document. Otherwise the document
/// becomes an object holding one entry per named path: the value at the
/// path, or `Null` where the document has none, so that every row has the
/// same shape and a table over them has the same columns -- a projection
/// over polymorphic documents shows its gaps rather than hiding them. A
/// nested path is keyed by its alias, or by the path as written; the row's
/// `key`, `score` and `distance` are not document fields and are untouched.
/// `score` and `distance` in the list are accepted for readability and
/// change nothing: a ranked query carries them whether or not they are named.
///
/// Last, after collapse, the cursor and the fetch, because each of those
/// reads fields the list may not name: `COLLAPSE BY` its path, the cursor its
/// sort key, the fetch the whole payload. The list used to be parsed and read
/// nowhere, so every surface returned the whole document whatever was asked.
fn project(input: &ExecInput<'_>, rows: &mut [Row]) {
    let projections = &input.select.projections;
    if projections.iter().any(|p| matches!(p, Projection::All)) {
        return;
    }
    for row in rows.iter_mut() {
        let mut fields: Vec<(String, Value)> = Vec::new();
        for p in projections {
            match p {
                Projection::Path { path, alias } => {
                    let name = alias.clone().unwrap_or_else(|| path.clone());
                    let value = row.doc.path(path).cloned().unwrap_or(Value::Null);
                    fields.push((name, value));
                }
                Projection::Snippet { path, words, alias } => {
                    let name = alias.clone().unwrap_or_else(|| format!("snippet({path})"));
                    let value = match row.doc.path(path) {
                        Some(v) => snippet_of(input, path, v, *words),
                        None => Value::Null,
                    };
                    fields.push((name, value));
                }
                _ => {}
            }
        }
        row.doc = Value::obj(fields);
    }
}

/// `snippet(path, n)` for one row: `n` of the field's words around the
/// first the statement's text query matched, the window placed to cover as
/// many matches as it can, each matched word in `<em>`, an ellipsis at a
/// cut end. The words are the analyzer's -- punctuation between them is
/// not kept -- and a match is a word the index's analyzer would have
/// indexed under one of the query's terms, or under one of its prefixes.
/// A field the query did not name, or a query with no text match, gets
/// the first `n` words with nothing marked; a field that is not text, or
/// an array of it, is `null`.
fn snippet_of(input: &ExecInput<'_>, path: &str, value: &Value, n: usize) -> Value {
    let text = match value {
        Value::Str(s) => s.clone(),
        Value::Array(items) => {
            let parts: Vec<&str> = items.iter().filter_map(|v| v.as_str()).collect();
            if parts.is_empty() {
                return Value::Null;
            }
            parts.join(" ")
        }
        _ => return Value::Null,
    };
    let an = Analyzer::parse(input.coll.analyzer_for(path));
    let (terms, prefixes) = snippet_terms(input.coll, input.select, path);
    // A keyword analyzer indexes the whole field as one term at position
    // zero: the snippet is the field, marked if it matched.
    let words = match an {
        Analyzer::Keyword => vec![text.trim().to_string()],
        _ => crate::text::analyzer::split_words(&text),
    };
    let mut analyzed = Vec::new();
    an.analyze(&text, 0, &mut analyzed);
    let matched: BTreeSet<usize> = analyzed
        .iter()
        .filter(|(t, _)| {
            terms.iter().any(|q| q == t) || prefixes.iter().any(|p| t.starts_with(p.as_str()))
        })
        .map(|(_, p)| *p as usize)
        .filter(|p| *p < words.len())
        .collect();
    let n = n.max(1);
    let start = match matched.iter().next() {
        Some(&first) => {
            // The window that begins at or before the first match and
            // covers the most matches; the earliest of the best.
            let lowest = first.saturating_sub(n - 1);
            (lowest..=first)
                .map(|s| (matched.range(s..s + n).count(), std::cmp::Reverse(s)))
                .max()
                .map(|(_, std::cmp::Reverse(s))| s)
                .unwrap_or(first)
        }
        None => 0,
    };
    let end = (start + n).min(words.len());
    let mut out = String::new();
    if start > 0 {
        out.push_str("… ");
    }
    for i in start..end {
        if i > start {
            out.push(' ');
        }
        if matched.contains(&i) {
            out.push_str("<em>");
            out.push_str(&words[i]);
            out.push_str("</em>");
        } else {
            out.push_str(&words[i]);
        }
    }
    if end < words.len() {
        out.push_str(" …");
    }
    Value::Str(out)
}

/// The analyzed terms and prefixes the statement's text queries name on
/// `path`, positive ones only: what a snippet marks.
fn snippet_terms(coll: &Collection, sel: &Select, path: &str) -> (Vec<String>, Vec<String>) {
    let an = Analyzer::parse(coll.analyzer_for(path));
    let (mut terms, mut prefixes) = (Vec::new(), Vec::new());
    let mut add = |p: &str, q: &str, negated: bool| {
        if p != path || negated {
            return;
        }
        if let Ok(tq) = TextQuery::parse(q, an) {
            tq.leaf_terms(&mut terms);
            let mut ps = Vec::new();
            tq.leaf_prefixes(&mut ps);
            prefixes.extend(ps.into_iter().filter(|(_, neg)| !neg).map(|(p, _)| p));
        }
    };
    if let Some(e) = &sel.predicate {
        walk_text_match(e, false, &mut |p, q, negated| add(p, q, negated));
    }
    if let Some(OrderBy::Hybrid(h)) = &sel.order {
        for s in &h.sources {
            if let HybridSource::Text { path: p, query } = s {
                add(p, query, false);
            }
        }
    }
    terms.sort();
    terms.dedup();
    (terms, prefixes)
}

/// The coordinator's half of an aggregate statement. Each shard answers a
/// scan whose hits are one per group -- the group's value as the sort key,
/// its JSON as the row key, the partial accumulators as the document (see
/// [`aggregate_on`]) -- and the partials of one group from every shard
/// merge into the group's row. `ORDER BY` names output fields (an alias,
/// or the call as written), `LIMIT` and `OFFSET` count groups; without a
/// `LIMIT` every group is returned, because the groups are the answer and
/// there is no page to stop at.
fn aggregate_select(
    input: &ExecInput<'_>,
    prefix: &Option<String>,
    t0: Instant,
    mut ex: Explain,
) -> Result<QueryResult> {
    let sel = input.select;
    if sel.collapse.is_some() {
        return Err(Error::Plan("COLLAPSE BY has no meaning with an aggregate".into()));
    }
    if sel.cursor.is_some() {
        return Err(Error::Plan(
            "AFTER has no meaning with an aggregate: the groups are one answer".into(),
        ));
    }
    if matches!(sel.order, Some(OrderBy::Distance { .. }) | Some(OrderBy::Hybrid(_))) {
        return Err(Error::Plan(
            "an aggregate is over every row the predicate admits; a ranked ORDER BY chooses \
             k of them, which is a different question -- put the distance in WHERE, or \
             aggregate in the client"
                .into(),
        ));
    }
    let partial = sel.with.partial_results;
    let specs: Vec<(AggFunc, Option<&str>)> = sel
        .projections
        .iter()
        .filter_map(|p| match p {
            Projection::Aggregate { func, path, .. } => Some((*func, path.as_deref())),
            _ => None,
        })
        .collect();
    // Group key (JSON) -> (group value, partials in spec order).
    let mut groups: BTreeMap<String, (Value, Vec<Value>)> = BTreeMap::new();
    let mut missing: Vec<String> = Vec::new();
    for si in input.unreachable {
        ex.shards.push(ShardExplain { index: *si, timed_out: true, ..Default::default() });
        missing.push(shard_name(*si));
    }
    // A plain `count(*)` -- no predicate, no group, no prefix -- is the
    // sum of the shards' live counts, which every holder keeps: no scan.
    // (Ten shards of 25k documents scanned in 2.3 s, and thirty-two at
    // once took twenty.) A holder from before the count call answers
    // none, and the scan below answers as before, for every shard.
    let plain_count = specs.len() == 1
        && matches!(specs[0], (AggFunc::Count, None))
        && sel.predicate.is_none()
        && sel.group_by.is_none()
        && prefix.is_none();
    let mut counted: Option<u64> = None;
    if plain_count {
        let mut total = 0u64;
        let mut every = true;
        let mut ex_count = Vec::new();
        let mut missing_count = Vec::new();
        for shard in input.shards {
            let si = shard.index();
            if input.unreachable.contains(&si) {
                continue;
            }
            if let Some(ms) = deadline::passed() {
                if !partial {
                    return Err(shard_deadline(si, ms));
                }
                ex_count.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
                missing_count.push(shard_name(si));
                continue;
            }
            match shard.count(input.ts) {
                Ok(Some(n)) => {
                    total += n;
                    ex_count.push(ShardExplain {
                        index: si,
                        counted: true,
                        manifest_version: shard.manifest_version(),
                        ..Default::default()
                    });
                }
                Ok(None) => {
                    every = false;
                    break;
                }
                Err(Error::Deadline(e)) => {
                    if !partial {
                        return Err(Error::Deadline(e));
                    }
                    ex_count.push(ShardExplain {
                        index: si,
                        timed_out: true,
                        ..Default::default()
                    });
                    missing_count.push(shard_name(si));
                }
                Err(e) => return Err(e),
            }
        }
        if every {
            counted = Some(total);
            ex.shards.extend(ex_count);
            missing.extend(missing_count);
            groups.insert(String::new(), (Value::Null, vec![Value::Int(total as i64)]));
        }
    }
    let scanned: &[Box<dyn ShardService + '_>] = if counted.is_some() { &[] } else { input.shards };
    for shard in scanned {
        let si = shard.index();
        if input.unreachable.contains(&si) {
            continue;
        }
        if let Some(p) = prefix {
            if !shard.may_hold(p) {
                ex.shards.push(ShardExplain {
                    index: si,
                    pruned: true,
                    prune_reason: Some("out of key range".into()),
                    manifest_version: shard.manifest_version(),
                    ..Default::default()
                });
                continue;
            }
        }
        if let Some(ms) = deadline::passed() {
            ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
            missing.push(shard_name(si));
            if partial {
                continue;
            }
            return Err(shard_deadline(si, ms));
        }
        let req = ScanRequest {
            coll: input.coll,
            select: sel,
            ts: input.ts,
            prefix: prefix.as_deref(),
            stats: input.stats,
            analyze: input.analyze,
            keep: usize::MAX,
            after: None,
            fields: &[],
            statement: &input.statement,
            params: input.params,
            frontiers: input.frontiers,
            facet: input.facet.as_deref(),
        };
        match shard.scan(&req) {
            Ok(a) => {
                if a.timed_out {
                    missing.push(shard_name(si));
                }
                ex.shards.push(a.explain);
                for h in a.hits {
                    // A node of an older version answers a scan with rows,
                    // not partials; its shard cannot be aggregated.
                    let partials = match h.doc.as_ref().and_then(|d| d.path(AGG_FIELD)) {
                        Some(Value::Array(v)) if v.len() == specs.len() => v.clone(),
                        _ => {
                            return Err(Error::Plan(format!(
                                "shard {si} answered rows where partial aggregates were asked; \
                                 the node holding it runs a version without aggregates"
                            )))
                        }
                    };
                    let group = h.sort.first().cloned().unwrap_or(Value::Null);
                    match groups.get_mut(&h.key) {
                        Some((_, acc)) => {
                            for ((func, _), (a, b)) in
                                specs.iter().zip(acc.iter_mut().zip(partials))
                            {
                                *a = merge_partial(*func, std::mem::replace(a, Value::Null), b)?;
                            }
                        }
                        None => {
                            groups.insert(h.key, (group, partials));
                        }
                    }
                }
            }
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
                missing.push(shard_name(si));
            }
            Err(e) => return Err(e),
        }
    }
    // No group and no row at all is still one row: `count(*)` of nothing is 0.
    if sel.group_by.is_none() && groups.is_empty() {
        let empty = specs.iter().map(|(f, _)| empty_partial(*f)).collect();
        groups.insert(String::new(), (Value::Null, empty));
    }
    let mut rows: Vec<Row> = Vec::with_capacity(groups.len());
    for (key, (group, acc)) in groups {
        let mut fields: Vec<(String, Value)> = Vec::new();
        let mut next = 0usize;
        for p in &sel.projections {
            match p {
                Projection::Path { path, alias } => {
                    fields.push((alias.clone().unwrap_or_else(|| path.clone()), group.clone()));
                }
                Projection::Aggregate { func, .. } => {
                    let name = p.aggregate_name().expect("an aggregate");
                    fields.push((name, finish_partial(*func, &acc[next])));
                    next += 1;
                }
                _ => {}
            }
        }
        rows.push(Row { key, doc: Value::obj(fields), score: None, distance: None });
    }
    if let Some(OrderBy::Fields(fields)) = &sel.order {
        for (name, _) in fields {
            if !rows.first().is_some_and(|r| r.doc.path(name).is_some()) && !rows.is_empty() {
                return Err(Error::Plan(format!(
                    "ORDER BY `{name}`: an aggregate statement orders by a field of its result \
                     (an alias, or the call as written); it has {}",
                    match &rows[0].doc {
                        Value::Object(f) => {
                            f.iter().map(|(k, _)| format!("`{k}`")).collect::<Vec<_>>().join(", ")
                        }
                        _ => String::new(),
                    }
                )));
            }
        }
        rows.sort_by(|a, b| {
            for (name, asc) in fields {
                let (x, y) = (a.doc.path(name), b.doc.path(name));
                let o = match (x, y) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (Some(v), None) if v.is_null() => std::cmp::Ordering::Greater,
                    (None, Some(v)) if v.is_null() => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (Some(x), Some(y)) => match (x.is_null(), y.is_null()) {
                        (true, true) => std::cmp::Ordering::Equal,
                        (true, false) => std::cmp::Ordering::Greater,
                        (false, true) => std::cmp::Ordering::Less,
                        _ => agg_cmp(x, y).unwrap_or(std::cmp::Ordering::Equal),
                    },
                };
                let o = if *asc { o } else { o.reverse() };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            a.key.cmp(&b.key)
        });
    }
    let total = rows.len();
    let rows: Vec<Row> =
        rows.into_iter().skip(sel.offset).take(sel.limit.unwrap_or(usize::MAX)).collect();
    ex.limit = sel.limit.unwrap_or(total);
    ex.notes.push(format!("aggregate: {total} group(s) merged from the shards' partials"));
    ex.total_micros = t0.elapsed().as_micros();
    let cut = truncated_prefixes(input.stats);
    ex.notes.extend(cut.iter().cloned());
    if !partial {
        deadline::check()?;
    }
    let mut all_missing = input.walk_missing.clone();
    all_missing.extend(missing);
    ex.missing = all_missing.clone();
    Ok(QueryResult {
        rows,
        explain: if input.analyze { Some(ex) } else { None },
        missing: all_missing,
        truncated_prefixes: cut,
        cut_walks: input.cut_walks.clone(),
        facets: Vec::new(),
        next_cursor: None,
    })
}

/// The field a shard's aggregate hit carries its partials under.
const AGG_FIELD: &str = "__agg";

/// A partial with nothing folded in yet.
fn empty_partial(func: AggFunc) -> Value {
    match func {
        AggFunc::Count => Value::Int(0),
        AggFunc::Sum | AggFunc::Min | AggFunc::Max => Value::Null,
        AggFunc::Avg => Value::Array(vec![Value::Null, Value::Int(0)]),
    }
}

/// Fold one row's value into a partial. `Null` (and an absent path) is
/// skipped by everything but `count(*)`, which is fed `Int(1)` per row.
fn fold_partial(func: AggFunc, acc: Value, v: &Value, path: &str) -> Result<Value> {
    if v.is_null() {
        return Ok(acc);
    }
    Ok(match func {
        AggFunc::Count => Value::Int(acc.as_i64().unwrap_or(0) + 1),
        AggFunc::Sum => add_numbers(acc, number_of(v, "sum", path)?),
        AggFunc::Min => match acc {
            Value::Null => v.clone(),
            a => {
                if agg_cmp(v, &a)? == std::cmp::Ordering::Less {
                    v.clone()
                } else {
                    a
                }
            }
        },
        AggFunc::Max => match acc {
            Value::Null => v.clone(),
            a => {
                if agg_cmp(v, &a)? == std::cmp::Ordering::Greater {
                    v.clone()
                } else {
                    a
                }
            }
        },
        AggFunc::Avg => {
            let (sum, n) = avg_parts(&acc);
            Value::Array(vec![add_numbers(sum, number_of(v, "avg", path)?), Value::Int(n + 1)])
        }
    })
}

/// Merge two partials of one group, from two shards.
fn merge_partial(func: AggFunc, a: Value, b: Value) -> Result<Value> {
    Ok(match func {
        AggFunc::Count => Value::Int(a.as_i64().unwrap_or(0) + b.as_i64().unwrap_or(0)),
        AggFunc::Sum => match (&a, &b) {
            (Value::Null, _) => b,
            (_, Value::Null) => a,
            _ => add_numbers(a, b),
        },
        AggFunc::Min | AggFunc::Max => match (&a, &b) {
            (Value::Null, _) => b,
            (_, Value::Null) => a,
            _ => {
                let o = agg_cmp(&b, &a)?;
                let take_b = match func {
                    AggFunc::Min => o == std::cmp::Ordering::Less,
                    _ => o == std::cmp::Ordering::Greater,
                };
                if take_b {
                    b
                } else {
                    a
                }
            }
        },
        AggFunc::Avg => {
            let (sa, na) = avg_parts(&a);
            let (sb, nb) = avg_parts(&b);
            let sum = match (&sa, &sb) {
                (Value::Null, _) => sb,
                (_, Value::Null) => sa,
                _ => add_numbers(sa, sb),
            };
            Value::Array(vec![sum, Value::Int(na + nb)])
        }
    })
}

/// The value a finished partial presents.
fn finish_partial(func: AggFunc, acc: &Value) -> Value {
    match func {
        AggFunc::Avg => {
            let (sum, n) = avg_parts(acc);
            match (sum.as_f64(), n) {
                (Some(s), n) if n > 0 => Value::Float(s / n as f64),
                _ => Value::Null,
            }
        }
        _ => acc.clone(),
    }
}

fn avg_parts(acc: &Value) -> (Value, i64) {
    match acc {
        Value::Array(v) if v.len() == 2 => (v[0].clone(), v[1].as_i64().unwrap_or(0)),
        _ => (Value::Null, 0),
    }
}

/// A row's value as a number for `sum` and `avg`, or the reason it is not.
fn number_of(v: &Value, func: &str, path: &str) -> Result<Value> {
    match v {
        Value::Int(_) | Value::Float(_) => Ok(v.clone()),
        other => Err(Error::Plan(format!(
            "{func}({path}): a value at `{path}` is {}, not a number",
            other.ty().name()
        ))),
    }
}

/// Integers stay integers until they overflow or meet a float.
fn add_numbers(a: Value, b: Value) -> Value {
    match (&a, &b) {
        (Value::Null, _) => b,
        (_, Value::Null) => a,
        (Value::Int(x), Value::Int(y)) => match x.checked_add(*y) {
            Some(s) => Value::Int(s),
            None => Value::Float(*x as f64 + *y as f64),
        },
        _ => Value::Float(a.as_f64().unwrap_or(0.0) + b.as_f64().unwrap_or(0.0)),
    }
}

/// How `min`, `max` and `ORDER BY` compare two non-null values: numbers
/// with numbers, strings with strings, timestamps with timestamps, booleans
/// with booleans; anything else is a mixed group, which is refused rather
/// than ordered by an accident of encoding.
fn agg_cmp(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => Ok(a
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&b.as_f64().unwrap_or(0.0))
            .unwrap_or(Ordering::Equal)),
        (Value::Str(x), Value::Str(y)) => Ok(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Ok(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Ok(x.cmp(y)),
        _ => Err(Error::Plan(format!(
            "min/max over mixed kinds: {} and {} at one path",
            a.ty().name(),
            b.ty().name()
        ))),
    }
}

/// The shard's half of an aggregate statement: every matching row of every
/// unit folded into the partials of its group, one hit per group. A
/// `count(*)` alone, ungrouped, never decodes a document: it is the
/// survivors' popcount. Everything else reads each row's document once for
/// the group value and the aggregated paths.
pub(crate) fn aggregate_on(shard: &Shard, si: usize, req: &ScanRequest<'_>) -> Result<ShardScan> {
    let sel = req.select;
    let specs: Vec<(AggFunc, Option<&str>)> = sel
        .projections
        .iter()
        .filter_map(|p| match p {
            Projection::Aggregate { func, path, .. } => Some((*func, path.as_deref())),
            _ => None,
        })
        .collect();
    let count_only =
        sel.group_by.is_none() && specs.iter().all(|(f, p)| *f == AggFunc::Count && p.is_none());
    let mut sx =
        ShardExplain { index: si, manifest_version: shard.manifest_version, ..Default::default() };
    let t0 = Instant::now();
    let snap = shard.snapshot_at(req.ts);
    let units = shard.sources(&snap);
    let mut groups: BTreeMap<String, (Value, Vec<Value>)> = BTreeMap::new();
    let mut timed_out = false;
    for unit in units.iter() {
        let n = unit.num_docs();
        if n == 0 {
            continue;
        }
        let ut = Instant::now();
        let mut ux = UnitExplain { label: unit.label(), docs: n, ..Default::default() };
        let io0 = unit.io_counters();
        let vis = unit.visibility(req.ts);
        ux.visible = vis.popcount();
        let mut filter = match req.prefix {
            Some(p) => unit.key_prefix(p),
            None => Bitmap::all(n),
        };
        filter.and_inplace(&vis);
        if let Some(e) = &sel.predicate {
            let bm = eval_expr(unit, e, &vis, &filter, req.stats, req.analyze, &mut ux)?;
            filter.and_inplace(&bm);
        }
        ux.survivors = filter.popcount();
        ux.selectivity = ux.survivors as f64 / n as f64;
        if count_only {
            let entry = groups.entry(String::new()).or_insert_with(|| {
                (Value::Null, specs.iter().map(|(f, _)| empty_partial(*f)).collect())
            });
            for acc in entry.1.iter_mut() {
                *acc = Value::Int(acc.as_i64().unwrap_or(0) + ux.survivors as i64);
            }
        } else {
            for ord in filter.iter() {
                if deadline::expired() {
                    break;
                }
                let doc = unit.document(ord)?;
                let group = match &sel.group_by {
                    Some(p) => doc.path(p).cloned().unwrap_or(Value::Null),
                    None => Value::Null,
                };
                let key = match &sel.group_by {
                    Some(_) => crate::json::to_string(&group),
                    None => String::new(),
                };
                let entry = groups.entry(key).or_insert_with(|| {
                    (group.clone(), specs.iter().map(|(f, _)| empty_partial(*f)).collect())
                });
                for ((func, path), acc) in specs.iter().zip(entry.1.iter_mut()) {
                    let v = match path {
                        Some(p) => doc.path(p).cloned().unwrap_or(Value::Null),
                        None => Value::Int(1),
                    };
                    *acc = fold_partial(
                        *func,
                        std::mem::replace(acc, Value::Null),
                        &v,
                        path.unwrap_or("*"),
                    )?;
                }
            }
        }
        if let (Some((l0, f0)), Some((l1, f1))) = (io0, unit.io_counters()) {
            ux.loads = l1.saturating_sub(l0);
            ux.faults = f1.saturating_sub(f0);
        }
        ux.micros = ut.elapsed().as_micros();
        sx.units.push(ux);
        if let Some(ms) = deadline::passed() {
            if !sel.with.partial_results {
                return Err(shard_deadline(si, ms));
            }
            timed_out = true;
            break;
        }
    }
    sx.timed_out = timed_out;
    sx.micros = t0.elapsed().as_micros();
    let hits = groups
        .into_iter()
        .map(|(key, (group, acc))| ScanHit {
            sort: if sel.group_by.is_some() { vec![group] } else { Vec::new() },
            key,
            doc: Some(Value::obj(vec![(AGG_FIELD.to_string(), Value::Array(acc))])),
            handle: (0, 0),
            parent: None,
        })
        .collect();
    Ok(ShardScan { hits, explain: sx, timed_out })
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
pub(crate) struct SourcePlanning {
    pub(crate) sources: Vec<SourcePlan>,
    method: FusionMethod,
    weights: Vec<f32>,
    rrf_c: f32,
    k_prime: usize,
}

pub(crate) fn plan_sources(coll: &Collection, sel: &Select, k: usize) -> Result<SourcePlanning> {
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
                    HybridSource::Hops { via, .. } => {
                        // The walk itself is checked and run by the
                        // coordinator before the plan; here it is a source
                        // with a name and a number.
                        let walk =
                            sources.iter().filter(|s| matches!(s, SourcePlan::Hops { .. })).count();
                        sources.push(SourcePlan::Hops { name: format!("hops({via})"), walk });
                    }
                }
            }
            if h.sources.len() == 1 && matches!(h.sources[0], HybridSource::Hops { .. }) {
                return Err(Error::Plan(
                    "hops() ranks beside another source; a walk on its own is the filter \
                     `WHERE id WITHIN k HOPS OF ...`"
                        .into(),
                ));
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
    if let Some((i, bad)) = query.iter().enumerate().find(|(_, x)| !x.is_finite()) {
        // The store side refuses a non-finite *stored* component because one
        // infinity gives a dimension an infinite range and dequantizes the
        // whole segment to NaN. A query carrying one is the same value arriving
        // from the other side: every distance it computes is NaN, and a NaN
        // sorts by whatever the comparator happens to do with it rather than
        // erroring. A literal that overflows f32 (`1e40`) and a bound
        // `Value::Float` parameter both get here, so the guard belongs on the
        // vector rather than on the parser.
        return Err(Error::Plan(format!(
            "query vector component {i} is not finite ({bad}); \
             `{path}` cannot be searched with it"
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
    analyze: bool,
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
                let bm = eval_expr(unit, p, vis, &acc, stats, analyze, ux)?;
                acc.and_inplace(&bm);
            }
            acc
        }
        Expr::Or(parts) => {
            let mut acc = Bitmap::new(n);
            for p in parts {
                acc.or_inplace(&eval_expr(unit, p, vis, candidates, stats, analyze, ux)?);
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
            let t = eval_expr(unit, inner, vis, &all, stats, analyze, ux)?;
            let mut acc = eval_defined(unit, inner, vis, &all, stats, analyze, ux)?;
            acc.andnot_inplace(&t);
            acc
        }
        Expr::Hops { via, .. } => {
            return Err(Error::Plan(format!(
                "a walk over `{via}` reached a unit unresolved; only the coordinator can walk"
            )))
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
            // The catalog declares the index -- `Db::run_select` refused the
            // statement otherwise -- but a unit sealed before it was declared
            // holds no region for it, and the index does not cover those
            // documents until compaction rewrites them (CREATE INDEX writes
            // nothing into a sealed segment; the missing region is a
            // compaction trigger). No rows from this unit, and the plan says
            // so.
            let no_region = |ux: &mut UnitExplain| {
                ux.access_paths.push(format!(
                    "text_match({path}) [no index region in this unit: sealed before the index]"
                ));
                Bitmap::new(unit.num_docs())
            };
            let Some(handle) = unit.text_handle(path)? else { return Ok(no_region(ux)) };
            let Some(src) = handle.source(path) else { return Ok(no_region(ux)) };
            let an = unit.analyzer(path)?.unwrap_or(Analyzer::Standard);
            let tq = TextQuery::parse(query, an)?;
            let empty = GlobalStats::default();
            let st = stats.get(path).unwrap_or(&empty);
            let c = scorer::compile(&tq, &src, vis, st, Bm25Params::default())?;
            let label = format!("text_match({path}, …) [filter]");
            ux.access_paths.push(label.clone());
            // The flag used to stop here, which made this — the commonest
            // shape a wide prefix takes — the one path that truncated with no
            // signal anywhere. The coordinator's verdict now reaches
            // `QueryResult` whatever the query shape, so what this adds is the
            // per-UNIT line: it is the only thing that can report the fallback
            // arm of `scorer::build`, where each unit expands the prefix
            // against its own dictionary and cuts it at its own place.
            let prefix_truncated = c.prefix_truncated;
            let bm = scorer::evaluate_to_bitmap(c, n);
            // Gated: the whole `UnitExplain` is dropped unless `analyze`, and
            // this line costs a `popcount` over the unit bitmap. Paying that
            // on every unit of every query for a plan nobody asked for is the
            // kind of cost that only ever shows up in someone else's p99.
            if analyze {
                ux.text.push(TextExplain {
                    source: label,
                    // A filter contributes no rank, so it has no scored terms
                    // to list; what it has is survivors — which is why it also
                    // carries its own strategy, so the renderer does not
                    // credit it with machinery it never entered.
                    strategy: TextStrategy::Filter,
                    terms: Vec::new(),
                    matched: bm.popcount(),
                    prefix_truncated,
                    stats_exact: st.exact,
                });
            }
            bm
        }
        Expr::VectorDistance { path, op, query, cmp, threshold } => {
            // A **must** with a distance in it: a filter, contributing no
            // rank, and exact -- see `VectorStore::within`. The threshold is
            // compared with the PRESENTED distance, the number the `distance`
            // column shows for the same operator, so `< 0.2` here and
            // `distance < 0.2` on the ranked path select the same rows. For
            // cosine that is after the query is normalised, exactly as the
            // ranked path prepares it, so a stored vector and any positive
            // scaling of it are both at 0.
            let label = format!(
                "{path} {} [{}] {} {threshold} [filter]",
                op.symbol(),
                query.len(),
                cmp.name()
            );
            ux.access_paths.push(label.clone());
            match unit.vector_handle(path)? {
                None => Bitmap::new(n),
                Some(vs) => {
                    let q = prepare_query(vs.metric, query);
                    let metric = vs.metric;
                    let (bm, report) = vs.within(&q, candidates, n, |d| {
                        let shown = distance::present(metric, d) as f64;
                        match cmp {
                            CmpOp::Eq => shown == *threshold,
                            CmpOp::Ne => shown != *threshold,
                            CmpOp::Lt => shown < *threshold,
                            CmpOp::Le => shown <= *threshold,
                            CmpOp::Gt => shown > *threshold,
                            CmpOp::Ge => shown >= *threshold,
                            _ => false,
                        }
                    });
                    if analyze {
                        ux.vector.push((label, report));
                    }
                    bm
                }
            }
        }
    })
}

/// Ordinals where `e` is *defined* (not NULL), for three-valued negation.
///
/// The De Morgan shape matters: `NOT (a AND b)` is defined where either
/// conjunct is defined and false, or both are defined — so the safe, and
/// standard, rule is that a compound is defined where all of its parts are.
///
/// `vis`, `stats`, `analyze` and `ux` are threaded through unchanged today
/// because the
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
    analyze: bool,
    ux: &mut UnitExplain,
) -> Result<Bitmap> {
    let n = unit.num_docs();
    Ok(match e {
        Expr::True => Bitmap::all(n),
        Expr::And(parts) | Expr::Or(parts) => {
            let mut acc = Bitmap::all(n);
            for p in parts {
                acc.and_inplace(&eval_defined(unit, p, vis, candidates, stats, analyze, ux)?);
            }
            acc
        }
        Expr::Not(inner) => eval_defined(unit, inner, vis, candidates, stats, analyze, ux)?,
        Expr::Compare { path, op, lit } => unit.comparable(path, *op, lit, candidates)?,
        Expr::Hops { via, .. } => {
            return Err(Error::Plan(format!(
                "a walk over `{via}` reached a unit unresolved; only the coordinator can walk"
            )))
        }
        // A text match is two-valued: a document either matches or it does not.
        Expr::TextMatch { .. } => Bitmap::all(n),
        // A distance is defined where the document has a vector; elsewhere the
        // predicate is NULL, so `NOT (d < t)` does not select a document that
        // has no distance to be outside the threshold with.
        Expr::VectorDistance { path, .. } => match unit.vector_handle(path)? {
            Some(vs) => {
                let mut bm = vs.present_docs(n);
                bm.and_inplace(candidates);
                bm
            }
            None => Bitmap::new(n),
        },
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
        // A full-precision distance per survivor: the most expensive leaf,
        // so it sees only what every other conjunct left.
        Expr::VectorDistance { .. } => 20,
        Expr::And(v) | Expr::Or(v) => v.iter().map(|x| predicate_cost(unit, x)).max().unwrap_or(1),
        Expr::Not(x) => predicate_cost(unit, x) + 1,
        // Bound to an `IN` before any unit sees it; the executor refuses one
        // that was not, so the cost is never read.
        Expr::True | Expr::Hops { .. } => 0,
    }
}

/// A structured predicate over `candidates` of one unit, for a walk's edge
/// filter: the same evaluator the executor runs, with no statistics, since
/// `walk::check_edge_filter` admits no leaf that needs them.
pub(crate) fn eval_structured(
    unit: &Searchable<'_>,
    e: &Expr,
    vis: &Bitmap,
    candidates: &Bitmap,
    ux: &mut UnitExplain,
) -> Result<Bitmap> {
    eval_expr(unit, e, vis, candidates, &BTreeMap::new(), false, ux)
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

/// `input` rather than the two fields it is read for: this gained `vis` — the
/// unit's visibility, which the text arm needs so a prefix it has to expand for
/// itself is masked by the same predicate the coordinator uses — and taking
/// `select` and `stats` from the struct that already carries both keeps the
/// signature at a size a reader can hold.
#[allow(clippy::too_many_arguments)]
fn run_source(
    unit: &Searchable<'_>,
    sp: &SourcePlan,
    vis: &Bitmap,
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
            let c = scorer::compile(query, &src, vis, st, Bm25Params::default())?;
            let hits = match c.scorer {
                // A memtable's ordinals are push order, so it hands the
                // collector its keys; a sealed segment's are key order
                // already. See `collect_top_k`.
                Some(s) => match unit {
                    Searchable::Mem(m) => {
                        let key_of = |ord: u32| m.docs[ord as usize].sort_key.as_str();
                        scorer::collect_top_k(
                            s,
                            filter,
                            c.excluded.as_ref(),
                            k_prime,
                            Some(&key_of),
                        )
                    }
                    Searchable::Seg(_) => {
                        scorer::collect_top_k(s, filter, c.excluded.as_ref(), k_prime, None)
                    }
                },
                None => Vec::new(),
            };
            ux.text.push(TextExplain {
                source: name.clone(),
                strategy: TextStrategy::Wand,
                terms: terms.clone(),
                matched: hits.len(),
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
            opts.max_visits = sel.with.max_visits;
            let (hits, mut report) = vs.search(query, k_prime, filter, &opts);
            if sel.with.exact {
                report.strategy = Some(Strategy::Exact);
            }
            ux.vector.push((name.clone(), report));
            Ok(hits)
        }
        // Scored by the coordinator's walk; a unit has nothing to add.
        SourcePlan::Hops { .. } => Ok(Vec::new()),
    }
}

/// A non-ranked query: filter, then order by key or by explicit fields.
///
/// The coordinator's half. Each shard retains its own best `offset + k` rows
/// through `ShardService::scan` and hands back their sort values and keys,
/// with the document only where placing the row needed it; the rows are
/// merged here under the same retention rule, and the documents of the rows
/// that make the page are fetched from their shards afterwards, one call per
/// shard. So a `LIMIT 5` over three shards still decodes five documents.
fn scan(
    input: &ExecInput<'_>,
    prefix: &Option<String>,
    k: usize,
    ex: &mut Explain,
) -> Result<(Vec<Row>, Vec<String>)> {
    let sel = input.select;
    let partial = sel.with.partial_results;
    // Rows past `offset + k` are thrown away at the end, so they are never
    // held: `SELECT * FROM docs LIMIT 1` used to decode and buffer every
    // matching document in the collection before taking one of them, which
    // is the out-of-memory-from-one-statement failure the ranked path's
    // candidate cap exists to prevent, reached by the other door.
    let keep = sel.offset.saturating_add(k);
    let fields: Vec<(String, bool)> = match &sel.order {
        Some(OrderBy::Fields(f)) => f.clone(),
        _ => Vec::new(),
    };
    // A key-ordered scan resumes on the primary key. A full cursor token
    // from a ranked query still works: its last field is that key.
    let after: Option<String> = sel.cursor.as_ref().map(|c| decode_cursor(c).2);
    let mut retained = Retained::new(keep, fields.iter().map(|(_, asc)| *asc).collect());
    let mut missing: Vec<String> = Vec::new();
    let mut manifests: BTreeMap<usize, u64> = BTreeMap::new();
    for si in input.unreachable {
        ex.shards.push(ShardExplain { index: *si, timed_out: true, ..Default::default() });
        missing.push(shard_name(*si));
    }
    for shard in input.shards {
        let si = shard.index();
        if input.unreachable.contains(&si) {
            continue;
        }
        if let Some(p) = prefix {
            if !shard.may_hold(p) {
                ex.shards.push(ShardExplain {
                    index: si,
                    pruned: true,
                    prune_reason: Some("out of key range".into()),
                    manifest_version: shard.manifest_version(),
                    ..Default::default()
                });
                continue;
            }
        }
        if let Some(ms) = deadline::passed() {
            ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
            missing.push(shard_name(si));
            if partial {
                continue;
            }
            return Err(shard_deadline(si, ms));
        }
        let req = ScanRequest {
            coll: input.coll,
            select: sel,
            ts: input.ts,
            prefix: prefix.as_deref(),
            stats: input.stats,
            analyze: input.analyze,
            keep,
            after: after.as_deref(),
            fields: &fields,
            statement: &input.statement,
            params: input.params,
            frontiers: input.frontiers,
            facet: input.facet.as_deref(),
        };
        match shard.scan(&req) {
            Ok(a) => {
                manifests.insert(si, a.explain.manifest_version);
                if a.timed_out {
                    missing.push(shard_name(si));
                }
                ex.shards.push(a.explain);
                for h in a.hits {
                    let rank = Rank::new(h.sort, h.key, &retained.asc);
                    let payload = match h.doc {
                        Some(d) => Payload::Doc(d),
                        None => Payload::Deferred(si, h.handle.0, h.handle.1),
                    };
                    retained.insert(rank, payload, h.parent);
                }
            }
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                ex.shards.push(ShardExplain { index: si, timed_out: true, ..Default::default() });
                missing.push(shard_name(si));
            }
            Err(e) => return Err(e),
        }
    }
    ex.shards.sort_by_key(|s| s.index);
    if let Some(parent_path) = &sel.collapse {
        // Every survivor is seen, so unlike the ranked path there is no
        // candidate depth to widen: the retained set is the best row of each
        // of the `offset + k` best parents, exactly.
        ex.collapse = Some((parent_path.clone(), 1));
    }
    // The page, then the documents its deferred rows need: one call per
    // shard, so a transport pays one round trip per shard and not per row.
    let page: Vec<(Rank, Payload)> =
        retained.into_sorted().into_iter().skip(sel.offset).take(k).collect();
    let mut wanted: BTreeMap<usize, Vec<(usize, u32)>> = BTreeMap::new();
    for (_, p) in &page {
        if let Payload::Deferred(si, ui, ord) = p {
            wanted.entry(*si).or_default().push((*ui, *ord));
        }
    }
    let mut fetched: BTreeMap<(usize, usize, u32), Value> = BTreeMap::new();
    for (si, handles) in wanted {
        let Some(shard) = input.shards.iter().find(|s| s.index() == si) else { continue };
        let version = manifests.get(&si).copied().unwrap_or(0);
        match shard.documents(version, input.ts, &handles) {
            Ok(docs) => {
                for (h, d) in handles.iter().zip(docs) {
                    fetched.insert((si, h.0, h.1), d);
                }
            }
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                missing.push(shard_name(si));
            }
            Err(e) => return Err(e),
        }
    }
    let mut rows: Vec<Row> = Vec::new();
    for (rank, payload) in page {
        let doc = match payload {
            Payload::Doc(doc) => doc,
            Payload::Deferred(si, ui, ord) => match fetched.remove(&(si, ui, ord)) {
                Some(d) => d,
                // Its shard stopped answering between the scan and the
                // fetch, and is in `missing`.
                None => continue,
            },
        };
        rows.push(Row { key: rank.key, doc, score: None, distance: None });
    }
    ex.fetched_payloads = rows.len();
    missing.sort();
    missing.dedup();
    Ok((rows, missing))
}

/// The shard's half of an unranked scan: its best `keep` rows over every
/// unit of its snapshot, under the same retention rule the coordinator
/// applies to the merge. A row is decoded here only when placing it needs
/// the document; otherwise it travels as a handle.
pub(crate) fn scan_on(shard: &Shard, si: usize, req: &ScanRequest<'_>) -> Result<ShardScan> {
    let sel = req.select;
    if sel.aggregates() {
        return aggregate_on(shard, si, req);
    }
    let needs_doc = !req.fields.is_empty() || sel.collapse.is_some();
    let mut sx =
        ShardExplain { index: si, manifest_version: shard.manifest_version, ..Default::default() };
    let t0 = Instant::now();
    let snap = shard.snapshot_at(req.ts);
    let units = shard.sources(&snap);
    let mut retained = Retained::new(req.keep, req.fields.iter().map(|(_, asc)| *asc).collect());
    let mut timed_out = false;
    for (ui, unit) in units.iter().enumerate() {
        let n = unit.num_docs();
        if n == 0 {
            continue;
        }
        let ut = Instant::now();
        let mut ux = UnitExplain { label: unit.label(), docs: n, ..Default::default() };
        let io0 = unit.io_counters();
        let vis = unit.visibility(req.ts);
        ux.visible = vis.popcount();
        let mut filter = match req.prefix {
            Some(p) => unit.key_prefix(p),
            None => Bitmap::all(n),
        };
        filter.and_inplace(&vis);
        if let Some(e) = &sel.predicate {
            let bm = eval_expr(unit, e, &vis, &filter, req.stats, req.analyze, &mut ux)?;
            filter.and_inplace(&bm);
        }
        ux.survivors = filter.popcount();
        ux.selectivity = ux.survivors as f64 / n as f64;
        for ord in filter.iter() {
            if deadline::expired() {
                break;
            }
            let key = unit.key(ord).unwrap_or("").to_string();
            if req.after.is_some_and(|a| key.as_str() <= a) {
                continue;
            }
            if needs_doc {
                let doc = unit.document(ord)?;
                let vals =
                    req.fields.iter().map(|(p, _)| doc.path(p).cloned().unwrap_or(Value::Null));
                let rank = Rank::new(vals.collect(), key, &retained.asc);
                // `COLLAPSE BY` applies to unranked queries too. Skipping
                // it would silently return `k` children of one parent for
                // a statement that asked for `k` distinct parents. A row
                // whose parent path is absent or NULL belongs to no group,
                // so it stands alone -- collapsing them together would
                // fold every unrelated document into one, which is the
                // same rule the ranked path applies.
                let parent = sel
                    .collapse
                    .as_ref()
                    .and_then(|p| doc.path(p))
                    .filter(|v| !v.is_null())
                    .map(crate::variant::encode_to_vec);
                retained.insert(rank, Payload::Doc(doc), parent);
            } else {
                let rank = Rank::new(Vec::new(), key, &retained.asc);
                retained.insert(rank, Payload::Deferred(si, ui, ord), None);
            }
        }
        if let (Some((l0, f0)), Some((l1, f1))) = (io0, unit.io_counters()) {
            ux.loads = l1.saturating_sub(l0);
            ux.faults = f1.saturating_sub(f0);
        }
        ux.micros = ut.elapsed().as_micros();
        sx.units.push(ux);
        if let Some(ms) = deadline::passed() {
            if !sel.with.partial_results {
                return Err(shard_deadline(si, ms));
            }
            timed_out = true;
            break;
        }
    }
    // Rows this shard retained before a cut are not withdrawn: they are
    // correct rows, and under `partial_results` the shard is reported
    // missing, which is the contract -- some of its rows may be absent.
    sx.timed_out = timed_out;
    sx.micros = t0.elapsed().as_micros();
    let hits = retained
        .into_sorted_with_parents()
        .into_iter()
        .map(|(rank, payload, parent)| {
            let (doc, handle) = match payload {
                Payload::Doc(d) => (Some(d), (0, 0)),
                Payload::Deferred(_, ui, ord) => (None, (ui, ord)),
            };
            ScanHit { sort: rank.vals, key: rank.key, doc, handle, parent }
        })
        .collect();
    Ok(ShardScan { hits, explain: sx, timed_out })
}

/// The shard's half of a ranked statement: every source evaluated over every
/// unit of its snapshot, merged per source and cut to `k_prime`. Identifiers
/// and raw scores only.
pub(crate) fn candidates_on(
    shard: &Shard,
    si: usize,
    req: &CandidatesRequest<'_>,
) -> Result<ShardCandidates> {
    let sel = req.select;
    let t0 = Instant::now();
    let mut sx =
        ShardExplain { index: si, manifest_version: shard.manifest_version, ..Default::default() };
    let snap = shard.snapshot_at(req.ts);
    let units = shard.sources(&snap);
    // Per-source heaps, merged across every unit of this shard (step 3).
    let mut heaps: Vec<Vec<Candidate>> = vec![Vec::new(); req.sources.len()];
    for unit in &units {
        let ut = Instant::now();
        let n = unit.num_docs();
        if n == 0 {
            continue;
        }
        let mut ux = UnitExplain { label: unit.label(), docs: n, ..Default::default() };
        let io0 = unit.io_counters();

        let vis = unit.visibility(req.ts);
        ux.visible = vis.popcount();
        let mut filter = match req.prefix {
            Some(p) => {
                ux.access_paths.push(format!("partition range `{}`", show_key(p)));
                unit.key_prefix(p)
            }
            None => Bitmap::all(n),
        };
        filter.and_inplace(&vis);
        if let Some(e) = &sel.predicate {
            let bm = eval_expr(unit, e, &vis, &filter, req.stats, req.analyze, &mut ux)?;
            filter.and_inplace(&bm);
        }
        ux.survivors = filter.popcount();
        ux.selectivity = ux.survivors as f64 / n as f64;

        if ux.survivors > 0 {
            for (i, sp) in req.sources.iter().enumerate() {
                let got =
                    run_source(unit, sp, &vis, &filter, req.k_prime, sel, req.stats, &mut ux)?;
                for (ord, raw) in got {
                    if let Some(key) = unit.key(ord) {
                        heaps[i].push(Candidate { key: key.to_string(), raw_score: raw });
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
        if let Some(ms) = deadline::passed() {
            if !sel.with.partial_results {
                return Err(shard_deadline(si, ms));
            }
            // A loop that stops on the deadline returns less than it was
            // asked for, so nothing a timed-out shard produced may reach
            // the merge.
            sx.timed_out = true;
            sx.micros = t0.elapsed().as_micros();
            return Ok(ShardCandidates {
                per_source: vec![Vec::new(); req.sources.len()],
                explain: sx,
                timed_out: true,
            });
        }
    }
    // Merge this shard's per-source heaps and truncate to k'. Identifiers
    // and raw scores only -- no ranks, no documents.
    for (i, sp) in req.sources.iter().enumerate() {
        let dir = sp.direction();
        heaps[i].sort_by(|a, b| cmp_dir(dir, a.raw_score, b.raw_score).then(a.key.cmp(&b.key)));
        heaps[i].dedup_by(|a, b| a.key == b.key);
        heaps[i].truncate(req.k_prime);
    }
    sx.micros = t0.elapsed().as_micros();
    Ok(ShardCandidates { per_source: heaps, explain: sx, timed_out: false })
}

/// The documents behind scan handles, under the manifest they were issued
/// against. A moved manifest means the units are not the units the handles
/// name, and that is `SnapshotGone`, not a different document.
pub(crate) fn documents_on(
    shard: &Shard,
    manifest_version: u64,
    ts: Timestamp,
    handles: &[(usize, u32)],
) -> Result<Vec<Value>> {
    if shard.manifest_version != manifest_version {
        return Err(Error::SnapshotGone(format!(
            "manifest v{manifest_version} moved to v{} between the scan and the fetch",
            shard.manifest_version
        )));
    }
    let snap = shard.snapshot_at(ts);
    let units = shard.sources(&snap);
    handles
        .iter()
        .map(|(ui, ord)| {
            units
                .get(*ui)
                .ok_or_else(|| Error::SnapshotGone(format!("unit {ui} is gone")))?
                .document(*ord)
        })
        .collect()
}

fn shard_name(si: usize) -> String {
    format!("shard {si}")
}

/// The refusal at a shard boundary: which shard, and the budget.
pub(crate) fn shard_deadline(si: usize, ms: u64) -> Error {
    Error::Deadline(format!(
        "shard {si} not finished within {ms} ms; raise it with WITH (deadline_ms = N), lift it \
         with WITH (no_deadline), or use WITH (partial_results) to opt in to incomplete answers"
    ))
}

/// Where a retained row's document is: decoded, because the order or the
/// collapse had to read it, or still in its unit, to be decoded only if the
/// row makes the page.
enum Payload {
    Doc(Value),
    Deferred(usize, usize, u32),
}

/// A row's place in the scan's order: the `ORDER BY` values, then the primary
/// key as the final tie-break, always (§7.2). Carries its directions so that
/// it can be a map key.
#[derive(Clone)]
struct Rank {
    vals: Vec<Value>,
    key: String,
    asc: std::sync::Arc<[bool]>,
}

impl Rank {
    fn new(vals: Vec<Value>, key: String, asc: &std::sync::Arc<[bool]>) -> Rank {
        Rank { vals, key, asc: asc.clone() }
    }
}

impl PartialEq for Rank {
    fn eq(&self, other: &Rank) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for Rank {}
impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Rank) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Rank {
    fn cmp(&self, other: &Rank) -> std::cmp::Ordering {
        for (i, (a, b)) in self.vals.iter().zip(&other.vals).enumerate() {
            let o = compare_total(a, b);
            let o = if self.asc.get(i).copied().unwrap_or(true) { o } else { o.reverse() };
            if o != std::cmp::Ordering::Equal {
                return o;
            }
        }
        self.key.cmp(&other.key)
    }
}

/// The rows a scan is still holding, never more than the page can show.
///
/// Rows arrive in no particular order and the best `cap` of them are kept;
/// a row worse than the worst retained one is dropped on arrival, and a row
/// that displaces one drops the worst. Under `COLLAPSE BY` a parent holds at
/// most one row, its best, so the set is the best row of each of the `cap`
/// best parents: a parent whose best row ranks among those is never lost,
/// because the worst retained row only ever improves, and a parent's earlier,
/// worse row is replaced when its best arrives. That is what the sort-then-
/// collapse-then-page it replaces computed, at the memory of one page.
struct Retained {
    cap: usize,
    asc: std::sync::Arc<[bool]>,
    entries: BTreeMap<Rank, (Payload, Option<Vec<u8>>)>,
    by_parent: BTreeMap<Vec<u8>, Rank>,
}

impl Retained {
    fn new(cap: usize, asc: Vec<bool>) -> Retained {
        Retained { cap, asc: asc.into(), entries: BTreeMap::new(), by_parent: BTreeMap::new() }
    }

    fn insert(&mut self, rank: Rank, payload: Payload, parent: Option<Vec<u8>>) {
        if self.cap == 0 {
            return;
        }
        if let Some(p) = &parent {
            if let Some(cur) = self.by_parent.get(p) {
                if rank < *cur {
                    let cur = cur.clone();
                    self.entries.remove(&cur);
                    self.by_parent.remove(p);
                } else {
                    return;
                }
            }
        }
        if self.entries.len() >= self.cap {
            if let Some((worst, _)) = self.entries.iter().next_back() {
                if rank >= *worst {
                    return;
                }
            }
        }
        if let Some(p) = &parent {
            self.by_parent.insert(p.clone(), rank.clone());
        }
        self.entries.insert(rank, (payload, parent));
        if self.entries.len() > self.cap {
            if let Some((_, (_, Some(p)))) = self.entries.pop_last() {
                self.by_parent.remove(&p);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn into_sorted(self) -> Vec<(Rank, Payload)> {
        self.entries.into_iter().map(|(r, (p, _))| (r, p)).collect()
    }

    fn into_sorted_with_parents(self) -> Vec<(Rank, Payload, Option<Vec<u8>>)> {
        self.entries.into_iter().map(|(r, (p, parent))| (r, p, parent)).collect()
    }
}

/// Whether a shard's key range can hold anything under `prefix`.
pub(crate) fn shard_may_hold(shard: &Shard, prefix: &str) -> bool {
    range_may_hold(shard.key_range.as_ref(), prefix)
}

/// Whether a key range can hold anything under `prefix`; `None` is a shard
/// with no bounds, which holds everything.
pub(crate) fn range_may_hold(r: Option<&(Option<String>, Option<String>)>, prefix: &str) -> bool {
    if let Some(r) = r {
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

/// The SELECT a DELETE runs to find its keys: every row the predicate
/// selects, no order, no page. One function so that a coordinator and a
/// holder on another node build the same statement from the same text.
pub fn select_for_delete(collection: &str, predicate: &Expr) -> Select {
    Select {
        projections: vec![Projection::All],
        collection: collection.to_string(),
        predicate: Some(predicate.clone()),
        order: None,
        limit: Some(usize::MAX),
        offset: 0,
        cursor: None,
        collapse: None,
        group_by: None,
        facets: Vec::new(),
        facet_top: 0,
        with: WithOpts::default(),
    }
}

/// The aggregate one facet is: the path's value and a count over every row
/// the statement's predicate admits, the most first and equal counts by
/// value, the top `top` of them. What a holder builds from the statement
/// it parsed when a scan request names a facet, and what the coordinator
/// merges as it merges any aggregate.
pub fn facet_select(base: &Select, path: &str, top: usize) -> Select {
    Select {
        projections: vec![
            Projection::Path { path: path.to_string(), alias: Some("value".into()) },
            Projection::Aggregate { func: AggFunc::Count, path: None, alias: Some("n".into()) },
        ],
        collection: base.collection.clone(),
        predicate: base.predicate.clone(),
        order: Some(OrderBy::Fields(vec![("n".into(), false), ("value".into(), true)])),
        limit: Some(top.max(1)),
        offset: 0,
        cursor: None,
        collapse: None,
        group_by: Some(path.to_string()),
        facets: Vec::new(),
        facet_top: 0,
        with: base.with.clone(),
    }
}

/// A payload by primary key, from the shards whose range can hold it. A
/// shard that stops answering here is treated like one that stopped
/// anywhere else: refused, or under `partial_results` reported and skipped.
fn fetch(
    shards: &[Box<dyn ShardService + '_>],
    key: &str,
    ts: Timestamp,
    partial: bool,
    missing: &mut Vec<String>,
) -> Result<Option<Value>> {
    for s in shards {
        let si = s.index();
        if missing.contains(&shard_name(si)) || !s.may_hold(key) {
            continue;
        }
        match s.get(key, ts) {
            Ok(Some(d)) => return Ok(Some(d)),
            Ok(None) => {}
            Err(Error::Deadline(e)) => {
                if !partial {
                    return Err(Error::Deadline(e));
                }
                missing.push(shard_name(si));
            }
            Err(e) => return Err(e),
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

    /// The flags `required_prefixes` records for one occurrence of a prefix
    /// leaf: `spelled_negated` is the `-` inside the `text_match` string,
    /// `sql_negated` is a `NOT` the SQL wrapped the whole call in.
    fn used(spelled_negated: bool, sql_negated: bool) -> PrefixUse {
        let effective = spelled_negated != sql_negated;
        PrefixUse {
            positive: !spelled_negated,
            negated: spelled_negated,
            loses_rows: !effective,
            keeps_rows: effective,
        }
    }

    /// One cut leaf on `body`, used in the polarities the flags describe.
    fn cut(used: PrefixUse) -> BTreeMap<String, GlobalStats> {
        let e = crate::text::scorer::Expansion {
            terms: vec!["a1".to_string(), "a2".to_string()],
            truncated: true,
            used,
        };
        let g = GlobalStats {
            expansions: BTreeMap::from([("a".to_string(), e)]),
            ..Default::default()
        };
        BTreeMap::from([("body".to_string(), g)])
    }

    /// A collection with one full-text path and one plain one, so a statement
    /// can name both an indexed and an unindexed path.
    fn text_coll() -> Collection {
        let mut c = Collection::new("notes", "id", None);
        c.indexes.push(crate::catalog::IndexDef::new(
            "notes_body",
            "body",
            crate::catalog::IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::default(),
        ));
        c
    }

    /// A collection with one two-dimensional cosine vector index, and the
    /// full-text index a hybrid ORDER BY needs alongside it.
    fn vector_coll() -> Collection {
        let mut c = text_coll();
        c.indexes.push(crate::catalog::IndexDef::new(
            "notes_emb",
            "emb",
            crate::catalog::IndexKind::Vector { dims: 2, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        c
    }

    /// `VectorStore::push` refuses a non-finite stored component; the query
    /// side used to accept one and answer every distance as NaN.
    #[test]
    fn a_non_finite_query_component_is_refused_the_way_a_stored_one_is() {
        let coll = vector_coll();
        // `1e40` is finite as a JSON number and infinite as an f32, so the
        // literal arrives already overflowed.
        for sql in [
            "SELECT id FROM notes ORDER BY emb <=> [1e40, 0.0] LIMIT 3",
            "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'a'), \
              emb <=> [0.0, 1e40]) LIMIT 3",
        ] {
            let Err(e) = plan_sources(&coll, &select(sql), 3) else {
                panic!("planned a query with an infinite component: {sql}");
            };
            assert!(e.to_string().contains("is not finite"), "{sql}: {e}");
        }
    }

    fn select(sql: &str) -> Select {
        match crate::sql::parser::parse(sql, &[]).unwrap() {
            Statement::Select(s) => *s,
            _ => panic!("not a SELECT: {sql}"),
        }
    }

    #[test]
    fn prefixes_named_here_are_paths_required_terms_already_created() {
        // `Db::run_select` relies on this and cannot check it: it merges each
        // resolved expansion into the `want` map `required_terms` built, and
        // then attaches the expansion to the `GlobalStats` the gather produced
        // for that path. A path that reaches `required_prefixes` but not
        // `required_terms` has no entry to attach to, so every unit falls onto
        // `scorer::build`'s no-coordinator arm and expands the prefix locally
        // — which is the per-unit expansion the coordinator exists to replace,
        // and it fails by ranking differently rather than by erroring.
        //
        // It holds because both functions walk the same two sites behind the
        // same `fulltext_index` guard and the same `TextQuery::parse`, and
        // `required_terms` creates its entry before it has any term to put in
        // it. `run_select` used to defend itself with a redundant
        // `want.entry(path).or_default()` instead, which pinned nothing: a
        // write that is a no-op while the invariant holds is still a no-op
        // once it breaks. This is the pin in its place.
        let coll = text_coll();
        // A pure negation on the only indexed path: nothing is scored, so the
        // term list is empty and the entry has to exist anyway. `title` is
        // unindexed, and neither function may name it.
        for sql in [
            "SELECT id FROM notes WHERE text_match(body, '-alph*')",
            "SELECT id FROM notes WHERE NOT text_match(body, 'alph*')",
            "SELECT id FROM notes WHERE text_match(body, '-alph*') AND text_match(title, 'b*')",
            "SELECT id FROM notes ORDER BY hybrid(text_match(body, 'alph*'), \
              emb <=> [1.0, 0.0]) LIMIT 3",
        ] {
            let sel = select(sql);
            let terms = required_terms(&coll, &sel);
            let prefixes = required_prefixes(&coll, &sel);
            assert!(!prefixes.is_empty(), "the statement has to expand something: {sql}");
            for path in prefixes.keys() {
                assert!(
                    terms.contains_key(path),
                    "`{path}` is expanded but has no `want` entry to attach it to: {sql}"
                );
            }
            assert!(!terms.contains_key("title"), "an unindexed path must not be named: {sql}");
        }
    }

    #[test]
    fn a_cut_leaf_is_reported_in_every_polarity_the_statement_spelled_it_in() {
        // The message is the only thing a caller has to find the clause that
        // was cut, so the spelling in it has to be a spelling they wrote.
        let one = |st| {
            let mut v = truncated_prefixes(&st);
            assert_eq!(v.len(), 1, "{v:?}");
            v.pop().unwrap()
        };

        let m = one(cut(used(false, false)));
        assert!(m.contains("text_match(body, 'a*')"), "{m}");
        assert!(m.contains("documents are missing from this answer"), "{m}");

        // A cut EXCLUSION set keeps rows rather than losing them, so both
        // halves of this line differ from the one above.
        let m = one(cut(used(true, false)));
        assert!(m.contains("text_match(body, '-a*')"), "{m}");
        assert!(!m.contains("'a*'"), "the sign is not optional: {m}");
        assert!(m.contains("should have excluded are still in this answer"), "{m}");

        // And the mixed statement — `a*` in one clause, `-a*` in another —
        // which is ONE expansion and therefore one line. It used to be printed
        // with the positive spelling alone beside the combined consequence, so
        // it told the reader that `'a*'` had kept rows it should have
        // excluded: a claim about a clause that does not exist, while the
        // clause that really did keep them went unnamed.
        let mut both = used(false, false);
        both.negated = true;
        both.keeps_rows = true;
        let m = one(cut(both));
        assert!(m.contains("text_match(body, 'a*' and '-a*')"), "both spellings: {m}");
        assert!(
            m.contains(
                "documents are missing from this answer AND documents it should have \
                        excluded are still in it"
            ),
            "{m}"
        );
    }

    /// SQL's own `NOT` wraps the whole `text_match` call, so the sign inside
    /// the query string is only half of the leaf's polarity. The report used to
    /// read the consequence off the spelling alone, which made `NOT
    /// text_match(body, 'a*')` — a statement that KEPT rows it had asked to
    /// exclude — say that documents were missing, and a caller acting on that
    /// widens the prefix and keeps more.
    #[test]
    fn a_cut_leaf_under_a_sql_not_reports_the_consequence_the_sql_gave_it() {
        let one = |st| {
            let mut v = truncated_prefixes(&st);
            assert_eq!(v.len(), 1, "{v:?}");
            v.pop().unwrap()
        };

        // `NOT text_match(body, 'a*')`: spelled positively, behaves as an
        // exclusion. Both halves have to come from different flags — the
        // caller's own spelling, and the consequence the SQL gave it.
        let m = one(cut(used(false, true)));
        assert!(m.contains("text_match(body, 'a*')"), "the caller's spelling: {m}");
        assert!(!m.contains("'-a*'"), "a spelling they never wrote: {m}");
        assert!(m.contains("should have excluded are still in this answer"), "{m}");

        // `NOT text_match(body, '-a*')` is a double negation, so it matches
        // again and a cut loses rows. Flipping the sign rather than OR-ing it
        // is what makes this line right.
        let m = one(cut(used(true, true)));
        assert!(m.contains("text_match(body, '-a*')"), "the caller's spelling: {m}");
        assert!(m.contains("documents are missing from this answer"), "{m}");
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
        let rows = vec![
            doc("c1", r#"{"parent_id": "p1"}"#),
            doc("c2", r#"{"parent_id": "p1"}"#),
            doc("c3", r#"{"parent_id": "p2"}"#),
            doc("c4", r#"{"parent_id": "p1"}"#),
            doc("c5", r#"{"parent_id": "p3"}"#),
        ];
        assert_eq!(retain_keys(rows, 10, "parent_id"), vec!["c1", "c3", "c5"]);
    }

    /// Documents with no parent are not one giant group; collapsing them
    /// together would delete unrelated rows from the answer.
    #[test]
    fn a_missing_or_null_parent_does_not_collapse_unrelated_rows_together() {
        let rows = vec![
            doc("a", r#"{"other": 1}"#),
            doc("b", r#"{"parent_id": null}"#),
            doc("c", r#"{"other": 2}"#),
            doc("d", r#"{"parent_id": "p"}"#),
            doc("e", r#"{"parent_id": "p"}"#),
        ];
        assert_eq!(retain_keys(rows, 10, "parent_id"), vec!["a", "b", "c", "d"]);
    }

    /// Feed `rows` to a `Retained` of capacity `cap` in the order given, key
    /// order as the rank, and return the keys it kept, in order.
    fn retain_keys(rows: Vec<(String, Value)>, cap: usize, parent: &str) -> Vec<String> {
        let mut r = Retained::new(cap, Vec::new());
        for (key, doc) in rows {
            let p = doc.path(parent).filter(|v| !v.is_null()).map(crate::variant::encode_to_vec);
            r.insert(Rank::new(Vec::new(), key, &r.asc.clone()), Payload::Doc(doc), p);
        }
        r.into_sorted().into_iter().map(|(rank, _)| rank.key).collect()
    }

    /// The retained set is never larger than the page, whatever arrives and in
    /// whatever order, and what it ends up holding is exactly what sorting
    /// everything, collapsing and taking the page would have held. 40 parents
    /// over 4000 rows in a scrambled order, with a fifth of the rows belonging
    /// to no parent, and every prefix of the arrival checked for the bound --
    /// a collector that only trimmed at the end would pass the final
    /// comparison and fail every intermediate one.
    #[test]
    fn a_scan_retains_no_more_rows_than_the_page_and_the_same_rows_as_a_full_sort() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut rows: Vec<(String, Value)> = (0..4000)
            .map(|i| {
                let parent = match next() % 5 {
                    0 => "null".to_string(),
                    _ => format!("\"p{:02}\"", next() % 40),
                };
                doc(&format!("k{:04}", i), &format!(r#"{{"parent_id": {parent}}}"#))
            })
            .collect();
        for i in (1..rows.len()).rev() {
            rows.swap(i, (next() % (i as u64 + 1)) as usize);
        }
        let expected: Vec<String> = {
            let mut all = rows.clone();
            all.sort_by(|a, b| a.0.cmp(&b.0));
            let mut seen: Vec<Vec<u8>> = Vec::new();
            all.retain(|(_, d)| match d.path("parent_id") {
                Some(p) if !p.is_null() => {
                    let b = crate::variant::encode_to_vec(p);
                    if seen.contains(&b) {
                        false
                    } else {
                        seen.push(b);
                        true
                    }
                }
                _ => true,
            });
            all.into_iter().take(7).map(|(k, _)| k).collect()
        };
        let mut r = Retained::new(7, Vec::new());
        for (key, d) in rows {
            let p = d.path("parent_id").filter(|v| !v.is_null()).map(crate::variant::encode_to_vec);
            r.insert(Rank::new(Vec::new(), key, &r.asc.clone()), Payload::Doc(d), p);
            assert!(r.len() <= 7, "the collector held {} rows for a page of 7", r.len());
        }
        let got: Vec<String> = r.into_sorted().into_iter().map(|(rank, _)| rank.key).collect();
        assert_eq!(got, expected);
    }
}
