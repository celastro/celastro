//! Parsed statements.

use crate::catalog::Metric;
use crate::column::CmpOp;
use crate::lifecycle::Rule;
use crate::residency::Tier;
use crate::value::{Value, ValueType};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateCollection(CreateCollection),
    CreateIndex(CreateIndex),
    Insert(Insert),
    Delete(DeleteStmt),
    Select(Box<Select>),
    Explain {
        analyze: bool,
        inner: Box<Statement>,
    },
    /// Operational statements. Flush and compaction are scheduled and visible,
    /// never an invisible background process (§12.1), so they are addressable.
    Flush {
        collection: String,
    },
    Compact {
        collection: String,
    },
    ShowSegments {
        collection: String,
    },
    ShowCatalog {
        collection: Option<String>,
    },
    /// `ALTER INDEX <name> ON <collection> SET TIER <tier>` — move one index
    /// between tiers by hand, outside any policy.
    AlterIndexTier {
        collection: String,
        index: String,
        tier: Tier,
    },
    /// `ALTER COLLECTION <name> SET (prefix_expansion = N)` — set how many
    /// dictionary terms a prefix on the collection expands to.
    AlterCollection {
        collection: String,
        prefix_expansion: Option<usize>,
        /// `SET (nodes_of = 'papers')`: point an existing collection at the
        /// node collection its edges join, so that a collection loaded before
        /// it had an adjacency index can get one.
        nodes_of: Option<String>,
    },
    CreateLifecyclePolicy(LifecycleDecl),
    DropLifecyclePolicy {
        name: String,
    },
    /// `BACKUP TO '<path or s3://bucket/prefix>'` -- the shards this node
    /// holds, pinned at one instant, copied incrementally.
    Backup {
        to: String,
    },
    /// `RESTORE FROM '<path or s3://bucket/prefix>' [NODE '<address>'] [AS OF
    /// <ts>]` -- into an empty database, this node's newest complete backup
    /// (or the named node's), or the one at `ts`.
    Restore {
        from: String,
        node: Option<String>,
        as_of: Option<u64>,
    },
    /// `DROP COLLECTION <name>` — the collection, its files, its objects in
    /// the store, and everything recorded against it. Irreversible.
    DropCollection {
        name: String,
    },
    /// `DROP INDEX <name> ON <collection>` — withdraw the declaration; the
    /// regions already sealed stay until compaction rewrites them.
    DropIndex {
        collection: String,
        index: String,
    },
    /// `LOCAL <statement>` — run it on this node only, without forwarding
    /// it to the other nodes holding the collection. What a coordinator
    /// sends the other holders, and what an operator runs to repair a node
    /// a forwarded statement did not reach.
    Local(Box<Statement>),
    /// `ATTACH NODE 'tcp://host:port'` — a node this one may place shards
    /// on. `DETACH NODE` refuses while a placement still names it.
    AttachNode {
        url: String,
    },
    DetachNode {
        url: String,
    },
    /// `MOVE SHARD <i> OF <collection> TO 'tcp://host:port'` — the shard's
    /// files go to the node at a pinned instant, the map switches on every
    /// holder, the source drops its copy. Writes to the shard are refused
    /// for the duration, naming the move.
    MoveShard {
        collection: String,
        shard: usize,
        to: String,
    },
    /// `REBALANCE <collection>` — the moves that put shard `i` on the
    /// `i`-th of this node and the attached ones, in attach order, as a
    /// `CREATE COLLECTION` with no nodes named would have.
    Rebalance {
        collection: String,
    },
    /// `PLACE SHARD <i> OF <collection> ON 'tcp://host:port'` — this node's
    /// placement map records the shard on that node; a copy this node holds
    /// of a shard placed elsewhere is dropped. What a move sends every holder
    /// as `LOCAL PLACE SHARD ...`, and what repairs a holder it did not reach.
    PlaceShard {
        collection: String,
        shard: usize,
        node: String,
    },
    /// Evaluate the policies and carry out whatever they call for. Like
    /// compaction, tiering is scheduled and visible rather than an invisible
    /// background process (§12.1).
    RunLifecycle {
        collection: Option<String>,
    },
    /// What is decoded in memory right now, per segment and component.
    ShowResidency {
        collection: Option<String>,
    },
    ShowLifecycle,
    /// Release every component whose tier says it has been idle long enough,
    /// then evict down to the node budget.
    UnloadIdle {
        collection: Option<String>,
    },
    /// Sample production queries, re-execute them exactly, report recall@k
    /// (§12.1).
    MeasureRecall {
        collection: String,
        k: usize,
        samples: usize,
    },
}

#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    pub path: String,
    pub ty: ValueType,
    pub not_null: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct CreateCollection {
    pub name: String,
    pub columns: Vec<DeclaredColumn>,
    pub partition_by: Option<String>,
    /// Boundary keys of the tablet map: `n` split points make `n+1` shards,
    /// range-partitioned on the composite `(partition_key, primary_key)`
    /// (§3.2). Dynamic split and merge are not implemented; this is how a
    /// multi-shard
    /// collection is created today.
    pub splits: Vec<String>,
    /// `WITH (prefix_expansion = N)`: how many dictionary terms a prefix on
    /// the collection expands to, or `None` for the engine's default.
    pub prefix_expansion: Option<usize>,
    /// `WITH (nodes = [...])`: the nodes to place the shards on, round-robin
    /// by shard index. Empty means every attached node and this one.
    pub nodes: Vec<String>,
    /// `WITH (nodes_of = 'papers')`: this is an edge collection whose `src`
    /// and `dst` are primary keys of `papers`.
    pub nodes_of: Option<String>,
    /// `WITH (undirected = true)`: a walk over this collection's edges
    /// follows them in both directions.
    pub undirected: bool,
}

#[derive(Debug, Clone)]
pub enum IndexSpec {
    FullText {
        analyzer: String,
    },
    Vector {
        dims: usize,
        metric: Metric,
    },
    Secondary,
    /// `USING adjacency (src, dst)`: the walk index of an edge collection.
    /// `path` is the column a hop probes, `to` the one it reads.
    Adjacency {
        to: String,
    },
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub collection: String,
    pub path: String,
    pub spec: IndexSpec,
    pub tier: Tier,
}

#[derive(Debug, Clone)]
pub struct LifecycleDecl {
    pub name: String,
    pub collection: String,
    /// Empty means every index in the collection, including ones added later.
    pub indexes: Vec<String>,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub collection: String,
    pub docs: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub collection: String,
    pub predicate: Option<Expr>,
}

/// Structured predicates and must-semantics text matching.
#[derive(Debug, Clone)]
pub enum Expr {
    Compare {
        path: String,
        op: CmpOp,
        lit: Value,
    },
    /// `text_match(body, 'query')` in `WHERE`: a filter, contributing no rank.
    TextMatch {
        path: String,
        query: String,
    },
    /// `embedding <=> $q < 0.2` in `WHERE`: a distance threshold, which is a
    /// filter contributing no rank. The threshold is compared with the
    /// distance as the `distance` column presents it for the same operator,
    /// so the two agree by construction. `cmp` is one of the six orderings.
    VectorDistance {
        path: String,
        op: DistOp,
        query: Vec<f32>,
        cmp: CmpOp,
        threshold: f64,
    },
    /// `id WITHIN k HOPS OF 'x' VIA cites [REVERSE] [WHERE <edge filter>]`:
    /// the primary keys reachable from `x` in one to `k` hops over the edge
    /// collection `via`, the start excluded. A filter contributing no rank,
    /// like `text_match`. The coordinator resolves it to a key set before
    /// the scatter and the shards see it as `id IN (...)`; an executor that
    /// meets it unresolved refuses, since no unit can walk on its own.
    Hops {
        path: String,
        k: usize,
        start: String,
        via: String,
        /// Follow the adjacency index against its declared order.
        reverse: bool,
        /// Structured predicates on the edge collection: none, one for
        /// every hop, or one per hop (`WHERE a THEN WHERE b`), the i-th at
        /// hop i.
        filters: Vec<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    /// Always true. What an empty `WHERE` parses to, so the executor has no
    /// special case.
    True,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistOp {
    /// `<->` L2
    L2,
    /// `<=>` cosine
    Cosine,
    /// `<#>` inner product
    InnerProduct,
}

impl DistOp {
    pub fn metric(self) -> Metric {
        match self {
            DistOp::L2 => Metric::L2,
            DistOp::Cosine => Metric::Cosine,
            DistOp::InnerProduct => Metric::InnerProduct,
        }
    }
    pub fn symbol(self) -> &'static str {
        match self {
            DistOp::L2 => "<->",
            DistOp::Cosine => "<=>",
            DistOp::InnerProduct => "<#>",
        }
    }
}

#[derive(Debug, Clone)]
pub enum HybridSource {
    Text {
        path: String,
        query: String,
    },
    Vector {
        path: String,
        op: DistOp,
        query: Vec<f32>,
    },
    /// `hops(id WITHIN 3 HOPS OF 'x' VIA cites)`: hop distance as a source,
    /// nearer ranking higher. The same clause as the filter, walked the
    /// same way; what differs is that each key keeps the hop it was first
    /// reached at.
    Hops {
        path: String,
        k: usize,
        start: String,
        via: String,
        reverse: bool,
        filters: Vec<Expr>,
    },
}

impl HybridSource {
    pub fn name(&self) -> String {
        match self {
            HybridSource::Text { path, .. } => format!("text({path})"),
            HybridSource::Vector { path, .. } => format!("vector({path})"),
            HybridSource::Hops { via, .. } => format!("hops({via})"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FusionMethod {
    /// Reciprocal rank fusion. Rank-based, so it needs no score calibration and
    /// tolerates IDF drift. The default.
    Rrf,
    /// Weighted linear combination. Viable **only** with normalisation
    /// performed once at the coordinator over the merged candidate set.
    Linear,
}

#[derive(Debug, Clone)]
pub struct HybridSpec {
    pub sources: Vec<HybridSource>,
    pub method: FusionMethod,
    pub weights: Vec<f32>,
    /// Per-source, per-shard candidate depth. Fusion quality depends on it, so
    /// it is exposed rather than hidden (§7.2).
    pub k_prime: Option<usize>,
    /// The `c` in `Σ wᵢ / (c + rankᵢ)`; 60 by convention.
    pub rrf_c: f32,
}

#[derive(Debug, Clone)]
pub enum OrderBy {
    Hybrid(HybridSpec),
    /// `ORDER BY embedding <-> $1` — an ANN access path, not a sort.
    Distance {
        path: String,
        op: DistOp,
        query: Vec<f32>,
    },
    Fields(Vec<(String, bool)>),
}

#[derive(Debug, Clone)]
pub enum Projection {
    All,
    Path {
        path: String,
        alias: Option<String>,
    },
    /// `score` and `distance` pseudo-columns.
    Score,
    Distance,
    /// `count(*)`, `count(path)`, `sum(path)`, `min(path)`, `max(path)`,
    /// `avg(path)`: one value over every row the statement matches, or one
    /// per group under `GROUP BY`. `path` is `None` for `count(*)`.
    Aggregate {
        func: AggFunc,
        path: Option<String>,
        alias: Option<String>,
    },
}

impl Projection {
    /// The name an aggregate's value is keyed by in the row: the alias, or
    /// the call as written (`count(*)`, `sum(n)`).
    pub fn aggregate_name(&self) -> Option<String> {
        match self {
            Projection::Aggregate { func, path, alias } => {
                Some(alias.clone().unwrap_or_else(|| {
                    format!("{}({})", func.name(), path.as_deref().unwrap_or("*"))
                }))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggFunc {
    pub fn name(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Avg => "avg",
        }
    }

    pub fn parse(s: &str) -> Option<AggFunc> {
        match s.to_ascii_lowercase().as_str() {
            "count" => Some(AggFunc::Count),
            "sum" => Some(AggFunc::Sum),
            "min" => Some(AggFunc::Min),
            "max" => Some(AggFunc::Max),
            "avg" => Some(AggFunc::Avg),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WithOpts {
    /// Brute-force vector search over every segment and exact two-phase term
    /// statistics. Far too slow for production; it exists so that "same results
    /// as single-node" is testable at all (§14).
    pub exact: bool,
    /// Exact global term statistics for this query's terms, one extra round
    /// trip (§8.2).
    pub exact_scoring: bool,
    /// A slow or failed shard does not silently produce a partial answer; this
    /// opts in, and the response carries the list of missing units (§8.4).
    pub partial_results: bool,
    pub ef_search: Option<usize>,
    /// `WITH (max_visits = N)`: a ceiling on the nodes a graph traversal may
    /// visit, for a caller that would rather have a short answer. It binds
    /// knowingly: the plan shows the count beside it.
    pub max_visits: Option<usize>,
    /// `WITH (max_frontier = N)`: the most keys one hop of a walk may yield;
    /// the rest are cut, lexicographically, and the answer says so.
    pub max_frontier: Option<usize>,
    /// `WITH (max_fanout = N)`: the most edges one node's expansion follows
    /// in a walk, which is what a hub costs; cut the same way.
    pub max_fanout: Option<usize>,
    /// Query deadline in milliseconds. Zero is exceeded at once.
    pub deadline_ms: Option<u64>,
    /// `WITH (no_deadline)`: run without a budget, whatever the `Db`'s
    /// default. The one way to lift the default from SQL.
    pub no_deadline: bool,
}

#[derive(Debug, Clone)]
pub struct Select {
    pub projections: Vec<Projection>,
    pub collection: String,
    pub predicate: Option<Expr>,
    pub order: Option<OrderBy>,
    pub limit: Option<usize>,
    pub offset: usize,
    /// `search_after` cursor: the last primary key of the previous page.
    pub cursor: Option<String>,
    /// `COLLAPSE BY parent_id`: keep the best-scoring child per parent, with
    /// `k` amplified accordingly (§5.4). The v1 answer to multi-vector
    /// documents.
    pub collapse: Option<String>,
    /// `GROUP BY path`: one row per distinct value at the path, the
    /// aggregates in the list computed over each group's rows.
    pub group_by: Option<String>,
    pub with: WithOpts,
}

impl Select {
    /// Whether the list aggregates: a statement of one row, or one per
    /// group, over every row the predicate admits.
    pub fn aggregates(&self) -> bool {
        self.projections.iter().any(|p| matches!(p, Projection::Aggregate { .. }))
            || self.group_by.is_some()
    }
}
