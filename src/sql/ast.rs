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
    CreateLifecyclePolicy(LifecycleDecl),
    DropLifecyclePolicy {
        name: String,
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
}

#[derive(Debug, Clone)]
pub enum IndexSpec {
    FullText { analyzer: String },
    Vector { dims: usize, metric: Metric },
    Secondary,
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
    Text { path: String, query: String },
    Vector { path: String, op: DistOp, query: Vec<f32> },
}

impl HybridSource {
    pub fn name(&self) -> String {
        match self {
            HybridSource::Text { path, .. } => format!("text({path})"),
            HybridSource::Vector { path, .. } => format!("vector({path})"),
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
}

#[derive(Debug, Clone, Default)]
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
    /// Query deadline in milliseconds.
    pub deadline_ms: Option<u64>,
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
    pub with: WithOpts,
}
