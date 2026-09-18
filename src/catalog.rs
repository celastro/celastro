//! The logical path catalog, collection definitions and index definitions.
//!
//! Documents are self-describing, but SQL needs types, so the engine maintains
//! a logical path catalog per collection (§2.1): for each observed field path,
//! the set of concrete types seen, presence count, and approximate cardinality.
//! In the distributed build this lives in the control plane (§10) and is
//! aggregated from per-segment statistics; here it lives in the same place,
//! fed by the same per-segment statistics, so the aggregation path is the one
//! that will be reused rather than a single-node shortcut.
//!
//! Two rules keep inference from producing surprises, and both are enforced in
//! [`PathStats::observe`]:
//!
//! * Integers and doubles at the same path are one widened numeric type, not
//!   polymorphism.
//! * JSON has no date type. A path is a `TIMESTAMP` only when declared in DDL
//!   or explicitly cast; undeclared ISO-8601 strings stay strings.

use std::collections::{BTreeMap, BTreeSet};

use crate::codec::*;
use crate::error::{Error, Result};
use crate::value::{Value, ValueType};

const CATALOG_MAGIC: &[u8; 4] = b"CLSC";
/// Bumped when the byte meaning of anything in the catalog changes.
///
/// The tier byte is the reason this exists: inserting a tier into the middle of
/// the ladder renumbers every byte above it, so a catalog written by a build
/// with a different ladder would be read with every tier shifted by one.
/// Refusing to open it is the point — silently promoting an on-disk index to a
/// RAM-resident one on upgrade is exactly the failure a version field prevents.
const CATALOG_VERSION: u8 = 8;
/// The oldest format this build reads. Version 3 differs from 2 only by the
/// per-collection prefix expansion cap, appended after each collection's path
/// statistics, so a 2 is read as a 3 whose every collection is at the default
/// and nothing else moves. Version 4 appends the node list and the placement
/// map after the activity clocks; a 3 is read as a 4 with no nodes and no
/// placement, and `Db::open` derives the placement of every collection from
/// its shard directories. Version 5 appends, after each collection's prefix
/// cap, the node collection its edges point into and whether its walks are
/// undirected, and adds the adjacency index kind; a 4 is read as a 5 whose
/// collections are not edge collections. Version 1 is refused: its tier
/// bytes name different tiers.
const CATALOG_VERSION_OLDEST: u8 = 2;

/// Where one shard of a collection lives: the node that holds it and the
/// key range it owns. The node is its advertised address, or empty for a
/// shard on whichever node holds this catalog — what a single-node database
/// has, and what a database created before it had an address keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tablet {
    pub node: String,
    pub lo: Option<String>,
    pub hi: Option<String>,
}

impl Tablet {
    pub fn owns(&self, key: &str) -> bool {
        self.lo.as_ref().map(|l| key >= l.as_str()).unwrap_or(true)
            && self.hi.as_ref().map(|h| key < h.as_str()).unwrap_or(true)
    }
}

/// How a path behaves across the collection. Drives shredding eligibility and
/// predicate semantics, not storage layout directly — shredding is a physical,
/// per-segment decision (§2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathClass {
    /// One concrete type across effectively all documents. Eligible for
    /// column shredding.
    Stable(ValueType),
    /// Multiple types at the same path. Comparison against a typed literal
    /// evaluates only against values of the matching type; other types
    /// evaluate to NULL. Ordering requires an explicit CAST.
    Polymorphic,
    /// Present in a small fraction of documents. Never shredded.
    Sparse(ValueType),
}

impl PathClass {
    pub fn shreddable(&self) -> Option<ValueType> {
        match self {
            PathClass::Stable(t) if !matches!(t, ValueType::Object) => Some(*t),
            _ => None,
        }
    }
}

/// Presence threshold below which a path is Sparse and never shredded.
const SPARSE_BELOW: f64 = 0.60;
/// A type seen in fewer than this fraction of a path's occurrences is treated
/// as noise rather than as evidence of polymorphism — one stray null in a
/// million documents should not cost a column.
const TYPE_NOISE_FLOOR: f64 = 0.001;

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PathStats {
    /// Occurrences per concrete type. `Null` is counted but never makes a path
    /// polymorphic on its own.
    pub types: BTreeMap<ValueType, u64>,
    pub present: u64,
    pub hll: Hll,
}

impl PathStats {
    pub fn observe(&mut self, v: &Value) {
        self.present += 1;
        let t = match v {
            // Numeric widening: Int and Float are one type at a path.
            Value::Int(_) | Value::Float(_) => ValueType::Number,
            other => other.ty(),
        };
        *self.types.entry(t).or_insert(0) += 1;
        let mut buf = Vec::with_capacity(16);
        crate::variant::encode(v, &mut buf);
        self.hll.add(fnv1a(&buf));
    }

    pub fn merge(&mut self, other: &PathStats) {
        for (t, n) in &other.types {
            *self.types.entry(*t).or_insert(0) += n;
        }
        self.present += other.present;
        self.hll.merge(&other.hll);
    }

    pub fn classify(&self, total_docs: u64) -> PathClass {
        let significant: Vec<(ValueType, u64)> = self
            .types
            .iter()
            .filter(|(t, n)| {
                **t != ValueType::Null
                    && (**n as f64) / (self.present.max(1) as f64) >= TYPE_NOISE_FLOOR
            })
            .map(|(t, n)| (*t, *n))
            .collect();
        let dominant =
            significant.iter().max_by_key(|(_, n)| *n).map(|(t, _)| *t).unwrap_or(ValueType::Null);
        if significant.len() > 1 {
            return PathClass::Polymorphic;
        }
        let presence = self.present as f64 / total_docs.max(1) as f64;
        if presence < SPARSE_BELOW {
            PathClass::Sparse(dominant)
        } else {
            PathClass::Stable(dominant)
        }
    }

    pub fn approx_cardinality(&self) -> u64 {
        self.hll.estimate()
    }
}

/// A 1 KiB HyperLogLog. Cardinality feeds the planner's cost model; being off
/// by a few percent changes nothing it decides.
#[derive(Debug, Clone)]
pub struct Hll {
    regs: Vec<u8>,
}

const HLL_P: u32 = 10;
const HLL_M: usize = 1 << HLL_P;

impl Default for Hll {
    fn default() -> Self {
        Hll { regs: vec![0; HLL_M] }
    }
}

impl Hll {
    pub fn add(&mut self, hash: u64) {
        let idx = (hash >> (64 - HLL_P)) as usize;
        let w = (hash << HLL_P) | (1 << (HLL_P - 1));
        let rank = w.leading_zeros() as u8 + 1;
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }

    pub fn merge(&mut self, other: &Hll) {
        for i in 0..HLL_M {
            if other.regs[i] > self.regs[i] {
                self.regs[i] = other.regs[i];
            }
        }
    }

    pub fn estimate(&self) -> u64 {
        let m = HLL_M as f64;
        let sum: f64 = self.regs.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        let zeros = self.regs.iter().filter(|&&r| r == 0).count();
        if raw <= 2.5 * m && zeros > 0 {
            (m * (m / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Cosine,
    L2,
    InnerProduct,
}

impl Metric {
    pub fn parse(s: &str) -> Result<Metric> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Ok(Metric::Cosine),
            "l2" | "euclidean" => Ok(Metric::L2),
            "ip" | "inner_product" | "dot" => Ok(Metric::InnerProduct),
            other => Err(Error::Schema(format!("unknown vector metric `{other}`"))),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::InnerProduct => "ip",
        }
    }
}

#[derive(Debug, Clone)]
pub enum IndexKind {
    FullText {
        analyzer: String,
    },
    Vector {
        dims: usize,
        metric: Metric,
    },
    /// Local secondary index. Global secondary indexes require cross-shard 2PC
    /// per write and are deliberately absent (§5.5).
    Secondary,
    /// The walk index of an edge collection: `path` is the column a hop
    /// probes, `to` the one it reads. No region of its own: both are declared
    /// columns, and the index is the declaration that a walk may use them and
    /// the tier they are kept at.
    Adjacency {
        to: String,
    },
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub path: String,
    pub kind: IndexKind,
    /// Where this index lives *now*. A declaration, not a guarantee: the node
    /// budget still wins under real pressure.
    pub tier: crate::residency::Tier,
    /// Where DDL said it should live. A lifecycle policy demotes `tier` below
    /// this; an access promotes it back — but never past it, so an index
    /// declared `cold` is not dragged into RAM by traffic. `ALTER INDEX ... SET
    /// TIER` moves both, because that is an operator restating the baseline.
    pub declared_tier: crate::residency::Tier,
    /// The incarnation of the collection this index was made on: the
    /// collection's `created_micros` at the time. A collection's tombstone
    /// takes every index made on an incarnation older than it, wherever
    /// the index has since been merged to, so two nodes reconciling in
    /// either order end the same. Zero from a catalog before format 8,
    /// which every tombstone outranks. Format 8.
    pub on_micros: u64,
}

impl IndexDef {
    pub fn new(name: &str, path: &str, kind: IndexKind, tier: crate::residency::Tier) -> IndexDef {
        IndexDef {
            name: name.to_string(),
            path: path.to_string(),
            kind,
            tier,
            declared_tier: tier,
            on_micros: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub path: String,
    pub ty: ValueType,
    pub not_null: bool,
}

/// Walk every path of `doc`, nested objects included, into `paths`.
pub fn observe_paths(paths: &mut BTreeMap<String, PathStats>, doc: &Value) {
    let Value::Object(fields) = doc else { return };
    let mut stack: Vec<(String, &Value)> = fields.iter().map(|(k, v)| (k.clone(), v)).collect();
    while let Some((path, v)) = stack.pop() {
        paths.entry(path.clone()).or_default().observe(v);
        if let Value::Object(sub) = v {
            for (k, sv) in sub {
                stack.push((format!("{path}.{k}"), sv));
            }
        }
    }
}

/// A document count and the per-path statistics of those documents, kept
/// apart from a [`Collection`] so that one set of documents can be tallied
/// twice into different totals -- the live view a planner reads, and the
/// sealed view a persist writes. Additive: `merge` sums counts and unions the
/// cardinality sketches, so tallies of disjoint document sets combine into
/// the tally of their union.
#[derive(Debug, Clone, Default)]
pub struct PathTally {
    pub docs: u64,
    pub paths: BTreeMap<String, PathStats>,
}

impl PathTally {
    pub fn observe_doc(&mut self, doc: &Value) {
        self.docs += 1;
        observe_paths(&mut self.paths, doc);
    }

    pub fn merge(&mut self, other: &PathTally) {
        self.docs += other.docs;
        for (p, st) in &other.paths {
            self.paths.entry(p.clone()).or_default().merge(st);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Collection {
    pub name: String,
    pub primary_key: String,
    /// Documents are range-partitioned on `(partition_key, primary_key)`, so a
    /// tenant is contiguous in every segment (§3.2).
    pub partition_key: Option<String>,
    pub declared: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    /// Inferred, aggregated from per-segment statistics.
    pub paths: BTreeMap<String, PathStats>,
    pub doc_count: u64,
    /// How many dictionary terms a prefix on this collection expands to, or
    /// `None` for the default, `PREFIX_EXPANSION_LIMIT`. Set by
    /// `CREATE COLLECTION ... WITH (prefix_expansion = N)` and
    /// `ALTER COLLECTION ... SET (prefix_expansion = N)`. The engine bounds it
    /// by its statistics cache, which is why the bound is not enforced here.
    /// Travels with an export, so a copy answers a prefix the way its source
    /// does.
    pub prefix_expansion: Option<usize>,
    /// The node collection this collection's edges point into, when it is
    /// an edge collection: every `src` and `dst` is a primary key of it.
    /// `CREATE COLLECTION ... WITH (nodes_of = 'papers')`.
    pub nodes_of: Option<String>,
    /// A walk over this collection's edges follows them in both directions.
    pub undirected: bool,
    /// When the collection was created, microseconds since the epoch on
    /// the creating node's clock; what a tombstone from another node is
    /// compared against when the two catalogs are reconciled. Zero for a
    /// collection from a catalog written before format 7, which every
    /// tombstone outranks. Format 7.
    pub created_micros: u64,
}

impl Collection {
    pub fn new(name: &str, primary_key: &str, partition_key: Option<String>) -> Self {
        Collection {
            name: name.to_string(),
            primary_key: primary_key.to_string(),
            partition_key,
            declared: Vec::new(),
            indexes: Vec::new(),
            paths: BTreeMap::new(),
            doc_count: 0,
            prefix_expansion: None,
            nodes_of: None,
            undirected: false,
            created_micros: 0,
        }
    }

    /// The adjacency index a walk over this collection uses, if declared.
    pub fn adjacency_index(&self) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| matches!(i.kind, IndexKind::Adjacency { .. }))
    }

    /// The prefix expansion cap in force: the collection's own, or the
    /// default.
    pub fn prefix_cap(&self) -> usize {
        self.prefix_expansion.unwrap_or(crate::text::scorer::PREFIX_EXPANSION_LIMIT)
    }

    pub fn declared_type(&self, path: &str) -> Option<ValueType> {
        self.declared.iter().find(|c| c.path == path).map(|c| c.ty)
    }

    /// The type the planner should assume for a path: a DDL declaration wins,
    /// otherwise inference.
    pub fn path_class(&self, path: &str) -> Option<PathClass> {
        if let Some(t) = self.declared_type(path) {
            return Some(PathClass::Stable(t));
        }
        self.paths.get(path).map(|s| s.classify(self.doc_count))
    }

    pub fn vector_index(&self, path: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.path == path && matches!(i.kind, IndexKind::Vector { .. }))
    }

    pub fn fulltext_index(&self, path: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.path == path && matches!(i.kind, IndexKind::FullText { .. }))
    }

    /// The segment component each index owns. A full-text index and a vector
    /// index on the same path are different components, so they tier
    /// independently.
    pub fn index_component(idx: &IndexDef) -> String {
        match idx.kind {
            IndexKind::FullText { .. } => crate::segment::text_component(&idx.path),
            IndexKind::Vector { .. } => crate::segment::vector_component(&idx.path),
            IndexKind::Secondary => crate::segment::column_component(&idx.path),
            // The probed column's map; the read column's map is tiered with
            // it, in `index_tiers`.
            IndexKind::Adjacency { .. } => crate::segment::adjacency_component(&idx.path),
        }
    }

    /// Tier per segment *component*, for the segments to adopt.
    ///
    /// Keyed by component rather than by path: two indexes can share a path
    /// (`fulltext(body)` and `secondary(body)`), and keying by path collapses
    /// them, so whichever was defined last silently dictates the other's
    /// residency. Where two indexes really do own one component, the warmest
    /// declaration wins — a component cannot satisfy a hot index by being
    /// released on a cold index's schedule.
    pub fn index_tiers(&self) -> BTreeMap<String, crate::residency::Tier> {
        let mut out: BTreeMap<String, crate::residency::Tier> = BTreeMap::new();
        for i in &self.indexes {
            let c = Collection::index_component(i);
            out.entry(c).and_modify(|t| *t = (*t).min(i.tier)).or_insert(i.tier);
            // An adjacency index owns two maps, one per column, so that a
            // walk can follow its edges either way; both follow its tier.
            if let IndexKind::Adjacency { to } = &i.kind {
                let c = crate::segment::adjacency_component(to);
                out.entry(c).and_modify(|t| *t = (*t).min(i.tier)).or_insert(i.tier);
            }
        }
        out
    }

    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name == name)
    }

    /// True when nothing in this collection needs a local copy of a segment.
    pub fn all_indexes_archived(&self) -> bool {
        !self.indexes.is_empty()
            && self.indexes.iter().all(|i| i.tier == crate::residency::Tier::Archived)
    }

    pub fn analyzer_for(&self, path: &str) -> &str {
        match self.fulltext_index(path).map(|i| &i.kind) {
            Some(IndexKind::FullText { analyzer }) => analyzer,
            _ => "standard",
        }
    }

    pub fn vector_dims(&self, path: &str) -> Option<usize> {
        match self.vector_index(path).map(|i| &i.kind) {
            Some(IndexKind::Vector { dims, .. }) => Some(*dims),
            _ => None,
        }
    }

    pub fn vector_metric(&self, path: &str) -> Option<Metric> {
        match self.vector_index(path).map(|i| &i.kind) {
            Some(IndexKind::Vector { metric, .. }) => Some(*metric),
            _ => None,
        }
    }

    /// Paths this writer should shred into columns. A physical, per-segment
    /// decision: declared columns always, plus inferred stable scalar paths.
    pub fn shred_candidates(&self) -> Vec<(String, ValueType)> {
        let mut out: Vec<(String, ValueType)> =
            self.declared.iter().map(|c| (c.path.clone(), c.ty)).collect();
        for (path, stats) in &self.paths {
            if out.iter().any(|(p, _)| p == path) {
                continue;
            }
            if let Some(t) = stats.classify(self.doc_count).shreddable() {
                out.push((path.clone(), t));
            }
        }
        out.sort();
        out
    }

    /// Fold one document's paths into the catalog. Called by the memtable on
    /// write and by the segment writer when it seals, so statistics never lag
    /// the data by more than a flush.
    pub fn observe_doc(&mut self, doc: &Value) {
        self.doc_count += 1;
        observe_paths(&mut self.paths, doc);
    }

    /// Validate a document against declared columns. Undeclared paths are
    /// always allowed: documents stay open beyond the DDL (§2.4).
    pub fn validate(&self, doc: &Value) -> Result<()> {
        for c in &self.declared {
            let v = doc.path(&c.path).unwrap_or(&Value::Null);
            if v.is_null() {
                if c.not_null {
                    return Err(Error::Schema(format!(
                        "column `{}` is NOT NULL but the document omits it",
                        c.path
                    )));
                }
                continue;
            }
            let actual = match v {
                Value::Int(_) | Value::Float(_) => ValueType::Number,
                other => other.ty(),
            };
            let ok = actual == c.ty
                // A declared TIMESTAMP accepts an ISO-8601 string; the writer
                // casts it. This is the "declared in DDL" half of §2.1.
                || (c.ty == ValueType::Timestamp
                    && matches!(v, Value::Str(s) if crate::time::parse_iso8601(s).is_some()))
                || (c.ty == ValueType::Number && actual == ValueType::Timestamp);
            if !ok {
                return Err(Error::Schema(format!(
                    "column `{}` is declared {} but the document has {}",
                    c.path,
                    c.ty.name(),
                    actual.name()
                )));
            }
        }
        Ok(())
    }

    /// Apply DDL-declared casts in place. Only declared paths are touched, so
    /// an undeclared ISO-8601 string stays a string (§2.1).
    pub fn coerce(&self, doc: &mut Value) {
        for c in &self.declared {
            if c.ty != ValueType::Timestamp {
                continue;
            }
            if let Some(Value::Str(s)) = doc.path(&c.path) {
                if let Some(m) = crate::time::parse_iso8601(s) {
                    // Cannot be refused: the path already holds a string in a
                    // document that was parsed under the same depth bound, and
                    // a scalar replacing a scalar deepens nothing.
                    let _ = doc.set_path(&c.path, Value::Timestamp(m));
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Catalog {
    pub collections: BTreeMap<String, Collection>,
    /// Index lifecycle policies, keyed by name. Control-plane state like
    /// everything else here (§10): a policy is a rule the cluster agrees on,
    /// not a per-node setting.
    pub policies: BTreeMap<String, crate::lifecycle::LifecyclePolicy>,
    /// Per `(collection, index)` creation and last-access times, which is what
    /// the policies are evaluated against. Persisted, because "idle for seven
    /// days" must not be reset by a restart.
    pub activity: BTreeMap<(String, String), crate::lifecycle::IndexActivity>,
    /// The attached nodes that are coordinators: they hold no shards and
    /// take none from a placement, a rebalance or a move, but every DDL
    /// reaches them so they can plan. Learned from a node's `hello` at
    /// `ATTACH`. Format 6.
    pub coordinators: BTreeSet<String>,
    /// The other nodes this one may place shards on, in the order they were
    /// attached, which is the order a default placement walks. Control
    /// plane, so it is here and not in a flag; membership is declared.
    pub nodes: Vec<String>,
    /// Per collection, one entry per shard index: which node holds it and
    /// the key range it owns. Every node holding a shard carries the same
    /// map, so any of them can coordinate.
    pub placement: BTreeMap<String, Vec<Tablet>>,
    /// Data-plane readers observe catalog versions and never block on DDL
    /// (§10).
    pub version: u64,
    /// What was dropped and when: a collection under its name, an index
    /// under [`Catalog::tombstone`], each with the drop's microseconds.
    /// What lets two catalogs that changed apart -- across a split, or
    /// while a node was down -- be reconciled without resurrecting a
    /// drop: a definition older than its tombstone is dropped on the node
    /// that still has it, and one younger is the operator making it again.
    /// Format 7.
    pub dropped: BTreeMap<String, u64>,
    /// When this data directory was made, microseconds. A definition
    /// older than the directory that names this node as a holder is not
    /// one this node missed while away; it is one whose data this
    /// directory never had, and reconciliation refuses to grow an empty
    /// shard for it. Zero for a directory from before format 7. Format 7.
    pub born_micros: u64,
}

impl Catalog {
    /// The tombstone key of an index: the collection's name, a slash, the
    /// index's. A collection's tombstone is its name alone, and a name can
    /// hold no slash, since it names a directory.
    pub fn tombstone(collection: &str, index: &str) -> String {
        format!("{collection}/{index}")
    }

    pub fn get(&self, name: &str) -> Result<&Collection> {
        self.collections
            .get(name)
            .ok_or_else(|| Error::Plan(format!("no such collection `{name}`")))
    }

    pub fn get_mut(&mut self, name: &str) -> Result<&mut Collection> {
        self.collections
            .get_mut(name)
            .ok_or_else(|| Error::Plan(format!("no such collection `{name}`")))
    }

    pub fn create(&mut self, c: Collection) -> Result<()> {
        if self.collections.contains_key(&c.name) {
            return Err(Error::Schema(format!("collection `{}` already exists", c.name)));
        }
        // One path, one declaration. Two `ColumnDef`s for the same path are two
        // answers to "what type is this": if they disagree, `validate_doc`
        // checks both and the collection can never accept another write; if
        // they agree, `shred_candidates` names the path twice and the segment
        // writer emits two columns under one region name, of which the reader
        // sees whichever the directory kept. This is the one door every
        // `CREATE COLLECTION` goes through, and it is before anything is
        // written, which is the only place the answer can still be "no".
        let mut seen = std::collections::BTreeSet::new();
        for d in &c.declared {
            if !seen.insert(d.path.as_str()) {
                return Err(Error::Schema(format!(
                    "column `{}` is declared twice in collection `{}`",
                    d.path, c.name
                )));
            }
        }
        let mut c = c;
        // A definition adopted from another node keeps the instant that
        // node made it, so both compare it against the same tombstones.
        if c.created_micros == 0 {
            c.created_micros = crate::time::now_micros().max(0) as u64;
        }
        self.collections.insert(c.name.clone(), c);
        self.version += 1;
        Ok(())
    }

    pub fn add_index(&mut self, collection: &str, idx: IndexDef) -> Result<()> {
        let c = self.get_mut(collection)?;
        if c.indexes.iter().any(|i| i.name == idx.name) {
            return Err(Error::Schema(format!("index `{}` already exists", idx.name)));
        }
        let mut idx = idx;
        if idx.on_micros == 0 {
            idx.on_micros = c.created_micros;
        }
        c.indexes.push(idx);
        self.version += 1;
        Ok(())
    }

    // --- Persistence. Under replication this is a control-plane consensus
    // transaction; here it
    // is a file, but the encoding is the same one the control plane would ship.

    /// Encode with a magic, a format version and a trailing checksum.
    ///
    /// The catalog holds strictly more state than a shard manifest — every
    /// collection, index, tier, policy and activity clock — and until this was
    /// framed it was written with a bare truncating write and read back with no
    /// integrity check at all. A crash halfway through left a file that does
    /// not decode, and the whole database is unopenable with every segment
    /// intact. The manifest has done it this way from the start; the catalog
    /// now does too.
    pub fn encode(&self) -> Vec<u8> {
        self.encode_as(CATALOG_VERSION)
    }

    /// The catalog laid out as a given format version writes it. Only the
    /// current version is ever written; the older layout is produced here so
    /// the read path for it is tested against bytes shaped the way an older
    /// build shaped them, not against a current body with its version byte
    /// changed.
    fn encode_as(&self, format: u8) -> Vec<u8> {
        let mut body = self.encode_body(format);
        let crc = crc32(&body);
        put_u32(&mut body, crc);
        let mut out = Vec::with_capacity(body.len() + 5);
        out.extend_from_slice(CATALOG_MAGIC);
        out.push(format);
        out.extend_from_slice(&body);
        out
    }

    pub fn decode(b: &[u8]) -> Result<Catalog> {
        if b.len() < 9 || &b[0..4] != CATALOG_MAGIC {
            return Err(Error::Storage("catalog: not a celastro catalog (bad magic)".into()));
        }
        let v = b[4];
        if !(CATALOG_VERSION_OLDEST..=CATALOG_VERSION).contains(&v) {
            let why = if v < CATALOG_VERSION_OLDEST {
                "; its tier bytes name different tiers in this build, so reading it would \
                 shift every index one rung up the ladder"
            } else {
                ""
            };
            return Err(Error::Storage(format!(
                "catalog format version {v} is not readable by this build (expected \
                 {CATALOG_VERSION_OLDEST} to {CATALOG_VERSION}){why}"
            )));
        }
        let rest = &b[5..];
        let (body, tail) = rest.split_at(rest.len() - 4);
        if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
            return Err(Error::Storage("catalog: checksum mismatch".into()));
        }
        Catalog::decode_body(body, v)
    }

    fn encode_body(&self, format: u8) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.version);
        put_uvarint(&mut out, self.collections.len() as u64);
        for c in self.collections.values() {
            put_str(&mut out, &c.name);
            put_str(&mut out, &c.primary_key);
            match &c.partition_key {
                Some(p) => {
                    out.push(1);
                    put_str(&mut out, p);
                }
                None => out.push(0),
            }
            put_uvarint(&mut out, c.doc_count);
            put_uvarint(&mut out, c.declared.len() as u64);
            for d in &c.declared {
                put_str(&mut out, &d.path);
                out.push(d.ty as u8);
                out.push(d.not_null as u8);
            }
            put_uvarint(&mut out, c.indexes.len() as u64);
            for i in &c.indexes {
                put_str(&mut out, &i.name);
                put_str(&mut out, &i.path);
                match &i.kind {
                    IndexKind::FullText { analyzer } => {
                        out.push(0);
                        put_str(&mut out, analyzer);
                    }
                    IndexKind::Vector { dims, metric } => {
                        out.push(1);
                        put_uvarint(&mut out, *dims as u64);
                        out.push(*metric as u8);
                    }
                    IndexKind::Secondary => out.push(2),
                    IndexKind::Adjacency { to } => {
                        out.push(3);
                        put_str(&mut out, to);
                    }
                }
                out.push(i.tier.as_u8());
                out.push(i.declared_tier.as_u8());
                if format >= 8 {
                    put_uvarint(&mut out, i.on_micros);
                }
            }
            put_uvarint(&mut out, c.paths.len() as u64);
            for (p, s) in &c.paths {
                put_str(&mut out, p);
                put_uvarint(&mut out, s.present);
                put_uvarint(&mut out, s.types.len() as u64);
                for (t, n) in &s.types {
                    out.push(*t as u8);
                    put_uvarint(&mut out, *n);
                }
                out.extend_from_slice(&s.hll.regs);
            }
            // Version 3 appends the cap; zero is "unset", which is why the
            // setting itself can never be zero.
            if format >= 3 {
                put_uvarint(&mut out, c.prefix_expansion.unwrap_or(0) as u64);
            }
            // Version 5 appends the edge-collection fields; an empty name is
            // "not an edge collection".
            if format >= 5 {
                put_str(&mut out, c.nodes_of.as_deref().unwrap_or(""));
                out.push(c.undirected as u8);
            }
            if format >= 7 {
                put_uvarint(&mut out, c.created_micros);
            }
        }
        crate::lifecycle::encode_policies(&self.policies, &mut out);
        crate::lifecycle::encode_activity(&self.activity, &mut out);
        if format >= 4 {
            put_uvarint(&mut out, self.nodes.len() as u64);
            for n in &self.nodes {
                put_str(&mut out, n);
            }
            put_uvarint(&mut out, self.placement.len() as u64);
            for (name, tablets) in &self.placement {
                put_str(&mut out, name);
                put_uvarint(&mut out, tablets.len() as u64);
                for t in tablets {
                    put_str(&mut out, &t.node);
                    put_opt_str(&mut out, t.lo.as_deref());
                    put_opt_str(&mut out, t.hi.as_deref());
                }
            }
            if format >= 6 {
                put_uvarint(&mut out, self.coordinators.len() as u64);
                for n in &self.coordinators {
                    put_str(&mut out, n);
                }
            }
            if format >= 7 {
                put_uvarint(&mut out, self.born_micros);
                put_uvarint(&mut out, self.dropped.len() as u64);
                for (k, t) in &self.dropped {
                    put_str(&mut out, k);
                    put_uvarint(&mut out, *t);
                }
            }
        }
        out
    }

    fn decode_body(b: &[u8], format: u8) -> Result<Catalog> {
        let bad = || Error::Storage("catalog: truncated".into());
        let mut i = 0usize;
        let version = get_uvarint(b, &mut i).ok_or_else(bad)?;
        let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let mut collections = BTreeMap::new();
        for _ in 0..n {
            let name = get_str(b, &mut i).ok_or_else(bad)?;
            let pk = get_str(b, &mut i).ok_or_else(bad)?;
            let has_part = *b.get(i).ok_or_else(bad)?;
            i += 1;
            let partition_key =
                if has_part == 1 { Some(get_str(b, &mut i).ok_or_else(bad)?) } else { None };
            let mut c = Collection::new(&name, &pk, partition_key);
            c.doc_count = get_uvarint(b, &mut i).ok_or_else(bad)?;
            let nd = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..nd {
                let path = get_str(b, &mut i).ok_or_else(bad)?;
                let ty = decode_ty(*b.get(i).ok_or_else(bad)?)?;
                i += 1;
                let not_null = *b.get(i).ok_or_else(bad)? == 1;
                i += 1;
                c.declared.push(ColumnDef { path, ty, not_null });
            }
            let ni = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..ni {
                let iname = get_str(b, &mut i).ok_or_else(bad)?;
                let ipath = get_str(b, &mut i).ok_or_else(bad)?;
                let tag = *b.get(i).ok_or_else(bad)?;
                i += 1;
                let kind = match tag {
                    0 => IndexKind::FullText { analyzer: get_str(b, &mut i).ok_or_else(bad)? },
                    1 => {
                        let dims = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                        let m = *b.get(i).ok_or_else(bad)?;
                        i += 1;
                        IndexKind::Vector {
                            dims,
                            metric: match m {
                                0 => Metric::Cosine,
                                1 => Metric::L2,
                                _ => Metric::InnerProduct,
                            },
                        }
                    }
                    3 => IndexKind::Adjacency { to: get_str(b, &mut i).ok_or_else(bad)? },
                    _ => IndexKind::Secondary,
                };
                // Bounded rather than saturating: `Tier::from_u8` maps every
                // unknown byte to `archived`, which is the one tier that
                // relocates files. A byte this build does not know is a catalog
                // it should not be interpreting.
                let tier_byte = |b: Option<&u8>| -> Result<crate::residency::Tier> {
                    match b {
                        Some(v) => crate::residency::Tier::try_from_u8(*v).ok_or_else(|| {
                            Error::Storage(format!("catalog: unknown tier byte {v}"))
                        }),
                        None => Err(bad()),
                    }
                };
                let tier = tier_byte(b.get(i))?;
                i += 1;
                let declared_tier = tier_byte(b.get(i))?;
                i += 1;
                let on_micros =
                    if format >= 8 { get_uvarint(b, &mut i).ok_or_else(bad)? } else { 0 };
                c.indexes.push(IndexDef {
                    name: iname,
                    path: ipath,
                    kind,
                    tier,
                    declared_tier,
                    on_micros,
                });
            }
            let np = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..np {
                let path = get_str(b, &mut i).ok_or_else(bad)?;
                let mut st = PathStats::default();
                st.present = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let nt = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                for _ in 0..nt {
                    let t = decode_ty(*b.get(i).ok_or_else(bad)?)?;
                    i += 1;
                    let cnt = get_uvarint(b, &mut i).ok_or_else(bad)?;
                    st.types.insert(t, cnt);
                }
                let regs = b.get(i..i + HLL_M).ok_or_else(bad)?;
                i += HLL_M;
                st.hll.regs.copy_from_slice(regs);
                c.paths.insert(path, st);
            }
            if format >= 3 {
                let cap = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                c.prefix_expansion = if cap == 0 { None } else { Some(cap) };
            }
            if format >= 5 {
                let of = get_str(b, &mut i).ok_or_else(bad)?;
                c.nodes_of = if of.is_empty() { None } else { Some(of) };
                c.undirected = *b.get(i).ok_or_else(bad)? == 1;
                i += 1;
            }
            if format >= 7 {
                c.created_micros = get_uvarint(b, &mut i).ok_or_else(bad)?;
            }
            collections.insert(name, c);
        }
        let policies = crate::lifecycle::decode_policies(b, &mut i)?;
        let activity = crate::lifecycle::decode_activity(b, &mut i)?;
        let mut nodes = Vec::new();
        let mut placement = BTreeMap::new();
        let mut coordinators = BTreeSet::new();
        if format >= 4 {
            let nn = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..nn {
                nodes.push(get_str(b, &mut i).ok_or_else(bad)?);
            }
            let np = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..np {
                let name = get_str(b, &mut i).ok_or_else(bad)?;
                let nt = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut tablets = Vec::with_capacity(nt);
                for _ in 0..nt {
                    tablets.push(Tablet {
                        node: get_str(b, &mut i).ok_or_else(bad)?,
                        lo: get_opt_str(b, &mut i).ok_or_else(bad)?,
                        hi: get_opt_str(b, &mut i).ok_or_else(bad)?,
                    });
                }
                placement.insert(name, tablets);
            }
            if format >= 6 {
                let nc = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                for _ in 0..nc {
                    coordinators.insert(get_str(b, &mut i).ok_or_else(bad)?);
                }
            }
        }
        let mut dropped = BTreeMap::new();
        let mut born_micros = 0;
        if format >= 7 {
            born_micros = get_uvarint(b, &mut i).ok_or_else(bad)?;
            let nd = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            for _ in 0..nd {
                let k = get_str(b, &mut i).ok_or_else(bad)?;
                let t = get_uvarint(b, &mut i).ok_or_else(bad)?;
                dropped.insert(k, t);
            }
        }
        Ok(Catalog {
            collections,
            policies,
            activity,
            nodes,
            placement,
            coordinators,
            version,
            dropped,
            born_micros,
        })
    }
}

fn decode_ty(b: u8) -> Result<ValueType> {
    Ok(match b {
        0 => ValueType::Null,
        1 => ValueType::Bool,
        2 => ValueType::Number,
        3 => ValueType::Str,
        4 => ValueType::Timestamp,
        5 => ValueType::Array,
        6 => ValueType::Object,
        _ => return Err(Error::Storage("catalog: bad type tag".into())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    #[test]
    fn numeric_widening_is_not_polymorphism() {
        let mut c = Collection::new("t", "id", None);
        c.observe_doc(&json::parse(r#"{"id":"a","n":1}"#).unwrap());
        c.observe_doc(&json::parse(r#"{"id":"b","n":1.5}"#).unwrap());
        assert_eq!(c.path_class("n"), Some(PathClass::Stable(ValueType::Number)));
    }

    #[test]
    fn mixed_types_are_polymorphic_and_iso_strings_stay_strings() {
        let mut c = Collection::new("t", "id", None);
        for _ in 0..50 {
            c.observe_doc(&json::parse(r#"{"id":"a","x":1,"when":"2026-01-01"}"#).unwrap());
        }
        for _ in 0..50 {
            c.observe_doc(&json::parse(r#"{"id":"b","x":"one"}"#).unwrap());
        }
        assert_eq!(c.path_class("x"), Some(PathClass::Polymorphic));
        // Undeclared ISO-8601 is text, and it is sparse (50 of 100 docs).
        assert_eq!(c.path_class("when"), Some(PathClass::Sparse(ValueType::Str)));
    }

    #[test]
    fn declared_timestamp_is_cast_on_write() {
        let mut c = Collection::new("articles", "id", None);
        c.declared.push(ColumnDef {
            path: "published_at".into(),
            ty: ValueType::Timestamp,
            not_null: false,
        });
        let mut d = json::parse(r#"{"id":"a","published_at":"2026-09-08T12:00:00Z"}"#).unwrap();
        c.validate(&d).unwrap();
        c.coerce(&mut d);
        assert!(matches!(d.path("published_at"), Some(Value::Timestamp(_))));
    }

    #[test]
    fn cardinality_estimate_survives_near_identical_keys() {
        // Sequential ids differing in one or two characters are the normal case
        // for a primary key, and they are exactly the input that a hash with a
        // weak avalanche buckets into almost nothing.
        let mut st = PathStats::default();
        for i in 0..5000 {
            st.observe(&Value::Str(format!("note-{i:05}")));
        }
        let est = st.approx_cardinality();
        assert!(
            (est as f64 - 5000.0).abs() / 5000.0 < 0.10,
            "cardinality estimate {est} is not within 10% of 5000"
        );
    }

    #[test]
    fn catalog_round_trips() {
        let mut cat = Catalog::default();
        let mut c = Collection::new("articles", "id", Some("tenant_id".into()));
        c.declared.push(ColumnDef { path: "tenant_id".into(), ty: ValueType::Str, not_null: true });
        c.indexes.push(IndexDef::new(
            "articles_emb",
            "embedding",
            IndexKind::Vector { dims: 8, metric: Metric::Cosine },
            crate::residency::Tier::Cached,
        ));
        c.observe_doc(&json::parse(r#"{"id":"a","tenant_id":"t1"}"#).unwrap());
        c.prefix_expansion = Some(2048);
        c.indexes[0].on_micros = 5;
        cat.create(c).unwrap();
        cat.born_micros = 17;
        cat.dropped.insert("old".into(), 40);
        cat.dropped.insert(Catalog::tombstone("articles", "gone"), 41);
        let back = Catalog::decode(&cat.encode()).unwrap();
        let a = back.get("articles").unwrap();
        assert_eq!(a.partition_key.as_deref(), Some("tenant_id"));
        assert!(a.created_micros > 0, "a creation is stamped and survives");
        assert_eq!(back.born_micros, 17);
        assert_eq!(a.indexes[0].on_micros, 5, "an index knows its incarnation");
        assert_eq!(back.dropped.get("old"), Some(&40));
        assert_eq!(back.dropped.get("articles/gone"), Some(&41));
        assert_eq!(a.indexes[0].tier, crate::residency::Tier::Cached, "tier survives a reopen");
        assert_eq!(a.vector_dims("embedding"), Some(8));
        assert_eq!(a.doc_count, 1);
        assert_eq!(a.prefix_expansion, Some(2048), "the prefix cap survives a reopen");

        cat.nodes.push("tcp://b:9000".into());
        cat.placement.insert(
            "articles".into(),
            vec![
                Tablet { node: String::new(), lo: None, hi: Some("m".into()) },
                Tablet { node: "tcp://b:9000".into(), lo: Some("m".into()), hi: None },
            ],
        );
        let back = Catalog::decode(&cat.encode()).unwrap();
        assert_eq!(back.nodes, cat.nodes, "the node list survives a reopen");
        assert_eq!(back.placement, cat.placement, "and so does the placement");
        let three = Catalog::decode(&cat.encode_as(3)).unwrap();
        assert!(three.nodes.is_empty() && three.placement.is_empty(), "a 3 has neither");

        // An edge collection: which collection it points into, whether its
        // walks are undirected, and the adjacency index's second column all
        // survive a reopen, and a 4 -- which has none of them -- is read as
        // a plain collection.
        let mut e = Collection::new("cites", "id", None);
        e.nodes_of = Some("articles".into());
        e.undirected = true;
        e.indexes.push(IndexDef::new(
            "cites_adj",
            "src",
            IndexKind::Adjacency { to: "dst".into() },
            crate::residency::Tier::Cached,
        ));
        cat.create(e).unwrap();
        let back = Catalog::decode(&cat.encode()).unwrap();
        let e = back.get("cites").unwrap();
        assert_eq!(e.nodes_of.as_deref(), Some("articles"));
        assert!(e.undirected);
        assert!(
            matches!(&e.adjacency_index().unwrap().kind, IndexKind::Adjacency { to } if to == "dst")
        );
        assert_eq!(
            Collection::index_component(e.adjacency_index().unwrap()),
            crate::segment::adjacency_component("src")
        );
        let tiers = e.index_tiers();
        assert_eq!(tiers.get("adj:src"), Some(&crate::residency::Tier::Cached));
        assert_eq!(
            tiers.get("adj:dst"),
            Some(&crate::residency::Tier::Cached),
            "both maps tier together"
        );
        let four = Catalog::decode(&cat.encode_as(4)).unwrap();
        let e4 = four.get("cites").unwrap();
        assert!(e4.nodes_of.is_none() && !e4.undirected, "a 4 knows no edge collections");
    }

    /// A catalog written by a 0.14 build has no cap field: it is read at the
    /// default, with everything that follows the collections -- policies and
    /// activity -- still found where the older layout put them. The bytes are
    /// laid out by the version-2 writer, not by the current one with a
    /// changed version byte, because the difference between the two IS the
    /// field this test is about. The far side is pinned too: a version this
    /// build does not know is refused naming the range it reads, and version
    /// 1 still gets the reason it is refused.
    #[test]
    fn a_version_2_catalog_is_read_with_every_collection_at_the_default_cap() {
        let mut cat = Catalog::default();
        let mut c = Collection::new("articles", "id", None);
        c.indexes.push(IndexDef::new(
            "articles_body",
            "body",
            IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::Active,
        ));
        c.observe_doc(&json::parse(r#"{"id":"a","body":"x"}"#).unwrap());
        c.prefix_expansion = Some(1024);
        cat.create(c).unwrap();
        cat.activity.insert(
            ("articles".into(), "articles_body".into()),
            crate::lifecycle::IndexActivity::new(7),
        );

        let old = cat.encode_as(2);
        assert_eq!(old[4], 2);
        let back = Catalog::decode(&old).unwrap();
        let a = back.get("articles").unwrap();
        assert_eq!(a.prefix_expansion, None, "no field, so the default");
        assert_eq!(a.prefix_cap(), crate::text::scorer::PREFIX_EXPANSION_LIMIT);
        assert_eq!(a.doc_count, 1, "and the rest of the collection is intact");
        assert_eq!(a.indexes.len(), 1);
        assert!(
            back.activity.contains_key(&("articles".into(), "articles_body".into())),
            "what follows the collections is found where the old layout put it"
        );
        assert_eq!(
            Catalog::decode(&cat.encode()).unwrap().get("articles").unwrap().prefix_expansion,
            Some(1024),
            "the current layout carries the setting"
        );

        let mut future = cat.encode();
        future[4] = CATALOG_VERSION + 1;
        let e = Catalog::decode(&future).unwrap_err().to_string();
        assert!(e.contains("not readable") && e.contains("expected 2 to 8"), "{e}");
        let mut ancient = cat.encode();
        ancient[4] = 1;
        let e = Catalog::decode(&ancient).unwrap_err().to_string();
        assert!(e.contains("tier bytes"), "version 1 is refused for its own reason: {e}");
    }

    /// A path declared twice is not a harmless repetition. With disagreeing
    /// types it is a collection that accepts no further writes at all, because
    /// `validate_doc` walks both declarations and one of them always fails;
    /// with agreeing types the writer shreds the path twice into one region
    /// name. Neither survives a reopen any better than it started, so the
    /// refusal belongs at creation.
    #[test]
    fn a_path_cannot_be_declared_twice() {
        let mut cat = Catalog::default();
        let mut c = Collection::new("notes", "id", None);
        c.declared.push(ColumnDef { path: "n".into(), ty: ValueType::Number, not_null: false });
        c.declared.push(ColumnDef { path: "n".into(), ty: ValueType::Str, not_null: false });
        let e = cat.create(c).unwrap_err();
        assert!(matches!(e, Error::Schema(_)), "{e}");
        assert!(e.to_string().contains("declared twice"), "{e}");
        assert!(cat.collections.is_empty(), "the rejected collection must not be in the catalog");

        // The same two types on two different paths are ordinary DDL.
        let mut c = Collection::new("notes", "id", None);
        c.declared.push(ColumnDef { path: "n".into(), ty: ValueType::Number, not_null: false });
        c.declared.push(ColumnDef { path: "s".into(), ty: ValueType::Str, not_null: false });
        cat.create(c).unwrap();
        assert_eq!(cat.get("notes").unwrap().declared.len(), 2);
    }
}
