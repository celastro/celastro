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

use std::collections::BTreeMap;

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
const CATALOG_VERSION: u8 = 2;

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
}

impl IndexDef {
    pub fn new(name: &str, path: &str, kind: IndexKind, tier: crate::residency::Tier) -> IndexDef {
        IndexDef { name: name.to_string(), path: path.to_string(), kind, tier, declared_tier: tier }
    }
}

#[derive(Debug, Clone)]
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
        }
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
                    doc.set_path(&c.path, Value::Timestamp(m));
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
    /// Data-plane readers observe catalog versions and never block on DDL
    /// (§10).
    pub version: u64,
}

impl Catalog {
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
        self.collections.insert(c.name.clone(), c);
        self.version += 1;
        Ok(())
    }

    pub fn add_index(&mut self, collection: &str, idx: IndexDef) -> Result<()> {
        let c = self.get_mut(collection)?;
        if c.indexes.iter().any(|i| i.name == idx.name) {
            return Err(Error::Schema(format!("index `{}` already exists", idx.name)));
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
        let mut body = self.encode_body();
        let crc = crc32(&body);
        put_u32(&mut body, crc);
        let mut out = Vec::with_capacity(body.len() + 5);
        out.extend_from_slice(CATALOG_MAGIC);
        out.push(CATALOG_VERSION);
        out.extend_from_slice(&body);
        out
    }

    pub fn decode(b: &[u8]) -> Result<Catalog> {
        if b.len() < 9 || &b[0..4] != CATALOG_MAGIC {
            return Err(Error::Storage("catalog: not a celastro catalog (bad magic)".into()));
        }
        let v = b[4];
        if v != CATALOG_VERSION {
            let why = if v < CATALOG_VERSION {
                "; its tier bytes name different tiers in this build, so reading it would \
                 shift every index one rung up the ladder"
            } else {
                ""
            };
            return Err(Error::Storage(format!(
                "catalog format version {v} is not readable by this build (expected \
                 {CATALOG_VERSION}){why}"
            )));
        }
        let rest = &b[5..];
        let (body, tail) = rest.split_at(rest.len() - 4);
        if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
            return Err(Error::Storage("catalog: checksum mismatch".into()));
        }
        Catalog::decode_body(body)
    }

    fn encode_body(&self) -> Vec<u8> {
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
                }
                out.push(i.tier.as_u8());
                out.push(i.declared_tier.as_u8());
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
        }
        crate::lifecycle::encode_policies(&self.policies, &mut out);
        crate::lifecycle::encode_activity(&self.activity, &mut out);
        out
    }

    fn decode_body(b: &[u8]) -> Result<Catalog> {
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
                c.indexes.push(IndexDef { name: iname, path: ipath, kind, tier, declared_tier });
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
            collections.insert(name, c);
        }
        let policies = crate::lifecycle::decode_policies(b, &mut i)?;
        let activity = crate::lifecycle::decode_activity(b, &mut i)?;
        Ok(Catalog { collections, policies, activity, version })
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
        cat.create(c).unwrap();
        let back = Catalog::decode(&cat.encode()).unwrap();
        let a = back.get("articles").unwrap();
        assert_eq!(a.partition_key.as_deref(), Some("tenant_id"));
        assert_eq!(a.indexes[0].tier, crate::residency::Tier::Cached, "tier survives a reopen");
        assert_eq!(a.vector_dims("embedding"), Some(8));
        assert_eq!(a.doc_count, 1);
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
