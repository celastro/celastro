//! The memtable: recent writes, fully searchable, at exact recall.
//!
//! Recent writes live in an in-memory structure with an inverted index and a
//! **flat vector list**. Vector search over it is exact brute force, so freshly
//! written documents are searchable at full recall — a property ANN-only
//! designs cannot offer, and the reason "freshness without recall loss" is a
//! goal rather than a hope (§1, §4.3).
//!
//! Brute force over 50k × 1536 dimensions is single-digit milliseconds with the
//! kernels in [`crate::vector::distance`], which is what bounds the freshness
//! tax and sets the flush threshold.
//!
//! Memtable memory is a **node-level budget, not a per-tablet entitlement**
//! (§4.3): a node hosts hundreds of tablets and 50k float32 vectors at 1536
//! dimensions is 300 MB per memtable. [`MemtableBudget`] is that node-level
//! accounting, and tablets flush early under pressure.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use crate::bitmap::Bitmap;
use crate::catalog::{Collection, IndexKind};
use crate::column::{matches, CmpOp};
use crate::error::Result;
use crate::mvcc::{DeleteLog, Ordinals};
use crate::segment::{analyze_field, extract_vector, PendingDoc};
use crate::text::analyzer::Analyzer;
use crate::text::postings::InvertedBuilder;
use crate::text::TextSource;
use crate::time::Timestamp;
use crate::value::Value;
use crate::vector::VectorStore;

#[derive(Debug, Clone, Copy)]
pub struct FlushThresholds {
    /// Byte threshold for this memtable.
    pub max_bytes: usize,
    /// Vector-count threshold. Order 20–50k; the bound on the brute-force scan
    /// is what makes freshness cheap.
    pub max_vectors: usize,
}

impl Default for FlushThresholds {
    fn default() -> Self {
        FlushThresholds { max_bytes: 64 << 20, max_vectors: 32_768 }
    }
}

/// Node-level memtable memory accounting.
///
/// Per-tablet thresholds are upper bounds, not entitlements. When the node
/// budget is under pressure, tablets flush early in order of size — the
/// alternative is hundreds of tablets each believing it may hold 300 MB.
#[derive(Debug)]
pub struct MemtableBudget {
    used: AtomicUsize,
    limit: usize,
}

impl MemtableBudget {
    pub fn new(limit: usize) -> Arc<MemtableBudget> {
        Arc::new(MemtableBudget { used: AtomicUsize::new(0), limit })
    }
    pub fn add(&self, n: usize) {
        self.used.fetch_add(n, AtomicOrdering::Relaxed);
    }
    pub fn release(&self, n: usize) {
        // Saturating *and* atomic. The budget is shared by every tablet on the
        // node, so clamping against a separate `used()` load lets two releases
        // that read the same total each subtract all of it: `used` wraps past
        // zero, `under_pressure` is then permanently true, and the node flushes
        // a segment per document for the rest of its life.
        let _ = self.used.fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |u| {
            Some(u.saturating_sub(n))
        });
    }
    pub fn used(&self) -> usize {
        self.used.load(AtomicOrdering::Relaxed)
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// True once the node is over budget, regardless of any single tablet's
    /// own threshold.
    pub fn under_pressure(&self) -> bool {
        self.used() > self.limit
    }
}

pub struct MemDoc {
    pub sort_key: String,
    pub commit_ts: Timestamp,
    pub doc: Value,
}

pub struct Memtable {
    pub docs: Vec<MemDoc>,
    /// sort key → every ordinal ever written for it, oldest first.
    ///
    /// A chain, not a single entry: the newest version is what the write path
    /// supersedes, but a reader pinned at an older snapshot must still find the
    /// version that was current *then*. Collapsing this to one ordinal is how a
    /// point lookup quietly starts returning the future (§4.4).
    pub by_key: BTreeMap<String, Vec<u32>>,
    pub ordinals: Ordinals,
    /// Behind a lock because a frozen memtable is shared immutably while its
    /// segment is being built, and a delete can land in that window (§4.5).
    pub deletes: RwLock<DeleteLog>,
    text: BTreeMap<String, (InvertedBuilder, Analyzer)>,
    pub vectors: BTreeMap<String, VectorStore>,
    pub bytes: usize,
    budget: Option<Arc<MemtableBudget>>,
}

impl Memtable {
    pub fn new(coll: &Collection, budget: Option<Arc<MemtableBudget>>) -> Memtable {
        let mut text = BTreeMap::new();
        let mut vectors = BTreeMap::new();
        for idx in &coll.indexes {
            match &idx.kind {
                IndexKind::FullText { analyzer } => {
                    text.insert(
                        idx.path.clone(),
                        (InvertedBuilder::new(), Analyzer::parse(analyzer)),
                    );
                }
                IndexKind::Vector { dims, metric } => {
                    vectors.insert(idx.path.clone(), VectorStore::new(*dims, *metric));
                }
                IndexKind::Secondary => {}
            }
        }
        Memtable {
            docs: Vec::new(),
            by_key: BTreeMap::new(),
            ordinals: Ordinals::default(),
            deletes: RwLock::new(DeleteLog::new()),
            text,
            vectors,
            bytes: 0,
            budget,
        }
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    pub fn num_vectors(&self) -> usize {
        self.vectors.values().map(|v| v.len()).max().unwrap_or(0)
    }

    /// The newest version of `sort_key`, ignoring visibility. The write path's
    /// read-before-write.
    pub fn find(&self, sort_key: &str) -> Option<u32> {
        self.by_key.get(sort_key).and_then(|v| v.last().copied())
    }

    /// The version of `sort_key` visible at `t`: committed at or before `t` and
    /// not deleted by then. Both conjuncts, always.
    pub fn find_at(&self, sort_key: &str, t: Timestamp) -> Option<u32> {
        let d = self.deletes.read().unwrap();
        self.by_key.get(sort_key)?.iter().rev().copied().find(|&o| {
            self.ordinals.commit_ts.get(o as usize).map(|c| *c <= t).unwrap_or(false)
                && !d.is_deleted_at(o, t)
        })
    }

    pub fn mark_deleted(&self, ord: u32, ts: Timestamp) {
        self.deletes.write().unwrap().mark(ord, ts);
    }

    pub fn is_deleted_at(&self, ord: u32, t: Timestamp) -> bool {
        self.deletes.read().unwrap().is_deleted_at(ord, t)
    }

    pub fn delete_entries(&self) -> Vec<(u32, Timestamp)> {
        self.deletes.read().unwrap().iter().collect()
    }

    /// Append one version. The caller has already resolved the previous version
    /// and marked it dead, in the same commit.
    pub fn insert(&mut self, sort_key: String, commit_ts: Timestamp, doc: Value) -> Result<u32> {
        let ord = self.docs.len() as u32;
        for (path, (ib, an)) in self.text.iter_mut() {
            let mut toks = Vec::new();
            if let Some(v) = doc.path(path) {
                analyze_field(v, *an, &mut toks);
            }
            ib.add_doc(ord, &toks);
        }
        for (path, vs) in self.vectors.iter_mut() {
            if let Some(v) = doc.path(path) {
                if let Some(mut f) = extract_vector(v) {
                    crate::vector::distance::prepare(vs.metric, &mut f);
                    vs.push(ord, &f)?;
                }
            }
        }
        let sz = doc.heap_size() + sort_key.len() + 64;
        self.bytes += sz;
        if let Some(b) = &self.budget {
            b.add(sz);
        }
        self.ordinals.push(sort_key.clone(), commit_ts);
        self.by_key.entry(sort_key.clone()).or_default().push(ord);
        self.docs.push(MemDoc { sort_key, commit_ts, doc });
        Ok(ord)
    }

    pub fn text_source(&self, path: &str) -> Option<TextSource<'_>> {
        let (ib, _) = self.text.get(path)?;
        Some(TextSource::Memory { terms: &ib.terms, doc_lens: &ib.doc_lens })
    }

    pub fn analyzer(&self, path: &str) -> Option<Analyzer> {
        self.text.get(path).map(|(_, a)| *a)
    }

    pub fn local_doc_freq(&self, path: &str, term: &str) -> u64 {
        self.text
            .get(path)
            .and_then(|(ib, _)| ib.terms.get(term))
            .map(|t| t.ords.len() as u64)
            .unwrap_or(0)
    }

    pub fn total_doc_len(&self, path: &str) -> u64 {
        self.text.get(path).map(|(ib, _)| ib.total_doc_len()).unwrap_or(0)
    }

    /// Evaluate a structured predicate. There are no columns here — the
    /// memtable is small and mutable, so predicates run over the values
    /// directly, through the same [`matches`] used by the variant fallback and
    /// mirrored by every column implementation.
    pub fn filter(&self, path: &str, op: CmpOp, lit: &Value) -> Bitmap {
        self.eval(path, op, lit, false)
    }

    /// `want_comparable` asks for the ordinals where the predicate is
    /// *defined* rather than true — the negation half of three-valued logic.
    pub fn eval(&self, path: &str, op: CmpOp, lit: &Value, want_comparable: bool) -> Bitmap {
        let mut out = Bitmap::new(self.docs.len());
        for (i, d) in self.docs.iter().enumerate() {
            let v = d.doc.path(path).cloned().unwrap_or(Value::Null);
            let hit = if want_comparable {
                crate::column::comparable(&v, op, lit)
            } else {
                matches(&v, op, lit)
            };
            if hit {
                out.set(i);
            }
        }
        out
    }

    /// The ordinal range covering a partition-key prefix. The memtable is not
    /// sorted, so this is a scan rather than a range — which is fine at
    /// memtable size and is exactly the cost the flush removes.
    pub fn key_prefix(&self, prefix: &str) -> Bitmap {
        let mut out = Bitmap::new(self.docs.len());
        for (i, d) in self.docs.iter().enumerate() {
            if d.sort_key.starts_with(prefix) {
                out.set(i);
            }
        }
        out
    }

    pub fn should_flush(&self, th: &FlushThresholds) -> bool {
        self.bytes >= th.max_bytes
            || self.num_vectors() >= th.max_vectors
            || self.budget.as_ref().map(|b| b.under_pressure()).unwrap_or(false)
    }

    /// Hand the contents to a segment builder. Documents already dead at
    /// `gc_horizon` are dropped rather than written — the flush is the cheapest
    /// place in the system to forget something.
    pub fn drain_into(&self, gc_horizon: Timestamp) -> Vec<PendingDoc> {
        let d = self.deletes.read().unwrap();
        self.docs
            .iter()
            .enumerate()
            .filter(|(i, _)| !d.is_deleted_at(*i as u32, gc_horizon))
            .map(|(_, d)| PendingDoc {
                sort_key: d.sort_key.clone(),
                commit_ts: d.commit_ts,
                doc: d.doc.clone(),
            })
            .collect()
    }

    pub fn release_budget(&self) {
        if let Some(b) = &self.budget {
            b.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{IndexDef, Metric};
    use crate::json;
    use crate::vector::SearchOpts;

    fn coll() -> Collection {
        let mut c = Collection::new("t", "id", None);
        c.indexes.push(IndexDef::new(
            "t_body",
            "body",
            IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::default(),
        ));
        c.indexes.push(IndexDef::new(
            "t_emb",
            "emb",
            IndexKind::Vector { dims: 4, metric: Metric::L2 },
            crate::residency::Tier::default(),
        ));
        c
    }

    #[test]
    fn fresh_writes_are_searchable_at_exact_recall() {
        let c = coll();
        let mut m = Memtable::new(&c, None);
        for i in 0..500 {
            let d = json::parse(&format!(
                r#"{{"id":"d{i}","body":"document number {i} about vectors","emb":[{},{},{},1.0]}}"#,
                i as f32 / 500.0,
                (i % 7) as f32,
                (i % 3) as f32
            ))
            .unwrap();
            m.insert(format!("d{i:04}"), 100 + i as u64, d).unwrap();
        }
        // Exact by construction: the flat list has no graph to approximate with.
        let vs = &m.vectors["emb"];
        assert!(vs.graph.is_none());
        let q = vs.vector(42).to_vec();
        let (hits, _) = vs.search(&q, 5, &Bitmap::all(500), &SearchOpts::default());
        assert_eq!(hits[0].0, 42);
        assert!(hits[0].1.abs() < 1e-6);

        let mut brute: Vec<(u32, f32)> = (0..500)
            .map(|i| (i as u32, crate::vector::distance::distance(Metric::L2, &q, vs.vector(i))))
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
        assert_eq!(
            hits.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            brute.iter().take(5).map(|(o, _)| *o).collect::<Vec<_>>()
        );
    }

    #[test]
    fn predicates_and_text_work_before_any_flush() {
        let c = coll();
        let mut m = Memtable::new(&c, None);
        for i in 0..20 {
            let d =
                json::parse(&format!(r#"{{"id":"d{i}","n":{i},"body":"quick brown fox {i}"}}"#))
                    .unwrap();
            m.insert(format!("d{i:02}"), 100 + i as u64, d).unwrap();
        }
        assert_eq!(m.filter("n", CmpOp::Ge, &Value::Int(15)).popcount(), 5);
        let src = m.text_source("body").unwrap();
        assert_eq!(src.doc_freq("quick"), 20);
        assert_eq!(src.doc_freq("fox"), 20);
    }

    #[test]
    fn node_budget_forces_early_flush() {
        let budget = MemtableBudget::new(1024);
        let c = coll();
        let mut m = Memtable::new(&c, Some(budget.clone()));
        let th = FlushThresholds { max_bytes: usize::MAX, max_vectors: usize::MAX };
        assert!(!m.should_flush(&th));
        for i in 0..50 {
            m.insert(
                format!("d{i:02}"),
                100,
                json::parse(&format!(r#"{{"id":"d{i}","body":"padding padding padding"}}"#))
                    .unwrap(),
            )
            .unwrap();
        }
        // The tablet's own thresholds are untouched; the node's are not.
        assert!(budget.under_pressure());
        assert!(m.should_flush(&th));
        m.release_budget();
        assert!(!budget.under_pressure());
    }

    #[test]
    fn concurrent_releases_cannot_wrap_the_budget_below_zero() {
        use std::sync::Barrier;
        const THREADS: usize = 4;
        const ROUNDS: usize = 500;
        let budget = MemtableBudget::new(1024);
        // Each round hands out one tablet's worth and then has every thread
        // release it at the same instant. Clamping the subtraction against a
        // separate load lets more than one of them subtract the whole total,
        // and the wrap leaves `used` near `usize::MAX` for good.
        let start = Arc::new(Barrier::new(THREADS + 1));
        let done = Arc::new(Barrier::new(THREADS + 1));
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let b = budget.clone();
            let start = start.clone();
            let done = done.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    start.wait();
                    b.release(100);
                    done.wait();
                }
            }));
        }
        let mut wrapped = false;
        for _ in 0..ROUNDS {
            budget.add(100);
            start.wait();
            done.wait();
            // Checked here rather than only at the end: once it wraps it stays
            // wrapped, but the round that did it is the interesting one.
            wrapped |= budget.used() != 0;
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(!wrapped, "a concurrent release drove the budget past zero");
        assert!(!budget.under_pressure());
    }
}
