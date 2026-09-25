//! The recall harness.
//!
//! > The recall harness is the most important artifact in the sequence. Every
//! > later optimization is a potential silent recall regression, and every
//! > distributed criterion is checked against it. (§14)
//!
//! Two things live here:
//!
//! * [`measure_recall`] — the continuous production measurement of §12.1.
//!   Sample a fraction of production vector queries, re-execute them with exact
//!   brute force in the background, and report recall@k as a time series per
//!   collection. This is the only way to catch the silent regressions from
//!   deletes, quantization and compaction-policy changes: none of them fails a
//!   test, and none of them shows up in a benchmark on fresh data.
//! * Shard-count comparison — the distributed exit criterion. In exact mode,
//!   results must be bit-identical regardless of shard count. Lives in the
//!   integration suite rather than here; this module carries the recall half.

use std::collections::BTreeSet;

use crate::bitmap::Bitmap;
use crate::codec::Rng;
use crate::engine::{Db, LoggedVectorQuery};
use crate::error::{Error, Result};
use crate::vector::{distance, SearchOpts};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RecallReport {
    pub collection: String,
    pub path: String,
    pub k: usize,
    pub samples: usize,
    /// Mean recall@k against exact brute force.
    pub recall: f64,
    /// The worst single query. A good mean hides a filter shape that returns
    /// nothing.
    pub worst: f64,
    /// Queries replayed from the production sample versus synthesised.
    pub from_production_log: usize,
    pub notes: Vec<String>,
}

impl RecallReport {
    pub fn render(&self) -> String {
        format!(
            "recall@{k} on {c}.{p}: mean {r:.4}, worst {w:.4} over {n} sample(s) \
             ({log} replayed from the production query log)\n{notes}",
            k = self.k,
            c = self.collection,
            p = self.path,
            r = self.recall,
            w = self.worst,
            n = self.samples,
            log = self.from_production_log,
            notes = self.notes.iter().map(|n| format!("  note: {n}\n")).collect::<String>()
        )
    }
}

/// Measure recall@k for a collection's vector index.
///
/// The comparison is against exact brute force over **every** searchable unit
/// at the same snapshot and through the same visibility bitmap — so what is
/// measured is the approximation introduced by the index, not a difference in
/// what is visible.
pub fn measure_recall(
    db: &mut Db,
    collection: &str,
    k: usize,
    samples: usize,
) -> Result<RecallReport> {
    let coll = db.catalog.get(collection)?.clone();
    let path = coll
        .indexes
        .iter()
        .find(|i| matches!(i.kind, crate::catalog::IndexKind::Vector { .. }))
        .map(|i| i.path.clone())
        .ok_or_else(|| Error::Plan(format!("`{collection}` has no vector index to measure")))?;

    let logged: Vec<LoggedVectorQuery> =
        db.logged_queries(collection).into_iter().filter(|q| q.path == path).collect();
    let mut notes = Vec::new();
    let ts = db.read_ts();

    // Build the query set: production queries first, synthesised from stored
    // vectors only to make up the numbers.
    let mut queries: Vec<Vec<f32>> =
        logged.iter().rev().take(samples).map(|q| q.query.clone()).collect();
    let from_log = queries.len();
    if queries.len() < samples {
        notes.push(format!(
            "only {} sampled production quer{} available; the rest are synthesised from stored \
             vectors, which is a friendlier distribution than the real one",
            from_log,
            if from_log == 1 { "y" } else { "ies" }
        ));
        let mut rng = Rng::new(0xA11CE ^ ts);
        let shards = db.shards(collection)?;
        let mut pool: Vec<Vec<f32>> = Vec::new();
        for s in shards {
            let snap = s.snapshot_at(ts);
            for unit in s.sources(&snap) {
                if let Some(vs) = unit.vector_handle(&path)? {
                    let vis = unit.visibility(ts);
                    for v in vis.iter().take(256) {
                        // Map document ordinal back to a vector ordinal.
                        if let Some(vi) = vs.vec_to_doc.iter().position(|d| *d == v) {
                            pool.push(vs.vector(vi).to_vec());
                        }
                    }
                }
            }
        }
        while queries.len() < samples && !pool.is_empty() {
            let i = rng.next_usize(pool.len());
            // Perturb, so the query is near a real vector rather than exactly
            // one of them — an exact hit is the easiest possible query.
            let mut q = pool[i].clone();
            for x in q.iter_mut() {
                *x += rng.next_normal() * 0.05;
            }
            distance::prepare(coll.vector_metric(&path).unwrap(), &mut q);
            queries.push(q);
        }
    }
    if queries.is_empty() {
        return Err(Error::Plan(format!(
            "`{collection}` has no vectors to measure recall against"
        )));
    }

    let mut total = 0.0f64;
    let mut worst = 1.0f64;
    let n = queries.len();
    {
        let shards = db.shards(collection)?;
        for q in &queries {
            let approx = search_all(shards, &path, q, k, ts, false)?;
            let exact = search_all(shards, &path, q, k, ts, true)?;
            let a: BTreeSet<&String> = approx.iter().map(|(key, _)| key).collect();
            let e: BTreeSet<&String> = exact.iter().map(|(key, _)| key).collect();
            let hits = a.intersection(&e).count();
            let r = if e.is_empty() { 1.0 } else { hits as f64 / e.len() as f64 };
            total += r;
            worst = worst.min(r);
        }
    }

    Ok(RecallReport {
        collection: collection.to_string(),
        path,
        k,
        samples: n,
        recall: total / n as f64,
        worst,
        from_production_log: from_log,
        notes,
    })
}

/// Run one vector query across every shard and unit, either through the index
/// or by exact brute force, and merge globally.
fn search_all(
    shards: &[crate::shard::Shard],
    path: &str,
    query: &[f32],
    k: usize,
    ts: crate::time::Timestamp,
    exact: bool,
) -> Result<Vec<(String, f32)>> {
    let opts = SearchOpts { exact, ..Default::default() };
    let mut all: Vec<(String, f32)> = Vec::new();
    for s in shards {
        let snap = s.snapshot_at(ts);
        for unit in s.sources(&snap) {
            let n = unit.num_docs();
            if n == 0 {
                continue;
            }
            let Some(vs) = unit.vector_handle(path)? else { continue };
            let vis = unit.visibility(ts);
            let (hits, _) = vs.search(query, k, &vis, &opts);
            for (ord, d) in hits {
                if let Some(key) = unit.key(ord) {
                    all.push((key.to_string(), d));
                }
            }
        }
    }
    all.sort_by(|a, b| {
        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
    });
    all.dedup_by(|a, b| a.0 == b.0);
    all.truncate(k);
    Ok(all)
}

/// Exact-mode brute force over an explicit filter, for tests that want ground
/// truth without going through the planner.
pub fn ground_truth(
    shards: &[crate::shard::Shard],
    path: &str,
    query: &[f32],
    k: usize,
    ts: crate::time::Timestamp,
    extra_filter: Option<&dyn Fn(&str) -> bool>,
) -> Result<Vec<(String, f32)>> {
    let mut all: Vec<(String, f32)> = Vec::new();
    for s in shards {
        let snap = s.snapshot_at(ts);
        for unit in s.sources(&snap) {
            let Some(vs) = unit.vector_handle(path)? else { continue };
            let vis = unit.visibility(ts);
            let mut f = Bitmap::new(unit.num_docs());
            for ord in vis.iter() {
                let key = unit.key(ord).unwrap_or("");
                if extra_filter.map(|g| g(key)).unwrap_or(true) {
                    f.set(ord as usize);
                }
            }
            let (hits, _) =
                vs.search(query, k, &f, &SearchOpts { exact: true, ..Default::default() });
            for (ord, d) in hits {
                if let Some(key) = unit.key(ord) {
                    all.push((key.to_string(), d));
                }
            }
        }
    }
    all.sort_by(|a, b| {
        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
    });
    all.truncate(k);
    Ok(all)
}
