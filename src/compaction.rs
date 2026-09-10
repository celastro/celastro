//! Compaction, shaped by vector build cost.
//!
//! Leveled compaction repeatedly rewrites large segments, and rebuilding a
//! graph over tens of millions of vectors is hours of CPU. So: **size-tiered
//! compaction with a hard segment cap** (§9.1). Segments at the cap are never
//! merged again. Search cost grows with segment count, but each build is
//! bounded and predictable — which is the trade that matters, because an
//! unbounded build is not slow, it is an outage.
//!
//! Three triggers, in priority order:
//!
//! 1. **Dead ratio** above threshold (§4.4). This one is not about space: it is
//!    what keeps the visibility `k`-amplification in §6 bounded, and therefore
//!    what stops recall from decaying silently as deletes accumulate.
//! 2. **Size tiers**: `fanout` segments at one level merge into one at the next.
//! 3. **Format and parameter upgrades** (§12.3) — a rolling rebuild, never a
//!    migration.

use crate::error::Result;
use crate::segment::{PendingDoc, Segment, SegmentBuilder};
use crate::shard::Shard;
use crate::time::Timestamp;

#[derive(Debug, Clone, Copy)]
pub struct CompactionOpts {
    /// Segments at one level before they merge.
    pub tier_fanout: usize,
    /// The hard cap. Production order is 5–10M vectors; the number that matters
    /// is that it exists.
    pub segment_cap: usize,
    /// Dead ratio above which a segment is rewritten on its own.
    pub dead_ratio: f64,
    /// Rewrite every segment whose format version is older than this, oldest
    /// first. Zero disables; values above the current `FORMAT_VERSION` are
    /// clamped to it, since a rewrite cannot produce a format this build does
    /// not know how to write.
    pub upgrade_below_format: u32,
}

impl Default for CompactionOpts {
    fn default() -> Self {
        CompactionOpts {
            tier_fanout: 4,
            segment_cap: 5_000_000,
            dead_ratio: 0.30,
            upgrade_below_format: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    /// Rewrite one segment to drop its dead documents, or to move it to a newer
    /// format or index parameters.
    Rewrite { input: u64, reason: Reason },
    /// Merge a tier.
    Merge { inputs: Vec<u64>, level: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    DeadRatio,
    FormatUpgrade,
}

/// A segment at the cap is retired from merging. Its dead documents are still
/// collected, because that trigger is about recall, not size.
fn at_cap(num_docs: usize, num_vectors: usize, opts: &CompactionOpts) -> bool {
    num_docs.max(num_vectors) >= opts.segment_cap
}

/// The horizon version GC actually runs at: a backup pins `gc_horizon`, which
/// holds it back (§12.5), and otherwise it is now.
fn retain_horizon(shard: &Shard, now: Timestamp) -> Timestamp {
    if shard.opts.gc_horizon > 0 {
        shard.opts.gc_horizon.min(now)
    } else {
        now
    }
}

pub fn plan(shard: &Shard, t: Timestamp, opts: &CompactionOpts) -> Option<Job> {
    // 1. Dead ratio, worst first, measured at the horizon a rewrite could
    // actually collect at rather than at `t`. With `gc_horizon` pinned, rows
    // that died after it are carried into the output unchanged, so the
    // rewritten segment has the same ratio at `t`, is selected again on the
    // next pass, and the scheduler spends its whole budget rebuilding one
    // segment's graph until the backup finishes. That is the spin the format
    // clamp below exists to prevent, in its other form.
    let horizon = retain_horizon(shard, t);
    let mut worst: Option<(u64, f64)> = None;
    for h in &shard.segments {
        let r = h.dead_ratio(horizon);
        if r > opts.dead_ratio && worst.map(|(_, w)| r > w).unwrap_or(true) {
            worst = Some((h.id(), r));
        }
    }
    if let Some((id, _)) = worst {
        return Some(Job::Rewrite { input: id, reason: Reason::DeadRatio });
    }

    // 2. Format upgrades. Clamped to what this build can actually write: a
    // rewrite produces `FORMAT_VERSION`, so asking for anything beyond it would
    // re-select the same segment forever and spin the scheduler.
    let target = opts.upgrade_below_format.min(crate::segment::FORMAT_VERSION);
    if target > 0 {
        if let Some(h) = shard
            .segments
            .iter()
            .filter(|h| h.segment.format_version < target)
            .min_by_key(|h| h.id())
        {
            return Some(Job::Rewrite { input: h.id(), reason: Reason::FormatUpgrade });
        }
    }

    // 3. Size tiers.
    let mut by_level: std::collections::BTreeMap<u32, Vec<u64>> = Default::default();
    for h in &shard.segments {
        if at_cap(h.segment.num_docs(), h.segment.num_vectors(), opts) {
            continue;
        }
        by_level.entry(h.segment.level).or_default().push(h.id());
    }
    for (level, ids) in by_level {
        if ids.len() >= opts.tier_fanout {
            let inputs: Vec<u64> = ids.into_iter().take(opts.tier_fanout).collect();
            return Some(Job::Merge { inputs, level: level + 1 });
        }
    }
    None
}

/// Run one job.
///
/// Version GC happens here and nowhere else. Documents not visible at
/// `retain_from` are dropped; readers that pinned an older snapshot are
/// unaffected because they hold `Arc`s to the *input* segments, which
/// [`Shard::install_compaction`] does not unlink while anyone still references
/// them. A backup pins `gc_horizon`, which holds `retain_from` back (§12.5),
/// and a version superseded after it is kept as well — in an output segment of
/// its own, because one segment holds one version per key.
pub fn run(shard: &mut Shard, job: &Job, opts: &CompactionOpts) -> Result<()> {
    let now = shard.clock.peek();
    let retain_from = retain_horizon(shard, now);
    let (inputs, out_level) = match job {
        Job::Rewrite { input, .. } => {
            let level = shard
                .segments
                .iter()
                .find(|h| h.id() == *input)
                .map(|h| h.segment.level)
                .unwrap_or(0);
            (vec![*input], level)
        }
        Job::Merge { inputs, level } => (inputs.clone(), *level),
    };

    let (mut docs, carried) = shard.collect_for_compaction(&inputs, retain_from)?;
    // Newest version of a key first, which is what makes the depth below count
    // versions down from the survivor.
    docs.sort_by(|a, b| a.sort_key.cmp(&b.sort_key).then(b.commit_ts.cmp(&a.commit_ts)));

    // A pinned horizon makes `collect_for_compaction` carry versions that were
    // superseded after it, so the same key can arrive here more than once —
    // and `SegmentBuilder::build` keeps only the newest version of a key.
    // Writing them into one segment would therefore drop exactly the rows the
    // horizon was pinned to preserve, silently. So version `d` of every key
    // goes to output layer `d` instead: each output still holds one version per
    // key, and a reader at the horizon finds the version that was live for it
    // because the newer one is not yet visible to it.
    let mut depths: Vec<usize> = Vec::with_capacity(docs.len());
    let mut prev: Option<&str> = None;
    for pd in &docs {
        let d = match prev {
            Some(k) if k == pd.sort_key.as_str() => depths[depths.len() - 1] + 1,
            _ => 0,
        };
        prev = Some(pd.sort_key.as_str());
        depths.push(d);
    }
    let mut layers: Vec<Vec<PendingDoc>> = Vec::new();
    for (pd, d) in docs.into_iter().zip(depths) {
        while layers.len() <= d {
            layers.push(Vec::new());
        }
        layers[d].push(pd);
    }

    let mut outputs: Vec<Segment> = Vec::new();
    if layers.is_empty() {
        shard.install_compaction(&inputs, outputs, &carried)?;
        return Ok(());
    }
    // Split at the cap rather than producing one oversized segment: this is
    // the point of the policy, and it is the only place the cap is enforced.
    let chunk = opts.segment_cap.max(1);
    let coll = shard.coll.clone();
    // Deepest layer first, so the surviving version of a key still lands in the
    // highest-numbered output. Only that layer is promoted: a superseded
    // version stays at the level the merge drained, because a merge that put
    // `tier_fanout` segments back at its output level would have the tier
    // trigger re-select its own output forever. Since `k` inputs hold at most
    // `k` versions of a key, a merge leaves at most `k - 1` segments behind and
    // the level it drains strictly shrinks.
    for (depth, layer) in layers.into_iter().enumerate().rev() {
        let level = if depth == 0 { out_level } else { out_level.saturating_sub(1) };
        let mut rest = layer;
        while !rest.is_empty() {
            let take = chunk.min(rest.len());
            let piece: Vec<PendingDoc> = rest.drain(..take).collect();
            let id = shard.next_segment_id;
            shard.next_segment_id += 1;
            let mut b = SegmentBuilder::new(shard.opts.build);
            for pd in piece {
                b.add(pd);
            }
            let seg = b.build(id, level, &coll)?;
            outputs.push(seg);
        }
    }
    shard.install_compaction(&inputs, outputs, &carried)?;
    Ok(())
}

/// Run jobs until the shard is quiet, bounded so a pathological policy cannot
/// spin. Compaction is scheduled and rate-limited, never an invisible
/// background process (§12.1) — this is the scheduler's inner loop.
pub fn run_to_quiescence(
    shard: &mut Shard,
    opts: &CompactionOpts,
    max_jobs: usize,
) -> Result<usize> {
    let mut n = 0;
    while n < max_jobs {
        let t = shard.clock.peek();
        let Some(job) = plan(shard, t, opts) else { break };
        run(shard, &job, opts)?;
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Collection, ColumnDef, IndexDef, IndexKind, Metric};
    use crate::json;
    use crate::shard::{ShardOpts, KEY_SEP};
    use crate::time::Hlc;
    use crate::value::{Value, ValueType};
    use std::sync::Arc;

    fn coll() -> Collection {
        let mut c = Collection::new("a", "id", Some("tenant_id".into()));
        c.declared.push(ColumnDef { path: "tenant_id".into(), ty: ValueType::Str, not_null: true });
        c.indexes.push(IndexDef::new(
            "b",
            "body",
            IndexKind::FullText { analyzer: "english".into() },
            crate::residency::Tier::default(),
        ));
        c.indexes.push(IndexDef::new(
            "e",
            "emb",
            IndexKind::Vector { dims: 4, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        c
    }

    fn doc(i: usize) -> Value {
        json::parse(&format!(
            r#"{{"id":"d{i:05}","tenant_id":"t0","body":"doc {i} text","emb":[{},1.0,0.5,0.25]}}"#,
            i as f32 / 100.0
        ))
        .unwrap()
    }

    /// The same document as [`doc`], with a different body — an update to it.
    fn doc_with_body(i: usize, body: &str) -> Value {
        json::parse(&format!(
            r#"{{"id":"d{i:05}","tenant_id":"t0","body":"{body}","emb":[0.0,1.0,0.5,0.25]}}"#
        ))
        .unwrap()
    }

    fn body_at(s: &Shard, key: &str, t: Timestamp) -> Option<String> {
        let doc = s.get(key, t).unwrap()?;
        doc.path("body").and_then(|v| v.as_str()).map(|b| b.to_string())
    }

    fn shard_with(n_segments: usize, per: usize) -> Shard {
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        for seg in 0..n_segments {
            for i in 0..per {
                s.insert(doc(seg * per + i)).unwrap();
            }
            s.flush().unwrap();
        }
        s
    }

    #[test]
    fn size_tiers_merge_at_the_fanout() {
        let mut s = shard_with(4, 25);
        let opts = CompactionOpts::default();
        assert_eq!(s.segments.len(), 4);
        let job = plan(&s, s.clock.peek(), &opts).unwrap();
        assert_eq!(job, Job::Merge { inputs: vec![1, 2, 3, 4], level: 1 });
        run(&mut s, &job, &opts).unwrap();
        assert_eq!(s.segments.len(), 1);
        assert_eq!(s.segments[0].segment.level, 1);
        assert_eq!(s.segments[0].segment.num_docs(), 100);
        assert_eq!(s.num_docs(s.clock.peek()), 100);
        // And nothing more to do.
        assert!(plan(&s, s.clock.peek(), &opts).is_none());
    }

    #[test]
    fn the_cap_stops_further_merging() {
        let s = shard_with(4, 25);
        // A cap below the tier size: the merge output is split, and the pieces
        // are then excluded from further merges even though there are four.
        let opts = CompactionOpts { segment_cap: 25, ..Default::default() };
        let job = plan(&s, s.clock.peek(), &opts);
        // Every input is already at the cap, so there is nothing to merge.
        assert!(job.is_none(), "{job:?}");

        let mut s2 = shard_with(4, 10);
        let opts2 = CompactionOpts { segment_cap: 15, ..Default::default() };
        let job = plan(&s2, s2.clock.peek(), &opts2).unwrap();
        run(&mut s2, &job, &opts2).unwrap();
        // 40 documents, cap 15 → three segments, all at or under the cap.
        assert_eq!(s2.segments.len(), 3);
        assert!(s2.segments.iter().all(|h| h.segment.num_docs() <= 15));
        assert_eq!(s2.num_docs(s2.clock.peek()), 40);
        // Two of the three are at the cap and retire; one is not, and one
        // segment is below the fanout, so the policy stops.
        assert!(plan(&s2, s2.clock.peek(), &opts2).is_none());
    }

    #[test]
    fn dead_ratio_triggers_a_rewrite_and_reclaims() {
        let mut s = shard_with(1, 100);
        for i in 0..40 {
            s.delete(&format!("t0{KEY_SEP}d{i:05}")).unwrap();
        }
        let t = s.clock.peek();
        let opts = CompactionOpts::default();
        assert!(s.segments[0].dead_ratio(t) > 0.3);
        let job = plan(&s, t, &opts).unwrap();
        assert_eq!(job, Job::Rewrite { input: 1, reason: Reason::DeadRatio });
        run(&mut s, &job, &opts).unwrap();
        assert_eq!(s.segments.len(), 1);
        // The dead documents are gone from the file, not just hidden.
        assert_eq!(s.segments[0].segment.num_docs(), 60);
        assert_eq!(s.segments[0].dead_ratio(s.clock.peek()), 0.0);
        assert_eq!(s.num_docs(s.clock.peek()), 60);
        assert!(s.get(&format!("t0{KEY_SEP}d00000"), s.clock.peek()).unwrap().is_none());
        assert!(s.get(&format!("t0{KEY_SEP}d00099"), s.clock.peek()).unwrap().is_some());
    }

    #[test]
    fn a_reader_pinned_before_compaction_still_reads() {
        let mut s = shard_with(4, 25);
        let snap = s.snapshot();
        let held: Vec<_> = snap.segments.clone();
        let opts = CompactionOpts::default();
        let job = plan(&s, s.clock.peek(), &opts).unwrap();
        run(&mut s, &job, &opts).unwrap();
        // The shard no longer lists them, but the pinned handles are alive and
        // still answer — superseded segments stay available until no reader
        // references them (§4.4).
        assert_eq!(s.segments.len(), 1);
        assert_eq!(held.len(), 4);
        assert_eq!(held[0].segment.num_docs(), 25);
        assert_eq!(
            held[0].segment.document(0).unwrap().path("id").unwrap().as_str(),
            Some("d00000")
        );
    }

    #[test]
    fn a_format_upgrade_target_this_build_cannot_write_does_not_spin() {
        let mut s = shard_with(2, 10);
        // Asking to upgrade "below 99" when this build writes version 1: a
        // rewrite produces version 1, which is still below 99, so an unclamped
        // policy re-selects the same segment forever and burns the whole job
        // budget rewriting it.
        let opts = CompactionOpts { upgrade_below_format: 99, ..Default::default() };
        assert!(
            plan(&s, s.clock.peek(), &opts).is_none(),
            "there is nothing this build can upgrade a version-1 segment to"
        );
        assert_eq!(run_to_quiescence(&mut s, &opts, 64).unwrap(), 0);
        assert_eq!(s.segments.len(), 2);
        assert_eq!(s.num_docs(s.clock.peek()), 20);
    }

    #[test]
    fn a_pinned_gc_horizon_retains_dead_rows_without_erasing_new_ones() {
        let mut s = shard_with(2, 20);
        // A backup pins the horizon here, then work continues.
        let horizon = s.clock.peek();
        s.delete(&format!("t0{KEY_SEP}d00000")).unwrap();
        for i in 100..120 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        s.opts.gc_horizon = horizon;

        let before = s.num_docs(s.clock.peek());
        assert_eq!(before, 59, "60 written, 1 deleted");
        assert!(
            s.get(&format!("t0{KEY_SEP}d00000"), horizon).unwrap().is_some(),
            "PRE-COMPACTION: reader at the horizon should still see the row"
        );
        let opts = CompactionOpts { tier_fanout: 2, ..Default::default() };
        run_to_quiescence(&mut s, &opts, 8).unwrap();

        // Everything written after the horizon survives...
        assert_eq!(s.num_docs(s.clock.peek()), 59);
        assert!(s.get(&format!("t0{KEY_SEP}d00119"), s.clock.peek()).unwrap().is_some());
        // ...and the row that died after the horizon is retained but still
        // invisible, so a reader at the horizon could still see it.
        assert!(s.get(&format!("t0{KEY_SEP}d00000"), s.clock.peek()).unwrap().is_none());
        assert!(s.get(&format!("t0{KEY_SEP}d00000"), horizon).unwrap().is_some());
    }

    #[test]
    fn a_pinned_gc_horizon_does_not_rewrite_the_same_segment_forever() {
        let mut s = shard_with(1, 100);
        // A backup pins the horizon, and then most of the segment dies.
        let horizon = s.clock.peek();
        for i in 0..40 {
            s.delete(&format!("t0{KEY_SEP}d{i:05}")).unwrap();
        }
        s.opts.gc_horizon = horizon;

        let t = s.clock.peek();
        let opts = CompactionOpts::default();
        assert!(s.segments[0].dead_ratio(t) > 0.3, "the rows are dead as of now...");
        // ...but not one of them may be collected yet, so a rewrite would
        // reproduce the segment it read, the planner would select it again on
        // the next pass, and the whole job budget would go on rebuilding one
        // segment's graph for as long as the backup runs.
        assert!(plan(&s, t, &opts).is_none(), "nothing to reclaim at the pinned horizon");
        assert_eq!(run_to_quiescence(&mut s, &opts, 64).unwrap(), 0);
        assert_eq!(s.segments.len(), 1);
        assert_eq!(s.segments[0].segment.num_docs(), 100);

        // Releasing the horizon reclaims them, in one job.
        s.opts.gc_horizon = 0;
        assert_eq!(run_to_quiescence(&mut s, &opts, 64).unwrap(), 1);
        assert_eq!(s.segments.len(), 1);
        assert_eq!(s.segments[0].segment.num_docs(), 60);
    }

    #[test]
    fn merging_an_updated_key_keeps_the_version_a_pinned_horizon_still_reads() {
        let mut s = shard_with(1, 4);
        // The horizon is pinned first, and only then is the key updated: the
        // old version dies *after* the horizon, so a reader there must still
        // see it. An update is not a delete — nothing marks the new version
        // dead, so the old one only survives if it reaches its own segment.
        let horizon = s.clock.peek();
        s.insert(doc_with_body(0, "second version")).unwrap();
        s.flush().unwrap();
        s.opts.gc_horizon = horizon;

        let key = format!("t0{KEY_SEP}d00000");
        assert_eq!(
            body_at(&s, &key, horizon).as_deref(),
            Some("doc 0 text"),
            "PRE-MERGE: the reader at the horizon sees the version that was live for it"
        );

        let opts = CompactionOpts { tier_fanout: 2, ..Default::default() };
        assert_eq!(run_to_quiescence(&mut s, &opts, 8).unwrap(), 1, "one merge, and no spin");

        // Two outputs, because one segment holds at most one version of a key.
        assert_eq!(s.segments.len(), 2);
        assert_eq!(body_at(&s, &key, horizon).as_deref(), Some("doc 0 text"));
        assert_eq!(body_at(&s, &key, s.clock.peek()).as_deref(), Some("second version"));
        // Exactly one version is visible at either timestamp: retaining the
        // superseded row must not double-count the key.
        assert_eq!(s.num_docs(horizon), 4);
        assert_eq!(s.num_docs(s.clock.peek()), 4);
    }

    #[test]
    fn compaction_unlinks_the_files_it_retired() {
        let dir = std::env::temp_dir().join(format!("celastro-gc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        s.attach_dir(&dir).unwrap();
        for seg in 0..4 {
            for i in 0..10 {
                s.insert(doc(seg * 10 + i)).unwrap();
            }
            s.flush().unwrap();
        }
        s.delete(&format!("t0{KEY_SEP}d00000")).unwrap();
        s.persist_manifest().unwrap();
        let count = |p: &str| std::fs::read_dir(dir.join(p)).unwrap().count();
        assert_eq!(count("segments"), 4);
        run_to_quiescence(&mut s, &CompactionOpts::default(), 8).unwrap();
        assert_eq!(s.segments.len(), 1);
        assert_eq!(count("segments"), 1, "retired segment files must be unlinked");
        assert_eq!(count("deletes"), 0, "and so must their delete logs");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn everything_is_still_searchable_after_compaction() {
        use crate::bitmap::Bitmap;
        use crate::vector::SearchOpts;
        let mut s = shard_with(4, 30);
        let opts = CompactionOpts::default();
        run_to_quiescence(&mut s, &opts, 8).unwrap();
        let t = s.clock.peek();
        let snap = s.snapshot_at(t);
        let sources = s.sources(&snap);
        let mut total = 0;
        for src in &sources {
            let vis = src.visibility(t);
            if let Some(vs) = src.vector_handle("emb").unwrap() {
                let q = vec![0.5f32, 1.0, 0.5, 0.25];
                let (hits, _) = vs.search(&q, 5, &vis, &SearchOpts::default());
                total += hits.len();
            }
            let h = src.text_handle("body").unwrap();
            if let Some(txt) = h.as_ref().and_then(|x| x.source("body")) {
                assert!(txt.doc_freq("text") > 0 || vis.popcount() == 0);
            }
            let _ = Bitmap::all(src.num_docs());
        }
        assert!(total > 0);
        assert_eq!(s.num_docs(t), 120);
    }
}
