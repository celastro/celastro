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

pub fn plan(shard: &Shard, t: Timestamp, opts: &CompactionOpts) -> Option<Job> {
    // 1. Dead ratio, worst first, measured at the horizon a rewrite could
    // actually collect at rather than at `t`. With `gc_horizon` pinned, rows
    // that died after it are carried into the output unchanged, so the
    // rewritten segment has the same ratio at `t`, is selected again on the
    // next pass, and the scheduler spends its whole budget rebuilding one
    // segment's graph until the backup finishes. That is the spin the format
    // clamp below exists to prevent, in its other form.
    let horizon = shard.retain_from(t);
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
    let retain_from = shard.retain_from(now);
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

    let (docs, carried) = shard.collect_for_compaction(&inputs, retain_from)?;

    // A pinned horizon makes `collect_for_compaction` carry versions that were
    // superseded after it, so the same key can arrive here more than once —
    // and `SegmentBuilder::build` keeps only the newest version of a key.
    // Writing them into one segment would therefore drop exactly the rows the
    // horizon was pinned to preserve, silently. So version `d` of every key
    // goes to output layer `d` instead: each output still holds one version per
    // key, and a reader at the horizon finds the version that was live for it
    // because the newer one is not yet visible to it.
    let layers = crate::segment::layer_by_version(docs);

    let mut outputs: Vec<Segment> = Vec::new();
    if layers.is_empty() {
        shard.install_compaction(&inputs, outputs, &carried, retain_from)?;
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
    shard.install_compaction(&inputs, outputs, &carried, retain_from)?;
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
    use crate::codec::Rng;
    use crate::json;
    use crate::shard::{ShardOpts, KEY_SEP};
    use crate::time::{Hlc, MAX_TS};
    use crate::value::{Value, ValueType};
    use std::collections::{BTreeMap, BTreeSet};
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

    #[test]
    fn a_compaction_that_fails_to_build_does_not_claim_it_collected() {
        // `retain_floor` says "at or above me nothing has been collected", so
        // it may only move at the point the collection becomes reader-visible.
        // Raising it next to `collect_for_compaction` — which only decides what
        // to drop — claims a collection that a later build error abandons.
        let mut s = shard_with(2, 20);
        let opts = CompactionOpts { tier_fanout: 2, ..Default::default() };
        s.insert(doc(999)).unwrap();
        let before = s.retain_floor;
        let job = plan(&s, s.clock.peek(), &opts).expect("two segments at a fanout of two");
        // Every stored vector has four dimensions, so building under a catalog
        // that declares eight fails — after the collection has been decided and
        // before anything is installed.
        let mut wrong = coll();
        wrong.indexes.retain(|i| i.path != "emb");
        wrong.indexes.push(IndexDef::new(
            "e",
            "emb",
            IndexKind::Vector { dims: 8, metric: Metric::Cosine },
            crate::residency::Tier::default(),
        ));
        s.coll = wrong;
        assert!(
            run(&mut s, &job, &opts).is_err(),
            "the build has to fail for this to test anything"
        );
        assert_eq!(s.segments.len(), 2, "nothing was installed");
        assert_eq!(
            s.retain_floor, before,
            "the floor claimed a collection that never reached a reader"
        );
    }

    #[test]
    fn a_compaction_raises_the_retain_floor_a_flush_did_not() {
        // `retain_floor` is documented as maxed over every flush *and*
        // compaction, but the two halves are easy to confuse: in any schedule
        // where a flush precedes the jobs at the same clock reading, the flush
        // has already set the floor to the value the compaction would have set,
        // and deleting compaction's half changes nothing anywhere. So tick the
        // clock after the last seal: from here only the compaction can move the
        // floor to where it ends up.
        let mut s = shard_with(2, 20);
        let opts = CompactionOpts { tier_fanout: 2, ..Default::default() };
        let after_flush = s.retain_floor;
        assert!(after_flush > 0, "the seals collected at their own horizon");

        // A write neither seal saw, so `retain_from(now)` is now strictly above
        // the floor the last one left.
        s.insert(doc(999)).unwrap();
        let now = s.clock.peek();
        assert!(now > after_flush, "the clock did not move");

        let job = plan(&s, now, &opts).expect("two segments at a fanout of two");
        run(&mut s, &job, &opts).unwrap();
        assert!(
            s.retain_floor > after_flush,
            "the compaction collected at {} and left the floor at {after_flush}",
            s.retain_from(s.clock.peek())
        );
        // Not equality against a fresh reading: the clock is an HLC and moves
        // on its own, so `run` collected at some instant at or after `now`.
        assert!(
            s.retain_floor >= now,
            "the floor names {} but the collection ran at or after {now}",
            s.retain_floor
        );
    }

    #[test]
    fn a_pin_below_the_floor_does_not_walk_the_floor_backwards() {
        // The documented backup workflow arrives exactly here: the shard has
        // been running unpinned, so every seal left the floor at its own `now`,
        // and then an operator pins `gc_horizon` at an older instant to hold
        // history still while the copy is taken. The pin holds the *next*
        // collection back — it cannot un-collect the ones that already ran. A
        // floor that followed the pin down would promise history nothing kept:
        // "at or above me nothing has been collected" is a claim only a floor
        // that never moves backwards can make.
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        let early = s.clock.peek();
        for i in 0..20 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        let floor = s.retain_floor;
        assert!(early < floor, "the unpinned seal reports the horizon it ran at");

        // The backup pin, below where this shard has already collected.
        s.opts.gc_horizon = early;
        for i in 20..40 {
            s.insert(doc(i)).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(
            s.retain_floor, floor,
            "a seal under the backup pin walked the floor back to {early}"
        );

        // And the other collector, which raises the floor on a line of its own.
        let opts = CompactionOpts { tier_fanout: 2, ..Default::default() };
        let job = plan(&s, s.clock.peek(), &opts).expect("two segments at a fanout of two");
        run(&mut s, &job, &opts).unwrap();
        assert_eq!(
            s.retain_floor, floor,
            "a compaction under the backup pin walked the floor back to {early}"
        );
    }

    // ------------------------------------------------------------------
    // The version-preservation property (R4).
    //
    // The theorem: let `retain_floor` be the highest horizon any *version-
    // collecting* operation has actually collected at — a flush or a
    // compaction job, and only those, since a write only ever adds a version
    // and marks the previous one dead at its own commit timestamp. Then for
    // every key and every `t >= retain_floor`, no operation changes what
    // `get(key, t)` returns, and what it returns is the version the write path
    // itself said was live at `t`.
    //
    // The floor is what makes that statement true rather than merely
    // optimistic. "Collection never changes what a reader sees" is false —
    // collecting below the floor is the entire point of compaction — so a test
    // encoding it could only ever be made to pass by weakening the engine.
    // Below the floor this test asserts nothing at all.
    //
    // The floor is read off the shard rather than recomputed here. `run` and
    // `flush` re-read the clock themselves, so a horizon the test sampled
    // before the call can be *below* the one the engine used, which is the
    // unsafe direction: the test would demand preservation across a window the
    // engine was entitled to collect, and would fail on a correct engine.
    // ------------------------------------------------------------------

    const KEYS: usize = 16;
    const STEPS: usize = 120;
    /// How many of the returned timestamps stay probeable. The probe set is
    /// the whole cost of the test, and it would otherwise grow with the
    /// schedule; a bounded uniform sample is enough, because each seed keeps a
    /// different set of windows.
    const PROBE_CAP: usize = 6;
    const SEEDS: u64 = 6;
    /// The schedule has to keep producing the shapes the property is about,
    /// *per seed*: if retuning ever drops one of the three policies below
    /// this, the assertions still all pass while testing nothing.
    const MIN_MULTI: usize = 3;
    /// Likewise for the rewrite path, which is only reachable under the pin
    /// because phase 0 kills rows before it.
    const MIN_REWRITES: usize = 1;

    fn pkey(k: usize) -> String {
        format!("t0{KEY_SEP}d{k:05}")
    }

    /// Version `v` of key `k`. `v` is a global counter, so it identifies the
    /// write that produced the document and not just its content — which is
    /// what lets the assertions say *which* version a reader found.
    fn pdoc(k: usize, v: u64) -> Value {
        json::parse(&format!(
            r#"{{"id":"d{k:05}","tenant_id":"t0","v":{v},"body":"doc {k} version {v} text","emb":[{},1.0,0.5,0.25]}}"#,
            k as f32 / 100.0
        ))
        .unwrap()
    }

    /// The oracle: per key, the versions the write path handed back, ascending.
    /// `None` is a delete. Every timestamp in here came out of `insert` or
    /// `delete` — the test never invents one, so it can never disagree with the
    /// engine about when a version became live.
    #[derive(Default)]
    struct Model {
        hist: BTreeMap<usize, Vec<(Timestamp, Option<u64>)>>,
    }

    impl Model {
        fn put(&mut self, k: usize, ts: Timestamp, v: Option<u64>) {
            self.hist.entry(k).or_default().push((ts, v));
        }

        fn at(&self, k: usize, t: Timestamp) -> Option<u64> {
            let h = self.hist.get(&k)?;
            h.iter().rev().find(|(ts, _)| *ts <= t).and_then(|(_, v)| *v)
        }

        fn live(&self, t: Timestamp) -> BTreeSet<String> {
            self.hist.keys().filter(|k| self.at(**k, t).is_some()).map(|k| pkey(*k)).collect()
        }
    }

    /// A bounded uniform sample of the timestamps writes returned. Uniform
    /// rather than "the most recent few" on purpose: the interesting probes are
    /// the ones deep inside the history, where a version that has since been
    /// superseded was still the live one.
    struct Reservoir {
        seen: usize,
        ts: Vec<Timestamp>,
        /// Timestamps that are never sampled out, held apart from the sample
        /// rather than in a prefix of it — a prefix survives only as long as
        /// the sample is already full when the first one is added.
        kept: Vec<Timestamp>,
    }

    impl Reservoir {
        fn new() -> Reservoir {
            Reservoir { seen: 0, ts: Vec::new(), kept: Vec::new() }
        }

        fn offer(&mut self, t: Timestamp, rng: &mut Rng) {
            self.seen += 1;
            if self.ts.len() < PROBE_CAP {
                self.ts.push(t);
            } else {
                // Classic reservoir: the new timestamp displaces a slot with
                // probability `PROBE_CAP / seen`, and no slot is favoured.
                let j = rng.next_usize(self.seen);
                if j < PROBE_CAP {
                    self.ts[j] = t;
                }
            }
        }

        /// Keep a timestamp permanently — the pinned horizon is the one
        /// timestamp the whole schedule is about.
        fn keep(&mut self, t: Timestamp) {
            self.kept.push(t);
        }

        fn probes(&self) -> impl Iterator<Item = &Timestamp> {
            self.ts.iter().chain(self.kept.iter())
        }
    }

    type Observed = BTreeMap<(usize, Timestamp), Option<u64>>;

    /// What every key looks like at every probe. Materialised into an owned map
    /// because a `Snapshot` borrows the shard and so cannot be held across the
    /// flush or the merge whose effect is being measured.
    fn observe(s: &Shard, probes: &[Timestamp]) -> Observed {
        let mut out = BTreeMap::new();
        for k in 0..KEYS {
            let key = pkey(k);
            for &t in probes {
                let v = s
                    .get(&key, t)
                    .unwrap()
                    .and_then(|d| d.path("v").and_then(|x| x.as_i64()))
                    .map(|x| x as u64);
                out.insert((k, t), v);
            }
        }
        out
    }

    /// Each sampled timestamp and the instant before it. The `ts - 1` probes
    /// are the ones with teeth: they land strictly inside the window in which
    /// the version that write superseded was still the live one, which is
    /// exactly what a pinned horizon exists to protect.
    fn probe_set(res: &Reservoir, s: &Shard) -> Vec<Timestamp> {
        let mut v: Vec<Timestamp> = Vec::with_capacity(2 * (res.ts.len() + res.kept.len()) + 2);
        for &t in res.probes() {
            v.push(t);
            if t > 0 {
                v.push(t - 1);
            }
        }
        v.push(s.clock.peek());
        // Safe as a read timestamp: `MAX_TS` is the sentinel for "never
        // deleted" in a row's `delete_ts`, not a reserved instant.
        v.push(MAX_TS);
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Every key a scan would find at `t`, one entry per visible row — so a key
    /// that two segments both expose shows up twice. `locate` cannot see that
    /// failure, because it stops at the first visible hit.
    fn visible_keys(s: &Shard, t: Timestamp) -> Vec<String> {
        let snap = s.snapshot_at(t);
        let mut out = Vec::new();
        for src in s.sources(&snap) {
            for ord in src.visibility(t).iter() {
                out.push(src.key(ord).unwrap_or_default().to_string());
            }
        }
        out.sort();
        out
    }

    /// The invariant. Both maps come from one probe set, and the floor is
    /// applied here rather than when they were built: the operation between
    /// them is what raised the floor, so filtering at build time would compare
    /// two maps with different key sets and fail for the wrong reason.
    fn assert_preserved(before: &Observed, after: &Observed, floor: Timestamp, ctx: &str) {
        for (&(k, t), v) in before {
            if t >= floor {
                assert_eq!(
                    after.get(&(k, t)),
                    Some(v),
                    "{ctx}: key {k} at {t} changed across an operation that collected no lower than {floor}"
                );
            }
        }
    }

    /// Raise the tracked floor from the shard's own. Checks the two things the
    /// theorem needs of it: it never moves backwards, and while a horizon is
    /// pinned nothing pushes it past the pin — a collector that read the raw
    /// clock instead of [`Shard::retain_from`] would leave the floor at `now`
    /// and shrink the window under test to the present instant.
    ///
    /// Not `== pinned`: an operation that collected nothing — a flush of an
    /// empty memtable — leaves the floor wherever it was, which is below the
    /// pin until the first real collection. That the floor does reach the pin
    /// is asserted once, at the end of the pinned phase.
    fn raise_floor(s: &Shard, floor: &mut Timestamp, pinned: Option<Timestamp>, ctx: &str) {
        assert!(
            s.retain_floor >= *floor,
            "{ctx}: retain floor went backwards, {} < {floor}",
            s.retain_floor
        );
        if let Some(h) = pinned {
            assert!(
                s.retain_floor <= h,
                "{ctx}: something collected at {}, past the pinned horizon {h}",
                s.retain_floor
            );
        }
        *floor = s.retain_floor;
    }

    /// Everything that must be true of the shard after a collecting operation.
    fn check(s: &Shard, m: &Model, probes: &[Timestamp], floor: Timestamp, ctx: &str) {
        // Against the oracle, not just against the previous observation: a diff
        // alone would accept a version the write path never produced, and it
        // would accept a shard that had been wrong since before the operation.
        for (&(k, t), got) in observe(s, probes).iter() {
            if t >= floor {
                assert_eq!(
                    *got,
                    m.at(k, t),
                    "{ctx}: key {k} at {t} is not the version the write path made live"
                );
            }
        }
        for &t in &[floor, s.clock.peek()] {
            let vis = visible_keys(s, t);
            let uniq: BTreeSet<&String> = vis.iter().collect();
            assert_eq!(uniq.len(), vis.len(), "{ctx}: a key is visible twice at {t}");
            assert_eq!(
                uniq.into_iter().cloned().collect::<BTreeSet<String>>(),
                m.live(t),
                "{ctx}: live set at {t}"
            );
        }
    }

    /// How often the schedule actually reached the shapes the property is
    /// about. Asserted at the end so that future tuning cannot quietly turn
    /// this into a test of the single-version path.
    ///
    /// Per seed, not summed: the seeds run three different policies, and a
    /// total lets five of them stop reaching the shape entirely while the
    /// sixth carries the floor on its own.
    #[derive(Default)]
    struct Coverage {
        multi_segment_flushes: usize,
        multi_output_jobs: usize,
        /// `Job::Rewrite` planned *inside* phase 1, i.e. under the pin and
        /// inside the per-operation assertions. Without pre-pin deaths this is
        /// zero and the whole rewrite path escapes the property.
        pinned_rewrites: usize,
    }

    fn run_schedule(seed: u64, copts: &CompactionOpts, cov: &mut Coverage) {
        let mut rng = Rng::new(seed);
        let mut s = Shard::new(coll(), Arc::new(Hlc::new()), ShardOpts::default());
        let mut m = Model::default();
        let mut res = Reservoir::new();
        let mut ver = 0u64;
        let mut floor: Timestamp = 0;

        // Phase 0: one version of each key, sealed, with nothing pinned. Its
        // timestamps are deliberately not offered to the reservoir: they are
        // all below the pin and so permanently below the floor, and a probe
        // below the floor asserts nothing. Sampling them would spend the whole
        // probe budget on nothing for the early steps of phase 1 — exactly the
        // steps where the first flushes and merges happen.
        for k in 0..KEYS {
            ver += 1;
            let ts = s.insert(pdoc(k, ver)).unwrap();
            m.put(k, ts, Some(ver));
        }
        s.flush().unwrap();
        // Half the keys die *before* the pin, so their rows are collectable at
        // the pinned horizon. Without this `plan` measures `dead_ratio` at the
        // pin, every death in phase 1 is above it, the ratio is identically
        // zero and no `Job::Rewrite` is ever planned — the whole rewrite path
        // would then run only in phase 2, outside every per-operation
        // assertion in this test.
        for k in (0..KEYS).step_by(2) {
            if k % 4 == 0 {
                if let Some(ts) = s.delete(&pkey(k)).unwrap() {
                    m.put(k, ts, None);
                }
            } else {
                ver += 1;
                let ts = s.insert(pdoc(k, ver)).unwrap();
                m.put(k, ts, Some(ver));
            }
        }
        s.flush().unwrap();
        raise_floor(&s, &mut floor, None, &format!("seed {seed} phase 0"));
        // One write the seals did not see, so the clock ticks past the floor
        // phase 0 left behind and `floor == horizon` at the end of phase 1 is
        // a claim about what phase 1 collected rather than one that already
        // held before it started.
        ver += 1;
        let ts = s.insert(pdoc(1, ver)).unwrap();
        m.put(1, ts, Some(ver));
        // The backup starts here. Everything from now to the end of phase 1 is
        // under the theorem; unpinned, the floor would follow `now` and only
        // the present would be assertable.
        let horizon = s.clock.peek();
        assert!(horizon > floor, "seed {seed}: the pinned window is empty before it starts");
        s.opts.gc_horizon = horizon;
        res.keep(horizon);
        assert!(
            s.segments.iter().any(|h| h.dead_ratio(horizon) > copts.dead_ratio),
            "seed {seed}: nothing is dead enough at the pin for a rewrite to be planned under it"
        );

        // Phase 1: interleaved writes, deletes, flushes and merges, pinned.
        for step in 0..STEPS {
            let ctx = format!("seed {seed} step {step}");
            let probes = probe_set(&res, &s);
            let r = rng.next_usize(100);
            let k = rng.next_usize(KEYS);
            if r < 50 {
                // Creates a key, supersedes a live one, or resurrects a deleted
                // one — `insert` cannot tell the three apart, and neither can
                // the model.
                ver += 1;
                let ts = s.insert(pdoc(k, ver)).unwrap();
                m.put(k, ts, Some(ver));
                res.offer(ts, &mut rng);
            } else if r < 70 {
                if let Some(ts) = s.delete(&pkey(k)).unwrap() {
                    m.put(k, ts, None);
                    res.offer(ts, &mut rng);
                }
            } else if r < 82 {
                let ids: BTreeSet<u64> = s.segments.iter().map(|h| h.id()).collect();
                let before = observe(&s, &probes);
                s.flush().unwrap();
                raise_floor(&s, &mut floor, Some(horizon), &ctx);
                assert_preserved(&before, &observe(&s, &probes), floor, &format!("{ctx} flush"));
                if s.segments.iter().filter(|h| !ids.contains(&h.id())).count() > 1 {
                    cov.multi_segment_flushes += 1;
                }
                check(&s, &m, &probes, floor, &format!("{ctx} after flush"));
            } else {
                // One job at a time rather than `run_to_quiescence`, so that a
                // failure names the `Job` that caused it.
                let mut jobs = 0;
                while let Some(job) = plan(&s, s.clock.peek(), copts) {
                    jobs += 1;
                    if matches!(job, Job::Rewrite { .. }) {
                        cov.pinned_rewrites += 1;
                    }
                    assert!(jobs < 64, "{ctx}: the planner will not quiesce, last {job:?}");
                    let ids: BTreeSet<u64> = s.segments.iter().map(|h| h.id()).collect();
                    let before = observe(&s, &probes);
                    run(&mut s, &job, copts).unwrap();
                    raise_floor(&s, &mut floor, Some(horizon), &ctx);
                    assert_preserved(
                        &before,
                        &observe(&s, &probes),
                        floor,
                        &format!("{ctx} {job:?}"),
                    );

                    let new: Vec<u64> =
                        s.segments.iter().map(|h| h.id()).filter(|id| !ids.contains(id)).collect();
                    if new.len() > 1 {
                        cov.multi_output_jobs += 1;
                    }
                    if let (Job::Merge { level, .. }, Some(&top)) = (&job, new.iter().max()) {
                        // Recover each output's version depth from what is in
                        // it, not from the expression production used: the
                        // newest version of a key among the outputs is the one
                        // that survived, so an output holding only survivors is
                        // depth 0 and any other is deeper. Asserting the *range*
                        // of `if depth == 0 { level } else { level - 1 }` would
                        // be that same expression on both sides of the equals,
                        // and could only fail if the line grew a third case.
                        let mut newest: BTreeMap<&str, Timestamp> = BTreeMap::new();
                        for h in s.segments.iter().filter(|h| new.contains(&h.id())) {
                            let o = &h.segment.ordinals;
                            for (k, ts) in o.keys.iter().zip(o.commit_ts.iter()) {
                                let e = newest.entry(k.as_str()).or_insert(*ts);
                                *e = (*e).max(*ts);
                            }
                        }
                        for h in s.segments.iter().filter(|h| new.contains(&h.id())) {
                            let o = &h.segment.ordinals;
                            let n = o.len();
                            assert!(n > 0, "{ctx}: {job:?} installed an empty segment");
                            let survivors = o
                                .keys
                                .iter()
                                .zip(o.commit_ts.iter())
                                .filter(|(k, ts)| newest[k.as_str()] == **ts)
                                .count();
                            // One segment holds one version per key, and all of
                            // them at the same depth — mixing depths is the
                            // failure the layering exists to prevent.
                            assert!(
                                survivors == 0 || survivors == n,
                                "{ctx}: {job:?} mixed {survivors} surviving and {} superseded \
                                 versions into one output",
                                n - survivors
                            );
                            // A superseded version stays at the level the merge
                            // drained; only the survivor is promoted.
                            let want =
                                if survivors == n { *level } else { level.saturating_sub(1) };
                            assert_eq!(
                                h.segment.level,
                                want,
                                "{ctx}: {job:?} put a depth-{} output at level {}",
                                u32::from(survivors != n),
                                h.segment.level
                            );
                        }
                        let survivor = s.segments.iter().find(|h| h.id() == top).unwrap();
                        assert_eq!(
                            survivor.segment.level, *level,
                            "{ctx}: {job:?} did not promote the surviving version"
                        );
                    }
                    check(&s, &m, &probes, floor, &format!("{ctx} after {job:?}"));
                }
            }
        }

        assert_eq!(
            floor, horizon,
            "seed {seed}: the pinned phase never collected, so nothing above the pin was tested"
        );

        // Phase 2: the backup finishes. The floor jumps to now, everything
        // below it becomes collectable, and the engine has to be able to
        // actually settle — collecting nothing would also stop the planner.
        s.opts.gc_horizon = 0;
        s.flush().unwrap();
        let n = run_to_quiescence(&mut s, copts, 64).unwrap();
        assert!(n < 64, "seed {seed}: released the horizon and never quiesced");
        assert!(plan(&s, s.clock.peek(), copts).is_none(), "seed {seed}: still not quiet");
        raise_floor(&s, &mut floor, None, &format!("seed {seed} phase 2"));
        let probes = probe_set(&res, &s);
        check(&s, &m, &probes, floor, &format!("seed {seed} after release"));
        let now = s.clock.peek();
        assert!(
            s.segments.iter().all(|h| h.dead_ratio(now) <= copts.dead_ratio),
            "seed {seed}: quiesced without collecting what the released horizon freed"
        );
    }

    #[test]
    fn interleaved_writes_and_collection_keep_every_version_above_the_retain_floor() {
        for seed in 0..SEEDS {
            // Three policies, because retention interacts with each trigger
            // differently: a low dead ratio rewrites the rows phase 0 killed
            // before the pin as fast as it can see them, while leaving every
            // phase-1 death behind, and a small cap makes the split path run
            // over layered output.
            let copts = match seed % 3 {
                1 => CompactionOpts { tier_fanout: 2, dead_ratio: 0.10, ..Default::default() },
                2 => CompactionOpts { tier_fanout: 2, segment_cap: 8, ..Default::default() },
                _ => CompactionOpts::default(),
            };
            // Per seed, so one policy cannot carry the floor for the others.
            let mut cov = Coverage::default();
            run_schedule(seed, &copts, &mut cov);
            assert!(
                cov.multi_segment_flushes >= MIN_MULTI
                    && cov.multi_output_jobs >= MIN_MULTI
                    && cov.pinned_rewrites >= MIN_REWRITES,
                "seed {seed} stopped exercising retention: {} multi-segment flushes and {} \
                 multi-output jobs (both need {MIN_MULTI}), {} pinned rewrites (needs \
                 {MIN_REWRITES}); retune KEYS/STEPS or the weights",
                cov.multi_segment_flushes,
                cov.multi_output_jobs,
                cov.pinned_rewrites
            );
        }
    }
}
