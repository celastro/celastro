//! Rank fusion — at the coordinator, and nowhere else.
//!
//! This is a **correctness constraint, not a placement preference** (§7.2).
//! Rank fusion over segment-local or shard-local ranks is not equivalent to
//! fusion over global ranks: rank 1 in a 1,000-document segment and rank 1 in a
//! 10M-document segment would receive the same contribution. Fusing early and
//! merging fused scores produces a subtly wrong global order that no test on a
//! single node will catch — so this module takes raw per-source candidate lists
//! and does the ranking itself, and [`fuse`] is the only function in the engine
//! that assigns a rank.
//!
//! Every ordering here — per-source lists and the fused result — **breaks ties
//! by primary key**. That is what makes results deterministic across runs,
//! shard counts and replica choice, and therefore what makes the determinism
//! goal in §1 testable rather than aspirational.

use std::collections::BTreeMap;

use crate::sql::ast::FusionMethod;

/// One candidate as a shard returns it: an identifier, which source found it,
/// and that source's *raw* score. Not a rank — the shard is not in a position
/// to compute one.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub key: String,
    pub raw_score: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// BM25: bigger is better.
    HigherIsBetter,
    /// Distance: smaller is better.
    LowerIsBetter,
}

impl Direction {
    fn cmp(self, a: f32, b: f32) -> std::cmp::Ordering {
        match self {
            Direction::HigherIsBetter => b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal),
            Direction::LowerIsBetter => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceList {
    pub name: String,
    pub direction: Direction,
    pub weight: f32,
    pub candidates: Vec<Candidate>,
}

impl SourceList {
    /// Merge one source's per-shard lists into one globally ordered list.
    ///
    /// BM25 scores are comparable across shards because scoring uses global
    /// term statistics (§8.2); distances are comparable by construction. That
    /// is the precondition for this function existing at all.
    pub fn sort_and_dedup(&mut self) {
        let dir = self.direction;
        self.candidates.sort_by(|a, b| dir.cmp(a.raw_score, b.raw_score).then(a.key.cmp(&b.key)));
        self.candidates.dedup_by(|a, b| a.key == b.key);
    }
}

#[derive(Debug, Clone)]
pub struct Fused {
    pub key: String,
    pub score: f32,
    /// Per source: `(rank, raw score)` if this source found the document.
    /// A document surfaced by only one source gets that source's contribution
    /// only — `hybrid()` is a union, not an intersection (§2.4).
    pub contributions: Vec<Option<(usize, f32)>>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FusionExplain {
    pub method: String,
    pub sources: Vec<String>,
    pub candidates_per_source: Vec<usize>,
    pub union_size: usize,
    /// For linear fusion, the min/max used for normalisation, computed once
    /// over the merged set.
    pub normalisation: Vec<(f32, f32)>,
}

/// Merge, rank globally, fuse, take top `k`.
pub fn fuse(
    mut sources: Vec<SourceList>,
    method: FusionMethod,
    rrf_c: f32,
    k: usize,
) -> (Vec<Fused>, FusionExplain) {
    let mut explain = FusionExplain {
        method: match method {
            FusionMethod::Rrf => "rrf".into(),
            FusionMethod::Linear => "weighted_linear".into(),
        },
        sources: sources.iter().map(|s| s.name.clone()).collect(),
        ..Default::default()
    };

    // 1. Merge each source's lists globally.
    for s in sources.iter_mut() {
        s.sort_and_dedup();
    }
    explain.candidates_per_source = sources.iter().map(|s| s.candidates.len()).collect();

    // 2. Assign global per-source ranks.
    let mut ranks: Vec<BTreeMap<&str, (usize, f32)>> = Vec::with_capacity(sources.len());
    for s in &sources {
        let mut m = BTreeMap::new();
        for (i, c) in s.candidates.iter().enumerate() {
            m.insert(c.key.as_str(), (i + 1, c.raw_score));
        }
        ranks.push(m);
    }

    // The union of candidates across sources.
    let mut keys: Vec<&str> = Vec::new();
    for s in &sources {
        for c in &s.candidates {
            keys.push(c.key.as_str());
        }
    }
    keys.sort_unstable();
    keys.dedup();
    explain.union_size = keys.len();

    // 3. Fuse.
    let norms: Vec<(f32, f32)> = sources
        .iter()
        .map(|s| {
            // Normalisation is computed **once, here, over the merged candidate
            // set**. Per-shard normalisation makes scores incomparable across
            // shards and is the classic mistake (§7.3).
            let mut lo = f32::INFINITY;
            let mut hi = f32::NEG_INFINITY;
            for c in &s.candidates {
                lo = lo.min(c.raw_score);
                hi = hi.max(c.raw_score);
            }
            if !lo.is_finite() {
                (0.0, 1.0)
            } else {
                (lo, hi)
            }
        })
        .collect();
    if method == FusionMethod::Linear {
        explain.normalisation = norms.clone();
    }

    let mut out: Vec<Fused> = keys
        .iter()
        .map(|key| {
            let mut score = 0.0f32;
            let mut contributions = Vec::with_capacity(sources.len());
            for (i, s) in sources.iter().enumerate() {
                match ranks[i].get(key) {
                    Some(&(rank, raw)) => {
                        score += match method {
                            FusionMethod::Rrf => s.weight / (rrf_c + rank as f32),
                            FusionMethod::Linear => {
                                let (lo, hi) = norms[i];
                                let span = hi - lo;
                                let unit = if span > 0.0 {
                                    let unit = (raw - lo) / span;
                                    // Normalise to "bigger is better" so that a
                                    // distance and a BM25 score can be added at
                                    // all.
                                    match s.direction {
                                        Direction::HigherIsBetter => unit,
                                        Direction::LowerIsBetter => 1.0 - unit,
                                    }
                                } else {
                                    // A source with one candidate, or with every
                                    // score equal, has no spread to normalise
                                    // against. Dividing by an epsilon instead
                                    // gives unit 0.0, which the flip above turns
                                    // into full credit for a distance source and
                                    // none at all for a text one — a sole
                                    // perfect text hit would contribute zero.
                                    // Such a candidate is both the best and the
                                    // worst of its source, so it scores 1.0
                                    // whichever way the source points.
                                    1.0
                                };
                                s.weight * unit
                            }
                        };
                        contributions.push(Some((rank, raw)));
                    }
                    None => contributions.push(None),
                }
            }
            Fused { key: (*key).to_string(), score, contributions }
        })
        .collect();

    out.sort_by(|a, b| {
        b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal).then(a.key.cmp(&b.key))
    });
    out.truncate(k);
    (out, explain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(k: &str, s: f32) -> Candidate {
        Candidate { key: k.to_string(), raw_score: s }
    }

    fn text(cands: Vec<Candidate>) -> SourceList {
        SourceList {
            name: "text".into(),
            direction: Direction::HigherIsBetter,
            weight: 1.0,
            candidates: cands,
        }
    }

    fn vector(cands: Vec<Candidate>) -> SourceList {
        SourceList {
            name: "vector".into(),
            direction: Direction::LowerIsBetter,
            weight: 1.0,
            candidates: cands,
        }
    }

    #[test]
    fn hybrid_is_a_union_not_an_intersection() {
        let (out, ex) = fuse(
            vec![text(vec![c("a", 9.0), c("b", 5.0)]), vector(vec![c("b", 0.1), c("z", 0.2)])],
            FusionMethod::Rrf,
            60.0,
            10,
        );
        assert_eq!(ex.union_size, 3);
        let keys: Vec<&str> = out.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"z"), "a single-source document must still surface");
        // `b` is found by both, so it wins.
        assert_eq!(out[0].key, "b");
        // And `z`'s text contribution is absent, not zero-imputed.
        let z = out.iter().find(|f| f.key == "z").unwrap();
        assert_eq!(z.contributions[0], None);
        assert!(z.contributions[1].is_some());
    }

    /// The bug §7.2 exists to prevent: fusing per-shard and then merging the
    /// fused scores. Rank 1 in a 1,000-document segment and rank 1 in a
    /// 10M-document segment receive the same contribution, so a document that
    /// loses on every raw score is promoted purely for being the best of a
    /// small pile.
    #[test]
    fn fusing_early_gives_a_different_and_wrong_answer() {
        // Shard A is tiny and holds exactly one document: locally it is rank 1
        // in both sources, globally it is near the bottom of both.
        let shard_a_text = vec![c("small-1", 0.4)];
        let shard_a_vec = vec![c("small-1", 0.9)];
        // Shard B is large. Its documents are better on every raw score.
        let shard_b_text: Vec<Candidate> =
            (1..=20).map(|i| c(&format!("big-{i:02}"), 20.0 - i as f32)).collect();
        let shard_b_vec: Vec<Candidate> =
            (1..=20).map(|i| c(&format!("big-{i:02}"), 0.01 * i as f32)).collect();

        // Correct: merge raw, rank globally, fuse once.
        let mut all_text = shard_a_text.clone();
        all_text.extend(shard_b_text.clone());
        let mut all_vec = shard_a_vec.clone();
        all_vec.extend(shard_b_vec.clone());
        let (correct, _) = fuse(vec![text(all_text), vector(all_vec)], FusionMethod::Rrf, 60.0, 3);

        // Wrong: fuse inside each shard, then merge fused scores.
        let (fa, _) =
            fuse(vec![text(shard_a_text), vector(shard_a_vec)], FusionMethod::Rrf, 60.0, 3);
        let (fb, _) =
            fuse(vec![text(shard_b_text), vector(shard_b_vec)], FusionMethod::Rrf, 60.0, 3);
        let mut early: Vec<Fused> = fa.into_iter().chain(fb).collect();
        early.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap().then(a.key.cmp(&b.key)));
        early.truncate(3);

        let ck: Vec<&str> = correct.iter().map(|f| f.key.as_str()).collect();
        let ek: Vec<&str> = early.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(ck, vec!["big-01", "big-02", "big-03"]);
        // The tiny shard's local rank-1 document displaces a document that beat
        // it on both raw scores.
        assert!(ek.contains(&"small-1"), "{ek:?}");
        assert_ne!(ck, ek);
    }

    #[test]
    fn ties_break_by_primary_key_so_order_is_deterministic() {
        // Three documents with identical scores from both sources.
        let t = text(vec![c("c", 1.0), c("a", 1.0), c("b", 1.0)]);
        let v = vector(vec![c("b", 0.5), c("c", 0.5), c("a", 0.5)]);
        let (out, _) = fuse(vec![t, v], FusionMethod::Rrf, 60.0, 3);
        assert_eq!(out.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(), vec!["a", "b", "c"]);
        // Reordering the inputs — which is what a different shard count does —
        // changes nothing.
        let t2 = text(vec![c("a", 1.0), c("b", 1.0), c("c", 1.0)]);
        let v2 = vector(vec![c("a", 0.5), c("b", 0.5), c("c", 0.5)]);
        let (out2, _) = fuse(vec![t2, v2], FusionMethod::Rrf, 60.0, 3);
        assert_eq!(
            out.iter().map(|f| f.key.clone()).collect::<Vec<_>>(),
            out2.iter().map(|f| f.key.clone()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn linear_normalises_once_over_the_merged_set() {
        let t = text(vec![c("a", 10.0), c("b", 0.0)]);
        let v = vector(vec![c("a", 1.0), c("b", 0.0)]);
        let (out, ex) = fuse(vec![t, v], FusionMethod::Linear, 60.0, 2);
        assert_eq!(ex.normalisation, vec![(0.0, 10.0), (0.0, 1.0)]);
        // `a` is best on text (unit 1.0) and worst on distance (unit 0.0);
        // `b` is the reverse. Equal weights make them tie, and the primary key
        // breaks it.
        assert!((out[0].score - out[1].score).abs() < 1e-6);
        assert_eq!(out[0].key, "a");
    }

    /// A source with nothing to normalise against used to be normalised anyway:
    /// `span.max(1e-9)` made its unit score 0.0, and the direction flip turned
    /// that into full credit for a distance source and none at all for a text
    /// one. A sole perfect text hit therefore contributed exactly nothing while
    /// a vector source in the identical state contributed everything.
    #[test]
    fn a_lone_candidate_scores_the_same_whichever_way_its_source_points() {
        let t = text(vec![c("a-text-only", 9.9)]);
        let v = vector(vec![c("b-vector-only", 0.02)]);
        let (out, _) = fuse(vec![t, v], FusionMethod::Linear, 60.0, 2);
        let a = out.iter().find(|f| f.key == "a-text-only").unwrap();
        let b = out.iter().find(|f| f.key == "b-vector-only").unwrap();
        assert!(a.score > 0.0, "a sole hit must get credit, not zero: {}", a.score);
        assert!((a.score - b.score).abs() < 1e-6, "text {} vs vector {}", a.score, b.score);

        // The same holds when a source has several candidates that all tie:
        // every one of them is both the best and the worst of that source.
        let (flat, _) =
            fuse(vec![text(vec![c("x", 4.0), c("y", 4.0)])], FusionMethod::Linear, 60.0, 2);
        assert!(flat[0].score > 0.0, "an all-equal source must not score zero");
        assert!((flat[0].score - flat[1].score).abs() < 1e-6);
    }

    #[test]
    fn rrf_ignores_score_magnitude_and_that_is_the_point() {
        // Two documents at near-identical similarity get separated by a full
        // rank step. This is RRF's known weakness; the test pins the behaviour
        // so a future change to it is deliberate.
        let v = vector(vec![c("a", 0.1000), c("b", 0.1001)]);
        let (out, _) = fuse(vec![v], FusionMethod::Rrf, 60.0, 2);
        let sep = out[0].score - out[1].score;
        assert!(sep > 1e-4, "rank step should dominate the score gap: {sep}");
    }

    #[test]
    fn weights_shift_the_balance() {
        let mut t = text(vec![c("t-only", 5.0)]);
        t.weight = 3.0;
        let v = vector(vec![c("v-only", 0.01)]);
        let (out, _) = fuse(vec![t, v], FusionMethod::Rrf, 60.0, 2);
        assert_eq!(out[0].key, "t-only");
    }
}
