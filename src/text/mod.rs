//! The full-text subsystem.
//!
//! Two things live behind one interface here: the encoded postings of a sealed
//! segment, and the in-memory postings of the memtable. Both expose the same
//! cursor, so the scorer in [`scorer`] is written once and the freshly written
//! document and the year-old one are searched by identical code.

pub mod analyzer;
pub mod postings;
pub mod query;
pub mod scorer;

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use postings::{DictParts, PostingCursor, TermDict, TermMeta, TermPostings, EXHAUSTED};

/// A cursor over one term's postings, from either backing store.
pub enum PostingsRef<'a> {
    Encoded(PostingCursor<'a>),
    Mem(MemCursor<'a>),
}

impl<'a> PostingsRef<'a> {
    pub fn advance(&mut self, target: u32) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.advance(target),
            PostingsRef::Mem(c) => c.advance(target),
        }
    }
    pub fn doc(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.doc(),
            PostingsRef::Mem(c) => c.doc(),
        }
    }
    pub fn tf(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.tf(),
            PostingsRef::Mem(c) => c.tf(),
        }
    }
    pub fn doc_freq(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.doc_freq(),
            PostingsRef::Mem(c) => c.doc_freq(),
        }
    }
    /// Horizon up to which [`block_max_tf`](Self::block_max_tf) and
    /// [`block_min_dl`](Self::block_min_dl) are valid bounds.
    pub fn block_last_ord(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.block_last_ord(),
            PostingsRef::Mem(c) => c.block_last_ord(),
        }
    }
    pub fn block_max_tf(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.block_meta().map(|m| m.max_tf).unwrap_or(1),
            PostingsRef::Mem(c) => c.max_tf,
        }
    }
    pub fn block_min_dl(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.block_meta().map(|m| m.min_dl).unwrap_or(1),
            PostingsRef::Mem(c) => c.min_dl,
        }
    }
    pub fn global_max_tf(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.global_max_tf(),
            PostingsRef::Mem(c) => c.max_tf,
        }
    }
    pub fn global_min_dl(&self) -> u32 {
        match self {
            PostingsRef::Encoded(c) => c.global_min_dl(),
            PostingsRef::Mem(c) => c.min_dl,
        }
    }
    pub fn positions(&self) -> Vec<u32> {
        match self {
            PostingsRef::Encoded(c) => c.positions(),
            PostingsRef::Mem(c) => c.positions(),
        }
    }
}

/// Cursor over the memtable's uncompressed postings.
pub struct MemCursor<'a> {
    tp: &'a TermPostings,
    idx: usize,
    pub max_tf: u32,
    pub min_dl: u32,
}

impl<'a> MemCursor<'a> {
    fn new(tp: &'a TermPostings, doc_lens: &[u32]) -> MemCursor<'a> {
        let max_tf = tp.tfs.iter().copied().max().unwrap_or(1);
        let min_dl = tp
            .ords
            .iter()
            .map(|&o| doc_lens.get(o as usize).copied().unwrap_or(1).max(1))
            .min()
            .unwrap_or(1);
        MemCursor { tp, idx: 0, max_tf, min_dl }
    }

    pub fn advance(&mut self, target: u32) -> u32 {
        // Galloping search: memtable lists are short but can be dense, and a
        // linear scan turns a conjunction into quadratic work.
        let n = self.tp.ords.len();
        if self.idx >= n {
            return EXHAUSTED;
        }
        if self.tp.ords[self.idx] >= target {
            return self.tp.ords[self.idx];
        }
        let mut step = 1;
        let mut lo = self.idx;
        while lo + step < n && self.tp.ords[lo + step] < target {
            lo += step;
            step *= 2;
        }
        let hi = (lo + step + 1).min(n);
        self.idx = lo + self.tp.ords[lo..hi].partition_point(|&o| o < target);
        if self.idx >= n {
            EXHAUSTED
        } else {
            self.tp.ords[self.idx]
        }
    }

    pub fn doc(&self) -> u32 {
        self.tp.ords.get(self.idx).copied().unwrap_or(EXHAUSTED)
    }
    pub fn tf(&self) -> u32 {
        self.tp.tfs.get(self.idx).copied().unwrap_or(0)
    }
    pub fn doc_freq(&self) -> u32 {
        self.tp.ords.len() as u32
    }
    pub fn block_last_ord(&self) -> u32 {
        self.tp.ords.last().copied().unwrap_or(EXHAUSTED)
    }
    pub fn positions(&self) -> Vec<u32> {
        self.tp.positions.get(self.idx).cloned().unwrap_or_default()
    }
}

/// Where a scorer gets its postings and document lengths. Both variants live
/// in one enum rather than behind a trait object: there are exactly two, and
/// concrete types keep the inner loop free of virtual dispatch.
pub enum TextSource<'a> {
    Sealed { dict: TermDict<'a>, postings: &'a [u8], doc_lens: &'a [u32] },
    Memory { terms: &'a BTreeMap<String, TermPostings>, doc_lens: &'a [u32] },
}

impl<'a> TextSource<'a> {
    pub fn sealed(dict: &'a DictParts, postings: &'a [u8], doc_lens: &'a [u32]) -> Self {
        TextSource::Sealed { dict: TermDict::new(dict), postings, doc_lens }
    }

    pub fn doc_lens(&self) -> &'a [u32] {
        match self {
            TextSource::Sealed { doc_lens, .. } => doc_lens,
            TextSource::Memory { doc_lens, .. } => doc_lens,
        }
    }

    pub fn num_docs(&self) -> u32 {
        self.doc_lens().len() as u32
    }

    pub fn total_doc_len(&self) -> u64 {
        self.doc_lens().iter().map(|&l| l as u64).sum()
    }

    /// Sum of document lengths over the ordinals `vis` marks visible, which is
    /// the numerator of BM25's `avgdl` at that snapshot. `total_doc_len` is
    /// the same sum over *physical* rows: pair it only with a physical
    /// document count, because over a masked denominator it gives the average
    /// of a corpus that does not exist.
    pub fn visible_doc_len(&self, vis: &crate::bitmap::Bitmap) -> u64 {
        vis.masked_sum(self.doc_lens())
    }

    /// Local document frequency. The scorer uses *global* statistics for `idf`
    /// (§8.2); this is only for planning and for the exact-statistics gather.
    pub fn doc_freq(&self, term: &str) -> u32 {
        match self {
            TextSource::Sealed { dict, .. } => dict.get(term).map(|m| m.doc_freq).unwrap_or(0),
            TextSource::Memory { terms, .. } => {
                terms.get(term).map(|t| t.ords.len() as u32).unwrap_or(0)
            }
        }
    }

    /// Open a cursor over `term`'s postings. `Ok(None)` means the term is not
    /// in this source; an error means it is, and its postings are unreadable.
    ///
    /// The distinction is the point. The dictionary has just said `doc_freq >
    /// 0`, so a postings extent that runs off the end of the region, or that
    /// does not decode, is a corrupt segment — not a missing term. Reporting
    /// it as "no such term" turns corruption into a quietly smaller result
    /// set, which is the one failure mode a search index must not have.
    pub fn try_cursor(&self, term: &str) -> Result<Option<PostingsRef<'_>>> {
        match self {
            TextSource::Sealed { dict, postings, .. } => {
                let m: TermMeta = match dict.get(term) {
                    Some(m) => m,
                    None => return Ok(None),
                };
                let bad = || Error::Storage(format!("postings: `{term}` extent is unreadable"));
                // Widened before adding: `postings_off + postings_len` computed
                // in u32 wraps on a corrupt extent, and hands `get` a range that
                // looks perfectly in bounds.
                let start = m.postings_off as usize;
                let end = start.checked_add(m.postings_len as usize).ok_or_else(bad)?;
                let slice = postings.get(start..end).ok_or_else(bad)?;
                Ok(Some(PostingsRef::Encoded(PostingCursor::open(slice)?)))
            }
            TextSource::Memory { terms, doc_lens } => {
                Ok(terms.get(term).map(|tp| PostingsRef::Mem(MemCursor::new(tp, doc_lens))))
            }
        }
    }

    /// [`try_cursor`](Self::try_cursor) for the callers whose signature cannot
    /// carry an error. Prefer `try_cursor` in anything that can report one:
    /// this spelling cannot tell "no such term" from "corrupt postings".
    pub fn cursor(&self, term: &str) -> Option<PostingsRef<'_>> {
        self.try_cursor(term).ok().flatten()
    }

    /// The first `limit` terms beginning with `prefix`, IN SORTED ORDER, from
    /// the PHYSICAL dictionary.
    ///
    /// The ordering is load-bearing outside this file and not an accident of
    /// the two implementations: `Db::run_select` unions this answer over every
    /// unit of every shard and takes the first `PREFIX_EXPANSION_LIMIT` of the
    /// union, which is the collection's true first-`limit` only because a term
    /// among the `limit` smallest globally is among the `limit` smallest of
    /// whichever unit holds it. Return these unordered and a prefix query
    /// silently starts meaning something different in every unit.
    ///
    /// Every query path wants [`live_terms_with_prefix`](Self::live_terms_with_prefix)
    /// instead, because a term no live document holds still occupies a slot
    /// here. What is left for this spelling is the physical picture: fixtures,
    /// assertions and tooling that want to see what a dictionary is carrying.
    pub fn terms_with_prefix(&self, prefix: &str, limit: usize) -> Vec<String> {
        match self {
            TextSource::Sealed { dict, .. } => {
                dict.terms_with_prefix(prefix, limit).into_iter().map(|(t, _)| t).collect()
            }
            TextSource::Memory { terms, .. } => terms
                .range(prefix.to_string()..)
                .take_while(|(t, _)| t.starts_with(prefix))
                .take(limit)
                .map(|(t, _)| t.clone())
                .collect(),
        }
    }

    /// The first `limit` terms beginning with `prefix` that at least one
    /// document `vis` marks visible still holds, IN SORTED ORDER.
    ///
    /// The cap counts LIVE terms, and that is the whole difference from
    /// [`terms_with_prefix`](Self::terms_with_prefix). Enumerating the first
    /// `limit` physical terms and filtering the answer afterwards lets a run of
    /// dead terms spend the budget, so the terms behind it never get asked
    /// about: which garbage a unit has not yet compacted away then decides what
    /// the query means. Here the enumeration runs PAST a dead run, so the
    /// answer is a function of the live corpus at `t` alone — see
    /// `crate::shard::Shard::prefix_terms` for why unioning per-unit answers
    /// is still exactly the collection's first `limit` live terms.
    ///
    /// Cost is one probe per term enumerated, and the probe stops at a term's
    /// first live posting — for a live term that is one cursor open and one
    /// block decode, work the query repeats in `scorer::build` a moment later.
    /// Stepping over a dead term was measured at ~1 us, against 237-468 us to
    /// gather one term's frequency at the coordinator.
    pub fn live_terms_with_prefix(
        &self,
        prefix: &str,
        limit: usize,
        vis: &crate::bitmap::Bitmap,
    ) -> Vec<String> {
        let live = vis.popcount();
        // Nothing is dead in this unit at `t`, so every term is live and the
        // probe can only say so. The walk is then byte-for-byte the physical
        // one, for the cost of a popcount the caller has already paid — and
        // that is most units most of the time.
        //
        // Against `vis.len()` — the unit's document count — because that is
        // the condition actually meant: no ordinal in this unit is dead.
        // `doc_lens().len()` is the tempting spelling and is only equal to it
        // by a second invariant, that `InvertedBuilder::add_doc` is called for
        // EVERY ordinal, including the ones carrying no text on this path, so
        // `doc_lens` is padded out to the unit's length. Were that ever to
        // change, `doc_lens` would stop short of the trailing text-less
        // documents, and a unit with exactly that many dead ordinals would take
        // the fast path — skipping the mask on precisely the units that need
        // it. This spelling does not depend on the other invariant at all.
        if live == vis.len() {
            return self.terms_with_prefix(prefix, limit);
        }
        // The opposite extreme, and it is not rare: a segment every one of
        // whose documents has been superseded or tombstoned holds no live term
        // at all. Without this the probe still opens a cursor per term and
        // `next_set` still scans the whole bitmap to find nothing, which was
        // measured at 12 us a term over a 50000-term dead run — the walk over a
        // unit that is pure garbage should be the CHEAPEST case, not the
        // dearest.
        if live == 0 {
            return Vec::new();
        }
        match self {
            TextSource::Sealed { dict, postings, .. } => dict
                .terms_with_prefix_where(prefix, limit, &mut |_, m| {
                    sealed_has_live_posting(postings, m, vis)
                })
                .into_iter()
                .map(|(t, _)| t)
                .collect(),
            TextSource::Memory { terms, .. } => terms
                .range(prefix.to_string()..)
                .take_while(|(t, _)| t.starts_with(prefix))
                // Before `take`, not after: the cap counts live terms.
                .filter(|(_, tp)| tp.ords.iter().any(|o| vis.get(*o as usize)))
                .take(limit)
                .map(|(t, _)| t.clone())
                .collect(),
        }
    }

    /// Every term this source holds, with its PHYSICAL document frequency —
    /// the memtable's `ords.len()` before a seal, the built dictionary's
    /// deduplicated count after it.
    ///
    /// Deliberately not a source of scoring statistics, and it used to be one.
    /// Neither number is masked by visibility, so both count versions a write
    /// superseded and rows a delete tombstoned, and which of those still exist
    /// is a per-shard seal and compaction decision: an IDF built from this
    /// moves with the shard count. `crate::shard::Shard::term_stats` answers
    /// the same question masked at a snapshot, and that is what both scoring
    /// paths ask. What is left here is fixtures and assertions that want the
    /// physical picture, which is a real thing to want — just not to rank by.
    pub fn all_terms(&self) -> Vec<(String, u32)> {
        match self {
            TextSource::Sealed { dict, .. } => {
                dict.all_terms().into_iter().map(|(t, m)| (t, m.doc_freq)).collect()
            }
            TextSource::Memory { terms, .. } => {
                terms.iter().map(|(t, p)| (t.clone(), p.ords.len() as u32)).collect()
            }
        }
    }
}

/// Does the sealed term `m` describes hold at least one posting `vis` marks
/// visible?
///
/// A galloping intersection of the posting list with the bitmap, not a scan.
/// Each turn either finds a live posting and stops, or consumes one live
/// ordinal this term does not hold, so it runs at most
/// `min(df, popcount(vis))` times and skips whole posting blocks between turns.
/// On a live term it stops at the first posting: one cursor open and one block
/// decode, work the query repeats in `scorer::build` a moment later.
///
/// Takes the [`TermMeta`] the enumeration already decoded rather than the term
/// string, and that is a measured decision, not a style one.
/// [`TextSource::try_cursor`] would look the term up again, and a dictionary
/// lookup decodes and allocates the term's whole block — so probing every term
/// of a block re-decoded that block once per term. Over a 50000-term dead run
/// that was 580 ms against 20 ms.
///
/// An UNREADABLE extent answers "live", not "dead". Dropping a term because its
/// postings are corrupt turns corruption into a quietly smaller expansion,
/// which is the one failure mode [`TextSource::try_cursor`]'s doc comment
/// exists to refuse; letting it through costs one term in the expansion, and
/// the gather that follows opens the same extent and raises the error there.
fn sealed_has_live_posting(postings: &[u8], m: &TermMeta, vis: &crate::bitmap::Bitmap) -> bool {
    let start = m.postings_off as usize;
    // Widened before adding, for the same reason `try_cursor` widens: the sum
    // computed in u32 wraps on a corrupt extent and yields a range that looks
    // perfectly in bounds.
    let Some(end) = start.checked_add(m.postings_len as usize) else { return true };
    let Some(slice) = postings.get(start..end) else { return true };
    let Ok(mut c) = PostingCursor::open(slice) else { return true };
    let mut d = c.advance(0);
    while d != EXHAUSTED {
        if vis.get(d as usize) {
            return true;
        }
        match vis.next_set(d as usize + 1) {
            Some(n) => d = c.advance(n as u32),
            None => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::analyzer::Analyzer;
    use crate::text::postings::InvertedBuilder;

    /// The dictionary says the term has postings, so a postings region that
    /// cannot hold them is corruption. Swallowing it into `None` would answer
    /// the query with silently fewer rows instead of failing.
    #[test]
    fn unreadable_postings_for_a_known_term_are_an_error_not_a_missing_term() {
        let mut b = InvertedBuilder::new();
        for (i, text) in ["alpha beta", "beta gamma", "gamma delta"].iter().enumerate() {
            let mut toks = Vec::new();
            Analyzer::Standard.analyze(text, 0, &mut toks);
            b.add_doc(i as u32, &toks);
        }
        let (dict, post, _) = b.finish();
        let dict = DictParts::parse(&dict).unwrap();
        let lens = b.doc_lens.clone();

        let src = TextSource::sealed(&dict, &post, &lens);
        assert!(src.try_cursor("gamma").unwrap().is_some());
        // A term that really is absent stays `Ok(None)`.
        assert!(src.try_cursor("epsilon").unwrap().is_none());

        // Same dictionary, no postings behind it.
        let corrupt = TextSource::sealed(&dict, &[], &lens);
        assert!(corrupt.try_cursor("gamma").is_err());
    }
}
