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

    pub fn cursor(&self, term: &str) -> Option<PostingsRef<'_>> {
        match self {
            TextSource::Sealed { dict, postings, .. } => {
                let m: TermMeta = dict.get(term)?;
                let slice = postings
                    .get(m.postings_off as usize..(m.postings_off + m.postings_len) as usize)?;
                PostingCursor::open(slice).ok().map(PostingsRef::Encoded)
            }
            TextSource::Memory { terms, doc_lens } => {
                let tp = terms.get(term)?;
                Some(PostingsRef::Mem(MemCursor::new(tp, doc_lens)))
            }
        }
    }

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
