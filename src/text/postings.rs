//! Term dictionary and block-max postings.
//!
//! Postings are stored in blocks of [`BLOCK_SIZE`] ordinals. Each block carries
//! its last ordinal, its maximum term frequency and its minimum document
//! length. Those three numbers are what makes block-max WAND possible: BM25 is
//! monotone increasing in `tf` and decreasing in `dl`, so
//! `bm25(idf, max_tf, min_dl)` is a sound upper bound for every document in the
//! block, and whole blocks can be skipped without decoding them.
//!
//! The bound deliberately does *not* bake in `idf` or `avgdl`. Those are global
//! term statistics supplied per query (§8.2) so that scores are comparable
//! across shards; a bound computed at write time from local statistics would
//! quietly stop being a bound the moment the cluster changed shape.

use std::collections::BTreeMap;

use crate::codec::*;
use crate::error::{Error, Result};

pub const BLOCK_SIZE: usize = 128;

/// What the dictionary knows about a term.
#[derive(Debug, Clone, Copy, Default)]
pub struct TermMeta {
    pub doc_freq: u32,
    pub postings_off: u32,
    pub postings_len: u32,
}

// --------------------------------------------------------------------------
// Building
// --------------------------------------------------------------------------

#[derive(Default)]
pub struct TermPostings {
    pub ords: Vec<u32>,
    pub tfs: Vec<u32>,
    pub positions: Vec<Vec<u32>>,
}

/// Accumulates an inverted index for one segment. Terms arrive in document
/// order; the builder sorts once at the end.
#[derive(Default)]
pub struct InvertedBuilder {
    pub terms: BTreeMap<String, TermPostings>,
    pub doc_lens: Vec<u32>,
}

impl InvertedBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one analyzed field occurrence. `ord` must be non-decreasing across
    /// calls for a given term, which document-order indexing guarantees.
    pub fn add_doc(&mut self, ord: u32, tokens: &[(String, u32)]) {
        while self.doc_lens.len() <= ord as usize {
            self.doc_lens.push(0);
        }
        self.doc_lens[ord as usize] += tokens.len() as u32;
        let mut per_term: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
        for (t, p) in tokens {
            per_term.entry(t.as_str()).or_default().push(*p);
        }
        for (term, mut positions) in per_term {
            positions.sort_unstable();
            let e = self.terms.entry(term.to_string()).or_default();
            e.ords.push(ord);
            e.tfs.push(positions.len() as u32);
            e.positions.push(positions);
        }
    }

    pub fn num_terms(&self) -> usize {
        self.terms.len()
    }

    pub fn total_doc_len(&self) -> u64 {
        self.doc_lens.iter().map(|&l| l as u64).sum()
    }

    /// Serialise into `(dictionary, postings, doc_lens)`.
    pub fn finish(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut postings = Vec::new();
        let mut metas: Vec<(String, TermMeta)> = Vec::with_capacity(self.terms.len());
        for (term, tp) in &self.terms {
            let off = postings.len() as u32;
            encode_term_postings(tp, &self.doc_lens, &mut postings);
            metas.push((
                term.clone(),
                TermMeta {
                    doc_freq: tp.ords.len() as u32,
                    postings_off: off,
                    postings_len: postings.len() as u32 - off,
                },
            ));
        }
        let dict = encode_dict(&metas);
        let mut lens = Vec::with_capacity(self.doc_lens.len() * 4);
        for l in &self.doc_lens {
            put_u32(&mut lens, *l);
        }
        (dict, postings, lens)
    }
}

/// Per-term layout:
///
/// ```text
/// num_docs   u32
/// num_blocks u32
/// block meta [num_blocks] : last_ord u32, max_tf u32, min_dl u32,
///                           count u32, data_off u32, pos_off u32
/// data   : per block, delta-varint ordinals then varint term frequencies
/// pos    : per block, per doc, varint count then delta-varint positions
/// ```
fn encode_term_postings(tp: &TermPostings, doc_lens: &[u32], out: &mut Vec<u8>) {
    let n = tp.ords.len();
    let nblocks = n.div_ceil(BLOCK_SIZE);
    let mut data = Vec::new();
    let mut pos = Vec::new();
    let mut metas: Vec<[u32; 6]> = Vec::with_capacity(nblocks.min(1024));
    for b in 0..nblocks {
        let lo = b * BLOCK_SIZE;
        let hi = (lo + BLOCK_SIZE).min(n);
        let data_off = data.len() as u32;
        let pos_off = pos.len() as u32;
        let mut prev = 0u32;
        for i in lo..hi {
            put_uvarint(&mut data, (tp.ords[i] - prev) as u64);
            prev = tp.ords[i];
        }
        for i in lo..hi {
            put_uvarint(&mut data, tp.tfs[i] as u64);
        }
        for i in lo..hi {
            let ps = &tp.positions[i];
            put_uvarint(&mut pos, ps.len() as u64);
            let mut pp = 0u32;
            for p in ps {
                put_uvarint(&mut pos, (p - pp) as u64);
                pp = *p;
            }
        }
        let max_tf = tp.tfs[lo..hi].iter().copied().max().unwrap_or(1);
        let min_dl = tp.ords[lo..hi]
            .iter()
            .map(|&o| doc_lens.get(o as usize).copied().unwrap_or(1).max(1))
            .min()
            .unwrap_or(1);
        metas.push([tp.ords[hi - 1], max_tf, min_dl, (hi - lo) as u32, data_off, pos_off]);
    }
    put_u32(out, n as u32);
    put_u32(out, nblocks as u32);
    for m in &metas {
        for v in m {
            put_u32(out, *v);
        }
    }
    put_u32(out, data.len() as u32);
    out.extend_from_slice(&data);
    out.extend_from_slice(&pos);
}

const DICT_BLOCK: usize = 32;

/// Sorted term dictionary with front coding inside blocks and an uncompressed
/// first term per block for binary search. The design calls for an FST; a
/// front-coded sorted block dictionary has the same asymptotics for lookup and
/// is strictly better for the prefix-range scan that `text_match` prefix terms
/// need, at the cost of some space. Swapping in an FST is contained to this
/// file.
fn encode_dict(metas: &[(String, TermMeta)]) -> Vec<u8> {
    let mut blocks = Vec::new();
    let mut index: Vec<(String, u32)> = Vec::new();
    for chunk in metas.chunks(DICT_BLOCK) {
        index.push((chunk[0].0.clone(), blocks.len() as u32));
        let mut prev = "";
        for (term, m) in chunk {
            let shared = common_prefix(prev, term);
            put_uvarint(&mut blocks, shared as u64);
            put_str(&mut blocks, &term[shared..]);
            put_uvarint(&mut blocks, m.doc_freq as u64);
            put_uvarint(&mut blocks, m.postings_off as u64);
            put_uvarint(&mut blocks, m.postings_len as u64);
            prev = term;
        }
        put_uvarint(&mut blocks, u64::MAX); // block terminator
    }
    let mut out = Vec::new();
    put_u32(&mut out, metas.len() as u32);
    put_u32(&mut out, index.len() as u32);
    for (t, off) in &index {
        put_str(&mut out, t);
        put_u32(&mut out, *off);
    }
    put_u32(&mut out, blocks.len() as u32);
    out.extend_from_slice(&blocks);
    out
}

fn common_prefix(a: &str, b: &str) -> usize {
    let (ab, bb) = (a.as_bytes(), b.as_bytes());
    let mut i = 0;
    while i < ab.len() && i < bb.len() && ab[i] == bb[i] {
        i += 1;
    }
    // Never split a UTF-8 sequence.
    while i > 0 && !b.is_char_boundary(i) {
        i -= 1;
    }
    i
}

// --------------------------------------------------------------------------
// Reading
// --------------------------------------------------------------------------

/// The parsed dictionary: the block index in memory, the front-coded blocks
/// still as bytes. Parsing the index costs one pass and allocates one string
/// per block, so it happens once when a segment is opened rather than once per
/// query — a term lookup then costs a binary search over `index` and one block
/// decode.
#[derive(Debug, Clone, Default)]
pub struct DictParts {
    pub num_terms: u32,
    pub index: Vec<(String, u32)>,
    pub blocks: Vec<u8>,
}

impl DictParts {
    pub fn parse(b: &[u8]) -> Result<DictParts> {
        let bad = || Error::Storage("term dict: truncated".into());
        let mut i = 0;
        let num_terms = get_u32(b, &mut i).ok_or_else(bad)?;
        let nidx = get_u32(b, &mut i).ok_or_else(bad)? as usize;
        // Capacities from a count the bytes gave are bounded: a mutated
        // count of four billion was an allocation that aborted the process,
        // which is a crash a file can cause.
        let mut index = Vec::with_capacity(nidx.min(1024));
        for _ in 0..nidx {
            let t = get_str(b, &mut i).ok_or_else(bad)?;
            let off = get_u32(b, &mut i).ok_or_else(bad)?;
            index.push((t, off));
        }
        let blen = get_u32(b, &mut i).ok_or_else(bad)? as usize;
        let blocks = b.get(i..i + blen).ok_or_else(bad)?.to_vec();
        Ok(DictParts { num_terms, index, blocks })
    }
}

#[derive(Clone, Copy)]
pub struct TermDict<'a> {
    parts: &'a DictParts,
}

impl<'a> TermDict<'a> {
    pub fn new(parts: &'a DictParts) -> TermDict<'a> {
        TermDict { parts }
    }

    pub fn num_terms(&self) -> u32 {
        self.parts.num_terms
    }

    fn index(&self) -> &'a [(String, u32)] {
        &self.parts.index
    }

    fn block_for(&self, term: &str) -> Option<usize> {
        let index = self.index();
        if index.is_empty() {
            return None;
        }
        match index.binary_search_by(|(t, _)| t.as_str().cmp(term)) {
            Ok(i) => Some(i),
            Err(0) => None,
            Err(i) => Some(i - 1),
        }
    }

    pub fn get(&self, term: &str) -> Option<TermMeta> {
        let bi = self.block_for(term)?;
        for (t, m) in self.iter_block(bi) {
            match t.as_str().cmp(term) {
                std::cmp::Ordering::Equal => return Some(m),
                std::cmp::Ordering::Greater => return None,
                _ => {}
            }
        }
        None
    }

    fn iter_block(&self, bi: usize) -> Vec<(String, TermMeta)> {
        let blocks = &self.parts.blocks;
        let start = self.index()[bi].1 as usize;
        let mut i = start;
        let mut prev = String::new();
        let mut out = Vec::with_capacity(DICT_BLOCK);
        while let Some(shared) = get_uvarint(blocks, &mut i) {
            if shared == u64::MAX {
                break;
            }
            let Some(suffix) = get_str(blocks, &mut i) else { break };
            let mut term = String::with_capacity(shared as usize + suffix.len());
            // `shared` comes from the file; an unchecked slice panics on a
            // corrupt dictionary rather than reporting it.
            let Some(head) = prev.get(..shared as usize) else { break };
            term.push_str(head);
            term.push_str(&suffix);
            let doc_freq = get_uvarint(blocks, &mut i).unwrap_or(0) as u32;
            let postings_off = get_uvarint(blocks, &mut i).unwrap_or(0) as u32;
            let postings_len = get_uvarint(blocks, &mut i).unwrap_or(0) as u32;
            out.push((term.clone(), TermMeta { doc_freq, postings_off, postings_len }));
            prev = term;
        }
        out
    }

    /// Terms in `[prefix, prefix+\u{221e})`, capped. Backs prefix queries in the
    /// `text_match` grammar (§2.4).
    pub fn terms_with_prefix(&self, prefix: &str, limit: usize) -> Vec<(String, TermMeta)> {
        self.terms_with_prefix_where(prefix, limit, &mut |_, _| true)
    }

    /// [`terms_with_prefix`](Self::terms_with_prefix) with `keep` deciding
    /// which of the matching terms count.
    ///
    /// The cap counts KEPT terms, and that — not the filtering — is why this
    /// exists rather than the caller filtering the returned window. A rejected
    /// term must not spend the budget: filter a fixed-size unmasked window
    /// instead and a run of terms the caller rejects eats the whole cap and
    /// displaces every accepted term behind it. The live-masked caller in
    /// [`crate::text::TextSource::live_terms_with_prefix`] is where that bites
    /// — a design pass measured the filter-afterwards shape recovering 213 of
    /// 300 matching documents on a fixture where counting the cap in KEPT terms
    /// recovers all of them, and still moving with the compaction schedule.
    ///
    /// Enumeration therefore runs PAST a rejected run, so its cost is the cap
    /// plus the rejected terms it steps over — bounded by what the dictionary
    /// holds, never by the accepted vocabulary.
    pub fn terms_with_prefix_where(
        &self,
        prefix: &str,
        limit: usize,
        keep: &mut dyn FnMut(&str, &TermMeta) -> bool,
    ) -> Vec<(String, TermMeta)> {
        let mut out = Vec::new();
        let start = self.block_for(prefix).unwrap_or(0);
        let index = self.index();
        for bi in start..index.len() {
            if crate::deadline::expired() {
                break;
            }
            if out.len() >= limit {
                break;
            }
            if index[bi].0.as_str() > prefix && !index[bi].0.starts_with(prefix) {
                break;
            }
            for (t, m) in self.iter_block(bi) {
                if t.starts_with(prefix) {
                    if !keep(&t, &m) {
                        continue;
                    }
                    out.push((t, m));
                    if out.len() >= limit {
                        break;
                    }
                } else if t.as_str() > prefix && !t.starts_with(prefix) {
                    return out;
                }
            }
        }
        out
    }

    pub fn all_terms(&self) -> Vec<(String, TermMeta)> {
        let mut out = Vec::new();
        for bi in 0..self.index().len() {
            out.extend(self.iter_block(bi));
        }
        out
    }
}

/// Per-block metadata, decoded lazily.
#[derive(Clone, Copy, Debug)]
pub struct BlockMeta {
    pub last_ord: u32,
    pub max_tf: u32,
    pub min_dl: u32,
    pub count: u32,
    pub data_off: u32,
    pub pos_off: u32,
}

pub const EXHAUSTED: u32 = u32::MAX;

#[cfg(test)]
mod fuzz_tests {
    use super::*;

    /// The dictionary and the posting lists, mutated: a parser that reads
    /// a count and a cursor that walks blocks must refuse or stop, never
    /// panic or loop, whatever the bytes say.
    #[test]
    fn fuzz_dictionary_and_postings_never_panic() {
        let mut b = InvertedBuilder::new();
        let words = ["graph", "search", "vector", "index", "segment", "fusion", "rank", "hop"];
        for ord in 0..300u32 {
            let toks: Vec<(String, u32)> = (0..6)
                .map(|k| {
                    (words[(ord as usize * 3 + k) % words.len()].to_string(), (k as u32 % 3) + 1)
                })
                .collect();
            b.add_doc(ord, &toks);
        }
        let (dict, postings, _lens) = b.finish();
        assert!(DictParts::parse(&dict).is_ok());
        crate::fuzz::sweep(31, &[dict], 6000, |bytes| {
            let _ = DictParts::parse(bytes);
        });
        crate::fuzz::sweep(32, &[postings], 6000, |bytes| {
            if let Ok(mut c) = PostingCursor::open(bytes) {
                let _ = c.doc_freq();
                let mut at = c.advance(0);
                let mut steps = 0;
                while at != EXHAUSTED && steps < 10_000 {
                    at = c.advance(at.saturating_add(1));
                    steps += 1;
                }
            }
        });
    }
}

/// A cursor over one term's postings. `advance` is the only movement
/// primitive; block skipping happens inside it.
pub struct PostingCursor<'a> {
    meta: Vec<BlockMeta>,
    data: &'a [u8],
    pos_data: &'a [u8],
    num_docs: u32,
    block: usize,
    buf_ords: Vec<u32>,
    buf_tfs: Vec<u32>,
    idx: usize,
    loaded: bool,
    /// Sticky. Without it, a cursor that ran off the end would restart from
    /// block 0 the next time a disjunction re-advanced it — resurrecting a
    /// spent list and looping forever.
    exhausted: bool,
}

impl<'a> PostingCursor<'a> {
    pub fn open(b: &'a [u8]) -> Result<PostingCursor<'a>> {
        let bad = || Error::Storage("postings: truncated".into());
        let mut i = 0;
        let num_docs = get_u32(b, &mut i).ok_or_else(bad)?;
        let nblocks = get_u32(b, &mut i).ok_or_else(bad)? as usize;
        let mut meta = Vec::with_capacity(nblocks.min(1024));
        for _ in 0..nblocks {
            let last_ord = get_u32(b, &mut i).ok_or_else(bad)?;
            let max_tf = get_u32(b, &mut i).ok_or_else(bad)?;
            let min_dl = get_u32(b, &mut i).ok_or_else(bad)?;
            let count = get_u32(b, &mut i).ok_or_else(bad)?;
            let data_off = get_u32(b, &mut i).ok_or_else(bad)?;
            let pos_off = get_u32(b, &mut i).ok_or_else(bad)?;
            meta.push(BlockMeta { last_ord, max_tf, min_dl, count, data_off, pos_off });
        }
        let dlen = get_u32(b, &mut i).ok_or_else(bad)? as usize;
        let data = b.get(i..i + dlen).ok_or_else(bad)?;
        let pos_data = b.get(i + dlen..).ok_or_else(bad)?;
        Ok(PostingCursor {
            meta,
            data,
            pos_data,
            num_docs,
            block: 0,
            buf_ords: Vec::with_capacity(BLOCK_SIZE),
            buf_tfs: Vec::with_capacity(BLOCK_SIZE),
            idx: 0,
            loaded: false,
            exhausted: false,
        })
    }

    pub fn doc_freq(&self) -> u32 {
        self.num_docs
    }

    fn load_block(&mut self, b: usize) {
        let m = self.meta[b];
        let mut i = m.data_off as usize;
        self.buf_ords.clear();
        self.buf_tfs.clear();
        // A count the file gave, bounded by the bytes behind it (a posting
        // is a byte at least), and a delta summed without overflow: a
        // mutated block was an allocation that ended the process, and a
        // sum that panicked.
        let count = (m.count as usize).min(self.data.len().saturating_sub(i).max(1));
        let mut prev = 0u32;
        for _ in 0..count {
            let d = get_uvarint(self.data, &mut i).unwrap_or(0) as u32;
            prev = prev.saturating_add(d);
            self.buf_ords.push(prev);
        }
        for _ in 0..count {
            self.buf_tfs.push(get_uvarint(self.data, &mut i).unwrap_or(1) as u32);
        }
        self.block = b;
        self.idx = 0;
        self.loaded = true;
    }

    /// Move to the first posting with ordinal >= `target`; returns [`EXHAUSTED`]
    /// if there is none.
    pub fn advance(&mut self, target: u32) -> u32 {
        if self.meta.is_empty() || self.exhausted {
            return EXHAUSTED;
        }
        // Skip whole blocks by their last ordinal. This is the part that makes
        // the postings "block-max": no decoding happens for skipped blocks.
        let mut b = if self.loaded { self.block } else { 0 };
        while b < self.meta.len() && self.meta[b].last_ord < target {
            b += 1;
        }
        if b >= self.meta.len() {
            self.loaded = false;
            self.exhausted = true;
            return EXHAUSTED;
        }
        if !self.loaded || b != self.block {
            self.load_block(b);
        }
        while self.idx < self.buf_ords.len() && self.buf_ords[self.idx] < target {
            self.idx += 1;
        }
        if self.idx >= self.buf_ords.len() {
            // Only reachable when target lands past the block's decoded tail.
            if b + 1 < self.meta.len() {
                self.load_block(b + 1);
                return self.buf_ords.first().copied().unwrap_or(EXHAUSTED);
            }
            self.loaded = false;
            self.exhausted = true;
            return EXHAUSTED;
        }
        self.buf_ords[self.idx]
    }

    pub fn doc(&self) -> u32 {
        if self.exhausted || !self.loaded || self.idx >= self.buf_ords.len() {
            EXHAUSTED
        } else {
            self.buf_ords[self.idx]
        }
    }

    pub fn tf(&self) -> u32 {
        if !self.loaded || self.idx >= self.buf_tfs.len() {
            0
        } else {
            self.buf_tfs[self.idx]
        }
    }

    /// Upper bound on `tf` and lower bound on `dl` across the whole list.
    /// Together with a query-time `idf` these give the global `max_score` that
    /// WAND pivots on.
    pub fn global_max_tf(&self) -> u32 {
        self.meta.iter().map(|m| m.max_tf).max().unwrap_or(1)
    }

    pub fn global_min_dl(&self) -> u32 {
        self.meta.iter().map(|m| m.min_dl).min().unwrap_or(1)
    }

    pub fn block_meta(&self) -> Option<BlockMeta> {
        if self.loaded {
            self.meta.get(self.block).copied()
        } else {
            self.meta.first().copied()
        }
    }

    /// Ordinal of the last document in the current block — the horizon up to
    /// which `block_max` is a valid bound.
    pub fn block_last_ord(&self) -> u32 {
        self.block_meta().map(|m| m.last_ord).unwrap_or(EXHAUSTED)
    }

    /// Positions for the current posting. Only decoded when a phrase node asks,
    /// which is why positions live in their own region.
    pub fn positions(&self) -> Vec<u32> {
        if !self.loaded {
            return Vec::new();
        }
        let m = self.meta[self.block];
        let mut i = m.pos_off as usize;
        let mut out = Vec::new();
        for j in 0..=self.idx.min(m.count as usize - 1) {
            let n = get_uvarint(self.pos_data, &mut i).unwrap_or(0) as usize;
            if j == self.idx {
                let mut p = 0u32;
                for _ in 0..n {
                    p += get_uvarint(self.pos_data, &mut i).unwrap_or(0) as u32;
                    out.push(p);
                }
                return out;
            }
            for _ in 0..n {
                get_uvarint(self.pos_data, &mut i);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build() -> (Vec<u8>, Vec<u8>) {
        let mut b = InvertedBuilder::new();
        for ord in 0..500u32 {
            let mut toks = vec![("common".to_string(), 0u32)];
            if ord % 7 == 0 {
                toks.push(("rare".to_string(), 1));
            }
            if ord % 100 == 0 {
                toks.push(("scarce".to_string(), 2));
                toks.push(("scarce".to_string(), 3));
            }
            b.add_doc(ord, &toks);
        }
        let (dict, post, _) = b.finish();
        (dict, post)
    }

    #[test]
    fn dictionary_lookup_and_prefix() {
        let (dict, _) = build();
        let parts = DictParts::parse(&dict).unwrap();
        let d = TermDict::new(&parts);
        assert_eq!(d.num_terms(), 3);
        assert_eq!(d.get("common").unwrap().doc_freq, 500);
        assert_eq!(d.get("rare").unwrap().doc_freq, 72);
        assert!(d.get("nothere").is_none());
        let p = d.terms_with_prefix("sc", 10);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, "scarce");
    }

    #[test]
    fn a_capped_prefix_walk_returns_the_first_terms_and_counts_only_the_kept_ones() {
        // Two properties of one walk, and both are load-bearing well outside
        // this file.
        //
        // FIRST, not any: `Db::run_select` unions each unit's answer and takes
        // the first `PREFIX_EXPANSION_LIMIT` of the union, which is the
        // collection's true first-`limit` only because each unit answered with
        // its own first-`limit`. Return some other `limit` of them and a prefix
        // query means something different in every unit, silently.
        //
        // And the cap counts KEPT terms, so the walk runs PAST a rejected run
        // instead of spending the budget on it. That is what makes a rejected
        // term — in the live-masked caller, a term no visible document holds —
        // cost a step rather than a slot.
        let mut b = InvertedBuilder::new();
        for i in 0..300u32 {
            b.add_doc(i, &[(format!("a{i:05}"), 0)]);
        }
        let (dict, _, _) = b.finish();
        let parts = DictParts::parse(&dict).unwrap();
        let d = TermDict::new(&parts);

        let got: Vec<String> = d.terms_with_prefix("a", 10).into_iter().map(|(t, _)| t).collect();
        assert_eq!(got.first().map(String::as_str), Some("a00000"), "the FIRST ten, in order");
        assert_eq!(got.last().map(String::as_str), Some("a00009"));

        // The first 100 terms rejected. A window-then-filter implementation
        // returns nothing here, because all ten of its slots went to rejects;
        // counting the cap in kept terms returns `a00100`..`a00109`.
        let got: Vec<String> = d
            .terms_with_prefix_where("a", 10, &mut |t, _| t >= "a00100")
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert_eq!(got.len(), 10, "ten KEPT terms, not ten terms looked at");
        assert_eq!(got.first().map(String::as_str), Some("a00100"));
        assert_eq!(got.last().map(String::as_str), Some("a00109"));

        // A rejected run longer than the whole dictionary is exhausted rather
        // than looped on, and the prefix bound still holds.
        assert!(d.terms_with_prefix_where("a", 10, &mut |_, _| false).is_empty());
    }

    #[test]
    fn cursor_skips_blocks_and_finds_every_posting() {
        let (dict, post) = build();
        let parts = DictParts::parse(&dict).unwrap();
        let d = TermDict::new(&parts);
        let m = d.get("rare").unwrap();
        let slice = &post[m.postings_off as usize..(m.postings_off + m.postings_len) as usize];
        let mut c = PostingCursor::open(slice).unwrap();
        let mut got = Vec::new();
        let mut cur = c.advance(0);
        while cur != EXHAUSTED {
            got.push(cur);
            cur = c.advance(cur + 1);
        }
        let want: Vec<u32> = (0..500u32).filter(|o| o % 7 == 0).collect();
        assert_eq!(got, want);

        // A long jump must land exactly, not merely somewhere plausible.
        let mut c2 = PostingCursor::open(slice).unwrap();
        assert_eq!(c2.advance(300), 301);
        assert_eq!(c2.advance(495), 497);
        assert_eq!(c2.advance(499), EXHAUSTED);
    }

    #[test]
    fn positions_round_trip() {
        let (dict, post) = build();
        let parts = DictParts::parse(&dict).unwrap();
        let d = TermDict::new(&parts);
        let m = d.get("scarce").unwrap();
        let slice = &post[m.postings_off as usize..(m.postings_off + m.postings_len) as usize];
        let mut c = PostingCursor::open(slice).unwrap();
        assert_eq!(c.advance(100), 100);
        assert_eq!(c.positions(), vec![2, 3]);
        assert_eq!(c.tf(), 2);
    }
}
