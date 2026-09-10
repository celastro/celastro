//! Dense ordinal bitmaps.
//!
//! Every index type in a segment produces sets in the segment's ordinal space
//! (§4.2), and this is the representation they meet in. A segment is capped at
//! a few million documents, so a flat word array costs ~1.25 MB at 10M
//! ordinals — cheap enough that the complexity of a compressed (roaring-style)
//! container buys nothing here, and intersection stays a tight word loop.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    pub fn new(len: usize) -> Self {
        Bitmap { words: vec![0u64; len.div_ceil(64)], len }
    }

    pub fn all(len: usize) -> Self {
        let mut b = Bitmap { words: vec![u64::MAX; len.div_ceil(64)], len };
        b.mask_tail();
        b
    }

    /// A contiguous ordinal range `[start, end)`. Because segments are sorted
    /// on `(partition_key, primary_key)`, a tenant occupies exactly such a
    /// range in every segment (§3.2) — the partition-key filter is this, not a
    /// scan.
    pub fn range(len: usize, start: usize, end: usize) -> Self {
        let mut b = Bitmap::new(len);
        let end = end.min(len);
        if start >= end {
            return b;
        }
        let (fw, lw) = (start / 64, (end - 1) / 64);
        if fw == lw {
            let mask = (!0u64 << (start % 64)) & (!0u64 >> (63 - ((end - 1) % 64)));
            b.words[fw] = mask;
        } else {
            b.words[fw] = !0u64 << (start % 64);
            for w in b.words[fw + 1..lw].iter_mut() {
                *w = !0u64;
            }
            b.words[lw] = !0u64 >> (63 - ((end - 1) % 64));
        }
        b
    }

    fn mask_tail(&mut self) {
        let rem = self.len % 64;
        if rem != 0 {
            if let Some(last) = self.words.last_mut() {
                *last &= (1u64 << rem) - 1;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    #[inline]
    pub fn set(&mut self, i: usize) {
        debug_assert!(i < self.len);
        self.words[i / 64] |= 1u64 << (i % 64);
    }

    #[inline]
    pub fn clear(&mut self, i: usize) {
        debug_assert!(i < self.len);
        self.words[i / 64] &= !(1u64 << (i % 64));
    }

    #[inline]
    pub fn get(&self, i: usize) -> bool {
        i < self.len && (self.words[i / 64] >> (i % 64)) & 1 == 1
    }

    pub fn popcount(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn and_inplace(&mut self, other: &Bitmap) {
        let n = self.words.len().min(other.words.len());
        for i in 0..n {
            self.words[i] &= other.words[i];
        }
        for w in self.words[n..].iter_mut() {
            *w = 0;
        }
    }

    pub fn or_inplace(&mut self, other: &Bitmap) {
        if other.len > self.len {
            self.words.resize(other.len.div_ceil(64), 0);
            self.len = other.len;
        }
        for i in 0..other.words.len().min(self.words.len()) {
            self.words[i] |= other.words[i];
        }
    }

    pub fn andnot_inplace(&mut self, other: &Bitmap) {
        for i in 0..self.words.len().min(other.words.len()) {
            self.words[i] &= !other.words[i];
        }
    }

    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let mut out = self.clone();
        out.and_inplace(other);
        out
    }

    pub fn negate(&self) -> Bitmap {
        let mut out = Bitmap { words: self.words.iter().map(|w| !w).collect(), len: self.len };
        out.mask_tail();
        out
    }

    /// Cardinality of the intersection without materialising it — used by the
    /// filtered-vector-search cost model (§5.3), which needs selectivity
    /// measured rather than estimated.
    pub fn and_popcount(&self, other: &Bitmap) -> usize {
        let n = self.words.len().min(other.words.len());
        (0..n).map(|i| (self.words[i] & other.words[i]).count_ones() as usize).sum()
    }

    /// Sum of `vals` over the ordinals this bitmap marks set.
    ///
    /// Word-wise rather than one `get` per ordinal: an all-ones word is the
    /// common case in a segment with few deletes, and summing its 64 lengths
    /// as a slice keeps that case at the speed of the unmasked sum it
    /// replaces. `vals` shorter than the bitmap contributes zero past its end
    /// — `doc_lens` is ordinal-dense, because `InvertedBuilder::add_doc` pads
    /// with zeros up to `ord` for every document, so the short case only
    /// arises from a segment with no doclens region at all — the
    /// `unwrap_or_default` in `Segment::text_index`. It is a silent contract all
    /// the same: a future sparse or skip-encoded length store would
    /// under-count here rather than fail.
    pub fn masked_sum(&self, vals: &[u32]) -> u64 {
        let mut total = 0u64;
        for (wi, &w) in self.words.iter().enumerate() {
            if w == 0 {
                continue;
            }
            let base = wi * 64;
            if w == !0u64 && base + 64 <= vals.len() {
                total += vals[base..base + 64].iter().map(|&v| v as u64).sum::<u64>();
                continue;
            }
            let mut cur = w;
            while cur != 0 {
                let b = cur.trailing_zeros() as usize;
                cur &= cur - 1;
                if let Some(&v) = vals.get(base + b) {
                    total += v as u64;
                }
            }
        }
        total
    }

    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter { bm: self, word: 0, cur: if self.words.is_empty() { 0 } else { self.words[0] } }
    }

    pub fn to_vec(&self) -> Vec<u32> {
        self.iter().collect()
    }

    pub fn from_sorted(len: usize, ords: impl IntoIterator<Item = u32>) -> Bitmap {
        let mut b = Bitmap::new(len);
        for o in ords {
            if (o as usize) < len {
                b.set(o as usize);
            }
        }
        b
    }

    /// First set bit at or after `from`, if any.
    pub fn next_set(&self, from: usize) -> Option<usize> {
        if from >= self.len {
            return None;
        }
        let mut w = from / 64;
        let mut cur = self.words[w] & (!0u64 << (from % 64));
        loop {
            if cur != 0 {
                return Some(w * 64 + cur.trailing_zeros() as usize);
            }
            w += 1;
            if w >= self.words.len() {
                return None;
            }
            cur = self.words[w];
        }
    }
}

pub struct BitmapIter<'a> {
    bm: &'a Bitmap,
    word: usize,
    cur: u64,
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        loop {
            if self.cur != 0 {
                let bit = self.cur.trailing_zeros();
                self.cur &= self.cur - 1;
                return Some((self.word * 64) as u32 + bit);
            }
            self.word += 1;
            if self.word >= self.bm.words.len() {
                return None;
            }
            self.cur = self.bm.words[self.word];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_matches_naive() {
        for len in [1usize, 63, 64, 65, 200] {
            for start in 0..len {
                for end in start..=len {
                    let b = Bitmap::range(len, start, end);
                    let got: Vec<u32> = b.to_vec();
                    let want: Vec<u32> = (start..end).map(|x| x as u32).collect();
                    assert_eq!(got, want, "len={len} start={start} end={end}");
                }
            }
        }
    }

    #[test]
    fn masked_sum_matches_a_naive_loop() {
        // Including the cases the word-wise fast path exists to get wrong: a
        // full word that runs past the end of `vals` must not take the slice
        // branch, and a final word with bits beyond `len` must not contribute.
        for len in [0usize, 1, 63, 64, 65, 129, 200] {
            let patterns: Vec<Bitmap> = vec![
                Bitmap::new(len),
                Bitmap::all(len),
                Bitmap::from_sorted(len, (0..len as u32).filter(|i| i % 2 == 0)),
                Bitmap::from_sorted(len, (0..len as u32).filter(|i| i + 1 == len as u32)),
                Bitmap::from_sorted(len, (0..len as u32).skip(len.saturating_sub(3))),
            ];
            for b in &patterns {
                for vlen in [0usize, len / 2, len, len + 7] {
                    // Two magnitudes, because the fast path and the slow path
                    // reach the same total by different arithmetic: the slow
                    // one widens each element to u64 before adding, the fast
                    // one sums a whole word's worth at once. Only wide values
                    // can tell them apart — a full word of these overflows 32
                    // bits, so a fast path that accumulated narrowly and
                    // widened at the end would disagree here and nowhere in
                    // the small-value cases below it.
                    for scale in [1u32, u32::MAX / 20] {
                        let vals: Vec<u32> =
                            (0..vlen).map(|i| ((i as u32 % 17) + 1) * scale).collect();
                        let want: u64 = (0..len)
                            .filter(|i| b.get(*i))
                            .map(|i| vals.get(i).copied().unwrap_or(0) as u64)
                            .sum();
                        assert_eq!(b.masked_sum(&vals), want, "len={len} vlen={vlen} x{scale}");
                    }
                }
            }
        }
        // The same asymmetry stated as a single fact, at the widest input the
        // element type admits: `total` is u64 and every branch has to reach it
        // that way.
        assert_eq!(
            Bitmap::all(64).masked_sum(&[u32::MAX; 64]),
            64 * u32::MAX as u64,
            "the word-wise fast path must widen before it adds"
        );
        // `masked_sum` reads whole words, so it inherits rather than
        // re-checks the tail invariant: every constructor clears the bits of
        // the final word that sit beyond `len`, and a stray one there would
        // silently add a document length that does not exist.
        let vals: Vec<u32> = vec![1; 128];
        assert_eq!(Bitmap::all(70).masked_sum(&vals), 70, "no bit past `len` contributes");
    }

    #[test]
    fn set_ops() {
        let mut a = Bitmap::new(200);
        for i in [1usize, 5, 64, 199] {
            a.set(i);
        }
        let mut b = Bitmap::new(200);
        for i in [5usize, 64, 100] {
            b.set(i);
        }
        assert_eq!(a.and_popcount(&b), 2);
        let mut c = a.clone();
        c.andnot_inplace(&b);
        assert_eq!(c.to_vec(), vec![1, 199]);
        assert_eq!(a.negate().popcount(), 196);
        assert_eq!(a.next_set(6), Some(64));
    }
}
