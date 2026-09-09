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
