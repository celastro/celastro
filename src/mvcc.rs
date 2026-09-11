//! Commit timestamps, the delete log, and the visibility predicate.
//!
//! Indexes return ordinals for documents that may be deleted, superseded, or —
//! for a reader at an older snapshot — not yet committed. Visibility at
//! snapshot `T` is:
//!
//! ```text
//! visible(ord, T) = commit_ts(ord) ≤ T  ∧  ¬(delete_ts(ord) ≤ T)
//! ```
//!
//! The first conjunct is not academic (§4.4). A follower reading at a closed
//! timestamp is behind the leader, and a sealed segment can contain commits
//! newer than that follower's snapshot. Checking only deletes — which is the
//! natural thing to write, and what R2 of the design did — makes a single-node
//! test suite pass and a replicated cluster return the future.
//!
//! The payoff of routing every kind of invalidation through one log is in
//! [`DeleteLog::mark`]: **one entry invalidates the document across every index
//! type at once** — postings, vector graph, columns, secondary indexes. Nothing
//! is ever unlinked from an index, which is what lets deleted vectors stay in
//! the HNSW graph as routing nodes.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::bitmap::Bitmap;
use crate::codec::*;
use crate::error::{Error, Result};
use crate::time::{Timestamp, MAX_TS};

/// `ordinals.map`: ordinal → primary key and commit timestamp.
#[derive(Debug, Default, Clone)]
pub struct Ordinals {
    pub keys: Vec<String>,
    pub commit_ts: Vec<Timestamp>,
}

impl Ordinals {
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn push(&mut self, key: String, ts: Timestamp) {
        self.keys.push(key);
        self.commit_ts.push(ts);
    }
    pub fn key(&self, ord: u32) -> Option<&str> {
        self.keys.get(ord as usize).map(|s| s.as_str())
    }

    /// Segments are primary-key sorted, so this is a binary search — the same
    /// property that makes a tenant a contiguous ordinal range (§3.2).
    pub fn find(&self, key: &str) -> Option<u32> {
        self.keys.binary_search_by(|k| k.as_str().cmp(key)).ok().map(|i| i as u32)
    }

    /// The ordinal range covering `[lo, hi]` in primary-key order.
    pub fn range(&self, lo: Option<&str>, hi_inclusive: Option<&str>) -> (usize, usize) {
        let start = match lo {
            Some(l) => self.keys.partition_point(|k| k.as_str() < l),
            None => 0,
        };
        let end = match hi_inclusive {
            Some(h) => self.keys.partition_point(|k| k.as_str() <= h),
            None => self.keys.len(),
        };
        (start, end.max(start))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.keys.len() as u64);
        let mut prev = "";
        for (k, ts) in self.keys.iter().zip(&self.commit_ts) {
            // Front coding: keys are sorted, and in a partitioned collection
            // they share a long tenant prefix.
            let shared = {
                let (a, b) = (prev.as_bytes(), k.as_bytes());
                let mut i = 0;
                while i < a.len() && i < b.len() && a[i] == b[i] {
                    i += 1;
                }
                while i > 0 && !k.is_char_boundary(i) {
                    i -= 1;
                }
                i
            };
            put_uvarint(&mut out, shared as u64);
            put_str(&mut out, &k[shared..]);
            put_u64(&mut out, *ts);
            prev = k;
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<Ordinals> {
        let bad = || Error::Storage("ordinals: truncated".into());
        let mut i = 0usize;
        // The count comes off the file, and both vectors below are reserved
        // from it before a single entry is read: a corrupt count is then an
        // allocation request of arbitrary size, which the process answers by
        // aborting rather than by returning the `Error::Storage` this
        // signature promises. One entry costs at least ten bytes -- a shared
        // varint, a suffix length varint and eight bytes of commit timestamp
        // -- so a count past what the buffer could hold is corruption.
        let n = usize::try_from(get_uvarint(b, &mut i).ok_or_else(bad)?).map_err(|_| bad())?;
        if n > b.len().saturating_sub(i) / 10 {
            return Err(bad());
        }
        let mut o = Ordinals { keys: Vec::with_capacity(n), commit_ts: Vec::with_capacity(n) };
        let mut prev = String::new();
        for _ in 0..n {
            let shared = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
            let suffix = get_str(b, &mut i).ok_or_else(bad)?;
            let ts = get_u64(b, &mut i).ok_or_else(bad)?;
            // From a file: `shared` is untrusted. Validate it before it sizes
            // anything -- an unchecked slice is a panic on a corrupt or
            // truncated segment, and reserving `shared + suffix.len()` first
            // is an allocation abort (and, with overflow checks on, the
            // addition itself overflows), both on a path that runs at every
            // reopen.
            let prefix = prev.get(..shared).ok_or_else(bad)?;
            let mut k = String::with_capacity(prefix.len() + suffix.len());
            k.push_str(prefix);
            k.push_str(&suffix);
            o.keys.push(k.clone());
            o.commit_ts.push(ts);
            prev = k;
        }
        Ok(o)
    }
}

/// The only mutable per-segment state (§4.1). Replicated through Raft in the
/// distributed build; here it is an append-only file next to the segment.
#[derive(Debug, Default)]
pub struct DeleteLog {
    entries: BTreeMap<u32, Timestamp>,
}

impl DeleteLog {
    pub fn new() -> DeleteLog {
        DeleteLog::default()
    }

    /// Record that `ord` stops being visible at `ts`.
    ///
    /// An update is a new version in the memtable plus one of these on the old
    /// ordinal, with `delete_ts` equal to the update's commit timestamp — so no
    /// reader ever sees two versions and there is no read-time deduplication
    /// anywhere in the engine (§4.4).
    pub fn mark(&mut self, ord: u32, ts: Timestamp) {
        // Keep the earliest death: re-deleting cannot resurrect.
        let e = self.entries.entry(ord).or_insert(ts);
        if ts < *e {
            *e = ts;
        }
    }

    pub fn delete_ts(&self, ord: u32) -> Timestamp {
        self.entries.get(&ord).copied().unwrap_or(MAX_TS)
    }

    /// `MAX_TS` is the "never deleted" sentinel, so it must not compare as
    /// deleted even at a snapshot of `MAX_TS` — which is exactly the timestamp
    /// the write path uses to mean "the newest version, whatever it is".
    pub fn is_deleted_at(&self, ord: u32, t: Timestamp) -> bool {
        let d = self.delete_ts(ord);
        d != MAX_TS && d <= t
    }

    /// Number of ordinals dead at `t`. Feeds the dead-ratio compaction trigger
    /// (§4.4), which is what keeps the visibility amplification in §6 bounded.
    pub fn dead_count(&self, t: Timestamp) -> usize {
        self.entries.values().filter(|&&ts| ts <= t).count()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, Timestamp)> + '_ {
        self.entries.iter().map(|(o, t)| (*o, *t))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (o, t) in &self.entries {
            put_u32(&mut out, *o);
            put_u64(&mut out, *t);
        }
        out
    }

    pub fn decode(b: &[u8]) -> Result<DeleteLog> {
        let mut i = 0usize;
        let mut d = DeleteLog::default();
        while i < b.len() {
            let o =
                get_u32(b, &mut i).ok_or_else(|| Error::Storage("delete log: truncated".into()))?;
            let t =
                get_u64(b, &mut i).ok_or_else(|| Error::Storage("delete log: truncated".into()))?;
            let e = d.entries.entry(o).or_insert(t);
            if t < *e {
                *e = t;
            }
        }
        Ok(d)
    }
}

/// Materialised visibility per snapshot timestamp, cached (§4.4).
///
/// The cache is keyed by timestamp because closed timestamps advance in steps:
/// a wave of follower reads at the same closed timestamp shares one bitmap, and
/// a leader read at a fresh timestamp builds one and discards it.
#[derive(Default)]
pub struct VisibilityCache {
    inner: Mutex<Vec<(Timestamp, u64, Bitmap)>>,
}

const VISIBILITY_CACHE_ENTRIES: usize = 8;

impl VisibilityCache {
    pub fn get_or_build(
        &self,
        t: Timestamp,
        delete_epoch: u64,
        build: impl FnOnce() -> Bitmap,
    ) -> Bitmap {
        {
            let g = self.inner.lock().unwrap();
            if let Some((_, _, bm)) = g.iter().find(|(ts, e, _)| *ts == t && *e == delete_epoch) {
                return bm.clone();
            }
        }
        let bm = build();
        let mut g = self.inner.lock().unwrap();
        g.push((t, delete_epoch, bm.clone()));
        if g.len() > VISIBILITY_CACHE_ENTRIES {
            g.remove(0);
        }
        bm
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

/// Build the visibility bitmap for a segment at snapshot `t`.
pub fn visibility(ords: &Ordinals, dlog: &DeleteLog, t: Timestamp) -> Bitmap {
    let n = ords.len();
    let mut bm = Bitmap::new(n);
    for (i, &cts) in ords.commit_ts.iter().enumerate() {
        if cts <= t {
            bm.set(i);
        }
    }
    for (ord, dts) in dlog.iter() {
        if dts <= t && (ord as usize) < n {
            bm.clear(ord as usize);
        }
    }
    bm
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ords() -> Ordinals {
        let mut o = Ordinals::default();
        for (i, k) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            o.push(k.to_string(), 100 + i as u64 * 10);
        }
        o
    }

    #[test]
    fn a_reader_behind_the_leader_cannot_see_newer_commits() {
        let o = ords();
        let d = DeleteLog::new();
        // Snapshot at 115: only the commits at 100 and 110 are visible, even
        // though the segment physically contains all five.
        let v = visibility(&o, &d, 115);
        assert_eq!(v.to_vec(), vec![0, 1]);
        assert_eq!(visibility(&o, &d, MAX_TS).popcount(), 5);
    }

    #[test]
    fn one_delete_entry_invalidates_at_and_after_its_timestamp() {
        let o = ords();
        let mut d = DeleteLog::new();
        d.mark(2, 200);
        assert_eq!(visibility(&o, &d, 150).to_vec(), vec![0, 1, 2, 3, 4]);
        assert_eq!(visibility(&o, &d, 250).to_vec(), vec![0, 1, 3, 4]);
        assert_eq!(d.dead_count(250), 1);
        assert_eq!(d.dead_count(150), 0);
    }

    #[test]
    fn an_update_never_shows_two_versions() {
        // Old ordinal 1 dies exactly when the new version commits at 500.
        let mut o = ords();
        let mut d = DeleteLog::new();
        o.push("b".to_string(), 500);
        d.mark(1, 500);
        for t in [499u64, 500, 501] {
            let v = visibility(&o, &d, t);
            let bs: Vec<u32> = v.iter().filter(|&i| o.keys[i as usize] == "b").collect();
            assert_eq!(bs.len(), 1, "at t={t} there must be exactly one visible `b`");
        }
    }

    #[test]
    fn ordinals_round_trip_and_range_scan() {
        let mut o = Ordinals::default();
        for i in 0..1000 {
            o.push(format!("tenant-7/doc-{i:05}"), 1000 + i as u64);
        }
        let back = Ordinals::decode(&o.encode()).unwrap();
        assert_eq!(back.keys, o.keys);
        assert_eq!(back.commit_ts, o.commit_ts);
        let (s, e) = back.range(Some("tenant-7/doc-00010"), Some("tenant-7/doc-00019"));
        assert_eq!((s, e), (10, 20));
        assert_eq!(back.find("tenant-7/doc-00500"), Some(500));
    }

    #[test]
    fn delete_log_round_trips() {
        let mut d = DeleteLog::new();
        d.mark(3, 900);
        d.mark(7, 800);
        d.mark(3, 950); // later mark must not extend life
        let back = DeleteLog::decode(&d.encode()).unwrap();
        assert_eq!(back.delete_ts(3), 900);
        assert_eq!(back.delete_ts(7), 800);
        assert_eq!(back.delete_ts(9), MAX_TS);
    }

    /// The entry count is the first thing in the region and the last thing
    /// anyone checks, so a damaged one has to be rejected before it sizes the
    /// two vectors. Unbounded, this is `Vec::with_capacity(usize::MAX)`: an
    /// allocation the process answers by dying, on a file it was asked to
    /// report on.
    #[test]
    fn an_ordinals_count_the_region_cannot_hold_is_rejected_not_reserved() {
        let mut b = Vec::new();
        put_uvarint(&mut b, u64::MAX);
        let e = Ordinals::decode(&b).unwrap_err();
        assert!(matches!(e, Error::Storage(_)), "{e}");

        // One byte short of the ten an entry costs is still a lie about the
        // region, and the real encoding of one entry still decodes.
        let mut b = Vec::new();
        put_uvarint(&mut b, 1);
        b.extend_from_slice(&[0u8; 9]);
        assert!(Ordinals::decode(&b).is_err());
        let mut o = Ordinals::default();
        o.push("k".into(), 7);
        assert_eq!(Ordinals::decode(&o.encode()).unwrap().keys, vec!["k".to_string()]);
    }

    /// Front coding makes every key an offset into the one before it, and the
    /// offset is a file-supplied number. Sizing the new key from it before it
    /// is validated overflows the addition and reserves from a length no
    /// previous key ever had.
    #[test]
    fn an_ordinals_prefix_longer_than_the_previous_key_is_rejected_before_it_sizes_anything() {
        let mut b = Vec::new();
        put_uvarint(&mut b, 1); // one entry
        put_uvarint(&mut b, u64::MAX); // shared prefix, from a damaged file
        put_str(&mut b, "x");
        put_u64(&mut b, 42);
        let e = Ordinals::decode(&b).unwrap_err();
        assert!(matches!(e, Error::Storage(_)), "{e}");

        // A shared length that is merely longer than the previous key, rather
        // than absurd, is the same corruption and the same answer.
        let mut b = Vec::new();
        put_uvarint(&mut b, 1);
        put_uvarint(&mut b, 5);
        put_str(&mut b, "x");
        put_u64(&mut b, 42);
        assert!(Ordinals::decode(&b).is_err());
    }
}
