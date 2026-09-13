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
/// distributed build; here it is a file beside the segment, rewritten whole
/// and published atomically by `Shard::persist_manifest`.
#[derive(Debug, Default)]
pub struct DeleteLog {
    entries: BTreeMap<u32, Timestamp>,
}

/// The delete log's frame. It was the one file in the format with none: a
/// bare run of `(u32 ordinal, u64 timestamp)` pairs, so a log that lost its
/// tail at a record boundary decoded cleanly and the documents in the lost
/// records came back, and a log with a flipped byte deleted a different
/// document at a different time. Every other file refuses both.
///
/// ```text
/// "CLDL"  u32 version  u64 count  (u32 ordinal, u64 timestamp) × count  u32 crc32
/// ```
///
/// The checksum covers everything before it. The count is redundant with the
/// length and is checked against it, so a truncation is reported as what it
/// is rather than as a checksum failure -- and it is never trusted to size an
/// allocation.
pub const DELETE_LOG_MAGIC: &[u8; 4] = b"CLDL";
pub const DELETE_LOG_VERSION: u32 = 1;
const DELETE_LOG_ENTRY: usize = 4 + 8;
const DELETE_LOG_HEADER: usize = 4 + 4 + 8;

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
        let mut out =
            Vec::with_capacity(DELETE_LOG_HEADER + DELETE_LOG_ENTRY * self.entries.len() + 4);
        out.extend_from_slice(DELETE_LOG_MAGIC);
        put_u32(&mut out, DELETE_LOG_VERSION);
        put_u64(&mut out, self.entries.len() as u64);
        for (o, t) in &self.entries {
            put_u32(&mut out, *o);
            put_u64(&mut out, *t);
        }
        let crc = crc32(&out);
        put_u32(&mut out, crc);
        out
    }

    /// Decode a published log, framed or not.
    ///
    /// A log without the frame is one written before the frame existed, and it
    /// is accepted as it was then so that an existing database opens: a whole
    /// number of records, at least one, because an empty log is never
    /// published. What that acceptance cannot do is notice a tail lost at a
    /// record boundary, which is the defect the frame exists for; the log is
    /// rewritten framed by the next publication that touches it, and the
    /// window closes there. The dispatch is on the magic. A legacy log whose
    /// first ordinal spells `CLDL` in little-endian would be misread as
    /// framed and refused; that ordinal is past 1.2 billion documents in one
    /// segment, and refused is the safe direction.
    pub fn decode(b: &[u8]) -> Result<DeleteLog> {
        if b.is_empty() {
            // Neither shape is ever published empty, so an empty file is a
            // publication that did not finish, not a log with nothing in it.
            return Err(Error::Storage("delete log: empty".into()));
        }
        if !b.starts_with(DELETE_LOG_MAGIC) {
            if b.len() % DELETE_LOG_ENTRY != 0 {
                return Err(Error::Storage("delete log: truncated".into()));
            }
            return Ok(DeleteLog::entries_of(b));
        }
        if b.len() < DELETE_LOG_HEADER + 4 {
            return Err(Error::Storage("delete log: truncated".into()));
        }
        let (body, tail) = b.split_at(b.len() - 4);
        if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
            return Err(Error::Storage("delete log: checksum mismatch".into()));
        }
        let mut i = DELETE_LOG_MAGIC.len();
        let version = get_u32(body, &mut i).unwrap();
        if version != DELETE_LOG_VERSION {
            return Err(Error::Storage(format!(
                "delete log: format version {version} unsupported (supports {DELETE_LOG_VERSION})"
            )));
        }
        let count = get_u64(body, &mut i).unwrap();
        let entries = &body[DELETE_LOG_HEADER..];
        // Checked against the length rather than used to size anything: the
        // count is disk bytes, and `count * 12` can overflow before the
        // comparison would have refused it.
        if count.checked_mul(DELETE_LOG_ENTRY as u64) != Some(entries.len() as u64) {
            return Err(Error::Storage(format!(
                "delete log: {count} entries declared, {} bytes present",
                entries.len()
            )));
        }
        Ok(DeleteLog::entries_of(entries))
    }

    /// `b` is a whole number of records, checked by the caller.
    fn entries_of(b: &[u8]) -> DeleteLog {
        let mut i = 0usize;
        let mut d = DeleteLog::default();
        while i < b.len() {
            let o = get_u32(b, &mut i).unwrap();
            let t = get_u64(b, &mut i).unwrap();
            let e = d.entries.entry(o).or_insert(t);
            if t < *e {
                *e = t;
            }
        }
        d
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

    /// What a log looked like before the frame: bare records, nothing else.
    fn legacy(entries: &[(u32, Timestamp)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (o, t) in entries {
            put_u32(&mut out, *o);
            put_u64(&mut out, *t);
        }
        out
    }

    /// A log written before the frame existed still decodes, to the same
    /// entries, so an existing database opens. The unframed shape is accepted
    /// only as it was ever written -- whole records, at least one.
    #[test]
    fn a_delete_log_without_the_frame_still_decodes() {
        let back = DeleteLog::decode(&legacy(&[(3, 900), (7, 800), (3, 950)])).unwrap();
        assert_eq!(back.delete_ts(3), 900);
        assert_eq!(back.delete_ts(7), 800);
        assert_eq!(back.len(), 2);
        let cut = legacy(&[(3, 900)]);
        for n in 1..cut.len() {
            assert!(DeleteLog::decode(&cut[..n]).is_err(), "a partial record decoded at {n} bytes");
        }
        assert!(DeleteLog::decode(&[]).is_err(), "an empty file is not an empty log");
    }

    /// The frame is not decoration. Every byte of a framed log is covered:
    /// cut anywhere, or change anywhere, and it is refused rather than read
    /// as a log with fewer or different deletions. The two halves of the
    /// frame are separately load-bearing -- the checksum catches the flip,
    /// the count catches a cut at a record boundary with a recomputed
    /// checksum, which is what a tool that "repairs" the file would produce.
    #[test]
    fn a_framed_delete_log_refuses_every_truncation_and_every_flipped_byte() {
        let mut d = DeleteLog::new();
        d.mark(3, 900);
        d.mark(7, 800);
        d.mark(11, 700);
        let full = d.encode();
        assert!(full.starts_with(DELETE_LOG_MAGIC));
        assert_eq!(DeleteLog::decode(&full).unwrap().len(), 3);
        for n in 0..full.len() {
            assert!(DeleteLog::decode(&full[..n]).is_err(), "cut to {n} bytes decoded");
        }
        for i in 0..full.len() {
            let mut b = full.clone();
            b[i] ^= 0x01;
            assert!(DeleteLog::decode(&b).is_err(), "a flipped bit at {i} decoded");
        }
        // Lose the last record and re-sign: the count is what refuses it.
        let mut cut = full[..full.len() - 4 - DELETE_LOG_ENTRY].to_vec();
        let crc = crc32(&cut);
        put_u32(&mut cut, crc);
        match DeleteLog::decode(&cut) {
            Err(Error::Storage(m)) => assert!(m.contains("3 entries declared"), "{m}"),
            Err(e) => panic!("the wrong failure: {e}"),
            Ok(d) => panic!("a re-signed log missing a record decoded with {} entries", d.len()),
        }
        // A version this reader does not know is refused by name.
        let mut next = full.clone();
        next[4..8].copy_from_slice(&(DELETE_LOG_VERSION + 1).to_le_bytes());
        let crc = crc32(&next[..next.len() - 4]);
        let n = next.len();
        next[n - 4..].copy_from_slice(&crc.to_le_bytes());
        match DeleteLog::decode(&next) {
            Err(Error::Storage(m)) => assert!(m.contains("format version 2"), "{m}"),
            other => panic!("an unknown version was not refused by name: {other:?}"),
        }
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
