//! The coordinator's view of a shard.
//!
//! A query reaches a shard through exactly the calls on [`ShardService`] and
//! nothing else: term statistics, prefix expansion, candidate generation, an
//! unranked scan, and payload fetches. [`Local`] answers them from a
//! [`Shard`] in this process by direct call, which is the whole of what
//! celastro does today. The simulator ([`crate::sim`]) wraps a `Local` and
//! injects faults between the coordinator and the shard; a network
//! transport, when there is one, is another implementation of the same
//! trait, and the tests written against the simulator are the tests it has
//! to pass.
//!
//! What crosses the boundary is what a network could carry: identifiers, raw
//! scores, statistics, documents — never a rank, which only
//! [`crate::plan::fusion::fuse`] assigns (§7.2). The three metadata calls
//! (`index`, `manifest_version`, `may_hold`) are the coordinator's own tablet
//! map and are never faulted: a coordinator that does not know which shards
//! exist has nothing to fan out to.

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::Collection;
use crate::error::Result;
use crate::plan::exec::{self, SourcePlan};
use crate::plan::explain::ShardExplain;
use crate::plan::fusion::Candidate;
use crate::plan::walk::{self, ExpandRequest, HopExpansion};
use crate::shard::Shard;
use crate::sql::ast::Select;
use crate::text::scorer::GlobalStats;
use crate::time::Timestamp;
use crate::value::Value;

/// One path's term statistics over a shard's live corpus at an instant:
/// documents, their summed length, and each asked-for term's document
/// frequency. The coordinator sums these across shards; a sum is only
/// meaningful over triples measured at one instant, which is why they travel
/// together.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TermStats {
    pub num_docs: u64,
    pub total_doc_len: u64,
    pub doc_freq: BTreeMap<String, u64>,
}

/// What a shard needs to generate candidates for a ranked statement.
pub struct CandidatesRequest<'a> {
    pub coll: &'a Collection,
    pub select: &'a Select,
    pub ts: Timestamp,
    /// The partition-key prefix the statement is scoped to, if any.
    pub prefix: Option<&'a str>,
    pub sources: &'a [SourcePlan],
    /// Candidates per source this shard should return, at most.
    pub k_prime: usize,
    pub stats: &'a BTreeMap<String, GlobalStats>,
    pub analyze: bool,
    /// The statement's text and parameters, for a shard on another node,
    /// which parses them with the same crate into the same `select`.
    pub statement: &'a str,
    pub params: &'a [Value],
    /// The key set each `WITHIN k HOPS OF` of the statement resolved to, in
    /// predicate order. A shard on another node re-parses the statement and
    /// binds these the way the coordinator did (`walk::bind_hops`).
    pub frontiers: &'a [Vec<String>],
}

/// A shard's answer to a ranked statement: per source, at most `k_prime`
/// candidates, identifiers and raw scores only. `timed_out` means the shard
/// stopped at the deadline under `partial_results` and `per_source` is
/// empty: nothing a timed-out shard produced may reach the merge.
pub struct ShardCandidates {
    pub per_source: Vec<Vec<Candidate>>,
    pub explain: ShardExplain,
    pub timed_out: bool,
}

/// What a shard needs to run its half of an unranked scan.
pub struct ScanRequest<'a> {
    pub coll: &'a Collection,
    pub select: &'a Select,
    pub ts: Timestamp,
    pub prefix: Option<&'a str>,
    pub stats: &'a BTreeMap<String, GlobalStats>,
    pub analyze: bool,
    /// Rows the shard should retain: the page plus the offset in front of it.
    pub keep: usize,
    /// Resume after this primary key, for a cursor.
    pub after: Option<&'a str>,
    /// The `ORDER BY` fields and their directions; empty for key order.
    pub fields: &'a [(String, bool)],
    pub statement: &'a str,
    pub params: &'a [Value],
    pub frontiers: &'a [Vec<String>],
}

/// One row a shard retained. `doc` is present when the shard had to decode
/// the document to place the row (an `ORDER BY` field or a `COLLAPSE BY`
/// parent); otherwise `handle` names it — unit index and ordinal under the
/// shard's manifest version at scan time — and the coordinator fetches only
/// the rows that make the page through [`ShardService::documents`].
pub struct ScanHit {
    pub sort: Vec<Value>,
    pub key: String,
    pub doc: Option<Value>,
    pub handle: (usize, u32),
    pub parent: Option<Vec<u8>>,
}

/// A shard's answer to an unranked scan: its best `keep` rows. Unlike the
/// ranked answer, rows retained before a `partial_results` cut are kept and
/// `timed_out` says the shard may hold more: they are correct rows, and the
/// contract of that option is that some of a missing shard's rows may be
/// absent, not that all of them are.
pub struct ShardScan {
    pub hits: Vec<ScanHit>,
    pub explain: ShardExplain,
    pub timed_out: bool,
}

/// The calls a query makes on a shard, and nothing else.
///
/// An implementation that cannot deliver a call within the statement's
/// deadline returns [`crate::Error::Deadline`]; the coordinator treats that
/// exactly as a shard that ran out of time in this process — refused, or
/// under `WITH (partial_results)` reported in `missing` — so a slow shard and
/// an unreachable one are one case.
pub trait ShardService {
    /// The shard's position in its collection's tablet map. Answers are
    /// attributed by this, not by the order the services were handed over
    /// in, so that order can be anything.
    fn index(&self) -> usize;
    fn manifest_version(&self) -> u64;
    /// Whether the shard's key range can hold anything under `prefix` (a
    /// partition prefix or a whole key).
    fn may_hold(&self, prefix: &str) -> bool;
    fn term_stats(&self, path: &str, terms: &[String], ts: Timestamp) -> Result<TermStats>;
    /// The lexicographically first `limit` live terms under `prefix` on
    /// `path`, within `key_prefix` when given. The coordinator unions these
    /// and cuts at the collection's cap.
    fn prefix_terms(
        &self,
        path: &str,
        prefix: &str,
        ts: Timestamp,
        limit: usize,
        key_prefix: Option<&str>,
    ) -> Result<Vec<String>>;
    fn candidates(&self, req: &CandidatesRequest<'_>) -> Result<ShardCandidates>;
    fn scan(&self, req: &ScanRequest<'_>) -> Result<ShardScan>;
    /// The documents behind scan handles, in order. Refused with
    /// [`crate::Error::SnapshotGone`] if the manifest moved since the scan
    /// that issued them.
    fn documents(
        &self,
        manifest_version: u64,
        ts: Timestamp,
        handles: &[(usize, u32)],
    ) -> Result<Vec<Value>>;
    /// A document by primary key, visible at `ts`.
    fn get(&self, key: &str, ts: Timestamp) -> Result<Option<Value>>;
    /// The live documents at `ts`, what a plain `count(*)` sums: `None`
    /// from a holder that cannot say (one from before the call), which
    /// the coordinator answers by scanning instead.
    fn count(&self, ts: Timestamp) -> Result<Option<u64>>;
    /// One hop of a walk over this shard of an edge collection: the live
    /// edges leaving the frontier that the filter admits, as `(from, to)`
    /// pairs, sorted and distinct, at most `limit` per `from`, and how many
    /// units had to be scanned for want of an adjacency region.
    fn expand(&self, req: &ExpandRequest<'_>) -> Result<HopExpansion>;
    /// Which of `keys` are primary keys of documents visible at `ts` on
    /// this shard, sorted and distinct. How a walk tells a node from a
    /// dangling edge.
    fn present(&self, keys: &[String], ts: Timestamp) -> Result<Vec<String>>;
}

/// A shard in this process, answered by direct call.
pub struct Local<'a> {
    pub shard: &'a Shard,
    pub index: usize,
}

impl ShardService for Local<'_> {
    fn index(&self) -> usize {
        self.index
    }

    fn manifest_version(&self) -> u64 {
        self.shard.manifest_version
    }

    fn may_hold(&self, prefix: &str) -> bool {
        exec::shard_may_hold(self.shard, prefix)
    }

    fn term_stats(&self, path: &str, terms: &[String], ts: Timestamp) -> Result<TermStats> {
        let (num_docs, total_doc_len, doc_freq) = self.shard.term_stats(path, terms, ts)?;
        Ok(TermStats { num_docs, total_doc_len, doc_freq })
    }

    fn prefix_terms(
        &self,
        path: &str,
        prefix: &str,
        ts: Timestamp,
        limit: usize,
        key_prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut out = BTreeSet::new();
        self.shard.prefix_terms(path, prefix, ts, limit, key_prefix, &mut out)?;
        Ok(out.into_iter().collect())
    }

    fn candidates(&self, req: &CandidatesRequest<'_>) -> Result<ShardCandidates> {
        self.shard.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        exec::candidates_on(self.shard, self.index, req)
    }

    fn scan(&self, req: &ScanRequest<'_>) -> Result<ShardScan> {
        self.shard.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        exec::scan_on(self.shard, self.index, req)
    }

    fn documents(
        &self,
        manifest_version: u64,
        ts: Timestamp,
        handles: &[(usize, u32)],
    ) -> Result<Vec<Value>> {
        exec::documents_on(self.shard, manifest_version, ts, handles)
    }

    fn get(&self, key: &str, ts: Timestamp) -> Result<Option<Value>> {
        self.shard.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.shard.get(key, ts)
    }

    fn count(&self, ts: Timestamp) -> Result<Option<u64>> {
        Ok(Some(self.shard.num_docs(ts) as u64))
    }

    fn expand(&self, req: &ExpandRequest<'_>) -> Result<HopExpansion> {
        self.shard.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        walk::expand_on(self.shard, req)
    }

    fn present(&self, keys: &[String], ts: Timestamp) -> Result<Vec<String>> {
        walk::present_on(self.shard, &self.shard.coll, keys, ts)
    }
}
