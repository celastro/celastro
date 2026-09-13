//! # celastro
//!
//! A hybrid document database: structured SQL, BM25 full-text and vector
//! similarity are three first-class retrieval modes evaluated in a *single*
//! query plan rather than orchestrated across separate services.
//!
//! The load-bearing idea is the **ordinal-space invariant** (§4.2):
//! inside a segment every document has a compact `u32` ordinal, and every index
//! type produces sets in that same space. Structured predicates become bitmaps,
//! text matching produces posting lists of ordinals, vector search produces an
//! ordinal set, and visibility is a bitmap. Hybrid candidate generation is
//! therefore bitmap intersection plus per-source scoring — no joins, no
//! identifier translation, no cross-service calls. An index that cannot speak
//! segment-local ordinals does not belong in a segment.
//!
//! This is a single node, with multiple shards in one process and explicit
//! key-range splits: real immutable segments, size-tiered compaction under a
//! hard segment cap, tiered vector indexes, storage tiers with lifecycle
//! policies, and runtime filtered-search strategy selection. Replication and
//! consensus are not here, but the boundary they attach to is: shards return
//! `(primary_key, source, raw_score)` candidates and fusion happens exclusively
//! in [`plan::fusion`] at the coordinator, because fusing segment-local ranks
//! is not equivalent to fusing global ranks (§7.2).

pub mod bitmap;
pub mod catalog;
pub mod codec;
pub mod column;
pub mod compaction;
pub mod deadline;
pub mod engine;
pub mod error;
pub mod harness;
pub mod json;
pub mod lifecycle;
pub mod memtable;
pub mod mvcc;
pub mod plan;
pub mod residency;
pub mod segment;
pub mod serve;
pub mod shard;
pub mod sql;
pub mod text;
pub mod time;
pub mod value;
pub mod variant;
pub mod vector;

pub use engine::{Db, DbOpts, Outcome};
pub use error::{Error, Result};
pub use residency::Tier;
pub use value::{Value, ValueType};
