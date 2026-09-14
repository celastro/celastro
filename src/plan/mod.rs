//! Planning and execution.
//!
//! The split matters more than the code: [`exec`] is the coordinator plus the
//! per-shard, per-segment work, and [`fusion`] is the coordinator *only*.
//! Nothing below the coordinator ever computes a rank, because a rank computed
//! from a shard-local list is not the rank fusion needs (§7.2).

pub mod exec;
pub mod explain;
pub mod fusion;
pub mod service;

pub use exec::{run_select, ExecInput, QueryResult, Row};
pub use explain::Explain;
