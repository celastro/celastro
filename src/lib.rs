//! # celastro
//!
//! A minimal single-node document database: JSON documents in named
//! collections, kept in memory and in an append-only log per collection that
//! is synced before every write is acknowledged, served over HTTP.

pub mod json;
pub mod server;
pub mod store;

/// This build's version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
