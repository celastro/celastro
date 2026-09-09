//! The SQL front end.
//!
//! The surface is small on purpose. What matters is that the semantics users
//! will otherwise guess wrong are pinned down (§2.4), and every one of them is
//! decided here rather than somewhere in the executor:
//!
//! * `text_match(...)` inside `hybrid()` is a **should**: it contributes
//!   candidates and a rank. `text_match(...)` in `WHERE` is a **must**: it
//!   filters. Both can appear in one query, and they are different nodes in the
//!   parsed statement, not a flag on one node.
//! * `hybrid()` produces the **union** of candidates from its sources. A
//!   document surfaced by only one source gets that source's contribution only.
//! * `LIMIT` is **required** with `hybrid()`. Without it the query is a full
//!   scan and the planner rejects it.
//! * Distance operators follow pgvector conventions: `<->` L2, `<=>` cosine,
//!   `<#>` inner product. `ORDER BY <distance> LIMIT k` is recognised as an ANN
//!   access path, not a sort.
//! * Deep pagination uses cursors. `OFFSET n` is supported but costs `k + n`
//!   per shard, and says so in `EXPLAIN`.

pub mod ast;
pub mod lexer;
pub mod parser;

pub use ast::*;
pub use parser::parse;
