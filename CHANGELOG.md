# Changelog

## 0.1.0 — 2026-10-06

The first release: a single-node document database. JSON documents in named
collections over HTTP (`PUT`, `GET`, `DELETE` by id; a listing in id order
with equality filters and paging), an append-only log per collection synced
before every write is acknowledged, a torn last record cut at start and a
damaged one refused, a bearer token, and one process per directory. Static
binaries for Linux on x86-64 and ARM64.
