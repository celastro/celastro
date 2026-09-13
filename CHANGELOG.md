# Changelog

What changed for someone running the previous version. Each entry is written
from the point of view of upgrading INTO that version, so the paragraph under
0.4.0 is what a 0.3.0 user needs to know. Versions on
[crates.io](https://crates.io/crates/celastro); tags `vX.Y.Z` in this
repository.

## Unreleased

**`serve` handles SIGTERM and SIGINT.** As PID 1 in a container it received
neither, so `docker stop` waited ten seconds and killed it; it now ends the
accept loop, saves and exits 0, and the banner says so. The route is an
in-tree `signal(2)` binding, recorded in `src/signal.rs`, because `std` has
no signal API and the crate takes no dependencies. Only `serve` installs it.
`--init` is no longer needed for `serve`.

**The statistics cache ages by its own collection's writes.** The refresh
gate compared against an engine-wide write count, so a burst on an
unrelated collection ended this one's epoch and unanchored its globals. It
counts writes to the collection it guards now, so the refresh interval
describes the staleness rather than bounding it.

## 0.10.1 — 2026-09-13

A patch: a durability fix, and documentation. Nothing observable moves
short of a power loss during a tier move.

**A tier move is durable, and the durability guarantee says where it
stops.** Moving a segment between `segments/` and `archive/` renamed the
file and fsynced nothing, so a crash could take the new name back and leave
a segment the manifest names in neither directory; both directories are
fsynced now, destination first, like every other publication. The crate
docs, the README and the design notes state that the directory fsync behind
the guarantee is a POSIX operation and a no-op off unix.

## 0.10.0 — 2026-09-13

A behaviour change for a query with more tied documents than its candidate
depth in an unflushed memtable, hence a minor. No public signature changed
and the on-disk format is unchanged.

**A score tie inside a memtable no longer depends on insertion order.** A
unit hands the coordinator its best `k'` candidates, and inside a memtable a
tie at the last slot went by push order rather than by key, so with more
tied documents than `k'` in an unflushed memtable the answer depended on how
many memtables the corpus was split across. Ties now go by key everywhere, at
a cost only the memtable pays.

## 0.9.0 — 2026-09-13

Breaking for library callers who built an options or report struct
literally, or called into a shard's storage; the SQL surface and the
on-disk format are unchanged. The version after this one is 0.10.0.

**The public surface is deliberate.** Every options struct (`DbOpts`,
`SearchOpts`, `ResidencyOpts`, `Placement`, `FlushThresholds`,
`CompactionOpts`, `BuildOpts`, `HnswParams`, `Bm25Params`, `ShardOpts`,
`WithOpts`) and every report struct (`Explain` and its parts,
`VectorReport`, `RecallReport`, `IndexActivity`, `PathStats`) is `#[non_exhaustive]`: build options from `Default` and set
fields, read reports, and a field added later cannot break you again the
way 0.3.0 and 0.8.0 did. `Placement::new(node_id, replicas)` replaces its
struct literal. `Shard` is reachable through `Db::shards` for reading only;
its writes, its flush and compaction, its publication and its write-ahead
log are crate-private now, as are its statistics gathers with documented
preconditions. Breaking for a caller who built any of those structs
literally or called into a shard's storage; both were undocumented.

## 0.8.0 — 2026-09-13

Two behaviour changes on a published crate, hence a minor: every statement
now has a deadline, and a filtered vector search may choose a different
strategy than before. No public signature was removed; `DbOpts`,
`SearchOpts` and `VectorReport` each gained a field, and `WithOpts` gained
two. The on-disk format is unchanged.

**Every statement has a deadline, and it is enforced inside the work.** A
query's CPU used to be bounded only by an opt-in: `WITH (deadline_ms)`
existed, was off by default, and was checked between shards, so one shard's
work ran as long as it ran. A `Db` now gives every query 30 seconds
(`DbOpts::statement_deadline_ms`), a statement may raise it with
`WITH (deadline_ms = N)` or lift it with `WITH (no_deadline)`, and the
graph traversal, the brute-force pass, the WAND loop, the prefix walk and the
scan each stop when it passes. A statement that ran out is refused with the
budget named, or, under `WITH (partial_results)`, answered with the shards
that ran out listed as missing. `EXPLAIN ANALYZE` shows the budget. A
statement that used to finish slowly may now be refused; name a budget if
thirty seconds is not enough.

**Filtered vector search prices the traversal that would run.** The cost
model charged a filter-aware traversal `ef` visits when its real cost is
about `ef / s`, so at low selectivity it chose a traversal of most of the
segment over an exact scan of the few survivors, twenty times the work it had
costed. It now prices the arm it would run, which picks the scan far more
often on segments under a hundred thousand documents; a scan is exact, so
where the choice changes the answer can only improve. `WITH (max_visits = N)`
bounds a traversal for a caller that would rather have a short answer, and
`EXPLAIN ANALYZE` reports `visits` beside the budget so it shows whether it
bound.

**The console says when a query was cut.** A wide prefix that hit the
expansion cap was reported by the shells and both JSON wires and not by the
browser console, which showed the short table and said nothing. It now shows
the same `TRUNCATED —` lines beside its partial-result warning.

## 0.7.0 — 2026-09-13

A new predicate shape, hence a minor. No public signature changed and the
on-disk format is unchanged.

**A distance threshold in `WHERE`.** `WHERE embedding <=> [..] < 0.2` is a
predicate: it composes with structured and text predicates, contributes no
rank, and selects exactly the rows whose `distance` column on the
nearest-neighbour path would satisfy the comparison, for the same operator.
Exact match is `<-> [..] <= 0` for L2; for cosine, normalisation leaves an
identical vector within rounding of 0 rather than at it, so ask for
`<=> [..] < 0.000001`. It is evaluated exactly, by a full-precision pass over
the rows the other predicates left, and `EXPLAIN ANALYZE` says so. Under
`NOT` it is three-valued: a document with no vector is on neither side.

**The console offers its source.** The page names the licence and links the
source of the running version, and `/api/health` carries the same URL and
the licence identifier. The link is the repository the crate declares at the
tag of the running version; a fork that serves the console must point
`repository` in Cargo.toml at its own source.

## 0.6.1 — 2026-09-13

A patch: nothing observable moves except how much memory and time a scan
costs.

**An unranked scan holds one page, not the collection.** `SELECT * FROM docs
LIMIT 1` used to decode and buffer every matching document before taking one
of them, so memory for a scan was bounded by the collection rather than by
`LIMIT` and `OFFSET`. A scan now retains at most `offset + k` rows as it goes,
and a key-ordered scan does not decode a document until it is known to be on
the page. `ORDER BY` on a field still reads every survivor to place it, but
holds one page of them. `COLLAPSE BY` and cursors answer exactly as before.

## 0.6.0 — 2026-09-13

A behaviour change on every query surface, hence a minor: rows carry what
the SELECT list names. No public signature changed and the on-disk format is
unchanged.

**The SELECT list is honoured.** It was parsed and read nowhere, so every
query returned the whole document on every surface: the shells printed every
field and the JSON wire carried the whole `doc` whatever was asked. A row now
carries the named paths only, keyed by alias or by the path as written, with
`Null` where a document lacks one so that rows share a shape. `*` keeps the
whole document, and a ranked query's `score` and `distance` stay on the row
whether or not the list names them. Anything that read a field it did not
name gets `Null` for it now; name it.

## 0.5.0 — 2026-09-13

The delete log gained a frame, so this is a format change with a compatible
read path: a 0.4.0 directory opens as it is, and its delete logs are rewritten
in the new shape as they are next published. No public signature changed.

**An unreadable file fails the open instead of opening empty.** CATALOG,
MANIFEST and every delete log used to be read as absent when the read failed
with anything but "no such file", so an I/O error at open produced a database
with no collections, a shard with no segments or a segment with no deletions —
and the next persist wrote that emptiness over the real file. Each now fails
the open with an error naming the file. A genuinely absent file still means
absent.

**The delete log is framed.** It was the one file in the format with no magic,
version or checksum, so a log that lost its tail at a record boundary decoded
as a shorter log and the documents in the lost records came back. A framed log
refuses every truncation and every flipped byte. A log written by an earlier
version still decodes, and the next publication that touches it rewrites it
framed.

**The catalog counts every document once.** The persisted catalog used to
include the memtable's documents, which the write-ahead log also holds, so
every reopen replayed and counted them again on top of a total that already
had them: four documents read as 8, 16 and 24 across four sessions, while
`SELECT *` answered 4 rows throughout. `SHOW CATALOG`, the `catalog` verb and
the planner's per-path `present` counts were all wrong by the same amount. The
persisted catalog now counts sealed documents only and the replay counts the
rest, so the number is exact whether or not the last session persisted. An
existing directory's inflated count is not repaired — the persisted total is
trusted as the sealed baseline — but it stops growing.

## 0.4.0 — 2026-09-13

An acknowledged write now survives a power loss.
Nothing on the 0.3.0 write path was fsynced: the WAL record was written and
never synced, and the rename that publishes a segment or a manifest was never
made durable, so an acknowledged insert could be lost with the page cache. The
record is now synced before the acknowledgement and every publication is
followed by a directory fsync. It is also faster, because a persist used to
rewrite CATALOG and every MANIFEST with unchanged bytes on every statement: an
acknowledged insert went from 2360 to 644 microseconds at one shard and from
8856 to 688 at six.

Three things change on the way back in. A reopen now unlinks segment and
delete-log files the manifest does not name — the leftovers of a publication
that failed — and never hands their ids out again, where 0.3.0 could reopen a
new segment carrying a dead one's delete log. A segment whose whole-body
checksum does not match is refused at open; 0.3.0 wrote the checksum and did
not check it. And a value nested deeper than 128 levels, reachable only
through `set_path` and never through SQL or JSON, is refused on decode rather
than aborting the process.

Two answers move. An integer past 2^53 in a shredded `Number` column used to be
rounded to a double on the way in, so `2^53 = 2^53 + 1` was true; such values
are now compared exactly. And a negative `dims`, `ef_search` or `deadline_ms`
is refused rather than wrapped to the maximum.

No public signature changed and the on-disk format is the same, so a 0.3.0
directory opens as it is.

## 0.3.0 — 2026-09-11

Cut hours after 0.2.0 because 0.2.0 can lose data. 0.2.0 and 0.1.0 are yanked
for the same reason; both still resolve for anyone pinned to them.

Fix a data-loss bug, and two things break.

A `DELETE` whose `text_match` predicate was cut by the expansion cap used to
run. In the negated shape that is not a short answer, it is a wrong one: a
truncated exclusion set deletes documents the predicate asked to spare, and
0.2.0 destroyed 488 rows in a case whose correct answer was none. Such a
statement is now REFUSED and nothing is written. If you relied on it completing,
spell the prefix as narrower pieces — each deletes exactly what it names.

A prefix query also used to answer differently depending on how the data
happened to be laid out on disk. The expansion cap applied per storage unit, so
`WHERE text_match(body,'a*')` returned 2199, 3928 or 3953 rows of the same 6000
matching documents at one, three and six shards. The expansion is now resolved
once against the live corpus, so the answer is the same everywhere — and, where
the cap binds, smaller than the largest of those. It is a real trade and the
number is in the prefix section below.

Breaking for library users: `GlobalStats` gained `expansions` and `QueryResult`
gained `truncated_prefixes`, so struct literals of either need updating. Both
are now `#[non_exhaustive]`, so the next field will not break you.

## 0.2.0 — 2026-09-11 (yanked)

Absolute BM25 scores move. The default query path
used to derive its statistics from physical rows, which counted superseded and
tombstoned versions and so drifted with flush and compaction timing — and, since
those are per shard, with the shard count. It now measures the live corpus, so
the same documents in the same order come back with different numbers against
them. Relative ranking is what this corrects rather than disturbs: a term is no
longer weighted by how much dead data happens to be on disk beside it. Anything
comparing scores against a stored threshold needs re-baselining; anything
comparing them against each other does not.

## 0.1.0 — 2026-09-10 (yanked)

First published version.
