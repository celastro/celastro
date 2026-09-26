# Tuning

Every knob that changes how fast celastro runs or how much memory it takes,
where it is read from, its default, and when to move it. All of them are
environment variables read once when `celastro` starts (`serve`, `exec`,
`run`, `repl`); a value that does not parse stops the start with the
variable's name. In the chart they are the `tuning:` map, one entry per
variable, set on every pod. Sizes take a `K`, `M` or `G` suffix (binary:
`64M` is 64 MiB).

The defaults were chosen for a node with a few gigabytes and an SSD. The
numbers quoted below were measured on four cores against 50,000 documents
with text and 128-dimensional vectors; they are there to show the shape of
the effect, not to be reproduced.

## Writes

| variable | default | what it does |
|---|---|---|
| `CELASTRO_INSERT_BATCH` | `1000` | Documents of one `INSERT` appended to a shard's log before it is synced. A statement of more is taken in chunks of this many, one `fdatasync` each; a statement of fewer is one sync. The statement is acknowledged only when every chunk is on disk, so a larger value costs nothing in durability, only the memory of the records in flight. 1,000 documents: 621 ms at one sync per document, 19 ms at one sync per thousand. Raise it for a bulk load of large statements; lower it if a statement's documents are large and memory is tight. |
| `CELASTRO_MEMTABLE_MAX_BYTES` | `64M` | A shard's memtable is sealed into a segment once it holds this many bytes of documents. Smaller: less memory per shard and smaller segments (more of them to search until compaction merges them); larger: fewer, bigger segments and more memory during a load. The first knob for a pod with a memory limit. |
| `CELASTRO_MEMTABLE_MAX_VECTORS` | `4096` | The same seal, by vectors held. At the default -- the flat tier's size -- a seal writes a flat segment and builds no graph; the graph is built when `COMPACT` merges segments past the flat tier. This is the knob that governs a vector ingest: measured over 50,000 documents with 128-dimensional vectors, `32768` (the default until 0.33.0) took 729 MB and 148 s, `16384` 437 MB and 177 s, `8192` 291 MB and 177 s, `4096` 219 MB and 10 s. What the smaller segments cost a query until they are compacted: a vector top ten 8.8 → 10.6 ms, a hybrid 8.4 → 9.2 ms, BM25 and point lookups nothing. Raise it only if you would rather build graphs during ingest than run `COMPACT` after it. Measured on the survey corpus: a memtable left to grow to 50,000 vectors replays from its WAL in 3.3 s (67 MB, 65 µs a row, linear) and reopens in 0.05 s once sealed -- rows without vectors replay much faster, a million small documents from a 93 MB log in 5.1 s (5 µs a row; `a_large_write_ahead_log_replays_every_row` with `CELASTRO_WAL_ROWS`), and `celastro_wal_bytes` is what a restart would replay -- but the seal itself builds the segment's graph under the write lock -- 146 s for those 50,000 -- so this cap is what bounds a seal's stall; the default keeps seals flat. |
| `CELASTRO_MEMTABLE_MAX_VERSIONS` | `8` | The same seal, by the longest version chain of one key, and only while a GC horizon is pinned (a backup, export or move in flight holds versions back): a seal then fans out into every retained version, and this bounds how many. |
| `CELASTRO_MEMTABLE_BUDGET_BYTES` | `1G` | The node-wide budget for every memtable together. Past it the largest is sealed early whatever its own thresholds say. With `MEMTABLE_MAX_BYTES`, what bounds a node's memory during ingest. |

**A pod with a memory limit.** Measured with the defaults above on the
same 50,000 documents plus 250,000 edge documents, loaded as 300
statements of 1,000: under a 512 MiB limit the load completes in 17 s;
under 256 MiB it is killed with the edges two thirds in. So: requests of
512 MiB and a limit of 1 GiB is the starting point for a node that
ingests, and `CELASTRO_MEMTABLE_MAX_BYTES=16M` with
`CELASTRO_RESIDENCY_BUDGET_BYTES` at half the limit for a smaller one.
The residency budget bounds what queries decode; the memtable thresholds
bound what a load holds; the two together are the node's memory, plus the
process itself and the page cache the kernel reclaims under pressure.

## Memory of the indexes

| variable | default | what it does |
|---|---|---|
| `CELASTRO_RESIDENCY_BUDGET_BYTES` | `4G` | How much decoded index the node keeps in memory across every collection. Past it the residency manager evicts, coldest tier first, `active` last. Set it to what the pod may use minus the memtable budget and headroom; a budget that yields to a declaration is not a budget. |
| `CELASTRO_CACHED_IDLE_UNLOAD_SECS` | `60` | An index on the `cached` tier is unloaded this long after its last use. |
| `CELASTRO_ARCHIVED_IDLE_UNLOAD_SECS` | `300` | The same for an index faulted in from the `archived` tier. |
| `CELASTRO_ARCHIVED_ACCESS` | `fault-in` | What a query that needs an archived index does: `fault-in` (one round trip to the store, then served) or `refuse` (an error, so a latency-bound service never waits on the store). |

## Statements

| variable | default | what it does |
|---|---|---|
| `CELASTRO_STATEMENT_DEADLINE_MS` | `30000` | The budget every statement runs under unless it names its own (`WITH (deadline_ms = N)`, `WITH (no_deadline)`). `0` lifts it. Checked inside the loops -- walks, brute-force distance, WAND, prefix expansion -- every 128 steps. |
| `CELASTRO_RECALL_SAMPLE_RATE` | `8` | One vector query in this many is kept for the recall harness (`MEASURE RECALL`). `0` keeps none. The cost is a copy of the query vector. |
| `CELASTRO_LIFECYCLE_INTERVAL_WRITES` | `0` | Run the lifecycle policies every this many writes; `0` runs them only on `RUN LIFECYCLE`. |
| `CELASTRO_MAX_CONNECTIONS` | `64` | Connections the console serves at once; a thread each. Past it the kernel's backlog holds the rest. Reads run side by side up to this many; raise it for many idle clients, lower it for a node whose threads should stay few. |

## Compaction

| variable | default | what it does |
|---|---|---|
| `CELASTRO_GROUP_COMMIT` | `on` | `serve` syncs a write statement's log after the lock is let go, together with every writer that appended meanwhile, and acknowledges it after that sync; a read never sees a write before it is durable. At a disk 50 ms slow to sync, a point read beside a writer took one sync (p99 61-67 ms) under the lock and 0.5-0.9 ms with it, and eight writers made 74 writes a second rather than 18. `off` syncs under the lock, one statement at a time. `celastro_wal_syncs_total` and `celastro_wal_sync_writers_total` show the grouping. |
| `CELASTRO_AUTO_COMPACT` | `on` | `serve` runs a compactor thread that compacts on its own: every second it asks whether a shard has a job, builds it and writes its segments with no lock held, and installs it under the lock, which then only publishes the manifest; one job at a time, logged with the milliseconds it held the lock (`lock_ms`). Seals have a thread of their own beside it, so none waits out a compaction's build. `off` leaves compaction to `COMPACT` and seals inline. Scripts and `exec` compact only on `COMPACT`. |
| `CELASTRO_COMPACTION_TIER_FANOUT` | `4` | Segments of one size class merged into the next when this many have gathered. Lower merges sooner (fewer segments to search, more rewriting); higher the reverse. |
| `CELASTRO_COMPACTION_SEGMENT_CAP` | `5000000` | The largest segment compaction makes, in documents. |
| `CELASTRO_COMPACTION_DEBT` | `32` | Backpressure: flat (level 0) segments a shard may hold before each write to it waits, so a load cannot run further ahead of compaction than reads can bear. `celastro_backpressure_waits_total` in the metrics counts the waits. |
| `CELASTRO_COMPACTION_DEBT_WAIT_MS` | `20` | The wait per flat segment past the debt, a second at most per write. |
| `CELASTRO_CATALOG_FORMAT` | this build's | The catalog format written to disk, pinned below this build's so the previous release can still open the directory after a rollback. Costs what the newer fields carry: at 7 and 8, the tombstones and instants the reconciliation compares by, so a drop made while pinned does not reconcile after a restart. Lift it once the release is trusted. |
| `CELASTRO_SEAL_IDENTITY` | `3` (this build's) | The identity and framing encrypted files are written under. `1` pins the writes to what 0.83.0 and earlier read: the shard and file alone in the seal identity (no collection), logs without a header and an id of their own, a `CELK1`/`CELK2` key ring. `2` pins them to what 0.86.0 and 0.87.0 read: a log's records under the file's key rather than a key per log (`CWL1` rather than `CWL2`), an archived copy's trailer empty rather than naming its place, a backup's record in the clear rather than sealed. Reads are always every form. Set it through the first days on a release that raised the form (0.84.1, 0.87.0) so a rollback can still open the directory; lift it once the release is trusted, and `celastro key rotate` re-seals every file under the current form. `1` costs what the identity guards against: a file of one collection standing in for another's, a log's record for another log's, a file cut at a frame boundary opening shorter. `2` costs the bound on a shard's log: its records share one key for the data key's life, and a shard writing twenty thousand records a second reaches the random-nonce bound (2^32) in days. Nothing for a database in the clear. |
| `CELASTRO_WIRE_TOKEN_ALSO` | unset | A second token the wire accepts from a peer, for the rollouts of a token rotation; [SECURITY.md](../SECURITY.md#rotating-the-wire-token) has the three steps. |
| `CELASTRO_WIRE_MAX_CONNECTIONS` | `1024` | Connections the wire serves at once; past it a connection is accepted and closed, and `celastro_wire_connections_refused_total` counts it. A thread per connection is what it bounds. |
| `CELASTRO_WIRE_IDLE_SECS` | `300` | A wire connection that carried no frame for this long is closed; the peer's next call reconnects on its own. `0` keeps them. |
| `CELASTRO_REPLICATION` | `sync` | Whether a write is acknowledged once every live follower has it on disk (`sync`) or at once (`async`, the followers trailing by the shipping lag), for a collection that sets no `confirm` of its own. A follower that is away holds no acknowledgement either way; `confirm = 'quorum'` on the collection is the rule that refuses instead. |
| `CELASTRO_REGION` | unset | The region this node runs in, a name; its hello carries it, `SHOW HEALTH` shows it, a collection with `regions = n` spreads its copies over that many, and a promotion under `confirm = 'all'` prefers a copy in the holder's region. `CELASTRO_REGIONS=eu,eu,us` is the same picked by the node's ordinal (`celastro-2` takes the third), for a StatefulSet whose pods share one environment. |
| `CELASTRO_STEWARD` | the lowest attached address | The node that renews every node's lease on each reconcile sweep and, with automatic failover on, promotes. The same on every node, or the lowest address decides. |
| `CELASTRO_AUTO_FAILOVER` | `off` | `on` makes the steward promote the follower with the most recent copy once a holder has missed two sweeps *and* its lease has run out, and makes a holder whose lease ran out refuse writes until it is renewed. Off, promotion is `PROMOTE SHARD` by hand and no lease gates a write. |
| `CELASTRO_STEWARDS` | unset | The addresses (comma-separated) that elect the steward among themselves; set, `CELASTRO_STEWARD` is ignored. Three is the number: two of them make a majority, and one down changes nothing. A steward that cannot reach a majority for half a lease stops renewing leases and steps down; a new steward promotes nothing for a lease and a quarter after its election. `SHOW HEALTH` names the steward and its term. Unset, the steward is `CELASTRO_STEWARD` or the lowest attached address, as before. |
| `CELASTRO_REPLACE_SECS` | `600` | How long a follower is away -- that many seconds of missed sweeps -- before the steward, with `CELASTRO_AUTO_FAILOVER=on`, gives its copy up for lost and runs `REPLACE COPY`: the lost node struck from the map at the next term, a live data node following in its place (in a region the collection's `regions` still asks for, else the holder's), shipped from nothing. Zero never replaces. Long enough that a restart, a rollout or a network blip is not a copy re-shipped whole; the lost node, back, drops the copy it kept. |
| `CELASTRO_LEASE_SECS` | `60` | How long a lease lasts; the steward renews every node's every quarter of it, on a thread of its own, so a sweep that stretches under load renews nothing late. It is also how long a failover takes at least: the steward promotes nothing until the lost holder's lease has run out (a holder cut off rather than dead is still taking writes under it), counted from its last renewal and from the steward's own start. Ten seconds is the drills' setting; sixty is a minute of a lost holder's shards away. |
| (the seal's build) | off the lock | With `serve`, a seal that is due freezes the memtable and the console's sealer thread builds and writes its segments holding no lock; two frozen seals it has not caught up with are the bound, past which a write builds inline. `FLUSH` seals inline. A library caller without the thread builds inline unless it sets `DbOpts::background_seal` and runs the triple itself. |
| `CELASTRO_CLOCK_OFFSET_MICROS` | `0` | A drill's offset on every read of the wall clock, so one node sees a jumped clock without the kernel's moving; the start-up log says so. Never for a database anyone relies on. |
| `CELASTRO_RECONCILE_SECS` | `30` | How often the console pulls every known peer's catalog and adopts the definitions and drops this node missed while they could not reach each other. `0` turns the sweep off; `ATTACH NODE` reconciles regardless. `celastro_catalog_reconciled_total` counts what it changed. |
| `CELASTRO_COMPACTION_DEAD_RATIO` | `0.30` | A segment whose deleted or superseded share passes this is rewritten on its own, whatever its size class. |

## The vector index

These shape a segment when it is sealed; a segment already built keeps
the shape it was built with.

| variable | default | what it does |
|---|---|---|
| `CELASTRO_VECTOR_QUANTIZER` | `sq8` | The compact code the graph is searched with: `sq8` (one byte a dimension) or `one-bit` (a bit a dimension, eight times smaller, coarser). Full-precision rerank follows either. |
| `CELASTRO_HNSW_M` | `16` | Neighbours per node above the bottom layer. More: better recall, more memory per vector, slower build. |
| `CELASTRO_HNSW_M0` | `32` | Neighbours per node on the bottom layer. |
| `CELASTRO_HNSW_EF_CONSTRUCTION` | `200` | The beam at build time. More: better graph, slower build. The build is where a vector collection's CPU goes: at 200, a 16,384-node graph over 128 dimensions takes 33 s on one core (85 s before 0.33.1) and `COMPACT` over 50,000 vectors 94 s (0.35.0; 247 s before 0.33.1), most of it distance evaluations -- some 35,000 per node inserted, each a cache miss on a random vector, which is why wider SIMD did little and fewer evaluations is the lever. Halving it roughly halves both; measure recall with `MEASURE RECALL` before settling. |
| `CELASTRO_FLAT_TIER_MAX` | `4096` | Segments with this many vectors or fewer are searched brute force rather than through a graph: exact, and faster than a graph at this size. |

## What is not a knob

The accept loop, the write-ahead log's sync per statement, the lock reads
share, and the sizes of the wire's frames and the console's requests are
fixed: each was measured before it was set, and the design notes say
why. The chart's `probes.*` and `resources` are the pod's business.
