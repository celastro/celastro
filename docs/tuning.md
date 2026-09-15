# Tuning

Every knob that changes how fast celastro runs or how much memory it takes,
where it is read from, its default, and when to move it. All of them are
environment variables read once when `celastro-cli` starts (`serve`, `exec`,
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
| `CELASTRO_MEMTABLE_MAX_VECTORS` | `4096` | The same seal, by vectors held. At the default -- the flat tier's size -- a seal writes a flat segment and builds no graph; the graph is built when `COMPACT` merges segments past the flat tier. This is the knob that governs a vector ingest: measured over 50,000 documents with 128-dimensional vectors, `32768` (the default until 0.33.0) took 729 MB and 148 s, `16384` 437 MB and 177 s, `8192` 291 MB and 177 s, `4096` 219 MB and 10 s. What the smaller segments cost a query until they are compacted: a vector top ten 8.8 → 10.6 ms, a hybrid 8.4 → 9.2 ms, BM25 and point lookups nothing. Raise it only if you would rather build graphs during ingest than run `COMPACT` after it. |
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
| `CELASTRO_COMPACTION_TIER_FANOUT` | `4` | Segments of one size class merged into the next when this many have gathered. Lower merges sooner (fewer segments to search, more rewriting); higher the reverse. |
| `CELASTRO_COMPACTION_SEGMENT_CAP` | `5000000` | The largest segment compaction makes, in documents. |
| `CELASTRO_COMPACTION_DEAD_RATIO` | `0.30` | A segment whose deleted or superseded share passes this is rewritten on its own, whatever its size class. |

## The vector index

These shape a segment when it is sealed; a segment already built keeps
the shape it was built with.

| variable | default | what it does |
|---|---|---|
| `CELASTRO_VECTOR_QUANTIZER` | `sq8` | The compact code the graph is searched with: `sq8` (one byte a dimension) or `one-bit` (a bit a dimension, eight times smaller, coarser). Full-precision rerank follows either. |
| `CELASTRO_HNSW_M` | `16` | Neighbours per node above the bottom layer. More: better recall, more memory per vector, slower build. |
| `CELASTRO_HNSW_M0` | `32` | Neighbours per node on the bottom layer. |
| `CELASTRO_HNSW_EF_CONSTRUCTION` | `200` | The beam at build time. More: better graph, slower seal. |
| `CELASTRO_FLAT_TIER_MAX` | `4096` | Segments with this many vectors or fewer are searched brute force rather than through a graph: exact, and faster than a graph at this size. |

## What is not a knob

The accept loop, the write-ahead log's sync per statement, the lock reads
share, and the sizes of the wire's frames and the console's requests are
fixed: each was measured before it was set, and the design notes say
why. The chart's `probes.*` and `resources` are the pod's business.
