# celastro

A hybrid document database. Structured SQL, BM25 full-text and vector similarity
are three first-class retrieval modes evaluated in a **single query plan**,
rather than orchestrated across separate services.

Written in Rust with **zero dependencies outside `std`** — no crates, no C
libraries. The bitmaps, term dictionary, block-max postings, quantizers, HNSW
graph, SQL parser, JSON parser and binary codecs are all in the tree.

```
cargo test --release                     # 354 tests
cargo run --release -- --demo            # guided tour over a small corpus
cargo run --release -- --dir ./data      # persistent REPL
cargo run --release -- --file script.sql # run a script
```

**Status.** Single node, with multiple shards in one process and explicit
key-range splits. Immutable segments, size-tiered compaction under a hard
segment cap, MVCC with snapshot reads, tiered vector indexes, runtime
filtered-search strategy selection, storage tiers with lifecycle policies, and
`EXPLAIN ANALYZE` over all of it. Replication, consensus and cross-shard
transactions are not here — see
[What is deliberately not here](#what-is-deliberately-not-here).

---

## The load-bearing idea

Inside a segment every document has a compact `u32` ordinal, and **every index
type produces sets in that same space**:

| index | produces |
|---|---|
| structured predicates | bitmap |
| text matching | posting lists of ordinals |
| vector search | ordinal set (via `vec_ordinals.map`) |
| visibility | bitmap |

Hybrid candidate generation is therefore bitmap intersection plus per-source
scoring — no joins, no identifier translation, no cross-service calls. This is
the mechanism behind the single-pass claim, and it is treated as a load-bearing
invariant: an index that cannot speak segment-local ordinals does not belong in
the segment.

`segment::tests::all_three_retrieval_modes_meet_in_one_ordinal_space` is the
test that pins it.

---

## Where things live

| concern | module | notes |
|---|---|---|
| logical path catalog, polymorphism, numeric widening | `catalog` | `PathClass`, `PathStats::observe` |
| arrays, multi-value columns, position gaps | `column`, `text::analyzer` | `ColumnData::MultiStr` carries a value→bitmap index |
| variant blob + shredded columns | `variant`, `column` | duplication, not remainder shredding |
| SQL surface and pinned semantics | `sql` | `LIMIT` required with `hybrid()`, `<->` / `<=>` / `<#>` |
| range layout on `(partition_key, primary_key)` | `shard::sort_key` | a tenant is a contiguous ordinal range |
| segment format, ordinal space | `segment` | self-describing footer (`CLST` magic), format versioning |
| memtable, node-level write budget | `memtable` | flat exact vectors; `MemtableBudget` |
| commit timestamps, delete log, visibility | `mvcc` | `commit_ts ≤ T ∧ ¬(delete_ts ≤ T)` |
| block-max WAND | `text::postings`, `text::scorer` | the filter is an admission predicate |
| tiered vector index, two-stage scoring | `vector`, `vector::hnsw`, `vector::quant` | SQ8 and 1-bit codes, full-precision rerank |
| runtime filtered-search selection | `vector::VectorStore::choose` | brute force / post-filter / ACORN-style |
| `COLLAPSE BY` | `plan::exec` | with `k` amplification |
| rank fusion, coordinator only | `plan::fusion` | RRF and weighted linear |
| scatter-gather, query-then-fetch | `plan::exec` | shards return `(pk, source, raw score)` |
| global term statistics | `engine::Db::gather_stats` | cached approximate, or exact two-phase |
| size-tiered compaction with a hard cap | `compaction` | dead-ratio, tier and format-upgrade triggers |
| control plane | `engine::Db` | catalog, tablet map, statistics cache |
| storage tiers, cache residency | `residency`, `segment` | lazy per-component loading, node budget, `SHOW RESIDENCY` |
| placement: which replica holds an index | `residency::Placement` | the `minimal` tier's decision function |
| index lifecycle policies | `lifecycle` | `CREATE LIFECYCLE POLICY … MOVE TO … AFTER n days` |
| `EXPLAIN ANALYZE`, continuous recall | `plan::explain`, `harness` | every runtime decision is reported |
| format versioning | `segment`, `compaction` | rolling rebuild through compaction, never a migration |
| backup horizon | `compaction::run` | `gc_horizon` holds version GC back |

Source comments cite section numbers (`§4.2`, `§8.4`) of the architecture
specification this implements. That document is not in the repository; the
citations are there so a reader can tell which choices are *required by the
design* and which are local implementation decisions.

## Tiers, residency and lifecycle

An index declares how ready it wants to be. A node decides what it can afford.
A policy connects the two over time — and the three are deliberately separate,
because they are answerable by different people on different timescales.

```sql
CREATE INDEX items_body ON items USING fulltext (body)
  WITH (analyzer = 'english', tier = 'active');          -- RAM, every replica
CREATE INDEX items_emb  ON items USING vector (embedding)
  WITH (dims = 768, metric = 'cosine', tier = 'minimal');-- RAM, exactly one node
ALTER INDEX items_emb ON items SET TIER 'archived';      -- object store

CREATE LIFECYCLE POLICY cool_down ON items FOR (items_body, items_emb)
  MOVE TO minimal  AFTER 30 minutes OF INACTIVITY,
  MOVE TO cached   AFTER 6 hours OF INACTIVITY,
  MOVE TO archived AFTER 7 days OF INACTIVITY,
  MOVE TO archived AFTER 90 days SINCE CREATION;

RUN LIFECYCLE ON items;   -- explicit, like COMPACT: it moves gigabytes
UNLOAD IDLE;              -- release idle components, then evict to budget
SHOW CATALOG items;       -- every index and the tier it is on
SHOW RESIDENCY;           -- what is decoded right now, and what it cost
SHOW LIFECYCLE;           -- every policy, with each index's idle time and age
```

Durations take `minutes`, `hours` or `days`, singular or plural, and the
`min` / `hr` / `day` abbreviations. `OF INACTIVITY` (or `SINCE ACCESS`) is the
default trigger; `SINCE CREATION` is the other question you might be asking.

### The ladder

| tier | decoded on | bytes live | first query after idle | aliases accepted |
|---|---|---|---|---|
| `active` | every node holding the tablet | local NVMe | already there | `hot`, `ram`, `memory`, `resident` |
| `minimal` | exactly one node, whatever the replica count | local NVMe | already there on that node; one segment read elsewhere | `warm`, `pinned`, `single`, `one_copy` |
| `cached` | no node between queries | local NVMe | one segment read | `cold`, `disk`, `ssd`, `nvme` |
| `archived` | never | archive store | one archive round trip, or refused | `archive`, `s3`, `object_store` |

Each name states its own guarantee rather than a temperature, because the tiers
do not differ along one axis. `active` and `minimal` are both "decoded in RAM";
they differ in *how many nodes*. `cached` and `archived` are both "not kept
decoded"; they differ in *where the bytes are*. A hot/warm/cold ladder hides
that, and hiding it is how an operator ends up believing `warm` is a slower
`hot` when it is in fact the same speed on one node and `cached` everywhere
else. (`warm` still parses. People will type it.)

### `minimal`: the tier that ignores the replication factor

`active` scales its memory cost with the number of replicas — three replicas
means three decoded copies — which is what you want for an index on the
critical path and ruinous for one that merely has to be *available*. `minimal`
says: somebody keeps this ready, exactly one somebody, and I do not care how
many nodes the tablet lives on.

```
1 replica(s) -> 1 decoded copy(ies)
3 replica(s) -> 1 decoded copy(ies)
5 replica(s) -> 1 decoded copy(ies)
```

The designated holder is `sorted(replicas)[mix64(hash(collection/component)) % n]`
— computed independently by every node from the tablet map it already has, so
nobody has to be told, and stable under everything but a change to that list.
Keyed on `collection/component` rather than including the shard, because the
holder should be one node per index, not one per tablet: a collection with sixty
shards would otherwise put a copy on every node in the cluster, which is exactly
what the tier exists to prevent.

Every other node resolves `minimal` to `cached`. That is the whole design, and
it works only because a tier is never a correctness boundary — a node that is
not the holder does not fail, it pays one segment read. The catalog keeps the
*declaration*; the segments, the ledger, the idle sweeper and `SHOW RESIDENCY`
all speak the *resolution*, and the resolution never leaks back into the
catalog — once the catalog is cluster state, that would demote the index for the
holder too.

A misconfiguration where `replicas` is non-empty and does not contain
`node_id` makes *no* node the holder — the guarantee silently becomes zero
copies rather than one — so it is refused at startup rather than obeyed.

### What each tier costs

A graph traversal is a chain of *dependent* reads, so an archived vector index
cannot serve a query at anything like an interactive p99 target — the design
budgets hundreds of milliseconds for it. An archived index is not a slower
served index; it is an index you have decided to stop serving.
`archived_access = Refuse` says that out loud, and the default fault-in is
reported by `EXPLAIN` rather than hidden:

```
    segment 3      docs=50000  visible=50000  survivors=1200  s=0.0240  8.10 ms
      residency: 2 component(s) decoded on demand, 1 faulted in from the archive
```

### Tuning

| `DbOpts::residency` | default | what it does |
|---|---|---|
| `budget_bytes` | 4 GiB | node-level ceiling on decoded index structures |
| `active_idle_unload` | `None` | an `active` index is kept until the budget forces a choice |
| `minimal_idle_unload` | `None` | same, on the one node designated to hold it |
| `cached_idle_unload` | 60 s | how long a `cached` index survives its last use |
| `archived_idle_unload` | 300 s | how long a fault-in is reused before being dropped |
| `archived_access` | `FaultIn` | or `Refuse`, to fail loudly instead of stalling |
| `DbOpts::placement` | single node | `{ node_id, replicas }` — only `minimal` consults it |
| `DbOpts::lifecycle_interval_writes` | `0` (off) | run policies every N writes, if you would rather not cron it |

### Four decisions worth naming

**A tier is a priority, not a guarantee.** The node budget outranks every
declaration. Eviction walks the ladder from the bottom and, within a rung, takes
the least recently used — but if only `active` components remain, `active`
components go. A budget that yields to a declaration is not a budget.

**A tier is never a correctness boundary.** Every component rebuilds from the
segment file, so unloading, evicting, archiving and faulting in change latency
and memory and nothing else. The tests state this as an invariant: run a query,
disturb residency, run the same query, compare the rows. It is also what makes
`minimal` implementable at all.

**A policy only demotes; use promotes — except for retention.** An inactivity
rule says "nobody wanted this", so the next query is evidence to the contrary
and restores the index to its *declared* tier — never past it, so an index
declared `cached` is not dragged into RAM by traffic. A `SINCE CREATION` rule
says "this is old", and a query is not an argument against age. Promoting there
would archive the index on the next run, promote it on the next query, and move
the segment files back and forth forever, so retention demotions stay put.

**The furthest matching rule wins.** An index idle for a fortnight under
`minimal after 30 min, archived after 7 days` goes straight to archived, not one
rung per run. The outcome is a function of the state, not of how often the
runner happens to fire.

### How it works

Segments load per component. `Segment::open` reads the footer, the ordinals map
and the blob index — everything else (`docs`, `col:<path>`, `text:<path>`,
`vec:<path>`) is a `Lazy` cell decoded on first use and released on demand.
Measured: opening a 6.5 MB segment reads 1.65% of it. Each region carries its
own checksum, so a component checks its integrity when it is decoded; a
whole-body check at open would mean reading every byte of every segment at every
startup, which is the cost lazy loading exists to avoid (`Segment::verify` still
scrubs the whole file on request).

Readers take the `Arc` out of the cell and let go of the lock, so an eviction
racing a query only drops the segment's own reference — the same discipline the
manifest uses for retired segment files. Both the load and the unload path
report to the ledger *under* that lock: release it first and a loader can
complete a reload inside the gap, and the evictor's bookkeeping then erases the
charge for bytes that are, at that moment, in RAM.

The ledger is node-wide and keyed by a per-open-segment uid rather than by
segment id, because ids are assigned per shard and two tablets both have a
segment 1. Its resident-byte total is only ever changed under the ledger lock,
with the delta computed from the entry's own recorded state, which makes the
accounting self-correcting: a duplicate load, a re-load, or an unload of
something already unloaded moves the total by exactly the right amount.

Archiving relocates the segment *file* between `segments/` and `archive/`. The
unit is the file, not the index, so a segment is archived only when nothing in
the collection still wants a local copy — coarser than the per-index tier, and
deliberately so, since splitting a segment by index would break the
ordinal-space invariant that makes hybrid retrieval a bitmap intersection.

**One gap, stated plainly.** The design allows full-precision vectors to be the
one component that stays cold. Here they are not: `vectors.full` loads as part
of the `vec:` component, so a resident vector index costs its full-precision
copy too — about `4 × dims` bytes per vector more than budgeted. `VectorStore`
holds `full` inline and reranking indexes into it directly, so splitting it into
its own residency component is a change to the vector module rather than to this
one, and it is not done.

---

## What is deliberately not here

Everything that needs more than one process: consensus and replication, follower
reads and closed timestamps, hedged requests, two-phase commit for multi-shard
writes, stateless compaction workers, dynamic shard split and merge, and a
deterministic simulator. The `archived` tier writes to a local directory that
stands in for object storage — the residency and lifecycle machinery around it
is real, the S3 client is not.

The *boundaries* those attach to are real, and that is the point of having built
them first:

- **Fusion happens only at the coordinator.** Shards return raw per-source
  candidate lists and never compute a rank. `plan::fusion::fuse` is the only
  function in the engine that assigns one.
- **A collection can have many shards**, created with explicit key-range split
  points (`CREATE COLLECTION … WITH (splits = ['t1','t2'])`). They run in one
  process, but a shard never sees another shard's candidates.
- **Readers pin both a timestamp and a manifest version.** Segment handles are
  `Arc`s; a compaction that retires a segment cannot unlink a file a reader
  still holds.
- **Visibility checks both conjuncts**, including `commit_ts ≤ T`, which only
  starts to matter once a follower can read behind a leader.
- **Placement is derived, not assigned.** Every node computes the holder of a
  `minimal` index from the tablet map it already has, so no tier change costs a
  coordination round.

That combination makes the distributed exit criterion testable now:
`exact_mode_is_bit_identical_across_shard_counts` runs the same corpus at 1, 3
and 6 shards and compares fused scores bit for bit.

---

## Design notes

Things worth knowing before reading the code, stated as they stand today.

**Fusion happens only at the coordinator, and this is not an optimisation.**
Reciprocal rank fusion over per-shard ranks is not the same function as fusion
over the global rank: a document ranked 3rd on its shard and 40th globally
contributes `1/(k+3)` instead of `1/(k+40)`. Shards therefore return raw
per-source candidate lists and never compute a rank; `plan::fusion::fuse` is the
only function in the engine that assigns one. Score normalisation has the same
shape — normalising per shard makes the scale shard-dependent, so it happens
once, over the merged set.

**`k'` truncation, not approximation, is what breaks shard-count identity.**
`n` shards each return up to `k'` candidates where one shard returns `k'`, so
the candidate union differs with the shard count and the fusion differs with it
— even in exact mode, where ANN and statistics are removed as sources of
variance. Bit-identical results across shard counts hold only at `k'` above the
candidate count, which is what `exact_mode_is_bit_identical_across_shard_counts`
pins.

**`search_after` over an approximate index is not cheaper than `OFFSET`.** A
graph search has no resume primitive: an HNSW heap cannot restart from a
distance without re-traversing, so an ANN cursor costs `depth + k` per shard,
exactly what `OFFSET` costs. Its advantage is stability under concurrent writes,
not cost. A text source genuinely can resume from a score threshold; the two
cases are worth keeping separate in one's head.

**The cost model picks brute force far more often than a static planner would.**
With `m0 = 32`, an ANN traversal touches roughly `ef × 32` code distances, so
exact scan wins until survivors exceed about `11 × ef` — around 1,400 documents
at `ef = 128`. Filter-aware traversal earns its place only when a small
*fraction* is still a large *count*, order 10⁵ documents per segment. The
regimes are separated by absolute counts that depend on `m0`, dimensionality and
the filter, which is the argument for choosing at runtime from measured
selectivity rather than estimating.

**1-bit quantization needs a much deeper rerank set than SQ8.** Measured on
isotropic Gaussians — the worst case for sign quantization — 5× rerank gives
recall@20 of 0.73, and 20× gives 0.94. The 32× smaller code is paid for in
full-precision reads, which lands on the cold-tier read budget rather than on
memory.

**Anything that slices bits off one end of a hash needs an avalanche
finalizer.** Raw FNV-1a leaves the high bits poorly distributed for short,
similar inputs: hashing `note-0000` … `note-0599` and taking the top 10 bits as
a HyperLogLog bucket lands all 600 in 9 buckets. Nothing errors — the planner
just receives cardinality estimates wrong by two orders of magnitude. `codec`
applies a splitmix64 finalizer for this reason.

**A tier is never a correctness boundary, and the tests enforce it.** Every
segment component rebuilds from the file, so unloading, evicting, archiving and
faulting in change latency and memory and nothing else. Each residency test runs
a query, disturbs residency, and runs the same query expecting identical rows.
Without that property the `minimal` tier is not implementable at all, because a
node that is not the designated holder has to answer anyway.

**Three query shapes are refused rather than mis-answered.** `AFTER` with
`COLLAPSE BY`, `AFTER` with `ORDER BY <field>`, and a negation as one side of an
explicit `OR`. Each has a defensible semantics that is not implemented; refusing
is cheaper to reason about than a plausible wrong answer.

---

## A worked query

```sql
SELECT id, title, doc.author.name
FROM articles
WHERE tenant_id = $1
  AND status = 'published'
  AND published_at > now() - interval '30 days'
ORDER BY hybrid(
    text_match(body, $2),
    embedding <=> $3,
    method => 'rrf'
  )
LIMIT 10;
```

`EXPLAIN ANALYZE` on the demo corpus:

```
Query plan  (snapshot ts=…, limit=5, k'=100)
  scatter: 1 of 3 shard(s) scanned, 2 pruned by partition key
  term statistics: cached approximate
  shard 0: PRUNED (key prefix `t1/` outside range)
  shard 1 (manifest v1, 0.07 ms):
    segment 1      docs=300  visible=300  survivors=43  s=0.1433  0.06 ms
      filter: partition range `t1/` -> tenant_id = "t1" [column] -> tags @> "starred" [column]
      text[text(body)]: block-max WAND, terms=["fusion", "candidate"], candidates=9
      vector[vector(embedding)]: strategy=brute_force tier=Hnsw s=0.1433 survivors=43
                                 ef=0 amp=0.0x reranked=43 reprobes=0
  shard 2: PRUNED (key prefix `t1/` outside range)
  fusion at coordinator: method=rrf sources=["text(body)", "vector(embedding)"]
                         candidates=[9, 43] union=43
  fetch: 5 payload(s) from winning shards only, 0.01 ms
  total: 0.11 ms
```

One scatter-gather, one targeted fetch. The vector strategy was chosen at
runtime from measured selectivity, and `EXPLAIN` says which one and why.

---

## Running in a container

`Dockerfile` at the repository root builds the image in two stages: a musl
toolchain compiles one statically linked binary, and the image that ships is
`FROM scratch`. It carries `celastro-cli` — the full command-line tool: `serve`,
`exec`, `run`, `repl`, `demo`, `catalog` — a copy of the licence, and nothing
else.

```
docker build -t celastro .
docker run --rm celastro version       # celastro-cli 0.1.0
docker run --rm celastro demo          # the guided tour, in memory, no volume
```

`docker export` takes a container, not an image, so seeing the whole filesystem
means creating one, listing it and throwing it away:

```
$ cid=$(docker create celastro)
$ docker export "$cid" | tar -tv
-rwxr-xr-x 0/0               0 2026-09-10 10:25 .dockerenv
-rw-r--r-- 0/0           34523 2026-09-10 10:06 LICENSE
-rwxr-xr-x 0/0         1960248 2026-09-10 10:21 celastro-cli
drwxr-xr-x 65532/65532       0 2026-09-10 10:25 data/
-rw-r--r-- 65532/65532       0 2026-09-10 10:21 data/.keep
drwxr-xr-x 0/0               0 2026-09-10 10:25 dev/
-rwxr-xr-x 0/0               0 2026-09-10 10:25 dev/console
drwxr-xr-x 0/0               0 2026-09-10 10:25 dev/pts/
drwxr-xr-x 0/0               0 2026-09-10 10:25 dev/shm/
drwxr-xr-x 0/0               0 2026-09-10 10:25 etc/
-rwxr-xr-x 0/0               0 2026-09-10 10:25 etc/hostname
-rwxr-xr-x 0/0               0 2026-09-10 10:25 etc/hosts
lrwxrwxrwx 0/0               0 2026-09-10 10:25 etc/mtab -> /proc/mounts
-rwxr-xr-x 0/0               0 2026-09-10 10:25 etc/resolv.conf
drwxr-xr-x 0/0               0 2026-09-10 10:25 proc/
drwxr-xr-x 0/0               0 2026-09-10 10:25 sys/
$ docker rm "$cid" >/dev/null
```

Sixteen entries, four of them from this file: the licence, the binary, the data
directory and the `.keep` that makes the directory exist. The other twelve —
`.dockerenv` and everything under `dev/`, `etc/`, `proc/` and `sys/` — are the
runtime's, made for every container whatever the image, and every one of them is
zero-length here. The timestamps are the build's and the container's, so yours
will differ.

Both files are root-owned — the binary mode 0755, the licence 0644 — on
purpose: the unprivileged user this image runs as can read and execute the
binary, and nothing in the container can write it. A process able to overwrite
its own executable has a capability with no legitimate use and one obvious
misuse, so the `COPY` that places it carries no `--chown`. UID 65532 owns what
it has to own — the data directory — and no more. No shell, no libc, no package
manager, nothing to patch, and nothing running as root.

Size. The stable figure is the content, because the compiler is pinned and the
binary reproduces byte for byte: 1,960,248 bytes of binary and 34,523 of
licence, about 2.0 MB uncompressed and about 919 kB compressed. What Docker
*prints* is neither of those unconditionally — it depends on the image store,
which `docker info | grep driver-type` names. On Docker 29.1.3 with the
containerd store (`io.containerd.snapshotter.v1`), `docker images` reports DISK
USAGE 2.93 MB and CONTENT SIZE 919 kB — disk usage counts the compressed blobs
*and* the unpacked snapshot — and `docker image inspect --format '{{.Size}}'`
prints the compressed content size, `918584` on the build behind this
paragraph. On the older non-containerd store the same field is the uncompressed
total instead, the 2.0 MB that `docker history` breaks down as 1.97 MB + 41 kB
+ 8.19 kB.

Do not hold `.Size` to the byte. Builds of this source with this Dockerfile have
printed 918581, 918582, 918583, 918584, 918585 and 918586, and two independent
compiles — the second with `--no-cache`, minutes after the first — put the same
1,960,248-byte binary with the same SHA-256 inside images whose `.Size` differed.
Compressing image metadata is not a reproducible operation; compiling this
source is.

`docker run --read-only` works — `demo` and a volume-backed `exec` both complete
under it — because nothing is written outside the data directory.

### First contact: which flags the REPL needs

`CMD` is `repl`, so a bare `docker run` starts an interactive session with no
stdin attached. The first read hits end of file and it leaves at once:

```
$ docker run --rm celastro
celastro — SQL, BM25 and vector search in one query plan.
`\h` for help, `exit` to leave. Statements end with `;` or a blank line.

celastro> $
```

Exit 0, no error, nothing wrong — but not what anyone meant to run.

| invocation | what it does |
|---|---|
| `docker run celastro` | banner, one prompt, exit 0: stdin is already at EOF |
| `docker run -i celastro` | reads piped stdin — how to feed it a script |
| `docker run -t celastro` | a pty with nobody on it: blocks forever |
| `docker run -it celastro` | the interactive session |

`-t` without `-i` is the trap. It does not exit; it waits on a terminal that
will never send anything, and because nothing in the image handles signals
(see below) `docker stop` waits out the full ten-second grace period and then
kills it: exit 137.

With a volume, the entrypoint is bare so global flags precede the verb. `-i` is
what lets the statements below arrive on stdin; `-it` is the same session typed
by hand:

```
$ docker volume create celastro-data
celastro-data
$ docker run --rm -i -v celastro-data:/data celastro --dir /data repl <<'SQL'
CREATE COLLECTION m (id TEXT PRIMARY KEY, note TEXT);
INSERT INTO m VALUES ('{"id":"r1","note":"written from the repl"}');
SQL
celastro — SQL, BM25 and vector search in one query plan.
`\h` for help, `exit` to leave. Statements end with `;` or a blank line.

celastro> collection `m` created with 1 shard(s)
(2.12 ms)
celastro> 1 document(s) written at ts 7327891161097814016
(0.10 ms)
celastro> $
```

The prompt is written before each read, so a piped session prints it ahead of
the answer to the statement it just read, and the last prompt has nothing behind
it — the shell's own prompt lands on the same line.

A collection created in the first container is there in the second:

```
$ docker run --rm -v celastro-data:/data celastro --dir /data exec 'SELECT id, note FROM m'
key | id | note
----+----+----------------------
r1  | r1 | written from the repl
1 row(s)
(0.11 ms)
$ docker run --rm -v celastro-data:/data celastro --dir /data catalog
m  1 document(s)
  primary key   id
  partition by  (none)
  indexes       (none)
```

Without `--dir` the database is in memory, which is what `demo` needs and what
makes `exec` usable with no volume at all.

### The data directory and who owns it

A fresh **named volume** is seeded from the image's `/data`, ownership included,
so `-v celastro-data:/data` needs no preparation: UID 65532 owns it and can
write.

A **bind mount** is not seeded. Docker creates a missing host directory as
`root:root`, and 65532 cannot write there. The failure is two lines, and the
first one arrives on the first statement, because `CREATE COLLECTION` persists
the catalog as it runs rather than at close:

```
$ docker run --rm -v "$PWD/data:/data" celastro --dir /data exec 'CREATE COLLECTION notes (id TEXT PRIMARY KEY)'
error: io error: Permission denied (os error 13)
could not save: io error: Permission denied (os error 13)
$ ls -ldn ./data
drwxr-xr-x 2 0 0 4096 Sep 10 10:25 ./data
```

Exit 1, and the directory Docker made is there, owned by root. A read-only
statement against it fails too, and the shape is worth recognising: the query
itself answers — against an empty database, so it answers with a planner error —
and the save on the way out is the permission failure.

```
$ docker run --rm -v "$PWD/data:/data" celastro --dir /data exec 'SELECT * FROM notes'
error: planner error: no such collection `notes`
could not save: io error: Permission denied (os error 13)
```

Every session saves as it closes, so a container that only read still exits 1
on a data directory it cannot write. Both fixes below start from the state
above — a `./data` that Docker has already created as `root:root` — and both
therefore start with `sudo`, because that directory is root's and changing a
file's owner is privileged on Linux. Either one is enough:

| fix | prepare, once | then run | files land owned by |
|---|---|---|---|
| run as yourself | `sudo chown "$(id -u):$(id -g)" ./data` | `docker run --user "$(id -u):$(id -g)" -v "$PWD/data:/data" …` | you |
| hand the directory to the image's UID | `sudo chown 65532:65532 ./data` | `docker run -v "$PWD/data:/data" …` | 65532 |

The first `sudo` is only the cost of having let Docker create the directory.
Create it yourself and it is already yours, and the `--user` flag alone is
enough with no privilege anywhere — the `sudo rm` below only undoes the
directory Docker made, and a reader who never let it be created starts at the
`mkdir`:

```
$ sudo rm -rf ./data
$ mkdir ./data
$ docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/data:/data" celastro --dir /data exec 'CREATE COLLECTION notes (id TEXT PRIMARY KEY)'
collection `notes` created with 1 shard(s)
(2.35 ms)
$ ls -ln ./data
total 8
-rw-r--r-- 1 1000 1000   32 Sep 10 10:25 CATALOG
drwxr-xr-x 3 1000 1000 4096 Sep 10 10:25 collections
```

`--user` alone against a directory Docker made is not a fix: your UID cannot
write a `root:root` directory any more than 65532 can, and the failure is the
same two lines. The `chown` is the part that does the work; the `--user` only
decides whose name ends up on the files.

Nothing in celastro resolves a UID to a name, and the image has no `passwd`
file to resolve it in, so `--user` may name any pair of numbers.

### The console binds loopback, and `-p` therefore cannot reach it

`celastro-cli serve` puts a browser console on `127.0.0.1` and on nothing else.
`Server::bind` in `src/serve.rs` states why:

> 127.0.0.1 and nothing else: never 0.0.0.0, never `::`, never a name that might
> resolve to a routable address. This endpoint executes arbitrary SQL, so a bind
> reachable from the LAN is a remote code execution surface, not a convenience.

A second guard sits behind the first: `host_is_local` accepts only `localhost`,
`127.0.0.1` and `[::1]`, each with an optional port, and refuses everything
else. That one is aimed at DNS rebinding — a page can point a name it controls
at 127.0.0.1 and the browser will treat the replies as same-origin, which a
loopback bind does nothing about and a `Host` check does. A per-run token from
`/dev/urandom` is the third. Observed against a running console: no token → 401;
a valid token with `Host: evil.example` → 403.

**`docker run -p` cannot reach a loopback bind.** A published port forwards to
the container's *external* interface; the console is listening on the
container's loopback, which is a different interface inside the same namespace.
The symptom is not a refusal, which is the confusing part: publishing the port
is enough for the connection to be accepted on the host, and the failure only
arrives after the handshake, when the forward finds nothing at the other end:

```
$ cid=$(docker run -d -p 8787:8787 -v celastro-data:/data celastro --dir /data serve)
$ curl -sv http://127.0.0.1:8787/
*   Trying 127.0.0.1:8787...
* Connected to 127.0.0.1 (127.0.0.1) port 8787
> GET / HTTP/1.1
> Host: 127.0.0.1:8787
> User-Agent: curl/8.5.0
> Accept: */*
> 
* Recv failure: Connection reset by peer
* Closing connection
```

`-d` prints a 64-character container id; capturing it keeps the transcript
readable and leaves a handle to stop the container with.

The container's log stops at the lines `serve` printed on startup, and a
connection that had arrived and then failed would have left one more — `serve`
prints `celastro-cli: connection dropped: …` for that. Nothing reached the
accept loop. Do not answer this by binding `0.0.0.0`. That bind is the thing the
design removed, and a published SQL console is a published shell.

That container is still holding host port 8787, so it has to go before anything
else can bind it:

```
$ docker rm -f "$cid" >/dev/null
```

**`--network host` is the supported way**, because it makes the host's loopback
and the container's the same interface:

```
$ docker run --rm --network host -v celastro-data:/data celastro --dir /data serve
celastro-cli serving on 127.0.0.1:8787 — Ctrl-C to stop
The token in that URL is the only thing protecting this database. Anyone who can
read this terminal, this process's environment or its command line can use it, and
the server answers every request that carries it. Treat the URL as a password, and
stop the server when you are done.
http://127.0.0.1:8787/?t=fdd2b8856f798668b6f29478e4f1fd5b
```

Six lines, and only the URL is on stdout — pipe the command and you get that
line and nothing else, which is the point of printing it there. The banner and
the four-line warning are diagnostics on stderr. Docker copies the two streams
to a terminal independently, so their order relative to each other is not
fixed: the URL lands last here and first about as often.

That URL, token included, is what to `curl` or open. It is also how to stop the
server: the command above holds the terminal, and Ctrl-C does not end it —
nothing in the image handles signals (see below), so the interrupt reaches a
PID 1 that ignores it and the container keeps serving, banner line
notwithstanding. From a second terminal:

```
$ curl -s -X POST 'http://127.0.0.1:8787/api/shutdown?t=fdd2b8856f798668b6f29478e4f1fd5b'
{"ok":true,"kind":"ack","message":"shutting down"}
```

The container exits 0 with the database closed properly, and the terminal that
was holding it comes back.

What `--network host` costs, plainly: the container gets no network namespace of
its own. It shares the host's interfaces and its port space, so `--port` binds
on the host and collides like a host process; any process on the host —
including any other `--network host` container — can reach the console if it has
the token; and it is a Linux mode — Docker Desktop offers host networking on
macOS and Windows only as a setting you turn on deliberately. That is the trade,
and it is a real one: the alternative is not a safer bind, it is running the
console outside the container against the same directory, or not running it.

### `panic = "abort"` makes the container the unit of recovery

`[profile.release]` sets `panic = "abort"` — "a reachable panic is a bug, not a
recoverable condition". There is no unwind, no `catch_unwind` boundary and no
cleanup path: the process ends where it stands, and from outside the container
that is a process that was running and then was not. What would be a caught
exception elsewhere is a container exit here, so the restart policy is the
error handling.

Nothing in the image handles signals either, and PID 1 with a default
disposition does not receive the ones a supervisor sends:

| stopping it | observed |
|---|---|
| `docker stop` | ten seconds of waiting, then SIGKILL: exit 137 |
| `docker stop`, container started with `--init` | under a second, exit 143 |
| `POST /api/shutdown` (`serve` only) | immediate, exit 0, closed cleanly |

So run `serve` with `--init` — Docker's init takes PID 1 and forwards the
signal to a child that is killable — or expect every stop to take ten seconds.

Being killed abruptly is survivable. `serve` persists after every statement that
changed something: two statements through the console, then
`docker kill --signal=KILL`, then a fresh container over the same volume, and
the row was there. A restart resumes from the last committed statement, not from
the last clean shutdown.

`--restart on-failure` is the policy that fits: a panic exits non-zero, and
`on-failure:N` bounds the retries. Give it a bound, and use it only on `serve`.
The one-shot verbs are jobs, not services — `exec` against an unwritable
directory under `--restart on-failure:3` was dutifully restarted three times
before Docker gave up, and `--restart always` would have retried it forever.

---

## Test map

Every test is named after the failure it prevents. The ones that pin a stated
guarantee:

| criterion | test |
|---|---|
| shredded and unshredded predicates agree | `shard::tests::shredded_and_unshredded_paths_answer_identically` |
| a corrupt segment byte is an error, never a panic | `segment::tests::every_corrupt_body_byte_is_rejected_cleanly` |
| a write can never land inside a pinned snapshot | `time::tests::a_write_can_never_land_inside_an_already_pinned_snapshot` |
| hybrid queries provably correct | `hybrid_retrieval_is_a_union_of_all_three_modes`, `text_match_is_a_must_in_where_and_a_should_in_hybrid` |
| harness trusted | `the_recall_harness_catches_a_deliberate_regression` |
| recall@10 ≥ 0.95 under sustained deletes | `recall_at_10_holds_under_sustained_deletes` (40% deleted, before / after / post-compaction) |
| exact mode bit-identical across shard counts | `exact_mode_is_bit_identical_across_shard_counts` (1, 3, 6 shards) |
| approximate mode within tolerance | `approximate_mode_across_shard_counts_stays_within_recall_tolerance` |
| WAND ≡ brute force | `text::scorer::tests::wand_agrees_with_brute_force` |
| fusing early is wrong | `plan::fusion::tests::fusing_early_gives_a_different_and_wrong_answer` |
| filtered-search strategy selection | `vector::tests::{few_survivors_pick_brute_force_and_are_exact, high_selectivity_picks_post_filter, middling_selectivity_picks_filter_aware}` |
| visibility under deletes and updates | `mvcc::tests::*`, `shard::tests::*` |
| segments survive a reopen | `a_database_survives_reopen` |
| every version above the retain floor survives writes interleaved with collection | `compaction::tests::interleaved_writes_and_collection_keep_every_version_above_the_retain_floor` (6 seeds × 120 interleaved steps against a pinned horizon) |
| an unpinned seal collects nothing and moves no score | `shard::tests::an_unpinned_flush_does_not_move_the_scoring_statistics`, `shard::tests::an_unpinned_flush_keeps_a_snapshot_below_it_readable` |
| a flush that emits several segments installs all of them or none | `shard::tests::a_flush_that_fails_partway_installs_nothing` |
| one replica holds a `minimal` index, whatever the replica count | `exactly_one_replica_holds_a_minimal_index`, `the_holder_count_does_not_grow_with_the_replica_count` |
| every node agrees on the holder without being told | `the_designation_is_agreed_without_coordination_and_is_stable` |
| a non-holder caches it and still answers | `a_node_that_is_not_the_holder_treats_minimal_as_cached` |
| resolution never becomes the declaration | `resolution_never_leaks_back_into_the_catalog` |
| eviction walks the ladder from the bottom | `eviction_walks_the_ladder_from_the_bottom`, `residency::tests::eviction_takes_the_coldest_and_stalest_first` |
| a tier never changes an answer | `unloading_an_idle_index_frees_memory_and_the_next_query_is_unchanged`, `a_node_over_its_budget_releases_the_colder_index_first` |
| opening a shard decodes nothing | `a_reopened_shard_decodes_nothing_until_it_is_queried` |
| eviction prefers the colder index | `a_node_over_its_budget_releases_the_colder_index_first` |
| a storage fault is not a shorter answer | `a_refused_archived_read_fails_the_query_instead_of_shortening_it` |
| retired segments leave the ledger | `compaction_releases_the_ledger_entries_of_the_segments_it_retires` |
| policies demote, use promotes | `an_access_promotes_a_demoted_index_back_to_its_declared_tier`, `an_age_rule_does_not_flap_against_access_promotion` |
| the idle clock survives a restart | `a_query_persists_its_access_clock_without_any_explicit_flush` |

## Layout

```
src/
  value.rs json.rs variant.rs    document model, JSON, binary encoding
  bitmap.rs codec.rs time.rs     ordinal bitmaps, varints, hybrid logical clock
  catalog.rs                     logical path catalog, index definitions
  column.rs                      shredded columns, zone maps, blooms
  text/                          analyzer, dictionary, block-max postings, BM25 WAND
  vector/                        distance kernels, quantization, HNSW, strategy selection
  mvcc.rs                        commit timestamps, delete log, visibility
  memtable.rs segment.rs         the LSM halves
  shard.rs compaction.rs         tablet, manifest, WAL, compaction policy
  residency.rs lifecycle.rs      tiers, placement, memory accounting, lifecycle policies
  sql/                           lexer, parser, AST
  plan/                          coordinator, fusion, EXPLAIN
  engine.rs harness.rs           Db facade, recall harness
  serve.rs serve/                the local browser console, loopback-only
  bin/celastro.rs                REPL, script runner, demo
  bin/celastro-cli.rs            serve, exec, run, repl, demo, catalog
tests/
  integration.rs                 end-to-end behaviour
  tiering.rs                     tiers, residency, lifecycle policies
```

---

## Contributing

Bug reports and patches are welcome. Two conventions the tree follows:

- **A test is named after the failure it prevents**, not after the feature it
  covers — `an_update_before_a_flush_does_not_lose_the_document`, not
  `test_flush`. A test that passes with the behaviour it names removed is worse
  than no test; if you are unsure yours has teeth, delete the fix and check that
  it fails.
- **No dependencies outside `std`.** This is the point of the project, not an
  accident of the environment.

Before a change lands: `cargo test`, `cargo fmt --all -- --check`, and
`cargo clippy --all-targets -- -D warnings`. A change also has to build on the
crate's declared `rust-version`, and must not rewrite `Cargo.lock` — a lockfile
written in a format that floor cannot parse makes the floor a lie.

These are gates, not suggestions, but they are enforced by the maintainer's
build rather than by anything in this repository. Nothing here will run them for
you, so run them yourself.

---

## License

[GNU Affero General Public License v3.0](LICENSE).

You may use, modify, self-host and redistribute this freely. If you run a
modified version as a network service, the AGPL requires you to offer that
version's source to its users — which is the whole reason for choosing it over
Apache-2.0 here. A permissive license is precisely what allows a hosted
derivative to be built on this work and closed; the AGPL does not prevent
competition, it requires that improvements come back.

Contributions are accepted under the same license.
