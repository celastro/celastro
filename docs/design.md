# Design

How celastro is built and why, for someone reading the code. The
[README](../README.md) covers what it is and how to run it.

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
| commit timestamps, delete log, visibility | `mvcc` | `commit_ts ≤ T ∧ ¬(delete_ts ≤ T)`; the delete log is framed (`CLDL` magic, version, count, checksum) |
| block-max WAND | `text::postings`, `text::scorer` | the filter is an admission predicate |
| tiered vector index, two-stage scoring | `vector`, `vector::hnsw`, `vector::quant` | SQ8 and 1-bit codes, full-precision rerank |
| runtime filtered-search selection | `vector::VectorStore::choose` | brute force / post-filter / ACORN-style |
| `COLLAPSE BY` | `plan::exec` | with `k` amplification |
| `count`, `sum`, `min`, `max`, `avg`, `GROUP BY` | `plan::exec` `aggregate_on`, `aggregate_select` | partials per shard as scan hits, merged at the coordinator |
| rank fusion, coordinator only | `plan::fusion` | RRF and weighted linear |
| a bounded graph walk, resolved before the scatter | `plan::walk` | `WITHIN k HOPS OF` as a filter or a `hops(...)` source; `expand` and `present` on `ShardService` |
| the wire between nodes, a shard's move | `wire`, `engine` "moves" | length-prefixed frames of the crate's codec, a shared token, one holder per shard |
| encryption in transit | `tls`, `crypto` | an in-tree TLS 1.3 under the wire and the console; `CELASTRO_TLS_*` |
| encryption at rest | `cipher`, `shard`, `engine` | ChaCha20-Poly1305 frames per file under a data key `KEY` holds wrapped under the master; `CELASTRO_MASTER_KEY_FILE` |
| the console and its guards | `serve` | a token on every request, loopback or `--bind`, a thread per connection |
| a seeded fault schedule on the shard boundary | `sim` | drops, restarts, reorder; "a fault can shorten an answer only by saying so" |
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
DROP INDEX items_emb ON items;   -- withdraw the declaration; sealed regions wait for compaction
DROP COLLECTION items;           -- files, store objects, statistics, clocks: all of it
ATTACH NODE 'tcp://db-b:2352';   -- a node this one may place shards on
LOCAL FLUSH items;               -- this node's shards only, never forwarded
```

Durations take `minutes`, `hours` or `days`, singular or plural, and the
`min` / `hr` / `day` abbreviations. `OF INACTIVITY` (or `SINCE ACCESS`) is the
default trigger; `SINCE CREATION` is the other question you might be asking.

### The ladder

| tier | decoded on | bytes live | first query after idle |
|---|---|---|---|
| `active` | every node holding the tablet | local NVMe | already there |
| `minimal` | exactly one node, whatever the replica count | local NVMe | already there on that node; one segment read elsewhere |
| `cached` | no node between queries | local NVMe | one segment read |
| `archived` | never | archive store | one archive round trip, or refused |

A tier has one name, the one the reports print. The temperature words that
used to be accepted beside them were a second vocabulary for a ladder whose
point is that its rungs are not temperatures.

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

**A pinned seal fans out, and the fan-out is bounded by depth rather than by
bytes.** A segment holds one version per key, so a seal under a pinned
`gc_horizon` emits one segment per retained version layer: `D` retained
versions of one hot key are `D` files, and the memtable's byte threshold was
the only bound on `D`. The memtable now tracks its longest version chain and,
while a horizon is pinned, seals when it reaches `FlushThresholds::max_versions`
(8 by default), so one seal emits at most that many segments. That bounds the
burst and not the total: it trades more frequent seals for smaller ones. The
fix that would reduce the total — a segment holding a run of versions of one
key, with `Ordinals::find` picking the version visible at `t` inside the run —
is an on-disk format change plus a redesign of the MVCC ordinal layout, and it
is deliberately not taken until something needs it. Unpinned, depth is not a
reason to seal: that seal keeps one version per key and emits one segment
however deep the chains ran.

**The wire: a collection spread over nodes, any holder coordinating.** A
cluster is a set of nodes, each with its own directory, its own console and
one advertised address (`CELASTRO_NODE`, `DbOpts::node`). Membership is
declared -- `ATTACH NODE` is DDL and lives in the catalog -- and a collection's
placement is one entry per shard index naming the holder and the key range,
carried by every holder, so any of them can coordinate a statement. `CREATE
COLLECTION` computes the placement (shard `i` on the `i`-th node named, or on
this node and the attached ones in turn) and has every other holder adopt the
same definition and map, building the shards placed on it; a holder that did
not take it is named and the statement is idempotent there. A write routes by
key to the owner's node and is acknowledged after that node did, so the
durability guarantee is kept by the disk the row lands on; a query fans out
per shard through `ShardService`, local shards by direct call and the rest
through `wire::Remote`, and the answer is fused where the statement arrived.
DDL and the operational statements run here and are then re-run on every
other holder with a `LOCAL` prefix: there is no two-phase commit. A node a
definition did not reach -- down, or across a split -- catches up by
reconciliation rather than by an operator: every drop leaves a tombstone in
the catalog with its instant, every collection carries the instant it was
made and every index has its activity clock, and `Db::reconcile` folds a
peer's catalog into this node's by name, last writer wins. A definition the
peer has and this node lacks is adopted unless a younger tombstone here says
it was dropped; a tombstone the peer has drops the definition here if it is
older. `ATTACH NODE` reconciles at once, so a restarted pod catches up as it
attaches its peers, and the console's sweep pulls every known peer's catalog
every `CELASTRO_RECONCILE_SECS` (30) in a thread of its own, so a split
heals within a sweep of the link returning. The statement that could not
reach a node therefore succeeds with a note naming it, for what the
reconciliation carries -- `CREATE COLLECTION`, `CREATE INDEX`, `DROP INDEX`,
`DROP COLLECTION`, a policy's creation, and a placement after a move, which
merges by a different rule since a map is a claim about files rather than a
definition: a node's own word about the shards it holds, or held, is final,
so for a collection both nodes have, a shard the peer's map puts on the peer
moves to the peer in this node's map, one this node's map puts on the peer
that the peer's map puts elsewhere moves there, and one both claim is kept
and named for a `MOVE SHARD`. What it does not carry -- an `ALTER`, a
policy's drop -- is still refused naming the nodes and the `LOCAL` statement
to run there. One refusal is deliberate: a
data node adopts a collection whose map names it as a holder only if the
collection is younger than the node's data directory. Older means the
directory never had those shards' data -- a node restarted from an empty
volume -- and growing empty shards for it would turn lost data into an
answer; `SHOW HEALTH` names the collection as `NOT ADOPTED` until it is
restored or dropped. Clocks matter to the rule only across a split, within
their skew: a drop on one side and a re-creation on the other, closer in
time than the two nodes' clocks differ, resolves as the clocks say.

The merge has to converge: whatever order the sweeps run in, every node must
end with the same definitions, and they must be what the instants say. A
property test (`tests/reconcile.rs`) runs random histories of creations and
drops over three nodes, with reconciliations in the middle, then reconciles
every ordered pair in random orders until a round changes nothing, and
compares every node against a model built from the catalogs' own instants.
Its second seed found the first hole: a drop applied by reconciliation
stamped a tombstone at *now*, which then outranked every re-creation made
between the drop and the sweep. Its seventh found the second, which is why
an index records the incarnation of the collection it was made on
(`IndexDef::on_micros`, format 8): a collection dropped and re-created on
one side is two incarnations under one name, and an index made on the dead
one could reach the live one through a node that had not yet heard of the
drop, after which the drop no longer took it. The rule is now exact: a
collection lives if its incarnation is younger than its tombstone; an index
lives if it is younger than its own tombstone and was made on an incarnation
that lives -- wherever it has been merged since. Four hundred seeds pass;
`CELASTRO_RECONCILE_SEEDS` runs more.

Two things the rule leans on are checked at the door. The clock: every
timestamp and every tombstone is a wall clock through the HLC, so `hello`
carries the node's clock, `ATTACH` refuses a peer more than five seconds
from this node's (`CLOCK_REFUSE_MICROS`) naming both and NTP, and `SHOW
HEALTH` shows every peer's offset and flags one past half a second
(`CLOCK_WARN_MICROS`). The process: `hello` carries the epoch of the process
behind the address, the instant it opened its database, and every node
keeps the largest it has seen per peer. A larger one is a restart, said once;
a smaller one after a larger is an older process still answering at the
address -- a pod replaced while its predecessor runs on a partitioned node
-- and `SHOW HEALTH` says so on every call that sees it, as does the sweep
in the log. That is detection, not fencing: a write from the older process
is still taken, because a request frame carries no epoch to refuse it by,
and adding one is a wire version. `Db::pretend` lets a test, or a drill,
claim an epoch or a clock offset. The sweep dials sixteen peers at a time,
outside the lock: an unreachable peer costs its connect timeout, and a
hundred of them one after another was a sweep of many minutes during which
a split that had healed stayed unreconciled.

A retry is the other thing a client does to a cluster. The contract
(`tests/retry.rs`) is that every statement delivered twice with nothing
written in between leaves what delivering it once leaves: the second
delivery is the same effect or a refusal that changes nothing -- a second
`INSERT` supersedes with the same document, a second `CREATE` is refused,
a second `DELETE` by key finds nothing. The one shape a retry can change is
a `DELETE ... WHERE` delivered again after a write it did not see, which
takes the new rows too, because a predicate is evaluated when it runs; a
client that cannot know nothing wrote in between deletes by key.

Mixed versions were drilled on kind: three pods on 0.45.0, one upgraded to
0.46.0 by a partitioned rollout, DDL from each side, a move each way, the
rollout finished, then a rollback attempted. DDL worked both ways, since the
wire version is what gates a frame and it did not change. The move from the
newer node to the older failed: the move ships the collection's definition
as a catalog, encoded in the newer format, which the older refused as
unreadable. So `hello` now carries the newest catalog format the node reads
and a catalog sent to a peer is encoded in that format (`Node::encode_for_peer`);
a peer from before the field is placed by its version. The rollback
crash-looped every pod on "catalog format version 7 is not readable": a
release that raises the format writes a file the previous one cannot open,
which is inherent, so `CELASTRO_CATALOG_FORMAT` pins the written format for
the first days on a new release and the README's Deployment section says
so.

A backup taken node by node is a set of instants with no cut through the
cluster: an edge in one node's backup can point at a node the other's does
not yet have. `BACKUP CLUSTER TO` chooses one instant on the node it
reaches, takes that node's backup at it, and after its own copy sends
`LOCAL BACKUP TO ... AS OF <instant>` to every other data node in turn,
outside its lock and without a deadline. A node given an instant observes
it on its clock, so every commit from then on is after it and the cut is
exact; an instant further ahead of its clock than `ATTACH` allows for skew
is refused. The set restores with `RESTORE FROM ... AS OF <instant>` on
each node. Per-shard read and write counters on the metrics page
(`celastro_shard_reads_total`, `celastro_shard_writes_total`, by collection
and shard) are what shows a hot shard, which range partitioning with fixed
split keys can make.

Three smaller doors from the same list. A certificate's end takes every
peer at once, so `SHOW HEALTH` names when this node's certificate and its
first trust anchor expire, flags either inside two weeks, and the metrics
carry both instants; a peer's refusal of an expired one names the time. A
node is attached only by the address it calls itself, so one process cannot
be two holders under two names. And the wire caps the connections it serves
(`CELASTRO_WIRE_MAX_CONNECTIONS`, a thread each) and closes one that carried
no frame for `CELASTRO_WIRE_IDLE_SECS`, since a peer that opens a connection
per statement and never closes one was otherwise a thread without end; the
client side reconnects on its own for a connection the holder closed.

The resilience suite (`tests/resilience.rs`, `#[ignore]`, run with
`--ignored`) holds the slow, cluster-shaped properties the drills and the
pitfalls named, so they run when asked and the gates stay fast: the
reconciliation over four nodes, forty statements and four hundred seeds
(two minutes); a node restarted five times under a load of writes and
reads through the others, every acknowledged write there when it is back
and every failure meanwhile naming the node or a deadline -- measured at
3,222 writes and reads with none refused, since a restart takes
milliseconds and the dial retry absorbs it; a write-ahead log of two
hundred thousand rows replaying (0.9 s, 4 µs a row); and a cluster backup
taken under a writer that keeps every edge's endpoints ahead of the edge,
restored node by node into fresh databases and checked to be one cut. Its
first run found a bug the gate tests could not: a stopped node kept serving
a connection that never paused, because the connection thread checked the
stop flag only when a read timed out, and so held its directory against
the restart; the flag is now checked per frame.

The drills' side of the suite runs on kind, from the private tooling that
drives the box, one scenario per script with its outcome asserted: the
split, the mixed versions, a slow link. The mixed scenario's first run as
an assertion found two things a reader of the drill's log had passed over.
The console of the last pod rolled answered nothing for 45 s: a node
attaching its peers at start fetched each peer's hello and catalog under
the write lock, and the peer that vanished between the pre-dial and the
attach -- the next pod of the same rollout -- held every statement for a
deadline; `Db::attach_prepared` takes what was fetched without the lock.
And a pod complained of a peer's clock ten seconds off, on one kernel:
`hello` reported the HLC's physical part, which runs ahead of the wall by
whatever a peer's timestamps pushed it to. It reports the wall clock now,
and `SHOW HEALTH` names an HLC more than a second ahead of the wall.
The ten seconds were the measurement: the sweep compared the peer's clock
with its own after the call that followed the hello, and a call that
waited was read as skew. The wire now stamps the receipt instant on the
hello and every comparison uses it. And the 45 s was not the console at
all: a restarted pod has a new address, the cluster's DNS keeps the old
one for up to 30 s (the window the five-node experiment found), and the
probe dialled a dead address until its own timeout. A rolling restart
after two shard moves, on one version, reproduced both, and a runner that
waits for every name to resolve to its pod's current address measured the
window at 29 s; with that wait the mixed-version scenario passes end to
end, the move each way included. The window is the cluster's to shorten
(a shorter TTL on the headless service's records) and a client's to
absorb (a connect timeout under the DNS TTL and a retry); the pods absorb
it with the dial retry and the attach loop. The `dns` scenario measured
it: 11 s from the pod being ready to its name following it with the
kubernetes plugin's default `ttl 30`, and 0.2 s with `ttl 5`.

The seal was the last thing that built under the lock. A seal of a large
vector memtable built its graph there -- 146 s for 50,000 vectors in the
recovery drill -- and the node answered nothing meanwhile, which to its
peers was a partition. It now has compaction's shape: `Shard::seal_freeze`
under the lock moves the memtable into `frozen` (where `Loc::Frozen` has
let reads and deletes find one since the tiering work), lays its rows out
as the layers a build makes of them, reserves the segment ids and rotates
the write-ahead log aside; `Shard::seal_build` builds the segments holding
nothing; `Shard::seal_install` persists and publishes them, applies the
deletes the frozen memtable took before and during the build, lets it go
and removes its rotated log. The console's maintenance thread runs the
triple ahead of compactions, and turns the freezing on
(`DbOpts::background_seal`) when it starts; without it a due seal builds
inline as before, and `FLUSH` seals everything inline, frozen memtables
first. Two frozen seals the thread has not caught up with are the bound,
past which the write path builds inline: backpressure, as with compaction
debt. A build that fails is requeued with the rows still readable in the
frozen memtable and durable in the rotated log, counted as a seal failure;
a process that ends between freeze and install replays the rotated log
with the live one at the next open. The check writes 20,000 vectors
through the console and answers point lookups while the graph builds: 22 s
of build, 106 lookups meanwhile, the slowest 67 ms. Its first run found the
maintenance step deadlocked on itself -- the lock guard taken to reserve
the seal was a temporary in an `if let`, alive through the build and the
install that takes the lock again -- which is the kind of thing this suite
is for.

Two scenarios need a machine per node, and ran on a cluster of four small
virtual machines driven by a script of their own. The clock jump: one node's kernel
clock set an hour ahead under the cluster, then two behind. The others
flagged it within a sweep; a re-`ATTACH` was refused naming NTP; a write
through it committed an hour ahead and read back through any node at once
(a read's snapshot is the maximum of the holders' clocks); and the other
nodes' HLCs did not follow it -- a node's jump stays its own, which was
not what the plan predicted and is the better outcome. Set behind, the
node's own HLC ran two hours ahead of its wall and `SHOW HEALTH` said so.
The zombie: a second process claiming a node's address on the spare
machine, the name repointed on the others and back, with the wire's idle
close short so a redial follows the name. `SHOW HEALTH` named it, "AN
OLDER PROCESS ANSWERS HERE TOO", and the finding stands as predicted: a
write forwarded to the old process is taken there and the new process
never sees it (a read through it lists the shard as missing, since its
volume is empty). That was the divergence, and 0.50.0 closes the half of it
that needs no wire change: a node asks a fresh connection for a hello
before the first statement goes down it and refuses an epoch older than
the newest it has seen at the address (`Node::check_fresh`), so a write
toward the old process is refused rather than taken; the drill's assertion
flipped and passes. What waits for the next wire version is the other
half, a holder refusing a call from a stale caller, which needs the
caller's epoch in the frame. Hello answers without the database lock when
a statement holds it, from the process's fixed identity: the pull of a
move asks it of a source that holds its lock for the whole move, and the
first run with the check found that out.

Seven more scenarios ran on 2026-09-18, each with its outcome asserted. A
pod deleted under a write load (`loss`): 2,443 writes acknowledged, two
refused, none lost, the far shard named by `partial_results` meanwhile.
Every pod killed at once, no grace, under load (`powerloss`): 1,749
acknowledged, 145 refused, none lost after every node replayed its log. A
link cut five seconds and healed five, three times, under load (`flap`):
6,833 acknowledged, none refused, none lost -- a five-second cut sits
inside the dial retry and the deadline. A move during a split
(`movesplit`): the move across the split aborts naming the target, the
move within a half completes with a note, and the far side's map corrects
itself from the holders' word within a sweep of the heal, so its count and
its writes to the moved shard answer. A pod back on an empty volume
(`lostvolume`): nothing adopted, the shard named as missing. And the token
rotation (`rotation`), whose first run found the deadlock above; with the
grace token the three rollouts answer every count between them.

What crosses the wire is length-prefixed frames of the crate's own codec,
carrying the wire version, the shared token (`CELASTRO_WIRE_TOKEN`, compared
in constant time), the call, and what is left of the statement's deadline,
which the holder arms for its half. A node that does not answer -- slow,
partitioned, or gone -- is `Error::Deadline` at the coordinator, exactly what a
slow shard in this process produces, so `partial_results` names its shard and
nothing else changes. That is the rule the simulator pinned before the wire
existed, and the reason the wire's tests are the simulator's property run
across three real nodes: written through any of them, every node answers what
the same corpus answers in one process, bit for bit. The statistics cache
ages by the sum of every holder's write counter, fetched with the snapshot
instant in one call per remote node per statement, so a write that landed
elsewhere is read and ages the cache here. Two consequences worth knowing:
the coordinator's `ts` is the maximum of the holders' clocks, so a statement
reads at least what every node had committed when it began; and a unit sealed
before an index was declared holds no region for it and answers no rows for
that path until compaction rewrites it, which the plan says -- `CREATE INDEX`
writes nothing into a sealed segment, and a `DROP INDEX` that seals a
memtable makes such a unit on purpose. Compaction does the backfill: a
segment lacking a region for a declared index is a rewrite job of its own
(`Reason::IndexBackfill`), oldest first, one per pass, so `COMPACT` after
`CREATE INDEX` is the rolling rebuild and no size-tier accident is needed.

The catalog format is version 4 for the node list and the placement, and 5
for the edge-collection fields of the section after this one; a 3 is read
with every collection placed wholly on this node, derived from its shard
directories at open, and a 4 as one with no edge collections.

**A shard moves.** `MOVE SHARD i OF c TO 'node'` is three steps from
wherever it is issued: the source pins the shard (`Db::begin_move` -- the
same per-shard export a collection copy takes, held by handle so no file
goes away under it, and from that instant writes to the shard are refused
naming the move), the target pulls the files in chunks and adopts the
directory (`Db::pull_here`, `adopt_shard`: the incoming directory is
complete before it is renamed into place, `MANIFEST` last, so a pull that
stops short leaves nothing a reopen mistakes for a shard), and the map
switches by `LOCAL PLACE SHARD` on every holder -- target first, others,
source last, so the source's copy is dropped only once everyone else can
find the new one. The pin is shared with the wire server outside the
engine's lock: a coordinator that is also the source holds its lock for the
whole statement while the target reads from the pin, and a target pulls
without its own lock, serving its other shards meanwhile. What is not here:
a write to a moving shard waits nowhere -- it is refused, and the client
retries once the map has switched -- and a move is not resumable across a
restart of the source (the pin is memory; the map is unchanged until the
switch, so the statement is re-run). `REBALANCE c` is the moves that put
shard `i` on the `i`-th node in attach order, and `DETACH NODE` of a node
holding shards refuses with that plan.

Not built, by decision: replication, so a node that is down is a shard that
is down -- and only that shard (0.34.0): the counters call every statement
opens with does not fail the statement when a holder is silent; the
statement fails at the first shard call it makes to that node, which a
predicate that pins the key to a live shard never makes. A text query is
the exception by design: its scores come from every holder's term
statistics, so it is refused or, under `partial_results`, served from the
rest and says so. Measured on kind with five pods and one scaled away:
what was every statement failing became the lost shard's statements
failing.

**The deterministic simulator states what a transport has to keep, before
there is one.** `celastro::sim` puts a seeded fault schedule on the
coordinator-to-shard boundary: per call, a shard's answer can be dropped (a
partition, seen by the coordinator as the same `Deadline` a slow shard
produces), or the shard can crash and a replacement open its directory and
answer instead, serving the rest of the statement; and the shards can be
handed to the coordinator in a permuted order. Delay is not a separate fault:
the coordinator is synchronous, so a reply inside the deadline is a reply and
one outside it is a drop, and what arrival order could change is covered by
reorder. The property, checked over twenty seeds and every query shape: **a
fault can shorten an answer only by saying so.** Without `partial_results` a
faulted statement is refused or answers bit for bit what it answers with no
faults; with it, `missing` names exactly the shards whose calls were dropped,
a shard given up on is not asked again by any later stage of the statement,
every row is a real one, and a statistics fill that lost a shard serves its
own statement and is never written to the cache. A restart is a real reopen —
the WAL replayed, the manifest read — and its answers are bit-identical to the
live shard's, which is the durability work made observable. "No row from a
missing shard" is deliberately not the claim: a shard can answer the candidate
call and then stop answering fetches, and the rows it already contributed are
correct rows. What the boundary cost: an unranked scan now returns each
shard's best `offset + k` rows as sort values and keys, with documents fetched
afterwards for the rows that make the page, one call per shard, so a
`LIMIT 5` over three shards still decodes five documents; and the plan lists
shards by index whatever order they answered in.

**`DROP` is ordered so that a crash anywhere in it opens cleanly.** A
collection's directory is renamed aside first — one atomic step, the point of
no return — then the catalog is published without the entry, then the
directory is removed. `Db::open` completes a drop that stopped after the
rename, which it recognises as a catalog naming a collection whose directory
is aside, and removes any directory left aside. The objects an archived tier
put in the store are deleted before the shards are dropped, while they can
still be named; a crash between the rename and that deletion is the one case
that leaves something behind, under the collection's prefix in the store.
Everything recorded against the name goes with the entry — the statistics
cache above all, whose key carries no catalog identity, so that a collection
recreated under the same name is measured afresh rather than answered from its
predecessor's frequencies. `DROP INDEX` withdraws a declaration: the planner
refuses the path, the decoded component is released, the clock and the
statistics go, and the memtables are rebuilt without it. The regions already
sealed stay until compaction rewrites their segments, which mirrors
`CREATE INDEX` writing nothing into them -- with the difference that a
missing region is a compaction trigger and a stale one is not. Both refuse while a lifecycle policy
names what they would drop, so the policy is dropped knowingly.

## Two limits a document can meet

A value may nest at most 128 deep, counted as containers enclosing a value,
and the bound is one constant (`value::MAX_DEPTH`) applied at every door:
`json::parse` on the way in, `variant::decode` on the way back from disk,
and `Value::set_path` for a value assembled in memory. The last one used to
be unbounded, so a value built past the limit encoded and then could not be
decoded — a document written and never readable. It is refused at the call
now, which makes the encoder's infallibility true rather than assumed.

A field whose name contains a dot cannot be reached by any path expression.
`a.b` always means the nested path `a` → `b`; a quoted `"a.b"` is refused
rather than silently resolved against the nested path, and the refusal says
why. Reaching such a field would need quotedness carried through the token
and the AST plus an escaping convention in every stored path string, which is
a format decision deliberately not taken: store the field under a name
without a dot.

## What is deliberately not here

Consensus and replication, follower reads and closed timestamps, hedged
requests, two-phase commit for multi-shard writes and for multi-node DDL,
stateless compaction workers, dynamic shard split and merge, and a graph
database's pattern language, unbounded paths and analytics — the bounded walk in the section after this one is a retrieval
mode, and says what it is not. A collection's shards can be spread over nodes and any
holder can coordinate, but each shard has exactly one holder and a statement
that changes the catalog reaches the holders one by one, reporting the ones it
did not reach. The
`archived` tier is an S3-compatible object store when one is configured, a
directory on any mount when one is named (`CELASTRO_ARCHIVE_DIR`; the same
trait, so NFS is the cluster's business), and the shard-local directory
otherwise; the client is in-tree, plain HTTP or, since 0.42.0, HTTPS over the in-tree TLS (`CELASTRO_ARCHIVE_CA`, or the system bundle): the TLS (0.28.0) encrypts
the wire and the console, not the archive client, yet. Backups (0.30.0)
go through the same trait: `BACKUP TO` and `RESTORE FROM` in `backup.rs`.

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
- **A read holds no exclusive lock.** `Db::read` takes `&self`: what a read
  used to write -- the statistics cache, the recall sample, the connection
  cache -- sits behind its own small lock, the statistics a read merges
  before planning are merged into a copy, and the index accesses it notes
  wait for a writer (`apply_touches`) rather than being applied under the
  read. The console and the wire hold an `RwLock<Db>`: reads share it,
  writes and DDL take it alone. (0.31.0; until then one mutex serialised
  every statement, and a walk at 50 ms capped a node at 12 of them a second
  whatever the core count.)
- **A query reaches a shard through one boundary**, `plan::service::ShardService`:
  statistics, prefix expansion, candidates, an unranked scan, payload fetches,
  the two calls of a walk, and nothing else. A shard on this node answers by direct call, a shard on
  another node through `wire::Remote`, and the simulator puts a seeded fault
  schedule on the same seam.

That combination makes the distributed exit criterion testable now:
`exact_mode_is_bit_identical_across_shard_counts` runs the same corpus at 1, 3
and 6 shards and compares fused scores bit for bit, and
`exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes`
does the same for the global statistics themselves, under a workload that
leaves dead rows behind for each shard to collect on its own schedule.

---

## Graph-constrained hybrid search

The design for an edge, made before any code (a design pass on 2026-09-14)
so that its questions were answered where they constrain each other; the
slices in *Shipped* below were built against it, and where the code settled
a detail the design left open, the decision says so.

**The query.** *Documents within `k` hops of node `x`, matching text `t`,
nearest to vector `v`, under structured predicate `p`* — the shape retrieval
over a citation graph takes. If a `k`-hop neighbourhood can be made an
ordinal set, it intersects with the other three sources for free, and "one
plan, bit-identical across shard counts" extends to it.

**What this is not.** Not a graph database: no pattern language, no unbounded
paths, no shortest path, no centrality (batch analytics, not a query plan),
no index-free adjacency. Immutable segments cannot chase pointers; adjacency
is an index rebuilt at compaction, and a deep walk pays a probe per hop. The
win is the fused query and the bounded behaviour, not the hop.

### Decisions

1. **An edge is a document in its own collection**, with `src` and `dst`
   columns declared and typed, and a `kind` and whatever properties it
   carries as ordinary fields. MVCC, tiers, lifecycle, `DROP`, the WAL,
   export and placement come free; an edge predicate is a structured bitmap
   on that collection. The alternative — adjacency as an array on the node
   document — makes every edge write a node rewrite and was rejected.

2. **An edge collection points into exactly one node collection**, named at
   creation: `CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT
   NULL, dst TEXT NOT NULL) WITH (nodes_of = 'papers')`. Every `dst` and
   `src` is a primary key of that collection. The two collections may be
   placed differently across nodes, because of the next decision.

3. **The frontier is a set of primary keys until the last hop.** A hop is a
   secondary-index probe on `src` (or `dst`) over the edge collection; the
   keys it yields are the next frontier. Only the *final* frontier is turned
   into an ordinal bitmap, per segment of the node collection, exactly as
   `id IN (...)` is today. The invariant "no identifier translation" holds:
   there is no second identifier space and no per-segment map, and the
   hybrid intersection is untouched. The cost is `k` index probes plus one
   key-set-to-bitmap pass per segment. The per-hop lookup through
   `ShardService::get` (a chain of dependent reads) and a global node-id
   space (the second identifier space the invariant forbids) were rejected.

4. **A filter first, a source later.** `WHERE id WITHIN 2 HOPS OF 'p1' VIA
   cites` is one more `Expr` variant beside `text_match` and a distance
   threshold: it selects and contributes no rank. Hop distance as a
   `SourceList` into fusion, so that nearer nodes rank higher, was a
   separable later decision and shipped in 0.26.0 as `hops(...)` inside
   `hybrid(...)`: the same clause, walked the same way, each key scored by
   the hop it was first reached at, lower better; the coordinator hands
   the list to the executor and the shards answer that source empty. A
   pattern surface, if the filter ever grows into one, is SQL/PGQ, which
   is SQL and stays inside the front end's shape.

5. **Bounded, and a cut says so.** `k` is required; there is no unbounded
   walk. `WITH (max_frontier = N)` caps the key set after any hop and
   `WITH (max_fanout = N)` caps one node's expansion, which is what a hub
   costs. Hitting either is reported the way a truncated prefix is: on the
   response (`cut_walks`, beside `missing` and `truncated_prefixes`), in the
   plan, per hop. The statement deadline applies per hop. `EXPLAIN ANALYZE`
   shows the frontier after each hop and which cap bound. Both cuts keep the
   lexicographically first — the first `N` targets of a node by key, the
   first `N` keys a hop found — so a cut answer is the same cut answer at
   every layout, and the frontier cap is applied to what a hop found before
   the liveness check of decision 9, so that the work a cap bounds is
   bounded by the cap: a key it cut is neither checked nor counted.

6. **Shards.** A shard never sees another shard's candidates, so a frontier
   crossing shards is expanded through the coordinator each round: two more
   calls on `plan::service::ShardService`, `expand(frontier, filter, ts) ->
   (from, to) pairs` on every shard of the edge collection and
   `present(keys, ts) -> keys` on every shard of the node collection that may
   hold them, which the simulator covers for free and the wire carries as
   calls 13 and 14. Pairs rather than keys, so that the fan-out cap is exact
   across shards: a shard returns at most `N + 1` targets per source, the
   coordinator keeps the first `N` of the union, and the extra one is how it
   knows the cap bound. The final key set reaches the shards bound into the
   statement as an `IN` list, and a shard on another node — which re-parses
   the statement text — is handed the same list and binds it the same way,
   so every unit evaluates the same predicate. The exit test is the existing
   one extended: a `k`-hop statement is bit-identical at 1, 3 and 6 shards,
   and a shard that stops answering mid-walk shortens the answer only by
   saying so.

7. **Residency.** `USING adjacency (src, dst)` takes a tier like any index
   and a policy may demote it. A walk that finds it below `cached` is
   refused naming the tier and the index, rather than paying a chain of
   archive fault-ins per hop; the operator raises the tier or narrows the
   walk.

8. **Direction and kinds.** Outgoing edges by default — from the column the
   adjacency index names first to the one it names second; `VIA cites
   REVERSE` follows them the other way, which is what *what cites `x`* needs
   and costs a keyword rather than a second edge collection; a collection
   created `WITH (undirected = true)` follows both. `VIA cites WHERE kind =
   'cites'` is a structured bitmap on the edge collection applied at every
   hop: comparisons and their `AND`/`OR`/`NOT`, a compound one in
   parentheses so that the `AND` after the walk belongs to the statement.
   One filter is for the whole walk; `WHERE a THEN WHERE b` (0.26.0) gives
   hop i the i-th, and a count that is neither one nor `k` is refused.

9. **Dangling edges.** An edge whose `dst` is deleted at the statement's
   instant, or never existed, is followed and resolves to nothing: skipped
   in the answer, counted in the plan per hop. MVCC already hides the node;
   the count is how an operator sees rot accumulating.

10. **The hop set** is every node reachable in `1..k` hops, the start node
    excluded: the neighbourhood, which is what a retrieval filter wants.
    `OR id = 'x'` puts the start back.

### What it cost before the walk, measured

Measured on 2026-09-14, before the walk existed, with the graph step done as
client round trips, so the number the first slice had to beat was written
down first. The corpus, used by every section after this one: 50,000
documents of 20 words from a 2,000-word Zipf-like vocabulary, a
128-dimensional embedding each in 64 clusters, and 249,950 citation edges by
preferential attachment, so the graph has hubs (the most-cited document has
7,717 citers). Twenty statements, *within 2 hops of x, matching one common
word, nearest to v, top 10*, ten from hubs; here run as three statements
each (a hop by secondary index, a hop by `WHERE src IN (...)`, and the fused
statement with the frontier as an `IN` list). Every timing is the best of
five over the console's HTTP API; every fused statement returned identical
rows over ten runs, here and in every section below.

Outgoing walks — what `x` cites and what those cite — reach 17 to 27
documents. The three statements cost 96 to 100 ms together, 27 to 31 ms for
the fused one, against 26 to 31 ms for the same text-and-vector statement
without the graph filter: the walk costs two extra round trips of a
statement's fixed overhead and nothing else. Incoming walks from the ten
most-cited documents — what cites `x` and what cites those — reach 18,147 to
43,861 documents, up to 88% of the corpus. There the three statements cost
278 to 862 ms: the second hop carries 2,533 to 7,717 keys in and 18,000 to
44,000 rows out and takes 198 to 709 ms, and the fused statement carries the
whole frontier as literals and takes 34 to 75 ms, against 26 to 30 ms for the
same statement without the graph filter.

Those are the numbers after a fix this measurement found: an `IN` list was
evaluated per document against every literal, and the same hub walks cost
5.0 to 17.8 seconds; it is one scan against a set now.

What follows: matching a 44,000-key neighbourhood inside the statement costs
10 to 45 ms, so decision 3 starts from a pass that is already cheap, and the
cost left is the key set travelling out and back as literals — 200 to 700 ms
on loopback. A two-hop neighbourhood of a hub is most of the corpus, so
`max_frontier` will bind on real graphs at small `k`, which is why a cut that
says so is part of the design.

### What it costs with the walk in the plan, measured

Measured on 2026-09-14 on the first slice, same box and corpus, the twenty
statements each one statement now: `WHERE id WITHIN 2 HOPS OF x VIA cites
[REVERSE] AND text_match(body, t) ORDER BY embedding <=> v LIMIT 10`. Every
statement returned exactly the rows the three-statement version had.

Outgoing walks, 17 to 27 documents: 53 to 56 ms fused, against 96 to 100
ms as three statements and 26 to 32 ms for the statement without the walk.
The walk itself is 25 to 28 ms of that, `EXPLAIN ANALYZE` says, and almost
none of it is the frontier: a hop is a scan of the probed column of every
unit of the edge collection against the frontier as a set, and a liveness
check is the same scan of the node collection's key column, so two hops
over 250,000 edges cost the same 25 ms whether the frontier is five keys or
five thousand. Incoming walks from the ten most-cited documents, 18,147 to
43,861 documents: 165 to 463 ms fused, against 278 to 862 ms as three
statements. The largest, 43,861 keys: hop 1 expands one key over 7,180
edges in 11 ms and checks 7,180 keys in 25 ms; hop 2 expands 7,180 keys
over 77,767 edges in 128 ms and checks 36,681 keys in 155 ms; the fused
statement over the 43,861-key set then costs 30 ms, the same as it did with
the keys as literals.

So nothing crosses the client and the answer is the same; what was left was
the slice's own: a hub walk six to seventeen times the statement without it,
because expand was a column scan per unit rather than a probe and the check
built its key set once per unit.

### What it costs with the region, measured

Measured on 2026-09-14 on 0.21.0, same box and corpus, after `COMPACT
cites` had rewritten both segments with the adjacency region (2.4 s for
249,950 edges). Every statement returned the same rows as before.

Outgoing walks: 26 to 31 ms fused, which is the statement without the walk
(26 to 33 ms); the walk itself is 0.9 to 2.0 ms — two probes and two
lookups, nothing scanned. Incoming walks from the hubs: 127 to 392 ms
fused, against 165 to 463 with the scan and 278 to 862 as three
statements. The largest, 43,861 keys: hop 1 expands one key in 4 ms and
checks 7,180 keys in 21 ms; hop 2 expands 7,180 keys over 77,767 edges in
52 ms and checks 36,681 keys in 148 ms; the coordinator's own set handling
— sorting 77,767 pairs, the seen and answer sets — is the 94 ms between
the hops' sum and the walk's 320 ms; the fused statement over the key set
is 32 ms, as before.

So an outgoing walk costs what the statement without it costs, and a hub
walk is bounded by the edges it follows and the keys it checks. What was
left was proportional to the neighbourhood: a liveness check of one lookup
per key (about 4 µs over pointer-chased strings) and a coordinator keeping
its frontiers as ordered sets of owned strings. The next section is the
merge that replaced both.

### What it costs with the check as a merge, measured

Measured on 2026-09-15, 0.23.1 against 0.23.0 on one box (not the earlier
sections' box, so 0.23.0's numbers here are not theirs), the corpus
regenerated from the same seed with the adjacency index declared before the
load. Every statement returned the same rows and frontiers on both versions.

The liveness check of an unpartitioned node collection is one galloping pass
of the sorted frontier over each segment's sorted keys, against the
visibility bitmap the scatter reads anyway; and the coordinator's frontier,
seen, present and answer sets are sorted vectors, each step one merge.

Outgoing walks, 17 to 27 documents: unchanged, 28 to 31 ms fused with the
walk at 0.7 to 1.2 ms of it. Incoming walks from the ten most-cited
documents, 18,147 to 43,861 documents: the walk in the plan 75 to 224 ms
on 0.23.0, 22 to 108 ms on 0.23.1; fused, 97 to 247 ms against 64 to 160,
with the statement without the walk at 26 to 32 ms on both. Per hop, the
check at hop 1 (7,180 keys at most) 5.6 to 15.4 ms down to 1.4 to 2.8, and
at hop 2 (up to 36,681 keys) 41.7 to 119.2 ms down to 6.1 to 33.1 — a
factor of four to five, from about 3 µs a key to about 0.7; expand
unchanged. The coordinator's own time — the walk less its hops' expand
and check — 15 to 59 ms down to 4 to 44. The largest, 43,861 keys: hop 1
expands one key in 2.4 ms and checks 7,180 keys in 2.7 ms; hop 2 expands
7,180 keys over 77,767 edges in 34 ms and checks 36,681 keys in 27 ms; the
walk is 103 ms against 217, and the fused statement over the key set 32
ms, as before.

What the coordinator keeps is mostly the sort, an instrumented run of that
walk says: the 77,767 `to`s hop 2 returns, sorted and deduplicated to 36,681
keys, 32 ms; the merges after the check, cloning those keys twice, 18 ms;
the difference against the seen set, 4 ms. A hash pass before the sort and
a frontier borrowed from the answer would take both down; neither is taken
until a walk of this shape is somebody's.

### Shipped

The fourth slice, in 0.26.0: `hops(...)` as a fusion source and `THEN
WHERE` per-hop edge filters, both bit-identical across shard counts and
over the wire, which now carries the hop with an expansion. The tests:
`engine::tests::a_hop_source_ranks_nearer_nodes_higher_and_per_hop_filters_apply_in_order`,
the two statements added to `a_hop_statement_is_bit_identical_across_shard_counts`
and to the three-node walk test, and
`sql::parser::tests::a_walk_parses_as_a_filter_with_a_one_term_edge_filter`
extended.

The third slice, in 0.23.1: the liveness check of an unpartitioned node
collection as one galloping merge per segment (`Shard::present_sorted`),
and the coordinator's sets as sorted vectors merged in one pass each. No
answer, plan line or surface changed. The tests:
`shard::tests::a_merged_liveness_check_agrees_with_a_lookup_per_key_under_updates_and_deletes`,
`plan::walk::tests::the_sorted_set_merges_agree_with_ordered_sets`, and
every test of the first two slices unchanged.

The second slice, in 0.21.0: the adjacency region — one sorted
value-to-ordinals map per column the index names, in every segment sealed
after it, its own component under the index's tier — probed per frontier
key by `expand`; the liveness check as a key lookup on an unpartitioned node
collection; the units a hop had to scan counted in the plan; and compaction
backfilling any index a segment was sealed before, which `COMPACT` after
`CREATE INDEX` now does for every index kind. The tests:
`segment::tests::an_adjacency_region_probes_both_columns_and_round_trips`,
`engine::tests::a_hop_probes_the_region_and_scans_only_units_sealed_before_it`,
and every test of the first slice unchanged.

The first slice, in 0.20.0: an edge collection with `WITH (nodes_of = ...)`
— or `ALTER COLLECTION ... SET (nodes_of = ...)` for one loaded before —
and `CREATE INDEX ... USING adjacency (src, dst)`; `WITHIN k HOPS OF` fused
with `text_match` and `<=>` in one plan; bit-identical across shard counts
and across nodes; a cut walk says so on the response and in the plan;
`EXPLAIN ANALYZE` shows the frontier per hop; the measurement re-run against
the fused plan; the README says what this is and is not. The tests:
`engine::tests::a_hop_filter_selects_the_neighbourhood_and_nothing_else`,
`a_hop_statement_is_bit_identical_across_shard_counts`,
`a_cut_walk_says_which_cap_bound_it`,
`a_walk_over_a_cold_adjacency_index_is_refused_naming_the_tier`,
`a_dangling_edge_is_skipped_and_counted`,
`sim::tests::a_faulted_walk_refuses_or_agrees_and_a_partial_one_says_so`
and, over a real transport,
`a_walk_over_collections_spread_over_three_nodes_answers_what_one_process_answers`.
Not in it, and open: a pattern surface. Hop distance as a source and
per-hop edge filters followed in 0.26.0.

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

**A score tie at the `k'` boundary inside a memtable is broken by key, at a
price only the memtable pays.** A unit hands the coordinator its best `k'`
candidates in the coordinator's own order, score then key. A sealed segment's
ordinals are its keys, so the ordinal stands in for free and WAND's bar is the
k-th score itself: a document that can only tie has the larger key and
correctly loses without being scored. A memtable's ordinals are push order, so
it hands the collector its keys, and the bar is one ULP below the k-th score
so that the ties the comparator has to see are scored rather than pruned.
That un-prunes every tie in the unit -- the cost the earlier attempts refused
to pay collection-wide -- and pays it only inside a memtable, whose size the
flush thresholds bound.

**`k'` truncation, not approximation, is what breaks shard-count identity.**
`n` shards each return up to `k'` candidates where one shard returns `k'`, so
the candidate union differs with the shard count and the fusion differs with it
— even in exact mode, where ANN and statistics are removed as sources of
variance. Bit-identical results across shard counts hold only at `k'` above the
candidate count, which is what `exact_mode_is_bit_identical_across_shard_counts`
pins. The statistics half of that claim holds on both paths, but for different
reasons. The two-phase gather masks all three of `num_docs`, the length sum and
`doc_freq` by snapshot visibility on this query, so the triple is a function of
the live corpus alone. The cached statistics the default path reads are the
same masked sums, gathered at an instant the query pins and refreshed on a
write counter no shard's seal or compaction schedule touches — so its residual
is staleness, and staleness is not a shard-count dependence: one shard and six
cross the same refresh points after the same writes and measure the same corpus
there. Every triple the default path returns is a set of live sums at ONE
instant, at most that counter's interval behind the query: stale, never mixed. They used to be
counted over physical rows instead, and physical rows move with flush and
compaction timing, so they moved with the shard count. Prefix expansion is
inside the gather too: the coordinator resolves each `foo*` against every unit
of every shard, takes the lexicographically first `PREFIX_EXPANSION_LIMIT` terms
of that union, and gathers a global `df` for each of them, so an expanded term is
weighted by the collection rather than by whichever segment happened to score it.
The enumeration is masked by the same snapshot the gather uses, so the terms a
prefix names are the first `PREFIX_EXPANSION_LIMIT` of the collection's LIVE
matching vocabulary. That last word is load-bearing and it cost a measurement
to learn: a term whose every posting is dead contributes no rows, but under a
cap it DISPLACES a term that would have. Enumerating the physical dictionary
instead, a collection of 2000 documents with 500 of them rewritten answered the
same `a*` with 12 rows at one shard and 512 at four, and a smaller one answered
0 rows before a `COMPACT` and 400 after — which is the flush and compaction
schedule back in the answer through the one door the cap leaves open. The mask
has to be applied *inside* the walk, so that the cap counts live terms and a run
of dead ones is stepped over rather than paid for; filtering a fixed-size
physical window afterwards was measured to recover 213 of 300 matching documents
on a smaller fixture, and to still move with the compaction schedule. A wide
prefix is still a partial answer — the cap binds — but the partiality is now a
property of the data instead of the flush and compaction schedule. A statement
that constrains the partition key is expanded over that partition: the budget is
per statement, so spending it on another tenant's vocabulary expanded a
tenant-scoped query out of its own partition and answered zero rows.

The mask is not a tax. Stepping over a dead term costs about a microsecond,
measured as the slope between a 10000-term and a 50000-term dead run in front of
the same 20000 live terms (15.6 ms and 53.1 ms, best of five), against 237–468 µs
to gather one term's frequency — so skipping a dead term during enumeration is
some 250x cheaper than over-enumerating one at the coordinator. A unit with no
garbage takes a `popcount` and then the identical physical walk, and a unit that
is *entirely* garbage answers in constant time. Where the corpus is
update-heavy the mask *deletes* work rather than adding it: 200 dead terms of
500 dead postings each went from 745 ms returning 312 rows to 19 ms returning
all 512, because the dead terms never reach the frequency gather at all.

**A wide `foo*` is a partial answer, and pinning it made the answer smaller.**
A prefix expands to at most the collection's `prefix_expansion` dictionary
terms, 512 unless set.
That cap used to be applied by each searchable unit to its own dictionary, so
the terms a query named were the union of every unit's own lexicographic cut —
more units meant a larger union. Measured on 6000 documents each holding two
terms drawn from a 4000-term vocabulary, so that all 6000 match — the fixture
`tests/integration.rs::a_prefix_query_finds_the_same_documents_at_every_shard_count`
builds, so these numbers are re-runnable rather than quoted from a harness that
is not in the crate: `SELECT id FROM items WHERE text_match(body, 'a*')`
returned **2473 rows at one shard, 2523 at three and 2611 at six**, silently.
The coordinator now resolves the prefix once and caps the union, and the same
query returns **1511 rows at all three** (and its negation the exact
complement, 4489). That is deterministic, and it is *lower recall than the
largest of the answers it replaces*. The trade is deliberate: an answer that depends on
when the last compaction ran cannot be reasoned about at all, while a smaller
answer that is the same every time — and that says it is smaller — can.

The shape to plan against is `rows ≈ n · (1 − (1 − cap/V)^t)`, for `n`
documents over a matching vocabulary of `V` terms with `t` of them per
document. The cap only bites when `V` is large relative to it: on a design-pass fixture
of the same shape at 700 distinct terms, the corpus goes from 5637/5782/5783
rows to 5582 everywhere, a 1% change. At 4000 it is the difference above. (That
sweep and the cap dial below were measured on a separate harness with different
unit counts, so their absolute row counts do not line up with the in-tree
test's; what transfers is the shape.) The cut is lexicographic, so `a*`
keeps the terms nearest the start of the alphabet — arbitrary, but stable, and
stable is the property that was missing. (Choosing the cap's members by global
document frequency would cover more documents and is equally shard-count
independent, but it needs a `df` for the whole union first, whose cost is
unbounded in the vocabulary. Rejected, not deferred.)

Raising the cap is a real dial with a real price, not a free recall win. At a
pinned cap of 2048 on a 20000-document fixture, coverage of that 4000-term
vocabulary rises from 1499 to 4705 of 6000 (78%) while `a*` goes from 171 ms to
around 1200 ms ranked and 1000 ms filtered — roughly 7x, because the work is one
cursor per resolved term in every unit and that grows with the cap directly
while recall improves sub-linearly.

The dial is per collection: `CREATE COLLECTION ... WITH (prefix_expansion = N)`
or `ALTER COLLECTION items SET (prefix_expansion = 2048)`, shown by
`SHOW CATALOG`, persisted in the catalog and carried by an export, so a copy
answers a prefix the way its source does. The ceiling is 4096, the size of the
per-path statistics cache, because one expansion wider than the cache would
evict its own terms and never warm. The leaf budget below is *derived* from the
cap rather than set beside it — `4096 / prefix_expansion`, so eight distinct
prefixes per statement at 512, four at 1024, two at 2048, one at the ceiling —
which is the one way to keep the two from contradicting each other: an operator
sets one number, and no statement can be admitted that the cache cannot hold.

What that shape costs, stated so nobody rediscovers it. Every query on the
collection pays the raised cap, and two full-text paths on one collection share
it: there is no per-statement or per-path dial. A wide cap and many prefixes
in one statement are not both available, because the cache behind both is
fixed. The answer to `a*` becomes a function of a setting as well as of the
data, so two instances agree only at the same setting — which is why the export
carries it. The statement deadline still binds, so on a large collection a
raised cap can turn a cut answer into a deadline refusal unless the deadline is
raised with it. And the setting moved the catalog format to version 3: this
build reads version 2, while a 0.14 build refuses a catalog or export written
here, so a downgrade after the first open means restoring a copy.

**Every query that was cut says so, whether or not it asked.** Truncation is
reported on `QueryResult::truncated_prefixes` — as `truncated_prefixes` in the
HTTP and `--json` responses, as a `TRUNCATED —` line in the shells between the
rows and the row count, and as the same line in the browser console beside its
partial-result warning — for
ranked queries and `WHERE text_match(...)` predicates alike. It used to be reachable only through
`EXPLAIN ANALYZE`, and only on the ranking path: the filter path, which is the
commonest shape a wide prefix takes, discarded the flag entirely. What is
reported is the number of terms *kept*, never the number dropped: counting those
means enumerating the whole matching vocabulary, which is the cost the cap
exists to refuse.

The message carries the leaf's polarity, because cutting the two costs opposite
things. A cut `a*` loses rows. A cut `-a*` is a short *exclusion* set, so it
fails to remove rows and the answer has extra ones — and the leaf is printed as
it was written, sign and all, so a statement holding both can be told apart. A
statement spelling one prefix in both polarities gets one line naming both, and
both consequences, because it is one expansion doing both kinds of damage.

**A `DELETE` whose predicate was cut is refused, and nothing is deleted.** A
cut `SELECT` is recoverable — widen the prefix, run it again, the rows are
still there. A cut `DELETE` is not, and in the negated shape it is not even
short: measured on 1000 documents each holding `zed a#####`, every one of which
`zed -a*` excludes, so the correct answer is zero deletions, a cut exclusion
set covering 512 of the terms made the other 488 deletable and they went. The
direction of the damage cannot be read off the leaf either — SQL's `NOT` wraps
the whole `text_match` call and inverts it, and one statement may spell both
polarities — so the refusal covers both shapes and names the leaf that was cut.
Delete by key, or narrow the prefix until it expands to at most the
collection's cap and delete the pieces; that loop deletes exactly what each
piece names. The refusal names the cap in force. There is deliberately no
opt-in to run the cut statement anyway: a flag would exist only to make an
irreversible mistake convenient, and the three ways out — by key, by
narrowing, or by raising `prefix_expansion` when the vocabulary fits under
the ceiling — each delete exactly what they name.

One statement may name at most `4096 / prefix_expansion` distinct
`(path, prefix)` pairs — eight at the default cap — *summed* over its indexed
paths. Each distinct one costs a dictionary walk in every unit of every shard
plus up to the cap's worth of gathered frequencies, and nothing in the
`text_match` grammar bounds how many a query string holds: 24 of them measured
at 1.2 s and gathered 12288 terms into a 4096-entry statistics cache, which then
evicted its own entries so the identical statement never warmed. Repeats of one
prefix on one path are expanded and gathered once, which is what the limit
counts — though each occurrence is still evaluated separately in every unit, so
spelling one prefix 256 times is a slow statement that this bound admits. The
same prefix on two paths is two enumerations and counts twice, because a term
list is only valid for the dictionary it came from. Over the bound the statement
is refused rather than quietly trimmed — a silent aggregate cap would be the
same failure one level up.

**The search over codes is bound the same way.** A profile of hybrid
queries showed 44% of the server's samples in the per-candidate SQ8
distance, so the query's parts of it were folded out once
(`Codes::prepare`, `PreparedQuery::distance`: for the dot product `Σ
q·lo + Σ (q·s)·c`, for L2 `Σ a² - 2 Σ (a·s)·c + Σ s²·c²` with `a = q - lo`,
one AVX2 pass over the code's bytes for the sums). It ranks identically
to float rounding and costs 4% less CPU per hybrid query, not 40%: the
samples were stalls on the random candidates' bytes, and a faster kernel
does not fetch them sooner. The console's own fixed cost per request was
not JSON either but a thread per connection -- a clone and a stack to
zero for every request -- and a pool of `max_connections` workers took a
point lookup from 0.7 to 0.65 ms of CPU and the node from 870 to 990
lookups a second; a rendezvous channel between the accept loop and the
workers was tried first and lost the hand-off race to the saturation
pause on nearly every connection, so the channel is buffered to the cap.

**The graph build is not bound by reading the vectors; building over
the SQ8 codes was tried and does not pay.** After 0.35.0 the build's
profile was distance evaluations on random vectors, and a software
prefetch and AVX2 had gained little and 18%, so the reading looked like
cache misses and the lever like fewer bytes per evaluation. Measured on
the survey's corpus (50,000 vectors of 128 dimensions, `COMPACT` over
the flat segments): links measured between SQ8 codes decoded on the way,
scalar, 379 s against 92 s over the full vectors; the same with the
per-vector terms folded out and the cross term as one AVX2 weighted byte
dot (eight codes widened per step, a quarter of the bytes read), 106 s;
recall@10 0.885 against 0.865 both times, which is two hits in a
thousand. The bytes were not the bound: a quarter of them at two and a
half times the instructions came out slower, so the evaluation is
arithmetic and the graph's bookkeeping, and the lever left is fewer
evaluations -- `CELASTRO_HNSW_EF_CONSTRUCTION`, already a knob, with
`MEASURE RECALL` to say what it costs -- or a different candidate
structure, which is a design and not a kernel. The code was not kept:
a knob that trades 14% of the build for noise is a trap.

**An aggregate is a scan whose hits are partials, so it crosses the wire
as a scan.** `count(*)`, `count(path)`, `sum`, `min`, `max`, `avg` and
`GROUP BY` fold on the shard: `aggregate_on` walks the same filtered
bitmap the scan walks and keeps one accumulator per group, reading each
row's document once for the group value and the aggregated paths -- and
not at all for an ungrouped `count(*)`, which is the survivors' popcount.
The shard answers a `ShardScan` whose hits are one per group: the group's
value as the sort key, its JSON as the row key, the partials under one
field of the document. That shape is chosen so a node holding a shard
parses the statement it was sent and folds, and the answer travels in the
frame a scan already has; a node of a version before aggregates answers
rows instead, which the coordinator detects and refuses by name rather
than summing documents. `aggregate_select` merges the partials of one
group from every shard (counts add, sums add -- integers stay integers
until they overflow or meet a float -- `min`/`max` compare within one
kind and refuse a mixed group, `avg` carries its sum and count until the
end), orders the groups by the result's fields (an alias or the call as
written), and pages them; without `LIMIT` every group is returned,
because the groups are the answer. A ranked `ORDER BY` is refused: it
chooses k rows, which is a different question. `partial_results` keeps
its meaning -- a shard that ran out of time is reported missing and its
rows are absent from the fold.

**`search_after` over an approximate index is not cheaper than `OFFSET`.** A
graph search has no resume primitive: an HNSW heap cannot restart from a
distance without re-traversing, so an ANN cursor costs `depth + k` per shard,
exactly what `OFFSET` costs. Its advantage is stability under concurrent writes,
not cost. A text source genuinely can resume from a score threshold; the two
cases are worth keeping separate in one's head.

**The cost model picks brute force far more often than a static planner would.**
With `m0 = 32`, a traversal costs about `11 × dims` per node visited, so an
exact scan wins until survivors exceed about `11 × visits`. A post-filtered
traversal visits about `ef` nodes — around 1,400 survivors at `ef = 128`. A
filter-aware traversal visits about `ef / s`, because its heap fills only with
admitted nodes, so it earns its place only when a small *fraction* is still a
large *count*: the scan wins until `n > 11 × ef / s²`, which at ten percent and
`ef = 128` is a segment of 140,000 documents. The model used to price both arms
at `ef` visits, which chose a full traversal over a one-percent scan; it now
prices the arm that would run. A caller can bound the traversal with
`WITH (max_visits = N)`, knowingly — a budget that binds returns fewer or worse
documents — and the plan reports `visits` beside `budget` so it shows whether
it bound. The regimes are separated by absolute counts that depend on `m0`,
dimensionality and the filter, which is the argument for choosing at runtime
from measured selectivity rather than estimating.

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

**The SELECT list narrows the document; `key`, `score` and `distance` are the
row's own.** A row is the primary key, the fused score or the distance where
the query ranked, and the document cut down to the named paths — `Null` where
a document has no such path, so that a projection over polymorphic documents
shows its gaps as empty cells rather than ragged rows. Naming `score` or
`distance` is accepted and changes nothing: a ranked query carries them
whether or not they are asked for. Projection is the last step, after
`COLLAPSE BY`, the cursor and the fetch, because each of those reads fields
the list may not name.

**A distance threshold in `WHERE` is a filter, exact, in the units the
`distance` column shows.** `embedding <=> $q < 0.2` selects the rows whose
distance to `$q` is below 0.2 and ranks nothing; it composes with the other
predicates as a bitmap, and it is applied after the cheaper ones so that the
full-precision distance is computed only for what they left. Exact on
purpose: an approximate traversal can miss a vector inside the threshold, and
a predicate that is sometimes false for a row that satisfies it is not a
predicate. So the cost is `survivors × dims`, which is what a filter over the
candidates costs, and `EXPLAIN ANALYZE` reports it as brute force. The
comparison is against the presented distance -- for cosine after the query is
normalised, as the ranked path prepares it, so a stored vector and any positive
scaling of it are within rounding of 0 -- within, not at: the normalised
components are f32-rounded, so `1 - dot` of a vector with itself lands a few
ULPs either side of zero, and exact match for cosine is `< 0.000001` rather
than `<= 0`. L2 is exactly 0 for identical vectors, so `<= 0` is exact there.
For `<#>` the presented value is the negated inner product, so a threshold on
it reads the other way.
Under `NOT` a document with no vector has no distance and is on neither side.

**A copy of a collection is the source at one instant, taken without
stopping it.** `Db::export_collection` pins a snapshot: the sealed segments
the manifest names, held by their `Arc`s so no compaction can unlink one
before it is copied; each delete log as it stands at the pin;
and the memtable's visible rows sealed into one fresh segment of the copy's
own, built from the snapshot and touching nothing in the source. Between the
pin and `write_to` the source keeps taking writes and none reach the copy.
`write_to` publishes every file under a temporary directory and renames it
into place at the end, so the destination is absent or complete; `import`
adopts one into another instance the same way. No `gc_horizon` is pinned:
the entry that planned this expected to need one, but a handle's `Arc` is
what keeps a file, and the files are what is copied.

**A deployment's probes ask the database, and ready means attached.** The
chart's probes run `celastro health` inside the pod, which asks the
console for `/api/health` — served without the token, by decision, and
answered by reading the catalog, so it measures the database and not the
process. With more than one pod the readiness probe adds `--attached
replicas-1`: a pod is routed to only once it has verified every peer since it
started, because a statement it coordinates reaches the shards it does not
hold through them. The peers are dialled outside the database lock, which a
verification found the hard way: dialled under it, a peer not yet up held
every probe behind a five-second connect timeout and the pod failed its own
liveness check.

**The `archived` tier is an object store, reached the way the design budgets
for.** One S3-compatible surface: a bucket, a key that reads like the path it
stands in for, `PUT`, ranged `GET`, `HEAD` and `DELETE`, path-style and signed
with Signature Version 4, over plain HTTP or the in-tree TLS (0.42.0; verified against `CELASTRO_ARCHIVE_CA` or the system bundle, wildcard names allowed in the leftmost label, every resolved address tried in turn): the TLS covers the wire and the
console, not this client, yet.
SHA-256, HMAC, the signer and a small HTTP/1.1 client are in-tree and pinned
against the published vectors, AWS's own worked example included. A remote
segment is opened by reading its footer with two ranged reads and each
component faults in with one more -- the chain of dependent round trips the
design already charges an archived read for, reported by `EXPLAIN` like any
fault-in; nothing is cached locally, so the object is the segment's only copy
while it is archived. Moving to the store `PUT`s the local file and unlinks it
only once the store has acknowledged; moving back fetches the object, publishes
it into `segments/` like any other file, and only then deletes it, so a failure
between the two steps of either move leaves both copies and the open prefers
the local one. A retired segment's object is deleted by the sweep that
retires it, because nothing lists the store: an object a failed publication
left behind is not reclaimed, which is the one thing the local directory does
that the store does not. Credentials come from the environment at open and
are never written anywhere.

**The TLS is in the tree, and this is what it is.** `src/crypto` holds
SHA-512, HKDF, ChaCha20-Poly1305, the 25519 field, X25519, Ed25519, DER,
PEM and X.509, and `crypto::tls13` the record layer and both sides of the
handshake: TLS 1.3 only, one suite, one group, Ed25519 for the node's
own signature, server authentication only, session resumption by PSK
ticket with (EC)DHE (0.40.0: the ticket is `psk | issued | age_add`
sealed under HKDF of the node's private key, so every node behind one
certificate opens every other's tickets and a restart changes nothing;
the client keeps one ticket per name, address and anchor set for a day;
the binder is pinned to RFC 8448's resumed trace, the truncation of the
ClientHello included; a ticket that does not open is a full handshake, a
binder that does not verify is a refusal; the server never accepts a PSK
without a key share), but no 0-RTT, client certificates,
HelloRetryRequest or key update. A stock client speaks that subset; an Ed25519 leaf is the one thing it asks of an issuer. Verifying
is wider than signing: chains and CertificateVerify from RSA (PKCS#1 v1.5
and PSS, SHA-256) and ECDSA P-256 are accepted (0.29.0; `bignum`, `rsa`,
`p256`, public-key operations only, so no timing concern), which is what
lets `celastro tls secret` reach a cluster's API, whose certificate
no cluster issues as Ed25519 and which asks for a client certificate (the
client answers with an empty one, as the RFC has it). Every primitive
is pinned against its RFC or FIPS vectors, the key schedule against RFC
8448's trace, the whole against itself over loopback and against a stock
client on kind. What "constant-time" means here: nothing branches on or
indexes by a secret; the field and scalar arithmetic run the same
instructions whatever the values, a conditional on a secret bit is a mask,
and secrets are compared by folding every byte. The compiler is not asked
to keep that -- there is no `black_box` in the floor's `std` -- so the code
keeps it by having no branch to remove. It was written for a crate that
takes no dependency, by the user's decision after rustls had shipped
behind a feature and been withdrawn; it is unaudited, and the README says
so. The archive client speaks it since 0.42.0.

**Every parser is fuzzed, because a panic is an abort.** `panic = "abort"`
makes a reachable panic in a parser a crash a peer or a corrupt file can
cause, so `src/fuzz.rs` (tests only) is a seeded mutator -- bits flipped,
bytes set to edge values, ranges cut or doubled, inserts, truncation,
splices of two samples, and a length-shaped window set to a huge or tiny
integer -- and every parser that reads the network or a file has a test
that feeds it thousands of mutants of valid input and asks only that it
return. The first run found the class the length mutation exists for: a
decoder that reserves a `Vec` for a count it has just read aborts the
process on the allocation when the count is 2^50, before any bounds check
runs. `codec::get_count` refuses a count larger than the bytes that
remain, since every item is at least a byte, and every count-sized
reservation reads through it or through `bounded_len`. The sweeps are
deterministic by seed and bounded in rounds, so a failure is a repeatable
input and the suite's time is known; a real crash found later is a new
sample for the sweep that missed it.

**Encryption at rest is a property of the bytes a file holds, so every
path is either a content path or a copy path.** A content path makes or
reads a file's meaning -- a segment sealed or opened, a manifest, a delete
log, the WAL, `RANGE`, `CATALOG` -- and goes through `cipher::Cipher`:
`seal_file` frames the plaintext (64 KiB per frame, `nonce | ciphertext |
tag`, the file's identity and the frame's index in the AAD), `open_file`
and `read_range` open it, the latter only the frames a ranged read
touches, so an archived segment faults in component by component as it
did. A copy path -- a backup, an export, a move over the wire, a tier
move into the store and back -- moves the framed bytes as they lie and
never reads them; an `ExportShard` is therefore already in the file
regime when it is built, and a restore, a move or an import that lands
its files where they came from needs the same data key, which is why a
cluster's pods are given one (`CELASTRO_KEY_FILE`, `key init`) and a
backup and an export carry `KEY`. The one path that crosses regimes is
`import_collection`: it opens each file under the export's key and seals
it under the database's, which is how a plain database takes a key and
how one changes keys. A file's identity is its shard directory's name and
its own (`shard-0003/00000000000000a1.seg`), the same in `segments/`,
`archive/` and the store, so a file cannot stand in for another and a
tier move needs no rewrite; root files are their bare names. The WAL is
length-prefixed frames, one per record, the record's ordinal in the AAD,
so a torn tail ends the replay where the CRC would have and a frame
cannot be replayed from another log. The data key is per database, drawn
at the first open of an empty directory and kept in `KEY` wrapped under
the master (`CELK1 | nonce | ciphertext | tag`); per-file keys are HKDF
of it and the identity; the master (`CELASTRO_MASTER_KEY_FILE`, 32 bytes
or 64 hex digits, or `CELASTRO_MASTER_KEY`) is never written, and `key
rekey` rewraps `KEY` under a new one without touching a data file. A
directory with `KEY` and no master is refused, and so is a plain
directory with data offered a master: encrypting in place would be a
rewrite of every file behind a running database's back, and the export
and import say the same thing honestly. Backups and exports of an
encrypted database are encrypted (`KEY` alongside), a restore into a
plain database or under another master is refused before a byte is
written, and the console's token is what it was: the key protects the
bytes at rest, the token the console. What it costs, on the survey's
corpus (0.37.0): the batched load 23% longer (a seal is one more pass
over each segment, and every WAL record is a frame), `COMPACT` 2%, the
reopen twice as long at a quarter of a second, and the reads -- point
lookups, BM25, vector, hybrid, the walk -- within noise: a segment's
components are opened once into the residency cache, and the frames are
opened on the way in.

**`serve` is a well-behaved PID 1, by an in-tree `signal(2)` binding.** The
kernel does not deliver a default-disposition signal to PID 1, so a container
running `serve` could only be stopped by the ten-second SIGKILL or by
`--init`. `std` has no signal API, so the handler is one `extern "C"`
declaration and one atomic store, and the accept loop polls the listener
rather than blocking on it, because a blocking accept is restarted after the
handler runs. Only `serve` installs it: at a REPL a handled Ctrl-C would be
swallowed by the restarted read. The route is recorded in `src/signal.rs`.

**A statement is all or nothing on the log, and a seal is nobody's
statement.** Measured on a 48 MB tmpfs filled by inserts (0.43.1): the
batch whose append hit `ENOSPC` had the 289 records that fit replayed at
the next reopen, for a statement the client was told had failed; and a
seal that failed inside a batch's mutation loop failed the statement with
384 of its rows applied in memory and all 500 on the log, so the running
node showed rows a reopen did not. Two rules follow. The log is marked
before a statement's records and cut back to the mark (`set_len`, which
needs no space) when an append or the sync fails, so a refused statement
leaves nothing to replay. And the seal a write triggers runs after the
whole batch is applied and cannot fail the write: the write is on the log
and in memory, which is what was promised; the seal's failure is counted
on the shard (`seal_failures`, in `/api/metrics`) and the next write
tries again -- on a disk that stays full, each following append is
refused cleanly instead. A directory that vanished under a running node
(unmounted, removed) is the third case the same run showed: the log's
open descriptor accepts bytes into a file no reopen can find, so a write
is refused when the `LOCK` this process holds is not where it was, and
the health probe says the node is not well so its supervisor restarts it
where the missing directory can be seen. Tests inject the failures
through the durability probe (`fail_next`, `fail_after`) at the append,
the temporary file's fsync and by renaming the directory aside.

**Dedicated coordinators, and how many.** A node started with
`CELASTRO_ROLE=coordinator` holds no shards -- `plan_tablets`,
`rebalance` and `move_shard` skip or refuse it by the role its `hello`
declared at `ATTACH`, kept in the catalog (format 6) -- and hears every
definition (`fan_out_of` splits statements: a definition reaches holders
and coordinators, a seal or a compaction reaches holders), so it plans
over the data nodes' shards as they do, on cores with no seal or
compaction of their own; and at `ATTACH` it pulls the node's catalog
(`Call::Catalog`) and adopts the collections, maps and policies it
lacks, so a coordinator that arrives late or restarts empty plans at
once -- a data node does not pull, since one that lost its volume must
not quietly grow empty shards for a map that names it. Measured on kind
(2026-09-18, the survey corpus
of 50,000 documents spread over `N` data nodes, one coordinator taking
every client request, CPU seconds per container from the node's
`crictl stats` over 400 requests at concurrency 16): the coordinator's
CPU per request grows with `N` -- hybrid 6.2 ms at `N = 3`, 11.5 ms at
`N = 6`, about 2 ms per data node fused; text and vector about 0.5 ms
per data node; a point lookup 1.0-1.7 ms whatever `N`, which is the
console's HTTP and JSON and the forward -- while a data node's falls as
its shard shrinks (hybrid 10.8 ms at 17,000 documents a shard, 4.5 ms at
8,000). So the coordinators a cluster needs are `N × c₁ / d`: the data
nodes, times the fusion's per-node cost, over the shard's per-request
cost. On shards this small that is one coordinator per four or five data
nodes for hybrid traffic and one per seven for text; larger shards raise
it in proportion, since `d` grows with the shard and `c₁` does not. For
point lookups the coordinator costs more than the lookup, so the role
buys nothing there. What would move the ratio: cheaper JSON on the
console (the fixed 1 ms), and a merge that stops early once the top `k`
cannot change. The numbers are relative -- every pod shared four cores
-- and the ratio is what they are for.

**Durability is unix-shaped, and every mover is inside it.** The guarantee
rests on fsyncing the directory a rename landed in, which is a POSIX
operation; off unix `sync_dir` is a no-op and the guarantee weakens to what
the filesystem does on its own. The crate docs say so where the guarantee is
stated, because a guarantee that silently weakens by platform belongs in the
document a reader meets and not in a comment. Every rename the database makes
is followed by that fsync, and the invariant test reads the whole event log
to prove it -- including the tier move that relocates a segment between
`segments/` and `archive/`, which used to be the one rename outside the
publication path: the manifest names the segment by id and looks for it in
both directories, so a rename whose entries a crash took back was a segment
the manifest named and neither directory held.

**Compaction runs itself in `serve`, and stays scheduled and visible.**
Until 0.36.0 nothing ran a job unless `COMPACT` was said, and 0.33.0's
flat seals made that a query cost that only grew. The console's
maintenance thread asks every second, under the write lock for a moment,
whether a shard has a job (`compaction::reserve`: the planner's answer
with its inputs pinned by their handles and its output ids reserved),
builds it holding nothing (`compaction::build`, the same rows, layers and
pieces `run` makes), and installs it under the lock
(`compaction::install`), where a shard that moved on declines it. One
job at a time, a log line each, `CELASTRO_AUTO_COMPACT=off` to stop it:
what §12.1 asked for, minus the waiting to be asked.

**A statement's cost is bounded by a deadline that is on by default and
checked inside the loops.** Thirty seconds unless the `Db` or the statement
says otherwise; `WITH (deadline_ms = N)` raises it, `WITH (no_deadline)` lifts
it. The check is not only between shards: the graph traversal, the brute-force
distance pass, the WAND loop, the prefix walk and the unranked scan each ask a
thread-local clock every 128 steps and stop when it has passed. A loop that
stops returns less than it was asked for, which is the silent partial answer
this engine refuses to give -- so the loop never reports the cut; the executor
asks after every unit and at the end of the statement, and turns a passed
deadline into a refusal naming the budget, or, under `partial_results`, into
the shard listed as missing. Nothing a timed-out shard produced reaches the
merge. Writes, DDL and maintenance carry no budget.

**What is API is stated, and it is small.** `Db` and what it hands out.
Options structs are `#[non_exhaustive]` and built from `Default` field by
field; report structs are `#[non_exhaustive]` and read. A field added to
either is not a breaking change, which matters for a `0.x` crate that has
grown one in most releases. `Shard` is reachable through `Db::shards` for
reading -- its catalog, key range, segment set, snapshot and summaries -- and
its writes, flush, compaction, publication and WAL are crate-private, along
with the statistics gathers whose preconditions are documented rather than
enforced: a precondition on a crate-private call is the crate's own to keep.
The crate docs carry the contract as a doctest.

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
| exact mode bit-identical across shard counts, through a score tie at the k-th slot inside a memtable | `a_score_tie_inside_a_memtable_is_bit_identical_across_shard_counts` (300 tied documents in reverse key order, unflushed, at 1, 3 and 6 shards, and equal to the sealed answer), `text::scorer::tests::a_memtable_score_tie_is_broken_by_key_and_not_by_push_order` (the collector, with the tie arriving after the heap is full so the pruning bar is exercised), `exact_mode_is_bit_identical_across_shard_counts` (1, 3, 6 shards), `exact_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes` (linear fusion, so a length norm can reach the assertion) |
| exact global statistics are a function of the live corpus | `exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes`, `shard::tests::the_length_numerator_matches_a_brute_force_fold_at_every_snapshot` |
| a freshly refreshed default gather matches `WITH (exact_scoring)` for Term, Phrase and Prefix queries, and the default triple is identical at every shard count fresh or stale | `default_statistics_are_identical_across_shard_counts_under_updates_and_deletes`, `default_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes`, `engine::tests::a_freshly_refreshed_cache_answers_exactly_what_the_exact_gather_answers`, `engine::tests::a_prefix_query_in_a_fresh_epoch_ranks_like_exact_scoring` (the Prefix leg: the only test that runs both arms and compares them), `engine::tests::a_prefix_only_query_still_gets_real_globals` (a prefix query's globals, which is a different claim) |
| a prefix query means the same thing at every shard count | `a_prefix_query_ranks_the_same_at_every_shard_count` (the document frequencies), `a_prefix_query_finds_the_same_documents_at_every_shard_count` (the expansion set itself, positive and negated, with no scoring in it), `engine::tests::a_prefix_resolves_to_the_same_terms_at_every_shard_count` (the cap applies to the union), `engine::tests::a_prefix_term_living_in_one_shard_is_weighted_by_the_whole_collection` |
| a prefix names the LIVE vocabulary, so neither a dead term nor the compaction schedule can displace a live one out of the cap | `a_prefix_query_names_the_live_vocabulary_not_the_physical_one` (absolute answers, before and after `COMPACT`, at 1/4 and 1/6 shards), `engine::tests::no_term_in_a_resolved_expansion_comes_back_with_a_zero_frequency`, `engine::tests::a_prefix_expansion_over_an_unflushed_memtable_takes_the_first_terms` |
| a truncated prefix expansion is reported, on every query shape and without `EXPLAIN ANALYZE` | `engine::tests::a_truncated_prefix_says_so_on_a_plain_query_of_either_shape`, `engine::tests::an_expansion_of_exactly_the_cap_dropped_nothing_and_must_not_say_it_did` (the boundary), `text::scorer::tests::an_expansion_reports_truncation_only_when_a_term_was_actually_dropped` (the no-coordinator arm), `a_truncated_exclusion_says_rows_were_kept_not_that_rows_are_missing` (the negated leaf, whose consequence is the opposite one), `plan::exec::tests::a_cut_leaf_is_reported_in_every_polarity_the_statement_spelled_it_in` (one statement spelling both, which is one expansion and one line naming both), `celastro::tests::a_cut_prefix_reaches_the_json_the_way_a_missing_tablet_does` and `serve::tests::a_document_containing_a_quote_cannot_break_out_of_the_json_response` (the two JSON wires), `celastro::tests::a_cut_prefix_is_reported_by_this_shell_one_line_per_leaf` and `celastro::tests::a_cut_prefix_is_reported_by_this_shell_one_line_per_leaf` (the `TRUNCATED —` block each shell renders; where it is placed is pinned by the two placement tests below), `engine::tests::a_statement_carrying_more_prefix_leaves_than_the_budget_is_refused` (and that the bound counts DISTINCT prefixes, which is what its refusal now says), `engine::tests::a_delete_whose_predicate_was_cut_is_refused_rather_than_deleting_what_it_did_not_name` (both shapes: a cut DELETE writes nothing), `serve::tests::a_delete_whose_predicate_was_cut_is_refused_over_http_and_deletes_nothing` (that refusal's shape over HTTP: a 200 carrying `ok:false`, never an ack) |
| the cached statistics are live sums over every unit and shard, at one instant: stale, never mixed | `engine::tests::the_cached_statistics_are_live_sums_over_every_unit_of_every_shard`, `engine::tests::a_term_filled_mid_epoch_is_measured_against_the_document_count_it_will_be_divided_by` (inserts), `engine::tests::a_frequency_and_the_count_it_is_divided_by_are_never_from_different_instants` (deletes, where `doc_freq ≤ num_docs` is what a mixed instant breaks) |
| the entry cap bounds what is retained, never what is answered | `engine::tests::the_per_term_statistics_stay_bounded_at_the_entry_cap`, `engine::tests::the_entry_cap_survives_an_epoch_rollover` |
| a stale statistic is not a shard-dependent one | `engine::tests::the_statistics_refresh_at_the_same_write_counts_whatever_the_shard_count`, `engine::tests::the_epoch_clock_keeps_running_when_every_query_fills_a_new_term` |
| a cached statistic is gathered at the current query's timestamp, never at a stored one | `engine::tests::a_fill_later_in_the_epoch_gathers_at_the_query_timestamp_and_not_a_stored_one` (the detector), `engine::tests::a_statistic_gathered_at_a_pinned_timestamp_does_not_stay_true_at_that_timestamp` (the demonstration) |
| a query answers with every term it asked for, cached or freshly gathered | `engine::tests::a_second_query_at_the_same_instant_still_gets_the_first_query_s_frequency` (two gathers at one instant, overlapping term lists: the second re-gathers only what is missing, so the rest has to come out of the cache) |
| a term named twice in one term list is counted once | `engine::tests::a_term_named_twice_is_counted_once_on_both_arms` (the term list is a `Vec` on a public method; counted twice it is `df > num_docs`, not a stale number) |
| the average document length falls back only with nothing to average | `engine::tests::the_average_document_length_falls_back_only_when_there_is_nothing_to_average` (zero documents, where the division is 0/0, and one, where there is a real average) |
| reading the past is `WITH (exact_scoring)`, and the default path says so | `engine::tests::a_historical_timestamp_on_the_default_arm_is_a_caller_error_and_not_an_approximation` |
| IDF is total: no statistics make it negative | `text::scorer::tests::idf_is_never_negative_however_incoherent_the_statistics` |
| approximate mode within tolerance | `approximate_mode_across_shard_counts_stays_within_recall_tolerance` |
| WAND ≡ brute force | `text::scorer::tests::wand_agrees_with_brute_force` |
| fusing early is wrong | `plan::fusion::tests::fusing_early_gives_a_different_and_wrong_answer` |
| filtered-search strategy selection prices the traversal that would run, and a visit budget binds knowingly | `vector::tests::{few_survivors_pick_brute_force_and_are_exact, high_selectivity_picks_post_filter, a_ten_percent_filter_on_a_small_segment_is_scanned_not_traversed, filter_aware_is_chosen_by_its_visits_and_a_budget_bounds_them}` (the last asserts the visit count against the model's bound, and that a budget of 64 stops the walk at 64 and says so) |
| visibility under deletes and updates | `mvcc::tests::*`, `shard::tests::*` |
| segments survive a reopen | `a_database_survives_reopen` |
| an acknowledged write is on the disk, and every step is in the order the guarantee needs | `shard::tests::the_three_fsyncs_are_syscalls_and_not_bookkeeping` (the floor under the rest: each fsync helper is handed a descriptor the kernel refuses to sync and has to report it, so none of them can be satisfied by bookkeeping), `shard::tests::an_insert_appends_its_wal_record_and_then_makes_it_durable`, `shard::tests::a_batch_of_inserts_is_appended_whole_and_synced_once`, `engine::tests::a_statement_of_many_documents_syncs_once_per_insert_batch` (a statement of many documents: every record appended, one sync, then the rows; a document that cannot be taken keeps the whole batch out of the log; a key recurring in the batch is two versions in order), `shard::tests::an_insert_that_supersedes_a_document_syncs_the_record_that_supersedes_it`, `shard::tests::a_delete_makes_its_wal_record_durable_before_it_returns`, `shard::tests::a_publication_syncs_the_bytes_then_renames_then_syncs_the_name` (a directory synced BEFORE its rename is a directory synced for nothing), `shard::tests::a_seal_publishes_the_delete_logs_durably_and_before_the_manifest`, `shard::tests::a_seal_publishes_the_manifest_before_it_empties_the_wal`, `engine::tests::creating_a_collection_makes_the_directories_that_hold_it_durable` (the directory entries: an fsynced file whose directory was never synced is a file nothing names), `engine::tests::the_tablet_map_is_published_durably_and_a_damaged_one_is_refused` (and that an empty line in it is an unbounded end, not the bound `""`, which is a shard owning no keys), `engine::tests::every_publication_fsyncs_the_directory_it_renamed_into` (the same claim stated once over the whole event log rather than once per file, so the call site written next is covered without a test of its own) |
| a write that could not be made durable is reported rather than acknowledged | `shard::tests::a_wal_sync_that_fails_is_reported_and_leaves_the_shard_as_it_was`, `shard::tests::a_seal_whose_manifest_cannot_be_published_keeps_the_wal`, `shard::tests::a_publication_that_failed_after_the_rename_is_retried_rather_than_believed`, `engine::tests::a_catalog_that_disappeared_is_republished_rather_than_skipped`, `shard::tests::a_manifest_that_was_replaced_underneath_the_shard_is_republished`, `shard::tests::a_delete_log_that_was_removed_underneath_the_shard_is_republished` (the delete-log half of the same skip, where believing the cache brings every deleted document back), `engine::tests::a_catalog_publication_that_failed_after_the_rename_is_retried` (the catalog's own copy of the claim: a publication that failed after the rename must not be cached as published) |
| a publication that fails leaves the shard describing what is on the disk, and no id it has spoken for is handed out again | `shard::tests::a_seal_whose_manifest_publication_fails_installs_nothing`, `shard::tests::a_compaction_whose_publication_fails_keeps_its_inputs_and_retires_them_on_the_retry` (the same claim at the other call site, and the leak that rode on it: inputs a failed compaction dropped are named by no manifest and unlinked by nothing), `shard::tests::a_reopen_does_not_hand_out_a_segment_id_the_disk_already_holds` (the manifest is not the whole record of which ids are spoken for, and the segment that reuses one inherits the delete log of the segment that was never published), `shard::tests::an_attach_to_a_populated_directory_refuses_the_ids_it_already_holds` (the guard is on the directory, not on one of the two ways into it), `shard::tests::a_reopen_reads_every_directory_a_segment_id_can_be_hiding_in` (a tiered segment's file is in `archive/` and a delete log in neither of the other two), `shard::tests::a_reopen_reclaims_the_files_of_a_publication_that_never_landed` (the other side of the same coin: the outputs a failed publication abandoned are unlinked at the one moment nothing can be holding them, and what the manifest names is left alone) |
| a file the open could not read is reported, never read as absent | `engine::tests::a_catalog_that_cannot_be_read_fails_the_open_rather_than_opening_empty`, `shard::tests::a_manifest_that_cannot_be_read_fails_the_open_rather_than_opening_empty`, `shard::tests::a_delete_log_that_cannot_be_read_fails_the_open_rather_than_resurrecting` (each pins both halves: absent still opens, unreadable fails naming the file -- a short or corrupt file was already refused, so an unreadable one was the only failure the open believed) |
| a damaged delete log is refused, never read as a shorter one | `shard::tests::a_damaged_delete_log_fails_the_open_rather_than_losing_a_deletion` (every truncation and every flipped bit of a published log fails the open naming the file), `mvcc::tests::a_framed_delete_log_refuses_every_truncation_and_every_flipped_byte` (the count and the checksum are separately load-bearing: a log that lost a record and was re-signed is refused by the count), `shard::tests::a_delete_log_written_before_the_frame_opens_and_is_rewritten_framed`, `mvcc::tests::a_delete_log_without_the_frame_still_decodes` (an existing database opens, and the next publication closes its unframed window) |
| the catalog counts every document once, however many times the directory is reopened | `engine::tests::a_reopen_with_an_unflushed_wal_counts_its_documents_once` (three claims, each the mutation that passes the others: right before any statement, unchanged across reopens with an unflushed WAL, and a record no persist ever saw is counted once) |
| the SELECT list decides what a row carries | `engine::tests::the_select_list_decides_what_a_row_carries` (a named path is kept and an unnamed one is not, an alias renames, a nested path is keyed as written, a missing path is `Null` rather than absent, `*` keeps everything, and a ranked query keeps its `score`) |
| an unranked scan holds one page, and answers like one that held everything | `plan::exec::tests::a_scan_retains_no_more_rows_than_the_page_and_the_same_rows_as_a_full_sort` (the collector: never more than the page at any point of a scrambled arrival, under `COLLAPSE BY`, and the same rows a full sort-collapse-page yields), `engine::tests::a_scan_under_a_small_limit_decodes_the_page_and_answers_like_a_full_one` (a key-ordered scan decodes exactly the rows it returns, past an `OFFSET` and a cursor; a field order decodes every survivor and still answers the same; a collapse returns one row per parent) |
| a distance threshold in `WHERE` agrees with the `distance` column, composes, and is three-valued | `engine::tests::a_distance_threshold_in_where_agrees_with_the_distance_column` (both metrics, five thresholds each way, an AND with a structured predicate, `NOT` leaving the vectorless document on neither side, exact match at `<= 0`, the plan naming brute force, and the three refusals), `a_distance_threshold_returns_the_same_rows_at_every_shard_count` (equality, not a tolerance: there is no candidate depth in a predicate), `sql::parser::tests::a_distance_threshold_parses_as_a_predicate_and_not_as_an_order` |
| a collection's prefix expansion cap is a setting, its leaf budget is derived from it, and the cache is the ceiling | `engine::tests::a_collection_can_raise_its_prefix_expansion_and_pays_with_its_leaf_budget` (the union cut, the budget, the ceiling, the persisted setting, the export, the DELETE refusal and the CREATE option, each the mutation that fails it), `text::scorer::tests::an_expansion_reports_truncation_only_when_a_term_was_actually_dropped` (the no-coordinator arm reads the cap off the statistics), `catalog::tests::a_version_2_catalog_is_read_with_every_collection_at_the_default_cap` (the format step: 2 reads at the default, 1 and 4 are refused), `sql::parser::tests::a_collection_s_prefix_expansion_is_set_at_creation_or_altered_later` |
| a pinned seal emits at most `max_versions` segments, and an unpinned one does not seal on depth | `shard::tests::a_pinned_seal_fans_out_to_at_most_max_versions_segments` (twelve versions at a threshold of four: three seals of four pinned, one seal of one unpinned) |
| a dropped collection leaves nothing behind under its name, and an interrupted drop completes at the next open | `engine::tests::dropping_a_collection_removes_it_and_everything_recorded_against_its_name` (files, statistics and clocks gone; a recreated collection measured afresh; the policy refusal; the interrupted state completed and swept at open), `archive_s3::dropping_a_collection_deletes_its_objects_from_the_store` |
| a dropped index is withdrawn everywhere the declaration reached | `engine::tests::dropping_an_index_withdraws_the_declaration_and_what_was_recorded_against_it` (the planner, the statistics, the clock, a reopen, and a re-declaration that finds the sealed regions), `sql::parser::tests::drop_collection_and_drop_index_parse_and_name_what_they_drop` |
| a fault on the coordinator-to-shard boundary can shorten an answer only by saying so, and a seeded run reproduces exactly | `sim::tests::a_fault_cannot_change_an_answer_without_saying_so` (twenty seeds of drops and restarts, every query shape: refused or bit-identical, never different), `sim::tests::a_partial_answer_names_every_shard_that_did_not_answer_and_carries_only_real_rows` (`missing` is exactly the dropped shards, no second call to a shard given up on, real rows only, and the cache holds no partial sum afterwards), `sim::tests::a_shard_that_restarted_answers_exactly_what_it_did_before` (every call answered by a replacement opened from the directory), `sim::tests::the_order_shards_answer_in_does_not_change_the_answer` (and the plan lists shards by index), `sim::tests::a_seeded_run_reproduces_its_trace_and_its_answers` |
| a walk is the neighbourhood and nothing else, the same at every layout and across nodes, and a cut or a dangling edge is said, never hidden | `engine::tests::a_hop_filter_selects_the_neighbourhood_and_nothing_else` (1..k, the start excluded, the edge filter at every hop, `REVERSE`, `OR`/`NOT`, fused with text and a distance, every refusal), `engine::tests::a_hop_statement_is_bit_identical_across_shard_counts` (1, 3 and 6 shards of both collections, memtable and segments, a deleted node and a dangling edge), `a_walk_over_collections_spread_over_three_nodes_answers_what_one_process_answers` (the same through the wire, and a holder that stops answering is a deadline or a named absence), `engine::tests::a_cut_walk_says_which_cap_bound_it` (both caps, the lexicographically first kept, the line on the response, in the plan and in the console's JSON), `engine::tests::a_dangling_edge_is_skipped_and_counted` (a never-existed and a deleted target, per hop, and nothing walked through a deleted node), `engine::tests::a_walk_over_a_cold_adjacency_index_is_refused_naming_the_tier`, `sim::tests::a_faulted_walk_refuses_or_agrees_and_a_partial_one_says_so` (twenty seeds over `expand` and `present`: refused or bit-identical, a partial answer inside the unfaulted neighbourhood and short only with `missing`), `sql::parser::tests::a_walk_parses_as_a_filter_with_a_one_term_edge_filter`, `catalog::tests::catalog_round_trips` (format 5: `nodes_of`, `undirected`, the adjacency kind, and a 4 read as a plain collection) |
| random histories of creations and drops on three nodes, reconciled pairwise in random orders, converge on every node to what the instants say, indexes on dead incarnations included | `reconcile::random_histories_on_three_nodes_converge_to_the_last_word_on_each_name` |
| every statement delivered twice with nothing written in between leaves what once leaves; a `DELETE ... WHERE` retried after a write takes the new row too, by contract | `retry::every_statement_delivered_twice_leaves_what_once_leaves`, `retry::a_delete_by_predicate_retried_after_a_write_takes_the_new_row_too` |
| a write through one node is read through every other at once, counts through different nodes never go backwards, and a delete through one is gone through all | `wire::a_write_through_one_node_is_read_through_every_other_at_once` |
| a backup `AS OF` an earlier instant holds what was visible then; an instant ahead of the clock is refused; `BACKUP CLUSTER` backs every node up at one instant, verifiable on each | `backup::a_backup_as_of_an_earlier_instant_holds_what_was_visible_then`, `wire::a_cluster_backup_is_one_instant_on_every_node` |
| an expired certificate is refused by a peer naming the time; `SHOW HEALTH` says when the certificate and the CA expire and flags either inside two weeks | `tls::an_expired_certificate_is_refused_by_a_peer_and_named_by_health_ahead_of_time` |
| a node is attached only by the address it calls itself | `wire::a_node_is_attached_only_by_the_name_it_calls_itself` |
| past the cap a wire connection is closed at once and counted; an idle one is closed after the idle time and the next call reconnects | `wire::idle_wire_connections_are_capped_and_closed` |
| a due seal with a sealer running freezes rather than builds: the rows stay readable and deletable in the frozen memtable, the log is rotated aside, and the install commits the segment with the delete made meanwhile and removes the rotated log | `shard::background_seal_tests::a_frozen_memtable_is_read_and_deleted_until_its_seal_is_installed` |
| a seal frozen but not installed when the process ends replays from its rotated log with the live one, and the next seal covers both | `shard::background_seal_tests::a_seal_frozen_but_not_installed_replays_from_its_rotated_log` |
| a point lookup through the console answers while a 20,000-vector seal builds off the lock | `resilience::a_point_lookup_answers_while_a_large_vector_seal_builds` (`--ignored`) |
| a fresh connection to a process older than the newest seen at its address is refused before a statement goes down it, and a hello still names it | `wire::a_fresh_connection_to_an_older_process_is_refused` |
| the resilience suite, run when asked: the reconciliation over four nodes and four hundred seeds; no acknowledged write lost across five restarts under load, and no failure that is not the node or a deadline; a 200,000-row log replays every row; a cluster backup under load restores to one cut | `resilience::*` (`--ignored`) |
| a peer whose clock is more than five seconds off is refused at ATTACH naming both clocks; one under that is attached and `SHOW HEALTH` shows its offset and flags it past half a second | `wire::a_peer_whose_clock_is_off_is_refused_or_named` |
| a hello with a newer epoch is a restart, said once; an older epoch after it is a second process at the address, said on every `SHOW HEALTH` that sees it | `wire::an_older_process_answering_at_an_attached_address_is_named` |
| a move made while a node was away reaches its map when it reconnects, from the old holder's word or the new one's, and its count routes to the shard where it is | `wire::a_move_made_while_a_node_was_away_reaches_its_map_when_it_reconnects` |
| a node away through DDL catches up when it reattaches: the index made and the one dropped while it was away, a collection created without it whose shard it then builds, a re-creation younger than its tombstone kept, and a drop flowing the other way; an `ALTER` is still refused naming the node | `wire::a_node_away_through_ddl_catches_up_when_it_reattaches` |
| a data node restarted from an empty directory does not grow empty shards for a collection older than the directory; it says so once, `SHOW HEALTH` says so until it is settled, a younger collection is adopted, and a coordinator adopts everything | `wire::a_fresh_directory_does_not_grow_empty_shards_for_an_older_collection` |
| a collection spread over three nodes, written through any of them, answers on every node what one process answers, and DDL reaches every holder | `wire::a_collection_spread_over_three_nodes_answers_what_one_process_answers` (placement by attach order, routed writes, bit-identical answers on every node against a single-process reference, the plan with remote blocks, partition pruning across nodes, DELETE by predicate, FLUSH and DROP INDEX fanning out and `LOCAL` not, DETACH refused while a node holds a shard, export refused, placement surviving a restart, DROP COLLECTION reaching every holder), `catalog::tests::catalog_round_trips` (the node list and the placement) |
| a node that does not answer is a deadline and nothing quieter, and the wire refuses the wrong token and the wrong version by name | `wire::a_node_that_does_not_answer_is_a_deadline_and_nothing_quieter`, `wire::tests::*` (addresses, the codec, the token comparison) |
| a shard moves between nodes with no row lost or duplicated, every node agrees on the map, a pinned shard refuses writes naming the move, and an emptied node detaches | `wire::a_shard_moves_between_nodes_and_every_node_agrees` (source and target both elsewhere, target here, source here; answers on every node equal one process's after each; the refusal on a pinned shard and the write after the abort; `REBALANCE`; `DETACH` refused with the plan and accepted once empty; the map after a restart) |
| the console offers the source of the running version | `serve::tests::the_console_offers_the_source_of_the_running_version` (on the page, absolute, naming the version and the licence, and on the health endpoint for a client that never renders the page) |
| a statement cannot run past its deadline, and the deadline is on by default | `deadline::tests::a_deadline_is_armed_per_statement_and_restored_when_the_statement_ends`, `vector::tests::a_search_stops_when_the_deadline_has_passed` (brute force, graph traversal and the threshold pass each stop at once), `text::scorer::tests::scoring_stops_when_the_deadline_has_passed` (top-k and the filter walk), `engine::tests::a_statement_past_its_deadline_is_refused_by_default_and_the_budget_is_named` (every query shape refused, `partial_results` reports the shards instead, `no_deadline` lifts it, and a default `Db` shows its budget in the plan) |
| the console says a query was cut, and the shells say it where a reader looks | `serve::tests::the_console_script_reads_and_renders_a_truncated_expansion` (a static check on the script: the field is read and rendered as the shells render it), `celastro::tests::a_cut_prefix_is_printed_between_the_table_and_the_row_count`, `celastro::tests::a_cut_prefix_is_printed_between_the_rows_and_the_row_count` (through a writer, so the placement is pinned and not only the text) |
| one process per data directory | `engine::tests::a_directory_is_opened_by_one_db_at_a_time` (`flock` on `LOCK`: a second open is refused naming the holder's pid, and succeeds once the first is dropped; the kernel releases a crashed holder's lock) |
| a tier move publishes its renames like everything else | `engine::tests::an_archive_move_fsyncs_both_directories_and_survives_a_reopen` (the rename into `archive/` and back is recorded, no rename is left without a directory fsync after it, and the moved segment is found at the next open) |
| the statistics cache ages by its own collection's writes | `engine::tests::writes_to_another_collection_do_not_age_this_ones_statistics` (a refresh interval of writes to B leaves A's epoch and anchor where they were; the same writes to A end it) |
| `serve` ends cleanly on SIGTERM, promptly, with the last write saved | `serve_signals::sigterm_shuts_the_console_down_cleanly_and_the_last_write_survives` (the real binary, a real signal, an exit bounded in time, and a reopen that finds the collection created a moment before), `signal::tests::the_handlers_install_and_nothing_is_requested_until_a_signal_arrives` |
| a refused write leaves no record and no row; a failed seal does not fail the write and is retried; a vanished directory refuses writes and is not well | `shard::tests::a_write_the_log_refuses_leaves_no_record_and_no_row` (an append that fails at the third record of a batch: the log cut back, memory untouched, a reopen with the acknowledged rows; the same for one document and a delete), `shard::tests::a_seal_that_fails_leaves_the_write_acknowledged_and_is_retried` (the segment's temporary fsync fails: the batch acknowledged and visible, the failure counted, the next write seals, a reopen has every row), `engine::tests::a_vanished_directory_refuses_writes_and_is_not_well` |
| a coordinator holds no shards, takes none, hears every definition, and answers what a data node answers | `wire::a_coordinator_holds_no_shards_and_answers_over_the_data_nodes` (created at the coordinator: three shards over two data nodes and none here; an index made at a data node in the coordinator's catalog; every query answering what one process answers; a move and a placement naming it refused; a rebalance skipping it; `SHOW HEALTH` naming the roles) |
| retention removes older backups and only the pool segments nobody names | `backup::keep_removes_older_backups_and_the_pool_segments_nobody_references` (`tests/backup.rs`: three backups, `KEEP 2`; the oldest instant gone and refused `AS OF`, the newest restores and verifies, a pool segment only the oldest named is gone and one the newer ones share is kept) |
| a backup's record carries a checksum per object; `VERIFY BACKUP` reads everything back, a restore checks as it writes, and a flipped byte is named and refused | `backup::verify_backup_reads_every_object_back_and_a_flipped_byte_is_named_and_refused` (`tests/backup.rs`: the verify's ack, a pool segment with one byte flipped named by VERIFY and refused by RESTORE with nothing adopted, a version-1 record verified by size with a note) |
| a backup restores what was there at the pin, copies only what is new the second time, refuses a damaged destination before writing, offers an older instant, and runs its copy with the console's lock let go | `backup::*` (`tests/backup.rs`: the round trips on a directory, the pool's dedup counted in the ack, a pool file truncated then removed, `AS OF`, a bare name confined to `backup_dir`, and the console path through `celastro send`), `archive_s3::a_backup_to_a_bucket_restores_from_it` (`s3://` through the archive's endpoint, `ListObjectsV2` naming what is there), `archive_s3::the_archived_tier_on_a_directory_store_holds_the_segments_and_reopens_from_them`, `objstore::tests::a_directory_store_holds_objects_as_published_files` |
| every parser that reads the network or a file answers `Ok` or `Err` to thousands of mutants of valid input, never panics or aborts | `fuzz::*` is the seeded mutator (`src/fuzz.rs`, tests only); the targets are `x509::tests::fuzz_certificate_parsing_never_panics`, `pem::tests::fuzz_pem_decoding_never_panics`, `tls13::tests::fuzz_handshake_message_parsing_never_panics` (hellos, Certificate, NewSessionTicket, and a mutated ticket never opens), `serve::tests::fuzz_request_heads_never_panic`, `objstore::tests::fuzz_store_responses_never_panic`, `wire::tests::fuzz_wire_answers_never_panic`, `shard::tests::fuzz_manifest_and_wal_never_panic`, `mvcc::tests::fuzz_delete_logs_and_ordinals_never_panic`, `engine::tests::fuzz_catalog_decoding_never_panics`, `segment::tests::fuzz_segment_and_component_decoding_never_panics`, `hnsw::tests::fuzz_graph_decoding_never_panics`, `quant::tests::fuzz_code_decoding_never_panics`, `variant::tests::fuzz_variant_decoding_never_panics`, `json::tests::fuzz_json_parsing_never_panics`, `parser::tests::fuzz_sql_parsing_never_panics`, `query::tests::fuzz_text_query_parsing_never_panics`. First run: the wire's answers and the manifest reserved a `Vec` for a count read from the input, and a count of 2^50 aborted the process on the allocation -- `codec::get_count` now refuses a count larger than the bytes left |
| an aggregate is the fold over every admitted row, on one node or three, and refuses what it cannot mean | `aggregates::*` (`tests/aggregates.rs`: every function over a flushed segment and a memtable with a delete, nulls skipped, the empty fold, `GROUP BY` with `ORDER BY` an alias and a page, the null group, the refusals), `wire::an_aggregate_over_three_nodes_answers_what_one_process_answers`, `parser::tests::aggregates_parse_with_their_names_and_the_group_by_path` |
| an encrypted database writes no plaintext anywhere and opens under its master only | `encryption::an_encrypted_database_holds_no_plaintext_and_opens_under_its_master_only` (a marker string grepped for under the directory, the store, a backup and an export after seals, a delete, a compaction and a tier move both ways; reopen through the WAL; no master and another master refused), `encryption::a_plain_database_with_data_is_not_encrypted_in_place`, `encryption::a_torn_wal_tail_stops_the_replay_where_the_last_whole_record_ended`, `encryption::a_backup_restores_under_the_same_master_and_is_refused_without_it`, `encryption::an_import_crosses_key_regimes_which_is_how_a_database_takes_or_changes_a_key`, `encryption::a_key_file_makes_the_nodes_of_a_cluster_share_one_data_key_so_a_shard_moves`, `cipher::tests::*` (frames round-trip, a ranged read opens only its frames, the wrong key, index or identity fails, the wrapped key opens under its master only, a torn record log stops at the tear) |
| the archived tier works against an S3-compatible store exactly as against a directory | `archive_s3::*` (an in-process S3 that checks every request is signed: a tier move puts and later deletes the object, a reopen with nothing local asks the store and answers, `Refuse` never touches it, a retired segment's object is deleted, credentials come only from the environment, an https endpoint is refused with the reason), `objstore::tests::*` (SHA-256, HMAC and the SigV4 signer against the published vectors) |
| a health probe measures the database, needs no token, and stays behind the Host check | `serve::tests::the_health_probe_needs_no_token_and_reports_the_database`, `serve::tests::the_probe_tells_serving_from_unwell_from_absent` (the client half: serving, unwell and absent are three answers), `celastro::tests::health_takes_a_port_and_nothing_else`; the chart itself is verified by hand against a `kind` cluster, as its README records |
| a copy is the source at its pinned instant, absent or complete, and adoptable elsewhere | `engine::tests::a_copy_is_the_source_at_its_pinned_instant_whatever_happens_after` (three shards, sealed and memtable rows, deletes on both; inserts, deletes, updates, a flush and a compaction between the pin and the write; the copy answers the source's pinned rows byte for byte and the source no longer does), `engine::tests::an_interrupted_copy_leaves_no_destination_to_open_by_mistake`, `engine::tests::an_import_adds_the_collection_to_another_instance`, `celastro::tests::export_and_import_take_their_arguments_and_no_more` |
| a record the crash tore is discarded, every record before it kept, and no record after it applied | `shard::tests::replay_stops_at_a_record_whose_crc_does_not_match` (three records with the damage in the middle: a log whose last record is the damaged one cannot tell stopping from skipping) |
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
  serve.rs serve/                the console: loopback by default, --bind for a network
  wire.rs sim.rs                 the wire between nodes; the seeded fault simulator
  tls.rs crypto/                 encryption in transit: the TLS 1.3 and its primitives
  cipher.rs                      encryption at rest: frames per file under a wrapped data key
  backup.rs                      BACKUP TO and RESTORE FROM over the object store trait
  signal.rs deadline.rs          SIGTERM for PID 1; the per-statement deadline
  bin/celastro.rs                the command: serve, exec, run, repl, demo, catalog, health,
                                 export, import, send, key, tls; `celastro-cli` is its old name
tests/
  integration.rs                 end-to-end behaviour
  tiering.rs                     tiers, residency, lifecycle policies
  wire.rs                        three nodes in one process, every node answering what one does
  tls.rs                         the console and the wire over TLS
  encryption.rs                  no plaintext at rest; refusals; backup, import and move under a key
  aggregates.rs                  count, sum, min, max, avg and GROUP BY over segments and a memtable
  pki/                           RSA and P-256 chains and signatures from openssl
```
