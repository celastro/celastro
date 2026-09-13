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
and 6 shards and compares fused scores bit for bit, and
`exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes`
does the same for the global statistics themselves, under a workload that
leaves dead rows behind for each shard to collect on its own schedule.

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
A prefix expands to at most `PREFIX_EXPANSION_LIMIT` (512) dictionary terms.
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

**Every query that was cut says so, whether or not it asked.** Truncation is
reported on `QueryResult::truncated_prefixes` — as `truncated_prefixes` in the
HTTP and `--json` responses, and as a `TRUNCATED —` line in the shells — for
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
Delete by key, or narrow the prefix until it expands to at most 512 terms and
delete the pieces; that loop deletes exactly what each piece names.

One statement may name at most eight distinct `(path, prefix)` pairs, *summed*
over its indexed paths. Each distinct one costs a dictionary walk in every unit
of every shard plus up to 512 gathered frequencies, and nothing in the
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

**The SELECT list narrows the document; `key`, `score` and `distance` are the
row's own.** A row is the primary key, the fused score or the distance where
the query ranked, and the document cut down to the named paths — `Null` where
a document has no such path, so that a projection over polymorphic documents
shows its gaps as empty cells rather than ragged rows. Naming `score` or
`distance` is accepted and changes nothing: a ranked query carries them
whether or not they are asked for. Projection is the last step, after
`COLLAPSE BY`, the cursor and the fetch, because each of those reads fields
the list may not name.

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
| exact mode bit-identical across shard counts | `exact_mode_is_bit_identical_across_shard_counts` (1, 3, 6 shards), `exact_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes` (linear fusion, so a length norm can reach the assertion) |
| exact global statistics are a function of the live corpus | `exact_statistics_are_identical_across_shard_counts_under_updates_and_deletes`, `shard::tests::the_length_numerator_matches_a_brute_force_fold_at_every_snapshot` |
| a freshly refreshed default gather matches `WITH (exact_scoring)` for Term, Phrase and Prefix queries, and the default triple is identical at every shard count fresh or stale | `default_statistics_are_identical_across_shard_counts_under_updates_and_deletes`, `default_mode_is_bit_identical_across_shard_counts_under_updates_and_deletes`, `engine::tests::a_freshly_refreshed_cache_answers_exactly_what_the_exact_gather_answers`, `engine::tests::a_prefix_query_in_a_fresh_epoch_ranks_like_exact_scoring` (the Prefix leg: the only test that runs both arms and compares them), `engine::tests::a_prefix_only_query_still_gets_real_globals` (a prefix query's globals, which is a different claim) |
| a prefix query means the same thing at every shard count | `a_prefix_query_ranks_the_same_at_every_shard_count` (the document frequencies), `a_prefix_query_finds_the_same_documents_at_every_shard_count` (the expansion set itself, positive and negated, with no scoring in it), `engine::tests::a_prefix_resolves_to_the_same_terms_at_every_shard_count` (the cap applies to the union), `engine::tests::a_prefix_term_living_in_one_shard_is_weighted_by_the_whole_collection` |
| a prefix names the LIVE vocabulary, so neither a dead term nor the compaction schedule can displace a live one out of the cap | `a_prefix_query_names_the_live_vocabulary_not_the_physical_one` (absolute answers, before and after `COMPACT`, at 1/4 and 1/6 shards), `engine::tests::no_term_in_a_resolved_expansion_comes_back_with_a_zero_frequency`, `engine::tests::a_prefix_expansion_over_an_unflushed_memtable_takes_the_first_terms` |
| a truncated prefix expansion is reported, on every query shape and without `EXPLAIN ANALYZE` | `engine::tests::a_truncated_prefix_says_so_on_a_plain_query_of_either_shape`, `engine::tests::an_expansion_of_exactly_the_cap_dropped_nothing_and_must_not_say_it_did` (the boundary), `text::scorer::tests::an_expansion_reports_truncation_only_when_a_term_was_actually_dropped` (the no-coordinator arm), `a_truncated_exclusion_says_rows_were_kept_not_that_rows_are_missing` (the negated leaf, whose consequence is the opposite one), `plan::exec::tests::a_cut_leaf_is_reported_in_every_polarity_the_statement_spelled_it_in` (one statement spelling both, which is one expansion and one line naming both), `celastro-cli::tests::a_cut_prefix_reaches_the_json_the_way_a_missing_tablet_does` and `serve::tests::a_document_containing_a_quote_cannot_break_out_of_the_json_response` (the two JSON wires), `celastro::tests::a_cut_prefix_is_reported_by_this_shell_one_line_per_leaf` and `celastro-cli::tests::a_cut_prefix_is_reported_by_this_shell_one_line_per_leaf` (the `TRUNCATED —` block each shell renders, which no test reached before; where the block is placed in the output is still unpinned), `engine::tests::a_statement_carrying_more_prefix_leaves_than_the_budget_is_refused` (and that the bound counts DISTINCT prefixes, which is what its refusal now says), `engine::tests::a_delete_whose_predicate_was_cut_is_refused_rather_than_deleting_what_it_did_not_name` (both shapes: a cut DELETE writes nothing), `serve::tests::a_delete_whose_predicate_was_cut_is_refused_over_http_and_deletes_nothing` (that refusal's shape over HTTP: a 200 carrying `ok:false`, never an ack) |
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
| filtered-search strategy selection | `vector::tests::{few_survivors_pick_brute_force_and_are_exact, high_selectivity_picks_post_filter, middling_selectivity_picks_filter_aware}` |
| visibility under deletes and updates | `mvcc::tests::*`, `shard::tests::*` |
| segments survive a reopen | `a_database_survives_reopen` |
| an acknowledged write is on the disk, and every step is in the order the guarantee needs | `shard::tests::the_three_fsyncs_are_syscalls_and_not_bookkeeping` (the floor under the rest: each fsync helper is handed a descriptor the kernel refuses to sync and has to report it, so none of them can be satisfied by bookkeeping), `shard::tests::an_insert_appends_its_wal_record_and_then_makes_it_durable`, `shard::tests::an_insert_that_supersedes_a_document_syncs_the_record_that_supersedes_it`, `shard::tests::a_delete_makes_its_wal_record_durable_before_it_returns`, `shard::tests::a_publication_syncs_the_bytes_then_renames_then_syncs_the_name` (a directory synced BEFORE its rename is a directory synced for nothing), `shard::tests::a_seal_publishes_the_delete_logs_durably_and_before_the_manifest`, `shard::tests::a_seal_publishes_the_manifest_before_it_empties_the_wal`, `engine::tests::creating_a_collection_makes_the_directories_that_hold_it_durable` (the directory entries: an fsynced file whose directory was never synced is a file nothing names), `engine::tests::the_tablet_map_is_published_durably_and_a_damaged_one_is_refused` (and that an empty line in it is an unbounded end, not the bound `""`, which is a shard owning no keys), `engine::tests::every_publication_fsyncs_the_directory_it_renamed_into` (the same claim stated once over the whole event log rather than once per file, so the call site written next is covered without a test of its own) |
| a write that could not be made durable is reported rather than acknowledged | `shard::tests::a_wal_sync_that_fails_is_reported_and_leaves_the_shard_as_it_was`, `shard::tests::a_seal_whose_manifest_cannot_be_published_keeps_the_wal`, `shard::tests::a_publication_that_failed_after_the_rename_is_retried_rather_than_believed`, `engine::tests::a_catalog_that_disappeared_is_republished_rather_than_skipped`, `shard::tests::a_manifest_that_was_replaced_underneath_the_shard_is_republished`, `shard::tests::a_delete_log_that_was_removed_underneath_the_shard_is_republished` (the delete-log half of the same skip, where believing the cache brings every deleted document back), `engine::tests::a_catalog_publication_that_failed_after_the_rename_is_retried` (the catalog's own copy of the claim: a publication that failed after the rename must not be cached as published) |
| a publication that fails leaves the shard describing what is on the disk, and no id it has spoken for is handed out again | `shard::tests::a_seal_whose_manifest_publication_fails_installs_nothing`, `shard::tests::a_compaction_whose_publication_fails_keeps_its_inputs_and_retires_them_on_the_retry` (the same claim at the other call site, and the leak that rode on it: inputs a failed compaction dropped are named by no manifest and unlinked by nothing), `shard::tests::a_reopen_does_not_hand_out_a_segment_id_the_disk_already_holds` (the manifest is not the whole record of which ids are spoken for, and the segment that reuses one inherits the delete log of the segment that was never published), `shard::tests::an_attach_to_a_populated_directory_refuses_the_ids_it_already_holds` (the guard is on the directory, not on one of the two ways into it), `shard::tests::a_reopen_reads_every_directory_a_segment_id_can_be_hiding_in` (a tiered segment's file is in `archive/` and a delete log in neither of the other two), `shard::tests::a_reopen_reclaims_the_files_of_a_publication_that_never_landed` (the other side of the same coin: the outputs a failed publication abandoned are unlinked at the one moment nothing can be holding them, and what the manifest names is left alone) |
| a file the open could not read is reported, never read as absent | `engine::tests::a_catalog_that_cannot_be_read_fails_the_open_rather_than_opening_empty`, `shard::tests::a_manifest_that_cannot_be_read_fails_the_open_rather_than_opening_empty`, `shard::tests::a_delete_log_that_cannot_be_read_fails_the_open_rather_than_resurrecting` (each pins both halves: absent still opens, unreadable fails naming the file -- a short or corrupt file was already refused, so an unreadable one was the only failure the open believed) |
| a damaged delete log is refused, never read as a shorter one | `shard::tests::a_damaged_delete_log_fails_the_open_rather_than_losing_a_deletion` (every truncation and every flipped bit of a published log fails the open naming the file), `mvcc::tests::a_framed_delete_log_refuses_every_truncation_and_every_flipped_byte` (the count and the checksum are separately load-bearing: a log that lost a record and was re-signed is refused by the count), `shard::tests::a_delete_log_written_before_the_frame_opens_and_is_rewritten_framed`, `mvcc::tests::a_delete_log_without_the_frame_still_decodes` (an existing database opens, and the next publication closes its unframed window) |
| the catalog counts every document once, however many times the directory is reopened | `engine::tests::a_reopen_with_an_unflushed_wal_counts_its_documents_once` (three claims, each the mutation that passes the others: right before any statement, unchanged across reopens with an unflushed WAL, and a record no persist ever saw is counted once) |
| the SELECT list decides what a row carries | `engine::tests::the_select_list_decides_what_a_row_carries` (a named path is kept and an unnamed one is not, an alias renames, a nested path is keyed as written, a missing path is `Null` rather than absent, `*` keeps everything, and a ranked query keeps its `score`) |
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
  serve.rs serve/                the local browser console, loopback-only
  bin/celastro.rs                REPL, script runner, demo
  bin/celastro-cli.rs            serve, exec, run, repl, demo, catalog
tests/
  integration.rs                 end-to-end behaviour
  tiering.rs                     tiers, residency, lifecycle policies
```
