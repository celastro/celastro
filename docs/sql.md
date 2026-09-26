# The statements

Every statement celastro accepts, with an example and what it answered,
grouped by what it is for. The examples are run by
[`scripts/examples.sh`](../scripts/examples.sh) on every release; the
commands that send them are in [docs/commands.md](commands.md), and the
reasons behind each one in [docs/design.md](design.md). Identifiers are
case-sensitive; keywords are not. A statement ends with `;` or a blank
line in a script or the REPL.

## Collections and indexes

A collection is documents (JSON objects) under a primary key. The
declared columns are the paths with a type or a constraint; every other
path a document carries is stored and searchable as well, its type
inferred (`SHOW CATALOG` shows what was inferred).

```sql
CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT, words INT);
CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL)
  WITH (nodes_of = 'notes');
CREATE COLLECTION events (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)
  PARTITION BY (tenant) WITH (splits = ['m', 't'], prefix_expansion = 1024);
```

```
collection `notes` created with 1 shard(s)
```

`PARTITION BY (col)` prefixes every key with that column, so a predicate
on it prunes shards; `splits = [...]` fixes the key ranges of the shards
(two splits, three shards) and cannot be changed later; `nodes = [...]`
names the nodes the shards go to (below); `replicas = n` is how many
copies each shard has, the holder and `n - 1` followers (two by
default; `ALTER COLLECTION ... SET (replicas = n)` re-plans them);
`regions = n` spreads those copies over at least `n` regions when the
nodes carry one (`CELASTRO_REGION`); `confirm = 'all' | 'quorum' |
'none'` is what acknowledges a write -- every live follower, a majority
of the copies (a follower away holds nothing, and with no majority the
write is refused, never acknowledged on one disk), or this disk alone;
unset is the node's `CELASTRO_REPLICATION`; `nodes_of` says an edge
collection points into a node collection; `undirected = true` follows
its edges both ways; `prefix_expansion` caps how many dictionary terms a
`text_match` prefix expands to (512).

Four kinds of index. A `fulltext` index takes an `analyzer`
(`standard`, `english`); a `vector` index needs `dims` and a `metric`
(`cosine`, `l2`, `dot`); a `secondary` index serves equality and range
predicates on a path; an `adjacency` index over `(src, dst)` serves the
graph walk. An index may be created on a `tier` other than `active`.

```sql
CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english');
CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine');
CREATE INDEX notes_topic ON notes USING secondary (topic);
CREATE INDEX cites_adj ON cites USING adjacency (src, dst);
```

```
index `notes_body` created on the active tier
```

```sql
ALTER COLLECTION cites SET (nodes_of = 'notes');
ALTER INDEX notes_topic ON notes SET TIER 'cached';
DROP INDEX notes_topic ON notes;
DROP COLLECTION cites;
```

```
collection `cites` its edges point into `notes`
index `notes_topic` moved active -> cached
index `notes_topic` dropped from `notes`
collection `cites` dropped
```

`DROP` is final. `SHOW CATALOG [collection]` is the definition, the
placement of every shard, and every path seen with its inferred type:

```
collection notes (pk=id, partition_by=None, docs=4, prefix_expansion=512)
  shard 0 on this node [, )
  index notes_body on body FullText { analyzer: "english" } tier=active
  index notes_emb on embedding Vector { dims: 4, metric: Cosine } tier=active
  path body                     Stable(Str) present=4 distinct≈4
  path topic                    Stable(Str) present=4 distinct≈3
```

## Writing

`INSERT` takes one JSON document per row; a thousand in one statement
cost one log sync. A document whose key exists replaces it. `DELETE`
needs a predicate: by key, or any `WHERE` a `SELECT` takes.

```sql
INSERT INTO notes VALUES
  ('{"id":"n1","topic":"search","words":7,"body":"BM25 ranks documents by term frequency","embedding":[0.9,0.1,0.0,0.0]}'),
  ('{"id":"n2","topic":"storage","words":8,"body":"An LSM tree seals a memtable into segments","embedding":[0.0,0.0,0.9,0.1]}');
DELETE FROM notes WHERE id = 'n4';
DELETE FROM notes WHERE topic = 'search';
```

```
2 document(s) written at ts 7331102756530368512
1 document(s) deleted
1 document(s) deleted
```

Every write is on disk before it is acknowledged. A `DELETE ... WHERE`
whose predicate is a cut prefix (`text_match(body, 'a*')` past the
expansion cap) is refused with nothing deleted, since it would not
delete what it describes.

## Reading

### Predicates

`=`, `<`, `>`, `<=`, `>=`, `!=`, `IN (...)`, `LIKE 'pre%'`, `IS [NOT]
NULL`, `AND`, `OR`, `NOT`, and `ANY(path) = value` over an array.

```sql
SELECT id, topic FROM notes WHERE topic = 'storage' ORDER BY id;
SELECT id FROM notes WHERE topic IN ('search', 'graphs') AND words > 6 AND NOT topic = 'graphs';
SELECT id FROM notes WHERE topic LIKE 'sea%' AND missing IS NULL;
```

```
key | id | topic
----+----+--------
n2  | n2 | storage
n3  | n3 | storage
2 row(s)
```

### Text

`text_match(path, 'query')` in `WHERE` is a must: every term has to
match. A trailing `*` is a prefix, a leading `-` excludes, quotes make a
phrase. Ranked by BM25 it goes inside `hybrid(...)` in `ORDER BY`.

```sql
SELECT id FROM notes WHERE text_match(body, 'segments memtable');
SELECT id FROM notes WHERE text_match(body, 'segment* -memtable');
SELECT id FROM notes ORDER BY hybrid(text_match(body, 'segments')) LIMIT 2;
```

```
key | score    | id
----+----------+---
n2  | 0.706918 | n2
n3  | 0.706918 | n3
2 row(s)
next cursor: #3f34f899|2|n3
```

`snippet(path, n)` in the select list is why a row matched: `n` of the
field's words around the first the query matched, placed to cover as
many matches as it can, each matched word in `<em>`, an ellipsis where
the window cuts. A prefix marks the words it expanded to; a negated
term marks nothing. The words are the index's -- what the analyzer
split, without the punctuation between -- so it is for showing beside a
result, not for quoting.

```sql
SELECT id, snippet(body, 6) FROM notes WHERE text_match(body, 'segment*');
```

```
key | id | snippet(body)
----+----+------------------------------------------------------
n2  | n2 | … the <em>segments</em> a seal writes and the …
```

### Vectors

`path <=> [..]` is the distance in the index's metric: in `ORDER BY` it
ranks, in `WHERE` with `< d` it is a threshold. `WITH (exact)` searches
brute force rather than through the graph; `WITH (ef_search = N)`
widens the graph search.

```sql
SELECT id FROM notes ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2;
SELECT id FROM notes WHERE embedding <=> [0.0,0.0,1.0,0.0] < 0.1;
SELECT id FROM notes ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2 WITH (exact);
```

```
key | distance | id
----+----------+---
n2  | 0.006116 | n2
n3  | 0.015268 | n3
2 row(s)
```

### Hybrid

`hybrid(source, source, ...)` fuses ranked sources -- a `text_match`, a
distance, a `hops(...)` walk -- with reciprocal rank fusion (`method =>
'rrf'`, the default) or a weighted sum of normalised scores (`method =>
'linear', weights => [...]`); `k => N` is how many candidates each
source contributes, over the whole collection whichever shards hold them
(the default is four times the `LIMIT`, at least 100).

```sql
SELECT id, topic FROM notes
ORDER BY hybrid(text_match(body, 'segments'), embedding <=> [0.0,0.0,1.0,0.0], method => 'rrf') LIMIT 3;
SELECT id FROM notes
ORDER BY hybrid(text_match(body, 'segments'), embedding <=> [0.0,0.0,1.0,0.0], method => 'linear', weights => [0.3, 0.7]) LIMIT 3;
```

```
key | score    | id | topic
----+----------+----+--------
n2  | 0.032787 | n2 | storage
n3  | 0.032258 | n3 | storage
n4  | 0.015873 | n4 | graphs
3 row(s)
```

### Graph walks

`id WITHIN k HOPS OF 'start' VIA edges` selects every node one to `k`
hops from the start, the start excluded, following `src → dst`;
`REVERSE` follows `dst → src`; `VIA edges WHERE ...` filters the edges,
`WHERE a THEN WHERE b` per hop. `hops(...)` ranks by hop distance inside
`hybrid`. `WITH (max_fanout = N, max_frontier = N)` bound what a hub can
cost; a cut is reported on the answer.

```sql
SELECT id FROM papers WHERE id WITHIN 2 HOPS OF 'p1' VIA cites AND text_match(body, 'retrieval') LIMIT 10;
SELECT id FROM papers WHERE id WITHIN 1 HOP OF 'p3' VIA cites REVERSE LIMIT 10;
SELECT id FROM papers
ORDER BY hybrid(text_match(body, 'retrieval'), hops(id WITHIN 2 HOPS OF 'p1' VIA cites)) LIMIT 10
WITH (max_fanout = 100, max_frontier = 1000);
```

### Aggregates

`count(*)`, `count(path)`, `sum`, `min`, `max`, `avg`, over every row
the predicate admits, in one row or one per `GROUP BY` value; `ORDER
BY`, `LIMIT` and `OFFSET` then apply to the groups. Each shard folds its
own rows and the coordinator merges, so a count moves no document.

```sql
SELECT count(*) FROM notes;          -- no predicate: the holders' live counts summed, no scan
SELECT topic, count(*) AS n, sum(words) AS words, avg(words) AS avg_words FROM notes GROUP BY topic ORDER BY n DESC LIMIT 5;
SELECT min(words), max(words) FROM notes WHERE words > 0;
```

```
key       | avg_words | n | topic   | words
----------+-----------+---+---------+------
"storage" | 7.0       | 2 | storage | 14
"graphs"  | 5.0       | 1 | graphs  | 5
"search"  | 7.0       | 1 | search  | 7
3 row(s)
```

### Facets

`FACET path[, path...] [TOP n]` after the trailing clauses answers, beside
the rows, each path's top `n` values (ten unless said) by count over
every row the predicate admits -- the candidate set, not the page -- the
most first and equal counts by value; a row without the path counts as
`null`. Each facet is one aggregate over the same shards, merged as a
`GROUP BY` is, so it costs a pass over the predicate per path. Not
beside an aggregate, which is already that count.

```sql
SELECT id FROM notes WHERE text_match(body, 'segments') LIMIT 2 FACET topic, kind TOP 3;
```

```
key | id
----+---
n2  | n2
n3  | n3
2 row(s)
facet topic: "storage" (2), "search" (1)
facet kind: "note" (3)
```

The console's JSON carries them as `facets`: `{"topic":[["storage",2],["search",1]]}`.

### Paging

`LIMIT n OFFSET m` on any ordering. `AFTER '<cursor>'` continues a
primary-key or ranked ordering from where the previous page ended: the
answer's `next cursor` (`next_cursor` in JSON), or a key.

```sql
SELECT id FROM notes ORDER BY id LIMIT 2 OFFSET 2;
SELECT id FROM notes LIMIT 2 AFTER 'n2';
SELECT id FROM notes ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2 AFTER '#bc7a2700|2|n3';
```

### Deadlines and partial answers

Every statement runs under a deadline (`CELASTRO_STATEMENT_DEADLINE_MS`,
30 s). `WITH (deadline_ms = N)` sets its own, `WITH (no_deadline)` lifts
it. A statement past its deadline is refused naming the budget; `WITH
(partial_results)` answers from the shards that made it and names the
ones that did not in `missing`.

```sql
SELECT id FROM notes LIMIT 1 WITH (deadline_ms = 5000);
SELECT count(*) FROM notes WITH (partial_results, deadline_ms = 3000);
```

```json
{"ok":true,"kind":"rows","count":1,"missing":["shard 1"],"rows":[{"key":"","doc":{"count(*)":2}}]}
```

### EXPLAIN

`EXPLAIN` in front of a `SELECT` prints the plan; `EXPLAIN ANALYZE` the
plan that ran with its timings: what each shard scanned, which index
served which predicate, the vector strategy, the fusion, the fetch. A
statement whose every shard is on one other node goes there whole, as
one call, and the plan says `forwarded whole to tcp://... as one call`
above the plan as that node rendered it.

```sql
EXPLAIN ANALYZE SELECT id FROM notes WHERE topic = 'storage' ORDER BY embedding <=> [0.0,0.0,1.0,0.0] LIMIT 2;
```

```
Query plan  (snapshot ts=7331102758246993920, limit=2, k'=10, deadline=30000 ms)
  scatter: 1 of 1 shard(s) scanned, 0 pruned by partition key
  term statistics: cached approximate
  shard 0 (manifest v0, 0.03 ms):
    memtable       docs=4       visible=4       survivors=2       s=0.5000  0.02 ms
      filter: topic = "storage" [variant decode]
      vector[vector(embedding)]: strategy=brute_force tier=Flat s=0.5000 survivors=2 ef=0 amp=0.0x visits=0 reranked=2 reprobes=0
  fetch: 2 payload(s) from winning shards only, 0.01 ms
  total: 0.05 ms
```

## Storage and maintenance

`FLUSH` seals the memtables into segments; `COMPACT` merges segments
(a `serve` does both on its own; `CELASTRO_AUTO_COMPACT=off` leaves
compaction to the statement). `SHOW SEGMENTS` is what is on disk.

```sql
FLUSH notes;
SHOW SEGMENTS notes;
COMPACT notes;
```

```
1 shard(s) flushed
shard  segment  level  docs      vectors   dead
0      1        0      4         4         0.0%
0      memtable -      0         0         -
0 compaction job(s) run
```

Indexes live on tiers -- `active` (always in memory), `minimal`,
`cached` (unloaded when idle), `archived` (a directory or an S3 bucket,
faulted in on use) -- moved by `ALTER INDEX ... SET TIER` or by a
lifecycle policy. `SHOW RESIDENCY` is what is in memory now, against the
budget; `UNLOAD IDLE` releases what is idle or over budget.

```sql
SHOW RESIDENCY notes;
UNLOAD IDLE ON notes;
CREATE LIFECYCLE POLICY cool ON notes FOR (notes_emb)
  MOVE TO cached AFTER 30 minutes OF INACTIVITY,
  MOVE TO archived AFTER 7 days SINCE CREATION;
SHOW LIFECYCLE;
RUN LIFECYCLE;
DROP LIFECYCLE POLICY cool;
```

```
resident 0 B of 4.0 GiB budget (peak 0 B), 0 load(s), 0 unload(s), 0 fault(s) from archive
released 0 B idle, 0 B over-budget; 0 B resident of 4.0 GiB budget
lifecycle policy `cool` created
CREATE LIFECYCLE POLICY cool ON notes FOR (notes_emb)
  MOVE TO cached AFTER 30 minutes OF INACTIVITY,
  MOVE TO archived AFTER 7 days SINCE CREATION;
    notes_emb            tier=active    declared=active    idle=0.2 seconds    age=0.7 seconds
no index is due to move
lifecycle policy `cool` dropped
```

`RUN LIFECYCLE` applies the policies now; `CELASTRO_LIFECYCLE_INTERVAL_WRITES`
runs them every so many writes. `MEASURE RECALL ON c WITH (k = N,
samples = N)` measures the vector index's recall against brute force,
replaying sampled production queries where it has them:

```
recall@2 on notes.embedding: mean 1.0000, worst 1.0000 over 2 sample(s) (0 replayed from the production query log)
```

## Backups

`BACKUP TO '<dir or s3://bucket/prefix>'` pins every shard this node
holds at one instant and copies it, with the lock let go; a second
backup copies only new segments. `KEEP n` removes older backups after
the copy. `VERIFY BACKUP` reads every object back against the record.
`RESTORE FROM` into an empty `--dir` takes the newest complete backup,
or the one `AS OF` the instant a backup reported; `NODE '<address>'`
takes another node's.

```sql
BACKUP TO '/mnt/backups' KEEP 7;
VERIFY BACKUP '/mnt/backups';
RESTORE FROM '/mnt/backups';
RESTORE FROM '/mnt/backups' AS OF 7331102761890652160 NODE 'tcp://10.0.0.2:7876';
```

```
backup 7331102761890652160 to /mnt/backups: 1 collection(s), 1 shard(s), 0 segment(s) copied (0 bytes), 1 already there; kept 7, removed 0 older backup(s) and 0 pool segment(s) nobody references
verified backup 7331102761890652160 of node `local` at /mnt/backups: 5 object(s), 8654 bytes, every one as recorded
restored backup 7331102761890652160 of node `local` from /mnt/backups: 1 collection(s), 1 shard(s), 3228 bytes
```

A cluster is backed up node by node, each under its own name; `BACKUP
CLUSTER TO` sent to one node backs every data node up at one instant,
so the set restores to one cut with `RESTORE FROM ... AS OF <that
instant>` on each. Each peer's copy is started detached and polled
until it is done; a peer that cannot be reached within fifteen seconds,
whose copy fails, or that falls silent for a minute while copying is
named `NOT on` in the answer, and the others' backups stand -- a set
with a node missing is not one cut, and the answer says which. `BACKUP
TO ... AS OF <instant> DETACHED` is the form a peer is sent (its copy on
a thread of its own, the statement answering at once) and `BACKUP
STATUS <instant>` says how it went there:

```sql
BACKUP CLUSTER TO '/mnt/backups';
```

```
backup 7331102771519565824 to /mnt/backups: 1 collection(s), 1 shard(s), ...; at the same instant 7331102771519565824 on tcp://127.0.0.1:7878: backup ..., on tcp://127.0.0.1:7879: backup ...
```

`CELASTRO_BACKUP_DIR` makes a bare name resolve under it and refuses a
path that leaves it.

**Any instant.** A backup is exact at its instant. With
`CELASTRO_LOG_ARCHIVE=<dir or s3://bucket/prefix>` every held shard's
write-ahead log is copied there as a seal rotates it -- before the seal
removes it, and a seal the archive refuses is tried again with the log
still on the disk -- and `BACKUP LOG TO '<the same>'` copies the live
logs now (a job every minute is a one-minute point). `RESTORE FROM '<the
same>' AS OF <any instant>` then takes the newest backup at or before it,
replays the archived logs up to it, and says how far it reached: the
instant asked, or where the archive ends, or a gap where a log was never
archived. The logs are under `nodes/<node>/logs/<collection>/shard-NNNN/`,
named by their rotation number and the instants they span. A restore
forks a timeline of its own for every shard it opens: what the restored
node writes is archived under it (`0001-<number>-...`) beside the run's
it left, the fork's instant is recorded (`timeline-0001`), and a later
restore follows the chain of timelines, reading the run left behind only
up to the fork. An archive written before 0.81.0 reads as timeline 0;
a node before 0.81.0 restoring from one written after reaches the
instants before the first fork.

```sql
BACKUP LOG TO '/mnt/backups';
RESTORE FROM '/mnt/backups' AS OF 7331102771519565824;
```

```
archived the live log of 1 shard(s) to /mnt/backups (0 empty), reaching 7331102771519565824
restored backup 7331102761890652160 of node `local` from /mnt/backups: 1 collection(s), 1 shard(s), 3228 bytes; 2 archived log(s) replayed, reaching 7331102771519565824
```

## Two or more nodes

`ATTACH NODE` makes a peer known (or `CELASTRO_ATTACH` at start);
`DETACH NODE` forgets one, refusing while it holds shards. A collection
created with more shards than one goes shard `i` to the `i`-th attached
node, wrapping, or to `WITH (nodes = [...])`, and the next node follows
it: a copy fed by the holder's log, on which every write is confirmed
before the client hears of it. Every holder and follower carries the
definition and the placement, so any node coordinates any statement.

```sql
ATTACH NODE 'tcp://127.0.0.1:7878';
CREATE COLLECTION notes (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)
  PARTITION BY (tenant) WITH (splits = ['m', 't']);
CREATE INDEX notes_body ON notes USING fulltext (body);
SHOW HEALTH;
```

```
node tcp://127.0.0.1:7878 attached
collection `notes` created with 3 shard(s) on tcp://127.0.0.1:7877, tcp://127.0.0.1:7878, tcp://127.0.0.1:7879
index `notes_body` created on the active tier; and on tcp://127.0.0.1:7878, tcp://127.0.0.1:7879
this node: tcp://127.0.0.1:7877, data, celastro 0.84.1, 1 collection(s), 1 shard(s) held, directory present
node tcp://127.0.0.1:7878: up, data, celastro 0.84.1, 0 ms, clock -0.0 s
node tcp://127.0.0.1:7879: up, data, celastro 0.84.1, 0 ms, clock -0.0 s
shard 0 of `notes`: on tcp://127.0.0.1:7877, reachable
shard 1 of `notes`: on tcp://127.0.0.1:7878, reachable
shard 2 of `notes`: on tcp://127.0.0.1:7879, reachable
steward: tcp://127.0.0.1:7877 (this node); automatic failover off
shard 0 of `notes`: follower tcp://127.0.0.1:7878 live, confirmed to ts 7331216705097039872, 0 behind
follows shard 2 of `notes` at term 0: caught up to ts 7331216705237512192
3 of 3 node(s) answer; 0 shard(s) unreachable
```

`SHOW HEALTH` names every node with its role, whether it answers, its
clock against this one, whether an older process still answers at its
address, every shard with whether its holder does, the steward, every
follower of a shard held here with its state (`live`, `catching up`,
`asking` -- and `DEGRADED` when a follower away leaves a write on this
disk alone), and every copy this node follows.

`PROMOTE SHARD i OF c ON 'follower'` makes a follower the holder at the
next term -- what to run when a holder is lost, from any node; the
follower answers every acknowledged row -- and the old holder, back,
demotes its copy and follows. A copy that is not caught up is refused
-- promoted, what it lacks would be lost -- unless the statement ends
in `FORCE`, which takes it as it is. With `CELASTRO_AUTO_FAILOVER=on`
the steward does it once a holder has missed two sweeps, caught-up
copies only.

```sql
PROMOTE SHARD 1 OF notes ON 'tcp://127.0.0.1:7879';
```

```
shard 1 of `notes` promoted here at term 1 (was on tcp://127.0.0.1:7878); tcp://127.0.0.1:7878 follow it; map switched here and on tcp://127.0.0.1:7877; not on tcp://127.0.0.1:7878: ...
```

`REPLACE COPY OF SHARD i OF c ON 'lost' WITH 'node'` is for a follower
that is not coming back: the copy on `lost` is struck from the map at
the next term and `node` -- any attached data node that neither holds
nor follows the shard -- follows in its place, shipped from nothing by
the holder until it is caught up. The lost node, back, takes the map at
the higher term and drops the copy it kept. With
`CELASTRO_AUTO_FAILOVER=on` the steward does it once a follower has
been away for `CELASTRO_REPLACE_SECS` (ten minutes), placing the copy
where the collection's `regions` ask, else in the holder's region.

```sql
REPLACE COPY OF SHARD 1 OF notes ON 'tcp://127.0.0.1:7878' WITH 'tcp://127.0.0.1:7877';
```

```
shard 1 of `notes`: the copy on tcp://127.0.0.1:7878 is replaced by one on tcp://127.0.0.1:7877 at term 2; the holder ships it from nothing; map switched here and on tcp://127.0.0.1:7877; not on tcp://127.0.0.1:7878: ...
```

`MOVE SHARD` carries a shard to another node without stopping the
collection: the source pins it (writes to it are refused meanwhile,
naming the move), the target pulls the files and switches the map on
every node. `SPLIT SHARD i OF c AT 'key'` makes two shards of one:
shard `i` keeps the keys below `key` and a new shard, the next index,
takes the rest on the same node, no row moving -- the remedy for a hot
shard, which a move can only relocate; `MOVE SHARD` then spreads it.
Without `AT` the holder splits at the middle of the shard's keys and
the answer names it. The rows a split leaves outside a range stay on
disk, invisible and counted as dead, until the next compaction. `MERGE
SHARDS a AND b OF c` is the way back: two adjacent shards on one node
become one, shard `b`'s rows rebuilt into shard `a` (name the larger
first) and `b`'s entry left as a marker that owns no key, so nothing
renumbers. `REBALANCE` is the moves that put shard `i` on the `i`-th
node. `PLACE SHARD` writes a map entry without moving data, the repair
for a node the switch did not reach; `LOCAL` in front of a definition
or a placement statement applies it to the node it reaches and carries
it nowhere (a query always reads every shard, wherever it is).

```sql
MOVE SHARD 2 OF notes TO 'tcp://127.0.0.1:7877';
REBALANCE notes;
SPLIT SHARD 1 OF notes AT 'p';
MOVE SHARD 3 OF notes TO 'tcp://127.0.0.1:7877';
MERGE SHARDS 1 AND 3 OF notes;
MOVE SHARD 3 OF notes TO 'tcp://127.0.0.1:7878';
MERGE SHARDS 1 AND 3 OF notes;
LOCAL PLACE SHARD 0 OF notes ON 'tcp://127.0.0.1:7877';
DETACH NODE 'tcp://127.0.0.1:7879';
```

```
shard 2 of `notes` moved from tcp://127.0.0.1:7879 to tcp://127.0.0.1:7877; 3 file(s), map switched here and on tcp://127.0.0.1:7878, tcp://127.0.0.1:7879
shard 2 of `notes` moved from tcp://127.0.0.1:7877 to tcp://127.0.0.1:7879; 3 file(s), map switched here and on tcp://127.0.0.1:7878, tcp://127.0.0.1:7877
shard 1 of `notes` split at 'p': shard 3 is [p, t) on this node, 3 file(s); map switched here and on tcp://127.0.0.1:7877, tcp://127.0.0.1:7879
shard 3 of `notes` moved from tcp://127.0.0.1:7878 to tcp://127.0.0.1:7877; 3 file(s), map switched here and on tcp://127.0.0.1:7879, tcp://127.0.0.1:7878
shards 1 and 3 of `notes` are on different nodes (tcp://127.0.0.1:7878 and tcp://127.0.0.1:7877); bring them together first: MOVE SHARD 3 OF notes TO 'tcp://127.0.0.1:7878'
shard 3 of `notes` moved from tcp://127.0.0.1:7877 to tcp://127.0.0.1:7878; 3 file(s), map switched here and on tcp://127.0.0.1:7879, tcp://127.0.0.1:7877
shards 1 and 3 of `notes` merged: shard 1 is [m, t) on this node, 0 row(s) of shard 3 rebuilt into it, shard 3 owns no key; map switched here and on tcp://127.0.0.1:7877, tcp://127.0.0.1:7879
shard 0 of `notes` placed on tcp://127.0.0.1:7877
node tcp://127.0.0.1:7879 holds 1 shard(s); move them first: MOVE SHARD 2 OF notes TO 'tcp://127.0.0.1:7877'
```

A split and a merge work on one node as well: `SPLIT SHARD 0 OF notes
AT 'n3'` on the single-node `notes` above answers `shard 1 is [n3, ) on
this node`, `SHOW CATALOG notes` then shows `shard 0 on this node [,
n3)` and `shard 1 on this node [n3, )`, and `MERGE SHARDS 0 AND 1 OF
notes` answers `shard 0 is [, ) on this node, 2 row(s) of shard 1
rebuilt into it, shard 1 owns no key`.

A node that does not answer is a deadline at the coordinator, and
`WITH (partial_results)` names its shards in `missing` (above). A
definition made while a node could not be reached succeeds with a note
naming it, and the node adopts it when it is back or within the next
sweep (`CELASTRO_RECONCILE_SECS`).
