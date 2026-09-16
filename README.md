# celastro

A hybrid document database: structured SQL, BM25 full-text, vector similarity
and a bounded graph walk are four retrieval modes evaluated in **one query
plan**, not orchestrated across services. Rust, **zero dependencies outside
`std`**: the bitmaps and postings, the HNSW index, the SQL front end and the
TLS are all in the tree.

```sh
cargo install celastro        # `celastro-cli`, and the older `celastro` REPL
```

**Status.** Single-writer nodes; a collection's shards can be spread over
nodes, moved between them, and any node coordinates a statement over all of
them. Immutable segments, MVCC snapshot reads, size-tiered compaction, tiered
vector indexes, storage tiers with lifecycle policies, and `EXPLAIN ANALYZE`
over all of it. No replication, consensus or cross-shard transactions — see
[What is deliberately not here](docs/design.md#what-is-deliberately-not-here).
Releases are in [CHANGELOG.md](CHANGELOG.md).

## Quick start

A server on a directory, and a client talking to it. Locally or in a
cluster, that is the whole loop.

```sh
cat > quickstart.sql <<'EOF'
CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT);
CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english');
CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine');
INSERT INTO notes VALUES ('{"id":"n1","topic":"search","body":"BM25 ranks documents by term frequency","embedding":[0.9,0.1,0.0,0.0]}');
INSERT INTO notes VALUES ('{"id":"n2","topic":"storage","body":"An LSM tree seals a memtable into segments","embedding":[0.0,0.0,0.9,0.1]}');
EOF
celastro-cli --dir ./data run quickstart.sql
celastro-cli --dir ./data serve
```

```
http://127.0.0.1:8787/?t=31beccfc...        # the console's URL; the token follows ?t=
```

From another terminal, with that token, one statement at a time or over
HTTP as an application would:

```sh
export CELASTRO_TOKEN=31beccfc...
celastro-cli send http://127.0.0.1:8787 "SELECT id FROM notes WHERE text_match(body, 'segments')"
curl -s -H "X-Celastro-Token: $CELASTRO_TOKEN" -H "Content-Type: application/json" \
     http://127.0.0.1:8787/api/query -d @- <<'EOF'
{"sql": "SELECT id, topic FROM notes ORDER BY hybrid(text_match(body, 'documents'), embedding <=> [0.8,0.2,0.0,0.0], method => 'rrf') LIMIT 3"}
EOF
```

```
{"ok":true,"kind":"rows","count":1,...,"rows":[{"key":"n2","score":null,"distance":null,"doc":{"id":"n2"}}]}
{"ok":true,"kind":"rows","count":2,...,"rows":[{"key":"n1","score":0.0328,"distance":null,"doc":{"id":"n1","topic":"search"}},{"key":"n2",...}]}
```

The second query is text and vector in one plan, fused by reciprocal rank
fusion; `WHERE` takes structured predicates, `text_match`, and a distance
threshold. `celastro-cli --dir ./data exec "<SQL>"` runs a statement without
a server, `EXPLAIN ANALYZE` in front of a query prints the plan that ran,
and `celastro-cli demo` is a guided tour in memory. Every write is on the
disk before it is acknowledged. Two limits worth knowing early: a prefix
such as `text_match(body, 'comp*')` expands to at most 512 dictionary terms
and says `TRUNCATED` when cut, and `DROP` is final.

## Counting and summing

```sql
SELECT count(*) FROM notes WHERE topic = 'storage';
SELECT topic, count(*) AS n, avg(words) FROM notes GROUP BY topic ORDER BY n DESC LIMIT 5;
```

`count(*)`, `count(path)`, `sum`, `min`, `max` and `avg` fold every row the
predicate admits -- text matches and distance thresholds included -- into
one row, or one per `GROUP BY` value; each shard folds its own rows and
the coordinator merges the partials, so a count never moves a document.
`ORDER BY`, `LIMIT` and `OFFSET` then apply to the groups, by the result's
fields. Nulls and absent paths are skipped by everything but `count(*)`.

## Walking a graph

An edge is a document in a collection of its own that points into a node
collection, and a walk is a filter beside the others, or a fusion source:

```sql
CREATE COLLECTION papers (id TEXT PRIMARY KEY);
CREATE INDEX papers_body ON papers USING fulltext (body);
CREATE INDEX papers_emb ON papers USING vector (embedding) WITH (dims = 4, metric = 'cosine');
CREATE COLLECTION cites (id TEXT PRIMARY KEY, src TEXT NOT NULL, dst TEXT NOT NULL)
  WITH (nodes_of = 'papers');
CREATE INDEX cites_adj ON cites USING adjacency (src, dst);

-- everything p1 cites, and what those cite, matching the text, nearest the vector
SELECT id FROM papers
WHERE id WITHIN 2 HOPS OF 'p1' VIA cites AND text_match(body, 'retrieval')
ORDER BY embedding <=> [0.5,0.5,0.0,0.0] LIMIT 10;
```


`WITHIN k HOPS OF` selects every node reachable in one to `k` hops, the start
excluded. `VIA cites` follows edges `src` → `dst`; `REVERSE` follows them the
other way, and a collection created `WITH (undirected = true)` follows both.
`VIA cites WHERE kind = 'cites'` filters the edges at every hop, `WHERE a THEN
WHERE b` gives each hop its own. `ORDER BY hybrid(text_match(body, 'x'),
hops(id WITHIN 3 HOPS OF 'p1' VIA cites))` ranks by hop distance beside the
other sources instead. `k` is required; `WITH (max_fanout = N)` and `WITH
(max_frontier = N)` bound what a hub can cost, and a cut says so on the
response and in the plan. The walk runs before the rest of the plan and the
neighbourhood joins the other sets as one more bitmap, so the answer is the
same at any number of shards or nodes. Not a graph database: no pattern
language, no unbounded paths, no analytics.

## The command line

`celastro-cli` is the tool. Without `--dir` the database is in memory.

| command | what it does |
|---|---|
| `serve [--port N] [--bind ADDR] [--open]` | the console: on loopback, or on a network with `--bind` |
| `exec <SQL>`, `run <FILE>`, `repl` | one statement, a script, an interactive session |
| `demo` | the guided tour, in memory |
| `catalog` | collections and their indexes |
| `health [--port N] [--attached N]` | exit 0 if a console is serving — and has verified `N` peers; a container's probes |
| `export <COLLECTION> <DIR>`, `import <DIR>` | copy a collection as of an instant, without stopping the source; adopt one |
| `send <URL> <SQL>` | one statement to a running console, the token from `CELASTRO_TOKEN` — what a backup CronJob runs |
| `version` | |

`--dir <DIR>` opens a persistent database; `--json` makes every command's
output machine-readable, failures included. Statements end with `;` or a
blank line. Exit codes: 0, 1 a runtime or SQL error, 2 a usage error.

## The console

`celastro-cli --dir ./data serve` prints a URL with a token on stdout and
serves a browser console on `127.0.0.1:8787`: SQL, the catalog, `EXPLAIN
ANALYZE` rendered. The endpoint executes arbitrary SQL, so on loopback three
guards sit in front of it — the bind, a `Host` check against DNS rebinding,
and a per-run token every request needs. `POST /api/shutdown` with the token
stops it after a clean save.

`serve --bind 0.0.0.0` puts it on a network, for nodes behind a Service or a
load balancer: the console then answers the token in `CELASTRO_TOKEN` (yours,
at least sixteen printable bytes, the same at every node), accepts whatever
`Host` routed to it, and requires a browser's `Origin` to be that host.
Connections are served at once, up to sixty-four, and reads run side by
side on every core; a statement that changes something runs alone, since
the engine is single-writer. `/api/health` names the node that answered.
A `serve` also compacts on its own — one job at a time, built outside the
lock, `CELASTRO_AUTO_COMPACT=off` to leave it to `COMPACT`.

## Encryption

In transit: `CELASTRO_TLS_CERT`, `CELASTRO_TLS_KEY` and `CELASTRO_TLS_CA`
(PEM) put the console and the wire on TLS 1.3; `celastro-cli tls init
./tls <name>` makes a set, and the chart's `tls.enabled` does it for you.
At rest: `celastro-cli key master ./master.key`, then
`CELASTRO_MASTER_KEY_FILE=./master.key` on every start, encrypts every file
under `--dir`, the archived tier, backups and exports; a cluster shares one
data key (`celastro-cli key init`, `CELASTRO_KEY_FILE`, the chart's
`encryption.existingSecret`). Both are written in this repository and
unaudited; [SECURITY.md](SECURITY.md) says what each protects and what it
does not.

## Two or more nodes

Each node is its own process and directory, started with an address and the
secret every node shares, serving its shards to the others:

```
CELASTRO_NODE=tcp://10.0.0.2:2352 CELASTRO_WIRE_TOKEN=... celastro-cli --dir ./data serve --shard-bind 0.0.0.0
```

```sql
ATTACH NODE 'tcp://10.0.0.3';               -- port 2352 unless given
CREATE COLLECTION notes (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)
  PARTITION BY (tenant) WITH (splits = ['m', 't']);   -- three shards, one per node
MOVE SHARD 1 OF notes TO 'tcp://10.0.0.3:2352';
REBALANCE notes;
```

Shard `i` goes to the `i`-th attached node, wrapping (or the `i`-th of `WITH
(nodes = [...])`). Every holder carries the same definition and placement,
so a statement issued at any of them reaches the right shards: writes are
forwarded to the owner and acknowledged after it acknowledged, queries fan
out and fuse where they arrived, DDL runs on every holder, and `LOCAL`
prefixes a statement to one node only. A node that does not answer is a
deadline at the coordinator, with `WITH (partial_results)` naming its shard;
a statement that never asks the lost node's shards — a key the predicate
pins to a live shard, a partition on one — answers without it, while a
text query, scored against every holder's statistics, needs them all. A
node that is coming back is dialled again for two seconds before it
counts as gone.
A shard moves without stopping the collection, its files being all that
crosses the wire.

## Kubernetes and containers

`chart/celastro` runs one pod or a cluster: a `StatefulSet` whose pods attach
each other, `console.expose` for a Service that spreads clients over the
pods, `tls.enabled` for certificates the chart makes once (or yours, or
cert-manager's), `archive.*` for an S3 bucket or an NFS claim behind the
`archived` tier, and `backup.schedule` for a CronJob that backs every pod
up to that claim or a bucket. Its [README](chart/celastro/README.md) records what was verified and
how.

```
helm install celastro chart/celastro --set replicas=3 --set console.expose=true --set tls.enabled=true
```

Each release publishes `ghcr.io/celastro/celastro:<version>`: a static
`celastro-cli` in an image `FROM scratch`, nothing running as root.

```
docker run --rm ghcr.io/celastro/celastro:0.38.0 demo
docker run --rm --network host -v celastro-data:/data ghcr.io/celastro/celastro:0.38.0 --dir /data serve
```

`serve` needs `--network host` (a published port cannot reach a loopback
bind) or `--bind` with a token; it handles SIGTERM, so `docker stop` ends it
saved. [docs/container.md](docs/container.md) has the rest: volumes and
ownership, the REPL's stdin, what each flag costs. One process per data
directory: `serve` holds `<dir>/LOCK` with `flock`, a second open is
refused naming the holder, and a crash releases it.

## The archived tier

An index moved to the `archived` tier leaves local storage — a directory
beside the segments by default, a directory on any mount named by
`CELASTRO_ARCHIVE_DIR` (an NFS volume is the case it was written for), or
an S3-compatible bucket named by `CELASTRO_ARCHIVE_ENDPOINT` (`host:port`,
plain HTTP), `CELASTRO_ARCHIVE_BUCKET` and the `AWS_*` credentials, read
from the environment at open and never written anywhere. Any store that
speaks S3's `PUT`, ranged `GET`, `HEAD`, `DELETE` and `ListObjectsV2` with
Signature Version 4 will do.

## Backups

```sql
BACKUP TO '/mnt/backups/nightly';          -- or 's3://bucket/prefix'
RESTORE FROM '/mnt/backups/nightly';       -- into an empty --dir; AS OF <ts> for an older one
```

`BACKUP TO` pins every shard this node holds at one instant and copies it
to a directory (any mount) or a bucket (through the archive's endpoint and
credentials, the bucket the destination names). Sealed segments go into a
pool under the destination once — a second backup copies only the segments
that are new — and each backup owns its catalog, manifests, delete logs and
the rows that were still in memory, plus a record naming every object it
needs. The copy runs after the statement let go of the node's lock, so
other statements are answered meanwhile. `RESTORE FROM` takes the newest
complete backup, or the one `AS OF` the instant `BACKUP` reported, verifies
every object is there at its recorded size, and only then writes; the
shards come back placed on the restoring node. Each node backs up under
its own name (`CELASTRO_NODE`, or `local`), so a cluster's pods share one
destination and each restores its own; `NODE '<address>'` takes another
node's. With `CELASTRO_BACKUP_DIR`
set, a bare name resolves under it and no path may leave it — what a
console reachable over a network should have. A cluster is backed up node
by node; `celastro-cli send <URL> "BACKUP TO '…'"` sends the statement to
a running console, which is what the chart's CronJob runs on every pod.

## How it works

Inside a segment every document has a compact `u32` ordinal, and every index
type — structured predicates, text, vectors, adjacency, visibility —
produces sets in that space. Hybrid candidate generation is bitmap
intersection plus per-source scoring, with no joins and no identifier
translation; that is the mechanism behind the single-plan claim, and it is
treated as a load-bearing invariant. Around it: immutable segments with a
self-describing footer, a write-ahead log fsynced before a write is
acknowledged (once per statement: a thousand documents in one `INSERT`
cost one sync), memtables sealed flat at the flat tier's size so a load
builds no graph (`COMPACT` does, later), block-max WAND for BM25, a tiered HNSW index with SQ8 and 1-bit
codes and full-precision rerank, runtime choice between brute force,
post-filter and filter-aware vector search, and a deterministic simulator
that puts partitions, crashes and reordering on the coordinator-to-shard
boundary. The whole of it, with its measurements and the map from each
guarantee to the test that pins it, is in [docs/design.md](docs/design.md).

## Tuning

Every performance knob is an environment variable read at start —
`CELASTRO_INSERT_BATCH` (documents per log sync in one `INSERT`, 1000),
the memtable and residency budgets, the deadline, compaction, the vector
build, the console's connection cap — and [docs/tuning.md](docs/tuning.md)
lists each with its default and when to move it. The chart sets them
through its `tuning:` map.

## Building and testing

```
cargo build --release
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

No dependencies outside `std`, Rust 1.75 or later, and a change must not
rewrite `Cargo.lock`. Every test is named after the failure it prevents.

## Contributing, security, licence

Issues are welcome; pull requests are not accepted, for reasons
[CONTRIBUTING.md](CONTRIBUTING.md) sets out. Vulnerabilities go through
[SECURITY.md](SECURITY.md), never a public issue.

[GNU Affero General Public License v3.0](LICENSE): use, modify, self-host and
redistribute it freely; run a modified version as a network service and the
AGPL requires you to offer that version's source to its users, which is why
it was chosen. Copyright (C) 2026 celastro; the notice is in
[COPYRIGHT](COPYRIGHT).
