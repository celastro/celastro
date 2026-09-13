# celastro

A hybrid document database. Structured SQL, BM25 full-text and vector similarity
are three first-class retrieval modes evaluated in a **single query plan**,
rather than orchestrated across separate services.

Written in Rust with **zero dependencies outside `std`** — no crates, no C
libraries. The bitmaps, term dictionary, block-max postings, quantizers, HNSW

```sh
cargo install celastro        # the `celastro` REPL and the `celastro-cli` tool
```

**Status.** Single node, with multiple shards in one process and explicit
key-range splits. Immutable segments, size-tiered compaction under a hard
segment cap, MVCC with snapshot reads, tiered vector indexes, runtime
filtered-search strategy selection, storage tiers with lifecycle policies, and
`EXPLAIN ANALYZE` over all of it. Replication, consensus and cross-shard
transactions are not here — see
[What is deliberately not here](https://github.com/celastro/celastro/blob/main/docs/design.md#what-is-deliberately-not-here).

Releases and what changes between them are in [CHANGELOG.md](https://github.com/celastro/celastro/blob/main/CHANGELOG.md).

---

## Quick start

Put this in `quickstart.sql`. Documents are JSON; the collection declares the
primary key and any typed columns, an index declares how a path is searched,
and the SELECT list picks the fields that come back.

```sql
CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT);
CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english');
CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine');

INSERT INTO notes VALUES ('{"id":"n1","topic":"search","body":"BM25 ranks documents by term frequency and document length","embedding":[0.9,0.1,0.0,0.0]}');
INSERT INTO notes VALUES ('{"id":"n2","topic":"search","body":"Vector search finds the nearest neighbours in embedding space","embedding":[0.1,0.9,0.0,0.0]}');
INSERT INTO notes VALUES ('{"id":"n3","topic":"storage","body":"An LSM tree seals a memtable into immutable segments","embedding":[0.0,0.0,0.9,0.1]}');
INSERT INTO notes VALUES ('{"id":"n4","topic":"storage","body":"Compaction merges segments and drops dead versions","embedding":[0.0,0.0,0.1,0.9]}');

-- a structured predicate
SELECT id, topic FROM notes WHERE topic = 'storage';
-- full text: text_match in WHERE is a must
SELECT id FROM notes WHERE text_match(body, 'segments');
-- nearest neighbours
SELECT id FROM notes ORDER BY embedding <=> [0.8,0.2,0.0,0.0] LIMIT 2;
-- a distance threshold is a filter; this one is exact match for cosine
SELECT id FROM notes WHERE embedding <=> [0.9,0.1,0.0,0.0] < 0.000001;
-- all three in one plan, fused with reciprocal rank fusion
SELECT id, topic FROM notes
ORDER BY hybrid(text_match(body, 'search documents'), embedding <=> [0.5,0.5,0.0,0.0], method => 'rrf')
LIMIT 3;
```

Run it against a directory, so the data is there next time:

```
$ celastro-cli --dir ./data run quickstart.sql
collection `notes` created with 1 shard(s)
index `notes_body` created on the active tier
index `notes_emb` created on the active tier
1 document(s) written at ts 7328898005277564928
...
key | id | topic
----+----+--------
n3  | n3 | storage
n4  | n4 | storage
2 row(s)
key | id
----+---
n3  | n3
n4  | n4
2 row(s)
key | distance | id
----+----------+---
n1  | 0.009008 | n1
n2  | 0.651813 | n2
2 row(s)
key | id
----+---
n1  | n1
1 row(s)
key | score    | id | topic
----+----------+----+--------
n1  | 0.032787 | n1 | search
n2  | 0.032258 | n2 | search
n3  | 0.015873 | n3 | storage
3 row(s)
```

`key` is the row's primary key and `score` or `distance` its rank, whichever
the query produced; the rest of the columns are the SELECT list. A distance in
`WHERE` selects rows and ranks nothing, in the same units the `distance`
column shows: for cosine an identical vector lands within floating-point
rounding of zero, so exact match is a small threshold; for L2 it is `<= 0`.

Every write is on the disk before it is acknowledged, so the collection is
there in the next process:

```
$ celastro-cli --dir ./data exec "SELECT * FROM notes WHERE text_match(body, 'compaction')"
key | body                                             | embedding         | id | topic
----+--------------------------------------------------+-------------------+----+--------
n4  | Compaction merges segments and drops dead versi… | [0.0,0.0,0.1,0.9] | n4 | storage
1 row(s)
```

`EXPLAIN ANALYZE` in front of any query prints the plan that ran: which shards
were scanned, which vector strategy was picked from the measured selectivity,
and where the fusion happened.

```
$ celastro-cli --dir ./data exec "EXPLAIN ANALYZE SELECT * FROM notes ORDER BY hybrid(text_match(body, 'search documents'), embedding <=> [0.5,0.5,0.0,0.0], method => 'rrf') LIMIT 3"
Query plan  (snapshot ts=7328898005345050624, limit=3, k'=100)
  scatter: 1 of 1 shard(s) scanned, 0 pruned by partition key
  term statistics: cached approximate
  shard 0 (manifest v0, 0.01 ms):
    memtable       docs=4       visible=4       survivors=4       s=1.0000  0.01 ms
      text[text(body)]: block-max WAND, terms=["search", "document"], candidates=2
      vector[vector(embedding)]: strategy=brute_force tier=Flat s=1.0000 survivors=4 ef=0 amp=0.0x reranked=4 reprobes=0
  fusion at coordinator: method=rrf sources=["text(body)", "vector(embedding)"] candidates=[2, 4] union=4
  fetch: 3 payload(s) from winning shards only, 0.00 ms
  total: 0.04 ms
```

`celastro-cli demo` builds a 400-document corpus across three shards and walks
through the same ideas at a size where the plan has choices to make. It runs in
memory and needs nothing.

---

## The command line

`celastro-cli` is the tool. Without `--dir` the database is in memory, which is
what `demo` needs and what makes `exec` usable with nothing on disk.

| command | what it does |
|---|---|
| `serve [--port N] [--open]` | the browser console, on `127.0.0.1` only |
| `exec <SQL>` | run one statement and print the result |
| `run <FILE>` | run a script of statements |
| `repl` | interactive session on stdin |
| `demo` | build a small hybrid corpus and show it working |
| `catalog` | list collections and their indexes |
| `health [--port N]` | exit 0 if a console is serving on that port; a container's probe |
| `version` | print the version |

| global flag | |
|---|---|
| `--dir <DIR>` | open a persistent database (default: in memory) |
| `--json` | machine-readable output on stdout, for every command, failures included |

Statements end with `;` or a blank line. Exit codes: 0 success, 1 a runtime or
SQL error, 2 a usage error.

`celastro` is the original REPL and script runner, kept because its interface
is in use: `celastro --dir ./data`, `celastro --file setup.sql`,
`celastro --demo`.

## The browser console

```
$ celastro-cli --dir ./data serve
celastro-cli serving on 127.0.0.1:8787 — Ctrl-C, SIGTERM or POST /api/shutdown to stop
The token in that URL is the only thing protecting this database. Anyone who can
read this terminal, this process's environment or its command line can use it, and
the server answers every request that carries it. Treat the URL as a password, and
stop the server when you are done.
http://127.0.0.1:8787/?t=fdd2b8856f798668b6f29478e4f1fd5b
```

Open the URL. The console runs SQL against the database, shows the catalog,
and renders `EXPLAIN ANALYZE` output. Only the URL is on stdout, so
`celastro-cli serve | xargs xdg-open` works, and `--open` does the same
without the pipe.

It binds loopback and nothing else, and it will not be talked into more: the
endpoint executes arbitrary SQL, so a bind reachable from the network is a
remote shell. Three guards sit in front of it — the loopback bind, a `Host`
check against DNS rebinding, and a per-run token — and every request needs the
token. `POST /api/shutdown` with the token stops the server after a clean
save. The console, like the CLI, saves after every statement that changed
something.

## Copying a collection

A collection is copied as of an instant, without stopping the source:

```
celastro-cli --dir ./data export notes ./notes-copy       # a database directory of its own
celastro-cli --dir ./elsewhere import ./notes-copy         # adopted beside what is there
```

The export pins a snapshot, copies the segment files it names, copies each
delete log as it stands, and seals the rows still in
memory into one segment of the copy's own; writes that land on the source
meanwhile do not reach it. The destination is written under a temporary name
and renamed into place at the end, so it is either absent or complete. The
copy opens as a database on its own, or `import` adopts it into another one.

## Kubernetes

`chart/celastro` is a Helm chart for one instance: a `StatefulSet` of one
pod with the data directory on a `PersistentVolumeClaim`, because celastro is
a single process and there is no cluster to scale. Its probes run the binary
itself, `celastro-cli health`, which asks the console inside the pod whether
it is serving and has its catalog. The console binds loopback, so it is
reached with `kubectl port-forward` and the URL printed in the pod's log. The
`archived` tier can be pointed at a bucket through the chart's `archive`
values, with the credentials in a `Secret`.

```
helm install celastro chart/celastro
kubectl logs celastro-0 | grep '^http'
kubectl port-forward celastro-0 8787:8787
```

The chart's README says what was verified and how.

## The archived tier and an object store

An index moved to the `archived` tier leaves local storage. By default that
means a directory beside the segments that stands in for object storage. Point
it at an S3-compatible store instead and the segment is `PUT` there as one
object, read back by ranged `GET`s when a query needs it, and deleted when a
compaction retires it:

```
export CELASTRO_ARCHIVE_ENDPOINT=127.0.0.1:9000   # host:port, plain HTTP
export CELASTRO_ARCHIVE_BUCKET=celastro
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...
celastro-cli --dir ./data serve
```

Any server that speaks S3's `PUT`, `GET`, `HEAD` and `DELETE` with Signature
Version 4 will do; MinIO is the one this was written against. The endpoint is
plain HTTP because the crate carries no TLS: run MinIO on the same host, or a
TLS-terminating proxy in front of a real bucket. The credentials are read from
the environment at open and never written anywhere.

## Running in a container

The `Dockerfile` builds a statically linked `celastro-cli` into an image
`FROM scratch`: no shell, no libc, nothing running as root.

```
docker build -t celastro .
docker run --rm celastro demo                                            # in memory
docker volume create celastro-data
docker run --rm -i -v celastro-data:/data celastro --dir /data repl < quickstart.sql
docker run --rm --network host -v celastro-data:/data celastro --dir /data serve
```

`serve` needs `--network host`, because a published port cannot reach a
loopback bind. It handles SIGTERM and SIGINT itself, so `docker stop` ends it
promptly with the database saved. Bind mounts, ownership, the REPL's stdin
behaviour and what each of those flags costs are in
[docs/container.md](https://github.com/celastro/celastro/blob/main/docs/container.md).

---

## What it is

Inside a segment every document has a compact `u32` ordinal, and every index
type — structured predicates, text matching, vector search, visibility —
produces sets in that same space. Hybrid candidate generation is therefore
bitmap intersection plus per-source scoring, with no joins and no identifier
translation. That is the mechanism behind the single-pass claim, and it is
treated as a load-bearing invariant.

Around it: immutable segments with a self-describing footer, size-tiered
compaction, MVCC with snapshot reads, a write-ahead log that is fsynced before
a write is acknowledged (the directory fsync behind that guarantee is a POSIX
operation, so off unix the guarantee is best-effort), block-max WAND for BM25,
a tiered HNSW index with SQ8
and 1-bit codes and full-precision rerank, runtime selection between brute
force, post-filter and filter-aware vector search, and storage tiers with
lifecycle policies.

What is deliberately not here: everything that needs more than one process.
Consensus and replication, follower reads, two-phase commit across shards,
dynamic shard split and merge, and a real object-store client for the
`archived` tier. The boundaries those attach to are built and tested; the
distributed pieces are not.

The whole of it — the ordinal-space invariant, where things live in the tree,
tiers and residency, the design notes with their measurements, the worked
query, and the map from each stated guarantee to the test that pins it — is in
[docs/design.md](https://github.com/celastro/celastro/blob/main/docs/design.md).

---

## Building and testing

```
cargo build --release
cargo test --release
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

No dependencies outside `std`, and Rust 1.75 or later. A change also has to
build on that floor and must not rewrite `Cargo.lock`. These are gates, but
nothing in this repository runs them for you.

Every test is named after the failure it prevents, not the feature it covers.
A test that passes with the behaviour it names removed is worse than no test.

## Contributing, security, licence

Issues are welcome and wanted; pull requests are not accepted, for reasons
[CONTRIBUTING.md](CONTRIBUTING.md) sets out. Vulnerabilities go through
[SECURITY.md](SECURITY.md), never a public issue.

[GNU Affero General Public License v3.0](LICENSE). You may use, modify,
self-host and redistribute this freely. If you run a modified version as a
network service, the AGPL requires you to offer that version's source to its
users, which is the reason for choosing it.
