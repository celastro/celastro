# The command line and the console's API

Every command `celastro` has, with an example and what it printed. The
examples are run by [`scripts/examples.sh`](../scripts/examples.sh)
against a built binary, so they are current for the release they ship
with; the SQL each one sends is in [docs/sql.md](sql.md). Timings vary and
are shown as they came.

`celastro` is one binary: the server, the client and the tools. Without
`--dir` the database is in memory and gone when the command ends; with
`--dir <DIR>` it is on disk, one process at a time (`<DIR>/LOCK`). With
`--url <URL>` the command is a client of a console that is already
serving, wherever it is.

## One node, no server

**`run <FILE>`** runs a script of statements against `--dir`, no server
involved. Statements end with `;` or a blank line.

```sh
cat > quickstart.sql <<'SQL'
CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT, words INT);
CREATE INDEX notes_body ON notes USING fulltext (body) WITH (analyzer = 'english');
CREATE INDEX notes_emb ON notes USING vector (embedding) WITH (dims = 4, metric = 'cosine');
INSERT INTO notes VALUES
  ('{"id":"n1","topic":"search","words":7,"body":"BM25 ranks documents by term frequency","embedding":[0.9,0.1,0.0,0.0]}'),
  ('{"id":"n2","topic":"storage","words":8,"body":"An LSM tree seals a memtable into segments","embedding":[0.0,0.0,0.9,0.1]}');
SQL
celastro --dir ./data run quickstart.sql
```

```
collection `notes` created with 1 shard(s)
(10.41 ms)
index `notes_body` created on the active tier
(2.41 ms)
index `notes_emb` created on the active tier
(2.39 ms)
2 document(s) written at ts 7331102756530368512
(1.43 ms)
```

**`exec <SQL>`** runs one statement. Rows come as a table whose first
column is the row's key; a ranked query adds `score` or `distance`.

```sh
celastro --dir ./data exec "SELECT id, topic FROM notes WHERE topic = 'storage' ORDER BY id"
```

```
key | id | topic
----+----+--------
n2  | n2 | storage
1 row(s)
(5.00 ms)
```

A statement that opens with a SQL comment goes after a bare `--`:
`celastro exec -- "-- a comment first
SELECT 1"`.

**`--json`** makes every command's output one JSON object, failures
included (`{"ok":false,"error":...}` on stdout), so a pipeline never has
to read stderr:

```sh
celastro --json --dir ./data exec "SELECT id FROM notes ORDER BY id LIMIT 1"
```

```json
{"count":1,"cut_walks":[],"elapsed_ms":2,"kind":"rows","missing":[],"next_cursor":null,"ok":true,"rows":[{"distance":null,"doc":{"id":"n2"},"key":"n2","score":null}],"truncated_prefixes":[]}
```

**`repl`** is the same on stdin, `\h` for help, `exit` to leave.
**`catalog`** lists the collections and their indexes:

```
notes  4 document(s)
  primary key   id
  partition by  (none)
  index notes_body on body — fulltext, analyzer english, tier active
  index notes_emb on embedding — vector, 4 dims, cosine, tier active
```

**`demo`** builds a small hybrid corpus in memory and walks through a
vector query, a flush, `SHOW SEGMENTS`, an `EXPLAIN ANALYZE` of a
hybrid query, `MEASURE RECALL`, `COMPACT` and `SHOW RESIDENCY`, each
statement printed before its answer. **`version`** prints the version;
**`help`** prints every command and flag with the environment variables
each reads.

## Copying a collection

**`export <COLLECTION> <DIR>`** copies one collection, as of the instant
the command starts, into a new database directory, without stopping the
source; **`import <DIR>`** adopts it into another database.

```sh
celastro --dir ./data export notes ./exp
celastro --dir ./data2 import ./exp
celastro --dir ./data2 exec "SELECT count(*) FROM notes"
```

```
exported `notes` at ts 7331102761147289600 to ./exp
imported `notes` from ./exp
key | count(*)
----+---------
    | 2
```

Backups (`BACKUP TO`, `RESTORE FROM`, `VERIFY BACKUP`) are statements,
in [docs/sql.md](sql.md#backups).

## Encryption at rest

Off until a master key is given. **`key master <FILE>`** writes one
(64 hex digits, readable by you only); `CELASTRO_MASTER_KEY_FILE` on
every start then encrypts every file under `--dir`, and every backup and
export it writes. A database made with the key is refused without it.

```sh
celastro key master ./master.key
CELASTRO_MASTER_KEY_FILE=./master.key celastro --dir ./enc exec "CREATE COLLECTION s (id TEXT PRIMARY KEY)"
celastro --dir ./enc exec "SELECT id FROM s"
```

```
wrote a master key to ./master.key
collection `s` created with 1 shard(s)
could not open ./enc: storage error: ./enc is encrypted; set CELASTRO_MASTER_KEY_FILE (or CELASTRO_MASTER_KEY) to open it
```

A cluster shares one data key: **`key init <FILE>`** writes it wrapped
under the master, `CELASTRO_KEY_FILE` names it at every node's first
start (the chart's `encryption.existingSecret`), and **`key rekey <KEY>
<MASTER>`** rewraps it under a new master when the master rotates:

```sh
CELASTRO_MASTER_KEY_FILE=./master.key celastro key init ./data.key
celastro key master ./master2.key
CELASTRO_MASTER_KEY_FILE=./master.key celastro key rekey ./data.key ./master2.key
```

```
wrote a data key, wrapped under the master key, to ./data.key
wrote a master key to ./master2.key
./data.key is now wrapped under the master key in ./master2.key
```

The data key itself rotates with **`key rotate <DIR>`**: every file and
every log record under the directory is sealed again under a fresh data
key and `KEY` rewrapped, with no process serving the directory (a
cluster rotates node by node, each stopped for its turn -- the nodes
no longer need to share one data key once they hold their own copies).
The new key goes to `KEY.next` first, so a rotation cut short is
finished by running it again, and a node refuses to open a directory
with a `KEY.next` until then. Backups and exports made before carry
their own `KEY` and open as they did; an index at the archived tier is
refused (its objects are under the old key), move it back first.
**`check <DIR>`** opens every frame of every file and names what does
not open, with nothing written: what to run on a volume you doubt.

```sh
CELASTRO_MASTER_KEY_FILE=./master.key celastro key rotate ./data
CELASTRO_MASTER_KEY_FILE=./master.key celastro check ./data
```

```
./data: 14 file(s) and 120 log record(s) sealed under a new data key; KEY rewrapped
./data: 14 file(s) and 120 log record(s) open under the data key; nothing is damaged
```

## TLS

**`tls init <DIR> <NAME> [<NAMES>] [<DAYS>]`** writes a CA and a
certificate for `NAME` (and the comma-separated `NAMES`, DNS names or IP
addresses), valid `DAYS` days:

```sh
celastro tls init ./tls localhost 127.0.0.1 365
```

```
wrote ca.crt, ca.key, tls.crt and tls.key into ./tls: a certificate for localhost, 127.0.0.1 valid 365 days, signed by a CA of its own
```

`CELASTRO_TLS_CERT`, `CELASTRO_TLS_KEY` and `CELASTRO_TLS_CA` on a
`serve` put the console and the wire on TLS 1.3; a client verifies with
`CELASTRO_TLS_CA`. **`tls secret <SECRET> <NAME>`** writes the same
material as a Kubernetes Secret from inside a pod, which the chart's
`tls.enabled` runs. [SECURITY.md](../SECURITY.md) has what each protects.

## A served console

**`serve`** prints its URL, token included, on stdout before it serves;
the notes go to stderr. On loopback the token is per run; with `--bind`
the console is on a network and answers the token in `CELASTRO_TOKEN`
(at least sixteen printable bytes, the same at every node).

```sh
export CELASTRO_TOKEN=examples-token-0123456789abcdef
celastro --dir ./data serve --bind 127.0.0.1 --port 18787 &
```

```
http://127.0.0.1:18787/?t=examples-token-0123456789abcdef
```

`--port 0` asks for a free port, `--open` opens a browser, and under
`--json` the first line is `{"url":...,"addr":...,"token":...}`.
`CELASTRO_LOG=json` makes the log lines JSON. A server saves after every
statement that changed something and again when it stops.

**`health [--port N] [--attached N]`** exits 0 when a console is serving
on loopback -- and, with `--attached N`, has verified `N` other nodes
since it started. A container's liveness and readiness probes run it.

```sh
celastro health --port 18787
```

```
serving on 127.0.0.1:18787
```

**`send <URL> <SQL>`** sends one statement to a console and prints its
JSON answer; exit 1 when the answer says `ok:false`. `https://` verifies
with `CELASTRO_TLS_CA`. What a backup CronJob runs.

```sh
celastro send http://127.0.0.1:18787 "SELECT id FROM notes ORDER BY id LIMIT 1"
```

```json
{"ok":true,"kind":"rows","count":1,"elapsed_ms":0,"missing":[],"truncated_prefixes":[],"cut_walks":[],"next_cursor":null,"rows":[{"key":"n2","score":null,"distance":null,"doc":{"id":"n2"}}]}
```

**`--url <URL>`** makes `exec`, `run`, `repl` and `catalog` clients of
that console, rendered as they would be locally; the token is
`CELASTRO_TOKEN` or the `?t=` of the URL `serve` printed, so that line
can be pasted as it is:

```sh
celastro --url http://127.0.0.1:18787 exec "SELECT id FROM notes ORDER BY id LIMIT 1"
celastro --url "http://127.0.0.1:18787/?t=examples-token-0123456789abcdef" catalog
printf 'SELECT count(*) FROM notes;\n' | celastro --url http://127.0.0.1:18787 repl
```

```
key | id
----+---
n2  | n2
1 row(s)
(0 ms at the console)
```

Over TLS, the same with the certificate's names:

```sh
CELASTRO_TLS_CERT=./tls/tls.crt CELASTRO_TLS_KEY=./tls/tls.key CELASTRO_TLS_CA=./tls/ca.crt \
  celastro --dir ./data serve --bind 127.0.0.1 --port 18788 &
CELASTRO_TLS_CA=./tls/ca.crt celastro send https://127.0.0.1:18788 "SELECT id FROM notes LIMIT 1"
```

## The console's HTTP API

Every request but `/api/health` carries the token in the
`X-Celastro-Token` header (on loopback, `?t=` in the URL is accepted
too). `POST /api/query` takes `{"sql": "..."}` and answers the same JSON
`send` prints; an error is `{"ok":false,"error":"..."}` with status 200,
so a client reads `ok`.

```sh
T="X-Celastro-Token: $CELASTRO_TOKEN"
curl -s -H "$T" -H "Content-Type: application/json" http://127.0.0.1:18787/api/query -d '{"sql": "SELECT count(*) FROM notes"}'
curl -s http://127.0.0.1:18787/api/health
curl -s -H "$T" http://127.0.0.1:18787/api/catalog
curl -s -H "$T" http://127.0.0.1:18787/api/metrics
curl -s -X POST -H "$T" http://127.0.0.1:18787/api/shutdown
```

```json
{"ok":true,"kind":"rows","count":1,"elapsed_ms":0,"missing":[],"truncated_prefixes":[],"cut_walks":[],"next_cursor":null,"rows":[{"key":"","score":null,"distance":null,"doc":{"count(*)":2}}]}
{"ok":true,"name":"celastro","version":"0.55.0","source":"https://github.com/celastro/celastro/tree/v0.55.0","license":"AGPL-3.0-only","copyright":"Copyright (C) 2026 celastro","collections":1,"node":null,"attached":0}
{"ok":true,"collections":[{"name":"notes","primary_key":"id","partition_key":null,"doc_count":4,"indexes":[{"name":"notes_body","path":"body","kind":"fulltext","tier":"active"},...]}]}
{"ok":true,"kind":"ack","message":"shutting down"}
```

`/api/health` needs no token: `ok:true` says the process answers,
`attached` how many peers it has verified, and `busy:true` that the
database lock was held when it was asked (alive, not ready). It says
`ok:false` when the data directory is gone. `/api/metrics` is the text
format a Prometheus scraper reads: statements and their time, refusals,
compactions, connections, reconciliations, backpressure, TLS
resumptions, the certificate's expiry, and per collection the shards,
segments and documents held here and each shard's reads and writes:

```
celastro_statements_total 5
celastro_statement_seconds_max 0.000318
celastro_compactions_total 1
celastro_attached_nodes 0
celastro_shards{collection="notes"} 1
celastro_documents{collection="notes"} 2
celastro_shard_reads_total{collection="notes",shard="0"} 4
celastro_shard_writes_total{collection="notes",shard="0"} 0
```

`POST /api/shutdown` stops the server after a clean save.

## Two or more nodes

A node is a `serve` with an address, the wire's shared secret, and its
shards served to the others; three on one machine, for the examples:

```sh
export CELASTRO_TOKEN=examples-token-0123456789abcdef
export CELASTRO_WIRE_TOKEN=examples-wire-token-0123456789
for i in 1 2 3; do
  CELASTRO_NODE=tcp://127.0.0.1:2352$i celastro --dir ./node$i serve \
    --bind 127.0.0.1 --port $((18788 + i)) --shard-bind 127.0.0.1:2352$i &
done
celastro send http://127.0.0.1:18789 "ATTACH NODE 'tcp://127.0.0.1:23522'"
celastro send http://127.0.0.1:18789 "ATTACH NODE 'tcp://127.0.0.1:23523'"
celastro health --port 18789 --attached 2
```

```
{"ok":true,"kind":"ack","message":"node tcp://127.0.0.1:23522 attached"}
{"ok":true,"kind":"ack","message":"node tcp://127.0.0.1:23523 attached"}
serving on 127.0.0.1:18789
```

On real hosts, `--bind 0.0.0.0 --shard-bind 0.0.0.0`, `CELASTRO_NODE`
the host's own address, and `CELASTRO_ATTACH=tcp://a,tcp://b,tcp://c`
(the same list on every node; its own address is skipped) instead of
`ATTACH NODE` by hand. `CELASTRO_ROLE=coordinator` makes a node that
holds no shards and only coordinates. `CELASTRO_REPLICATION=async`
acknowledges a write before its follower has it; `CELASTRO_AUTO_FAILOVER=on`
lets the steward (`CELASTRO_STEWARD`, or the lowest address) promote a
follower when a holder stops answering. Every node's console answers every
statement over every node's shards, so a client needs one URL: any
node's, or a load balancer's with `/api/health` as its check. The
cluster statements -- placement, moves, `LOCAL`, `SHOW HEALTH`,
`partial_results` -- are in [docs/sql.md](sql.md#two-or-more-nodes).

## The image

`ghcr.io/celastro/celastro:<version>` is the same binary `FROM scratch`,
not root, handling SIGTERM; the flags and statements are the same.

```sh
docker run --rm ghcr.io/celastro/celastro:0.55.0 version
docker run --rm ghcr.io/celastro/celastro:0.55.0 demo
docker run -d --name celastro -p 8787:8787 -v celastro-data:/data \
  -e CELASTRO_TOKEN=0123456789abcdef0123456789abcdef \
  ghcr.io/celastro/celastro:0.55.0 --dir /data serve --bind 0.0.0.0
docker run --rm --network host -e CELASTRO_TOKEN=0123456789abcdef0123456789abcdef \
  ghcr.io/celastro/celastro:0.55.0 send http://127.0.0.1:8787 "SHOW HEALTH"
```

[docs/container.md](container.md) has the volume's ownership, the REPL's
stdin, and what each flag costs.

## Exit codes and environment

Exit 0 is success, 1 a runtime or SQL error, 2 a usage error. Every
`CELASTRO_*` variable is read once at start: the addresses and tokens
above, the keys and certificates, `CELASTRO_ARCHIVE_*` and
`CELASTRO_BACKUP_DIR` for the archived tier and the backups, and the
tunables in [docs/tuning.md](tuning.md). A value that does not parse
stops the start naming the variable.
