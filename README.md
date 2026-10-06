# celastro

A minimal single-node document database: JSON documents in named
collections, served over HTTP. One static binary, no dependencies outside
Rust's standard library.

Every collection is held in memory and in an append-only log on disk. A
write is appended to its collection's log and synced to disk before it is
acknowledged; starting the server replays the logs.

## Install

Download the binary for your platform from the
[releases](https://github.com/celastro/celastro/releases) (Linux, x86-64 and
ARM64, statically linked) and check it against `SHA256SUMS`, or build it
(Rust 1.89 or later):

```sh
cargo build --release
./target/release/celastro version
```

## Run

```sh
export CELASTRO_TOKEN=0123456789abcdef0123456789abcdef
celastro serve --dir ./data              # http://127.0.0.1:8787
```

`--dir` is where the data lives (`./data` by default), `--bind` and `--port`
where it listens (`127.0.0.1:8787`). With `CELASTRO_TOKEN` set, every request
but `/health` must carry `Authorization: Bearer <token>`; serving on anything
but loopback is refused without it. One process per directory: a second is
refused while the first holds the directory's lock.

## API

| request | does |
|---|---|
| `GET /health` | `{"ok":true}`; needs no token |
| `GET /collections` | each collection's name and document count |
| `PUT /collections/{c}/docs/{id}` | write the JSON object in the body under `id`, replacing any document there |
| `GET /collections/{c}/docs/{id}` | the document, or 404 |
| `DELETE /collections/{c}/docs/{id}` | remove it; `deleted` says whether it was there |
| `GET /collections/{c}/docs` | documents in id order: `where.{path}={value}` filters (repeatable, `a.b` for nested fields), `after={id}` and `limit={n}` (1 to 1000, default 100) page |

```sh
H="Authorization: Bearer $CELASTRO_TOKEN"
curl -X PUT -H "$H" localhost:8787/collections/products/docs/P1001 \
  -d '{"name":"P1001","category":"C10","price":12.5}'
curl -H "$H" localhost:8787/collections/products/docs/P1001
curl -H "$H" "localhost:8787/collections/products/docs?where.category=C10&limit=50"
curl -X DELETE -H "$H" localhost:8787/collections/products/docs/P1001
```

```
{"ok":true}
{"ok":true,"id":"P1001","doc":{"name":"P1001","category":"C10","price":12.5}}
{"ok":true,"docs":[{"id":"P1001","doc":{"name":"P1001","category":"C10","price":12.5}}],"next":null}
{"ok":true,"deleted":true}
```

A listing's `next` is the id to pass as `after` for the following page, or
`null` on the last one. A filter matches a string field equal to the value,
or any other field whose JSON text is the value (`where.price=12.5`,
`where.active=true`). Every answer is JSON with `ok`; an error is
`{"ok":false,"error":"..."}` with a 4xx or 5xx status.

A collection name is 1 to 64 letters, digits, `_` or `-`; an id is 1 to 256
bytes with no control characters; a document is a JSON object of at most
16 MiB.

## What it is not

- **One node.** No replication: back up the directory by stopping the server
  and copying it.
- **No compaction.** A collection's log grows with every write and delete;
  the whole data set is held in memory.
- **No indexes.** A filter reads every document of the collection.
- **No TLS.** Put it behind a proxy that terminates TLS when it is reachable
  beyond the machine.

## Durability

A write is acknowledged after its log record is synced (`fdatasync`), and a
new collection's file after its directory is synced. Each record carries its
length and a CRC-32. A crash can leave the last record torn, and the next
start cuts it, since it was never acknowledged; a damaged record with more
of the log after it is refused by name rather than dropped with what follows.

## License

GNU Affero General Public License v3.0 only; see [LICENSE](LICENSE).
