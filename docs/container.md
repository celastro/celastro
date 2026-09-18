# Running in a container

`Dockerfile` builds the image in two stages: a musl toolchain compiles one
statically linked `celastro`, and the image that
ships is `FROM scratch` — the binary, the licence, the copyright notice, and
nothing else.

Each release publishes the image as `ghcr.io/celastro/celastro:<version>`,
with `latest` following the newest release, built from the tagged tree by the
same `Dockerfile`; pull that, or build it:

```
docker pull ghcr.io/celastro/celastro:0.45.0
docker build -t celastro .
docker run --rm celastro version       # the version the image was built from
docker run --rm celastro demo          # the guided tour, in memory, no volume
```

The image holds the binary, `LICENSE`, `COPYRIGHT` and an empty `/data`;
everything else `docker export` lists is the runtime's. The binary and the
two notices are root-owned and not writable by the user the image runs as: a
process able to overwrite its own executable has a capability with no
legitimate use. UID 65532 owns `/data` and nothing else. No shell, no libc,
no package manager, nothing running as root. The image is a few megabytes;
the binary reproduces byte for byte and its size is the stable number, what
`docker images` prints is not.

`docker run --read-only` works — `demo` and a volume-backed `exec` both complete
under it — because nothing is written outside the data directory.

## First contact: which flags the REPL needs

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

`-t` without `-i` is the trap: it waits on a terminal that never sends
anything, and since only `serve` handles signals, `docker stop` waits out the
ten-second grace period and kills it (exit 137).

The entrypoint is bare, so global flags precede the verb; `-i` lets a script
arrive on stdin:

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

(The prompt is written before each read, so a piped session prints it ahead
of each answer.) A collection created in one container is there in the next:

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

## The data directory and who owns it

A fresh **named volume** is seeded from the image's `/data`, ownership included,
so `-v celastro-data:/data` needs no preparation: UID 65532 owns it and can
write.

A **bind mount** is not seeded. Docker creates a missing host directory as
`root:root`, which 65532 cannot write, and the failure arrives on the first
statement, because the catalog is persisted as it changes:

```
$ docker run --rm -v "$PWD/data:/data" celastro --dir /data exec 'CREATE COLLECTION notes (id TEXT PRIMARY KEY)'
error: io error: Permission denied (os error 13)
could not save: io error: Permission denied (os error 13)
$ ls -ldn ./data
drwxr-xr-x 2 0 0 4096 Sep 10 10:25 ./data
```

Every session saves as it closes, so even a container that only read exits 1
on a directory it cannot write. Either fix is enough; both start with `sudo`
only because Docker already made the directory root's:

| fix | prepare, once | then run | files land owned by |
|---|---|---|---|
| run as yourself | `sudo chown "$(id -u):$(id -g)" ./data` | `docker run --user "$(id -u):$(id -g)" -v "$PWD/data:/data" …` | you |
| hand the directory to the image's UID | `sudo chown 65532:65532 ./data` | `docker run -v "$PWD/data:/data" …` | 65532 |

Create the directory yourself and it is already yours, and `--user` alone is
enough with no privilege anywhere:

```
$ sudo rm -rf ./data
$ mkdir ./data
$ docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/data:/data" celastro --dir /data exec 'CREATE COLLECTION notes (id TEXT PRIMARY KEY)'
collection `notes` created with 1 shard(s)
```

`--user` alone against a directory Docker made is not a fix: the `chown` does
the work, `--user` only decides whose name ends up on the files. The image
has no `passwd` file and nothing in celastro resolves a UID, so `--user` may
name any pair of numbers.

## A probe from inside the image

`celastro health` asks the console on `--port` whether it is serving —
over TLS when the certificates are in the environment — and exits 0 only for
a 200 that says so; `--attached N` also requires `N` peers verified since
start, for a readiness probe. It exists because the image has no shell and no
curl and the console binds loopback. The answer comes from the database, so a
process up with a database it could not open is not healthy. `/api/health` is
the one path served without the token: a probe cannot know one, and the
answer executes nothing. The Helm chart uses it for both probes.

## The archived tier can live in an object store

`CELASTRO_ARCHIVE_ENDPOINT` (`http://host:port`, or `https://host` verified
by the PEM bundle in `CELASTRO_ARCHIVE_CA` -- the image has no system
bundle, so mount one) and
`CELASTRO_ARCHIVE_BUCKET`, with `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` (`CELASTRO_ARCHIVE_PREFIX` and `_REGION` optional),
make the `archived` tier an S3-compatible bucket instead of a directory in the
volume. Pass them with `-e`; the credentials are never written to the data
directory.

`CELASTRO_ARCHIVE_DIR` puts the `archived` tier in a directory instead — a
mounted NFS volume, say — and `CELASTRO_BACKUP_DIR` is where `BACKUP TO
'<name>'` and `RESTORE FROM '<name>'` resolve a bare name, and the only
directory a path in those statements may point into; both are read at
open.

## The console binds loopback, and `-p` therefore cannot reach it

By default `celastro serve` puts the console on `127.0.0.1` and nothing
else: the endpoint executes arbitrary SQL, so a bind reachable from a network
is a remote shell, and three guards sit in front of it — the loopback bind, a
`Host` allow-list against DNS rebinding (`localhost`, `127.0.0.1`, `[::1]`),
and a per-run token from `/dev/urandom`. Observed: no token → 401; a valid
token with `Host: evil.example` → 403.

**`docker run -p` cannot reach a loopback bind.** A published port forwards to
the container's external interface; the console listens on its loopback. The
symptom is not a refusal: the connection is accepted on the host and reset
when the forward finds nothing at the other end:

```
$ cid=$(docker run -d -p 8787:8787 -v celastro-data:/data celastro --dir /data serve)
$ curl -sv http://127.0.0.1:8787/
* Connected to 127.0.0.1 (127.0.0.1) port 8787
* Recv failure: Connection reset by peer
```

Two ways out: `--network host` for a console on this machine, or `--bind`
with a token (and certificates) for one on a network, below. That container
still holds host port 8787, so it has to go first:

```
$ docker rm -f "$cid" >/dev/null
```

**`--network host`** makes the host's loopback and the container's one
interface:

```
$ docker run --rm --network host -v celastro-data:/data celastro --dir /data serve
celastro serving on 127.0.0.1:8787 — Ctrl-C, SIGTERM or POST /api/shutdown to stop
The token in that URL is the only thing protecting this database. Anyone who can
read this terminal, this process's environment or its command line can use it, and
the server answers every request that carries it. Treat the URL as a password, and
stop the server when you are done.
http://127.0.0.1:8787/?t=fdd2b8856f798668b6f29478e4f1fd5b
```

Only the URL is on stdout; the banner is stderr. Ctrl-C ends the server
saved, exit 0, and so does a `POST` from a second terminal:

```
$ curl -s -X POST 'http://127.0.0.1:8787/api/shutdown?t=fdd2b8856f798668b6f29478e4f1fd5b'
{"ok":true,"kind":"ack","message":"shutting down"}
```

What `--network host` costs: no network namespace of its own, so `--port`
collides on the host like a host process, any process on the host with the
token can reach the console, and it is a Linux mode (Docker Desktop offers it
only as a setting you turn on).

**`--bind 0.0.0.0`** is for a console on a network — nodes behind a Service or
a load balancer. It needs `CELASTRO_TOKEN` in the environment, at least
sixteen printable bytes, the same at every node; the console then answers on
every interface, `-p 8787:8787` reaches it, and the `Host` check gives way to
the token:

```
$ docker run -d -p 8787:8787 -e CELASTRO_TOKEN=0123456789abcdef0123456789abcdef \
    -v celastro-data:/data celastro --dir /data serve --bind 0.0.0.0
$ curl -s -H 'X-Celastro-Token: 0123456789abcdef0123456789abcdef' http://127.0.0.1:8787/api/health
```

Plain HTTP unless the image is also given certificates: mount them and set
`CELASTRO_TLS_CERT`, `CELASTRO_TLS_KEY` and `CELASTRO_TLS_CA` (all three, PEM,
Ed25519 — `celastro tls init` makes a set) and the console and the wire
serve TLS 1.3, with the CA verifying every peer. The chart's `console.expose`
and `tls.enabled` are these two, with the token and the certificates in
Secrets and a Service over the pods.

Logs: `serve` writes one line per event to stderr, timestamped and
levelled; `CELASTRO_LOG=json` makes them JSON lines, which is what a
cluster's log collector wants.

Encryption at rest: mount a master key and set `CELASTRO_MASTER_KEY_FILE`
to it (`celastro key master` writes one), and every file under
`/data`, the tier and the backups are encrypted under a data key kept
wrapped in `/data/KEY`; a cluster's pods also take `CELASTRO_KEY_FILE`, the
one wrapped data key `celastro key init` wrote, so they share it. The
chart's `encryption.existingSecret` mounts both.

## `panic = "abort"` makes the container the unit of recovery

`[profile.release]` sets `panic = "abort"`: a reachable panic is a bug, the
process ends where it stands, and the restart policy is the error handling.

`serve` handles SIGTERM and SIGINT itself (an in-tree `signal(2)` binding,
`src/signal.rs`), so as PID 1 it receives the signal, ends the accept loop,
saves and exits 0. The other verbs install no handler — a REPL's handled
Ctrl-C would be swallowed by the restarted read, and a killed job is a job
that stops — and PID 1 with a default disposition does not receive the signal
at all, which is what the ten seconds below are:

| stopping it | observed |
|---|---|
| `docker stop`, `serve` | prompt, exit 0, closed cleanly |
| `docker stop`, `repl` | ten seconds of waiting, then SIGKILL: exit 137 |
| `docker stop`, `repl`, container started with `--init` | under a second, exit 143 |
| `POST /api/shutdown` (`serve` only) | immediate, exit 0, closed cleanly |

Being killed abruptly is survivable: every statement that changed something
was persisted before it was acknowledged, so a restart resumes from the last
committed statement (verified with `docker kill --signal=KILL` and a fresh
container over the same volume). `--restart on-failure:N` fits `serve` — a
panic exits non-zero — and only `serve`: the one-shot verbs are jobs, and an
`exec` against an unwritable directory under `on-failure:3` was dutifully
restarted three times.
