# Running in a container

`Dockerfile` at the repository root builds the image in two stages: a musl
toolchain compiles one statically linked binary, and the image that ships is
`FROM scratch`. It carries `celastro-cli` — the full command-line tool: `serve`,
`exec`, `run`, `repl`, `demo`, `catalog` — a copy of the licence, and nothing
else.

```
docker build -t celastro .
docker run --rm celastro version       # the version the image was built from
docker run --rm celastro demo          # the guided tour, in memory, no volume
```

`docker export` takes a container, not an image, so seeing the whole filesystem
means creating one, listing it and throwing it away:

```
$ cid=$(docker create celastro)
$ docker export "$cid" | tar -tv
-rwxr-xr-x 0/0               0 2026-09-10 10:25 .dockerenv
-rw-r--r-- 0/0           34523 2026-09-10 10:06 LICENSE
-rwxr-xr-x 0/0         1856216 2026-09-10 21:15 celastro-cli
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
binary reproduces byte for byte: 1,856,216 bytes of binary and 34,523 of
licence, about 1.89 MB uncompressed and about 895 kB compressed. What Docker
*prints* is neither of those unconditionally — it depends on the image store,
which `docker info | grep driver-type` names. On Docker 29.1.3 with the
containerd store (`io.containerd.snapshotter.v1`), `docker images` reports DISK
USAGE 2.81 MB and CONTENT SIZE 895 kB — disk usage counts the compressed blobs
*and* the unpacked snapshot — and `docker image inspect --format '{{.Size}}'`
prints the compressed content size, `895262` on the build behind this
paragraph. On the older non-containerd store the same field is the uncompressed
total instead, the 1.89 MB that `docker history` breaks down as 1.86 MB + 41 kB
+ 8.19 kB.

Do not hold `.Size` to the byte. Two independent `--no-cache` builds of this
source both printed 895262 here, but on the previous toolchain pin the same
exercise produced six different values between 918581 and 918586 around an
identical binary. Compressing image metadata is not a reproducible operation;
compiling this source is, and the binary size is the number to quote.

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

`-t` without `-i` is the trap. It does not exit; it waits on a terminal that
will never send anything, and because the REPL handles no signals (only
`serve` does -- see below) `docker stop` waits out the full ten-second grace
period and then kills it: exit 137.

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

## The data directory and who owns it

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

## The console binds loopback, and `-p` therefore cannot reach it

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
celastro-cli serving on 127.0.0.1:8787 — Ctrl-C, SIGTERM or POST /api/shutdown to stop
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

That URL, token included, is what to `curl` or open. Ctrl-C in the terminal
holding the command ends the server, saved and exit 0: `serve` handles SIGINT
and SIGTERM itself, as the banner says. It can also be stopped from a second
terminal, which is how a browser session ends it:

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

## `panic = "abort"` makes the container the unit of recovery

`[profile.release]` sets `panic = "abort"` — "a reachable panic is a bug, not a
recoverable condition". There is no unwind, no `catch_unwind` boundary and no
cleanup path: the process ends where it stands, and from outside the container
that is a process that was running and then was not. What would be a caught
exception elsewhere is a container exit here, so the restart policy is the
error handling.

`serve` handles SIGTERM and SIGINT itself -- an in-tree binding to
`signal(2)`, the route being recorded in `src/signal.rs` -- so as PID 1 it
receives the signal a supervisor sends and ends the accept loop; the caller
then saves and exits 0. The other verbs install no handler: the REPL because a
handled Ctrl-C would be swallowed by a restarted read at a terminal, and the
one-shot verbs because a job that is killed is a job that stops. PID 1 with a
default disposition does not receive the signal at all, which is what the
ten seconds below are:

| stopping it | observed |
|---|---|
| `docker stop`, `serve` | prompt, exit 0, closed cleanly |
| `docker stop`, `repl` | ten seconds of waiting, then SIGKILL: exit 137 |
| `docker stop`, `repl`, container started with `--init` | under a second, exit 143 |
| `POST /api/shutdown` (`serve` only) | immediate, exit 0, closed cleanly |

So `serve` needs nothing; an interactive `repl` left running as PID 1 wants
`--init`, or expect its stop to take ten seconds.

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
