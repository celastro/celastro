# Changelog

What changed for someone running the previous version. Each entry is written
from the point of view of upgrading INTO that version, so the paragraph under
0.4.0 is what a 0.3.0 user needs to know. Versions on
[crates.io](https://crates.io/crates/celastro); tags `vX.Y.Z` in this
repository.

## 0.76.0 — 2026-09-24

**A write's log sync no longer holds up reads (group commit).** A
write statement held the database's lock through its `fdatasync`, so on
a slow disk every read waited out a sync and writers queued on the
disk's latency. `serve` now appends and applies a write under the lock,
lets it go, and syncs the log with every writer that arrived meanwhile.
What a read sees has not changed: a row, a delete or a replacement is
seen only once it is on disk, and a write is acknowledged only once a
read would see it. With the sync made 50 ms slower, a point read beside
a writer went from one sync (p99 61-67 ms) to under a millisecond, and
eight writers made 74 writes a second where they had made 18.
`CELASTRO_GROUP_COMMIT=off` keeps the sync under the lock; the embedded
API is unchanged.

A sync that fails is not retried, since a page cache that failed one
cannot be trusted with a second: the write is refused and cut from the
log, that shard takes no more writes, reads on the node stay at the
instant before it, `/api/health` answers `ok:false`, and a restart
replays the log. The new alert `CelastroLogSyncFailed` fires on it.

**A read waiting at a writer's release goes before the next writer.**
A writer that asked for the lock the moment it let go had been first
back every time, so a point read under a steady insert waited out
several writes: a median of 234 ms at a 50 ms sync, now one sync or
less. A console read also takes the lock once rather than three times.

**Metrics.** `celastro_statement_seconds` carries a `kind` label
(select, insert, delete, ddl, other); summed by `le` it is the histogram
it was, and the dashboard reads it unchanged, with a p95 per kind
beside it. New: `celastro_resident_bytes` per collection and tier;
`celastro_statements_partial_total` and `celastro_shards_missing_total`
for answers that came back without a shard; `celastro_wal_syncs_total`,
`celastro_wal_sync_writers_total` and `celastro_wal_sync_failed`. The
dashboard draws the write-ahead log, the shards missing and the writes
per sync, and the chart's rules gain `CelastroSealNotLanding` and
`CelastroLogSyncFailed` (six in all; the chart is 0.37.0).

Tests: a crash at every durability point -- each log record, fsync,
rename and truncation, and each record torn -- opens with exactly what
was acknowledged, with group commit and without; a million-row log
replays; a compaction on a full disk leaves the shard as it was.

Upgrading: a query on `celastro_statement_seconds_sum` or `_count` that
did not aggregate now returns a series per kind; wrap it in `sum()`.
Nothing else to do.

## 0.75.0 — 2026-09-24

**A seal whose install failed is tried again.** A seal builds a memtable's
segments and then installs them: the segment files, then the manifest
that names them. When the install failed -- a disk that filled between
the build and the write, a directory that would not take a file -- the
seal was dropped. Nothing acknowledged was lost: the rows stayed
readable in the frozen memtable and durable in the log it rotated
aside, and a restart replayed them. But until that restart nothing
sealed them, so a node whose disk filled during a seal and then got its
room back kept those rows in memory and on the log indefinitely, and
the `celastro_seal_failures_total` counter did not count it. The
install now puts the seal back on its shard's queue, as a failed build
always has; the maintenance thread retries it a second later, with no
write needed to prompt it, and counts the failure.

**`celastro_wal_bytes`**, a gauge per collection: the bytes of
write-ahead log a restart of this node would replay -- the live log and
the rotated logs of seals that have not landed. It rises with writes
and falls at each seal, so one that only grows is a seal that is not
landing, and its value is how long the next restart's replay will take,
at the rate the resilience suite prints. The dashboard does not draw it
yet; the Compactions panel's description says what to read beside it.

Nothing to do on upgrade. A node on 0.74.0 holding a dropped seal
replays its log at the restart the upgrade is, as it always would have.

## 0.74.0 — 2026-09-24

**A dashboard, an alert set, and a Monitoring section in the console.**
`/api/metrics` has spoken the Prometheus text format since the console
did, and nothing drew it, nothing scraped it, and the page an operator
already has open said nothing about whether the node was well. Three
things change that, and one of them changes the metrics themselves.

**Statement latency is a histogram.** `celastro_statement_seconds` now
carries `_bucket{le="..."}` at eleven bounds from a millisecond to ten
seconds, and `_count` beside them, so `histogram_quantile` answers and a
dashboard can draw a p95 instead of a mean. The old
`celastro_statement_seconds_sum` keeps its name, its value and its
meaning; what changed is the family's `# TYPE` line, from `counter` to
`histogram`. A scraper that
recorded the sum goes on recording it. A p95 sitting exactly at 10 means
the statement was slower than the widest bucket, not that it took ten
seconds.

**The console accepts `Authorization: Bearer <token>`** wherever it
accepts `X-Celastro-Token`: the same secret, compared the same way,
under the name scrapers speak. Prometheus, and the `PodMonitor` the
chart emits for it, can send an `Authorization` header from a Secret and
cannot send an arbitrary one, so without this an authenticated
`/api/metrics` could not be scraped over a network without putting the
token in a URL, where every proxy on the way logs it. It costs no CSRF
protection: both headers are non-simple, so a cross-origin page still
cannot attach either without a preflight this console never answers.

**[deploy/grafana/celastro.json](deploy/grafana/celastro.json)** imports
as it is -- the only thing it asks for is a Prometheus datasource -- and
draws four rows: statements, the cluster, storage, and the states that
page someone. Every panel's description says what its query is and what
a bad value means, including the traps: a p95 pinned at the widest
bucket, a document count that drops because a node stopped being
scraped, a "failed" line that is mostly people mistyping SQL.

**The chart scrapes it.** `--set monitoring.enabled=true` emits a
`PodMonitor` over every pod's `/api/metrics` and a `PrometheusRule`
with four alerts -- a data-key ring nobody retired, a certificate inside
thirty days, a node short of its peers, a data directory that has gone.
Both need the prometheus-operator CRDs; without them the install fails
naming the missing kind rather than monitoring nothing. Turning it on
also puts the console on the pod's network and generates the console
token if there is none, because a scrape comes from another pod; it does
not create the console Service, so with `console.expose` still false the
scraper is the only thing that can reach the console. On hosts,
[deploy/README.md](deploy/README.md#watching-it) has the `scrape_config`.

**The console has a Monitoring section.** `SHOW HEALTH` rendered as it
prints -- this node, every node it knows and whether each answers, every
shard and where it is, the certificate, the data-key ring -- with the
lines the report already marks in capitals (`DOWN`, `UNREACHABLE`,
`CLOCK OFF`, `EXPIRES SOON`, `GONE`, `NOT ADOPTED`) coloured rather than
left to be noticed; and beside it statements a second, refusals,
failures, connections, compactions and a p95, each a number and a
sparkline. It refreshes with the sidebar's Refresh and on a ten-second
timer that can be turned off, and it asks for nothing but `/api/query`
and `/api/metrics` on this node. The rates are the page's own arithmetic
and live in the tab: the node keeps no history, so they start when the
page opens and a reload starts them again, which the panel says in as
many words.

Nothing was renamed. The metric names are the contract, and a release
that changes one will say so here.

## 0.73.0 — 2026-09-24

**The wire's default port is 7876, and an address written without one
changes meaning.** `tcp://host` meant `tcp://host:2352` through 0.72.1;
here it means `tcp://host:7876`, and `serve --shard-bind ADDR` binds
`ADDR:7876`. An address is kept as it was written -- the placement map
holds the string and each node resolves it when it dials -- so what an
upgrade does depends on whether the port is in it:

- **Every address names its port**, which is what `celastro install`,
  the Helm chart and [deploy/ssh](deploy/ssh/celastro-cluster.sh) all
  write: nothing changes. The cluster goes on serving 2352, and the new
  default reaches only clusters started after the upgrade and any
  address or `--shard-bind` written bare.
- **An address is bare**: a rolling upgrade breaks at the first node.
  That node binds 7876 and dials its peers on 7876; they are listening
  on 2352 and dialing 2352, and a holder that cannot be reached is a
  shard that cannot be read. Either upgrade the whole cluster at once --
  stop every node, replace the binary, start them again, and the bare
  addresses resolve to 7876 on all of them with nothing in the map to
  change -- or write `:2352` into `CELASTRO_NODE`, `CELASTRO_ATTACH`,
  `--shard-bind` and every attached address *before* upgrading anything.
  The second is a change of each node's address rather than of its
  settings, so it goes one node at a time: move its shards away,
  `DETACH NODE`, restart it on the explicit address, `ATTACH NODE
  'tcp://host:2352'`, move the shards back. Either way the ports can be
  dropped again once every node is on this release.

The chart is the same question in one line: `wire.port` defaults to
`7876`, a pod's address is `tcp://<pod>.<release>:<wire.port>`, and
`helm upgrade` rolls the pods one at a time -- so an existing release
takes `--set wire.port=2352` (or that value in its own file) to keep
the addresses its map holds. A new release needs nothing.

A node's backups are keyed by its address as written
(`nodes/<host>_7876/`), so an address that gains, loses or changes its
port starts a new lineage under the destination: its `LATEST` is
elsewhere, the next backup is a full one rather than a repeat, and
restoring what the old address wrote means naming it --
`RESTORE FROM '/mnt/backups' NODE 'tcp://host:2352'`.

The firewall between the nodes wants 7876 where it wanted 2352. The
console's 8787 is unchanged; every client names it explicitly, so it
was never the port with a default to get wrong.

Nothing else changed: this release is the port, the documents and the
deployment files that carry it, and a test pinning the installer's copy
of it to the library's.

## 0.72.1 — 2026-09-23

**`key reseal` will not touch a backup.** A backup keeps its segments at
`pool/<collection>/<shard>/<id>.seg` and the archived tier keeps its own
at `<prefix><collection>/<shard>/<id>.seg` -- the same shape, and the
identity a file was sealed under is the same string read off either. If
the tier and a backup destination shared a bucket and prefix, 0.72.0's
`key reseal` would have re-sealed the backup's objects under the new
data key, leaving every record that named them with a hash that no
longer matched: `VERIFY BACKUP` would have called the backup damaged and
a restore would have refused it. Now `key retire` and `key reseal` look
only at the collections the catalog says are at the archived tier, and
both refuse outright when a backup has been written under the same
prefix, naming the setting to change. Nobody could have hit this without
pointing both at one place; nothing else changed for anyone who did not.

**A re-seal no longer holds an object three times over.** It read the
object, its plaintext and its re-sealed copy all at once -- about ninety
megabytes for a thirty-megabyte segment. Frames are independent, so it
now re-seals one frame at a time into a scratch file, which is also
removed when the command ends rather than left beside the database.

`Secret` no longer derives `PartialEq`: `==` on secret bytes stops at the
first difference, and the derive invited a timing oracle with no
warning. `Secret::ct_eq` is the comparison the type offers. Nothing in
the tree compared two of them.

The `BACKUP` record had two parsers in one file, one strict and one
silently skipping; there is one now, so a restore and the hash recall
cannot drift apart over what a line means.

A walk of the archived tier asks one question per object rather than
two. `ObjectStore::head` reads *up to* a given number of bytes and says
`None` when there is no such object, where `get_range` refuses a short
read and so needs the length known first; `key retire` and `key reseal`
took a `HEAD` and a `GET` per object and now take one `GET`.

## 0.72.0 — 2026-09-22

**The Ansible role is gone; a shell script over ssh takes its place.**
`deploy/ansible/` is removed and `deploy/ssh/celastro-cluster.sh` does
what it did: the release binary onto each host, checked against the
release's `SHA256SUMS`, then `celastro install` with that node's own
address and the list of every node, one host at a time and each waited
for before the next, then every node asked whether it has verified
every other.

```sh
CELASTRO_TOKEN='a-long-random-token' CELASTRO_WIRE_TOKEN='another' \
  deploy/ssh/celastro-cluster.sh 10.0.0.2 10.0.0.3 10.0.0.4
```

It needs ssh here and curl, tar and systemd there, where the role
needed ansible-core here and Python on every host -- an interpreter on
each machine in order to install a binary that has none. Its settings
are environment variables rather than inventory variables, listed at
the top of the script, and a host whose ssh address is not the address
the nodes reach it at is written `root@203.0.113.10=10.0.0.2`. Moving
off the role is the host list on a command line and the tokens in the
environment rather than a vault; `celastro install` itself is
unchanged, so a cluster the role installed is one the script upgrades.
If you would rather keep Ansible, the role was thin on purpose -- one
`celastro install` per host -- and wrapping that command in a role of
your own is shorter than the one that was here.

**Keys erase their working buffers, and what is left is measured.** The
key schedule used to hand its intermediates to the allocator as they
were: HMAC's padded key, its two pads and its two message buffers, and
HKDF's `T(i-1)` buffer and its output `Vec`, all of them key material
and none of them overwritten, so a derivation left derived bytes in
freed heap that any later allocation could have been given. They are
wiped now, and the expansion writes into the caller's buffer rather than
returning one. `wipe` is `#[inline(never)]` with a compiler fence and a
`black_box`, so it cannot be reasoned away as a dead store; the file
keys, the ticket key, the X25519 scalar and a stream's traffic secrets
are held in a type that erases itself when dropped.

What remains is stated rather than implied. `cargo test --release --
--ignored core_dump` searches the process's own memory for known keys
and reports what it finds: none for a file key, none for the X25519
scalar, none for a derived block expanded into a caller's buffer, and
**one copy of a traffic secret after a handshake** -- a value moved into
a field leaves the local it was moved out of, and Rust never drops a
moved-from value, so nothing wipes it. The README says so in those
terms. No wire format, no API and no behaviour changed.

**The data-key ring can be asked about, and finished.** A rotation with
an index at the archived tier keeps the old data key in a ring, because
the objects in the store are where the rotation does not reach. Until
now the only thing that knew whether the ring was still needed was
`SHOW HEALTH`, and `key retire` dropped it on trust.

`celastro key retire <DIR>` now walks the archived tier first and
**refuses** while any object still opens only under a previous key,
naming the collections; `--check` reports and writes nothing, `--force`
is the old behaviour. The walk reads one frame per object rather than
the object, since a frame authenticates on its own. A store it cannot
reach is a refusal, not a retirement.

`celastro key reseal <DIR>` finishes the rotation instead of undoing
it: the objects still under an old key are fetched, sealed again under
the current one, written back, and the ring retired. It skips what it
has already moved, so running it again after a failure finishes the
rest. Both read the tier through the same `CELASTRO_ARCHIVE_*` settings
a node uses.

A ring that nothing can need -- no index at the archived tier at all --
is now dropped by the next open and logged, which costs no store
access. `celastro_data_key_ring_size` is a new gauge: above zero is a
rotation nobody finished.

**A repeat backup no longer reads the whole database to copy nothing.**
The record carries a SHA-256 beside every size, and a segment already in
the destination's pool was read and hashed in full to fill that column
while uploading nothing -- so the second backup of a database read all
of it, and so did every backup after. The hash now comes from the
record the last backup wrote. Three backups of a 36.6 MB corpus with
nothing changing between them read 35.7 MB each before this; they now
read 35.7 MB, then nothing, then nothing.

Nothing about the record changed, and `VERIFY BACKUP` still reads every
object back. A backup falls back to reading the file where it must: no
previous backup at that destination, a record it cannot read, a version
1 record with no hashes, or a key the last record does not name at the
same size. The ack says how many hashes it recalled.

**The operator's token no longer reaches the log.** `serve` printed its
URL with `?t=<token>` on stdout before serving, so a `CELASTRO_TOKEN`
set by whoever runs the database landed in the pod's log under the chart
and in the journal under the quadlet -- a token that outlives the
process, and that every node behind one Service answers, readable by
anything that can read logs. That URL is now printed without the query,
and `--json`'s first line leaves the `token` field out, whenever the
token came from the environment.

Nothing changes for a console on loopback with no `CELASTRO_TOKEN`: it
draws a token for the run, prints it, and it dies with the process,
which is the only way anyone learns it. The one cost is `--open`
against a console using `CELASTRO_TOKEN` -- the browser gets a URL it
cannot authenticate with, and the token has to be pasted.

## 0.71.0 — 2026-09-21

**The binary installs itself, and the releases carry it.** `celastro
install` makes the binary the systemd service `celastro` on the host it
runs on -- the binary under `/usr/local/bin`, a system user, the data
directory, the tokens and addresses in `/etc/celastro/celastro.env`, the
unit -- starts it and waits until it answers; the same command on every
host, with `--node` and `--attach`, is the cluster, and the same command
from a newer binary is the upgrade. Each release now attaches
`celastro-<version>-linux-amd64` and `-arm64` tarballs with a
`SHA256SUMS`: the static binaries the image carries, taken from it.
`deploy/` holds the ways to run it: the Helm chart, moved there from
`chart/` (`helm install celastro deploy/chart/celastro`; the chart
itself is unchanged), a cloud-init file that installs at a machine's
first boot, an Ansible role that installs one host at a time from its
inventory, and a podman quadlet that runs the image under systemd.
`--port`, `--bind` and `--shard-bind` mean something to `install` as
well as to `serve`.

## 0.70.1 — 2026-09-21

**An old certificate set is caught before it breaks the wire.** A set
made by `tls init` before 0.67.0 names server authentication alone and
cannot serve as a client certificate; a node with such a set and
`CELASTRO_TLS_CLIENT_AUTH=required` refuses to start, naming the
certificate and the fix (a new set with `celastro tls init`, or an
issuer that names client authentication too), where before every
peer would have refused it on the wire with nothing to say why. `SHOW
HEALTH` warns of such a set even with the requirement off.

## 0.70.0 — 2026-09-21

**A backup streams each segment from its file.** The copy held every
segment whole in memory for its put, so a node's peak memory during a
backup followed its largest segment (measured: 150 MB over the
baseline for a 30 MB segment, more for the larger segments a
compaction makes). A sealed segment now streams from its file to the
destination, hashed on the way for the record, whether the destination
is a directory or an S3 store; only the segment built from the memtable
at the pin is bytes. The store client's read timeout scales with the
object's size (a second per 100 KB on top of thirty), so a slow
destination takes long rather than failing on a large object. Measured
against a store capped at 4 Mbit/s with the numbers in the design
notes.

## 0.69.0 — 2026-09-21

**What the reviews had accepted, done.** Randomness comes from
`getrandom(2)` on Linux (blocking until the pool is seeded, so a key is
never drawn from an unseeded boot), `/dev/urandom` elsewhere or on a
kernel without the call. A client that offers early data and sends a
record of it, though no ticket of this server ever allowed it, is
served: the server skips the records it cannot open under the
handshake key, up to the protocol's bound, where it refused the
connection before (RFC 8446 §4.2.10). The handshake's secrets -- the
ephemeral key, the shared secret and every traffic secret -- are wiped
when the handshake ends, and a stream's traffic secrets when it
closes. `SHOW HEALTH` says when the data key ring keeps previous keys
for the archived tier, and that `celastro key retire` drops them. A
test holds a backup's pin through a compaction and restores what was
pinned.

## 0.68.1 — 2026-09-21

**A peer that restarted mid-backup is judged by its record.** A cluster
backup polling a peer that came back with no memory of the copy it
was asked for named it `NOT on` even when the copy had completed
before the restart; the coordinator now looks for that peer's record
at the destination -- written last, so present only for a complete
copy -- and names the backup complete or the peer restarted during
its copy. Found by the `backupnode` drill.

## 0.68.0 — 2026-09-21

**A cluster backup no longer waits forever for a peer.** `BACKUP
CLUSTER` asked each peer for its copy in one call with no deadline, so
a peer cut off before or during its copy was a cluster backup that
never answered (the drill waited 400 s and gave up). Each peer's copy
is started detached now (`BACKUP TO ... AS OF <instant> DETACHED`, the
copy on a thread of the peer's own) and polled with `BACKUP STATUS
<instant>` every two seconds; a peer that cannot be reached within
fifteen seconds to start, whose copy fails, or that answers no poll for
a minute is named `NOT on` and the others' backups stand. A peer from
before this release is asked the old way, bounded at ten minutes.

## 0.67.0 — 2026-09-21

**The crypto module reviewed in-tree, and what the review found fixed.**
Every primitive now also runs Wycheproof's vectors (`tests/wycheproof/`,
the project's files as published: X25519, Ed25519, ChaCha20-Poly1305,
HKDF-SHA-256, ECDSA P-256, RSA-PSS -- 1,672 cases) beside the RFCs';
the constant-time claim was read line by line and the timing test
extended to the field arithmetic, the scalar reduction and the ticket;
the TLS was run against Go's crypto/tls and OpenSSL as clients and
servers in every mode; X.509 was read against RFC 5280 with hostile
material made by openssl. Found and fixed:

- The TLS never checked an X25519 shared secret for zero, which a
  low-order public key yields (RFC 8446 §7.4.2 says abort); both sides
  refuse it now with `illegal_parameter`.
- A server's refusal of a client's flight -- a missing certificate
  under `CELASTRO_TLS_CLIENT_AUTH=required` -- went out under the
  handshake keys, which a conforming client reads as a bad record MAC
  rather than the alert; the server writes under its application keys
  from its Finished on, as §7.1 has it.
- A KeyUpdate from the peer closed the connection ("not supported");
  both sides handle it now, and this side can send one (§4.6.3).
- A connection cut inside a record read as a clean end of stream; it
  is an error now (a close_notify or a cut between records is the end).
- The ECDSA verifier accepted a signature integer with its high bit set
  or a leading zero it did not need (BER, not DER), which is a second
  encoding of the same signature; refused (Wycheproof tcId 6 caught it).
- X.509: an extension marked critical that the parser does not read
  (a name or policy constraint) is refused rather than ignored; key
  usage is read, an intermediate without keyCertSign does not link a
  chain, and a leaf's extended key usage must name the purpose -- server
  authentication for a server, client authentication on the wire. The
  certificates this crate issues name both purposes now; **a
  certificate made by an earlier `tls init` names server authentication
  only and will not serve as a client certificate under
  `CELASTRO_TLS_CLIENT_AUTH=required` -- make a new set.**
- The timing test, extended to the field arithmetic, found the scalar
  reduction (`sc25519::reduce_512`, under every Ed25519 signature)
  8 % apart between a small and a large scalar: the compiler had turned
  a masked select back into a branch. The masks in the scalar and field
  arithmetic, in `ct_eq` and in Poly1305 now go through
  `std::hint::black_box`; the pair measures 0.1 % apart.
- The tool refuses core dumps (`prctl(PR_SET_DUMPABLE, 0)` on Linux)
  from its first line: a dump of a process holding a data key and a
  TLS key is those keys on disk.

Accepted as they are, with the reasons in docs/design.md: 0-RTT
refused by never offering `early_data`; the random source read from
`/dev/urandom` by file; a TLS key that leaks opens two days of
tickets; the wiping of stack copies not attempted. The README's
"unaudited" now reads "reviewed in-tree, unaudited outside it".

## 0.66.0 — 2026-09-21

**HelloRetryRequest.** A client that lists X25519 among its groups but
sends a share of another group first is asked for an X25519 share and
the handshake completes on its second flight, the transcript restarted
from the message hash as the RFC has it; a stock client configured
that way (`openssl s_client -groups P-256:X25519`) now connects where
it was refused. As a client the node answers a retry that asks for the
share it withheld or carries a cookie, and refuses one asking for a
group this build lacks, or asking again for a share it sent.

**Forwarded documents travel in one call per holder.** A statement's
documents for another node's shards went one wire call and one sync
each; they go as the wire's `insert_many` now, written and confirmed
there as one, the answer saying how many landed and what stopped it. A
holder too old to know the call, or a shard that moved under the
batch, is fed one at a time from where the batch stopped, as before.

**A raised replica count reaches a follower that never had the
collection.** `ALTER ... SET (replicas = n)` or `(regions = n)` hands
a node the map now names, and that never had the collection, the
definition and the map whole, as CREATE hands them, and it makes its
copy; the carried `LOCAL ALTER` was refused there before and the copy
was never made. The nodes the map no longer names hear the statement
too.

**The data key rotates under an archived tier.** A rotation no longer
refuses an index at the archived tier: the old key stays in a ring
behind the new one in `KEY` (a `CELK2` form; a `KEY` with one key
keeps the form every release reads), a file that does not open under
the current key is tried under each previous one, and `celastro key
retire <DIR>` drops the ring once nothing is under it.

**The image is built for arm64 too.** `scripts/image.sh` builds the
amd64 image as before and an arm64 one from a cross-compiled static
binary (`Dockerfile.prebuilt`), and pushes one manifest under the tag,
so a pull on either machine gets its own.

**Three more parsers under the fuzzer.** The text dictionary and the
posting lists (from a built component, mutated), the TLS record layer
end to end (a proxy that damages one byte of every connection, either
way, through the handshake and the data), and SigV4's canonicalisation
(which took a date shorter than eight bytes as a panic; it takes it as
it is now). The posting-list sweep found two more: a capacity taken
from a count the bytes gave, which on a mutated count was an
allocation that aborted the process, and a block offset summed
without a bound; both are bounded now. A file a peer or a disk hands
this node cannot end it.

**Measured: compaction runs itself after a load.** 40,000 documents
seeded into a served node in 21 seconds left each shard four flat
segments; the maintenance thread compacted them into one level-1
segment per shard within three minutes, 18 seconds each, with nothing
asked of it. The backlog's "automatic compaction after a load" was a
note from before the maintenance thread; it is closed as measured.

## 0.65.0 — 2026-09-21

**Client certificates on the wire.** `CELASTRO_TLS_CLIENT_AUTH=required`
(the chart's `tls.clientAuth`) makes a node's wire ask every peer for
its certificate and refuse one that presents none or a chain the CA
did not sign, before the token is looked at; every node presents its
own certificate when asked, so the shared token is a second factor
rather than the only one. The console keeps answering the token alone,
and `SHOW HEALTH` says the wire requires a peer's certificate. A
ticket issued before the requirement does not resume past it.

**The data key rotates.** `celastro key rotate <DIR>` seals every file
and every log record under the directory again under a fresh data key
and rewraps `KEY`, with no process serving it; the new key goes to
`KEY.next` first, so a rotation cut short is finished by running it
again, and a node refuses to open a directory with a `KEY.next` until
then. Backups and exports made before open as they did (each carries
its `KEY`); an index at the archived tier is refused, move it back
first. `celastro check <DIR>` opens every frame of every file and
names what does not open, writing nothing.

**A ticket key per day.** A TLS session ticket is sealed under a key
derived from the node's TLS key and the day, and opened under today's
or yesterday's: a TLS key that leaks opens two days of tickets rather
than its whole life.

**Measured, not changed.** `scripts/reproducible.sh` builds one commit
twice at a fixed path and compares the binaries; the crate's timing
test (`cargo test --release --lib crypto::timing -- --ignored`) samples
the constant-time claims on a real CPU. Both are run by hand; their
numbers are in the design notes.

## 0.64.1 — 2026-09-21

**The steward's first replacement no longer stops it.** The read lock
that produced the replacement plan was held through the loop that
takes the write lock to run it, so the steward stood still at its
first `REPLACE COPY`, renewed no lease, and every node refused writes
a lease later. Found by the `replaced` drill on 0.64.0, which is not a
release to run with `CELASTRO_AUTO_FAILOVER=on` and a follower away
for longer than `CELASTRO_REPLACE_SECS`.

## 0.64.0 — 2026-09-21

**A statement whose every shard is on one other node goes there whole,
as one call.** The coordinator asked that node its counters, then the
scan, then the fetch: three round trips for an answer that came from
the one node, and across a sea three times the sea's. Now the
statement itself travels, as the wire's `query` call, and the holder
coordinates it over its own shards by direct call, at no older an
instant than the sender's read-your-writes one, so a write just
forwarded through the same node is in the answer. `EXPLAIN` says
`forwarded whole to tcp://... as one call` above the plan as the
holder rendered it. A holder too old to know the call is asked shard
by shard as before; a forwarded statement is never forwarded on; a
statement with a shard here, or on two nodes, scatters as before.

**A copy lost for good is replaced.** `REPLACE COPY OF SHARD i OF c ON
'lost' WITH 'node'` strikes the lost follower from the map at the next
term and has `node` -- an attached data node that neither holds nor
follows the shard -- follow in its place, shipped from nothing by the
holder; the lost node, back, takes the map at the higher term and
drops the copy it kept. With `CELASTRO_AUTO_FAILOVER=on` the steward
runs it once a follower has been away for `CELASTRO_REPLACE_SECS`
(ten minutes; zero never), placing the copy in a region the
collection's `regions` still asks for, else in the holder's own. A
lost holder is promoted first and its copy replaced after the same
wait. Until now a shard whose follower was gone stayed one copy short
until an operator re-planned the collection by hand.

## 0.63.8 — 2026-09-20

**A statement asks only the holders it can reach.** Before every
statement the coordinator asked every holder's counters, for the
read-your-writes instant and the statistics epoch; a lookup pinned to
one shard by its partition key paid a round trip to every holder
anyway, and across a sea that was the sea's round trip on a lookup
whose own shard answered in four milliseconds. The holders whose
shards the predicate prunes away are not asked; their last answer
stands in, so the statistics age a little late for shards the
statement never reads. A test turns another shard's holder into a
black hole and expects the pinned lookup to answer in its own time.

## 0.63.7 — 2026-09-20

**A copy's caught-up instant survives a seal.** The ship marks a copy
stood on were in its log, and a seal truncates the log; a copy
reopened after a seal with no write since said "not caught up" and
was copied from nothing -- every copy of the two-datacentre cluster,
after every restart. The instant is kept in `SHIPPED` beside the log,
written at every seal and at a demotion's cut, read at open.

## 0.63.6 — 2026-09-20

**A re-plan of the followers agrees on every node.** `ALTER COLLECTION
... SET (replicas = n)` or `(regions = n)` is carried to every node and
each re-plans; the ring of data nodes started at the node doing the
planning, so each node named different followers, and a holder shipped
to two nodes that said they did not follow the shard for as long as the
maps disagreed. The ring is in address order everywhere now; a test
plans on each of five nodes and gets one answer.

## 0.63.5 — 2026-09-20

**A followed copy's seals go to the background sealer.** A copy
froze its seals as a held shard does, but the sealer reserved only
held shards, so a copy's third seal was built inline under the ship
lock: fourteen to twenty-seven seconds on the two-datacentre run,
every acknowledgement waiting on that copy with it, and the seed's
carries running out their deadline behind it. The sealer reserves,
builds and installs the copies' seals now, off the lock.

## 0.63.4 — 2026-09-20

**A shard's remaining copies stay in the holder's region.** With
`regions = 2` the first follower comes from the other region, as
before; the rest now come from the holder's own region before any
other, so a majority of the copies sits in one datacentre and a
quorum write never waits for the other. In ring order the rest fell
wherever the ring went, and four of ten shards on the two-datacentre
run paid a cross-region round trip on every write. `ALTER COLLECTION
... SET (regions = n)` re-plans an existing collection.

## 0.63.3 — 2026-09-20

**A follower's call is its own, not a round's.** 0.63.2 still joined
each round on its slowest call, so a write arriving while the cut
follower's call was pending was not sent to the live follower until
that deadline ran out: ten seconds again. Each follower's call runs on
a thread the shipper does not wait for; a follower with a call out
gets no other until it answers, and the rest go on. The first write
after a cut is acknowledged as fast as the live copy answers.

## 0.63.2 — 2026-09-20

**Each follower's answer is applied as it lands.** 0.63.1 sent to
every follower at once and then waited for every call before applying
any answer, so the live follower's confirmation still waited out the
cut follower's ten-second deadline: the first write after the cut was
acknowledged ten seconds later. Each thread applies its own answer
now, and the write is acknowledged as soon as the live copy has it.

## 0.63.1 — 2026-09-20

**The shipper reaches every follower at once.** It sent to its
followers in turn, so a follower away held the round for its
ten-second deadline before the live follower was sent anything, and
under quorum the live follower's confirmation is the write's
acknowledgement: on the `regions` drill the first write after the
minority region was cut off was acknowledged sixteen seconds later.
Each follower's call runs on a thread of its own now; the write after
a cut is acknowledged as fast as the live copy answers.

## 0.63.0 — 2026-09-20

**Regions, and a quorum acknowledgement (HA2, steps 1 and 2).** A node
carries `CELASTRO_REGION` (or picks one from `CELASTRO_REGIONS`, a list
by the node's ordinal, for a StatefulSet); its hello says it, and
`SHOW HEALTH` names each node's and each follower's region. A
collection created `WITH (replicas = 3, regions = 2)` gets a shard's
copies spread over that many regions -- the first followers one from
each region the holder's is not, then the rest -- and `ALTER
COLLECTION ... SET (regions = n)` re-plans them. `WITH (confirm =
'quorum')` acknowledges a write once a majority of the copies, holder
included, have it on disk: of three, the holder and one follower, so
a follower away does not hold writes, and with no majority the write
is refused rather than acknowledged on one disk alone -- with the
copies over two regions, a region failing loses nothing acknowledged.
`'all'` is HA1's rule (every live follower), `'none'` acknowledges on
this disk, and unset is the node's `CELASTRO_REPLICATION`. Under
quorum the steward promotes the most recent caught-up copy, the one
that took part in the last acknowledgement; under `all` it prefers a
copy in the holder's region. The health says `confirm = quorum, 2 of 3
copies live` and `BELOW QUORUM` when writes are refused. Catalog
format 10 (a peer at 9 reads the collections without the two fields).

Found on the way: a follower down held the shipper's whole round for
its backoff, up to five seconds, so the live follower's
acknowledgements waited with it; the backoff is the follower's now.

## 0.62.4 — 2026-09-20

**The elector carries what the answers make.** The votes come after
the pre-votes are granted, and the first heartbeats after the votes:
sends the machine makes while the answers are fed back. The elector
carried only the sends of a tick and dropped those, so on a cluster
the pre-votes were granted and the votes never asked -- no steward,
ever, since 0.62.2, while the machine's own tests (whose harness
delivers everything) passed. The answers' sends go out in turn now, a
few rounds at most.

## 0.62.3 — 2026-09-20

**The pre-vote's ask leaves the timeout alone.** 0.62.2 counted a
node's own ask as a steward heard, so every node was within a timeout
of its own last ask and none was ever quiet enough to answer another
yes: no steward was ever elected. The asks are paced by the timeout
on a clock of their own, and only a steward's heartbeat or a vote
given resets the quiet.

## 0.62.2 — 2026-09-20

**A pre-vote before a vote.** A node that has heard no steward for
the timeout first asks the group whether it would be voted for, at
the next term, changing nothing on either side; only a majority of
yeses makes it stand. A node cut off from the group asks every
timeout and hears nothing, so its term stays where it was, and when
it is back it takes the steward's next heartbeat -- where before it
came back with a term inflated alone and forced an election the
group did not need (the `stewardsplit` drill's old steward, back
after the heal, was steward again at term 13). A steward with a
fresh majority answers no pre-vote yes. The wire's `vote` carries
the pre flag.

## 0.62.1 — 2026-09-20

**The steward's heartbeats reach every peer at once.** Sent in turn,
a peer that did not answer (a name not yet resolving at start) held
the heartbeat to the others for its three-second deadline, past the
followers' timeout, and the group re-elected itself every timeout --
twelve terms in three minutes on the first drill. Votes and
heartbeats go out to every peer in parallel now, each with a deadline
of a quarter lease at most.

## 0.62.0 — 2026-09-20

**The steward by election.** `CELASTRO_STEWARDS=tcp://a:2352,tcp://b:2352,tcp://c:2352`
names a group that elects the steward among themselves, with a term:
a node that hears no steward for half a lease stands, a majority of
the group makes it steward, its lease renewals are its heartbeats and
carry the term, a node refuses a lease from a lower term than it has
seen, and a steward that cannot reach a majority for half a lease
stops renewing and steps down -- so the holders on its side run out of
lease and refuse writes within a lease. A new steward promotes nothing
for a lease and a quarter after its election, by when every lease the
old one granted has run out; with that, two holders taking writes for
one shard cannot happen through a steward cut off from the rest,
assuming clocks that run at comparable rates. The term and the vote
are kept in `STEWARD` in the data directory, so a restarted node
grants no second vote in a term. `SHOW HEALTH` says `steward: … (elected,
term N)`. The election is a pure state machine (`src/steward.rs`)
driven by deterministic tests through partitions, split votes and
restarts; the wire gains `vote`, and `lease` carries the term (a node
from before answers a renewal as it did). Unset, the steward is
`CELASTRO_STEWARD` or the lowest address, as before. HA1's step 3.

## 0.61.1 — 2026-09-20

**The steward promotes nothing until the lost holder's lease has run
out.** It promoted after two missed sweeps -- twenty seconds into a
sixty-second lease -- so a holder cut off from the steward rather than
stopped kept taking writes under a lease that was still good while the
new holder took writes too, and what the old one took meanwhile was
cut at its demotion. Nothing the drills did produced it: they stop the
process. The steward now waits `CELASTRO_LEASE_SECS` from its last
renewal to that holder and from its own start (a lease the previous
process at its address granted is still good), and says so in the log
(`failover_waits_for_lease`, with the seconds left). A failover is a
lease long at least; the drills set the lease to ten seconds.

## 0.61.0 — 2026-09-20

**A plain `count(*)` is the holders' counts summed.** `SELECT count(*)
FROM c` with no predicate, no `GROUP BY` and no key prefix asks each
holder for its shard's live count at the statement's instant (a new
wire call, `count`) and sums them: on the five-node suite's corpus the
scan took 2.3 s and twenty seconds at thirty-two concurrent; the count
is milliseconds. `EXPLAIN` says `counted, not scanned` per shard. A
holder running a release from before this answers that it does not
know the call, and the coordinator scans as before. Everything with a
predicate or a group stays a scan: a shard's range holds any number of
tenants, so `GROUP BY tenant` is not per shard.

## 0.60.0 — 2026-09-20

**A holder's return no longer costs minutes; a manifest carries the
catch-up floor; an insert refused by one holder says which rows landed.**
Every failure shape of the five-node suite that took a holder away
crawled for two to four minutes after it came back: the demotion
opened the old holder's copy "not caught up" and the new holder
re-copied the whole shard to it from nothing while serving it. A
holder's shipper now keeps `CONFIRMED` in the shard's directory -- the
instant every live follower has confirmed, written at most once a
second and never ahead of the truth -- and a demotion cuts the copy
there: the log records above it, which this node took after the
promotion and nobody confirmed, are dropped and the log rewritten, and
the copy follows from that instant. A copy whose sealed segments hold
versions above the cut starts from nothing, as before, and the log
says which. The catch-up chunk is 500 rows rather than 2,000, so a
catch-up that does run interleaves with the holder's statements.

The catch-up floor (0.59.1's delete floor) is in the manifest now,
trailing, so a holder restarted after a compaction that forgot a
delete still starts a follower from before it from nothing; a manifest
without the field reads as zero and is rewritten with it at the next
seal. A merge raises it too, and replaces the merged shard's shipper:
the rows a merge absorbs keep their own, older timestamps, so a
follower catching up from where it stood never received them, and a
copy promoted after a merge answered two rows of three (one run in
four of the documented examples, since 0.59.1's floor stopped
resetting every follower). And a kept copy's key range now follows
the map at a split or a merge: left with the old one, the copy masked
out the rows the merge absorbed even once they arrived.

An `INSERT` over several holders is per holder: when one refuses (a
node away, a lease run out) the rows the others took have landed, and
the error now says so -- `NOT written: 2 on tcp://… (did not answer);
written: 2 here, 3 on tcp://…` -- with the note that inserts are
idempotent by key, so running the statement again is safe. The
five-node seed lost 88 documents to a refusal it reported without
saying what had landed.

*Format:* the shard manifest gains a trailing field; 0.59.x reads a
0.60.0 manifest (it ignores the tail) and 0.60.0 reads older ones.

## 0.59.1 — 2026-09-20

**A promotion keeps the copy's files; a follower catching up holds no
write; a follower away across a seal catches up from where it stood.**
Three things the five-node suite's failure shapes found in 0.59.0.
The promotion of a copy retired the copy's segments, which unlinks
them, and then opened the shard from a manifest naming files that were
gone: `segment … named by the manifest is missing`, and the shard
stayed down with its holder. The tests' copies had never sealed a
segment; a copy on a real node has. The copy now closes and its files
are the shard's; a promotion that still fails puts the copy back as a
follower and says why. The demotion had the same retirement. Second,
a write waited for a follower that was catching up, so a node back
from a minute away held every write to the shards it follows until
its copy was complete -- minutes, under a load. Only a live follower
holds the acknowledgement now; a catching-up one is like an away one:
the write is on the holder's disk alone, `SHOW HEALTH` says so, and
the write reaches the copy behind its catch-up. Third, a follower
whose copy stood before the shard's version floor was reset and copied
from nothing, and every seal raises that floor to now -- so under a
write load every follower that had been away at all was copied from
nothing on return. The reset is off a catch-up floor now, raised only by
a compaction that dropped a dead row, which is the only way a delete
is forgotten. The steward also logs a candidate it passes over for
being behind (`failover_candidate_behind`).

## 0.59.0 — 2026-09-20

**The lock on a `Db` is the crate's own, and a wire read is never held
back by a waiting writer.** The standard lock lets a writer that waits
hold every new reader behind it -- fair on one machine, and across
nodes the closing of a cycle: a statement holds its coordinator's
shared lock while it waits on another node's shard, and that node's
wire read waited behind a writer that waited for that node's own
statements, waiting on the first. Under the five-node suite's mixed
load every node had such a writer at any moment (a forwarded insert,
a statement's own write), and reads and writes alike waited out the
thirty-second deadline on idle CPUs. `celastro::lock::RwLock` has the
standard API and one more entry, `read_served`, which yields to a
writer that holds the lock and to none that waits; the wire's shard
reads take it, so a wait across the network ends at local work and no
cycle closes. Writers do not starve: the statements starting on a
node still yield to a waiting writer. The replication step -- the
next catch-up chunk of every follower -- is a read now and runs under
the served lock too; under the exclusive one its try found a reader
every time on a node under sustained reads, so followers never caught
up and every write waited for their confirmation until the deadline.

*Upgrading an embedded use:* wrap the database in
`celastro::lock::RwLock` rather than `std::sync::RwLock` -- the
console and the wire take that type. Everything else is unchanged.

## 0.58.5 — 2026-09-20

**The catalog fetch and `SHOW HEALTH` take the shared lock.** The
sweep's catalog fetch, a read, took the write lock on the peer it
asked, so every node queued a writer every few seconds; and `SHOW
HEALTH` dialled every peer under the write lock, holding every reader
on the node for as long as a peer took to answer. Both are reads now.
With 0.58.3 and 0.58.4 this is what the five-node suite's stalls came
to: a queued writer somewhere, readers behind it, and a statement
holding its read lock across the fan-out on another node.

## 0.58.4 — 2026-09-20

**Eight connections to a peer, and a call waits for one only within
its deadline.** One connection carried every call to a peer, one call
at a time, behind a lock without a deadline: on five real nodes under
thirty-two concurrent lookups the calls queued on it for far longer
than any of them took -- a slow scan ahead of a point lookup's counters
call, and the wait for the lock not counted against the statement's
budget -- and the statements went past their deadlines waiting for a
connection. A node now keeps eight connections to each peer; a call
takes the first free one or waits for one within its deadline and is
refused naming the wait.

## 0.58.3 — 2026-09-20

**The periodic work never queues for the lock.** On five real nodes,
idle, thirty-two concurrent point lookups took thirty seconds each and
a node's own health call eighteen: a lock wait, not work. A statement
holds its coordinator's read lock while it waits on other nodes; the
lock prefers writers, so a writer waiting on it holds every new reader
behind it; and the replication step, the seals and the compactions
took the write lock every second on every node. A statement on one node
waiting on a second, whose wire reads waited behind its queued writer,
which waited for its own statements, which waited on the first: cycles
a deadline broke. The periodic work now tries the lock and skips the
tick when it is busy (a built seal or compaction waits up to five
seconds before it insists; a sweep's merge skips the peer until the
next sweep), so no housekeeping writer ever holds a reader behind it.
What remains is the statements' own writes, brief and lock-free of the
network since 0.53.0.

## 0.58.2 — 2026-09-20

**A pooled connection idle for ten seconds is asked a hello before its
next call.** On five real nodes, every node restarted, the coordinators'
pooled connections to the restarted peers were half-open: a write into
one succeeded and the read waited out the statement deadline, and every
concurrent call to that peer queued behind it -- a fan-out that stalled
thirty seconds on an idle cluster, point lookups timing out at eight
workers. The hello costs a round trip once per idle connection and,
when it fails within two seconds, a redial.

## 0.58.1 — 2026-09-20

**The steward renews leases on a thread of its own.** The renewals rode
the catalog sweep, and on five real nodes under an ingest the sweep ran
past the lease: every holder refused writes for a lease the steward was
late to renew. Now a thread renews every peer's lease every quarter of
`CELASTRO_LEASE_SECS`, five seconds a call, no lock taken; the sweep
keeps the failover decision. The README's cargo install moved into the
Deployment table.

## 0.58.0 — 2026-09-19

Every shard has a follower, a write is acknowledged on two disks, and a
follower can be promoted -- by hand, or by the steward when the holder
stops answering. High availability, hence a minor; the catalog format
is 9 and the wire speaks version 6 (older peers are spoken to at 5).

**Followers.** A collection has `replicas = 2` by default: every shard
has its holder and one follower, the next data node in the placement
order (`WITH (replicas = n)` at `CREATE COLLECTION`, `ALTER COLLECTION
... SET (replicas = n)` to re-plan; a cluster of one has no follower).
A follower holds a real copy of the shard under
`collections/<c>/followed/`, fed by the holder's log: every record the
holder writes is shipped to its followers after the holder's own fsync,
and in `CELASTRO_REPLICATION=sync` (the default) the client is
acknowledged only once every live follower has it on disk; `async`
acknowledges at once. A follower that is away does not hold the
acknowledgement -- the write is on the holder's disk alone, and `SHOW
HEALTH` says `DEGRADED` -- and is caught up from where it stood when it
answers again, in chunks, or from nothing if it is new or too far
behind. Definitions reach followers as they reach holders; a move keeps
the copies (the old holder becomes a follower); a split or a merge
resets the followers' copies, which catch up.

**Promotion.** `PROMOTE SHARD i OF c ON 'follower'` makes the follower
the holder at the next term and the old holder a follower. Every map
entry carries a term; the higher term wins wherever two maps disagree,
so the old holder, back or reconnected, demotes its copy and is caught
up from the new holder, dropping what it took after the promotion --
which was never acknowledged, since its follower had already left it.
`SHOW CATALOG` shows the followers and the term; `SHOW HEALTH` shows
every follower's state and lag and every copy this node follows.

**The steward and automatic failover.** One node is the steward
(`CELASTRO_STEWARD`, or the lowest attached address): it renews every
node's lease on each reconcile sweep. With `CELASTRO_AUTO_FAILOVER=on`
(off by default) the steward promotes the follower with the most
recent copy once a holder has missed two sweeps, and a holder whose
lease ran out (`CELASTRO_LEASE_SECS`, 60) refuses writes until it is
renewed -- so a holder the steward cannot reach is not taking writes
while its follower is promoted. With failover off, promotion is the
operator's and no lease gates a write.

The embedded API's `Db::insert` returns once the write is on this
node's disk; `Db::confirmation()` is what to wait on, with the lock let
go, for the followers.

## 0.57.0 — 2026-09-19

A split picks its own key, and shards merge, hence a minor.

**`SPLIT SHARD i OF c` with no key** takes the middle of the shard's
keys, sealed and in memory alike: the holder picks it, so the
statement issued anywhere splits at the median the holder sees, and
the answer names the key. A shard with fewer than two keys is refused.

**`MERGE SHARDS a AND b OF c`** makes one shard of two adjacent ones on
one node: shard `b`'s rows are rebuilt into shard `a` as segments of
its own (the memtable sealed first, every live row of every segment,
versions layered and deletes carried as a compaction carries them),
shard `a`'s range becomes the union, and shard `b`'s directory goes.
Its entry stays in the map as a merged marker with an empty range that
owns no key, so no shard renumbers and every read, move and rebalance
skips it; `SHOW CATALOG` says `merged away`. Before the range widens,
what an earlier split left in shard `a` outside its range is dropped
for good, so nothing answers twice after a restart. A merge is row work
under the lock, with shard `b`'s rows in memory meanwhile, so name the
larger shard first. Two shards on different nodes are refused naming
the move that brings them together; two that are not adjacent are
refused. A peer that missed either statement learns the ranges from the
holder's catalog at the next sweep.

## 0.56.0 — 2026-09-19

A shard can be split, hence a minor.

**`SPLIT SHARD i OF c AT 'key'`.** Shard `i`, holding `[lo, hi)`, keeps
`[lo, key)` and a new shard -- the next index -- holds `[key, hi)` on the
same node; `MOVE SHARD` then carries it wherever the load should go.
The remedy for a hot shard that a move could only relocate. No row
moves: the holder makes the new shard's directory from the pinned files
of shard `i` -- the export a move takes, linked when the directory is
in the clear and re-sealed under the new name when it is encrypted --
and from then on each shard answers only the keys in its range, the
memtable and every segment alike; the rows outside a range stay on disk,
invisible, counted as dead, and the next compaction drops them. So a
split takes what a hard link takes, under the lock. Issued at any node,
the statement goes to the holder; the holder carries the new map to
every peer, and a peer that was unreachable learns it from the holder's
catalog at the next sweep. `SHOW CATALOG` shows the ranges; `SHOW
SEGMENTS` counts the rows a split left behind as dead until they go.

## 0.55.1 — 2026-09-19

Documentation: every command and every statement with a worked example.

**Two reference documents, run on every release.** `docs/commands.md`
is the command line and the console's HTTP API -- one node, the tools,
a served console, TLS, a cluster on loopback, the image -- and
`docs/sql.md` every statement by what it is for, each with an example
and what it answered. `scripts/examples.sh` runs all of them against a
release build and fails on the first answer that changed, so the
documents cannot drift; it is a gate. The README keeps the quick start
and points at them; its image tags name this release.

## 0.55.0 — 2026-09-19

The last statement that waited under the lock for a holder no longer
does, and deferred work may go back under the lock, hence a minor.

**A delete by predicate waits for no holder under the lock.** Its keys
come from every holder, so a holder that could not be reached was a
select that waited for it under the lock -- the one statement 0.53.0
left there. Now the delete asks every holder whether it answers with
the lock let go, is refused by the one that does not (`DELETE refused
and NOTHING was deleted: <node> ... did not answer`), and selects and
deletes its keys under the lock only once every holder has answered.
For that, deferred work may now go back under the lock:
`Outcome::finished_with(&RwLock<Db>)` finishes it, and the console and
the wire use it; `Outcome::finished()` still finishes work that needs
no lock and refuses work that does, naming the other. A holder lost
between the two steps is waited for under the lock, as before.

**A move pins a source elsewhere with the lock let go, and every peer
learns the switch.** The resilience suite's new property -- shards moved
at every step under a load and a scan that never stops -- found a move
asking another node for the pin under the coordinator's lock while that
node waited on the same lock to answer a scan: thirty seconds each, and
a pin made late that refused writes with nobody to abort it. The pin on
a source elsewhere is now asked for as deferred work, within the
statement's budget, and a pin asked for past its deadline is not made.
A node that coordinates moves and holds no shard of the collection was
told of no switch and sent the next move to the old holder: the switch
now reaches every peer that attached. And a write or delete carried to
a holder after the map moved under it follows the holder's refusal to
the node it names, once. The suite's next run found a key acknowledged
and missing from a scan: the scan, planned on the old map, read the
source's copy after the target had taken the write. So the target
fences the source once it holds every file (`fence_move`, a new wire
call an older source may not know, in which case the old window stays):
from then until the map switches, a read of the shard on the source is
refused naming the move, as a write has been since the pin.

## 0.54.0 — 2026-09-19

A partial answer costs one bounded wait for every holder that cannot be
reached, not the whole budget, and a busy node is alive, hence a minor.

**A partial statement pays for the holders it cannot reach once, and
together.** The same ten-node split drill, run again on 0.53.0, showed
what was left: a statement with `partial_results` across the cut named
the near shards missing along with the far ones. A statement's first
step asks every holder for its clock and write counter, one holder
after another, and a fresh connection to a holder behind a partition
waited a fixed five seconds for its hello -- deadline or not -- so five
far holders spent the budget before a near shard was asked. Now the
dial and the hello are bounded by what is left of the deadline, every
holder's counters are asked at once, and under `partial_results` that
step gets half the budget: half to learn who is there, half to read
from those who are. A statement over ten holders with five behind a
cut answers the five it can reach and names the five it cannot.

**A node whose lock is held is alive.** `/api/health` read the catalog
under the database lock, so a statement holding the lock for its
deadline made the liveness probe wait with it, and the pods under the
split were restarted for being busy. The probe now tries the lock and,
when it is held, answers `{"ok":true,"busy":true,"attached":0}` at once:
alive to the liveness probe, not ready to the readiness probe, which
asks for the attached count, and counts nothing.

## 0.53.0 — 2026-09-19

No statement waits for a holder under the lock, hence a minor.

**A statement waits for no holder under the lock.** The split drill at
ten nodes -- two subnets of five, the link cut for ten minutes -- found
two things a statement did under the lock while a holder could not be
reached: a definition's fan-out dialled the far holders one after
another (45 s for five), and a write forwarded to a far shard waited out
the deadline; either held the console, and the pods' liveness probes
restarted them three and four times. Now a definition is applied here
under the lock and carried to the holders as deferred work holding
nothing, every holder at once; a forwarded write and a forwarded delete
by key are carried the same way, holder by holder at once, after the
documents of this node's shards are written; and a collection's spread
to its holders is deferred too. What still waits under the lock is a
`DELETE ... WHERE` whose predicate reaches a holder that cannot be
reached, since the keys are needed before anything can be deferred. The
wire finishes deferred work with its lock let go as well.

## 0.52.1 — 2026-09-19

**The README starts with Docker.** Most readers are not Rust developers:
the first thing on the page is now `docker run` with a volume and a
token, then the statements over `curl`, with the image as a client; the
`cargo install` path follows under its own heading. Every line of the
Docker block runs as written against the published image.

## 0.52.0 — 2026-09-19

Wire version 5, hence a minor.

**Wire version 5, and the zombie fence's server half.** A frame carries
its caller's address and epoch after the token, and a holder that has
seen a newer process at that address refuses the call, naming both: the
older process's own forwards are what this stops, as the client half of
0.50.x stopped calls toward it. The bump is compatible: a node's hello
says the newest version it accepts, a node sends 5 only to a peer that
accepts it and 4 otherwise, and every node accepts 4, so a rolling
upgrade across the bump talks in both directions throughout. A process's
identity for the frames is its address and epoch, carried by every
connection the engine makes.

## 0.51.0 — 2026-09-19

The move's copy off every lock, hence a minor.

**A move's copy holds no lock, on either end.** `MOVE SHARD` did its
whole copy under the coordinator's lock, and the target pulled under its
own; a drill found the cycle that makes: a move issued to a third node
took the whole deadline while a write forwarded through the target to
the coordinator waited on the coordinator's lock, which waited on the
target's. Now the checks and the pin happen under the lock and the copy
is deferred work holding nothing: the target pulls the pinned files
without its lock, adopts them under it, switches the map on every node
itself (the source last, which releases the pin), and the answer is what
it switched. `REBALANCE` pins its moves under the lock and copies them
one after another the same way. Writes to a moving shard are refused
while it is pinned, naming the move, and no longer wait behind a
coordinator that is also the source. The move's long calls use
connections of their own: a pool connection is one call at a time, and
a pull that took the copy's length on it made a write forwarded to the
same peer wait for it under the source's lock, which the target's switch
back to the source then waited on -- the shape the busy-source test
now pins.

**Certificates carry key identifiers.** `celastro tls init` writes
`subjectKeyIdentifier` and `authorityKeyIdentifier` (the leading 160
bits of the SHA-256 of the key), so a client that keeps two CAs under
one name for a rotation's middle step -- OpenSSL, so curl and Python --
picks the one that signed the leaf; verified with `openssl verify`
against a bundle of two same-named CAs in either order. A node tried
every anchor already.

## 0.50.1 — 2026-09-18

**A pooled connection to a superseded process is let go.** 0.50.0
checked fresh connections only; the drill on real machines found a
write riding a connection that a hello had just shown to be the older
process. A hello that shows an epoch older than the newest seen at the
address now drops the connection it came over, and a pooled connection
whose process has since been superseded is dropped before the next call;
either way the next call dials afresh and is refused.

## 0.50.0 — 2026-09-18

The zombie fence's client half, hence a minor.

**A fresh connection asks who answers.** Two processes at one address --
a pod replaced while its predecessor still runs, the old one reached
through a stale name -- took a write on the old one that the new one
never saw; the drill on real machines showed it. A node now asks a
fresh connection for a hello before the first statement goes down it,
and refuses an answer whose epoch is older than the newest it has seen
at the address: one round trip per connection, and connections are
pooled. The statement is refused naming the older process, or the shard
is missing under `partial_results`; `SHOW HEALTH` still names the
process from a hello, since a hello is how it looks. The other half --
a holder refusing a call from a stale caller -- needs the caller's epoch
in the frame and waits for the next wire version. Hello itself answers
without the database lock when a statement holds it, from the process's
fixed identity, since the pull of a move asks it of a source that holds
its lock for the whole move.

**A drill-only clock offset.** `CELASTRO_CLOCK_OFFSET_MICROS` offsets
every read of the wall clock in the process, so one node of a cluster on
kind sees a jumped clock without the kernel's moving; the start-up log
says so, and it is never for a database anyone relies on. The clock-jump
scenario runs with it on kind, and ran for real on a cluster of virtual
machines with one node's kernel clock set an hour ahead and then two behind: the
others flag the node within a sweep, an `ATTACH` of it is refused
naming NTP, its writes commit an hour ahead and read back through any
node, the other nodes' clocks do not follow it, and set behind its own
HLC runs ahead of its wall until the wall catches up.

## 0.49.0 — 2026-09-18

The seal builds off the lock, hence a minor.

**A seal no longer pauses the node.** A seal of a large vector memtable
built its graph under the write lock -- 146 s for 50,000 vectors in the
recovery drill -- and the node answered nothing meanwhile, which to its
peers was a partition. With the console's maintenance thread running, a
seal that is due now freezes the memtable under the lock (its rows laid
out for the build, the segment ids reserved, the write-ahead log rotated
aside, the memtable kept where reads and deletes find it), builds the
segments on the maintenance thread holding nothing, and installs them
under the lock again: compaction's shape, for the seal. Two frozen seals
the thread has not caught up with are the bound; past it the write path
builds inline, which is backpressure. `FLUSH` still seals everything
inline, frozen memtables first. The resilience suite's new check writes
20,000 vectors through the console and answers point lookups while the
graph builds: 22 s of build, 106 lookups meanwhile, the slowest 67 ms.
Its first run found a deadlock in the maintenance step itself, a lock
guard living through the build as an `if let` temporary; fixed before
the first tag with it.

**The write-ahead log rotates.** A frozen memtable's log is renamed
aside as `wal.<n>.log` and a fresh one takes the rows written meanwhile;
the rotated file goes when its segments are installed, and a process
that ends before then replays it with the live log at the next open.

## 0.48.0 — 2026-09-18

The resilience drills' findings, hence a minor: a rotation that rolls,
a map that heals after a move under a split, and an empty volume that
is a missing shard.

**A token rotation rolls.** A rotation by one rolling update deadlocked:
the first pod on the new token could attach nobody, was never ready, and
the rollout never moved, leaving it partitioned. `CELASTRO_WIRE_TOKEN_ALSO`
is a second token the wire accepts, and a rotation is three rollouts
(accept the new token too; send it and still accept the old; drop the
old), each leaving every pair of nodes a token in common. The chart's
`wire.tokenAlso` carries it; SECURITY.md has the steps.

**A holder without the collection is a missing shard.** A node back on
an empty volume adopts nothing older than its directory, and a statement
that reached it for a shard was refused with "no such collection". It is
now the same as a holder that does not answer: refused naming the shard
and the empty volume, or missing under `partial_results`.

**A move's map switch that missed a node is a note**, since the node
learns the map from the holders' word when it reconnects.

**A move made across a split reaches the far side's map.** A move
within one half of a split completed there and left the other half's
map naming the old holder, which then refused the forwarded writes as
not its own; the reconciliation carried definitions and drops, not
placement. It now carries a holder's own word: for a collection both
nodes have, a shard the peer's map puts on the peer moves to the peer in
this node's map, and a shard this node's map puts on the peer that the
peer's map puts elsewhere moves there (`Db::reconcile_from`). A shard
both claim to hold is kept and named for a `MOVE SHARD`.

**Clock skew is measured as the hello is read.** A sweep computed a
peer's skew after the call that followed the hello, and accused a peer
of eleven seconds that were a wait, not skew. The wire stamps the receipt
instant on the hello, and `ATTACH`, `SHOW HEALTH` and the sweep compare
against that.

## 0.47.1 — 2026-09-18

Two fixes the resilience suite's first drills found.

**A node attaching its peers at start dials nothing under the lock.** The
mixed-version drill, run as a scenario of the resilience runner, found
the console of the last pod rolled blocked for 45 s: `ATTACH` fetched the
peer's hello and catalog under the write lock, and a peer that vanished
between the dial and the attach -- the next pod of the rollout -- held
every statement for a deadline. A node at start now fetches both first
and attaches with `Db::attach_prepared`; the operator's `ATTACH NODE`
still dials, bounded by its deadline.

**`hello` reports the wall clock, not the HLC.** The HLC runs ahead of
the wall by whatever a peer's timestamps pushed it to, which is not skew,
and two pods on one kernel accused each other of ten seconds by it. `SHOW
HEALTH` now names an HLC that runs more than a second ahead of the wall
clock, since every commit from then on carries it; where the ten seconds
came from in that drill is an open question in the design notes.

## 0.47.0 — 2026-09-18

The reconciliation proven, mixed versions drilled, and a cluster-wide
backup, hence a minor.

**The reconciliation converges, and a property test says so.** Random
histories of creations and drops over three nodes, reconciled pairwise in
random orders until nothing changes, end on every node with what the
instants say. The test found two holes in 0.46.0's merge: a drop applied
by reconciliation stamped a tombstone at *now*, so a re-creation made
between the drop and the sweep was dropped too; and an index made on an
incarnation of a collection that was dropped and re-created could reach
the live incarnation through a node that had not yet heard of the drop,
and then outlive it. An index now records the incarnation it was made on
(catalog format 8; 2 through 7 are read as before), and a collection's
tombstone takes every index made on an older one, wherever it has been
merged to.

**Clocks and processes are checked at the door.** `hello` carries the
node's clock and the epoch of the process behind the address. `ATTACH
NODE` refuses a peer whose clock is more than five seconds from this
node's, naming both, since every timestamp and every tombstone compares
by it; `SHOW HEALTH` shows each peer's offset and flags one past half a
second. A hello with a newer epoch is a restart, said once; an older
epoch after a newer one is a second process answering at the same
address -- a pod replaced while its predecessor still runs -- and `SHOW
HEALTH` and the sweep's log say so. Detection, not fencing: a request
frame carries no epoch yet.

**The sweep dials sixteen peers at a time**, outside the lock, so a sweep
over many unreachable peers takes one connect timeout and not one per
peer.

**Mixed versions: a catalog crosses the wire in the format the peer
reads.** A drill with 0.45.0 and 0.46.0 in one cluster showed a shard
move from the newer node to the older refused as "catalog format version
7 is not readable": the move ships the definition as a catalog in the
sender's format. `hello` now carries the newest format a node reads and a
catalog sent to a peer is encoded in it; a peer from before the field is
placed by its version. The same drill showed a rollback after the rollout
crash-looping on the same message, which is inherent: a release that
raises the format writes a file the previous one cannot open. Hence
`CELASTRO_CATALOG_FORMAT`, which pins the written format below this
build's for the first days on a release, at the cost of what the newer
fields carry.

**`BACKUP CLUSTER TO`: one instant on every node.** Backed up node by
node, a cluster is a set of instants with no cut through it. `BACKUP
CLUSTER TO '…'` sent to one node backs every data node up at one instant
that node chooses and carries to the others; `BACKUP TO '…' AS OF <ts>`
is the per-node form it runs there, and is refused for an instant further
ahead of the node's clock than a cluster allows. The set restores with
`RESTORE FROM '…' AS OF <ts>` on each node.

**Per-shard counters.** `celastro_shard_reads_total` and
`celastro_shard_writes_total`, by collection and shard, on the metrics
page: what shows a hot shard.

**Certificate expiry is named ahead of time.** `SHOW HEALTH` says when
this node's certificate and the first of its trust anchors expire and
flags either inside two weeks; `celastro_tls_certificate_expiry_seconds`
and `celastro_tls_ca_expiry_seconds` carry the instants for an alert.

**The wire caps its connections and closes idle ones.**
`CELASTRO_WIRE_MAX_CONNECTIONS` (1024) bounds the threads a peer that
never closes a connection can take, past it a connection is closed at
once and `celastro_wire_connections_refused_total` counts it;
`CELASTRO_WIRE_IDLE_SECS` (300) closes a connection that carried no
frame, and the peer's next call reconnects.

**The resilience suite.** `cargo test --release --test resilience --
--ignored` runs the slow, cluster-shaped properties: the reconciliation
over four nodes and four hundred random histories, no acknowledged write
lost across a node restarting five times under load, a two-hundred-
thousand-row write-ahead log replaying every row, and a cluster backup
under load restoring to one consistent cut. Its first run found that a
stopped node kept serving a connection that never paused -- the
connection thread checked the stop flag only when a read timed out --
and so held its directory against the restart; the flag is checked per
frame now, which is also what makes a busy node stop on SIGTERM.

**A retry's contract is tested.** Every statement delivered twice with
nothing written in between leaves what once leaves; the one shape a retry
can change, a `DELETE ... WHERE` delivered again after a write it did not
see, is named as the contract.

## 0.46.0 — 2026-09-18

The catalog reconciles itself, hence a minor.

**A definition made while a node could not be reached reaches it when it
can.** A drill on a three-node cluster split for ninety seconds showed the
harm a split does: no data diverged, but a `CREATE INDEX` on each side
left the two catalogs different after the link returned, with nothing to
reconcile them. Now every drop leaves a tombstone in the catalog with its
instant, every collection carries the instant it was made, and a node
folds a peer's catalog into its own by name, last writer wins:
`ATTACH NODE` reconciles at once, so a restarted pod catches up as it
attaches its peers, and the console pulls every known peer's catalog
every `CELASTRO_RECONCILE_SECS` (30; `0` turns it off), so a split heals
within a sweep. A `CREATE COLLECTION`, `CREATE INDEX`, `DROP INDEX`, `DROP
COLLECTION` or policy that could not reach a node therefore succeeds with
a note naming it, where it was refused before; an `ALTER`, a move's
placement and a policy's drop are still refused naming the node and the
`LOCAL` statement to run there. `celastro_catalog_reconciled_total`
counts what the sweep changed, and each change is a `catalog_reconciled`
log line.

**A data node that comes back empty does not grow empty shards.** A
collection older than the node's data directory whose map names the node
is data the directory never had, not a definition it missed; the
reconciliation refuses it, says so once, and `SHOW HEALTH` names it `NOT
ADOPTED` until it is restored or dropped. A coordinator, holding nothing,
adopts everything, as it did.

The catalog is format 7 (creation instants, tombstones, the directory's
birth); formats 2 through 6 are read as before.

## 0.45.0 — 2026-09-18

Dedicated coordinators, hence a minor.

**The console serves from a pool of workers.** A thread per connection
cost a clone and a fresh stack for every request, since the console
closes each connection after one; `max_connections` workers are started
once and take connections from a queue bounded to the same cap. Measured
on one node with the survey corpus: point lookups from 820-920 to
970-1,000 requests a second at concurrency 16, with the server's CPU per
lookup from 0.7 to 0.65 ms.

**A query is prepared once against the SQ8 codes.** The graph search
scored each candidate by decoding its code; now the query's parts of the
distance are folded out once and a candidate costs one AVX2 pass over
its bytes. The ranking is unchanged to float rounding (a test says so),
and the gain is small -- hybrid queries from 7.7 to 7.4 ms of CPU each
-- because the search is bound by the memory latency of fetching random
candidates' codes, not by the arithmetic, the same finding as the build's.

**`CELASTRO_ROLE=coordinator`.** A node that holds no shards and only
coordinates: a placement, `REBALANCE` and `MOVE SHARD` never land a
shard on it (they refuse by name), every definition -- a collection, an
index, a tier, a policy, a shard's move -- reaches it so it plans over
the data nodes' shards exactly as they do, and `FLUSH`, `COMPACT` and a
lifecycle run go to the holders only. Its role travels in the wire's
`hello`, is kept in the catalog (format 6; a catalog from an earlier
version reads as before), and shows in `SHOW HEALTH`. A node from
before roles is a data node. `DbOpts::role`, `engine::Role`. The chart's
`coordinators.replicas` runs them as a second StatefulSet with the
console Service over them alone. A coordinator pulls the catalog of every
node it attaches (a new wire call; a node from before it answers nothing),
so one that arrives late or restarts from an empty volume plans as soon
as it has attached. How many is measured in docs/design.md:
one per four or five data nodes for hybrid traffic on small shards, more
per coordinator as shards grow, none for point lookups.

## 0.44.0 — 2026-09-18

Operations: what a node says about itself and what it does when the
disk misbehaves; a minor.

**`SHOW HEALTH`.** From any node: this node (version, collections, shards
held, whether its directory is still there, seal failures), every
attached node dialled once with whether it answered and how fast, every
shard with its holder and whether that holder answered, and a summary --
what an operator asks first when a statement was refused naming a shard.

**Structured logs.** What `serve` and the wire say -- a connection
dropped, an accept failing, a compaction done or not installed or
failed, a wire connection that could not be set up, a seal that failed
-- is one line per event on stderr with an ISO 8601 timestamp, a level
and `key="value"` fields, or a JSON line each with `CELASTRO_LOG=json`.
`celastro::log` for the library.

**Backpressure.** A shard holding more flat segments than
`CELASTRO_COMPACTION_DEBT` (32) makes each write to it wait
`CELASTRO_COMPACTION_DEBT_WAIT_MS` (20) per segment past the debt, a
second at most, so a load cannot run further ahead of compaction than
reads can bear; `celastro_backpressure_waits_total` and `_seconds_sum`
count it.

**`BACKUP TO '<dest>' KEEP <n>`.** After the copy, this node's backups at
the destination beyond the newest `n` are removed -- the record first, so
a prune that stops halfway leaves an incomplete backup rather than a
complete one with holes -- and then the pool segments of the shards this
node holds that no remaining backup of any node names. The chart's
`backup.keep` puts it on the CronJob.

**A refused write leaves no record and no row.** A full disk showed the
gap: a batch whose log append failed partway had the records that fit
replayed on the next reopen -- 289 rows of a statement the client was
told had failed -- and a seal that failed in the middle of a batch failed
the statement with half of it applied in memory and all of it on the log,
so the running node showed 384 rows a reopen did not. Now the log is
marked before a statement's records and cut back to the mark when an
append or the sync fails, so nothing of a refused statement survives; a
seal that fails does not fail the write that triggered it (that write is
on the log and in memory, which is what was promised) but is counted
(`celastro_seal_failures_total`) and retried by the next write; and the
seal runs after a batch's documents, not between them.

**A directory that vanishes under a running node is refused.** The log's
descriptor still accepts bytes into a file no reopen can find, so a write
was acknowledged into nothing and `/api/health` said ok. Now a write is
refused naming the directory when its `LOCK` is not where it was, reads
still answer from memory, `/api/health` answers `ok:false` with the
reason so a probe restarts the node, and `celastro_directory_present` is
0 in the metrics.

**`VERIFY BACKUP '<dest>' [NODE '<address>'] [AS OF <ts>]`.** Every
object the backup's record names is read back and checked against its
recorded size and, for a backup written from this version on, its
SHA-256 -- the record now carries one per object -- and a restore checks
the same as it writes, so a byte flipped in the pool is named and refused
rather than restored. A record from an earlier version verifies by size
and says so. The reading runs after the statement let go of the lock.

**Measured: recovery after `kill -9`.** On the survey corpus with nothing
sealing, a node killed after 4,000, 12,000 and 21,000 acknowledged rows
reopened with exactly those rows in 0.29, 0.80 and 1.41 s; all 50,000
unsealed replayed from a 67 MB WAL in 3.3 s at 335 MB, and reopened in
0.05 s once sealed. The seal of that memtable built its graph under the
write lock for 146 s, which is what `CELASTRO_MEMTABLE_MAX_VECTORS`
bounds; docs/tuning.md says so.

**`GET /api/metrics`.** The process's counters -- statements run and
failed, their total and longest time, requests refused, compactions the
maintenance thread installed and their time, connections served, TLS
handshakes that resumed -- and what the node holds now: collections,
attached nodes, and per collection the shards held here with their
segments and visible documents. Prometheus text exposition, one `TYPE`
line per name, the token required as for every other endpoint.

## 0.43.1 — 2026-09-16

Secrets wipe themselves when dropped; a patch.

**Zeroised on drop.** The data key, the master key (now
`cipher::Secret<32>` in `DbOpts::master_key`, which prints as nothing and
converts from `[u8; 32]`), TLS traffic keys and IVs, session tickets and
the resumption secret, the node's Ed25519 seed, S3 credentials, and the
console's token and the wire client's overwrite themselves with zeros when they
go out of scope, by a volatile write per byte the optimiser does not
remove. Nothing changes for a caller except the type of `master_key`,
which takes `.into()`.

## 0.43.0 — 2026-09-16

The console's token on a network, hence a minor.

**On a network bind the API takes the token in the header only.** With
`--bind` off loopback, `/api/*` refuses a token given as `?t=` in the
URL, since that URL is written into every proxy's and balancer's access
log on the way; `X-Celastro-Token` is the way, as `celastro --url`,
`send`, the chart's probes and CronJob already do. The page and its two
assets still take `?t=`, because a `<link>` and a `<script>` can carry
nothing else; on loopback nothing changes and the URL `serve` prints
stays the way in. **A refused source waits**: a hundred milliseconds more
per refusal in the last minute, two seconds at most, so a token is not
guessed at line rate; the table is bounded to a thousand sources.

## 0.42.1 — 2026-09-16

Two crashes a mutated input could cause, found by a fuzzer that now runs
in the test suite; a patch.

**A count read from the input no longer sizes an allocation unchecked.**
The wire's decoders (a scan answer, a candidate list, a tablet map) and
the shard manifest reserved a `Vec` for the count they had just read; a
count of 2^50 -- a corrupt manifest, or a frame from a peer holding the
wire token -- aborted the process on the allocation before any bounds
check. `codec::get_count` refuses a count larger than the bytes left.

**Every parser is fuzzed.** `src/fuzz.rs` (tests only) is a seeded
mutator; sixteen tests feed thousands of mutants of valid input to every
parser that reads the network or a file -- X.509, PEM, the handshake
messages and tickets, the console's request heads, the S3 responses, the
wire's answers, the manifest, the WAL, delete logs, the catalog,
segments and their columns and adjacency indexes, the graph, the codes,
the variant, JSON, SQL and text queries -- and ask only that they return.

## 0.42.0 — 2026-09-16

The archive client speaks TLS, hence a minor.

**`CELASTRO_ARCHIVE_ENDPOINT=https://...`.** The `archived` tier and
`BACKUP TO 's3://...'` reach an S3-compatible store over the in-tree TLS,
the chain verified against the PEM bundle `CELASTRO_ARCHIVE_CA` names or,
absent that, the system's bundle in its usual places; a certificate the
parser cannot read in a bundle is skipped, a wildcard in the leftmost
label names what RFC 6125 says it does, and every address the endpoint
resolves to is tried in turn. The chart's `archive.caSecret` mounts the
bundle, since the image carries none. Checked against a TLS fake in the
tests and against `s3.us-east-1.amazonaws.com` from the tree: the
handshake verified Amazon's chain against the system bundle and the
signed request came back with the 403 a made-up key earns. A plain
`http://` endpoint is unchanged; a bare `host:port` still means http.

## 0.41.0 — 2026-09-16

The command is `celastro`, hence a minor.

**`celastro-cli` is now `celastro`.** The tool is the only binary anyone
uses, so it takes the crate's name; the older `celastro` REPL, a subset
of it (`--dir D` was `--dir D repl`, `--file F` was `run F`, `--demo` was
`demo`), is gone. `cargo install celastro` installs `celastro` and, for a
release or two, `celastro-cli`, a few lines that print one to stderr
saying the name changed and exec the `celastro` beside them; the image carries both `/celastro`
(its entrypoint now) and `/celastro-cli`, so a chart or a script from
before this release keeps working until it is updated. Every document
and the chart say `celastro`.

## 0.40.0 — 2026-09-16

TLS session resumption, hence a minor. Nothing changes for a plain
console, and a client of an earlier version speaks to this one as before.

**Session tickets.** After every TLS handshake the server sends a
NewSessionTicket, and a client that connects again within a day offers it
and skips the certificate flight: one round trip and no signature on
either side. The ticket is the PSK, its issue time and its age mask,
sealed under a key HKDF derives from the node's TLS private key, so every
node serving the same certificate -- a chart release's pods behind their
Service -- opens every other's tickets, and a restart changes nothing. PSK
with (EC)DHE only; the client keeps one ticket per name, address and set
of trust anchors, in process, so `celastro-cli --url`'s session, the
wire's dials and a browser's requests all resume; a ticket the server
cannot open is a full handshake, a binder that does not verify a refusal.
Pinned to RFC 8448's resumed trace (the resumption master secret, the
PSK, the early and binder secrets, and the binder over the 477 octets of
the truncated ClientHello), and checked against OpenSSL 3.0's `s_client`
(`Reused, TLSv1.3`) and curl 8.5 on the second of two requests.
`tls::resumed_handshakes()` counts them.

**A Deployment section in the README**: one process, a container, VMs
and Kubernetes side by side, with what each holds and how clients reach
it.

## 0.39.0 — 2026-09-16

The command line is a client, hence a minor.

**`celastro-cli --url <URL>`.** `exec`, `run`, `repl` and `catalog` then
talk to a console that is already serving -- on this machine or another,
one node of a cluster or a load balancer over all of them -- and render
the answers as the local path does: rows as a table with the count, the
missing shards and the cuts; an acknowledgement as its line; a plan as
its text; a refusal as `error:`. `--json` passes the console's document
through unchanged. The token is `CELASTRO_TOKEN`, or the `?t=` of the
URL `serve` printed, so that line pastes as it is; `https://` is
verified by the CA in `CELASTRO_TLS_CA`. `send` takes the token from the
URL too now, and always sends the `Content-Type` the console requires.
`--url` with `--dir`, or with a command that opens, serves or makes
files, is a usage error. The README says how a cluster is reached, on
VMs and on Kubernetes. `QueryResult::of_rows` for the library.

## 0.38.0 — 2026-09-16

Aggregates, hence a minor.

**`count(*)`, `count(path)`, `sum`, `min`, `max`, `avg` and `GROUP BY
path`.** One row, or one per group, over every row the predicate admits
-- a text match or a distance threshold in `WHERE` included. Each shard
folds its own rows into partials (an ungrouped `count(*)` never decodes
a document) and the coordinator merges them, so a count over a cluster
moves numbers, not rows. `ORDER BY`, `LIMIT` and `OFFSET` apply to the
groups by the result's fields (an alias, or the call as written:
`count(*)`, `sum(n)`); without `LIMIT` every group is returned. Nulls
and absent paths are skipped by everything but `count(*)`; `sum` and
`avg` refuse a non-number, `min` and `max` a group of mixed kinds; a
bare path beside an aggregate is refused at parse time unless it is the
`GROUP BY` path; a ranked `ORDER BY`, `COLLAPSE BY` and `AFTER` are
refused. A node holding a shard folds the statement it is sent, so a
cluster with a node older than this version answers a count with a
refusal naming the shard rather than a wrong number. `Projection::Aggregate`,
`AggFunc` and `Select::group_by` in the AST.

## 0.37.0 — 2026-09-16

Encryption at rest, hence a minor. Nothing changes for a database opened
without a master key.

**Every file under `--dir` can be encrypted.** With
`CELASTRO_MASTER_KEY_FILE` (32 bytes, or 64 hex digits; `celastro-cli key
master` writes one) or `CELASTRO_MASTER_KEY` set, an empty directory
draws a data key, keeps it in `<dir>/KEY` wrapped under the master, and
from then on writes every segment, delete log, manifest, WAL record,
`RANGE` and `CATALOG` -- and every object the archived tier puts in a
store, every backup and every export -- as ChaCha20-Poly1305 frames under
a per-file key derived from it. Opening without the master is refused,
and so is offering a master to a plain directory that holds data: a plain
database takes a key by `celastro-cli export` and `import` into a fresh
directory opened with one, and an import between two encrypted databases
recodes from one key to the other. Backups of an encrypted database are
encrypted and carry `KEY`; `RESTORE FROM` adopts it under the same master
and refuses a plain database or another master. A cluster's pods share
one data key: `celastro-cli key init <FILE>` writes it wrapped, and
`CELASTRO_KEY_FILE` names it at every pod's first start, so `MOVE SHARD`,
`REBALANCE` and a restore on another pod work as before; the chart's
`encryption.existingSecret` mounts the master and that key. `celastro-cli
key rekey <KEY> <MASTER>` rewraps the data key under a new master without
touching a data file. `DbOpts::master_key` and `DbOpts::key_file` for the
library; `Segment`'s source gains an `Encrypted` wrapper that opens only
the frames a ranged read touches, so an archived segment faults in as it
did. SECURITY.md says what it protects and what it does not. Measured on
the survey's corpus (50,000 documents with text and 128-d vectors, 250,000
edges, one node): the batched load 18.1 s plain and 22.2 s encrypted,
`COMPACT` 92 s and 94 s, the reopen 0.24 s and 0.43 s, and every read
class -- point lookups, BM25, vector, hybrid, a two-hop walk -- within
noise of plain at concurrency 1 and 16. Verified on kind: three pods
sharing one key from `encryption.existingSecret`, a shard moved, a backup
taken, no plaintext on the node's disk, a pod restarted opening under
the key, and the same pod refusing to start with the Secret removed.

**Smaller.** The README's quick start shows a server and a client on the
same machine, and its transport-encryption section is a paragraph
pointing at SECURITY.md, which now states what the TLS does not do
(resumption, client certificates, HelloRetryRequest, 0-RTT, key update).

## 0.36.0 — 2026-09-15

Compaction runs itself, hence a minor.

**`serve` compacts on its own.** A maintenance thread asks every second,
under the write lock for a moment, whether any shard has a compaction to
do; builds it with no lock held -- the inputs are pinned by their
handles, so a minute of merging costs the statements nothing -- and
installs it under the lock, where a shard that moved on meanwhile (a
`COMPACT` took the inputs, a drop) declines it. One job at a time, a
log line each, `CELASTRO_AUTO_COMPACT=off` to leave it to `COMPACT`.
Since 0.33.0 seals are flat and the graphs were built only when
`COMPACT` was said; now they are built as the flat segments gather.
`Db::compaction_reserve`, `compaction_build`, `compaction_install` and
`compaction::{reserve, build, install}` for the library. Watched on a
node with twelve flat segments of 50,000 vectors: three merges of four
in about 30 s each while inserts went on at 10-30 ms and vector queries
at 7-60 ms, which then settled from 45 ms over the flat segments to 8
ms over the graphs.

## 0.35.0 — 2026-09-15

The wire's port, and the second half of the build speed; hence a minor.

**The wire's default port is 2352.** `tcp://host` means `tcp://host:2352`
in every address (`CELASTRO_NODE`, `ATTACH NODE`, `MOVE SHARD`,
`CELASTRO_ATTACH`), `serve --shard-bind ADDR` binds `ADDR:2352`, and the
chart's `wire.port` defaults to it; a port given explicitly is used as it
is. Until now every example said 9000 and no default existed
(`wire::DEFAULT_WIRE_PORT`, `wire::with_default_port`).

**The distance kernels use AVX2 and FMA where the CPU has them**
(`std::arch`, detected once at first use; the portable loop elsewhere),
and the HNSW build keeps each link's distance beside it, so pruning an
overflowing neighbour list measures nothing twice. `COMPACT` over 50,000
vectors 115 → 94 s; recall on the harness identical (0.865 mean, 0.60
worst over 100 replayed queries). The build is bound by the cache
misses of some 35,000 random vector reads per inserted node, not by
arithmetic: a prefetch a loop ahead changed nothing and was not kept.
Scores can differ in their last bits between machines with and without
FMA; every claim the crate pins is within one process.

## 0.34.0 — 2026-09-15

What a lost node costs, narrowed; hence a minor.

**A lost node costs its shards and nothing else.** Every statement opens
with a counters call to every holder (the read-your-writes instant and
the statistics epoch); until now a holder that did not answer it failed
the statement, whatever the statement was about. The call's failure is
no longer the statement's: without `partial_results` the statement goes
on and fails at the first shard call it makes to that node, so a
statement that never asks the lost shards -- a primary-key equality on a
`splits` collection now prunes to the owning shard, as a partition-key
equality always did -- answers. A text query needs every holder's term
statistics and is refused or partial as before. The refusal names the
node and the call for a per-node call, and the shard for a shard call;
it used to say "shard 0" for `counters`, the placeholder that call is
sent with.

**A rolling upgrade rolls.** Attaching a peer refused a different crate
version, so the first pod of a new version could attach none of the old
ones, was never ready (`--attached replicas-1`), and the StatefulSet's
rollout stopped there -- found while upgrading a five-pod cluster on
kind. The wire version each frame carries is the compatibility that
matters and is still checked; the crate versions may differ.

**A node that is coming back is dialled again.** A dial that fails -- a
name that does not resolve, a port that refuses -- is retried with
backoff for two seconds, within the statement's deadline, before the node
counts as gone -- once per node per two seconds, so a dead node costs a
statement one window and not one per call it makes; a pod restarting no
longer costs the statements that arrive while it binds. On kind, a pod
deleted and recreated cost 6 of 59 scans over the 40 s around it; a
pod scaled away and back stays unreachable for the cluster DNS's
negative-cache time (23 s measured), which the retry is not meant to
cover. A name the cluster's DNS has cached as absent is
not covered, and is not meant to be.

## 0.33.1 — 2026-09-15

A performance fix, hence a patch.

**The distance functions vectorise.** `perf` put 78% of an HNSW build in
`distance::dot`: an indexed loop paying a bounds check per element. `dot`
and `l2_squared` run over `chunks_exact(4)` now, the same four
accumulators in the same order (every score bit for bit what it was),
and the compiler turns the lanes into one SSE register. A 16,384-node
graph over 128 dimensions builds in 34 s instead of 85; `COMPACT` over
50,000 vectors in 115 s instead of 247.

## 0.33.0 — 2026-09-15

A default that changes what a load costs, and a lock, hence a minor.

**A memtable seals at the flat tier.** `CELASTRO_MEMTABLE_MAX_VECTORS`
defaults to 4,096 (was 32,768): a seal writes a flat segment and builds
no HNSW graph; `COMPACT` builds it when segments merge past the flat
tier. Measured over 50,000 documents with 128-dimensional vectors: 729 MB
and 148 s before, 219 MB and 10 s after; the whole 300,000-document load
under a 512 MiB cgroup limit completes in 17 s where it was killed at
28,000 documents. Queries over the uncompacted flat segments pay one or
two milliseconds. `Value::heap_size` counts an element's inline slot and
nothing for a scalar, so the byte threshold sees a vector at its size.
`docs/tuning.md` carries the sweep and a starting point for a pod's
`resources`.

**One process per data directory.** `Db::open` holds `<dir>/LOCK` with
`flock(2)` for the life of the `Db`; a second open -- another process,
or a second `Db` in this one -- is refused naming the directory and the
holder's pid, and a crash releases the lock. Off unix the file is the
lock and a crashed holder's is removed by hand.

## 0.32.0 — 2026-09-15

The tunables are named, hence a minor.

**`CELASTRO_INSERT_BATCH`** (1000): how many documents of one `INSERT` a
shard appends before it syncs the log; a statement of more is taken in
chunks of that many, one `fdatasync` each. `DbOpts::insert_batch` for
the library.

**Every performance knob is an environment variable**, read at start by
`celastro-cli` and refused by name when it does not parse: the memtable
thresholds and budget, the residency budget and idle unloads, the
statement deadline, the recall sample rate, the lifecycle interval,
compaction's fanout, cap and dead ratio, the vector quantizer and HNSW
parameters, the flat tier's size, and the console's connection cap
(`CELASTRO_MAX_CONNECTIONS`, `Server::with_max_connections`). Sizes take
`K`/`M`/`G`. [docs/tuning.md](docs/tuning.md) lists each with its default
and when to move it; the chart's `tuning:` map sets them on every pod.

## 0.31.1 — 2026-09-15

A write-path fix, hence a patch.

**A statement of many documents syncs once.** `INSERT INTO t VALUES
(...), (...), ...` appended and `fdatasync`ed the log once per document —
a thousand documents were a thousand disk round trips, 0.6 s on the
box's SSD. The promise is the statement's, so the shard now validates
every document first, appends every record, syncs once, and only then
changes its memory (`Shard::insert_many`, `Db::insert_many`; documents
another node owns are still forwarded one at a time). A thousand
documents: 621 → 19 ms in a script, 880 → 16 ms over the console, one
`fdatasync`. A key that recurs
within one statement takes the one-at-a-time path, so its versions
supersede each other as two statements would have.

## 0.31.0 — 2026-09-15

Reads no longer take the node's one lock, hence a minor.

**Reads run side by side.** `Db::read` (and `read_with`) answer a
`SELECT`, an `EXPLAIN` of one, or either behind `LOCAL` under `&self`;
`Db::is_read` says which statements those are. The console and the wire
hold an `RwLock<Db>`: reads share it and proceed on every core, a
statement that changes something takes it alone. What a read used to
write moved behind its own lock -- the statistics cache, the recall
sample, the cache of connections to other nodes -- the per-shard
statistics a read merges before planning are merged into a copy rather
than into the catalog, and the index accesses a read notes are applied
by the next writer or by the console right after the read
(`Db::apply_touches`, `touches_pending`), so a fault-in still promotes a
demoted index within the request that caused it. `Db::execute`,
`query` and the rest keep their signatures. Measured on four cores
against 50k documents at sixteen clients, before → after: point lookups
887 → 1,195 req/s, BM25 top ten 278 → 609, vector top ten 227 → 542,
hybrid 151 → 471, a two-hop walk with a hybrid rerank 13 → 56 req/s
(p50 1,053 → 205 ms) — the four cores at last.

**`crypto` owns its primitives.** SHA-256, HMAC-SHA-256 and `hex` moved
from the S3 signer into `crypto`, where the TLS key schedule finds them
without reaching up; the same program byte for byte.

## 0.30.0 — 2026-09-15

Backups, and the archived tier on any mount, hence a minor.

**`BACKUP TO '<dir or s3://bucket/prefix>'`** copies the shards this node
holds, pinned at one instant, to a directory or a bucket: sealed segments
into a pool once (a second backup copies only the new ones), and per
backup the catalog, manifests, delete logs, the rows that were in memory,
and a record naming every object; `LATEST` names the newest complete one.
The statement pins under the lock and copies after it — a new
`Outcome::Deferred`, which the console, the CLI and the wire finish once
the lock is let go — so a node backing up keeps answering. **`RESTORE
FROM '<src>' [NODE '<address>'] [AS OF <ts>]`** into an empty `--dir`
verifies every object at its recorded size before it writes, and places
the shards on the restoring node; each node backs up under its own name,
so a cluster's pods share a destination and each restores its own. `CELASTRO_BACKUP_DIR` confines the paths a statement may
name. **`celastro-cli send <URL> <SQL>`** carries a statement to a running
console over http or https (`CELASTRO_TOKEN`, `CELASTRO_TLS_CA`).

**`CELASTRO_ARCHIVE_DIR`** puts the `archived` tier in a directory on any
mount — an NFS volume — behind the same `ObjectStore` trait as the bucket
(`DirStore`: temp, fsync, rename, directory fsync per object), and the
trait gained `list` (`ListObjectsV2` on S3).

**The chart** (0.8.0) takes `archive.existingClaim`, an RWX claim mounted
at `archive.mountPath` (`/archive`) that carries the tier (unless a bucket
is configured) and the backups, and `backup.schedule` with `backup.to`, a
CronJob that sends `BACKUP TO` to every pod's console.

## 0.29.1 — 2026-09-15

A latency fix, hence a patch.

**A request no longer waits for a clock to be accepted.** The console's
and the wire's accept loops slept 25 ms between polls of their listener
(so a SIGTERM would be noticed: a blocking `accept` restarts after the
handler), which put up to 25 ms in front of every connection — a point
lookup measured 25 ms at concurrency one, and so did a BM25 query and a
single-document insert. Both loops now wait on the listener with
`poll(2)` (an `extern "C"` binding beside `signal(2)`'s), which a
connection ends at once and a handler interrupts; the shutdown flag is
re-read within 100 ms at the latest. Off unix the sleep stays. Measured
over the console at concurrency one on four cores, 50k documents: a point
lookup 25.2 → 1.3 ms, a BM25 top ten 24.9 → 4.4 ms, a vector top ten 25.2
→ 5.2 ms, a hybrid top ten 25.1 → 7.5 ms (p50).

## 0.29.0 — 2026-09-15

The chart makes its own TLS material again and the TLS verifies what other
issuers sign, hence a minor.

**RSA and ECDSA chains are verified.** The node's own certificate is still
Ed25519, but the CA above it, any intermediate, and a server the node dials
as a client may sign with RSA (PKCS#1 v1.5 or PSS, SHA-256) or ECDSA P-256:
`crypto::bignum`, `crypto::rsa` and `crypto::p256`, public-key operations
only, pinned against openssl's output under `tests/pki`. A cert-manager CA
that is RSA now works, so long as the `Certificate` asks an Ed25519 key.

**`celastro-cli tls secret <SECRET> <NAME> [<NAMES>] [<DAYS>]`** is `tls
init` for a pod: it reads the service account, asks the cluster's API for
the Secret, and writes one of type `kubernetes.io/tls` with fresh material
when there is none. The API is reached over the crate's own TLS, verified
as `kubernetes.default.svc` — the reason the paragraph above exists — and
the client now answers a `CertificateRequest`, which every API server
sends, with the empty `Certificate` the RFC asks for.

**The chart's `tls.enabled` without a source** no longer refuses: a
pre-install and pre-upgrade hook Job runs `tls secret` with a
ServiceAccount that may get and create Secrets in the namespace, and the
Secret it wrote outlives the release until deleted. `tls.days` is back for
that path. `tls.existingSecret` and `tls.certManager.issuerRef` are as they
were.

## 0.28.0 — 2026-09-15

The TLS is in the tree and the crate is back at zero dependencies, hence a
minor; 0.27.0 and 0.27.1, which carried rustls behind a feature, are
yanked.

**TLS 1.3, written here.** `src/crypto` holds SHA-512, HKDF,
ChaCha20-Poly1305, the 25519 field, X25519, Ed25519, DER, PEM and X.509;
`crypto::tls13` the record layer and both sides of the handshake. One
suite (`TLS_CHACHA20_POLY1305_SHA256`), one group, **Ed25519 certificates
only**; no resumption, client certificates, HelloRetryRequest or key
update. Every primitive is pinned against its RFC vectors and the key
schedule against RFC 8448. `CELASTRO_TLS_CERT`, `_KEY` and `_CA` mean what
they meant; the `tls` feature is gone, so every build has it. It is
unaudited, and the README says so.

**`celastro-cli tls init <DIR> <NAME> [<NAMES>] [<DAYS>]`** writes a CA and a
certificate for `NAME`, `NAMES`, `localhost` and 127.0.0.1, as the four PEM
files `serve` reads.

**The chart's `tls.enabled`** now takes its material from
`tls.existingSecret` (made with `tls init`) or a cert-manager issuer, for
which the emitted `Certificate` asks Ed25519; with neither it is refused at
install, since the chart cannot make Ed25519 material itself. `tls.days` is
gone with the generated path.

## 0.27.1 — 2026-09-15

Documentation only, hence a patch: the crate's page carries the README.

**Leaner documentation.** Every document trimmed in place — the README
for a first reader (what it is, the quick start, each surface in a
paragraph, where the rest lives), the container guide, the chart's README
(its verification history as one list), SECURITY (which now describes the
network bind and TLS as they are) and the design notes (the graph
measurements tightened, the layout completed with the wire, the simulator
and the `tls` module). Stale statements corrected on the way: the console
"offers no flag" to leave loopback, the chart is "one pod", the crate
"takes no dependencies", "moving a shard" is not here.

## 0.27.0 — 2026-09-15

Encryption in transit, behind a feature, and the first dependency, hence a
minor.

**The `tls` feature.** `cargo build --features tls` — the image is built
so — brings rustls with the ring provider, the crate's first and only
dependency, and with it `CELASTRO_TLS_CERT`, `CELASTRO_TLS_KEY` and
`CELASTRO_TLS_CA` (PEM; all three or none) put the wire between nodes and
the console over TLS 1.3: each node serves its certificate, verifies every
peer against the CA by the name it dialled, and `celastro-cli health`
verifies its own console as `localhost`. The tokens stay. Without the
feature the crate is std-only as before, and a build without it refuses to
start with those variables set rather than serve plain. The README's claim
goes from "zero dependencies" to "minimal dependencies", and says which.

**The chart's `tls.enabled`.** A CA and a certificate naming every pod,
both Services, `localhost` and `127.0.0.1`, made once and kept across
upgrades; or `tls.existingSecret`; or `tls.certManager.issuerRef` for a
cert-manager `Certificate`. Off by default, and an install without it is
what it was.

**A node dials its peers outside the database lock.** The attach thread
ran `ATTACH NODE` under the lock, and the dial inside it — five seconds per
peer that is not up yet — held every statement and every health probe
behind it; with the wire's handshake on top, a pod failed its own liveness
probe three times while its peers were starting and was restarted once
per install. The peer is dialled first, without the lock, and attached
only once it answered. A three-pod TLS install now reaches ready in 49
seconds with no restarts, against 77 with one each.

## 0.26.0 — 2026-09-15

A walk ranks, and filters per hop, hence a minor.

**`hops(...)` is a fusion source.** `ORDER BY hybrid(text_match(body, 'x'),
hops(id WITHIN 3 HOPS OF 'p1' VIA cites))` scores each node by the hop it
was first reached at, lower better, and fuses it beside the other sources
under either method; the plan lists the walk as `hops(...)` and the source
as `hops(cites)`. Alone in `hybrid()` it is refused: a walk on its own is
the filter. Bit-identical across shard counts and over the wire.

**`THEN WHERE` gives each hop its own edge filter.** `VIA cites WHERE kind
= 'a' THEN WHERE kind = 'b'` applies the i-th filter at hop i; one filter
still applies to every hop, and a count that is neither one nor `k` is
refused with the counts. The plan's hop lines say `edge filter i of n`.
The wire's expansion now carries the hop (wire version 4).

## 0.25.0 — 2026-09-15

The console serves connections at once, hence a minor.

**A thread per connection, the lock around the statement.** The console
used to serve one connection at a time on one thread with the database
locked for the whole request, which was right for a console on loopback
and wrong for one behind a Service: a client slow to send or to read
stalled every other. Now each connection has a thread, at most sixty-four
at once (the listener stops accepting past that and the kernel's backlog
holds the rest), and the database is locked only around the statement and
the persist that follows it — reading the request, parsing, the guards and
writing the answer all happen outside it. Statements still serialise per
node: the engine is single-writer, so a node runs one statement at a time
whatever the thread count; what the threads buy is that the one running
is never waiting on a socket. Pinned by a test over a real listener: an
idle connection does not delay another client's statement.

## 0.24.0 — 2026-09-15

The console can serve a cluster's clients, hence a minor.

**The console binds where it is told.** `celastro-cli serve --bind 0.0.0.0`
(or another routable address) puts the console on a network, for nodes
behind a Service or a load balancer. It then answers the token in
`CELASTRO_TOKEN` — the operator's, at least sixteen printable bytes, the
same at every node — instead of a per-run one, the `Host` allow-list gives
way to the token (a client reaches it by whatever name routes to it) and a
browser's `Origin` must be the `Host` the same request named. Without
`--bind` nothing changes: loopback, a per-run token, the three guards.
`/api/health` now names the node that answered (`node`, null for a node
without an address).

**The chart exposes it on request.** `console.expose=true` binds every pod
on all interfaces, gives them one token from a `Secret` the chart generates
once and keeps across upgrades (or `console.token`, or
`console.existingSecret`), and adds a Service `<release>-console` with a
cluster IP — the console closes every connection after one request, so a
ClusterIP spreads requests per request over the pods, and any pod
coordinates a statement over every pod's shards. Plain HTTP: for a network
you trust, or behind an ingress that terminates TLS.

**Ready means attached.** `/api/health` reports `attached`, the other nodes
this process has verified since it started, and `celastro-cli health
--attached N` exits 0 only once that reaches `N`. The chart's readiness
probe asks for `replicas - 1` when there is more than one pod, so a pod
that has just restarted is not routed to until it can coordinate over its
peers; liveness still asks only whether it serves. The headless Service
now publishes a pod's address before the pod is ready, which that probe
needs: a pod attaches its peers by name, and a name that resolved only
once its pod was ready would have left every pod waiting for every other.

## 0.23.1 — 2026-09-15

A walk got cheaper again and nothing else changed, hence a patch.

**A walk's liveness check is a merge.** The keys a hop found are checked
against an unpartitioned node collection by one pass of the sorted frontier
over each segment's sorted keys, galloping, instead of a binary search per
key; and the coordinator keeps its frontier, seen, present and answer sets
as sorted vectors merged in one pass each, instead of ordered sets of owned
strings. No answer, plan line or cap changes; the `check` time the plan
prints per hop does, by a factor of four to five on the measured hub walks
(41.7 to 119.2 ms down to 6.1 to 33.1 at the second hop), and the walks
with it (75 to 224 ms down to 22 to 108). docs/design.md has the
measurement and what the coordinator still spends.

## 0.23.0 — 2026-09-14

A cluster from a chart, hence a minor.

**The chart runs a cluster.** `helm install celastro chart/celastro --set
replicas=3` starts three pods that attach each other and spread a
collection's shards one per pod; the wire port, the shared token (a
`Secret` generated once and kept across upgrades, or one named) and each
pod's address are the chart's to render. Underneath, `celastro-cli serve`
reads `CELASTRO_ATTACH=tcp://a:9000,tcp://b:9000`: the peers it attaches as
they answer, its own address skipped, retried until they do, so every node
of a cluster can be given the same list and a restarted pod re-attaches on
its own.

## 0.22.0 — 2026-09-14

A capability the cluster did not have, hence a minor.

**A shard moves between nodes.** `MOVE SHARD i OF c TO 'tcp://host:port'`,
from any node: the source pins the shard at an instant and refuses writes
to it naming the move, the target pulls its files over the wire and opens
them, and every holder takes the new map -- the target first, the source
last, which drops its copy only then. Reads are answered throughout.
`REBALANCE c` moves each shard to the node a `CREATE COLLECTION` with no
nodes named would have placed it on, and `DETACH NODE` of a node holding
shards is refused naming the `MOVE SHARD` statements that would empty it.
`LOCAL PLACE SHARD i OF c ON 'node'` is the repair for a holder a move did
not reach. The wire version is 3: four more calls.

## 0.21.0 — 2026-09-14

A walk got cheaper and compaction does one more thing, hence a minor.

**An adjacency index probes.** `USING adjacency (src, dst)` now writes a
region per column into every segment sealed after it — a sorted
value-to-ordinals map — and a hop probes it per frontier key instead of
scanning the column of every unit against the frontier; the liveness check
of an unpartitioned node collection is a key lookup per key instead of a
scan of the key column. `EXPLAIN ANALYZE` says per hop how many units had
no region and were scanned: the memtable, and any segment sealed before the
index was declared. The region is its own component (`adj:<column>`) under
the index's tier, so `SHOW RESIDENCY` lists it and a policy moves it.

**Compaction backfills an index.** A segment lacking a region for an index
the collection declares — sealed before `CREATE INDEX`, of any kind — is now
a rewrite job of its own, oldest first, one per pass. `COMPACT` after
`CREATE INDEX` rebuilds such segments; before this only a size-tier merge
that happened to include one did, which two large segments never had. A
text index on a loaded collection therefore starts answering from every
segment after a `COMPACT`, which it did not before.

## 0.20.0 — 2026-09-14

A new retrieval mode, hence a minor.

**A bounded graph walk in the plan.** An edge collection is a collection
created `WITH (nodes_of = '<node collection>')` — or pointed there later by
`ALTER COLLECTION ... SET (nodes_of = ...)` — with an adjacency index over
its two key columns, `CREATE INDEX ... USING adjacency (src, dst)`. `WHERE id
WITHIN k HOPS OF 'x' VIA cites` then selects the nodes reachable in one to
`k` hops, the start excluded, beside `text_match` and a distance in the same
statement and the same plan: the coordinator walks first, at the statement's
instant, and the neighbourhood reaches every unit as a key set, so the answer
is the same at any shard count and across nodes. `REVERSE` walks the index
backwards, `WITH (undirected = true)` at creation walks both ways, `VIA cites
WHERE kind = 'x'` filters the edges at every hop. `WITH (max_fanout = N)` and
`WITH (max_frontier = N)` bound a hub; a cut is reported on the response
(`cut_walks`, a `CUT` line in both shells and the console) and per hop in
`EXPLAIN ANALYZE`, which shows the frontier after every hop. A dangling edge
is skipped and counted. A walk over an adjacency index below `cached` is
refused naming the tier. The wire version is 2: a walk crosses it as two
more calls, and every node has to run this version.

## 0.19.0 — 2026-09-14

A statement that worked stops working, hence a minor.

**A tier has one name.** `active`, `minimal`, `cached` and `archived` are
the only spellings `CREATE INDEX ... WITH (tier = ...)`, `ALTER INDEX ...
SET TIER` and a lifecycle policy's `MOVE TO` accept; the temperature words
that were taken as aliases — `hot`, `ram`, `memory`, `resident`, `warm`,
`pinned`, `single`, `one_copy`, `cold`, `disk`, `ssd`, `nvme`, `archive`,
`s3`, `object`, `object_store` — are refused naming the four. A statement
that used one has to be rewritten, which is why this is a minor and not a
patch.

## 0.18.2 — 2026-09-14

A fix that moves no behaviour, hence a patch.

**`IN` with a long list is one scan against a set.** It was one equality
scan per literal on a column and one comparison per literal per document
elsewhere, so a list of thousands of keys — the shape a client-side join
produces — cost seconds where the same statement without it cost
milliseconds: measured in docs/design.md, a 7,717-key `IN` over 250,000
documents went from 12 s to 33 ms. Past four literals the list is prepared
once and each document is a lookup; the semantics are unchanged and pinned
against the linear definition. `column::InSet` is public.

## 0.18.1 — 2026-09-14

A notice and a field, hence a patch.

**The tree now carries a copyright notice, `COPYRIGHT`, and the image ships
it beside the licence.** The notice applies AGPL-3.0-only, version 3 only,
to this program and names its holder; `/api/health` reports the holder as
`copyright` beside `license`.

## 0.18.0 — 2026-09-14

A cluster, hence a minor. The catalog format moves to version 4 with the
previous one still readable.

**The image is published.** Each release pushes `ghcr.io/celastro/celastro`
tagged with the version and with `latest`, built from the tagged tree by the
repository's `Dockerfile`; 0.17.0 is the first. The chart pulls it by
default and moves to appVersion 0.17.0.

**A collection's shards can be spread over nodes.** Every node is started
with an address (`CELASTRO_NODE`) and the shared `CELASTRO_WIRE_TOKEN`, and
`celastro-cli serve --shard-bind ADDR:PORT` serves its shards to the others.
`ATTACH NODE 'tcp://host:port'` declares a peer; `CREATE COLLECTION ... WITH
(nodes = [...])` places shard `i` on the `i`-th node named, or on this node
and the attached ones in turn; every holder carries the definition and the
placement, so any of them takes any statement: writes route to the owner,
queries fan out and fuse where they arrived, DDL and `FLUSH` run on every
holder, and `LOCAL <statement>` runs on one node only. A node that does not
answer is a deadline at the coordinator and `partial_results` names its
shard. `DETACH NODE` refuses while the node holds a shard; an export needs
every shard local. The library gains `DbOpts::node`, `Db::attach_node`,
`Db::detach_node`, `Db::adopt_collection`, `Db::insert_here`,
`Db::delete_key_here`, the `wire` module and `Shard::index`; `Db::run_select`
takes the statement's parameters; `Statement` gains `Local`, `AttachNode` and
`DetachNode`, and `CreateCollection` a `nodes` list. Moving a shard between
nodes is not built yet.

**The catalog format is version 4**, for the node list and the placement. A
version-3 catalog is read with every collection placed on this node.

**A unit sealed before an index was declared answers no rows for that
path** instead of refusing the statement, and the plan says which unit and
why; the catalog still refuses a path no index declares.

## 0.17.0 — 2026-09-14

A new module and a library type that changed shape, hence a minor. The
on-disk format is unchanged.

**A deterministic simulator, and the boundary it drives.** A query now
reaches a shard through `plan::service::ShardService` — statistics, prefix
expansion, candidates, an unranked scan, payload fetches — answered by direct
call as before. `celastro::sim::Sim`, installed with `Db::install_sim`, puts
a seeded schedule of drops, crash-and-restarts and reordering on that
boundary and records a trace; its tests pin that a fault can shorten an
answer only by saying so. Two visible consequences: an unranked scan fetches
the documents of the rows that make the page in one call per shard, and
`EXPLAIN ANALYZE` lists shards by index whatever order they answered in.
`ExecInput` (a library type) now takes services rather than shards, and
`FlushThresholds`, `ShardOpts` and the new types are `Clone`.

## 0.16.0 — 2026-09-13

Two new statements, a new flush threshold and a fallible `set_path`, hence
a minor. The on-disk format is unchanged.

**`DROP COLLECTION` and `DROP INDEX` exist.** A dropped collection takes its
files, its objects in an archived-tier store, its statistics and its access
clocks with it, and a collection recreated under the same name is measured
afresh; the drop is ordered so that a crash in the middle is completed at
the next open. A dropped index is withdrawn from the planner, the residency
ledger, the clocks and the statistics; the regions already sealed stay until
compaction rewrites them. Both refuse while a lifecycle policy names what
they would drop. The library gains `Db::drop_collection`, `Db::drop_index`,
`Statement::DropCollection` and `Statement::DropIndex`.

**A seal under a pinned `gc_horizon` emits at most `max_versions` segments.**
`FlushThresholds` gains `max_versions` (default 8): while a horizon is pinned
the memtable seals once its longest version chain reaches it, so a hot key's
retained versions fan out into bounded bursts instead of one flush of as many
files as versions. Unpinned behaviour is unchanged. `Memtable` gains
`should_flush_pinned` and `version_depth`.

**`Value::set_path` is fallible** and refuses a value that would nest deeper
than 128, the bound `json::parse` and the variant decoder already apply; the
constant is `value::MAX_DEPTH` and `Value::depth()` reports a value's
nesting. Before this, a value built in memory past the limit encoded and then
could not be decoded. The refusal for a quoted identifier containing a dot
now says that such a field is unreachable, and the README records both
limits and that a cut `DELETE` has no opt-in by decision.

## 0.15.0 — 2026-09-13

A new capability and a catalog format step with a compatible read path,
hence a minor.

**A collection's prefix expansion cap is a setting.** `CREATE COLLECTION ...
WITH (prefix_expansion = N)` and `ALTER COLLECTION <name> SET
(prefix_expansion = N)` set how many dictionary terms a prefix on that
collection expands to; the default is still 512 and `SHOW CATALOG` shows the
value in force. The ceiling is 4096, the size of the per-path statistics
cache. The number of distinct prefixes one statement may name is now derived
from the cap — `4096 / prefix_expansion`, so eight at the default, two at
2048 — instead of being a second constant, and the refusals for a cut
`DELETE` and for an over-budget statement name the collection's cap. The
library gains `Db::set_prefix_expansion`, `Statement::AlterCollection`,
`CreateCollection::prefix_expansion`, `Collection::prefix_expansion` /
`prefix_cap()` and `GlobalStats::prefix_cap`; `GlobalStats` now implements
`Default` by hand, with the cap at 512.

**The catalog format is version 3.** This build reads a version-2 catalog and
a version-2 export, with every collection at the default cap. A 0.14 or
earlier build refuses a catalog or export written by this one, so take a copy
before upgrading if you may want to go back.

## 0.14.0 — 2026-09-13

A new capability, hence a minor. Two verbs and three library calls were
added; the on-disk format is unchanged.

**A collection can be copied between instances, as of an instant, without
stopping the source.** `celastro-cli export <collection> <dir>` writes a
database directory holding the collection as a reader saw it at the pin,
and `import <dir>` adopts it into another database. Writes that land on the
source while the copy is in flight do not reach it; the destination is
absent or complete, never half-populated. In the library:
`Db::export_collection`, `CollectionExport::write_to`, `Db::import_collection`.

## 0.13.0 — 2026-09-13

A new verb and a chart, hence a minor. `/api/health` no longer requires the
token. The on-disk format is unchanged.

**A Helm chart, and a health probe for it.** `chart/celastro` deploys one
instance as a `StatefulSet` with a persistent data volume, probes that ask
the database rather than the process, and the `archived` tier optionally in
a bucket. `celastro-cli health [--port N]` is the probe: it exits 0 when a
console on that port answers that it is serving. `/api/health` is served
without the token now -- a probe cannot know one -- and reports the
collection count, which it reads from the catalog.

## 0.12.0 — 2026-09-13

A new capability, hence a minor. `DbOpts` gained a field, which the
contract stated at 0.9.0 makes a non-breaking addition; the on-disk format
is unchanged.

**The `archived` tier can live in an S3-compatible object store.** Set
`DbOpts::archive` -- from the CLI, `CELASTRO_ARCHIVE_ENDPOINT` and
`CELASTRO_ARCHIVE_BUCKET` -- with `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` in the environment, and a segment moved to the tier
is `PUT` there as one object, read by ranged `GET`s as it faults in, and
deleted when a compaction retires it. Plain HTTP only: the crate carries no
TLS, so the endpoint is a MinIO on the same host or a TLS-terminating proxy.
Nothing changes for a database with no endpoint configured, which keeps the
local directory.

## 0.11.0 — 2026-09-13

A new capability and a behaviour change, hence a minor. No public signature
changed beyond a new `signal` module; the on-disk format is unchanged.

**`serve` handles SIGTERM and SIGINT.** As PID 1 in a container it received
neither, so `docker stop` waited ten seconds and killed it; it now ends the
accept loop, saves and exits 0, and the banner says so. The route is an
in-tree `signal(2)` binding, recorded in `src/signal.rs`, because `std` has
no signal API and the crate takes no dependencies. Only `serve` installs it.
`--init` is no longer needed for `serve`.

**The statistics cache ages by its own collection's writes.** The refresh
gate compared against an engine-wide write count, so a burst on an
unrelated collection ended this one's epoch and unanchored its globals. It
counts writes to the collection it guards now, so the refresh interval
describes the staleness rather than bounding it.

## 0.10.1 — 2026-09-13

A patch: a durability fix, and documentation. Nothing observable moves
short of a power loss during a tier move.

**A tier move is durable, and the durability guarantee says where it
stops.** Moving a segment between `segments/` and `archive/` renamed the
file and fsynced nothing, so a crash could take the new name back and leave
a segment the manifest names in neither directory; both directories are
fsynced now, destination first, like every other publication. The crate
docs, the README and the design notes state that the directory fsync behind
the guarantee is a POSIX operation and a no-op off unix.

## 0.10.0 — 2026-09-13

A behaviour change for a query with more tied documents than its candidate
depth in an unflushed memtable, hence a minor. No public signature changed
and the on-disk format is unchanged.

**A score tie inside a memtable no longer depends on insertion order.** A
unit hands the coordinator its best `k'` candidates, and inside a memtable a
tie at the last slot went by push order rather than by key, so with more
tied documents than `k'` in an unflushed memtable the answer depended on how
many memtables the corpus was split across. Ties now go by key everywhere, at
a cost only the memtable pays.

## 0.9.0 — 2026-09-13

Breaking for library callers who built an options or report struct
literally, or called into a shard's storage; the SQL surface and the
on-disk format are unchanged. The version after this one is 0.10.0.

**The public surface is deliberate.** Every options struct (`DbOpts`,
`SearchOpts`, `ResidencyOpts`, `Placement`, `FlushThresholds`,
`CompactionOpts`, `BuildOpts`, `HnswParams`, `Bm25Params`, `ShardOpts`,
`WithOpts`) and every report struct (`Explain` and its parts,
`VectorReport`, `RecallReport`, `IndexActivity`, `PathStats`) is `#[non_exhaustive]`: build options from `Default` and set
fields, read reports, and a field added later cannot break you again the
way 0.3.0 and 0.8.0 did. `Placement::new(node_id, replicas)` replaces its
struct literal. `Shard` is reachable through `Db::shards` for reading only;
its writes, its flush and compaction, its publication and its write-ahead
log are crate-private now, as are its statistics gathers with documented
preconditions. Breaking for a caller who built any of those structs
literally or called into a shard's storage; both were undocumented.

## 0.8.0 — 2026-09-13

Two behaviour changes on a published crate, hence a minor: every statement
now has a deadline, and a filtered vector search may choose a different
strategy than before. No public signature was removed; `DbOpts`,
`SearchOpts` and `VectorReport` each gained a field, and `WithOpts` gained
two. The on-disk format is unchanged.

**Every statement has a deadline, and it is enforced inside the work.** A
query's CPU used to be bounded only by an opt-in: `WITH (deadline_ms)`
existed, was off by default, and was checked between shards, so one shard's
work ran as long as it ran. A `Db` now gives every query 30 seconds
(`DbOpts::statement_deadline_ms`), a statement may raise it with
`WITH (deadline_ms = N)` or lift it with `WITH (no_deadline)`, and the
graph traversal, the brute-force pass, the WAND loop, the prefix walk and the
scan each stop when it passes. A statement that ran out is refused with the
budget named, or, under `WITH (partial_results)`, answered with the shards
that ran out listed as missing. `EXPLAIN ANALYZE` shows the budget. A
statement that used to finish slowly may now be refused; name a budget if
thirty seconds is not enough.

**Filtered vector search prices the traversal that would run.** The cost
model charged a filter-aware traversal `ef` visits when its real cost is
about `ef / s`, so at low selectivity it chose a traversal of most of the
segment over an exact scan of the few survivors, twenty times the work it had
costed. It now prices the arm it would run, which picks the scan far more
often on segments under a hundred thousand documents; a scan is exact, so
where the choice changes the answer can only improve. `WITH (max_visits = N)`
bounds a traversal for a caller that would rather have a short answer, and
`EXPLAIN ANALYZE` reports `visits` beside the budget so it shows whether it
bound.

**The console says when a query was cut.** A wide prefix that hit the
expansion cap was reported by the shells and both JSON wires and not by the
browser console, which showed the short table and said nothing. It now shows
the same `TRUNCATED —` lines beside its partial-result warning.

## 0.7.0 — 2026-09-13

A new predicate shape, hence a minor. No public signature changed and the
on-disk format is unchanged.

**A distance threshold in `WHERE`.** `WHERE embedding <=> [..] < 0.2` is a
predicate: it composes with structured and text predicates, contributes no
rank, and selects exactly the rows whose `distance` column on the
nearest-neighbour path would satisfy the comparison, for the same operator.
Exact match is `<-> [..] <= 0` for L2; for cosine, normalisation leaves an
identical vector within rounding of 0 rather than at it, so ask for
`<=> [..] < 0.000001`. It is evaluated exactly, by a full-precision pass over
the rows the other predicates left, and `EXPLAIN ANALYZE` says so. Under
`NOT` it is three-valued: a document with no vector is on neither side.

**The console offers its source.** The page names the licence and links the
source of the running version, and `/api/health` carries the same URL and
the licence identifier. The link is the repository the crate declares at the
tag of the running version; a fork that serves the console must point
`repository` in Cargo.toml at its own source.

## 0.6.1 — 2026-09-13

A patch: nothing observable moves except how much memory and time a scan
costs.

**An unranked scan holds one page, not the collection.** `SELECT * FROM docs
LIMIT 1` used to decode and buffer every matching document before taking one
of them, so memory for a scan was bounded by the collection rather than by
`LIMIT` and `OFFSET`. A scan now retains at most `offset + k` rows as it goes,
and a key-ordered scan does not decode a document until it is known to be on
the page. `ORDER BY` on a field still reads every survivor to place it, but
holds one page of them. `COLLAPSE BY` and cursors answer exactly as before.

## 0.6.0 — 2026-09-13

A behaviour change on every query surface, hence a minor: rows carry what
the SELECT list names. No public signature changed and the on-disk format is
unchanged.

**The SELECT list is honoured.** It was parsed and read nowhere, so every
query returned the whole document on every surface: the shells printed every
field and the JSON wire carried the whole `doc` whatever was asked. A row now
carries the named paths only, keyed by alias or by the path as written, with
`Null` where a document lacks one so that rows share a shape. `*` keeps the
whole document, and a ranked query's `score` and `distance` stay on the row
whether or not the list names them. Anything that read a field it did not
name gets `Null` for it now; name it.

## 0.5.0 — 2026-09-13

The delete log gained a frame, so this is a format change with a compatible
read path: a 0.4.0 directory opens as it is, and its delete logs are rewritten
in the new shape as they are next published. No public signature changed.

**An unreadable file fails the open instead of opening empty.** CATALOG,
MANIFEST and every delete log used to be read as absent when the read failed
with anything but "no such file", so an I/O error at open produced a database
with no collections, a shard with no segments or a segment with no deletions —
and the next persist wrote that emptiness over the real file. Each now fails
the open with an error naming the file. A genuinely absent file still means
absent.

**The delete log is framed.** It was the one file in the format with no magic,
version or checksum, so a log that lost its tail at a record boundary decoded
as a shorter log and the documents in the lost records came back. A framed log
refuses every truncation and every flipped byte. A log written by an earlier
version still decodes, and the next publication that touches it rewrites it
framed.

**The catalog counts every document once.** The persisted catalog used to
include the memtable's documents, which the write-ahead log also holds, so
every reopen replayed and counted them again on top of a total that already
had them: four documents read as 8, 16 and 24 across four sessions, while
`SELECT *` answered 4 rows throughout. `SHOW CATALOG`, the `catalog` verb and
the planner's per-path `present` counts were all wrong by the same amount. The
persisted catalog now counts sealed documents only and the replay counts the
rest, so the number is exact whether or not the last session persisted. An
existing directory's inflated count is not repaired — the persisted total is
trusted as the sealed baseline — but it stops growing.

## 0.4.0 — 2026-09-13

An acknowledged write now survives a power loss.
Nothing on the 0.3.0 write path was fsynced: the WAL record was written and
never synced, and the rename that publishes a segment or a manifest was never
made durable, so an acknowledged insert could be lost with the page cache. The
record is now synced before the acknowledgement and every publication is
followed by a directory fsync. It is also faster, because a persist used to
rewrite CATALOG and every MANIFEST with unchanged bytes on every statement: an
acknowledged insert went from 2360 to 644 microseconds at one shard and from
8856 to 688 at six.

Three things change on the way back in. A reopen now unlinks segment and
delete-log files the manifest does not name — the leftovers of a publication
that failed — and never hands their ids out again, where 0.3.0 could reopen a
new segment carrying a dead one's delete log. A segment whose whole-body
checksum does not match is refused at open; 0.3.0 wrote the checksum and did
not check it. And a value nested deeper than 128 levels, reachable only
through `set_path` and never through SQL or JSON, is refused on decode rather
than aborting the process.

Two answers move. An integer past 2^53 in a shredded `Number` column used to be
rounded to a double on the way in, so `2^53 = 2^53 + 1` was true; such values
are now compared exactly. And a negative `dims`, `ef_search` or `deadline_ms`
is refused rather than wrapped to the maximum.

No public signature changed and the on-disk format is the same, so a 0.3.0
directory opens as it is.

## 0.3.0 — 2026-09-11

Cut hours after 0.2.0 because 0.2.0 can lose data. 0.2.0 and 0.1.0 are yanked
for the same reason; both still resolve for anyone pinned to them.

Fix a data-loss bug, and two things break.

A `DELETE` whose `text_match` predicate was cut by the expansion cap used to
run. In the negated shape that is not a short answer, it is a wrong one: a
truncated exclusion set deletes documents the predicate asked to spare, and
0.2.0 destroyed 488 rows in a case whose correct answer was none. Such a
statement is now REFUSED and nothing is written. If you relied on it completing,
spell the prefix as narrower pieces — each deletes exactly what it names.

A prefix query also used to answer differently depending on how the data
happened to be laid out on disk. The expansion cap applied per storage unit, so
`WHERE text_match(body,'a*')` returned 2199, 3928 or 3953 rows of the same 6000
matching documents at one, three and six shards. The expansion is now resolved
once against the live corpus, so the answer is the same everywhere — and, where
the cap binds, smaller than the largest of those. It is a real trade and the
number is in the prefix section below.

Breaking for library users: `GlobalStats` gained `expansions` and `QueryResult`
gained `truncated_prefixes`, so struct literals of either need updating. Both
are now `#[non_exhaustive]`, so the next field will not break you.

## 0.2.0 — 2026-09-11 (yanked)

Absolute BM25 scores move. The default query path
used to derive its statistics from physical rows, which counted superseded and
tombstoned versions and so drifted with flush and compaction timing — and, since
those are per shard, with the shard count. It now measures the live corpus, so
the same documents in the same order come back with different numbers against
them. Relative ranking is what this corrects rather than disturbs: a term is no
longer weighted by how much dead data happens to be on disk beside it. Anything
comparing scores against a stored threshold needs re-baselining; anything
comparing them against each other does not.

## 0.1.0 — 2026-09-10 (yanked)

First published version.
