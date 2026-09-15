# Changelog

What changed for someone running the previous version. Each entry is written
from the point of view of upgrading INTO that version, so the paragraph under
0.4.0 is what a 0.3.0 user needs to know. Versions on
[crates.io](https://crates.io/crates/celastro); tags `vX.Y.Z` in this
repository.

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
