# celastro

celastro on Kubernetes: a `StatefulSet` of `replicas` pods, each with its
data on its own `PersistentVolumeClaim`. One pod is a database. More than one
is a cluster: each pod attaches the others as it comes up, a collection
created `WITH (splits = [...])` at any of them is spread one shard per pod,
and any pod takes any statement for it. There is no replication — a shard has
one holder — so a pod that is down is its shards down until it is back.

## A cluster

```
helm install celastro chart/celastro --set replicas=3
```

Every pod is started with its address, the wire token from a `Secret` the
chart generates once and keeps across upgrades (or `wire.existingSecret`),
and the list of its peers, which it attaches as they answer. Then, at any pod:

```sql
CREATE COLLECTION notes (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)
  PARTITION BY (tenant) WITH (splits = ['m', 't']);   -- three shards, one per pod
```

Raising `replicas` adds attached nodes; `REBALANCE notes` moves shards onto
them, `MOVE SHARD i OF notes TO 'tcp://celastro-3.celastro:9000'` moves one
by hand. Lowering `replicas` strands the shards on the removed pods' volumes:
move them off first. The wire is plain TCP with the shared token inside the
cluster network, encrypted with `tls.enabled` (below).

Clients reach a cluster through the console, exposed:

```
helm install celastro chart/celastro --set replicas=3 --set console.expose=true
```

Every pod then serves the console on all interfaces with one token from the
`Secret` `celastro-console` (generated once and kept, or `console.token`, or
`console.existingSecret`), and the Service `celastro-console` — a cluster IP,
not headless — spreads requests over the ready pods, per request, because
the console closes every connection after one. `/api/health` names the pod
that answered. Plain HTTP with the token as the only guard: keep the Service
inside a network you trust, put an ingress in front of it
(`console.service.type`), or set `tls.enabled`.

### Encryption in transit

```
helm install celastro chart/celastro --set replicas=3 --set console.expose=true --set tls.enabled=true
```

Every pod then serves the wire and the console over TLS 1.3 with one
certificate and verifies every other pod against one CA. The certificate is
Ed25519 (the binary's TLS signs with nothing else; the CA above it may also
be RSA or P-256), from one of three places:

- Nothing named, as above: a pre-install hook Job runs `celastro-cli tls
  secret` in the cluster, which makes a CA and a certificate naming every
  pod, both Services and `localhost`, and writes them as the Secret
  `<release>-tls` through the API (a ServiceAccount and a Role that may `get`
  and `create` Secrets in the namespace, both hooks too). A Secret already
  there is left alone, so an upgrade keeps the material; delete it to have
  the next upgrade make new material. `tls.days` is the validity. Hooks
  are not the release's, so `helm uninstall` leaves the Secret, the
  account, the Role and its binding behind: `kubectl delete secret
  <release>-tls` and `kubectl delete sa,role,rolebinding -l
  app.kubernetes.io/instance=<release>` remove them.
- `tls.existingSecret`: a Secret with `tls.crt`, `tls.key` and `ca.crt`,
  made by the binary and loaded with `kubectl`:

  ```
  celastro-cli tls init ./tls celastro celastro-0.celastro,celastro-1.celastro,celastro-2.celastro,celastro-console,celastro.default.svc,celastro-console.default.svc
  kubectl create secret generic celastro-tls --from-file=./tls/tls.crt --from-file=./tls/tls.key --from-file=./tls/ca.crt
  helm install celastro chart/celastro --set replicas=3 --set console.expose=true --set tls.enabled=true --set tls.existingSecret=celastro-tls
  ```

  The certificate has to name every pod (`<release>-<i>.<release>`), both
  Services and `localhost` (the tool adds `localhost` and `127.0.0.1`; the
  probe asks the pod's own console as `localhost`).
- `tls.certManager.issuerRef.name`: the chart emits a cert-manager
  `Certificate` with `privateKey.algorithm: Ed25519` and those names, and the
  issuer fills the Secret and renews it (a pod reads the files at start, so a
  renewal reaches it at its next restart). The issuer's Secrets have to carry
  `ca.crt` — a CA issuer does.

Clients verify the console against `ca.crt` from the Secret, as the notes
say. The tokens stay in force
with TLS on; the archive endpoint stays plain HTTP.

A pod that has just restarted has a new address; a dial that fails is
retried for two seconds, which covers a restart, and the other pods may
still hold the old address for the cluster DNS TTL (30 seconds on
kubeadm) or a name scaled away for its negative-cache time; until then a
statement they coordinate over its shards fails naming the shard (`did not
answer`), `WITH (partial_results)` being the opt-in to an answer without it.
A client that retries rides it out; measured below.

### Backups, and the archived tier on a mount

```
helm install celastro chart/celastro --set replicas=3 --set archive.existingClaim=celastro-nfs --set backup.schedule="0 3 * * *"
```

`archive.existingClaim` is a ReadWriteMany claim — an NFS volume is the
usual one — mounted at `/archive` on every pod. The `archived` tier moves
its segments into `/archive/tier` (unless `archive.endpoint` names a
bucket, which then keeps the tier), and `BACKUP TO` writes under
`/archive/backups`: `CELASTRO_BACKUP_DIR` is set there, so a statement
from the console can name `nightly` and nothing outside it. With a
schedule, a CronJob runs one container per pod, each sending `BACKUP TO
'<backup.to>'` to that pod's console (over TLS when `tls.enabled`), so
every pod backs up the shards it holds; the first run copies everything,
later runs only the segments that are new. `backup.to` may also be
`s3://bucket/prefix`, through the archive's endpoint and credentials. To
restore, start an empty release (a new name, or the same one with its
volumes gone), mount the same claim, and run on each pod — `celastro-cli
send` from a pod with the token, or the console UI — `RESTORE FROM
'nightly'`: every pod backs up under its own address, so a pod restores
what its namesake wrote (`NODE 'tcp://<pod>.<release>:9000'` for another
one's), and a pod that held shards `0` and `2` restores those. The pods
have no NFS client of their own: the claim is the cluster's.

## Install

The chart pulls `ghcr.io/celastro/celastro:<appVersion>`, the image each
release publishes from the tagged tree.

```
helm install celastro chart/celastro
```

For an image of your own, build it, put it where the cluster can pull it (or
load it into a local cluster), and point the chart at it:

```
docker build -t celastro:0.34.0 .
kind load docker-image celastro:0.34.0        # for a kind cluster
helm install celastro chart/celastro --set image.repository=celastro
```

`image.repository` and `image.tag` take a registry of your own the same way.

## Reaching the console

Unless `console.expose` is on, the console binds `127.0.0.1` inside the pod
(it executes SQL; a bind reachable from the network would be a remote shell)
and is reached with `kubectl port-forward`, the URL with its token being
printed on the pod's stdout at every start:

```
kubectl logs celastro-0 | grep '^http'
kubectl port-forward celastro-0 8787:8787
```

Treat the URL as a password. A new token is printed at every start, so after
a restart read the log again.

With `console.expose=true` the token is the one in the `Secret`, read with
`kubectl get secret celastro-console -o jsonpath='{.data.CELASTRO_TOKEN}' |
base64 -d`, sent as `X-Celastro-Token` (or `?t=`) with every request to
`http://celastro-console:8787` from inside the cluster; the notes `helm`
prints say the same with the release's names filled in.

## Probes

Both probes run `/celastro-cli --port 8787 health` inside the pod: the image
has no shell and no curl, and the binary asks the console itself, over TLS
when it is on. The answer comes from the database, so a process up with a
database it could not open is not ready; the path needs no token. With more
than one replica the readiness probe adds `--attached <replicas-1>`, so a pod
is routed to only once it has verified every other pod since it started —
which needs the headless Service to publish a pod's address before it is
ready, or no pod could reach another. Liveness stays the plain `health`, so a
peer that is down does not get every pod restarted.

## Upgrading

`helm upgrade` with a new `image.tag` rolls the pods one by one; a pod
on the new version attaches the old ones as it comes up (0.34.0 -- until
then the attach refused a different version and the rollout stalled at
the first pod), and the wire's own version is what decides whether two
versions speak. A release that bumps the wire version says so in the
changelog; those need every pod restarted together
(`updateStrategy: OnDelete`, delete them all).

## Stopping

`serve` handles SIGTERM: it stops accepting, saves, and exits 0 inside the
30-second grace period. No init process is needed.

## Values

| value | default | what it is |
|---|---|---|
| `image.repository`, `image.tag` | `ghcr.io/celastro/celastro`, the chart's `appVersion` | the image; `pullPolicy` is `IfNotPresent` |
| `replicas` | `1` | pods; more than one is a cluster of nodes |
| `wire.port` | `9000` | the port pods serve their shards on to each other |
| `wire.token`, `wire.existingSecret` | empty | the shared token, or a `Secret` with the key `CELASTRO_WIRE_TOKEN`; both empty generates one, kept across upgrades |
| `port` | `8787` | the console's port inside the pod |
| `console.expose` | `false` | serve the console on every interface, one token at every pod, behind the Service `<release>-console` |
| `console.service.type` | `ClusterIP` | that Service's type |
| `console.token`, `console.existingSecret` | empty | the console token (at least sixteen characters), or a `Secret` with the key `CELASTRO_TOKEN`; both empty generates one, kept across upgrades |
| `persistence.size`, `persistence.storageClass` | `10Gi`, the cluster default | the data volume |
| `archive.endpoint` | empty | `host:port` of an S3-compatible store, plain HTTP; empty keeps the `archived` tier in the data volume |
| `archive.bucket`, `archive.prefix`, `archive.region` | empty | the bucket, and optional key prefix and region |
| `archive.existingSecret` | empty | a `Secret` with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `archive.accessKeyId`, `archive.secretAccessKey` | empty | the pair, if the chart is to make the `Secret` |
| `archive.existingClaim`, `archive.mountPath` | empty, `/archive` | a ReadWriteMany claim (NFS, typically) mounted on every pod: the `archived` tier in `<mountPath>/tier` unless a bucket is configured, backups under `<mountPath>/backups` |
| `backup.schedule`, `backup.to` | empty, `nightly` | with a schedule, a CronJob sends `BACKUP TO '<to>'` to every pod's console: a name under the claim's `backups`, or `s3://bucket/prefix` |
| `tls.enabled` | `false` | the wire and the console over TLS 1.3, one Ed25519 certificate per release, every pod verified against one CA; without the next two, a hook Job makes the material once |
| `tls.days` | `3650` | the validity of the certificate the Job makes |
| `tls.existingSecret` | empty | a `Secret` with `tls.crt`, `tls.key` and `ca.crt` from `celastro-cli tls init`, naming every pod, both Services and `localhost` |
| `tls.certManager.issuerRef.name`, `.kind`, `.group` | empty, `ClusterIssuer`, `cert-manager.io` | with a name, a cert-manager `Certificate` (Ed25519) is emitted for it |
| `tuning` | `{}` | `CELASTRO_*` performance variables set on every pod, e.g. `tuning.CELASTRO_INSERT_BATCH=5000`; the binary's `docs/tuning.md` lists them |
| `probes.periodSeconds`, `probes.failureThreshold`, `probes.timeoutSeconds` | `10`, `3`, `5` | both probes; the timeout is above the default because a probe waits behind a statement that changes something |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | empty | passed through |

The archive endpoint is plain HTTP (the TLS covers the wire and the
console, not that client): point it at a store in the cluster, or at a
TLS-terminating proxy in front of a bucket.

## What was verified, and how

All against a `kind` cluster (kind v0.24, Helm v3.16), each with an image
built from the tree at the time:

- **One pod** (the chart's first release): `helm lint` clean; `helm install
  --wait` ready in 9 seconds on a 1 GiB claim; a collection and a row through
  a port-forward; `/api/health` without a token reporting one collection;
  `kubectl delete pod` returned at once (`serve` handled SIGTERM) and the
  replacement answered the row; `helm upgrade --wait` with a changed probe
  period rolled the pod in 5 seconds with the row intact; no warning events.
- **The registry** (0.17.0): a bare install pulled the image from
  `ghcr.io/celastro/celastro` anonymously, 1 MB in under three seconds.
- **The cluster** (chart 0.3.0): `--set replicas=3` ready in 11 seconds,
  every pod attached to both others; a collection `WITH (splits = ['t1',
  't2'])` on the three pods, ninety documents landing on their owners, hybrid
  and partition-scoped statements answered across pods with `EXPLAIN ANALYZE`
  listing all three shards, a `MOVE SHARD` and a `REBALANCE` back with the
  rows intact; a deleted pod back and re-attached in 3 seconds; `--set
  replicas=4` rolled in 33 seconds, kept the token, and the fourth pod was
  attached. Not verified: a `StorageClass` other than kind's, and the
  `archive` values against a real bucket (the client is tested against an
  in-process S3 in the crate).
- **The exposed console** (chart 0.4.0): three pods ready in 19 seconds,
  readiness holding each until it had attached the others. From a client pod
  through `celastro-console`: no token 401, wrong token 401, health without a
  token 200, a foreign `Origin` 403 and the Service's own 200; 90 health
  requests answered 29/30/31 by the three pods; a spread collection created
  and thirty rows read back through the Service from all three pods. An
  upgrade rolled the pods and kept the token; in the window after it, 3
  statements failed over 16.8 seconds (`shard 0 ... did not answer`: the
  restarted pod's old address, held until the DNS TTL ran out) before ten in
  a row succeeded.
- **Encryption in transit** (chart 0.5.0): three pods ready in 49 seconds
  with no restarts (an earlier build had one per pod: peers dialled under the
  database lock, the liveness probe timing out behind it). The generated
  certificate carried eleven names. From a client pod with `ca.crt` from the
  Secret: health over TLS verified as `celastro-console`, `elsewhere.example`
  refused by the client, plain HTTP answered with a TLS alert, a handshake to
  `celastro-0.celastro:9000` as TLSv1.3 `TLS_AES_256_GCM_SHA384` with a
  certificate naming the pod and `localhost`, thirty rows through the TLS
  console. An upgrade kept the certificate; the same checks passed with the
  material under `tls.existingSecret` and with cert-manager v1.16 issuing
  from a CA `ClusterIssuer`. Rendered without `tls.enabled`, the manifests
  contain no TLS.
- **The in-tree TLS** (chart 0.6.0, celastro 0.28.0): material from
  `celastro-cli tls init` through `tls.existingSecret` — three pods ready, no
  restarts; `tls.enabled` without a source refused at install. From a client
  pod, python's `ssl` verified the console as `celastro-console` against the
  CA, refused `elsewhere.example`, got a TLS alert for plain HTTP, and shook
  hands with a pod's wire port as TLSv1.3 `TLS_CHACHA20_POLY1305_SHA256`
  with a certificate naming eight hosts; thirty rows through the TLS console.
  `openssl s_client` through a port-forward: TLSv1.3, that suite, peer
  signature type Ed25519, `Verification: OK`. cert-manager v1.16 with an
  Ed25519 CA `ClusterIssuer` through `tls.certManager.issuerRef`: the
  `Certificate` Ready, the same checks passed, no restarts.
- **The Job makes the material** (chart 0.7.0, celastro 0.29.0):
  `tls.enabled` alone, three pods ready in 50 seconds, the hook Job gone
  on success, the Secret of type `kubernetes.io/tls` with an Ed25519
  certificate naming eleven hosts and 127.0.0.1, `openssl verify` OK
  against its CA. The same client checks passed; an upgrade left the
  certificate byte for byte. cert-manager v1.16 with an RSA-2048 CA
  (`sha256WithRSAEncryption`) and with a P-256 CA (`ecdsa-with-SHA256`)
  each signing the Ed25519 leaf: `Certificate` Ready, pods ready in 39 and
  49 seconds, the checks passed, no restarts. The first build of the Job
  failed every attempt with "expected the server's Certificate": the API
  server sends a `CertificateRequest`, which the client now answers.
- **Backups and the tier on NFS** (chart 0.8.0, celastro 0.30.0): an NFS
  server in the kind cluster, a ReadWriteMany claim on it as
  `archive.existingClaim`, TLS on, `backup.schedule` set. Three pods ready
  with `CELASTRO_ARCHIVE_DIR=/archive/tier` and `CELASTRO_BACKUP_DIR=
  /archive/backups`; 3,000 documents over three shards; the text index
  moved to `archived` put one segment per non-empty shard on the mount
  (`nfs4`) and the text query still answered all 3,000. The CronJob run by
  hand: three containers, each pod's console answering `BACKUP TO
  'nightly'` for the shard it holds; a second run copied 0 segments. The
  release uninstalled with its volumes, a fresh one installed on the same
  claim, `RESTORE FROM 'nightly'` sent to each pod: each restored its own
  node's backup, 3,000 rows and the text query back, no restarts. The
  first build shared one `LATEST` between pods and every pod restored the
  last writer's shard, which is why backups are per node now.
- **A lost node of five** (celastro 0.34.0): five pods, a collection
  split five ways, 5,000 rows, a backup per node on the NFS claim. A pod
  scaled away: a scan without `partial_results` fails in 1.7 s naming the
  shard and the node, with it answers 4,003 rows and names `shard 2`; a
  point lookup on a live shard answers without `partial_results` (it
  failed until 0.34.0, on the counters call); an insert into the lost
  shard fails at once, one into a live shard succeeds; DDL applies on the
  four holders and names the fifth. The pod back: ready in 5 s, reachable
  after the DNS negative-cache time (23 s). A pod deleted and recreated:
  6 of 59 scans failed over the 40 s around it. A pod lost with its
  volume: back empty in 20 s, `RESTORE FROM 'nightly'` on it brings its
  shard back. The rolling upgrade from the previous image stalled at the
  first pod until the attach stopped refusing a different crate version.
- **Ingest under a memory limit** (celastro 0.33.0): the binary of the
  image, 300 statements of 1,000 documents (50,000 with text and 128-d
  vectors, 250,000 edges) in a cgroup with `MemoryMax`. With the old
  memtable default (32,768 vectors a seal) a 512 MiB cap killed the load
  at 28,000 documents and 256 MiB at 14,000; with the new default (4,096,
  the flat tier) 512 MiB completes in 17 s at the cap, 256 MiB is killed
  with 137,000 of the edges in. Uncapped: 967 MB and 280 s before, 219 MB
  and 10 s for the 50,000 documents after. `resources` in `values.yaml`
  carries the starting point.
