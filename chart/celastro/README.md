# celastro

celastro on Kubernetes: a `StatefulSet` of `replicas` pods, each with its
data directory on its own `PersistentVolumeClaim`. One pod is a database.
More than one is a cluster: each pod is a node with a stable address inside
the headless service, serves its shards to the others on the wire port, and
attaches every other pod as it comes up, so a collection created `WITH
(splits = [...])` at any of them is spread one shard per pod, in pod order,
and any pod takes any statement for it. There is no replication between the
pods -- a shard has exactly one holder -- so a pod that is down is its
shards down until it is back on its volume.

## A cluster

```
helm install celastro chart/celastro --set replicas=3
```

Every pod is started with `CELASTRO_NODE=tcp://<pod>.<service>:9000`, the
shared token from a `Secret` the chart generates once and keeps across
upgrades (or `wire.existingSecret`), and `CELASTRO_ATTACH` naming every
pod's address; each attaches the others, retrying until they answer, and
logs `attached tcp://...` for each. Then, over a port-forward to any pod:

```sql
CREATE COLLECTION notes (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)
  PARTITION BY (tenant) WITH (splits = ['m', 't']);   -- three shards, one per pod
```

Raising `replicas` later adds attached nodes; `REBALANCE notes` moves shards
onto them, and `MOVE SHARD i OF notes TO 'tcp://celastro-3.celastro:9000'`
moves one by hand. Lowering `replicas` strands the shards on the removed
pods' volumes: move them off first, then scale down. The wire is plain TCP
with the shared token and no TLS, inside the cluster network only.

## Install

The chart pulls `ghcr.io/celastro/celastro:<appVersion>`, the image each
release publishes from the tagged tree (see the repository's `Dockerfile`).

```
helm install celastro chart/celastro
```

To run an image of your own instead, build one and put it where the cluster
can pull it, or load it into a local cluster, then point the chart at it:

```
docker build -t celastro:0.23.0 .
kind load docker-image celastro:0.23.0        # for a kind cluster
helm install celastro chart/celastro --set image.repository=celastro
```

`image.repository` and `image.tag` take a registry of your own the same way.

## Reaching the console

The console binds `127.0.0.1` inside the pod, by design: it executes SQL,
and a bind reachable from the network would be a remote shell. Nothing routes
to it, and the chart's `Service` is headless. It is reached with
`kubectl port-forward`, which connects inside the pod's network namespace,
and the URL, token included, is printed on the pod's stdout at every start:

```
kubectl logs celastro-0 | grep '^http'
kubectl port-forward celastro-0 8787:8787
```

Treat the URL as a password. A new token is printed at every start, so after
a restart read the log again.

## Probes

Both probes run `/celastro-cli --port 8787 health` inside the pod. The image
has no shell and no curl, and a probe from outside the pod could not reach a
loopback bind, so the binary asks the console itself. The console answers
`/api/health` only after reading its catalog, so a process that is up with a
database it could not open is not ready; the path needs no token, because a
probe cannot know one, and it executes nothing.

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
| `persistence.size`, `persistence.storageClass` | `10Gi`, the cluster default | the data volume |
| `archive.endpoint` | empty | `host:port` of an S3-compatible store, plain HTTP; empty keeps the `archived` tier in the data volume |
| `archive.bucket`, `archive.prefix`, `archive.region` | empty | the bucket, and optional key prefix and region |
| `archive.existingSecret` | empty | a `Secret` with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `archive.accessKeyId`, `archive.secretAccessKey` | empty | the pair, if the chart is to make the `Secret` |
| `probes.periodSeconds`, `probes.failureThreshold` | `10`, `3` | both probes |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | empty | passed through |

The endpoint is plain HTTP because the binary carries no TLS: point it at a
MinIO in the cluster, or at a TLS-terminating proxy in front of a bucket.

## What was verified, and how

Against a `kind` cluster (Kubernetes via kind v0.24, Helm v3.16), with the
image built from the tree at the commit that added this chart:

- `helm lint` passes.
- `helm install --wait` reached `Running`, `READY 1/1`, in 9 seconds; the
  claim bound to a 1 GiB volume.
- Over `kubectl port-forward`, a collection was created and a document
  inserted through `/api/query`; `/api/health` answered without a token and
  reported one collection.
- `kubectl delete pod celastro-0` returned at once, because `serve` handled
  the SIGTERM; the replacement pod became ready, and `/api/catalog` and a
  `SELECT` over the forwarded port returned the collection and the row.
- `helm upgrade --wait` with a changed probe period completed in 5 seconds,
  rolled the pod, and the row was still there.
- The pod's events show both probes as configured and no warnings.

And again on 2026-09-14 with the chart at appVersion 0.17.0 and nothing
loaded into the cluster by hand: `helm install --wait` pulled
`ghcr.io/celastro/celastro:0.17.0` from the registry anonymously (the pod's
events show the pull, 1 MB, in under three seconds), the pod reached
`READY 1/1`, and `/api/health` over the forwarded port reported version
0.17.0.

The cluster, on 2026-09-14 with the chart at 0.3.0 and an image built from
the tree: `helm install --set replicas=3 --wait` had three pods `READY 1/1`
in 11 seconds, and every pod logged `attached` for both others within 40
seconds of the install (pod DNS resolves a few seconds after start, which
the retry covers). Over a port-forward to pod 0: a collection created `WITH
(splits = ['t1', 't2'])` answered "3 shard(s) on" the three pod addresses,
two indexes reached every holder, ninety documents inserted through pod 0
landed on their owners, a hybrid statement and a partition-scoped one
answered across the pods and `EXPLAIN ANALYZE` listed all three shards, a
`MOVE SHARD` between two pods and a `REBALANCE` back both completed with
the rows intact. `kubectl delete pod celastro-1` came back ready and
re-attached its peers in 3 seconds, and its tenant answered through pod 0.
`helm upgrade --set replicas=4 --wait` rolled the pods in 33 seconds, kept
the generated token, and the fourth pod was attached by the others.

Not verified: a real `StorageClass` other than kind's, and the `archive`
values against a bucket in the cluster. The client behind them is tested
against an in-process S3 in the crate's own tests.
