# celastro

One instance of celastro: a `StatefulSet` of exactly one pod with its data
directory on a `PersistentVolumeClaim`. That is the whole shape, and it is
not a default someone should raise. celastro is a single process -- its
shards run inside it, and there is no replication, consensus or shared
storage between two processes -- so `replicas` is a fact written into the
template rather than a value. A second pod would be a second, unrelated
database that happened to share a name.

## Install

The chart pulls `ghcr.io/celastro/celastro:<appVersion>`, the image each
release publishes from the tagged tree (see the repository's `Dockerfile`).

```
helm install celastro chart/celastro
```

To run an image of your own instead, build one and put it where the cluster
can pull it, or load it into a local cluster, then point the chart at it:

```
docker build -t celastro:0.20.0 .
kind load docker-image celastro:0.20.0        # for a kind cluster
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

Not verified: a real `StorageClass` other than kind's, and the `archive`
values against a bucket in the cluster. The client behind them is tested
against an in-process S3 in the crate's own tests.
