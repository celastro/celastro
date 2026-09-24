# Deploying celastro

One static binary, one shape of data directory, and these ways to put
it on machines. Every one of them ends in the same process: `celastro
--dir DIR --port 8787 serve --bind 0.0.0.0 [--shard-bind 0.0.0.0:7876]`
with `CELASTRO_TOKEN`, and for a cluster `CELASTRO_NODE`,
`CELASTRO_ATTACH` and `CELASTRO_WIRE_TOKEN`, as the [README](../README.md#two-or-more-nodes)
describes them. What differs is who writes that down and starts it.

| | what it is | fits when |
|---|---|---|
| [`celastro install`](#the-binary-and-celastro-install) | the binary writes its own systemd service | a host you can ssh to; the simplest, and what the others call |
| [cloud-init](cloud-init/celastro.yaml) | the install at a machine's first boot, from user-data | machines made by a cloud or a plan, addresses known up front |
| [over ssh](ssh/celastro-cluster.sh) | a shell script: the binary onto each host, then the install, one host at a time | several hosts, whether or not their addresses were known up front; rolling upgrades |
| [podman quadlet](quadlet/celastro.container) | the published image, run by systemd through podman | a host that runs images rather than binaries |
| [Helm chart](chart/celastro/README.md) | a StatefulSet of nodes with Services, Secrets, TLS, backups | Kubernetes |

## The binary, and `celastro install`

Each release on GitHub carries `celastro-<version>-linux-amd64.tar.gz`
and `-arm64.tar.gz` (the binary, `celastro-cli`, the licence and the
notice) with a `SHA256SUMS`: the same static binaries the image
`ghcr.io/celastro/celastro:<version>` runs, taken out of it. On the host:

```sh
v=0.74.0
curl -fsSL "https://github.com/celastro/celastro/releases/download/v$v/celastro-$v-linux-$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/').tar.gz" \
  | sudo tar -xzC /usr/local/bin celastro
sudo CELASTRO_TOKEN='a-long-random-token' celastro install
```

That is one node: the service `celastro`, running as the system user
`celastro` over `/var/lib/celastro`, its settings in
`/etc/celastro/celastro.env` (root and the service user can read it,
nobody else), the console on every interface at 8787 answering the
token. The command returns once the service answers its health probe.
A cluster is the same command on every host, each with its own address
and all with the same list:

```sh
sudo CELASTRO_TOKEN='a-long-random-token' CELASTRO_WIRE_TOKEN='another' \
  celastro install --node 10.0.0.2 --attach 10.0.0.2,10.0.0.3,10.0.0.4
```

The tokens come from the environment and never from a flag, so they are
in no shell history or process list. `--tls DIR` copies the set `celastro
tls init DIR celastro <every address>` made; `--client-auth` makes the
wire require a peer's certificate; `--master-key FILE --data-key FILE`
are encryption at rest (`key master`, `key init`), given at the first
install of every node; `--role coordinator` a node that holds no shards;
`--env NAME=VALUE` anything from [docs/tuning.md](../docs/tuning.md),
`CELASTRO_AUTO_FAILOVER=on` for one. `celastro help` lists them all.

An upgrade is the new binary and the same command again, one host at a
time: the install replaces the binary, rewrites the settings and
restarts the service, and returns when the node answers. The data
directory is never touched. A change of settings is the same. To read
the log, `journalctl -u celastro`; the URL `serve` prints with the
token in it is discarded rather than written there.

The firewall: 8787 to the clients (or to a balancer with `/api/health`
as its check), 7876 between the nodes and to nobody else. The token is
what protects the console, the wire token and TLS the wire.

## cloud-init

[cloud-init/celastro.yaml](cloud-init/celastro.yaml) is user-data for a
machine's first boot: it fetches the release, checks it against the
release's `SHA256SUMS`, and runs the install with the values written at
the top of the file. Every cloud takes it as user-data (`user_data` in Terraform, a
`--user-data` flag on a cloud's own tool, "custom data", "startup
script"). It fits a single node as it is, and a cluster when the
addresses are known before the machines exist -- a static network, a
plan that assigns them, names your DNS will resolve -- since each
machine's file needs its own address and the whole list. When they are
not, install one node per machine with it and attach them afterwards
from any console (`ATTACH NODE 'tcp://10.0.0.3'`), or use [the ssh
script](#several-hosts-over-ssh), which takes the addresses on its
command line once the machines exist. cloud-init runs once: an upgrade
is the install command by hand, or the script.

## Several hosts, over ssh

[ssh/celastro-cluster.sh](ssh/celastro-cluster.sh) is the install above
run on one host after another: the release binary onto each host
(checked against the release's `SHA256SUMS`), then `celastro install`
with that node's own address and the list of every node, then the next
host. The install returns only once its node answers, so a cluster
stays up through the run; when every node is installed the script asks
each one whether it has verified every other, and fails naming the node
that has not.

```sh
CELASTRO_TOKEN='a-long-random-token' CELASTRO_WIRE_TOKEN='another' \
  deploy/ssh/celastro-cluster.sh 10.0.0.2 10.0.0.3 10.0.0.4
```

A host is what you ssh to. When the address the nodes reach each other
at is not that one -- a private network behind a public one -- give the
pair, ssh target first: `root@203.0.113.10=10.0.0.2`.

The same command with a newer `CELASTRO_VERSION` is the rolling
upgrade; with changed settings, the rolling change. Every run installs
(there is no "already installed, nothing to do"): the binary is
replaced, the settings rewritten, the service restarted, node by node.

The rest is environment variables, each listed at the top of the
script: `CELASTRO_TLS_DIR` and `CELASTRO_CLIENT_AUTH`,
`CELASTRO_MASTER_KEY` and `CELASTRO_DATA_KEY` (files here, copied to
each host for the install and taken away again),
`CELASTRO_ROLE=coordinator`, `CELASTRO_ENV` for anything in
[docs/tuning.md](../docs/tuning.md), `CELASTRO_BINARY` to send a binary
of your own instead of fetching a release. The tokens are read from the
environment and reach the hosts on ssh's stdin, so they are in no
command line, here or there.

Needs ssh here, and curl, tar and systemd there. Nothing else on either
side: no agent, no interpreter, no inventory, and no state on the hosts
but the service itself. If you already run a configuration manager,
what is worth taking from the script is the little it does per host --
one `celastro install` -- and the address list it builds; a role or a
manifest around that command will be shorter than this file.

## podman quadlet

[quadlet/celastro.container](quadlet/celastro.container) runs the
published image under systemd through podman: the host's network (so
8787 and 7876 are the machine's own ports and `CELASTRO_NODE` is its
address), a named volume for the data (seeded from the image with its
ownership, so nothing is prepared by hand), the settings from
[quadlet/celastro.env](quadlet/celastro.env) at
`/etc/celastro/celastro.env`.

```sh
sudo install -m 0644 deploy/quadlet/celastro.container /etc/containers/systemd/
sudo install -d -m 0700 /etc/celastro && sudo install -m 0600 deploy/quadlet/celastro.env /etc/celastro/   # then edit it
sudo systemctl daemon-reload && sudo systemctl start celastro
```

An upgrade is the `Image=` line changed and those two commands again.
Podman 4.4 or later. The image's own notes -- volumes and ownership,
what each flag costs -- are in [docs/container.md](../docs/container.md).

## Kubernetes

[chart/celastro](chart/celastro/README.md): `helm install celastro
deploy/chart/celastro --set replicas=3`. The chart's README records
what was verified and how.

## Watching it

`/api/metrics` is the Prometheus text format, on the console's port and
behind the console's token: statements and their latency as a histogram,
refusals, the nodes this one has attached, reconciliations, backpressure,
compactions and seal failures, the data-key ring, the certificate's
expiry, and per collection the shards, segments and documents held here
with each shard's reads and writes. Two things read it.

**The dashboard.** [grafana/celastro.json](grafana/celastro.json) imports
as it is -- the only thing it asks for is a Prometheus datasource -- and
draws four rows: statements, the cluster, storage, and the handful of
states that page someone. Every panel's description says what its query
is and what a bad value looks like, which is also where the traps are
written down (a p95 pinned at the widest bucket, a document count that
drops because a node stopped being scraped).

**The scrape.** Under Kubernetes the chart does it: `--set
monitoring.enabled=true` emits a PodMonitor for every pod and a
PrometheusRule with four alerts -- with `--set
monitoring.labels.release=<your kube-prometheus-stack release>` beside
it, since that stack takes only the objects labelled with its own
release and otherwise creates nothing and scrapes nothing. On hosts, this is the scrape config --
the token goes in a header, because a `?t=` in the URL is written into
every proxy's access log on the way:

```yaml
scrape_configs:
  - job_name: celastro
    metrics_path: /api/metrics
    # `celastro install` writes CELASTRO_TOKEN into
    # /etc/celastro/celastro.env; this file holds the same value, readable
    # only by Prometheus. The console also accepts the token as
    # `X-Celastro-Token`, which `http_headers` can send instead.
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/celastro-token
    # With TLS on the console (CELASTRO_TLS_CERT), add:
    # scheme: https
    # tls_config: { ca_file: /etc/prometheus/celastro-ca.crt }
    static_configs:
      - targets: ['10.0.0.2:8787', '10.0.0.3:8787', '10.0.0.4:8787']
```

Each node answers for itself: the collection totals on the dashboard are
a sum over the nodes, so a node missing from this list is a collection
that looks smaller than it is rather than a gap that announces itself.

## What was verified

For 0.72.0, on this repository's own test machines:

- `celastro install` as root (Debian 13, amd64, systemd 257): one node,
  then the same host again with a new setting (the service restarted on
  the rewritten settings with the data kept, the shape of an upgrade),
  then with `--tls` and `--client-auth` as a cluster of one, then
  `--no-start`; a backup and `SHOW HEALTH` under the unit's hardening
  (`ProtectSystem=strict`, the data directory the one writable path);
  the journal without the token; the refusal when not root.
- **cloud-init on three fresh machines** (Ubuntu 24.04, amd64, one
  region, a private network between them): each machine fetched the
  release, checked it against `SHA256SUMS` and was serving 80 to 90
  seconds after it was created, its journal carrying no token.
- **[ssh/celastro-cluster.sh](ssh/celastro-cluster.sh) over those same
  three machines**, reached at their public addresses with their private
  ones as the wire: the cluster formed in 19 s and every node had
  verified the other two; a collection split three ways put one shard on
  each host and a row in each range counted as 3 from another node; the
  same command again was a rolling change of 15 s, through which a probe
  at the third node answered correctly every time and never saw an
  error. Then, over the same cluster: a binary of one's own instead of a
  release (`CELASTRO_BINARY`, 12 s, replaced under the running service
  with the data kept), and `CELASTRO_TLS_DIR` with
  `CELASTRO_CLIENT_AUTH=on` (18 s, the wire re-formed with client
  certificates, `/etc/celastro/tls` readable by root and the service
  user alone, the staged copies gone, the data still there over https).
  Both refusals: no console token, and several hosts with no wire token,
  each caught before any host is touched.
- The quadlet, by a drill rather than by hand, 0.72.0 then 0.72.1 on
  podman 4.9.3: serving 4 s after `systemctl start`; the named volume
  seeded from the image and owned by 65532; the journal carrying the
  served URL and not the token; a row written, `systemctl stop` exiting 0
  in 1 s (SIGTERM handled, not `TimeoutStopSec` waited out), the row
  there after `start`; the container SIGKILLed and back serving the row
  4 s later by `Restart=on-failure`; and **the upgrade as the unit
  describes it** -- the `Image=` line moved from 0.72.0 to 0.72.1,
  `daemon-reload`, `restart` -- serving in 5 s with the binary reporting
  0.72.1 and the row written on 0.72.0 reading. The same drill against a
  release that does not exist fails its first leg and still removes
  everything it put in place.
- Not covered: more than three machines; a machine added to a cluster
  that is already running (`ATTACH NODE` does that, the script installs
  a set); the key files over several machines, though they are staged
  the way the certificates are; an ssh user that is not root.
