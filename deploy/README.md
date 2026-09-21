# Deploying celastro

One static binary, one shape of data directory, and these ways to put
it on machines. Every one of them ends in the same process: `celastro
--dir DIR --port 8787 serve --bind 0.0.0.0 [--shard-bind 0.0.0.0:2352]`
with `CELASTRO_TOKEN`, and for a cluster `CELASTRO_NODE`,
`CELASTRO_ATTACH` and `CELASTRO_WIRE_TOKEN`, as the [README](../README.md#two-or-more-nodes)
describes them. What differs is who writes that down and starts it.

| | what it is | fits when |
|---|---|---|
| [`celastro install`](#the-binary-and-celastro-install) | the binary writes its own systemd service | a host you can ssh to; the simplest, and what the others call |
| [cloud-init](cloud-init/celastro.yaml) | the install at a machine's first boot, from user-data | machines made by a cloud or a plan, addresses known up front |
| [Ansible](ansible/) | a thin role: the binary onto each host, then the install, one host at a time | several hosts whose addresses live in an inventory; rolling upgrades |
| [podman quadlet](quadlet/celastro.container) | the published image, run by systemd through podman | a host that runs images rather than binaries |
| [Helm chart](chart/celastro/README.md) | a StatefulSet of nodes with Services, Secrets, TLS, backups | Kubernetes |

## The binary, and `celastro install`

Each release on GitHub carries `celastro-<version>-linux-amd64.tar.gz`
and `-arm64.tar.gz` (the binary, `celastro-cli`, the licence and the
notice) with a `SHA256SUMS`: the same static binaries the image
`ghcr.io/celastro/celastro:<version>` runs, taken out of it. On the host:

```sh
v=0.71.0
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
as its check), 2352 between the nodes and to nobody else. The token is
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
from any console (`ATTACH NODE 'tcp://10.0.0.3'`), or use the Ansible
role, which reads the addresses from its inventory once the machines
are up. cloud-init runs once: an upgrade is the install command by
hand, or the role.

## Ansible

[ansible/celastro.yml](ansible/celastro.yml) and the role beside it put
the release (or a binary of your own, `celastro_binary`) on every host
of the `celastro` group and run the install on each, one host at a
time, each waited for before the next; a last play asks every node
whether it has verified every other. [ansible/inventory.example.yml](ansible/inventory.example.yml)
is the inventory's shape and [ansible/roles/celastro/defaults/main.yml](ansible/roles/celastro/defaults/main.yml)
every setting. The tokens go in a vault. TLS and the keys are files on
the machine running the play, copied to each host for the install.

```sh
cd deploy/ansible && cp inventory.example.yml inventory.yml   # fill it in
ansible-playbook -i inventory.yml celastro.yml
```

The same play with a new `celastro_version` is the rolling upgrade;
with changed settings, the rolling change. Every run installs (there
is no "already installed, nothing to do"): the binary is replaced, the
settings rewritten, the service restarted, node by node. Needs
ansible-core 2.11 or later on the machine running it and Python on the
hosts; nothing else.

## podman quadlet

[quadlet/celastro.container](quadlet/celastro.container) runs the
published image under systemd through podman: the host's network (so
8787 and 2352 are the machine's own ports and `CELASTRO_NODE` is its
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

## What was verified

For 0.71.0, on this repository's own test machines (Debian 13 and Ubuntu
24.04, amd64):

- `celastro install` as root: one node, then the same host again from
  a rebuilt binary (an upgrade: the service restarted on the new
  binary with the data kept), then with `--tls` and `--client-auth`,
  then `--no-start`; the unit's hardening (`ProtectSystem=strict`, the
  data directory the one writable path) let the node seal, compact and
  back up.
- cloud-init: three fresh machines from `cloud-init/celastro.yaml` as
  single nodes at first boot, each serving before the ssh key was
  accepted.
- Ansible: the role over those three machines, forming the cluster
  with `--node` and `--attach` from the inventory; a collection split
  three ways spread one shard per host; the same play run again as the
  rolling restart with the cluster answering throughout.
- quadlet: the image under podman on one host, a statement through the
  console, a stop and a start with the data kept on the named volume.
