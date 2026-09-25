#!/bin/sh
#
# celastro over one host or several, one at a time: the release binary
# onto each host, then `celastro install` with the cluster's addresses,
# then the next host. The install returns only once its node answers, so
# a cluster stays up through the run. A first run installs; the same run
# with a newer CELASTRO_VERSION is the rolling upgrade; with changed
# settings, the rolling change. Every run installs -- there is no
# "already installed, nothing to do".
#
#   CELASTRO_TOKEN=a-long-random-token CELASTRO_WIRE_TOKEN=another \
#     ./celastro-cluster.sh 10.0.0.2 10.0.0.3 10.0.0.4
#
# A host is what you ssh to. When the address the nodes reach each other
# at differs from it -- a private network behind a public one -- write
# the pair, ssh target first:
#
#   ./celastro-cluster.sh root@203.0.113.10=10.0.0.2 root@203.0.113.11=10.0.0.3
#
# Needs ssh here, and curl, tar and systemd there. Nothing else on either
# side: no agent, no interpreter, no inventory, no state but the service.
# The tokens are read from this environment and reach the hosts over the
# ssh channel on stdin, so they are in no command line, here or there.
#
# Settings are environment variables:
#
#   CELASTRO_TOKEN         the console's token; at least sixteen bytes (required)
#   CELASTRO_WIRE_TOKEN    the wire's, shared by the nodes (required for a cluster)
#   CELASTRO_VERSION       the release to install                     (0.78.0)
#   CELASTRO_RELEASE_URL   where its files are                        (the GitHub release)
#   CELASTRO_BINARY        a binary here to send instead of a release (none)
#   CELASTRO_SSH_USER      when a host does not name one              (root)
#   CELASTRO_SSH_OPTS      further ssh options                        (none)
#   CELASTRO_PORT          the console's port                         (8787)
#   CELASTRO_BIND          the console's interfaces                   (0.0.0.0)
#   CELASTRO_WIRE_PORT     the wire's port                            (7876)
#   CELASTRO_DIR           the data directory on each host            (/var/lib/celastro)
#   CELASTRO_CLUSTER       `on` for one node others will join later   (on when given several hosts)
#   CELASTRO_ROLE          `coordinator` for nodes that hold no shards (none)
#   CELASTRO_TLS_DIR       a directory here with tls.crt, tls.key, ca.crt (none)
#   CELASTRO_CLIENT_AUTH   `on` to make the wire require a peer's certificate (off)
#   CELASTRO_MASTER_KEY    a key file here, for encryption at rest    (none)
#   CELASTRO_DATA_KEY      the data key beside it                     (none)
#   CELASTRO_ENV           further settings, `NAME=VALUE NAME=VALUE`  (none)
#   CELASTRO_HEALTH_RETRIES, CELASTRO_HEALTH_DELAY  the wait for the peers (45, 2s)
#
# `celastro install` takes everything else; see deploy/README.md.
set -eu

VERSION=${CELASTRO_VERSION:-0.78.0}
RELEASE_URL=${CELASTRO_RELEASE_URL:-https://github.com/celastro/celastro/releases/download/v$VERSION}
BINARY=${CELASTRO_BINARY:-}
SSH_USER=${CELASTRO_SSH_USER:-root}
SSH_OPTS=${CELASTRO_SSH_OPTS:-}
PORT=${CELASTRO_PORT:-8787}
BIND=${CELASTRO_BIND:-0.0.0.0}
WIRE_PORT=${CELASTRO_WIRE_PORT:-7876}
DATA_DIR=${CELASTRO_DIR:-/var/lib/celastro}
ROLE=${CELASTRO_ROLE:-}
TLS_DIR=${CELASTRO_TLS_DIR:-}
CLIENT_AUTH=${CELASTRO_CLIENT_AUTH:-}
MASTER_KEY=${CELASTRO_MASTER_KEY:-}
DATA_KEY=${CELASTRO_DATA_KEY:-}
EXTRA_ENV=${CELASTRO_ENV:-}
RETRIES=${CELASTRO_HEALTH_RETRIES:-45}
DELAY=${CELASTRO_HEALTH_DELAY:-2}
STAGING=/etc/celastro/staging

die() { printf 'celastro-cluster: %s\n' "$*" >&2; exit 1; }
info() { printf '==> %s\n' "$*"; }

[ $# -gt 0 ] || die "give the hosts, one argument each; see the top of this file"
[ -n "${CELASTRO_TOKEN:-}" ] || die "CELASTRO_TOKEN is required"
[ "${#CELASTRO_TOKEN}" -ge 16 ] || die "CELASTRO_TOKEN wants at least sixteen printable bytes"
command -v ssh >/dev/null || die "ssh is required"
[ -z "$BINARY" ] || [ -f "$BINARY" ] || die "CELASTRO_BINARY: no such file: $BINARY"
for f in "$TLS_DIR" "$MASTER_KEY" "$DATA_KEY"; do
  [ -z "$f" ] || [ -e "$f" ] || die "no such file or directory: $f"
done
if [ -n "$MASTER_KEY$DATA_KEY" ] && { [ -z "$MASTER_KEY" ] || [ -z "$DATA_KEY" ]; }; then
  die "CELASTRO_MASTER_KEY and CELASTRO_DATA_KEY go together"
fi

# Several hosts are a cluster; one is a single node unless it is told that
# others will join it later, which is what makes it serve its wire.
CLUSTER=${CELASTRO_CLUSTER:-}
if [ -z "$CLUSTER" ]; then
  if [ $# -gt 1 ]; then CLUSTER=on; else CLUSTER=off; fi
fi
[ "$CLUSTER" != on ] || [ -n "${CELASTRO_WIRE_TOKEN:-}" ] ||
  die "a cluster needs CELASTRO_WIRE_TOKEN as well"
WIRE_TOKEN=${CELASTRO_WIRE_TOKEN:-}

# A value as one shell word the remote shell will read back unchanged.
q() { printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"; }

# `host` is what ssh connects to, `addr` the address the other nodes use.
# Without a pair they are the same thing, less any user@.
ssh_target() {
  t=${1%%=*}
  case $t in *@*) printf '%s' "$t" ;; *) printf '%s@%s' "$SSH_USER" "$t" ;; esac
}
wire_addr() {
  a=${1#*=}
  [ "$a" != "$1" ] || { t=${1%%=*}; a=${t#*@}; }
  printf '%s' "$a"
}

# shellcheck disable=SC2086  # SSH_OPTS is a list of options on purpose
rsh() { target=$1; shift; ssh -o ConnectTimeout=10 $SSH_OPTS "$target" "$@"; }

# Every node, the same list on every node; each one skips its own.
ATTACH=
for h in "$@"; do
  ATTACH="${ATTACH:+$ATTACH,}tcp://$(wire_addr "$h"):$WIRE_PORT"
done

n=$#
info "celastro $VERSION on $n host(s)${BINARY:+ (the binary at $BINARY)}, one at a time"

for h in "$@"; do
  target=$(ssh_target "$h")
  addr=$(wire_addr "$h")
  info "$target"

  # Staged where only root reads them; the install copies them into place
  # and this script takes the staging away again.
  if [ -n "$TLS_DIR" ] || [ -n "$MASTER_KEY" ]; then
    rsh "$target" "/bin/sh -c 'set -e; rm -rf $STAGING; mkdir -p $STAGING/tls; chmod 0700 /etc/celastro $STAGING'" </dev/null
    for f in tls.crt tls.key ca.crt; do
      [ -z "$TLS_DIR" ] || rsh "$target" "/bin/sh -c 'umask 077; cat > $STAGING/tls/$f'" < "$TLS_DIR/$f"
    done
    [ -z "$MASTER_KEY" ] || rsh "$target" "/bin/sh -c 'umask 077; cat > $STAGING/master.key'" < "$MASTER_KEY"
    [ -z "$DATA_KEY" ] || rsh "$target" "/bin/sh -c 'umask 077; cat > $STAGING/data.key'" < "$DATA_KEY"
  fi

  # A running binary cannot be written to (ETXTBSY), but it can be
  # replaced: the new one lands beside it and is renamed over it.
  if [ -n "$BINARY" ]; then
    gzip -c "$BINARY" | rsh "$target" "/bin/sh -c 'set -e; gzip -dc > /usr/local/bin/celastro.new; chmod 0755 /usr/local/bin/celastro.new; mv /usr/local/bin/celastro.new /usr/local/bin/celastro'"
  fi

  flags=
  [ "$CLUSTER" != on ] || flags="--node tcp://$addr:$WIRE_PORT --attach $(q "$ATTACH")"
  [ -z "$ROLE" ] || flags="$flags --role $(q "$ROLE")"
  [ -z "$TLS_DIR" ] || flags="$flags --tls $STAGING/tls"
  [ "$CLIENT_AUTH" != on ] || flags="$flags --client-auth"
  [ -z "$MASTER_KEY" ] || flags="$flags --master-key $STAGING/master.key --data-key $STAGING/data.key"
  for e in $EXTRA_ENV; do flags="$flags --env $(q "$e")"; done

  # The script goes over stdin rather than as an argument, so neither the
  # tokens nor anything else is a command line on the host. What is single
  # quoted below is for the remote shell to expand, not this one.
  # shellcheck disable=SC2016
  {
    printf 'set -eu\numask 077\n'
    if [ -z "$BINARY" ]; then
      printf 'case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) echo "no release for $(uname -m)" >&2; exit 1 ;; esac\n'
      printf 'tmp=$(mktemp -d); cd "$tmp"\n'
      printf 'file=celastro-%s-linux-$arch.tar.gz\n' "$VERSION"
      printf 'curl -fsSLO %s/"$file"\n' "$(q "$RELEASE_URL")"
      printf 'curl -fsSLO %s/SHA256SUMS\n' "$(q "$RELEASE_URL")"
      printf 'sha256sum --check --ignore-missing SHA256SUMS\n'
      printf 'tar -xzf "$file" celastro\n'
      printf 'chmod 0755 celastro; mv celastro /usr/local/bin/celastro.new; mv /usr/local/bin/celastro.new /usr/local/bin/celastro\n'
      printf 'cd /; rm -rf "$tmp"\n'
    fi
    # An empty wire token is not the same as no wire token, so a single
    # node that is not a cluster is given none at all.
    printf 'CELASTRO_TOKEN=%s ' "$(q "$CELASTRO_TOKEN")"
    [ -z "$WIRE_TOKEN" ] || printf 'CELASTRO_WIRE_TOKEN=%s ' "$(q "$WIRE_TOKEN")"
    printf '\\\n  /usr/local/bin/celastro --dir %s --port %s --bind %s install%s\n' \
      "$(q "$DATA_DIR")" "$(q "$PORT")" "$(q "$BIND")" "${flags:+ $flags}"
    printf 'rm -rf %s\n' "$STAGING"
  } | rsh "$target" /bin/sh -s
done

[ "$CLUSTER" = on ] || { info "done: one node serving on port $PORT"; exit 0; }

# Installed in turn, a node cannot have seen the ones that came after it.
# Now that every node is up, each must have verified every other.
info "every node has verified the other $((n - 1))"
probe_env=
[ -z "$TLS_DIR" ] || probe_env='CELASTRO_TLS_CERT=/etc/celastro/tls/tls.crt CELASTRO_TLS_KEY=/etc/celastro/tls/tls.key CELASTRO_TLS_CA=/etc/celastro/tls/ca.crt '
for h in "$@"; do
  target=$(ssh_target "$h")
  i=0
  while :; do
    if out=$(rsh "$target" "${probe_env}/usr/local/bin/celastro health --port $PORT --attached $((n - 1))" </dev/null 2>&1); then
      break
    fi
    i=$((i + 1))
    [ "$i" -lt "$RETRIES" ] || die "$target has not verified every other node: $out"
    sleep "$DELAY"
  done
  printf '  %s: %s\n' "$target" "$out"
done
info "done: $n nodes, each attached to the other $((n - 1))"
