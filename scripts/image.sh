#!/usr/bin/env bash
#
# The image for both architectures: amd64 by the Dockerfile on this
# machine, arm64 from a static binary cross-compiled here and put in an
# image by Dockerfile.prebuilt, and one manifest under the version tag
# and under `latest`, so a pull on either machine gets its own.
#
#   scripts/image.sh build <version>    the two images, tagged <version>-amd64 and -arm64
#   scripts/image.sh push <version>     push both and the manifests (docker logged in)
#
# Needs: docker; the aarch64-unknown-linux-musl target (`rustup target add`)
# and a linker for it (Debian's gcc-aarch64-linux-gnu).
set -euo pipefail
cmd=${1:-}; v=${2:-}
[[ -n $cmd && -n $v ]] || { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
img=ghcr.io/celastro/celastro
src=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$src"
case $cmd in
  build)
    export DOCKER_BUILDKIT=1
    docker build -q --provenance=false -t "$img:$v-amd64" .
    RUSTFLAGS="-C strip=symbols -C linker=aarch64-linux-gnu-gcc" \
      cargo build -q --release --locked --offline --target aarch64-unknown-linux-musl --bin celastro --bin celastro-cli
    # A context of its own: the repository's .dockerignore is an allowlist
    # that keeps target/ out, and the prebuilt image copies from the root.
    rm -rf dist/arm64; mkdir -p dist/arm64
    cp target/aarch64-unknown-linux-musl/release/celastro target/aarch64-unknown-linux-musl/release/celastro-cli LICENSE COPYRIGHT dist/arm64/
    docker build -q --provenance=false --platform linux/arm64 -f Dockerfile.prebuilt -t "$img:$v-arm64" dist/arm64
    echo "built $img:$v-amd64 and $img:$v-arm64" ;;
  push)
    docker push -q "$img:$v-amd64"; docker push -q "$img:$v-arm64"
    # One index over the two, made in the registry by buildx's imagetools
    # (`docker manifest create` refuses an image BuildKit stored as an
    # index of its own).
    docker buildx imagetools create -t "$img:$v" -t "$img:latest" "$img:$v-amd64" "$img:$v-arm64" >/dev/null
    docker buildx imagetools inspect "$img:$v" | grep -E "Platform:" | sed 's/^ *//' | sort -u
    echo "pushed $img:$v and $img:latest for amd64 and arm64" ;;
  *) echo "unknown command: $cmd" >&2; exit 1 ;;
esac
