#!/usr/bin/env bash
#
# Two builds of one commit give one binary, or the script says so: what a
# reader needs to check that a binary they were handed is this source.
# Both builds happen at the same fixed path (a package's identity hashes
# its path into every symbol) with the path remapped out of the binary,
# under whatever toolchain rustup selects here, so the hash printed is
# the one anyone with this toolchain gets. Prints both hashes; exit 0
# only when they agree.
#
#   scripts/reproducible.sh [ref]     (default HEAD)
set -euo pipefail
ref=${1:-HEAD}
src=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
work=/tmp/celastro-reproducible
build=$work/src
hashes=()
for i in 1 2; do
  rm -rf "$build"; mkdir -p "$work"
  git clone -q --shared --no-checkout "$src" "$build"
  git -C "$build" checkout -q --detach "$(git -C "$src" rev-parse "$ref")"
  ( cd "$build" && RUSTFLAGS="--remap-path-prefix=$build=/celastro --remap-path-prefix=$HOME=/home" \
      cargo build -q --release --bin celastro )
  hashes+=("$(sha256sum "$build/target/release/celastro" | cut -d' ' -f1)")
  echo "build $i: ${hashes[$((i-1))]}"
done
rm -rf "$build"
if [[ ${hashes[0]} == "${hashes[1]}" ]]; then
  echo "REPRODUCIBLE: $(git -C "$src" rev-parse --short "$ref") builds to ${hashes[0]}"
else
  echo "NOT REPRODUCIBLE"; exit 1
fi
