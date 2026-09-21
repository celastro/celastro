#!/usr/bin/env bash
#
# The release's binaries: taken out of the two images scripts/image.sh
# built, so what a host installs is byte for byte what the image runs,
# packed with the licence and the notice, summed, and attached to the
# GitHub release for the tag.
#
#   scripts/release.sh binaries <version>   dist/release/celastro-<version>-linux-{amd64,arm64}.tar.gz and SHA256SUMS
#   scripts/release.sh publish <version>    the release v<version> on GitHub, notes from CHANGELOG.md, the files attached;
#                                           a token with contents:write on stdin (`cat token | scripts/release.sh publish 1.2.3`)
#
# Needs: docker with the images from `scripts/image.sh build <version>`;
# curl and jq for publish; the tag v<version> pushed.
set -euo pipefail
cmd=${1:-}; v=${2:-}
[[ -n $cmd && -n $v ]] || { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
img=ghcr.io/celastro/celastro
repo=celastro/celastro
src=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$src"
out=dist/release

case $cmd in
  binaries)
    rm -rf "$out"; mkdir -p "$out"
    for arch in amd64 arm64; do
      # A container is the one way to read a file out of an image that
      # has no shell; it never runs.
      id=$(docker create --platform "linux/$arch" "$img:$v-$arch")
      stage=$(mktemp -d)
      for f in celastro celastro-cli LICENSE COPYRIGHT; do docker cp -q "$id:/$f" "$stage/$f"; done
      docker rm "$id" >/dev/null
      chmod 0755 "$stage/celastro" "$stage/celastro-cli"; chmod 0644 "$stage/LICENSE" "$stage/COPYRIGHT"
      # The same archive from the same files: a fixed owner, order and time,
      # so the sum does not move between two runs on one image.
      tar --owner=0 --group=0 --numeric-owner --mtime="@$(git log -1 --format=%ct "v$v" 2>/dev/null || date +%s)" --sort=name \
        -czf "$out/celastro-$v-linux-$arch.tar.gz" -C "$stage" celastro celastro-cli LICENSE COPYRIGHT
      rm -rf "$stage"
    done
    (cd "$out" && sha256sum celastro-*.tar.gz > SHA256SUMS && cat SHA256SUMS)
    echo "wrote $out/celastro-$v-linux-{amd64,arm64}.tar.gz and $out/SHA256SUMS" ;;
  publish)
    for f in "$out/celastro-$v-linux-amd64.tar.gz" "$out/celastro-$v-linux-arm64.tar.gz" "$out/SHA256SUMS"; do
      [[ -f $f ]] || { echo "no $f: run \`scripts/release.sh binaries $v\` first" >&2; exit 1; }
    done
    IFS= read -r token || true
    [[ -n $token ]] || { echo "the token comes on stdin" >&2; exit 1; }
    # curl reads the header from a config file so the token is on no
    # command line; the file is a pipe that only this process sees.
    auth() { printf 'header = "Authorization: Bearer %s"\nheader = "Accept: application/vnd.github+json"\n' "$token"; }
    api="https://api.github.com/repos/$repo"
    # The changelog's section for the version is the release's notes.
    notes=$(awk -v v="$v" '
      /^## / { on = (index($0, "## " v " ") == 1) ; if (on) next }
      on { print }' CHANGELOG.md | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}')
    body=$(jq -n --arg tag "v$v" --arg name "v$v" --arg notes "$notes" '{tag_name:$tag, name:$name, body:$notes, draft:false, prerelease:false}')
    release=$(curl -sS -K <(auth) "$api/releases/tags/v$v")
    id=$(printf '%s' "$release" | jq -r '.id // empty')
    if [[ -z $id ]]; then
      release=$(curl -sS -K <(auth) -X POST "$api/releases" -d "$body")
      id=$(printf '%s' "$release" | jq -r '.id // empty')
      [[ -n $id ]] || { echo "could not create the release: $(printf '%s' "$release" | jq -r '.message // .')" >&2; exit 1; }
      echo "created release v$v"
    else
      curl -sS -K <(auth) -X PATCH "$api/releases/$id" -d "$body" >/dev/null
      echo "release v$v exists; notes updated"
    fi
    upload="https://uploads.github.com/repos/$repo/releases/$id/assets"
    for f in "$out/celastro-$v-linux-amd64.tar.gz" "$out/celastro-$v-linux-arm64.tar.gz" "$out/SHA256SUMS"; do
      name=$(basename "$f")
      # An asset already there is replaced, so a rerun converges.
      old=$(printf '%s' "$release" | jq -r --arg n "$name" '.assets[]? | select(.name==$n) | .id')
      [[ -z $old ]] || curl -sS -K <(auth) -X DELETE "$api/releases/assets/$old"
      type="application/gzip"; [[ $name == SHA256SUMS ]] && type="text/plain"
      r=$(curl -sS -K <(auth) -X POST -H "Content-Type: $type" --data-binary "@$f" "$upload?name=$name")
      state=$(printf '%s' "$r" | jq -r '.state // .message // "?"')
      echo "  $name: $state"
      [[ $state == uploaded ]] || exit 1
    done
    echo "published https://github.com/$repo/releases/tag/v$v" ;;
  *) echo "unknown command: $cmd" >&2; exit 1 ;;
esac
