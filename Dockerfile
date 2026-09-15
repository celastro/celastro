# celastro in a container.
#
#   docker build -t celastro .
#   docker run --rm -it -v celastro-data:/data celastro --dir /data repl
#
# Two stages. The first compiles one statically linked binary against musl; the
# second is `scratch`, so the image is that binary, the licence it is conveyed
# under, a data directory and nothing else — no shell, no libc, no package
# manager, nothing to patch.

# ---- build ---------------------------------------------------------------

# Alpine's own host target is <arch>-unknown-linux-musl, which links crt-static
# by default, so `cargo build --release` already produces a static binary and
# no target triple is written down anywhere. That is what makes this file work
# unchanged on arm64: a hardcoded x86_64-unknown-linux-musl would need a
# cross-linker there.
#
# The tag names both halves of the toolchain — the compiler version and the
# Alpine it sits on — so a rebuild gets rustc 1.98 on Alpine 3.21 rather than
# whatever `latest` has become, and `--locked` below keeps working against a
# lockfile this repository does not regenerate. A tag is not a digest: the
# registry can repoint this one at a rebuilt image, so what it buys is a fixed
# toolchain version, not a byte-identical base. Today it resolves to
# rust@sha256:88a07cc2e9b783133cddf0ea84e759a1185eb6e7ff104deea18e985d84503af7;
# write that digest in here instead if you need the stronger property.
#
# Bumping the compiler here does not relax the declared MSRV: 1.75 is what the
# library promises and what the msrv gate checks, and this only fixes which
# newer compiler builds the shipped binary.
FROM rust:1.98-alpine3.21 AS build

# The musl crt objects and the linker the compiler drives.
RUN apk add --no-cache musl-dev

WORKDIR /src
COPY Cargo.toml Cargo.lock ./

# The whole of src/, not only the .rs files: src/serve.rs builds the browser
# console into the binary with include_str! on serve/index.html, serve/app.js
# and serve/style.css. Narrowing this COPY — or letting .dockerignore exclude
# non-Rust files under src/ — breaks the build with an include_str! error that
# points at a file nobody deleted.
COPY src ./src

# The one size lever, and it is applied after codegen: -C strip=symbols drops
# the symbol table from the linked binary and changes nothing about what was
# compiled. [profile.release] stays the maintainer's, so the code in this image
# is the code `cargo build --release` produces on the same target.
ENV RUSTFLAGS="-C strip=symbols"

# No dependency-caching stage, deliberately. [dependencies] in Cargo.toml is
# empty by policy, so Cargo.lock names exactly one package — this one — and the
# usual dummy-main.rs or cargo-chef layer would cache nothing while adding a
# layer and a way to be wrong.
#
# --offline is that policy as a build gate: nothing is vendored into this layer
# and the builder has no reason to reach the network, so a dependency that ever
# appears in Cargo.toml fails here, loudly, instead of quietly resolving.
# --locked refuses to rewrite Cargo.lock, which CONTRIBUTING.md requires.
#
# Only celastro-cli is built. It is a superset of the older `celastro` binary
# (--dir D -> --dir D repl, --file F -> run F, --demo -> demo) and each binary
# would statically link its own copy of std, so shipping both would roughly
# double the image for no new function.
RUN cargo build --release --locked --offline --bin celastro-cli

# The licence and the copyright notice are staged here rather than COPYed
# straight into `scratch` so their mode is fixed by this file instead of by
# the umask of whoever checked the repository out — `scratch` has no shell to
# chmod in. After the build, so they cannot invalidate it.
COPY LICENSE /LICENSE
COPY COPYRIGHT /COPYRIGHT
RUN chmod 0644 /LICENSE /COPYRIGHT

# scratch has no shell, so the mount point and its ownership have to be made
# here, in a stage that has one. 65532 is the conventional unprivileged
# non-root UID; nothing in celastro resolves a UID to a name, so no passwd file
# is needed and an operator can substitute any --user they like.
RUN mkdir -p /data && chown 65532:65532 /data && touch /data/.keep \
    && chown 65532:65532 /data/.keep

# ---- image ---------------------------------------------------------------

FROM scratch

LABEL org.opencontainers.image.title="celastro" \
      org.opencontainers.image.description="Hybrid document database: SQL, BM25 and vector retrieval in a single query plan" \
      org.opencontainers.image.source="https://github.com/celastro/celastro" \
      org.opencontainers.image.licenses="AGPL-3.0-only"

# No --chown on the binary, deliberately: it lands root-owned and mode 0755, so
# the unprivileged user this image runs as can read and execute it and nobody
# inside the container can write it. Giving it to 65532 would hand the running
# process write access to its own executable, which is a capability with no use
# and one obvious misuse.
COPY --from=build /src/target/release/celastro-cli /celastro-cli

# AGPL-3.0-only asks that the licence and the notices travel with the object
# code they cover (§4), and this image is that object code plus nothing — the
# SPDX string in the LABEL above is metadata, not a copy. 34,523 bytes of
# licence and 660 of notice against about 2 MB. Root-owned and
# world-readable, like the binary.
COPY --from=build /LICENSE /LICENSE
COPY --from=build /COPYRIGHT /COPYRIGHT

# COPY of a directory copies its contents, not the directory, so /data would
# not exist in this image at all if it were empty upstairs. The .keep file is
# what makes /data — and its owner — exist here, and Docker seeds a fresh named
# volume from the image content at this path including that ownership, which is
# what lets UID 65532 write to `-v celastro-data:/data` with no setup.
COPY --from=build --chown=65532:65532 /data /data

USER 65532:65532
WORKDIR /data

# The data directory is not baked into the entrypoint. Without --dir the tool
# opens an in-memory database, which is what `demo` needs (it refuses --dir
# outright) and what makes `exec` usable with no volume at all. A bare
# entrypoint also composes: global flags precede the verb, so
# `docker run IMAGE --dir /data exec 'SELECT ...'` reads the way it should.
#
# No VOLUME /data either: it would create an anonymous volume on every run that
# forgot -v, and they accumulate unnoticed.
ENTRYPOINT ["/celastro-cli"]
CMD ["repl"]
