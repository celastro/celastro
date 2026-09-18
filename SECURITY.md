# Security policy

## Reporting a vulnerability

**Do not open a public issue for a vulnerability.**

Use GitHub's private reporting instead:
[**Report a vulnerability**](https://github.com/celastro/celastro/security/advisories/new).
That opens a private advisory visible only to the maintainers, and it is the
only supported channel.

Please include a reproduction — a corpus, a query, a byte sequence or an HTTP
request — and what you believe the impact is. Expect an initial response within
a week.

## Supported versions

celastro is pre-1.0, at `0.x`. Only `main` is supported; there are no
backported fixes to earlier tags.

## The two trust boundaries

For the library and the two REPLs the boundary is **untrusted input**, not
untrusted callers. `celastro serve` adds a second: an HTTP listener that
executes arbitrary SQL, so there the caller is untrusted too — on loopback by
default, on a network with `--bind`, and over TLS when certificates are
given.

### Untrusted input

- **Malformed segment files.** A corrupt or hand-crafted segment on disk that
  causes out-of-bounds reads, a panic in a decoder, or memory unsafety rather
  than a clean `Error`.
- **Malicious JSON or SQL.** Parser input that causes unbounded allocation,
  non-terminating parses, or stack exhaustion.
- **Codec and quantizer boundaries.** Block decoders and vector codecs index
  deliberately for speed; a length or offset from a file that escapes its bounds
  check is in scope.
- **MVCC visibility.** Any way to read a version a snapshot should not see.

### The console (`celastro serve`)

On loopback the console is guarded by a per-run token from `/dev/urandom`
(`?t=` or `X-Celastro-Token`), a `Host` allow-list and a same-origin check;
with `--bind` it answers the operator's token from `CELASTRO_TOKEN` and the
`Host` allow-list gives way to the token, a browser's `Origin` having to be
the `Host` it named; with certificates it serves TLS 1.3 and verifies peers.
Everything behind the token is a SQL prompt, so anything that reaches the
executor without it is a serious finding:

- **Reaching `/api/query` without a valid token**, by any route, on either
  bind.
- **Token recovery.** Any way for a page, or a process that cannot already
  read the console's own output or environment, to learn or narrow the token.
  The comparison is constant-time by construction; a timing signal counts.
- **DNS rebinding** on loopback: a `Host` past `localhost`, `127.0.0.1` and
  `[::1]`. On a network bind, an `Origin` other than the request's own `Host`
  that gets a state-changing request through.
- **Cross-origin requests.** State-changing requests require an `Origin` that
  is ours or absent, `Sec-Fetch-Site: same-origin` when present, and
  `application/json`. A cross-origin page that reaches the executor anyway is
  in scope.
- **HTTP parsing.** The server is hand-rolled on `std::net`. Request
  smuggling, header injection, duplicate `Content-Length` or `Host` handling
  and anything that desynchronises a connection are in scope.
- **XSS in the console page.** The page builds every node with `textContent`;
  a stored document that executes script in it is in scope.
- **Bypassing the limits.** The 1 MiB body cap, the per-request deadline and
  the connection cap are what keep the listener from being held open. A
  request that evades one is in scope.
- **TLS.** A peer accepted without a chain to the CA, a name the
  certificate does not carry accepted, a plain connection served where TLS
  was configured, or a weakness in the in-tree TLS itself (below).

### The token, on a network

With `--bind` off loopback the API (`/api/*`) takes the token in the
`X-Celastro-Token` header only: a `?t=` in a URL is written into every
proxy's and balancer's access log and into a browser's history. The page
and its two assets still take `?t=`, since a `<link>` and a `<script>`
can carry nothing else. Tokens compare in constant time, and a source
address that was refused waits a hundred milliseconds more per refusal in
the last minute, two seconds at most, before it is answered again.

### Secrets in memory

The data key, the master key, TLS traffic keys, session tickets, the
node's private key, S3 credentials and the tokens overwrite themselves
with zeros when they are dropped (`cipher::wipe`, a volatile write per
byte), so a key does not outlive its use in freed memory that a later
allocation, a core dump or a swap file could show. A running process
holds them; that is the boundary.

## What is not in scope

- Anything requiring write access to the data directory. A caller who can
  rewrite segments can already do anything; the trust boundary is below that.
- **Anyone who already has the console token.** It is printed on stdout and
  sits in the URL, so a local user who can read the process's output or the
  browser's history is inside the boundary by design, not past it.
- **That `serve` executes arbitrary SQL.** That is the feature. The question is
  only ever whether something reached it without the token.
- **Exposing the console deliberately.** `--bind` is a documented decision
  about your network with the token as its guard; so is a reverse proxy or an
  SSH tunnel in front of the loopback bind.
- Resource exhaustion from queries the caller is authorised to run. There is no
  query governor, and that is a known absence rather than a vulnerability.
- Missing features listed under "What is deliberately not here" in the README.
  No replication means no replication vulnerabilities.

## Rotating the wire token

A rotation by one rolling update deadlocks: the first pod on the new token
can attach nobody, is never ready, and the rollout never moves, which
leaves that pod partitioned from the rest until someone intervenes. A
drill showed exactly that. So a node accepts a second token,
`CELASTRO_WIRE_TOKEN_ALSO`, and a rotation is three rollouts, each one
leaving every pair of nodes with a token in common:

1. every node accepts the new token too (`CELASTRO_WIRE_TOKEN_ALSO=new`);
2. every node sends the new token and still accepts the old
   (`CELASTRO_WIRE_TOKEN=new`, `CELASTRO_WIRE_TOKEN_ALSO=old`);
3. the old one is dropped (`CELASTRO_WIRE_TOKEN_ALSO` unset).

The chart's `wire.tokenAlso` is the second token; it is visible in the
release's values while set, which is why the third step clears it. The
console token is per node and per client and rotates on its own terms:
clients take the new one when the Secret changes and the pods restart.

A certificate rotation has the same shape, and the CA file may hold
several certificates for it: every node trusts the new CA too (a rollout),
every node presents a leaf from the new CA and still trusts the old (a
rollout), the old CA is dropped (a rollout). Certificates made by
`celastro tls init` carry subject and authority key identifiers, so a
client whose library matches an issuer by key identifier when there is
one -- OpenSSL, so curl and Python -- picks the right CA of two under one
name; material made before 0.51.0 carries none, and for it the new CA
needs a different name, since such a client then takes the first of that
name and fails the signature against it. A node tries every anchor either
way. A drill runs the three steps under TLS on the wire and the console
and checks every count between them.

## The TLS, and what it is

The TLS is written in this repository (`src/crypto`), because the crate
carries no dependency, and **nobody outside it has reviewed it**. What it
is: TLS 1.3 only; one cipher suite, `TLS_CHACHA20_POLY1305_SHA256`; X25519
key exchange; the node's own certificate is Ed25519 (material from
cert-manager needs `privateKey.algorithm: Ed25519`); the CA above it and
any intermediate may be Ed25519, RSA (PKCS#1 v1.5 or PSS with SHA-256) or
ECDSA P-256, and as a client the node accepts servers signing with those
too, which is how it reaches a Kubernetes API and answers its
`CertificateRequest` with an empty certificate. Session resumption by
ticket (0.40.0): after every handshake the server sends a
NewSessionTicket sealed under a key derived from its TLS private key --
the same on every node serving the same certificate, so a ticket resumes
at any of them -- and a client that offers it within a day, with a key
share (PSK with (EC)DHE only, so forward secrecy is kept), skips the
certificate flight; a ticket the server cannot open, or a binder that does
not verify, is a full handshake or a refusal, never a downgrade. This
client offers a ticket only to the name, address and trust anchors it was
issued under. No client certificates, no HelloRetryRequest, no 0-RTT, no
key update; a stock client speaks that subset. Every primitive is pinned against its RFC
vectors and the key schedule against RFC 8448; nothing branches on or
indexes by a secret, by masks rather than by asking the compiler. The
archive client reaches an `https://` store over the same TLS, verifying
the chain against the bundle `CELASTRO_ARCHIVE_CA` names or the system's,
with wildcard names in the leftmost label as RFC 6125 has them; a plain
`http://` endpoint is what it says. A finding against any of this is in
scope. Every parser -- X.509, PEM, the handshake messages, the console's
request heads, the store's responses, the wire's frames, and every file
format -- is fuzzed in the test suite with a seeded mutator
(`src/fuzz.rs`), and a decoder that reserves memory for a count it read
is bounded by the bytes that remain.

A certificate has an end, and at it every peer refuses this node and
every client does too, all at once. `SHOW HEALTH` says when this node's
certificate and the first of its trust anchors expire and flags either
inside two weeks; the metrics page carries both instants
(`celastro_tls_certificate_expiry_seconds`, `celastro_tls_ca_expiry_seconds`)
for an alert. Rotate by the chart's or your own issuer before then; a
rotation's safe order across a cluster is not yet drilled.

## Encryption at rest, and what it is

Off by default: without a master key every file is written in the clear,
and nothing warns about it beyond this sentence and the README's. With
`CELASTRO_MASTER_KEY_FILE` or `CELASTRO_MASTER_KEY` set, every file
the database writes -- segments, delete logs, manifests, the write-ahead
log, `RANGE`, `CATALOG`, the objects an archived tier puts in a store,
backups and exports -- is a sequence of ChaCha20-Poly1305 frames (64 KiB
of plaintext each, a fresh random nonce per frame, the file's identity and
the frame's index authenticated) under a key HKDF derives per file from
one data key; the data key is drawn at the first open and kept in
`<dir>/KEY` wrapped under the master key, which is never written. The
same in-tree, unaudited primitives as the TLS.

What it protects against: a copied volume, a lost disk, a bucket or a
backup read by someone without the master key -- none of it is readable
or quietly alterable, and a file cannot be swapped for another. What it
does not: a caller with the master key, or with the running process (the
data key is in memory, and every row a query touches is plaintext there);
the sizes and names of files and the shape of the directory, which are
not hidden; a torn frame at the WAL's tail, which is dropped as a torn
record is. A finding against any of this is in scope.
