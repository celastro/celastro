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
untrusted callers. `celastro-cli serve` adds a second: an HTTP listener that
executes arbitrary SQL, so there the caller is untrusted too — on loopback by
default, on a network with `--bind`, and over TLS with the `tls` feature.

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

### The console (`celastro-cli serve`)

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
- **TLS.** With the `tls` feature: a peer accepted without a chain to the
  CA, a name the certificate does not carry accepted, or a plain connection
  served where TLS was configured.

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
