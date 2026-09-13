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

celastro is an embedded, single-node engine, and for the library and the two
REPLs the interesting boundary is **untrusted input**, not untrusted callers.
`celastro-cli serve` adds a second boundary: it is a real HTTP listener that
executes arbitrary SQL, so there the caller is untrusted too.

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

### The browser console (`celastro-cli serve`)

The console binds `127.0.0.1` only, on port 8787 by default, and is guarded by a
per-run token from `/dev/urandom` presented as `?t=` or `X-Celastro-Token`.
Everything behind that token is a SQL prompt, so anything that reaches the
executor without it is a serious finding:

- **Reaching `/query` or `/exec` without a valid token**, by any route.
- **Token recovery.** Any way for a page, or a local process that cannot
  already read the console's own output, to learn or narrow the token. The
  comparison is constant-time by construction; a timing signal in it counts.
- **DNS rebinding.** The `Host` allow-list admits only `localhost`,
  `127.0.0.1` and `[::1]`. A `Host` that gets past it is in scope.
- **Cross-origin requests.** State-changing requests require an `Origin` that
  is ours or absent, `Sec-Fetch-Site: same-origin` when present, and
  `application/json`. A cross-origin page that reaches the executor anyway —
  through a form post, a preflight-free content type, or a header the parser
  treats differently from the browser — is in scope.
- **HTTP parsing.** The server is hand-rolled on `std::net::TcpListener`.
  Request smuggling, header injection into a response, duplicate `Content-Length`
  or `Host` handling, and anything that desynchronises the connection are in
  scope.
- **XSS in the console page.** The page renders documents straight out of the
  database and builds every node with `textContent`. A stored document that
  executes script in the console is in scope.
- **Bypassing the limits.** The 1 MiB body cap and the absolute per-request
  deadline are what keep a single-threaded loop from being held open. A request
  that evades either is in scope.

## What is not in scope

- Anything requiring write access to the data directory. A caller who can
  rewrite segments can already do anything; the trust boundary is below that.
- **Anyone who already has the console token.** It is printed on stdout and
  sits in the URL, so a local user who can read the process's output or the
  browser's history is inside the boundary by design, not past it.
- **That `serve` executes arbitrary SQL.** That is the feature. The question is
  only ever whether something reached it without the token.
- **Deliberately exposing the console.** It binds loopback and offers no flag to
  do otherwise; putting a reverse proxy or an SSH tunnel in front of it is a
  decision about your network, not a defect in this one.
- Resource exhaustion from queries the caller is authorised to run. There is no
  query governor, and that is a known absence rather than a vulnerability.
- Missing features listed under "What is deliberately not here" in the README.
  No replication means no replication vulnerabilities.
