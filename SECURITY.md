# Security policy

## Reporting a vulnerability

**Do not open a public issue for a vulnerability.**

Use GitHub's private reporting instead:
[**Report a vulnerability**](https://github.com/celastro/celastro/security/advisories/new).
That opens a private advisory visible only to the maintainers, and it is the
only supported channel.

Please include a reproduction — a corpus, a query or a byte sequence — and what
you believe the impact is. Expect an initial response within a week.

## Supported versions

celastro is pre-1.0 at `0.1.x`. Only `main` is supported; there are no
backported fixes to earlier tags.

## What is in scope

Because celastro is an embedded, single-node engine with no network listener,
the interesting boundary is **untrusted input**, not untrusted callers:

- **Malformed segment files.** A corrupt or hand-crafted segment on disk that
  causes out-of-bounds reads, a panic in a decoder, or memory unsafety rather
  than a clean `Error`.
- **Malicious JSON or SQL.** Parser input that causes unbounded allocation,
  non-terminating parses, or stack exhaustion.
- **Codec and quantizer boundaries.** Block decoders and vector codecs index
  deliberately for speed; a length or offset from a file that escapes its bounds
  check is in scope.
- **MVCC visibility.** Any way to read a version a snapshot should not see.

## What is not in scope

- Anything requiring write access to the data directory. A caller who can
  rewrite segments can already do anything; the trust boundary is below that.
- Resource exhaustion from queries the caller is authorised to run. There is no
  query governor, and that is a known absence rather than a vulnerability.
- Missing features listed under "What is deliberately not here" in the README.
  No replication means no replication vulnerabilities.
