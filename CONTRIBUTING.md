# Contributing to celastro

Issues are welcome and wanted. Pull requests are not accepted.

## Please open issues

The most valuable thing anyone outside the project can contribute is a precise
bug report. Specifically:

- **Wrong results.** A query that returns something the SQL semantics say it
  should not, ideally with the smallest corpus that shows it.
- **Corruption or panics.** A reachable panic is a bug by definition — the
  release profile aborts rather than unwinds, so a panic is never a recoverable
  condition being handled.
- **Plan regressions.** `EXPLAIN ANALYZE` output where the chosen strategy is
  clearly worse than an available alternative.
- **Documentation that is wrong**, as opposed to absent. The README describes
  invariants; if the code disagrees with it, one of the two is a bug.

Feature requests and design discussion belong in issues too. Say what you want
to be true and why; that is a conversation worth having.

## Why pull requests are not accepted

This is not a judgement about the quality of anyone's patches, and it is not a
soft "no" that becomes yes for a good enough diff. It is structural:

- **The invariants are load-bearing and only partly written down.** Inside a
  segment every index type produces sets in the same `u32` ordinal space. A
  change that is locally correct can violate that globally, and reviewing for it
  requires context that is not in the tree yet.
- **The minimal-dependency rule is absolute.** Nothing outside `std` without
  the `tls` feature, and behind it only rustls with the ring provider and what
  those two bring (Apache-2.0, ISC and MIT, all of them). A patch that adds a
  dependency cannot be taken no matter what it does, and that is easier to
  state up front than to litigate per PR.
- **Provenance.** celastro is AGPL-3.0-only with no CLA. Keeping the history
  free of outside copyright keeps relicensing and dual-licensing decisions
  simple, and there is no mechanism here for negotiating that per contributor.

A PR opened from a fork will be closed with a pointer to this file. That is a
policy response, not a review.

## Your fork

The AGPL grants what it grants. Fork it, modify it, run it, redistribute it
under the same terms — nothing in this document narrows the license. The only
thing being declined is the merge back.

## Security

Do not report vulnerabilities in a public issue. See [SECURITY.md](SECURITY.md).
