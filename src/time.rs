//! Hybrid logical clock and timestamp formatting.
//!
//! Every version carries a commit timestamp (§6). A hybrid logical clock keeps
//! those timestamps close to wall time — so `now() - interval '30 days'` means
//! something — while guaranteeing that a node never issues a timestamp that
//! goes backwards, which snapshot isolation depends on. In a single-node build
//! the causality half is only exercised by `observe`, but the type is the one
//! the distributed layer needs, so it exists from the start.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds since the Unix epoch in the high bits, a logical counter in the
/// low 12. Packing both into one u64 keeps timestamps comparable with a plain
/// integer compare everywhere downstream.
pub type Timestamp = u64;

const LOGICAL_BITS: u32 = 12;
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;

pub fn physical_micros(ts: Timestamp) -> i64 {
    (ts >> LOGICAL_BITS) as i64
}

/// The logical counter — how many timestamps were issued inside the same
/// microsecond. Only meaningful when comparing two timestamps from one clock.
pub fn logical(ts: Timestamp) -> u64 {
    ts & LOGICAL_MASK
}

pub fn from_micros(micros: i64) -> Timestamp {
    (micros as u64) << LOGICAL_BITS
}

pub const MIN_TS: Timestamp = 0;
pub const MAX_TS: Timestamp = u64::MAX;

#[derive(Debug)]
pub struct Hlc {
    packed: AtomicU64,
}

impl Default for Hlc {
    fn default() -> Self {
        Hlc::new()
    }
}

impl Hlc {
    pub fn new() -> Self {
        Hlc { packed: AtomicU64::new(0) }
    }

    fn wall() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
    }

    /// Issue a timestamp strictly greater than every timestamp this clock has
    /// issued or observed.
    pub fn now(&self) -> Timestamp {
        let wall = Self::wall() << LOGICAL_BITS;
        loop {
            let prev = self.packed.load(Ordering::Acquire);
            let next = if wall > prev { wall } else { prev + 1 };
            if self
                .packed
                .compare_exchange_weak(prev, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Absorb a timestamp seen from elsewhere — the read-your-writes token a
    /// client carries between coordinators (§6).
    pub fn observe(&self, remote: Timestamp) {
        loop {
            let prev = self.packed.load(Ordering::Acquire);
            if prev >= remote {
                return;
            }
            if self
                .packed
                .compare_exchange_weak(prev, remote, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// A timestamp for a read snapshot: at least as new as everything already
    /// committed, and **published**, so that every later `now()` is strictly
    /// greater.
    ///
    /// Publishing is the whole point. Returning `max(committed, wall)` without
    /// storing it hands out a timestamp the clock has not committed to — and
    /// then a write in the same microsecond gets `committed + 1`, which can be
    /// *less than or equal to* a snapshot a reader is already holding. The
    /// write would appear inside a snapshot taken before it, which is precisely
    /// what snapshot isolation forbids. It also shows up as flaky tests, which
    /// is how this one was found.
    pub fn peek(&self) -> Timestamp {
        let wall = Self::wall() << LOGICAL_BITS;
        loop {
            let prev = self.packed.load(Ordering::Acquire);
            if prev >= wall {
                return prev;
            }
            if self
                .packed
                .compare_exchange_weak(prev, wall, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return wall;
            }
        }
    }
}

// --- Calendar formatting (no chrono; timestamps are proleptic Gregorian UTC) ---

pub fn format_micros(micros: i64) -> String {
    let (days, rem) = div_floor(micros, 86_400_000_000);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    let (h, mi, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    if frac == 0 {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:06}Z")
    }
}

/// Parse a subset of ISO-8601: `YYYY-MM-DD` optionally followed by
/// `THH:MM:SS[.ffffff]` and an optional `Z`. Deliberately strict — this only
/// runs on explicit casts and DDL-declared timestamp columns (§2.1).
pub fn parse_iso8601(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 10 {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    if b[4] != b'-' {
        return None;
    }
    let mo: i64 = s.get(5..7)?.parse().ok()?;
    if b[7] != b'-' {
        return None;
    }
    let d: i64 = s.get(8..10)?.parse().ok()?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let mut micros = days_from_civil(y, mo as u32, d as u32) * 86_400_000_000;
    if b.len() > 10 {
        if b[10] != b'T' && b[10] != b' ' {
            return None;
        }
        let rest = &s[11..];
        let rest = rest.strip_suffix('Z').unwrap_or(rest);
        let mut parts = rest.split(':');
        let h: i64 = parts.next()?.parse().ok()?;
        let mi: i64 = parts.next().unwrap_or("0").parse().ok()?;
        let sec_part = parts.next().unwrap_or("0");
        let (sec, frac) = match sec_part.split_once('.') {
            Some((a, f)) => {
                let mut f = f.to_string();
                while f.len() < 6 {
                    f.push('0');
                }
                (a.parse::<i64>().ok()?, f.get(0..6)?.parse::<i64>().ok()?)
            }
            None => (sec_part.parse::<i64>().ok()?, 0),
        };
        micros += (h * 3600 + mi * 60 + sec) * 1_000_000 + frac;
    }
    Some(micros)
}

fn div_floor(a: i64, b: i64) -> (i64, i64) {
    let q = a.div_euclid(b);
    (q, a.rem_euclid(b))
}

/// Howard Hinnant's days-from-civil algorithm.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = ((m + 9) % 12) as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn now_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_round_trip() {
        for s in ["1970-01-01", "2026-09-08", "2000-02-29", "1999-12-31T23:59:59Z"] {
            let m = parse_iso8601(s).unwrap();
            let back = format_micros(m);
            assert!(back.starts_with(&s[0..10]), "{s} -> {back}");
        }
        assert_eq!(parse_iso8601("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_iso8601("1970-01-02"), Some(86_400_000_000));
    }

    /// A snapshot pinned by `peek` must be closed: nothing may later commit at
    /// or before it.
    #[test]
    fn a_write_can_never_land_inside_an_already_pinned_snapshot() {
        let c = Hlc::new();
        for _ in 0..10_000 {
            let snapshot = c.peek();
            let commit = c.now();
            assert!(
                commit > snapshot,
                "commit {commit} landed at or before pinned snapshot {snapshot}"
            );
        }
    }

    #[test]
    fn hlc_is_monotonic() {
        let c = Hlc::new();
        let mut last = 0;
        for _ in 0..1000 {
            let t = c.now();
            assert!(t > last);
            last = t;
        }
        c.observe(last + 10_000);
        assert!(c.now() > last + 10_000);
    }
}
