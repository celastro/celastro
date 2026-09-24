//! Hybrid logical clock and timestamp formatting.
//!
//! Every version carries a commit timestamp (§6). A hybrid logical clock keeps
//! those timestamps close to wall time — so `now() - interval '30 days'` means
//! something — while guaranteeing that a node never issues a timestamp that
//! goes backwards, which snapshot isolation depends on. In a single-node build
//! the causality half is only exercised by `observe`, but the type is the one
//! the distributed layer needs, so it exists from the start.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// The writes whose log records are appended and not yet synced, by
    /// their first timestamp: group commit (§6). A read is held below the
    /// oldest of them, so nothing a crash could still take back is ever
    /// seen, and `horizon` is that bound, `MAX_TS` when nothing is in flight.
    in_flight: Mutex<BTreeMap<Timestamp, usize>>,
    horizon: AtomicU64,
    settled: Condvar,
    /// A write's log failed to sync: its rows stay applied and hidden, the
    /// horizon stays below them until a restart, and nothing waits on it.
    failed: std::sync::atomic::AtomicBool,
}

impl Default for Hlc {
    fn default() -> Self {
        Hlc::new()
    }
}

impl Hlc {
    pub fn new() -> Self {
        Hlc {
            packed: AtomicU64::new(0),
            in_flight: Mutex::new(BTreeMap::new()),
            horizon: AtomicU64::new(MAX_TS),
            settled: Condvar::new(),
            failed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn wall() -> u64 {
        now_micros().max(0) as u64
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

impl Hlc {
    /// `t`, held below every write still waiting for its log's sync: the
    /// timestamp a read may use. A write is appended and applied under the
    /// database's lock and synced after it is let go, so between the two a
    /// reader could otherwise see a row a crash would take back. Every read
    /// snapshot passes through here; a write in flight has a timestamp above
    /// the value returned, so it is invisible until it is durable.
    pub fn visible(&self, t: Timestamp) -> Timestamp {
        t.min(self.horizon.load(Ordering::Acquire))
    }

    /// Whether any write is appended and not yet synced.
    pub fn writes_in_flight(&self) -> bool {
        self.horizon.load(Ordering::Acquire) != MAX_TS
    }

    /// A write whose timestamps begin at `first` is on the log and not yet
    /// synced. Called under the database's write lock, so no reader can pin a
    /// snapshot between the timestamp's issue and this.
    pub fn begin(&self, first: Timestamp) {
        let mut f = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        *f.entry(first).or_insert(0) += 1;
        self.horizon.store(Self::bound(&f), Ordering::Release);
    }

    /// The write begun at `first` is durable, or was refused and taken back.
    pub fn end(&self, first: Timestamp) {
        let mut f = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(n) = f.get_mut(&first) {
            *n -= 1;
            if *n == 0 {
                f.remove(&first);
            }
        }
        self.horizon.store(Self::bound(&f), Ordering::Release);
        self.settled.notify_all();
    }

    /// Wait until a read would see `t`: every write that began before it has
    /// settled. What an acknowledgement waits for, so a client reads its own
    /// write the moment it is told of it. `false` at `until`.
    pub fn wait_visible(&self, t: Timestamp, until: Instant) -> bool {
        let mut f = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if Self::bound(&f) >= t {
                return true;
            }
            if self.failed.load(Ordering::Acquire) {
                return false;
            }
            let now = Instant::now();
            if now >= until {
                return false;
            }
            let wait = (until - now).min(Duration::from_millis(100));
            f = self.settled.wait_timeout(f, wait).unwrap_or_else(|p| p.into_inner()).0;
        }
    }

    /// A write in flight will never settle: its log failed to sync. Reads
    /// stay below it; waiters stop waiting.
    pub fn fail(&self) {
        let _f = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        self.failed.store(true, Ordering::Release);
        self.settled.notify_all();
    }

    /// Whether a write's log failed to sync since this node started.
    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn bound(f: &BTreeMap<Timestamp, usize>) -> Timestamp {
        f.keys().next().map_or(MAX_TS, |first| first - 1)
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
    // `days_from_civil` normalises an impossible day instead of rejecting it, so
    // without a real month length here "2026-02-31" would parse as 2026-03-03 —
    // a range query would silently mean another day, and a caller that rewrites
    // the coerced value would store one.
    if !(1..=12).contains(&mo) || !(1..=days_in_month(y, mo)).contains(&d) {
        return None;
    }
    let mut micros = days_from_civil(y, mo as u32, d as u32).checked_mul(86_400_000_000)?;
    if b.len() > 10 {
        if b[10] != b'T' && b[10] != b' ' {
            return None;
        }
        let rest = &s[11..];
        let rest = rest.strip_suffix('Z').unwrap_or(rest);
        let mut parts = rest.split(':');
        let h = parse_time_field(parts.next()?, 23)?;
        let mi = parse_time_field(parts.next().unwrap_or("0"), 59)?;
        let sec_part = parts.next().unwrap_or("0");
        // 60 seconds is allowed so a leap second reads as the following second
        // rather than being rejected outright.
        let (sec, frac) = match sec_part.split_once('.') {
            Some((a, f)) => {
                if f.is_empty() || !f.bytes().all(|c| c.is_ascii_digit()) {
                    return None;
                }
                let mut f = f.to_string();
                while f.len() < 6 {
                    f.push('0');
                }
                // All ASCII digits and at least six of them, so both the slice
                // and the parse are infallible.
                (parse_time_field(a, 60)?, f[0..6].parse::<i64>().ok()?)
            }
            None => (parse_time_field(sec_part, 60)?, 0),
        };
        // Each field is range-checked, so the clock offset itself cannot
        // overflow; the add is checked because the date half is caller-sized.
        micros = micros.checked_add((h * 3600 + mi * 60 + sec) * 1_000_000 + frac)?;
    }
    Some(micros)
}

/// One clock field: one or two ASCII digits, no larger than `max`. The bound is
/// load-bearing — an hour or a seconds count arrives straight from SQL text, and
/// unbounded `h * 3600 * 1_000_000` overflows i64, which panics under
/// `cargo test` and wraps to a wrong instant in a release build.
fn parse_time_field(s: &str, max: i64) -> Option<i64> {
    if s.is_empty() || s.len() > 2 || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let v: i64 = s.parse().ok()?;
    (v <= max).then_some(v)
}

fn is_leap_year(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Length of a month in the proleptic Gregorian calendar, so that parsing can
/// reject days that never existed.
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if is_leap_year(y) => 29,
        _ => 28,
    }
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

/// A drill's offset on every read of the wall clock, microseconds:
/// `CELASTRO_CLOCK_OFFSET_MICROS`, so one node of a cluster on kind sees
/// a jumped clock without the kernel's clock moving. Zero outside a
/// drill. It tests the code's reaction to a jump, not the operating
/// system's; a VM per node is the real drill.
static CLOCK_OFFSET: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub fn set_clock_offset(micros: i64) {
    CLOCK_OFFSET.store(micros, std::sync::atomic::Ordering::Relaxed);
}

/// The wall clock, microseconds since the epoch, plus a drill's offset.
pub fn now_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
        + CLOCK_OFFSET.load(std::sync::atomic::Ordering::Relaxed)
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

    /// These strings reach `parse_iso8601` straight from SQL text, so an
    /// unbounded field is an overflow panic in debug and a wrong instant in
    /// release.
    #[test]
    fn an_out_of_range_clock_field_is_rejected_rather_than_overflowing_the_micros_multiply() {
        assert_eq!(parse_iso8601("1970-01-01T99999999999999:00:00"), None);
        assert_eq!(parse_iso8601("1970-01-01T00:00:99999999999999999"), None);
        assert_eq!(parse_iso8601("1970-01-01T24:00:00"), None);
        assert_eq!(parse_iso8601("1970-01-01T00:60:00"), None);
        assert_eq!(parse_iso8601("1970-01-01T00:00:61"), None);
        assert_eq!(parse_iso8601("1970-01-01T-5:00:00"), None);
        // A leap second still parses, as the second that follows it.
        assert_eq!(parse_iso8601("1970-01-01T00:00:60"), Some(60_000_000));
        assert_eq!(parse_iso8601("1970-01-01T23:59:59.999999Z"), Some(86_399_999_999));
    }

    /// `days_from_civil` normalises, so an unvalidated day silently becomes a
    /// different date instead of a parse failure.
    #[test]
    fn a_day_past_the_end_of_its_month_is_rejected_rather_than_rolling_into_the_next() {
        assert_eq!(parse_iso8601("2026-02-31"), None);
        assert_eq!(parse_iso8601("2026-04-31"), None);
        assert_eq!(parse_iso8601("2026-01-32"), None);
        assert_eq!(parse_iso8601("2026-01-00"), None);
        // Leap years, including the century rules on either side of 2000.
        assert_eq!(parse_iso8601("2026-02-29"), None);
        assert_eq!(parse_iso8601("1900-02-29"), None);
        assert!(parse_iso8601("2024-02-29").is_some());
        assert!(parse_iso8601("2000-02-29").is_some());
        assert!(parse_iso8601("2026-01-31").is_some());
        assert!(parse_iso8601("2026-04-30").is_some());
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
