//! The statement deadline, consultable from any loop without carrying it.
//!
//! A statement's CPU used to be bounded only by an opt-in: `WITH
//! (deadline_ms)` existed, was off by default, and was checked between
//! shards -- so one shard's work, a wide prefix walk or a low-selectivity
//! graph traversal, ran to completion however long that took. The deadline
//! is now on by default (`DbOpts::statement_deadline_ms`) and is consulted
//! INSIDE the expensive loops: graph traversal, brute-force distance, WAND,
//! the prefix walk and the unranked scan each stop when it has passed.
//!
//! A loop that stops early returns less than it was asked for, which is
//! exactly the silent partial answer this engine refuses to give -- so a
//! loop never reports the cut itself. The executor asks [`passed`] after
//! every unit and at the end of the statement, and turns a passed deadline
//! into `Error::Deadline`, or into a missing shard under
//! `WITH (partial_results)`. The loop's early exit bounds the work; the
//! executor's check bounds the promise.
//!
//! Thread-local rather than a parameter, because the loops that need it are
//! six calls deep in modules that know nothing about statements, and a
//! parameter threaded through `Scorer::advance` would change a trait every
//! text query implements. One statement runs on one thread, and a statement
//! that runs another (a `DELETE` evaluating its predicate) nests: the guard
//! restores whatever was armed before it.

use std::cell::Cell;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

thread_local! {
    /// The instant the current statement must finish by, and the budget it
    /// was given, for the message.
    static ARMED: Cell<Option<(Instant, u64)>> = const { Cell::new(None) };
    /// Calls to [`expired`] since the clock was last read. The clock is read
    /// every [`STRIDE`] calls: a loop step is tens of nanoseconds and
    /// `Instant::now` is about as much again, so reading it every step would
    /// double the cost of the loop it guards.
    static TICKS: Cell<u32> = const { Cell::new(0) };
    /// Set once the clock has been seen past the deadline, so that every
    /// later ask is answered without the clock and no loop runs another
    /// stride after the first one that noticed.
    static TRIPPED: Cell<bool> = const { Cell::new(false) };
}

const STRIDE: u32 = 128;

/// Arm a deadline of `ms` milliseconds from now for the current thread, or
/// none. The previous arming is restored when the guard drops.
pub fn arm(ms: Option<u64>) -> Armed {
    let previous = ARMED.with(|a| a.get());
    let next = ms.map(|ms| (Instant::now() + Duration::from_millis(ms), ms));
    ARMED.with(|a| a.set(next));
    TICKS.with(|t| t.set(0));
    let tripped = TRIPPED.with(|t| t.replace(false));
    Armed { previous, tripped }
}

pub struct Armed {
    previous: Option<(Instant, u64)>,
    tripped: bool,
}

impl Drop for Armed {
    fn drop(&mut self) {
        ARMED.with(|a| a.set(self.previous));
        TRIPPED.with(|t| t.set(self.tripped));
    }
}

/// The budget the current statement was given, if any.
/// What is left of the statement's budget, in milliseconds, or `None` under
/// no deadline. Zero once it has passed. What a call across the wire hands
/// the other node, which arms it for its half of the work.
pub fn remaining_ms() -> Option<u64> {
    ARMED
        .with(|a| a.get())
        .map(|(until, _)| until.saturating_duration_since(Instant::now()).as_millis() as u64)
}

pub fn limit_ms() -> Option<u64> {
    ARMED.with(|a| a.get()).map(|(_, ms)| ms)
}

/// Whether the deadline has passed, cheaply: for the inside of a loop. The
/// clock is read on the first call after arming and every `STRIDE` calls
/// after, so a loop that stops on this runs at most `STRIDE` steps past the
/// deadline -- and once it has been seen to pass, every later ask says so
/// without the clock. Never true when nothing is armed.
pub fn expired() -> bool {
    let Some((until, _)) = ARMED.with(|a| a.get()) else { return false };
    if TRIPPED.with(|t| t.get()) {
        return true;
    }
    let n = TICKS.with(|t| {
        let n = t.get();
        t.set(n.wrapping_add(1));
        n
    });
    if n % STRIDE != 0 {
        return false;
    }
    let over = Instant::now() >= until;
    if over {
        TRIPPED.with(|t| t.set(true));
    }
    over
}

/// The budget, if the deadline has passed: the precise check, for the
/// executor at a unit or shard boundary. Reads the clock unless a loop has
/// already seen it pass.
pub fn passed() -> Option<u64> {
    let (until, ms) = ARMED.with(|a| a.get())?;
    if TRIPPED.with(|t| t.get()) || Instant::now() >= until {
        TRIPPED.with(|t| t.set(true));
        return Some(ms);
    }
    None
}

/// `Err` if the deadline has passed, naming the budget and the two ways to
/// change it.
pub fn check() -> Result<()> {
    match passed() {
        Some(ms) => Err(exceeded(ms)),
        None => Ok(()),
    }
}

pub fn exceeded(ms: u64) -> Error {
    Error::Deadline(format!(
        "the statement did not finish within {ms} ms; raise it with WITH (deadline_ms = N), \
         lift it for this statement with WITH (no_deadline), or set \
         DbOpts::statement_deadline_ms"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing armed is never expired, however often it is asked. A budget of
    /// zero is expired on the first ask and stays so. The guard restores what
    /// was armed before it, so a nested statement cannot leave its parent
    /// without a deadline, or with the wrong one.
    #[test]
    fn a_deadline_is_armed_per_statement_and_restored_when_the_statement_ends() {
        assert!(!expired() && passed().is_none() && check().is_ok());
        {
            let _outer = arm(Some(60_000));
            assert_eq!(limit_ms(), Some(60_000));
            assert!(!expired(), "sixty seconds passed at once");
            {
                let _inner = arm(Some(0));
                assert_eq!(limit_ms(), Some(0));
                assert!(expired(), "a zero budget was not expired on the first ask");
                for _ in 0..(STRIDE * 3) {
                    expired();
                }
                assert!(passed().is_some());
                assert!(matches!(check(), Err(Error::Deadline(m)) if m.contains("0 ms")));
            }
            assert_eq!(limit_ms(), Some(60_000), "the inner guard did not restore the outer");
            assert!(check().is_ok());
            assert!(!expired(), "the inner statement's verdict leaked into the outer");
        }
        assert_eq!(limit_ms(), None);
        assert!(!expired());
    }
}
