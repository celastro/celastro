//! The lock on a [`Db`](crate::engine::Db), with the one rule the standard
//! one cannot express.
//!
//! A statement holds its coordinator's shared lock while it waits on the
//! other nodes' shards; a wire read that serves such a statement takes
//! the shared lock on the node that holds the shard. With the standard
//! lock a writer waiting on a node holds every new reader behind it --
//! the fair choice on one machine, and across nodes the closing of a
//! cycle: A's statement waits on B's wire read, held behind B's waiting
//! writer, which waits for B's readers -- B's statements, waiting on A's
//! wire reads, held behind A's waiting writer, waiting for A's readers:
//! A's statement. Under a mixed load every node has such a writer at any
//! moment (a forwarded insert, a statement's own write), and the five-node
//! suite saw its reads and writes wait out the thirty-second deadline on
//! idle CPUs.
//!
//! This lock has two kinds of shared acquisition. [`RwLock::read`] is the
//! standard one, and yields to a waiting writer: a statement that starts
//! here does. [`RwLock::read_served`] yields to a writer that holds the
//! lock, never to one that waits: a wire read serving another node's
//! statement does, since the coordinator behind it is holding its own
//! lock the whole time. Every wait across the network then ends in a
//! served read, which waits for local work only -- a holding writer, which
//! since 0.53.0 never waits on the network -- so no cycle can close.
//! Writers do not starve: the statements that start on a node are held
//! back the moment one waits, and the served reads are the short ones.
//!
//! Nor do readers, since 0.76.0. A writer's release lets in every read
//! that was already waiting before the next writer may take the lock --
//! one turn each, and a read that arrives after the release still queues
//! behind a waiting writer. Without that turn, a writer that asked again
//! the moment it let go was always first back: a point read under a steady
//! insert waited out several writes, each with its log sync, and with the
//! sync made 50 ms slower the read's median was 234 ms rather than one
//! sync's worth.
//!
//! Since 0.77.0 the writers take their turns in phases. The writers
//! waiting when one of them takes the lock after readers are a phase: they
//! go one after another, and only after the last of them are the reads
//! waiting let in -- where before every release let them in, so under a
//! mixed load each write waited out a round of reads and the write rate
//! was one per round (40-50 a second beside four vector readers, falling
//! as the collection grew and the rounds lengthened). A writer that asks
//! after its phase began is in the next one, so a writer that asks again
//! the moment it let go is still never first back, and a read waits out at
//! most the phase that was waiting when it arrived.
//!
//! The rest is the standard API -- `read`, `write`, `try_read`,
//! `try_write`, the guards and the poisoning -- so a `Db` is used as it
//! always was.

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, LockResult, Mutex, PoisonError, TryLockError, TryLockResult};

/// A reader-writer lock whose served reads a waiting writer cannot hold
/// back. See the module notes.
pub struct RwLock<T> {
    state: Mutex<State>,
    changed: Condvar,
    poisoned: AtomicBool,
    cell: UnsafeCell<T>,
}

#[derive(Default)]
struct State {
    readers: usize,
    writer: bool,
    writers_waiting: usize,
    /// Plain reads waiting now.
    readers_waiting: usize,
    /// Writer releases so far: a read that saw a lower number when it
    /// began waiting was waiting at a release, and has its turn.
    releases: u64,
    /// Reads let in by the last release that have not yet taken the lock.
    /// A writer waits for them as it waits for the readers holding it.
    admitted: usize,
    /// Writers are numbered as they start waiting; those numbered up to
    /// `phase_end` are the current phase, and `phase_left` of them have not
    /// had the lock yet. Reads are let in when it reaches zero.
    next_ticket: u64,
    phase_end: u64,
    phase_left: usize,
}

// The value moves between threads only through the guards, which borrow
// the lock; the lock hands out `&T` to many readers or `&mut T` to one
// writer, which is what `Sync` promises of it.
unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            state: Mutex::new(State {
                readers: 0,
                writer: false,
                writers_waiting: 0,
                readers_waiting: 0,
                releases: 0,
                admitted: 0,
                next_ticket: 0,
                phase_end: 0,
                phase_left: 0,
            }),
            changed: Condvar::new(),
            poisoned: AtomicBool::new(false),
            cell: UnsafeCell::new(value),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The shared lock, behind any writer that waits for it: what a
    /// statement starting on this node takes.
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let mut s = self.state();
        let since = s.releases;
        s.readers_waiting += 1;
        loop {
            // Let in by a release it waited through: its turn, ahead of
            // the writers waiting now.
            if !s.writer && s.releases > since && s.admitted > 0 {
                s.admitted -= 1;
                break;
            }
            if !s.writer && s.writers_waiting == 0 {
                // In without a turn; one it was given goes unused, and must
                // not hold a writer back.
                if s.releases > since && s.admitted > 0 {
                    s.admitted -= 1;
                }
                break;
            }
            s = self.changed.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        s.readers_waiting -= 1;
        s.readers += 1;
        drop(s);
        self.reading()
    }

    /// The shared lock, behind a writer that holds it and no other: what
    /// a wire read serving another node's statement takes, so that the
    /// coordinator holding its own lock across the call is never made to
    /// wait for this node's writers, whose wait may end at that
    /// coordinator.
    pub fn read_served(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let mut s = self.state();
        while s.writer {
            s = self.changed.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        s.readers += 1;
        drop(s);
        self.reading()
    }

    /// The shared lock if it is free of writers, holding and waiting.
    pub fn try_read(&self) -> TryLockResult<RwLockReadGuard<'_, T>> {
        let mut s = self.state();
        if s.writer || s.writers_waiting > 0 {
            return Err(TryLockError::WouldBlock);
        }
        s.readers += 1;
        drop(s);
        self.reading().map_err(TryLockError::Poisoned)
    }

    /// The exclusive lock, once every reader and writer before it is done:
    /// in the current phase if it was waiting when the phase began, or as
    /// the first of the next.
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let mut s = self.state();
        s.next_ticket += 1;
        let ticket = s.next_ticket;
        s.writers_waiting += 1;
        loop {
            let free = !s.writer && s.readers == 0 && s.admitted == 0;
            // Behind a phase it is not in: the phase goes first.
            let turn = s.phase_left == 0 || ticket <= s.phase_end;
            if free && turn {
                break;
            }
            s = self.changed.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        s.writers_waiting -= 1;
        if ticket <= s.phase_end {
            s.phase_left -= 1;
        } else {
            // The first after the reads: a phase of itself and every writer
            // waiting now.
            s.phase_end = s.next_ticket;
            s.phase_left = s.writers_waiting;
        }
        s.writer = true;
        drop(s);
        self.writing()
    }

    /// The exclusive lock if nobody holds the lock at all. A writer that
    /// tries and steps back holds no reader behind it, which is what the
    /// periodic work wants.
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        let mut s = self.state();
        if s.writer || s.readers > 0 || s.admitted > 0 {
            return Err(TryLockError::WouldBlock);
        }
        s.writer = true;
        drop(s);
        self.writing().map_err(TryLockError::Poisoned)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    pub fn into_inner(self) -> LockResult<T> {
        let poisoned = self.is_poisoned();
        let value = self.cell.into_inner();
        if poisoned {
            Err(PoisonError::new(value))
        } else {
            Ok(value)
        }
    }

    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        let poisoned = self.is_poisoned();
        let value = self.cell.get_mut();
        if poisoned {
            Err(PoisonError::new(value))
        } else {
            Ok(value)
        }
    }

    fn reading(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let g = RwLockReadGuard { lock: self };
        if self.is_poisoned() {
            Err(PoisonError::new(g))
        } else {
            Ok(g)
        }
    }

    fn writing(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let g = RwLockWriteGuard { lock: self, panicking: std::thread::panicking() };
        if self.is_poisoned() {
            Err(PoisonError::new(g))
        } else {
            Ok(g)
        }
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> RwLock<T> {
        RwLock::new(T::default())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RwLock");
        match self.try_read() {
            Ok(g) => d.field("data", &&*g),
            Err(TryLockError::Poisoned(p)) => d.field("data", &&**p.get_ref()),
            Err(TryLockError::WouldBlock) => d.field("data", &format_args!("<locked>")),
        };
        d.field("poisoned", &self.is_poisoned()).finish_non_exhaustive()
    }
}

/// The shared side, held until dropped.
pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // Readers hold the lock shared: nothing writes while one exists.
        unsafe { &*self.lock.cell.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let mut s = self.lock.state();
        s.readers -= 1;
        // A writer waits for the last reader; the count is what it checks.
        if s.readers == 0 {
            self.lock.changed.notify_all();
        }
    }
}

/// The exclusive side, held until dropped; a panic while holding it
/// poisons the lock, as the standard one does.
pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>,
    panicking: bool,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.cell.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // The one writer holds the lock exclusively: nothing else reads or
        // writes while it exists.
        unsafe { &mut *self.lock.cell.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        if !self.panicking && std::thread::panicking() {
            self.lock.poisoned.store(true, Ordering::Relaxed);
        }
        let mut s = self.lock.state();
        s.writer = false;
        s.releases += 1;
        // The reads waiting go in at the end of the phase; before it, the
        // next of its writers does.
        s.admitted = if s.phase_left == 0 { s.readers_waiting } else { 0 };
        self.lock.changed.notify_all();
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    /// A read waiting when a writer lets go goes in before the next writer,
    /// even one that was waiting first. Before 0.76.0 the writer
    /// always won, and a steady writer held a read back for several writes.
    #[test]
    fn a_read_waiting_at_a_release_goes_before_the_next_writer() {
        // A hundred rounds: a lock that let the writer race the read would
        // win some and lose some, and one round could pass either way.
        for round in 0..100 {
            let lock = Arc::new(RwLock::new(Vec::<&str>::new()));
            let wait_for = |readers: usize, writers: usize| {
                let started = Instant::now();
                loop {
                    let s = lock.state();
                    if s.readers_waiting == readers && s.writers_waiting == writers {
                        return;
                    }
                    drop(s);
                    assert!(started.elapsed() < Duration::from_secs(10), "never queued");
                    thread::sleep(Duration::from_micros(200));
                }
            };
            let first = lock.write().unwrap();
            let writer = {
                let lock = lock.clone();
                thread::spawn(move || lock.write().unwrap().push("second writer"))
            };
            wait_for(0, 1);
            let reader = {
                let lock = lock.clone();
                // What the read saw: empty, if it went before the writer.
                thread::spawn(move || lock.read().unwrap().len())
            };
            wait_for(1, 1);
            drop(first);
            writer.join().unwrap();
            let seen = reader.join().unwrap();
            assert_eq!(
                seen, 0,
                "round {round}: the read waiting at the release went after the writer"
            );
        }
    }

    /// The writers waiting when a phase begins all go before the reads that
    /// queued behind them: the read sees every one of their writes. Before
    /// 0.77.0 the first writer's release let the read in, and under a
    /// mixed load every write waited out a round of reads.
    #[test]
    fn the_writers_waiting_when_a_phase_begins_all_go_before_the_reads_behind_them() {
        for round in 0..50 {
            let lock = Arc::new(RwLock::new(Vec::<usize>::new()));
            let wait_for = |readers: usize, writers: usize| {
                let started = Instant::now();
                loop {
                    let s = lock.state();
                    if s.readers_waiting == readers && s.writers_waiting == writers {
                        return;
                    }
                    drop(s);
                    assert!(started.elapsed() < Duration::from_secs(10), "never queued");
                    thread::sleep(Duration::from_micros(200));
                }
            };
            let held = lock.read().unwrap();
            let writers: Vec<_> = (0..3)
                .map(|i| {
                    let lock = lock.clone();
                    thread::spawn(move || lock.write().unwrap().push(i))
                })
                .collect();
            wait_for(0, 3);
            let reader = {
                let lock = lock.clone();
                thread::spawn(move || lock.read().unwrap().len())
            };
            wait_for(1, 3);
            drop(held);
            for w in writers {
                w.join().unwrap();
            }
            assert_eq!(
                reader.join().unwrap(),
                3,
                "round {round}: the read went in before the phase's writers were done"
            );
        }
    }

    /// A writer that arrives while a phase is going is in the next one: the
    /// read that was waiting goes before it. What bounds a read's wait to
    /// one phase, however many writers keep arriving.
    #[test]
    fn a_writer_arriving_during_a_phase_waits_behind_the_reads() {
        use std::sync::atomic::AtomicBool;
        for round in 0..50 {
            let lock = Arc::new(RwLock::new(Vec::<usize>::new()));
            let gate = Arc::new(AtomicBool::new(false));
            let wait_for = |readers: usize, writers: usize| {
                let started = Instant::now();
                loop {
                    let s = lock.state();
                    if s.readers_waiting == readers && s.writers_waiting == writers {
                        return;
                    }
                    drop(s);
                    assert!(started.elapsed() < Duration::from_secs(10), "never queued");
                    thread::sleep(Duration::from_micros(200));
                }
            };
            let held = lock.read().unwrap();
            // The phase: three writers, the first of them holding the lock
            // until the late writer and the read have queued.
            let phase: Vec<_> = (0..3)
                .map(|i| {
                    let (lock, gate) = (lock.clone(), gate.clone());
                    thread::spawn(move || {
                        let mut g = lock.write().unwrap();
                        g.push(i);
                        while !gate.load(Ordering::Acquire) {
                            thread::sleep(Duration::from_micros(200));
                        }
                    })
                })
                .collect();
            wait_for(0, 3);
            drop(held);
            wait_for(0, 2);
            let late = {
                let lock = lock.clone();
                thread::spawn(move || lock.write().unwrap().push(99))
            };
            wait_for(0, 3);
            let reader = {
                let lock = lock.clone();
                thread::spawn(move || lock.read().unwrap().clone())
            };
            wait_for(1, 3);
            gate.store(true, Ordering::Release);
            for w in phase {
                w.join().unwrap();
            }
            late.join().unwrap();
            let seen = reader.join().unwrap();
            assert_eq!(seen.len(), 3, "round {round}: the read saw {seen:?}");
            assert!(!seen.contains(&99), "round {round}: the late writer went before the read");
        }
    }

    /// Under writers that never let up, a read's wait is bounded by one
    /// phase: eight writers holding the lock three milliseconds each and
    /// asking again the moment they let go, a reader beside them for two
    /// seconds, and no read waits more than a hundred times a phase. A
    /// missed wake-up, or a phase that admits the writers arriving during
    /// it, would show here as a wait of the whole run.
    #[test]
    fn a_reads_wait_under_writers_that_never_let_up_is_one_phase() {
        use std::sync::atomic::AtomicBool;
        let lock = Arc::new(RwLock::new(0u64));
        let stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let (lock, stop) = (lock.clone(), stop.clone());
                thread::spawn(move || {
                    let mut n = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let mut g = lock.write().unwrap();
                        *g += 1;
                        thread::sleep(Duration::from_millis(3));
                        drop(g);
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        let mut worst = Duration::ZERO;
        let mut reads = 0u32;
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(2) {
            let t = Instant::now();
            let g = lock.read().unwrap();
            worst = worst.max(t.elapsed());
            drop(g);
            reads += 1;
            thread::sleep(Duration::from_millis(1));
        }
        stop.store(true, Ordering::Relaxed);
        let writes: u64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
        assert!(writes > 100, "the writers barely ran: {writes}");
        assert!(reads > 5, "the reads barely ran: {reads}");
        // A phase is eight holds of three milliseconds; a hundred of those
        // is far past any scheduling noise and far short of the run.
        assert!(worst < Duration::from_millis(2400), "a read waited {worst:?}: more than a phase");
    }

    /// A waiting writer holds a plain read behind it and a served read
    /// not at all; it gets the lock the moment the readers before it are
    /// gone.
    #[test]
    fn a_waiting_writer_holds_back_a_read_but_not_a_served_read() {
        let lock = Arc::new(RwLock::new(0u32));
        let first = lock.read().unwrap();
        let writer = {
            let lock = lock.clone();
            thread::spawn(move || {
                let mut g = lock.write().unwrap();
                *g += 1;
            })
        };
        // The writer is waiting once the state says so.
        let t0 = Instant::now();
        while lock.state().writers_waiting == 0 {
            assert!(t0.elapsed() < Duration::from_secs(5), "the writer never queued");
            thread::yield_now();
        }
        assert!(matches!(lock.try_read(), Err(TryLockError::WouldBlock)));
        let served = lock.read_served().unwrap();
        assert_eq!(*served, 0);
        let plain = {
            let lock = lock.clone();
            thread::spawn(move || {
                let t0 = Instant::now();
                let g = lock.read().unwrap();
                (*g, t0.elapsed())
            })
        };
        thread::sleep(Duration::from_millis(50));
        assert!(!plain.is_finished(), "a plain read got in past a waiting writer");
        drop(served);
        drop(first);
        writer.join().unwrap();
        let (seen, waited) = plain.join().unwrap();
        assert_eq!(seen, 1, "the plain read ran before the writer it was held behind");
        assert!(waited >= Duration::from_millis(40), "the plain read waited {waited:?}");
    }

    /// The exclusive side is exclusive, the try forms step back rather
    /// than wait, and a panic under the write guard poisons the lock the
    /// way the standard one does, recoverable through `into_inner`.
    #[test]
    fn writes_are_exclusive_tries_step_back_and_a_panic_poisons() {
        let lock = Arc::new(RwLock::new(Vec::<u32>::new()));
        let held = lock.write().unwrap();
        assert!(matches!(lock.try_read(), Err(TryLockError::WouldBlock)));
        assert!(matches!(lock.try_write(), Err(TryLockError::WouldBlock)));
        drop(held);
        let r = lock.read().unwrap();
        assert!(matches!(lock.try_write(), Err(TryLockError::WouldBlock)));
        assert!(lock.try_read().is_ok());
        assert!(lock.read_served().is_ok());
        drop(r);
        let workers: Vec<_> = (0..8)
            .map(|i| {
                let lock = lock.clone();
                thread::spawn(move || {
                    for _ in 0..200 {
                        lock.write().unwrap().push(i);
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        assert_eq!(lock.read().unwrap().len(), 1600);
        let poisoner = {
            let lock = lock.clone();
            thread::spawn(move || {
                let _g = lock.write().unwrap();
                panic!("under the write guard");
            })
        };
        assert!(poisoner.join().is_err());
        assert!(lock.is_poisoned());
        assert!(lock.read().is_err());
        let g = lock.write().unwrap_or_else(|p| p.into_inner());
        assert_eq!(g.len(), 1600);
        drop(g);
        let v = Arc::try_unwrap(lock).unwrap().into_inner().unwrap_or_else(|p| p.into_inner());
        assert_eq!(v.len(), 1600);
    }
}
