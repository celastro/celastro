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
}

// The value moves between threads only through the guards, which borrow
// the lock; the lock hands out `&T` to many readers or `&mut T` to one
// writer, which is what `Sync` promises of it.
unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> RwLock<T> {
        RwLock {
            state: Mutex::new(State { readers: 0, writer: false, writers_waiting: 0 }),
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
        while s.writer || s.writers_waiting > 0 {
            s = self.changed.wait(s).unwrap_or_else(|p| p.into_inner());
        }
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

    /// The exclusive lock, once every reader and writer before it is done.
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let mut s = self.state();
        s.writers_waiting += 1;
        while s.writer || s.readers > 0 {
            s = self.changed.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        s.writers_waiting -= 1;
        s.writer = true;
        drop(s);
        self.writing()
    }

    /// The exclusive lock if nobody holds the lock at all. A writer that
    /// tries and steps back holds no reader behind it, which is what the
    /// periodic work wants.
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        let mut s = self.state();
        if s.writer || s.readers > 0 {
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
