//! Shutdown on SIGTERM and SIGINT, so that `serve` is a well-behaved PID 1.
//!
//! In a container the process is PID 1, and the kernel does not deliver a
//! signal with a default disposition to PID 1 at all: `docker stop` sent
//! SIGTERM, waited out its ten-second grace period, and sent SIGKILL -- exit
//! 137, every time, unless the container was started with `--init` so that
//! something else held PID 1. A handled signal is delivered, and a handled
//! SIGTERM lets the accept loop return so the caller can save and exit 0.
//!
//! **The route, recorded here because it was a decision.** `std` exposes no
//! signal API, and the crate's rule is no dependencies outside `std`, which
//! rules out `signal-hook` and `ctrlc`. So this is an in-tree binding: one
//! `extern "C"` declaration of `signal(2)`, which every libc the crate is
//! linked against provides, and one handler. `signal` rather than
//! `sigaction`, because `sigaction` takes a struct whose layout is the
//! platform-specific part and `signal` takes two words; the BSD-versus-SysV
//! difference in what `signal` does to a blocked syscall does not matter to a
//! handler that sets a flag and returns, and the accept loop that reads the
//! flag polls rather than blocks for exactly that reason. The handler is
//! async-signal-safe: an atomic store and nothing else.
//!
//! Installed by `serve` only. At a REPL a Ctrl-C with a handler installed
//! restarts the blocked read (glibc's `signal` sets `SA_RESTART`) and the
//! interrupt is swallowed, where the default disposition ends the process
//! the way a person at a terminal expects. The one-shot verbs are jobs, and a
//! job that is killed is a job that stops.

use std::cell::RefCell;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

static STOP: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
mod sys {
    pub const SIGINT: i32 = 2;
    pub const SIGTERM: i32 = 15;
    /// `SIG_ERR`: `(sighandler_t)-1`.
    pub const SIG_ERR: usize = usize::MAX;
    pub const POLLIN: i16 = 1;
    /// `struct pollfd`, as every unix lays it out.
    #[repr(C)]
    pub struct PollFd {
        pub fd: i32,
        pub events: i16,
        pub revents: i16,
    }
    extern "C" {
        pub fn signal(sig: i32, handler: extern "C" fn(i32)) -> usize;
        pub fn poll(fds: *mut PollFd, nfds: std::os::raw::c_ulong, timeout: i32) -> i32;
    }
}

/// Wait until `listener` has a connection to accept, or `timeout` passes, or
/// a signal interrupts the wait; the answer is whether there is one. What an
/// accept loop waits on instead of a clock: the wait ends the moment a
/// connection arrives, so a request never pays for a sleep, and `poll(2)` is
/// the one wait a handler always interrupts -- `signal(2)` installs with
/// `SA_RESTART`, which restarts a blocking `accept` and never a `poll` -- so
/// the loop re-reads [`shutdown_requested`] within `timeout` at the latest.
/// Off unix there is no `poll` to bind and the wait is the sleep it always
/// was, reported as ready so the caller tries the accept.
pub fn wait_readable(listener: &TcpListener, timeout: Duration) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let mut fd = sys::PollFd { fd: listener.as_raw_fd(), events: sys::POLLIN, revents: 0 };
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: one `pollfd` on the stack, its count given as one, and the
        // descriptor is the listener's for as long as the borrow lasts.
        unsafe { sys::poll(&mut fd, 1, ms) > 0 }
    }
    #[cfg(not(unix))]
    {
        std::thread::sleep(timeout.min(Duration::from_millis(25)));
        true
    }
}

#[cfg(unix)]
extern "C" fn stop(_sig: i32) {
    STOP.store(true, Ordering::SeqCst);
}

/// Install the handlers for SIGTERM and SIGINT. Returns whether they are in
/// place: `false` off unix, where nothing is installed and the process keeps
/// whatever disposition it had.
pub fn install_shutdown_handlers() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: `signal` takes a signal number and a handler with the C
        // ABI, and the handler touches nothing but an atomic. Both numbers
        // are the POSIX values on every unix the crate builds for.
        let a = unsafe { sys::signal(sys::SIGINT, stop) };
        let b = unsafe { sys::signal(sys::SIGTERM, stop) };
        a != sys::SIG_ERR && b != sys::SIG_ERR
    }
    #[cfg(not(unix))]
    {
        false
    }
}

thread_local! {
    /// A stop this thread's builds watch beside the process's: the console's
    /// maintenance threads arm it with the server's own flag.
    static STOP_HERE: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

/// Watch `flag` as well as the process's signals on this thread, so a build
/// in flight here gives up when the console is asked to stop
/// (`/api/shutdown`) as it does on SIGTERM.
pub fn stop_this_thread_with(flag: Arc<AtomicBool>) {
    STOP_HERE.with(|s| *s.borrow_mut() = Some(flag));
}

/// Whether a long build on this thread should give up: the process was
/// signalled, or the flag this thread watches is set.
pub fn stopping() -> bool {
    shutdown_requested()
        || STOP_HERE.with(|s| s.borrow().as_ref().is_some_and(|f| f.load(Ordering::Acquire)))
}

/// The error a build gives up with: an `Interrupted` io error, which the
/// maintenance step tells from a failure by [`interrupted`].
pub fn stop_error() -> crate::error::Error {
    crate::error::Error::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "the node is stopping",
    ))
}

/// `Err` when a build should stop, as the builds ask between their pieces.
pub fn check_stop() -> crate::error::Result<()> {
    if stopping() {
        Err(stop_error())
    } else {
        Ok(())
    }
}

/// Whether `e` is a build giving up for the stop, not a failure.
pub fn interrupted(e: &crate::error::Error) -> bool {
    matches!(e, crate::error::Error::Io(io) if io.kind() == std::io::ErrorKind::Interrupted)
}

/// Whether a SIGTERM or SIGINT has arrived since the handlers were installed.
pub fn shutdown_requested() -> bool {
    STOP.load(Ordering::SeqCst)
}

/// No core dump of this process: it holds a data key and TLS private keys
/// in memory, and a dump is those keys on a disk the operator did not
/// choose. Linux only (`prctl(PR_SET_DUMPABLE, 0)`, which also keeps
/// another user's debugger out); elsewhere nothing changes. Called at the
/// tool's start, before any key is read.
pub fn refuse_core_dumps() {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn prctl(option: i32, arg2: u64, arg3: u64, arg4: u64, arg5: u64) -> i32;
        }
        const PR_SET_DUMPABLE: i32 = 4;
        // The one failure is EINVAL for an option this kernel lacks, which
        // every kernel since 2.3 has; nothing to do about a failure anyway.
        unsafe {
            prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The handlers install, and installing twice is fine: `serve` is the
    /// one caller, but a test binary running several `serve`s is not.
    /// Whether a delivered signal reaches the flag is proved by the
    /// integration test that sends the real binary a real SIGTERM; raising
    /// one here would set a process-global flag under every other test in
    /// this binary.
    #[test]
    fn the_handlers_install_and_nothing_is_requested_until_a_signal_arrives() {
        assert!(!shutdown_requested());
        assert_eq!(install_shutdown_handlers(), cfg!(unix));
        assert_eq!(install_shutdown_handlers(), cfg!(unix));
        assert!(!shutdown_requested());
    }

    /// The wait ends when a connection arrives, not when a clock does: a
    /// connection made after the wait began is seen well inside a timeout
    /// that would otherwise be paid in full, and a listener nobody dials
    /// reports nothing when the timeout passes.
    #[test]
    fn the_wait_ends_when_a_connection_arrives_and_reports_nothing_when_none_does() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let t0 = std::time::Instant::now();
        assert!(!wait_readable(&listener, Duration::from_millis(50)));
        assert!(t0.elapsed() >= Duration::from_millis(45), "{:?}", t0.elapsed());
        let addr = listener.local_addr().unwrap();
        let dial = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            std::net::TcpStream::connect(addr).unwrap()
        });
        let t0 = std::time::Instant::now();
        assert!(wait_readable(&listener, Duration::from_secs(5)));
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        let _keep = dial.join().unwrap();
        listener.accept().unwrap();
    }
}
