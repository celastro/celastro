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

use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
mod sys {
    pub const SIGINT: i32 = 2;
    pub const SIGTERM: i32 = 15;
    /// `SIG_ERR`: `(sighandler_t)-1`.
    pub const SIG_ERR: usize = usize::MAX;
    extern "C" {
        pub fn signal(sig: i32, handler: extern "C" fn(i32)) -> usize;
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

/// Whether a SIGTERM or SIGINT has arrived since the handlers were installed.
pub fn shutdown_requested() -> bool {
    STOP.load(Ordering::SeqCst)
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
}
