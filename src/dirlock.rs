//! One process per data directory.
//!
//! `Db::open` takes `<dir>/LOCK` with `flock(2)` and holds it for the life
//! of the `Db`; a second open -- another process, or a second `Db` in this
//! one -- is refused naming the directory and the holder's pid. Two writers
//! publishing manifests and catalogs over each other is the one thing the
//! durability story cannot survive, and until 0.33.0 nothing stopped them.
//! The kernel drops the lock when the process ends, however it ends, so a
//! crash leaves nothing stale; the file stays, empty of meaning, and is
//! never removed -- removing it while another process holds or waits on it
//! is the classic way to hand two processes the same lock. Off unix, where
//! there is no `flock`, the file is created exclusively with the pid inside
//! and removed at close, and one left by a crash has to be removed by hand;
//! the message says so.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// The held lock: dropping it releases the directory.
pub(crate) struct DirLock {
    _file: fs::File,
    path: PathBuf,
}

#[cfg(unix)]
mod sys {
    pub const LOCK_EX: i32 = 2;
    pub const LOCK_NB: i32 = 4;
    extern "C" {
        pub fn flock(fd: i32, operation: i32) -> i32;
    }
}

pub(crate) fn take(dir: &Path) -> Result<DirLock> {
    let path = dir.join("LOCK");
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        // SAFETY: a descriptor this process owns and an operation flag; the
        // call touches nothing else.
        let r = unsafe { sys::flock(file.as_raw_fd(), sys::LOCK_EX | sys::LOCK_NB) };
        if r != 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::WouldBlock {
                let holder = fs::read_to_string(&path).unwrap_or_default();
                let holder = holder.trim();
                let who =
                    if holder.is_empty() { String::new() } else { format!(" (pid {holder})") };
                return Err(Error::Storage(format!(
                    "{} is open in another process{who}; one process per data directory",
                    dir.display()
                )));
            }
            return Err(Error::Io(e));
        }
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        Ok(DirLock { _file: file, path })
    }
    #[cfg(not(unix))]
    {
        let mut file = match fs::OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = fs::read_to_string(&path).unwrap_or_default();
                return Err(Error::Storage(format!(
                    "{} is open in another process (pid {}); one process per data directory -- \
                     if none is running, remove {}",
                    dir.display(),
                    holder.trim(),
                    path.display()
                )));
            }
            Err(e) => return Err(e.into()),
        };
        writeln!(file, "{}", std::process::id())?;
        Ok(DirLock { _file: file, path })
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        // On unix the kernel releases the lock with the descriptor and the
        // file stays; elsewhere the file is the lock.
        #[cfg(not(unix))]
        {
            let _ = fs::remove_file(&self.path);
        }
        let _ = &self.path;
    }
}
