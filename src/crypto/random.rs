//! Randomness: the kernel's, and nothing cleverer. The console's per-run
//! token draws from the same file, for the same reason.

use std::io::Read;

use crate::error::{Error, Result};

/// `n` bytes from `/dev/urandom`, or an error naming it: a key from a
/// weaker source is worse than no key.
pub fn bytes(n: usize) -> Result<Vec<u8>> {
    let mut f = std::fs::File::open("/dev/urandom").map_err(|e| {
        Error::Io(std::io::Error::new(e.kind(), format!("cannot open /dev/urandom: {e}")))
    })?;
    let mut out = vec![0u8; n];
    f.read_exact(&mut out).map_err(|e| {
        Error::Io(std::io::Error::new(e.kind(), format!("cannot read /dev/urandom: {e}")))
    })?;
    Ok(out)
}

pub fn array32() -> Result<[u8; 32]> {
    let v = bytes(32)?;
    let mut a = [0u8; 32];
    a.copy_from_slice(&v);
    Ok(a)
}
