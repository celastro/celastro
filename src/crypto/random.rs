//! Randomness: the kernel's, and nothing cleverer. The console's per-run
//! token draws from the same source, for the same reason.

use crate::error::{Error, Result};

/// `n` bytes from the kernel: `getrandom(2)` where there is one (Linux;
/// blocking until the pool is seeded, so a key is never drawn from an
/// unseeded boot), and `/dev/urandom` elsewhere or on a kernel without
/// the call. An error names the source; a key from a weaker source is
/// worse than no key.
pub fn bytes(n: usize) -> Result<Vec<u8>> {
    let mut out = vec![0u8; n];
    fill(&mut out)?;
    Ok(out)
}

/// `buf` filled from the kernel, in place: what a key is drawn through,
/// so no freed copy of it is left on the way.
pub fn fill(buf: &mut [u8]) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        if fill_getrandom(buf)? {
            return Ok(());
        }
    }
    fill_urandom(buf)
}

/// `buf` filled by `getrandom(2)`; `Ok(false)` on a kernel without it
/// (ENOSYS), when the caller falls back to the device.
#[cfg(target_os = "linux")]
fn fill_getrandom(buf: &mut [u8]) -> Result<bool> {
    extern "C" {
        fn getrandom(buf: *mut u8, len: usize, flags: u32) -> isize;
    }
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = unsafe { getrandom(buf[filled..].as_mut_ptr(), buf.len() - filled, 0) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            match e.raw_os_error() {
                Some(4) => continue,          // EINTR: asked again
                Some(38) => return Ok(false), // ENOSYS: no such call here
                _ => {
                    return Err(Error::Io(std::io::Error::new(
                        e.kind(),
                        format!("getrandom failed: {e}"),
                    )))
                }
            }
        }
        filled += n as usize;
    }
    Ok(true)
}

fn fill_urandom(buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom").map_err(|e| {
        Error::Io(std::io::Error::new(e.kind(), format!("cannot open /dev/urandom: {e}")))
    })?;
    f.read_exact(buf).map_err(|e| {
        Error::Io(std::io::Error::new(e.kind(), format!("cannot read /dev/urandom: {e}")))
    })
}

pub fn array32() -> Result<[u8; 32]> {
    let mut a = [0u8; 32];
    fill(&mut a)?;
    Ok(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two draws differ, every length is filled, and the device path gives
    /// the same shape of answer as the syscall.
    #[test]
    fn draws_differ_and_every_length_is_filled() {
        let a = bytes(32).unwrap();
        let b = bytes(32).unwrap();
        assert_ne!(a, b);
        assert_eq!(bytes(0).unwrap().len(), 0);
        assert_eq!(bytes(1000).unwrap().len(), 1000);
        let mut d = vec![0u8; 64];
        fill_urandom(&mut d).unwrap();
        assert!(d.iter().any(|&x| x != 0));
    }
}
