//! Encryption at rest: every file the database writes, under a data key
//! the user's master key wraps.
//!
//! **Keys.** One *data key* per database, 32 random bytes made at the first
//! open with a master key present and kept in `<dir>/KEY` wrapped under
//! the master key (ChaCha20-Poly1305, the key file's own identity as the
//! AAD). The master key comes from the environment and is never written;
//! the data key never leaves memory. A file is encrypted under a *file
//! key* derived from the data key and the file's identity (its path
//! relative to the collection, or its bare name at the root), so two files
//! never share a key and a file cannot be moved to stand in for another.
//!
//! **Frames.** A file is a sequence of frames of at most [`CHUNK`] bytes
//! of plaintext: `nonce[12] | ciphertext | tag[16]`, the frame's index in
//! its AAD so frames cannot be reordered within a file. Frames before the
//! last are exactly [`CHUNK`] long, so a ranged read of the plaintext --
//! a segment's footer, a component faulted in from an archive -- is a
//! ranged read of the frames that cover it, which is what keeps an
//! archived segment's reads ranged. The nonce is random: with a key per
//! file and frames of 64 KiB, the count under one key stays far below
//! the bound RFC 8439 gives random nonces.
//!
//! **What is and is not protected.** The bytes on the volume, in the
//! archive's bucket, in a backup. A running process holds the data key;
//! whoever can read its memory can read the data. The wire is the TLS's
//! business; the console's token the Secret's.

use std::sync::Arc;

use crate::crypto::chacha20poly1305::{open, seal};
use crate::crypto::hkdf;
use crate::error::{Error, Result};

/// Plaintext bytes per frame.
pub const CHUNK: usize = 64 * 1024;
const NONCE: usize = 12;
const TAG: usize = 16;
/// A frame's bytes for `n` plaintext bytes.
const fn framed(n: usize) -> usize {
    NONCE + n + TAG
}
const FRAME: usize = framed(CHUNK);

/// The data key, ready to derive file keys.
pub struct Cipher {
    data_key: [u8; 32],
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cipher(..)")
    }
}

/// The wrapped data key as `KEY` holds it: `"CELK1"` | nonce | ciphertext | tag.
const KEY_MAGIC: &[u8; 5] = b"CELK1";
const KEY_AAD: &[u8] = b"celastro data key v1";

impl Cipher {
    /// A fresh data key, from the kernel's randomness.
    pub fn generate() -> Result<Cipher> {
        Ok(Cipher { data_key: crate::crypto::random::array32()? })
    }

    /// The data key wrapped under `master`, as the bytes of `KEY`.
    pub fn wrap(&self, master: &[u8; 32]) -> Result<Vec<u8>> {
        let nonce_bytes = crate::crypto::random::bytes(NONCE)?;
        let mut nonce = [0u8; NONCE];
        nonce.copy_from_slice(&nonce_bytes);
        let mut data = self.data_key.to_vec();
        let tag = seal(master, &nonce, KEY_AAD, &mut data);
        let mut out = Vec::with_capacity(KEY_MAGIC.len() + NONCE + 32 + TAG);
        out.extend_from_slice(KEY_MAGIC);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&data);
        out.extend_from_slice(&tag);
        Ok(out)
    }

    /// The data key `KEY` holds, unwrapped under `master`; refused with the
    /// reason when the master key is not the one it was wrapped under.
    pub fn unwrap(key_file: &[u8], master: &[u8; 32]) -> Result<Cipher> {
        if key_file.len() != KEY_MAGIC.len() + NONCE + 32 + TAG || &key_file[..5] != KEY_MAGIC {
            return Err(Error::Storage("KEY is not a wrapped data key".into()));
        }
        let mut nonce = [0u8; NONCE];
        nonce.copy_from_slice(&key_file[5..5 + NONCE]);
        let mut data = key_file[5 + NONCE..5 + NONCE + 32].to_vec();
        let mut tag = [0u8; TAG];
        tag.copy_from_slice(&key_file[5 + NONCE + 32..]);
        if !open(master, &nonce, KEY_AAD, &mut data, &tag) {
            return Err(Error::Storage(
                "the master key does not open this database's KEY; the database was encrypted \
                 under another"
                    .into(),
            ));
        }
        let mut data_key = [0u8; 32];
        data_key.copy_from_slice(&data);
        Ok(Cipher { data_key })
    }

    /// The key of one file, from its identity.
    fn file_key(&self, id: &str) -> [u8; 32] {
        let prk = hkdf::extract(b"celastro file key v1", &self.data_key);
        let okm = hkdf::expand(&prk, id.as_bytes(), 32);
        let mut k = [0u8; 32];
        k.copy_from_slice(&okm);
        k
    }

    /// The whole of `plain` as frames.
    pub fn seal_file(&self, id: &str, plain: &[u8]) -> Result<Vec<u8>> {
        let key = self.file_key(id);
        let frames = plain.len().div_ceil(CHUNK).max(1);
        let mut out = Vec::with_capacity(frames * NONCE + plain.len() + frames * TAG);
        if plain.is_empty() {
            // An empty file is one empty frame, so it still authenticates.
            self.push_frame(&key, id, 0, &[], &mut out)?;
            return Ok(out);
        }
        for (index, c) in plain.chunks(CHUNK).enumerate() {
            self.push_frame(&key, id, index as u64, c, &mut out)?;
        }
        Ok(out)
    }

    fn push_frame(
        &self,
        key: &[u8; 32],
        id: &str,
        index: u64,
        c: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let nonce_bytes = crate::crypto::random::bytes(NONCE)?;
        let mut nonce = [0u8; NONCE];
        nonce.copy_from_slice(&nonce_bytes);
        let mut data = c.to_vec();
        let tag = seal(key, &nonce, &aad(id, index), &mut data);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&data);
        out.extend_from_slice(&tag);
        Ok(())
    }

    /// The whole of a framed file.
    pub fn open_file(&self, id: &str, framed_bytes: &[u8]) -> Result<Vec<u8>> {
        let key = self.file_key(id);
        let mut out = Vec::with_capacity(framed_bytes.len());
        let mut index = 0u64;
        let mut rest = framed_bytes;
        loop {
            let take = rest.len().min(FRAME);
            if take < NONCE + TAG {
                return Err(Error::Storage(format!("{id}: an encrypted frame is torn")));
            }
            let (frame, after) = rest.split_at(take);
            out.extend_from_slice(&self.open_frame(&key, id, index, frame)?);
            rest = after;
            index += 1;
            if rest.is_empty() {
                break;
            }
        }
        Ok(out)
    }

    fn open_frame(&self, key: &[u8; 32], id: &str, index: u64, frame: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE];
        nonce.copy_from_slice(&frame[..NONCE]);
        let mut data = frame[NONCE..frame.len() - TAG].to_vec();
        let mut tag = [0u8; TAG];
        tag.copy_from_slice(&frame[frame.len() - TAG..]);
        if !open(key, &nonce, &aad(id, index), &mut data, &tag) {
            return Err(Error::Storage(format!(
                "{id}: frame {index} does not authenticate; the file is damaged or under another key"
            )));
        }
        Ok(data)
    }

    /// The plaintext length of a framed file of `framed_len` bytes.
    pub fn plain_len(framed_len: u64) -> Result<u64> {
        if framed_len < (NONCE + TAG) as u64 {
            return Err(Error::Storage("an encrypted file is shorter than one frame".into()));
        }
        let full = framed_len / FRAME as u64;
        let rest = framed_len % FRAME as u64;
        let last = if rest == 0 { 0 } else { rest - (NONCE + TAG) as u64 };
        if rest != 0 && rest < (NONCE + TAG) as u64 {
            return Err(Error::Storage("an encrypted file ends in a torn frame".into()));
        }
        Ok(full * CHUNK as u64 + last)
    }

    /// `len` plaintext bytes from `off`, through `read(framed_off, framed_len)`
    /// on the framed file: the frames covering the range, opened, and the
    /// range cut out. What a segment's ranged reads become.
    pub fn read_range(
        &self,
        id: &str,
        read: &dyn Fn(u64, u64) -> Result<Vec<u8>>,
        off: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let key = self.file_key(id);
        let first = off / CHUNK as u64;
        let last = (off + len - 1) / CHUNK as u64;
        let framed_off = first * FRAME as u64;
        let framed_len = (last - first + 1) * FRAME as u64;
        // The last frame of the file may be short; ask for what covers the
        // range and let the reader cut at the file's end.
        let bytes = read(framed_off, framed_len)?;
        let mut plain = Vec::with_capacity(bytes.len());
        let mut rest = bytes.as_slice();
        let mut index = first;
        while !rest.is_empty() {
            let take = rest.len().min(FRAME);
            if take < NONCE + TAG {
                return Err(Error::Storage(format!("{id}: an encrypted frame is torn")));
            }
            let (frame, after) = rest.split_at(take);
            plain.extend_from_slice(&self.open_frame(&key, id, index, frame)?);
            rest = after;
            index += 1;
        }
        let start = (off - first * CHUNK as u64) as usize;
        let end = start + len as usize;
        plain
            .get(start..end)
            .map(|s| s.to_vec())
            .ok_or_else(|| Error::Storage(format!("{id}: a ranged read ran past the file")))
    }

    /// The framed range that covers plaintext `[off, off + len)`, for a
    /// caller that reads the framed bytes itself: `(framed_off, framed_len)`.
    pub fn framed_span(off: u64, len: u64) -> (u64, u64) {
        if len == 0 {
            return (0, 0);
        }
        let first = off / CHUNK as u64;
        let last = (off + len - 1) / CHUNK as u64;
        (first * FRAME as u64, (last - first + 1) * FRAME as u64)
    }
}

impl Cipher {
    /// One record of an append-only log as a length-prefixed frame:
    /// `u32 len | nonce | ciphertext | tag`, `index` the record's ordinal in
    /// the log's AAD. What the WAL appends.
    pub fn seal_record(&self, id: &str, index: u64, plain: &[u8]) -> Result<Vec<u8>> {
        let key = self.file_key(id);
        let mut frame = Vec::with_capacity(4 + framed(plain.len()));
        frame.extend_from_slice(&(framed(plain.len()) as u32).to_le_bytes());
        self.push_frame(&key, id, index, plain, &mut frame)?;
        Ok(frame)
    }

    /// The records of a log of length-prefixed frames, in order, stopping
    /// at the first frame that is torn or does not authenticate -- the
    /// rule a WAL's CRC applies to a torn tail.
    pub fn open_records(&self, id: &str, log: &[u8]) -> Vec<Vec<u8>> {
        let key = self.file_key(id);
        let mut out = Vec::new();
        let mut i = 0usize;
        let mut index = 0u64;
        while i + 4 <= log.len() {
            let len = u32::from_le_bytes([log[i], log[i + 1], log[i + 2], log[i + 3]]) as usize;
            let Some(frame) = log.get(i + 4..i + 4 + len) else { break };
            if len < NONCE + TAG {
                break;
            }
            match self.open_frame(&key, id, index, frame) {
                Ok(p) => out.push(p),
                Err(_) => break,
            }
            i += 4 + len;
            index += 1;
        }
        out
    }
}

fn aad(id: &str, index: u64) -> Vec<u8> {
    let mut a = Vec::with_capacity(id.len() + 8);
    a.extend_from_slice(id.as_bytes());
    a.extend_from_slice(&index.to_be_bytes());
    a
}

/// A shared cipher, or none: what every writer and reader of the database's
/// files carries.
pub type Shared = Option<Arc<Cipher>>;

/// Overwrite `bytes` with zeros in a way the optimiser does not remove: a
/// volatile write per byte and a fence after. What every secret does to
/// itself when it is dropped, so a key does not outlive its use in freed
/// memory a later allocation, a core dump or a swap file could show.
pub fn wipe(bytes: &mut [u8]) {
    for b in bytes.iter_mut() {
        // SAFETY: `b` is a valid, exclusive reference into `bytes`.
        unsafe { std::ptr::write_volatile(b, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

/// Wipe a string's bytes before it is dropped.
pub fn wipe_string(s: &mut String) {
    // SAFETY: zeros are valid UTF-8, and the string is cleared right after.
    wipe(unsafe { s.as_bytes_mut() });
    s.clear();
}

/// `N` secret bytes that wipe themselves when dropped and print as
/// nothing: the master key as the options hold it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret<const N: usize>([u8; N]);

impl<const N: usize> From<[u8; N]> for Secret<N> {
    fn from(b: [u8; N]) -> Self {
        Secret(b)
    }
}

impl<const N: usize> std::ops::Deref for Secret<N> {
    type Target = [u8; N];
    fn deref(&self) -> &[u8; N] {
        &self.0
    }
}

impl<const N: usize> std::fmt::Debug for Secret<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret<{N}>(..)")
    }
}

impl<const N: usize> Drop for Secret<N> {
    fn drop(&mut self) {
        wipe(&mut self.0);
    }
}

impl Drop for Cipher {
    fn drop(&mut self) {
        wipe(&mut self.data_key);
    }
}

/// A new master key, as `key master` writes it: 64 hex digits.
pub fn new_master_hex() -> Result<String> {
    Ok(crate::crypto::hex(&crate::crypto::random::array32()?))
}

/// Rewrap the data key in the file `key` -- wrapped under `old` -- under
/// `new`, in place and atomically. The data key does not change, so
/// nothing under it is touched: a master key rotates in one small write.
pub fn rekey_file(key: &std::path::Path, old: &[u8; 32], new: &[u8; 32]) -> Result<()> {
    let wrapped =
        std::fs::read(key).map_err(|e| Error::Storage(format!("{}: {e}", key.display())))?;
    let rewrapped = Cipher::unwrap(&wrapped, old)?.wrap(new)?;
    crate::shard::atomic_write(key, &rewrapped)
}

/// A master key as a file or a variable holds it: 32 raw bytes, or 64 hex
/// digits with whitespace around them ignored.
pub fn parse_master(bytes: &[u8]) -> Result<[u8; 32]> {
    if bytes.len() == 32 {
        return Ok(bytes.try_into().expect("32 bytes"));
    }
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
        let mut out = [0u8; 32];
        for (i, o) in out.iter_mut().enumerate() {
            *o = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex digits");
        }
        return Ok(out);
    }
    Err(Error::Storage(format!(
        "a master key is 32 bytes, or 64 hex digits; this is {} bytes",
        bytes.len()
    )))
}

/// The master key the environment names: `CELASTRO_MASTER_KEY_FILE`, a
/// file [`parse_master`] reads, or `CELASTRO_MASTER_KEY`, hex in the
/// variable itself. Both set is refused, so a stale one cannot shadow the
/// other. `None` when neither is set: the database is then plain, or
/// refused if it is not.
pub fn master_from_env(var: &dyn Fn(&str) -> Option<String>) -> Result<Option<[u8; 32]>> {
    match (var("CELASTRO_MASTER_KEY_FILE"), var("CELASTRO_MASTER_KEY")) {
        (Some(_), Some(_)) => Err(Error::Storage(
            "CELASTRO_MASTER_KEY_FILE and CELASTRO_MASTER_KEY are both set; set one".into(),
        )),
        (Some(path), None) => {
            let bytes = std::fs::read(&path)
                .map_err(|e| Error::Storage(format!("CELASTRO_MASTER_KEY_FILE {path}: {e}")))?;
            parse_master(&bytes).map(Some)
        }
        (None, Some(hex)) => parse_master(hex.as_bytes()).map(Some),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_secret_wipes_itself_and_prints_nothing() {
        let mut b = [7u8; 40];
        wipe(&mut b);
        assert_eq!(b, [0u8; 40]);
        let mut st = String::from("hunter2");
        wipe_string(&mut st);
        assert!(st.is_empty());
        let s: Secret<32> = [9u8; 32].into();
        assert_eq!(format!("{s:?}"), "Secret<32>(..)");
        assert_eq!(*s, [9u8; 32]);
    }

    use super::*;

    fn master(b: u8) -> [u8; 32] {
        [b; 32]
    }

    /// Wrap and unwrap round-trip; the wrong master key is refused with the
    /// reason; a damaged KEY is refused.
    #[test]
    fn the_data_key_is_wrapped_under_the_master_key_and_nothing_else_opens_it() {
        let c = Cipher::generate().unwrap();
        let wrapped = c.wrap(&master(1)).unwrap();
        let again = Cipher::unwrap(&wrapped, &master(1)).unwrap();
        assert_eq!(again.data_key, c.data_key);
        let e = Cipher::unwrap(&wrapped, &master(2)).unwrap_err().to_string();
        assert!(e.contains("master key does not open"), "{e}");
        let mut bad = wrapped.clone();
        bad[20] ^= 1;
        assert!(Cipher::unwrap(&bad, &master(1)).is_err());
        assert!(Cipher::unwrap(b"nonsense", &master(1)).is_err());
        assert_ne!(wrapped, c.wrap(&master(1)).unwrap(), "a fresh nonce each time");
    }

    /// Files of every size around the frame boundary round-trip whole and
    /// by range; the identity and the frame index are bound.
    #[test]
    fn files_round_trip_whole_and_by_range_and_frames_cannot_move() {
        let c = Cipher { data_key: [7; 32] };
        for n in [0usize, 1, 100, CHUNK - 1, CHUNK, CHUNK + 1, 2 * CHUNK + 17, 3 * CHUNK] {
            let plain: Vec<u8> = (0..n).map(|i| (i * 31 % 251) as u8).collect();
            let framed = c.seal_file("shard-0000/segments/1.seg", &plain).unwrap();
            assert_eq!(Cipher::plain_len(framed.len() as u64).unwrap(), n as u64, "n={n}");
            assert_eq!(c.open_file("shard-0000/segments/1.seg", &framed).unwrap(), plain, "n={n}");
            let read = |off: u64, len: u64| -> Result<Vec<u8>> {
                let end = (off + len).min(framed.len() as u64) as usize;
                Ok(framed[off as usize..end].to_vec())
            };
            for (off, len) in
                [(0u64, 1u64), (0, n as u64), (n as u64 / 2, n as u64 / 3), (CHUNK as u64 - 5, 10)]
            {
                if off + len > n as u64 {
                    continue;
                }
                let got = c.read_range("shard-0000/segments/1.seg", &read, off, len).unwrap();
                assert_eq!(
                    got,
                    &plain[off as usize..(off + len) as usize],
                    "n={n} off={off} len={len}"
                );
            }
            if n > 0 {
                assert!(
                    c.open_file("shard-0000/segments/2.seg", &framed).is_err(),
                    "another file's identity"
                );
            }
        }
        // Two frames swapped do not authenticate.
        let plain = vec![1u8; 2 * CHUNK];
        let framed = c.seal_file("f", &plain).unwrap();
        let mut swapped = Vec::new();
        swapped.extend_from_slice(&framed[FRAME..]);
        swapped.extend_from_slice(&framed[..FRAME]);
        assert!(c.open_file("f", &swapped).is_err());
    }
}

// ------------------------------------------------------- rotation, check

/// What a walk over an encrypted directory did or found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Walked {
    /// Framed files opened (and, on a rotation, sealed again).
    pub files: usize,
    /// Records of the logs opened (and sealed again).
    pub records: usize,
    /// Files a rotation found already under the new key: a run resumed.
    pub already: usize,
    /// Files that opened under neither key, or not at all: what a check
    /// reports and a rotation stops on. Each names the file and why.
    pub failures: Vec<String>,
}

/// The files a database directory holds that are not framed under the
/// data key: the lock, the wrapped key itself (and the one a rotation is
/// moving to), and the marks written as plain text.
fn is_plain_file(name: &str) -> bool {
    matches!(name, "LOCK" | "KEY" | "KEY.next" | "STEWARD" | "CONFIRMED" | "SHIPPED")
}

/// A shard's log: a frame per record, the record's ordinal in the AAD.
fn is_log(name: &str) -> bool {
    name.starts_with("wal") && name.ends_with(".log")
}

/// Every framed file under `dir`, in the order the walk finds them:
/// `f(path, id, is_log)`. The identity is what the shard gives a file --
/// the shard directory's name and the file's own, whichever of
/// `segments/`, `archive/` or `deletes/` holds it -- and a root file's is
/// its name. A move in flight (an `incoming/` directory) is refused: its
/// files are the source's until the move ends.
fn walk_framed(
    dir: &std::path::Path,
    f: &mut dyn FnMut(&std::path::Path, &str, bool) -> Result<()>,
) -> Result<()> {
    fn shard_dir(
        dir: &std::path::Path,
        shard: &str,
        f: &mut dyn FnMut(&std::path::Path, &str, bool) -> Result<()>,
    ) -> Result<()> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.file_type()?.is_dir() {
                if name == "incoming" {
                    return Err(Error::Storage(format!(
                        "{}: a move is in flight (incoming/); finish or abort it first",
                        dir.display()
                    )));
                }
                shard_dir(&entry.path(), shard, f)?;
            } else if !is_plain_file(&name) {
                f(&entry.path(), &format!("{shard}/{name}"), is_log(&name))?;
            }
        }
        Ok(())
    }
    fn shards_in(
        dir: &std::path::Path,
        f: &mut dyn FnMut(&std::path::Path, &str, bool) -> Result<()>,
    ) -> Result<()> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().to_string();
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if name.starts_with("shard-") {
                shard_dir(&entry.path(), &name, f)?;
            } else if name == "followed" {
                shards_in(&entry.path(), f)?;
            } else if name == "incoming" {
                return Err(Error::Storage(format!(
                    "{}: a move is in flight (incoming/); finish or abort it first",
                    dir.display()
                )));
            }
        }
        Ok(())
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if entry.file_type()?.is_dir() {
            if name == "collections" {
                let mut colls: Vec<_> =
                    std::fs::read_dir(entry.path())?.collect::<std::io::Result<_>>()?;
                colls.sort_by_key(|e| e.file_name());
                for c in colls {
                    if c.file_type()?.is_dir() {
                        shards_in(&c.path(), f)?;
                    }
                }
            }
        } else if !is_plain_file(&name) {
            f(&entry.path(), &name, is_log(&name))?;
        }
    }
    Ok(())
}

/// Every framed file and every log record under `dir` opened under
/// `cipher`, nothing written: what `celastro check` prints. A file that
/// does not open is a failure named with its reason, not an error, so one
/// run reports every damaged file.
pub fn check_dir(dir: &std::path::Path, cipher: &Cipher) -> Result<Walked> {
    let mut w = Walked::default();
    walk_framed(dir, &mut |path, id, log| {
        let bytes = std::fs::read(path)?;
        if log {
            let records = cipher.open_records(id, &bytes);
            let opened: usize = records.iter().map(|r| r.len() + 4 + NONCE + TAG).sum();
            if opened < bytes.len() {
                w.failures.push(format!(
                    "{}: {} record(s) open, then the log does not (torn or under another key)",
                    path.display(),
                    records.len()
                ));
            }
            w.records += records.len();
            w.files += 1;
        } else {
            match cipher.open_file(id, &bytes) {
                Ok(_) => w.files += 1,
                Err(e) => w.failures.push(format!("{}: {e}", path.display())),
            }
        }
        Ok(())
    })?;
    Ok(w)
}

/// Every framed file and every log record under `dir` opened under `from`
/// and sealed under `to`, each file replaced atomically. Resumable: a file
/// that opens under `to` already is counted and left; one that opens
/// under neither stops the walk with the file named, nothing else
/// touched. The plain files stay as they are.
pub fn recode_dir(dir: &std::path::Path, from: &Cipher, to: &Cipher) -> Result<Walked> {
    let mut w = Walked::default();
    walk_framed(dir, &mut |path, id, log| {
        let bytes = std::fs::read(path)?;
        if log {
            if bytes.is_empty() {
                return Ok(());
            }
            let records = from.open_records(id, &bytes);
            let opened: usize = records.iter().map(|r| r.len() + 4 + NONCE + TAG).sum();
            if records.is_empty() || opened < bytes.len() {
                // Under the new key already, or damaged: told apart by
                // opening under the new one.
                let under_new = to.open_records(id, &bytes);
                let opened_new: usize = under_new.iter().map(|r| r.len() + 4 + NONCE + TAG).sum();
                if opened_new == bytes.len() {
                    w.already += 1;
                    return Ok(());
                }
                return Err(Error::Storage(format!(
                    "{}: {} record(s) open under the current key, then the log does not; \
                     nothing was changed",
                    path.display(),
                    records.len()
                )));
            }
            let mut out = Vec::with_capacity(bytes.len());
            for (i, r) in records.iter().enumerate() {
                out.extend_from_slice(&to.seal_record(id, i as u64, r)?);
            }
            crate::shard::atomic_write(path, &out)?;
            w.records += records.len();
            w.files += 1;
        } else {
            let plain = match from.open_file(id, &bytes) {
                Ok(p) => p,
                Err(e) => {
                    if to.open_file(id, &bytes).is_ok() {
                        w.already += 1;
                        return Ok(());
                    }
                    return Err(Error::Storage(format!(
                        "{}: {e}; nothing was changed",
                        path.display()
                    )));
                }
            };
            crate::shard::atomic_write(path, &to.seal_file(id, &plain)?)?;
            w.files += 1;
        }
        Ok(())
    })?;
    Ok(w)
}

/// The file a rotation writes its new wrapped key to before it touches
/// anything; its presence is a rotation not finished.
pub const KEY_NEXT: &str = "KEY.next";

/// A new data key for the database at `dir`: every framed file and log
/// record sealed again under a fresh key, then `KEY` rewrapped. The
/// database must not be open. The new key goes to `KEY.next` first, so an
/// interrupted rotation is finished by running it again (files already
/// under the new key are recognised) and a node refuses to open a
/// directory with a `KEY.next` until then. Backups and exports made
/// before carry their own `KEY` and open as they did; an index at the
/// archived tier is refused, since its objects are under the old key
/// where a rotation does not reach.
pub fn rotate_data_key(dir: &std::path::Path, master: &[u8; 32]) -> Result<Walked> {
    let key_path = dir.join("KEY");
    let wrapped = std::fs::read(&key_path).map_err(|e| {
        Error::Storage(format!("{}: {e} (not an encrypted database?)", key_path.display()))
    })?;
    let old = Cipher::unwrap(&wrapped, master)?;
    let next_path = dir.join(KEY_NEXT);
    let new = match crate::shard::read_optional(&next_path)? {
        Some(next) => Cipher::unwrap(&next, master).map_err(|e| {
            Error::Storage(format!("{}: {e}; the interrupted rotation's key", next_path.display()))
        })?,
        None => {
            let c = Cipher::generate()?;
            crate::shard::atomic_write(&next_path, &c.wrap(master)?)?;
            c
        }
    };
    // The catalog first: an index at the archived tier has objects the
    // walk does not reach.
    if let Some(bytes) = crate::shard::read_optional(&dir.join("CATALOG"))? {
        let plain =
            old.open_file("CATALOG", &bytes).or_else(|_| new.open_file("CATALOG", &bytes))?;
        let catalog = crate::catalog::Catalog::decode(&plain)?;
        let archived: Vec<String> = catalog
            .collections
            .values()
            .flat_map(|c| {
                c.indexes
                    .iter()
                    .filter(|i| i.tier == crate::residency::Tier::Archived)
                    .map(move |i| format!("{}.{}", c.name, i.name))
            })
            .collect();
        if !archived.is_empty() {
            let _ = std::fs::remove_file(&next_path);
            return Err(Error::Storage(format!(
                "a rotation does not reach the archived tier; move {} back first (ALTER INDEX \
                 ... SET TIER)",
                archived.join(", ")
            )));
        }
    }
    let w = recode_dir(dir, &old, &new)?;
    std::fs::rename(&next_path, &key_path)?;
    crate::shard::sync_dir(dir)?;
    Ok(w)
}
