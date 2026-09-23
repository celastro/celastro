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

/// The data key, ready to derive file keys, and the data keys before it
/// that a rotation kept because the archived tier's objects are still
/// under them: a file that does not open under the current key is tried
/// under each in turn, so a rotation need not move an archive back first.
pub struct Cipher {
    data_key: [u8; 32],
    previous: Vec<[u8; 32]>,
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cipher(..)")
    }
}

/// The wrapped data key as `KEY` holds it: `"CELK1"` | nonce | ciphertext |
/// tag for one key; `"CELK2"` | count | that many (nonce | ciphertext | tag),
/// the current key first and the ring of previous ones after it.
const KEY_MAGIC: &[u8; 5] = b"CELK1";
const KEY_MAGIC_RING: &[u8; 5] = b"CELK2";
const KEY_AAD: &[u8] = b"celastro data key v1";
const WRAPPED: usize = NONCE + 32 + TAG;

impl Cipher {
    /// A fresh data key, from the kernel's randomness.
    pub fn generate() -> Result<Cipher> {
        Ok(Cipher { data_key: crate::crypto::random::array32()?, previous: Vec::new() })
    }

    /// A cipher over `data_key` with `previous` behind it (tests).
    pub fn with_previous(data_key: [u8; 32], previous: Vec<[u8; 32]>) -> Cipher {
        Cipher { data_key, previous }
    }

    /// How many previous data keys this cipher still opens files under.
    pub fn previous_keys(&self) -> usize {
        self.previous.len()
    }

    /// Keep `old`'s current key and ring behind this one's: what a rotation
    /// does when the archived tier's objects stay under the old key.
    pub fn keep_previous(&mut self, old: &Cipher) {
        let mut ring = vec![old.data_key];
        ring.extend(old.previous.iter().copied());
        ring.retain(|k| *k != self.data_key);
        self.previous = ring;
    }

    /// The same data key with no ring behind it: what a retirement leaves.
    /// There is no `Clone` on `Cipher` on purpose -- a cipher copied by
    /// accident is a second copy of a key -- so this is the one way to make
    /// another, and it says in its name what it is for.
    pub fn without_previous(&self) -> Cipher {
        Cipher { data_key: self.data_key, previous: Vec::new() }
    }

    /// Forget the previous keys: `key retire`, once nothing is under them.
    pub fn retire_previous(&mut self) {
        for k in self.previous.iter_mut() {
            wipe(k);
        }
        self.previous.clear();
    }

    /// The data key wrapped under `master`, as the bytes of `KEY`: the one
    /// key in the form every release reads, the ring in the form from
    /// 0.66.0 when there is one.
    pub fn wrap(&self, master: &[u8; 32]) -> Result<Vec<u8>> {
        let wrap_one = |k: &[u8; 32], out: &mut Vec<u8>| -> Result<()> {
            let nonce_bytes = crate::crypto::random::bytes(NONCE)?;
            let mut nonce = [0u8; NONCE];
            nonce.copy_from_slice(&nonce_bytes);
            let mut data = k.to_vec();
            let tag = seal(master, &nonce, KEY_AAD, &mut data);
            out.extend_from_slice(&nonce);
            out.extend_from_slice(&data);
            out.extend_from_slice(&tag);
            Ok(())
        };
        let mut out = Vec::with_capacity(6 + WRAPPED * (1 + self.previous.len()));
        if self.previous.is_empty() {
            out.extend_from_slice(KEY_MAGIC);
            wrap_one(&self.data_key, &mut out)?;
            return Ok(out);
        }
        out.extend_from_slice(KEY_MAGIC_RING);
        out.push((1 + self.previous.len()) as u8);
        wrap_one(&self.data_key, &mut out)?;
        for k in &self.previous {
            wrap_one(k, &mut out)?;
        }
        Ok(out)
    }

    /// The data key `KEY` holds, unwrapped under `master`, with its ring if
    /// it has one; refused with the reason when the master key is not the
    /// one it was wrapped under.
    pub fn unwrap(key_file: &[u8], master: &[u8; 32]) -> Result<Cipher> {
        let unwrap_one = |at: usize| -> Result<[u8; 32]> {
            let mut nonce = [0u8; NONCE];
            nonce.copy_from_slice(&key_file[at..at + NONCE]);
            let mut data = key_file[at + NONCE..at + NONCE + 32].to_vec();
            let mut tag = [0u8; TAG];
            tag.copy_from_slice(&key_file[at + NONCE + 32..at + WRAPPED]);
            if !open(master, &nonce, KEY_AAD, &mut data, &tag) {
                return Err(Error::Storage(
                    "the master key does not open this database's KEY; the database was \
                     encrypted under another"
                        .into(),
                ));
            }
            let mut k = [0u8; 32];
            k.copy_from_slice(&data);
            wipe(&mut data);
            Ok(k)
        };
        if key_file.len() == 5 + WRAPPED && &key_file[..5] == KEY_MAGIC {
            return Ok(Cipher { data_key: unwrap_one(5)?, previous: Vec::new() });
        }
        if key_file.len() >= 6 && &key_file[..5] == KEY_MAGIC_RING {
            let n = key_file[5] as usize;
            if n == 0 || key_file.len() != 6 + n * WRAPPED {
                return Err(Error::Storage("KEY is not a wrapped data key ring".into()));
            }
            let data_key = unwrap_one(6)?;
            let mut previous = Vec::with_capacity(n - 1);
            for i in 1..n {
                previous.push(unwrap_one(6 + i * WRAPPED)?);
            }
            return Ok(Cipher { data_key, previous });
        }
        Err(Error::Storage("KEY is not a wrapped data key".into()))
    }

    /// The key of one file, from its identity, under the current data key.
    fn file_key(&self, id: &str) -> Secret<32> {
        Self::file_key_under(&self.data_key, id)
    }

    fn file_key_under(data_key: &[u8; 32], id: &str) -> Secret<32> {
        let prk = hkdf::extract(b"celastro file key v1", data_key);
        hkdf::expand::<32>(&prk, id.as_bytes())
    }

    /// The file's key under the current data key, then under each previous
    /// one: what a reader tries in turn.
    fn file_keys(&self, id: &str) -> Vec<Secret<32>> {
        let mut v = Vec::with_capacity(1 + self.previous.len());
        v.push(self.file_key(id));
        for k in &self.previous {
            v.push(Self::file_key_under(k, id));
        }
        v
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

    /// The whole of a framed file: under the current key, or under a
    /// previous one the ring keeps.
    pub fn open_file(&self, id: &str, framed_bytes: &[u8]) -> Result<Vec<u8>> {
        let keys = self.file_keys(id);
        let mut last = None;
        for key in &keys {
            match self.open_file_under(key, id, framed_bytes) {
                Ok(p) => return Ok(p),
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one key"))
    }

    fn open_file_under(&self, key: &[u8; 32], id: &str, framed_bytes: &[u8]) -> Result<Vec<u8>> {
        let key = *key;
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
        // Under the current key, then under each the ring keeps.
        let keys = self.file_keys(id);
        let mut last = None;
        for key in &keys {
            match self.read_range_under(key, id, read, off, len) {
                Ok(p) => return Ok(p),
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one key"))
    }

    fn read_range_under(
        &self,
        key: &[u8; 32],
        id: &str,
        read: &dyn Fn(u64, u64) -> Result<Vec<u8>>,
        off: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let key = *key;
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
        // The key the first record opens under decides the log's: a log is
        // under one key, the current or one the ring keeps.
        let keys = self.file_keys(id);
        for key in &keys {
            let out = self.open_records_under(key, id, log);
            if !out.is_empty() || log.len() < 4 {
                return out;
            }
        }
        Vec::new()
    }

    fn open_records_under(&self, key: &[u8; 32], id: &str, log: &[u8]) -> Vec<Vec<u8>> {
        let key = *key;
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
/// volatile write per byte, a fence after, and the slice handed to
/// `black_box` so the writes cannot be proved unread. What every secret
/// does to itself when it is dropped, so a key does not outlive its use in
/// freed memory a later allocation, a core dump or a swap file could show.
///
/// `#[inline(never)]` is part of the contract, not a hint about code size:
/// inlined into a caller that drops the buffer immediately afterwards, the
/// whole loop is a dead store, and a volatile write is only guaranteed
/// against *elision of the write itself* -- the fence and the black_box are
/// what stop the surrounding reasoning, and they are cheaper to trust when
/// the optimiser cannot see both sides of the call at once.
#[inline(never)]
pub fn wipe(bytes: &mut [u8]) {
    for b in bytes.iter_mut() {
        // SAFETY: `b` is a valid, exclusive reference into `bytes`.
        unsafe { std::ptr::write_volatile(b, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    std::hint::black_box(bytes);
}

/// Wipe a string's bytes before it is dropped.
pub fn wipe_string(s: &mut String) {
    // SAFETY: zeros are valid UTF-8, and the string is cleared right after.
    wipe(unsafe { s.as_bytes_mut() });
    s.clear();
}

/// `N` secret bytes that wipe themselves when dropped and print as
/// nothing: the master key as the options hold it.
/// Equality is deliberately not derived. `==` on `[u8; N]` compares byte
/// by byte and stops at the first difference, which over a secret is a
/// timing oracle, and a derive would let any future caller write it with
/// no warning. [`Secret::ct_eq`] is the comparison this type offers.
#[derive(Clone)]
pub struct Secret<const N: usize>([u8; N]);

impl<const N: usize> From<[u8; N]> for Secret<N> {
    fn from(mut b: [u8; N]) -> Self {
        let s = Secret(b);
        // `[u8; N]` is Copy, so this took a copy and the caller still holds
        // its own. Erase the copy that landed here: it is one of the stack
        // copies this type exists to bound, and the only one this function
        // is in a position to reach. The caller's is the caller's problem,
        // which is why the paths that matter hand out `Secret` rather than
        // arrays in the first place.
        wipe(&mut b);
        s
    }
}

impl<const N: usize> Secret<N> {
    /// `N` zero bytes, to be written into and dropped like any other
    /// secret: what a derivation expands into.
    pub fn zero() -> Self {
        Secret([0u8; N])
    }

    /// The bytes, to write into. There is no `into_inner`: a secret that
    /// could be moved out as a bare array would leave an unwiped copy
    /// behind, which is the whole point of the type.
    pub fn bytes_mut(&mut self) -> &mut [u8; N] {
        &mut self.0
    }

    /// Whether two secrets are the same, in time that does not depend on
    /// where they first differ.
    pub fn ct_eq(&self, other: &Secret<N>) -> bool {
        crate::crypto::ct_eq(&self.0, &other.0)
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
        for k in self.previous.iter_mut() {
            wipe(k);
        }
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
        let c = Cipher { data_key: [7; 32], previous: Vec::new() };
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
    /// Previous data keys the rotation kept in a ring, for an archived
    /// tier whose objects are still under them.
    pub kept_keys: usize,
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
/// archived tier keeps the old key in a ring behind the new one, since
/// its objects are under the old key where a rotation does not reach, and
/// `retire_keys` drops the ring once they are not.
pub fn rotate_data_key(dir: &std::path::Path, master: &[u8; 32]) -> Result<Walked> {
    let key_path = dir.join("KEY");
    let wrapped = std::fs::read(&key_path).map_err(|e| {
        Error::Storage(format!("{}: {e} (not an encrypted database?)", key_path.display()))
    })?;
    let old = Cipher::unwrap(&wrapped, master)?;
    let next_path = dir.join(KEY_NEXT);
    let mut new = match crate::shard::read_optional(&next_path)? {
        Some(next) => Cipher::unwrap(&next, master).map_err(|e| {
            Error::Storage(format!("{}: {e}; the interrupted rotation's key", next_path.display()))
        })?,
        None => Cipher::generate()?,
    };
    // The catalog first: an index at the archived tier has objects the
    // walk does not reach, so the old key stays behind the new one in a
    // ring and those objects open as before, until `key retire`.
    if let Some(bytes) = crate::shard::read_optional(&dir.join("CATALOG"))? {
        let plain =
            old.open_file("CATALOG", &bytes).or_else(|_| new.open_file("CATALOG", &bytes))?;
        let catalog = crate::catalog::Catalog::decode(&plain)?;
        let archived = catalog
            .collections
            .values()
            .any(|c| c.indexes.iter().any(|i| i.tier == crate::residency::Tier::Archived));
        if archived && new.previous.is_empty() {
            new.keep_previous(&old);
        }
    }
    if !next_path.exists() {
        crate::shard::atomic_write(&next_path, &new.wrap(master)?)?;
    }
    let mut w = recode_dir(dir, &old, &new)?;
    w.kept_keys = new.previous.len();
    std::fs::rename(&next_path, &key_path)?;
    crate::shard::sync_dir(dir)?;
    Ok(w)
}

/// `key retire`: the ring of previous data keys dropped from `KEY`, once
/// nothing is under them any more (an archived index moved back and out
/// again after the rotation, or dropped). Objects still under them stop
/// opening. How many were dropped.
/// How many bytes of a framed file are enough to tell which key sealed it:
/// one whole frame, which authenticates on its own.
pub const HEAD_BYTES: usize = FRAME;

/// What an archived object's key says about it: the collection it belongs
/// to, the shard, and the identity it was sealed under.
///
/// The object key is `<prefix><collection>/<shard>/<id>.seg` and the seal
/// identity is `<shard>/<id>.seg` (`Shard::file_id`), so one is read off
/// the other and neither has to be guessed from a manifest.
pub fn archived_seal_id(object_key: &str) -> Option<(String, String, String)> {
    let mut parts = object_key.rsplitn(3, '/');
    let file = parts.next()?.to_string();
    let shard = parts.next()?.to_string();
    let coll = parts.next()?.rsplit('/').next()?.to_string();
    if !file.ends_with(".seg") {
        return None;
    }
    Some((coll, shard.clone(), format!("{shard}/{file}")))
}

impl Cipher {
    /// Which key of the ring opens `id`'s first frame: `Some(0)` for the
    /// current key, `Some(n)` for the nth previous one, `None` for a frame
    /// no key in the ring opens.
    ///
    /// `head` is the first [`HEAD_BYTES`] of the file, or the whole of it
    /// when it is shorter -- a frame authenticates on its own, so nothing
    /// more needs fetching to answer this.
    pub fn opening_key(&self, id: &str, head: &[u8]) -> Option<usize> {
        let keys = self.file_keys(id);
        for (n, key) in keys.iter().enumerate() {
            if self.open_file_under(key, id, head).is_ok() {
                return Some(n);
            }
        }
        None
    }

    /// `framed` opened under whichever key of the ring seals it and sealed
    /// again under the current one, a frame at a time into `out`.
    ///
    /// Frames are independent -- a nonce, the ciphertext and a tag, with
    /// the file's identity and the frame's index as associated data -- so
    /// a re-seal does not need the file in one piece. Holding one frame
    /// rather than the object, its plaintext and its re-sealed copy all at
    /// once is the same economy that made a backup stream a segment from
    /// its file instead of reading it whole (0.70.0); an archived segment
    /// is the same size as that one.
    ///
    /// Which key opens it is decided from the first frame, so the object is
    /// not decrypted once per candidate key to find out.
    pub fn reseal_into(&self, id: &str, sealed: &[u8], out: &mut dyn std::io::Write) -> Result<()> {
        let head = &sealed[..sealed.len().min(HEAD_BYTES)];
        let Some(which) = self.opening_key(id, head) else {
            return Err(Error::Storage(format!(
                "{id}: no data key this KEY holds opens it; it is damaged, or was sealed under a \
                 key that has been retired"
            )));
        };
        let keys = self.file_keys(id);
        let from = &keys[which];
        let to = self.file_key(id);
        let (mut index, mut rest) = (0u64, sealed);
        loop {
            let take = rest.len().min(FRAME);
            if take < NONCE + TAG {
                return Err(Error::Storage(format!("{id}: an encrypted frame is torn")));
            }
            let (frame, after) = rest.split_at(take);
            let mut plain = self.open_frame(from, id, index, frame)?;
            let mut one = Vec::with_capacity(framed(plain.len()));
            self.push_frame(&to, id, index, &plain, &mut one)?;
            wipe(&mut plain);
            out.write_all(&one)
                .map_err(|e| Error::Storage(format!("{id}: writing a re-sealed frame: {e}")))?;
            rest = after;
            index += 1;
            if rest.is_empty() {
                return Ok(());
            }
        }
    }

    /// [`Cipher::reseal_into`] into a buffer, for a caller that wants the
    /// bytes rather than a writer.
    pub fn reseal_file(&self, id: &str, framed: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(framed.len());
        self.reseal_into(id, framed, &mut out)?;
        Ok(out)
    }
}

/// What a walk of the archived tier found: the objects still sealed under a
/// previous data key, by collection.
#[derive(Debug, Default)]
pub struct ArchiveWalk {
    pub objects: usize,
    /// Objects that open only under a previous key, and the collections
    /// they belong to.
    pub under_previous: usize,
    pub collections: std::collections::BTreeSet<String>,
    /// Objects no key in the ring opens: neither retiring nor re-sealing
    /// helps these, and a walk says so rather than passing over them.
    pub unopenable: Vec<String>,
}

/// Whether a backup has been written under this prefix as well.
///
/// **A backup's layout and the archived tier's are the same shape.** A
/// backup keeps its segments at `pool/<collection>/<shard>/<id>.seg` and
/// the tier keeps its own at `<prefix><collection>/<shard>/<id>.seg`, and
/// the seal identity read off either is the same string -- so a tier and a
/// backup destination sharing a bucket and a prefix cannot be told apart
/// by the shape of a key. Re-sealing a backup's pool object under the
/// current data key would leave every record that names it with a hash
/// that no longer matches: `VERIFY BACKUP` would call the backup damaged
/// and a restore would refuse it.
///
/// `nodes/<slug>/LATEST` is written by every completed backup and by
/// nothing the tier writes, so its presence is the overlap, and the
/// commands refuse rather than guess.
pub fn backups_share_this_prefix(
    store: &dyn crate::objstore::ObjectStore,
    prefix: &str,
) -> Result<bool> {
    Ok(store.list(&format!("{prefix}nodes/"))?.iter().any(|k| k.ends_with("/LATEST")))
}

/// The archived objects of `colls`, and which key of the ring opens each.
/// One ranged read of a frame per object; nothing is downloaded whole.
///
/// It lists each collection's own prefix rather than the whole of
/// `prefix`, so that nothing outside the tier is ever considered -- see
/// [`backups_share_this_prefix`] for what else can be under there.
pub fn walk_archive(
    cipher: &Cipher,
    store: &dyn crate::objstore::ObjectStore,
    prefix: &str,
    colls: &[String],
) -> Result<ArchiveWalk> {
    let mut w = ArchiveWalk::default();
    for name in colls {
        for key in store.list(&format!("{prefix}{name}/"))? {
            let Some((coll, _shard, id)) = archived_seal_id(&key) else { continue };
            if &coll != name {
                continue;
            }
            // One request per object: whether it is there and what its
            // first frame says are the same question.
            let Some(head) = store.head(&key, HEAD_BYTES as u64)? else { continue };
            w.objects += 1;
            match cipher.opening_key(&id, &head) {
                Some(0) => {}
                Some(_) => {
                    w.under_previous += 1;
                    w.collections.insert(coll);
                }
                None => w.unopenable.push(key),
            }
        }
    }
    Ok(w)
}

/// Re-seal every archived object of `colls` that is not already under the
/// current data key, so a rotation can be finished without moving the tier
/// back.
///
/// Object by object: a ranged read says which key seals it, an object that
/// needs it is fetched, re-sealed a frame at a time into a scratch file and
/// `put_file`d back, so neither the upload nor the re-seal holds the object
/// more than once over. A failure part way leaves what it has already done
/// -- every object is independent, and running it again finishes the rest.
///
/// `scratch` is a directory this makes and removes; it is not the data
/// directory, so a run cut short leaves nothing beside the database.
pub fn reseal_archive(
    cipher: &Cipher,
    store: &dyn crate::objstore::ObjectStore,
    prefix: &str,
    colls: &[String],
    scratch: &std::path::Path,
) -> Result<usize> {
    // A previous run cut short is the normal reason to be here.
    let _ = std::fs::remove_dir_all(scratch);
    std::fs::create_dir_all(scratch)
        .map_err(|e| Error::Storage(format!("{}: {e}", scratch.display())))?;
    let tmp = scratch.join("object.part");
    let mut done = 0usize;
    let out = (|| -> Result<usize> {
        for name in colls {
            for key in store.list(&format!("{prefix}{name}/"))? {
                let Some((coll, _shard, id)) = archived_seal_id(&key) else { continue };
                if &coll != name {
                    continue;
                }
                let Some(head) = store.head(&key, HEAD_BYTES as u64)? else { continue };
                match cipher.opening_key(&id, &head) {
                    Some(0) | None => continue,
                    Some(_) => {}
                }
                let sealed = store.get(&key)?;
                let mut f = std::fs::File::create(&tmp)
                    .map_err(|e| Error::Storage(format!("{}: {e}", tmp.display())))?;
                cipher.reseal_into(&id, &sealed, &mut f)?;
                drop(sealed);
                use std::io::Write;
                f.flush().map_err(|e| Error::Storage(format!("{}: {e}", tmp.display())))?;
                f.sync_all().map_err(|e| Error::Storage(format!("{}: {e}", tmp.display())))?;
                drop(f);
                store.put_file(&key, &tmp)?;
                done += 1;
            }
        }
        Ok(done)
    })();
    let _ = std::fs::remove_dir_all(scratch);
    out
}

pub fn retire_keys(dir: &std::path::Path, master: &[u8; 32]) -> Result<usize> {
    let key_path = dir.join("KEY");
    let wrapped = std::fs::read(&key_path).map_err(|e| {
        Error::Storage(format!("{}: {e} (not an encrypted database?)", key_path.display()))
    })?;
    let mut cipher = Cipher::unwrap(&wrapped, master)?;
    let n = cipher.previous_keys();
    if n > 0 {
        cipher.retire_previous();
        crate::shard::atomic_write(&key_path, &cipher.wrap(master)?)?;
    }
    Ok(n)
}

#[cfg(test)]
mod ring_tests {
    use super::*;

    /// A file sealed under one data key opens under a cipher that keeps
    /// that key behind its own, and the ring survives a wrap and unwrap;
    /// a one-key file wraps in the form every release reads; retiring
    /// the ring is what stops the old file opening.
    #[test]
    fn a_previous_key_in_the_ring_opens_what_was_sealed_under_it() {
        let master = [9u8; 32];
        let old = Cipher::generate().unwrap();
        let sealed = old.seal_file("shard-0000/a.seg", b"rows").unwrap();
        let log = [
            old.seal_record("shard-0000/wal.log", 0, b"r0").unwrap(),
            old.seal_record("shard-0000/wal.log", 1, b"r1").unwrap(),
        ]
        .concat();
        let mut new = Cipher::generate().unwrap();
        assert!(new.open_file("shard-0000/a.seg", &sealed).is_err());
        new.keep_previous(&old);
        assert_eq!(new.previous_keys(), 1);
        assert_eq!(new.open_file("shard-0000/a.seg", &sealed).unwrap(), b"rows");
        assert_eq!(new.open_records("shard-0000/wal.log", &log).len(), 2);
        let range = new
            .read_range(
                "shard-0000/a.seg",
                &|off, len| {
                    Ok(sealed[off as usize..(off + len).min(sealed.len() as u64) as usize].to_vec())
                },
                1,
                2,
            )
            .unwrap();
        assert_eq!(range, b"ow");
        let wrapped = new.wrap(&master).unwrap();
        assert_eq!(&wrapped[..5], b"CELK2");
        let back = Cipher::unwrap(&wrapped, &master).unwrap();
        assert_eq!(back.previous_keys(), 1);
        assert_eq!(back.open_file("shard-0000/a.seg", &sealed).unwrap(), b"rows");
        let plain = old.wrap(&master).unwrap();
        assert_eq!(&plain[..5], b"CELK1");
        assert_eq!(Cipher::unwrap(&plain, &master).unwrap().previous_keys(), 0);
        let mut retired = back;
        retired.retire_previous();
        assert!(retired.open_file("shard-0000/a.seg", &sealed).is_err());
        assert_eq!(&retired.wrap(&master).unwrap()[..5], b"CELK1");
    }
}

/// H5's proof, as far as a byte search can prove anything: what is left of
/// a secret in this process's own memory once the code that used it has
/// dropped it.
///
/// A core dump is a copy of exactly these regions, so searching them from
/// inside is the search `gcore` would allow, without the tool, without a
/// second process, and as a test rather than a procedure:
/// `/proc/self/maps` names the writable private mappings (the heap and the
/// stacks) and `/proc/self/mem` reads them.
///
/// **The needle is never held in the clear.** It is XORed into `masked` as
/// it is produced, and the scan compares each candidate byte against
/// `masked[j] ^ MASK[j]`, computed one byte at a time. The 32 plaintext
/// bytes therefore exist nowhere contiguously, so every contiguous run the
/// scan finds is a copy some code path left behind -- not the needle
/// looking at itself, which is the trap that makes this kind of test lie.
///
/// Ignored: it reads its own address space and takes a second or two. Run
/// it in release, where the optimiser has actually had its way with the
/// wipes -- a debug build proves nothing about what release code leaves.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod core_dump {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};

    const MASK: [u8; 32] = [0x5c; 32];

    pub(crate) fn mask(s: &[u8; 32]) -> [u8; 32] {
        std::array::from_fn(|i| s[i] ^ MASK[i])
    }

    /// The writable private mappings, which is what a core dump keeps.
    fn regions() -> Vec<(u64, u64)> {
        let maps = std::fs::read_to_string("/proc/self/maps").expect("/proc/self/maps");
        let mut out = Vec::new();
        for line in maps.lines() {
            let mut f = line.split_whitespace();
            let (range, perms) = (f.next().unwrap_or(""), f.next().unwrap_or(""));
            let path = f.nth(3).unwrap_or("");
            // Writable and private. File-backed mappings of our own binary
            // are skipped: constants live there and are not what a wipe is
            // about. `[vvar]` and friends cannot be read at all.
            if !perms.starts_with("rw") || !perms.contains('p') {
                continue;
            }
            if !(path.is_empty() || path == "[heap]" || path == "[stack]") {
                continue;
            }
            let Some((a, b)) = range.split_once('-') else { continue };
            let (Ok(a), Ok(b)) = (u64::from_str_radix(a, 16), u64::from_str_radix(b, 16)) else {
                continue;
            };
            out.push((a, b));
        }
        out
    }

    /// Which mapping an address falls in, for a hit worth explaining.
    fn where_is(addr: u64) -> String {
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
        for line in maps.lines() {
            let mut f = line.split_whitespace();
            let range = f.next().unwrap_or("");
            let path = f.nth(4).unwrap_or("");
            if let Some((a, b)) = range.split_once('-') {
                let (Ok(a), Ok(b)) = (u64::from_str_radix(a, 16), u64::from_str_radix(b, 16))
                else {
                    continue;
                };
                if addr >= a && addr < b {
                    return if path.is_empty() { "anonymous".into() } else { path.into() };
                }
            }
        }
        "gone".into()
    }

    /// How many times the unmasked needle appears in this process's memory,
    /// and where.
    pub(crate) fn occurrences(masked: &[u8; 32]) -> usize {
        let mut mem = std::fs::File::open("/proc/self/mem").expect("/proc/self/mem");
        let mut buf = vec![0u8; 1 << 20];
        let mut hits = 0usize;
        let mut found: Vec<u64> = Vec::new();
        for (start, end) in regions() {
            let mut at = start;
            while at < end {
                let want = ((end - at) as usize).min(buf.len());
                if mem.seek(SeekFrom::Start(at)).is_err() {
                    break;
                }
                let n = match mem.read(&mut buf[..want]) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                for (off, w) in buf[..n].windows(32).enumerate() {
                    if (0..32).all(|j| w[j] == masked[j] ^ MASK[j]) {
                        hits += 1;
                        found.push(at + off as u64);
                    }
                }
                at += n as u64;
            }
            // The buffer now holds a copy of the region just read. Left
            // alone, it would be counted a second time when the scan
            // reaches the buffer's own pages.
            wipe(&mut buf);
        }
        for a in found.iter().take(4) {
            eprintln!("  a copy at {a:#x} in {}", where_is(*a));
        }
        hits
    }

    /// Derive a file key, use it, drop it -- then look for it.
    #[test]
    #[ignore]
    fn a_file_key_is_not_left_in_memory() {
        let data_key = [0xa7u8; 32];
        let masked = {
            let k = Cipher::file_key_under(&data_key, "shard-0000/a.seg");
            mask(&k)
        };
        // The same derivation as the real path, used and dropped.
        {
            let c = Cipher { data_key, previous: Vec::new() };
            let _ = c.seal_file("shard-0000/a.seg", b"a segment's bytes");
        }
        let n = occurrences(&masked);
        eprintln!("file key: {n} copy(ies) left in memory");
        assert_eq!(n, 0, "a file key is still in memory {n} time(s) after its last use");
    }

    /// The private scalar X25519 clamps onto its stack.
    #[test]
    #[ignore]
    fn the_x25519_scalar_is_not_left_in_memory() {
        let scalar = [0x9du8; 32];
        let mut clamped = scalar;
        clamped[0] &= 248;
        clamped[31] &= 127;
        clamped[31] |= 64;
        let masked = mask(&clamped);
        {
            let mut base = [0u8; 32];
            base[0] = 9;
            let _shared = crate::crypto::x25519::x25519(&scalar, &base);
        }
        let n = occurrences(&masked);
        eprintln!("x25519 clamped scalar: {n} copy(ies) left in memory");
        // `scalar` itself is the caller's and is deliberately still alive;
        // the clamped form is the copy the ladder made, and it is wiped.
        assert_eq!(n, 0, "the clamped scalar is still in memory {n} time(s)");
    }

    /// An HKDF block: the expansion's own buffers.
    #[test]
    #[ignore]
    fn an_hkdf_block_is_not_left_in_memory() {
        let prk = [0x31u8; 32];
        let masked = {
            let out = crate::crypto::hkdf::expand::<32>(&prk, b"celastro h5");
            mask(&out)
        };
        for _ in 0..4 {
            let mut s = Secret::<32>::zero();
            crate::crypto::hkdf::expand_into(&prk, b"celastro h5", s.bytes_mut());
        }
        let n = occurrences(&masked);
        eprintln!("hkdf block, expanded into a caller's Secret: {n} copy(ies) left in memory");
        assert_eq!(n, 0, "a derived block is still in memory {n} time(s)");
    }

    /// The residue this whole entry is about, measured rather than argued.
    ///
    /// Returning a `Secret` by value can leave the callee's copy behind:
    /// the move memcpies the bytes to the caller and the moved-from source
    /// is never dropped, so `Drop` never runs on it and it is never wiped.
    /// It sits in a frame that has returned until that stack is reused.
    /// `#[inline(always)]` on the returning function does not remove it --
    /// tried, and the count did not move -- and neither does anything else
    /// available from Rust: this is the part of the claim that the language
    /// cannot make, which is why the number is recorded instead of hidden.
    ///
    /// Whether it happens at all depends on where the compiler chose to
    /// build the value: the file key above is also returned by value and
    /// leaves nothing, because that return is in tail position and is built
    /// in the caller's slot. So this asserts a bound, not an equality.
    #[test]
    #[ignore]
    fn a_secret_returned_by_value_can_leave_one_copy() {
        let prk = [0x53u8; 32];
        let masked = {
            let out = crate::crypto::hkdf::expand::<32>(&prk, b"by value");
            mask(&out)
        };
        for _ in 0..4 {
            let _ = crate::crypto::hkdf::expand::<32>(&prk, b"by value");
        }
        let n = occurrences(&masked);
        eprintln!("hkdf block, returned by value: {n} copy(ies) left in memory");
        assert!(n <= 1, "a returned secret left {n} copies, which is more than the one recorded");
    }
}

#[cfg(test)]
mod archive_ring_tests {
    use super::*;
    use crate::objstore::{DirStore, ObjectStore};

    fn colls(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn temp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "celastro-ring-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// The seal identity is read off the object key, so a walk needs no
    /// manifest to know what a segment was sealed as.
    #[test]
    fn an_object_key_names_the_collection_and_the_seal_identity() {
        let (coll, shard, id) =
            archived_seal_id("docs/shard-0000/0000000000000007.seg").expect("a segment key");
        assert_eq!((coll.as_str(), shard.as_str()), ("docs", "shard-0000"));
        assert_eq!(id, "shard-0000/0000000000000007.seg");
        // A prefix in front of it changes only the collection's position.
        let (coll, _, id) =
            archived_seal_id("celastro/archive/docs/shard-0002/0000000000000001.seg").unwrap();
        assert_eq!((coll.as_str(), id.as_str()), ("docs", "shard-0002/0000000000000001.seg"));
        // Anything that is not a segment is not one.
        assert!(archived_seal_id("docs/shard-0000/MANIFEST").is_none());
        assert!(archived_seal_id("lonely.seg").is_none());
    }

    /// The walk tells an object under the current key from one under a
    /// previous key, reading a frame of each rather than the whole object,
    /// and names the collection a stale one belongs to.
    #[test]
    fn the_walk_finds_what_is_still_under_a_previous_key() {
        let root = temp("walk");
        let store = DirStore::new(&root).unwrap();
        let old = Cipher::generate().unwrap();
        let mut new = Cipher::generate().unwrap();
        new.keep_previous(&old);

        // Big enough that a whole-object read would be a different thing
        // from the ranged read the walk does.
        let plain = vec![7u8; CHUNK * 3 + 11];
        let stale_id = "shard-0000/0000000000000001.seg";
        store.put(&format!("docs/{stale_id}"), &old.seal_file(stale_id, &plain).unwrap()).unwrap();
        let fresh_id = "shard-0001/0000000000000002.seg";
        store.put(&format!("docs/{fresh_id}"), &new.seal_file(fresh_id, &plain).unwrap()).unwrap();
        let other_id = "shard-0000/0000000000000003.seg";
        store.put(&format!("logs/{other_id}"), &old.seal_file(other_id, &plain).unwrap()).unwrap();

        let w = walk_archive(&new, &store, "", &colls(&["docs", "logs"])).unwrap();
        assert_eq!(w.objects, 3);
        assert_eq!(w.under_previous, 2, "two were sealed under the old key");
        assert_eq!(
            w.collections.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["docs", "logs"],
            "the walk names every collection with a stale object"
        );
        assert!(w.unopenable.is_empty());
    }

    /// An object no key in the ring opens is reported rather than passed
    /// over: retiring would hide it, and it is not the ring's fault.
    #[test]
    fn an_object_under_no_key_at_all_is_named() {
        let root = temp("stranger");
        let store = DirStore::new(&root).unwrap();
        let stranger = Cipher::generate().unwrap();
        let mine = Cipher::generate().unwrap();
        let id = "shard-0000/0000000000000001.seg";
        store.put(&format!("docs/{id}"), &stranger.seal_file(id, b"not mine").unwrap()).unwrap();
        let w = walk_archive(&mine, &store, "", &colls(&["docs"])).unwrap();
        assert_eq!(w.under_previous, 0);
        assert_eq!(w.unopenable, vec!["docs/shard-0000/0000000000000001.seg".to_string()]);
    }

    /// Re-sealing moves every stale object onto the current key, leaves the
    /// ones already there alone, and keeps the bytes.
    #[test]
    fn re_sealing_puts_every_object_under_the_current_key() {
        let root = temp("reseal");
        let store = DirStore::new(&root).unwrap();
        let old = Cipher::generate().unwrap();
        let mut new = Cipher::generate().unwrap();
        new.keep_previous(&old);

        let plain = vec![3u8; CHUNK + 5];
        let stale_id = "shard-0000/0000000000000001.seg";
        store.put(&format!("docs/{stale_id}"), &old.seal_file(stale_id, &plain).unwrap()).unwrap();
        let fresh_id = "shard-0000/0000000000000002.seg";
        let fresh_bytes = new.seal_file(fresh_id, &plain).unwrap();
        store.put(&format!("docs/{fresh_id}"), &fresh_bytes).unwrap();

        let tmp = temp("reseal-tmp");
        let n = reseal_archive(&new, &store, "", &colls(&["docs"]), &tmp).unwrap();
        assert_eq!(n, 1, "only the stale object is rewritten");
        assert_eq!(
            store.get(&format!("docs/{fresh_id}")).unwrap(),
            fresh_bytes,
            "an object already under the current key is not touched"
        );

        let after = walk_archive(&new, &store, "", &colls(&["docs"])).unwrap();
        assert_eq!(after.under_previous, 0, "nothing is under a previous key now");
        assert_eq!(after.objects, 2);

        // The point of all of it: the rows survive, and they survive the
        // ring being retired, which is what would have destroyed them.
        let mut retired = new.without_previous();
        retired.retire_previous();
        assert_eq!(
            retired.open_file(stale_id, &store.get(&format!("docs/{stale_id}")).unwrap()).unwrap(),
            plain
        );
        // Not merely emptied: the scratch directory is taken away, so a run
        // leaves nothing beside the database at all.
        assert!(!tmp.exists(), "a re-seal left its scratch directory behind");
    }

    /// A backup's objects are not the archived tier's, however alike they
    /// look.
    ///
    /// `pool/<collection>/<shard>/<id>.seg` and the tier's
    /// `<collection>/<shard>/<id>.seg` yield the same seal identity, so a
    /// walk that listed the prefix whole would judge a backup's segments as
    /// the tier's and a re-seal would rewrite them -- leaving every record
    /// that names them with a hash that no longer matches. Two guards: the
    /// walk lists each collection's own prefix, and a prefix that holds
    /// backups is refused outright.
    #[test]
    fn a_backup_under_the_same_prefix_is_neither_walked_nor_re_sealed() {
        let root = temp("shared");
        let store = DirStore::new(&root).unwrap();
        let old = Cipher::generate().unwrap();
        let mut new = Cipher::generate().unwrap();
        new.keep_previous(&old);

        // A backup, as `backup::run` lays one out: a pool segment under the
        // old key, and the LATEST that says a backup was written here.
        let pooled = "shard-0000/0000000000000001.seg";
        let pool_key = format!("pool/docs/{pooled}");
        let pool_bytes = old.seal_file(pooled, b"a backup's segment").unwrap();
        store.put(&pool_key, &pool_bytes).unwrap();
        store.put("nodes/local/LATEST", b"123\n").unwrap();
        // And one real archived object of the tier, also under the old key.
        let mine = "shard-0000/0000000000000002.seg";
        store.put(&format!("docs/{mine}"), &old.seal_file(mine, b"the tier's").unwrap()).unwrap();

        // The overlap is detectable, and the commands refuse on it.
        assert!(backups_share_this_prefix(&store, "").unwrap());
        assert!(!backups_share_this_prefix(&store, "elsewhere/").unwrap());

        // Even if it were not refused, scoping to the collection keeps the
        // walk off `pool/`: one object seen, not two.
        let w = walk_archive(&new, &store, "", &colls(&["docs"])).unwrap();
        assert_eq!(w.objects, 1, "the backup's pool segment was walked as the tier's");
        assert_eq!(w.under_previous, 1);

        let tmp = temp("shared-tmp");
        assert_eq!(reseal_archive(&new, &store, "", &colls(&["docs"]), &tmp).unwrap(), 1);
        assert_eq!(
            store.get(&pool_key).unwrap(),
            pool_bytes,
            "a re-seal rewrote a backup's pool object; every record naming it is now wrong"
        );
    }

    /// Re-sealing is resumable: it only ever moves an object forward, so
    /// running it twice is running it once.
    #[test]
    fn re_sealing_twice_is_re_sealing_once() {
        let root = temp("again");
        let store = DirStore::new(&root).unwrap();
        let old = Cipher::generate().unwrap();
        let mut new = Cipher::generate().unwrap();
        new.keep_previous(&old);
        let id = "shard-0000/0000000000000001.seg";
        store.put(&format!("docs/{id}"), &old.seal_file(id, b"rows").unwrap()).unwrap();
        let tmp = temp("again-tmp");
        assert_eq!(reseal_archive(&new, &store, "", &colls(&["docs"]), &tmp).unwrap(), 1);
        assert_eq!(
            reseal_archive(&new, &store, "", &colls(&["docs"]), &tmp).unwrap(),
            0,
            "nothing left to do"
        );
    }
}
