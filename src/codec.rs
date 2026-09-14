//! Byte-level primitives shared by the variant encoding, the segment format
//! and the WAL. Little-endian throughout; varints are LEB128; signed values are
//! zigzagged first.

pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

pub fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn get_uvarint(b: &[u8], i: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = *b.get(*i)?;
        *i += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

pub fn get_ivarint(b: &[u8], i: &mut usize) -> Option<i64> {
    let u = get_uvarint(b, i)?;
    Some(((u >> 1) as i64) ^ -((u & 1) as i64))
}

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_f32(out: &mut Vec<u8>, v: f32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn get_u32(b: &[u8], i: &mut usize) -> Option<u32> {
    let s = b.get(*i..*i + 4)?;
    *i += 4;
    Some(u32::from_le_bytes(s.try_into().unwrap()))
}

pub fn get_u64(b: &[u8], i: &mut usize) -> Option<u64> {
    let s = b.get(*i..*i + 8)?;
    *i += 8;
    Some(u64::from_le_bytes(s.try_into().unwrap()))
}

pub fn get_f32(b: &[u8], i: &mut usize) -> Option<f32> {
    let s = b.get(*i..*i + 4)?;
    *i += 4;
    Some(f32::from_le_bytes(s.try_into().unwrap()))
}

pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Read a length-prefixed string.
///
/// The length is `checked_add`ed onto the cursor rather than added: a corrupt
/// or hostile length prefix is otherwise an arithmetic overflow — an abort in
/// debug, and in release a wrapped range that reads as an ordinary short-input
/// error only by luck. Decoders run on bytes that may have been damaged; that
/// is the whole reason they return `Option`.
pub fn get_str(b: &[u8], i: &mut usize) -> Option<String> {
    let n = get_uvarint(b, i)? as usize;
    let end = i.checked_add(n)?;
    let s = b.get(*i..end)?;
    *i = end;
    String::from_utf8(s.to_vec()).ok()
}

/// An optional string: a presence byte, then the string.
pub fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => {
            out.push(1);
            put_str(out, s);
        }
        None => out.push(0),
    }
}

pub fn get_opt_str(b: &[u8], i: &mut usize) -> Option<Option<String>> {
    let present = *b.get(*i)?;
    *i += 1;
    if present == 0 {
        Some(None)
    } else {
        get_str(b, i).map(Some)
    }
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub fn get_bytes<'a>(b: &'a [u8], i: &mut usize) -> Option<&'a [u8]> {
    let n = get_uvarint(b, i)? as usize;
    let end = i.checked_add(n)?;
    let s = b.get(*i..end)?;
    *i = end;
    Some(s)
}

/// FNV-1a with a splitmix64 finalizer. Not a cryptographic hash and never used
/// as one.
///
/// The finalizer is not decoration. Raw FNV-1a leaves the **high** bits poorly
/// distributed for short, similar inputs: hashing `note-0000` … `note-0599`
/// and taking the top 10 bits as a bucket lands all 600 in 9 buckets, which
/// silently turned the HyperLogLog in [`crate::catalog`] into a cardinality
/// estimator that reported 9 distinct values for 600. Anything that slices bits
/// off one end of a hash needs the avalanche.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix64(h)
}

/// splitmix64's finalizer: avalanche, so every output bit depends on every
/// input bit.
#[inline]
pub fn mix64(mut z: u64) -> u64 {
    z ^= z >> 33;
    z = z.wrapping_mul(0xff51_afd7_ed55_8ccd);
    z ^= z >> 33;
    z = z.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    z ^ (z >> 33)
}

/// CRC32 (IEEE). Table-driven, because this now covers whole segment bodies at
/// seal and at open, not just a footer — a bitwise loop over a multi-gigabyte
/// segment is not a rounding error.
const fn crc_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static CRC_TABLE: [u32; 256] = crc_table();

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc = CRC_TABLE[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

/// A small deterministic PRNG (xorshift128+). Every randomised decision in the
/// engine — HNSW level assignment, recall sampling — draws from a seeded
/// instance so that runs are reproducible, which is what makes the determinism
/// goal in §1 testable at all.
#[derive(Clone)]
pub struct Rng {
    s0: u64,
    s1: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let s0 = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1);
        let s1 = fnv1a(&seed.to_le_bytes()).max(1);
        Rng { s0, s1 }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.s0;
        let y = self.s1;
        self.s0 = y;
        x ^= x << 23;
        self.s1 = x ^ y ^ (x >> 17) ^ (y >> 26);
        self.s1.wrapping_add(y)
    }

    /// Uniform in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn next_usize(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// Standard normal via Box-Muller; used to generate test vectors.
    pub fn next_normal(&mut self) -> f32 {
        let u1 = (self.next_f64()).max(1e-12);
        let u2 = self.next_f64();
        ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_known_vectors() {
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }

    /// Raw FNV-1a buckets sequential ids into almost nothing when you slice the
    /// high bits. The finalizer is why the catalog's cardinality estimates work.
    #[test]
    fn hash_high_bits_are_well_distributed() {
        let mut buckets = std::collections::BTreeSet::new();
        for i in 0..600 {
            buckets.insert(fnv1a(format!("note-{i:04}").as_bytes()) >> 54);
        }
        assert!(buckets.len() > 350, "only {} of 1024 buckets used", buckets.len());
    }
}
