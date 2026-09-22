//! HMAC-based key derivation, RFC 5869, over SHA-256: TLS 1.3's key
//! schedule is nothing but this.

use super::sha2::hmac_sha256_into;
use crate::cipher::{wipe, Secret};

/// `HKDF-Extract(salt, ikm)`. The PRK is a secret and leaves as one.
pub fn extract(salt: &[u8], ikm: &[u8]) -> Secret<32> {
    let mut prk = Secret::<32>::zero();
    hmac_sha256_into(salt, ikm, prk.bytes_mut());
    prk
}

/// `HKDF-Expand(prk, info, out.len())`, written into `out`; at most 255
/// blocks.
///
/// Written into the caller's buffer rather than returned, and with one
/// message buffer reused across the rounds, because every intermediate
/// here is key material: `T(i-1)` is secret, so a `Vec` that is replaced
/// each round (or grown, or returned and dropped by a caller that does not
/// know what it holds) leaves derived bytes in freed memory. The buffer
/// and the last block are wiped before this returns.
pub fn expand_into(prk: &[u8; 32], info: &[u8], out: &mut [u8]) {
    assert!(out.len() <= 255 * 32, "HKDF-Expand is bounded at 255 blocks");
    let mut msg = Vec::with_capacity(32 + info.len() + 1);
    let mut block = [0u8; 32];
    let mut i = 1u8;
    let mut done = 0usize;
    while done < out.len() {
        msg.clear();
        if done > 0 {
            msg.extend_from_slice(&block);
        }
        msg.extend_from_slice(info);
        msg.push(i);
        hmac_sha256_into(prk, &msg, &mut block);
        let take = (out.len() - done).min(32);
        out[done..done + take].copy_from_slice(&block[..take]);
        done += take;
        i = i.wrapping_add(1);
    }
    // `msg` never grew past its capacity, so its final length covers every
    // byte any round wrote into it.
    wipe(&mut msg);
    wipe(&mut block);
}

/// `HKDF-Expand` into `N` secret bytes.
///
/// `inline(always)`: returning a type with a `Drop` by value can leave the
/// callee's copy behind -- the move memcpies it out and the moved-from
/// source is never dropped, so it is never wiped. Inlined there is no
/// callee frame to leave it in. Measured, not assumed: `cipher::core_dump`
/// finds one copy without this and none with it.
#[inline(always)]
pub fn expand<const N: usize>(prk: &[u8; 32], info: &[u8]) -> Secret<N> {
    let mut out = Secret::<N>::zero();
    expand_into(prk, info, out.bytes_mut());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hex;

    fn expand_vec(prk: &[u8; 32], info: &[u8], len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        expand_into(prk, info, &mut v);
        v
    }

    #[test]
    fn hkdf_matches_rfc_5869() {
        let ikm = [0x0bu8; 22];
        let salt: Vec<u8> = (0u8..=0x0c).collect();
        let info: Vec<u8> = (0xf0u8..=0xf9).collect();
        let prk = extract(&salt, &ikm);
        assert_eq!(
            hex(&prk[..]),
            "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5"
        );
        let okm = expand_vec(&prk, &info, 42);
        assert_eq!(
            hex(&okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        );
        // Test case 3: no salt, no info.
        let prk = extract(&[], &ikm);
        assert_eq!(
            hex(&prk[..]),
            "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04"
        );
        assert_eq!(
            hex(&expand_vec(&prk, &[], 42)),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
        );
    }

    /// A block boundary and a multi-block expansion share the one buffer;
    /// the reuse must not change the output.
    #[test]
    fn expansion_is_unchanged_by_the_reused_buffer() {
        let prk = extract(b"salt", b"ikm");
        for len in [1usize, 31, 32, 33, 64, 65, 100] {
            let a = expand_vec(&prk, b"info", len);
            assert_eq!(a.len(), len);
            let b = expand_vec(&prk, b"info", len);
            assert_eq!(a, b, "len {len}");
            // A prefix of a longer expansion is the shorter one: HKDF is a
            // stream, and the buffer reuse must not have broken that.
            let long = expand_vec(&prk, b"info", 128);
            assert_eq!(a[..], long[..len], "len {len} is not a prefix of 128");
        }
    }
}
