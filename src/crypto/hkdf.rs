//! HMAC-based key derivation, RFC 5869, over SHA-256: TLS 1.3's key
//! schedule is nothing but this.

use super::sha2::hmac_sha256;

/// `HKDF-Extract(salt, ikm)`.
pub fn extract(salt: &[u8], ikm: &[u8]) -> [u8; 32] {
    hmac_sha256(salt, ikm)
}

/// `HKDF-Expand(prk, info, len)`, `len` at most 255 × 32.
pub fn expand(prk: &[u8; 32], info: &[u8], len: usize) -> Vec<u8> {
    assert!(len <= 255 * 32, "HKDF-Expand is bounded at 255 blocks");
    let mut out = Vec::with_capacity(len);
    let mut prev: Vec<u8> = Vec::new();
    let mut i = 1u8;
    while out.len() < len {
        let mut msg = Vec::with_capacity(prev.len() + info.len() + 1);
        msg.extend_from_slice(&prev);
        msg.extend_from_slice(info);
        msg.push(i);
        let block = hmac_sha256(prk, &msg);
        let take = (len - out.len()).min(32);
        out.extend_from_slice(&block[..take]);
        prev = block.to_vec();
        i = i.wrapping_add(1);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hex;

    #[test]
    fn hkdf_matches_rfc_5869() {
        let ikm = [0x0bu8; 22];
        let salt: Vec<u8> = (0u8..=0x0c).collect();
        let info: Vec<u8> = (0xf0u8..=0xf9).collect();
        let prk = extract(&salt, &ikm);
        assert_eq!(hex(&prk), "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5");
        let okm = expand(&prk, &info, 42);
        assert_eq!(
            hex(&okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        );
        // Test case 3: no salt, no info.
        let prk = extract(&[], &ikm);
        assert_eq!(hex(&prk), "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04");
        assert_eq!(
            hex(&expand(&prk, &[], 42)),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
        );
    }
}
