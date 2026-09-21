//! ChaCha20-Poly1305, RFC 7539: the stream cipher, the one-time
//! authenticator, and the AEAD construction over them. The suite TLS 1.3
//! runs here, chosen because it is constant-time in plain integer
//! arithmetic, which AES without hardware is not.

/// One 64-byte ChaCha20 block for `key`, `counter` and `nonce`.
fn block(key: &[u8; 32], counter: u32, nonce: &[u8; 12]) -> [u8; 64] {
    let mut s = [0u32; 16];
    s[0] = 0x61707865;
    s[1] = 0x3320646e;
    s[2] = 0x79622d32;
    s[3] = 0x6b206574;
    for i in 0..8 {
        s[4 + i] = u32::from_le_bytes([key[4 * i], key[4 * i + 1], key[4 * i + 2], key[4 * i + 3]]);
    }
    s[12] = counter;
    for i in 0..3 {
        s[13 + i] = u32::from_le_bytes([
            nonce[4 * i],
            nonce[4 * i + 1],
            nonce[4 * i + 2],
            nonce[4 * i + 3],
        ]);
    }
    let init = s;
    for _ in 0..10 {
        quarter(&mut s, 0, 4, 8, 12);
        quarter(&mut s, 1, 5, 9, 13);
        quarter(&mut s, 2, 6, 10, 14);
        quarter(&mut s, 3, 7, 11, 15);
        quarter(&mut s, 0, 5, 10, 15);
        quarter(&mut s, 1, 6, 11, 12);
        quarter(&mut s, 2, 7, 8, 13);
        quarter(&mut s, 3, 4, 9, 14);
    }
    let mut out = [0u8; 64];
    for i in 0..16 {
        out[4 * i..4 * i + 4].copy_from_slice(&s[i].wrapping_add(init[i]).to_le_bytes());
    }
    out
}

fn quarter(s: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
    s[a] = s[a].wrapping_add(s[b]);
    s[d] = (s[d] ^ s[a]).rotate_left(16);
    s[c] = s[c].wrapping_add(s[d]);
    s[b] = (s[b] ^ s[c]).rotate_left(12);
    s[a] = s[a].wrapping_add(s[b]);
    s[d] = (s[d] ^ s[a]).rotate_left(8);
    s[c] = s[c].wrapping_add(s[d]);
    s[b] = (s[b] ^ s[c]).rotate_left(7);
}

/// XOR `data` with the keystream from `counter` on: encryption and
/// decryption are the same call.
pub fn chacha20(key: &[u8; 32], counter: u32, nonce: &[u8; 12], data: &mut [u8]) {
    for (i, chunk) in data.chunks_mut(64).enumerate() {
        let ks = block(key, counter.wrapping_add(i as u32), nonce);
        for (b, k) in chunk.iter_mut().zip(ks.iter()) {
            *b ^= k;
        }
    }
}

/// Poly1305 over `msg` with the 32-byte one-time `key`: r clamped, s
/// added, arithmetic in five 26-bit limbs with the reduction of 2^130 - 5.
pub fn poly1305(key: &[u8; 32], msg: &[u8]) -> [u8; 16] {
    let t0 = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
    let t1 = u32::from_le_bytes([key[4], key[5], key[6], key[7]]);
    let t2 = u32::from_le_bytes([key[8], key[9], key[10], key[11]]);
    let t3 = u32::from_le_bytes([key[12], key[13], key[14], key[15]]);
    let r0 = (t0) & 0x3ffffff;
    let r1 = ((t0 >> 26) | (t1 << 6)) & 0x3ffff03;
    let r2 = ((t1 >> 20) | (t2 << 12)) & 0x3ffc0ff;
    let r3 = ((t2 >> 14) | (t3 << 18)) & 0x3f03fff;
    let r4 = (t3 >> 8) & 0x00fffff;
    let (s1, s2, s3, s4) = (r1 * 5, r2 * 5, r3 * 5, r4 * 5);
    let (mut h0, mut h1, mut h2, mut h3, mut h4) = (0u32, 0u32, 0u32, 0u32, 0u32);
    let mut i = 0;
    while i < msg.len() {
        let take = (msg.len() - i).min(16);
        let mut m = [0u8; 17];
        m[..take].copy_from_slice(&msg[i..i + take]);
        m[take] = 1;
        i += take;
        let m0 = u32::from_le_bytes([m[0], m[1], m[2], m[3]]);
        let m1 = u32::from_le_bytes([m[4], m[5], m[6], m[7]]);
        let m2 = u32::from_le_bytes([m[8], m[9], m[10], m[11]]);
        let m3 = u32::from_le_bytes([m[12], m[13], m[14], m[15]]);
        h0 += m0 & 0x3ffffff;
        h1 += ((m0 >> 26) | (m1 << 6)) & 0x3ffffff;
        h2 += ((m1 >> 20) | (m2 << 12)) & 0x3ffffff;
        h3 += ((m2 >> 14) | (m3 << 18)) & 0x3ffffff;
        h4 += (m3 >> 8) | ((m[16] as u32) << 24);
        let d0 = h0 as u64 * r0 as u64
            + h1 as u64 * s4 as u64
            + h2 as u64 * s3 as u64
            + h3 as u64 * s2 as u64
            + h4 as u64 * s1 as u64;
        let mut d1 = h0 as u64 * r1 as u64
            + h1 as u64 * r0 as u64
            + h2 as u64 * s4 as u64
            + h3 as u64 * s3 as u64
            + h4 as u64 * s2 as u64;
        let mut d2 = h0 as u64 * r2 as u64
            + h1 as u64 * r1 as u64
            + h2 as u64 * r0 as u64
            + h3 as u64 * s4 as u64
            + h4 as u64 * s3 as u64;
        let mut d3 = h0 as u64 * r3 as u64
            + h1 as u64 * r2 as u64
            + h2 as u64 * r1 as u64
            + h3 as u64 * r0 as u64
            + h4 as u64 * s4 as u64;
        let mut d4 = h0 as u64 * r4 as u64
            + h1 as u64 * r3 as u64
            + h2 as u64 * r2 as u64
            + h3 as u64 * r1 as u64
            + h4 as u64 * r0 as u64;
        let mut c = (d0 >> 26) as u32;
        h0 = (d0 as u32) & 0x3ffffff;
        d1 += c as u64;
        c = (d1 >> 26) as u32;
        h1 = (d1 as u32) & 0x3ffffff;
        d2 += c as u64;
        c = (d2 >> 26) as u32;
        h2 = (d2 as u32) & 0x3ffffff;
        d3 += c as u64;
        c = (d3 >> 26) as u32;
        h3 = (d3 as u32) & 0x3ffffff;
        d4 += c as u64;
        c = (d4 >> 26) as u32;
        h4 = (d4 as u32) & 0x3ffffff;
        h0 += c * 5;
        c = h0 >> 26;
        h0 &= 0x3ffffff;
        h1 += c;
    }
    // Full carry, then the conditional subtraction of p = 2^130 - 5.
    let mut c = h1 >> 26;
    h1 &= 0x3ffffff;
    h2 += c;
    c = h2 >> 26;
    h2 &= 0x3ffffff;
    h3 += c;
    c = h3 >> 26;
    h3 &= 0x3ffffff;
    h4 += c;
    c = h4 >> 26;
    h4 &= 0x3ffffff;
    h0 += c * 5;
    c = h0 >> 26;
    h0 &= 0x3ffffff;
    h1 += c;
    let mut g0 = h0.wrapping_add(5);
    c = g0 >> 26;
    g0 &= 0x3ffffff;
    let mut g1 = h1.wrapping_add(c);
    c = g1 >> 26;
    g1 &= 0x3ffffff;
    let mut g2 = h2.wrapping_add(c);
    c = g2 >> 26;
    g2 &= 0x3ffffff;
    let mut g3 = h3.wrapping_add(c);
    c = g3 >> 26;
    g3 &= 0x3ffffff;
    let g4 = h4.wrapping_add(c).wrapping_sub(1 << 26);
    // If g4's top bit is clear the subtraction did not borrow: h >= p, take g.
    let mask = std::hint::black_box((g4 >> 31).wrapping_sub(1));
    let nmask = !mask;
    h0 = (h0 & nmask) | (g0 & mask);
    h1 = (h1 & nmask) | (g1 & mask);
    h2 = (h2 & nmask) | (g2 & mask);
    h3 = (h3 & nmask) | (g3 & mask);
    h4 = (h4 & nmask) | (g4 & mask);
    let f0 =
        (h0 | (h1 << 26)) as u64 + u32::from_le_bytes([key[16], key[17], key[18], key[19]]) as u64;
    let f1 = ((h1 >> 6) | (h2 << 20)) as u64
        + u32::from_le_bytes([key[20], key[21], key[22], key[23]]) as u64;
    let f2 = ((h2 >> 12) | (h3 << 14)) as u64
        + u32::from_le_bytes([key[24], key[25], key[26], key[27]]) as u64;
    let f3 = ((h3 >> 18) | (h4 << 8)) as u64
        + u32::from_le_bytes([key[28], key[29], key[30], key[31]]) as u64;
    let mut out = [0u8; 16];
    let f1 = f1 + (f0 >> 32);
    let f2 = f2 + (f1 >> 32);
    let f3 = f3 + (f2 >> 32);
    out[0..4].copy_from_slice(&(f0 as u32).to_le_bytes());
    out[4..8].copy_from_slice(&(f1 as u32).to_le_bytes());
    out[8..12].copy_from_slice(&(f2 as u32).to_le_bytes());
    out[12..16].copy_from_slice(&(f3 as u32).to_le_bytes());
    out
}

/// The tag over `aad` and `ciphertext` as RFC 7539 §2.8 lays them out.
fn tag(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
    let otk = {
        let mut z = [0u8; 32];
        chacha20(key, 0, nonce, &mut z);
        z
    };
    let mut mac_data = Vec::with_capacity(aad.len() + ciphertext.len() + 32);
    mac_data.extend_from_slice(aad);
    mac_data.resize(mac_data.len().div_ceil(16) * 16, 0);
    mac_data.extend_from_slice(ciphertext);
    mac_data.resize(mac_data.len().div_ceil(16) * 16, 0);
    mac_data.extend_from_slice(&(aad.len() as u64).to_le_bytes());
    mac_data.extend_from_slice(&(ciphertext.len() as u64).to_le_bytes());
    poly1305(&otk, &mac_data)
}

/// Encrypt `plaintext` in place and return the 16-byte tag.
pub fn seal(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], data: &mut [u8]) -> [u8; 16] {
    chacha20(key, 1, nonce, data);
    tag(key, nonce, aad, data)
}

/// Check `tag` and decrypt `data` in place; on a bad tag nothing is
/// decrypted and `false` comes back.
pub fn open(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    data: &mut [u8],
    tag_given: &[u8; 16],
) -> bool {
    let expected = tag(key, nonce, aad, data);
    if !super::ct_eq(&expected, tag_given) {
        return false;
    }
    chacha20(key, 1, nonce, data);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hex;

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn chacha20_block_and_stream_match_rfc_7539() {
        let key: [u8; 32] = core::array::from_fn(|i| i as u8);
        let nonce = [0, 0, 0, 9, 0, 0, 0, 0x4a, 0, 0, 0, 0];
        let b = block(&key, 1, &nonce);
        assert_eq!(
            hex(&b),
            "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4ed2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e"
        );
        // §2.4.2: the sunscreen plaintext under the encryption vector.
        let key: [u8; 32] = core::array::from_fn(|i| i as u8);
        let nonce = [0, 0, 0, 0, 0, 0, 0, 0x4a, 0, 0, 0, 0];
        let mut data = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.".to_vec();
        chacha20(&key, 1, &nonce, &mut data);
        assert!(hex(&data)
            .starts_with("6e2e359a2568f98041ba0728dd0d6981e97e7aec1d4360c20a27afccfd9fae0b"));
    }

    #[test]
    fn poly1305_matches_rfc_7539() {
        let key: [u8; 32] =
            unhex("85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b")
                .try_into()
                .unwrap();
        let t = poly1305(&key, b"Cryptographic Forum Research Group");
        assert_eq!(hex(&t), "a8061dc1305136c6c22b8baf0c0127a9");
    }

    #[test]
    fn the_aead_matches_rfc_7539_and_refuses_a_forged_tag() {
        let key: [u8; 32] = core::array::from_fn(|i| 0x80 + i as u8);
        let nonce = [7, 0, 0, 0, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47];
        let aad = unhex("50515253c0c1c2c3c4c5c6c7");
        let plain = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
        let mut data = plain.to_vec();
        let t = seal(&key, &nonce, &aad, &mut data);
        assert!(hex(&data).starts_with("d31a8d34648e60db7b86afbc53ef7ec2"));
        assert!(hex(&data).ends_with("3ff4def08e4b7a9de576d26586cec64b6116"));
        assert_eq!(hex(&t), "1ae10b594f09e26a7e902ecbd0600691");
        let mut forged = t;
        forged[3] ^= 1;
        let mut copy = data.clone();
        assert!(!open(&key, &nonce, &aad, &mut copy, &forged));
        assert_eq!(copy, data, "a refused tag decrypts nothing");
        assert!(open(&key, &nonce, &aad, &mut data, &t));
        assert_eq!(&data[..], &plain[..]);
    }
}
