//! RSA signature verification, RFC 8017: PKCS#1 v1.5 with SHA-256 (what an
//! X.509 chain another issuer signed carries) and RSA-PSS with SHA-256
//! (what TLS 1.3 requires of an RSA server's CertificateVerify). Public
//! keys and public operations only; this crate signs with Ed25519.

use super::bignum::Big;
use super::sha2::sha256;

/// An RSA public key: the modulus and the exponent.
#[derive(Clone, Debug)]
pub struct PublicKey {
    pub n: Big,
    pub e: Big,
}

impl PublicKey {
    fn modulus_len(&self) -> usize {
        self.n.bits().div_ceil(8)
    }

    /// `sig^e mod n` as `modulus_len` bytes, or `None` for a signature out
    /// of range.
    fn encoded(&self, sig: &[u8]) -> Option<Vec<u8>> {
        let s = Big::from_be_bytes(sig);
        if s.cmp_big(&self.n) != std::cmp::Ordering::Less || sig.len() != self.modulus_len() {
            return None;
        }
        s.powmod(&self.e, &self.n).to_be_bytes(self.modulus_len())
    }

    /// PKCS#1 v1.5 over SHA-256: the encoding is `00 01 ff..ff 00 DigestInfo`.
    pub fn verify_pkcs1_sha256(&self, msg: &[u8], sig: &[u8]) -> bool {
        const PREFIX: [u8; 19] = [
            0x30, 0x31, 0x30, 0x0d, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02,
            0x01, 0x05, 0x00, 0x04, 0x20,
        ];
        let Some(em) = self.encoded(sig) else { return false };
        let k = em.len();
        if k < 3 + 8 + PREFIX.len() + 32 || em[0] != 0 || em[1] != 1 {
            return false;
        }
        let t_len = PREFIX.len() + 32;
        let ps_end = k - t_len - 1;
        if em[ps_end] != 0 || ps_end < 10 || em[2..ps_end].iter().any(|&b| b != 0xff) {
            return false;
        }
        let mut expected = Vec::with_capacity(t_len);
        expected.extend_from_slice(&PREFIX);
        expected.extend_from_slice(&sha256(msg));
        super::ct_eq(&em[ps_end + 1..], &expected)
    }

    /// RSA-PSS over SHA-256 with a 32-byte salt and MGF1-SHA256, as
    /// `rsa_pss_rsae_sha256` in TLS 1.3 is defined.
    pub fn verify_pss_sha256(&self, msg: &[u8], sig: &[u8]) -> bool {
        let Some(em) = self.encoded(sig) else { return false };
        let em_bits = self.n.bits() - 1;
        let em_len = em_bits.div_ceil(8);
        // The encoding is emLen bytes; a modulus one bit past a byte
        // boundary leaves a leading zero to drop.
        let em = &em[em.len() - em_len..];
        let (h_len, s_len) = (32, 32);
        if em_len < h_len + s_len + 2 || em[em_len - 1] != 0xbc {
            return false;
        }
        let db_len = em_len - h_len - 1;
        let (masked_db, h) = em.split_at(db_len);
        let h = &h[..h_len];
        let top_bits = 8 * em_len - em_bits;
        if top_bits > 0 && masked_db[0] >> (8 - top_bits) != 0 {
            return false;
        }
        let mask = mgf1(h, db_len);
        let mut db: Vec<u8> = masked_db.iter().zip(&mask).map(|(a, b)| a ^ b).collect();
        if top_bits > 0 {
            db[0] &= 0xff >> top_bits;
        }
        let ps_len = db_len - s_len - 1;
        if db[..ps_len].iter().any(|&b| b != 0) || db[ps_len] != 1 {
            return false;
        }
        let salt = &db[ps_len + 1..];
        let mut m_prime = vec![0u8; 8];
        m_prime.extend_from_slice(&sha256(msg));
        m_prime.extend_from_slice(salt);
        super::ct_eq(&sha256(&m_prime), h)
    }
}

fn mgf1(seed: &[u8], len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 32);
    let mut counter = 0u32;
    while out.len() < len {
        let mut input = seed.to_vec();
        input.extend_from_slice(&counter.to_be_bytes());
        out.extend_from_slice(&sha256(&input));
        counter += 1;
    }
    out.truncate(len);
    out
}
