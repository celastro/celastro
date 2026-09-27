//! ECDSA verification on P-384 (secp384r1), FIPS 186-4 and SEC 1: what
//! the ECDSA chains of the public issuers use (a Let's Encrypt chain under
//! ISRG Root X2, Google Trust Services R4, DigiCert's G3 ECC roots), so
//! the archive's `https://` endpoint behind one is reached. The arithmetic
//! is `weierstrass`, shared with P-256; public operations only.

use super::sha2::{sha256, sha384};
use super::weierstrass::{hexbig, Affine, Curve};

fn curve() -> Curve {
    Curve {
        p: hexbig("fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffeffffffff0000000000000000ffffffff"),
        n: hexbig("ffffffffffffffffffffffffffffffffffffffffffffffffc7634d81f4372ddf581a0db248b0a77aecec196accc52973"),
        b: hexbig("b3312fa7e23ee7e4988e056be3f82d19181d9c6efe8141120314088f5013875ac656398d8a2ed19d2a85c8edd3ec2aef"),
        gx: hexbig("aa87ca22be8b05378eb1c71ef320ad746e1d3b628ba79b9859f741e082542a385502f25dbf55296c3a545e3872760ab7"),
        gy: hexbig("3617de4a96262c6f5d9e98bf9292dc29f8f41dbd289a147ce9da3113b5f0b8c00a60b1ce1d7e819d7a431d7c90ea0e5f"),
        bytes: 48,
    }
}

/// A P-384 public key from its uncompressed encoding `04 || x || y`.
#[derive(Clone, Debug)]
pub struct PublicKey(Affine);

impl PublicKey {
    pub fn from_uncompressed(bytes: &[u8]) -> Option<PublicKey> {
        curve().from_uncompressed(bytes).map(PublicKey)
    }

    /// ECDSA over SHA-384 with the signature as the DER `SEQUENCE { r, s }`.
    pub fn verify_sha384_der(&self, msg: &[u8], sig_der: &[u8]) -> bool {
        curve().verify_der(&self.0, &sha384(msg), sig_der)
    }

    /// ECDSA over SHA-256 (`ecdsa-with-SHA256` on a P-384 key: the whole
    /// digest, narrower than the order).
    pub fn verify_sha256_der(&self, msg: &[u8], sig_der: &[u8]) -> bool {
        curve().verify_der(&self.0, &sha256(msg), sig_der)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generator_is_on_the_curve_and_has_order_n() {
        assert!(curve().generator_has_order_n(), "n * G is the point at infinity");
    }
}
