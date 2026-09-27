//! ECDSA verification on P-256 (secp256r1), FIPS 186-4 and SEC 1: the one
//! curve besides the 25519 pair that a CA another issuer runs is likely
//! to use. The arithmetic is `weierstrass`, shared with P-384; public
//! operations only, so no constant-time obligation.

use super::sha2::{sha256, sha384};
use super::weierstrass::{hexbig, Affine, Curve};

fn curve() -> Curve {
    Curve {
        p: hexbig("ffffffff00000001000000000000000000000000ffffffffffffffffffffffff"),
        n: hexbig("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551"),
        b: hexbig("5ac635d8aa3a93e7b3ebbd55769886bc651d06b0cc53b0f63bce3c3e27d2604b"),
        gx: hexbig("6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"),
        gy: hexbig("4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5"),
        bytes: 32,
    }
}

/// A P-256 public key from its uncompressed encoding `04 || x || y`.
#[derive(Clone, Debug)]
pub struct PublicKey(Affine);

impl PublicKey {
    pub fn from_uncompressed(bytes: &[u8]) -> Option<PublicKey> {
        curve().from_uncompressed(bytes).map(PublicKey)
    }

    /// ECDSA over SHA-256 with the signature as the DER `SEQUENCE { r, s }`.
    pub fn verify_sha256_der(&self, msg: &[u8], sig_der: &[u8]) -> bool {
        curve().verify_der(&self.0, &sha256(msg), sig_der)
    }

    /// ECDSA over SHA-384 (`ecdsa-with-SHA384` on a P-256 key: the digest's
    /// leftmost 256 bits).
    pub fn verify_sha384_der(&self, msg: &[u8], sig_der: &[u8]) -> bool {
        curve().verify_der(&self.0, &sha384(msg), sig_der)
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
