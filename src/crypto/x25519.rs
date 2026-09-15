//! X25519, RFC 7748: the Montgomery ladder over the u-coordinate, every
//! step the same work, the swap a mask of the scalar bit.

use super::fe25519::Fe;

/// `scalar * u`, both 32 bytes as the RFC lays them out.
pub fn x25519(scalar: &[u8; 32], u: &[u8; 32]) -> [u8; 32] {
    let mut k = *scalar;
    k[0] &= 248;
    k[31] &= 127;
    k[31] |= 64;
    let x1 = Fe::from_bytes(u);
    let mut x2 = Fe::ONE;
    let mut z2 = Fe::ZERO;
    let mut x3 = x1;
    let mut z3 = Fe::ONE;
    let a24 = Fe::from_u64(121665);
    let mut swap = 0u64;
    for t in (0..255).rev() {
        let kt = ((k[t / 8] >> (t % 8)) & 1) as u64;
        swap ^= kt;
        Fe::cswap(&mut x2, &mut x3, swap);
        Fe::cswap(&mut z2, &mut z3, swap);
        swap = kt;
        let a = x2.add(z2);
        let aa = a.square();
        let b = x2.sub(z2);
        let bb = b.square();
        let e = aa.sub(bb);
        let c = x3.add(z3);
        let d = x3.sub(z3);
        let da = d.mul(a);
        let cb = c.mul(b);
        x3 = da.add(cb).square();
        z3 = x1.mul(da.sub(cb).square());
        x2 = aa.mul(bb);
        z2 = e.mul(aa.add(a24.mul(e)));
    }
    Fe::cswap(&mut x2, &mut x3, swap);
    Fe::cswap(&mut z2, &mut z3, swap);
    x2.mul(z2.invert()).to_bytes()
}

/// The public key of `secret`: the scalar times the base point u = 9.
pub fn public_key(secret: &[u8; 32]) -> [u8; 32] {
    let mut base = [0u8; 32];
    base[0] = 9;
    x25519(secret, &base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objstore::hex;

    fn unhex(s: &str) -> [u8; 32] {
        let v: Vec<u8> =
            (0..64).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect();
        v.try_into().unwrap()
    }

    #[test]
    fn x25519_matches_rfc_7748() {
        let alice = unhex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let bob = unhex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        let alice_pub = public_key(&alice);
        let bob_pub = public_key(&bob);
        let mut nine = [0u8; 32];
        nine[0] = 9;
        let checks = [
            (
                "alice public",
                hex(&alice_pub),
                "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a",
            ),
            (
                "bob public",
                hex(&bob_pub),
                "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f",
            ),
            (
                "shared, alice",
                hex(&x25519(&alice, &bob_pub)),
                "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742",
            ),
            (
                "shared, bob",
                hex(&x25519(&bob, &alice_pub)),
                "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742",
            ),
            (
                "one iteration from 9",
                hex(&x25519(&nine, &nine)),
                "422c8e7a6227d7bca1350b3e2bb7279f7897b87bb6854b783c60e80311ae3079",
            ),
        ];
        let wrong: Vec<String> = checks
            .iter()
            .filter(|(_, got, want)| got != want)
            .map(|(name, got, want)| format!("{name}: got {got}, want {want}"))
            .collect();
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
