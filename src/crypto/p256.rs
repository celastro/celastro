//! ECDSA verification on P-256 (secp256r1), FIPS 186-4 and SEC 1: the one
//! curve besides the 25519 pair that a CA another issuer runs is likely
//! to use. Jacobian coordinates over the big integers of `bignum`; public
//! operations only, so no constant-time obligation.

use std::cmp::Ordering;

use super::bignum::Big;
use super::sha2::sha256;

fn hexbig(s: &str) -> Big {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect();
    Big::from_be_bytes(&bytes)
}

struct Curve {
    p: Big,
    n: Big,
    b: Big,
    gx: Big,
    gy: Big,
}

fn curve() -> Curve {
    Curve {
        p: hexbig("ffffffff00000001000000000000000000000000ffffffffffffffffffffffff"),
        n: hexbig("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551"),
        b: hexbig("5ac635d8aa3a93e7b3ebbd55769886bc651d06b0cc53b0f63bce3c3e27d2604b"),
        gx: hexbig("6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"),
        gy: hexbig("4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5"),
    }
}

/// A point in Jacobian coordinates; `z == 0` is the point at infinity.
#[derive(Clone)]
struct Point {
    x: Big,
    y: Big,
    z: Big,
}

impl Curve {
    fn addm(&self, a: &Big, b: &Big) -> Big {
        a.add(b).rem(&self.p)
    }
    fn subm(&self, a: &Big, b: &Big) -> Big {
        a.add(&self.p).sub(b).rem(&self.p)
    }
    fn mulm(&self, a: &Big, b: &Big) -> Big {
        a.mulmod(b, &self.p)
    }

    fn infinity() -> Point {
        Point { x: Big::from_u32(1), y: Big::from_u32(1), z: Big::zero() }
    }

    fn double(&self, q: &Point) -> Point {
        if q.z.is_zero() {
            return q.clone();
        }
        // a = -3: M = 3(X - Z^2)(X + Z^2)
        let z2 = self.mulm(&q.z, &q.z);
        let m =
            self.mulm(&Big::from_u32(3), &self.mulm(&self.subm(&q.x, &z2), &self.addm(&q.x, &z2)));
        let y2 = self.mulm(&q.y, &q.y);
        let s = self.mulm(&Big::from_u32(4), &self.mulm(&q.x, &y2));
        let x3 = self.subm(&self.mulm(&m, &m), &self.addm(&s, &s));
        let y4 = self.mulm(&y2, &y2);
        let y3 = self.subm(&self.mulm(&m, &self.subm(&s, &x3)), &self.mulm(&Big::from_u32(8), &y4));
        let z3 = self.mulm(&Big::from_u32(2), &self.mulm(&q.y, &q.z));
        Point { x: x3, y: y3, z: z3 }
    }

    fn add(&self, a: &Point, b: &Point) -> Point {
        if a.z.is_zero() {
            return b.clone();
        }
        if b.z.is_zero() {
            return a.clone();
        }
        let z1z1 = self.mulm(&a.z, &a.z);
        let z2z2 = self.mulm(&b.z, &b.z);
        let u1 = self.mulm(&a.x, &z2z2);
        let u2 = self.mulm(&b.x, &z1z1);
        let s1 = self.mulm(&a.y, &self.mulm(&b.z, &z2z2));
        let s2 = self.mulm(&b.y, &self.mulm(&a.z, &z1z1));
        if u1 == u2 {
            if s1 == s2 {
                return self.double(a);
            }
            return Curve::infinity();
        }
        let h = self.subm(&u2, &u1);
        let r = self.subm(&s2, &s1);
        let h2 = self.mulm(&h, &h);
        let h3 = self.mulm(&h2, &h);
        let u1h2 = self.mulm(&u1, &h2);
        let x3 = self.subm(&self.subm(&self.mulm(&r, &r), &h3), &self.addm(&u1h2, &u1h2));
        let y3 = self.subm(&self.mulm(&r, &self.subm(&u1h2, &x3)), &self.mulm(&s1, &h3));
        let z3 = self.mulm(&h, &self.mulm(&a.z, &b.z));
        Point { x: x3, y: y3, z: z3 }
    }

    fn mul(&self, k: &Big, q: &Point) -> Point {
        let mut r = Curve::infinity();
        for i in (0..k.bits()).rev() {
            r = self.double(&r);
            if (k.to_be_bytes(32).expect("a scalar below n")[31 - i / 8] >> (i % 8)) & 1 == 1 {
                r = self.add(&r, q);
            }
        }
        r
    }

    fn affine_x(&self, q: &Point) -> Option<Big> {
        if q.z.is_zero() {
            return None;
        }
        let zi = q.z.invmod_prime(&self.p);
        Some(self.mulm(&q.x, &self.mulm(&zi, &zi)))
    }

    fn on_curve(&self, x: &Big, y: &Big) -> bool {
        // y^2 == x^3 - 3x + b
        let lhs = self.mulm(y, y);
        let x3 = self.mulm(&self.mulm(x, x), x);
        let rhs = self.addm(&self.subm(&x3, &self.mulm(&Big::from_u32(3), x)), &self.b);
        lhs == rhs
    }
}

/// A P-256 public key from its uncompressed encoding `04 || x || y`.
#[derive(Clone, Debug)]
pub struct PublicKey {
    x: Big,
    y: Big,
}

impl PublicKey {
    pub fn from_uncompressed(bytes: &[u8]) -> Option<PublicKey> {
        if bytes.len() != 65 || bytes[0] != 4 {
            return None;
        }
        let c = curve();
        let x = Big::from_be_bytes(&bytes[1..33]);
        let y = Big::from_be_bytes(&bytes[33..]);
        if x.cmp_big(&c.p) != Ordering::Less
            || y.cmp_big(&c.p) != Ordering::Less
            || !c.on_curve(&x, &y)
        {
            return None;
        }
        Some(PublicKey { x, y })
    }

    /// ECDSA over SHA-256 with the signature as the DER `SEQUENCE { r, s }`.
    pub fn verify_sha256_der(&self, msg: &[u8], sig_der: &[u8]) -> bool {
        let Some((r, s)) = parse_der_signature(sig_der) else { return false };
        self.verify_sha256(msg, &r, &s)
    }

    fn verify_sha256(&self, msg: &[u8], r: &Big, s: &Big) -> bool {
        let c = curve();
        let one = Big::from_u32(1);
        if r.cmp_big(&one) == Ordering::Less
            || r.cmp_big(&c.n) != Ordering::Less
            || s.cmp_big(&one) == Ordering::Less
            || s.cmp_big(&c.n) != Ordering::Less
        {
            return false;
        }
        let e = Big::from_be_bytes(&sha256(msg));
        let w = s.invmod_prime(&c.n);
        let u1 = e.mulmod(&w, &c.n);
        let u2 = r.mulmod(&w, &c.n);
        let g = Point { x: c.gx.clone(), y: c.gy.clone(), z: one.clone() };
        let q = Point { x: self.x.clone(), y: self.y.clone(), z: one };
        let x = c.add(&c.mul(&u1, &g), &c.mul(&u2, &q));
        match c.affine_x(&x) {
            Some(xa) => xa.rem(&c.n) == *r,
            None => false,
        }
    }
}

fn parse_der_signature(sig: &[u8]) -> Option<(Big, Big)> {
    use super::der;
    let (body, rest) = der::expect(sig, der::SEQUENCE).ok()?;
    if !rest.is_empty() {
        return None;
    }
    let (r, rest) = der::expect(body, der::INTEGER).ok()?;
    let (s, rest) = der::expect(rest, der::INTEGER).ok()?;
    if !rest.is_empty() {
        return None;
    }
    // DER, not BER: a signature integer is positive (no high bit without a
    // leading zero) and minimal (no leading zero before a low byte), and
    // one that is not is another encoding of the same signature, which a
    // verifier must not accept twice.
    let der_positive = |x: &[u8]| {
        !x.is_empty() && x[0] & 0x80 == 0 && !(x.len() > 1 && x[0] == 0 && x[1] & 0x80 == 0)
    };
    if !der_positive(r) || !der_positive(s) {
        return None;
    }
    Some((Big::from_be_bytes(r), Big::from_be_bytes(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generator_is_on_the_curve_and_has_order_n() {
        let c = curve();
        assert!(c.on_curve(&c.gx, &c.gy));
        let g = Point { x: c.gx.clone(), y: c.gy.clone(), z: Big::from_u32(1) };
        let ng = c.mul(&c.n, &g);
        assert!(ng.z.is_zero(), "n * G is the point at infinity");
        // 2G by doubling and by adding agree.
        let d = c.double(&g);
        let a = c.add(&g, &g);
        assert_eq!(c.affine_x(&d), c.affine_x(&a));
        // (n-1)G + G is infinity too.
        let nm1 = c.mul(&c.n.sub(&Big::from_u32(1)), &g);
        assert!(c.add(&nm1, &g).z.is_zero());
    }
}
