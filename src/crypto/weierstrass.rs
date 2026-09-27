//! The short Weierstrass curves with `a = -3` this build verifies ECDSA
//! on -- P-256 and P-384 (FIPS 186-4, SEC 1) -- in one arithmetic:
//! Jacobian coordinates over the big integers of `bignum`, the curve's
//! constants a value. Public operations only, so no constant-time
//! obligation; a signature's `r` and `s` are checked in range, the point
//! on the curve, the DER of the signature minimal.

use std::cmp::Ordering;

use super::bignum::Big;

pub(super) fn hexbig(s: &str) -> Big {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect();
    Big::from_be_bytes(&bytes)
}

pub(super) struct Curve {
    pub p: Big,
    pub n: Big,
    pub b: Big,
    pub gx: Big,
    pub gy: Big,
    /// Bytes of a coordinate and of a scalar: 32 for P-256, 48 for P-384.
    pub bytes: usize,
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
        let kb = k.to_be_bytes(self.bytes).expect("a scalar below n");
        for i in (0..k.bits()).rev() {
            r = self.double(&r);
            if (kb[self.bytes - 1 - i / 8] >> (i % 8)) & 1 == 1 {
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

impl Curve {
    /// Whether the generator is on the curve and `n * G` is the point at
    /// infinity, with doubling and adding in agreement: what a test of a
    /// curve's constants asks.
    #[cfg(test)]
    pub fn generator_has_order_n(&self) -> bool {
        let g = Point { x: self.gx.clone(), y: self.gy.clone(), z: Big::from_u32(1) };
        let d = self.double(&g);
        let a = self.add(&g, &g);
        let nm1 = self.mul(&self.n.sub(&Big::from_u32(1)), &g);
        self.on_curve(&self.gx, &self.gy)
            && self.mul(&self.n, &g).z.is_zero()
            && self.affine_x(&d) == self.affine_x(&a)
            && self.add(&nm1, &g).z.is_zero()
    }
}

/// A public key on one of the curves: an affine point on it.
#[derive(Clone, Debug)]
pub(super) struct Affine {
    pub x: Big,
    pub y: Big,
}

impl Curve {
    /// A point from its uncompressed encoding `04 || x || y`, on the curve.
    pub fn from_uncompressed(&self, bytes: &[u8]) -> Option<Affine> {
        if bytes.len() != 1 + 2 * self.bytes || bytes[0] != 4 {
            return None;
        }
        let x = Big::from_be_bytes(&bytes[1..1 + self.bytes]);
        let y = Big::from_be_bytes(&bytes[1 + self.bytes..]);
        if x.cmp_big(&self.p) != Ordering::Less
            || y.cmp_big(&self.p) != Ordering::Less
            || !self.on_curve(&x, &y)
        {
            return None;
        }
        Some(Affine { x, y })
    }

    /// ECDSA over a digest, the signature as the DER `SEQUENCE { r, s }`:
    /// `e` is the leftmost `bits(n)` bits of the digest (SEC 1 §4.1.4), so
    /// a wider digest is cut and a narrower one is taken whole.
    pub fn verify_der(&self, key: &Affine, digest: &[u8], sig_der: &[u8]) -> bool {
        let Some((r, s)) = parse_der_signature(sig_der) else { return false };
        let one = Big::from_u32(1);
        if r.cmp_big(&one) == Ordering::Less
            || r.cmp_big(&self.n) != Ordering::Less
            || s.cmp_big(&one) == Ordering::Less
            || s.cmp_big(&self.n) != Ordering::Less
        {
            return false;
        }
        // Both orders are a whole number of bytes (256 and 384 bits), so
        // the leftmost bits(n) bits of the digest are its leftmost bytes.
        debug_assert!(self.n.bits() % 8 == 0);
        let take = digest.len().min(self.n.bits() / 8);
        let e = Big::from_be_bytes(&digest[..take]);
        let w = s.invmod_prime(&self.n);
        let u1 = e.rem(&self.n).mulmod(&w, &self.n);
        let u2 = r.mulmod(&w, &self.n);
        let g = Point { x: self.gx.clone(), y: self.gy.clone(), z: one.clone() };
        let q = Point { x: key.x.clone(), y: key.y.clone(), z: one };
        let x = self.add(&self.mul(&u1, &g), &self.mul(&u2, &q));
        match self.affine_x(&x) {
            Some(xa) => xa.rem(&self.n) == r,
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
