//! Ed25519, RFC 8032: signatures over the twisted Edwards curve, points in
//! extended coordinates with the complete addition law, so one formula
//! serves every case and a scalar multiplication is a fixed sequence of
//! doublings and masked additions.

use super::fe25519::Fe;
use super::sc25519;
use super::sha2::sha512;
use crate::cipher::wipe;

/// A point in extended coordinates `(X : Y : Z : T)` with `x = X/Z`,
/// `y = Y/Z`, `xy = T/Z`.
#[derive(Clone, Copy)]
struct Point {
    x: Fe,
    y: Fe,
    z: Fe,
    t: Fe,
}

fn d() -> Fe {
    // -121665 / 121666
    Fe::from_u64(121665).neg().mul(Fe::from_u64(121666).invert())
}

fn sqrt_m1() -> Fe {
    // 2^((p-1)/4) = 2^(2 * (p-5)/8 + 1)
    Fe::from_u64(2).pow_p58().square().mul(Fe::from_u64(2))
}

fn base() -> Point {
    // y = 4/5, x positive.
    let y = Fe::from_u64(4).mul(Fe::from_u64(5).invert());
    decode(&{
        let mut b = y.to_bytes();
        b[31] &= 0x7f;
        b
    })
    .expect("the base point decodes")
}

impl Point {
    const IDENTITY: Point = Point { x: Fe::ZERO, y: Fe::ONE, z: Fe::ONE, t: Fe::ZERO };

    /// The complete addition law for a = -1, `k = 2d`.
    fn add(self, o: Point) -> Point {
        let a = self.y.sub(self.x).mul(o.y.sub(o.x));
        let b = self.y.add(self.x).mul(o.y.add(o.x));
        let c = self.t.mul(o.t).mul(d().add(d()));
        let dd = self.z.mul(o.z);
        let dd = dd.add(dd);
        let e = b.sub(a);
        let f = dd.sub(c);
        let g = dd.add(c);
        let h = b.add(a);
        Point { x: e.mul(f), y: g.mul(h), t: e.mul(h), z: f.mul(g) }
    }

    fn double(self) -> Point {
        self.add(self)
    }

    fn neg(self) -> Point {
        Point { x: self.x.neg(), y: self.y, z: self.z, t: self.t.neg() }
    }

    fn select(a: Point, b: Point, bit: u64) -> Point {
        Point {
            x: Fe::select(a.x, b.x, bit),
            y: Fe::select(a.y, b.y, bit),
            z: Fe::select(a.z, b.z, bit),
            t: Fe::select(a.t, b.t, bit),
        }
    }

    /// `scalar * self`, the scalar's 256 bits each costing a doubling and
    /// a masked addition, whatever they are.
    fn mul(self, scalar: &[u8; 32]) -> Point {
        let mut q = Point::IDENTITY;
        for i in (0..256).rev() {
            q = q.double();
            let bit = ((scalar[i / 8] >> (i % 8)) & 1) as u64;
            let sum = q.add(self);
            q = Point::select(q, sum, bit);
        }
        q
    }

    fn encode(self) -> [u8; 32] {
        let zi = self.z.invert();
        let x = self.x.mul(zi);
        let y = self.y.mul(zi);
        let mut out = y.to_bytes();
        out[31] ^= (x.is_negative() as u8) << 7;
        out
    }
}

/// A point from its encoding; `None` when the bytes name no point.
fn decode(b: &[u8; 32]) -> Option<Point> {
    let sign = b[31] >> 7;
    let mut yb = *b;
    yb[31] &= 0x7f;
    let y = Fe::from_bytes(&yb);
    // x^2 = (y^2 - 1) / (d y^2 + 1)
    let y2 = y.square();
    let u = y2.sub(Fe::ONE);
    let v = d().mul(y2).add(Fe::ONE);
    let v3 = v.square().mul(v);
    let v7 = v3.square().mul(v);
    let mut x = u.mul(v3).mul(u.mul(v7).pow_p58());
    let vx2 = v.mul(x.square());
    if vx2.ct_eq(u.neg()) {
        x = x.mul(sqrt_m1());
    } else if !vx2.ct_eq(u) {
        return None;
    }
    if x.is_zero() && sign == 1 {
        return None;
    }
    if x.is_negative() as u8 != sign {
        x = x.neg();
    }
    Some(Point { x, y, z: Fe::ONE, t: x.mul(y) })
}

/// The expanded secret: the clamped scalar and the nonce prefix.
/// The expanded secret: the clamped scalar and the nonce prefix. Both are
/// the seed's, and every copy made on the way to them and from them is
/// wiped: the scalar signs, the prefix makes every nonce, and a nonce
/// recovered from a core dump gives the scalar back from any signature.
fn expand(seed: &[u8; 32]) -> ([u8; 32], [u8; 32]) {
    let mut h = sha512(seed);
    let mut a = [0u8; 32];
    a.copy_from_slice(&h[..32]);
    a[0] &= 248;
    a[31] &= 63;
    a[31] |= 64;
    let mut prefix = [0u8; 32];
    prefix.copy_from_slice(&h[32..]);
    wipe(&mut h);
    (a, prefix)
}

/// The public key of a 32-byte seed.
pub fn public_key(seed: &[u8; 32]) -> [u8; 32] {
    let (mut a, mut prefix) = expand(seed);
    let pk = base().mul(&a).encode();
    wipe(&mut a);
    wipe(&mut prefix);
    pk
}

/// A signature over `msg` by `seed`.
pub fn sign(seed: &[u8; 32], msg: &[u8]) -> [u8; 64] {
    let (mut a, mut prefix) = expand(seed);
    let pk = base().mul(&a).encode();
    let mut rm = Vec::with_capacity(32 + msg.len());
    rm.extend_from_slice(&prefix);
    rm.extend_from_slice(msg);
    let mut rh = sha512(&rm);
    wipe(&mut rm);
    let mut r = sc25519::reduce_512(&rh);
    wipe(&mut rh);
    let mut r_bytes = sc25519::to_bytes(&r);
    let rb = base().mul(&r_bytes).encode();
    wipe(&mut r_bytes);
    let mut km = Vec::with_capacity(64 + msg.len());
    km.extend_from_slice(&rb);
    km.extend_from_slice(&pk);
    km.extend_from_slice(msg);
    let k = sc25519::reduce_512(&sha512(&km));
    let mut a_scalar = sc25519::reduce_256(&a);
    let s = sc25519::muladd(&k, &a_scalar, &r);
    sc25519::wipe(&mut a_scalar);
    sc25519::wipe(&mut r);
    wipe(&mut a);
    wipe(&mut prefix);
    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(&rb);
    sig[32..].copy_from_slice(&sc25519::to_bytes(&s));
    sig
}

/// Whether `sig` is `pk`'s signature over `msg`. A non-canonical `S`, an
/// encoding that names no point, or a mismatch all answer `false`.
pub fn verify(pk: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool {
    let mut rb = [0u8; 32];
    rb.copy_from_slice(&sig[..32]);
    let mut sb = [0u8; 32];
    sb.copy_from_slice(&sig[32..]);
    if !sc25519::is_canonical(&sb) {
        return false;
    }
    let (Some(a), Some(r)) = (decode(pk), decode(&rb)) else { return false };
    let mut km = Vec::with_capacity(64 + msg.len());
    km.extend_from_slice(&rb);
    km.extend_from_slice(pk);
    km.extend_from_slice(msg);
    let k = sc25519::reduce_512(&sha512(&km));
    // [S]B == R + [k]A  <=>  [S]B + [k](-A) == R
    let lhs = base().mul(&sb).add(a.neg().mul(&sc25519::to_bytes(&k)));
    super::ct_eq(&lhs.encode(), &r.encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hex;

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn the_curve_constants_and_the_base_point_are_the_published_ones() {
        assert_eq!(
            hex(&d().to_bytes()),
            "a3785913ca4deb75abd841414d0a700098e879777940c78c73fe6f2bee6c0352"
        );
        let b = base();
        let zi = b.z.invert();
        assert_eq!(
            hex(&b.x.mul(zi).to_bytes()),
            "1ad5258f602d56c9b2a7259560c72c695cdcd6fd31e2a4c0fe536ecdd3366921"
        );
        assert_eq!(
            hex(&b.y.mul(zi).to_bytes()),
            "5866666666666666666666666666666666666666666666666666666666666666"
        );
        assert!(sqrt_m1().square().ct_eq(Fe::ONE.neg()));
        // Decoding the base point's encoding gives it back.
        let again = decode(&b.encode()).unwrap();
        assert_eq!(hex(&again.encode()), hex(&b.encode()));
        // 2B, by add and by double, agree; B + (-B) is the identity.
        assert_eq!(hex(&b.add(b).encode()), hex(&b.double().encode()));
        assert_eq!(hex(&b.add(b.neg()).encode()), hex(&Point::IDENTITY.encode()));
    }

    #[test]
    fn ed25519_matches_rfc_8032_and_refuses_a_changed_message() {
        let seed: [u8; 32] =
            unhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
                .try_into()
                .unwrap();
        let pk = public_key(&seed);
        assert_eq!(hex(&pk), "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
        let sig = sign(&seed, b"");
        assert_eq!(
            hex(&sig),
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        );
        assert!(verify(&pk, b"", &sig));
        assert!(!verify(&pk, b"x", &sig));
        let seed2: [u8; 32] =
            unhex("4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")
                .try_into()
                .unwrap();
        let pk2 = public_key(&seed2);
        assert_eq!(hex(&pk2), "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c");
        let sig2 = sign(&seed2, &[0x72]);
        assert_eq!(
            hex(&sig2),
            "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
        );
        assert!(verify(&pk2, &[0x72], &sig2));
        assert!(!verify(&pk, &[0x72], &sig2), "another key's signature");
        let mut bad = sig2;
        bad[10] ^= 1;
        assert!(!verify(&pk2, &[0x72], &bad));
        // A non-canonical S is refused, not reduced.
        let mut big = sig2;
        big[63] |= 0x10;
        assert!(!verify(&pk2, &[0x72], &big));
    }
}
