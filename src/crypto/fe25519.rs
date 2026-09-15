//! Arithmetic in GF(2^255 - 19), five 51-bit limbs in `u64`, products in
//! `u128`. Every operation runs the same instructions whatever the values:
//! a conditional is a mask, and nothing is looked up by a secret.

const MASK51: u64 = (1 << 51) - 1;

/// A field element; limbs are kept below 2^52 after every operation.
#[derive(Clone, Copy, Debug)]
pub struct Fe(pub [u64; 5]);

impl Fe {
    pub const ZERO: Fe = Fe([0; 5]);
    pub const ONE: Fe = Fe([1, 0, 0, 0, 0]);

    /// From 32 little-endian bytes, the top bit ignored as RFC 7748 says.
    pub fn from_bytes(b: &[u8; 32]) -> Fe {
        let load = |i: usize| -> u64 {
            let mut v = [0u8; 8];
            v.copy_from_slice(&b[i..i + 8]);
            u64::from_le_bytes(v)
        };
        Fe([
            load(0) & MASK51,
            (load(6) >> 3) & MASK51,
            (load(12) >> 6) & MASK51,
            (load(19) >> 1) & MASK51,
            (load(24) >> 12) & MASK51,
        ])
    }

    /// The canonical 32 bytes: fully reduced below p, little-endian.
    pub fn to_bytes(self) -> [u8; 32] {
        let mut h = self.weak_reduce().0;
        // Add 19 and see whether it carries out of bit 255: if so h >= p and
        // subtracting p is adding 19 and dropping the carry.
        let mut q = (h[0] + 19) >> 51;
        q = (h[1] + q) >> 51;
        q = (h[2] + q) >> 51;
        q = (h[3] + q) >> 51;
        q = (h[4] + q) >> 51;
        h[0] += 19 * q;
        h[1] += h[0] >> 51;
        h[0] &= MASK51;
        h[2] += h[1] >> 51;
        h[1] &= MASK51;
        h[3] += h[2] >> 51;
        h[2] &= MASK51;
        h[4] += h[3] >> 51;
        h[3] &= MASK51;
        h[4] &= MASK51;
        let mut out = [0u8; 32];
        let words = [
            h[0] | (h[1] << 51),
            (h[1] >> 13) | (h[2] << 38),
            (h[2] >> 26) | (h[3] << 25),
            (h[3] >> 39) | (h[4] << 12),
        ];
        for (i, w) in words.iter().enumerate() {
            out[8 * i..8 * i + 8].copy_from_slice(&w.to_le_bytes());
        }
        out
    }

    fn weak_reduce(self) -> Fe {
        let mut h = self.0;
        let c = [h[0] >> 51, h[1] >> 51, h[2] >> 51, h[3] >> 51, h[4] >> 51];
        h[0] = (h[0] & MASK51) + c[4] * 19;
        h[1] = (h[1] & MASK51) + c[0];
        h[2] = (h[2] & MASK51) + c[1];
        h[3] = (h[3] & MASK51) + c[2];
        h[4] = (h[4] & MASK51) + c[3];
        Fe(h)
    }

    pub fn add(self, o: Fe) -> Fe {
        Fe([
            self.0[0] + o.0[0],
            self.0[1] + o.0[1],
            self.0[2] + o.0[2],
            self.0[3] + o.0[3],
            self.0[4] + o.0[4],
        ])
        .weak_reduce()
    }

    /// `self - o`, by adding 16p first so no limb goes negative.
    pub fn sub(self, o: Fe) -> Fe {
        Fe([
            self.0[0] + 36028797018963664 - o.0[0],
            self.0[1] + 36028797018963952 - o.0[1],
            self.0[2] + 36028797018963952 - o.0[2],
            self.0[3] + 36028797018963952 - o.0[3],
            self.0[4] + 36028797018963952 - o.0[4],
        ])
        .weak_reduce()
    }

    pub fn neg(self) -> Fe {
        Fe::ZERO.sub(self)
    }

    pub fn mul(self, o: Fe) -> Fe {
        let a = self.0;
        let b = o.0;
        let m = |x: u64, y: u64| x as u128 * y as u128;
        let b1 = b[1] * 19;
        let b2 = b[2] * 19;
        let b3 = b[3] * 19;
        let b4 = b[4] * 19;
        let c0 = m(a[0], b[0]) + m(a[4], b1) + m(a[3], b2) + m(a[2], b3) + m(a[1], b4);
        let mut c1 = m(a[1], b[0]) + m(a[0], b[1]) + m(a[4], b2) + m(a[3], b3) + m(a[2], b4);
        let mut c2 = m(a[2], b[0]) + m(a[1], b[1]) + m(a[0], b[2]) + m(a[4], b3) + m(a[3], b4);
        let mut c3 = m(a[3], b[0]) + m(a[2], b[1]) + m(a[1], b[2]) + m(a[0], b[3]) + m(a[4], b4);
        let mut c4 = m(a[4], b[0]) + m(a[3], b[1]) + m(a[2], b[2]) + m(a[1], b[3]) + m(a[0], b[4]);
        let mask = MASK51 as u128;
        c1 += c0 >> 51;
        let mut h0 = (c0 & mask) as u64;
        c2 += c1 >> 51;
        let mut h1 = (c1 & mask) as u64;
        c3 += c2 >> 51;
        let h2 = (c2 & mask) as u64;
        c4 += c3 >> 51;
        let h3 = (c3 & mask) as u64;
        let carry = (c4 >> 51) as u64;
        let h4 = (c4 & mask) as u64;
        h0 += carry * 19;
        h1 += h0 >> 51;
        h0 &= MASK51;
        Fe([h0, h1, h2, h3, h4])
    }

    pub fn square(self) -> Fe {
        self.mul(self)
    }

    /// `self^(2^n)` by repeated squaring.
    fn pow2k(self, k: u32) -> Fe {
        let mut r = self;
        for _ in 0..k {
            r = r.square();
        }
        r
    }

    /// `(self^(2^250 - 1), self^11)`: the common prefix of the inversion and
    /// the square-root exponentiations, addition chain as in the reference.
    fn pow22501(self) -> (Fe, Fe) {
        let t0 = self.square();
        let t1 = t0.pow2k(2);
        let t2 = self.mul(t1);
        let t3 = t0.mul(t2);
        let t4 = t3.square();
        let t5 = t2.mul(t4);
        let t6 = t5.pow2k(5);
        let t7 = t6.mul(t5);
        let t8 = t7.pow2k(10);
        let t9 = t8.mul(t7);
        let t10 = t9.pow2k(20);
        let t11 = t10.mul(t9);
        let t12 = t11.pow2k(10);
        let t13 = t12.mul(t7);
        let t14 = t13.pow2k(50);
        let t15 = t14.mul(t13);
        let t16 = t15.pow2k(100);
        let t17 = t16.mul(t15);
        let t18 = t17.pow2k(50);
        let t19 = t18.mul(t13);
        (t19, t3)
    }

    /// `self^-1`, as `self^(p-2)`; zero inverts to zero.
    pub fn invert(self) -> Fe {
        let (t19, t3) = self.pow22501();
        let t20 = t19.pow2k(5);
        t20.mul(t3)
    }

    /// `self^((p-5)/8)`, the exponent the square root needs.
    pub fn pow_p58(self) -> Fe {
        let (t19, _) = self.pow22501();
        let t20 = t19.pow2k(2);
        t20.mul(self)
    }

    /// Whether the canonical encoding is odd.
    pub fn is_negative(self) -> bool {
        self.to_bytes()[0] & 1 == 1
    }

    pub fn is_zero(self) -> bool {
        super::ct_eq(&self.to_bytes(), &[0u8; 32])
    }

    /// The same canonical value, decided without a branch on the bytes.
    pub fn ct_eq(self, o: Fe) -> bool {
        super::ct_eq(&self.to_bytes(), &o.to_bytes())
    }

    /// Swap `a` and `b` when `bit` is 1, by masks.
    pub fn cswap(a: &mut Fe, b: &mut Fe, bit: u64) {
        let mask = 0u64.wrapping_sub(bit);
        for i in 0..5 {
            let t = mask & (a.0[i] ^ b.0[i]);
            a.0[i] ^= t;
            b.0[i] ^= t;
        }
    }

    /// `if bit { b } else { a }`, by masks.
    pub fn select(a: Fe, b: Fe, bit: u64) -> Fe {
        let mask = 0u64.wrapping_sub(bit);
        let mut out = [0u64; 5];
        for i in 0..5 {
            out[i] = a.0[i] ^ (mask & (a.0[i] ^ b.0[i]));
        }
        Fe(out)
    }

    /// A small constant.
    pub fn from_u64(v: u64) -> Fe {
        Fe([v & MASK51, v >> 51, 0, 0, 0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_field_round_trips_inverts_and_reduces_canonically() {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(11);
        }
        bytes[31] &= 0x7f;
        let a = Fe::from_bytes(&bytes);
        assert_eq!(a.to_bytes(), bytes);
        assert!(a.mul(a.invert()).ct_eq(Fe::ONE));
        // p and p + 1 reduce to 0 and 1.
        let mut p = [0xffu8; 32];
        p[0] = 0xed;
        p[31] = 0x7f;
        assert!(Fe::from_bytes(&p).is_zero());
        let mut p1 = p;
        p1[0] = 0xee;
        assert!(Fe::from_bytes(&p1).ct_eq(Fe::ONE));
        // (p - 1) + 1 wraps to zero.
        let mut pm1 = p;
        pm1[0] = 0xec;
        assert!(Fe::from_bytes(&pm1).add(Fe::ONE).is_zero());
        assert!(a.sub(a).is_zero(), "a - a");
        assert!(a.neg().add(a).is_zero(), "-a + a");
        assert!(Fe::from_u64(7).sub(Fe::from_u64(9)).add(Fe::from_u64(2)).is_zero(), "7 - 9 + 2");
        let two_inv = Fe::from_u64(2).invert();
        assert!(two_inv.add(two_inv).ct_eq(Fe::ONE), "1/2 + 1/2");
        // p - 1 is a square root of -1's square: sqrt(-1)^2 == -1.
        let i = Fe::from_u64(2).pow_p58().square().mul(Fe::from_u64(2));
        assert!(i.square().ct_eq(Fe::ONE.neg()), "sqrt(-1)^2 = -1: {:?}", i.to_bytes());
        assert!(Fe::from_u64(3).is_negative() && !Fe::from_u64(4).is_negative());
        let (mut x, mut y) = (a, Fe::ONE);
        Fe::cswap(&mut x, &mut y, 1);
        assert!(x.ct_eq(Fe::ONE) && y.ct_eq(a));
        Fe::cswap(&mut x, &mut y, 0);
        assert!(x.ct_eq(Fe::ONE) && y.ct_eq(a));
    }
}
