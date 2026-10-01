//! Unsigned big integers, for the public-key operations of RSA, P-256 and
//! P-384 verification: little-endian `u32` limbs, schoolbook multiplication,
//! a shift-and-subtract remainder for the odd division and a Barrett
//! [`Reducer`] for the many reductions a curve needs. Slow by the standards
//! of the field and fast enough for a signature check per connection, and
//! -- being applied to public values only -- under no obligation to run in
//! constant time.

use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Big(Vec<u32>);

impl Big {
    pub fn zero() -> Big {
        Big(Vec::new())
    }

    pub fn from_u32(v: u32) -> Big {
        let mut b = Big(vec![v]);
        b.trim();
        b
    }

    pub fn from_be_bytes(bytes: &[u8]) -> Big {
        let mut limbs = Vec::with_capacity(bytes.len() / 4 + 1);
        let mut i = bytes.len();
        while i > 0 {
            let start = i.saturating_sub(4);
            let mut v = 0u32;
            for &b in &bytes[start..i] {
                v = (v << 8) | b as u32;
            }
            limbs.push(v);
            i = start;
        }
        let mut b = Big(limbs);
        b.trim();
        b
    }

    /// Big-endian, left-padded to `len` bytes; `None` if it does not fit.
    pub fn to_be_bytes(&self, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0u8; len];
        let mut pos = len;
        for limb in &self.0 {
            for k in 0..4 {
                let byte = ((limb >> (8 * k)) & 0xff) as u8;
                if pos == 0 {
                    if byte != 0 {
                        return None;
                    }
                    continue;
                }
                pos -= 1;
                out[pos] = byte;
            }
        }
        Some(out)
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    pub fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    pub fn bits(&self) -> usize {
        match self.0.last() {
            None => 0,
            Some(&top) => 32 * (self.0.len() - 1) + (32 - top.leading_zeros() as usize),
        }
    }

    fn bit(&self, i: usize) -> bool {
        self.0.get(i / 32).is_some_and(|l| (l >> (i % 32)) & 1 == 1)
    }

    pub fn cmp_big(&self, o: &Big) -> Ordering {
        match self.0.len().cmp(&o.0.len()) {
            Ordering::Equal => {}
            other => return other,
        }
        for i in (0..self.0.len()).rev() {
            match self.0[i].cmp(&o.0[i]) {
                Ordering::Equal => {}
                other => return other,
            }
        }
        Ordering::Equal
    }

    pub fn add(&self, o: &Big) -> Big {
        let n = self.0.len().max(o.0.len());
        let mut out = Vec::with_capacity(n + 1);
        let mut carry = 0u64;
        for i in 0..n {
            let s = *self.0.get(i).unwrap_or(&0) as u64 + *o.0.get(i).unwrap_or(&0) as u64 + carry;
            out.push(s as u32);
            carry = s >> 32;
        }
        if carry > 0 {
            out.push(carry as u32);
        }
        let mut b = Big(out);
        b.trim();
        b
    }

    /// `self - o`, which must not be negative.
    pub fn sub(&self, o: &Big) -> Big {
        debug_assert!(self.cmp_big(o) != Ordering::Less);
        let mut out = Vec::with_capacity(self.0.len());
        let mut borrow = 0i64;
        for i in 0..self.0.len() {
            let d = self.0[i] as i64 - *o.0.get(i).unwrap_or(&0) as i64 - borrow;
            if d < 0 {
                out.push((d + (1 << 32)) as u32);
                borrow = 1;
            } else {
                out.push(d as u32);
                borrow = 0;
            }
        }
        let mut b = Big(out);
        b.trim();
        b
    }

    /// `self - o` in place, no allocation; `self` must not be less.
    pub fn sub_assign(&mut self, o: &Big) {
        debug_assert!(self.cmp_big(o) != Ordering::Less);
        let mut borrow = 0i64;
        for i in 0..self.0.len() {
            let d = self.0[i] as i64 - *o.0.get(i).unwrap_or(&0) as i64 - borrow;
            if d < 0 {
                self.0[i] = (d + (1 << 32)) as u32;
                borrow = 1;
            } else {
                self.0[i] = d as u32;
                borrow = 0;
            }
        }
        self.trim();
    }

    /// `self / b^n` for the limb base `b = 2^32`: the limbs from `n` on.
    fn shr_limbs(&self, n: usize) -> Big {
        if n >= self.0.len() {
            return Big::zero();
        }
        let mut b = Big(self.0[n..].to_vec());
        b.trim();
        b
    }

    /// `self mod b^n`: the low `n` limbs.
    fn low_limbs(&self, n: usize) -> Big {
        let mut b = Big(self.0[..n.min(self.0.len())].to_vec());
        b.trim();
        b
    }

    /// `b^n`, one past the top of an `n`-limb number.
    fn one_shl_limbs(n: usize) -> Big {
        let mut limbs = vec![0u32; n + 1];
        limbs[n] = 1;
        Big(limbs)
    }

    /// `self / m`, shift-and-subtract: what a [`Reducer`] needs once.
    fn div_slow(&self, m: &Big) -> Big {
        assert!(!m.is_zero(), "division by zero");
        let mut q = vec![0u32; self.0.len().max(1)];
        let mut r = Big::zero();
        for i in (0..self.bits()).rev() {
            r.shl1_add_bit(self.bit(i));
            if r.cmp_big(m) != Ordering::Less {
                r.sub_assign(m);
                q[i / 32] |= 1 << (i % 32);
            }
        }
        let mut b = Big(q);
        b.trim();
        b
    }

    pub fn mul(&self, o: &Big) -> Big {
        if self.is_zero() || o.is_zero() {
            return Big::zero();
        }
        let mut out = vec![0u32; self.0.len() + o.0.len()];
        for (i, &a) in self.0.iter().enumerate() {
            let mut carry = 0u64;
            for (j, &b) in o.0.iter().enumerate() {
                let t = out[i + j] as u64 + a as u64 * b as u64 + carry;
                out[i + j] = t as u32;
                carry = t >> 32;
            }
            let mut k = i + o.0.len();
            while carry > 0 {
                let t = out[k] as u64 + carry;
                out[k] = t as u32;
                carry = t >> 32;
                k += 1;
            }
        }
        let mut b = Big(out);
        b.trim();
        b
    }

    fn shl1_add_bit(&mut self, bit: bool) {
        let mut carry = bit as u32;
        for limb in self.0.iter_mut() {
            let next = *limb >> 31;
            *limb = (*limb << 1) | carry;
            carry = next;
        }
        if carry > 0 {
            self.0.push(carry);
        }
    }

    /// `self mod m`, `m` not zero.
    pub fn rem(&self, m: &Big) -> Big {
        assert!(!m.is_zero(), "division by zero");
        if self.cmp_big(m) == Ordering::Less {
            return self.clone();
        }
        let mut r = Big::zero();
        for i in (0..self.bits()).rev() {
            r.shl1_add_bit(self.bit(i));
            if r.cmp_big(m) != Ordering::Less {
                r = r.sub(m);
            }
        }
        r
    }

    pub fn mulmod(&self, o: &Big, m: &Big) -> Big {
        self.mul(o).rem(m)
    }
}

/// Reduction modulo one fixed `m` by Barrett (HAC 14.42): a division at
/// construction, then two multiplications and at most two subtractions per
/// reduction of a product -- what makes a P-384 verification milliseconds
/// of work rather than the second that bit-serial reduction of every
/// product cost, which a peer could spend of this node per handshake
/// (0.97.0). Public values only, so variable time is fine.
pub struct Reducer {
    m: Big,
    mu: Big,
    k: usize,
}

impl Reducer {
    pub fn new(m: &Big) -> Reducer {
        assert!(!m.is_zero(), "a modulus of zero");
        let k = m.0.len();
        let mu = Big::one_shl_limbs(2 * k).div_slow(m);
        Reducer { m: m.clone(), mu, k }
    }

    /// `x mod m` for any `x` below `m^2` by Barrett; beyond that, the slow
    /// way, which is never the product of two reduced values.
    pub fn reduce(&self, x: &Big) -> Big {
        if x.cmp_big(&self.m) == Ordering::Less {
            return x.clone();
        }
        if x.0.len() > 2 * self.k {
            return x.rem(&self.m);
        }
        let q1 = x.shr_limbs(self.k - 1);
        let q2 = q1.mul(&self.mu);
        let q3 = q2.shr_limbs(self.k + 1);
        let r1 = x.low_limbs(self.k + 1);
        let r2 = q3.mul(&self.m).low_limbs(self.k + 1);
        let mut r = if r1.cmp_big(&r2) != Ordering::Less {
            r1.sub(&r2)
        } else {
            r1.add(&Big::one_shl_limbs(self.k + 1)).sub(&r2)
        };
        while r.cmp_big(&self.m) != Ordering::Less {
            r.sub_assign(&self.m);
        }
        r
    }

    pub fn mulmod(&self, a: &Big, b: &Big) -> Big {
        self.reduce(&a.mul(b))
    }

    /// `base^e mod m`, square-and-multiply.
    pub fn powmod(&self, base: &Big, e: &Big) -> Big {
        let mut result = self.reduce(&Big::from_u32(1));
        let base = self.reduce(base);
        for i in (0..e.bits()).rev() {
            result = self.mulmod(&result, &result);
            if e.bit(i) {
                result = self.mulmod(&result, &base);
            }
        }
        result
    }

    /// `a^-1 mod m` for a prime `m`, by Fermat.
    pub fn invmod_prime(&self, a: &Big) -> Big {
        let e = self.m.sub(&Big::from_u32(2));
        self.powmod(a, &e)
    }
}

impl Big {
    /// `self^e mod m`, square-and-multiply.
    pub fn powmod(&self, e: &Big, m: &Big) -> Big {
        let mut result = Big::from_u32(1).rem(m);
        let base = self.rem(m);
        for i in (0..e.bits()).rev() {
            result = result.mulmod(&result, m);
            if e.bit(i) {
                result = result.mulmod(&base, m);
            }
        }
        result
    }

    /// `self^-1 mod p` for a prime `p`, by Fermat.
    #[cfg(test)]
    pub fn invmod_prime(&self, p: &Big) -> Big {
        let e = p.sub(&Big::from_u32(2));
        self.powmod(&e, p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Barrett reducer agrees with shift-and-subtract on products of
    /// values below the modulus, at the moduli the curves use and at odd
    /// ones, including the edge where the product is just under `m^2`.
    #[test]
    fn the_reducer_agrees_with_the_slow_remainder() {
        let moduli = [
            Big::from_be_bytes(&[0xff; 12]),
            Big::from_be_bytes(&[
                0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
            ]),
            Big::from_be_bytes(&[0x01, 0x00, 0x00, 0x00, 0x01]),
            Big::from_u32(7),
        ];
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for m in &moduli {
            let r = Reducer::new(m);
            let mm1 = m.sub(&Big::from_u32(1));
            for x in [Big::zero(), Big::from_u32(1), mm1.clone(), mm1.mul(&mm1)] {
                assert_eq!(r.reduce(&x), x.rem(m));
            }
            for _ in 0..200 {
                let mut bytes = vec![0u8; m.bits() / 8 + 1];
                for b in bytes.iter_mut() {
                    *b = next() as u8;
                }
                let a = Big::from_be_bytes(&bytes).rem(m);
                let b = Big::from_be_bytes(&bytes[1..]).rem(m);
                assert_eq!(r.mulmod(&a, &b), a.mulmod(&b, m));
                assert_eq!(r.reduce(&a.mul(&b)), a.mul(&b).rem(m));
            }
            if m.bits() > 32 {
                let a = mm1.sub(&Big::from_u32(3));
                assert_eq!(r.invmod_prime(&a).cmp_big(m), Ordering::Less);
            }
        }
    }

    #[test]
    fn big_integers_add_multiply_reduce_and_exponentiate() {
        let a = Big::from_be_bytes(&[0xff; 9]);
        let b = Big::from_u32(2);
        assert_eq!(a.bits(), 72);
        let sum = a.add(&Big::from_u32(1));
        assert_eq!(sum.to_be_bytes(10).unwrap(), [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            a.mul(&b).to_be_bytes(10).unwrap(),
            [1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]
        );
        assert_eq!(a.rem(&Big::from_u32(10)), Big::from_u32(5)); // 2^72 - 1 ≡ 5 (mod 10)
                                                                 // 3^5 mod 7 = 243 mod 7 = 5; 2^-1 mod 7 = 4.
        assert_eq!(Big::from_u32(3).powmod(&Big::from_u32(5), &Big::from_u32(7)), Big::from_u32(5));
        assert_eq!(Big::from_u32(2).invmod_prime(&Big::from_u32(7)), Big::from_u32(4));
        assert!(a.to_be_bytes(8).is_none(), "too big to fit");
        assert_eq!(Big::from_be_bytes(&[0, 0, 7]), Big::from_u32(7));
        assert_eq!(a.sub(&a), Big::zero());
    }
}
