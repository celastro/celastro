//! Arithmetic modulo the group order l = 2^252 + 27742317777372353535851937790883648493,
//! for Ed25519's scalars. Values are four `u64` limbs, little-endian, below
//! l; a 512-bit input is reduced by a fixed 512-step binary division, so a
//! secret scalar costs the same whatever it is.

/// l, little-endian limbs.
const L: [u64; 4] = [0x5812631a5cf5d3ed, 0x14def9dea2f79cd6, 0, 0x1000000000000000];

/// `a < L`? — masks only.
fn lt_l(a: &[u64; 4]) -> bool {
    let mut borrow = 0u64;
    for i in 0..4 {
        let (d, b1) = a[i].overflowing_sub(L[i]);
        let (_, b2) = d.overflowing_sub(borrow);
        borrow = (b1 | b2) as u64;
    }
    borrow == 1
}

/// `a - L` when `a >= L`, `a` otherwise, without a branch.
fn reduce_once(a: &mut [u64; 4]) {
    let mut d = [0u64; 4];
    let mut borrow = 0u64;
    for i in 0..4 {
        let (x, b1) = a[i].overflowing_sub(L[i]);
        let (y, b2) = x.overflowing_sub(borrow);
        d[i] = y;
        borrow = (b1 | b2) as u64;
    }
    // borrow == 1 means a < L: keep a.
    // The mask through a barrier: `keep` is 0 or all ones, and a compiler
    // that sees that may turn the select into a branch on it, which is a
    // branch on the scalar's bits. `black_box` keeps the value opaque.
    let keep = std::hint::black_box(0u64.wrapping_sub(borrow));
    for i in 0..4 {
        a[i] = (a[i] & keep) | (d[i] & !keep);
    }
}

/// A 512-bit little-endian number reduced modulo l, bit by bit.
pub fn reduce_512(bytes: &[u8; 64]) -> [u64; 4] {
    let mut r = [0u64; 4];
    for bit in (0..512).rev() {
        // r = 2r + bit, then r -= l if r >= l. r < l before, so 2r + 1 < 2l
        // and one conditional subtraction reduces it.
        let carry_in = ((bytes[bit / 8] >> (bit % 8)) & 1) as u64;
        let mut carry = carry_in;
        for limb in r.iter_mut() {
            let next = *limb >> 63;
            *limb = (*limb << 1) | carry;
            carry = next;
        }
        // r < 2l < 2^253, so the shifted value fits four limbs.
        reduce_once(&mut r);
    }
    r
}

/// A 256-bit little-endian scalar reduced modulo l.
pub fn reduce_256(bytes: &[u8; 32]) -> [u64; 4] {
    let mut wide = [0u8; 64];
    wide[..32].copy_from_slice(bytes);
    reduce_512(&wide)
}

/// `(a * b + c) mod l`.
pub fn muladd(a: &[u64; 4], b: &[u64; 4], c: &[u64; 4]) -> [u64; 4] {
    let mut prod = [0u128; 8];
    for i in 0..4 {
        let mut carry = 0u128;
        for j in 0..4 {
            let t = prod[i + j] + a[i] as u128 * b[j] as u128 + carry;
            prod[i + j] = t & 0xffff_ffff_ffff_ffff;
            carry = t >> 64;
        }
        prod[i + 4] += carry;
    }
    let mut carry = 0u128;
    for i in 0..8 {
        let t = prod[i] + if i < 4 { c[i] as u128 } else { 0 } + carry;
        prod[i] = t & 0xffff_ffff_ffff_ffff;
        carry = t >> 64;
    }
    let mut bytes = [0u8; 64];
    for i in 0..8 {
        bytes[8 * i..8 * i + 8].copy_from_slice(&(prod[i] as u64).to_le_bytes());
    }
    reduce_512(&bytes)
}

/// Zero a scalar that held a secret, in a way the compiler keeps.
pub fn wipe(a: &mut [u64; 4]) {
    *a = [0u64; 4];
    std::hint::black_box(a);
}

pub fn to_bytes(a: &[u64; 4]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..4 {
        out[8 * i..8 * i + 8].copy_from_slice(&a[i].to_le_bytes());
    }
    out
}

pub fn from_bytes(b: &[u8; 32]) -> [u64; 4] {
    let mut a = [0u64; 4];
    for i in 0..4 {
        let mut w = [0u8; 8];
        w.copy_from_slice(&b[8 * i..8 * i + 8]);
        a[i] = u64::from_le_bytes(w);
    }
    a
}

/// Whether 32 bytes encode a scalar below l — what a signature's `S` must.
pub fn is_canonical(b: &[u8; 32]) -> bool {
    lt_l(&from_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalars_reduce_and_multiply_modulo_the_group_order() {
        assert!(!is_canonical(&to_bytes(&L)));
        let mut lm1 = L;
        lm1[0] -= 1;
        assert!(is_canonical(&to_bytes(&lm1)));
        // l reduces to 0, l + 1 to 1, and 2^512 - 1 to a known residue.
        assert_eq!(reduce_256(&to_bytes(&L)), [0; 4]);
        let mut lp1 = L;
        lp1[0] += 1;
        assert_eq!(reduce_256(&to_bytes(&lp1)), [1, 0, 0, 0]);
        let all = [0xffu8; 64];
        let r = reduce_512(&all);
        assert!(lt_l(&r));
        // (l - 1) * (l - 1) + 0 = 1 mod l.
        assert_eq!(muladd(&lm1, &lm1, &[0; 4]), [1, 0, 0, 0]);
        // 2 * 3 + 4 = 10.
        assert_eq!(muladd(&[2, 0, 0, 0], &[3, 0, 0, 0], &[4, 0, 0, 0]), [10, 0, 0, 0]);
    }
}
