//! Distance kernels.
//!
//! Everything downstream works in **distance** — lower is better — regardless
//! of metric, so that a single heap, a single `ORDER BY ... ASC` and a single
//! fusion input shape serve all three metrics. Cosine vectors are normalised at
//! insert, which turns cosine distance into `1 - dot` and removes a square root
//! and two norms from the inner loop.
//!
//! The loops are unrolled by four and written over slices so that the compiler
//! can autovectorise them. There is no explicit SIMD intrinsic here: on this
//! shape of loop `rustc` emits the same vector code, and intrinsics would cost
//! portability for nothing.

use crate::catalog::Metric;

/// `dot` and `l2_squared` dispatch once, at first use, on what the CPU has:
/// AVX2 with FMA when `x86_64` has them, the portable four-lane loop
/// otherwise. The two round differently -- FMA rounds a multiply-add once
/// where the portable loop rounds twice, and eight lanes sum in another
/// order -- so a score can differ in its last bits between machines. Every
/// claim the crate pins is within one process, where the function is one.
#[cfg(target_arch = "x86_64")]
mod avx {
    use std::arch::x86_64::*;
    use std::sync::atomic::{AtomicU8, Ordering};

    /// 0 not yet asked, 1 absent, 2 present.
    static STATE: AtomicU8 = AtomicU8::new(0);

    #[inline]
    pub fn available() -> bool {
        match STATE.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let ok = is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma");
                STATE.store(if ok { 2 } else { 1 }, Ordering::Relaxed);
                ok
            }
        }
    }

    /// The eight lanes of an accumulator, summed.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn hsum(v: __m256) -> f32 {
        let lo = _mm256_castps256_ps128(v);
        let hi = _mm256_extractf128_ps(v, 1);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_hadd_ps(s, s);
        let s = _mm_hadd_ps(s, s);
        _mm_cvtss_f32(s)
    }

    /// `(Σ w1[d]·c[d], Σ w2[d]·c[d]²)` over a byte code: eight bytes
    /// widened to floats per step, both sums in one pass. What a query
    /// prepared against SQ8 codes needs per candidate.
    ///
    /// # Safety
    /// The caller has checked `available()`.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn code_sums(c: &[u8], w1: &[f32], w2: &[f32]) -> (f32, f32) {
        let n = c.len().min(w1.len()).min(w2.len());
        let (pc, p1, p2) = (c.as_ptr(), w1.as_ptr(), w2.as_ptr());
        let mut s1 = _mm256_setzero_ps();
        let mut s2 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 8 <= n {
            let x = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_loadl_epi64(
                pc.add(i) as *const __m128i
            )));
            s1 = _mm256_fmadd_ps(x, _mm256_loadu_ps(p1.add(i)), s1);
            s2 = _mm256_fmadd_ps(_mm256_mul_ps(x, x), _mm256_loadu_ps(p2.add(i)), s2);
            i += 8;
        }
        let (mut a, mut b) = (hsum(s1), hsum(s2));
        while i < n {
            let x = c[i] as f32;
            a += w1[i] * x;
            b += w2[i] * x * x;
            i += 1;
        }
        (a, b)
    }

    /// `Σ w[d]·c[d]` over a byte code.
    ///
    /// # Safety
    /// The caller has checked `available()`.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn code_dot(c: &[u8], w: &[f32]) -> f32 {
        let n = c.len().min(w.len());
        let (pc, pw) = (c.as_ptr(), w.as_ptr());
        let mut s0 = _mm256_setzero_ps();
        let mut s1 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 16 <= n {
            let x0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_loadl_epi64(
                pc.add(i) as *const __m128i
            )));
            let x1 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_loadl_epi64(
                pc.add(i + 8) as *const __m128i
            )));
            s0 = _mm256_fmadd_ps(x0, _mm256_loadu_ps(pw.add(i)), s0);
            s1 = _mm256_fmadd_ps(x1, _mm256_loadu_ps(pw.add(i + 8)), s1);
            i += 16;
        }
        while i + 8 <= n {
            let x0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_loadl_epi64(
                pc.add(i) as *const __m128i
            )));
            s0 = _mm256_fmadd_ps(x0, _mm256_loadu_ps(pw.add(i)), s0);
            i += 8;
        }
        let mut s = hsum(_mm256_add_ps(s0, s1));
        while i < n {
            s += w[i] * c[i] as f32;
            i += 1;
        }
        s
    }

    /// # Safety
    /// The caller has checked `available()`.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        // Two accumulators over sixteen elements, so the two fused adds do
        // not wait on each other.
        while i + 16 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
                acc1,
            );
            i += 16;
        }
        while i + 8 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            i += 8;
        }
        let mut s = hsum(_mm256_add_ps(acc0, acc1));
        while i < n {
            s += a[i] * b[i];
            i += 1;
        }
        s
    }

    /// # Safety
    /// The caller has checked `available()`.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        while i + 16 <= n {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            let d1 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i + 8)), _mm256_loadu_ps(pb.add(i + 8)));
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            acc1 = _mm256_fmadd_ps(d1, d1, acc1);
            i += 16;
        }
        while i + 8 <= n {
            let d = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            acc0 = _mm256_fmadd_ps(d, d, acc0);
            i += 8;
        }
        let mut s = hsum(_mm256_add_ps(acc0, acc1));
        while i < n {
            let d = a[i] - b[i];
            s += d * d;
            i += 1;
        }
        s
    }
}

#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if avx::available() {
            // SAFETY: `available` checked avx2 and fma on this CPU.
            return unsafe { avx::dot(a, b) };
        }
    }
    dot_scalar(a, b)
}

/// `Σ w[d]·c[d]` over a byte code with float weights.
#[inline]
pub fn code_dot(c: &[u8], w: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if avx::available() {
            // SAFETY: `available` checked avx2 and fma on this CPU.
            return unsafe { avx::code_dot(c, w) };
        }
    }
    let n = c.len().min(w.len());
    let mut s = 0.0f32;
    for i in 0..n {
        s += w[i] * c[i] as f32;
    }
    s
}

/// `(Σ w1[d]·c[d], Σ w2[d]·c[d]²)` over a byte code, in one pass.
#[inline]
pub fn code_sums(c: &[u8], w1: &[f32], w2: &[f32]) -> (f32, f32) {
    #[cfg(target_arch = "x86_64")]
    {
        if avx::available() {
            // SAFETY: `available` checked avx2 and fma on this CPU.
            return unsafe { avx::code_sums(c, w1, w2) };
        }
    }
    let n = c.len().min(w1.len()).min(w2.len());
    let (mut a, mut b) = (0.0f32, 0.0f32);
    for i in 0..n {
        let x = c[i] as f32;
        a += w1[i] * x;
        b += w2[i] * x * x;
    }
    (a, b)
}

#[inline]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if avx::available() {
            // SAFETY: `available` checked avx2 and fma on this CPU.
            return unsafe { avx::l2_squared(a, b) };
        }
    }
    l2_squared_scalar(a, b)
}

#[inline]
fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    // Four accumulators over chunks of four, then the tail: the same
    // operations in the same order as the indexed loop this replaces, so
    // every score is bit for bit what it was -- but over `chunks_exact`,
    // whose slices the compiler knows are four long, there is no bounds
    // check per element and the four lanes become one SSE register. The
    // HNSW build spent 78% of its time in the indexed version (P6).
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);
    let (ca, cb) = (a.chunks_exact(4), b.chunks_exact(4));
    let (ra, rb) = (ca.remainder(), cb.remainder());
    let mut acc = [0.0f32; 4];
    for (x, y) in ca.zip(cb) {
        acc[0] += x[0] * y[0];
        acc[1] += x[1] * y[1];
        acc[2] += x[2] * y[2];
        acc[3] += x[3] * y[3];
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3];
    for (x, y) in ra.iter().zip(rb) {
        s += x * y;
    }
    s
}

#[inline]
fn l2_squared_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);
    let (ca, cb) = (a.chunks_exact(4), b.chunks_exact(4));
    let (ra, rb) = (ca.remainder(), cb.remainder());
    let mut acc = [0.0f32; 4];
    for (x, y) in ca.zip(cb) {
        let d0 = x[0] - y[0];
        let d1 = x[1] - y[1];
        let d2 = x[2] - y[2];
        let d3 = x[3] - y[3];
        acc[0] += d0 * d0;
        acc[1] += d1 * d1;
        acc[2] += d2 * d2;
        acc[3] += d3 * d3;
    }
    let mut s = acc[0] + acc[1] + acc[2] + acc[3];
    for (x, y) in ra.iter().zip(rb) {
        let d = x - y;
        s += d * d;
    }
    s
}

pub fn norm(a: &[f32]) -> f32 {
    dot(a, a).sqrt()
}

/// Scale `v` to unit length, or leave it alone if it has no direction.
///
/// The norm is taken *after* dividing through by the largest component. On the
/// raw vector `dot(v, v)` overflows to `+inf` once the norm passes
/// `sqrt(f32::MAX)`, and `1.0 / inf` is zero, so a perfectly ordinary
/// large-magnitude vector was overwritten with zeros — a document pointing
/// exactly along the query then ranked last rather than first. At the other
/// end a small vector's squares flush to denormals and the old absolute
/// `n > 1e-12` floor declined to normalise it at all, leaving a non-unit
/// vector in a store whose cosine distance assumes unit length.
///
/// Both divisions are real divisions. Multiplying by a precomputed `1.0 / m`
/// reintroduces a quieter version of the same bug: for `m` near `f32::MAX` the
/// reciprocal is itself denormal and carries only a few bits of precision.
pub fn normalize(v: &mut [f32]) {
    let mut m = 0.0f32;
    for x in v.iter() {
        // A non-finite component has no direction to preserve. The store side
        // refuses one outright; this leaves it untouched rather than turning
        // the whole vector into zeros.
        if !x.is_finite() {
            return;
        }
        let a = x.abs();
        if a > m {
            m = a;
        }
    }
    if m == 0.0 {
        return;
    }
    for x in v.iter_mut() {
        *x /= m;
    }
    // The largest component is now exactly 1, so `n` is at least 1 and at most
    // `sqrt(len)` — inside the float range whatever the input magnitude was.
    let n = norm(v);
    for x in v.iter_mut() {
        *x /= n;
    }
}

/// Distance under `metric`, assuming cosine inputs are already normalised.
///
/// L2 is reported *squared*. It is monotone in true L2, so top-k, thresholds
/// and ordering are all identical, and the square root would be paid on every
/// comparison for no information. `EXPLAIN` labels it, and the SQL layer takes
/// the root only where a distance value is returned to the user.
#[inline]
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::Cosine => 1.0 - dot(a, b),
        Metric::L2 => l2_squared(a, b),
        Metric::InnerProduct => -dot(a, b),
    }
}

/// Prepare a vector for storage under `metric`.
pub fn prepare(metric: Metric, v: &mut [f32]) {
    if metric == Metric::Cosine {
        normalize(v);
    }
}

/// The value a user should see for a distance the engine computed.
pub fn present(metric: Metric, d: f32) -> f32 {
    match metric {
        Metric::L2 => d.max(0.0).sqrt(),
        _ => d,
    }
}

#[cfg(test)]
mod tests {
    /// The vector kernel and the portable loop agree to rounding, at every
    /// length that exercises a sixteen-, eight- and tail-element path.
    #[test]
    fn the_vector_kernel_agrees_with_the_portable_loop() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % 20_000) as f32 / 10_000.0 - 1.0
        };
        for n in [1usize, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 100, 128, 129, 384, 1536] {
            let a: Vec<f32> = (0..n).map(|_| next()).collect();
            let b: Vec<f32> = (0..n).map(|_| next()).collect();
            let (d, ds) = (super::dot(&a, &b), super::dot_scalar(&a, &b));
            assert!((d - ds).abs() <= 1e-4 * (1.0 + ds.abs()), "dot n={n}: {d} vs {ds}");
            let (l, ls) = (super::l2_squared(&a, &b), super::l2_squared_scalar(&a, &b));
            assert!((l - ls).abs() <= 1e-4 * (1.0 + ls.abs()), "l2 n={n}: {l} vs {ls}");
        }
    }

    use super::*;

    #[test]
    fn kernels_agree_with_naive() {
        let a: Vec<f32> = (0..37).map(|i| (i as f32) * 0.13 - 2.0).collect();
        let b: Vec<f32> = (0..37).map(|i| (i as f32) * -0.07 + 1.0).collect();
        let nd: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        assert!((dot(&a, &b) - nd).abs() < 1e-3);
        let nl: f32 = a.iter().zip(&b).map(|(x, y)| (x - y) * (x - y)).sum();
        assert!((l2_squared(&a, &b) - nl).abs() < 1e-3);
    }

    /// `dot(v, v)` is `+inf` here, so the old `1.0 / n` was zero and wrote the
    /// zero vector — every distance from it identical, and a document that
    /// points exactly along the query indistinguishable from one that points
    /// away.
    #[test]
    fn a_large_vector_normalises_rather_than_collapsing_to_zero() {
        let mut v = vec![3.0e19f32, 4.0e19, 0.0];
        prepare(Metric::Cosine, &mut v);
        assert!((v[0] - 0.6).abs() < 1e-5 && (v[1] - 0.8).abs() < 1e-5, "{v:?}");
        let q = vec![0.6f32, 0.8, 0.0];
        assert!(distance(Metric::Cosine, &v, &q).abs() < 1e-5, "{v:?}");
    }

    /// The mirror image: the norm underflows below the old absolute `1e-12`
    /// floor, so the vector was left un-normalised in a store whose cosine
    /// distance assumes unit length.
    #[test]
    fn a_tiny_vector_normalises_rather_than_being_left_alone() {
        let mut v = vec![3.0e-20f32, 4.0e-20, 0.0];
        prepare(Metric::Cosine, &mut v);
        assert!((norm(&v) - 1.0).abs() < 1e-5, "{v:?}");
        assert!((v[0] - 0.6).abs() < 1e-5 && (v[1] - 0.8).abs() < 1e-5, "{v:?}");
    }

    #[test]
    fn a_zero_vector_is_left_alone() {
        let mut v = vec![0.0f32; 4];
        prepare(Metric::Cosine, &mut v);
        assert_eq!(v, vec![0.0f32; 4]);
    }

    #[test]
    fn cosine_after_normalisation() {
        let mut a = vec![3.0f32, 4.0, 0.0];
        let mut b = vec![3.0f32, 4.0, 0.0];
        prepare(Metric::Cosine, &mut a);
        prepare(Metric::Cosine, &mut b);
        assert!(distance(Metric::Cosine, &a, &b).abs() < 1e-6);
    }
}
