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

#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len();
    let chunks = n / 4;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    for i in 0..chunks {
        let j = i * 4;
        s0 += a[j] * b[j];
        s1 += a[j + 1] * b[j + 1];
        s2 += a[j + 2] * b[j + 2];
        s3 += a[j + 3] * b[j + 3];
    }
    let mut s = s0 + s1 + s2 + s3;
    for i in chunks * 4..n {
        s += a[i] * b[i];
    }
    s
}

#[inline]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len();
    let chunks = n / 4;
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    for i in 0..chunks {
        let j = i * 4;
        let d0 = a[j] - b[j];
        let d1 = a[j + 1] - b[j + 1];
        let d2 = a[j + 2] - b[j + 2];
        let d3 = a[j + 3] - b[j + 3];
        s0 += d0 * d0;
        s1 += d1 * d1;
        s2 += d2 * d2;
        s3 += d3 * d3;
    }
    let mut s = s0 + s1 + s2 + s3;
    for i in chunks * 4..n {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

pub fn norm(a: &[f32]) -> f32 {
    dot(a, a).sqrt()
}

pub fn normalize(v: &mut [f32]) {
    let n = norm(v);
    if n > 1e-12 {
        let inv = 1.0 / n;
        for x in v.iter_mut() {
            *x *= inv;
        }
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

    #[test]
    fn cosine_after_normalisation() {
        let mut a = vec![3.0f32, 4.0, 0.0];
        let mut b = vec![3.0f32, 4.0, 0.0];
        prepare(Metric::Cosine, &mut a);
        prepare(Metric::Cosine, &mut b);
        assert!(distance(Metric::Cosine, &a, &b).abs() < 1e-6);
    }
}
