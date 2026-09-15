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
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
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
