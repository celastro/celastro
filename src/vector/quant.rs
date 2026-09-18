//! Quantized codes.
//!
//! Two-stage scoring is used throughout the vector tier (§5.2): traverse over
//! quantized codes, then rerank the top candidates against full-precision
//! vectors. The codes are the part that must be memory-resident; the
//! full-precision vectors are the only component allowed to be cold (§8.4).
//!
//! Sizes per 1M vectors at 1536 dimensions, which is what the choice is
//! actually about:
//!
//! | representation | size |
//! |---|---|
//! | float32 (cold, rerank only) | 6.1 GB |
//! | SQ8 codes (resident) | 1.5 GB |
//! | 1-bit codes (resident) | 0.19 GB |
//!
//! Both quantizers use **asymmetric** distance: the query stays in full
//! precision and only the stored side is quantized. That halves the error for
//! free, and it is why 1-bit codes with a full-precision rerank are viable at
//! all.

use crate::catalog::Metric;
use crate::codec::*;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantizer {
    /// 8 bits per dimension, per-dimension affine scale.
    Sq8,
    /// 1 bit per dimension: the sign of the centred value. Estimation is
    /// RaBitQ-flavoured — a per-vector scale times the query's signed sum.
    OneBit,
    /// No quantization: traversal reads full precision. What the memtable uses,
    /// where the set is small and exactness is the point.
    None,
}

/// What [`Codes::prepare`] makes of a query: the weights and the constant
/// that turn each candidate's distance into one pass over its code.
pub struct PreparedQuery {
    metric: Metric,
    w1: Vec<f32>,
    w2: Vec<f32>,
    constant: f32,
}

impl PreparedQuery {
    /// The distance from the prepared query to code `i`, as
    /// [`Codes::distance`] would answer it.
    #[inline]
    pub fn distance(&self, codes: &Codes, i: usize) -> f32 {
        let base = i * codes.dims;
        let code = &codes.data[base..base + codes.dims];
        match self.metric {
            Metric::L2 => {
                let (s1, s2) = crate::vector::distance::code_sums(code, &self.w1, &self.w2);
                (self.constant + s1 + s2).max(0.0)
            }
            Metric::Cosine => {
                1.0 - (self.constant + crate::vector::distance::code_dot(code, &self.w1))
            }
            Metric::InnerProduct => {
                -(self.constant + crate::vector::distance::code_dot(code, &self.w1))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Codes {
    pub quantizer: Quantizer,
    pub dims: usize,
    pub count: usize,
    /// `dims` bytes per vector for Sq8; `ceil(dims/8)` for OneBit.
    data: Vec<u8>,
    /// Per-dimension lower bound and step (Sq8), or per-dimension centroid
    /// (OneBit).
    lo: Vec<f32>,
    step: Vec<f32>,
    /// Per-vector scale, OneBit only: `‖v - c‖ / sqrt(dims)`, which turns the
    /// signed sum into an estimate of the dot product.
    scale: Vec<f32>,
}

impl Codes {
    pub fn empty(quantizer: Quantizer, dims: usize) -> Codes {
        Codes {
            quantizer,
            dims,
            count: 0,
            data: Vec::new(),
            lo: vec![0.0; dims],
            step: vec![1.0; dims],
            scale: Vec::new(),
        }
    }

    pub fn code_len(&self) -> usize {
        match self.quantizer {
            Quantizer::Sq8 => self.dims,
            Quantizer::OneBit => self.dims.div_ceil(8),
            Quantizer::None => 0,
        }
    }

    pub fn memory_bytes(&self) -> usize {
        self.data.len() + self.scale.len() * 4 + self.lo.len() * 8
    }

    /// Fit and encode a whole vector set. Quantizer parameters are per segment,
    /// which is what makes a segment self-contained: nothing outside it is
    /// needed to interpret its codes, and re-quantizing is a rolling rebuild
    /// through compaction rather than a migration (§12.3).
    /// A query prepared against these codes, for SQ8: the per-query parts
    /// of every distance folded out once, so that each candidate costs one
    /// pass over its bytes with float weights (`distance::code_dot`,
    /// `code_sums`) instead of a decode. For the dot product,
    /// `Σ q·(lo + s·c) = Σ q·lo + Σ (q·s)·c`; for L2, with `a = q - lo`,
    /// `Σ (a - s·c)² = Σ a² - 2 Σ (a·s)·c + Σ s²·c²`. `None` for the other
    /// quantizers, which keep [`Codes::distance`].
    pub fn prepare(&self, metric: Metric, query: &[f32]) -> Option<PreparedQuery> {
        if self.quantizer != Quantizer::Sq8 || query.len() != self.dims {
            return None;
        }
        let dims = self.dims;
        Some(match metric {
            Metric::L2 => {
                let mut w1 = Vec::with_capacity(dims);
                let mut w2 = Vec::with_capacity(dims);
                let mut constant = 0.0f32;
                for d in 0..dims {
                    let a = query[d] - self.lo[d];
                    constant += a * a;
                    w1.push(-2.0 * a * self.step[d]);
                    w2.push(self.step[d] * self.step[d]);
                }
                PreparedQuery { metric, w1, w2, constant }
            }
            _ => {
                let mut w1 = Vec::with_capacity(dims);
                let mut constant = 0.0f32;
                for d in 0..dims {
                    constant += query[d] * self.lo[d];
                    w1.push(query[d] * self.step[d]);
                }
                PreparedQuery { metric, w1, w2: Vec::new(), constant }
            }
        })
    }

    pub fn build(quantizer: Quantizer, dims: usize, vectors: &[f32]) -> Codes {
        let count = vectors.len().checked_div(dims).unwrap_or(0);
        let mut c = Codes::empty(quantizer, dims);
        c.count = count;
        if count == 0 || quantizer == Quantizer::None {
            return c;
        }
        match quantizer {
            Quantizer::Sq8 => {
                let mut lo = vec![f32::INFINITY; dims];
                let mut hi = vec![f32::NEG_INFINITY; dims];
                for v in vectors.chunks_exact(dims) {
                    for d in 0..dims {
                        lo[d] = lo[d].min(v[d]);
                        hi[d] = hi[d].max(v[d]);
                    }
                }
                // A non-finite range would give `step = inf`, and then every
                // code in that dimension dequantizes to `lo + 0 * inf` = NaN —
                // for every vector, not only the offending one. Callers reject
                // non-finite vectors, but a quantizer that can poison an entire
                // segment from one bad value should not depend on that.
                for d in 0..dims {
                    if !lo[d].is_finite() {
                        lo[d] = 0.0;
                    }
                    if !hi[d].is_finite() {
                        hi[d] = lo[d];
                    }
                }
                let step: Vec<f32> = (0..dims)
                    .map(|d| {
                        let r = hi[d] - lo[d];
                        if !r.is_finite() || r <= 0.0 {
                            1.0
                        } else {
                            r / 255.0
                        }
                    })
                    .collect();
                let mut data = vec![0u8; count * dims];
                for (i, v) in vectors.chunks_exact(dims).enumerate() {
                    for d in 0..dims {
                        let q = ((v[d] - lo[d]) / step[d]).round();
                        data[i * dims + d] = q.clamp(0.0, 255.0) as u8;
                    }
                }
                c.lo = lo;
                c.step = step;
                c.data = data;
            }
            Quantizer::OneBit => {
                let mut centroid = vec![0.0f32; dims];
                for v in vectors.chunks_exact(dims) {
                    for d in 0..dims {
                        centroid[d] += v[d];
                    }
                }
                for x in centroid.iter_mut() {
                    *x /= count as f32;
                }
                let bytes = dims.div_ceil(8);
                let mut data = vec![0u8; count * bytes];
                let mut scale = vec![0.0f32; count];
                for (i, v) in vectors.chunks_exact(dims).enumerate() {
                    let mut nrm = 0.0f32;
                    for d in 0..dims {
                        let x = v[d] - centroid[d];
                        nrm += x * x;
                        if x >= 0.0 {
                            data[i * bytes + d / 8] |= 1 << (d % 8);
                        }
                    }
                    scale[i] = (nrm / dims as f32).sqrt();
                }
                c.lo = centroid;
                c.step = vec![1.0; dims];
                c.data = data;
                c.scale = scale;
            }
            Quantizer::None => {}
        }
        c
    }

    /// Decode one code back to full precision. Used by the estimator and by
    /// tests that want to see the quantization error directly.
    pub fn decode(&self, i: usize, out: &mut [f32]) {
        match self.quantizer {
            Quantizer::Sq8 => {
                let base = i * self.dims;
                for d in 0..self.dims {
                    out[d] = self.lo[d] + self.data[base + d] as f32 * self.step[d];
                }
            }
            Quantizer::OneBit => {
                let bytes = self.dims.div_ceil(8);
                let base = i * bytes;
                let s = self.scale[i];
                for d in 0..self.dims {
                    let bit = (self.data[base + d / 8] >> (d % 8)) & 1;
                    out[d] = self.lo[d] + if bit == 1 { s } else { -s };
                }
            }
            Quantizer::None => out.fill(0.0),
        }
    }

    /// Approximate distance from a full-precision query to stored vector `i`.
    ///
    /// Asymmetric: the query is never quantized. For Sq8 this is a
    /// dequantize-and-compute over a byte array, which stays in cache where the
    /// float vector would not.
    pub fn distance(&self, metric: Metric, query: &[f32], i: usize) -> f32 {
        match self.quantizer {
            Quantizer::Sq8 => {
                let base = i * self.dims;
                let code = &self.data[base..base + self.dims];
                match metric {
                    Metric::L2 => {
                        let mut s = 0.0f32;
                        for d in 0..self.dims {
                            let x = self.lo[d] + code[d] as f32 * self.step[d];
                            let diff = query[d] - x;
                            s += diff * diff;
                        }
                        s
                    }
                    _ => {
                        let mut s = 0.0f32;
                        for d in 0..self.dims {
                            s += query[d] * (self.lo[d] + code[d] as f32 * self.step[d]);
                        }
                        if metric == Metric::Cosine {
                            1.0 - s
                        } else {
                            -s
                        }
                    }
                }
            }
            Quantizer::OneBit => {
                let bytes = self.dims.div_ceil(8);
                let base = i * bytes;
                let s = self.scale[i];
                // dot(q, c + s·sign) = dot(q, c) + s·Σ ±q_d
                let mut signed = 0.0f32;
                let mut qc = 0.0f32;
                let mut cross = 0.0f32;
                for d in 0..self.dims {
                    let bit = (self.data[base + d / 8] >> (d % 8)) & 1;
                    signed += if bit == 1 { query[d] } else { -query[d] };
                    qc += query[d] * self.lo[d];
                    cross += if bit == 1 { self.lo[d] } else { -self.lo[d] };
                }
                let est_dot = qc + s * signed;
                match metric {
                    Metric::Cosine => 1.0 - est_dot,
                    Metric::InnerProduct => -est_dot,
                    // ‖q-x̂‖² = ‖q‖² - 2·q·x̂ + ‖x̂‖², and with x̂ = c + s·sign,
                    // ‖x̂‖² = ‖c‖² + 2s·Σ ±c_d + s²·dims. The middle term is
                    // *not* constant per vector — it depends on the sign
                    // pattern — so dropping it makes the estimate disagree with
                    // the decoded vector by a wide margin.
                    Metric::L2 => {
                        let qq: f32 = query.iter().map(|x| x * x).sum();
                        let cc: f32 = self.lo.iter().map(|c| c * c).sum();
                        let xx = cc + 2.0 * s * cross + s * s * self.dims as f32;
                        qq - 2.0 * est_dot + xx
                    }
                }
            }
            Quantizer::None => f32::INFINITY,
        }
    }

    pub fn encode_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(match self.quantizer {
            Quantizer::Sq8 => 0u8,
            Quantizer::OneBit => 1,
            Quantizer::None => 2,
        });
        put_uvarint(&mut out, self.dims as u64);
        put_uvarint(&mut out, self.count as u64);
        for d in 0..self.dims {
            put_f32(&mut out, self.lo[d]);
            put_f32(&mut out, self.step[d]);
        }
        put_uvarint(&mut out, self.scale.len() as u64);
        for s in &self.scale {
            put_f32(&mut out, *s);
        }
        put_bytes(&mut out, &self.data);
        out
    }

    pub fn decode_bytes(b: &[u8]) -> Result<Codes> {
        let bad = || Error::Storage("codes: truncated".into());
        let mut i = 0usize;
        let q = match *b.first().ok_or_else(bad)? {
            0 => Quantizer::Sq8,
            1 => Quantizer::OneBit,
            _ => Quantizer::None,
        };
        i += 1;
        let dims = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        // Each dimension carries an eight-byte `(lo, step)` pair and each
        // scale four bytes, so the bytes left over bound both repeat counts.
        // Sizing a `Vec` from the raw value instead lets a short header ask
        // for an allocation the process cannot satisfy.
        if dims > (b.len() - i) / 8 {
            return Err(bad());
        }
        let count = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let mut lo = Vec::with_capacity(dims);
        let mut step = Vec::with_capacity(dims);
        for _ in 0..dims {
            lo.push(get_f32(b, &mut i).ok_or_else(bad)?);
            step.push(get_f32(b, &mut i).ok_or_else(bad)?);
        }
        let ns = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        if ns > (b.len() - i) / 4 {
            return Err(bad());
        }
        let mut scale = Vec::with_capacity(ns);
        for _ in 0..ns {
            scale.push(get_f32(b, &mut i).ok_or_else(bad)?);
        }
        let data = get_bytes(b, &mut i).ok_or_else(bad)?.to_vec();
        let c = Codes { quantizer: q, dims, count, data, lo, step, scale };
        // `distance` and `decode` slice `data[i * code_len ..]` and read
        // `scale[i]` for every `i < count`, and no caller bounds-checks first.
        // A `count` the payload cannot back panics mid-query on a segment that
        // was accepted at open time.
        let need = c.count.checked_mul(c.code_len()).ok_or_else(bad)?;
        if c.data.len() < need {
            return Err(Error::Storage("codes: data shorter than count".into()));
        }
        if c.quantizer == Quantizer::OneBit && c.scale.len() < c.count {
            return Err(Error::Storage("codes: missing per-vector scales".into()));
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    /// A prepared query answers what the per-candidate decode answers, for
    /// every metric, to float rounding: the kernel is the fast path of the
    /// graph search and must not move a ranking.
    #[test]
    fn a_prepared_query_answers_what_the_decode_answers() {
        let (n, dims) = (200usize, 128usize);
        let mut rng = crate::codec::Rng::new(11);
        let mut data: Vec<f32> = (0..n * dims).map(|_| rng.next_normal()).collect();
        for v in data.chunks_exact_mut(dims) {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter_mut().for_each(|x| *x /= norm);
        }
        let codes = Codes::build(Quantizer::Sq8, dims, &data);
        for metric in [Metric::L2, Metric::Cosine, Metric::InnerProduct] {
            for q in [7usize, 42, 199] {
                let query = &data[q * dims..(q + 1) * dims];
                let p = codes.prepare(metric, query).expect("sq8");
                for i in 0..n {
                    let want = codes.distance(metric, query, i);
                    let got = p.distance(&codes, i);
                    assert!(
                        (want - got).abs() <= 1e-4 * (1.0 + want.abs()),
                        "{metric:?} q{q} i{i}: {want} vs {got}"
                    );
                }
            }
        }
        assert!(Codes::build(Quantizer::OneBit, dims, &data)
            .prepare(Metric::L2, &data[..dims])
            .is_none());
    }

    #[test]
    fn fuzz_code_decoding_never_panics() {
        let (n, dims) = (64usize, 16usize);
        let mut rng = crate::codec::Rng::new(5);
        let data: Vec<f32> = (0..n * dims).map(|_| rng.next_normal()).collect();
        let sq8 = Codes::build(Quantizer::Sq8, dims, &data).encode_bytes();
        let one = Codes::build(Quantizer::OneBit, dims, &data).encode_bytes();
        crate::fuzz::sweep(81, &[sq8, one], 5000, |b| {
            if let Ok(c) = Codes::decode_bytes(b) {
                let q = vec![0.5f32; c.dims];
                if c.count > 0 {
                    let _ = c.distance(Metric::L2, &q, 0);
                    let _ = c.distance(Metric::Cosine, &q, c.count - 1);
                }
            }
        });
    }
    use super::*;
    use crate::codec::Rng;
    use crate::vector::distance;

    fn corpus(n: usize, dims: usize, metric: Metric) -> Vec<f32> {
        let mut rng = Rng::new(7);
        let mut v = Vec::with_capacity(n * dims);
        for _ in 0..n {
            let mut x: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(metric, &mut x);
            v.extend_from_slice(&x);
        }
        v
    }

    /// The property that matters is not absolute error but **rank
    /// preservation**: the codes only have to get the right candidates into the
    /// rerank set.
    fn rank_agreement(q: Quantizer, metric: Metric, rerank_mult: usize) -> f64 {
        let (n, dims, k) = (2000usize, 64usize, 20usize);
        let data = corpus(n, dims, metric);
        let codes = Codes::build(q, dims, &data);
        let mut rng = Rng::new(99);
        let mut hits = 0usize;
        let trials = 20;
        for _ in 0..trials {
            let mut query: Vec<f32> = (0..dims).map(|_| rng.next_normal()).collect();
            distance::prepare(metric, &mut query);
            let mut exact: Vec<(usize, f32)> = (0..n)
                .map(|i| (i, distance::distance(metric, &query, &data[i * dims..(i + 1) * dims])))
                .collect();
            exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let truth: Vec<usize> = exact[..k].iter().map(|(i, _)| *i).collect();

            let mut approx: Vec<(usize, f32)> =
                (0..n).map(|i| (i, codes.distance(metric, &query, i))).collect();
            approx.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            // Rerank a candidate set, as the real search path does.
            let cand: Vec<usize> = approx[..k * rerank_mult].iter().map(|(i, _)| *i).collect();
            let mut reranked: Vec<(usize, f32)> = cand
                .iter()
                .map(|&i| (i, distance::distance(metric, &query, &data[i * dims..(i + 1) * dims])))
                .collect();
            reranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            for (i, _) in reranked.iter().take(k) {
                if truth.contains(i) {
                    hits += 1;
                }
            }
        }
        hits as f64 / (trials * k) as f64
    }

    #[test]
    fn sq8_rerank_recovers_exact_top_k() {
        let r = rank_agreement(Quantizer::Sq8, Metric::Cosine, 5);
        assert!(r >= 0.99, "sq8 cosine recall@20 after 5x rerank = {r}");
        let r = rank_agreement(Quantizer::Sq8, Metric::L2, 5);
        assert!(r >= 0.99, "sq8 l2 recall@20 after 5x rerank = {r}");
    }

    #[test]
    fn one_bit_rerank_is_usable() {
        // 1-bit codes are 32x smaller than float32 and lose real information.
        // What they must preserve is enough ordering to feed the rerank set —
        // and the price of the smaller code is a *deeper* rerank set, not a
        // worse answer. On isotropic Gaussian vectors, which is the worst case
        // for sign-based quantization, 5x is not enough and 20x is; that ratio
        // is the number to carry into the sizing decision in §13, because it is
        // extra full-precision reads, not extra resident memory.
        let shallow = rank_agreement(Quantizer::OneBit, Metric::Cosine, 5);
        let deep = rank_agreement(Quantizer::OneBit, Metric::Cosine, 20);
        assert!(deep > shallow, "deeper rerank should help: {shallow} -> {deep}");
        assert!(deep >= 0.90, "1-bit cosine recall@20 after 20x rerank = {deep}");
    }

    /// Hand-rolled header, because `encode_bytes` cannot describe more vectors
    /// than the payload carries and that is exactly what has to be rejected.
    fn hand_encoded(tag: u8, dims: u64, count: u64, nscale: u64, data_len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(tag);
        put_uvarint(&mut out, dims);
        put_uvarint(&mut out, count);
        for _ in 0..dims {
            put_f32(&mut out, 0.0);
            put_f32(&mut out, 1.0);
        }
        put_uvarint(&mut out, nscale);
        for _ in 0..nscale {
            put_f32(&mut out, 1.0);
        }
        let data = vec![0u8; data_len];
        put_bytes(&mut out, &data);
        out
    }

    #[test]
    fn a_code_array_shorter_than_the_count_is_rejected_rather_than_slicing_past_the_end() {
        // Ten Sq8 vectors of four dimensions need forty code bytes.
        assert!(Codes::decode_bytes(&hand_encoded(0, 4, 10, 0, 8)).is_err());
        // The same header with the payload it claims decodes, so the rejection
        // is the short payload and not the hand-rolled framing.
        assert!(Codes::decode_bytes(&hand_encoded(0, 4, 10, 0, 40)).is_ok());
    }

    #[test]
    fn one_bit_codes_missing_a_scale_per_vector_are_rejected_rather_than_indexing_scale() {
        // The codes themselves are long enough — one byte covers four
        // dimensions — so it is only `scale[i]`, read for every vector by
        // `distance`, that the stream fails to supply.
        assert!(Codes::decode_bytes(&hand_encoded(1, 4, 6, 2, 6)).is_err());
        assert!(Codes::decode_bytes(&hand_encoded(1, 4, 6, 6, 6)).is_ok());
    }

    #[test]
    fn codes_round_trip_through_bytes() {
        let data = corpus(50, 16, Metric::Cosine);
        let c = Codes::build(Quantizer::Sq8, 16, &data);
        let back = Codes::decode_bytes(&c.encode_bytes()).unwrap();
        assert_eq!(back.count, 50);
        let q = &data[0..16];
        for i in 0..50 {
            let a = c.distance(Metric::Cosine, q, i);
            let b = back.distance(Metric::Cosine, q, i);
            assert!((a - b).abs() < 1e-6);
        }
    }
}
