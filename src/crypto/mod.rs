//! The primitives an in-tree TLS 1.3 needs, and nothing more: SHA-256 and
//! HMAC (the S3 signer's too), SHA-512, HKDF, ChaCha20-Poly1305, and the
//! curve25519 pair — X25519 for key agreement, Ed25519 for signatures —
//! with RSA and P-256 verification for what other issuers sign. Each is
//! written from its RFC and pinned against the vectors published there;
//! none is a port of another library. Nothing here reaches up into the
//! database: this module is the bottom of the tree.
//!
//! What "constant-time" means here, stated once: nothing branches on, or
//! indexes memory by, a secret. Field and scalar arithmetic run the same
//! instructions whatever the values; a conditional on a secret bit is a
//! mask; comparisons of secrets fold every byte before deciding. The
//! compiler is not asked to keep that promise — there is no `black_box` in
//! the MSRV's `std` — so the code keeps it by having no branch to remove.
//!
//! This is unaudited. It is here because the crate carries no dependency,
//! by decision, and the README says so.

// The record layer and the handshake are the callers; until they land the
// primitives are reached by their tests alone.
#![allow(dead_code)]

pub mod bignum;
pub mod chacha20poly1305;
pub mod der;
pub mod ed25519;
pub mod fe25519;
pub mod hkdf;
pub mod p256;
pub mod pem;
pub mod random;
pub mod rsa;
pub mod sc25519;
pub mod sha2;
pub mod tls13;
#[cfg(test)]
mod wycheproof;
pub mod x25519;
pub mod x509;

/// `a == b`, deciding only after every byte has been folded in.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    // The fold is opaque to the compiler, so it cannot be told to stop at
    // the first difference.
    std::hint::black_box(diff) == 0
}

/// Lowercase hex, the one spelling of bytes the crate prints.
pub(crate) fn hex(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len() * 2);
    for x in b {
        out.push_str(&format!("{x:02x}"));
    }
    out
}

/// The "no branch on a secret" claim, measured rather than asserted by
/// construction: two inputs that differ only in the secret, sampled in
/// alternation many times, must not be told apart by their medians. Noisy
/// by nature, so it is run by hand on a real CPU and its numbers read:
/// `cargo test --release --lib crypto::timing -- --ignored --nocapture`.
#[cfg(test)]
mod timing {
    use std::hint::black_box;
    use std::time::Instant;

    /// The medians of `n` samples of each, taken in alternation so that a
    /// drift of the machine falls on both alike.
    fn medians(n: usize, mut a: impl FnMut(), mut b: impl FnMut()) -> (f64, f64) {
        let (mut ta, mut tb) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for _ in 0..n / 10 {
            a();
            b();
        }
        for _ in 0..n {
            let t = Instant::now();
            a();
            ta.push(t.elapsed().as_nanos() as f64);
            let t = Instant::now();
            b();
            tb.push(t.elapsed().as_nanos() as f64);
        }
        ta.sort_by(f64::total_cmp);
        tb.sort_by(f64::total_cmp);
        (ta[n / 2], tb[n / 2])
    }

    /// Within five percent of each other, or within the clock's own
    /// thirty nanoseconds for the operations that take about that long.
    fn flat(name: &str, n: usize, a: impl FnMut(), b: impl FnMut()) {
        let (ma, mb) = medians(n, a, b);
        let apart = (ma - mb).abs();
        let rel = apart / ma.max(mb);
        eprintln!("{name}: medians {ma:.0} ns and {mb:.0} ns, {:.2}% apart", rel * 100.0);
        assert!(rel < 0.05 || apart < 30.0, "{name}: {ma} ns against {mb} ns");
    }

    #[test]
    #[ignore]
    fn nothing_here_is_told_apart_by_its_timing() {
        // A compare that is equal against one that differs in its first
        // byte: an early exit would end the second at once.
        let x = [7u8; 64];
        let mut y = x;
        y[0] ^= 1;
        flat(
            "ct_eq, 64 bytes",
            200_000,
            || {
                black_box(super::ct_eq(black_box(&x), black_box(&x)));
            },
            || {
                black_box(super::ct_eq(black_box(&x), black_box(&y)));
            },
        );
        // A scalar of few set bits against one of every bit: a ladder that
        // skipped on zero bits would show.
        let mut lo = [0u8; 32];
        lo[0] = 8;
        let hi = [0xffu8; 32];
        let mut base = [0u8; 32];
        base[0] = 9;
        flat(
            "x25519",
            3_000,
            || {
                black_box(super::x25519::x25519(black_box(&lo), &base));
            },
            || {
                black_box(super::x25519::x25519(black_box(&hi), &base));
            },
        );
        // Two seeds: a signature's cost must not depend on the key.
        flat(
            "ed25519 sign",
            3_000,
            || {
                black_box(super::ed25519::sign(black_box(&[1u8; 32]), b"the message"));
            },
            || {
                black_box(super::ed25519::sign(black_box(&[0xfeu8; 32]), b"the message"));
            },
        );
        // The field: a product and an inversion of a small element against
        // one of every bit, and the scalar reduction of a small value
        // against a large one -- the arithmetic under every signature.
        let small = super::fe25519::Fe::from_u64(3);
        let big = super::fe25519::Fe::from_bytes(&[0x7fu8; 32]);
        flat(
            "fe25519 mul",
            50_000,
            || {
                black_box(black_box(small).mul(black_box(small)));
            },
            || {
                black_box(black_box(big).mul(black_box(big)));
            },
        );
        flat(
            "fe25519 invert",
            3_000,
            || {
                black_box(black_box(small).invert());
            },
            || {
                black_box(black_box(big).invert());
            },
        );
        let mut low = [0u8; 64];
        low[0] = 1;
        let high = [0xffu8; 64];
        flat(
            "sc25519 reduce_512",
            10_000,
            || {
                black_box(super::sc25519::reduce_512(black_box(&low)));
            },
            || {
                black_box(super::sc25519::reduce_512(black_box(&high)));
            },
        );
        // A ticket wrong in the first byte of its tag against one wrong in
        // the last: the open must reject both in the same time. (A ticket
        // that opens takes longer than one that does not -- the AEAD
        // decrypts only after the tag verifies -- and that difference is
        // the outcome the peer sees anyway, not a secret.)
        let tkey = [6u8; 32];
        let ticket = super::tls13::seal_ticket(&tkey, &[8u8; 32], 1_700_000_000, 5).unwrap();
        let last = ticket.len() - 1;
        let mut wrong_first = ticket.clone();
        wrong_first[last - 15] ^= 1;
        let mut wrong_last = ticket.clone();
        wrong_last[last] ^= 1;
        flat(
            "ticket open, a wrong tag",
            20_000,
            || {
                black_box(super::tls13::open_ticket(&tkey, black_box(&wrong_first)));
            },
            || {
                black_box(super::tls13::open_ticket(&tkey, black_box(&wrong_last)));
            },
        );
        // A tag wrong in its first byte against one wrong in its last: a
        // compare that stopped at the first difference would show.
        let key = [3u8; 32];
        let nonce = [4u8; 12];
        let mut data = [5u8; 256];
        let tag = super::chacha20poly1305::seal(&key, &nonce, b"", &mut data);
        let mut first = tag;
        first[0] ^= 1;
        let mut last = tag;
        last[15] ^= 1;
        flat(
            "aead open, a wrong tag",
            50_000,
            || {
                let mut d = data;
                black_box(super::chacha20poly1305::open(
                    &key,
                    &nonce,
                    b"",
                    &mut d,
                    black_box(&first),
                ));
            },
            || {
                let mut d = data;
                black_box(super::chacha20poly1305::open(
                    &key,
                    &nonce,
                    b"",
                    &mut d,
                    black_box(&last),
                ));
            },
        );
    }
}
