//! The primitives an in-tree TLS 1.3 needs, and nothing more: SHA-256 (the
//! S3 signer's, shared), SHA-512, HMAC and HKDF, ChaCha20-Poly1305, and the
//! curve25519 pair — X25519 for key agreement, Ed25519 for signatures. Each
//! is written from its RFC and pinned against the vectors published there;
//! none is a port of another library.
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

pub mod chacha20poly1305;
pub mod der;
pub mod ed25519;
pub mod fe25519;
pub mod hkdf;
pub mod pem;
pub mod random;
pub mod sc25519;
pub mod sha2;
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
    diff == 0
}
