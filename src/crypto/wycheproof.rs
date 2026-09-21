//! Wycheproof's vectors (Google's, now C2SP's; Apache-2.0, the files under
//! `tests/wycheproof/` as published), a second source beside the RFCs':
//! the edge cases a reference vector never reaches -- small-order and
//! non-canonical points, a signature bent every way, non-minimal DER, a
//! tag a byte short, a length at every boundary. Every file runs whole.

use super::{chacha20poly1305 as aead, ed25519, hkdf, p256, rsa, x25519};
use crate::{json, Value};

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

fn int(v: &Value, key: &str) -> i64 {
    match v.get(key) {
        Some(Value::Int(n)) => *n,
        _ => -1,
    }
}

/// `(group, test)` for every test in the file.
fn tests(file: &str) -> Vec<(Value, Value)> {
    let v = json::parse(file).expect("a Wycheproof file parses");
    let mut out = Vec::new();
    for g in v.get("testGroups").and_then(Value::as_array).unwrap_or(&[]) {
        for t in g.get("tests").and_then(Value::as_array).unwrap_or(&[]) {
            out.push((g.clone(), t.clone()));
        }
    }
    assert!(out.len() > 50, "{} tests in the file", out.len());
    out
}

fn name(t: &Value) -> String {
    format!("tcId {} ({})", int(t, "tcId"), text(t, "comment"))
}

#[test]
fn x25519_agrees_with_wycheproof_on_every_vector() {
    let mut checked = 0;
    for (_, t) in tests(include_str!("../../tests/wycheproof/x25519_test.json")) {
        let (public, private, shared) =
            (unhex(text(&t, "public")), unhex(text(&t, "private")), unhex(text(&t, "shared")));
        if public.len() != 32 || private.len() != 32 {
            continue;
        }
        let got = x25519::x25519(&private.try_into().unwrap(), &public.try_into().unwrap());
        // "acceptable" is a low-order or non-canonical public key: the
        // computation is still the RFC's, and the caller (the TLS) refuses
        // the zero it yields.
        assert_eq!(got.to_vec(), shared, "{}", name(&t));
        checked += 1;
    }
    assert!(checked >= 500, "{checked} vectors");
}

#[test]
fn ed25519_agrees_with_wycheproof_on_every_vector() {
    let mut checked = 0;
    for (g, t) in tests(include_str!("../../tests/wycheproof/ed25519_test.json")) {
        let pk = unhex(text(g.get("publicKey").unwrap(), "pk"));
        let pk: [u8; 32] = pk.try_into().expect("a 32-byte key");
        let msg = unhex(text(&t, "msg"));
        let sig = unhex(text(&t, "sig"));
        let valid = text(&t, "result") == "valid";
        let ok = match <[u8; 64]>::try_from(sig.as_slice()) {
            Ok(s) => ed25519::verify(&pk, &msg, &s),
            Err(_) => false,
        };
        assert_eq!(ok, valid, "{}", name(&t));
        checked += 1;
    }
    assert!(checked >= 140, "{checked} vectors");
}

#[test]
fn chacha20_poly1305_agrees_with_wycheproof_on_every_vector() {
    let mut checked = 0;
    for (g, t) in tests(include_str!("../../tests/wycheproof/chacha20_poly1305_test.json")) {
        let (key, iv, aad) =
            (unhex(text(&t, "key")), unhex(text(&t, "iv")), unhex(text(&t, "aad")));
        let (msg, ct, tag) =
            (unhex(text(&t, "msg")), unhex(text(&t, "ct")), unhex(text(&t, "tag")));
        let valid = text(&t, "result") == "valid";
        let sizes = (int(&g, "keySize"), int(&g, "ivSize"), int(&g, "tagSize"));
        if sizes != (256, 96, 128) {
            // The AEAD has one key, nonce and tag size; another size is
            // refused by the type before any byte is looked at.
            assert!(!valid, "{}: another size cannot be valid", name(&t));
            continue;
        }
        let key: [u8; 32] = key.try_into().unwrap();
        let iv: [u8; 12] = iv.try_into().unwrap();
        let tag: [u8; 16] = tag.try_into().unwrap();
        let mut data = ct.clone();
        let opened = aead::open(&key, &iv, &aad, &mut data, &tag);
        assert_eq!(opened, valid, "{}", name(&t));
        if valid {
            assert_eq!(data, msg, "{}", name(&t));
            let mut sealed = msg.clone();
            let t2 = aead::seal(&key, &iv, &aad, &mut sealed);
            assert!(sealed == ct && t2 == tag, "{}: seal", name(&t));
        }
        checked += 1;
    }
    assert!(checked >= 300, "{checked} vectors");
}

#[test]
fn hkdf_sha256_agrees_with_wycheproof_on_every_vector() {
    let mut checked = 0;
    for (_, t) in tests(include_str!("../../tests/wycheproof/hkdf_sha256_test.json")) {
        let (ikm, salt, info) =
            (unhex(text(&t, "ikm")), unhex(text(&t, "salt")), unhex(text(&t, "info")));
        let size = int(&t, "size") as usize;
        let okm = unhex(text(&t, "okm"));
        if text(&t, "result") != "valid" {
            // The one invalid shape is a size past 255 blocks, which the
            // expander refuses by returning short.
            assert!(size > 255 * 32, "{}", name(&t));
            continue;
        }
        let prk = hkdf::extract(&salt, &ikm);
        assert_eq!(hkdf::expand(&prk, &info, size), okm, "{}", name(&t));
        checked += 1;
    }
    assert!(checked >= 80, "{checked} vectors");
}

#[test]
fn ecdsa_p256_agrees_with_wycheproof_on_every_vector() {
    let mut checked = 0;
    for (g, t) in tests(include_str!("../../tests/wycheproof/ecdsa_secp256r1_sha256_test.json")) {
        let point = unhex(text(g.get("publicKey").unwrap(), "uncompressed"));
        let pk = p256::PublicKey::from_uncompressed(&point).expect("a P-256 point");
        let msg = unhex(text(&t, "msg"));
        let sig = unhex(text(&t, "sig"));
        let valid = text(&t, "result") == "valid";
        assert_eq!(pk.verify_sha256_der(&msg, &sig), valid, "{}", name(&t));
        checked += 1;
    }
    assert!(checked >= 450, "{checked} vectors");
}

#[test]
fn rsa_pss_agrees_with_wycheproof_on_every_vector() {
    use super::bignum::Big;
    let mut checked = 0;
    for (g, t) in
        tests(include_str!("../../tests/wycheproof/rsa_pss_2048_sha256_mgf1_32_test.json"))
    {
        let key = g.get("publicKey").unwrap();
        let pk = rsa::PublicKey {
            n: Big::from_be_bytes(&unhex(text(key, "modulus"))),
            e: Big::from_be_bytes(&unhex(text(key, "publicExponent"))),
        };
        let msg = unhex(text(&t, "msg"));
        let sig = unhex(text(&t, "sig"));
        let valid = text(&t, "result") == "valid";
        assert_eq!(pk.verify_pss_sha256(&msg, &sig), valid, "{}", name(&t));
        checked += 1;
    }
    assert!(checked >= 100, "{checked} vectors");
}
