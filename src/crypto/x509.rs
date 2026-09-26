//! X.509, RFC 5280 as far as a node needs it: parse a certificate, verify
//! a leaf against a CA by signature, validity, issuer and name, and build
//! a CA and a leaf for `celastro tls init`. A node's own certificate is
//! Ed25519, the one scheme this crate signs with; a chain another issuer
//! signed may be RSA (PKCS#1 v1.5, SHA-256) or ECDSA P-256 (SHA-256) at any
//! link, since those are only verified.

use crate::error::{Error, Result};

use super::bignum::Big;
use super::der::{self, BIT_STRING, BOOLEAN, INTEGER, OCTET_STRING, OID, SEQUENCE, SET};
use super::{ed25519, p256, pem, random, rsa};

const OID_CN: &[u8] = &[0x55, 0x04, 0x03];
const OID_SAN: &[u8] = &[0x55, 0x1d, 0x11];
const OID_BASIC_CONSTRAINTS: &[u8] = &[0x55, 0x1d, 0x13];
const OID_EXT_KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x25];
const OID_KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x0f];
/// The most certificates a peer's chain may carry, the leaf included. A
/// real chain is two to four; every link past the leaf is a signature
/// check on a key the peer chose.
const MAX_CHAIN: usize = 8;
/// `subjectKeyIdentifier` and `authorityKeyIdentifier`: what lets a
/// client that keeps two CAs under one name -- a rotation's middle step
/// -- pick the one that signed a leaf. The identifier is the leading 160
/// bits of the SHA-256 of the public key (RFC 7093, method 1).
const OID_SKID: &[u8] = &[0x55, 0x1d, 0x0e];
const OID_AKID: &[u8] = &[0x55, 0x1d, 0x23];

/// A key's identifier, as the two extensions carry it.
pub fn key_identifier(public: &[u8]) -> Vec<u8> {
    super::sha2::sha256(public)[..20].to_vec()
}
const OID_SERVER_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01];
const OID_CLIENT_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02];
/// 1.2.840.113549.1.1.1, rsaEncryption; 1.2.840.113549.1.1.11, sha256WithRSAEncryption.
const OID_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];
const OID_RSA_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b];
/// 1.2.840.10045.2.1, id-ecPublicKey; 1.2.840.10045.3.1.7, prime256v1;
/// 1.2.840.10045.4.3.2, ecdsa-with-SHA256.
const OID_EC: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01];
const OID_P256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];
const OID_ECDSA_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02];

/// A subject's public key, by algorithm.
#[derive(Debug, Clone)]
pub enum PublicKey {
    Ed25519([u8; 32]),
    Rsa(rsa::PublicKey),
    P256(p256::PublicKey),
}

/// How a certificate is signed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigAlg {
    Ed25519,
    RsaSha256,
    EcdsaSha256,
    /// A trust anchor's, when it is not one of the three: never verified,
    /// so never matched by `signed_by`.
    Unverified,
}

fn bad(what: &str) -> Error {
    Error::Plan(format!("certificate: {what}"))
}

/// An optional DER BOOLEAN: absent is false, and present it is the one
/// byte `0x00` or `0xff` (X.690 §11.1) -- any other value is BER, which
/// a DER certificate does not carry, and validators disagree on.
fn der_bool(flag: Option<&[u8]>) -> Result<bool> {
    match flag {
        None => Ok(false),
        Some([0x00]) => Ok(false),
        Some([0xff]) => Ok(true),
        Some(_) => Err(bad("a BOOLEAN that is not 0x00 or 0xff")),
    }
}

/// What a certificate says, as far as verification reads it.
#[derive(Debug, Clone)]
pub struct Certificate {
    /// The whole certificate, DER.
    pub der: Vec<u8>,
    /// The signed part, DER, as bytes to verify the signature over.
    tbs: Vec<u8>,
    /// `issuer` and `subject` as their raw `Name` DER, compared byte for byte.
    issuer: Vec<u8>,
    subject: Vec<u8>,
    /// Seconds since the epoch.
    pub not_before: i64,
    pub not_after: i64,
    pub public_key: PublicKey,
    sig_alg: SigAlg,
    signature: Vec<u8>,
    pub dns_names: Vec<String>,
    pub ip_addresses: Vec<Vec<u8>>,
    pub is_ca: bool,
    /// The key usage extension, when present.
    pub key_usage: Option<KeyUsage>,
    /// The extended key usage extension, when present.
    pub ext_key_usage: Option<ExtKeyUsage>,
}

/// Parse a DER certificate: one whose signature this build can verify.
pub fn parse(der_bytes: &[u8]) -> Result<Certificate> {
    parse_as(der_bytes, false)
}

/// Parse a trust anchor. An anchor's own signature is never verified --
/// it is trusted by being in the list -- so the algorithm it was signed
/// with need not be one this build has: a system root self-signed with
/// SHA-384 (most of them) still anchors a chain whose links are SHA-256.
/// Its key must still be one this build verifies with.
pub fn parse_anchor(der_bytes: &[u8]) -> Result<Certificate> {
    parse_as(der_bytes, true)
}

fn parse_as(der_bytes: &[u8], anchor: bool) -> Result<Certificate> {
    let (cert, rest) = der::expect(der_bytes, SEQUENCE)?;
    if !rest.is_empty() {
        return Err(bad("bytes after the certificate"));
    }
    let (_, tbs_body, after_tbs) = der::read(cert)?;
    let tbs_len = cert.len() - after_tbs.len();
    let tbs = cert[..tbs_len].to_vec();
    let (sig_alg_der, after_alg) = der::expect(after_tbs, SEQUENCE)?;
    let sig_alg = match signature_algorithm(sig_alg_der) {
        Ok(a) => a,
        Err(_) if anchor => SigAlg::Unverified,
        Err(e) => return Err(e),
    };
    let (sig_bits, after_sig) = der::expect(after_alg, BIT_STRING)?;
    if !after_sig.is_empty() {
        return Err(bad("bytes after the signature"));
    }
    if sig_bits.is_empty() || sig_bits[0] != 0 {
        return Err(bad("a signature with unused bits"));
    }
    let signature = sig_bits[1..].to_vec();
    if sig_alg == SigAlg::Ed25519 && signature.len() != 64 {
        return Err(bad("an Ed25519 signature is 64 bytes"));
    }

    // TBSCertificate.
    let (version, rest) = der::optional(tbs_body, 0xa0)?;
    if let Some(v) = version {
        let (n, _) = der::expect(v, INTEGER)?;
        if n != [2] {
            return Err(bad("only version 3 certificates carry names"));
        }
    }
    let (_serial, rest) = der::expect(rest, INTEGER)?;
    let (tbs_alg, rest) = der::expect(rest, SEQUENCE)?;
    // The same AlgorithmIdentifier inside and out (RFC 5280 §4.1.1.2),
    // compared as bytes, so it holds for an anchor's algorithm this build
    // does not name too.
    if tbs_alg != sig_alg_der {
        return Err(bad("the signed part names another signature algorithm than the signature"));
    }
    let (_, issuer_body, after_issuer) = der::read(rest)?;
    let issuer = rest[..rest.len() - after_issuer.len()].to_vec();
    let _ = issuer_body;
    let (validity, rest) = der::expect(after_issuer, SEQUENCE)?;
    let (not_before, v_rest) = time(validity)?;
    let (not_after, _) = time(v_rest)?;
    let (_, _subject_body, after_subject) = der::read(rest)?;
    let subject = rest[..rest.len() - after_subject.len()].to_vec();
    let (spki, rest) = der::expect(after_subject, SEQUENCE)?;
    let public_key = public_key(spki)?;
    let mut dns_names = Vec::new();
    let mut ip_addresses = Vec::new();
    let mut is_ca = false;
    let mut key_usage = None;
    let mut ext_key_usage = None;
    // Skip issuerUniqueID / subjectUniqueID if present ([1], [2]).
    let (_, rest) = der::optional(rest, 0x81)?;
    let (_, rest) = der::optional(rest, 0x82)?;
    let (extensions, _) = der::optional(rest, 0xa3)?;
    if let Some(ext_wrapper) = extensions {
        let (mut exts, _) = der::expect(ext_wrapper, SEQUENCE)?;
        let mut seen: Vec<&[u8]> = Vec::new();
        while !exts.is_empty() {
            let (ext, rest) = der::expect(exts, SEQUENCE)?;
            exts = rest;
            let (oid, ext_rest) = der::expect(ext, OID)?;
            // RFC 5280 §4.2: an extension appears once. Twice, two
            // validators may read two certificates (the first cA=TRUE and
            // the second cA=FALSE, say), so the certificate is refused
            // rather than read one way.
            if seen.contains(&oid) {
                return Err(bad(&format!("an extension given twice ({})", super::hex(oid))));
            }
            seen.push(oid);
            let (critical, ext_rest) = der::optional(ext_rest, BOOLEAN)?;
            let critical = der_bool(critical)?;
            let (value, _) = der::expect(ext_rest, OCTET_STRING)?;
            if oid == OID_KEY_USAGE {
                // A BIT STRING: the unused-bit count, then the bits from the
                // most significant of the first byte; digitalSignature is
                // bit 0 and keyCertSign bit 5.
                let (bits, _) = der::expect(value, BIT_STRING)?;
                let byte0 = bits.get(1).copied().unwrap_or(0);
                key_usage = Some(KeyUsage {
                    digital_signature: byte0 & 0x80 != 0,
                    key_cert_sign: byte0 & 0x04 != 0,
                });
            } else if oid == OID_EXT_KEY_USAGE {
                let (mut purposes, _) = der::expect(value, SEQUENCE)?;
                let mut eku = ExtKeyUsage::default();
                while !purposes.is_empty() {
                    let (purpose, rest) = der::expect(purposes, OID)?;
                    purposes = rest;
                    eku.server_auth |= purpose == OID_SERVER_AUTH;
                    eku.client_auth |= purpose == OID_CLIENT_AUTH;
                }
                ext_key_usage = Some(eku);
            } else if oid == OID_SAN {
                let (mut names, _) = der::expect(value, SEQUENCE)?;
                while !names.is_empty() {
                    let (tag, body, rest) = der::read(names)?;
                    names = rest;
                    match tag {
                        0x82 => dns_names.push(String::from_utf8_lossy(body).to_ascii_lowercase()),
                        0x87 => ip_addresses.push(body.to_vec()),
                        _ => {}
                    }
                }
            } else if oid == OID_BASIC_CONSTRAINTS {
                let (bc, _) = der::expect(value, SEQUENCE)?;
                let (flag, _) = der::optional(bc, BOOLEAN)?;
                is_ca = der_bool(flag)?;
            } else if critical && oid != OID_SKID && oid != OID_AKID {
                // RFC 5280 §4.2: an extension marked critical that this
                // parser does not understand -- name or policy constraints,
                // say -- is a certificate it must not accept, since the
                // constraint would go unenforced.
                return Err(bad(&format!(
                    "a critical extension this parser does not understand ({})",
                    super::hex(oid)
                )));
            }
        }
    }
    Ok(Certificate {
        der: der_bytes.to_vec(),
        tbs,
        issuer,
        subject,
        not_before,
        not_after,
        public_key,
        sig_alg,
        signature,
        dns_names,
        ip_addresses,
        is_ca,
        key_usage,
        ext_key_usage,
    })
}

/// The key usage bits a certificate carries, when it carries the extension.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyUsage {
    pub digital_signature: bool,
    pub key_cert_sign: bool,
}

/// The extended key usages, when the extension is present.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExtKeyUsage {
    pub server_auth: bool,
    pub client_auth: bool,
}

/// What a chain is verified for: the purpose the leaf's extended key
/// usage must name when the extension is there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    ServerAuth,
    ClientAuth,
}

fn expect_ed25519_algorithm(alg: &[u8]) -> Result<()> {
    let (oid, _) = der::expect(alg, OID)?;
    if oid != der::ED25519_OID {
        return Err(bad("only an Ed25519 key is read here; this one is another algorithm"));
    }
    Ok(())
}

/// The signature algorithm an `AlgorithmIdentifier` names, of the three
/// this build verifies.
fn signature_algorithm(alg: &[u8]) -> Result<SigAlg> {
    let (oid, _) = der::expect(alg, OID)?;
    match oid {
        o if o == der::ED25519_OID => Ok(SigAlg::Ed25519),
        o if o == OID_RSA_SHA256 => Ok(SigAlg::RsaSha256),
        o if o == OID_ECDSA_SHA256 => Ok(SigAlg::EcdsaSha256),
        _ => Err(bad("the signature algorithm is not one this build verifies (Ed25519, \
             sha256WithRSAEncryption, ecdsa-with-SHA256)")),
    }
}

/// A `SubjectPublicKeyInfo` of the three kinds this build reads.
fn public_key(spki: &[u8]) -> Result<PublicKey> {
    let (alg, rest) = der::expect(spki, SEQUENCE)?;
    let (oid, alg_rest) = der::expect(alg, OID)?;
    let (key_bits, _) = der::expect(rest, BIT_STRING)?;
    if key_bits.is_empty() || key_bits[0] != 0 {
        return Err(bad("a public key with unused bits"));
    }
    let key = &key_bits[1..];
    match oid {
        o if o == der::ED25519_OID => {
            if key.len() != 32 {
                return Err(bad("an Ed25519 public key is 32 bytes"));
            }
            let mut k = [0u8; 32];
            k.copy_from_slice(key);
            Ok(PublicKey::Ed25519(k))
        }
        o if o == OID_RSA => {
            let (seq, _) = der::expect(key, SEQUENCE)?;
            let (n, seq_rest) = der::expect(seq, INTEGER)?;
            let (e, _) = der::expect(seq_rest, INTEGER)?;
            let n = Big::from_be_bytes(n);
            let e = Big::from_be_bytes(e);
            // The exponent is bounded too: a verification costs a modular
            // multiplication per bit of it, and a certificate an attacker
            // made can carry one as long as the modulus -- minutes of a
            // core per signature, before any token is seen. Every real
            // exponent fits in 32 bits (65537 is the one in use).
            // And it is odd and at least 3: with `e = 1` the "signature"
            // is the encoding itself, and an even exponent is no
            // permutation at all -- neither is a key, only a key-shaped
            // way to make every well-formed encoding verify.
            let e_small = if e.bits() > 32 {
                0
            } else {
                e.to_be_bytes(4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]])).unwrap_or(0)
            };
            if n.bits() < 2048 || n.bits() > 8192 || e_small < 3 || e_small % 2 == 0 {
                return Err(bad(
                    "an RSA key outside 2048 to 8192 bits, or with an exponent that is even, \
                     under 3 or past 32 bits",
                ));
            }
            Ok(PublicKey::Rsa(rsa::PublicKey { n, e }))
        }
        o if o == OID_EC => {
            let (curve, _) = der::expect(alg_rest, OID)?;
            if curve != OID_P256 {
                return Err(bad("an EC key on a curve other than P-256"));
            }
            let pk = p256::PublicKey::from_uncompressed(key)
                .ok_or_else(|| bad("a P-256 key that is not an uncompressed point on the curve"))?;
            Ok(PublicKey::P256(pk))
        }
        _ => Err(bad("the public key is not one this build reads (Ed25519, RSA, P-256)")),
    }
}

/// A `Time`: UTCTime `YYMMDDHHMMSSZ` or GeneralizedTime `YYYYMMDDHHMMSSZ`,
/// as seconds since the epoch.
fn time(input: &[u8]) -> Result<(i64, &[u8])> {
    let (tag, body, rest) = der::read(input)?;
    // Digits and a final `Z`, checked byte by byte before anything is
    // sliced: the check used to be "valid UTF-8", which a multibyte
    // character passes, and a slice through the middle of one is a panic
    // -- in a release build, the process -- reachable by any peer that
    // presents a certificate, before it is verified (fixed in 0.87.0).
    // A sign or a space is not a digit either, whatever `parse` makes of it.
    let (digits, z) = match body.split_last() {
        Some((&b'Z', digits)) if digits.iter().all(u8::is_ascii_digit) => (digits, true),
        _ => (body, false),
    };
    if !z {
        return Err(bad("a validity time that is not digits ending in Z"));
    }
    let s = std::str::from_utf8(digits).map_err(|_| bad("a time that is not ASCII"))?;
    let (year, tail) = match tag {
        der::UTC_TIME if s.len() == 12 => {
            let yy: i64 = s[..2].parse().map_err(|_| bad("a bad UTCTime"))?;
            (if yy < 50 { 2000 + yy } else { 1900 + yy }, &s[2..])
        }
        der::GENERALIZED_TIME if s.len() == 14 => {
            (s[..4].parse().map_err(|_| bad("a bad GeneralizedTime"))?, &s[4..])
        }
        _ => return Err(bad("a validity time in a form this build does not read")),
    };
    let num =
        |a: usize, b: usize| -> Result<u32> { tail[a..b].parse().map_err(|_| bad("a bad time")) };
    let (m, d, hh, mm, ss) = (num(0, 2)?, num(2, 4)?, num(4, 6)?, num(6, 8)?, num(8, 10)?);
    // A month, day or time of day outside the calendar is not a time the
    // arithmetic below can be trusted with (a leap second, 60, is allowed).
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return Err(bad("a validity time outside the calendar"));
    }
    let days = crate::time::days_from_civil(year, m, d);
    Ok((days * 86_400 + hh as i64 * 3600 + mm as i64 * 60 + ss as i64, rest))
}

/// Why a certificate is not accepted; each names the check that failed.
fn refuse(what: String) -> Error {
    Error::Plan(format!("certificate refused: {what}"))
}

impl Certificate {
    /// Whether `ca` signed this certificate: issuer named, signature over
    /// the signed part good. The CA's own `basicConstraints` must say CA
    /// unless it is the trust anchor itself, which the caller decides.
    pub fn signed_by(&self, ca: &Certificate) -> bool {
        if self.issuer != ca.subject {
            return false;
        }
        match (&self.sig_alg, &ca.public_key) {
            (SigAlg::Ed25519, PublicKey::Ed25519(pk)) => {
                let mut sig = [0u8; 64];
                if self.signature.len() != 64 {
                    return false;
                }
                sig.copy_from_slice(&self.signature);
                ed25519::verify(pk, &self.tbs, &sig)
            }
            (SigAlg::RsaSha256, PublicKey::Rsa(pk)) => {
                pk.verify_pkcs1_sha256(&self.tbs, &self.signature)
            }
            (SigAlg::EcdsaSha256, PublicKey::P256(pk)) => {
                pk.verify_sha256_der(&self.tbs, &self.signature)
            }
            _ => false,
        }
    }

    /// The Ed25519 key, when that is what the subject holds.
    pub fn ed25519_key(&self) -> Option<&[u8; 32]> {
        match &self.public_key {
            PublicKey::Ed25519(k) => Some(k),
            _ => None,
        }
    }

    /// Valid at `now` (seconds since the epoch).
    pub fn valid_at(&self, now: i64) -> bool {
        self.not_before <= now && now <= self.not_after
    }

    /// Whether this certificate may sign others: its key usage names
    /// keyCertSign, or it names nothing (RFC 5280 §4.2.1.3).
    pub fn may_sign_certificates(&self) -> bool {
        self.key_usage.map_or(true, |k| k.key_cert_sign)
    }

    /// Refused unless the leaf's extensions allow `purpose`: the extended
    /// key usage, when present, must name it (§4.2.1.12), and the key
    /// usage, when present, must allow a signature (§4.2.1.3) -- the TLS
    /// only ever asks a certificate to sign.
    pub fn fit_for(&self, purpose: Purpose) -> Result<()> {
        if let Some(k) = self.key_usage {
            if !k.digital_signature {
                return Err(refuse(
                    "the certificate's key usage does not allow a signature".into(),
                ));
            }
        }
        if let Some(e) = self.ext_key_usage {
            let (named, what) = match purpose {
                Purpose::ServerAuth => (e.server_auth, "server authentication"),
                Purpose::ClientAuth => (e.client_auth, "client authentication"),
            };
            if !named {
                return Err(refuse(format!(
                    "the certificate's extended key usage does not name {what}"
                )));
            }
        }
        Ok(())
    }

    /// Whether the certificate names `host`: a DNS name, case-insensitively,
    /// exactly or by a wildcard in the leftmost label only (`*.example.com`
    /// names `a.example.com`, not `example.com` or `a.b.example.com`, as RFC
    /// 6125 has it), or an IP address literal.
    pub fn names(&self, host: &str) -> bool {
        // An address literal is matched against the certificate's
        // addresses and nothing else: a dNSName spelt like one is not it
        // (RFC 6125 §1.7.2), whatever an issuer put there.
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            let bytes: Vec<u8> = match ip {
                std::net::IpAddr::V4(v) => v.octets().to_vec(),
                std::net::IpAddr::V6(v) => v.octets().to_vec(),
            };
            return self.ip_addresses.contains(&bytes);
        }
        let lower = host.to_ascii_lowercase();
        if self.dns_names.contains(&lower) {
            return true;
        }
        // A wildcard stands for one whole, non-empty leftmost label.
        if let Some((first, parent)) = lower.split_once('.') {
            if !first.is_empty()
                && !parent.is_empty()
                && parent.contains('.')
                && self
                    .dns_names
                    .iter()
                    .any(|n| n.strip_prefix("*.").is_some_and(|rest| rest == parent))
            {
                return true;
            }
        }
        false
    }
}

/// Verify a peer's chain against the trust anchors: the leaf (first) must
/// name `host` and be valid at `now`; each certificate must be signed by
/// the next, or by an anchor; every non-anchor signer must be a CA.
pub fn verify_chain(
    chain: &[Certificate],
    anchors: &[Certificate],
    host: &str,
    now: i64,
) -> Result<()> {
    let leaf = chain.first().ok_or_else(|| refuse("no certificate was presented".into()))?;
    if !leaf.valid_at(now) {
        return Err(refuse(format!("the certificate is not valid at this time ({now})")));
    }
    if !leaf.names(host) {
        return Err(refuse(format!(
            "the certificate does not name `{host}`; it names {:?}{}",
            leaf.dns_names,
            if leaf.ip_addresses.is_empty() {
                String::new()
            } else {
                " and IP addresses".to_string()
            }
        )));
    }
    chain_reaches_anchor_for(chain, anchors, now, Purpose::ServerAuth)
}

/// [`verify_chain`] without the name: the leaf is valid now, fit for
/// `purpose`, and the chain links up to a certificate in `anchors`. What a
/// server checks of a client's certificate, which names no host to match.
pub fn chain_reaches_anchor(
    chain: &[Certificate],
    anchors: &[Certificate],
    now: i64,
) -> Result<()> {
    chain_reaches_anchor_for(chain, anchors, now, Purpose::ClientAuth)
}

/// [`chain_reaches_anchor`] with the leaf's purpose named; a server's chain
/// goes through [`verify_chain`], which names `ServerAuth`.
pub fn chain_reaches_anchor_for(
    chain: &[Certificate],
    anchors: &[Certificate],
    now: i64,
    purpose: Purpose,
) -> Result<()> {
    let leaf = chain.first().ok_or_else(|| refuse("no certificate was presented".into()))?;
    if !leaf.valid_at(now) {
        return Err(refuse(format!("the certificate is not valid at this time ({now})")));
    }
    leaf.fit_for(purpose)?;
    // A chain is walked a signature check per link, and a peer chooses how
    // many links it sends: bounded, so a message of thousands of
    // certificates is refused at once rather than verified for hours.
    if chain.len() > MAX_CHAIN {
        return Err(refuse(format!(
            "a chain of {} certificates; at most {MAX_CHAIN} are read",
            chain.len()
        )));
    }
    let mut current = leaf;
    for depth in 0..chain.len().max(1) {
        if anchors.iter().any(|a| current.signed_by(a) && a.valid_at(now)) {
            return Ok(());
        }
        let Some(next) = chain.get(depth + 1) else { break };
        // An intermediate is a CA whose key usage, when it names any,
        // allows it to sign certificates.
        if !(next.is_ca
            && next.valid_at(now)
            && next.may_sign_certificates()
            && current.signed_by(next))
        {
            return Err(refuse(
                "the chain does not link: a certificate is not signed by the next".into(),
            ));
        }
        current = next;
    }
    Err(refuse("the chain does not reach a certificate this node trusts".into()))
}

/// A key pair: the 32-byte seed and the public key.
impl Drop for KeyPair {
    fn drop(&mut self) {
        crate::cipher::wipe(&mut self.seed);
    }
}

#[derive(Clone)]
pub struct KeyPair {
    pub seed: [u8; 32],
    pub public: [u8; 32],
}

impl KeyPair {
    pub fn generate() -> Result<KeyPair> {
        let seed = random::array32()?;
        Ok(KeyPair { seed, public: ed25519::public_key(&seed) })
    }

    /// PKCS#8, as `openssl` and cert-manager write an Ed25519 key.
    pub fn to_pkcs8_der(&self) -> Vec<u8> {
        der::sequence(&[
            &der::integer(&[0]),
            &der::ed25519_algorithm(),
            &der::tlv(OCTET_STRING, &der::tlv(OCTET_STRING, &self.seed)),
        ])
    }

    pub fn from_pkcs8_der(bytes: &[u8]) -> Result<KeyPair> {
        let (body, _) = der::expect(bytes, SEQUENCE)?;
        let (_version, rest) = der::expect(body, INTEGER)?;
        let (alg, rest) = der::expect(rest, SEQUENCE)?;
        expect_ed25519_algorithm(alg)
            .map_err(|_| bad("only an Ed25519 private key is read by this build"))?;
        let (outer, _) = der::expect(rest, OCTET_STRING)?;
        let (seed_bytes, _) = der::expect(outer, OCTET_STRING)?;
        if seed_bytes.len() != 32 {
            return Err(bad("an Ed25519 seed is 32 bytes"));
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(seed_bytes);
        Ok(KeyPair { seed, public: ed25519::public_key(&seed) })
    }
}

/// The DER for a `Name` of one `CN`.
fn name(cn: &str) -> Vec<u8> {
    der::sequence(&[&der::tlv(
        SET,
        &der::sequence(&[&der::tlv(OID, OID_CN), &der::tlv(der::UTF8_STRING, cn.as_bytes())]),
    )])
}

/// A validity time: UTCTime before 2050, GeneralizedTime from then on, as
/// RFC 5280 requires.
fn time_der(secs: i64) -> Vec<u8> {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = crate::time::civil_from_days(days);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    if y < 2050 {
        der::tlv(
            der::UTC_TIME,
            format!("{:02}{m:02}{d:02}{hh:02}{mm:02}{ss:02}Z", y % 100).as_bytes(),
        )
    } else {
        der::tlv(
            der::GENERALIZED_TIME,
            format!("{y:04}{m:02}{d:02}{hh:02}{mm:02}{ss:02}Z").as_bytes(),
        )
    }
}

/// What to put in a certificate.
pub struct Spec<'a> {
    pub common_name: &'a str,
    pub dns_names: &'a [String],
    pub ip_addresses: &'a [std::net::IpAddr],
    pub not_before: i64,
    pub not_after: i64,
    pub is_ca: bool,
}

/// Build and sign a certificate for `subject` with `issuer`'s key; the
/// issuer's name is `issuer_cn`. A CA signs itself with its own pair.
pub fn issue(
    spec: &Spec<'_>,
    subject: &KeyPair,
    issuer_cn: &str,
    issuer: &KeyPair,
) -> Result<Vec<u8>> {
    let serial = random::bytes(16)?;
    let mut extensions: Vec<Vec<u8>> = Vec::new();
    let ext = |oid: &[u8], critical: bool, value: Vec<u8>| -> Vec<u8> {
        let mut parts: Vec<Vec<u8>> = vec![der::tlv(OID, oid)];
        if critical {
            parts.push(der::tlv(BOOLEAN, &[0xff]));
        }
        parts.push(der::tlv(OCTET_STRING, &value));
        let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
        der::sequence(&refs)
    };
    if spec.is_ca {
        extensions.push(ext(
            OID_BASIC_CONSTRAINTS,
            true,
            der::sequence(&[&der::tlv(BOOLEAN, &[0xff])]),
        ));
        // keyCertSign and cRLSign, critical: RFC 5280 §4.2.1.3 has a CA
        // name its usage, and a validator that requires it would refuse
        // the CA without.
        extensions.push(ext(OID_KEY_USAGE, true, der::tlv(BIT_STRING, &[1, 0x06])));
    } else {
        extensions.push(ext(OID_BASIC_CONSTRAINTS, true, der::sequence(&[])));
        extensions.push(ext(
            OID_EXT_KEY_USAGE,
            false,
            der::sequence(&[&der::tlv(OID, OID_SERVER_AUTH), &der::tlv(OID, OID_CLIENT_AUTH)]),
        ));
    }
    // The subject's key identifier, and the issuer's as the authority's:
    // a CA signing itself carries the same in both.
    extensions.push(ext(OID_SKID, false, der::tlv(OCTET_STRING, &key_identifier(&subject.public))));
    extensions.push(ext(
        OID_AKID,
        false,
        der::sequence(&[&der::tlv(0x80, &key_identifier(&issuer.public))]),
    ));
    if !spec.dns_names.is_empty() || !spec.ip_addresses.is_empty() {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for n in spec.dns_names {
            names.push(der::tlv(0x82, n.as_bytes()));
        }
        for ip in spec.ip_addresses {
            let bytes: Vec<u8> = match ip {
                std::net::IpAddr::V4(v) => v.octets().to_vec(),
                std::net::IpAddr::V6(v) => v.octets().to_vec(),
            };
            names.push(der::tlv(0x87, &bytes));
        }
        let refs: Vec<&[u8]> = names.iter().map(|p| p.as_slice()).collect();
        extensions.push(ext(OID_SAN, false, der::sequence(&refs)));
    }
    let ext_refs: Vec<&[u8]> = extensions.iter().map(|p| p.as_slice()).collect();
    let spki = der::sequence(&[&der::ed25519_algorithm(), &der::bit_string(&subject.public)]);
    let tbs = der::sequence(&[
        &der::tlv(0xa0, &der::integer(&[2])),
        &der::integer(&serial),
        &der::ed25519_algorithm(),
        &name(issuer_cn),
        &der::sequence(&[&time_der(spec.not_before), &time_der(spec.not_after)]),
        &name(spec.common_name),
        &spki,
        &der::tlv(0xa3, &der::sequence(&ext_refs)),
    ]);
    let signature = ed25519::sign(&issuer.seed, &tbs);
    Ok(der::sequence(&[&tbs, &der::ed25519_algorithm(), &der::bit_string(&signature)]))
}

/// The files `celastro tls init` writes: a CA and a leaf it signed,
/// each as PEM certificate and PKCS#8 key.
pub struct Material {
    pub ca_cert: String,
    pub ca_key: String,
    pub cert: String,
    pub key: String,
}

/// A self-signed CA and a leaf for `common_name` with `dns_names` and
/// `ip_addresses`, valid from an hour ago for `days`.
pub fn make(
    common_name: &str,
    dns_names: &[String],
    ip_addresses: &[std::net::IpAddr],
    days: i64,
) -> Result<Material> {
    let now = crate::time::now_micros() / 1_000_000;
    let ca = KeyPair::generate()?;
    let leaf = KeyPair::generate()?;
    let ca_cn = format!("{common_name} CA");
    let ca_spec = Spec {
        common_name: &ca_cn,
        dns_names: &[],
        ip_addresses: &[],
        not_before: now - 3600,
        not_after: now + days * 86_400,
        is_ca: true,
    };
    let ca_der = issue(&ca_spec, &ca, &ca_cn, &ca)?;
    let leaf_spec = Spec {
        common_name,
        dns_names,
        ip_addresses,
        not_before: now - 3600,
        not_after: now + days * 86_400,
        is_ca: false,
    };
    let leaf_der = issue(&leaf_spec, &leaf, &ca_cn, &ca)?;
    Ok(Material {
        ca_cert: pem::encode("CERTIFICATE", &ca_der),
        ca_key: pem::encode("PRIVATE KEY", &ca.to_pkcs8_der()),
        cert: pem::encode("CERTIFICATE", &leaf_der),
        key: pem::encode("PRIVATE KEY", &leaf.to_pkcs8_der()),
    })
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_certificate_parsing_never_panics() {
        let m = make("localhost", &["a.example".to_string()], &["127.0.0.1".parse().unwrap()], 30)
            .unwrap();
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for pem_text in [&m.cert, &m.ca_cert] {
            samples.extend(crate::crypto::pem::decode_all(pem_text, "CERTIFICATE").unwrap());
        }
        for text in [
            include_str!("../../tests/pki/rsa-ca.crt"),
            include_str!("../../tests/pki/ec-ca.crt"),
            include_str!("../../tests/pki/leaf-by-rsa.crt"),
            include_str!("../../tests/pki/leaf-by-ec.crt"),
        ] {
            samples.extend(crate::crypto::pem::decode_all(text, "CERTIFICATE").unwrap());
        }
        let anchor = parse(&samples[1]).unwrap();
        crate::fuzz::sweep(0x5eed_c0de, &samples, 6000, |b| {
            if let Ok(c) = parse(b) {
                // Whatever parsed is also verified and named without panicking.
                let _ = verify_chain(
                    std::slice::from_ref(&c),
                    std::slice::from_ref(&anchor),
                    "localhost",
                    1_800_000_000,
                );
                let _ = c.names("a.example");
                let _ = c.signed_by(&anchor);
            }
        });
    }
    use super::*;

    #[test]
    fn a_made_chain_parses_verifies_by_name_and_time_and_refuses_what_it_should() {
        let names = vec!["localhost".to_string(), "celastro-0.celastro".to_string()];
        let ips = vec!["127.0.0.1".parse().unwrap()];
        let m = make("celastro", &names, &ips, 365).unwrap();
        let ca_der = pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap().remove(0);
        let leaf_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap().remove(0);
        let ca = parse(&ca_der).unwrap();
        let leaf = parse(&leaf_der).unwrap();
        assert!(ca.is_ca && !leaf.is_ca);
        assert_eq!(
            ca.key_usage,
            Some(KeyUsage { digital_signature: false, key_cert_sign: true }),
            "a CA names keyCertSign (RFC 5280 §4.2.1.3)"
        );
        // The leaf names its issuer's key, and the CA its own, so a client
        // holding two CAs under one name picks the right one.
        let ca_id = key_identifier(ca.ed25519_key().unwrap());
        let skid = [&[0x06, 0x03][..], OID_SKID, &[0x04, 0x16, 0x04, 0x14][..], &ca_id].concat();
        assert!(ca_der.windows(skid.len()).any(|w| w == skid), "the CA carries its key identifier");
        let akid = [&[0x06, 0x03][..], OID_AKID, &[0x04, 0x18, 0x30, 0x16, 0x80, 0x14][..], &ca_id]
            .concat();
        assert!(leaf_der.windows(akid.len()).any(|w| w == akid), "the leaf names the CA's key");
        assert!(ca_der.windows(akid.len()).any(|w| w == akid), "the CA names its own key");
        assert_eq!(leaf.dns_names, names);
        assert_eq!(leaf.ip_addresses, vec![vec![127, 0, 0, 1]]);
        assert!(ca.signed_by(&ca), "a CA signs itself");
        assert!(leaf.signed_by(&ca));
        assert!(!ca.signed_by(&leaf));
        let now = crate::time::now_micros() / 1_000_000;
        let (one, anchors) = (std::slice::from_ref(&leaf), std::slice::from_ref(&ca));
        verify_chain(one, anchors, "localhost", now).unwrap();
        verify_chain(one, anchors, "LOCALHOST", now).unwrap();
        verify_chain(one, anchors, "127.0.0.1", now).unwrap();
        verify_chain(&[leaf.clone(), ca.clone()], anchors, "celastro-0.celastro", now).unwrap();
        let e = verify_chain(one, anchors, "elsewhere.example", now).unwrap_err();
        assert!(e.to_string().contains("does not name"), "{e}");
        let e = verify_chain(one, anchors, "localhost", now + 400 * 86_400).unwrap_err();
        assert!(e.to_string().contains("not valid at this time"), "{e}");
        // Another CA does not anchor this leaf.
        let other = make("other", &names, &ips, 365).unwrap();
        let other_ca = parse(&pem::decode_all(&other.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let e = verify_chain(one, std::slice::from_ref(&other_ca), "localhost", now).unwrap_err();
        assert!(e.to_string().contains("does not reach"), "{e}");
        // A flipped byte in the signed part is refused.
        let mut tampered = leaf_der.clone();
        let i = tampered.len() / 2;
        tampered[i] ^= 1;
        if let Ok(t) = parse(&tampered) {
            assert!(!t.signed_by(&ca));
        }
        // The keys round-trip through PKCS#8.
        let key_der = pem::decode_all(&m.key, "PRIVATE KEY").unwrap().remove(0);
        let kp = KeyPair::from_pkcs8_der(&key_der).unwrap();
        assert_eq!(Some(&kp.public), leaf.ed25519_key());
        // A certificate of another algorithm is refused by name: the same
        // leaf with its signature algorithm's OID (in the signed part and
        // outside it) changed to one this build does not verify. Changed
        // in one place only, the two disagree, which is refused first.
        let ed = [0x06, 0x03, 0x2b, 0x65, 0x70];
        let at: Vec<usize> =
            leaf_der.windows(5).enumerate().filter(|(_, w)| *w == ed).map(|(i, _)| i).collect();
        assert_eq!(at.len(), 3, "the signature algorithm twice and the key's once");
        let mut other_alg = leaf_der.clone();
        for i in [at[0], at[2]] {
            other_alg[i..i + 5].copy_from_slice(&[0x06, 0x03, 0x2b, 0x65, 0x6f]);
        }
        let e = parse(&other_alg).unwrap_err().to_string();
        assert!(e.contains("not one this build"), "{e}");
        let mut disagree = leaf_der.clone();
        disagree[at[0]..at[0] + 5].copy_from_slice(&[0x06, 0x03, 0x2b, 0x65, 0x6f]);
        let e = parse(&disagree).unwrap_err().to_string();
        assert!(e.contains("another signature algorithm"), "{e}");
    }

    fn pki(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/pki/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
    }

    /// What the parser and the chain refuse, from openssl-made material: a
    /// CA carrying a critical extension this parser does not read (a name
    /// constraint) does not parse; a leaf whose extended key usage names
    /// client authentication alone serves no server but is a client; and
    /// a leaf signed by an intermediate whose key usage does not allow
    /// certificate signing does not reach the root through it.
    #[test]
    fn a_critical_extension_a_key_usage_and_a_purpose_are_enforced() {
        let now = crate::time::now_micros() / 1_000_000;
        let der = |f: &str| pem::decode_all(&pki(f), "CERTIFICATE").unwrap().remove(0);
        let e = parse(&der("nc-ca.crt")).unwrap_err().to_string();
        assert!(e.contains("critical extension"), "{e}");
        let client = parse(&der("client-only.crt")).unwrap();
        assert_eq!(
            client.ext_key_usage,
            Some(ExtKeyUsage { server_auth: false, client_auth: true })
        );
        let e = verify_chain(
            std::slice::from_ref(&client),
            std::slice::from_ref(&client),
            "localhost",
            now,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("server authentication"), "{e}");
        chain_reaches_anchor(std::slice::from_ref(&client), std::slice::from_ref(&client), now)
            .unwrap();
        let root = parse(&der("root2.crt")).unwrap();
        let inter = parse(&der("inter-nosign.crt")).unwrap();
        let leaf = parse(&der("leaf-by-inter.crt")).unwrap();
        assert!(root.may_sign_certificates() && !inter.may_sign_certificates());
        assert!(inter.is_ca && leaf.signed_by(&inter) && inter.signed_by(&root));
        let e = verify_chain(
            &[leaf.clone(), inter.clone()],
            std::slice::from_ref(&root),
            "localhost",
            now,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("does not link"), "{e}");
        // The same leaf under an intermediate allowed to sign links: the
        // check is the key usage, nothing else about the chain.
        let ec_ca = parse(&der("ec-ca.crt")).unwrap();
        assert!(ec_ca.may_sign_certificates());
    }

    /// Chains another issuer signed: an RSA-2048 CA and a P-256 CA, made by
    /// openssl, each signing an Ed25519 leaf; and the two signature schemes
    /// TLS 1.3 asks of such servers, over a known message.
    #[test]
    fn rsa_and_p256_chains_and_signatures_from_openssl_verify() {
        let now = crate::time::now_micros() / 1_000_000;
        for (ca_file, leaf_file, alg) in [
            ("rsa-ca.crt", "leaf-by-rsa.crt", SigAlg::RsaSha256),
            ("ec-ca.crt", "leaf-by-ec.crt", SigAlg::EcdsaSha256),
        ] {
            let ca = parse(&pem::decode_all(&pki(ca_file), "CERTIFICATE").unwrap()[0]).unwrap();
            let leaf = parse(&pem::decode_all(&pki(leaf_file), "CERTIFICATE").unwrap()[0]).unwrap();
            assert_eq!(leaf.sig_alg, alg, "{leaf_file}");
            assert!(leaf.ed25519_key().is_some(), "the leaf is Ed25519");
            assert!(ca.is_ca && ca.signed_by(&ca), "{ca_file} signs itself");
            assert!(leaf.signed_by(&ca), "{leaf_file} by {ca_file}");
            verify_chain(std::slice::from_ref(&leaf), std::slice::from_ref(&ca), "localhost", now)
                .unwrap();
            verify_chain(std::slice::from_ref(&leaf), std::slice::from_ref(&ca), "127.0.0.1", now)
                .unwrap();
            let mut tampered = leaf.clone();
            let i = tampered.tbs.len() / 2;
            tampered.tbs[i] ^= 1;
            assert!(!tampered.signed_by(&ca), "a changed signed part is refused");
        }
        let msg = std::fs::read(format!("{}/tests/pki/msg", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let spki_der = pem::decode_all(&pki("rsa-pub.pem"), "PUBLIC KEY").unwrap().remove(0);
        let (spki, _) = der::expect(&spki_der, SEQUENCE).unwrap();
        let rsa_pub = match public_key(spki).unwrap() {
            PublicKey::Rsa(k) => k,
            other => panic!("{other:?}"),
        };
        let pss = pem::base64_decode(pki("pss.sig.b64").trim()).unwrap();
        assert!(rsa_pub.verify_pss_sha256(&msg, &pss));
        assert!(!rsa_pub.verify_pss_sha256(b"another message", &pss));
        assert!(!rsa_pub.verify_pkcs1_sha256(&msg, &pss), "a PSS signature is not a v1.5 one");
        let ec_ca = parse(&pem::decode_all(&pki("ec-ca.crt"), "CERTIFICATE").unwrap()[0]).unwrap();
        let ec_pub = match &ec_ca.public_key {
            PublicKey::P256(k) => k.clone(),
            other => panic!("{other:?}"),
        };
        let ecdsa = pem::base64_decode(pki("ecdsa.sig.b64").trim()).unwrap();
        assert!(ec_pub.verify_sha256_der(&msg, &ecdsa));
        assert!(!ec_pub.verify_sha256_der(b"another message", &ecdsa));
    }

    /// One DER element: a tag, a length in the short or the long form, a
    /// body. For building a key nobody would issue.
    fn tlv(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        let n = body.len();
        if n < 128 {
            out.push(n as u8);
        } else if n < 256 {
            out.extend_from_slice(&[0x81, n as u8]);
        } else {
            out.extend_from_slice(&[0x82, (n >> 8) as u8, n as u8]);
        }
        out.extend_from_slice(body);
        out
    }

    /// An RSA exponent is refused past 32 bits, and a chain past eight
    /// certificates, before any signature is checked: each is a cost the
    /// peer chooses -- a modular multiplication per bit of the exponent, a
    /// verification per link -- and a peer is not yet anyone when its
    /// certificate is read.
    #[test]
    fn a_wide_exponent_and_a_long_chain_are_refused_before_they_are_verified() {
        let spki_der = pem::decode_all(&pki("rsa-pub.pem"), "PUBLIC KEY").unwrap().remove(0);
        let (spki, _) = der::expect(&spki_der, SEQUENCE).unwrap();
        let (alg, key_rest) = der::expect(spki, SEQUENCE).unwrap();
        let (bits, _) = der::expect(key_rest, BIT_STRING).unwrap();
        let (seq, _) = der::expect(&bits[1..], SEQUENCE).unwrap();
        let (n, _) = der::expect(seq, INTEGER).unwrap();
        // The same modulus, with an exponent of the given bytes.
        let with_e = |e: &[u8]| -> Vec<u8> {
            let inner = tlv(SEQUENCE, &[tlv(INTEGER, n), tlv(INTEGER, e)].concat());
            let mut bit_body = vec![0u8];
            bit_body.extend_from_slice(&inner);
            [tlv(SEQUENCE, alg), tlv(BIT_STRING, &bit_body)].concat()
        };
        assert!(public_key(&with_e(&[1, 0, 1])).is_ok(), "65537 is the exponent in use");
        assert!(public_key(&with_e(&[0, 0xff, 0xff, 0xff, 0xff])).is_ok(), "32 bits");
        let e = public_key(&with_e(&[1, 0, 0, 0, 1])).unwrap_err().to_string();
        assert!(e.contains("past 32 bits"), "33 bits: {e}");
        let e = public_key(&with_e(&[0x7f; 256])).unwrap_err().to_string();
        assert!(e.contains("past 32 bits"), "an exponent as wide as the modulus: {e}");
        // And under 3 or even it is not a permutation: with 1 the encoding
        // is its own signature, so every well-formed one would verify.
        for e in [&[1u8][..], &[2u8][..], &[0x01, 0x00, 0x00][..]] {
            let msg = public_key(&with_e(e)).unwrap_err().to_string();
            assert!(msg.contains("even, under 3"), "{e:?}: {msg}");
        }
        assert!(public_key(&with_e(&[3])).is_ok(), "3 is odd and a permutation");

        let now = crate::time::now_micros() / 1_000_000;
        let ca = parse(&pem::decode_all(&pki("rsa-ca.crt"), "CERTIFICATE").unwrap()[0]).unwrap();
        let leaf =
            parse(&pem::decode_all(&pki("leaf-by-rsa.crt"), "CERTIFICATE").unwrap()[0]).unwrap();
        let anchors = std::slice::from_ref(&ca);
        // Eight are read: the leaf reaches the anchor at the first link and
        // the rest are never looked at. Nine are refused unread.
        let eight: Vec<Certificate> = std::iter::repeat(leaf.clone()).take(8).collect();
        verify_chain(&eight, anchors, "localhost", now).unwrap();
        let nine: Vec<Certificate> = std::iter::repeat(leaf.clone()).take(9).collect();
        let e = verify_chain(&nine, anchors, "localhost", now).unwrap_err().to_string();
        assert!(e.contains("a chain of 9 certificates"), "{e}");
    }

    /// A validity time is digits and a `Z`, checked before anything is
    /// sliced: a multibyte character in a thirteen-byte body is valid UTF-8
    /// and used to be sliced through, which is a panic -- and in a release
    /// build the process -- from any peer presenting a certificate. A sign
    /// where `parse` would take one, and a month past twelve, are refused.
    #[test]
    fn a_validity_time_of_anything_but_digits_is_refused_not_a_crash() {
        let t = |body: &[u8]| -> Result<i64> { time(&tlv(der::UTC_TIME, body)).map(|(s, _)| s) };
        let mut multibyte = b"0\xc3\xa9".to_vec(); // "0é", three bytes
        multibyte.extend_from_slice(b"0926120000");
        assert_eq!(multibyte.len(), 13);
        let e = t(&multibyte).unwrap_err().to_string();
        assert!(e.contains("digits ending in Z"), "{e}");
        let e = t(b"+10926120000Z").unwrap_err().to_string();
        assert!(e.contains("digits ending in Z"), "{e}");
        assert!(t(b"260926120000").is_err(), "no Z");
        assert!(t(b"26092612000Z").is_err(), "short");
        let e = t(b"261326120000Z").unwrap_err().to_string();
        assert!(e.contains("outside the calendar"), "month 13: {e}");
        assert!(t(b"260932120000Z").is_err(), "day 32");
        assert!(t(b"260926250000Z").is_err(), "hour 25");
        let secs = t(b"260926120000Z").unwrap();
        assert_eq!(secs, crate::time::days_from_civil(2026, 9, 26) * 86_400 + 12 * 3600);
        let g = tlv(der::GENERALIZED_TIME, b"20260926120000Z");
        let (secs2, rest) = time(&g).unwrap();
        assert!(rest.is_empty());
        assert_eq!(secs, secs2, "the two forms agree on an instant");
        let g = tlv(der::GENERALIZED_TIME, b"2026092612000\xc3\xa9Z");
        assert!(time(&g).is_err());
    }

    /// A certificate whose signature nobody will check, for the parser's
    /// rules about extensions: `exts` in its extension block, a zero
    /// signature.
    fn unsigned_cert(exts: &[Vec<u8>]) -> Vec<u8> {
        let kp = KeyPair::generate().unwrap();
        let spki = der::sequence(&[&der::ed25519_algorithm(), &der::bit_string(&kp.public)]);
        let ext_refs: Vec<&[u8]> = exts.iter().map(|p| p.as_slice()).collect();
        let tbs = der::sequence(&[
            &der::tlv(0xa0, &der::integer(&[2])),
            &der::integer(&[7]),
            &der::ed25519_algorithm(),
            &name("x"),
            &der::sequence(&[&time_der(1_700_000_000), &time_der(1_900_000_000)]),
            &name("x"),
            &spki,
            &der::tlv(0xa3, &der::sequence(&ext_refs)),
        ]);
        der::sequence(&[&tbs, &der::ed25519_algorithm(), &der::bit_string(&[0u8; 64])])
    }

    /// An extension given twice is refused (RFC 5280 §4.2 has each once;
    /// two readers would take two certificates from it), and a BOOLEAN is
    /// `0x00` or `0xff`, not the BER `0x01` validators disagree on.
    #[test]
    fn a_repeated_extension_and_a_ber_boolean_are_refused() {
        let bc = |flag: &[u8]| -> Vec<u8> {
            der::sequence(&[
                &der::tlv(OID, OID_BASIC_CONSTRAINTS),
                &der::tlv(BOOLEAN, &[0xff]),
                &der::tlv(OCTET_STRING, &der::sequence(&[&der::tlv(BOOLEAN, flag)])),
            ])
        };
        let c = parse(&unsigned_cert(&[bc(&[0xff])])).unwrap();
        assert!(c.is_ca);
        let c = parse(&unsigned_cert(&[bc(&[0x00])])).unwrap();
        assert!(!c.is_ca);
        let e = parse(&unsigned_cert(&[bc(&[0xff]), bc(&[0x00])])).unwrap_err().to_string();
        assert!(e.contains("given twice"), "{e}");
        let e = parse(&unsigned_cert(&[bc(&[0x01])])).unwrap_err().to_string();
        assert!(e.contains("not 0x00 or 0xff"), "{e}");
        // The critical flag is a BOOLEAN too.
        let ber_critical = der::sequence(&[
            &der::tlv(OID, OID_BASIC_CONSTRAINTS),
            &der::tlv(BOOLEAN, &[0x01]),
            &der::tlv(OCTET_STRING, &der::sequence(&[])),
        ]);
        let e = parse(&unsigned_cert(&[ber_critical])).unwrap_err().to_string();
        assert!(e.contains("not 0x00 or 0xff"), "{e}");
    }

    /// An address literal matches the certificate's addresses only -- a
    /// dNSName spelt like one is not it -- and a wildcard stands for one
    /// whole, non-empty label.
    #[test]
    fn an_address_literal_and_a_wildcard_match_what_rfc_6125_says() {
        let names = vec!["127.0.0.1".to_string(), "*.example.com".to_string()];
        let m = make("x", &names, &[], 30).unwrap();
        let leaf = parse(&pem::decode_all(&m.cert, "CERTIFICATE").unwrap()[0]).unwrap();
        assert!(!leaf.names("127.0.0.1"), "a dNSName is not an address");
        assert!(leaf.names("a.example.com") && leaf.names("A.Example.COM"));
        assert!(!leaf.names(".example.com"), "an empty label is no label");
        assert!(!leaf.names("example.com") && !leaf.names("a.b.example.com"));
        assert!(!leaf.names("aexample.com") && !leaf.names("example.com."));
        let m = make("x", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let leaf = parse(&pem::decode_all(&m.cert, "CERTIFICATE").unwrap()[0]).unwrap();
        assert!(leaf.names("127.0.0.1") && !leaf.names("127.0.0.2") && !leaf.names("::1"));
    }

    /// A trust anchor self-signed with an algorithm this build does not
    /// have still anchors a chain: its own signature is never verified,
    /// and the links to it are. As a leaf or an intermediate the same
    /// certificate is refused by name.
    #[test]
    fn an_anchor_signed_with_an_unknown_algorithm_still_anchors() {
        let m = make("ca", &["localhost".to_string()], &[], 30).unwrap();
        let mut ca_der = pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap().remove(0);
        let leaf = parse(&pem::decode_all(&m.cert, "CERTIFICATE").unwrap()[0]).unwrap();
        // The Ed25519 OID stands three times in a self-signed CA: the
        // signature algorithm in the signed part, the key's, the outer
        // one. The first and the last become an OID nobody verifies.
        let ed = [0x06, 0x03, 0x2b, 0x65, 0x70];
        let at: Vec<usize> =
            ca_der.windows(5).enumerate().filter(|(_, w)| *w == ed).map(|(i, _)| i).collect();
        assert_eq!(at.len(), 3, "{at:?}");
        for i in [at[0], at[2]] {
            ca_der[i..i + 5].copy_from_slice(&[0x06, 0x03, 0x2b, 0x65, 0x6f]);
        }
        let e = parse(&ca_der).unwrap_err().to_string();
        assert!(e.contains("not one this build"), "{e}");
        let anchor = parse_anchor(&ca_der).unwrap();
        assert_eq!(anchor.sig_alg, SigAlg::Unverified);
        assert!(!anchor.signed_by(&anchor), "its own signature is never verified");
        assert!(leaf.signed_by(&anchor));
        let now = crate::time::now_micros() / 1_000_000;
        verify_chain(std::slice::from_ref(&leaf), std::slice::from_ref(&anchor), "localhost", now)
            .unwrap();
    }
}
