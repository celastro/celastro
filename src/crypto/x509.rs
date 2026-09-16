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
const OID_SERVER_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01];
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
}

fn bad(what: &str) -> Error {
    Error::Plan(format!("certificate: {what}"))
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
}

/// Parse a DER certificate whose keys and signature are Ed25519.
pub fn parse(der_bytes: &[u8]) -> Result<Certificate> {
    let (cert, rest) = der::expect(der_bytes, SEQUENCE)?;
    if !rest.is_empty() {
        return Err(bad("bytes after the certificate"));
    }
    let (_, tbs_body, after_tbs) = der::read(cert)?;
    let tbs_len = cert.len() - after_tbs.len();
    let tbs = cert[..tbs_len].to_vec();
    let (sig_alg_der, after_alg) = der::expect(after_tbs, SEQUENCE)?;
    let sig_alg = signature_algorithm(sig_alg_der)?;
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
    if signature_algorithm(tbs_alg)? != sig_alg {
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
    // Skip issuerUniqueID / subjectUniqueID if present ([1], [2]).
    let (_, rest) = der::optional(rest, 0x81)?;
    let (_, rest) = der::optional(rest, 0x82)?;
    let (extensions, _) = der::optional(rest, 0xa3)?;
    if let Some(ext_wrapper) = extensions {
        let (mut exts, _) = der::expect(ext_wrapper, SEQUENCE)?;
        while !exts.is_empty() {
            let (ext, rest) = der::expect(exts, SEQUENCE)?;
            exts = rest;
            let (oid, ext_rest) = der::expect(ext, OID)?;
            let (_critical, ext_rest) = der::optional(ext_rest, BOOLEAN)?;
            let (value, _) = der::expect(ext_rest, OCTET_STRING)?;
            if oid == OID_SAN {
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
                if let (Some(flag), _) = der::optional(bc, BOOLEAN)? {
                    is_ca = flag.first().copied().unwrap_or(0) != 0;
                }
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
    })
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
            if n.bits() < 2048 || n.bits() > 8192 || e.is_zero() {
                return Err(bad("an RSA key outside 2048 to 8192 bits, or with no exponent"));
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
    let s = std::str::from_utf8(body).map_err(|_| bad("a time that is not ASCII"))?;
    let (year, tail) = match tag {
        der::UTC_TIME if s.len() == 13 => {
            let yy: i64 = s[..2].parse().map_err(|_| bad("a bad UTCTime"))?;
            (if yy < 50 { 2000 + yy } else { 1900 + yy }, &s[2..])
        }
        der::GENERALIZED_TIME if s.len() == 15 => {
            (s[..4].parse().map_err(|_| bad("a bad GeneralizedTime"))?, &s[4..])
        }
        _ => return Err(bad("a validity time in a form this build does not read")),
    };
    if !tail.ends_with('Z') {
        return Err(bad("a validity time not in UTC"));
    }
    let num =
        |a: usize, b: usize| -> Result<u32> { tail[a..b].parse().map_err(|_| bad("a bad time")) };
    let (m, d, hh, mm, ss) = (num(0, 2)?, num(2, 4)?, num(4, 6)?, num(6, 8)?, num(8, 10)?);
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

    /// Whether the certificate names `host`: a DNS name, case-insensitively,
    /// exactly or by a wildcard in the leftmost label only (`*.example.com`
    /// names `a.example.com`, not `example.com` or `a.b.example.com`, as RFC
    /// 6125 has it), or an IP address literal.
    pub fn names(&self, host: &str) -> bool {
        let lower = host.to_ascii_lowercase();
        if self.dns_names.contains(&lower) {
            return true;
        }
        if host.parse::<std::net::IpAddr>().is_err() {
            if let Some((_, parent)) = lower.split_once('.') {
                if !parent.is_empty()
                    && parent.contains('.')
                    && self
                        .dns_names
                        .iter()
                        .any(|n| n.strip_prefix("*.").is_some_and(|rest| rest == parent))
                {
                    return true;
                }
            }
        }
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            let bytes: Vec<u8> = match ip {
                std::net::IpAddr::V4(v) => v.octets().to_vec(),
                std::net::IpAddr::V6(v) => v.octets().to_vec(),
            };
            return self.ip_addresses.contains(&bytes);
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
    let mut current = leaf;
    for depth in 0..chain.len().max(1) {
        if anchors.iter().any(|a| current.signed_by(a) && a.valid_at(now)) {
            return Ok(());
        }
        let Some(next) = chain.get(depth + 1) else { break };
        if !(next.is_ca && next.valid_at(now) && current.signed_by(next)) {
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
    } else {
        extensions.push(ext(OID_BASIC_CONSTRAINTS, true, der::sequence(&[])));
        extensions.push(ext(
            OID_EXT_KEY_USAGE,
            false,
            der::sequence(&[&der::tlv(OID, OID_SERVER_AUTH)]),
        ));
    }
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
        // leaf with its key's OID changed to one this build does not read.
        let mut other_alg = leaf_der.clone();
        let ed = [0x06, 0x03, 0x2b, 0x65, 0x70];
        let pos = other_alg.windows(5).position(|w| w == ed).unwrap();
        other_alg[pos..pos + 5].copy_from_slice(&[0x06, 0x03, 0x2b, 0x65, 0x6f]);
        let e = parse(&other_alg).unwrap_err();
        assert!(e.to_string().contains("not one this build"), "{e}");
    }

    fn pki(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/pki/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
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
}
