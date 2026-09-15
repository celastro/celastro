//! TLS 1.3, RFC 8446, the subset a cluster needs and a stock client
//! speaks: one cipher suite (`TLS_CHACHA20_POLY1305_SHA256`), one group
//! (X25519), one signature scheme (Ed25519), server authentication only. No
//! resumption, no 0-RTT, no client certificates, no HelloRetryRequest, no
//! renegotiation of keys after the handshake. A client that offers none of
//! these is answered with an alert naming why.
//!
//! The record layer, the key schedule and both sides of the handshake are
//! here; [`TlsStream`] wraps a `TcpStream` and completes the handshake on
//! first use, so a listener's accept loop never blocks on a peer.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use super::chacha20poly1305 as aead;
use super::hkdf;
use super::sha2::{hmac_sha256, sha256};
use super::x509::{self, Certificate, KeyPair};
use super::{ed25519, random, x25519};

const SUITE_CHACHA: u16 = 0x1303;
const GROUP_X25519: u16 = 0x001d;
const SIG_ED25519: u16 = 0x0807;
const VERSION_13: u16 = 0x0304;
const LEGACY_VERSION: u16 = 0x0303;
const MAX_PLAINTEXT: usize = 1 << 14;

const CT_CHANGE_CIPHER_SPEC: u8 = 20;
const CT_ALERT: u8 = 21;
const CT_HANDSHAKE: u8 = 22;
const CT_APPLICATION_DATA: u8 = 23;

const HS_CLIENT_HELLO: u8 = 1;
const HS_SERVER_HELLO: u8 = 2;
const HS_NEW_SESSION_TICKET: u8 = 4;
const HS_ENCRYPTED_EXTENSIONS: u8 = 8;
const HS_CERTIFICATE: u8 = 11;
const HS_CERTIFICATE_VERIFY: u8 = 15;
const HS_FINISHED: u8 = 20;
const HS_KEY_UPDATE: u8 = 24;

const EXT_SERVER_NAME: u16 = 0;
const EXT_SUPPORTED_GROUPS: u16 = 10;
const EXT_SIGNATURE_ALGORITHMS: u16 = 13;
const EXT_SUPPORTED_VERSIONS: u16 = 43;
const EXT_KEY_SHARE: u16 = 51;

const ALERT_CLOSE_NOTIFY: u8 = 0;
const ALERT_HANDSHAKE_FAILURE: u8 = 40;
const ALERT_BAD_CERTIFICATE: u8 = 42;
const ALERT_DECRYPT_ERROR: u8 = 51;
const ALERT_PROTOCOL_VERSION: u8 = 70;
const ALERT_INTERNAL_ERROR: u8 = 80;
const ALERT_UNEXPECTED_MESSAGE: u8 = 10;

fn err(what: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("tls: {}", what.into()))
}

// ------------------------------------------------------------ key schedule

fn expand_label(secret: &[u8; 32], label: &str, context: &[u8], len: usize) -> Vec<u8> {
    let full = format!("tls13 {label}");
    let mut info = Vec::with_capacity(4 + full.len() + context.len());
    info.extend_from_slice(&(len as u16).to_be_bytes());
    info.push(full.len() as u8);
    info.extend_from_slice(full.as_bytes());
    info.push(context.len() as u8);
    info.extend_from_slice(context);
    hkdf::expand(secret, &info, len)
}

fn derive_secret(secret: &[u8; 32], label: &str, transcript_hash: &[u8; 32]) -> [u8; 32] {
    let v = expand_label(secret, label, transcript_hash, 32);
    let mut out = [0u8; 32];
    out.copy_from_slice(&v);
    out
}

/// One direction's keys: derived from a traffic secret, used with a
/// sequence number that never repeats within them.
struct Keys {
    key: [u8; 32],
    iv: [u8; 12],
    seq: u64,
}

impl Keys {
    fn from_secret(secret: &[u8; 32]) -> Keys {
        let k = expand_label(secret, "key", &[], 32);
        let iv = expand_label(secret, "iv", &[], 12);
        let mut key = [0u8; 32];
        key.copy_from_slice(&k);
        let mut ivb = [0u8; 12];
        ivb.copy_from_slice(&iv);
        Keys { key, iv: ivb, seq: 0 }
    }

    fn nonce(&mut self) -> [u8; 12] {
        let mut n = self.iv;
        for (i, b) in self.seq.to_be_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        self.seq += 1;
        n
    }
}

fn finished_verify(base: &[u8; 32], transcript_hash: &[u8; 32]) -> [u8; 32] {
    let fk = expand_label(base, "finished", &[], 32);
    let mut key = [0u8; 32];
    key.copy_from_slice(&fk);
    hmac_sha256(&key, transcript_hash)
}

// -------------------------------------------------------------- the stream

/// What the handshake needs from the node: its chain and key when serving,
/// the anchors and the peer's expected name when dialling.
pub struct ServerSide<'a> {
    pub chain_der: &'a [Vec<u8>],
    pub key: &'a KeyPair,
}

pub struct ClientSide<'a> {
    pub anchors: &'a [Certificate],
    pub host: &'a str,
}

enum Role {
    Server { chain_der: Vec<Vec<u8>>, key: KeyPair },
    Client { anchors: Vec<Certificate>, host: String },
}

/// A TCP socket speaking TLS 1.3, the handshake done on first use.
pub struct TlsStream {
    pub sock: TcpStream,
    role: Option<Role>,
    read_keys: Option<Keys>,
    write_keys: Option<Keys>,
    /// Decrypted application data not yet handed to the reader.
    pending: Vec<u8>,
    pending_at: usize,
    /// Bytes of the socket read ahead of a record boundary.
    inbuf: Vec<u8>,
    closed: bool,
}

impl TlsStream {
    pub fn server(sock: TcpStream, side: ServerSide<'_>) -> TlsStream {
        TlsStream::new(
            sock,
            Role::Server { chain_der: side.chain_der.to_vec(), key: side.key.clone() },
        )
    }

    pub fn client(sock: TcpStream, side: ClientSide<'_>) -> TlsStream {
        TlsStream::new(
            sock,
            Role::Client { anchors: side.anchors.to_vec(), host: side.host.to_string() },
        )
    }

    fn new(sock: TcpStream, role: Role) -> TlsStream {
        TlsStream {
            sock,
            role: Some(role),
            read_keys: None,
            write_keys: None,
            pending: Vec::new(),
            pending_at: 0,
            inbuf: Vec::new(),
            closed: false,
        }
    }

    /// Complete the handshake if it has not been done. An error here is
    /// the peer's alert or our own, and the socket is left as it is.
    pub fn handshake(&mut self) -> io::Result<()> {
        let Some(role) = self.role.take() else { return Ok(()) };
        let r = match role {
            Role::Server { chain_der, key } => self.server_handshake(&chain_der, &key),
            Role::Client { anchors, host } => self.client_handshake(&anchors, &host),
        };
        if let Err(e) = &r {
            // Tell the peer, once, and never mind if that fails too.
            let desc = alert_for(e);
            let _ = self.send_alert(desc);
        }
        r
    }

    // ------------------------------------------------------------ records

    /// One record from the socket: its type and its plaintext, decrypted
    /// when the read keys are on.
    fn read_record(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 5];
        self.read_exact_from_sock(&mut header)?;
        let ctype = header[0];
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if len > MAX_PLAINTEXT + 256 {
            return Err(err("a record longer than the protocol allows"));
        }
        let mut body = vec![0u8; len];
        self.read_exact_from_sock(&mut body)?;
        match &mut self.read_keys {
            None => Ok((ctype, body)),
            Some(keys) => {
                if ctype == CT_CHANGE_CIPHER_SPEC {
                    // Middlebox compatibility: a plaintext CCS may arrive
                    // between the hellos and the encrypted flight. Ignored.
                    return Ok((CT_CHANGE_CIPHER_SPEC, body));
                }
                if ctype != CT_APPLICATION_DATA || len < 16 {
                    return Err(err("an unencrypted record after the keys were set"));
                }
                let nonce = keys.nonce();
                let (data, tag) = body.split_at_mut(len - 16);
                let mut tag_arr = [0u8; 16];
                tag_arr.copy_from_slice(tag);
                if !aead::open(&keys.key, &nonce, &header, data, &tag_arr) {
                    return Err(err("a record failed authentication"));
                }
                // Strip the zero padding and read the inner type.
                let mut end = data.len();
                while end > 0 && data[end - 1] == 0 {
                    end -= 1;
                }
                if end == 0 {
                    return Err(err("a record with no content type"));
                }
                let inner = data[end - 1];
                Ok((inner, data[..end - 1].to_vec()))
            }
        }
    }

    fn read_exact_from_sock(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        let take = self.inbuf.len().min(buf.len());
        if take > 0 {
            buf[..take].copy_from_slice(&self.inbuf[..take]);
            self.inbuf.drain(..take);
            filled = take;
        }
        while filled < buf.len() {
            let n = self.sock.read(&mut buf[filled..])?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "tls: the peer closed"));
            }
            filled += n;
        }
        Ok(())
    }

    /// Write one record of `ctype` with `plain`, encrypted when the write
    /// keys are on, in fragments of at most the protocol's maximum.
    fn write_record(&mut self, ctype: u8, plain: &[u8]) -> io::Result<()> {
        let chunks: Vec<&[u8]> = if plain.is_empty() {
            vec![&[][..]]
        } else {
            plain.chunks(MAX_PLAINTEXT - 1).collect()
        };
        for chunk in chunks {
            let mut out = Vec::with_capacity(chunk.len() + 5 + 17);
            match &mut self.write_keys {
                None => {
                    out.push(ctype);
                    out.extend_from_slice(&LEGACY_VERSION.to_be_bytes());
                    out.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
                    out.extend_from_slice(chunk);
                }
                Some(keys) => {
                    let mut inner = Vec::with_capacity(chunk.len() + 1);
                    inner.extend_from_slice(chunk);
                    inner.push(ctype);
                    let len = inner.len() + 16;
                    let header = [
                        CT_APPLICATION_DATA,
                        (LEGACY_VERSION >> 8) as u8,
                        LEGACY_VERSION as u8,
                        (len >> 8) as u8,
                        len as u8,
                    ];
                    let nonce = keys.nonce();
                    let tag = aead::seal(&keys.key, &nonce, &header, &mut inner);
                    out.extend_from_slice(&header);
                    out.extend_from_slice(&inner);
                    out.extend_from_slice(&tag);
                }
            }
            self.sock.write_all(&out)?;
        }
        Ok(())
    }

    fn send_alert(&mut self, description: u8) -> io::Result<()> {
        let level = if description == ALERT_CLOSE_NOTIFY { 1 } else { 2 };
        self.write_record(CT_ALERT, &[level, description])
    }

    /// The next handshake message, across records, appended to the
    /// transcript; CCS records are skipped.
    fn read_handshake(
        &mut self,
        hs_buf: &mut Vec<u8>,
        transcript: &mut Vec<u8>,
    ) -> io::Result<(u8, Vec<u8>)> {
        loop {
            if hs_buf.len() >= 4 {
                let len =
                    ((hs_buf[1] as usize) << 16) | ((hs_buf[2] as usize) << 8) | hs_buf[3] as usize;
                if hs_buf.len() >= 4 + len {
                    let msg: Vec<u8> = hs_buf.drain(..4 + len).collect();
                    transcript.extend_from_slice(&msg);
                    return Ok((msg[0], msg[4..].to_vec()));
                }
            }
            let (ctype, body) = self.read_record()?;
            match ctype {
                CT_HANDSHAKE => hs_buf.extend_from_slice(&body),
                CT_CHANGE_CIPHER_SPEC => continue,
                CT_ALERT => return Err(alert_error(&body)),
                _ => return Err(err("an unexpected record during the handshake")),
            }
        }
    }

    fn write_handshake(&mut self, ty: u8, body: &[u8], transcript: &mut Vec<u8>) -> io::Result<()> {
        let mut msg = Vec::with_capacity(body.len() + 4);
        msg.push(ty);
        msg.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
        msg.extend_from_slice(body);
        transcript.extend_from_slice(&msg);
        self.write_record(CT_HANDSHAKE, &msg)
    }

    // ------------------------------------------------------------- server

    fn server_handshake(&mut self, chain_der: &[Vec<u8>], key: &KeyPair) -> io::Result<()> {
        let mut transcript = Vec::new();
        let mut hs_buf = Vec::new();
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_CLIENT_HELLO {
            return Err(err("expected a ClientHello"));
        }
        let hello = parse_client_hello(&body)?;
        if !hello.versions.contains(&VERSION_13) {
            return Err(err("the client does not offer TLS 1.3"));
        }
        if !hello.suites.contains(&SUITE_CHACHA) {
            return Err(err("the client does not offer TLS_CHACHA20_POLY1305_SHA256"));
        }
        if !hello.sig_algs.contains(&SIG_ED25519) {
            return Err(err("the client does not accept Ed25519 signatures"));
        }
        let Some(client_share) = hello.x25519_share else {
            return Err(err("the client sent no X25519 key share; a retry is not supported"));
        };
        let eph = random::array32().map_err(|e| err(e.to_string()))?;
        let our_share = x25519::public_key(&eph);
        let shared = x25519::x25519(&eph, &client_share);
        let server_random = random::array32().map_err(|e| err(e.to_string()))?;
        // ServerHello.
        let mut sh = Vec::new();
        sh.extend_from_slice(&LEGACY_VERSION.to_be_bytes());
        sh.extend_from_slice(&server_random);
        sh.push(hello.session_id.len() as u8);
        sh.extend_from_slice(&hello.session_id);
        sh.extend_from_slice(&SUITE_CHACHA.to_be_bytes());
        sh.push(0);
        let mut exts = Vec::new();
        extension(&mut exts, EXT_SUPPORTED_VERSIONS, &VERSION_13.to_be_bytes());
        let mut ks = Vec::new();
        ks.extend_from_slice(&GROUP_X25519.to_be_bytes());
        ks.extend_from_slice(&32u16.to_be_bytes());
        ks.extend_from_slice(&our_share);
        extension(&mut exts, EXT_KEY_SHARE, &ks);
        sh.extend_from_slice(&(exts.len() as u16).to_be_bytes());
        sh.extend_from_slice(&exts);
        self.write_handshake(HS_SERVER_HELLO, &sh, &mut transcript)?;
        if !hello.session_id.is_empty() {
            // Middlebox compatibility mode, as the client asked by sending
            // a session id: one plaintext CCS before the encrypted flight.
            self.write_record(CT_CHANGE_CIPHER_SPEC, &[1])?;
        }
        // Keys.
        let early = hkdf::extract(&[0u8; 32], &[0u8; 32]);
        let empty_hash = sha256(&[]);
        let hs_secret = hkdf::extract(&derive_secret(&early, "derived", &empty_hash), &shared);
        let th = sha256(&transcript);
        let c_hs = derive_secret(&hs_secret, "c hs traffic", &th);
        let s_hs = derive_secret(&hs_secret, "s hs traffic", &th);
        self.write_keys = Some(Keys::from_secret(&s_hs));
        self.read_keys = Some(Keys::from_secret(&c_hs));
        // EncryptedExtensions, Certificate, CertificateVerify, Finished.
        self.write_handshake(HS_ENCRYPTED_EXTENSIONS, &[0, 0], &mut transcript)?;
        let mut cert = vec![0u8];
        let mut list = Vec::new();
        for c in chain_der {
            list.extend_from_slice(&(c.len() as u32).to_be_bytes()[1..]);
            list.extend_from_slice(c);
            list.extend_from_slice(&[0, 0]);
        }
        cert.extend_from_slice(&(list.len() as u32).to_be_bytes()[1..]);
        cert.extend_from_slice(&list);
        self.write_handshake(HS_CERTIFICATE, &cert, &mut transcript)?;
        let th = sha256(&transcript);
        let content = verify_content(true, &th);
        let sig = ed25519::sign(&key.seed, &content);
        let mut cv = Vec::new();
        cv.extend_from_slice(&SIG_ED25519.to_be_bytes());
        cv.extend_from_slice(&(sig.len() as u16).to_be_bytes());
        cv.extend_from_slice(&sig);
        self.write_handshake(HS_CERTIFICATE_VERIFY, &cv, &mut transcript)?;
        let th = sha256(&transcript);
        let fin = finished_verify(&s_hs, &th);
        self.write_handshake(HS_FINISHED, &fin, &mut transcript)?;
        // Application keys derive from the transcript through the server
        // Finished; the client's Finished is read under the handshake keys.
        let th_server_fin = sha256(&transcript);
        let master = hkdf::extract(&derive_secret(&hs_secret, "derived", &empty_hash), &[0u8; 32]);
        let c_ap = derive_secret(&master, "c ap traffic", &th_server_fin);
        let s_ap = derive_secret(&master, "s ap traffic", &th_server_fin);
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_FINISHED {
            return Err(err("expected the client's Finished"));
        }
        let expected = finished_verify(&c_hs, &th_server_fin);
        if !super::ct_eq(&expected, &body) {
            return Err(err("the client's Finished does not verify"));
        }
        self.write_keys = Some(Keys::from_secret(&s_ap));
        self.read_keys = Some(Keys::from_secret(&c_ap));
        Ok(())
    }

    // ------------------------------------------------------------- client

    fn client_handshake(&mut self, anchors: &[Certificate], host: &str) -> io::Result<()> {
        let mut transcript = Vec::new();
        let mut hs_buf = Vec::new();
        let eph = random::array32().map_err(|e| err(e.to_string()))?;
        let our_share = x25519::public_key(&eph);
        let client_random = random::array32().map_err(|e| err(e.to_string()))?;
        let session_id = random::array32().map_err(|e| err(e.to_string()))?;
        let mut ch = Vec::new();
        ch.extend_from_slice(&LEGACY_VERSION.to_be_bytes());
        ch.extend_from_slice(&client_random);
        ch.push(32);
        ch.extend_from_slice(&session_id);
        ch.extend_from_slice(&2u16.to_be_bytes());
        ch.extend_from_slice(&SUITE_CHACHA.to_be_bytes());
        ch.push(1);
        ch.push(0);
        let mut exts = Vec::new();
        if host.parse::<std::net::IpAddr>().is_err() {
            let mut sni = Vec::new();
            let mut entry = vec![0u8];
            entry.extend_from_slice(&(host.len() as u16).to_be_bytes());
            entry.extend_from_slice(host.as_bytes());
            sni.extend_from_slice(&(entry.len() as u16).to_be_bytes());
            sni.extend_from_slice(&entry);
            extension(&mut exts, EXT_SERVER_NAME, &sni);
        }
        let mut versions = vec![2u8];
        versions.extend_from_slice(&VERSION_13.to_be_bytes());
        extension(&mut exts, EXT_SUPPORTED_VERSIONS, &versions);
        let mut groups = Vec::new();
        groups.extend_from_slice(&2u16.to_be_bytes());
        groups.extend_from_slice(&GROUP_X25519.to_be_bytes());
        extension(&mut exts, EXT_SUPPORTED_GROUPS, &groups);
        let mut sigs = Vec::new();
        sigs.extend_from_slice(&2u16.to_be_bytes());
        sigs.extend_from_slice(&SIG_ED25519.to_be_bytes());
        extension(&mut exts, EXT_SIGNATURE_ALGORITHMS, &sigs);
        let mut ks = Vec::new();
        let mut entry = Vec::new();
        entry.extend_from_slice(&GROUP_X25519.to_be_bytes());
        entry.extend_from_slice(&32u16.to_be_bytes());
        entry.extend_from_slice(&our_share);
        ks.extend_from_slice(&(entry.len() as u16).to_be_bytes());
        ks.extend_from_slice(&entry);
        extension(&mut exts, EXT_KEY_SHARE, &ks);
        ch.extend_from_slice(&(exts.len() as u16).to_be_bytes());
        ch.extend_from_slice(&exts);
        self.write_handshake(HS_CLIENT_HELLO, &ch, &mut transcript)?;
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_SERVER_HELLO {
            return Err(err("expected a ServerHello"));
        }
        let sh = parse_server_hello(&body)?;
        if sh.version != Some(VERSION_13) {
            return Err(err("the server did not select TLS 1.3"));
        }
        if sh.suite != SUITE_CHACHA {
            return Err(err("the server selected a cipher suite this build does not have"));
        }
        let Some(server_share) = sh.x25519_share else {
            return Err(err("the server sent no X25519 key share"));
        };
        let shared = x25519::x25519(&eph, &server_share);
        let early = hkdf::extract(&[0u8; 32], &[0u8; 32]);
        let empty_hash = sha256(&[]);
        let hs_secret = hkdf::extract(&derive_secret(&early, "derived", &empty_hash), &shared);
        let th = sha256(&transcript);
        let c_hs = derive_secret(&hs_secret, "c hs traffic", &th);
        let s_hs = derive_secret(&hs_secret, "s hs traffic", &th);
        self.read_keys = Some(Keys::from_secret(&s_hs));
        self.write_keys = Some(Keys::from_secret(&c_hs));
        let (ty, _) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_ENCRYPTED_EXTENSIONS {
            return Err(err("expected EncryptedExtensions"));
        }
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_CERTIFICATE {
            return Err(err("expected the server's Certificate"));
        }
        let chain = parse_certificate_message(&body)?;
        let now = crate::time::now_micros() / 1_000_000;
        x509::verify_chain(&chain, anchors, host, now).map_err(|e| err(e.to_string()))?;
        let th_before_cv = sha256(&transcript);
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_CERTIFICATE_VERIFY {
            return Err(err("expected CertificateVerify"));
        }
        if body.len() < 4 || u16::from_be_bytes([body[0], body[1]]) != SIG_ED25519 {
            return Err(err("the server signed with an algorithm this build does not verify"));
        }
        let sig_len = u16::from_be_bytes([body[2], body[3]]) as usize;
        if sig_len != 64 || body.len() != 4 + 64 {
            return Err(err("a malformed CertificateVerify"));
        }
        let mut sig = [0u8; 64];
        sig.copy_from_slice(&body[4..]);
        let content = verify_content(true, &th_before_cv);
        if !ed25519::verify(&chain[0].public_key, &content, &sig) {
            return Err(err("the server's CertificateVerify does not verify"));
        }
        let th_before_fin = sha256(&transcript);
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_FINISHED {
            return Err(err("expected the server's Finished"));
        }
        if !super::ct_eq(&finished_verify(&s_hs, &th_before_fin), &body) {
            return Err(err("the server's Finished does not verify"));
        }
        let th_server_fin = sha256(&transcript);
        let master = hkdf::extract(&derive_secret(&hs_secret, "derived", &empty_hash), &[0u8; 32]);
        let c_ap = derive_secret(&master, "c ap traffic", &th_server_fin);
        let s_ap = derive_secret(&master, "s ap traffic", &th_server_fin);
        // Middlebox compatibility: a CCS before our first encrypted record.
        self.write_record(CT_CHANGE_CIPHER_SPEC, &[1])?;
        let fin = finished_verify(&c_hs, &th_server_fin);
        self.write_handshake(HS_FINISHED, &fin, &mut transcript)?;
        self.read_keys = Some(Keys::from_secret(&s_ap));
        self.write_keys = Some(Keys::from_secret(&c_ap));
        Ok(())
    }
}

fn extension(out: &mut Vec<u8>, ty: u16, body: &[u8]) {
    out.extend_from_slice(&ty.to_be_bytes());
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(body);
}

/// The signed content of a CertificateVerify.
fn verify_content(server: bool, transcript_hash: &[u8; 32]) -> Vec<u8> {
    let mut c = vec![0x20u8; 64];
    c.extend_from_slice(if server {
        b"TLS 1.3, server CertificateVerify"
    } else {
        b"TLS 1.3, client CertificateVerify"
    });
    c.push(0);
    c.extend_from_slice(transcript_hash);
    c
}

fn alert_error(body: &[u8]) -> io::Error {
    match body.get(1) {
        Some(&ALERT_CLOSE_NOTIFY) => {
            io::Error::new(io::ErrorKind::UnexpectedEof, "tls: the peer closed")
        }
        Some(d) => err(format!("the peer sent alert {d}")),
        None => err("a malformed alert"),
    }
}

fn alert_for(e: &io::Error) -> u8 {
    let m = e.to_string();
    if m.contains("does not verify") || m.contains("failed authentication") {
        ALERT_DECRYPT_ERROR
    } else if m.contains("certificate") {
        ALERT_BAD_CERTIFICATE
    } else if m.contains("TLS 1.3") {
        ALERT_PROTOCOL_VERSION
    } else if m.contains("expected") {
        ALERT_UNEXPECTED_MESSAGE
    } else if m.contains("does not offer")
        || m.contains("does not accept")
        || m.contains("key share")
    {
        ALERT_HANDSHAKE_FAILURE
    } else {
        ALERT_INTERNAL_ERROR
    }
}

// ----------------------------------------------------------------- parsing

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Reader<'a> {
        Reader { b, i: 0 }
    }
    fn u8(&mut self) -> io::Result<u8> {
        let v = *self.b.get(self.i).ok_or_else(|| err("a truncated message"))?;
        self.i += 1;
        Ok(v)
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(((self.u8()? as u16) << 8) | self.u8()? as u16)
    }
    fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let s = self.b.get(self.i..self.i + n).ok_or_else(|| err("a truncated message"))?;
        self.i += n;
        Ok(s)
    }
    fn vec8(&mut self) -> io::Result<&'a [u8]> {
        let n = self.u8()? as usize;
        self.bytes(n)
    }
    fn vec16(&mut self) -> io::Result<&'a [u8]> {
        let n = self.u16()? as usize;
        self.bytes(n)
    }
    fn done(&self) -> bool {
        self.i >= self.b.len()
    }
}

struct ClientHello {
    session_id: Vec<u8>,
    suites: Vec<u16>,
    versions: Vec<u16>,
    sig_algs: Vec<u16>,
    x25519_share: Option<[u8; 32]>,
}

fn parse_client_hello(body: &[u8]) -> io::Result<ClientHello> {
    let mut r = Reader::new(body);
    let _legacy_version = r.u16()?;
    let _random = r.bytes(32)?;
    let session_id = r.vec8()?.to_vec();
    let suites_bytes = r.vec16()?;
    let suites: Vec<u16> =
        suites_bytes.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
    let _compression = r.vec8()?;
    let mut versions = Vec::new();
    let mut sig_algs = Vec::new();
    let mut x25519_share = None;
    if !r.done() {
        let exts = r.vec16()?;
        let mut e = Reader::new(exts);
        while !e.done() {
            let ty = e.u16()?;
            let data = e.vec16()?;
            let mut d = Reader::new(data);
            match ty {
                EXT_SUPPORTED_VERSIONS => {
                    let list = d.vec8()?;
                    versions =
                        list.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
                }
                EXT_SIGNATURE_ALGORITHMS => {
                    let list = d.vec16()?;
                    sig_algs =
                        list.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
                }
                EXT_KEY_SHARE => {
                    let list = d.vec16()?;
                    let mut l = Reader::new(list);
                    while !l.done() {
                        let group = l.u16()?;
                        let share = l.vec16()?;
                        if group == GROUP_X25519 && share.len() == 32 {
                            let mut s = [0u8; 32];
                            s.copy_from_slice(share);
                            x25519_share = Some(s);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(ClientHello { session_id, suites, versions, sig_algs, x25519_share })
}

struct ServerHello {
    suite: u16,
    version: Option<u16>,
    x25519_share: Option<[u8; 32]>,
}

fn parse_server_hello(body: &[u8]) -> io::Result<ServerHello> {
    let mut r = Reader::new(body);
    let _legacy_version = r.u16()?;
    let random = r.bytes(32)?;
    // A HelloRetryRequest is a ServerHello with a fixed random; not supported.
    const HRR: [u8; 8] = [0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11];
    if random[..8] == HRR {
        return Err(err("the server asked for a retry, which this build does not support"));
    }
    let _session_id = r.vec8()?;
    let suite = r.u16()?;
    let _compression = r.u8()?;
    let mut version = None;
    let mut x25519_share = None;
    if !r.done() {
        let exts = r.vec16()?;
        let mut e = Reader::new(exts);
        while !e.done() {
            let ty = e.u16()?;
            let data = e.vec16()?;
            let mut d = Reader::new(data);
            match ty {
                EXT_SUPPORTED_VERSIONS => version = Some(d.u16()?),
                EXT_KEY_SHARE => {
                    let group = d.u16()?;
                    let share = d.vec16()?;
                    if group == GROUP_X25519 && share.len() == 32 {
                        let mut s = [0u8; 32];
                        s.copy_from_slice(share);
                        x25519_share = Some(s);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(ServerHello { suite, version, x25519_share })
}

fn parse_certificate_message(body: &[u8]) -> io::Result<Vec<Certificate>> {
    let mut r = Reader::new(body);
    let _context = r.vec8()?;
    let total = ((r.u8()? as usize) << 16) | ((r.u8()? as usize) << 8) | r.u8()? as usize;
    let list = r.bytes(total)?;
    let mut l = Reader::new(list);
    let mut chain = Vec::new();
    while !l.done() {
        let len = ((l.u8()? as usize) << 16) | ((l.u8()? as usize) << 8) | l.u8()? as usize;
        let der = l.bytes(len)?;
        let _exts = l.vec16()?;
        chain.push(x509::parse(der).map_err(|e| err(e.to_string()))?);
    }
    if chain.is_empty() {
        return Err(err("the server sent no certificate"));
    }
    Ok(chain)
}

// ---------------------------------------------------------------- Read/Write

impl Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.handshake()?;
        loop {
            if self.pending_at < self.pending.len() {
                let n = (self.pending.len() - self.pending_at).min(buf.len());
                buf[..n].copy_from_slice(&self.pending[self.pending_at..self.pending_at + n]);
                self.pending_at += n;
                return Ok(n);
            }
            if self.closed {
                return Ok(0);
            }
            let (ctype, body) = match self.read_record() {
                Ok(r) => r,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    self.closed = true;
                    return Ok(0);
                }
                Err(e) => return Err(e),
            };
            match ctype {
                CT_APPLICATION_DATA => {
                    self.pending = body;
                    self.pending_at = 0;
                }
                CT_ALERT => {
                    self.closed = true;
                    if body.get(1) == Some(&ALERT_CLOSE_NOTIFY) {
                        return Ok(0);
                    }
                    return Err(alert_error(&body));
                }
                CT_HANDSHAKE => {
                    // A ticket is ignored; a key update is not supported.
                    match body.first() {
                        Some(&HS_NEW_SESSION_TICKET) => {}
                        Some(&HS_KEY_UPDATE) => {
                            return Err(err("a key update, which this build does not support"))
                        }
                        _ => return Err(err("an unexpected handshake message")),
                    }
                }
                _ => return Err(err("an unexpected record")),
            }
        }
    }
}

impl Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.handshake()?;
        self.write_record(CT_APPLICATION_DATA, buf)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.sock.flush()
    }
}

impl TlsStream {
    /// Say the sending is over: a close_notify, then the socket's write half.
    pub fn close_notify(&mut self) -> io::Result<()> {
        if self.write_keys.is_some() {
            let _ = self.send_alert(ALERT_CLOSE_NOTIFY);
        }
        self.sock.shutdown(std::net::Shutdown::Write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::pem;
    use std::net::TcpListener;

    /// The key schedule against RFC 8448 §3's simple handshake: the
    /// handshake secrets from the published shared secret and transcript.
    #[test]
    fn the_key_schedule_matches_rfc_8448() {
        let unhex = |s: &str| -> Vec<u8> {
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
        };
        let shared: [u8; 32] =
            unhex("8bd4054fb55b9d63fdfbacf9f04b9f0d35e6d63f537563efd46272900f89492d")
                .try_into()
                .unwrap();
        let early = hkdf::extract(&[0u8; 32], &[0u8; 32]);
        assert_eq!(
            crate::objstore::hex(&early),
            "33ad0a1c607ec03b09e6cd9893680ce210adf300aa1f2660e1b22e10f170f92a"
        );
        let derived = derive_secret(&early, "derived", &sha256(&[]));
        assert_eq!(
            crate::objstore::hex(&derived),
            "6f2615a108c702c5678f54fc9dbab69716c076189c48250cebeac3576c3611ba"
        );
        let hs = hkdf::extract(&derived, &shared);
        assert_eq!(
            crate::objstore::hex(&hs),
            "1dc826e93606aa6fdc0aadc12f741b01046aa6b99f691ed221a9f0ca043fbeac"
        );
        // The transcript hash of ClientHello..ServerHello in that trace.
        let th: [u8; 32] =
            unhex("860c06edc07858ee8e78f0e7428c58edd6b43f2ca3e6e95f02ed063cf0e1cad8")
                .try_into()
                .unwrap();
        let c_hs = derive_secret(&hs, "c hs traffic", &th);
        assert_eq!(
            crate::objstore::hex(&c_hs),
            "b3eddb126e067f35a780b3abf45e2d8f3b1a950738f52e9600746a0e27a55a21"
        );
        let s_hs = derive_secret(&hs, "s hs traffic", &th);
        assert_eq!(
            crate::objstore::hex(&s_hs),
            "b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38"
        );
        // The trace's suite is AES-128-GCM, so its key is 16 bytes and its
        // iv 12: the same labels, expanded to those lengths.
        assert_eq!(
            crate::objstore::hex(&expand_label(&s_hs, "key", &[], 16)),
            "3fce516009c21727d0f2e4e86ee403bc"
        );
        assert_eq!(
            crate::objstore::hex(&expand_label(&s_hs, "iv", &[], 12)),
            "5d313eb2671276ee13000b30"
        );
    }

    /// Our client to our server over loopback: the handshake completes,
    /// bytes round-trip both ways, a close_notify ends the read side, and a
    /// client that trusts another CA is refused with a certificate error.
    #[test]
    fn a_client_and_a_server_of_this_crate_talk_and_a_wrong_anchor_is_refused() {
        let m = x509::make(
            "localhost",
            &["localhost".to_string()],
            &["127.0.0.1".parse().unwrap()],
            30,
        )
        .unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (chain_s, key_s) = (chain_der.clone(), key.clone());
        let server = std::thread::spawn(move || {
            let mut answers = Vec::new();
            for _ in 0..2 {
                let (sock, _) = listener.accept().unwrap();
                let mut s =
                    TlsStream::server(sock, ServerSide { chain_der: &chain_s, key: &key_s });
                let mut buf = Vec::new();
                match s.read_to_end(&mut buf) {
                    Ok(_) => {
                        s.write_all(b"echo: ").unwrap();
                        s.write_all(&buf).unwrap();
                        s.close_notify().unwrap();
                        answers.push(Ok(buf.len()));
                    }
                    Err(e) => answers.push(Err(e.to_string())),
                }
            }
            answers
        });
        let sock = TcpStream::connect(addr).unwrap();
        let mut c = TlsStream::client(
            sock,
            ClientSide { anchors: std::slice::from_ref(&anchor), host: "localhost" },
        );
        let payload = vec![7u8; 40_000];
        c.write_all(&payload).unwrap();
        c.close_notify().unwrap();
        let mut got = Vec::new();
        c.read_to_end(&mut got).unwrap();
        assert_eq!(&got[..6], b"echo: ");
        assert_eq!(&got[6..], &payload[..]);
        // A client trusting a different CA refuses the server's chain.
        let other = x509::make("localhost", &[], &[], 30).unwrap();
        let other_anchor =
            x509::parse(&pem::decode_all(&other.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let sock = TcpStream::connect(addr).unwrap();
        let mut c = TlsStream::client(
            sock,
            ClientSide { anchors: std::slice::from_ref(&other_anchor), host: "localhost" },
        );
        let e = c.write_all(b"x").unwrap_err();
        assert!(e.to_string().contains("does not reach"), "{e}");
        let answers = server.join().unwrap();
        assert_eq!(answers[0], Ok(40_000));
        assert!(answers[1].is_err(), "the server saw the client's alert");
    }
}
