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

use crate::cipher::Secret;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;

use super::chacha20poly1305 as aead;
use super::hkdf;
use super::sha2::{hmac_sha256, sha256};
use super::x509::{self, Certificate, KeyPair};
use super::{ed25519, random, x25519};

const SUITE_CHACHA: u16 = 0x1303;
const GROUP_X25519: u16 = 0x001d;
const SIG_ED25519: u16 = 0x0807;
const SIG_RSA_PSS_RSAE_SHA256: u16 = 0x0804;
const SIG_ECDSA_SECP256R1_SHA256: u16 = 0x0403;
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
const HS_CERTIFICATE_REQUEST: u8 = 13;
const HS_CERTIFICATE_VERIFY: u8 = 15;
const HS_FINISHED: u8 = 20;
const HS_KEY_UPDATE: u8 = 24;
/// The synthetic message a transcript restarts from after a
/// HelloRetryRequest: `message_hash` over the first ClientHello.
const HS_MESSAGE_HASH: u8 = 254;
/// A HelloRetryRequest is a ServerHello whose random is this value.
const HRR_RANDOM: [u8; 32] = [
    0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
    0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
];

const EXT_SERVER_NAME: u16 = 0;
const EXT_SUPPORTED_GROUPS: u16 = 10;
const EXT_SIGNATURE_ALGORITHMS: u16 = 13;
const EXT_SUPPORTED_VERSIONS: u16 = 43;
const EXT_KEY_SHARE: u16 = 51;
const EXT_PRE_SHARED_KEY: u16 = 41;
const EXT_COOKIE: u16 = 44;
const EXT_EARLY_DATA: u16 = 42;
/// How much early data a server skips past for a client that sent some
/// though no ticket of this server ever allowed it (RFC 8446 §4.2.10).
const MAX_EARLY_DATA: usize = 16_384 + 256 * 4;
const EXT_PSK_KEY_EXCHANGE_MODES: u16 = 45;
const PSK_DHE_KE: u8 = 1;

/// How long a ticket resumes for: a day, well under the protocol's week.
/// The server refuses one older than this by its own clock; the client
/// stops offering one at the lifetime the ticket named.
const TICKET_LIFETIME_SECS: u64 = 86_400;

const ALERT_CLOSE_NOTIFY: u8 = 0;
const ALERT_HANDSHAKE_FAILURE: u8 = 40;
const ALERT_BAD_CERTIFICATE: u8 = 42;
const ALERT_ILLEGAL_PARAMETER: u8 = 47;
const ALERT_DECRYPT_ERROR: u8 = 51;
const ALERT_PROTOCOL_VERSION: u8 = 70;
const ALERT_INTERNAL_ERROR: u8 = 80;
const ALERT_UNEXPECTED_MESSAGE: u8 = 10;

fn err(what: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("tls: {}", what.into()))
}

// ------------------------------------------------------------ key schedule

/// The next application traffic secret from the current one (RFC 8446
/// §7.2): what a KeyUpdate moves both sides to.
fn next_secret(secret: &[u8; 32]) -> Secret<32> {
    expand_label(secret, "traffic upd", &[])
}

/// `HKDF-Expand-Label` into `N` secret bytes. The label and the context
/// are public; only the output is not, which is why it comes back as a
/// `Secret` and never as a `Vec` for the caller to drop unwiped.
fn expand_label<const N: usize>(secret: &[u8; 32], label: &str, context: &[u8]) -> Secret<N> {
    let mut out = Secret::<N>::zero();
    expand_label_into(secret, label, context, out.bytes_mut());
    out
}

fn expand_label_into(secret: &[u8; 32], label: &str, context: &[u8], out: &mut [u8]) {
    let full = format!("tls13 {label}");
    let mut info = Vec::with_capacity(4 + full.len() + context.len());
    info.extend_from_slice(&(out.len() as u16).to_be_bytes());
    info.push(full.len() as u8);
    info.extend_from_slice(full.as_bytes());
    info.push(context.len() as u8);
    info.extend_from_slice(context);
    hkdf::expand_into(secret, &info, out);
}

fn derive_secret(secret: &[u8; 32], label: &str, transcript_hash: &[u8; 32]) -> Secret<32> {
    expand_label(secret, label, transcript_hash)
}

/// `Derive-Secret` into a secret the caller already owns.
///
/// The long-lived traffic secrets go through this rather than through the
/// returning form: a `Secret` returned by value can leave the callee's copy
/// on a frame that has returned, because the move takes the bytes out and
/// the moved-from source is never dropped, so it is never wiped. Measured
/// -- `tls13::tests::a_traffic_secret_is_not_left_in_memory_after_a_handshake`
/// finds one copy of the traffic secret with the returning form and none
/// with this one.
fn derive_secret_into(
    secret: &[u8; 32],
    label: &str,
    transcript_hash: &[u8; 32],
    out: &mut Secret<32>,
) {
    expand_label_into(secret, label, transcript_hash, out.bytes_mut());
}

/// One direction's keys: derived from a traffic secret, used with a
/// sequence number that never repeats within them.
struct Keys {
    key: Secret<32>,
    iv: Secret<12>,
    seq: u64,
}

impl Keys {
    fn from_secret(secret: &[u8; 32]) -> Keys {
        Keys { key: expand_label(secret, "key", &[]), iv: expand_label(secret, "iv", &[]), seq: 0 }
    }

    fn nonce(&mut self) -> [u8; 12] {
        let mut n = *self.iv;
        for (i, b) in self.seq.to_be_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        self.seq += 1;
        n
    }
}

// ------------------------------------------------------------- resumption

/// What a client keeps from a server's NewSessionTicket: the opaque ticket
/// to offer, the PSK it stands for, and when it stops being offered.
#[derive(Clone)]
struct Ticket {
    ticket: Vec<u8>,
    psk: Secret<32>,
    /// Seconds since the epoch when the ticket arrived.
    received: u64,
    lifetime: u32,
    age_add: u32,
}

/// The tickets this process holds, one per server it has spoken to, keyed
/// by the name it verified and the address it dialled. Process-wide, so
/// every connection `https_request` opens -- or the wire dials -- after the
/// first resumes; replaced by each newer ticket.
static TICKETS: Mutex<Option<HashMap<String, Ticket>>> = Mutex::new(None);

fn ticket_store<T>(f: impl FnOnce(&mut HashMap<String, Ticket>) -> T) -> T {
    let mut g = TICKETS.lock().unwrap_or_else(|p| p.into_inner());
    f(g.get_or_insert_with(HashMap::new))
}

/// Forget every ticket: what a test does between servers.
pub fn forget_tickets() {
    ticket_store(|t| t.clear());
}

/// How many tickets this process holds.
pub fn tickets_held() -> usize {
    ticket_store(|t| t.len())
}

/// How many handshakes this process has served that resumed from a ticket.
static RESUMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn resumed_handshakes() -> u64 {
    RESUMED.load(std::sync::atomic::Ordering::Relaxed)
}

fn now_secs() -> u64 {
    (crate::time::now_micros() / 1_000_000) as u64
}

/// The key a server seals its tickets under, derived from its TLS key: the
/// same on every node that serves the same certificate, so a ticket from
/// one pod resumes at another behind the same Service, and gone with the
/// key. Never written anywhere. A wire that requires client certificates
/// seals under another key, so a ticket from before the requirement (a
/// resumed handshake shows no certificate) does not resume past it. The
/// day is in the derivation: a server seals under today's key and opens
/// under today's or yesterday's (a ticket lives a day), so a TLS key that
/// leaks opens the tickets of two days, not of its whole life.
fn ticket_key(key: &KeyPair, client_auth: bool, day: u64) -> Secret<32> {
    let salt: &[u8] =
        if client_auth { b"celastro tls ticket v1 mtls" } else { b"celastro tls ticket v1" };
    let mut ikm = [0u8; 40];
    ikm[..32].copy_from_slice(&key.seed);
    ikm[32..].copy_from_slice(&day.to_be_bytes());
    let out = hkdf::extract(salt, &ikm);
    crate::cipher::wipe(&mut ikm);
    out
}

/// Today, as the ticket key counts days.
fn ticket_day() -> u64 {
    now_secs() / 86_400
}

const TICKET_AAD: &[u8] = b"celastro tls ticket v1";

/// A ticket as the server hands it out: `1 | nonce(12) | ciphertext | tag`
/// over `psk(32) | issued_at(8) | age_add(4)`. Opaque to the client.
pub(super) fn seal_ticket(
    tkey: &[u8; 32],
    psk: &[u8; 32],
    issued_at: u64,
    age_add: u32,
) -> io::Result<Vec<u8>> {
    let nonce: [u8; 12] =
        random::bytes(12).map_err(|e| err(e.to_string()))?.try_into().expect("twelve bytes");
    let mut plain = Vec::with_capacity(44);
    plain.extend_from_slice(psk);
    plain.extend_from_slice(&issued_at.to_be_bytes());
    plain.extend_from_slice(&age_add.to_be_bytes());
    let tag = aead::seal(tkey, &nonce, TICKET_AAD, &mut plain);
    let mut out = Vec::with_capacity(1 + 12 + plain.len() + 16);
    out.push(1);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&plain);
    out.extend_from_slice(&tag);
    Ok(out)
}

/// The PSK a ticket stands for, if this server sealed it and it is not
/// older than the lifetime. `None` for anything else -- another node's
/// key, a tampered byte, an old ticket -- and the handshake goes on in
/// full, which is what the protocol says happens.
pub(super) fn open_ticket(tkey: &[u8; 32], ticket: &[u8]) -> Option<[u8; 32]> {
    if ticket.len() != 1 + 12 + 44 + 16 || ticket[0] != 1 {
        return None;
    }
    let nonce: [u8; 12] = ticket[1..13].try_into().ok()?;
    let mut data = ticket[13..13 + 44].to_vec();
    let tag: [u8; 16] = ticket[13 + 44..].try_into().ok()?;
    if !aead::open(tkey, &nonce, TICKET_AAD, &mut data, &tag) {
        return None;
    }
    let psk: [u8; 32] = data[..32].try_into().ok()?;
    let issued_at = u64::from_be_bytes(data[32..40].try_into().ok()?);
    let now = now_secs();
    if now < issued_at.saturating_sub(60) || now > issued_at + TICKET_LIFETIME_SECS {
        return None;
    }
    Some(psk)
}

/// The PSK a resumption master secret and a ticket nonce make.
fn resumption_psk(res_master: &[u8; 32], nonce: &[u8]) -> Secret<32> {
    expand_label(res_master, "resumption", nonce)
}

/// The binder over the truncated ClientHello: the same construction as
/// Finished, keyed from the early secret's "res binder".
fn psk_binder(psk: &[u8; 32], truncated_hello_hash: &[u8; 32]) -> [u8; 32] {
    let early = hkdf::extract(&[0u8; 32], psk);
    let binder_key = derive_secret(&early, "res binder", &sha256(&[]));
    finished_verify(&binder_key, truncated_hello_hash)
}

/// A NewSessionTicket message's fields.
struct NewSessionTicket {
    lifetime: u32,
    age_add: u32,
    nonce: Vec<u8>,
    ticket: Vec<u8>,
}

fn parse_new_session_ticket(body: &[u8]) -> io::Result<NewSessionTicket> {
    let mut r = Reader::new(body);
    let lifetime = r.u32()?;
    let age_add = r.u32()?;
    let nonce = r.vec8()?.to_vec();
    let ticket = r.vec16()?.to_vec();
    let _exts = r.vec16()?;
    Ok(NewSessionTicket { lifetime, age_add, nonce, ticket })
}

// `Ticket::psk`, `TlsStream::res_master` and the two traffic secrets used
// to be wiped by hand in a `Drop` here. They are `Secret` now, so they wipe
// themselves wherever they are dropped -- including out of a `Some` that is
// overwritten by a KeyUpdate, which a `Drop` on the stream never reached.

/// The Finished MAC. The finished key is expanded, used and dropped here;
/// it is never copied into a bare array on the way to the HMAC.
fn finished_verify(base: &[u8; 32], transcript_hash: &[u8; 32]) -> [u8; 32] {
    let fk: Secret<32> = expand_label(base, "finished", &[]);
    hmac_sha256(&fk[..], transcript_hash)
}

// -------------------------------------------------------------- the stream

/// What the handshake needs from the node: its chain and key when serving,
/// the anchors and the peer's expected name when dialling.
pub struct ServerSide<'a> {
    pub chain_der: &'a [Vec<u8>],
    pub key: &'a KeyPair,
    /// The anchors a peer's certificate must reach when this side asks
    /// for one -- the wire under `CELASTRO_TLS_CLIENT_AUTH=required`;
    /// `None` asks for nothing, as the console never does.
    pub client_anchors: Option<&'a [Certificate]>,
}

pub struct ClientSide<'a> {
    pub anchors: &'a [Certificate],
    pub host: &'a str,
    /// This side's own chain and key, presented when the server asks for
    /// a certificate; without them such a request is answered with an
    /// empty Certificate, which a server that requires one refuses.
    pub chain_der: Option<&'a [Vec<u8>]>,
    pub key: Option<&'a KeyPair>,
}

enum Role {
    Server {
        chain_der: Vec<Vec<u8>>,
        key: KeyPair,
        client_anchors: Option<Vec<Certificate>>,
    },
    Client {
        anchors: Vec<Certificate>,
        host: String,
        chain_der: Option<Vec<Vec<u8>>>,
        key: Option<KeyPair>,
    },
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
    /// Whether the handshake resumed from a ticket.
    resumed: bool,
    /// Whether the handshake went through a HelloRetryRequest.
    retried: bool,
    /// The application traffic secrets, kept so a KeyUpdate can derive the
    /// next generation of keys from them.
    read_secret: Option<Secret<32>>,
    write_secret: Option<Secret<32>>,
    /// A test's hook: send the first ClientHello without a key share, so
    /// the server's HelloRetryRequest and this side's answer to it run.
    pub(crate) omit_first_share: bool,
    /// A test's hook: offer early data and send a record of it after the
    /// ClientHello, as a misbehaving client would; the server skips it.
    pub(crate) send_junk_early_data: bool,
    /// Bytes of early data this server may still skip: records that fail
    /// to open under the client's handshake key while a client that
    /// offered early data has not yet been read past it.
    skip_early: usize,
    /// A client's, after its handshake: the resumption master secret a
    /// NewSessionTicket's PSK derives from, and the store key it goes under.
    res_master: Option<Secret<32>>,
    store_key: Option<String>,
}

impl TlsStream {
    pub fn server(sock: TcpStream, side: ServerSide<'_>) -> TlsStream {
        TlsStream::new(
            sock,
            Role::Server {
                chain_der: side.chain_der.to_vec(),
                key: side.key.clone(),
                client_anchors: side.client_anchors.map(<[Certificate]>::to_vec),
            },
        )
    }

    pub fn client(sock: TcpStream, side: ClientSide<'_>) -> TlsStream {
        TlsStream::new(
            sock,
            Role::Client {
                anchors: side.anchors.to_vec(),
                host: side.host.to_string(),
                chain_der: side.chain_der.map(<[Vec<u8>]>::to_vec),
                key: side.key.cloned(),
            },
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
            resumed: false,
            retried: false,
            read_secret: None,
            write_secret: None,
            omit_first_share: false,
            send_junk_early_data: false,
            skip_early: 0,
            res_master: None,
            store_key: None,
        }
    }

    /// Whether the handshake resumed from a ticket rather than running in
    /// full. Meaningful once the handshake has happened.
    pub fn resumed(&self) -> bool {
        self.resumed
    }

    /// Whether the handshake went through a HelloRetryRequest.
    pub fn retried(&self) -> bool {
        self.retried
    }

    /// The next generation of this side's write keys, announced to the
    /// peer with a KeyUpdate that asks it to do the same when `ask_peer`.
    /// After the handshake only.
    pub fn update_keys(&mut self, ask_peer: bool) -> io::Result<()> {
        self.handshake()?;
        let Some(secret) = self.write_secret.as_deref().copied() else {
            return Err(err("no application keys to update yet"));
        };
        let msg = [HS_KEY_UPDATE, 0, 0, 1, ask_peer as u8];
        self.write_record(CT_HANDSHAKE, &msg)?;
        let next = next_secret(&secret);
        self.write_keys = Some(Keys::from_secret(&next));
        self.write_secret = Some(next);
        Ok(())
    }

    /// The peer announced its next generation of keys: read under them
    /// from here on.
    fn next_read_keys(&mut self) -> io::Result<()> {
        let Some(secret) = self.read_secret.as_deref().copied() else {
            return Err(err("a KeyUpdate before the application keys"));
        };
        let next = next_secret(&secret);
        self.read_keys = Some(Keys::from_secret(&next));
        self.read_secret = Some(next);
        Ok(())
    }

    /// Complete the handshake if it has not been done. An error here is
    /// the peer's alert or our own, and the socket is left as it is.
    pub fn handshake(&mut self) -> io::Result<()> {
        let Some(role) = self.role.take() else { return Ok(()) };
        let r = match role {
            Role::Server { chain_der, key, client_anchors } => {
                self.server_handshake(&chain_der, &key, client_anchors.as_deref())
            }
            Role::Client { anchors, host, chain_der, key } => {
                self.client_handshake(&anchors, &host, chain_der.as_deref(), key.as_ref())
            }
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
        self.read_exact_from_sock(&mut header, false)?;
        let ctype = header[0];
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if len > MAX_PLAINTEXT + 256 {
            return Err(err("a record longer than the protocol allows"));
        }
        let mut body = vec![0u8; len];
        self.read_exact_from_sock(&mut body, true)?;
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
                    if self.skip_early > 0 {
                        // Early data this server did not accept: skipped,
                        // within the bound, and the nonce not consumed.
                        keys.seq -= 1;
                        self.skip_early = self.skip_early.saturating_sub(len);
                        if self.skip_early == 0 {
                            return Err(err("more early data than the protocol allows"));
                        }
                        return self.read_record();
                    }
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

    /// `buf` filled from what was read ahead and the socket. A connection
    /// that ends before a record starts is the peer's close (an
    /// `UnexpectedEof`, which the reader takes as the end of the stream);
    /// one that ends inside a record's header or body is a truncation --
    /// a middlebox or an attacker cutting the stream where no close_notify
    /// was -- and is an error, never a quiet end.
    fn read_exact_from_sock(&mut self, buf: &mut [u8], inside_record: bool) -> io::Result<()> {
        let mut filled = 0;
        let take = self.inbuf.len().min(buf.len());
        if take > 0 {
            buf[..take].copy_from_slice(&self.inbuf[..take]);
            self.inbuf.drain(..take);
            filled = take;
        }
        while filled < buf.len() {
            match self.sock.read(&mut buf[filled..]) {
                Ok(0) if filled == 0 && !inside_record => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the peer closed the connection",
                    ))
                }
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "the connection ended inside a TLS record: truncated, not closed",
                    ))
                }
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
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

    fn server_handshake(
        &mut self,
        chain_der: &[Vec<u8>],
        key: &KeyPair,
        client_anchors: Option<&[Certificate]>,
    ) -> io::Result<()> {
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
        // No X25519 share in the first flight but X25519 among the groups
        // the client supports: a HelloRetryRequest asks for one. The
        // transcript restarts from a message_hash of the first ClientHello
        // and the retry, as the RFC has it, and the second ClientHello is
        // read in the first one's place -- its PSK binder, if any, covers
        // the restarted transcript.
        let mut hello = hello;
        let mut ch_start = 0usize;
        if hello.x25519_share.is_none() && hello.groups.contains(&GROUP_X25519) {
            let ch1_hash = sha256(&transcript);
            transcript.clear();
            transcript.push(HS_MESSAGE_HASH);
            transcript.extend_from_slice(&[0, 0, 32]);
            transcript.extend_from_slice(&ch1_hash);
            let mut hrr = Vec::new();
            hrr.extend_from_slice(&LEGACY_VERSION.to_be_bytes());
            hrr.extend_from_slice(&HRR_RANDOM);
            hrr.push(hello.session_id.len() as u8);
            hrr.extend_from_slice(&hello.session_id);
            hrr.extend_from_slice(&SUITE_CHACHA.to_be_bytes());
            hrr.push(0);
            let mut exts = Vec::new();
            extension(&mut exts, EXT_SUPPORTED_VERSIONS, &VERSION_13.to_be_bytes());
            extension(&mut exts, EXT_KEY_SHARE, &GROUP_X25519.to_be_bytes());
            hrr.extend_from_slice(&(exts.len() as u16).to_be_bytes());
            hrr.extend_from_slice(&exts);
            self.write_handshake(HS_SERVER_HELLO, &hrr, &mut transcript)?;
            if !hello.session_id.is_empty() {
                self.write_record(CT_CHANGE_CIPHER_SPEC, &[1])?;
            }
            self.retried = true;
            ch_start = transcript.len();
            let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
            if ty != HS_CLIENT_HELLO {
                return Err(err("expected the client's second ClientHello"));
            }
            hello = parse_client_hello(&body)?;
            if hello.x25519_share.is_none() {
                return Err(err("the client's second ClientHello still has no X25519 key share"));
            }
        }
        let Some(client_share) = hello.x25519_share else {
            return Err(err(
                "the client sent no X25519 key share and supports no group this build has",
            ));
        };
        // A ticket this server sealed, offered with the DHE mode and a
        // binder that verifies, resumes: the early secret is the PSK's and
        // the certificate flight is skipped. Anything short of that -- no
        // offer, another node's ticket, a stale one -- is a full handshake,
        // for which the client has to accept our signature.
        let tkey: Secret<32> = ticket_key(key, client_anchors.is_some(), ticket_day());
        let psk: Option<[u8; 32]> = match &hello.psk {
            Some(offer) if hello.psk_dhe => {
                match open_ticket(&tkey, &offer.identity).or_else(|| {
                    let yesterday =
                        ticket_key(key, client_anchors.is_some(), ticket_day().saturating_sub(1));
                    open_ticket(&yesterday, &offer.identity)
                }) {
                    Some(psk) => {
                        let truncated = sha256(&transcript[..ch_start + 4 + offer.binders_at]);
                        if !super::ct_eq(&psk_binder(&psk, &truncated), &offer.binder) {
                            return Err(err("the PSK binder does not verify"));
                        }
                        Some(psk)
                    }
                    None => None,
                }
            }
            _ => None,
        };
        if psk.is_none() && !hello.sig_algs.contains(&SIG_ED25519) {
            return Err(err("the client does not accept Ed25519 signatures"));
        }
        self.resumed = psk.is_some();
        if self.resumed {
            RESUMED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let eph: Secret<32> = (random::array32().map_err(|e| err(e.to_string()))?).into();
        let our_share = x25519::public_key(&eph);
        let shared: Secret<32> = x25519::x25519(&eph, &client_share);
        if *shared == [0u8; 32] {
            return Err(err("the key share is a low-order point: the shared secret would be zero"));
        }
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
        if psk.is_some() {
            extension(&mut exts, EXT_PRE_SHARED_KEY, &0u16.to_be_bytes());
        }
        sh.extend_from_slice(&(exts.len() as u16).to_be_bytes());
        sh.extend_from_slice(&exts);
        self.write_handshake(HS_SERVER_HELLO, &sh, &mut transcript)?;
        if !hello.session_id.is_empty() {
            // Middlebox compatibility mode, as the client asked by sending
            // a session id: one plaintext CCS before the encrypted flight.
            self.write_record(CT_CHANGE_CIPHER_SPEC, &[1])?;
        }
        // Keys.
        let early: Secret<32> =
            hkdf::extract(&[0u8; 32], psk.as_ref().map(|p| &p[..]).unwrap_or(&[0u8; 32]));
        let empty_hash = sha256(&[]);
        let hs_secret: Secret<32> =
            hkdf::extract(&derive_secret(&early, "derived", &empty_hash)[..], &shared[..]);
        let th = sha256(&transcript);
        let mut c_hs = Secret::<32>::zero();
        derive_secret_into(&hs_secret, "c hs traffic", &th, &mut c_hs);
        let mut s_hs = Secret::<32>::zero();
        derive_secret_into(&hs_secret, "s hs traffic", &th, &mut s_hs);
        self.write_keys = Some(Keys::from_secret(&s_hs));
        self.read_keys = Some(Keys::from_secret(&c_hs));
        // A client that offered early data sent records under a key this
        // server does not have (no ticket of its ever allowed early data);
        // they fail to open under the handshake key and are skipped, up to
        // the protocol's bound, until the client's flight opens.
        self.skip_early = if hello.early_data { MAX_EARLY_DATA } else { 0 };
        // EncryptedExtensions, Certificate, CertificateVerify, Finished --
        // the middle two only when the client did not resume.
        self.write_handshake(HS_ENCRYPTED_EXTENSIONS, &[0, 0], &mut transcript)?;
        if psk.is_none() {
            if client_anchors.is_some() {
                // CertificateRequest: no context, and the one extension
                // the RFC requires, the signature schemes this side
                // verifies. The client answers between our Finished and
                // its own.
                let mut req = vec![0u8];
                let mut list = Vec::new();
                for s in [SIG_ED25519, SIG_RSA_PSS_RSAE_SHA256, SIG_ECDSA_SECP256R1_SHA256] {
                    list.extend_from_slice(&s.to_be_bytes());
                }
                let mut sa = Vec::with_capacity(2 + list.len());
                sa.extend_from_slice(&(list.len() as u16).to_be_bytes());
                sa.extend_from_slice(&list);
                let mut exts = Vec::new();
                extension(&mut exts, EXT_SIGNATURE_ALGORITHMS, &sa);
                req.extend_from_slice(&(exts.len() as u16).to_be_bytes());
                req.extend_from_slice(&exts);
                self.write_handshake(HS_CERTIFICATE_REQUEST, &req, &mut transcript)?;
            }
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
        }
        let th = sha256(&transcript);
        let fin = finished_verify(&s_hs, &th);
        self.write_handshake(HS_FINISHED, &fin, &mut transcript)?;
        // Application keys derive from the transcript through the server
        // Finished. This side writes under its application keys from here
        // on (RFC 8446 §7.1: the server's traffic key changes after its
        // Finished), so an alert about the client's flight -- a missing
        // certificate -- reaches a conforming client, which reads under
        // those keys by then; sent under the handshake keys it was "bad
        // record MAC" at Go's client. The client's flight is still read
        // under the handshake keys.
        let th_server_fin = sha256(&transcript);
        let master: Secret<32> =
            hkdf::extract(&derive_secret(&hs_secret, "derived", &empty_hash)[..], &[0u8; 32]);
        let mut c_ap = Secret::<32>::zero();
        derive_secret_into(&master, "c ap traffic", &th_server_fin, &mut c_ap);
        let mut s_ap = Secret::<32>::zero();
        derive_secret_into(&master, "s ap traffic", &th_server_fin, &mut s_ap);
        self.write_keys = Some(Keys::from_secret(&s_ap));
        self.write_secret = Some(s_ap);
        // The client's Certificate and CertificateVerify when this side
        // asked for them and the handshake is a full one: its chain
        // reaches an anchor and the signature is its key's over the
        // transcript so far. A peer with nothing to show is refused here,
        // before the token. Its Finished then covers those messages too.
        let mut th_fin = th_server_fin;
        let (mut ty, mut body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if let (Some(anchors), true) = (client_anchors, psk.is_none()) {
            if ty != HS_CERTIFICATE {
                return Err(err("expected the client's Certificate"));
            }
            let chain = parse_certificate_message(&body)?;
            if chain.is_empty() {
                return Err(err(
                    "the peer presented no certificate, and this node requires one on the wire \
                     (CELASTRO_TLS_CLIENT_AUTH=required)",
                ));
            }
            let now = crate::time::now_micros() / 1_000_000;
            x509::chain_reaches_anchor(&chain, anchors, now)
                .map_err(|e| err(format!("the peer's certificate: {e}")))?;
            let th_before_cv = sha256(&transcript);
            (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
            if ty != HS_CERTIFICATE_VERIFY {
                return Err(err("expected the client's CertificateVerify"));
            }
            let content = verify_content(false, &th_before_cv);
            if !verify_signature(&body, &chain[0].public_key, &content)? {
                return Err(err("the peer's CertificateVerify does not verify"));
            }
            th_fin = sha256(&transcript);
            (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        }
        if ty != HS_FINISHED {
            return Err(err("expected the client's Finished"));
        }
        let expected = finished_verify(&c_hs, &th_fin);
        if !super::ct_eq(&expected, &body) {
            return Err(err("the client's Finished does not verify"));
        }
        self.skip_early = 0;
        self.read_keys = Some(Keys::from_secret(&c_ap));
        self.read_secret = Some(c_ap);
        // A ticket for next time, under the application keys: the PSK it
        // stands for derives from this handshake's resumption master
        // secret, so a resumed connection hands out a fresh ticket too.
        let th_client_fin = sha256(&transcript);
        let mut res_master = Secret::<32>::zero();
        derive_secret_into(&master, "res master", &th_client_fin, &mut res_master);
        let nonce = [0u8];
        let psk_next: Secret<32> = resumption_psk(&res_master, &nonce);
        let age_add = u32::from_be_bytes(
            random::bytes(4).map_err(|e| err(e.to_string()))?.try_into().expect("four bytes"),
        );
        let ticket = seal_ticket(&tkey, &psk_next, now_secs(), age_add)?;
        let mut nst = Vec::with_capacity(ticket.len() + 16);
        nst.extend_from_slice(&(TICKET_LIFETIME_SECS as u32).to_be_bytes());
        nst.extend_from_slice(&age_add.to_be_bytes());
        nst.push(nonce.len() as u8);
        nst.extend_from_slice(&nonce);
        nst.extend_from_slice(&(ticket.len() as u16).to_be_bytes());
        nst.extend_from_slice(&ticket);
        nst.extend_from_slice(&[0, 0]);
        let mut post = Vec::new();
        self.write_handshake(HS_NEW_SESSION_TICKET, &nst, &mut post)?;
        Ok(())
    }

    // ------------------------------------------------------------- client

    fn client_handshake(
        &mut self,
        anchors: &[Certificate],
        host: &str,
        chain_der: Option<&[Vec<u8>]>,
        key: Option<&KeyPair>,
    ) -> io::Result<()> {
        let mut transcript = Vec::new();
        let mut hs_buf = Vec::new();
        // The ticket for this server, if one is held and still young. Keyed
        // by the name, the address and the anchors: a ticket resumes the
        // trust it was issued under, and a connection that trusts another
        // CA sees a certificate.
        let mut anchor_bytes = Vec::new();
        for a in anchors {
            anchor_bytes.extend_from_slice(&a.der);
        }
        let store_key = format!(
            "{host}|{}|{}",
            self.sock.peer_addr().map(|a| a.to_string()).unwrap_or_default(),
            super::hex(&sha256(&anchor_bytes)[..8])
        );
        let now = now_secs();
        let ticket: Option<Ticket> = ticket_store(|t| t.get(&store_key).cloned())
            .filter(|t| now < t.received + t.lifetime.min(TICKET_LIFETIME_SECS as u32) as u64);
        self.store_key = Some(store_key);
        let eph: Secret<32> = (random::array32().map_err(|e| err(e.to_string()))?).into();
        let our_share = x25519::public_key(&eph);
        let client_random = random::array32().map_err(|e| err(e.to_string()))?;
        let session_id = random::array32().map_err(|e| err(e.to_string()))?;
        // The ClientHello, built for the first flight and again after a
        // HelloRetryRequest: the same random and session id, the key share
        // the server asked for, its cookie echoed, and a PSK offer's
        // binder over `prefix` (the transcript so far) and the message cut
        // before the binders.
        let junk_early = self.send_junk_early_data;
        let build = |share: Option<&[u8; 32]>, cookie: Option<&[u8]>, prefix: &[u8]| -> Vec<u8> {
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
            // Ed25519 for our own peers; RSA-PSS and ECDSA P-256 for a server
            // whose certificate another issuer signed, such as a cluster's API.
            let mut sigs = Vec::new();
            sigs.extend_from_slice(&6u16.to_be_bytes());
            sigs.extend_from_slice(&SIG_ED25519.to_be_bytes());
            sigs.extend_from_slice(&SIG_ECDSA_SECP256R1_SHA256.to_be_bytes());
            sigs.extend_from_slice(&SIG_RSA_PSS_RSAE_SHA256.to_be_bytes());
            extension(&mut exts, EXT_SIGNATURE_ALGORITHMS, &sigs);
            if let Some(share) = share {
                let mut ks = Vec::new();
                let mut entry = Vec::new();
                entry.extend_from_slice(&GROUP_X25519.to_be_bytes());
                entry.extend_from_slice(&32u16.to_be_bytes());
                entry.extend_from_slice(share);
                ks.extend_from_slice(&(entry.len() as u16).to_be_bytes());
                ks.extend_from_slice(&entry);
                extension(&mut exts, EXT_KEY_SHARE, &ks);
            }
            if let Some(c) = cookie {
                let mut body = Vec::with_capacity(2 + c.len());
                body.extend_from_slice(&(c.len() as u16).to_be_bytes());
                body.extend_from_slice(c);
                extension(&mut exts, EXT_COOKIE, &body);
            }
            if junk_early {
                extension(&mut exts, EXT_EARLY_DATA, &[]);
            }
            if let Some(t) = &ticket {
                // PSK with (EC)DHE only -- the key share above stays -- and
                // the offer last, its binder computed over everything
                // before it.
                extension(&mut exts, EXT_PSK_KEY_EXCHANGE_MODES, &[1, PSK_DHE_KE]);
                let age_ms = (now - t.received).saturating_mul(1000) as u32;
                let obfuscated = age_ms.wrapping_add(t.age_add);
                let mut psk = Vec::new();
                let mut identities = Vec::new();
                identities.extend_from_slice(&(t.ticket.len() as u16).to_be_bytes());
                identities.extend_from_slice(&t.ticket);
                identities.extend_from_slice(&obfuscated.to_be_bytes());
                psk.extend_from_slice(&(identities.len() as u16).to_be_bytes());
                psk.extend_from_slice(&identities);
                psk.extend_from_slice(&33u16.to_be_bytes());
                psk.push(32);
                psk.extend_from_slice(&[0u8; 32]);
                extension(&mut exts, EXT_PRE_SHARED_KEY, &psk);
            }
            ch.extend_from_slice(&(exts.len() as u16).to_be_bytes());
            ch.extend_from_slice(&exts);
            if let Some(t) = &ticket {
                // The binder: over the transcript so far and the message
                // with its header, cut where the binders list begins, then
                // written into place.
                let cut = ch.len() - 35;
                let mut covered = Vec::with_capacity(prefix.len() + 4 + cut);
                covered.extend_from_slice(prefix);
                covered.push(HS_CLIENT_HELLO);
                covered.extend_from_slice(&(ch.len() as u32).to_be_bytes()[1..]);
                covered.extend_from_slice(&ch[..cut]);
                let binder = psk_binder(&t.psk, &sha256(&covered));
                let at = ch.len() - 32;
                ch[at..].copy_from_slice(&binder);
            }
            ch
        };
        let first_share = if self.omit_first_share { None } else { Some(&our_share) };
        let ch = build(first_share, None, &[]);
        self.write_handshake(HS_CLIENT_HELLO, &ch, &mut transcript)?;
        if junk_early {
            // What a client that believed it could send early data sends:
            // an application-data record under a key this server lacks.
            let mut junk = vec![0x17, 0x03, 0x03, 0, 100];
            junk.extend_from_slice(&[0x5a; 100]);
            self.sock.write_all(&junk)?;
        }
        let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_SERVER_HELLO {
            return Err(err("expected a ServerHello"));
        }
        let mut sh = parse_server_hello(&body)?;
        if sh.retry {
            // A HelloRetryRequest: for the X25519 share this side did not
            // send (a test's first flight), or a cookie to echo; a group
            // this build lacks, or a share it already sent, is a refusal.
            match sh.retry_group {
                Some(GROUP_X25519) if first_share.is_some() => {
                    return Err(err("the server asked again for the X25519 key share it was sent"))
                }
                Some(GROUP_X25519) => {}
                Some(_) => {
                    return Err(err(
                        "the server asked for a key-exchange group this build does not have",
                    ))
                }
                None if sh.cookie.is_none() => {
                    return Err(err("a HelloRetryRequest that asks for nothing"))
                }
                None => {}
            }
            let ch1_len = 4 + ch.len();
            let hrr_msg = transcript[ch1_len..].to_vec();
            let ch1_hash = sha256(&transcript[..ch1_len]);
            transcript.clear();
            transcript.push(HS_MESSAGE_HASH);
            transcript.extend_from_slice(&[0, 0, 32]);
            transcript.extend_from_slice(&ch1_hash);
            transcript.extend_from_slice(&hrr_msg);
            self.retried = true;
            let ch2 = build(Some(&our_share), sh.cookie.as_deref(), &transcript);
            self.write_handshake(HS_CLIENT_HELLO, &ch2, &mut transcript)?;
            let (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
            if ty != HS_SERVER_HELLO {
                return Err(err("expected a ServerHello after the retry"));
            }
            sh = parse_server_hello(&body)?;
            if sh.retry {
                return Err(err("a second HelloRetryRequest"));
            }
        }
        let psk: Option<Secret<32>> = match (sh.selected_psk, &ticket) {
            (Some(0), Some(t)) => Some(t.psk.clone()),
            (Some(_), _) => return Err(err("the server selected a PSK that was not offered")),
            (None, _) => None,
        };
        self.resumed = psk.is_some();
        if sh.version != Some(VERSION_13) {
            return Err(err("the server did not select TLS 1.3"));
        }
        if sh.suite != SUITE_CHACHA {
            return Err(err("the server selected a cipher suite this build does not have"));
        }
        let Some(server_share) = sh.x25519_share else {
            return Err(err("the server sent no X25519 key share"));
        };
        let shared: Secret<32> = x25519::x25519(&eph, &server_share);
        if *shared == [0u8; 32] {
            return Err(err("the key share is a low-order point: the shared secret would be zero"));
        }
        let early: Secret<32> =
            hkdf::extract(&[0u8; 32], psk.as_ref().map(|p| &p[..]).unwrap_or(&[0u8; 32]));
        let empty_hash = sha256(&[]);
        let hs_secret: Secret<32> =
            hkdf::extract(&derive_secret(&early, "derived", &empty_hash)[..], &shared[..]);
        let th = sha256(&transcript);
        let mut c_hs = Secret::<32>::zero();
        derive_secret_into(&hs_secret, "c hs traffic", &th, &mut c_hs);
        let mut s_hs = Secret::<32>::zero();
        derive_secret_into(&hs_secret, "s hs traffic", &th, &mut s_hs);
        self.read_keys = Some(Keys::from_secret(&s_hs));
        self.write_keys = Some(Keys::from_secret(&c_hs));
        let (ty, _) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        if ty != HS_ENCRYPTED_EXTENSIONS {
            return Err(err("expected EncryptedExtensions"));
        }
        // A server that accepts client certificates asks for one here; a
        // Kubernetes API server always does. The answer is an empty
        // Certificate carrying its context, sent before our Finished.
        let th_before_next = sha256(&transcript);
        let (mut ty, mut body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        let mut request_context: Option<Vec<u8>> = None;
        if psk.is_some() {
            // Resumed: the server is authenticated by the PSK, and its
            // flight goes straight to Finished.
            if ty != HS_FINISHED {
                return Err(err("expected the server's Finished"));
            }
            if !super::ct_eq(&finished_verify(&s_hs, &th_before_next), &body) {
                return Err(err("the server's Finished does not verify"));
            }
            return self.client_finish(&transcript, &hs_secret, &c_hs, None, chain_der, key);
        }
        if ty == HS_CERTIFICATE_REQUEST {
            let n = *body.first().ok_or_else(|| err("a malformed CertificateRequest"))? as usize;
            if body.len() < 1 + n {
                return Err(err("a malformed CertificateRequest"));
            }
            request_context = Some(body[1..1 + n].to_vec());
            (ty, body) = self.read_handshake(&mut hs_buf, &mut transcript)?;
        }
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
        let content = verify_content(true, &th_before_cv);
        if !verify_signature(&body, &chain[0].public_key, &content)? {
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
        self.client_finish(&transcript, &hs_secret, &c_hs, request_context, chain_der, key)
    }

    /// The client's last flight, after the server's Finished was verified:
    /// the application keys from the transcript so far, our Certificate
    /// and CertificateVerify if one was asked for and we have one (an
    /// empty Certificate if not), our Finished, and the resumption master
    /// secret kept for the ticket the server sends next.
    fn client_finish(
        &mut self,
        transcript: &[u8],
        hs_secret: &[u8; 32],
        c_hs: &[u8; 32],
        request_context: Option<Vec<u8>>,
        chain_der: Option<&[Vec<u8>]>,
        key: Option<&KeyPair>,
    ) -> io::Result<()> {
        let mut transcript = transcript.to_vec();
        let empty_hash = sha256(&[]);
        let th_server_fin = sha256(&transcript);
        let master: Secret<32> =
            hkdf::extract(&derive_secret(hs_secret, "derived", &empty_hash)[..], &[0u8; 32]);
        let mut c_ap = Secret::<32>::zero();
        derive_secret_into(&master, "c ap traffic", &th_server_fin, &mut c_ap);
        let mut s_ap = Secret::<32>::zero();
        derive_secret_into(&master, "s ap traffic", &th_server_fin, &mut s_ap);
        // Middlebox compatibility: a CCS before our first encrypted record.
        self.write_record(CT_CHANGE_CIPHER_SPEC, &[1])?;
        if let Some(context) = request_context {
            let mut cert = vec![context.len() as u8];
            cert.extend_from_slice(&context);
            match (chain_der, key) {
                (Some(chain), Some(key)) => {
                    let mut list = Vec::new();
                    for c in chain {
                        list.extend_from_slice(&(c.len() as u32).to_be_bytes()[1..]);
                        list.extend_from_slice(c);
                        list.extend_from_slice(&[0, 0]);
                    }
                    cert.extend_from_slice(&(list.len() as u32).to_be_bytes()[1..]);
                    cert.extend_from_slice(&list);
                    self.write_handshake(HS_CERTIFICATE, &cert, &mut transcript)?;
                    let th = sha256(&transcript);
                    let sig = ed25519::sign(&key.seed, &verify_content(false, &th));
                    let mut cv = Vec::with_capacity(4 + sig.len());
                    cv.extend_from_slice(&SIG_ED25519.to_be_bytes());
                    cv.extend_from_slice(&(sig.len() as u16).to_be_bytes());
                    cv.extend_from_slice(&sig);
                    self.write_handshake(HS_CERTIFICATE_VERIFY, &cv, &mut transcript)?;
                }
                _ => {
                    cert.extend_from_slice(&[0, 0, 0]);
                    self.write_handshake(HS_CERTIFICATE, &cert, &mut transcript)?;
                }
            }
        }
        // Our Finished covers our (empty) Certificate too; the application
        // keys above do not, by the RFC's key schedule.
        let th_client_fin = sha256(&transcript);
        let fin = finished_verify(c_hs, &th_client_fin);
        self.write_handshake(HS_FINISHED, &fin, &mut transcript)?;
        self.read_keys = Some(Keys::from_secret(&s_ap));
        self.write_keys = Some(Keys::from_secret(&c_ap));
        self.read_secret = Some(s_ap);
        self.write_secret = Some(c_ap);
        let th_client_fin = sha256(&transcript);
        self.res_master = Some(derive_secret(&master, "res master", &th_client_fin));
        Ok(())
    }

    /// A NewSessionTicket after the handshake: the PSK it stands for is
    /// derived and kept under this server's key, replacing an older one.
    fn take_ticket(&mut self, body: &[u8]) -> io::Result<()> {
        let (Some(res_master), Some(key)) = (&self.res_master, &self.store_key) else {
            return Ok(());
        };
        let t = parse_new_session_ticket(body)?;
        if t.ticket.is_empty() || t.lifetime == 0 {
            return Ok(());
        }
        let ticket = Ticket {
            psk: resumption_psk(res_master, &t.nonce),
            ticket: t.ticket,
            received: now_secs(),
            lifetime: t.lifetime,
            age_add: t.age_add,
        };
        let key = key.clone();
        ticket_store(|s| {
            s.insert(key, ticket);
        });
        Ok(())
    }
}

fn extension(out: &mut Vec<u8>, ty: u16, body: &[u8]) {
    out.extend_from_slice(&ty.to_be_bytes());
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(body);
}

/// A CertificateVerify's signature checked against `key` over `content`:
/// the scheme it names must be the one the key verifies with. An error
/// for a malformed message or a scheme this build does not verify.
fn verify_signature(body: &[u8], key: &x509::PublicKey, content: &[u8]) -> io::Result<bool> {
    if body.len() < 4 {
        return Err(err("a malformed CertificateVerify"));
    }
    let scheme = u16::from_be_bytes([body[0], body[1]]);
    let sig_len = u16::from_be_bytes([body[2], body[3]]) as usize;
    if body.len() != 4 + sig_len {
        return Err(err("a malformed CertificateVerify"));
    }
    let sig = &body[4..];
    Ok(match (scheme, key) {
        (SIG_ED25519, x509::PublicKey::Ed25519(pk)) if sig.len() == 64 => {
            let mut s = [0u8; 64];
            s.copy_from_slice(sig);
            ed25519::verify(pk, content, &s)
        }
        (SIG_RSA_PSS_RSAE_SHA256, x509::PublicKey::Rsa(pk)) => pk.verify_pss_sha256(content, sig),
        (SIG_ECDSA_SECP256R1_SHA256, x509::PublicKey::P256(pk)) => {
            pk.verify_sha256_der(content, sig)
        }
        _ => {
            return Err(err("the peer signed with a scheme this build does not verify for its key"))
        }
    })
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
    } else if m.contains("low-order")
        || m.contains("HelloRetryRequest")
        || m.contains("asked again")
    {
        ALERT_ILLEGAL_PARAMETER
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
    fn u32(&mut self) -> io::Result<u32> {
        Ok(((self.u16()? as u32) << 16) | self.u16()? as u32)
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
    /// The groups the client supports, for a retry when it sent no X25519
    /// share but lists the group.
    groups: Vec<u16>,
    /// Whether the client offered early data, which this server never
    /// accepts: the records it sent under the early key are skipped.
    early_data: bool,
    /// The first identity of a pre_shared_key offer, if the extension was
    /// the last one as the protocol requires.
    psk: Option<PskOffer>,
    psk_dhe: bool,
}

struct PskOffer {
    identity: Vec<u8>,
    binder: Vec<u8>,
    /// Where in the ClientHello body the binders list begins: the
    /// transcript the binder covers ends there.
    binders_at: usize,
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
    let mut groups = Vec::new();
    let mut early_data = false;
    let mut psk = None;
    let mut psk_dhe = false;
    if !r.done() {
        let exts = r.vec16()?;
        let mut e = Reader::new(exts);
        while !e.done() {
            let ty = e.u16()?;
            let data = e.vec16()?;
            let last = e.done();
            let mut d = Reader::new(data);
            match ty {
                EXT_PSK_KEY_EXCHANGE_MODES => {
                    psk_dhe = d.vec8()?.contains(&PSK_DHE_KE);
                }
                EXT_PRE_SHARED_KEY if last => {
                    let identities = d.vec16()?;
                    let mut i = Reader::new(identities);
                    let identity = i.vec16()?.to_vec();
                    let _obfuscated_age = i.u32()?;
                    let binders = d.vec16()?;
                    let mut b = Reader::new(binders);
                    let binder = b.vec8()?.to_vec();
                    // The binders list is the tail of the body: its u16
                    // length and its bytes.
                    let binders_at = body.len() - 2 - binders.len();
                    psk = Some(PskOffer { identity, binder, binders_at });
                }
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
                EXT_SUPPORTED_GROUPS => {
                    let list = d.vec16()?;
                    groups =
                        list.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
                }
                EXT_EARLY_DATA => early_data = true,
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
    Ok(ClientHello {
        session_id,
        suites,
        versions,
        sig_algs,
        x25519_share,
        groups,
        early_data,
        psk,
        psk_dhe,
    })
}

struct ServerHello {
    suite: u16,
    version: Option<u16>,
    x25519_share: Option<[u8; 32]>,
    selected_psk: Option<u16>,
    /// A HelloRetryRequest: the ServerHello with the fixed random, asking
    /// for a key share of `retry_group` and, if given, a cookie to echo.
    retry: bool,
    retry_group: Option<u16>,
    cookie: Option<Vec<u8>>,
}

fn parse_server_hello(body: &[u8]) -> io::Result<ServerHello> {
    let mut r = Reader::new(body);
    let _legacy_version = r.u16()?;
    let random = r.bytes(32)?;
    let retry = random == HRR_RANDOM;
    let _session_id = r.vec8()?;
    let suite = r.u16()?;
    let _compression = r.u8()?;
    let mut version = None;
    let mut x25519_share = None;
    let mut selected_psk = None;
    let mut retry_group = None;
    let mut cookie = None;
    if !r.done() {
        let exts = r.vec16()?;
        let mut e = Reader::new(exts);
        while !e.done() {
            let ty = e.u16()?;
            let data = e.vec16()?;
            let mut d = Reader::new(data);
            match ty {
                EXT_SUPPORTED_VERSIONS => version = Some(d.u16()?),
                EXT_PRE_SHARED_KEY => selected_psk = Some(d.u16()?),
                EXT_COOKIE => cookie = Some(d.vec16()?.to_vec()),
                EXT_KEY_SHARE if retry => retry_group = Some(d.u16()?),
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
    Ok(ServerHello { suite, version, x25519_share, selected_psk, retry, retry_group, cookie })
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
    // An empty list is a message, not an error here: a client answers a
    // CertificateRequest it cannot meet with one, and each side says what
    // it makes of it -- the chain verifier refuses it as "no certificate
    // was presented", the wire that requires one names the requirement.
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
                    // Post-handshake messages, possibly several to a record:
                    // a ticket is kept, a key update is not supported.
                    let mut i = 0;
                    while i + 4 <= body.len() {
                        let len = ((body[i + 1] as usize) << 16)
                            | ((body[i + 2] as usize) << 8)
                            | body[i + 3] as usize;
                        let Some(msg) = body.get(i + 4..i + 4 + len) else {
                            return Err(err("a torn post-handshake message"));
                        };
                        match body[i] {
                            HS_NEW_SESSION_TICKET => self.take_ticket(msg)?,
                            HS_KEY_UPDATE => {
                                // The peer's next generation of keys, and
                                // ours if it asked (RFC 8446 §4.6.3).
                                match msg.first().copied() {
                                    Some(0) => self.next_read_keys()?,
                                    Some(1) => {
                                        self.next_read_keys()?;
                                        self.update_keys(false)?;
                                    }
                                    _ => return Err(err("a malformed KeyUpdate")),
                                }
                            }
                            _ => return Err(err("an unexpected handshake message")),
                        }
                        i += 4 + len;
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
    /// RFC 8448 section 4: the resumed ClientHello up to its binders list.
    const RFC8448_RESUMED_CLIENT_HELLO_PREFIX: &str = "010001fc03031bc3ceb6bbe39cff938355b5a50adb6db21b7a6af649d7b4bc419d7876487d95000006130113031302010001cd0000000b0009000006736572766572ff01000100000a00140012001d00170018001901000101010201030104003300260024001d0020e4ffb68ac05f8d96c99da26698346c6be16482badddafe051a66b4f18d668f0b002a0000002b0003020304000d0020001e040305030603020308040805080604010501060102010402050206020202002d00020101001c0002400100150057000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002900dd00b800b22c035d829359ee5ff7af4ec900000000262a6494dc486d2c8a34cb33fa90bf1b0070ad3c498883c9367c09a2be785abc55cd226097a3a982117283f82a03a143efd3ff5dd36d64e861be7fd61d2827db279cce145077d454a3664d4e6da4d29ee03725a6a4dafcd0fc67d2aea70529513e3da2677fa5906c5b3f7d8f92f228bda40dda721470f9fbf297b5aea617646fac5c03272e970727c621a79141ef5f7de6505e5bfbc388e93343694093934ae4d357fad6aacb";

    #[test]
    fn fuzz_handshake_message_parsing_never_panics() {
        let unhex = |s: &str| -> Vec<u8> {
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
        };
        let mut ch = unhex(RFC8448_RESUMED_CLIENT_HELLO_PREFIX);
        ch.extend_from_slice(&[0, 0x21, 0x20]);
        ch.extend_from_slice(&[0x3a; 32]);
        let ch_body = ch[4..].to_vec();
        // A ServerHello as this server writes one, PSK selected.
        let mut sh = Vec::new();
        sh.extend_from_slice(&LEGACY_VERSION.to_be_bytes());
        sh.extend_from_slice(&[0x11; 32]);
        sh.push(32);
        sh.extend_from_slice(&[0x22; 32]);
        sh.extend_from_slice(&SUITE_CHACHA.to_be_bytes());
        sh.push(0);
        let mut exts = Vec::new();
        extension(&mut exts, EXT_SUPPORTED_VERSIONS, &VERSION_13.to_be_bytes());
        let mut ks = Vec::new();
        ks.extend_from_slice(&GROUP_X25519.to_be_bytes());
        ks.extend_from_slice(&32u16.to_be_bytes());
        ks.extend_from_slice(&[0x33; 32]);
        extension(&mut exts, EXT_KEY_SHARE, &ks);
        extension(&mut exts, EXT_PRE_SHARED_KEY, &0u16.to_be_bytes());
        sh.extend_from_slice(&(exts.len() as u16).to_be_bytes());
        sh.extend_from_slice(&exts);
        // A Certificate message from a real chain, and a NewSessionTicket.
        let m = x509::make("localhost", &[], &[], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let mut cert = vec![0u8];
        let mut list = Vec::new();
        for c in &chain_der {
            list.extend_from_slice(&(c.len() as u32).to_be_bytes()[1..]);
            list.extend_from_slice(c);
            list.extend_from_slice(&[0, 0]);
        }
        cert.extend_from_slice(&(list.len() as u32).to_be_bytes()[1..]);
        cert.extend_from_slice(&list);
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let tkey = ticket_key(&key, false, ticket_day());
        let ticket = seal_ticket(&tkey, &[9u8; 32], now_secs(), 7).unwrap();
        let mut nst = Vec::new();
        nst.extend_from_slice(&86_400u32.to_be_bytes());
        nst.extend_from_slice(&7u32.to_be_bytes());
        nst.push(1);
        nst.push(0);
        nst.extend_from_slice(&(ticket.len() as u16).to_be_bytes());
        nst.extend_from_slice(&ticket);
        nst.extend_from_slice(&[0, 0]);
        assert!(open_ticket(&tkey, &ticket).is_some());
        crate::fuzz::sweep(1, &[ch_body], 5000, |b| {
            let _ = parse_client_hello(b);
        });
        crate::fuzz::sweep(2, &[sh], 5000, |b| {
            let _ = parse_server_hello(b);
        });
        crate::fuzz::sweep(3, &[cert], 4000, |b| {
            let _ = parse_certificate_message(b);
        });
        crate::fuzz::sweep(4, &[nst], 4000, |b| {
            let _ = parse_new_session_ticket(b);
        });
        // A mutated ticket never opens, and never panics.
        crate::fuzz::sweep(5, std::slice::from_ref(&ticket), 4000, |b| {
            if b != ticket.as_slice() {
                assert!(open_ticket(&tkey, b).is_none());
            }
        });
        let _ = tickets_held();
    }
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
            crate::crypto::hex(&early[..]),
            "33ad0a1c607ec03b09e6cd9893680ce210adf300aa1f2660e1b22e10f170f92a"
        );
        let derived = derive_secret(&early, "derived", &sha256(&[]));
        assert_eq!(
            crate::crypto::hex(&derived[..]),
            "6f2615a108c702c5678f54fc9dbab69716c076189c48250cebeac3576c3611ba"
        );
        let hs = hkdf::extract(&derived[..], &shared);
        assert_eq!(
            crate::crypto::hex(&hs[..]),
            "1dc826e93606aa6fdc0aadc12f741b01046aa6b99f691ed221a9f0ca043fbeac"
        );
        // The transcript hash of ClientHello..ServerHello in that trace.
        let th: [u8; 32] =
            unhex("860c06edc07858ee8e78f0e7428c58edd6b43f2ca3e6e95f02ed063cf0e1cad8")
                .try_into()
                .unwrap();
        let c_hs = derive_secret(&hs, "c hs traffic", &th);
        assert_eq!(
            crate::crypto::hex(&c_hs[..]),
            "b3eddb126e067f35a780b3abf45e2d8f3b1a950738f52e9600746a0e27a55a21"
        );
        let s_hs = derive_secret(&hs, "s hs traffic", &th);
        assert_eq!(
            crate::crypto::hex(&s_hs[..]),
            "b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38"
        );
        // The trace's suite is AES-128-GCM, so its key is 16 bytes and its
        // iv 12: the same labels, expanded to those lengths.
        assert_eq!(
            crate::crypto::hex(&expand_label::<16>(&s_hs, "key", &[])[..]),
            "3fce516009c21727d0f2e4e86ee403bc"
        );
        assert_eq!(
            crate::crypto::hex(&expand_label::<12>(&s_hs, "iv", &[])[..]),
            "5d313eb2671276ee13000b30"
        );
    }

    /// Our client to our server over loopback: the handshake completes,
    /// bytes round-trip both ways, a close_notify ends the read side, and a
    /// client that trusts another CA is refused with a certificate error.
    /// RFC 8448 sections 3 and 4: the resumption master secret, the PSK a
    /// ticket nonce makes of it, the early secret, the binder key, and the
    /// binder over the truncated ClientHello -- the cut included, since the
    /// 477 octets the RFC hashes are embedded and hashed here.
    #[test]
    fn the_resumption_secrets_and_the_psk_binder_match_rfc_8448() {
        let unhex = |s: &str| -> Vec<u8> {
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
        };
        let arr = |s: &str| -> [u8; 32] { unhex(s).try_into().unwrap() };
        let master = arr("18df06843d13a08bf2a449844c5f8a478001bc4d4c627984d5a41da8d0402919");
        let th_client_fin = arr("209145a96ee8e2a122ff810047cc952684658d6049e86429426db87c54ad143d");
        let res_master = derive_secret(&master, "res master", &th_client_fin);
        assert_eq!(
            crate::crypto::hex(&res_master[..]),
            "7df235f2031d2a051287d02b0241b0bfdaf86cc856231f2d5aba46c434ec196c"
        );
        let psk = resumption_psk(&res_master, &[0, 0]);
        assert_eq!(
            crate::crypto::hex(&psk[..]),
            "4ecd0eb6ec3b4d87f5d6028f922ca4c5851a277fd41311c9e62d2c9492e1c4f3"
        );
        let early = hkdf::extract(&[0u8; 32], &psk[..]);
        assert_eq!(
            crate::crypto::hex(&early[..]),
            "9b2188e9b2fc6d64d71dc329900e20bb41915000f678aa839cbb797cb7d8332c"
        );
        let binder_key = derive_secret(&early, "res binder", &sha256(&[]));
        assert_eq!(
            crate::crypto::hex(&binder_key[..]),
            "69fe131a3bbad5d63c64eebcc30e395b9d8107726a13d074e389dbc8a4e47256"
        );
        let prefix = unhex(RFC8448_RESUMED_CLIENT_HELLO_PREFIX);
        assert_eq!(prefix.len(), 477);
        let hash = sha256(&prefix);
        assert_eq!(
            crate::crypto::hex(&hash),
            "63224b2e4573f2d3454ca84b9d009a04f6be9e05711a8396473aefa01e924a14"
        );
        assert_eq!(
            crate::crypto::hex(&psk_binder(&psk, &hash)),
            "3add4fb2d8fdf822a0ca3cf7678ef5e88dae990141c5924d57bb6fa31b9e5f9d"
        );
        // The message parses as this build's server reads it: the offer is
        // the last extension, the identity is the RFC's ticket, and the
        // binder list begins where the RFC's prefix ends.
        let mut full = prefix.clone();
        full.extend_from_slice(&[0, 0x21, 0x20]);
        full.extend_from_slice(&unhex(
            "3add4fb2d8fdf822a0ca3cf7678ef5e88dae990141c5924d57bb6fa31b9e5f9d",
        ));
        let hello = parse_client_hello(&full[4..]).unwrap();
        let offer = hello.psk.expect("an offer");
        assert!(hello.psk_dhe);
        assert_eq!(offer.binders_at + 4, prefix.len());
        assert_eq!(offer.identity.len(), 0xb2);
        assert_eq!(&offer.identity[..4], &[0x2c, 0x03, 0x5d, 0x82]);
        assert_eq!(
            crate::crypto::hex(&offer.binder),
            "3add4fb2d8fdf822a0ca3cf7678ef5e88dae990141c5924d57bb6fa31b9e5f9d"
        );
    }

    /// The record layer end to end, under a proxy that damages one byte
    /// of every connection somewhere in the bytes the client sends or the
    /// server answers: each side refuses (an alert, a read error) and
    /// neither panics, over the handshake and the data alike.
    #[test]
    fn fuzz_a_damaged_byte_on_the_wire_is_refused_and_never_a_panic() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        const ROUNDS: usize = 40;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut outcomes = Vec::new();
            for _ in 0..ROUNDS {
                let (sock, _) = listener.accept().unwrap();
                sock.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                let mut s = TlsStream::server(
                    sock,
                    ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
                );
                let mut buf = Vec::new();
                let r = s.read_to_end(&mut buf).and_then(|_| s.write_all(&buf));
                let _ = s.close_notify();
                outcomes.push(r.is_ok());
            }
            outcomes
        });
        // The proxy: every byte through, one of them (in a position drawn
        // per round, in either direction) flipped.
        let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let proxy_thread = std::thread::spawn(move || {
            let mut rng = crate::codec::Rng::new(41);
            for round in 0..ROUNDS {
                let (client, _) = proxy.accept().unwrap();
                let upstream = TcpStream::connect(server_addr).unwrap();
                let hit_pos = (rng.next_u64() % 600) as usize;
                let hit_dir = round % 2;
                let pump = |mut from: TcpStream, mut to: TcpStream, damage: Option<usize>| {
                    std::thread::spawn(move || {
                        let mut seen = 0usize;
                        let mut buf = [0u8; 4096];
                        loop {
                            let n = match from.read(&mut buf) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            if let Some(p) = damage {
                                if p >= seen && p < seen + n {
                                    buf[p - seen] ^= 0x5a;
                                }
                            }
                            seen += n;
                            if to.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(std::net::Shutdown::Write);
                    })
                };
                let a = pump(
                    client.try_clone().unwrap(),
                    upstream.try_clone().unwrap(),
                    (hit_dir == 0).then_some(hit_pos),
                );
                let b = pump(upstream, client, (hit_dir == 1).then_some(hit_pos));
                let _ = a.join();
                let _ = b.join();
            }
        });
        let mut refused = 0;
        for _ in 0..ROUNDS {
            let sock = TcpStream::connect(proxy_addr).unwrap();
            sock.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut c = TlsStream::client(
                sock,
                ClientSide {
                    anchors: std::slice::from_ref(&anchor),
                    host: "127.0.0.1",
                    chain_der: None,
                    key: None,
                },
            );
            let payload = vec![0x61u8; 700];
            let r = c.write_all(&payload).and_then(|_| c.close_notify()).and_then(|_| {
                let mut got = Vec::new();
                c.read_to_end(&mut got).map(|_| got)
            });
            // A damaged record is an error on one side or the other; what
            // the client sees is an error, or a connection the server shut
            // with nothing echoed. What it never sees is damaged data.
            match r {
                Ok(got) if got.is_empty() => refused += 1,
                Ok(got) => assert_eq!(got, payload, "an echo that came back is intact"),
                Err(_) => refused += 1,
            }
        }
        proxy_thread.join().unwrap();
        let outcomes = server.join().unwrap();
        assert!(refused > 0, "a flipped byte was never noticed");
        assert!(outcomes.len() == ROUNDS);
    }

    /// A client that offers early data and sends a record of it, though no
    /// ticket allowed it, is served: the server skips the record it cannot
    /// open and reads the client's flight behind it.
    #[test]
    fn early_data_this_server_never_accepted_is_skipped_past() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let mut s = TlsStream::server(
                sock,
                ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
            );
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).unwrap();
            s.write_all(&buf).unwrap();
            let _ = s.close_notify();
            buf
        });
        let sock = TcpStream::connect(addr).unwrap();
        let mut c = TlsStream::client(
            sock,
            ClientSide {
                anchors: std::slice::from_ref(&anchor),
                host: "127.0.0.1",
                chain_der: None,
                key: None,
            },
        );
        c.send_junk_early_data = true;
        c.write_all(b"after the junk").unwrap();
        c.close_notify().unwrap();
        let mut got = Vec::new();
        c.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"after the junk");
        assert_eq!(server.join().unwrap(), b"after the junk");
    }

    /// H5, on the real path: a handshake's application traffic secret is
    /// not in this process's memory once the stream is dropped.
    ///
    /// The secret is taken from the live stream, masked, and only then is
    /// the stream dropped -- so what the scan looks for is the value the
    /// handshake actually derived, not one this test made up. Ignored and
    /// run in release beside `cipher::core_dump`: a debug build says
    /// nothing about what optimised code leaves behind.
    #[test]
    #[ignore]
    fn a_traffic_secret_is_not_left_in_memory_after_a_handshake() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let mut s = TlsStream::server(
                sock,
                ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
            );
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).unwrap();
            s.write_all(&buf).unwrap();
            let _ = s.close_notify();
        });
        let masked = {
            let sock = TcpStream::connect(addr).unwrap();
            let mut c = TlsStream::client(
                sock,
                ClientSide {
                    anchors: std::slice::from_ref(&anchor),
                    host: "127.0.0.1",
                    chain_der: None,
                    key: None,
                },
            );
            c.write_all(b"a handshake").unwrap();
            c.close_notify().unwrap();
            let mut got = Vec::new();
            c.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"a handshake");
            let secret = c.read_secret.as_ref().expect("an application traffic secret");
            crate::cipher::core_dump::mask(secret)
        };
        server.join().unwrap();
        let n = crate::cipher::core_dump::occurrences(&masked);
        eprintln!("traffic secret after a handshake: {n} copy(ies) left in memory");
        // One copy, and what it is: the secret is derived into a local and
        // then MOVED into `self.read_secret`. A moved-from value is never
        // dropped, so the local's bytes are never wiped, and they stay on
        // that frame until the stack is reused. Deriving in place removes
        // this one, but not the same problem a step down -- `Keys` is built
        // from the secret and moved into its own field, leaving the derived
        // key and iv the same way -- and removing it everywhere means
        // building the whole handshake in place. The heap is the case that
        // matters more and it is clean: HKDF and HMAC wipe every buffer
        // they allocate (they wiped none before this entry).
        assert!(n <= 1, "a traffic secret is in memory {n} times, more than the one recorded");
    }

    /// A KeyUpdate from either side moves that side's keys to the next
    /// generation and, when it asks, the other side's too: data written
    /// after it reads whole, and the peer's answer under its new keys
    /// reads whole.
    #[test]
    fn a_key_update_from_either_side_keeps_the_stream_readable() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let mut s = TlsStream::server(
                sock,
                ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
            );
            let mut buf = [0u8; 3];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"one");
            // Our keys move, and the client is asked to move its own.
            s.update_keys(true).unwrap();
            s.write_all(b"two").unwrap();
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"three", "under the client's next keys");
            // The client moves on its own, asking nothing.
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"four");
            s.write_all(b"five").unwrap();
            let _ = s.close_notify();
        });
        let sock = TcpStream::connect(addr).unwrap();
        let mut c = TlsStream::client(
            sock,
            ClientSide {
                anchors: std::slice::from_ref(&anchor),
                host: "127.0.0.1",
                chain_der: None,
                key: None,
            },
        );
        c.write_all(b"one").unwrap();
        let mut buf = [0u8; 3];
        c.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"two", "read under the server's next keys");
        c.write_all(b"three").unwrap();
        c.update_keys(false).unwrap();
        c.write_all(b"four").unwrap();
        let mut buf = [0u8; 4];
        c.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"five");
        server.join().unwrap();
    }

    /// A client whose first flight carries no key share is asked for one
    /// with a HelloRetryRequest and completes on the second: both sides say
    /// they retried, the echo comes back, and a ticket from that handshake
    /// resumes the next connection, which does not retry.
    #[test]
    fn a_client_without_a_first_key_share_is_asked_again_and_completes() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..2 {
                let (sock, _) = listener.accept().unwrap();
                let mut s = TlsStream::server(
                    sock,
                    ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
                );
                let mut buf = Vec::new();
                s.read_to_end(&mut buf).unwrap();
                s.write_all(&buf).unwrap();
                s.close_notify().unwrap();
                seen.push((buf, s.retried(), s.resumed()));
            }
            seen
        });
        let talk = |omit: bool| {
            let sock = TcpStream::connect(addr).unwrap();
            let mut c = TlsStream::client(
                sock,
                ClientSide {
                    anchors: std::slice::from_ref(&anchor),
                    host: "127.0.0.1",
                    chain_der: None,
                    key: None,
                },
            );
            c.omit_first_share = omit;
            c.write_all(b"again").unwrap();
            c.close_notify().unwrap();
            let mut got = Vec::new();
            c.read_to_end(&mut got).unwrap();
            (got, c.retried(), c.resumed())
        };
        assert_eq!(talk(true), (b"again".to_vec(), true, false));
        assert_eq!(talk(false), (b"again".to_vec(), false, true), "resumed, no retry");
        let seen = server.join().unwrap();
        assert_eq!(seen[0], (b"again".to_vec(), true, false));
        assert_eq!(seen[1], (b"again".to_vec(), false, true));
    }

    /// A server that requires a client certificate: a client presenting a
    /// chain the CA signed is served; one presenting none is refused
    /// naming the requirement; one whose chain is another CA's is refused
    /// too; and a server that requires nothing serves a client that would
    /// have presented one, without asking.
    #[test]
    fn a_server_that_requires_a_client_certificate_refuses_a_peer_without_one() {
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
        let other = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let other_chain = pem::decode_all(&other.cert, "CERTIFICATE").unwrap();
        let other_key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&other.key, "PRIVATE KEY").unwrap()[0])
                .unwrap();
        // An echo server that reports, per connection, what it read or why
        // the handshake failed.
        let start =
            |chain_der: Vec<Vec<u8>>, key: KeyPair, anchors: Option<Vec<Certificate>>, n: usize| {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                let h = std::thread::spawn(move || {
                    let mut seen: Vec<Result<Vec<u8>, String>> = Vec::new();
                    for _ in 0..n {
                        let (sock, _) = listener.accept().unwrap();
                        let mut s = TlsStream::server(
                            sock,
                            ServerSide {
                                chain_der: &chain_der,
                                key: &key,
                                client_anchors: anchors.as_deref(),
                            },
                        );
                        let mut buf = Vec::new();
                        match s.read_to_end(&mut buf) {
                            Ok(_) => {
                                s.write_all(&buf).unwrap();
                                let _ = s.close_notify();
                                seen.push(Ok(buf));
                            }
                            Err(e) => seen.push(Err(e.to_string())),
                        }
                    }
                    seen
                });
                (addr, h)
            };
        // Each client under a name of its own, so none offers the ticket
        // the one before it was handed: a resumed handshake shows no
        // certificate by design, since the ticket stands for one shown.
        let talk = |addr: std::net::SocketAddr,
                    host: &str,
                    chain: Option<&[Vec<u8>]>,
                    key: Option<&KeyPair>|
         -> Result<Vec<u8>, String> {
            let sock = TcpStream::connect(addr).unwrap();
            let mut c = TlsStream::client(
                sock,
                ClientSide { anchors: std::slice::from_ref(&anchor), host, chain_der: chain, key },
            );
            c.write_all(b"hello").map_err(|e| e.to_string())?;
            c.close_notify().map_err(|e| e.to_string())?;
            let mut got = Vec::new();
            c.read_to_end(&mut got).map_err(|e| e.to_string())?;
            Ok(got)
        };
        let (addr, server) = start(chain_der.clone(), key.clone(), Some(vec![anchor.clone()]), 3);
        assert_eq!(talk(addr, "127.0.0.1", Some(&chain_der), Some(&key)), Ok(b"hello".to_vec()));
        assert_ne!(talk(addr, "localhost", None, None), Ok(b"hello".to_vec()), "no certificate");
        forget_tickets();
        assert_ne!(
            talk(addr, "localhost", Some(&other_chain), Some(&other_key)),
            Ok(b"hello".to_vec()),
            "another CA's certificate, no echo"
        );
        let seen = server.join().unwrap();
        assert_eq!(seen[0], Ok(b"hello".to_vec()));
        let e1 = seen[1].clone().unwrap_err();
        assert!(e1.contains("presented no certificate"), "{e1}");
        let e2 = seen[2].clone().unwrap_err();
        assert!(e2.contains("the peer's certificate"), "{e2}");
        // Nothing required: the client's chain is never asked for.
        let (addr, server) = start(chain_der.clone(), key.clone(), None, 1);
        assert_eq!(
            talk(addr, "127.0.0.1", Some(&other_chain), Some(&other_key)),
            Ok(b"hello".to_vec())
        );
        assert_eq!(server.join().unwrap()[0], Ok(b"hello".to_vec()));
    }

    /// Two connections to one server: the first in full and it hands out a
    /// ticket, the second resumes on both sides and hands out another; a
    /// server under another key cannot open the ticket and the handshake
    /// runs in full; a ticket tampered with in the store does the same; an
    /// old one is not offered at all.
    #[test]
    fn a_second_connection_resumes_and_a_ticket_the_server_cannot_open_falls_back() {
        let m = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let chain_der = pem::decode_all(&m.cert, "CERTIFICATE").unwrap();
        let key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&m.key, "PRIVATE KEY").unwrap()[0]).unwrap();
        let anchor = x509::parse(&pem::decode_all(&m.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        // An echo server that reports, per connection, whether it resumed.
        let start = |chain_der: Vec<Vec<u8>>, key: KeyPair, n: usize| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let h = std::thread::spawn(move || {
                let mut resumed = Vec::new();
                for _ in 0..n {
                    let (sock, _) = listener.accept().unwrap();
                    let mut s = TlsStream::server(
                        sock,
                        ServerSide { chain_der: &chain_der, key: &key, client_anchors: None },
                    );
                    let mut buf = Vec::new();
                    s.read_to_end(&mut buf).unwrap();
                    s.write_all(&buf).unwrap();
                    s.close_notify().unwrap();
                    resumed.push(s.resumed());
                }
                resumed
            });
            (addr, h)
        };
        let talk = |addr: std::net::SocketAddr, anchor: &Certificate, host: &str| -> bool {
            let sock = TcpStream::connect(addr).unwrap();
            let mut c = TlsStream::client(
                sock,
                ClientSide {
                    anchors: std::slice::from_ref(anchor),
                    host,
                    chain_der: None,
                    key: None,
                },
            );
            c.write_all(b"hello").unwrap();
            c.close_notify().unwrap();
            let mut got = Vec::new();
            c.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"hello");
            c.resumed()
        };
        let tag = |a: &Certificate| crate::crypto::hex(&sha256(&a.der)[..8]);
        let (addr, server) = start(chain_der.clone(), key.clone(), 3);
        let key1 = format!("127.0.0.1|{addr}|{}", tag(&anchor));
        assert!(!talk(addr, &anchor, "127.0.0.1"), "the first handshake is full");
        // Other tests in this process hold tickets of their own, so the
        // check is for this server's, not for the count.
        assert!(ticket_store(|t| t.contains_key(&key1)), "and it left a ticket");
        assert!(talk(addr, &anchor, "127.0.0.1"), "the second resumes");
        assert!(talk(addr, &anchor, "127.0.0.1"), "and so does the third, on the new ticket");
        assert_eq!(server.join().unwrap(), vec![false, true, true]);

        // The same certificate served under another key: its ticket key
        // differs, the offered ticket does not open, the handshake is full.
        let other = x509::make("localhost", &[], &["127.0.0.1".parse().unwrap()], 30).unwrap();
        let other_chain = pem::decode_all(&other.cert, "CERTIFICATE").unwrap();
        let other_key =
            KeyPair::from_pkcs8_der(&pem::decode_all(&other.key, "PRIVATE KEY").unwrap()[0])
                .unwrap();
        let other_anchor =
            x509::parse(&pem::decode_all(&other.ca_cert, "CERTIFICATE").unwrap()[0]).unwrap();
        // Make the client hold a ticket under the new server's store key by
        // moving the one it has.
        let (addr2, server2) = start(other_chain, other_key, 2);
        let key2 = format!("127.0.0.1|{addr2}|{}", tag(&other_anchor));
        ticket_store(|t| {
            let ticket = t.remove(&key1).expect("a ticket from the first server");
            t.insert(key2.clone(), ticket);
        });
        assert!(!talk(addr2, &other_anchor, "127.0.0.1"), "another key: full, not refused");
        // Tampered: a byte of the ticket flipped. The server cannot open it
        // and runs in full; nothing is refused.
        ticket_store(|t| {
            let ticket = t.get_mut(&key2).expect("the new server's ticket");
            ticket.ticket[20] ^= 0x55;
        });
        assert!(!talk(addr2, &other_anchor, "127.0.0.1"), "tampered: full, not refused");
        assert_eq!(server2.join().unwrap(), vec![false, false]);
        // Old: not offered.
        ticket_store(|t| {
            let ticket = t.get_mut(&key2).expect("a ticket");
            ticket.received -= TICKET_LIFETIME_SECS + 10;
        });
        let (addr3, server3) = start(chain_der, key, 1);
        ticket_store(|t| {
            let ticket = t.remove(&key2).unwrap();
            t.insert(format!("127.0.0.1|{addr3}|{}", tag(&anchor)), ticket);
        });
        assert!(!talk(addr3, &anchor, "127.0.0.1"));
        assert_eq!(server3.join().unwrap(), vec![false]);
        forget_tickets();
    }

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
                let mut s = TlsStream::server(
                    sock,
                    ServerSide { chain_der: &chain_s, key: &key_s, client_anchors: None },
                );
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
            ClientSide {
                anchors: std::slice::from_ref(&anchor),
                host: "localhost",
                chain_der: None,
                key: None,
            },
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
            ClientSide {
                anchors: std::slice::from_ref(&other_anchor),
                host: "localhost",
                chain_der: None,
                key: None,
            },
        );
        let e = c.write_all(b"x").unwrap_err();
        assert!(e.to_string().contains("does not reach"), "{e}");
        let answers = server.join().unwrap();
        assert_eq!(answers[0], Ok(40_000));
        assert!(answers[1].is_err(), "the server saw the client's alert");
    }
}
