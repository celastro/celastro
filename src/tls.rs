//! Encryption in transit: the wire between nodes and the console's traffic
//! from clients, over the TLS 1.3 in `crate::crypto` -- one suite, X25519,
//! Ed25519 certificates, nothing else, and unaudited, as the README says.
//! Off unless every one of three files is named in the environment: the
//! node's certificate chain, its key, and the CA that every node's
//! certificate chains to. A node serves both listeners with the first two
//! and verifies every peer -- the wire to another node, and the health
//! probe to its own console -- against the third, by the name it reached
//! the peer with. The tokens stay: a certificate says which node is
//! talking, the token says it may.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};

/// The node's certificate chain, PEM, leaf first.
pub const CERT_ENV: &str = "CELASTRO_TLS_CERT";
/// The node's private key, PEM (PKCS#8, PKCS#1 or SEC1).
pub const KEY_ENV: &str = "CELASTRO_TLS_KEY";
/// The CA every node's certificate chains to, PEM; what a peer is verified
/// against.
pub const CA_ENV: &str = "CELASTRO_TLS_CA";

/// The four PEM files `celastro-cli tls init` writes: a CA, its key, and a
/// certificate it signed with its key.
pub struct Material {
    pub ca_cert: String,
    pub ca_key: String,
    pub cert: String,
    pub key: String,
}

/// A self-signed Ed25519 CA and a certificate for `name` carrying `dns`
/// and `ips`, valid from an hour ago for `days`.
pub fn make_material(
    name: &str,
    dns: &[String],
    ips: &[std::net::IpAddr],
    days: i64,
) -> Result<Material> {
    let m = crate::crypto::x509::make(name, dns, ips, days)?;
    Ok(Material { ca_cert: m.ca_cert, ca_key: m.ca_key, cert: m.cert, key: m.key })
}

/// A socket, plain or encrypted, as the wire and the console see one: the
/// bytes, and the four things they set on the socket underneath.
pub trait Stream: Read + Write + Send {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()>;
    fn set_nodelay(&self, on: bool) -> io::Result<()>;
    /// Say the sending is over: a close-notify when encrypted, then the
    /// write half of the socket.
    fn shutdown_write(&mut self) -> io::Result<()>;
}

impl Stream for TcpStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, d)
    }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, d)
    }
    fn set_nodelay(&self, on: bool) -> io::Result<()> {
        TcpStream::set_nodelay(self, on)
    }
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.shutdown(Shutdown::Write)
    }
}

/// The three file names, when all three are set. One or two set is refused:
/// a node half-configured for TLS would serve plain and think it did not.
fn names_from_env() -> Result<Option<(String, String, String)>> {
    let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    match (get(CERT_ENV), get(KEY_ENV), get(CA_ENV)) {
        (None, None, None) => Ok(None),
        (Some(c), Some(k), Some(a)) => Ok(Some((c, k, a))),
        _ => Err(Error::Plan(format!(
            "TLS needs all three of {CERT_ENV}, {KEY_ENV} and {CA_ENV}, or none of them"
        ))),
    }
}

/// What a node holds: its certificate chain and key to serve with, and the
/// CA every peer is verified against.
pub struct Tls {
    chain_der: Vec<Vec<u8>>,
    key: crate::crypto::x509::KeyPair,
    anchors: Vec<crate::crypto::x509::Certificate>,
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tls")
    }
}

fn read_err(what: &str, path: &str, e: impl fmt::Display) -> Error {
    Error::Plan(format!("cannot read the TLS {what} at {path}: {e}"))
}

impl Tls {
    /// From the three files, or `None` when none is named. The chain's
    /// leaf must be an Ed25519 certificate whose key is the private key
    /// given; the CA file may hold several certificates.
    pub fn from_env() -> Result<Option<Tls>> {
        let Some((cert, key, ca)) = names_from_env()? else { return Ok(None) };
        use crate::crypto::{pem, x509};
        let read = |what: &str, path: &str| -> Result<String> {
            std::fs::read_to_string(path).map_err(|e| read_err(what, path, e))
        };
        let chain_der = pem::decode_all(&read("certificate", &cert)?, "CERTIFICATE")
            .map_err(|e| read_err("certificate", &cert, e))?;
        if chain_der.is_empty() {
            return Err(read_err("certificate", &cert, "no certificate in the file"));
        }
        let leaf = x509::parse(&chain_der[0]).map_err(|e| read_err("certificate", &cert, e))?;
        let key_text = read("key", &key)?;
        let key_der =
            pem::decode_all(&key_text, "PRIVATE KEY").map_err(|e| read_err("key", &key, e))?;
        let Some(key_der) = key_der.first() else {
            return Err(read_err("key", &key, "no PRIVATE KEY block (PKCS#8) in the file"));
        };
        let pair = x509::KeyPair::from_pkcs8_der(key_der).map_err(|e| read_err("key", &key, e))?;
        if pair.public != leaf.public_key {
            return Err(Error::Plan("the TLS certificate and key do not go together".into()));
        }
        let ca_text = read("CA", &ca)?;
        let mut anchors = Vec::new();
        for der in pem::decode_all(&ca_text, "CERTIFICATE").map_err(|e| read_err("CA", &ca, e))? {
            anchors.push(x509::parse(&der).map_err(|e| read_err("CA", &ca, e))?);
        }
        if anchors.is_empty() {
            return Err(read_err("CA", &ca, "no certificate in the file"));
        }
        Ok(Some(Tls { chain_der, key: pair, anchors }))
    }

    /// A connection this node accepted, encrypted; the handshake happens
    /// on its first read or write.
    pub fn accept(&self, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
        use crate::crypto::tls13::{ServerSide, TlsStream};
        Ok(Box::new(TlsStream::server(
            sock,
            ServerSide { chain_der: &self.chain_der, key: &self.key },
        )))
    }

    /// A connection this node opened to `host`, encrypted and verified on
    /// its first use: the peer's chain reaches the CA and names `host`.
    pub fn connect(&self, sock: TcpStream, host: &str) -> io::Result<Box<dyn Stream>> {
        use crate::crypto::tls13::{ClientSide, TlsStream};
        Ok(Box::new(TlsStream::client(sock, ClientSide { anchors: &self.anchors, host })))
    }
}

impl Stream for crate::crypto::tls13::TlsStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.sock.set_read_timeout(d)
    }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.sock.set_write_timeout(d)
    }
    fn set_nodelay(&self, on: bool) -> io::Result<()> {
        self.sock.set_nodelay(on)
    }
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.close_notify()
    }
}

/// `sock`, encrypted by `tls` when there is one: the one call every listener
/// and every dial makes.
pub fn accept(tls: Option<&Arc<Tls>>, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
    match tls {
        Some(t) => t.accept(sock),
        None => Ok(Box::new(sock)),
    }
}

/// `sock` to `host`, encrypted and verified by `tls` when there is one.
pub fn connect(tls: Option<&Arc<Tls>>, sock: TcpStream, host: &str) -> io::Result<Box<dyn Stream>> {
    match tls {
        Some(t) => t.connect(sock, host),
        None => Ok(Box::new(sock)),
    }
}
