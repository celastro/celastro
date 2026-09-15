//! Encryption in transit, behind the `tls` cargo feature: the wire between
//! nodes and the console's traffic from clients, both over rustls. Off by
//! default, and off unless every one of three files is named in the
//! environment: the node's certificate, its key, and the CA that every
//! node's certificate chains to. A node serves both listeners with the
//! first two and verifies every peer -- the wire to another node, and the
//! health probe to its own console -- against the third, by the name it
//! reached the peer with. The tokens stay: a certificate says which node
//! is talking, the token says it may.
//!
//! Without the feature the module is the same surface with nothing behind
//! it: a build that cannot do TLS refuses to start with those variables
//! set, rather than serving plain and saying nothing.

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

#[cfg(feature = "tls")]
mod with_rustls {
    use super::*;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::{
        ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    };

    /// What a node holds: how it serves, and how it verifies a peer.
    pub struct Tls {
        server: Arc<ServerConfig>,
        client: Arc<ClientConfig>,
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
        /// From the three files, or `None` when none is named.
        pub fn from_env() -> Result<Option<Tls>> {
            let Some((cert, key, ca)) = names_from_env()? else { return Ok(None) };
            // ring, once per process; a second install is not an error.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert)
                .map_err(|e| read_err("certificate", &cert, e))?
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| read_err("certificate", &cert, e))?;
            if chain.is_empty() {
                return Err(read_err("certificate", &cert, "no certificate in the file"));
            }
            let key = PrivateKeyDer::from_pem_file(&key).map_err(|e| read_err("key", &key, e))?;
            let mut roots = RootCertStore::empty();
            let mut any = false;
            for c in CertificateDer::pem_file_iter(&ca).map_err(|e| read_err("CA", &ca, e))? {
                let c = c.map_err(|e| read_err("CA", &ca, e))?;
                roots.add(c).map_err(|e| read_err("CA", &ca, e))?;
                any = true;
            }
            if !any {
                return Err(read_err("CA", &ca, "no certificate in the file"));
            }
            let server = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .map_err(|e| {
                    Error::Plan(format!("the TLS certificate and key do not go together: {e}"))
                })?;
            let client =
                ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
            Ok(Some(Tls { server: Arc::new(server), client: Arc::new(client) }))
        }

        /// A connection this node accepted, encrypted.
        pub fn accept(&self, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
            let conn = ServerConnection::new(self.server.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Box::new(StreamOwned::new(conn, sock)))
        }

        /// A connection this node opened to `host`, encrypted and verified:
        /// the peer's certificate chains to the CA and names `host`.
        pub fn connect(&self, sock: TcpStream, host: &str) -> io::Result<Box<dyn Stream>> {
            let name = ServerName::try_from(host.to_string())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{host}: {e}")))?;
            let conn = ClientConnection::new(self.client.clone(), name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Box::new(StreamOwned::new(conn, sock)))
        }
    }

    impl Stream for StreamOwned<ServerConnection, TcpStream> {
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
            self.conn.send_close_notify();
            let _ = self.conn.complete_io(&mut self.sock);
            self.sock.shutdown(Shutdown::Write)
        }
    }

    impl Stream for StreamOwned<ClientConnection, TcpStream> {
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
            self.conn.send_close_notify();
            let _ = self.conn.complete_io(&mut self.sock);
            self.sock.shutdown(Shutdown::Write)
        }
    }
}

#[cfg(feature = "tls")]
pub use with_rustls::Tls;

/// The same surface with nothing behind it.
#[cfg(not(feature = "tls"))]
pub struct Tls {
    _never: (),
}

#[cfg(not(feature = "tls"))]
impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tls(unavailable)")
    }
}

#[cfg(not(feature = "tls"))]
impl Tls {
    /// `None` when no file is named; an error, naming the build, when one is.
    pub fn from_env() -> Result<Option<Tls>> {
        match names_from_env()? {
            None => Ok(None),
            Some(_) => Err(Error::Plan(format!(
                "{CERT_ENV} is set, but this build of celastro carries no TLS; build it with \
                 --features tls, or unset {CERT_ENV}, {KEY_ENV} and {CA_ENV}"
            ))),
        }
    }

    pub fn accept(&self, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
        Ok(Box::new(sock))
    }

    pub fn connect(&self, sock: TcpStream, _host: &str) -> io::Result<Box<dyn Stream>> {
        Ok(Box::new(sock))
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
