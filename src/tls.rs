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
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};

/// The node's certificate chain, PEM, leaf first.
pub const CERT_ENV: &str = "CELASTRO_TLS_CERT";
/// The node's private key, PEM: a `PRIVATE KEY` block holding an Ed25519
/// key in PKCS#8, as `celastro tls init`, openssl and cert-manager write it.
pub const KEY_ENV: &str = "CELASTRO_TLS_KEY";
/// The CA every node's certificate chains to, PEM; what a peer is verified
/// against.
pub const CA_ENV: &str = "CELASTRO_TLS_CA";
/// `required` makes the wire ask every peer for a certificate the CA
/// signed and refuse one without; unset or `off` asks for none.
pub const CLIENT_AUTH_ENV: &str = "CELASTRO_TLS_CLIENT_AUTH";

/// The four PEM files `celastro tls init` writes: a CA, its key, and a
/// certificate it signed with its key.
pub struct Material {
    pub ca_cert: String,
    pub ca_key: String,
    pub cert: String,
    pub key: String,
}

/// Standard base64, for a Secret's data.
pub fn base64(data: &[u8]) -> String {
    crate::crypto::pem::base64_encode(data)
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
    /// Complete the handshake, when the stream has one, under whatever
    /// read timeout is set: a listener runs it before its first read,
    /// with a timeout of its own, and stops at a failure rather than
    /// reading on. Nothing to do on a plain socket.
    fn handshake(&mut self) -> io::Result<()> {
        Ok(())
    }
    /// The names of the peer's certificate, when the stream asked for one
    /// and verified it; nothing on a plain socket.
    fn peer_names(&self) -> Option<Vec<String>> {
        None
    }
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
    /// When the leaf stops being valid, seconds since the epoch: what
    /// `SHOW HEALTH` and the metrics say, since at that instant every
    /// peer refuses this node and every client does too.
    not_after: i64,
    /// The earliest end among the anchors: when this node stops accepting
    /// everyone else.
    anchors_not_after: i64,
    /// Whether the wire asks every peer for a certificate the CA signed
    /// and refuses one without: `CELASTRO_TLS_CLIENT_AUTH=required`. The
    /// console never asks; browsers and tools speak to it with the token.
    client_auth: bool,
    /// Whether this node's own certificate can be presented as a client
    /// certificate: its extended key usage names client authentication,
    /// or names nothing. A set made by `tls init` before 0.67.0 names
    /// server authentication alone.
    serves_as_client: bool,
}

/// Refused when the wire is to require client certificates and this
/// node's own cannot be one: every peer would refuse this node, and the
/// cluster would fail on the wire with nothing to say which certificate
/// was at fault.
fn check_client_purpose(
    leaf: &crate::crypto::x509::Certificate,
    cert_path: &str,
    client_auth: bool,
) -> Result<()> {
    if client_auth && leaf.fit_for(crate::crypto::x509::Purpose::ClientAuth).is_err() {
        return Err(Error::Plan(format!(
            "{CLIENT_AUTH_ENV}=required, but the certificate at {cert_path} cannot serve as a \
             client certificate: its extended key usage names server authentication alone (a \
             set made by `celastro tls init` before 0.67.0, or an issuer asked for that alone). \
             Make a new set with `celastro tls init`, or have the issuer name client \
             authentication too, on every node, before turning the requirement on"
        )));
    }
    Ok(())
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
        let read = |what: &str, path: &str| -> Result<String> {
            std::fs::read_to_string(path).map_err(|e| read_err(what, path, e))
        };
        let (cert_text, mut key_text, ca_text) =
            (read("certificate", &cert)?, read("key", &key)?, read("CA", &ca)?);
        let client_auth = match std::env::var(CLIENT_AUTH_ENV).ok().filter(|v| !v.is_empty()) {
            None => false,
            Some(v) => match v.trim().to_ascii_lowercase().as_str() {
                "required" | "require" | "on" => true,
                "off" | "none" | "no" => false,
                other => {
                    return Err(Error::Plan(format!(
                        "{CLIENT_AUTH_ENV}: `{other}` is not `required` or `off`"
                    )))
                }
            },
        };
        let tls =
            Tls::from_texts((&cert, &cert_text), (&key, &key_text), (&ca, &ca_text), client_auth);
        // The key file's text is a copy of the seed too.
        crate::cipher::wipe_string(&mut key_text);
        tls.map(Some)
    }

    /// The material as text, each with the name its errors carry (a file's
    /// path, or whatever the caller has): what `from_env` does once the
    /// files are read, for a caller that has the PEMs in hand and no
    /// environment to speak of.
    pub(crate) fn from_texts(
        (cert, cert_text): (&str, &str),
        (key, key_text): (&str, &str),
        (ca, ca_text): (&str, &str),
        client_auth: bool,
    ) -> Result<Tls> {
        use crate::crypto::{pem, x509};
        let chain_der = pem::decode_all(cert_text, "CERTIFICATE")
            .map_err(|e| read_err("certificate", cert, e))?;
        if chain_der.is_empty() {
            return Err(read_err("certificate", cert, "no certificate in the file"));
        }
        let leaf = x509::parse(&chain_der[0]).map_err(|e| read_err("certificate", cert, e))?;
        let mut key_der =
            pem::decode_all(key_text, "PRIVATE KEY").map_err(|e| read_err("key", key, e))?;
        let Some(key_der_first) = key_der.first() else {
            let what = if key_text.contains("ENCRYPTED PRIVATE KEY") {
                "an encrypted private key, which this build does not read: decrypt it first"
            } else if key_text.contains("RSA PRIVATE KEY") || key_text.contains("EC PRIVATE KEY") {
                "an RSA or EC key; the node's key must be Ed25519, in a PKCS#8 PRIVATE KEY block"
            } else {
                "no PRIVATE KEY block (PKCS#8) in the file"
            };
            return Err(read_err("key", key, what));
        };
        let pair =
            x509::KeyPair::from_pkcs8_der(key_der_first).map_err(|e| read_err("key", key, e));
        // The seed came through the PEM text and its DER: both copies
        // wiped before they are freed, whatever the parse said.
        for d in key_der.iter_mut() {
            crate::cipher::wipe(d);
        }
        let pair = pair?;
        if leaf.ed25519_key() != Some(&pair.public) {
            return Err(Error::Plan(
                "the TLS certificate and key do not go together: the certificate's key must be the \
                 Ed25519 key given (a chain another issuer signed may be RSA or P-256 above it)"
                    .into(),
            ));
        }
        let mut anchors = Vec::new();
        for der in pem::decode_all(ca_text, "CERTIFICATE").map_err(|e| read_err("CA", ca, e))? {
            anchors.push(x509::parse_anchor(&der).map_err(|e| read_err("CA", ca, e))?);
        }
        if anchors.is_empty() {
            return Err(read_err("CA", ca, "no certificate in the file"));
        }
        let anchors_not_after = anchors.iter().map(|a| a.not_after).min().unwrap_or(0);
        check_client_purpose(&leaf, cert, client_auth)?;
        let serves_as_client = leaf.fit_for(crate::crypto::x509::Purpose::ClientAuth).is_ok();
        Ok(Tls {
            chain_der,
            key: pair,
            anchors,
            not_after: leaf.not_after,
            anchors_not_after,
            client_auth,
            serves_as_client,
        })
    }

    /// Whether the wire requires a peer's certificate.
    pub fn client_auth(&self) -> bool {
        self.client_auth
    }

    /// Whether this node's certificate can be presented as a client
    /// certificate, which a peer requiring one needs of it.
    pub fn serves_as_client(&self) -> bool {
        self.serves_as_client
    }

    /// When this node's certificate expires, seconds since the epoch.
    pub fn expires_at(&self) -> i64 {
        self.not_after
    }

    /// When the first of the trust anchors expires, seconds since the epoch.
    pub fn anchors_expire_at(&self) -> i64 {
        self.anchors_not_after
    }

    /// A connection this node accepted, encrypted; the handshake happens
    /// on its first read or write.
    pub fn accept(&self, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
        use crate::crypto::tls13::{ServerSide, TlsStream};
        Ok(Box::new(TlsStream::server(
            sock,
            ServerSide { chain_der: &self.chain_der, key: &self.key, client_anchors: None },
        )))
    }

    /// A connection the wire accepted: as [`accept`](Self::accept), and
    /// the peer is asked for a certificate the CA signed when this node
    /// requires one.
    pub fn accept_wire(&self, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
        use crate::crypto::tls13::{ServerSide, TlsStream};
        Ok(Box::new(TlsStream::server(
            sock,
            ServerSide {
                chain_der: &self.chain_der,
                key: &self.key,
                client_anchors: self.client_auth.then_some(self.anchors.as_slice()),
            },
        )))
    }

    /// A connection this node opened to `host`, encrypted and verified on
    /// its first use: the peer's chain reaches the CA and names `host`.
    /// This node's own certificate is presented when the peer asks.
    pub fn connect(&self, sock: TcpStream, host: &str) -> io::Result<Box<dyn Stream>> {
        use crate::crypto::tls13::{ClientSide, TlsStream};
        Ok(Box::new(TlsStream::client(
            sock,
            ClientSide {
                anchors: &self.anchors,
                host,
                chain_der: Some(&self.chain_der),
                key: Some(&self.key),
            },
        )))
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
    fn handshake(&mut self) -> io::Result<()> {
        crate::crypto::tls13::TlsStream::handshake(self)
    }
    fn peer_names(&self) -> Option<Vec<String>> {
        crate::crypto::tls13::TlsStream::peer_names(self).map(<[String]>::to_vec)
    }
}

/// One HTTP/1.1 request: the method, the path, extra headers, an optional
/// JSON body. `Host`, `Accept`, `Content-Type`, `Content-Length` and
/// `Connection: close` are added.
pub struct HttpRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: Option<&'a str>,
}

/// One HTTPS/1.1 request over this crate's TLS at `addr` (`host:port`),
/// the server verified against the CA in `ca_pem` as `server_name`,
/// `timeout` on the connect and each read (zero: no bound on the reads).
/// The status and the body come back; a chunked body is joined. What
/// `celastro tls secret` uses to reach a cluster's API from inside a
/// pod, and `celastro send` a console over TLS.
pub fn https_request(
    addr: &str,
    server_name: &str,
    ca_pem: &str,
    req: &HttpRequest<'_>,
    timeout: Duration,
) -> Result<(u16, String)> {
    use crate::crypto::tls13::{ClientSide, TlsStream};
    use crate::crypto::{pem, x509};
    let mut anchors = Vec::new();
    for der in pem::decode_all(ca_pem, "CERTIFICATE")? {
        anchors.push(x509::parse_anchor(&der)?);
    }
    if anchors.is_empty() {
        return Err(Error::Plan("no certificate in the CA given".into()));
    }
    let sock = dial(addr, timeout)?;
    let mut s = TlsStream::client(
        sock,
        ClientSide { anchors: &anchors, host: server_name, chain_der: None, key: None },
    );
    let request = http_text(server_name, req);
    s.write_all(request.as_bytes()).map_err(Error::Io)?;
    let mut raw = Vec::new();
    // A stream that ends without a close_notify is an error (0.87.0),
    // with what came before it in `raw`: an answer the headers frame is
    // whole whatever ended the stream; one framed by the end alone is
    // taken only from a stream that ended properly.
    let cut = match s.read_to_end(&mut raw) {
        Ok(_) => false,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => true,
        Err(e) => return Err(Error::Io(e)),
    };
    parse_response(&raw, cut)
}

/// How many TLS handshakes this process has served that resumed from a
/// session ticket rather than running in full. A client that connects
/// again within a day -- `celastro --url`, a browser, a node dialling
/// a peer -- resumes; the count says it did.
pub fn resumed_handshakes() -> u64 {
    crate::crypto::tls13::resumed_handshakes()
}

/// The same request in the clear, for a console that serves plain HTTP.
pub fn http_request(
    addr: &str,
    host: &str,
    req: &HttpRequest<'_>,
    timeout: Duration,
) -> Result<(u16, String)> {
    let mut sock = dial(addr, timeout)?;
    let request = http_text(host, req);
    sock.write_all(request.as_bytes()).map_err(Error::Io)?;
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).map_err(Error::Io)?;
    parse_response(&raw, false)
}

fn dial(addr: &str, timeout: Duration) -> Result<TcpStream> {
    let resolved = addr
        .to_socket_addrs()
        .map_err(Error::Io)?
        .next()
        .ok_or_else(|| Error::Plan(format!("{addr} resolves to nothing")))?;
    // A zero timeout means no bound on the reads (a backup answers when it
    // is done), never a connect that gives up at once.
    let connect = if timeout.is_zero() {
        Duration::from_secs(10)
    } else {
        timeout.min(Duration::from_secs(10))
    };
    let sock = TcpStream::connect_timeout(&resolved, connect).map_err(Error::Io)?;
    let io = if timeout.is_zero() { None } else { Some(timeout) };
    sock.set_read_timeout(io).map_err(Error::Io)?;
    sock.set_write_timeout(io).map_err(Error::Io)?;
    Ok(sock)
}

fn http_text(host: &str, req: &HttpRequest<'_>) -> String {
    let body = req.body.unwrap_or("");
    let mut text = format!("{} {} HTTP/1.1\r\nHost: {host}\r\n", req.method, req.path);
    for (k, v) in req.headers {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    text.push_str(&format!(
        "Accept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    ));
    text
}

fn parse_response(raw: &[u8], cut: bool) -> Result<(u16, String)> {
    let text = String::from_utf8_lossy(raw).to_string();
    let (head, rest) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| Error::Plan("the server's answer has no header end".into()))?;
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Plan(format!("the server's answer has no status: {head}")))?;
    let chunked = head.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let content_length: Option<usize> = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case("content-length").then(|| v.trim().parse().ok())?
    });
    let body = if chunked {
        if cut && !chunked_ended(rest) {
            return Err(Error::Plan("the server's answer was cut before its last chunk".into()));
        }
        dechunk(rest)
    } else if let Some(n) = content_length {
        if rest.len() < n {
            return Err(Error::Plan("the server's answer was cut short of its length".into()));
        }
        rest[..n].to_string()
    } else if cut {
        return Err(Error::Plan(
            "the server's answer had no length and the connection was cut before it ended".into(),
        ));
    } else {
        rest.to_string()
    };
    Ok((status, body))
}

/// Whether a chunked body reached its last, empty chunk.
fn chunked_ended(text: &str) -> bool {
    let mut rest = text;
    while let Some((size_line, after)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size_line.trim().split(';').next().unwrap_or(""), 16)
        else {
            return false;
        };
        if size == 0 {
            return true;
        }
        if after.len() < size {
            return false;
        }
        rest = after[size..].strip_prefix("\r\n").unwrap_or("");
    }
    false
}

/// The pieces of a chunked body, joined.
fn dechunk(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some((size_line, after)) = rest.split_once("\r\n") {
        let size = usize::from_str_radix(size_line.trim().split(';').next().unwrap_or("0"), 16)
            .unwrap_or(0);
        if size == 0 || after.len() < size {
            break;
        }
        out.push_str(&after[..size]);
        rest = after[size..].strip_prefix("\r\n").unwrap_or("");
    }
    out
}

/// `sock`, encrypted by `tls` when there is one: the one call every listener
/// and every dial makes.
pub fn accept(tls: Option<&Arc<Tls>>, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
    match tls {
        Some(t) => t.accept(sock),
        None => Ok(Box::new(sock)),
    }
}

/// `sock` as the wire accepted it: [`accept`] with the peer asked for its
/// certificate when the node requires one.
pub fn accept_wire(tls: Option<&Arc<Tls>>, sock: TcpStream) -> io::Result<Box<dyn Stream>> {
    match tls {
        Some(t) => t.accept_wire(sock),
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

#[cfg(test)]
mod purpose_tests {
    use super::*;
    use crate::crypto::x509::{self, ExtKeyUsage, Purpose};

    /// A certificate naming server authentication alone is refused with
    /// the requirement on and taken without it; one naming both, or
    /// nothing, is taken either way.
    #[test]
    fn a_server_only_certificate_is_refused_when_client_certificates_are_required() {
        let pem = std::fs::read_to_string(format!(
            "{}/tests/pki/client-only.crt",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let mut leaf =
            x509::parse(&crate::crypto::pem::decode_all(&pem, "CERTIFICATE").unwrap()[0]).unwrap();
        leaf.ext_key_usage = Some(ExtKeyUsage { server_auth: true, client_auth: false });
        assert!(leaf.fit_for(Purpose::ClientAuth).is_err());
        let e = check_client_purpose(&leaf, "./tls/tls.crt", true).unwrap_err().to_string();
        assert!(e.contains("before 0.67.0") && e.contains("./tls/tls.crt"), "{e}");
        check_client_purpose(&leaf, "./tls/tls.crt", false).unwrap();
        leaf.ext_key_usage = Some(ExtKeyUsage { server_auth: true, client_auth: true });
        check_client_purpose(&leaf, "./tls/tls.crt", true).unwrap();
        leaf.ext_key_usage = None;
        check_client_purpose(&leaf, "./tls/tls.crt", true).unwrap();
    }
}
