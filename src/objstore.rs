//! An object store for the `archived` tier: an S3-compatible endpoint, or the
//! local directory that stood in for one until now.
//!
//! **Scope, decided rather than discovered.** One surface: a bucket, a key,
//! and four operations -- `PUT` an object, `GET` a byte range of one, `HEAD`
//! for its size, `DELETE` it -- addressed path-style (`/bucket/key`) and
//! signed with AWS Signature Version 4, which every S3-compatible server
//! (S3 itself, MinIO, Ceph RGW, localstack) accepts. Credentials come from
//! the environment, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with an
//! optional `AWS_SESSION_TOKEN`, and never from the catalog or the manifest.
//! Out of scope, and deliberately: **TLS**, because `std` has no TLS and the
//! crate takes no dependencies, so the endpoint is plain HTTP -- a MinIO on
//! the same host, or a TLS-terminating proxy in front of a real bucket;
//! multipart upload, because a segment is one `PUT` and the size that would
//! need multipart is far past the segment cap; and listing, so an object a
//! failed publication left behind is not reclaimed the way a local orphan
//! is. Each of those is a later entry, not a gap this one hides.
//!
//! Everything under this module is in-tree for the same reason the rest of
//! the crate is: SHA-256 and HMAC for the signature, a small HTTP/1.1 client
//! over a `TcpStream`, and the signing itself. Each is pinned against the
//! published test vectors below, because a signer that is wrong by one byte
//! is a client that is refused by every request.
//!
//! The read model is the one the design budgets for: an archived read is a
//! chain of dependent round trips. A remote segment is opened by reading its
//! footer with two ranged `GET`s, and each component faults in with one more,
//! reported by `EXPLAIN` like any fault-in. Nothing is cached on local disk;
//! the object is the segment's only copy while it is archived, and moving it
//! back to a local tier is a `GET` of the whole object into `segments/`,
//! published like any other file, followed by the `DELETE`.

use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};

/// The four operations the archive tier needs, and no more.
pub trait ObjectStore: Send + Sync + fmt::Debug {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    /// `len` bytes from `off`. Short reads are errors, not partial answers.
    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// The object's size, or `None` when there is no such object.
    fn size(&self, key: &str) -> Result<Option<u64>>;
    fn delete(&self, key: &str) -> Result<()>;
}

/// Where the archive lives. `endpoint` is `host:port` of an S3-compatible
/// server reached over plain HTTP; `None` keeps the local directory.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ArchiveOpts {
    pub endpoint: Option<String>,
    pub bucket: String,
    /// Prepended to every key. `""` or `"celastro/"`.
    pub prefix: String,
    /// The region the signature names; MinIO accepts any, S3 wants its own.
    pub region: String,
}

/// A store and the key prefix every object of this database carries. What
/// a shard holds when the `archived` tier is an object store rather than a
/// directory.
#[derive(Debug, Clone)]
pub struct ArchiveHandle {
    pub store: std::sync::Arc<dyn ObjectStore>,
    pub prefix: String,
}

/// An S3-compatible store over plain HTTP.
pub struct S3Store {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl fmt::Debug for S3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the secret.
        write!(f, "S3Store({}/{}, region {})", self.endpoint, self.bucket, self.region)
    }
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const SERVICE: &str = "s3";

impl S3Store {
    /// Build from the options and the environment. The credentials are read
    /// here and held in memory; they are never written anywhere.
    pub fn from_env(opts: &ArchiveOpts) -> Result<S3Store> {
        let Some(endpoint) = opts.endpoint.clone() else {
            return Err(Error::Storage("archive: no endpoint configured".into()));
        };
        let endpoint =
            endpoint.strip_prefix("http://").unwrap_or(&endpoint).trim_end_matches('/').to_string();
        if endpoint.starts_with("https://") {
            return Err(Error::Storage(
                "archive: the endpoint must be plain http; this crate carries no TLS -- \
                 put a TLS-terminating proxy in front of the bucket"
                    .into(),
            ));
        }
        if opts.bucket.is_empty() {
            return Err(Error::Storage("archive: no bucket configured".into()));
        }
        let var = |name: &str| {
            std::env::var(name).ok().filter(|v| !v.is_empty()).ok_or_else(|| {
                Error::Storage(format!(
                    "archive: {name} is not set; credentials come from the environment"
                ))
            })
        };
        Ok(S3Store {
            endpoint,
            bucket: opts.bucket.clone(),
            region: if opts.region.is_empty() {
                "us-east-1".to_string()
            } else {
                opts.region.clone()
            },
            access_key: var("AWS_ACCESS_KEY_ID")?,
            secret_key: var("AWS_SECRET_ACCESS_KEY")?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok().filter(|v| !v.is_empty()),
        })
    }

    /// For tests and for callers that already hold credentials.
    pub fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Store {
        S3Store {
            endpoint: endpoint
                .strip_prefix("http://")
                .unwrap_or(endpoint)
                .trim_end_matches('/')
                .to_string(),
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            session_token: None,
        }
    }

    fn request(
        &self,
        method: &str,
        key: &str,
        range: Option<(u64, u64)>,
        body: &[u8],
    ) -> Result<Response> {
        let path = format!("/{}/{}", uri_encode(&self.bucket), uri_encode(key));
        let payload_hash = hex(&sha256(body));
        let date = amz_date(now_secs());
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), self.endpoint.clone()),
            ("x-amz-content-sha256".into(), payload_hash.clone()),
            ("x-amz-date".into(), date.clone()),
        ];
        if let Some((off, len)) = range {
            headers.push(("range".into(), format!("bytes={}-{}", off, off + len - 1)));
        }
        if let Some(t) = &self.session_token {
            headers.push(("x-amz-security-token".into(), t.clone()));
        }
        let auth = sigv4::authorization(
            &self.access_key,
            &self.secret_key,
            &self.region,
            SERVICE,
            method,
            &path,
            "",
            &headers,
            &payload_hash,
            &date,
        );
        let mut req = format!("{method} {path} HTTP/1.1\r\n");
        for (k, v) in &headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str(&format!(
            "authorization: {auth}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        ));
        let mut stream = connect(&self.endpoint)?;
        stream.write_all(req.as_bytes())?;
        stream.write_all(body)?;
        stream.flush()?;
        read_response(&mut stream, method == "HEAD")
    }

    fn fail(&self, what: &str, key: &str, r: &Response) -> Error {
        let body = String::from_utf8_lossy(&r.body);
        let body: String = body.chars().take(200).collect();
        Error::Storage(format!("archive: {what} {key}: HTTP {} {}", r.status, body.trim()))
    }
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let r = self.request("PUT", key, None, bytes)?;
        if r.status == 200 {
            Ok(())
        } else {
            Err(self.fail("PUT", key, &r))
        }
    }

    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let r = self.request("GET", key, Some((off, len)), &[])?;
        if r.status != 206 && r.status != 200 {
            return Err(self.fail("GET", key, &r));
        }
        if r.body.len() as u64 != len {
            return Err(Error::Storage(format!(
                "archive: GET {key} bytes {off}+{len}: the server returned {} bytes",
                r.body.len()
            )));
        }
        Ok(r.body)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let r = self.request("GET", key, None, &[])?;
        if r.status == 200 {
            Ok(r.body)
        } else {
            Err(self.fail("GET", key, &r))
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let r = self.request("HEAD", key, None, &[])?;
        match r.status {
            200 => {
                r.header("content-length").and_then(|v| v.parse::<u64>().ok()).map(Some).ok_or_else(
                    || Error::Storage(format!("archive: HEAD {key}: no content-length")),
                )
            }
            404 => Ok(None),
            _ => Err(self.fail("HEAD", key, &r)),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let r = self.request("DELETE", key, None, &[])?;
        if r.status == 204 || r.status == 200 || r.status == 404 {
            Ok(())
        } else {
            Err(self.fail("DELETE", key, &r))
        }
    }
}

// ------------------------------------------------------------------ HTTP

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

fn connect(endpoint: &str) -> Result<TcpStream> {
    let addr = endpoint
        .to_socket_addrs()
        .map_err(|e| Error::Storage(format!("archive: cannot resolve {endpoint}: {e}")))?
        .next()
        .ok_or_else(|| Error::Storage(format!("archive: {endpoint} resolves to nothing")))?;
    let s = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
        .map_err(|e| Error::Storage(format!("archive: cannot connect to {endpoint}: {e}")))?;
    s.set_read_timeout(Some(IO_TIMEOUT))?;
    s.set_write_timeout(Some(IO_TIMEOUT))?;
    Ok(s)
}

/// Read one HTTP/1.1 response in full: a status line, headers to the blank
/// line, then a body sized by `content-length`, by chunked framing, or by the
/// close of the connection (which the request asked for).
fn read_response(stream: &mut TcpStream, head_only: bool) -> Result<Response> {
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let head_end = find(&raw, b"\r\n\r\n")
        .ok_or_else(|| Error::Storage("archive: response without a header block".into()))?;
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| Error::Storage(format!("archive: bad status line `{status_line}`")))?;
    let mut headers = Vec::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let rest = &raw[head_end + 4..];
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked"));
    // A HEAD carries the object's content-length and no body.
    let body = if head_only {
        Vec::new()
    } else if chunked {
        dechunk(rest)?
    } else if let Some(n) = headers.iter().find(|(k, _)| k == "content-length") {
        let n: usize =
            n.1.parse().map_err(|_| Error::Storage("archive: bad content-length".into()))?;
        rest.get(..n)
            .ok_or_else(|| Error::Storage("archive: the response body was cut short".into()))?
            .to_vec()
    } else {
        rest.to_vec()
    };
    Ok(Response { status, headers, body })
}

fn dechunk(mut b: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let eol = find(b, b"\r\n").ok_or_else(|| Error::Storage("archive: bad chunk".into()))?;
        let size_str = String::from_utf8_lossy(&b[..eol]);
        let size = usize::from_str_radix(size_str.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| Error::Storage("archive: bad chunk size".into()))?;
        b = &b[eol + 2..];
        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(
            b.get(..size).ok_or_else(|| Error::Storage("archive: chunk cut short".into()))?,
        );
        b = b.get(size + 2..).unwrap_or(&[]);
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

// ------------------------------------------------------------------ SigV4

/// RFC 3986 unreserved characters pass; everything else is `%XX`; `/` is
/// kept, because this encodes a path.
pub(crate) fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub(crate) mod sigv4 {
    use super::{hex, hmac_sha256, sha256};

    /// The `Authorization` header value for a request, per AWS Signature
    /// Version 4. `headers` are the ones to sign, `(lowercase name, value)`,
    /// in any order; `canonical_uri` is the encoded path; `amz_date` is the
    /// `x-amz-date` value, whose first eight characters are the date scope.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn authorization(
        access_key: &str,
        secret_key: &str,
        region: &str,
        service: &str,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        amz_date: &str,
    ) -> String {
        let mut hs: Vec<(String, String)> =
            headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string())).collect();
        hs.sort();
        let signed: Vec<&str> = hs.iter().map(|(k, _)| k.as_str()).collect();
        let signed_headers = signed.join(";");
        let mut canonical = format!("{method}\n{canonical_uri}\n{canonical_query}\n");
        for (k, v) in &hs {
            canonical.push_str(&format!("{k}:{v}\n"));
        }
        canonical.push_str(&format!("\n{signed_headers}\n{payload_hash}"));
        let date = &amz_date[..8];
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex(&sha256(canonical.as_bytes()))
        );
        let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex(&hmac_sha256(&k_signing, to_sign.as_bytes()));
        format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        )
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `YYYYMMDDTHHMMSSZ` for a Unix time, in UTC. The civil-from-days
/// arithmetic is Howard Hinnant's, valid for every date this will ever see.
pub(crate) fn amz_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", rem / 3600, (rem % 3600) / 60, rem % 60)
}

// ------------------------------------------------------------------ hashing

pub(crate) fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub(crate) fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Vec::with_capacity(64 + msg.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(msg);
    let ih = sha256(&inner);
    let mut outer = Vec::with_capacity(96);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&ih);
    sha256(&outer)
}

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// SHA-256, FIPS 180-4, straight from the specification.
pub(crate) fn sha256(msg: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut data = msg.to_vec();
    let bit_len = (msg.len() as u64).wrapping_mul(8);
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());
    for block in data.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[4 * i],
                block[4 * i + 1],
                block[4 * i + 2],
                block[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (i, v) in [a, b, c, d, e, f, g, hh].iter().enumerate() {
            h[i] = h[i].wrapping_add(*v);
        }
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FIPS 180-4's two vectors and the empty message, whose digest is also
    /// the `x-amz-content-sha256` of every bodiless request.
    #[test]
    fn sha256_matches_the_published_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // A message over one block, so the padding path with a second block
        // runs.
        let long = vec![b'a'; 1000];
        assert_eq!(
            hex(&sha256(&long)),
            "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3"
        );
    }

    /// RFC 4231 test cases 2 and 3: a short key and a key longer than a
    /// block, which takes the hashed-key path.
    #[test]
    fn hmac_sha256_matches_rfc_4231() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let key = [0xaau8; 131];
        assert_eq!(
            hex(&hmac_sha256(&key, b"Test Using Larger Than Block-Size Key - Hash Key First")),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    /// The GET Object example from the S3 developer guide's signature
    /// calculations, signature and all. A signer that passes this one is
    /// accepted by S3; one that is off by a byte is refused by every request.
    #[test]
    fn sigv4_reproduces_the_aws_worked_example() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = vec![
            ("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), empty.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let auth = sigv4::authorization(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "s3",
            "GET",
            "/test.txt",
            "",
            &headers,
            empty,
            "20130524T000000Z",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn dates_and_paths_are_encoded_the_way_the_signature_expects() {
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
        assert_eq!(amz_date(951_782_400), "20000229T000000Z");
        assert_eq!(uri_encode("a/b c+d~e"), "a/b%20c%2Bd~e");
        assert_eq!(
            uri_encode("celastro/notes/shard-0000/0000000000000001.seg"),
            "celastro/notes/shard-0000/0000000000000001.seg"
        );
    }

    #[test]
    fn a_chunked_body_is_reassembled() {
        assert_eq!(dechunk(b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n").unwrap(), b"Wikipedia");
        assert!(dechunk(b"4\r\nWi").is_err());
    }
}
