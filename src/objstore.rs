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
//! need multipart is far past the segment cap. Listing (`ListObjectsV2`)
//! arrived with backups, which need to say what a destination holds; the
//! tier still never lists, so an object a failed publication left behind
//! is not reclaimed the way a local orphan is.
//!
//! [`DirStore`] is the same surface over a directory: the archived tier on
//! any mount -- NFS is the case it was written for -- and the destination
//! of a backup. It publishes each object the way the shards publish their
//! files (a temporary, an fsync, a rename, the directory's fsync).
//!
//! Everything under this module is in-tree for the same reason the rest of
//! the crate is: a small HTTP/1.1 client over a `TcpStream` and the signing
//! itself, over the SHA-256 and HMAC that live with the other primitives in
//! `crypto`. The signer is pinned against the published worked example
//! below, because a signer that is wrong by one byte is a client that is
//! refused by every request.
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

use crate::crypto::hex;
use crate::crypto::sha2::{hmac_sha256, sha256};
use crate::error::{Error, Result};

/// The four operations the archive tier needs, and the listing a backup
/// needs.
pub trait ObjectStore: Send + Sync + fmt::Debug {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    /// `len` bytes from `off`. Short reads are errors, not partial answers.
    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// The object's size, or `None` when there is no such object.
    fn size(&self, key: &str) -> Result<Option<u64>>;
    fn delete(&self, key: &str) -> Result<()>;
    /// Every key under `prefix`, sorted.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Where the archive lives. `endpoint` is `host:port` of an S3-compatible
/// server reached over plain HTTP; `dir` a directory on any mount; neither
/// keeps the shard-local directory.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ArchiveOpts {
    pub endpoint: Option<String>,
    /// A directory that stands behind the same trait as the bucket: the
    /// tier on an NFS mount, or on a disk that is not the data volume.
    pub dir: Option<std::path::PathBuf>,
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
        query: &str,
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
            query,
            &headers,
            &payload_hash,
            &date,
        );
        let target = if query.is_empty() { path.clone() } else { format!("{path}?{query}") };
        let mut req = format!("{method} {target} HTTP/1.1\r\n");
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
        let r = self.request("PUT", key, "", None, bytes)?;
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
        let r = self.request("GET", key, "", Some((off, len)), &[])?;
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
        let r = self.request("GET", key, "", None, &[])?;
        if r.status == 200 {
            Ok(r.body)
        } else {
            Err(self.fail("GET", key, &r))
        }
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let r = self.request("HEAD", key, "", None, &[])?;
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
        let r = self.request("DELETE", key, "", None, &[])?;
        if r.status == 204 || r.status == 200 || r.status == 404 {
            Ok(())
        } else {
            Err(self.fail("DELETE", key, &r))
        }
    }

    /// `ListObjectsV2`, page by page. The reply is XML; the keys are what
    /// is between `<Key>` and `</Key>`, unescaped, and a truncated page
    /// names the token the next one continues from.
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // The canonical query is sorted by parameter name, and the
            // signature covers it, so it is built in that order.
            let mut query = String::new();
            if let Some(t) = &token {
                query.push_str(&format!("continuation-token={}&", query_encode(t)));
            }
            query.push_str("list-type=2");
            if !prefix.is_empty() {
                query.push_str(&format!("&prefix={}", query_encode(prefix)));
            }
            let r = self.request("GET", "", &query, None, &[])?;
            if r.status != 200 {
                return Err(self.fail("LIST", prefix, &r));
            }
            let text = String::from_utf8_lossy(&r.body);
            keys.extend(xml_values(&text, "Key").into_iter().map(|k| xml_unescape(&k)));
            let truncated = xml_values(&text, "IsTruncated").first().map(|v| v == "true");
            token = match truncated {
                Some(true) => xml_values(&text, "NextContinuationToken")
                    .into_iter()
                    .next()
                    .map(|t| xml_unescape(&t)),
                _ => None,
            };
            if token.is_none() {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }
}

/// The text of every `<tag>...</tag>` in `xml`, in order.
fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(after[..j].to_string());
        rest = &after[j + close.len()..];
    }
    out
}

/// The five XML entities, which is all a key can carry.
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

/// A query value: everything but the unreserved characters is encoded,
/// `/` included, which is where it differs from [`uri_encode`].
fn query_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A directory as an object store. Keys are relative paths under the root
/// with `/` between components; an object is published as the shards
/// publish their files, so a reader on the same mount sees a whole object
/// or none, and a crash leaves at most a `.tmp` beside it, which the
/// listing skips. Written for an NFS mount, where those semantics hold
/// because the rename is one request to the server; on a local disk it is
/// the archived tier off the data volume.
#[derive(Debug, Clone)]
pub struct DirStore {
    root: std::path::PathBuf,
}

impl DirStore {
    pub fn new(root: &std::path::Path) -> Result<DirStore> {
        std::fs::create_dir_all(root)
            .map_err(|e| Error::Storage(format!("archive directory {}: {e}", root.display())))?;
        Ok(DirStore { root: root.to_path_buf() })
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// The path a key names, which stays under the root: a key with an
    /// empty, `.` or `..` component, or an absolute one, is refused.
    fn path(&self, key: &str) -> Result<std::path::PathBuf> {
        let bad = key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|c| c.is_empty() || c == "." || c == "..");
        if bad {
            return Err(Error::Storage(format!("archive: `{key}` is not a key")));
        }
        Ok(self.root.join(key))
    }

    fn walk(&self, dir: &std::path::Path, rel: &str, out: &mut Vec<String>) -> Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            let key = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
            if entry.file_type()?.is_dir() {
                self.walk(&entry.path(), &key, out)?;
            } else if !name.ends_with(".tmp") {
                out.push(key);
            }
        }
        Ok(())
    }
}

impl ObjectStore for DirStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let p = self.path(key)?;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::shard::atomic_write(&p, bytes)
    }

    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>> {
        use std::io::{Seek, SeekFrom};
        let p = self.path(key)?;
        let mut f = std::fs::File::open(&p)
            .map_err(|e| Error::Storage(format!("archive: GET {key}: {e}")))?;
        f.seek(SeekFrom::Start(off))?;
        let mut out = vec![0u8; len as usize];
        f.read_exact(&mut out)
            .map_err(|e| Error::Storage(format!("archive: GET {key} bytes {off}+{len}: {e}")))?;
        Ok(out)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let p = self.path(key)?;
        std::fs::read(&p).map_err(|e| Error::Storage(format!("archive: GET {key}: {e}")))
    }

    fn size(&self, key: &str) -> Result<Option<u64>> {
        let p = self.path(key)?;
        match std::fs::metadata(&p) {
            Ok(m) => Ok(Some(m.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Storage(format!("archive: HEAD {key}: {e}"))),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let p = self.path(key)?;
        match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("archive: DELETE {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        self.walk(&self.root, "", &mut out)?;
        out.retain(|k| k.starts_with(prefix));
        out.sort();
        Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(tag: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("celastro-dirstore-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    /// The directory store is the trait over files: an object is a file
    /// under the root, published whole, read whole or by range, sized,
    /// deleted, listed by prefix with `.tmp` leftovers skipped -- and a
    /// key cannot climb out of the root.
    #[test]
    fn a_directory_store_holds_objects_as_published_files() {
        let root = temp("ops");
        let s = DirStore::new(&root).unwrap();
        assert_eq!(s.size("a/b/one").unwrap(), None);
        s.put("a/b/one", b"hello world").unwrap();
        s.put("a/two", b"22").unwrap();
        std::fs::write(root.join("a").join("junk.tmp"), b"x").unwrap();
        assert_eq!(s.get("a/b/one").unwrap(), b"hello world");
        assert_eq!(s.get_range("a/b/one", 6, 5).unwrap(), b"world");
        assert!(s.get_range("a/b/one", 6, 50).is_err(), "a short read is an error");
        assert_eq!(s.size("a/b/one").unwrap(), Some(11));
        assert_eq!(s.list("a/").unwrap(), vec!["a/b/one".to_string(), "a/two".to_string()]);
        assert_eq!(s.list("a/b").unwrap(), vec!["a/b/one".to_string()]);
        s.delete("a/two").unwrap();
        s.delete("a/two").unwrap();
        assert_eq!(s.size("a/two").unwrap(), None);
        assert!(s.get("a/two").is_err());
        for bad in ["", "/etc/passwd", "a/../x", "./a", "a//b"] {
            assert!(s.put(bad, b"x").is_err(), "{bad} must be refused");
        }
        assert!(!root.join("a").join("b").join("one.tmp").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_replies_are_read_by_tag_and_unescaped() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>t&amp;1</NextContinuationToken>\
                   <Contents><Key>a/b&lt;c</Key></Contents><Contents><Key>a/d</Key></Contents></ListBucketResult>";
        assert_eq!(xml_values(xml, "Key"), vec!["a/b&lt;c", "a/d"]);
        assert_eq!(xml_unescape("a/b&lt;c"), "a/b<c");
        assert_eq!(xml_values(xml, "IsTruncated"), vec!["true"]);
        assert_eq!(xml_unescape(&xml_values(xml, "NextContinuationToken")[0]), "t&1");
        assert_eq!(query_encode("a/b c"), "a%2Fb%20c");
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
