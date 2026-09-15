//! PEM: base64 between `-----BEGIN x-----` and `-----END x-----`, RFC 7468,
//! and the base64 it needs.

use crate::error::{Error, Result};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { ALPHABET[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[n as usize & 63] as char } else { '=' });
    }
    out
}

pub fn base64_decode(text: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut pad = 0;
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                pad += 1;
                continue;
            }
            b'\n' | b'\r' | b' ' | b'\t' => continue,
            _ => return Err(Error::Plan(format!("base64: unexpected byte {c:#04x}"))),
        };
        if pad > 0 {
            return Err(Error::Plan("base64: data after padding".into()));
        }
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}

/// Every block labelled `label` in `text`, decoded, in order.
pub fn decode_all(text: &str, label: &str) -> Result<Vec<Vec<u8>>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(i) = rest.find(&begin) {
        let after = &rest[i + begin.len()..];
        let Some(j) = after.find(&end) else {
            return Err(Error::Plan(format!("PEM: a {label} block with no end")));
        };
        out.push(base64_decode(&after[..j])?);
        rest = &after[j + end.len()..];
    }
    Ok(out)
}

/// One block, 64 columns, as every tool writes them.
pub fn encode(label: &str, der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_and_pem_round_trip_and_refuse_garbage() {
        for (plain, b64) in [
            (&b""[..], ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64_encode(plain), b64);
            assert_eq!(base64_decode(b64).unwrap(), plain);
        }
        assert!(base64_decode("Zm9v!").is_err());
        assert!(base64_decode("Zg==Zg").is_err());
        let der: Vec<u8> = (0..200).map(|i| i as u8).collect();
        let pem = encode("CERTIFICATE", &der);
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----\n"));
        assert!(pem.lines().all(|l| l.len() <= 64));
        let two = format!("{pem}{pem}");
        let blocks = decode_all(&two, "CERTIFICATE").unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], der);
        assert!(decode_all(&pem, "PRIVATE KEY").unwrap().is_empty());
        assert!(decode_all("-----BEGIN CERTIFICATE-----\nZm9v", "CERTIFICATE").is_err());
    }
}
