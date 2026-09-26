//! DER, the little of it X.509 needs: a reader over tag-length-value with
//! definite lengths, and a writer that builds the same. Tags are matched by
//! their whole identifier octet, so a context-specific `[0]` is `0xa0` and
//! not "tag number 0".

use crate::error::{Error, Result};

pub const SEQUENCE: u8 = 0x30;
pub const SET: u8 = 0x31;
pub const INTEGER: u8 = 0x02;
pub const BIT_STRING: u8 = 0x03;
pub const OCTET_STRING: u8 = 0x04;
pub const NULL: u8 = 0x05;
pub const OID: u8 = 0x06;
pub const UTF8_STRING: u8 = 0x0c;
pub const PRINTABLE_STRING: u8 = 0x13;
pub const IA5_STRING: u8 = 0x16;
pub const UTC_TIME: u8 = 0x17;
pub const GENERALIZED_TIME: u8 = 0x18;
pub const BOOLEAN: u8 = 0x01;

fn bad(what: &str) -> Error {
    Error::Plan(format!("DER: {what}"))
}

/// One element: its tag, its contents, and what follows it.
pub fn read(input: &[u8]) -> Result<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first().ok_or_else(|| bad("truncated at a tag"))?;
    let (&first, rest) = rest.split_first().ok_or_else(|| bad("truncated at a length"))?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return Err(bad("a length that is indefinite or too long"));
        }
        let mut len = 0usize;
        for &b in &rest[..n] {
            len = (len << 8) | b as usize;
        }
        // DER's length is the shortest that holds it: a value under 0x80
        // belongs in the short form, and a leading zero byte is a longer
        // form than the value needs.
        if len < 0x80 || rest[0] == 0 {
            return Err(bad("a length not minimally encoded"));
        }
        (len, &rest[n..])
    };
    if rest.len() < len {
        return Err(bad("contents shorter than the length says"));
    }
    Ok((tag, &rest[..len], &rest[len..]))
}

/// The element, which must carry `tag`.
pub fn expect(input: &[u8], tag: u8) -> Result<(&[u8], &[u8])> {
    let (t, body, rest) = read(input)?;
    if t != tag {
        return Err(bad(&format!("expected tag {tag:#04x}, found {t:#04x}")));
    }
    Ok((body, rest))
}

/// The element with `tag` if it is next, else `None` and the input as is.
pub fn optional(input: &[u8], tag: u8) -> Result<(Option<&[u8]>, &[u8])> {
    if input.first() == Some(&tag) {
        let (body, rest) = expect(input, tag)?;
        Ok((Some(body), rest))
    } else {
        Ok((None, input))
    }
}

/// `tag` with `body`, length in the shortest form.
pub fn tlv(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 6);
    out.push(tag);
    let len = body.len();
    if len < 0x80 {
        out.push(len as u8);
    } else {
        let bytes = len.to_be_bytes();
        let skip = bytes.iter().take_while(|&&b| b == 0).count();
        out.push(0x80 | (bytes.len() - skip) as u8);
        out.extend_from_slice(&bytes[skip..]);
    }
    out.extend_from_slice(body);
    out
}

pub fn sequence(parts: &[&[u8]]) -> Vec<u8> {
    tlv(SEQUENCE, &parts.concat())
}

/// A non-negative INTEGER from big-endian magnitude bytes.
pub fn integer(magnitude: &[u8]) -> Vec<u8> {
    let trimmed = {
        let skip = magnitude.iter().take_while(|&&b| b == 0).count();
        &magnitude[skip.min(magnitude.len().saturating_sub(1))..]
    };
    let mut body = Vec::with_capacity(trimmed.len() + 1);
    if trimmed.is_empty() || trimmed[0] & 0x80 != 0 {
        body.push(0);
    }
    body.extend_from_slice(trimmed);
    tlv(INTEGER, &body)
}

/// A BIT STRING with no unused bits.
pub fn bit_string(bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(bytes.len() + 1);
    body.push(0);
    body.extend_from_slice(bytes);
    tlv(BIT_STRING, &body)
}

/// The Ed25519 algorithm identifier, `SEQUENCE { OID 1.3.101.112 }`.
pub const ED25519_OID: &[u8] = &[0x2b, 0x65, 0x70];

pub fn ed25519_algorithm() -> Vec<u8> {
    sequence(&[&tlv(OID, ED25519_OID)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn der_round_trips_and_refuses_what_it_should() {
        let inner = sequence(&[&integer(&[0x80]), &tlv(UTF8_STRING, b"x")]);
        let long = tlv(OCTET_STRING, &[7u8; 300]);
        let outer = sequence(&[&inner, &long]);
        let (body, rest) = expect(&outer, SEQUENCE).unwrap();
        assert!(rest.is_empty());
        let (i, rest) = expect(body, SEQUENCE).unwrap();
        let (n, rest2) = expect(i, INTEGER).unwrap();
        assert_eq!(n, &[0x00, 0x80], "a high bit gets a leading zero");
        let (s, _) = expect(rest2, UTF8_STRING).unwrap();
        assert_eq!(s, b"x");
        let (o, rest) = expect(rest, OCTET_STRING).unwrap();
        assert_eq!(o.len(), 300);
        assert!(rest.is_empty());
        assert_eq!(&long[..4], &[0x04, 0x82, 0x01, 0x2c]);
        assert!(read(&[0x30]).is_err(), "truncated length");
        assert!(read(&[0x30, 0x05, 0x01]).is_err(), "contents short");
        assert!(read(&[0x30, 0x80]).is_err(), "indefinite length");
        assert!(read(&[0x30, 0x81, 0x05, 0, 0, 0, 0, 0]).is_err(), "non-minimal length");
        let mut zero_led = vec![0x30, 0x82, 0x00, 0x81];
        zero_led.extend(std::iter::repeat(0u8).take(0x81));
        assert!(read(&zero_led).is_err(), "a long form with a leading zero, its bytes all there");
        assert!(read(&zero_led[1..]).is_err(), "and short of them");
        assert!(read(&[0x30, 0x81, 0x81]).is_err(), "a long form short of its bytes");
        assert!(expect(&outer, SET).is_err());
        assert_eq!(integer(&[0, 0, 5]), vec![0x02, 0x01, 0x05]);
        assert_eq!(integer(&[]), vec![0x02, 0x01, 0x00]);
    }
}
