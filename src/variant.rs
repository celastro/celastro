//! The self-describing binary document encoding stored in `docs.variant`.
//!
//! Every segment stores the complete document here, primary-key sorted, and
//! separately shreds stable hot paths into typed columns (§2.3). The
//! duplication costs 20–40% and it is deliberate: the dominant read in a
//! document store is the full-document fetch, and remainder shredding taxes
//! exactly that path. If storage cost ever dominates, this is the encoding that
//! changes, not the readers above it.

use crate::codec::*;
use crate::error::{Error, Result};
use crate::value::Value;

const T_NULL: u8 = 0x00;
const T_FALSE: u8 = 0x01;
const T_TRUE: u8 = 0x02;
const T_INT: u8 = 0x03;
const T_FLOAT: u8 = 0x04;
const T_STR: u8 = 0x05;
const T_ARRAY: u8 = 0x06;
const T_OBJECT: u8 = 0x07;
const T_TIMESTAMP: u8 = 0x08;

pub fn encode(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(T_NULL),
        Value::Bool(false) => out.push(T_FALSE),
        Value::Bool(true) => out.push(T_TRUE),
        Value::Int(i) => {
            out.push(T_INT);
            put_ivarint(out, *i);
        }
        Value::Float(f) => {
            out.push(T_FLOAT);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Timestamp(t) => {
            out.push(T_TIMESTAMP);
            put_ivarint(out, *t);
        }
        Value::Str(s) => {
            out.push(T_STR);
            put_str(out, s);
        }
        Value::Array(a) => {
            out.push(T_ARRAY);
            put_uvarint(out, a.len() as u64);
            for x in a {
                encode(x, out);
            }
        }
        Value::Object(o) => {
            out.push(T_OBJECT);
            put_uvarint(out, o.len() as u64);
            for (k, x) in o {
                put_str(out, k);
                encode(x, out);
            }
        }
    }
}

pub fn encode_to_vec(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(v, &mut out);
    out
}

// `decode`, `skip` and `decode_path_at` recurse once per level on bytes read
// off disk, so without a bound a corrupt or hostile blob overflows the stack
// — and a stack overflow is an abort, not a panic, so no `Result` and no
// `catch_unwind` can contain it. The limit is the one every write path
// applies -- `json::parse` on the way in, `Value::set_path` in memory -- so
// it refuses only blobs no supported write path could have produced.
use crate::value::MAX_DEPTH;

pub fn decode(b: &[u8], i: &mut usize) -> Result<Value> {
    decode_at(b, i, 0)
}

fn decode_at(b: &[u8], i: &mut usize, depth: usize) -> Result<Value> {
    if depth > MAX_DEPTH {
        return Err(Error::Storage(format!("variant: nested deeper than {MAX_DEPTH}")));
    }
    let tag = *b.get(*i).ok_or_else(|| Error::Storage("variant: truncated".into()))?;
    *i += 1;
    let bad = || Error::Storage("variant: truncated".into());
    Ok(match tag {
        T_NULL => Value::Null,
        T_FALSE => Value::Bool(false),
        T_TRUE => Value::Bool(true),
        T_INT => Value::Int(get_ivarint(b, i).ok_or_else(bad)?),
        T_FLOAT => {
            let s = b.get(*i..*i + 8).ok_or_else(bad)?;
            *i += 8;
            Value::Float(f64::from_le_bytes(s.try_into().unwrap()))
        }
        T_TIMESTAMP => Value::Timestamp(get_ivarint(b, i).ok_or_else(bad)?),
        T_STR => Value::Str(get_str(b, i).ok_or_else(bad)?),
        T_ARRAY => {
            let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
            let mut a = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                a.push(decode_at(b, i, depth + 1)?);
            }
            Value::Array(a)
        }
        T_OBJECT => {
            let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
            let mut o = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                let k = get_str(b, i).ok_or_else(bad)?;
                o.push((k, decode_at(b, i, depth + 1)?));
            }
            // Already sorted on write; preserve rather than re-sort.
            Value::Object(o)
        }
        other => return Err(Error::Storage(format!("variant: unknown tag {other:#x}"))),
    })
}

pub fn decode_one(b: &[u8]) -> Result<Value> {
    let mut i = 0;
    decode(b, &mut i)
}

/// Decode only the value at `path`, skipping the rest. This is what the
/// variant-decode access path uses when a predicate touches a path that this
/// segment did not shred (§2.1: segments written before a path was promoted
/// still work, just slower).
pub fn decode_path(b: &[u8], path: &str) -> Result<Value> {
    let mut i = 0;
    decode_path_at(b, &mut i, path, 0)
}

fn decode_path_at(b: &[u8], i: &mut usize, path: &str, depth: usize) -> Result<Value> {
    if depth > MAX_DEPTH {
        return Err(Error::Storage(format!("variant: nested deeper than {MAX_DEPTH}")));
    }
    let (head, rest) = match path.split_once('.') {
        Some((h, r)) => (h, Some(r)),
        None => (path, None),
    };
    let tag = *b.get(*i).ok_or_else(|| Error::Storage("variant: truncated".into()))?;
    if tag != T_OBJECT {
        return Ok(Value::Null);
    }
    *i += 1;
    let n = get_uvarint(b, i).ok_or_else(|| Error::Storage("variant: truncated".into()))? as usize;
    for _ in 0..n {
        let k = get_str(b, i).ok_or_else(|| Error::Storage("variant: truncated".into()))?;
        if k == head {
            return match rest {
                None => decode_at(b, i, depth + 1),
                Some(r) => decode_path_at(b, i, r, depth + 1),
            };
        }
        skip_at(b, i, depth + 1)?;
    }
    Ok(Value::Null)
}

fn skip_at(b: &[u8], i: &mut usize, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(Error::Storage(format!("variant: nested deeper than {MAX_DEPTH}")));
    }
    let bad = || Error::Storage("variant: truncated".into());
    let tag = *b.get(*i).ok_or_else(bad)?;
    *i += 1;
    match tag {
        T_NULL | T_FALSE | T_TRUE => {}
        T_INT | T_TIMESTAMP => {
            get_ivarint(b, i).ok_or_else(bad)?;
        }
        T_FLOAT => *i += 8,
        T_STR => {
            get_str(b, i).ok_or_else(bad)?;
        }
        T_ARRAY => {
            let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
            for _ in 0..n {
                skip_at(b, i, depth + 1)?;
            }
        }
        T_OBJECT => {
            let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
            for _ in 0..n {
                get_str(b, i).ok_or_else(bad)?;
                skip_at(b, i, depth + 1)?;
            }
        }
        other => return Err(Error::Storage(format!("variant: unknown tag {other:#x}"))),
    }
    if *i > b.len() {
        return Err(bad());
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_variant_decoding_never_panics() {
        let v = crate::json::parse(
            r#"{"id":"x","n":-7,"f":1.5e3,"b":true,"z":null,"t":["a","b",{"c":[1,[2,[3]]]}],"s":"caf\u00e9 \ud83d\ude00"}"#,
        )
        .unwrap();
        crate::fuzz::sweep(91, &[encode_to_vec(&v)], 8000, |b| {
            let _ = decode(b, &mut 0);
        });
    }
    use super::*;

    fn sample() -> Value {
        crate::json::parse(
            r#"{"id":"a1","n":42,"f":1.5,"tags":["x","y"],"author":{"name":"Ada","age":36},"ok":true,"nil":null}"#,
        )
        .unwrap()
    }

    /// `n` nested single-element arrays around a null.
    fn nest(n: usize) -> Vec<u8> {
        let mut b = Vec::new();
        for _ in 0..n {
            b.push(T_ARRAY);
            put_uvarint(&mut b, 1);
        }
        b.push(T_NULL);
        b
    }

    #[test]
    fn a_deeply_nested_blob_is_refused_rather_than_aborting_the_process() {
        // These bytes come off disk, so they are as trustworthy as the disk.
        // Recursing once per level overflows the stack, and a stack overflow
        // is an abort: no `Result` and no `catch_unwind` can contain it.
        let deep = nest(200_000);
        let e = decode_one(&deep).unwrap_err().to_string();
        assert!(e.contains("nested"), "{e}");
        // `skip` is the same recursion by another name: a path projection
        // walks past a sibling it does not want.
        let mut o = vec![T_OBJECT];
        put_uvarint(&mut o, 2);
        put_str(&mut o, "a");
        o.extend_from_slice(&deep);
        put_str(&mut o, "b");
        o.push(T_INT);
        put_ivarint(&mut o, 7);
        let e = decode_path(&o, "b").unwrap_err().to_string();
        assert!(e.contains("nested"), "{e}");
        // Ordinary nesting still round-trips, and the bound itself is
        // where it says it is.
        assert_eq!(encode_to_vec(&decode_one(&nest(16)).unwrap()), nest(16));
        assert!(decode_one(&nest(MAX_DEPTH)).is_ok());
        assert!(decode_one(&nest(MAX_DEPTH + 1)).is_err());
    }

    #[test]
    fn round_trip() {
        let v = sample();
        let b = encode_to_vec(&v);
        let back = decode_one(&b).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn path_projection_skips() {
        let b = encode_to_vec(&sample());
        assert_eq!(decode_path(&b, "author.name").unwrap(), Value::Str("Ada".into()));
        assert_eq!(decode_path(&b, "n").unwrap(), Value::Int(42));
        assert_eq!(decode_path(&b, "missing").unwrap(), Value::Null);
        assert_eq!(decode_path(&b, "author.missing").unwrap(), Value::Null);
    }
}
