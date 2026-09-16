//! A small JSON reader/writer over `Value`.
//!
//! In-tree because the crate has no dependencies. It is a strict RFC 8259
//! parser with one deliberate deviation: integers that fit in i64 stay
//! integers, so `Value::Int` and `Value::Float` at the same path widen to one
//! numeric type rather than looking like polymorphism (§2.1).

use crate::error::{Error, Result};
use crate::value::Value;

// The deepest nesting `parse` accepts, and the deepest the writers will
// descend. Shared with `variant::decode` and `Value::set_path`; the bound and
// why there is one are documented on the constant.
use crate::value::MAX_DEPTH;

pub fn parse(input: &str) -> Result<Value> {
    let mut p = Parser { b: input.as_bytes(), i: 0 };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != p.b.len() {
        return Err(Error::Schema(format!("trailing input at byte {}", p.i)));
    }
    Ok(v)
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn eat(&mut self, c: u8) -> Result<()> {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(Error::Schema(format!("expected '{}' at byte {}", c as char, self.i)))
        }
    }

    /// `depth` is the number of containers enclosing this value; it is what
    /// bounds the mutual recursion between `value`, `object` and `array`.
    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth >= MAX_DEPTH {
            let at = self.i;
            return Err(Error::Schema(format!("nesting deeper than {MAX_DEPTH} at byte {at}")));
        }
        match self.peek() {
            None => Err(Error::Schema("unexpected end of input".into())),
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => Ok(Value::Str(self.string()?)),
            Some(b't') => {
                self.lit("true")?;
                Ok(Value::Bool(true))
            }
            Some(b'f') => {
                self.lit("false")?;
                Ok(Value::Bool(false))
            }
            Some(b'n') => {
                self.lit("null")?;
                Ok(Value::Null)
            }
            Some(_) => self.number(),
        }
    }

    fn lit(&mut self, s: &str) -> Result<()> {
        if self.b[self.i..].starts_with(s.as_bytes()) {
            self.i += s.len();
            Ok(())
        } else {
            Err(Error::Schema(format!("bad literal at byte {}", self.i)))
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value> {
        self.eat(b'{')?;
        let mut fields = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(Value::obj(fields));
        }
        loop {
            self.ws();
            let k = self.string()?;
            self.ws();
            self.eat(b':')?;
            self.ws();
            let v = self.value(depth + 1)?;
            fields.push((k, v));
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Error::Schema(format!("bad object at byte {}", self.i))),
            }
        }
        Ok(Value::obj(fields))
    }

    fn array(&mut self, depth: usize) -> Result<Value> {
        self.eat(b'[')?;
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Value::Array(items));
        }
        loop {
            self.ws();
            items.push(self.value(depth + 1)?);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(Error::Schema(format!("bad array at byte {}", self.i))),
            }
        }
        Ok(Value::Array(items))
    }

    fn string(&mut self) -> Result<String> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or_else(|| Error::Schema("unterminated string".into()))?;
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let e = self.peek().ok_or_else(|| Error::Schema("bad escape".into()))?;
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let cp = self.hex4()?;
                            // Surrogate pair handling. Both halves are
                            // validated before the arithmetic: a high
                            // surrogate followed by `A` would otherwise
                            // evaluate `0x41 - 0xDC00` on a u32, which panics
                            // in debug and in release wraps around into a
                            // fabricated character.
                            if (0xD800..0xDC00).contains(&cp) {
                                if self.peek() != Some(b'\\') {
                                    return Err(Error::Schema("unpaired high surrogate".into()));
                                }
                                self.i += 1;
                                self.eat(b'u')?;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(Error::Schema("invalid low surrogate".into()));
                                }
                                let combined = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                out.push(
                                    char::from_u32(combined)
                                        .ok_or_else(|| Error::Schema("bad surrogate".into()))?,
                                );
                            } else {
                                out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                            }
                        }
                        _ => return Err(Error::Schema("unknown escape".into())),
                    }
                }
                _ if c < 0x20 => return Err(Error::Schema("control char in string".into())),
                _ => {
                    // Copy the whole UTF-8 sequence.
                    let start = self.i - 1;
                    let len = utf8_len(c);
                    self.i = start + len;
                    if self.i > self.b.len() {
                        return Err(Error::Schema("truncated utf-8".into()));
                    }
                    out.push_str(
                        std::str::from_utf8(&self.b[start..self.i])
                            .map_err(|_| Error::Schema("invalid utf-8".into()))?,
                    );
                }
            }
        }
        Ok(out)
    }

    fn hex4(&mut self) -> Result<u32> {
        let raw = self
            .b
            .get(self.i..self.i + 4)
            .ok_or_else(|| Error::Schema("short \\u escape".into()))?;
        // `u32::from_str_radix` also accepts a leading sign, so `\u+abc`
        // decoded as U+0ABC. RFC 8259 asks for exactly four hex digits.
        if !raw.iter().all(u8::is_ascii_hexdigit) {
            return Err(Error::Schema("bad \\u escape".into()));
        }
        let v = raw.iter().fold(0u32, |a, c| a * 16 + char::from(*c).to_digit(16).unwrap_or(0));
        self.i += 4;
        Ok(v)
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        let mut is_float = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' => self.i += 1,
                b'.' | b'e' | b'E' | b'+' | b'-' => {
                    is_float = true;
                    self.i += 1;
                }
                _ => break,
            }
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).unwrap();
        if s.is_empty() {
            return Err(Error::Schema(format!("bad number at byte {start}")));
        }
        // The scan above is deliberately loose so that the whole run is
        // reported as one bad number rather than as trailing input. What it
        // scanned is then held to the grammar, because the parse below is not:
        // `f64::from_str` takes `+1`, `1.` and `.5`, and the i64 arm takes
        // `01`, none of which are JSON. The module header promises RFC 8259.
        if !is_json_number(s.as_bytes()) {
            return Err(Error::Schema(format!("bad number `{s}` at byte {start}")));
        }
        if !is_float {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(Value::Int(i));
            }
        }
        let f = s.parse::<f64>().map_err(|_| Error::Schema(format!("bad number `{s}`")))?;
        // `1e999` parses to infinity, and every writer renders a non-finite
        // float as `null`, so accepting it here would mean a number silently
        // reads back as null. Refuse it at the door instead.
        if !f.is_finite() {
            return Err(Error::Schema(format!("number out of range `{s}`")));
        }
        Ok(Value::Float(f))
    }
}

/// RFC 8259 §6: `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
fn is_json_number(s: &[u8]) -> bool {
    fn digits(s: &[u8], i: &mut usize) -> bool {
        let start = *i;
        while s.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        *i > start
    }
    let mut i = 0usize;
    if s.first() == Some(&b'-') {
        i += 1;
    }
    match s.get(i) {
        // A leading zero is not the start of a longer integer: `01` is not 1.
        Some(b'0') => i += 1,
        Some(c) if c.is_ascii_digit() => {
            digits(s, &mut i);
        }
        _ => return false,
    }
    if s.get(i) == Some(&b'.') {
        i += 1;
        if !digits(s, &mut i) {
            return false;
        }
    }
    if matches!(s.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(s.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if !digits(s, &mut i) {
            return false;
        }
    }
    i == s.len()
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

pub fn to_string(v: &Value) -> String {
    let mut s = String::new();
    write_value(v, 0, &mut s);
    s
}

pub fn to_string_pretty(v: &Value) -> String {
    let mut s = String::new();
    write_pretty(v, 0, &mut s);
    s
}

fn write_value(v: &Value, depth: usize, out: &mut String) {
    // A value built in memory (by `set_path`, say) is not bounded by what the
    // parser would have accepted, and the writer recurses in step with it.
    // Emitting `null` past the limit keeps the output valid JSON without
    // overflowing the stack; nothing `parse` produces can reach here, because
    // a container at the deepest accepted level has to be empty.
    if depth >= MAX_DEPTH {
        out.push_str("null");
        return;
    }
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => {
            if f.is_finite() {
                out.push_str(&format_f64(*f));
            } else {
                out.push_str("null");
            }
        }
        Value::Timestamp(t) => {
            out.push('"');
            out.push_str(&crate::time::format_micros(*t));
            out.push('"');
        }
        Value::Str(s) => write_json_string(s, out),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(x, depth + 1, out);
            }
            out.push(']');
        }
        Value::Object(o) => {
            out.push('{');
            for (i, (k, x)) in o.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_value(x, depth + 1, out);
            }
            out.push('}');
        }
    }
}

fn write_pretty(v: &Value, depth: usize, out: &mut String) {
    // The same bound as `write_value`, for the same reason: this function
    // recurses on its own for containers and would never reach that check.
    if depth >= MAX_DEPTH {
        out.push_str("null");
        return;
    }
    let pad = "  ".repeat(depth);
    let pad1 = "  ".repeat(depth + 1);
    match v {
        Value::Array(a) if !a.is_empty() => {
            out.push_str("[\n");
            for (i, x) in a.iter().enumerate() {
                out.push_str(&pad1);
                write_pretty(x, depth + 1, out);
                if i + 1 < a.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&pad);
            out.push(']');
        }
        Value::Object(o) if !o.is_empty() => {
            out.push_str("{\n");
            for (i, (k, x)) in o.iter().enumerate() {
                out.push_str(&pad1);
                write_json_string(k, out);
                out.push_str(": ");
                write_pretty(x, depth + 1, out);
                if i + 1 < o.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&pad);
            out.push('}');
        }
        _ => write_value(v, depth, out),
    }
}

fn format_f64(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_json_parsing_never_panics() {
        let samples = [
            r#"{"id":"x","n":-7,"f":1.5e3,"b":true,"z":null,"t":["a","b",{"c":[1,[2,[3]]]}]}"#,
            r#""\u00e9\n\t\"\\""#,
            "[[[[[[[[[[1]]]]]]]]]]",
            "-0.0e-00",
        ];
        crate::fuzz::sweep_text(101, &samples, 8000, |t| {
            let _ = parse(t);
        });
    }
    use super::*;

    #[test]
    fn malformed_numbers_are_refused_rather_than_read_loosely() {
        // `f64::from_str` is far looser than RFC 8259, and the module header
        // claims strictness.
        for bad in ["+1", "01", "-01", "1.", ".5", "1.e5", "1e", "-", "1e+"] {
            assert!(parse(&format!("{{\"a\":{bad}}}")).is_err(), "accepted `{bad}`");
        }
        for good in ["0", "-0", "1", "12", "1.5", "-1.5e-3", "1E+2", "0.0", "1e5"] {
            assert!(parse(&format!("{{\"a\":{good}}}")).is_ok(), "refused `{good}`");
        }
        assert_eq!(parse("[1,-0.25,3e2]").unwrap().to_string(), "[1,-0.25,300.0]");
    }

    #[test]
    fn a_unicode_escape_requires_four_hex_digits() {
        // `u32::from_str_radix` accepts a leading `+`, so `\u+abc` used to
        // decode as U+0ABC.
        assert!(parse(r#"["\u+abc"]"#).is_err());
        assert!(parse(r#"["\u 041"]"#).is_err());
        assert_eq!(parse(r#""\u0041""#).unwrap(), Value::Str("A".into()));
    }

    #[test]
    fn an_invalid_low_surrogate_is_rejected_rather_than_fabricating_a_char() {
        // `0x41 - 0xDC00` underflows: a debug build panics here, a release
        // build wraps and stores a character nobody wrote.
        assert!(parse(r#"{"a":"\ud800\u0041"}"#).is_err());
        // A second high surrogate is not a valid low half either.
        assert!(parse(r#""\ud800\ud800""#).is_err());
        // A well-formed pair still decodes.
        assert_eq!(parse(r#""😀""#).unwrap(), Value::Str("\u{1F600}".into()));
    }

    #[test]
    fn a_high_surrogate_with_no_following_escape_is_rejected() {
        assert!(parse(r#""\ud800""#).is_err());
        assert!(parse(r#""\ud800x""#).is_err());
    }

    #[test]
    fn deeply_nested_input_is_rejected_instead_of_overflowing_the_stack() {
        // Unbounded, this recursion aborts the process: a stack overflow is
        // not a panic, so no Result and no catch_unwind can contain it.
        let err = parse(&"[".repeat(200_000)).unwrap_err();
        assert!(err.to_string().contains("nesting"), "{err}");
        let err = parse(&"{\"a\":".repeat(200_000)).unwrap_err();
        assert!(err.to_string().contains("nesting"), "{err}");
    }

    #[test]
    fn the_depth_limit_still_admits_documents_exactly_at_the_limit() {
        let ok = format!("{}{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(parse(&ok).is_ok(), "nesting of exactly MAX_DEPTH must parse");
        let deep = format!("{}{}", "[".repeat(MAX_DEPTH + 1), "]".repeat(MAX_DEPTH + 1));
        assert!(parse(&deep).is_err());
    }

    #[test]
    fn a_value_nested_past_the_writer_limit_is_truncated_not_overflowed() {
        let mut v = Value::Int(1);
        for _ in 0..MAX_DEPTH + 1 {
            v = Value::Array(vec![v]);
        }
        let expect = format!("{}null{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert_eq!(to_string(&v), expect);
    }

    #[test]
    fn an_out_of_range_number_is_rejected_rather_than_read_back_as_null() {
        // Accepting these would store infinity, which every writer renders as
        // `null`: the value you inserted is not the value you read back.
        assert!(parse("1e999").is_err());
        assert!(parse("-1e999").is_err());
        assert!(parse(r#"{"a":1e999}"#).is_err());
        // The neighbouring finite magnitude is still fine.
        assert!(parse("1e308").is_ok());
    }
}
