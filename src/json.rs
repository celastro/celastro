//! A small JSON reader/writer over `Value`.
//!
//! In-tree because the crate has no dependencies. It is a strict RFC 8259
//! parser with one deliberate deviation: integers that fit in i64 stay
//! integers, so `Value::Int` and `Value::Float` at the same path widen to one
//! numeric type rather than looking like polymorphism (§2.1).

use crate::error::{Error, Result};
use crate::value::Value;

pub fn parse(input: &str) -> Result<Value> {
    let mut p = Parser { b: input.as_bytes(), i: 0 };
    p.ws();
    let v = p.value()?;
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

    fn value(&mut self) -> Result<Value> {
        match self.peek() {
            None => Err(Error::Schema("unexpected end of input".into())),
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
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

    fn object(&mut self) -> Result<Value> {
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
            let v = self.value()?;
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

    fn array(&mut self) -> Result<Value> {
        self.eat(b'[')?;
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Value::Array(items));
        }
        loop {
            self.ws();
            items.push(self.value()?);
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
                            // Surrogate pair handling.
                            if (0xD800..0xDC00).contains(&cp) {
                                if self.peek() == Some(b'\\') {
                                    self.i += 1;
                                    self.eat(b'u')?;
                                    let lo = self.hex4()?;
                                    let combined = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                    out.push(
                                        char::from_u32(combined)
                                            .ok_or_else(|| Error::Schema("bad surrogate".into()))?,
                                    );
                                } else {
                                    out.push('\u{FFFD}');
                                }
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
        if self.i + 4 > self.b.len() {
            return Err(Error::Schema("short \\u escape".into()));
        }
        let s = std::str::from_utf8(&self.b[self.i..self.i + 4])
            .map_err(|_| Error::Schema("bad \\u escape".into()))?;
        let v = u32::from_str_radix(s, 16).map_err(|_| Error::Schema("bad \\u escape".into()))?;
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
        if !is_float {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(Value::Int(i));
            }
        }
        s.parse::<f64>().map(Value::Float).map_err(|_| Error::Schema(format!("bad number `{s}`")))
    }
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
    write_value(v, &mut s);
    s
}

pub fn to_string_pretty(v: &Value) -> String {
    let mut s = String::new();
    write_pretty(v, 0, &mut s);
    s
}

fn write_value(v: &Value, out: &mut String) {
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
                write_value(x, out);
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
                write_value(x, out);
            }
            out.push('}');
        }
    }
}

fn write_pretty(v: &Value, depth: usize, out: &mut String) {
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
        _ => write_value(v, out),
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
