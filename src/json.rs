//! A small JSON reader and writer: enough to validate a document, compare one
//! of its fields, and write it back out.

use std::fmt::Write as _;

/// How deeply arrays and objects may nest before a document is refused.
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// The number as it was written, checked against the JSON grammar, so
    /// that no precision is lost on the way back out.
    Number(String),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

impl Value {
    /// The field `key` of an object.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// A dotted path, `a.b.c`, through nested objects.
    pub fn path(&self, path: &str) -> Option<&Value> {
        path.split('.').try_fold(self, |v, key| v.get(key))
    }

    pub fn to_json(&self) -> String {
        let mut out = String::new();
        write_value(&mut out, self);
        out
    }
}

/// Parse one JSON value; anything but whitespace after it is an error.
pub fn parse(text: &str) -> Result<Value, String> {
    let mut p = Parser {
        s: text,
        b: text.as_bytes(),
        i: 0,
    };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != p.b.len() {
        return p.err("unexpected text after the value");
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn err<T>(&self, what: &str) -> Result<T, String> {
        Err(format!("{what} at byte {}", self.i))
    }

    fn ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.b.get(self.i) {
            self.i += 1;
        }
    }

    fn eat(&mut self, lit: &str) -> bool {
        if self.b[self.i..].starts_with(lit.as_bytes()) {
            self.i += lit.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value, String> {
        if depth > MAX_DEPTH {
            return self.err("nested too deeply");
        }
        match self.b.get(self.i) {
            None => self.err("unexpected end"),
            Some(b'n') if self.eat("null") => Ok(Value::Null),
            Some(b't') if self.eat("true") => Ok(Value::Bool(true)),
            Some(b'f') if self.eat("false") => Ok(Value::Bool(false)),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => self.err("unexpected character"),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value, String> {
        self.i += 1;
        let mut items = Vec::new();
        self.ws();
        if self.eat("]") {
            return Ok(Value::Array(items));
        }
        loop {
            self.ws();
            items.push(self.value(depth + 1)?);
            self.ws();
            if self.eat(",") {
                continue;
            }
            if self.eat("]") {
                return Ok(Value::Array(items));
            }
            return self.err("expected `,` or `]`");
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value, String> {
        self.i += 1;
        let mut fields: Vec<(String, Value)> = Vec::new();
        self.ws();
        if self.eat("}") {
            return Ok(Value::Object(fields));
        }
        loop {
            self.ws();
            if self.b.get(self.i) != Some(&b'"') {
                return self.err("expected a string key");
            }
            let key = self.string()?;
            if fields.iter().any(|(k, _)| *k == key) {
                return self.err(&format!("duplicate key `{key}`"));
            }
            self.ws();
            if !self.eat(":") {
                return self.err("expected `:`");
            }
            self.ws();
            let v = self.value(depth + 1)?;
            fields.push((key, v));
            self.ws();
            if self.eat(",") {
                continue;
            }
            if self.eat("}") {
                return Ok(Value::Object(fields));
            }
            return self.err("expected `,` or `}`");
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.i;
        while let Some(b'0'..=b'9') = self.b.get(self.i) {
            self.i += 1;
        }
        self.i - start
    }

    fn number(&mut self) -> Result<Value, String> {
        let start = self.i;
        self.eat("-");
        match self.b.get(self.i) {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return self.err("malformed number"),
        }
        if self.eat(".") && self.digits() == 0 {
            return self.err("malformed number");
        }
        if let Some(b'e' | b'E') = self.b.get(self.i) {
            self.i += 1;
            if let Some(b'+' | b'-') = self.b.get(self.i) {
                self.i += 1;
            }
            if self.digits() == 0 {
                return self.err("malformed number");
            }
        }
        Ok(Value::Number(self.s[start..self.i].to_string()))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        match self.s.get(self.i..self.i + 4) {
            Some(h) if h.bytes().all(|c| c.is_ascii_hexdigit()) => {
                self.i += 4;
                Ok(u32::from_str_radix(h, 16).expect("four hex digits"))
            }
            _ => self.err("malformed \\u escape"),
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let start = self.i;
            while let Some(&c) = self.b.get(self.i) {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.i += 1;
            }
            // The stops are ASCII, so the slice falls on character boundaries.
            out.push_str(&self.s[start..self.i]);
            match self.b.get(self.i) {
                None => return self.err("unterminated string"),
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.i += 1;
                    let escape = self.b.get(self.i).copied();
                    self.i += 1;
                    match escape {
                        Some(b'"') => out.push('"'),
                        Some(b'\\') => out.push('\\'),
                        Some(b'/') => out.push('/'),
                        Some(b'b') => out.push('\u{8}'),
                        Some(b'f') => out.push('\u{c}'),
                        Some(b'n') => out.push('\n'),
                        Some(b'r') => out.push('\r'),
                        Some(b't') => out.push('\t'),
                        Some(b'u') => {
                            let hi = self.hex4()?;
                            let code = if (0xD800..0xDC00).contains(&hi) {
                                if !self.eat("\\u") {
                                    return self.err("unpaired surrogate");
                                }
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return self.err("unpaired surrogate");
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            match char::from_u32(code) {
                                Some(c) => out.push(c),
                                None => return self.err("unpaired surrogate"),
                            }
                        }
                        _ => {
                            self.i -= 1;
                            return self.err("malformed escape");
                        }
                    }
                }
                Some(_) => return self.err("control character in a string"),
            }
        }
    }
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(n),
        Value::String(s) => write_str(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(fields) => {
            out.push('{');
            for (i, (k, item)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, k);
                out.push(':');
                write_value(out, item);
            }
            out.push('}');
        }
    }
}

/// `s` as a JSON string literal.
pub fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `s` as a JSON string literal, on its own.
pub fn quote(s: &str) -> String {
    let mut out = String::new();
    write_str(&mut out, s);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_reads_and_writes_back_the_same() {
        let text = r#"{"id":"a1","n":-12.5e3,"ok":true,"none":null,"tags":["x","y"],"nested":{"k":"\u00e9\n"}}"#;
        let v = parse(text).unwrap();
        assert_eq!(v.path("nested.k"), Some(&Value::String("é\n".into())));
        assert_eq!(v.get("n"), Some(&Value::Number("-12.5e3".into())));
        assert_eq!(parse(&v.to_json()).unwrap(), v);
        assert_eq!(
            v.to_json(),
            r#"{"id":"a1","n":-12.5e3,"ok":true,"none":null,"tags":["x","y"],"nested":{"k":"é\n"}}"#
        );
    }

    #[test]
    fn a_surrogate_pair_is_one_character() {
        assert_eq!(
            parse(r#""\ud83d\ude00""#).unwrap(),
            Value::String("😀".into())
        );
    }

    #[test]
    fn malformed_text_is_refused_with_its_place() {
        for bad in [
            "",
            "{",
            "[1,]",
            "{\"a\":1,}",
            "01",
            "1.",
            "-",
            "1e",
            "\"abc",
            "\"\\x\"",
            "\"\\ud800\"",
            "{\"a\":1,\"a\":2}",
            "{a:1}",
            "nul",
            "1 2",
            "\"\u{1}\"",
        ] {
            let e = parse(bad).unwrap_err();
            assert!(e.contains("at byte"), "{bad:?}: {e}");
        }
    }

    #[test]
    fn nesting_is_bounded() {
        let deep = "[".repeat(MAX_DEPTH + 2) + &"]".repeat(MAX_DEPTH + 2);
        assert!(parse(&deep).unwrap_err().contains("nested too deeply"));
        let fine = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(parse(&fine).is_ok());
    }
}
