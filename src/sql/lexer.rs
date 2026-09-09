//! Tokeniser.

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    /// A quoted string literal.
    Str(String),
    Int(i64),
    Float(f64),
    /// `$1`, bound by the caller.
    Param(usize),
    Punct(&'static str),
    Eof,
}

impl Tok {
    pub fn describe(&self) -> String {
        match self {
            Tok::Ident(s) => format!("`{s}`"),
            Tok::Str(s) => format!("'{s}'"),
            Tok::Int(i) => i.to_string(),
            Tok::Float(f) => f.to_string(),
            Tok::Param(n) => format!("${n}"),
            Tok::Punct(p) => format!("`{p}`"),
            Tok::Eof => "end of statement".into(),
        }
    }
}

/// Multi-character operators, longest first so that `<=>` never lexes as `<=`
/// followed by `>`.
const PUNCT: &[&str] = &[
    "<->", "<=>", "<#>", "=>", ">=", "<=", "<>", "!=", "||", "(", ")", "[", "]", ",", ".", ";",
    "*", "=", "<", ">", "+", "-", "/", "%",
];

pub fn lex(input: &str) -> Result<Vec<Tok>> {
    let cs: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // Comments.
        if c == '-' && i + 1 < cs.len() && cs[i + 1] == '-' {
            while i < cs.len() && cs[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            loop {
                if i >= cs.len() {
                    return Err(Error::Sql("unterminated string literal".into()));
                }
                if cs[i] == '\'' {
                    // Doubled quote is an escaped quote.
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(cs[i]);
                i += 1;
            }
            out.push(Tok::Str(s));
            continue;
        }
        if c == '"' {
            // Double-quoted identifier.
            i += 1;
            let start = i;
            while i < cs.len() && cs[i] != '"' {
                i += 1;
            }
            out.push(Tok::Ident(cs[start..i].iter().collect()));
            i += 1;
            continue;
        }
        if c == '$' {
            i += 1;
            let start = i;
            while i < cs.len() && cs[i].is_ascii_digit() {
                i += 1;
            }
            let n: usize = cs[start..i]
                .iter()
                .collect::<String>()
                .parse()
                .map_err(|_| Error::Sql("bad parameter reference".into()))?;
            out.push(Tok::Param(n));
            continue;
        }
        if c.is_ascii_digit() || (c == '.' && i + 1 < cs.len() && cs[i + 1].is_ascii_digit()) {
            let start = i;
            let mut is_float = false;
            while i < cs.len() && (cs[i].is_ascii_digit() || cs[i] == '.') {
                if cs[i] == '.' {
                    is_float = true;
                }
                i += 1;
            }
            if i < cs.len() && (cs[i] == 'e' || cs[i] == 'E') {
                is_float = true;
                i += 1;
                if i < cs.len() && (cs[i] == '+' || cs[i] == '-') {
                    i += 1;
                }
                while i < cs.len() && cs[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let s: String = cs[start..i].iter().collect();
            out.push(if is_float {
                Tok::Float(s.parse().map_err(|_| Error::Sql(format!("bad number `{s}`")))?)
            } else {
                match s.parse::<i64>() {
                    Ok(v) => Tok::Int(v),
                    Err(_) => {
                        Tok::Float(s.parse().map_err(|_| Error::Sql(format!("bad number `{s}`")))?)
                    }
                }
            });
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < cs.len() && (cs[i].is_alphanumeric() || cs[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(cs[start..i].iter().collect()));
            continue;
        }
        let rest: String = cs[i..].iter().take(3).collect();
        match PUNCT.iter().find(|p| rest.starts_with(**p)) {
            Some(p) => {
                out.push(Tok::Punct(p));
                i += p.chars().count();
            }
            None => return Err(Error::Sql(format!("unexpected character `{c}`"))),
        }
    }
    out.push(Tok::Eof);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_operators_lex_whole() {
        let t = lex("a <-> b <=> c <#> d <= e").unwrap();
        assert_eq!(t[1], Tok::Punct("<->"));
        assert_eq!(t[3], Tok::Punct("<=>"));
        assert_eq!(t[5], Tok::Punct("<#>"));
        assert_eq!(t[7], Tok::Punct("<="));
    }

    #[test]
    fn literals_and_params() {
        let t = lex("'it''s', 1, 2.5, 1e3, $2, \"Odd Name\"").unwrap();
        assert_eq!(t[0], Tok::Str("it's".into()));
        assert_eq!(t[2], Tok::Int(1));
        assert_eq!(t[4], Tok::Float(2.5));
        assert_eq!(t[6], Tok::Float(1000.0));
        assert_eq!(t[8], Tok::Param(2));
        assert_eq!(t[10], Tok::Ident("Odd Name".into()));
    }
}
