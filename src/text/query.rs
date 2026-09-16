//! The `text_match` query-string grammar (§2.4).
//!
//! ```text
//! query    := or_expr
//! or_expr  := and_expr (OR and_expr)*
//! and_expr := unary (AND? unary)*        -- juxtaposition is the default op
//! unary    := (NOT | '-') unary | primary
//! primary  := '(' query ')' | '"' phrase '"' | word ['*']
//! ```
//!
//! Juxtaposition defaults to OR, matching what users expect from a search box
//! and what makes `hybrid()` candidate generation behave: a should-clause that
//! required every term would starve fusion of candidates. Fuzzy matching is
//! v2 and is rejected here rather than silently ignored.

use crate::error::{Error, Result};
use crate::text::analyzer::Analyzer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextQuery {
    /// An analyzed term.
    Term(String),
    /// A prefix, matched against the term dictionary's sorted range.
    Prefix(String),
    /// Analyzed terms that must appear adjacently, in order.
    Phrase(Vec<String>),
    All(Vec<TextQuery>),
    Any(Vec<TextQuery>),
    Not(Box<TextQuery>),
    /// Matches nothing — what an all-stopword query analyzes down to.
    Empty,
}

impl TextQuery {
    pub fn parse(input: &str, analyzer: Analyzer) -> Result<TextQuery> {
        let toks = lex(input)?;
        let mut p = QParser { toks, i: 0, analyzer };
        let q = p.or_expr()?;
        if p.i != p.toks.len() {
            return Err(Error::Sql(format!(
                "unexpected `{}` in text_match query",
                p.toks[p.i].text()
            )));
        }
        Ok(q)
    }

    /// Every positive leaf term, for the coordinator's global-statistics
    /// lookup (§8.1: "look up the query's terms ... and attach only those").
    pub fn leaf_terms(&self, out: &mut Vec<String>) {
        match self {
            TextQuery::Term(t) => out.push(t.clone()),
            TextQuery::Prefix(_) | TextQuery::Empty => {}
            TextQuery::Phrase(ts) => out.extend(ts.iter().cloned()),
            TextQuery::All(v) | TextQuery::Any(v) => {
                for q in v {
                    q.leaf_terms(out);
                }
            }
            // Not recursed, and [`leaf_prefixes`](Self::leaf_prefixes) does
            // recurse: a negated term is never scored, so its `df` is never
            // read, while a negated prefix's term list is the exclusion set.
            TextQuery::Not(_) => {}
        }
    }

    /// Every prefix leaf, for the coordinator's expansion. The resolved term
    /// list is pinned once, at the coordinator, rather than re-derived by every
    /// searchable unit: the cap is applied to the UNION, so which terms the
    /// query names stops depending on how the corpus happens to be laid out.
    ///
    /// The `Not` arm recurses here and deliberately does NOT in
    /// [`leaf_terms`](Self::leaf_terms), and the asymmetry is the point rather
    /// than an oversight to tidy up. A negated TERM contributes no score, so
    /// its document frequency is never read and gathering one is wasted work.
    /// A negated PREFIX is different in kind: its expansion is the EXCLUSION
    /// set, so the list decides which documents the query drops. Leave it to
    /// each unit and the exclusion set moves with the layout, which is the same
    /// defect as on the positive side and just as silent.
    /// The POLARITY travels with each prefix, and it has to. Truncating a
    /// positive expansion loses rows; truncating a negated one does the
    /// opposite — the exclusion set is short, so rows the query should have
    /// dropped are still in the answer. A report that does not know which leaf
    /// it is describing says the inverse of what happened for `-a*`, which is
    /// worse than saying nothing.
    pub fn leaf_prefixes(&self, out: &mut Vec<(String, bool)>) {
        self.prefixes_under(false, out)
    }

    /// [`leaf_prefixes`](Self::leaf_prefixes) with the sign SEEDED, for a
    /// caller that already knows this query string sits under a negation it
    /// cannot see — SQL's own `NOT` wraps the whole `text_match` call, so the
    /// sign the mini-language records is only half of the leaf's real
    /// polarity. Seeding composes the two for free: the `Not` arm below flips,
    /// so `NOT text_match(body, '-a*')` comes back positive, which is what a
    /// double negation is.
    pub(crate) fn prefixes_under(&self, negated: bool, out: &mut Vec<(String, bool)>) {
        match self {
            TextQuery::Prefix(p) => out.push((p.clone(), negated)),
            TextQuery::Term(_) | TextQuery::Phrase(_) | TextQuery::Empty => {}
            TextQuery::All(v) | TextQuery::Any(v) => {
                for q in v {
                    q.prefixes_under(negated, out);
                }
            }
            // Flipped rather than set: `--a*` is a double negation and its
            // expansion is a matching set again, however unlikely a spelling
            // it is.
            TextQuery::Not(q) => q.prefixes_under(!negated, out),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            TextQuery::Empty => true,
            TextQuery::All(v) | TextQuery::Any(v) => v.iter().all(|q| q.is_empty()),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Quoted(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Star,
}

impl Tok {
    fn text(&self) -> String {
        match self {
            Tok::Word(w) => w.clone(),
            Tok::Quoted(w) => format!("\"{w}\""),
            Tok::LParen => "(".into(),
            Tok::RParen => ")".into(),
            Tok::And => "AND".into(),
            Tok::Or => "OR".into(),
            Tok::Not => "NOT".into(),
            Tok::Star => "*".into(),
        }
    }
}

fn lex(s: &str) -> Result<Vec<Tok>> {
    let mut out = Vec::new();
    let cs: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '-' => {
                out.push(Tok::Not);
                i += 1;
            }
            '~' => return Err(Error::Sql("fuzzy matching (`~`) is not supported in v1".into())),
            '"' => {
                i += 1;
                let start = i;
                while i < cs.len() && cs[i] != '"' {
                    i += 1;
                }
                if i >= cs.len() {
                    return Err(Error::Sql("unterminated phrase in text_match query".into()));
                }
                out.push(Tok::Quoted(cs[start..i].iter().collect()));
                i += 1;
            }
            _ => {
                let start = i;
                while i < cs.len()
                    && !cs[i].is_whitespace()
                    && !matches!(cs[i], '(' | ')' | '*' | '"' | '~')
                {
                    i += 1;
                }
                let w: String = cs[start..i].iter().collect();
                out.push(match w.as_str() {
                    "AND" | "&&" => Tok::And,
                    "OR" | "||" => Tok::Or,
                    "NOT" => Tok::Not,
                    _ => Tok::Word(w),
                });
            }
        }
    }
    Ok(out)
}

struct QParser {
    toks: Vec<Tok>,
    i: usize,
    analyzer: Analyzer,
}

impl QParser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    fn or_expr(&mut self) -> Result<TextQuery> {
        let mut parts = vec![self.and_expr()?];
        while self.peek() == Some(&Tok::Or) {
            self.i += 1;
            parts.push(self.and_expr()?);
        }
        Ok(if parts.len() == 1 { parts.pop().unwrap() } else { TextQuery::Any(parts) })
    }

    fn and_expr(&mut self) -> Result<TextQuery> {
        let mut explicit_and = false;
        let mut parts = vec![self.unary()?];
        loop {
            match self.peek() {
                Some(Tok::And) => {
                    self.i += 1;
                    explicit_and = true;
                    parts.push(self.unary()?);
                }
                Some(Tok::Word(_)) | Some(Tok::Quoted(_)) | Some(Tok::LParen) | Some(Tok::Not) => {
                    parts.push(self.unary()?);
                }
                _ => break,
            }
        }
        // A bare `-term` is a must-not on the whole run, not one alternative
        // among several. `quick -lazy` means "quick, but not lazy" in every
        // search box ever built; reading it as `quick OR (not lazy)` would ask
        // for the complement of `lazy`, which no posting list can produce.
        let (neg, pos): (Vec<TextQuery>, Vec<TextQuery>) =
            parts.into_iter().partition(|p| matches!(p, TextQuery::Not(_)));
        let body = match pos.len() {
            0 => None,
            1 => Some(pos.into_iter().next().unwrap()),
            // Juxtaposition is OR; an explicit AND anywhere in the run makes
            // the whole run conjunctive.
            _ => Some(if explicit_and { TextQuery::All(pos) } else { TextQuery::Any(pos) }),
        };
        Ok(match (body, neg.is_empty()) {
            (Some(b), true) => b,
            (Some(b), false) => {
                let mut all = vec![b];
                all.extend(neg);
                TextQuery::All(all)
            }
            (None, true) => TextQuery::Empty,
            (None, false) => TextQuery::All(neg),
        })
    }

    fn unary(&mut self) -> Result<TextQuery> {
        if self.peek() == Some(&Tok::Not) {
            self.i += 1;
            return Ok(TextQuery::Not(Box::new(self.unary()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<TextQuery> {
        match self.peek().cloned() {
            Some(Tok::LParen) => {
                self.i += 1;
                let q = self.or_expr()?;
                if self.peek() != Some(&Tok::RParen) {
                    return Err(Error::Sql("unbalanced parentheses in text_match query".into()));
                }
                self.i += 1;
                Ok(q)
            }
            Some(Tok::Quoted(s)) => {
                self.i += 1;
                let terms = self.analyzer.terms(&s);
                Ok(if terms.is_empty() { TextQuery::Empty } else { TextQuery::Phrase(terms) })
            }
            Some(Tok::Word(w)) => {
                self.i += 1;
                if self.peek() == Some(&Tok::Star) {
                    self.i += 1;
                    // A prefix is matched against raw dictionary terms, so it
                    // is folded but never stemmed — stemming a prefix would
                    // move it out of the range it is meant to open. Folding is
                    // not optional though: the dictionary holds `cafe`, so a
                    // merely lowercased `café` opens an empty range.
                    return Ok(TextQuery::Prefix(fold_prefix(self.analyzer, &w)));
                }
                let terms = self.analyzer.terms(&w);
                Ok(match terms.len() {
                    0 => TextQuery::Empty,
                    1 => TextQuery::Term(terms.into_iter().next().unwrap()),
                    _ => TextQuery::Any(terms.into_iter().map(TextQuery::Term).collect()),
                })
            }
            other => Err(Error::Sql(format!(
                "unexpected {} in text_match query",
                other.map(|t| t.text()).unwrap_or_else(|| "end of input".into())
            ))),
        }
    }
}

/// Fold a prefix exactly the way the field's analyzer folded the terms it
/// indexed. *Which* folding is as load-bearing as folding at all: `Keyword`
/// only trims and lowercases, so a keyword field holds `o'reilly` and `café`,
/// and the standard folding — which drops apostrophes and strips accents —
/// would compile `O'Rei*` to `orei` and open a dictionary range that term can
/// never be in.
fn fold_prefix(analyzer: Analyzer, w: &str) -> String {
    match analyzer {
        Analyzer::Keyword => w.trim().to_lowercase(),
        Analyzer::Standard | Analyzer::English => crate::text::analyzer::fold(w),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn fuzz_text_query_parsing_never_panics() {
        let samples = [
            "graph search",
            "\"exact phrase\" +must -not comp* OR (a b)",
            "caf\u{e9} \u{1f600} ***",
            "",
        ];
        let analyzer = crate::text::analyzer::Analyzer::parse("english");
        crate::fuzz::sweep_text(121, &samples, 6000, |t| {
            let _ = TextQuery::parse(t, analyzer);
        });
    }
    use super::*;

    #[test]
    fn juxtaposition_is_or_explicit_and_wins() {
        let q = TextQuery::parse("quick brown", Analyzer::English).unwrap();
        assert!(matches!(q, TextQuery::Any(_)));
        let q = TextQuery::parse("quick AND brown", Analyzer::English).unwrap();
        assert!(matches!(q, TextQuery::All(_)));
    }

    #[test]
    fn phrases_prefixes_and_negation() {
        let q = TextQuery::parse("\"vector index\" -deprecated hnsw*", Analyzer::English).unwrap();
        // The negation is lifted out of the implicit OR and applies to the
        // whole run: (phrase OR prefix) AND NOT deprecated.
        let TextQuery::All(parts) = q else { panic!("expected All, got {q:?}") };
        assert_eq!(parts.len(), 2);
        let TextQuery::Any(pos) = &parts[0] else { panic!("expected Any, got {:?}", parts[0]) };
        assert_eq!(pos[0], TextQuery::Phrase(vec!["vector".into(), "index".into()]));
        assert_eq!(pos[1], TextQuery::Prefix("hnsw".into()));
        assert!(matches!(parts[1], TextQuery::Not(_)));
    }

    #[test]
    fn a_bare_minus_is_a_must_not_on_the_whole_run() {
        let q = TextQuery::parse("quick -lazy", Analyzer::English).unwrap();
        let TextQuery::All(parts) = q else { panic!("expected All, got {q:?}") };
        assert_eq!(parts[0], TextQuery::Term("quick".into()));
        assert!(matches!(parts[1], TextQuery::Not(_)));
        // An explicit OR with a negation is a different, and unanswerable,
        // request — it survives parsing and is refused at compile time.
        let q = TextQuery::parse("quick OR -lazy", Analyzer::English).unwrap();
        assert!(matches!(q, TextQuery::Any(_)), "{q:?}");
    }

    #[test]
    fn leaf_prefixes_descends_into_not_and_leaf_terms_does_not() {
        // The asymmetry is deliberate and documented on both arms, and this is
        // the test that stops it being "tidied up" in either direction.
        //
        // `quick -lazy* -slow` parses to `quick AND NOT lazy* AND NOT slow`.
        // `slow` is negated, so it is never scored and its document frequency
        // is never read: gathering one would be a posting-list walk per shard
        // for a number nothing consumes. `lazy*` is negated too, but its
        // expansion is not a weight — it is the set of documents the query
        // throws away. Resolve that per searchable unit and the exclusion set
        // moves with the flush and compaction schedule, exactly as the
        // positive side did.
        let q = TextQuery::parse("quick -lazy* -slow fast*", Analyzer::English).unwrap();
        let (mut terms, mut prefixes) = (Vec::new(), Vec::new());
        q.leaf_terms(&mut terms);
        q.leaf_prefixes(&mut prefixes);
        assert_eq!(terms, vec!["quick".to_string()], "`slow` is negated: no df is ever read");
        // The polarity comes back with each prefix, because truncating the two
        // costs opposite things: a cut positive expansion loses rows, a cut
        // exclusion set keeps rows it was asked to drop. The report is written
        // off this flag, so getting it here wrong states the inverse of what
        // happened to the caller.
        assert_eq!(
            prefixes,
            vec![("fast".to_string(), false), ("lazy".to_string(), true)],
            "both prefixes, negated or not: each names a term list the answer depends on"
        );

        // A double negation is a matching set again. Flipping rather than
        // setting the flag is what makes that true, and nothing else in the
        // suite writes `--`.
        let q = TextQuery::parse("quick --lazy*", Analyzer::English).unwrap();
        let mut prefixes = Vec::new();
        q.leaf_prefixes(&mut prefixes);
        assert_eq!(prefixes, vec![("lazy".to_string(), false)], "{q:?}");
    }

    #[test]
    fn a_prefix_is_folded_the_same_way_the_dictionary_was() {
        // The indexer folds `Café` to `cafe`; a prefix that only lowercases
        // opens a range containing nothing.
        assert_eq!(
            TextQuery::parse("Café*", Analyzer::English).unwrap(),
            TextQuery::Prefix("cafe".into())
        );
        assert_eq!(
            TextQuery::parse("O'Rei*", Analyzer::Standard).unwrap(),
            TextQuery::Prefix("orei".into())
        );
    }

    /// A keyword field indexes `O'Reilly` as `o'reilly` and `Café` as `café`.
    /// Folding the prefix the standard way instead drops the apostrophe and
    /// the accent, and opens a dictionary range those terms cannot be in — the
    /// prefix matches nothing, forever.
    #[test]
    fn a_keyword_prefix_keeps_what_the_keyword_analyzer_kept() {
        assert_eq!(
            TextQuery::parse("O'Rei*", Analyzer::Keyword).unwrap(),
            TextQuery::Prefix("o'rei".into())
        );
        assert_eq!(
            TextQuery::parse("Café*", Analyzer::Keyword).unwrap(),
            TextQuery::Prefix("café".into())
        );
    }

    #[test]
    fn fuzzy_is_rejected_not_ignored() {
        assert!(TextQuery::parse("roam~2", Analyzer::English).is_err());
    }

    #[test]
    fn all_stopwords_analyzes_to_empty() {
        let q = TextQuery::parse("the and of", Analyzer::English).unwrap();
        assert!(q.is_empty());
    }
}
