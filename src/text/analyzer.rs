//! Analysis: text in, `(term, position)` out.
//!
//! Analyzer configuration is per index and stored in the catalog, and the same
//! analyzer is applied to query text (§5.1). Getting that wrong is the classic
//! way to build a search engine that silently cannot find its own documents,
//! so there is exactly one entry point — [`Analyzer::analyze`] — used by both
//! the indexer and the query parser.

/// Position gap inserted between elements of a text array, so a phrase query
/// cannot match across element boundaries (§2.2).
pub const ARRAY_POSITION_GAP: u32 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Analyzer {
    /// Unicode-ish word split, lowercase, ASCII folding. No stemming, no
    /// stopwords.
    Standard,
    /// Standard plus English stopwords and light suffix stripping.
    English,
    /// The whole input is one term. For identifiers and enums.
    Keyword,
}

impl Analyzer {
    pub fn parse(s: &str) -> Analyzer {
        match s.to_ascii_lowercase().as_str() {
            "english" => Analyzer::English,
            "keyword" => Analyzer::Keyword,
            _ => Analyzer::Standard,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Analyzer::Standard => "standard",
            Analyzer::English => "english",
            Analyzer::Keyword => "keyword",
        }
    }

    pub fn analyze(self, text: &str, start_position: u32, out: &mut Vec<(String, u32)>) {
        if self == Analyzer::Keyword {
            let t = text.trim().to_lowercase();
            if !t.is_empty() {
                out.push((t, start_position));
            }
            return;
        }
        let mut pos = start_position;
        for raw in split_words(text) {
            let folded = fold(&raw);
            if folded.is_empty() {
                continue;
            }
            if self == Analyzer::English {
                if is_stopword(&folded) {
                    // Stopwords still consume a position, so phrase offsets
                    // survive their removal.
                    pos += 1;
                    continue;
                }
                out.push((stem_english(&folded), pos));
            } else {
                out.push((folded, pos));
            }
            pos += 1;
        }
    }

    pub fn terms(self, text: &str) -> Vec<String> {
        let mut v = Vec::new();
        self.analyze(text, 0, &mut v);
        v.into_iter().map(|(t, _)| t).collect()
    }
}

/// The words of `text` as the analyzers see them, in order: what a
/// snippet is cut from, so its words are the ones the positions count.
pub(crate) fn split_words(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() || c == '_' || c == '\'' {
            cur.push(c);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Lowercase and strip diacritics for the Latin-1 range. Not a full Unicode
/// normalisation; it covers the accents that actually cost recall in Western
/// European text.
///
/// Public because a prefix query has to be folded the same way the dictionary
/// was — `Café*` folds to `cafe` and matches, where a bare lowercase `café`
/// cannot open a range that contains no accented terms.
pub fn fold(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars().flat_map(|c| c.to_lowercase()) {
        let folded = match c {
            'à'..='å' => 'a',
            'è'..='ë' => 'e',
            'ì'..='ï' => 'i',
            'ò'..='ö' => 'o',
            'ù'..='ü' => 'u',
            'ñ' => 'n',
            'ç' => 'c',
            'ý' | 'ÿ' => 'y',
            '\'' => continue,
            other => other,
        };
        out.push(folded);
    }
    out
}

const STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

fn is_stopword(w: &str) -> bool {
    STOPWORDS.binary_search(&w).is_ok()
}

/// A light English stemmer: the plural and past/progressive rules of Porter
/// step 1, which is where nearly all of the recall gain lives, without the
/// steps that produce unreadable stems and surprising collisions. Full Porter
/// or a Snowball port is a drop-in replacement behind this signature.
fn stem_english(w: &str) -> String {
    let b: Vec<char> = w.chars().collect();
    let n = b.len();
    if n <= 3 {
        return w.to_string();
    }
    let s: String = b.iter().collect();
    // Step 1a: plurals.
    let s = if s.ends_with("sses") {
        s[..s.len() - 2].to_string()
    } else if s.ends_with("ies") {
        format!("{}i", &s[..s.len() - 3])
    } else if s.ends_with("ss") {
        s
    } else if s.ends_with('s') {
        s[..s.len() - 1].to_string()
    } else {
        s
    };
    // Step 1b: -eed / -ed / -ing, only when a vowel remains in the stem.
    let has_vowel = |t: &str| t.chars().any(|c| "aeiouy".contains(c));

    if s.ends_with("eed") {
        if measure(&s[..s.len() - 3]) > 0 {
            s[..s.len() - 1].to_string()
        } else {
            s
        }
    } else if s.ends_with("ed") && has_vowel(&s[..s.len() - 2]) {
        fix_after_1b(&s[..s.len() - 2])
    } else if s.ends_with("ing") && has_vowel(&s[..s.len() - 3]) {
        fix_after_1b(&s[..s.len() - 3])
    } else {
        s
    }
}

fn fix_after_1b(stem: &str) -> String {
    if stem.ends_with("at") || stem.ends_with("bl") || stem.ends_with("iz") {
        format!("{stem}e")
    } else if ends_with_double_consonant(stem) && !"lsz".contains(stem.chars().last().unwrap()) {
        // Drop one *character*. The test that got us here matched on chars, so
        // slicing at `len - 1` bytes splits a multi-byte one — `aππed` stems to
        // `aππ` and then panics on a non-char-boundary slice, from ordinary
        // query text.
        let last = stem.chars().next_back().map(|c| c.len_utf8()).unwrap_or(0);
        stem[..stem.len() - last].to_string()
    } else {
        stem.to_string()
    }
}

fn ends_with_double_consonant(s: &str) -> bool {
    let c: Vec<char> = s.chars().collect();
    c.len() >= 2 && c[c.len() - 1] == c[c.len() - 2] && !"aeiou".contains(c[c.len() - 1])
}

/// Porter's `m`: the number of vowel-consonant transitions in the stem.
fn measure(s: &str) -> usize {
    let mut m = 0;
    let mut prev_vowel = false;
    for (i, c) in s.chars().enumerate() {
        let vowel = "aeiou".contains(c) || (c == 'y' && i > 0 && !prev_vowel);
        if prev_vowel && !vowel {
            m += 1;
        }
        prev_vowel = vowel;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stopwords_hold_positions() {
        let mut out = Vec::new();
        Analyzer::English.analyze("the quick brown fox", 0, &mut out);
        assert_eq!(out[0].0, "quick");
        // "the" consumed position 0, so "quick" is at 1 and a phrase query for
        // "quick brown" still sees adjacency.
        assert_eq!(out[0].1, 1);
        assert_eq!(out[1].1, 2);
    }

    #[test]
    fn indexing_and_query_analysis_agree() {
        let doc = Analyzer::English.terms("Running the RACES quickly");
        let q = Analyzer::English.terms("races running");
        assert!(doc.contains(&q[0]), "{doc:?} vs {q:?}");
        assert!(doc.contains(&q[1]), "{doc:?} vs {q:?}");
    }

    /// Analysis runs on arbitrary user text, including query strings.
    #[test]
    fn analysis_never_panics_on_non_ascii() {
        for w in ["aππed", "aщщing", "a日日ing", "ππ", "ßßed", "ééed", "naïveed", "ΑΑΑed", "🙂🙂ed"]
        {
            let _ = Analyzer::English.terms(w);
            let _ = Analyzer::Standard.terms(w);
        }
    }

    #[test]
    fn folding_and_keyword() {
        assert_eq!(Analyzer::Standard.terms("Café Crème"), vec!["cafe", "creme"]);
        assert_eq!(Analyzer::Keyword.terms("US-East 1"), vec!["us-east 1"]);
    }
}
