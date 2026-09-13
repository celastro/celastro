//! The document value model.
//!
//! Documents are self-describing JSON-like values (§2.1). The engine
//! never coerces silently: comparison against a typed literal only evaluates
//! against values of the matching type, everything else is NULL. That is the
//! Postgres `jsonb` behaviour and it is what keeps polymorphic paths from
//! producing surprises.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{Error, Result};

/// The deepest nesting a value may have, counted as containers enclosing a
/// value: a scalar may sit inside at most `MAX_DEPTH - 1` of them, so a value
/// holds at most `MAX_DEPTH - 1` nested containers with something in the
/// innermost. One bound in one place for every door a value comes through --
/// `json::parse` on the way in, `variant::decode` on the way back from disk,
/// and [`Value::set_path`] for a value built in memory -- because a value one
/// door admits and another refuses is a value that encodes and then cannot be
/// read back. The parser and the writers are recursive, so without a bound an
/// input like `"[".repeat(200_000)` overflows the stack and aborts the
/// process, which no `Result` and no `catch_unwind` can contain.
pub const MAX_DEPTH: usize = 128;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// A timestamp in microseconds since the Unix epoch. JSON has no date type,
    /// so this is only ever produced by DDL declaration or an explicit CAST
    /// (§2.1); an undeclared ISO-8601 string stays a string.
    Timestamp(i64),
    Array(Vec<Value>),
    /// Field order is normalised to sorted-by-key so that encoding is
    /// deterministic and equality is structural.
    Object(Vec<(String, Value)>),
}

/// The concrete types the catalog tracks per path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueType {
    Null,
    Bool,
    /// Integers and doubles at the same path are one widened numeric type, not
    /// polymorphism (§2.1).
    Number,
    Str,
    Timestamp,
    Array,
    Object,
}

impl ValueType {
    pub fn name(self) -> &'static str {
        match self {
            ValueType::Null => "null",
            ValueType::Bool => "boolean",
            ValueType::Number => "number",
            ValueType::Str => "text",
            ValueType::Timestamp => "timestamp",
            ValueType::Array => "array",
            ValueType::Object => "object",
        }
    }
}

impl Value {
    pub fn obj(fields: Vec<(String, Value)>) -> Value {
        let mut f = fields;
        f.sort_by(|a, b| a.0.cmp(&b.0));
        // A repeated key takes its last value, the way serde_json, `JSON.parse`
        // and Python's `json` all resolve one; first-wins silently discarded
        // the value the writer meant. The sort is stable, so the duplicates
        // arrive in source order, and `dedup_by` keeps the *earlier* of each
        // pair — hence the swap, which moves the later value into the slot
        // that survives.
        f.dedup_by(|a, b| {
            if a.0 == b.0 {
                std::mem::swap(a, b);
                true
            } else {
                false
            }
        });
        Value::Object(f)
    }

    pub fn ty(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int(_) | Value::Float(_) => ValueType::Number,
            Value::Str(_) => ValueType::Str,
            Value::Timestamp(_) => ValueType::Timestamp,
            Value::Array(_) => ValueType::Array,
            Value::Object(_) => ValueType::Object,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Timestamp(t) => Some(*t as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Timestamp(t) => Some(*t),
            Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(fields) => {
                fields.binary_search_by(|(k, _)| k.as_str().cmp(key)).ok().map(|i| &fields[i].1)
            }
            _ => None,
        }
    }

    /// Resolve a dotted path such as `author.name`. Array elements are not
    /// indexed here; scalar-array containment is handled by the predicate
    /// evaluator (§2.2).
    pub fn path(&self, path: &str) -> Option<&Value> {
        let mut cur = self;
        for seg in path.split('.') {
            cur = cur.get(seg)?;
        }
        Some(cur)
    }

    /// Set the value at a dotted path, creating the objects along it.
    ///
    /// Refused when the result would nest deeper than [`MAX_DEPTH`]: the path's
    /// segments each add a container, and the value brings its own. Nothing
    /// is changed by a refusal. This is the one door into a value that is not
    /// a parse, and it used to be the one that could build a value the
    /// encoder accepted and the decoder refused -- a document that was written
    /// and then could not be read.
    pub fn set_path(&mut self, path: &str, v: Value) -> Result<()> {
        let segments = path.split('.').count();
        let depth = segments + v.depth();
        if depth >= MAX_DEPTH {
            return Err(Error::Schema(format!(
                "nesting deeper than {MAX_DEPTH}: setting `{path}` would put a value inside \
                 {depth} containers"
            )));
        }
        self.set_path_unchecked(path, v);
        Ok(())
    }

    /// Containers on the longest path from this value down to a leaf: a scalar
    /// is 0, an empty container 1.
    pub fn depth(&self) -> usize {
        match self {
            Value::Array(a) => 1 + a.iter().map(Value::depth).max().unwrap_or(0),
            Value::Object(o) => 1 + o.iter().map(|(_, v)| v.depth()).max().unwrap_or(0),
            _ => 0,
        }
    }

    fn set_path_unchecked(&mut self, path: &str, v: Value) {
        let (head, rest) = match path.split_once('.') {
            Some((h, r)) => (h, Some(r)),
            None => (path, None),
        };
        if !matches!(self, Value::Object(_)) {
            *self = Value::Object(Vec::new());
        }
        let Value::Object(fields) = self else { unreachable!() };
        let idx = match fields.binary_search_by(|(k, _)| k.as_str().cmp(head)) {
            Ok(i) => i,
            Err(i) => {
                fields.insert(i, (head.to_string(), Value::Null));
                i
            }
        };
        match rest {
            None => fields[idx].1 = v,
            Some(r) => fields[idx].1.set_path_unchecked(r, v),
        }
    }

    /// A rough in-memory footprint, used for the memtable byte threshold (§4.3).
    pub fn heap_size(&self) -> usize {
        match self {
            Value::Null
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Timestamp(_) => 16,
            Value::Str(s) => 24 + s.len(),
            Value::Array(a) => 24 + a.iter().map(|v| v.heap_size()).sum::<usize>(),
            Value::Object(o) => {
                24 + o.iter().map(|(k, v)| 24 + k.len() + v.heap_size()).sum::<usize>()
            }
        }
    }

    /// Walk every leaf under this value, yielding `(dotted_path, leaf)`.
    /// Arrays report the array itself and then recurse into elements under the
    /// same path, which is what the catalog wants: `tags` is one path whose
    /// type is Array, and its element types are tracked separately.
    pub fn walk_paths(&self, prefix: &str, out: &mut Vec<(String, ValueType)>) {
        if let Value::Object(fields) = self {
            for (k, v) in fields {
                let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                out.push((p.clone(), v.ty()));
                if matches!(v, Value::Object(_)) {
                    v.walk_paths(&p, out);
                }
            }
        }
    }
}

/// Comparison used by predicates. Returns `None` (SQL NULL) when the two sides
/// are of different concrete types — the polymorphic rule from §2.1. Numbers
/// widen; timestamps compare with numbers only when the literal was declared or
/// cast to a timestamp, which the planner has already arranged.
pub fn compare_typed(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Str(x), Value::Str(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Int(_), Value::Float(_))
        | (Value::Float(_), Value::Int(_))
        | (Value::Float(_), Value::Float(_)) => {
            let (x, y) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            x.partial_cmp(&y)
        }
        (Value::Array(x), Value::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                match compare_typed(xi, yi)? {
                    Ordering::Equal => continue,
                    o => return Some(o),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        (Value::Object(x), Value::Object(y)) => {
            // Fields are normalised to sorted-by-key, so walking the two field
            // lists in step is a lexicographic comparison of the whole object.
            for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
                match kx.cmp(ky) {
                    Ordering::Equal => {}
                    o => return Some(o),
                }
                match compare_typed(vx, vy)? {
                    Ordering::Equal => continue,
                    o => return Some(o),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        _ => None,
    }
}

/// A total order over values, used only where determinism matters more than
/// semantics (sorting keys, tie-breaks). Unlike `compare_typed` this never
/// returns NULL: it orders by type rank first.
///
/// It deliberately does not just fall back on `compare_typed(..).unwrap_or(Equal)`.
/// Every case where `compare_typed` declines to answer — NaN, and any composite
/// holding values of differing types — would then read as "equal", which both
/// breaks structural equality (`{"a":1} == {"b":2}`) and hands `sort_by` a
/// comparator that is not a total order.
pub fn compare_total(a: &Value, b: &Value) -> Ordering {
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2,
            Value::Timestamp(_) => 3,
            Value::Str(_) => 4,
            Value::Array(_) => 5,
            Value::Object(_) => 6,
        }
    }
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(_), Value::Float(_))
        | (Value::Float(_), Value::Int(_))
        | (Value::Float(_), Value::Float(_)) => {
            let (x, y) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            if x.is_nan() || y.is_nan() {
                // A NaN is the only thing `partial_cmp` refuses to place, and
                // "unordered" read as Equal is what made `Float(NAN)` equal to
                // `Float(1.0)`. Seat every NaN after every real number and
                // equal to another NaN. `total_cmp` would do this too, but it
                // would also split `-0.0` from `0.0`.
                x.is_nan().cmp(&y.is_nan())
            } else {
                // Neither side is NaN, so the fallback is unreachable.
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                match compare_total(xi, yi) {
                    Ordering::Equal => continue,
                    o => return o,
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
                match kx.cmp(ky) {
                    Ordering::Equal => {}
                    o => return o,
                }
                match compare_total(vx, vy) {
                    Ordering::Equal => continue,
                    o => return o,
                }
            }
            x.len().cmp(&y.len())
        }
        // Null, Bool, Str and Timestamp against their own rank: `compare_typed`
        // is total there, and two Nulls are equal.
        _ => compare_typed(a, b).unwrap_or(Ordering::Equal),
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        compare_total(self, other) == Ordering::Equal
    }
}
impl Eq for Value {}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::json::to_string(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(fields: &[(&str, Value)]) -> Value {
        Value::obj(fields.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
    }

    /// The bound `json::parse` applies is the bound `set_path` applies, and
    /// both are the one `variant::decode` reads back: the deepest value the
    /// parser admits also round-trips through the encoder, and one level past
    /// it is refused at `set_path` rather than after the write. The value's
    /// own depth counts too, or a shallow path carrying a deep value would be
    /// the same hole one step over.
    #[test]
    fn set_path_refuses_what_the_decoder_could_not_read_back() {
        let deepest = (0..MAX_DEPTH - 1).map(|_| "a").collect::<Vec<_>>().join(".");
        let mut v = Value::Null;
        v.set_path(&deepest, Value::Int(1)).unwrap();
        assert_eq!(v.depth(), MAX_DEPTH - 1);
        let bytes = crate::variant::encode_to_vec(&v);
        let back = crate::variant::decode(&bytes, &mut 0).unwrap();
        assert_eq!(
            back.path(&deepest),
            Some(&Value::Int(1)),
            "the deepest admitted value reads back"
        );
        let rendered = crate::json::to_string(&v);
        assert_eq!(crate::json::parse(&rendered).unwrap().depth(), v.depth(), "and parses back");

        let one_more = format!("{deepest}.a");
        let mut w = Value::Null;
        let e = w.set_path(&one_more, Value::Int(1)).unwrap_err().to_string();
        assert!(e.contains("nesting deeper than"), "{e}");
        assert!(matches!(w, Value::Null), "a refusal changes nothing");
        assert!(
            crate::json::parse(&format!("{{\"x\":{}}}", rendered)).is_err(),
            "as the parser refuses it"
        );

        let mut deep = Value::Null;
        let mut nested = Value::Int(1);
        for _ in 0..MAX_DEPTH - 2 {
            nested = Value::Array(vec![nested]);
        }
        assert_eq!(nested.depth(), MAX_DEPTH - 2);
        deep.set_path("a", nested.clone()).unwrap();
        let e = deep.set_path("a.b", nested).unwrap_err().to_string();
        assert!(e.contains("nesting deeper than"), "the value's own depth counts: {e}");
    }

    #[test]
    fn a_repeated_key_takes_its_last_value() {
        let v = Value::obj(vec![("a".into(), Value::Int(1)), ("a".into(), Value::Int(2))]);
        assert_eq!(v.get("a"), Some(&Value::Int(2)));
        let v = Value::obj(vec![
            ("b".into(), Value::Int(9)),
            ("a".into(), Value::Int(1)),
            ("a".into(), Value::Int(2)),
            ("a".into(), Value::Int(3)),
        ]);
        assert_eq!(v.get("a"), Some(&Value::Int(3)));
        assert_eq!(v.get("b"), Some(&Value::Int(9)));
        // Every parsed object is built through `obj`, so this is what makes a
        // repeated key agree with serde_json, `JSON.parse` and Python.
        let p = crate::json::parse(r#"{"a":1,"b":0,"a":2}"#).unwrap();
        assert_eq!(p.get("a"), Some(&Value::Int(2)));
    }

    #[test]
    fn two_objects_with_different_fields_do_not_compare_equal() {
        let a = obj(&[("a", Value::Int(1))]);
        let b = obj(&[("b", Value::Int(2))]);
        assert_ne!(a, b);
        assert_eq!(compare_total(&a, &b), Ordering::Less);
        assert_eq!(compare_total(&b, &a), Ordering::Greater);
        assert_eq!(a, obj(&[("a", Value::Int(1))]));
    }

    #[test]
    fn an_object_with_an_extra_field_is_not_equal_to_its_prefix() {
        let short = obj(&[("a", Value::Int(1))]);
        let long = obj(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        assert_ne!(short, long);
        assert_eq!(compare_total(&short, &long), Ordering::Less);
    }

    #[test]
    fn nested_objects_differing_only_deep_down_are_not_equal() {
        let a = obj(&[("outer", obj(&[("inner", Value::Str("x".into()))]))]);
        let b = obj(&[("outer", obj(&[("inner", Value::Str("y".into()))]))]);
        assert_ne!(a, b);
    }

    #[test]
    fn nan_does_not_compare_equal_to_an_ordinary_number() {
        let nan = Value::Float(f64::NAN);
        assert_ne!(nan, Value::Float(1.0));
        assert_ne!(nan, Value::Int(1));
        // NaN sits after every real number and is equal only to itself, so
        // `sort_by(compare_total)` still sees a total order.
        assert_eq!(compare_total(&nan, &Value::Float(1.0)), Ordering::Greater);
        assert_eq!(compare_total(&Value::Float(1.0), &nan), Ordering::Less);
        assert_eq!(compare_total(&nan, &nan), Ordering::Equal);
        // Signed zeroes stay equal: the fix must not smuggle in `total_cmp`.
        assert_eq!(compare_total(&Value::Float(-0.0), &Value::Float(0.0)), Ordering::Equal);
    }

    #[test]
    fn values_of_different_types_inside_an_array_do_not_collapse_to_equal() {
        let a = Value::Array(vec![Value::Int(1)]);
        let b = Value::Array(vec![Value::Str("x".into())]);
        assert_ne!(a, b);
        assert_eq!(compare_total(&a, &b), Ordering::Less);
    }

    #[test]
    fn compare_typed_orders_objects_instead_of_answering_null() {
        let a = obj(&[("a", Value::Int(1))]);
        let b = obj(&[("a", Value::Int(2))]);
        assert_eq!(compare_typed(&a, &b), Some(Ordering::Less));
        assert_eq!(compare_typed(&a, &a), Some(Ordering::Equal));
        // A type mismatch under the same key is still SQL NULL, as it is for
        // arrays.
        let c = obj(&[("a", Value::Str("1".into()))]);
        assert_eq!(compare_typed(&a, &c), None);
    }
}
