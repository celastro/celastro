//! The document value model.
//!
//! Documents are self-describing JSON-like values (§2.1). The engine
//! never coerces silently: comparison against a typed literal only evaluates
//! against values of the matching type, everything else is NULL. That is the
//! Postgres `jsonb` behaviour and it is what keeps polymorphic paths from
//! producing surprises.

use std::cmp::Ordering;
use std::fmt;

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
        f.dedup_by(|a, b| a.0 == b.0);
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

    pub fn set_path(&mut self, path: &str, v: Value) {
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
            Some(r) => fields[idx].1.set_path(r, v),
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
        _ => None,
    }
}

/// A total order over values, used only where determinism matters more than
/// semantics (sorting keys, tie-breaks). Unlike `compare_typed` this never
/// returns NULL: it orders by type rank first.
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
    compare_typed(a, b).unwrap_or(Ordering::Equal)
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
