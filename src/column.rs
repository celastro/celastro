//! Shredded columns: typed, compressed, with zone maps and bloom filters.
//!
//! Shredding is a **physical, per-segment decision, not a schema commitment**
//! (§2.1). Each segment's footer records which paths it stored as columns; the
//! planner plans against logical paths and each segment reader chooses column
//! access or variant decode locally. A segment written before a path was
//! promoted still answers the query, just slower, and no migration is ever
//! required for a promotion.
//!
//! Everything here produces a [`Bitmap`] in the segment's ordinal space, which
//! is what lets a structured predicate meet a posting list and a vector result
//! without a join (§4.2).

use std::collections::BTreeMap;

use crate::bitmap::Bitmap;
use crate::codec::*;
use crate::error::{Error, Result};
use crate::value::{compare_typed, Value, ValueType};

/// Documents per zone-map entry. Small enough that a zone is a useful skip
/// unit, large enough that the maps stay a rounding error on segment size.
pub const ZONE_BLOCK: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
    /// `path IN (...)`; the literal is an array.
    In,
    /// `ANY(tags) = 'x'` and containment over a scalar array (§2.2).
    ArrayContains,
    /// `LIKE 'x%'`.
    Prefix,
}

impl CmpOp {
    pub fn name(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
            CmpOp::IsNull => "IS NULL",
            CmpOp::IsNotNull => "IS NOT NULL",
            CmpOp::In => "IN",
            CmpOp::ArrayContains => "@>",
            CmpOp::Prefix => "LIKE",
        }
    }
}

/// A tiny bloom filter, ~10 bits per value, 6 probes. Its only job is to let a
/// point lookup skip a segment without touching the column.
#[derive(Debug, Clone, Default)]
pub struct Bloom {
    bits: Vec<u64>,
    nbits: usize,
}

const BLOOM_PROBES: u32 = 6;

impl Bloom {
    pub fn with_capacity(n: usize) -> Bloom {
        // At least one whole word. Ten bits for a single value rounds up to 16,
        // which is `nbits / 64 == 0` words — a filter that claims 16 bits and
        // owns none, and indexes straight out of bounds on the first insert.
        let nbits = (n.max(1) * 10).next_power_of_two().max(64);
        Bloom { bits: vec![0; nbits / 64], nbits }
    }

    fn probes(&self, h: u64) -> impl Iterator<Item = usize> + '_ {
        let (h1, h2) = ((h >> 32) as u32 as u64, h as u32 as u64 | 1);
        let nbits = self.nbits as u64;
        (0..BLOOM_PROBES as u64)
            .map(move |i| ((h1.wrapping_add(i.wrapping_mul(h2))) % nbits) as usize)
    }

    pub fn add(&mut self, key: &[u8]) {
        if self.nbits == 0 {
            return;
        }
        let h = fnv1a(key);
        for p in self.probes(h).collect::<Vec<_>>() {
            self.bits[p / 64] |= 1 << (p % 64);
        }
    }

    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        if self.nbits == 0 {
            return true;
        }
        let h = fnv1a(key);
        self.probes(h).all(|p| (self.bits[p / 64] >> (p % 64)) & 1 == 1)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_uvarint(out, self.nbits as u64);
        for w in &self.bits {
            put_u64(out, *w);
        }
    }

    fn decode(b: &[u8], i: &mut usize) -> Option<Bloom> {
        let nbits = get_uvarint(b, i)? as usize;
        let mut bits = vec![0u64; nbits / 64];
        for w in bits.iter_mut() {
            *w = get_u64(b, i)?;
        }
        Some(Bloom { bits, nbits })
    }
}

/// Per-block min/max. Ordered by the same rules the predicate uses, so a zone
/// skip can never drop a matching document.
#[derive(Debug, Clone)]
pub enum Zones {
    None,
    Num(Vec<(f64, f64)>),
    Ts(Vec<(i64, i64)>),
    Str(Vec<(String, String)>),
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    Bool {
        present: Bitmap,
        vals: Bitmap,
    },
    Num {
        present: Bitmap,
        vals: Vec<f64>,
    },
    Ts {
        present: Bitmap,
        vals: Vec<i64>,
    },
    Str {
        present: Bitmap,
        offsets: Vec<u32>,
        data: Vec<u8>,
    },
    /// A scalar array stored as a multi-value column with offsets, plus a
    /// per-segment value→bitmap index (§2.2). `ANY(tags) = 'x'` and containment
    /// resolve to a bitmap lookup, not a scan.
    MultiStr {
        present: Bitmap,
        starts: Vec<u32>,
        offsets: Vec<u32>,
        data: Vec<u8>,
        index: BTreeMap<String, Bitmap>,
    },
    MultiNum {
        present: Bitmap,
        starts: Vec<u32>,
        vals: Vec<f64>,
    },
}

#[derive(Debug, Clone)]
pub struct Column {
    pub path: String,
    pub ty: ValueType,
    pub num_docs: usize,
    pub data: ColumnData,
    pub zones: Zones,
    pub bloom: Bloom,
    /// Ordinals holding a non-null value that is **not** of this column's type.
    ///
    /// A path is promoted to a column because it is *effectively* always one
    /// type; "effectively" leaves a handful of documents behind, and they must
    /// not be silently coerced into the column's zero value. They are excluded
    /// from `present` and listed here, and the reader answers them from the
    /// variant blob instead — the same fallback a segment uses for a path it
    /// never shredded at all. A column is an optimisation, so it is allowed to
    /// decline part of its job as long as it says which part.
    pub mismatch: Bitmap,
}

impl Column {
    fn present(&self) -> &Bitmap {
        match &self.data {
            ColumnData::Bool { present, .. }
            | ColumnData::Num { present, .. }
            | ColumnData::Ts { present, .. }
            | ColumnData::Str { present, .. }
            | ColumnData::MultiStr { present, .. }
            | ColumnData::MultiNum { present, .. } => present,
        }
    }

    pub fn is_multi(&self) -> bool {
        matches!(self.data, ColumnData::MultiStr { .. } | ColumnData::MultiNum { .. })
    }

    pub fn get(&self, ord: u32) -> Value {
        let i = ord as usize;
        if !self.present().get(i) {
            return Value::Null;
        }
        match &self.data {
            ColumnData::Bool { vals, .. } => Value::Bool(vals.get(i)),
            ColumnData::Num { vals, .. } => Value::Float(vals[i]),
            ColumnData::Ts { vals, .. } => Value::Timestamp(vals[i]),
            ColumnData::Str { offsets, data, .. } => {
                let (a, b) = (offsets[i] as usize, offsets[i + 1] as usize);
                Value::Str(String::from_utf8_lossy(&data[a..b]).into_owned())
            }
            ColumnData::MultiStr { starts, offsets, data, .. } => {
                let (s, e) = (starts[i] as usize, starts[i + 1] as usize);
                Value::Array(
                    (s..e)
                        .map(|j| {
                            let (a, b) = (offsets[j] as usize, offsets[j + 1] as usize);
                            Value::Str(String::from_utf8_lossy(&data[a..b]).into_owned())
                        })
                        .collect(),
                )
            }
            ColumnData::MultiNum { starts, vals, .. } => {
                let (s, e) = (starts[i] as usize, starts[i + 1] as usize);
                Value::Array((s..e).map(|j| Value::Float(vals[j])).collect())
            }
        }
    }

    /// Blocks this predicate cannot possibly match, from the zone maps. Returns
    /// `None` when zone maps do not apply, meaning "check everything".
    fn zone_skip(&self, op: CmpOp, lit: &Value) -> Option<Vec<bool>> {
        let nblocks = self.num_docs.div_ceil(ZONE_BLOCK);
        let mut keep = vec![true; nblocks];
        match (&self.zones, op) {
            (Zones::Num(z), CmpOp::Eq | CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge) => {
                let v = lit.as_f64()?;
                for (b, (lo, hi)) in z.iter().enumerate() {
                    keep[b] = match op {
                        CmpOp::Eq => *lo <= v && v <= *hi,
                        CmpOp::Lt => *lo < v,
                        CmpOp::Le => *lo <= v,
                        CmpOp::Gt => *hi > v,
                        CmpOp::Ge => *hi >= v,
                        _ => true,
                    };
                }
            }
            (Zones::Ts(z), CmpOp::Eq | CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge) => {
                let v = lit.as_i64()?;
                for (b, (lo, hi)) in z.iter().enumerate() {
                    keep[b] = match op {
                        CmpOp::Eq => *lo <= v && v <= *hi,
                        CmpOp::Lt => *lo < v,
                        CmpOp::Le => *lo <= v,
                        CmpOp::Gt => *hi > v,
                        CmpOp::Ge => *hi >= v,
                        _ => true,
                    };
                }
            }
            (
                Zones::Str(z),
                CmpOp::Eq | CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge | CmpOp::Prefix,
            ) => {
                let v = lit.as_str()?;
                for (b, (lo, hi)) in z.iter().enumerate() {
                    keep[b] = match op {
                        CmpOp::Eq => lo.as_str() <= v && v <= hi.as_str(),
                        // A prefix range is [v, v+1) in string order, so the
                        // block matters iff it straddles that range.
                        CmpOp::Prefix => {
                            hi.as_str() >= v && lo.as_str() <= &format!("{v}\u{10FFFF}")
                        }
                        CmpOp::Lt => lo.as_str() < v,
                        CmpOp::Le => lo.as_str() <= v,
                        CmpOp::Gt => hi.as_str() > v,
                        CmpOp::Ge => hi.as_str() >= v,
                        _ => true,
                    };
                }
            }
            _ => return None,
        }
        Some(keep)
    }

    /// Ordinals where this predicate is *defined* — the column half of
    /// [`comparable`]. `NOT p` and `<>` are `comparable ∖ p`.
    pub fn comparable_set(&self, op: CmpOp, lit: &Value) -> Bitmap {
        match op {
            CmpOp::IsNull | CmpOp::IsNotNull => Bitmap::all(self.num_docs),
            _ => {
                if self.type_admits(op, lit) {
                    // A multi-value row with no elements has nothing to compare.
                    match &self.data {
                        ColumnData::MultiStr { present, starts, .. }
                        | ColumnData::MultiNum { present, starts, .. } => {
                            let mut bm = present.clone();
                            for i in present.iter() {
                                if starts[i as usize] == starts[i as usize + 1] {
                                    bm.clear(i as usize);
                                }
                            }
                            bm
                        }
                        _ => self.present().clone(),
                    }
                } else {
                    Bitmap::new(self.num_docs)
                }
            }
        }
    }

    /// Can this column's stored type be compared with this literal at all?
    fn type_admits(&self, op: CmpOp, lit: &Value) -> bool {
        let probe = match &self.data {
            ColumnData::Bool { .. } => Value::Bool(true),
            ColumnData::Num { .. } | ColumnData::MultiNum { .. } => Value::Float(0.0),
            ColumnData::Ts { .. } => Value::Timestamp(0),
            ColumnData::Str { .. } | ColumnData::MultiStr { .. } => Value::Str(String::new()),
        };
        comparable(&probe, op, lit)
    }

    /// Evaluate a predicate into the ordinal space.
    ///
    /// Polymorphic semantics (§2.1) fall out of the representation: a column
    /// holds exactly one concrete type, so a literal of another type matches
    /// nothing — every comparison evaluates to NULL, and NULL is not true.
    ///
    /// The result covers only the ordinals this column actually stores; the
    /// caller must union in [`filter_variant`] over [`Column::mismatch`].
    pub fn filter(&self, op: CmpOp, lit: &Value) -> Bitmap {
        let n = self.num_docs;
        let mut out = Bitmap::new(n);
        match op {
            CmpOp::IsNull => {
                // A mismatched ordinal holds a value — just not one this column
                // can store. It is emphatically not NULL, so it is left to the
                // caller's variant pass rather than answered here.
                out = self.present().negate();
                out.andnot_inplace(&self.mismatch);
                return out;
            }
            CmpOp::IsNotNull => {
                // Likewise: `present` only, and the variant pass adds the
                // mismatched ordinals back as non-null.
                return self.present().clone();
            }
            CmpOp::In => {
                if let Value::Array(items) = lit {
                    for item in items {
                        out.or_inplace(&self.filter(CmpOp::Eq, item));
                    }
                }
                return out;
            }
            CmpOp::Ne => {
                // `<>` is the negation of `=` over the ordinals where `=` is
                // *defined*. Using `present` here instead would make a
                // type-mismatched literal match every row in the column.
                let eq = self.filter(CmpOp::Eq, lit);
                let mut r = self.comparable_set(CmpOp::Eq, lit);
                r.andnot_inplace(&eq);
                return r;
            }
            _ => {}
        }

        // Bloom rejection for point lookups: one hash instead of a scan.
        if op == CmpOp::Eq && !self.is_multi() {
            let key = bloom_key(lit);
            if let Some(k) = key.as_ref() {
                if !self.bloom.maybe_contains(k) {
                    return out;
                }
            }
        }
        if op == CmpOp::ArrayContains && !self.is_multi() {
            // A scalar is a one-element array, so containment is equality.
            return self.filter(CmpOp::Eq, lit);
        }
        if op == CmpOp::ArrayContains || (op == CmpOp::Eq && self.is_multi()) {
            if let ColumnData::MultiStr { index, .. } = &self.data {
                // Value → bitmap: the whole point of the multi-value index.
                if let Some(s) = lit.as_str() {
                    if let Some(bm) = index.get(s) {
                        let mut r = bm.clone();
                        // Index bitmaps are built at segment width already.
                        r.and_inplace(&Bitmap::all(n));
                        return r;
                    }
                }
                return out;
            }
        }

        let keep = self.zone_skip(op, lit);
        let block_live = |ord: usize| keep.as_ref().map(|k| k[ord / ZONE_BLOCK]).unwrap_or(true);

        match &self.data {
            ColumnData::Bool { present, vals } => {
                let Some(want) = lit.as_bool() else { return out };
                for i in present.iter() {
                    let v = vals.get(i as usize);
                    let ok = match op {
                        CmpOp::Eq => v == want,
                        CmpOp::Lt => !v & want,
                        CmpOp::Le => !v | (v == want),
                        CmpOp::Gt => v & !want,
                        CmpOp::Ge => v | (v == want),
                        _ => false,
                    };
                    if ok {
                        out.set(i as usize);
                    }
                }
            }
            ColumnData::Num { present, vals } => {
                let Some(want) = lit.as_f64() else { return out };
                if !matches!(lit, Value::Int(_) | Value::Float(_)) {
                    return out;
                }
                for i in present.iter() {
                    if !block_live(i as usize) {
                        continue;
                    }
                    if cmp_ok(op, vals[i as usize].partial_cmp(&want)) {
                        out.set(i as usize);
                    }
                }
            }
            ColumnData::Ts { present, vals } => {
                let want = match lit {
                    Value::Timestamp(t) => *t,
                    // A timestamp column compared to a string is only meaningful
                    // if the planner already cast it; it did not, so: NULL.
                    _ => return out,
                };
                for i in present.iter() {
                    if !block_live(i as usize) {
                        continue;
                    }
                    if cmp_ok(op, Some(vals[i as usize].cmp(&want))) {
                        out.set(i as usize);
                    }
                }
            }
            ColumnData::Str { present, offsets, data } => {
                let Some(want) = lit.as_str() else { return out };
                for i in present.iter() {
                    if !block_live(i as usize) {
                        continue;
                    }
                    let (a, b) = (offsets[i as usize] as usize, offsets[i as usize + 1] as usize);
                    let s = &data[a..b];
                    let ok = if op == CmpOp::Prefix {
                        s.starts_with(want.as_bytes())
                    } else {
                        cmp_ok(op, Some(s.cmp(want.as_bytes())))
                    };
                    if ok {
                        out.set(i as usize);
                    }
                }
            }
            ColumnData::MultiStr { present, starts, offsets, data, .. } => {
                let Some(want) = lit.as_str() else { return out };
                for i in present.iter() {
                    let (s, e) = (starts[i as usize] as usize, starts[i as usize + 1] as usize);
                    let hit = (s..e).any(|j| {
                        let (a, b) = (offsets[j] as usize, offsets[j + 1] as usize);
                        let v = &data[a..b];
                        if op == CmpOp::Prefix {
                            v.starts_with(want.as_bytes())
                        } else {
                            cmp_ok(op, Some(v.cmp(want.as_bytes())))
                        }
                    });
                    if hit {
                        out.set(i as usize);
                    }
                }
            }
            ColumnData::MultiNum { present, starts, vals } => {
                let Some(want) = lit.as_f64() else { return out };
                for i in present.iter() {
                    let (s, e) = (starts[i as usize] as usize, starts[i as usize + 1] as usize);
                    if (s..e).any(|j| cmp_ok(op, vals[j].partial_cmp(&want))) {
                        out.set(i as usize);
                    }
                }
            }
        }
        out
    }
}

fn cmp_ok(op: CmpOp, o: Option<std::cmp::Ordering>) -> bool {
    use std::cmp::Ordering::*;
    let Some(o) = o else { return false };
    match op {
        CmpOp::Eq | CmpOp::ArrayContains => o == Equal,
        CmpOp::Lt => o == Less,
        CmpOp::Le => o != Greater,
        CmpOp::Gt => o == Greater,
        CmpOp::Ge => o != Less,
        _ => false,
    }
}

fn bloom_key(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Str(s) => Some(s.as_bytes().to_vec()),
        Value::Int(_) | Value::Float(_) => Some(v.as_f64().unwrap().to_le_bytes().to_vec()),
        Value::Timestamp(t) => Some(t.to_le_bytes().to_vec()),
        Value::Bool(b) => Some(vec![*b as u8]),
        _ => None,
    }
}

// --------------------------------------------------------------------------
// Building
// --------------------------------------------------------------------------

pub struct ColumnBuilder {
    path: String,
    ty: ValueType,
    values: Vec<Option<Value>>,
}

/// Can this value be stored in a column of type `ty`?
///
/// A scalar is accepted into an array column as a one-element array, matching
/// [`matches`]. Anything else that does not fit is a mismatch, not a coercion:
/// storing a string in a numeric column as `0.0` and marking it present makes
/// `WHERE n = 0` return it.
fn fits(ty: ValueType, elem_str: bool, v: &Value) -> bool {
    match ty {
        ValueType::Bool => matches!(v, Value::Bool(_)),
        ValueType::Number => matches!(v, Value::Int(_) | Value::Float(_)),
        ValueType::Timestamp => matches!(v, Value::Timestamp(_)),
        ValueType::Str => matches!(v, Value::Str(_)),
        ValueType::Array => {
            let ok_elem = |e: &Value| {
                if elem_str {
                    matches!(e, Value::Str(_))
                } else {
                    matches!(e, Value::Int(_) | Value::Float(_))
                }
            };
            match v {
                Value::Array(a) => a.iter().all(ok_elem),
                other => ok_elem(other),
            }
        }
        _ => false,
    }
}

/// Borrow a value as a slice of elements, treating a scalar as one element.
fn as_elements(v: &Value) -> &[Value] {
    match v {
        Value::Array(a) => a.as_slice(),
        other => std::slice::from_ref(other),
    }
}

impl ColumnBuilder {
    pub fn new(path: &str, ty: ValueType) -> ColumnBuilder {
        ColumnBuilder { path: path.to_string(), ty, values: Vec::new() }
    }

    pub fn push(&mut self, ord: u32, v: Option<Value>) {
        while self.values.len() < ord as usize {
            self.values.push(None);
        }
        self.values.push(v);
    }

    pub fn finish(mut self, num_docs: usize) -> Column {
        while self.values.len() < num_docs {
            self.values.push(None);
        }

        // The layout follows the column's declared or inferred type, not
        // whatever the first document happened to contain. One array-valued
        // document must not flip a text column to multi-value and drop every
        // scalar in it.
        let elem_str = if self.ty == ValueType::Array {
            let mut n_str = 0usize;
            let mut n_num = 0usize;
            for v in self.values.iter().flatten() {
                for e in as_elements(v) {
                    match e {
                        Value::Str(_) => n_str += 1,
                        Value::Int(_) | Value::Float(_) => n_num += 1,
                        _ => {}
                    }
                }
            }
            n_str >= n_num
        } else {
            false
        };

        let mut present = Bitmap::new(num_docs);
        let mut mismatch = Bitmap::new(num_docs);
        for (i, v) in self.values.iter().enumerate() {
            match v {
                None => {}
                Some(x) if x.is_null() => {}
                Some(x) if fits(self.ty, elem_str, x) => present.set(i),
                Some(_) => mismatch.set(i),
            }
        }

        let stored = |i: usize| -> Option<&Value> {
            if present.get(i) {
                self.values[i].as_ref()
            } else {
                None
            }
        };

        let mut bloom = Bloom::with_capacity(present.popcount());
        let nblocks = num_docs.div_ceil(ZONE_BLOCK);

        let (data, zones) = match self.ty {
            ValueType::Array if elem_str => {
                let mut starts = Vec::with_capacity(num_docs + 1);
                let mut offsets = vec![0u32];
                let mut data = Vec::new();
                let mut index: BTreeMap<String, Bitmap> = BTreeMap::new();
                for i in 0..num_docs {
                    starts.push(offsets.len() as u32 - 1);
                    if let Some(v) = stored(i) {
                        for it in as_elements(v) {
                            if let Some(s) = it.as_str() {
                                data.extend_from_slice(s.as_bytes());
                                offsets.push(data.len() as u32);
                                bloom.add(s.as_bytes());
                                index
                                    .entry(s.to_string())
                                    .or_insert_with(|| Bitmap::new(num_docs))
                                    .set(i);
                            }
                        }
                    }
                }
                starts.push(offsets.len() as u32 - 1);
                (ColumnData::MultiStr { present, starts, offsets, data, index }, Zones::None)
            }
            ValueType::Array => {
                let mut starts = Vec::with_capacity(num_docs + 1);
                let mut vals = Vec::new();
                for i in 0..num_docs {
                    starts.push(vals.len() as u32);
                    if let Some(v) = stored(i) {
                        for it in as_elements(v) {
                            if let Some(f) = it.as_f64() {
                                vals.push(f);
                            }
                        }
                    }
                }
                starts.push(vals.len() as u32);
                (ColumnData::MultiNum { present, starts, vals }, Zones::None)
            }
            ValueType::Bool => {
                let mut vals = Bitmap::new(num_docs);
                for i in 0..num_docs {
                    if let Some(Value::Bool(b)) = stored(i) {
                        if *b {
                            vals.set(i);
                        }
                        bloom.add(&[*b as u8]);
                    }
                }
                (ColumnData::Bool { present, vals }, Zones::None)
            }
            ValueType::Timestamp => {
                let mut vals = vec![0i64; num_docs];
                let mut z = vec![(i64::MAX, i64::MIN); nblocks];
                for i in 0..num_docs {
                    if let Some(x) = stored(i).and_then(|x| x.as_i64()) {
                        vals[i] = x;
                        let b = i / ZONE_BLOCK;
                        z[b].0 = z[b].0.min(x);
                        z[b].1 = z[b].1.max(x);
                        bloom.add(&x.to_le_bytes());
                    }
                }
                normalise_zones_i64(&mut z);
                (ColumnData::Ts { present, vals }, Zones::Ts(z))
            }
            ValueType::Str => {
                let mut offsets = vec![0u32; num_docs + 1];
                let mut data = Vec::new();
                let mut z = vec![(String::new(), String::new()); nblocks];
                let mut seen = vec![false; nblocks];
                for i in 0..num_docs {
                    if let Some(s) = stored(i).and_then(|x| x.as_str()) {
                        data.extend_from_slice(s.as_bytes());
                        bloom.add(s.as_bytes());
                        let b = i / ZONE_BLOCK;
                        if !seen[b] {
                            z[b] = (s.to_string(), s.to_string());
                            seen[b] = true;
                        } else {
                            if s < z[b].0.as_str() {
                                z[b].0 = s.to_string();
                            }
                            if s > z[b].1.as_str() {
                                z[b].1 = s.to_string();
                            }
                        }
                    }
                    offsets[i + 1] = data.len() as u32;
                }
                for b in 0..nblocks {
                    if !seen[b] {
                        // An all-absent block must not skip anything, and must
                        // not claim a range either.
                        z[b] = (String::new(), "\u{10FFFF}".to_string());
                    }
                }
                (ColumnData::Str { present, offsets, data }, Zones::Str(z))
            }
            _ => {
                let mut vals = vec![0f64; num_docs];
                let mut z = vec![(f64::INFINITY, f64::NEG_INFINITY); nblocks];
                for i in 0..num_docs {
                    if let Some(x) = stored(i).and_then(|x| x.as_f64()) {
                        vals[i] = x;
                        let b = i / ZONE_BLOCK;
                        z[b].0 = z[b].0.min(x);
                        z[b].1 = z[b].1.max(x);
                        bloom.add(&x.to_le_bytes());
                    }
                }
                for e in z.iter_mut() {
                    if e.0 > e.1 {
                        *e = (f64::NEG_INFINITY, f64::INFINITY);
                    }
                }
                (ColumnData::Num { present, vals }, Zones::Num(z))
            }
        };

        Column { path: self.path, ty: self.ty, num_docs, data, zones, bloom, mismatch }
    }
}

fn normalise_zones_i64(z: &mut [(i64, i64)]) {
    for e in z.iter_mut() {
        if e.0 > e.1 {
            *e = (i64::MIN, i64::MAX);
        }
    }
}

// --------------------------------------------------------------------------
// Encoding
// --------------------------------------------------------------------------

fn put_bitmap(out: &mut Vec<u8>, bm: &Bitmap) {
    put_uvarint(out, bm.len() as u64);
    let v = bm.to_vec();
    put_uvarint(out, v.len() as u64);
    let mut prev = 0u32;
    for o in v {
        put_uvarint(out, (o - prev) as u64);
        prev = o;
    }
}

fn get_bitmap(b: &[u8], i: &mut usize) -> Option<Bitmap> {
    let len = get_uvarint(b, i)? as usize;
    let n = get_uvarint(b, i)? as usize;
    let mut bm = Bitmap::new(len);
    let mut prev = 0u32;
    for _ in 0..n {
        prev += get_uvarint(b, i)? as u32;
        bm.set(prev as usize);
    }
    Some(bm)
}

impl Column {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_str(&mut out, &self.path);
        out.push(self.ty as u8);
        put_uvarint(&mut out, self.num_docs as u64);
        match &self.data {
            ColumnData::Bool { present, vals } => {
                out.push(0);
                put_bitmap(&mut out, present);
                put_bitmap(&mut out, vals);
            }
            ColumnData::Num { present, vals } => {
                out.push(1);
                put_bitmap(&mut out, present);
                for v in vals {
                    put_u64(&mut out, v.to_bits());
                }
            }
            ColumnData::Ts { present, vals } => {
                out.push(2);
                put_bitmap(&mut out, present);
                for v in vals {
                    put_ivarint(&mut out, *v);
                }
            }
            ColumnData::Str { present, offsets, data } => {
                out.push(3);
                put_bitmap(&mut out, present);
                put_uvarint(&mut out, offsets.len() as u64);
                let mut prev = 0u32;
                for o in offsets {
                    put_uvarint(&mut out, (o - prev) as u64);
                    prev = *o;
                }
                put_bytes(&mut out, data);
            }
            ColumnData::MultiStr { present, starts, offsets, data, index } => {
                out.push(4);
                put_bitmap(&mut out, present);
                put_uvarint(&mut out, starts.len() as u64);
                for s in starts {
                    put_u32(&mut out, *s);
                }
                put_uvarint(&mut out, offsets.len() as u64);
                for o in offsets {
                    put_u32(&mut out, *o);
                }
                put_bytes(&mut out, data);
                put_uvarint(&mut out, index.len() as u64);
                for (k, bm) in index {
                    put_str(&mut out, k);
                    put_bitmap(&mut out, bm);
                }
            }
            ColumnData::MultiNum { present, starts, vals } => {
                out.push(5);
                put_bitmap(&mut out, present);
                put_uvarint(&mut out, starts.len() as u64);
                for s in starts {
                    put_u32(&mut out, *s);
                }
                put_uvarint(&mut out, vals.len() as u64);
                for v in vals {
                    put_u64(&mut out, v.to_bits());
                }
            }
        }
        match &self.zones {
            Zones::None => out.push(0),
            Zones::Num(z) => {
                out.push(1);
                put_uvarint(&mut out, z.len() as u64);
                for (a, b) in z {
                    put_u64(&mut out, a.to_bits());
                    put_u64(&mut out, b.to_bits());
                }
            }
            Zones::Ts(z) => {
                out.push(2);
                put_uvarint(&mut out, z.len() as u64);
                for (a, b) in z {
                    put_ivarint(&mut out, *a);
                    put_ivarint(&mut out, *b);
                }
            }
            Zones::Str(z) => {
                out.push(3);
                put_uvarint(&mut out, z.len() as u64);
                for (a, b) in z {
                    put_str(&mut out, a);
                    put_str(&mut out, b);
                }
            }
        }
        self.bloom.encode(&mut out);
        put_bitmap(&mut out, &self.mismatch);
        out
    }

    pub fn decode(b: &[u8]) -> Result<Column> {
        let bad = || Error::Storage("column: truncated".into());
        let mut i = 0usize;
        let path = get_str(b, &mut i).ok_or_else(bad)?;
        let ty = match *b.get(i).ok_or_else(bad)? {
            0 => ValueType::Null,
            1 => ValueType::Bool,
            2 => ValueType::Number,
            3 => ValueType::Str,
            4 => ValueType::Timestamp,
            5 => ValueType::Array,
            _ => ValueType::Object,
        };
        i += 1;
        let num_docs = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
        let tag = *b.get(i).ok_or_else(bad)?;
        i += 1;
        let data = match tag {
            0 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let vals = get_bitmap(b, &mut i).ok_or_else(bad)?;
                ColumnData::Bool { present, vals }
            }
            1 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let mut vals = Vec::with_capacity(num_docs);
                for _ in 0..num_docs {
                    vals.push(f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?));
                }
                ColumnData::Num { present, vals }
            }
            2 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let mut vals = Vec::with_capacity(num_docs);
                for _ in 0..num_docs {
                    vals.push(get_ivarint(b, &mut i).ok_or_else(bad)?);
                }
                ColumnData::Ts { present, vals }
            }
            3 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut offsets = Vec::with_capacity(n);
                let mut prev = 0u32;
                for _ in 0..n {
                    prev += get_uvarint(b, &mut i).ok_or_else(bad)? as u32;
                    offsets.push(prev);
                }
                let data = get_bytes(b, &mut i).ok_or_else(bad)?.to_vec();
                ColumnData::Str { present, offsets, data }
            }
            4 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let ns = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut starts = Vec::with_capacity(ns);
                for _ in 0..ns {
                    starts.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let no = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut offsets = Vec::with_capacity(no);
                for _ in 0..no {
                    offsets.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let data = get_bytes(b, &mut i).ok_or_else(bad)?.to_vec();
                let ni = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut index = BTreeMap::new();
                for _ in 0..ni {
                    let k = get_str(b, &mut i).ok_or_else(bad)?;
                    let bm = get_bitmap(b, &mut i).ok_or_else(bad)?;
                    index.insert(k, bm);
                }
                ColumnData::MultiStr { present, starts, offsets, data, index }
            }
            _ => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let ns = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut starts = Vec::with_capacity(ns);
                for _ in 0..ns {
                    starts.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let nv = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut vals = Vec::with_capacity(nv);
                for _ in 0..nv {
                    vals.push(f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?));
                }
                ColumnData::MultiNum { present, starts, vals }
            }
        };
        let ztag = *b.get(i).ok_or_else(bad)?;
        i += 1;
        let zones = match ztag {
            1 => {
                let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut z = Vec::with_capacity(n);
                for _ in 0..n {
                    let a = f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?);
                    let c = f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?);
                    z.push((a, c));
                }
                Zones::Num(z)
            }
            2 => {
                let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut z = Vec::with_capacity(n);
                for _ in 0..n {
                    let a = get_ivarint(b, &mut i).ok_or_else(bad)?;
                    let c = get_ivarint(b, &mut i).ok_or_else(bad)?;
                    z.push((a, c));
                }
                Zones::Ts(z)
            }
            3 => {
                let n = get_uvarint(b, &mut i).ok_or_else(bad)? as usize;
                let mut z = Vec::with_capacity(n);
                for _ in 0..n {
                    let a = get_str(b, &mut i).ok_or_else(bad)?;
                    let c = get_str(b, &mut i).ok_or_else(bad)?;
                    z.push((a, c));
                }
                Zones::Str(z)
            }
            _ => Zones::None,
        };
        let bloom = Bloom::decode(b, &mut i).ok_or_else(bad)?;
        let mismatch = get_bitmap(b, &mut i).ok_or_else(bad)?;
        Ok(Column { path, ty, num_docs, data, zones, bloom, mismatch })
    }
}

/// Is this value *comparable* with the literal under this operator?
///
/// SQL's three-valued logic turns on exactly this question. A value that is
/// absent, or present but of a type the literal cannot be compared with, makes
/// the predicate NULL rather than false — so it is not selected by `p`, and it
/// is **also not selected by `NOT p`**. Every place that needs a negation
/// (`<>`, `NOT (...)`) asks this first, which is what stops a type mismatch
/// from being quietly promoted to "true, but the other way round".
pub fn comparable(v: &Value, op: CmpOp, lit: &Value) -> bool {
    match op {
        CmpOp::IsNull | CmpOp::IsNotNull => true,
        _ if v.is_null() => false,
        CmpOp::In => match lit {
            Value::Array(items) => items.iter().any(|x| comparable(v, CmpOp::Eq, x)),
            _ => false,
        },
        CmpOp::Prefix => match v {
            Value::Array(a) => a.iter().any(|e| e.as_str().is_some()) && lit.as_str().is_some(),
            other => other.as_str().is_some() && lit.as_str().is_some(),
        },
        _ => match v {
            // A scalar array is compared element-wise, so it is comparable if
            // any element is.
            Value::Array(a) if !matches!(lit, Value::Array(_)) => {
                a.iter().any(|e| compare_typed(e, lit).is_some())
            }
            other => compare_typed(other, lit).is_some(),
        },
    }
}

/// Does one value satisfy `op lit`?
///
/// The single definition of predicate semantics. Column evaluation is an
/// optimisation of this function, never a redefinition of it — which is what
/// makes the "shredded or not, same answer" claim in §2.1 checkable rather
/// than aspirational, and what
/// `column::tests::column_and_variant_paths_agree` checks exhaustively.
///
/// A scalar and a one-element array are treated as the same thing throughout.
/// A path is scalar in some documents and an array in others far too often for
/// `tags = 'hot'` and `ANY(tags) = 'hot'` to answer differently depending on
/// which shape a particular document happened to use.
pub fn matches(v: &Value, op: CmpOp, lit: &Value) -> bool {
    match op {
        CmpOp::IsNull => return v.is_null(),
        CmpOp::IsNotNull => return !v.is_null(),
        _ => {}
    }
    if !comparable(v, op, lit) {
        return false;
    }
    match op {
        CmpOp::In => match lit {
            Value::Array(items) => items.iter().any(|x| matches(v, CmpOp::Eq, x)),
            _ => false,
        },
        // `NOT p` is `comparable ∧ ¬p`, and `<>` is the negation of `=`.
        CmpOp::Ne => !matches(v, CmpOp::Eq, lit),
        _ => match v {
            Value::Array(a) if !matches!(lit, Value::Array(_)) => {
                a.iter().any(|e| scalar_matches(e, op, lit))
            }
            other => scalar_matches(other, op, lit),
        },
    }
}

fn scalar_matches(v: &Value, op: CmpOp, lit: &Value) -> bool {
    if op == CmpOp::Prefix {
        return v.as_str().zip(lit.as_str()).map(|(s, p)| s.starts_with(p)).unwrap_or(false);
    }
    cmp_ok(op, compare_typed(v, lit))
}

/// Evaluate a predicate by decoding the variant blob — the fallback access path
/// when a segment did not shred the path (§2.1). Slower by design, never wrong.
/// Evaluate a predicate against the document blobs, for a path with no column.
///
/// The blob accessor returns a `Result`, and the error is propagated rather
/// than absorbed. An I/O failure — an archived segment on a node configured to
/// refuse, a segment file moved out from under a reader, a short read — must
/// not be indistinguishable from "this document does not match": that turns a
/// storage fault into a quietly shorter answer, which is the worst way for a
/// database to fail.
pub fn filter_variant(
    blobs: &dyn Fn(u32) -> Result<Option<Vec<u8>>>,
    num_docs: usize,
    path: &str,
    op: CmpOp,
    lit: &Value,
    candidates: &Bitmap,
) -> Result<Bitmap> {
    let mut out = Bitmap::new(num_docs);
    for ord in candidates.iter() {
        let Some(blob) = blobs(ord)? else { continue };
        let v = crate::variant::decode_path(&blob, path).unwrap_or(Value::Null);
        if matches(&v, op, lit) {
            out.set(ord as usize);
        }
    }
    Ok(out)
}

/// The `NOT p` companion of [`filter_variant`]: ordinals where `p` is
/// *defined* — comparable — regardless of whether it is true.
pub fn comparable_variant(
    blobs: &dyn Fn(u32) -> Result<Option<Vec<u8>>>,
    num_docs: usize,
    path: &str,
    op: CmpOp,
    lit: &Value,
    candidates: &Bitmap,
) -> Result<Bitmap> {
    let mut out = Bitmap::new(num_docs);
    for ord in candidates.iter() {
        let Some(blob) = blobs(ord)? else { continue };
        let v = crate::variant::decode_path(&blob, path).unwrap_or(Value::Null);
        if comparable(&v, op, lit) {
            out.set(ord as usize);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num_col(n: usize) -> Column {
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        for i in 0..n {
            b.push(i as u32, if i % 10 == 3 { None } else { Some(Value::Int(i as i64)) });
        }
        b.finish(n)
    }

    #[test]
    fn numeric_predicates_and_null_semantics() {
        let c = num_col(100);
        assert_eq!(c.filter(CmpOp::Eq, &Value::Int(5)).to_vec(), vec![5]);
        assert_eq!(c.filter(CmpOp::Lt, &Value::Int(4)).to_vec(), vec![0, 1, 2]);
        assert_eq!(c.filter(CmpOp::IsNull, &Value::Null).popcount(), 10);
        // `<> 5` is true only where a value is present: NULL is not <> anything.
        let ne = c.filter(CmpOp::Ne, &Value::Int(5));
        assert_eq!(ne.popcount(), 89);
        assert!(!ne.get(3));
        // A string literal against a numeric column is NULL everywhere, not an
        // error and not a coercion.
        assert!(c.filter(CmpOp::Eq, &Value::Str("5".into())).is_empty());
    }

    #[test]
    fn zone_maps_skip_blocks_without_losing_matches() {
        let n = ZONE_BLOCK * 3 + 7;
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        for i in 0..n {
            b.push(i as u32, Some(Value::Int(i as i64)));
        }
        let c = b.finish(n);
        // Everything below 5 lives in block 0; blocks 1..3 must be skippable.
        let keep = c.zone_skip(CmpOp::Lt, &Value::Int(5)).unwrap();
        assert_eq!(keep, vec![true, false, false, false]);
        assert_eq!(c.filter(CmpOp::Lt, &Value::Int(5)).to_vec(), vec![0, 1, 2, 3, 4]);
        // And a predicate that spans blocks still finds everything.
        assert_eq!(c.filter(CmpOp::Ge, &Value::Int(n as i64 - 3)).popcount(), 3);
    }

    #[test]
    fn scalar_array_containment_uses_the_value_index() {
        let mut b = ColumnBuilder::new("tags", ValueType::Array);
        for i in 0..50 {
            let mut tags = vec![Value::Str(format!("t{}", i % 5))];
            if i % 7 == 0 {
                tags.push(Value::Str("hot".into()));
            }
            b.push(i, Some(Value::Array(tags)));
        }
        let c = b.finish(50);
        let hot = c.filter(CmpOp::ArrayContains, &Value::Str("hot".into()));
        assert_eq!(hot.to_vec(), (0..50u32).filter(|i| i % 7 == 0).collect::<Vec<_>>());
        assert_eq!(c.filter(CmpOp::ArrayContains, &Value::Str("t3".into())).popcount(), 10);
        assert!(c.filter(CmpOp::ArrayContains, &Value::Str("absent".into())).is_empty());
    }

    #[test]
    fn string_column_prefix_and_bloom() {
        let mut b = ColumnBuilder::new("s", ValueType::Str);
        for i in 0..200 {
            b.push(i, Some(Value::Str(format!("tenant-{}/doc", i % 4))));
        }
        let c = b.finish(200);
        assert_eq!(c.filter(CmpOp::Prefix, &Value::Str("tenant-2".into())).popcount(), 50);
        assert!(c.bloom.maybe_contains(b"tenant-1/doc"));
        assert!(!c.bloom.maybe_contains(b"tenant-9/doc"));
        assert!(c.filter(CmpOp::Eq, &Value::Str("tenant-9/doc".into())).is_empty());
    }

    #[test]
    fn columns_round_trip() {
        for c in [
            num_col(300),
            {
                let mut b = ColumnBuilder::new("s", ValueType::Str);
                for i in 0..300 {
                    b.push(i, Some(Value::Str(format!("v{i}"))));
                }
                b.finish(300)
            },
            {
                let mut b = ColumnBuilder::new("tags", ValueType::Array);
                for i in 0..300 {
                    b.push(i, Some(Value::Array(vec![Value::Str(format!("t{}", i % 9))])));
                }
                b.finish(300)
            },
            {
                let mut b = ColumnBuilder::new("at", ValueType::Timestamp);
                for i in 0..300 {
                    b.push(i, Some(Value::Timestamp(1_700_000_000_000_000 + i as i64)));
                }
                b.finish(300)
            },
        ] {
            let back = Column::decode(&c.encode()).unwrap();
            for ord in [0u32, 1, 7, 299] {
                assert_eq!(back.get(ord), c.get(ord), "path {}", c.path);
            }
            assert_eq!(
                back.filter(CmpOp::IsNotNull, &Value::Null).popcount(),
                c.filter(CmpOp::IsNotNull, &Value::Null).popcount()
            );
        }
    }
}
