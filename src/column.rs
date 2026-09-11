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

use std::cmp::Ordering;
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
        let nbits = usize::try_from(get_uvarint(b, i)?).ok()?;
        // `probes` reduces modulo `nbits` while `bits` holds only `nbits / 64`
        // words, so an `nbits` that is not a whole power-of-two number of words
        // lets a probe address a word that was never allocated — and that panic
        // lands in `maybe_contains`, on the query path. The writer emits nothing
        // but powers of two of at least 64, so anything else is corruption.
        if nbits != 0 && (nbits < 64 || !nbits.is_power_of_two()) {
            return None;
        }
        let words = bounded_len((nbits / 64) as u64, 8, b.len().saturating_sub(*i))?;
        let mut bits = vec![0u64; words];
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

    /// Where each row's elements begin, for a multi-value column.
    fn starts(&self) -> Option<&[u32]> {
        match &self.data {
            ColumnData::MultiStr { starts, .. } => Some(starts.as_slice()),
            ColumnData::MultiNum { starts, .. } => Some(starts.as_slice()),
            _ => None,
        }
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
                // A zone bound is an `f64`, so a literal `f64` cannot hold
                // exactly is placed against the bounds by its rounded
                // neighbour, which can land on the far side of a bound and
                // declare a block dead that holds matches. The scan below
                // answers that literal exactly; zone skipping sits it out
                // rather than answer it approximately. Every literal that
                // round-trips — every literal any query has used — still
                // skips exactly as before.
                if round_tie(lit) != Ordering::Equal {
                    return None;
                }
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
                        // Every string with prefix `v` sorts at or after `v`,
                        // so a block whose maximum is below `v` holds none, and
                        // one whose minimum is above `v` holds none *unless*
                        // that minimum has the prefix itself. Appending a
                        // maximal char to `v` is not an upper bound —
                        // "ab\u{10FFFF}\u{10FFFF}" sorts after "ab\u{10FFFF}"
                        // and still starts with "ab" — and a live block declared
                        // dead silently drops 4096 matching documents.
                        CmpOp::Prefix => {
                            let floor_ok = lo.starts_with(v) || lo.as_str() <= v;
                            floor_ok && hi.as_str() >= v
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
                if !self.type_admits(op, lit) {
                    return Bitmap::new(self.num_docs);
                }
                let mut bm = self.present().clone();
                // A multi-value row with no elements has nothing to compare
                // against a scalar literal. Against an array literal it is
                // still a perfectly comparable (empty) array, which is what
                // makes `tags <> ['a']` true rather than undefined for `[]`.
                if !matches!(lit, Value::Array(_)) {
                    if let Some(starts) = self.starts() {
                        for i in self.present().iter() {
                            if starts[i as usize] == starts[i as usize + 1] {
                                bm.clear(i as usize);
                            }
                        }
                    }
                }
                bm
            }
        }
    }

    /// Can this column's stored type be compared with this literal at all?
    fn type_admits(&self, op: CmpOp, lit: &Value) -> bool {
        let probe = match &self.data {
            ColumnData::Bool { .. } => Value::Bool(true),
            ColumnData::Num { .. } => Value::Float(0.0),
            ColumnData::Ts { .. } => Value::Timestamp(0),
            ColumnData::Str { .. } => Value::Str(String::new()),
            // A multi-value column holds arrays, and [`comparable`] compares an
            // array with an array literal as a whole rather than element-wise.
            // Probing with a bare element made `tags <> ['a','b']` undefined
            // everywhere, so the negation returned nothing at all.
            ColumnData::MultiNum { .. } => Value::Array(vec![Value::Float(0.0)]),
            ColumnData::MultiStr { .. } => Value::Array(vec![Value::Str(String::new())]),
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
        if self.is_multi() && matches!(lit, Value::Array(_)) {
            // A multi-value column compared with an array literal is a
            // whole-array comparison, not containment: the value index answers
            // `element = x`, which says nothing about `tags = ['a','b']`.
            // Falling through to the scalar branches returned an empty bitmap,
            // so a document the memtable matched disappeared from the answer the
            // moment its segment was sealed.
            for ord in self.present().iter() {
                if matches(&self.get(ord), op, lit) {
                    out.set(ord as usize);
                }
            }
            return out;
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
                let tie = round_tie(lit);
                for i in present.iter() {
                    if !block_live(i as usize) {
                        continue;
                    }
                    if cmp_ok(op, cmp_rounded(vals[i as usize], want, tie)) {
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
                let tie = round_tie(lit);
                for i in present.iter() {
                    let (s, e) = (starts[i as usize] as usize, starts[i as usize + 1] as usize);
                    if (s..e).any(|j| cmp_ok(op, cmp_rounded(vals[j], want, tie))) {
                        out.set(i as usize);
                    }
                }
            }
        }
        out
    }
}

/// Which side of an integer literal the `f64` the column compares against
/// fell on, and `Equal` for every literal `f64` holds exactly.
///
/// A shredded numeric column stores `f64`, so an integer literal is matched
/// against stored values as `i as f64` — the neighbouring double, once the
/// magnitude passes 2^53. Where a stored value lands exactly on that double the
/// rounded comparison answers `Equal`, which is the one answer that cannot be
/// right, because the literal is not that double. It is a bounded error: any
/// other stored value is on the same side of the literal as of its neighbour,
/// so this one tie is the whole of the correction.
fn round_tie(lit: &Value) -> Ordering {
    match lit {
        // i128 because `i64::MAX as f64` is 2^63, which an `as i64` cast back
        // would saturate to `i64::MAX` and call a round trip.
        Value::Int(i) => ((*i as f64) as i128).cmp(&(*i as i128)),
        _ => Ordering::Equal,
    }
}

/// Compare a stored `f64` with a literal that reached here as `want`, where
/// `tie` is [`round_tie`] of that literal. `Ordering::Equal` for `tie` — the
/// ordinary case — leaves the comparison exactly as it was.
fn cmp_rounded(v: f64, want: f64, tie: Ordering) -> Option<Ordering> {
    match v.partial_cmp(&want) {
        Some(Ordering::Equal) => Some(tie),
        o => o,
    }
}

/// Can a numeric column hold this value without changing it?
///
/// The column stores `f64`, and an integer past 2^53 does not survive that.
/// A rounded integer is not the value the document holds: `= 9007199254740993`
/// answered no and `= 9007199254740992` yes, while the memtable and the variant
/// fallback answered the other way round, so the same query changed its answer
/// when a segment was sealed. The column declines the value instead — which is
/// what `Column::mismatch` is for — and the reader answers it from the
/// document blob, exactly. Nothing changes for a value that round-trips, which
/// is every integer written so far.
fn stores_exactly(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Float(_)) && round_tie(v) == Ordering::Equal
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
        Value::Int(_) | Value::Float(_) => Some(num_bloom_key(v.as_f64()?).to_vec()),
        Value::Timestamp(t) => Some(t.to_le_bytes().to_vec()),
        Value::Bool(b) => Some(vec![*b as u8]),
        _ => None,
    }
}

/// The bloom key of a number, keyed by value rather than by bit pattern.
///
/// `-0.0` and `0.0` compare `Equal` everywhere else in the engine, so they must
/// not hash apart: a segment holding `-0.0` would otherwise fail the bloom probe
/// for `WHERE n = 0` and be skipped whole, and a false negative is the one
/// mistake a bloom filter is never allowed to make. Both the add and the probe
/// path go through here so they cannot drift.
fn num_bloom_key(x: f64) -> [u8; 8] {
    let x = if x == 0.0 { 0.0 } else { x };
    x.to_le_bytes()
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
        ValueType::Number => stores_exactly(v),
        ValueType::Timestamp => matches!(v, Value::Timestamp(_)),
        ValueType::Str => matches!(v, Value::Str(_)),
        ValueType::Array => {
            let ok_elem = |e: &Value| {
                if elem_str {
                    matches!(e, Value::Str(_))
                } else {
                    stores_exactly(e)
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
                        bloom.add(&num_bloom_key(x));
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

/// The widest ordinal space a decoder will materialise. A segment holds a few
/// million documents, so anything past this is a damaged length rather than a
/// long bitmap — and `Bitmap::new` on a damaged length is an allocation the
/// process answers by aborting.
const MAX_ORDINALS: usize = 1 << 28;

/// Reject an element count the remaining bytes could not possibly hold, before
/// anything reserves for it.
///
/// Every count below is read out of the buffer and handed straight to
/// `Vec::with_capacity`, so a corrupt count is an allocation request of
/// arbitrary size: an abort instead of the `Error::Storage` a damaged segment is
/// supposed to produce. `min_bytes_each` is the smallest encoded size of one
/// element.
fn bounded_len(n: u64, min_bytes_each: usize, remaining: usize) -> Option<usize> {
    let n = usize::try_from(n).ok()?;
    if n > remaining / min_bytes_each.max(1) {
        return None;
    }
    Some(n)
}

fn get_bitmap(b: &[u8], i: &mut usize) -> Option<Bitmap> {
    let len = usize::try_from(get_uvarint(b, i)?).ok()?;
    if len > MAX_ORDINALS {
        return None;
    }
    // A set ordinal costs at least one byte of delta, so a count past what is
    // left in the buffer is corruption.
    let n = get_uvarint(b, i)?;
    let n = bounded_len(n, 1, b.len().saturating_sub(*i))?;
    let mut bm = Bitmap::new(len);
    let mut prev = 0u64;
    for _ in 0..n {
        // `Bitmap::set` bounds-checks only under `debug_assertions`, so an
        // ordinal past `len` either panics on the word index or sets a bit in
        // the padding beyond the logical length — after which `filter` walks
        // that ordinal and indexes a value vector that never had it. Accumulate
        // in u64 too: `prev += delta as u32` truncates a corrupt delta into a
        // plausible ordinal instead of rejecting it.
        prev = prev.checked_add(get_uvarint(b, i)?)?;
        if prev >= len as u64 {
            return None;
        }
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
        // Every count below is decoded from the buffer, so every one of them is
        // bounded by the bytes that could still hold those elements before it
        // reaches `Vec::with_capacity`. `rest` is how many bytes are left.
        let rest = |at: usize| b.len().saturating_sub(at);
        let data = match tag {
            0 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let vals = get_bitmap(b, &mut i).ok_or_else(bad)?;
                ColumnData::Bool { present, vals }
            }
            1 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(num_docs as u64, 8, rest(i)).ok_or_else(bad)?;
                let mut vals = Vec::with_capacity(n);
                for _ in 0..n {
                    vals.push(f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?));
                }
                ColumnData::Num { present, vals }
            }
            2 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(num_docs as u64, 1, rest(i)).ok_or_else(bad)?;
                let mut vals = Vec::with_capacity(n);
                for _ in 0..n {
                    vals.push(get_ivarint(b, &mut i).ok_or_else(bad)?);
                }
                ColumnData::Ts { present, vals }
            }
            3 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let n = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(n, 1, rest(i)).ok_or_else(bad)?;
                let mut offsets = Vec::with_capacity(n);
                let mut prev = 0u32;
                for _ in 0..n {
                    // Checked, not truncating: a corrupt delta must be an error,
                    // not a wrapped offset that slices `data` somewhere else.
                    let d = get_uvarint(b, &mut i).ok_or_else(bad)?;
                    let d = u32::try_from(d).map_err(|_| bad())?;
                    prev = prev.checked_add(d).ok_or_else(bad)?;
                    offsets.push(prev);
                }
                let data = get_bytes(b, &mut i).ok_or_else(bad)?.to_vec();
                ColumnData::Str { present, offsets, data }
            }
            4 => {
                let present = get_bitmap(b, &mut i).ok_or_else(bad)?;
                let ns = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let ns = bounded_len(ns, 4, rest(i)).ok_or_else(bad)?;
                let mut starts = Vec::with_capacity(ns);
                for _ in 0..ns {
                    starts.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let no = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let no = bounded_len(no, 4, rest(i)).ok_or_else(bad)?;
                let mut offsets = Vec::with_capacity(no);
                for _ in 0..no {
                    offsets.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let data = get_bytes(b, &mut i).ok_or_else(bad)?.to_vec();
                let ni = get_uvarint(b, &mut i).ok_or_else(bad)?;
                // An index entry is a length-prefixed key plus a bitmap: three
                // bytes even when both are empty.
                let ni = bounded_len(ni, 3, rest(i)).ok_or_else(bad)?;
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
                let ns = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let ns = bounded_len(ns, 4, rest(i)).ok_or_else(bad)?;
                let mut starts = Vec::with_capacity(ns);
                for _ in 0..ns {
                    starts.push(get_u32(b, &mut i).ok_or_else(bad)?);
                }
                let nv = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let nv = bounded_len(nv, 8, rest(i)).ok_or_else(bad)?;
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
                let n = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(n, 16, rest(i)).ok_or_else(bad)?;
                let mut z = Vec::with_capacity(n);
                for _ in 0..n {
                    let a = f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?);
                    let c = f64::from_bits(get_u64(b, &mut i).ok_or_else(bad)?);
                    z.push((a, c));
                }
                Zones::Num(z)
            }
            2 => {
                let n = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(n, 2, rest(i)).ok_or_else(bad)?;
                let mut z = Vec::with_capacity(n);
                for _ in 0..n {
                    let a = get_ivarint(b, &mut i).ok_or_else(bad)?;
                    let c = get_ivarint(b, &mut i).ok_or_else(bad)?;
                    z.push((a, c));
                }
                Zones::Ts(z)
            }
            3 => {
                let n = get_uvarint(b, &mut i).ok_or_else(bad)?;
                let n = bounded_len(n, 2, rest(i)).ok_or_else(bad)?;
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
        let col = Column { path, ty, num_docs, data, zones, bloom, mismatch };
        col.validate()?;
        Ok(col)
    }

    /// Cross-check the decoded pieces against each other.
    ///
    /// Each accessor indexes one decoded vector with an ordinal or an offset
    /// taken from another, so pieces that disagree are not an error that stays
    /// inside the decoder: they are an out-of-bounds panic on the first query
    /// that touches the segment. A damaged file has to fail as `Error::Storage`
    /// at open, not as a crash later on somebody's `SELECT`.
    fn validate(&self) -> Result<()> {
        let bad = |m: &str| Error::Storage(format!("column {}: {m}", self.path));
        if self.present().len() != self.num_docs || self.mismatch.len() != self.num_docs {
            return Err(bad("presence bitmap does not cover the column"));
        }
        let nblocks = self.num_docs.div_ceil(ZONE_BLOCK);
        let zones_fit = match &self.zones {
            Zones::None => true,
            Zones::Num(z) => z.len() == nblocks,
            Zones::Ts(z) => z.len() == nblocks,
            Zones::Str(z) => z.len() == nblocks,
        };
        // `zone_skip` writes one `keep[b]` per zone entry into a vector sized
        // from `num_docs`, so a zone map with more entries than the column has
        // blocks indexes past the end of it.
        if !zones_fit {
            return Err(bad("zone map does not cover the column"));
        }
        let nd = self.num_docs;
        match &self.data {
            ColumnData::Bool { vals, .. } => {
                if vals.len() != nd {
                    return Err(bad("boolean values do not cover the column"));
                }
            }
            ColumnData::Num { vals, .. } => {
                if vals.len() != nd {
                    return Err(bad("numeric values do not cover the column"));
                }
            }
            ColumnData::Ts { vals, .. } => {
                if vals.len() != nd {
                    return Err(bad("timestamp values do not cover the column"));
                }
            }
            ColumnData::Str { offsets, data, .. } => {
                if offsets.len() != nd + 1 || !nondecreasing(offsets) {
                    return Err(bad("string offsets do not cover the column"));
                }
                if offsets.last().copied().unwrap_or(0) as usize > data.len() {
                    return Err(bad("string offsets run past the data block"));
                }
            }
            ColumnData::MultiStr { starts, offsets, data, index, .. } => {
                if starts.len() != nd + 1 || !nondecreasing(starts) {
                    return Err(bad("array starts do not cover the column"));
                }
                // `get` reads `offsets[j]` and `offsets[j + 1]` for every
                // element `j` a row spans, so the last start must leave one
                // more offset behind it.
                if offsets.is_empty() || starts[nd] as usize >= offsets.len() {
                    return Err(bad("array starts run past the offsets"));
                }
                let end = offsets.last().copied().unwrap_or(0) as usize;
                if !nondecreasing(offsets) || end > data.len() {
                    return Err(bad("array offsets run past the data block"));
                }
                if index.values().any(|bm| bm.len() != nd) {
                    return Err(bad("value index does not cover the column"));
                }
            }
            ColumnData::MultiNum { starts, vals, .. } => {
                if starts.len() != nd + 1 || !nondecreasing(starts) {
                    return Err(bad("array starts do not cover the column"));
                }
                if starts[nd] as usize > vals.len() {
                    return Err(bad("array starts run past the values"));
                }
            }
        }
        Ok(())
    }
}

fn nondecreasing(v: &[u32]) -> bool {
    v.windows(2).all(|w| w[0] <= w[1])
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
/// optimisation of this function, never a redefinition of it: where a column
/// cannot represent a value exactly it declines it (`Column::mismatch`) and the
/// reader falls back here rather than answering from the approximation. That is
/// what the "shredded or not, same answer" claim in §2.1 rests on. No test
/// establishes it exhaustively — the divergences that have been found were
/// each closed with a case of their own.
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
    fn an_integer_f64_cannot_hold_is_declined_rather_than_rounded() {
        // 2^53 + 1 is the smallest integer f64 cannot hold; it rounds to 2^53.
        // Stored as f64 the column answers `= 2^53` yes and `= 2^53 + 1` no,
        // both the opposite of what the document says and of what the memtable
        // and the variant fallback answer.
        let big = (1i64 << 53) + 1;
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        b.push(0, Some(Value::Int(1)));
        b.push(1, Some(Value::Int(big)));
        let c = b.finish(2);
        assert!(c.mismatch.get(1), "a lossy integer belongs in the variant fallback");
        assert!(!c.present().get(1));
        assert!(!c.filter(CmpOp::Eq, &Value::Int(big - 1)).get(1), "rounded down into 2^53");
        assert!(!c.filter(CmpOp::Eq, &Value::Int(big)).get(1));
        // The fallback answers it, and answers it exactly.
        assert!(matches(&Value::Int(big), CmpOp::Eq, &Value::Int(big)));
        assert!(!matches(&Value::Int(big), CmpOp::Eq, &Value::Int(big - 1)));
        // Same for an element of a scalar array column.
        let mut b = ColumnBuilder::new("ns", ValueType::Array);
        b.push(0, Some(Value::Array(vec![Value::Int(1), Value::Int(big)])));
        b.push(1, Some(Value::Array(vec![Value::Int(2)])));
        let c = b.finish(2);
        assert!(c.mismatch.get(0), "an array holding a lossy integer is a mismatch");
        assert!(c.present().get(1));
    }

    #[test]
    fn an_integer_literal_f64_cannot_hold_does_not_match_its_rounded_neighbour() {
        let exact = 1i64 << 53; // representable, so it is stored in the column
        let lossy = exact + 1; // rounds to `exact`
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        b.push(0, Some(Value::Int(exact)));
        let c = b.finish(1);
        assert!(c.present().get(0) && c.mismatch.is_empty());
        // 2^53 is not 2^53 + 1, it is below it, and the column must say so.
        assert!(!matches(&Value::Int(exact), CmpOp::Eq, &Value::Int(lossy)));
        assert!(c.filter(CmpOp::Eq, &Value::Int(lossy)).is_empty());
        assert!(matches(&Value::Int(exact), CmpOp::Lt, &Value::Int(lossy)));
        assert!(c.filter(CmpOp::Lt, &Value::Int(lossy)).get(0), "zone or scan dropped it");
        assert!(c.filter(CmpOp::Le, &Value::Int(lossy)).get(0));
        assert!(!c.filter(CmpOp::Gt, &Value::Int(lossy)).get(0));
        assert!(!c.filter(CmpOp::Ge, &Value::Int(lossy)).get(0));
        assert!(c.filter(CmpOp::Ne, &Value::Int(lossy)).get(0));
        // The other rounding direction: 2^53 + 3 rounds up to 2^53 + 4.
        let up = exact + 3;
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        b.push(0, Some(Value::Int(exact + 4)));
        let c = b.finish(1);
        assert!(matches(&Value::Int(exact + 4), CmpOp::Gt, &Value::Int(up)));
        assert!(c.filter(CmpOp::Gt, &Value::Int(up)).get(0));
        assert!(c.filter(CmpOp::Eq, &Value::Int(up)).is_empty());
        // A multi-value column reads the literal the same way.
        let mut b = ColumnBuilder::new("ns", ValueType::Array);
        b.push(0, Some(Value::Array(vec![Value::Int(exact)])));
        let c = b.finish(1);
        assert!(c.filter(CmpOp::Eq, &Value::Int(lossy)).is_empty());
        assert!(c.filter(CmpOp::Lt, &Value::Int(lossy)).get(0));
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

    #[test]
    fn an_array_literal_still_matches_once_the_column_is_sealed() {
        let rows = [
            Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]),
            Value::Array(vec![Value::Str("b".into()), Value::Str("a".into())]),
            Value::Array(vec![Value::Str("a".into())]),
        ];
        let mut b = ColumnBuilder::new("tags", ValueType::Array);
        for (i, v) in rows.iter().enumerate() {
            b.push(i as u32, Some(v.clone()));
        }
        let c = b.finish(rows.len());
        let lit = Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]);
        // The column is an optimisation of `matches`, never a redefinition of
        // it: a sealed segment must not lose a row the memtable answered with.
        let (eq, ne) = (c.filter(CmpOp::Eq, &lit), c.filter(CmpOp::Ne, &lit));
        for (i, v) in rows.iter().enumerate() {
            assert_eq!(eq.get(i), matches(v, CmpOp::Eq, &lit), "= row {i}");
            assert_eq!(ne.get(i), matches(v, CmpOp::Ne, &lit), "<> row {i}");
        }
        assert_eq!(eq.to_vec(), vec![0]);
        assert_eq!(ne.to_vec(), vec![1, 2]);

        // The numeric multi-value layout has no value index at all, so it took
        // the same fall-through to an empty bitmap.
        let mut nb = ColumnBuilder::new("xs", ValueType::Array);
        nb.push(0, Some(Value::Array(vec![Value::Int(1), Value::Int(2)])));
        nb.push(1, Some(Value::Array(vec![Value::Int(1)])));
        let nc = nb.finish(2);
        let nlit = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(nc.filter(CmpOp::Eq, &nlit).to_vec(), vec![0]);
    }

    #[test]
    fn a_prefix_zone_keeps_a_block_whose_minimum_sorts_past_the_old_bound() {
        let mut b = ColumnBuilder::new("s", ValueType::Str);
        // Both start with "ab" and both sort *after* "ab\u{10FFFF}", which the
        // zone map used to treat as the top of the prefix range.
        b.push(0, Some(Value::Str("ab\u{10FFFF}\u{10FFFF}".into())));
        b.push(1, Some(Value::Str("ab\u{10FFFF}zzz".into())));
        let c = b.finish(2);
        assert_eq!(c.zone_skip(CmpOp::Prefix, &Value::Str("ab".into())).unwrap(), vec![true]);
        assert_eq!(c.filter(CmpOp::Prefix, &Value::Str("ab".into())).to_vec(), vec![0, 1]);
        // A block that really cannot hold the prefix is still skipped.
        assert_eq!(c.zone_skip(CmpOp::Prefix, &Value::Str("zz".into())).unwrap(), vec![false]);
    }

    #[test]
    fn negative_zero_is_not_a_bloom_false_negative_for_zero() {
        // Same value, so the same key: this is what stops the add path and the
        // probe path from drifting apart.
        assert_eq!(bloom_key(&Value::Float(-0.0)), bloom_key(&Value::Float(0.0)));
        let mut b = ColumnBuilder::new("n", ValueType::Number);
        b.push(0, Some(Value::Float(-0.0)));
        let c = b.finish(1);
        assert!(c.bloom.maybe_contains(&bloom_key(&Value::Int(0)).unwrap()));
        assert_eq!(c.filter(CmpOp::Eq, &Value::Int(0)).to_vec(), vec![0]);
    }

    #[test]
    fn a_bitmap_ordinal_past_the_decoded_length_is_rejected_not_set_in_the_padding() {
        let mut b = Vec::new();
        put_uvarint(&mut b, 8);
        put_uvarint(&mut b, 1);
        put_uvarint(&mut b, 100);
        let mut i = 0usize;
        assert!(get_bitmap(&b, &mut i).is_none());
        // The control: an ordinal the bitmap actually has still decodes.
        let mut ok = Vec::new();
        put_uvarint(&mut ok, 8);
        put_uvarint(&mut ok, 1);
        put_uvarint(&mut ok, 7);
        let mut i = 0usize;
        assert_eq!(get_bitmap(&ok, &mut i).unwrap().to_vec(), vec![7]);
    }

    #[test]
    fn a_bitmap_length_past_the_ordinal_cap_is_rejected_before_it_is_allocated() {
        let mut b = Vec::new();
        put_uvarint(&mut b, MAX_ORDINALS as u64 + 1);
        put_uvarint(&mut b, 0);
        let mut i = 0usize;
        assert!(get_bitmap(&b, &mut i).is_none());
    }

    /// A hand-built encoding of a two-document numeric column, with the shape
    /// of the presence bitmap under the test's control.
    fn encoded_num_column(present_len: u64, present_ord: u64) -> Vec<u8> {
        let mut b = Vec::new();
        put_str(&mut b, "n");
        b.push(ValueType::Number as u8);
        put_uvarint(&mut b, 2);
        b.push(1);
        put_uvarint(&mut b, present_len);
        put_uvarint(&mut b, 1);
        put_uvarint(&mut b, present_ord);
        put_u64(&mut b, 1f64.to_bits());
        put_u64(&mut b, 2f64.to_bits());
        b.push(1);
        put_uvarint(&mut b, 1);
        put_u64(&mut b, f64::NEG_INFINITY.to_bits());
        put_u64(&mut b, f64::INFINITY.to_bits());
        put_uvarint(&mut b, 64);
        put_u64(&mut b, u64::MAX);
        put_uvarint(&mut b, 2);
        put_uvarint(&mut b, 0);
        b
    }

    #[test]
    fn a_present_bitmap_wider_than_the_column_is_rejected_at_decode() {
        let ok = Column::decode(&encoded_num_column(2, 1)).unwrap();
        assert_eq!(ok.get(1), Value::Float(2.0));
        // Ordinal 99 of a two-document column: every predicate walks `present`
        // and would index `vals[99]` on a two-element vector.
        assert!(Column::decode(&encoded_num_column(100, 99)).is_err());
    }

    #[test]
    fn a_bloom_width_its_probes_cannot_address_is_rejected_at_decode() {
        // 65 bits is one word, but a probe reduces modulo 65 and can land on
        // bit 64 — which `maybe_contains` reads out of a second word that was
        // never allocated, on the query path.
        let mut b = Vec::new();
        put_uvarint(&mut b, 65);
        put_u64(&mut b, u64::MAX);
        let mut i = 0usize;
        assert!(Bloom::decode(&b, &mut i).is_none());
        let mut ok = Vec::new();
        Bloom::with_capacity(4).encode(&mut ok);
        let mut i = 0usize;
        assert_eq!(Bloom::decode(&ok, &mut i).unwrap().nbits, 64);
    }
}
