//! Index lifecycle policies: written-down rules for moving indexes between
//! tiers as they age or go idle.
//!
//! ```sql
//! CREATE LIFECYCLE POLICY archive_old ON articles
//!   FOR (articles_body, articles_emb)
//!   MOVE TO cached     AFTER 30 minutes OF INACTIVITY,
//!   MOVE TO archived AFTER 7 days     OF INACTIVITY;
//!
//! CREATE LIFECYCLE POLICY retire_by_age ON events
//!   MOVE TO archived AFTER 90 days SINCE CREATION;
//! ```
//!
//! Two design choices worth stating, because both could plausibly have gone the
//! other way:
//!
//! **A policy only ever demotes.** Promotion back towards RAM happens on
//! *access*, not on a rule: an archived index that gets queried faults in and
//! its idle clock restarts. A rule that promoted on a timer would fight the
//! access pattern rather than follow it, and would make a quiet Sunday look
//! like a reason to load half the cluster into memory.
//!
//! **The furthest matching rule wins.** With a 30-minute `cold` rule and a
//! 7-day `archived` rule, an index idle for a fortnight goes straight to
//! `archived` rather than stepping through `cold` on successive runs. Rules
//! describe a destination for a condition, not a sequence of hops, so the
//! outcome does not depend on how often the policy runner happens to fire.

use std::collections::BTreeMap;

use crate::codec::*;
use crate::error::{Error, Result};
use crate::residency::Tier;
use crate::time::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Minutes,
    Hours,
    Days,
}

impl Unit {
    pub fn parse(s: &str) -> Result<Unit> {
        match s.to_ascii_lowercase().trim_end_matches('s') {
            "minute" | "min" => Ok(Unit::Minutes),
            "hour" | "hr" => Ok(Unit::Hours),
            "day" => Ok(Unit::Days),
            other => Err(Error::Sql(format!(
                "unknown duration unit `{other}`; expected minutes, hours or days"
            ))),
        }
    }

    pub fn micros(self) -> u64 {
        match self {
            Unit::Minutes => 60_000_000,
            Unit::Hours => 3_600_000_000,
            Unit::Days => 86_400_000_000,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Unit::Minutes => "minutes",
            Unit::Hours => "hours",
            Unit::Days => "days",
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Unit::Minutes => 0,
            Unit::Hours => 1,
            Unit::Days => 2,
        }
    }

    fn from_u8(b: u8) -> Unit {
        match b {
            0 => Unit::Minutes,
            1 => Unit::Hours,
            _ => Unit::Days,
        }
    }
}

/// A duration as the user wrote it. The original unit is kept so that
/// `SHOW LIFECYCLE` prints back what was typed rather than a pile of
/// microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Every {
    pub n: u64,
    pub unit: Unit,
}

impl Every {
    pub fn new(n: u64, unit: Unit) -> Result<Every> {
        if n == 0 {
            return Err(Error::Sql("a lifecycle duration must be positive".into()));
        }
        n.checked_mul(unit.micros())
            .ok_or_else(|| Error::Sql(format!("duration `{n} {}` is out of range", unit.name())))?;
        Ok(Every { n, unit })
    }

    pub fn micros(self) -> u64 {
        self.n.saturating_mul(self.unit.micros())
    }
}

impl std::fmt::Display for Every {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let unit =
            if self.n == 1 { self.unit.name().trim_end_matches('s') } else { self.unit.name() };
        write!(f, "{} {}", self.n, unit)
    }
}

/// What the duration is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// Time since the index was last read by a query. The default, and the one
    /// that tracks what an index is actually costing.
    Inactivity,
    /// Time since the index was created. For data with a known shelf life,
    /// where the point is retention rather than usage.
    SinceCreation,
}

impl Trigger {
    pub fn name(self) -> &'static str {
        match self {
            Trigger::Inactivity => "OF INACTIVITY",
            Trigger::SinceCreation => "SINCE CREATION",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub to: Tier,
    pub after: Every,
    pub trigger: Trigger,
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MOVE TO {} AFTER {} {}", self.to.name(), self.after, self.trigger.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecyclePolicy {
    pub name: String,
    pub collection: String,
    /// Index names this policy covers. Empty means every index in the
    /// collection, including ones created later — which is usually what
    /// somebody writing a retention rule means.
    pub indexes: Vec<String>,
    pub rules: Vec<Rule>,
}

impl LifecyclePolicy {
    pub fn covers(&self, index: &str) -> bool {
        self.indexes.is_empty() || self.indexes.iter().any(|i| i == index)
    }

    pub fn render(&self) -> String {
        let mut s = format!("CREATE LIFECYCLE POLICY {} ON {}", self.name, self.collection);
        if !self.indexes.is_empty() {
            s.push_str(&format!(" FOR ({})", self.indexes.join(", ")));
        }
        s.push('\n');
        for (i, r) in self.rules.iter().enumerate() {
            s.push_str(&format!("  {r}{}\n", if i + 1 == self.rules.len() { ";" } else { "," }));
        }
        s
    }
}

/// Per-index facts a policy is evaluated against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexActivity {
    /// When the index was defined.
    pub created_micros: u64,
    /// When a query last read it. Equal to `created_micros` until first use.
    pub last_access_micros: u64,
    /// Which kind of rule last moved this index, if a policy did.
    ///
    /// This is what stops an age rule and access-promotion from fighting. An
    /// inactivity demotion says "nobody wanted this", so the next query is
    /// evidence to the contrary and promotes it back. An age demotion says
    /// "this is old", and a query is not evidence against that — promoting on
    /// access would archive it again on the next run, and again after the next
    /// query, moving the segment files back and forth forever.
    pub demoted_by: Option<Trigger>,
}

impl IndexActivity {
    pub fn new(now: u64) -> IndexActivity {
        IndexActivity { created_micros: now, last_access_micros: now, demoted_by: None }
    }

    /// May an access promote this index back toward its declared tier?
    pub fn promotable(&self) -> bool {
        !matches!(self.demoted_by, Some(Trigger::SinceCreation))
    }
}

/// The tier a policy says this index should be at, or `None` if no rule fires.
///
/// The furthest tier wins, so the answer does not depend on how often the
/// runner fires.
pub fn evaluate<'a>(
    policy: &'a LifecyclePolicy,
    index: &str,
    act: &IndexActivity,
    now: u64,
) -> Option<(Tier, &'a Rule)> {
    if !policy.covers(index) {
        return None;
    }
    let mut best: Option<(Tier, &Rule)> = None;
    for r in &policy.rules {
        let since = match r.trigger {
            Trigger::Inactivity => now.saturating_sub(act.last_access_micros),
            Trigger::SinceCreation => now.saturating_sub(act.created_micros),
        };
        if since < r.after.micros() {
            continue;
        }
        // The furthest tier wins; among rules reaching the same tier, the one
        // that actually fired with the longest window is the honest
        // explanation. Picking by tier alone can name a rule that did not fire.
        best = Some(match best {
            Some((bt, br)) if bt > r.to => (bt, br),
            Some((bt, br)) if bt == r.to && br.after.micros() >= r.after.micros() => (bt, br),
            _ => (r.to, r),
        });
    }
    best
}

/// One tier change a policy run decided on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub collection: String,
    pub index: String,
    pub from: Tier,
    pub to: Tier,
    pub policy: String,
    /// Which kind of rule fired. Recorded on the index, because access
    /// promotion reverses an inactivity demotion and must not reverse a
    /// retention one.
    pub trigger: Trigger,
    pub reason: String,
}

impl std::fmt::Display for Transition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}: {} -> {} ({}, {})",
            self.collection,
            self.index,
            self.from.name(),
            self.to.name(),
            self.policy,
            self.reason
        )
    }
}

/// Decide every tier change for one collection.
///
/// Only demotions are returned: promotion is what an access does, not what a
/// rule does.
pub fn plan(
    policies: &BTreeMap<String, LifecyclePolicy>,
    collection: &str,
    indexes: &[(String, Tier)],
    activity: &BTreeMap<String, IndexActivity>,
    now: u64,
) -> Vec<Transition> {
    let mut out = Vec::new();
    for (index, current) in indexes {
        let default_act = IndexActivity::new(now);
        let act = activity.get(index).unwrap_or(&default_act);
        let mut chosen: Option<(Tier, &LifecyclePolicy, &Rule)> = None;
        for p in policies.values().filter(|p| p.collection == collection) {
            let Some((t, rule)) = evaluate(p, index, act, now) else { continue };
            chosen = match chosen {
                Some((bt, _, _)) if bt >= t => chosen,
                _ => Some((t, p, rule)),
            };
        }
        if let Some((t, p, rule)) = chosen {
            if t.is_colder_than(*current) {
                let idle = now.saturating_sub(act.last_access_micros);
                let age = now.saturating_sub(act.created_micros);
                let reason = match rule.trigger {
                    Trigger::Inactivity => {
                        format!("idle {} >= {}", render_micros(idle), rule.after)
                    }
                    Trigger::SinceCreation => {
                        format!("age {} >= {}", render_micros(age), rule.after)
                    }
                };
                out.push(Transition {
                    collection: collection.to_string(),
                    index: index.clone(),
                    from: *current,
                    to: t,
                    policy: p.name.clone(),
                    trigger: rule.trigger,
                    reason,
                });
            }
        }
    }
    out
}

pub fn render_micros(m: u64) -> String {
    if m >= 86_400_000_000 {
        format!("{:.1} days", m as f64 / 86_400_000_000.0)
    } else if m >= 3_600_000_000 {
        format!("{:.1} hours", m as f64 / 3_600_000_000.0)
    } else if m >= 60_000_000 {
        format!("{:.1} minutes", m as f64 / 60_000_000.0)
    } else {
        format!("{:.1} seconds", m as f64 / 1_000_000.0)
    }
}

// --- Persistence, alongside the rest of the catalog (§10). ---

pub fn encode_policies(p: &BTreeMap<String, LifecyclePolicy>, out: &mut Vec<u8>) {
    put_uvarint(out, p.len() as u64);
    for pol in p.values() {
        put_str(out, &pol.name);
        put_str(out, &pol.collection);
        put_uvarint(out, pol.indexes.len() as u64);
        for i in &pol.indexes {
            put_str(out, i);
        }
        put_uvarint(out, pol.rules.len() as u64);
        for r in &pol.rules {
            out.push(r.to.as_u8());
            put_uvarint(out, r.after.n);
            out.push(r.after.unit.as_u8());
            out.push(match r.trigger {
                Trigger::Inactivity => 0,
                Trigger::SinceCreation => 1,
            });
        }
    }
}

pub fn decode_policies(b: &[u8], i: &mut usize) -> Result<BTreeMap<String, LifecyclePolicy>> {
    let bad = || Error::Storage("lifecycle: truncated".into());
    let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
    let mut out = BTreeMap::new();
    for _ in 0..n {
        let name = get_str(b, i).ok_or_else(bad)?;
        let collection = get_str(b, i).ok_or_else(bad)?;
        let ni = get_uvarint(b, i).ok_or_else(bad)? as usize;
        let mut indexes = Vec::with_capacity(ni);
        for _ in 0..ni {
            indexes.push(get_str(b, i).ok_or_else(bad)?);
        }
        let nr = get_uvarint(b, i).ok_or_else(bad)? as usize;
        let mut rules = Vec::with_capacity(nr);
        for _ in 0..nr {
            let to = Tier::from_u8(*b.get(*i).ok_or_else(bad)?);
            *i += 1;
            let cnt = get_uvarint(b, i).ok_or_else(bad)?;
            let unit = Unit::from_u8(*b.get(*i).ok_or_else(bad)?);
            *i += 1;
            let trig = *b.get(*i).ok_or_else(bad)?;
            *i += 1;
            // Through `Every::new`, not the struct literal: a zero duration
            // is satisfied by every index at every instant, so a catalog byte
            // that says `0` would demote everything on the first run. The
            // parser and the constructor both refuse it; the one path that
            // reads bytes off disk has to refuse it too.
            rules.push(Rule {
                to,
                after: Every::new(cnt, unit)?,
                trigger: if trig == 0 { Trigger::Inactivity } else { Trigger::SinceCreation },
            });
        }
        out.insert(name.clone(), LifecyclePolicy { name, collection, indexes, rules });
    }
    Ok(out)
}

/// Activity is per `(collection, index)` and, like the path catalog, lives in
/// the control plane so it survives a restart.
pub fn encode_activity(a: &BTreeMap<(String, String), IndexActivity>, out: &mut Vec<u8>) {
    put_uvarint(out, a.len() as u64);
    for ((c, i), act) in a {
        put_str(out, c);
        put_str(out, i);
        put_u64(out, act.created_micros);
        put_u64(out, act.last_access_micros);
        out.push(match act.demoted_by {
            None => 0,
            Some(Trigger::Inactivity) => 1,
            Some(Trigger::SinceCreation) => 2,
        });
    }
}

pub fn decode_activity(
    b: &[u8],
    i: &mut usize,
) -> Result<BTreeMap<(String, String), IndexActivity>> {
    let bad = || Error::Storage("lifecycle activity: truncated".into());
    let n = get_uvarint(b, i).ok_or_else(bad)? as usize;
    let mut out = BTreeMap::new();
    for _ in 0..n {
        let c = get_str(b, i).ok_or_else(bad)?;
        let idx = get_str(b, i).ok_or_else(bad)?;
        let created = get_u64(b, i).ok_or_else(bad)?;
        let last = get_u64(b, i).ok_or_else(bad)?;
        let d = *b.get(*i).ok_or_else(bad)?;
        *i += 1;
        let demoted_by = match d {
            1 => Some(Trigger::Inactivity),
            2 => Some(Trigger::SinceCreation),
            _ => None,
        };
        out.insert(
            (c, idx),
            IndexActivity { created_micros: created, last_access_micros: last, demoted_by },
        );
    }
    Ok(out)
}

/// Microseconds "now", in the same clock the shard commits with.
pub fn now_micros(clock: &crate::time::Hlc) -> u64 {
    crate::time::physical_micros(clock.peek() as Timestamp) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60_000_000;
    const HOUR: u64 = 3_600_000_000;
    const DAY: u64 = 86_400_000_000;

    fn policy(rules: Vec<Rule>) -> LifecyclePolicy {
        LifecyclePolicy { name: "p".into(), collection: "c".into(), indexes: vec![], rules }
    }

    fn rule(to: Tier, n: u64, unit: Unit, trigger: Trigger) -> Rule {
        Rule { to, after: Every::new(n, unit).unwrap(), trigger }
    }

    #[test]
    fn durations_parse_and_print_back_as_written() {
        assert_eq!(Every::new(1, Unit::Minutes).unwrap().to_string(), "1 minute");
        assert_eq!(Every::new(30, Unit::Minutes).unwrap().micros(), 30 * MIN);
        assert_eq!(Every::new(2, Unit::Hours).unwrap().micros(), 2 * HOUR);
        assert_eq!(Every::new(7, Unit::Days).unwrap().to_string(), "7 days");
        assert_eq!(Unit::parse("Days").unwrap(), Unit::Days);
        assert_eq!(Unit::parse("hr").unwrap(), Unit::Hours);
        assert!(Unit::parse("fortnights").is_err());
        assert!(Every::new(0, Unit::Days).is_err(), "a zero duration is a mistake");
        assert!(Every::new(u64::MAX, Unit::Days).is_err(), "overflow is refused");
    }

    #[test]
    fn the_furthest_matching_rule_wins() {
        let p = policy(vec![
            rule(Tier::Cached, 30, Unit::Minutes, Trigger::Inactivity),
            rule(Tier::Archived, 7, Unit::Days, Trigger::Inactivity),
        ]);
        let now = 100 * DAY;
        let idle_for = |d: u64| IndexActivity {
            created_micros: 0,
            last_access_micros: now - d,
            demoted_by: None,
        };

        assert!(evaluate(&p, "i", &idle_for(5 * MIN), now).is_none());
        assert_eq!(evaluate(&p, "i", &idle_for(45 * MIN), now).map(|(t, _)| t), Some(Tier::Cached));
        // Idle for a fortnight: straight to archived, not one hop per run.
        assert_eq!(
            evaluate(&p, "i", &idle_for(14 * DAY), now).map(|(t, _)| t),
            Some(Tier::Archived)
        );
    }

    #[test]
    fn creation_age_and_inactivity_are_different_questions() {
        let now = 100 * DAY;
        let by_age = policy(vec![rule(Tier::Archived, 90, Unit::Days, Trigger::SinceCreation)]);
        let by_idle = policy(vec![rule(Tier::Archived, 90, Unit::Days, Trigger::Inactivity)]);
        // Created long ago but queried a minute ago.
        let busy_old =
            IndexActivity { created_micros: 0, last_access_micros: now - MIN, demoted_by: None };
        assert_eq!(evaluate(&by_age, "i", &busy_old, now).map(|(t, _)| t), Some(Tier::Archived));
        assert!(evaluate(&by_idle, "i", &busy_old, now).is_none());
    }

    #[test]
    fn a_policy_covers_named_indexes_or_all_of_them() {
        let mut p = policy(vec![rule(Tier::Cached, 1, Unit::Minutes, Trigger::Inactivity)]);
        assert!(p.covers("anything"), "an empty list means every index");
        p.indexes = vec!["a".into(), "b".into()];
        assert!(p.covers("a"));
        assert!(!p.covers("c"));
    }

    #[test]
    fn planning_only_demotes_and_explains_itself() {
        let mut ps = BTreeMap::new();
        ps.insert(
            "p".to_string(),
            LifecyclePolicy {
                name: "p".into(),
                collection: "c".into(),
                indexes: vec![],
                rules: vec![
                    rule(Tier::Cached, 30, Unit::Minutes, Trigger::Inactivity),
                    rule(Tier::Archived, 7, Unit::Days, Trigger::Inactivity),
                ],
            },
        );
        let now = 100 * DAY;
        let mut act = BTreeMap::new();
        act.insert(
            "idle_index".to_string(),
            IndexActivity {
                created_micros: 0,
                last_access_micros: now - 2 * HOUR,
                demoted_by: None,
            },
        );
        act.insert(
            "busy_index".to_string(),
            IndexActivity { created_micros: 0, last_access_micros: now - MIN, demoted_by: None },
        );
        act.insert(
            "ancient".to_string(),
            IndexActivity {
                created_micros: 0,
                last_access_micros: now - 30 * DAY,
                demoted_by: None,
            },
        );

        let indexes = vec![
            ("idle_index".to_string(), Tier::Active),
            ("busy_index".to_string(), Tier::Active),
            ("ancient".to_string(), Tier::Active),
            // Already colder than the rule asks for: nothing to do, and
            // certainly no promotion.
            ("already_archived".to_string(), Tier::Archived),
        ];
        act.insert(
            "already_archived".to_string(),
            IndexActivity {
                created_micros: 0,
                last_access_micros: now - 2 * HOUR,
                demoted_by: None,
            },
        );

        let t = plan(&ps, "c", &indexes, &act, now);
        assert_eq!(t.len(), 2, "{t:?}");
        assert_eq!(t[0].index, "idle_index");
        assert_eq!(t[0].to, Tier::Cached);
        assert!(t[0].reason.contains("idle 2.0 hours"), "{}", t[0].reason);
        assert_eq!(t[1].index, "ancient");
        assert_eq!(t[1].to, Tier::Archived);
        // A rule never moves an index back towards RAM.
        assert!(t.iter().all(|x| x.to.is_colder_than(x.from)));
    }

    #[test]
    fn policies_round_trip_through_the_catalog_encoding() {
        let mut ps = BTreeMap::new();
        ps.insert(
            "p".to_string(),
            LifecyclePolicy {
                name: "p".into(),
                collection: "c".into(),
                indexes: vec!["a".into()],
                rules: vec![
                    rule(Tier::Cached, 30, Unit::Minutes, Trigger::Inactivity),
                    rule(Tier::Archived, 90, Unit::Days, Trigger::SinceCreation),
                ],
            },
        );
        let mut b = Vec::new();
        encode_policies(&ps, &mut b);
        let mut i = 0;
        assert_eq!(decode_policies(&b, &mut i).unwrap(), ps);

        let mut act = BTreeMap::new();
        act.insert(
            ("c".to_string(), "a".to_string()),
            IndexActivity { created_micros: 7, last_access_micros: 9, demoted_by: None },
        );
        let mut b = Vec::new();
        encode_activity(&act, &mut b);
        let mut i = 0;
        let back = decode_activity(&b, &mut i).unwrap();
        assert_eq!(back[&("c".to_string(), "a".to_string())].created_micros, 7);
        assert_eq!(back[&("c".to_string(), "a".to_string())].last_access_micros, 9);
    }
}
