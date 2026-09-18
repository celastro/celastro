//! Shared by `reconcile.rs` and `resilience.rs`: random histories of
//! creations and drops over N catalogs, reconciled pairwise in random
//! orders to a fixed point, against a model built from the catalogs' own
//! instants.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use celastro::engine::{Db, DbOpts};

pub fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-recon-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A small deterministic generator, so a failing seed is a failing seed.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

const COLLECTIONS: &[&str] = &["alpha", "beta", "gamma"];
const INDEXES: &[&str] = &["by_n", "by_m"];

/// What every node holds, as `collection` and `collection/index` names.
pub fn shape(db: &Db) -> Vec<String> {
    let mut v = Vec::new();
    for (name, c) in &db.catalog.collections {
        v.push(name.clone());
        for i in &c.indexes {
            v.push(format!("{name}/{}", i.name));
        }
    }
    v.sort();
    v
}

/// Run `seeds` histories of `ops` statements over `nodes` catalogs; a
/// history that does not converge to its model panics naming the seed
/// and the history.
pub fn converge(seeds: u64, nodes_n: usize, ops: usize) {
    for seed in 1..=seeds {
        let mut rng = Lcg(seed);
        let dirs: Vec<PathBuf> = (0..nodes_n).map(|i| dir(&format!("{seed}-{i}"))).collect();
        let mut nodes: Vec<Db> =
            dirs.iter().map(|d| Db::open(d, DbOpts::default()).unwrap()).collect();
        // The model. A name's fate is decided by instants, the way the merge
        // decides it: a collection survives if an incarnation of it was made
        // after its last drop; an index survives if it was made after its
        // last drop, on an incarnation that survives. An index made on an
        // incarnation a later-timed drop removes goes with it -- that is
        // what a drop means, and the merge must not resurrect it on the
        // incarnation that replaced it. The instants are read off the
        // catalogs themselves, so the model tests the rule and not the clock.
        let mut incarnations: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let mut coll_drops: BTreeMap<String, u64> = BTreeMap::new();
        let mut index_makes: BTreeMap<String, Vec<(u64, u64)>> = BTreeMap::new();
        let mut index_drops: BTreeMap<String, u64> = BTreeMap::new();
        let mut log = Vec::new();
        for _ in 0..ops {
            let n = rng.below(nodes.len());
            let coll = COLLECTIONS[rng.below(COLLECTIONS.len())];
            let has_coll = nodes[n].collection(coll).is_ok();
            let sql = match (has_coll, rng.below(4)) {
                (false, _) => format!("CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, n INT)"),
                (true, 0) => format!("DROP COLLECTION {coll}"),
                (true, _) => {
                    let idx = INDEXES[rng.below(INDEXES.len())];
                    if nodes[n].collection(coll).unwrap().index_by_name(idx).is_some() {
                        format!("DROP INDEX {idx} ON {coll}")
                    } else {
                        format!("CREATE INDEX {idx} ON {coll} USING secondary (n)")
                    }
                }
            };
            nodes[n].execute(&sql).unwrap_or_else(|e| panic!("seed {seed}: node {n}: {sql}: {e}"));
            log.push(format!("node {n}: {sql}"));
            let cat = &nodes[n].catalog;
            if sql.starts_with("CREATE COLLECTION") {
                incarnations
                    .entry(coll.into())
                    .or_default()
                    .push(cat.get(coll).unwrap().created_micros);
            } else if sql.starts_with("DROP COLLECTION") {
                coll_drops.insert(coll.into(), cat.dropped[coll]);
            } else if let Some(rest) = sql.strip_prefix("CREATE INDEX ") {
                let idx = rest.split(' ').next().unwrap();
                let key = format!("{coll}/{idx}");
                let made = cat.activity[&(coll.to_string(), idx.to_string())].created_micros;
                let on = cat.get(coll).unwrap().created_micros;
                index_makes.entry(key).or_default().push((made, on));
            } else if let Some(rest) = sql.strip_prefix("DROP INDEX ") {
                let idx = rest.split(' ').next().unwrap();
                let key = format!("{coll}/{idx}");
                index_drops.insert(key.clone(), cat.dropped[&key]);
            }
            // Two statements in one microsecond would tie on the clock the
            // tombstones compare by; a history never runs that fast, and
            // the test does not test the clock's resolution.
            std::thread::sleep(std::time::Duration::from_micros(20));
            // Sometimes a reconciliation in the middle of the history, so
            // the merge also runs against catalogs that changed after one.
            if rng.below(4) == 0 {
                let (i, j) = (rng.below(nodes.len()), rng.below(nodes.len()));
                if i != j {
                    let theirs = nodes[j].catalog.clone();
                    nodes[i].reconcile(&theirs).unwrap();
                    log.push(format!("node {i} reconciled from node {j}"));
                }
            }
        }
        let mut model: BTreeSet<String> = BTreeSet::new();
        for (coll, made) in &incarnations {
            let dropped = coll_drops.get(coll).copied().unwrap_or(0);
            if made.iter().any(|&t| t > dropped) {
                model.insert(coll.clone());
                for (key, makes) in &index_makes {
                    if key.starts_with(&format!("{coll}/")) {
                        let idropped = index_drops.get(key).copied().unwrap_or(0);
                        if makes.iter().any(|&(t, on)| t > idropped && on > dropped) {
                            model.insert(key.clone());
                        }
                    }
                }
            }
        }
        // Reconcile in random pairwise orders until a whole round changes
        // nothing.
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(
                rounds < 20,
                "seed {seed}: no fixed point after {rounds} rounds:\n{}",
                log.join("\n")
            );
            let mut pairs: Vec<(usize, usize)> = (0..nodes.len())
                .flat_map(|i| (0..nodes.len()).map(move |j| (i, j)))
                .filter(|(i, j)| i != j)
                .collect();
            for k in (1..pairs.len()).rev() {
                pairs.swap(k, rng.below(k + 1));
            }
            let mut changed = false;
            for (i, j) in pairs {
                let theirs = nodes[j].catalog.clone();
                changed |= !nodes[i].reconcile(&theirs).unwrap().is_empty();
            }
            if !changed {
                break;
            }
        }
        let want: Vec<String> = model.into_iter().collect();
        for (n, db) in nodes.iter().enumerate() {
            assert_eq!(
                shape(db),
                want,
                "seed {seed}: node {n} after {rounds} round(s):\n{}",
                log.join("\n")
            );
        }
        drop(nodes);
        for d in dirs {
            let _ = std::fs::remove_dir_all(&d);
        }
    }
}
