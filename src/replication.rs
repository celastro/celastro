//! Log shipping: a held shard's writes to the followers the map names for
//! it, and what a write waits for before it is acknowledged.
//!
//! A `Shipper` belongs to one held shard. The shard pushes every record it
//! logged -- the same record the write-ahead log holds -- and a thread of
//! the shipper's carries them to each follower in order over a connection
//! of its own, batched, and remembers per follower the latest instant it
//! confirmed on disk. In `sync` mode a write is acknowledged to the client
//! only once every follower has confirmed its instant, so an acknowledged
//! write is on two disks; in `async` mode it is acknowledged at once and
//! the followers trail by the shipping lag.
//!
//! A follower starts unknown: it may hold nothing, or a copy from before
//! this shipper existed (a holder restarted, a promotion). Its first
//! answer says where it stands, and the engine's replication step then
//! ships it the rows and deletes since that instant, in chunks, ending
//! with a mark; only then is it live and fed from the backlog. A follower
//! whose backlog would outgrow its cap is sent back to unknown and caught
//! up from where it stood, so a follower that is down for a day costs a
//! catch-up and not a node's memory.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::time::Timestamp;
use crate::Value;

/// A record as the write-ahead log holds it, or a mark of the protocol.
pub const SHIP_INSERT: u8 = 1;
pub const SHIP_DELETE: u8 = 2;
/// The follower persists the instant carried as where it stands; every
/// batch ends with one.
pub const SHIP_MARK: u8 = 3;
/// The follower drops what it holds before what follows: a catch-up from
/// nothing.
pub const SHIP_RESET: u8 = 10;
/// The catch-up is whole up to the instant carried: the follower is caught
/// up, and persists the instant.
pub const SHIP_CAUGHT_UP: u8 = 11;

#[derive(Clone, Debug)]
pub struct ShipItem {
    pub kind: u8,
    pub key: String,
    pub ts: Timestamp,
    pub doc: Option<Value>,
}

/// Where a follower stands, as the shipper knows it.
#[derive(Clone, Debug, PartialEq)]
pub enum FollowerState {
    /// Not asked yet, or sent back here: the thread asks where it stands.
    Unknown,
    /// Asked; the engine's step ships it what it lacks in chunks. `from` is
    /// where it stood, `upto` the instant the catch-up covers (fixed when
    /// the first chunk is cut), `cursor` the last key shipped, `reset`
    /// whether it started from nothing.
    CatchingUp { from: Timestamp, upto: Timestamp, cursor: Option<String>, reset: bool, done: bool },
    /// Fed from the backlog as writes come.
    Live,
}

pub struct Follower {
    pub url: String,
    pub node: Arc<crate::wire::Node>,
    pub state: FollowerState,
    /// The latest instant the follower confirmed on disk.
    pub acked: Timestamp,
    /// What it has not confirmed yet, in order: the writes as they came.
    /// Held back while a catch-up runs, since a live delete applied before
    /// the older row the catch-up carries would let the row come back.
    pub backlog: VecDeque<Arc<ShipItem>>,
    /// The catch-up chunk in flight, sent before anything in the backlog.
    pub catchup: VecDeque<Arc<ShipItem>>,
    pub last_error: Option<String>,
    /// Marks sent for a catch-up chunk are in the backlog too; when the
    /// caught-up mark is confirmed the follower is live.
    pub caught_up_mark: Option<Timestamp>,
}

/// Items a follower's backlog may hold before it is sent back to a catch-up.
pub const BACKLOG_CAP: usize = 100_000;
/// Items per frame.
pub const BATCH: usize = 500;

pub struct Shipper {
    pub collection: String,
    pub shard: usize,
    pub term: u64,
    pub sync: bool,
    inner: Mutex<Vec<Follower>>,
    cv: Condvar,
    stop: AtomicBool,
}

/// One follower's standing, for `SHOW HEALTH`.
#[derive(Clone, Debug)]
pub struct FollowerReport {
    pub url: String,
    pub state: String,
    pub acked: Timestamp,
    pub backlog: usize,
    pub last_error: Option<String>,
}

impl Shipper {
    pub fn new(
        collection: &str,
        shard: usize,
        term: u64,
        followers: Vec<(String, Arc<crate::wire::Node>)>,
        sync: bool,
    ) -> Arc<Shipper> {
        let inner = followers
            .into_iter()
            .map(|(url, node)| Follower {
                url,
                node,
                state: FollowerState::Unknown,
                acked: 0,
                backlog: VecDeque::new(),
                catchup: VecDeque::new(),
                last_error: None,
                caught_up_mark: None,
            })
            .collect();
        let s = Arc::new(Shipper {
            collection: collection.to_string(),
            shard,
            term,
            sync,
            inner: Mutex::new(inner),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let t = s.clone();
        std::thread::Builder::new()
            .name(format!("ship-{collection}-{shard}"))
            .spawn(move || t.run())
            .expect("a thread for the shipper");
        s
    }

    /// A shipper with no followers and no thread: what a shard without
    /// followers is given for a moment while its map is compared.
    pub fn idle() -> Shipper {
        Shipper {
            collection: String::new(),
            shard: 0,
            term: 0,
            sync: true,
            inner: Mutex::new(Vec::new()),
            cv: Condvar::new(),
            stop: AtomicBool::new(true),
        }
    }

    pub fn followers(&self) -> Vec<String> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).iter().map(|f| f.url.clone()).collect()
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.cv.notify_all();
    }

    /// A record the shard just logged, for every follower.
    pub fn push(&self, item: ShipItem) {
        let item = Arc::new(item);
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for f in g.iter_mut() {
            // A follower not yet asked where it stands is caught up from
            // there when it answers; nothing is kept for it meanwhile.
            if f.state == FollowerState::Unknown {
                continue;
            }
            if f.backlog.len() >= BACKLOG_CAP {
                f.backlog.clear();
                f.catchup.clear();
                f.state = FollowerState::Unknown;
                f.last_error = Some("backlog over its cap; catching up again".into());
                continue;
            }
            f.backlog.push_back(item.clone());
        }
        drop(g);
        self.cv.notify_all();
    }

    /// A catch-up chunk for one follower, the engine's step cutting it.
    pub fn push_catchup(
        &self,
        url: &str,
        items: Vec<ShipItem>,
        cursor: Option<String>,
        done: bool,
    ) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(f) = g.iter_mut().find(|f| f.url == url) {
            if let FollowerState::CatchingUp { cursor: c, done: d, upto, .. } = &mut f.state {
                *c = cursor;
                *d = done;
                if done {
                    f.caught_up_mark = Some(*upto);
                }
            }
            for it in items {
                f.catchup.push_back(Arc::new(it));
            }
        }
        drop(g);
        self.cv.notify_all();
    }

    /// The followers whose catch-up wants its next chunk: their state, with
    /// the backlog drained.
    pub fn catchups_due(&self) -> Vec<(String, FollowerState)> {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.iter()
            .filter(|f| {
                matches!(f.state, FollowerState::CatchingUp { done: false, .. })
                    && f.catchup.is_empty()
            })
            .map(|f| (f.url.clone(), f.state.clone()))
            .collect()
    }

    /// Fix the instant a follower's catch-up covers, once, when its first
    /// chunk is cut.
    pub fn fix_upto(&self, url: &str, now: Timestamp) -> Option<FollowerState> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let f = g.iter_mut().find(|f| f.url == url)?;
        if let FollowerState::CatchingUp { upto, .. } = &mut f.state {
            if *upto == 0 {
                *upto = now;
            }
        }
        Some(f.state.clone())
    }

    /// Wait until every follower confirmed `ts`, or the budget runs out.
    pub fn wait(&self, ts: Timestamp, budget: Option<u64>) -> Result<()> {
        if !self.sync {
            return Ok(());
        }
        let deadline = budget.map(|ms| Instant::now() + Duration::from_millis(ms));
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            // A follower that is away (not yet asked, or asked and silent)
            // or still catching up does not hold the acknowledgement: the
            // write is on this disk alone until the follower is live, which
            // `SHOW HEALTH` says, and it reaches the follower behind its
            // catch-up. Only a live follower has to confirm. (A catching-up
            // one held it, and a node back from a minute away held every
            // write to the shards it follows for the minutes its copy took.)
            if g.iter().all(|f| f.state != FollowerState::Live || f.acked >= ts) {
                return Ok(());
            }
            let wait_for = match deadline {
                Some(d) => match d.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => left,
                    _ => {
                        let behind: Vec<String> = g
                            .iter()
                            .filter(|f| f.acked < ts)
                            .map(|f| match &f.last_error {
                                Some(e) => format!("{} ({e})", f.url),
                                None => f.url.clone(),
                            })
                            .collect();
                        return Err(Error::Deadline(format!(
                            "shard {} of `{}` written here at ts {ts}, NOT confirmed on {} within \
                             the deadline: the write is on this node's disk and will reach the \
                             follower when it answers; retry to be sure",
                            self.shard,
                            self.collection,
                            behind.join(", ")
                        )));
                    }
                },
                None => Duration::from_secs(3600),
            };
            g = self.cv.wait_timeout(g, wait_for).unwrap_or_else(|p| p.into_inner()).0;
        }
    }

    pub fn report(&self) -> Vec<FollowerReport> {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.iter()
            .map(|f| FollowerReport {
                url: f.url.clone(),
                state: match &f.state {
                    FollowerState::Unknown => "asking".into(),
                    FollowerState::CatchingUp { reset, .. } => {
                        if *reset {
                            "copying from nothing".into()
                        } else {
                            "catching up".into()
                        }
                    }
                    FollowerState::Live => "live".into(),
                },
                acked: f.acked,
                backlog: f.backlog.len() + f.catchup.len(),
                last_error: f.last_error.clone(),
            })
            .collect()
    }

    fn run(self: Arc<Self>) {
        let mut backoff = Duration::from_millis(200);
        loop {
            if self.stop.load(Ordering::Acquire) {
                return;
            }
            // What to do, decided under the lock; done without it.
            enum Work {
                Ask(String, Arc<crate::wire::Node>),
                Send(String, Arc<crate::wire::Node>, Vec<Arc<ShipItem>>),
            }
            let work: Vec<Work> = {
                let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                g.iter()
                    .filter_map(|f| match &f.state {
                        FollowerState::Unknown => Some(Work::Ask(f.url.clone(), f.node.clone())),
                        FollowerState::CatchingUp { .. } if !f.catchup.is_empty() => {
                            Some(Work::Send(
                                f.url.clone(),
                                f.node.clone(),
                                f.catchup.iter().take(BATCH).cloned().collect(),
                            ))
                        }
                        FollowerState::Live if !f.backlog.is_empty() => Some(Work::Send(
                            f.url.clone(),
                            f.node.clone(),
                            f.backlog.iter().take(BATCH).cloned().collect(),
                        )),
                        _ => None,
                    })
                    .collect()
            };
            if work.is_empty() {
                let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                let _ = self.cv.wait_timeout(g, Duration::from_millis(500));
                continue;
            }
            let mut failed = false;
            for w in work {
                let _deadline = crate::deadline::arm(Some(10_000));
                match w {
                    Work::Ask(url, node) => {
                        match node.ship_status(&self.collection, self.shard, self.term) {
                            Ok((caught_up, at)) => {
                                let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                                if let Some(f) = g.iter_mut().find(|f| f.url == url) {
                                    f.state = FollowerState::CatchingUp {
                                        from: if caught_up { at } else { 0 },
                                        upto: 0,
                                        cursor: None,
                                        reset: !caught_up,
                                        done: false,
                                    };
                                    f.last_error = None;
                                }
                            }
                            Err(e) => {
                                failed = true;
                                self.note_error(&url, &e);
                                self.cv.notify_all();
                            }
                        }
                    }
                    Work::Send(url, node, items) => {
                        let plain: Vec<&ShipItem> = items.iter().map(|i| i.as_ref()).collect();
                        match node.ship(&self.collection, self.shard, self.term, &plain) {
                            Ok((caught_up, at)) => {
                                let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                                if let Some(f) = g.iter_mut().find(|f| f.url == url) {
                                    let queue = if f.state == FollowerState::Live {
                                        &mut f.backlog
                                    } else {
                                        &mut f.catchup
                                    };
                                    for _ in 0..items.len() {
                                        queue.pop_front();
                                    }
                                    f.acked = f.acked.max(at);
                                    f.last_error = None;
                                    if caught_up
                                        && matches!(
                                            f.state,
                                            FollowerState::CatchingUp { done: true, .. }
                                        )
                                        && f.caught_up_mark.is_some_and(|m| at >= m)
                                    {
                                        f.state = FollowerState::Live;
                                        f.caught_up_mark = None;
                                    }
                                }
                                self.cv.notify_all();
                            }
                            Err(e) => {
                                failed = true;
                                // A follower that does not answer, or answers
                                // with another term or no copy, is asked again
                                // where it stands when it answers: what it
                                // missed meanwhile is caught up from there.
                                let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
                                if let Some(f) = g.iter_mut().find(|f| f.url == url) {
                                    f.state = FollowerState::Unknown;
                                    f.backlog.clear();
                                    f.catchup.clear();
                                    f.caught_up_mark = None;
                                }
                                drop(g);
                                self.note_error(&url, &e);
                                self.cv.notify_all();
                            }
                        }
                    }
                }
            }
            if failed {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(5));
            } else {
                backoff = Duration::from_millis(200);
            }
        }
    }

    fn note_error(&self, url: &str, e: &Error) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(f) = g.iter_mut().find(|f| f.url == url) {
            f.last_error = Some(e.to_string());
        }
    }
}

impl Drop for Shipper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only a live follower holds the acknowledgement: one not yet asked,
    /// or catching up, leaves the write on this disk alone, said by the
    /// health; a live one behind the write is waited for, to the budget.
    #[test]
    fn a_follower_away_or_catching_up_does_not_hold_the_acknowledgement() {
        let node = Arc::new(crate::wire::Node::new("tcp://127.0.0.1:1", Some("t"), None).unwrap());
        let sh = Shipper::new("items", 0, 0, vec![("tcp://127.0.0.1:1".into(), node)], true);
        let set = |state: FollowerState| {
            let mut g = sh.inner.lock().unwrap();
            g[0].state = state;
            g[0].acked = 10;
        };
        let t0 = Instant::now();
        set(FollowerState::Unknown);
        sh.wait(20, Some(2000)).unwrap();
        set(FollowerState::CatchingUp { from: 0, upto: 0, cursor: None, reset: true, done: false });
        sh.wait(20, Some(2000)).unwrap();
        assert!(t0.elapsed() < Duration::from_millis(500), "an away follower held the write");
        set(FollowerState::Live);
        sh.wait(10, Some(2000)).unwrap();
        let e = sh.wait(20, Some(200)).unwrap_err().to_string();
        assert!(e.contains("NOT confirmed"), "{e}");
        assert!(t0.elapsed() >= Duration::from_millis(200));
        sh.stop();
    }
}
