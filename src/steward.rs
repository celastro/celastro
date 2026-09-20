//! The steward by election (HA1-3): a small group of nodes agrees on
//! which of them hands out leases and promotes followers, with a term,
//! so that the steward is no single point and a steward cut off from
//! the majority stops acting as one.
//!
//! No replicated log: the map is already fenced by shard term, and a
//! steward's term only has to order stewards. The election is the
//! familiar one -- a node that has heard no steward for a timeout asks
//! the group for votes at the next term, a majority makes it steward,
//! the heartbeats are the lease renewals it sends anyway, and a node
//! that sees a higher term tells the sender, which steps down. Two
//! stewards cannot both hold a majority in one term. What keeps two
//! *holders* from taking writes is the lease: a steward that cannot
//! reach a majority for half a lease stops renewing, so the leases on
//! its side run out within a lease; a new steward promotes nothing
//! for a lease and a quarter after its election, by which time every
//! lease the old one granted has run out -- given an election timeout
//! of at least half a lease and clocks that run at comparable rates.
//!
//! [`Election`] is a pure state machine: time comes in as an argument,
//! messages come in, actions come out, and the tests drive it through
//! partitions and drops deterministically. `serve` runs it on a thread
//! and carries the actions over the wire.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Steward,
}

/// What travels between the group's nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// Asks whether a vote at `term` would be given, changing nothing on
    /// either side: a node cut off from the group asks every timeout and
    /// hears no, so its term never climbs, and when it is back it takes
    /// the steward's heartbeat instead of forcing an election with a term
    /// it inflated alone.
    PreVote {
        term: u64,
        candidate: String,
    },
    PreVoteAnswer {
        term: u64,
        granted: bool,
    },
    /// Asks for a vote at `term`.
    Vote {
        term: u64,
        candidate: String,
    },
    /// A vote's answer, and the answerer's term either way.
    VoteAnswer {
        term: u64,
        granted: bool,
    },
    /// A steward's heartbeat (the lease renewal carries it).
    Heartbeat {
        term: u64,
    },
    /// A heartbeat's answer: accepted, or refused with the higher term.
    HeartbeatAnswer {
        term: u64,
        accepted: bool,
    },
}

/// What the machine wants done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send {
        to: String,
        msg: Msg,
    },
    /// The term and vote to write down before anything is sent.
    Persist {
        term: u64,
        voted_for: Option<String>,
    },
    BecameSteward {
        term: u64,
    },
    SteppedDown {
        term: u64,
    },
}

pub struct Election {
    me: String,
    group: Vec<String>,
    term: u64,
    voted_for: Option<String>,
    role: Role,
    /// Who leads `term`, as far as this node has heard.
    steward: Option<String>,
    votes: BTreeSet<String>,
    /// The pre-votes gathered for the next term, while asking.
    prevotes: BTreeSet<String>,
    /// The last heartbeat accepted from a steward, or vote granted to a
    /// candidate: what the election timeout counts from.
    heard: Instant,
    /// A steward's: the peers that accepted its last round, and when a
    /// round last reached a majority.
    round: BTreeSet<String>,
    majority_at: Instant,
    became_steward: Option<Instant>,
    lease: Duration,
    timeout: Duration,
}

impl Election {
    /// `group` includes `me`. `term` and `voted_for` as persisted, or
    /// zero and none. `timeout` is the election timeout, at least half
    /// the lease; a caller adds its own random slice.
    pub fn new(
        me: &str,
        group: &[String],
        term: u64,
        voted_for: Option<String>,
        lease: Duration,
        timeout: Duration,
        now: Instant,
    ) -> Election {
        let mut group = group.to_vec();
        if !group.iter().any(|g| g == me) {
            group.push(me.to_string());
        }
        group.sort();
        group.dedup();
        Election {
            me: me.to_string(),
            group,
            term,
            voted_for,
            role: Role::Follower,
            steward: None,
            votes: BTreeSet::new(),
            prevotes: BTreeSet::new(),
            heard: now,
            round: BTreeSet::new(),
            majority_at: now,
            became_steward: None,
            lease,
            timeout: timeout.max(lease / 2),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn steward(&self) -> Option<&str> {
        self.steward.as_deref()
    }

    pub fn group(&self) -> &[String] {
        &self.group
    }

    fn majority(&self) -> usize {
        self.group.len() / 2 + 1
    }

    fn peers(&self) -> impl Iterator<Item = &String> {
        self.group.iter().filter(move |g| **g != self.me)
    }

    /// How long a new steward holds off promotions: a lease and a
    /// quarter after its election, by when every lease the previous
    /// steward granted has run out.
    pub fn promotion_holdoff(&self) -> Duration {
        self.lease + self.lease / 4
    }

    /// Since when this node has been steward, if it is.
    pub fn steward_since(&self) -> Option<Instant> {
        self.became_steward
    }

    fn step_down(&mut self, term: u64, out: &mut Vec<Action>) {
        let was = self.role;
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            out.push(Action::Persist { term: self.term, voted_for: None });
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.prevotes.clear();
        self.round.clear();
        self.became_steward = None;
        if was == Role::Steward {
            out.push(Action::SteppedDown { term: self.term });
        }
    }

    /// Nothing heard from a steward for the timeout, and no fresh
    /// majority of its own if it is one.
    fn quiet(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.heard) >= self.timeout
    }

    /// The pre-votes are in: the real candidacy, at the next term.
    fn stand(&mut self, now: Instant, out: &mut Vec<Action>) {
        self.term += 1;
        self.voted_for = Some(self.me.clone());
        self.role = Role::Candidate;
        self.steward = None;
        self.votes.clear();
        self.prevotes.clear();
        self.votes.insert(self.me.clone());
        self.heard = now;
        out.push(Action::Persist { term: self.term, voted_for: Some(self.me.clone()) });
        if self.votes.len() >= self.majority() {
            self.become_steward(now, out);
            return;
        }
        for p in self.peers() {
            out.push(Action::Send {
                to: p.clone(),
                msg: Msg::Vote { term: self.term, candidate: self.me.clone() },
            });
        }
    }

    /// The clock moved: a follower that has heard nothing for the
    /// timeout stands; a candidate that got no majority stands again; a
    /// steward heartbeats, and one that has not reached a majority for
    /// half a lease steps down.
    pub fn tick(&mut self, now: Instant) -> Vec<Action> {
        let mut out = Vec::new();
        match self.role {
            Role::Steward => {
                if now.saturating_duration_since(self.majority_at) >= self.lease / 2 {
                    self.step_down(self.term, &mut out);
                    return out;
                }
                // A new round: the answers land through `on_message`.
                self.round.clear();
                self.round.insert(self.me.clone());
                for p in self.peers() {
                    out.push(Action::Send {
                        to: p.clone(),
                        msg: Msg::Heartbeat { term: self.term },
                    });
                }
            }
            Role::Follower | Role::Candidate => {
                if !self.quiet(now) {
                    return out;
                }
                // Ask first whether the group would vote: a node alone
                // gets no answer and stays at its term.
                self.heard = now;
                self.prevotes.clear();
                self.prevotes.insert(self.me.clone());
                if self.prevotes.len() >= self.majority() {
                    self.stand(now, &mut out);
                    return out;
                }
                for p in self.peers() {
                    out.push(Action::Send {
                        to: p.clone(),
                        msg: Msg::PreVote { term: self.term + 1, candidate: self.me.clone() },
                    });
                }
            }
        }
        out
    }

    fn become_steward(&mut self, now: Instant, out: &mut Vec<Action>) {
        self.role = Role::Steward;
        self.steward = Some(self.me.clone());
        self.majority_at = now;
        self.heard = now;
        self.became_steward = Some(now);
        self.round.clear();
        self.round.insert(self.me.clone());
        out.push(Action::BecameSteward { term: self.term });
        for p in self.peers() {
            out.push(Action::Send { to: p.clone(), msg: Msg::Heartbeat { term: self.term } });
        }
    }

    /// A message from `from`. Answers to send, and what to persist,
    /// come back as actions.
    pub fn on_message(&mut self, now: Instant, from: &str, msg: Msg) -> Vec<Action> {
        let mut out = Vec::new();
        match msg {
            Msg::PreVote { term, candidate: _ } => {
                // Would be granted: a term at least this node's, and no
                // steward heard for the timeout. Nothing changes here.
                let granted = term >= self.term && self.role != Role::Steward && self.quiet(now);
                out.push(Action::Send {
                    to: from.to_string(),
                    msg: Msg::PreVoteAnswer { term: self.term, granted },
                });
            }
            Msg::PreVoteAnswer { term, granted } => {
                if term > self.term {
                    self.step_down(term, &mut out);
                    return out;
                }
                if self.role != Role::Steward && granted && !self.prevotes.is_empty() {
                    self.prevotes.insert(from.to_string());
                    if self.prevotes.len() >= self.majority() {
                        self.stand(now, &mut out);
                    }
                }
            }
            Msg::Vote { term, candidate } => {
                if term > self.term {
                    self.step_down(term, &mut out);
                }
                // Granted to the first candidate of the current term, and
                // only when no steward has been heard for the timeout: a
                // steward that is alive keeps its group from wandering.
                let quiet = self.quiet(now) || self.steward.is_none();
                let granted = term == self.term
                    && self.role != Role::Steward
                    && quiet
                    && self.voted_for.as_deref().map_or(true, |v| v == candidate);
                if granted {
                    self.voted_for = Some(candidate.clone());
                    self.heard = now;
                    out.push(Action::Persist {
                        term: self.term,
                        voted_for: Some(candidate.clone()),
                    });
                }
                out.push(Action::Send {
                    to: from.to_string(),
                    msg: Msg::VoteAnswer { term: self.term, granted },
                });
            }
            Msg::VoteAnswer { term, granted } => {
                if term > self.term {
                    self.step_down(term, &mut out);
                    return out;
                }
                if self.role == Role::Candidate && term == self.term && granted {
                    self.votes.insert(from.to_string());
                    if self.votes.len() >= self.majority() {
                        self.become_steward(now, &mut out);
                    }
                }
            }
            Msg::Heartbeat { term } => {
                if term > self.term || (term == self.term && self.role != Role::Steward) {
                    if term > self.term {
                        self.step_down(term, &mut out);
                    } else if self.role == Role::Candidate {
                        self.role = Role::Follower;
                        self.votes.clear();
                    }
                    self.steward = Some(from.to_string());
                    self.heard = now;
                    out.push(Action::Send {
                        to: from.to_string(),
                        msg: Msg::HeartbeatAnswer { term: self.term, accepted: true },
                    });
                } else {
                    out.push(Action::Send {
                        to: from.to_string(),
                        msg: Msg::HeartbeatAnswer { term: self.term, accepted: false },
                    });
                }
            }
            Msg::HeartbeatAnswer { term, accepted } => {
                if term > self.term {
                    self.step_down(term, &mut out);
                    return out;
                }
                if self.role == Role::Steward && accepted && term == self.term {
                    self.round.insert(from.to_string());
                    if self.round.len() >= self.majority() {
                        self.majority_at = now;
                        self.heard = now;
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Three nodes driven by hand: every `Send` is delivered unless the
    /// link is cut, in the order the actions came.
    struct Group {
        nodes: BTreeMap<String, Election>,
        cut: BTreeSet<(String, String)>,
        t0: Instant,
    }

    impl Group {
        fn new(n: usize, lease: Duration) -> Group {
            let t0 = Instant::now();
            let names: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
            let nodes = names
                .iter()
                .map(|m| (m.clone(), Election::new(m, &names, 0, None, lease, lease / 2, t0)))
                .collect();
            Group { nodes, cut: BTreeSet::new(), t0 }
        }

        fn at(&self, secs: u64) -> Instant {
            self.t0 + Duration::from_secs(secs)
        }

        fn deliver(&mut self, now: Instant, from: &str, actions: Vec<Action>) -> Vec<Action> {
            let mut notable = Vec::new();
            let mut queue: Vec<(String, Action)> =
                actions.into_iter().map(|a| (from.to_string(), a)).collect();
            while let Some((sender, a)) = queue.pop() {
                match a {
                    Action::Send { to, msg } => {
                        if self.cut.contains(&(sender.clone(), to.clone()))
                            || self.cut.contains(&(to.clone(), sender.clone()))
                        {
                            continue;
                        }
                        let more = self.nodes.get_mut(&to).unwrap().on_message(now, &sender, msg);
                        queue.extend(more.into_iter().map(|a| (to.clone(), a)));
                    }
                    other => notable.push(other),
                }
            }
            notable
        }

        fn tick(&mut self, secs: u64, who: &str) -> Vec<Action> {
            let now = self.at(secs);
            let actions = self.nodes.get_mut(who).unwrap().tick(now);
            self.deliver(now, who, actions)
        }

        fn stewards(&self) -> Vec<String> {
            self.nodes
                .iter()
                .filter(|(_, e)| e.role() == Role::Steward)
                .map(|(n, _)| n.clone())
                .collect()
        }
    }

    /// The first node whose timeout runs out stands, gets the votes and
    /// is the steward; its heartbeats keep the others from standing.
    #[test]
    fn a_group_elects_one_steward_and_its_heartbeats_hold_the_rest() {
        let mut g = Group::new(3, Duration::from_secs(10));
        assert!(g.tick(3, "n1").is_empty(), "nothing stands before the timeout");
        let notable = g.tick(6, "n1");
        assert!(notable.contains(&Action::BecameSteward { term: 1 }), "{notable:?}");
        assert_eq!(g.stewards(), vec!["n1".to_string()]);
        assert_eq!(g.nodes["n0"].steward(), Some("n1"));
        assert_eq!(g.nodes["n2"].term(), 1);
        // n0 ticks at 9: it heard n1 at 6, so it stays put.
        assert!(g.tick(9, "n0").is_empty());
        // Heartbeats every tick, answered: the majority is fresh.
        g.tick(9, "n1");
        assert_eq!(g.stewards(), vec!["n1".to_string()]);
    }

    /// A steward cut off from the majority stops within half a lease;
    /// the majority elects another; the old one, healed, hears the
    /// higher term and follows.
    #[test]
    fn a_steward_cut_off_steps_down_and_the_majority_elects_another() {
        let mut g = Group::new(3, Duration::from_secs(10));
        g.tick(6, "n1");
        assert_eq!(g.stewards(), vec!["n1".to_string()]);
        g.cut.insert(("n1".into(), "n0".into()));
        g.cut.insert(("n1".into(), "n2".into()));
        // Its rounds reach nobody; after half a lease it steps down.
        assert!(g.tick(8, "n1").is_empty());
        let n = g.tick(12, "n1");
        assert!(n.contains(&Action::SteppedDown { term: 1 }), "{n:?}");
        assert!(g.stewards().is_empty());
        // n2 has heard nothing since 6: at 12 it stands and n0 votes.
        let n = g.tick(12, "n2");
        assert!(n.contains(&Action::BecameSteward { term: 2 }), "{n:?}");
        assert_eq!(g.stewards(), vec!["n2".to_string()]);
        // n2 heartbeats every quarter lease and keeps its majority.
        g.tick(14, "n2");
        // Healed: n2's next heartbeat reaches n1, which hears term 2,
        // follows n2 and does not stand at its own timeout.
        g.cut.clear();
        g.tick(17, "n2");
        assert!(g.tick(18, "n1").is_empty(), "n1 stood against a live steward");
        assert!(g.nodes["n1"].role() != Role::Steward);
        g.tick(19, "n2");
        assert_eq!(g.stewards(), vec!["n2".to_string()]);
        assert_eq!(g.nodes["n1"].steward(), Some("n2"));
        assert!(g.nodes["n1"].term() >= 2);
    }

    /// Two candidates in one term split the votes; nobody is steward
    /// until one of them stands again at a later term.
    #[test]
    fn a_split_vote_elects_nobody_until_the_next_term() {
        let mut g = Group::new(3, Duration::from_secs(10));
        // n0 and n2 stand at once: each votes for itself; n1 votes for
        // whichever asks first (n0, delivered first).
        g.cut.insert(("n0".into(), "n2".into()));
        let now = g.at(6);
        let a = g.nodes.get_mut("n0").unwrap().tick(now);
        let b = g.nodes.get_mut("n2").unwrap().tick(now);
        let n0 = g.deliver(now, "n0", a);
        let n2 = g.deliver(now, "n2", b);
        // n0's pre-votes and votes reached n1: a majority of two. n2's
        // pre-vote was granted too (nothing had changed on n1 yet), but
        // its vote found n1 voted: denied.
        assert!(n0.contains(&Action::BecameSteward { term: 1 }), "{n0:?}");
        assert!(!n2.iter().any(|a| matches!(a, Action::BecameSteward { .. })), "{n2:?}");
        assert_eq!(g.stewards(), vec!["n0".to_string()]);
    }

    /// A node cut off from the group asks every timeout and gets no
    /// pre-vote, so its term stays where it was; back, it takes the
    /// steward's next heartbeat and forces no election.
    #[test]
    fn a_node_cut_off_keeps_its_term_and_follows_again_when_back() {
        let mut g = Group::new(3, Duration::from_secs(10));
        g.tick(6, "n1");
        assert_eq!(g.stewards(), vec!["n1".to_string()]);
        g.cut.insert(("n0".into(), "n1".into()));
        g.cut.insert(("n0".into(), "n2".into()));
        for t in [9, 12, 15, 18, 21, 24, 27, 30] {
            g.tick(t, "n1");
        }
        for t in [12, 18, 24, 30] {
            let n = g.tick(t, "n0");
            assert!(!n.iter().any(|a| matches!(a, Action::Persist { .. })), "{n:?}");
        }
        assert_eq!(g.nodes["n0"].term(), 1, "an isolated node raised its term");
        assert_eq!(g.stewards(), vec!["n1".to_string()]);
        g.cut.clear();
        g.tick(33, "n1");
        assert_eq!(g.nodes["n0"].steward(), Some("n1"));
        assert_eq!(g.nodes["n0"].role(), Role::Follower);
        assert!(g.tick(34, "n0").is_empty(), "the healed node stood against a live steward");
        assert_eq!(g.nodes["n1"].term(), 1, "the steward's term moved");
    }

    /// The persisted term and vote come back on restart, and a restarted
    /// node grants no second vote in the term it already voted in.
    #[test]
    fn a_vote_survives_a_restart_of_the_voter() {
        let t0 = Instant::now();
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let lease = Duration::from_secs(10);
        let mut b = Election::new("b", &names, 0, None, lease, lease / 2, t0);
        let out = b.on_message(t0, "a", Msg::Vote { term: 1, candidate: "a".into() });
        assert!(out.contains(&Action::Persist { term: 1, voted_for: Some("a".into()) }));
        // Restarted with what it persisted: c's ask at term 1 is denied.
        let mut b = Election::new("b", &names, 1, Some("a".into()), lease, lease / 2, t0);
        let out = b.on_message(t0, "c", Msg::Vote { term: 1, candidate: "c".into() });
        assert!(out.contains(&Action::Send {
            to: "c".into(),
            msg: Msg::VoteAnswer { term: 1, granted: false }
        }));
        // At term 2 it may vote again.
        let out = b.on_message(t0, "c", Msg::Vote { term: 2, candidate: "c".into() });
        assert!(out.contains(&Action::Send {
            to: "c".into(),
            msg: Msg::VoteAnswer { term: 2, granted: true }
        }));
    }
}
