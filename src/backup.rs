//! Backup and restore: the shards a node holds, pinned at one instant and
//! copied to an object store -- a directory on any mount, or an
//! S3-compatible bucket -- and a database brought back from one.
//!
//! **The layout under a destination.** `pool/<collection>/shard-NNNN/<id>.seg`
//! holds every sealed segment any backup needed, once: segment ids never
//! recur within a shard (the manifest's counter only grows), so a pool
//! object that is there at the file's size is the file, and one there at
//! another size is refused rather than trusted. `nodes/<node>/backups/<ts>/`
//! holds what one backup owns -- `CATALOG`, and per shard `RANGE`,
//! `MANIFEST`, the delete logs and the segment sealed from the memtable at
//! the pin -- and, written last, its `BACKUP` record naming every object
//! the backup needs with its size. A backup with no record did not finish
//! and is not one; `nodes/<node>/LATEST` names the newest that did. The
//! node is `CELASTRO_NODE` with its scheme and punctuation folded
//! (`celastro-0.celastro_7876`), or `local` for a node with no address, so
//! the pods of a cluster back up side by side into one destination and
//! each restores its own by default. A restore reads the record, checks
//! every object is there at its size, and only then writes.
//!
//! **What is pinned and what is not.** The pin is `export_shard`'s: the
//! sealed segments by handle (an `Arc`, so a compaction cannot unlink them
//! under the copy), the delete logs as they stand, and the rows in memory
//! visible at the instant, sealed into one segment. Everything after the
//! instant is the source's business. The copy runs after the statement
//! returned its [`Deferred`] work and the caller
//! let go of the lock; the node answers other statements meanwhile.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crypto::hex;
use crate::crypto::sha2::sha256;
use crate::engine::{handle_bytes, Deferred, ExportShard, Outcome};
use crate::error::{Error, Result};
use crate::objstore::{ArchiveOpts, DirStore, ObjectStore, S3Store};
use crate::time::Timestamp;

/// What `BACKUP` pinned: per collection, the shards this node holds by
/// tablet index.
pub(crate) type Exported = Vec<(String, Vec<(usize, ExportShard)>)>;
/// One restored shard's files: (name under the shard directory, key, size,
/// SHA-256 as hex -- empty for a record written before checksums).
pub(crate) type ShardFiles = Vec<(String, String, u64, String)>;
/// A backup's files per collection and tablet index.
pub(crate) type BackupShards = Vec<(String, Vec<(usize, ShardFiles)>)>;

/// Where a backup goes or comes from.
#[derive(Clone)]
pub(crate) struct Target {
    pub(crate) store: Arc<dyn ObjectStore>,
    /// Prepended to every key; empty or ending in `/`.
    pub(crate) prefix: String,
    pub(crate) display: String,
}

impl Target {
    fn key(&self, rest: &str) -> String {
        format!("{}{rest}", self.prefix)
    }
}

/// Resolve `dest`: `s3://bucket/prefix` through the archive's endpoint,
/// region and credentials; otherwise a directory, relative to `backup_dir`
/// when there is one and confined to it, absolute when there is none.
pub(crate) fn target(
    archive: &ArchiveOpts,
    backup_dir: Option<&Path>,
    dest: &str,
) -> Result<Target> {
    if let Some(rest) = dest.strip_prefix("s3://") {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
            None => (rest.to_string(), String::new()),
        };
        if bucket.is_empty() {
            return Err(Error::Plan(
                "an s3:// destination names a bucket: s3://bucket/prefix".into(),
            ));
        }
        if archive.endpoint.is_none() {
            return Err(Error::Plan(
                "an s3:// destination needs CELASTRO_ARCHIVE_ENDPOINT (and the AWS_* credentials); \
                 the bucket in the destination is used, the archive's own is not"
                    .into(),
            ));
        }
        let mut opts = archive.clone();
        opts.bucket = bucket.clone();
        let store = S3Store::from_env(&opts)?;
        let prefix = if prefix.is_empty() { String::new() } else { format!("{prefix}/") };
        let display = format!("s3://{bucket}/{prefix}");
        return Ok(Target { store: Arc::new(store), prefix, display });
    }
    let path = Path::new(dest);
    let root: PathBuf = match backup_dir {
        Some(base) => {
            if path.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
                return Err(Error::Plan(format!("a backup path may not climb with `..`: {dest}")));
            }
            let joined = if path.is_absolute() { path.to_path_buf() } else { base.join(path) };
            if !joined.starts_with(base) {
                return Err(Error::Plan(format!(
                    "{dest} is outside the backup directory {}; a name under it, or a path into it",
                    base.display()
                )));
            }
            joined
        }
        None => {
            if !path.is_absolute() {
                return Err(Error::Plan(format!(
                    "{dest}: a backup path is absolute, or a name under CELASTRO_BACKUP_DIR when that is set"
                )));
            }
            path.to_path_buf()
        }
    };
    let display = root.display().to_string();
    Ok(Target { store: Arc::new(DirStore::new(&root)?), prefix: String::new(), display })
}

fn ts_key(ts: u64) -> String {
    format!("{ts:020}")
}

/// The key component a node's address becomes: `tcp://celastro-0.celastro:7876`
/// is `celastro-0.celastro_7876`, and no address is `local`.
pub(crate) fn node_slug(node: &str) -> String {
    let bare = node.split_once("://").map(|(_, r)| r).unwrap_or(node);
    let slug: String = bare
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' { c } else { '_' })
        .collect();
    if slug.is_empty() {
        "local".to_string()
    } else {
        slug
    }
}

/// The work `BACKUP` leaves for after the lock.
pub(crate) fn job(
    target: Target,
    ts: u64,
    node: String,
    catalog: Vec<u8>,
    key: Option<Vec<u8>>,
    colls: Exported,
    keep: Option<usize>,
) -> Deferred {
    Deferred::new(move || run(target, ts, node, catalog, key, colls, keep))
}

/// Where a node's write-ahead logs go as they are rotated, for a restore
/// to any instant (`CELASTRO_LOG_ARCHIVE`, or `BACKUP LOG TO` for the
/// live log now): under the node, per shard, each log named by its
/// rotation's sequence and the instants it spans --
/// `nodes/<node>/logs/<collection>/shard-NNNN/<seq>-<first>-<last>.log`.
/// A restore `AS OF t` takes the newest backup at or before `t` and then
/// the logs after it, in sequence, and stops at a gap in the sequence: a
/// log that was never archived is a stretch the restore cannot claim.
pub struct LogArchive {
    pub(crate) target: Target,
    pub(crate) slug: String,
}

/// One archived log, by name.
pub(crate) struct ArchivedLog {
    pub key: String,
    pub timeline: u64,
    pub seq: u64,
    pub first: Timestamp,
    pub last: Timestamp,
    /// The log runs past its timeline's end: read only to `last`, the
    /// rest being what the child timeline took over.
    pub cut: bool,
}

/// The archived logs a restore replays for one shard, in order, and the
/// timeline the restored shard writes from then on.
pub(crate) struct Replay {
    pub logs: Vec<ArchivedLog>,
    /// A log on the chain begins after the instant asked: nothing happened
    /// between the last one replayed and the instant.
    pub more: bool,
    /// The timeline the instant asked falls in: the restored shard forks
    /// from it, when it first archives a log of its own.
    pub parent: u64,
}

impl LogArchive {
    pub(crate) fn open(
        archive: &ArchiveOpts,
        backup_dir: Option<&Path>,
        dest: &str,
        node: &str,
    ) -> Result<LogArchive> {
        Ok(LogArchive { target: target(archive, backup_dir, dest)?, slug: node_slug(node) })
    }

    pub(crate) fn display(&self) -> &str {
        &self.target.display
    }

    fn prefix(&self, collection: &str, shard: usize) -> String {
        format!("nodes/{}/logs/{collection}/shard-{shard:04}/", self.slug)
    }

    /// A log's name: its number and the instants it spans, under its
    /// timeline from the first fork on -- timeline 0's logs keep the name
    /// a node before 0.81.0 reads, so such a node restoring from here
    /// still reaches the instants before any fork.
    fn key(
        &self,
        collection: &str,
        shard: usize,
        timeline: u64,
        seq: u64,
        first: Timestamp,
        last: Timestamp,
    ) -> String {
        let p = self.prefix(collection, shard);
        if timeline == 0 {
            format!("{p}{seq:06}-{first:020}-{last:020}.log")
        } else {
            format!("{p}{timeline:04}-{seq:06}-{first:020}-{last:020}.log")
        }
    }

    /// The marker of a timeline: what it forked from, and at what instant.
    fn timeline_key(&self, collection: &str, shard: usize, timeline: u64) -> String {
        format!("{}timeline-{timeline:04}", self.prefix(collection, shard))
    }

    /// Open timeline `timeline` for the shard, forked from `parent` at
    /// `at`: a restore to an instant writes it before the restored shard
    /// archives anything.
    pub(crate) fn put_timeline(
        &self,
        collection: &str,
        shard: usize,
        timeline: u64,
        parent: u64,
        at: Timestamp,
    ) -> Result<()> {
        let key = self.target.key(&self.timeline_key(collection, shard, timeline));
        self.target.store.put(&key, format!("{parent} {at}\n").as_bytes())?;
        Ok(())
    }

    /// The number the shard's next timeline takes: past every one recorded.
    pub(crate) fn next_timeline(&self, collection: &str, shard: usize) -> Result<u64> {
        Ok(self.timelines(collection, shard)?.iter().map(|t| t.0 + 1).max().unwrap_or(1))
    }

    /// The timelines under the shard: `(timeline, parent, switch instant)`,
    /// timeline 0 implied.
    fn timelines(&self, collection: &str, shard: usize) -> Result<Vec<(u64, u64, Timestamp)>> {
        let prefix = self.target.key(&format!("{}timeline-", self.prefix(collection, shard)));
        let mut out = Vec::new();
        for key in self.target.store.list(&prefix)? {
            let Some(n) = key.rsplit("timeline-").next().and_then(|s| s.parse::<u64>().ok()) else {
                continue;
            };
            let text = String::from_utf8_lossy(&self.target.store.get(&key)?).to_string();
            let mut parts = text.split_whitespace();
            let parent = parts.next().and_then(|s| s.parse::<u64>().ok());
            let at = parts.next().and_then(|s| s.parse::<Timestamp>().ok());
            if let (Some(parent), Some(at)) = (parent, at) {
                out.push((n, parent, at));
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Copy one log under `seq`, as the bytes it is -- under the shard's
    /// cipher when there is one, which the backup's KEY opens at a restore
    /// -- replayed first for the instants it spans. Nothing for an empty
    /// log. The same `seq` written again replaces what was there: a live
    /// log's copy, then the rotated whole of it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn put_log(
        &self,
        collection: &str,
        shard: usize,
        timeline: u64,
        seq: u64,
        path: &Path,
        cipher: &crate::cipher::Shared,
        id: &str,
    ) -> Result<Option<(Timestamp, Timestamp)>> {
        let records = crate::shard::Wal::replay(path, cipher, id)?;
        let (Some(first), Some(last)) =
            (records.iter().map(|r| r.ts).min(), records.iter().map(|r| r.ts).max())
        else {
            return Ok(None);
        };
        let key = self.key(collection, shard, timeline, seq, first, last);
        self.target.store.put_file(&self.target.key(&key), path)?;
        Ok(Some((first, last)))
    }

    /// The archived logs of a shard past `covered` -- the instant a backup
    /// holds everything up to -- in sequence, and the number the next log
    /// archived for the shard must take so none already there is
    /// replaced. The first log claimed straddles the instant, or is the
    /// shard's first log, or follows one the backup holds whole; from
    /// there the sequence runs unbroken and stops at the first number
    /// missing: a log that was never archived is a stretch a restore
    /// cannot claim.
    /// The archived logs a restore to `upto` replays over a backup that
    /// holds everything to `covered`: along the chain of timelines the
    /// latest one descends from -- each timeline's logs between its switch
    /// instant and its child's, so an abandoned run's logs are read only
    /// before its fork -- and within a timeline in sequence, from the log
    /// that straddles the segment's start (or the timeline's first, or
    /// the one after a log held whole) to the first number missing.
    pub(crate) fn logs_after(
        &self,
        collection: &str,
        shard: usize,
        covered: Timestamp,
        upto: Timestamp,
    ) -> Result<Replay> {
        let prefix = self.target.key(&self.prefix(collection, shard));
        let mut logs: Vec<ArchivedLog> = Vec::new();
        for key in self.target.store.list(&prefix)? {
            let name = key.rsplit('/').next().unwrap_or("");
            let Some(stem) = name.strip_suffix(".log") else { continue };
            let parts: Vec<&str> = stem.split('-').collect();
            let (timeline, rest) = match parts.len() {
                3 => (Some(0u64), &parts[..]),
                4 => (parts[0].parse::<u64>().ok(), &parts[1..]),
                _ => continue,
            };
            let seq = rest[0].parse::<u64>().ok();
            let first = rest[1].parse::<Timestamp>().ok();
            let last = rest[2].parse::<Timestamp>().ok();
            if let (Some(timeline), Some(seq), Some(first), Some(last)) =
                (timeline, seq, first, last)
            {
                logs.push(ArchivedLog { key: key.clone(), timeline, seq, first, last, cut: false });
            }
        }
        // One log per number and timeline, the one that reaches furthest:
        // a live log's copy and the rotation of it share a number.
        logs.sort_by_key(|l| (l.timeline, l.seq, std::cmp::Reverse(l.last)));
        logs.dedup_by_key(|l| (l.timeline, l.seq));
        let timelines = self.timelines(collection, shard)?;
        // The chain: the latest timeline, its parent, and so on to 0; then
        // in order from 0, each with the instant it ends at (its child's
        // switch), the latest open-ended.
        let mut chain: Vec<(u64, Timestamp)> = Vec::new();
        let mut at = timelines.iter().map(|t| t.0).max().unwrap_or(0);
        let mut start_of: BTreeMap<u64, Timestamp> = BTreeMap::new();
        loop {
            let Some((_, parent, switch)) = timelines.iter().find(|t| t.0 == at).copied() else {
                chain.push((at, 0));
                break;
            };
            chain.push((at, switch));
            start_of.insert(at, switch);
            if chain.len() > timelines.len() + 1 {
                break;
            }
            at = parent;
        }
        chain.reverse();
        let mut out: Vec<ArchivedLog> = Vec::new();
        let mut more = false;
        let mut parent = 0;
        for (k, (timeline, starts)) in chain.iter().enumerate() {
            let ends = chain.get(k + 1).map(|c| c.1).unwrap_or(Timestamp::MAX);
            if *starts <= upto {
                parent = *timeline;
            }
            // This timeline's segment, and the part of it past the backup.
            let from = covered.max(*starts);
            if from >= ends || from >= upto {
                continue;
            }
            let mine: Vec<&ArchivedLog> = logs.iter().filter(|l| l.timeline == *timeline).collect();
            let held_whole = mine.iter().filter(|l| l.last <= from).map(|l| l.seq).max();
            if mine.iter().any(|l| l.first > upto && l.first <= ends) {
                more = true;
            }
            let mut taken: Vec<ArchivedLog> = Vec::new();
            for l in mine.into_iter().filter(|l| l.last > from && l.first <= ends.min(upto)) {
                let claimed = match taken.last() {
                    Some(prev) => l.seq == prev.seq + 1,
                    None => l.first <= from || l.seq == 1 || held_whole == Some(l.seq - 1),
                };
                if !claimed {
                    break;
                }
                taken.push(ArchivedLog {
                    key: l.key.clone(),
                    timeline: l.timeline,
                    seq: l.seq,
                    first: l.first,
                    // Read only to the segment's end: what the child
                    // timeline took over from there.
                    last: l.last.min(ends),
                    cut: l.last > ends,
                });
            }
            out.extend(taken);
        }
        Ok(Replay { logs: out, more, parent })
    }

    pub(crate) fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.target.store.get(key)
    }
}

/// What the last backup of this node recorded: object key to its size and
/// SHA-256, as hex.
///
/// A pool object is immutable -- a segment id never recurs within a shard,
/// and one present at another size is already refused -- so a hash recorded
/// for a key at a size is the hash of that key at that size for good. That
/// is what lets a backup fill the record for an object it is not copying
/// without reading it: today it reads and hashes every segment already in
/// the pool, which is the whole database on the second backup and every one
/// after, to upload nothing.
///
/// The hash means exactly what it meant before. It described the local file
/// then and describes the same bytes now, because the object was put from
/// that file and the file cannot change. Empty for a destination with no
/// previous backup, a record that cannot be read, or a version 1 record
/// with no hashes -- each of which falls back to reading the file.
/// The entries of a `BACKUP` record: each object's key, its size, and its
/// SHA-256 as hex -- empty in a version 1 record, which carried no hashes.
///
/// One parser, because there are two readers of this format and they used
/// to have one each: a restore, which must refuse a record it cannot
/// understand, and the hash recall below, which must not care. They can no
/// longer drift apart over what a line means.
fn record_entries(record: &[u8], whose: &str) -> Result<Vec<(String, u64, String)>> {
    let record = String::from_utf8_lossy(record).to_string();
    let mut lines = record.lines();
    if lines.next() != Some("celastro backup") {
        return Err(Error::Storage(format!("{whose} is not a backup record")));
    }
    let mut files = Vec::new();
    for line in lines {
        // A header line (`version 2`, `ts ...`, `node ...`) has no tab.
        let Some((key, rest)) = line.split_once('\t') else { continue };
        let (len, hash) = rest.split_once('\t').unwrap_or((rest, ""));
        let len = len
            .parse::<u64>()
            .map_err(|_| Error::Storage(format!("{whose}: `{line}` is not a file entry")))?;
        files.push((key.to_string(), len, hash.to_string()));
    }
    Ok(files)
}

fn previous_hashes(
    target: &Target,
    mine: &str,
) -> std::collections::HashMap<String, (u64, String)> {
    let mut out = std::collections::HashMap::new();
    let Ok(latest) = target.store.get(&target.key(&format!("{mine}LATEST"))) else {
        return out;
    };
    let Ok(ts) = String::from_utf8_lossy(&latest).trim().parse::<u64>() else {
        return out;
    };
    let Ok(record) = target.store.get(&target.key(&format!("{mine}backups/{}/BACKUP", ts_key(ts))))
    else {
        return out;
    };
    // A record that cannot be read is a record whose hashes are not used;
    // every key then falls back to being read off its file, as before.
    for (key, len, hash) in record_entries(&record, "the last backup's record").unwrap_or_default()
    {
        if !hash.is_empty() {
            out.insert(key, (len, hash));
        }
    }
    out
}

fn run(
    target: Target,
    ts: u64,
    node: String,
    catalog: Vec<u8>,
    key: Option<Vec<u8>>,
    colls: Exported,
    keep: Option<usize>,
) -> Result<Outcome> {
    let mine = format!("nodes/{}/", node_slug(&node));
    let own = format!("{mine}backups/{}/", ts_key(ts));
    let mut files: Vec<(String, u64, String)> = Vec::new();
    let (mut copied, mut present, mut bytes, mut shards) = (0usize, 0usize, 0u64, 0usize);
    // What the last backup of this node already knows, so a segment that is
    // in the pool at its size is not read again to fill in its hash.
    let known = previous_hashes(&target, &mine);
    let mut recalled = 0usize;
    let put = |key: String, data: &[u8], files: &mut Vec<(String, u64, String)>| -> Result<()> {
        target.store.put(&target.key(&key), data)?;
        files.push((key, data.len() as u64, hex(&sha256(data))));
        Ok(())
    };
    for (name, shard_list) in &colls {
        for (index, ex) in shard_list {
            shards += 1;
            let sdir = format!("{name}/shard-{index:04}");
            for h in &ex.sealed {
                let key = format!("pool/{sdir}/{:016x}.seg", h.id());
                // A segment on disk streams from its file, hashed on the
                // way: the copy holds no segment whole, so a node's memory
                // during a backup does not follow its largest segment. One
                // held as bytes (a memtable's, a remote tier's) goes as
                // bytes.
                let (len, hash) = match h.segment.source().path().map(|p| p.to_path_buf()) {
                    Some(path) => {
                        let len = std::fs::metadata(&path)?.len();
                        match target.store.size(&target.key(&key))? {
                            Some(n) if n == len => {
                                present += 1;
                                // The object is there at its size and is not
                                // being copied. Its hash is whatever the last
                                // record said it was, if that record said.
                                match known.get(&key) {
                                    Some((l, h)) if *l == len => {
                                        recalled += 1;
                                        (len, h.clone())
                                    }
                                    _ => {
                                        let (l, h) = crate::objstore::file_sha256(&path)?;
                                        (l, hex(&h))
                                    }
                                }
                            }
                            Some(n) => {
                                return Err(Error::Storage(format!(
                                    "backup: {} holds {key} at {n} bytes, the segment is {len}; a \
                                     segment id came back with other contents, which the pool \
                                     cannot hold",
                                    target.display
                                )))
                            }
                            None => {
                                let out = target.store.put_file(&target.key(&key), &path)?;
                                copied += 1;
                                bytes += out.0;
                                (out.0, hex(&out.1))
                            }
                        }
                    }
                    None => {
                        let data = handle_bytes(h)?;
                        match target.store.size(&target.key(&key))? {
                            Some(n) if n == data.len() as u64 => present += 1,
                            Some(n) => {
                                return Err(Error::Storage(format!(
                                    "backup: {} holds {key} at {n} bytes, the segment is {}; a \
                                     segment id came back with other contents, which the pool \
                                     cannot hold",
                                    target.display,
                                    data.len()
                                )))
                            }
                            None => {
                                target.store.put(&target.key(&key), &data)?;
                                copied += 1;
                                bytes += data.len() as u64;
                            }
                        }
                        (data.len() as u64, hex(&sha256(&data)))
                    }
                };
                files.push((key, len, hash));
            }
            for (id, log) in &ex.deletes {
                if let Some(data) = log {
                    put(format!("{own}{sdir}/deletes/{id:016x}.dlog"), data, &mut files)?;
                }
            }
            if let Some((id, data)) = &ex.fresh {
                put(format!("{own}{sdir}/segments/{id:016x}.seg"), data, &mut files)?;
                bytes += data.len() as u64;
            }
            put(format!("{own}{sdir}/RANGE"), &ex.range, &mut files)?;
            put(format!("{own}{sdir}/MANIFEST"), &ex.manifest, &mut files)?;
        }
    }
    put(format!("{own}CATALOG"), &catalog, &mut files)?;
    // The wrapped data key travels with an encrypted backup: every file
    // above is framed under it, and a restore anywhere needs it -- and the
    // master that wraps it -- before it can read a byte.
    if let Some(k) = &key {
        put(format!("{own}KEY"), k, &mut files)?;
    }
    // Version 2 of the record: a SHA-256 beside every size, so a restore
    // and VERIFY BACKUP can tell a damaged object from an intact one of the
    // same length. A version 1 record (no third field) is still read.
    let mut record = format!("celastro backup\nversion 2\nts {ts}\nnode {node}\n");
    for (key, len, hash) in &files {
        record.push_str(&format!("{key}\t{len}\t{hash}\n"));
    }
    target.store.put(&target.key(&format!("{own}BACKUP")), record.as_bytes())?;
    target.store.put(&target.key(&format!("{mine}LATEST")), format!("{ts}\n").as_bytes())?;
    let mut ack = format!(
        "backup {ts} to {}: {} collection(s), {shards} shard(s), {copied} segment(s) copied \
         ({bytes} bytes), {present} already there{}",
        target.display,
        colls.len(),
        // Worth saying out loud: it is the difference between an
        // incremental that reads what it copies and one that reads the
        // whole database to copy nothing.
        if recalled > 0 {
            format!(" ({recalled} of them not read again, their hash from the last record)")
        } else {
            String::new()
        }
    );
    if let Some(keep) = keep {
        let held: Vec<String> = colls
            .iter()
            .flat_map(|(name, shards)| {
                shards.iter().map(move |(index, _)| format!("{name}/shard-{index:04}"))
            })
            .collect();
        let (pruned, freed) = prune(&target, &mine, keep, &held)?;
        ack.push_str(&format!("; kept {keep}, removed {pruned} older backup(s) and {freed} pool segment(s) nobody references"));
    }
    Ok(Outcome::Ack(ack))
}

/// Retention: remove this node's backups at the destination beyond the
/// newest `keep`, then the pool segments of the shards this node holds
/// that no remaining backup -- of any node at the destination -- still
/// names. Returns how many backups and how many pool segments went.
///
/// The record goes first, so a prune that stops halfway leaves an
/// incomplete backup rather than a complete one with holes. The pool is
/// swept only under the shards this node holds now, because those are the
/// ones this node alone writes; another node's backup in flight cannot be
/// putting segments there. A shard that moved away is swept by its new
/// holder, whose records name what it needs.
fn prune(target: &Target, mine: &str, keep: usize, held: &[String]) -> Result<(usize, usize)> {
    let all = instants(target, mine)?;
    let drop: Vec<u64> =
        if all.len() > keep { all[..all.len() - keep].to_vec() } else { Vec::new() };
    let mut pruned = 0usize;
    for ts in &drop {
        let own = target.key(&format!("{mine}backups/{}/", ts_key(*ts)));
        target.store.delete(&format!("{own}BACKUP"))?;
        for key in target.store.list(&own)? {
            target.store.delete(&key)?;
        }
        pruned += 1;
    }
    // What every remaining backup at the destination still names.
    let mut referenced = std::collections::BTreeSet::new();
    for node in nodes(target)? {
        let theirs = format!("nodes/{node}/");
        for ts in instants(target, &theirs)? {
            let record =
                target.store.get(&target.key(&format!("{theirs}backups/{}/BACKUP", ts_key(ts))))?;
            for line in String::from_utf8_lossy(&record).lines() {
                if let Some((key, _)) = line.split_once('\t') {
                    if key.starts_with("pool/") {
                        referenced.insert(key.to_string());
                    }
                }
            }
        }
    }
    let mut freed = 0usize;
    for shard in held {
        let prefix = target.key(&format!("pool/{shard}/"));
        for key in target.store.list(&prefix)? {
            let rest = &key[target.prefix.len()..];
            if !referenced.contains(rest) {
                target.store.delete(&key)?;
                freed += 1;
            }
        }
    }
    Ok((pruned, freed))
}

/// A backup read back and verified: its instant, its catalog and data key
/// as the store holds them (the catalog is framed when the key is there,
/// and the engine opens it), and per collection and shard the files to
/// write, each as (name under the shard directory, key, size).
pub(crate) struct Fetched {
    pub(crate) ts: u64,
    pub(crate) catalog: Vec<u8>,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) collections: BackupShards,
    /// Every object the record names: (key, size, SHA-256 hex or empty).
    pub(crate) files: Vec<(String, u64, String)>,
}

/// The nodes a destination holds backups of.
fn nodes(target: &Target) -> Result<Vec<String>> {
    let prefix = target.key("nodes/");
    let mut out = Vec::new();
    for key in target.store.list(&prefix)? {
        if let Some((node, "LATEST")) = key[prefix.len()..].split_once('/') {
            out.push(node.to_string());
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// The backups a destination holds of one node, newest last.
fn instants(target: &Target, mine: &str) -> Result<Vec<u64>> {
    let prefix = target.key(&format!("{mine}backups/"));
    let mut out = Vec::new();
    for key in target.store.list(&prefix)? {
        let rest = &key[prefix.len()..];
        if let Some((ts, "BACKUP")) = rest.split_once('/') {
            if let Ok(ts) = ts.parse::<u64>() {
                out.push(ts);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// The newest complete backup of `node` at or before `t`, for a restore to
/// that instant; none when every backup there is after it.
pub(crate) fn newest_at_or_before(target: &Target, node: &str, t: u64) -> Result<Option<u64>> {
    let mine = format!("nodes/{}/", node_slug(node));
    Ok(instants(target, &mine)?.into_iter().filter(|b| *b <= t).max())
}

/// The newest complete backup of `node` at the destination, or the one at
/// `as_of`, read and verified.
/// Whether `node`'s backup at `ts` is at the destination with its record:
/// what a cluster backup asks when a peer says it started no such backup
/// -- a peer that restarted mid-way keeps no memory of it, and the record
/// is written last, so its presence is the copy's completeness.
pub(crate) fn has_record(target: &Target, node: &str, ts: u64) -> bool {
    let slug = node_slug(node);
    let key = target.key(&format!("nodes/{slug}/backups/{}/BACKUP", ts_key(ts)));
    target.store.get(&key).is_ok()
}

pub(crate) fn fetch(target: &Target, node: &str, as_of: Option<u64>) -> Result<Fetched> {
    let slug = node_slug(node);
    let mine = format!("nodes/{slug}/");
    let ts = match as_of {
        Some(ts) => ts,
        None => {
            let latest = match target.store.get(&target.key(&format!("{mine}LATEST"))) {
                Ok(l) => l,
                Err(e) => {
                    let others = nodes(target)?;
                    let list =
                        if others.is_empty() { "none".to_string() } else { others.join(", ") };
                    return Err(Error::Storage(format!(
                        "no backup of node `{slug}` at {}: {e}; nodes backed up there: {list} \
                         (RESTORE FROM '...' NODE '<address>' takes another node's)",
                        target.display
                    )));
                }
            };
            String::from_utf8_lossy(&latest).trim().parse::<u64>().map_err(|_| {
                Error::Storage(format!("{mine}LATEST at {} is not an instant", target.display))
            })?
        }
    };
    let own = format!("{mine}backups/{}/", ts_key(ts));
    let record = match target.store.get(&target.key(&format!("{own}BACKUP"))) {
        Ok(r) => r,
        Err(e) => {
            let have = instants(target, &mine)?;
            let list = if have.is_empty() {
                "none".to_string()
            } else {
                have.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
            };
            return Err(Error::Storage(format!(
                "no complete backup {ts} of node `{slug}` at {}: {e}; complete backups there: {list}",
                target.display
            )));
        }
    };
    let files = record_entries(&record, &format!("{own}BACKUP at {}", target.display))?;
    // Every object at its recorded size, before anything is written.
    for (key, len, _) in &files {
        match target.store.size(&target.key(key))? {
            Some(n) if n == *len => {}
            Some(n) => {
                return Err(Error::Storage(format!(
                    "backup {ts} at {} is damaged: {key} is {n} bytes, the record says {len}",
                    target.display
                )))
            }
            None => {
                return Err(Error::Storage(format!(
                    "backup {ts} at {} is damaged: {key} is missing",
                    target.display
                )))
            }
        }
    }
    let catalog = target.store.get(&target.key(&format!("{own}CATALOG")))?;
    let key = if files.iter().any(|(k, _, _)| *k == format!("{own}KEY")) {
        Some(target.store.get(&target.key(&format!("{own}KEY")))?)
    } else {
        None
    };
    // Group the files by collection and shard. A pool key is
    // `pool/<coll>/shard-NNNN/<id>.seg`; an own key `nodes/<n>/backups/<ts>/<coll>/
    // shard-NNNN/<rest>`.
    let mut collections: BackupShards = Vec::new();
    for (key, len, hash) in &files {
        let (coll, shard, rest) = if let Some(r) = key.strip_prefix("pool/") {
            let mut it = r.splitn(3, '/');
            match (it.next(), it.next(), it.next()) {
                (Some(c), Some(s), Some(name)) => (c, s, format!("segments/{name}")),
                _ => continue,
            }
        } else if let Some(r) = key.strip_prefix(&own) {
            let mut it = r.splitn(3, '/');
            match (it.next(), it.next(), it.next()) {
                (Some(c), Some(s), Some(name)) if s.starts_with("shard-") => {
                    (c, s, name.to_string())
                }
                _ => continue,
            }
        } else {
            continue;
        };
        let Some(index) = shard.strip_prefix("shard-").and_then(|n| n.parse::<usize>().ok()) else {
            continue;
        };
        let entry = match collections.iter_mut().find(|(n, _)| n == coll) {
            Some(e) => e,
            None => {
                collections.push((coll.to_string(), Vec::new()));
                collections.last_mut().expect("just pushed")
            }
        };
        let shard_entry = match entry.1.iter_mut().find(|(i, _)| *i == index) {
            Some(e) => e,
            None => {
                entry.1.push((index, Vec::new()));
                entry.1.last_mut().expect("just pushed")
            }
        };
        shard_entry.1.push((rest, key.clone(), *len, hash.clone()));
    }
    Ok(Fetched { ts, catalog, key, collections, files })
}

/// An object read back against its record: the size always, the SHA-256
/// when the record has one. The error names the object.
fn checked(key: &str, data: &[u8], len: u64, hash: &str) -> Result<()> {
    if data.len() as u64 != len {
        return Err(Error::Storage(format!(
            "backup is damaged: {key} is {} bytes, the record says {len}",
            data.len()
        )));
    }
    if !hash.is_empty() && hex(&sha256(data)) != hash {
        return Err(Error::Storage(format!(
            "backup is damaged: {key} does not match its recorded checksum"
        )));
    }
    Ok(())
}

/// The work `VERIFY BACKUP` leaves for after the lock: every object read
/// back and checked.
pub(crate) fn verify_job(target: Target, slug: String, fetched: Fetched) -> Deferred {
    Deferred::new(move || verify(target, &slug, fetched))
}

fn verify(target: Target, slug: &str, fetched: Fetched) -> Result<Outcome> {
    let mut bytes = 0u64;
    let mut unchecked = 0usize;
    let mut damaged: Vec<String> = Vec::new();
    for (key, len, hash) in &fetched.files {
        let data = target.store.get(&target.key(key))?;
        if hash.is_empty() {
            unchecked += 1;
        }
        if let Err(e) = checked(key, &data, *len, hash) {
            damaged.push(e.to_string());
            if damaged.len() >= 5 {
                break;
            }
        }
        bytes += data.len() as u64;
    }
    if !damaged.is_empty() {
        return Err(Error::Storage(format!(
            "VERIFY BACKUP {} of node `{slug}` at {}: {}{}",
            fetched.ts,
            target.display,
            damaged.join("; "),
            if damaged.len() >= 5 { "; and possibly more" } else { "" }
        )));
    }
    let note = if unchecked > 0 {
        format!(
            "; {unchecked} object(s) checked by size only (written before checksums were recorded)"
        )
    } else {
        String::new()
    };
    Ok(Outcome::Ack(format!(
        "verified backup {} of node `{slug}` at {}: {} object(s), {bytes} bytes, every one as recorded{note}",
        fetched.ts,
        target.display,
        fetched.files.len()
    )))
}

/// Write one shard's files under `sdir` (created), `MANIFEST` last so a
/// directory that stops short is not a shard. Returns the bytes written.
pub(crate) fn write_shard(target: &Target, sdir: &Path, files: &ShardFiles) -> Result<u64> {
    std::fs::create_dir_all(sdir.join("segments"))?;
    std::fs::create_dir_all(sdir.join("deletes"))?;
    std::fs::create_dir_all(sdir.join("archive"))?;
    let mut bytes = 0u64;
    let mut manifest: Option<&(String, String, u64, String)> = None;
    for f in files {
        if f.0 == "MANIFEST" {
            manifest = Some(f);
            continue;
        }
        let data = target.store.get(&target.key(&f.1))?;
        checked(&f.1, &data, f.2, &f.3)?;
        crate::shard::atomic_write(&sdir.join(&f.0), &data)?;
        bytes += data.len() as u64;
    }
    let Some(m) = manifest else {
        return Err(Error::Storage(format!(
            "{}: the backup has no MANIFEST for it",
            sdir.display()
        )));
    };
    let data = target.store.get(&target.key(&m.1))?;
    checked(&m.1, &data, m.2, &m.3)?;
    crate::shard::atomic_write(&sdir.join("MANIFEST"), &data)?;
    bytes += data.len() as u64;
    crate::shard::sync_dir(&sdir.join("segments"))?;
    crate::shard::sync_dir(&sdir.join("deletes"))?;
    crate::shard::sync_dir(sdir)?;
    Ok(bytes)
}
