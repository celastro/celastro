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
//! (`celastro-0.celastro_2352`), or `local` for a node with no address, so
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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::engine::{handle_bytes, Deferred, ExportShard, Outcome};
use crate::error::{Error, Result};
use crate::objstore::{ArchiveOpts, DirStore, ObjectStore, S3Store};

/// What `BACKUP` pinned: per collection, the shards this node holds by
/// tablet index.
pub(crate) type Exported = Vec<(String, Vec<(usize, ExportShard)>)>;
/// One restored shard's files: (name under the shard directory, key, size).
pub(crate) type ShardFiles = Vec<(String, String, u64)>;
/// A backup's files per collection and tablet index.
pub(crate) type BackupShards = Vec<(String, Vec<(usize, ShardFiles)>)>;

/// Where a backup goes or comes from.
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

/// The key component a node's address becomes: `tcp://celastro-0.celastro:2352`
/// is `celastro-0.celastro_2352`, and no address is `local`.
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
) -> Deferred {
    Deferred::new(move || run(target, ts, node, catalog, key, colls))
}

fn run(
    target: Target,
    ts: u64,
    node: String,
    catalog: Vec<u8>,
    key: Option<Vec<u8>>,
    colls: Exported,
) -> Result<Outcome> {
    let mine = format!("nodes/{}/", node_slug(&node));
    let own = format!("{mine}backups/{}/", ts_key(ts));
    let mut files: Vec<(String, u64)> = Vec::new();
    let (mut copied, mut present, mut bytes, mut shards) = (0usize, 0usize, 0u64, 0usize);
    let put = |key: String, data: &[u8], files: &mut Vec<(String, u64)>| -> Result<()> {
        target.store.put(&target.key(&key), data)?;
        files.push((key, data.len() as u64));
        Ok(())
    };
    for (name, shard_list) in &colls {
        for (index, ex) in shard_list {
            shards += 1;
            let sdir = format!("{name}/shard-{index:04}");
            for h in &ex.sealed {
                let data = handle_bytes(h)?;
                let key = format!("pool/{sdir}/{:016x}.seg", h.id());
                match target.store.size(&target.key(&key))? {
                    Some(n) if n == data.len() as u64 => present += 1,
                    Some(n) => {
                        return Err(Error::Storage(format!(
                            "backup: {} holds {key} at {n} bytes, the segment is {}; a segment id \
                             came back with other contents, which the pool cannot hold",
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
                files.push((key, data.len() as u64));
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
    let mut record = format!("celastro backup\nversion 1\nts {ts}\nnode {node}\n");
    for (key, len) in &files {
        record.push_str(&format!("{key}\t{len}\n"));
    }
    target.store.put(&target.key(&format!("{own}BACKUP")), record.as_bytes())?;
    target.store.put(&target.key(&format!("{mine}LATEST")), format!("{ts}\n").as_bytes())?;
    Ok(Outcome::Ack(format!(
        "backup {ts} to {}: {} collection(s), {shards} shard(s), {copied} segment(s) copied \
         ({bytes} bytes), {present} already there",
        target.display,
        colls.len()
    )))
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

/// The newest complete backup of `node` at the destination, or the one at
/// `as_of`, read and verified.
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
    let record = String::from_utf8_lossy(&record).to_string();
    let mut lines = record.lines();
    if lines.next() != Some("celastro backup") {
        return Err(Error::Storage(format!(
            "{own}BACKUP at {} is not a backup record",
            target.display
        )));
    }
    let mut files: Vec<(String, u64)> = Vec::new();
    for line in lines {
        if let Some((key, len)) = line.split_once('\t') {
            let len = len.parse::<u64>().map_err(|_| {
                Error::Storage(format!("{own}BACKUP: `{line}` is not a file entry"))
            })?;
            files.push((key.to_string(), len));
        }
    }
    // Every object at its recorded size, before anything is written.
    for (key, len) in &files {
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
    let key = if files.iter().any(|(k, _)| *k == format!("{own}KEY")) {
        Some(target.store.get(&target.key(&format!("{own}KEY")))?)
    } else {
        None
    };
    // Group the files by collection and shard. A pool key is
    // `pool/<coll>/shard-NNNN/<id>.seg`; an own key `nodes/<n>/backups/<ts>/<coll>/
    // shard-NNNN/<rest>`.
    let mut collections: BackupShards = Vec::new();
    for (key, len) in &files {
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
        shard_entry.1.push((rest, key.clone(), *len));
    }
    Ok(Fetched { ts, catalog, key, collections })
}

/// Write one shard's files under `sdir` (created), `MANIFEST` last so a
/// directory that stops short is not a shard. Returns the bytes written.
pub(crate) fn write_shard(target: &Target, sdir: &Path, files: &ShardFiles) -> Result<u64> {
    std::fs::create_dir_all(sdir.join("segments"))?;
    std::fs::create_dir_all(sdir.join("deletes"))?;
    std::fs::create_dir_all(sdir.join("archive"))?;
    let mut bytes = 0u64;
    let mut manifest: Option<&(String, String, u64)> = None;
    for f in files {
        if f.0 == "MANIFEST" {
            manifest = Some(f);
            continue;
        }
        let data = target.store.get(&target.key(&f.1))?;
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
    crate::shard::atomic_write(&sdir.join("MANIFEST"), &data)?;
    bytes += data.len() as u64;
    crate::shard::sync_dir(&sdir.join("segments"))?;
    crate::shard::sync_dir(&sdir.join("deletes"))?;
    crate::shard::sync_dir(sdir)?;
    Ok(bytes)
}
