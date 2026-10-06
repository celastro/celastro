//! Collections of JSON documents, each held in memory and in an append-only
//! log on disk. A write is appended to its collection's log and synced before
//! it is acknowledged; opening the directory replays every log.
//!
//! A log record is `length (u32 LE) | CRC-32 of the payload (u32 LE) |
//! payload`, the payload `op (1 byte) | id length (u16 LE) | id | document`.
//! A crash can leave the last record torn; the open cuts a torn last record
//! away, since it was never acknowledged. A damaged record anywhere else is
//! refused by name rather than dropped with everything after it.

use crate::json::{self, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

/// The largest document a write may carry, in bytes of JSON text.
pub const MAX_DOC: usize = 16 << 20;
/// The most documents one listing returns.
pub const MAX_LIMIT: usize = 1000;
const MAX_ID: usize = 256;
const MAX_COLLECTION: usize = 64;
const HEADER: usize = 8;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Debug)]
pub enum Error {
    /// The request was wrong: a name, a document, a limit.
    Invalid(String),
    /// The disk refused, or the directory is damaged.
    Io(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Invalid(m) | Error::Io(m) => f.write_str(m),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Store {
    dir: PathBuf,
    /// Held for the life of the store: one process per directory.
    _lock: File,
    collections: HashMap<String, Collection>,
}

struct Collection {
    log: File,
    /// The log's length after the last acknowledged write.
    len: u64,
    /// Set when a sync failed: what the disk holds is no longer known, so the
    /// collection takes no more writes until a restart replays the log.
    failed: bool,
    docs: BTreeMap<String, String>,
}

/// One page of a listing.
pub struct Page {
    pub docs: Vec<(String, String)>,
    /// The id to pass as `after` for the next page, when there is one.
    pub next: Option<String>,
}

impl Store {
    /// Open (or create) the directory and replay every collection's log.
    pub fn open(dir: impl AsRef<Path>) -> Result<Store> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))?;
        if lock.try_lock().is_err() {
            return Err(Error::Io(format!(
                "{} is in use by another process (LOCK is held)",
                dir.display()
            )));
        }
        let mut collections = HashMap::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(name) = name.strip_suffix(".log") else {
                continue;
            };
            if check_collection(name).is_err() {
                continue;
            }
            collections.insert(name.to_string(), replay(&path)?);
        }
        Ok(Store {
            dir,
            _lock: lock,
            collections,
        })
    }

    /// Every collection, with how many documents it holds.
    pub fn collections(&self) -> Vec<(String, usize)> {
        let mut out: Vec<_> = self
            .collections
            .iter()
            .map(|(n, c)| (n.clone(), c.docs.len()))
            .collect();
        out.sort();
        out
    }

    pub fn get(&self, collection: &str, id: &str) -> Option<&str> {
        self.collections
            .get(collection)?
            .docs
            .get(id)
            .map(String::as_str)
    }

    /// Write `doc` under `id`, replacing what was there. Returns once the
    /// write is on disk.
    pub fn put(&mut self, collection: &str, id: &str, doc: &Value) -> Result<()> {
        check_collection(collection)?;
        check_id(id)?;
        if !matches!(doc, Value::Object(_)) {
            return Err(Error::Invalid("a document is a JSON object".into()));
        }
        let text = doc.to_json();
        if text.len() > MAX_DOC {
            return Err(Error::Invalid(format!(
                "a document is at most {MAX_DOC} bytes"
            )));
        }
        let c = self.collection(collection)?;
        c.append(&encode(OP_PUT, id, &text))?;
        c.docs.insert(id.to_string(), text);
        Ok(())
    }

    /// Remove `id`. Returns whether it was there; once it returns, the delete
    /// is on disk.
    pub fn delete(&mut self, collection: &str, id: &str) -> Result<bool> {
        check_collection(collection)?;
        check_id(id)?;
        let Some(c) = self.collections.get_mut(collection) else {
            return Ok(false);
        };
        if !c.docs.contains_key(id) {
            return Ok(false);
        }
        c.append(&encode(OP_DELETE, id, ""))?;
        c.docs.remove(id);
        Ok(true)
    }

    /// The documents of `collection` in id order, after `after`, whose fields
    /// equal every `(path, value)` filter, at most `limit` of them.
    pub fn list(
        &self,
        collection: &str,
        filters: &[(String, String)],
        after: Option<&str>,
        limit: usize,
    ) -> Result<Page> {
        check_collection(collection)?;
        if limit == 0 || limit > MAX_LIMIT {
            return Err(Error::Invalid(format!("limit is 1 to {MAX_LIMIT}")));
        }
        let Some(c) = self.collections.get(collection) else {
            return Ok(Page {
                docs: Vec::new(),
                next: None,
            });
        };
        let start = match after {
            Some(a) => Bound::Excluded(a),
            None => Bound::Unbounded,
        };
        let mut docs = Vec::new();
        let mut next = None;
        for (id, text) in c.docs.range::<str, _>((start, Bound::Unbounded)) {
            if !filters.is_empty() {
                let doc = json::parse(text).map_err(Error::Io)?;
                if !filters
                    .iter()
                    .all(|(path, want)| matches(doc.path(path), want))
                {
                    continue;
                }
            }
            if docs.len() == limit {
                next = docs.last().map(|(id, _): &(String, String)| id.clone());
                break;
            }
            docs.push((id.clone(), text.clone()));
        }
        Ok(Page { docs, next })
    }

    fn collection(&mut self, name: &str) -> Result<&mut Collection> {
        if !self.collections.contains_key(name) {
            let path = self.dir.join(format!("{name}.log"));
            let log = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&path)?;
            log.sync_all()?;
            // The new file's name is durable only once its directory is.
            File::open(&self.dir)?.sync_all()?;
            let c = Collection {
                log,
                len: 0,
                failed: false,
                docs: BTreeMap::new(),
            };
            self.collections.insert(name.to_string(), c);
        }
        Ok(self.collections.get_mut(name).expect("inserted above"))
    }
}

impl Collection {
    fn append(&mut self, record: &[u8]) -> Result<()> {
        if self.failed {
            return Err(Error::Io(
                "an earlier sync of this collection failed; restart to replay its log".into(),
            ));
        }
        if let Err(e) = self.log.write_all(record) {
            // Take a partial record back off, so that the next append does
            // not follow it and turn a torn tail into a damaged middle.
            if self.log.set_len(self.len).is_err() {
                self.failed = true;
            }
            return Err(e.into());
        }
        if let Err(e) = self.log.sync_data() {
            self.failed = true;
            return Err(e.into());
        }
        self.len += record.len() as u64;
        Ok(())
    }
}

/// A filter matches a string field equal to `want`, or any other field whose
/// JSON text is `want` (`42`, `true`, `null`).
fn matches(field: Option<&Value>, want: &str) -> bool {
    match field {
        None => false,
        Some(Value::String(s)) => s == want,
        Some(v) => v.to_json() == want,
    }
}

fn check_collection(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= MAX_COLLECTION
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "a collection name is 1 to {MAX_COLLECTION} letters, digits, `_` or `-`"
        )))
    }
}

fn check_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_ID || id.chars().any(char::is_control) {
        return Err(Error::Invalid(format!(
            "an id is 1 to {MAX_ID} bytes with no control characters"
        )));
    }
    Ok(())
}

fn encode(op: u8, id: &str, doc: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(3 + id.len() + doc.len());
    payload.push(op);
    payload.extend_from_slice(&(id.len() as u16).to_le_bytes());
    payload.extend_from_slice(id.as_bytes());
    payload.extend_from_slice(doc.as_bytes());
    let mut record = Vec::with_capacity(HEADER + payload.len());
    record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    record.extend_from_slice(&crc32(&payload).to_le_bytes());
    record.extend_from_slice(&payload);
    record
}

enum Decoded<'a> {
    Record {
        op: u8,
        id: &'a str,
        doc: &'a str,
        used: usize,
    },
    /// Fewer bytes than the record says it has: a torn tail.
    Short,
    /// The bytes do not check out; `used` is the record's length when its
    /// header is believable, `None` when even that is not.
    Damaged { used: Option<usize> },
}

fn decode(b: &[u8]) -> Decoded<'_> {
    if b.len() < HEADER {
        return Decoded::Short;
    }
    let len = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let sum = u32::from_le_bytes(b[4..8].try_into().unwrap());
    if len > MAX_DOC + MAX_ID + 3 {
        return Decoded::Damaged { used: None };
    }
    let Some(payload) = b.get(HEADER..HEADER + len) else {
        return Decoded::Short;
    };
    let used = HEADER + len;
    if crc32(payload) != sum || payload.len() < 3 {
        return Decoded::Damaged { used: Some(used) };
    }
    let op = payload[0];
    let id_len = u16::from_le_bytes([payload[1], payload[2]]) as usize;
    let Some(id) = payload.get(3..3 + id_len) else {
        return Decoded::Damaged { used: Some(used) };
    };
    let doc = &payload[3 + id_len..];
    match (std::str::from_utf8(id), std::str::from_utf8(doc), op) {
        (Ok(id), Ok(doc), OP_PUT | OP_DELETE) => Decoded::Record { op, id, doc, used },
        _ => Decoded::Damaged { used: Some(used) },
    }
}

fn replay(path: &Path) -> Result<Collection> {
    let mut log = OpenOptions::new().append(true).read(true).open(path)?;
    let mut bytes = Vec::new();
    log.read_to_end(&mut bytes)?;
    let mut docs = BTreeMap::new();
    let mut at = 0usize;
    while at < bytes.len() {
        match decode(&bytes[at..]) {
            Decoded::Record { op, id, doc, used } => {
                if op == OP_PUT {
                    docs.insert(id.to_string(), doc.to_string());
                } else {
                    docs.remove(id);
                }
                at += used;
                continue;
            }
            // What a crash leaves after the last acknowledged record: a
            // record cut short, one whose bytes do not check out and end the
            // file, or zeros the filesystem allocated and never filled.
            Decoded::Short => {}
            Decoded::Damaged { used: Some(used) } if at + used == bytes.len() => {}
            Decoded::Damaged { .. } if bytes[at..].iter().all(|&b| b == 0) => {}
            Decoded::Damaged { .. } => {
                return Err(Error::Io(format!(
                    "{}: the record at byte {at} is damaged and the log goes on past it; \
                     refusing to open rather than drop what follows",
                    path.display()
                )));
            }
        }
        eprintln!(
            "celastro: {}: cut a torn last record at byte {at} ({} bytes)",
            path.display(),
            bytes.len() - at
        );
        log.set_len(at as u64)?;
        log.sync_all()?;
        break;
    }
    Ok(Collection {
        log,
        len: at as u64,
        failed: false,
        docs,
    })
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = crc_table();

/// CRC-32 (IEEE 802.3).
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c = CRC_TABLE[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("celastro-store-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn doc(text: &str) -> Value {
        json::parse(text).unwrap()
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn writes_survive_a_reopen() {
        let dir = tmp("reopen");
        {
            let mut s = Store::open(&dir).unwrap();
            s.put("notes", "n1", &doc(r#"{"topic":"a"}"#)).unwrap();
            s.put("notes", "n2", &doc(r#"{"topic":"b"}"#)).unwrap();
            s.put("notes", "n1", &doc(r#"{"topic":"c"}"#)).unwrap();
            assert!(s.delete("notes", "n2").unwrap());
            assert!(!s.delete("notes", "n2").unwrap());
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.get("notes", "n1"), Some(r#"{"topic":"c"}"#));
        assert_eq!(s.get("notes", "n2"), None);
        assert_eq!(s.collections(), vec![("notes".to_string(), 1)]);
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_process_on_the_directory_is_refused() {
        let dir = tmp("lock");
        let s = Store::open(&dir).unwrap();
        let Err(e) = Store::open(&dir) else {
            panic!("a second open was allowed")
        };
        assert!(e.to_string().contains("in use"), "{e}");
        drop(s);
        assert!(Store::open(&dir).is_ok(), "the lock outlived its store");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_last_record_is_cut_and_the_rest_kept() {
        let dir = tmp("torn");
        {
            let mut s = Store::open(&dir).unwrap();
            s.put("c", "a", &doc(r#"{"v":1}"#)).unwrap();
            s.put("c", "b", &doc(r#"{"v":2}"#)).unwrap();
        }
        let log = dir.join("c.log");
        let full = fs::read(&log).unwrap();
        let cut = full.len() - 3;
        fs::write(&log, &full[..cut]).unwrap();
        {
            let mut s = Store::open(&dir).unwrap();
            assert_eq!(s.get("c", "a"), Some(r#"{"v":1}"#));
            assert_eq!(
                s.get("c", "b"),
                None,
                "the torn write was never acknowledged"
            );
            // The next write follows the last whole record, not the torn bytes.
            s.put("c", "d", &doc(r#"{"v":4}"#)).unwrap();
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.get("c", "d"), Some(r#"{"v":4}"#));
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_damaged_record_before_others_refuses_the_open() {
        let dir = tmp("damaged");
        {
            let mut s = Store::open(&dir).unwrap();
            s.put("c", "a", &doc(r#"{"v":1}"#)).unwrap();
            s.put("c", "b", &doc(r#"{"v":2}"#)).unwrap();
        }
        let log = dir.join("c.log");
        let mut bytes = fs::read(&log).unwrap();
        bytes[HEADER + 4] ^= 0xff; // inside the first record's payload
        fs::write(&log, &bytes).unwrap();
        let Err(e) = Store::open(&dir) else {
            panic!("a damaged middle was opened")
        };
        assert!(e.to_string().contains("byte 0 is damaged"), "{e}");
        assert_eq!(
            fs::read(&log).unwrap(),
            bytes,
            "the refused open changed the log"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_zero_filled_tail_is_cut_and_a_bad_length_mid_log_is_refused() {
        let dir = tmp("zeros");
        {
            let mut s = Store::open(&dir).unwrap();
            s.put("c", "a", &doc(r#"{"v":1}"#)).unwrap();
        }
        let log = dir.join("c.log");
        let good = fs::read(&log).unwrap();
        let mut zeros = good.clone();
        zeros.extend_from_slice(&[0u8; 4096]);
        fs::write(&log, &zeros).unwrap();
        {
            let s = Store::open(&dir).unwrap();
            assert_eq!(s.get("c", "a"), Some(r#"{"v":1}"#));
        }
        assert_eq!(fs::read(&log).unwrap(), good, "the zeros were not cut");
        // A length no record can have, with a record after it: refused, never
        // read as a torn tail that would take the record with it.
        let mut bad = good.clone();
        bad[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        bad.extend_from_slice(&good);
        fs::write(&log, &bad).unwrap();
        let Err(e) = Store::open(&dir) else {
            panic!("a bad length was opened")
        };
        assert!(e.to_string().contains("byte 0 is damaged"), "{e}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn listing_filters_and_pages_in_id_order() {
        let dir = tmp("list");
        let mut s = Store::open(&dir).unwrap();
        for i in 0..7 {
            let topic = if i % 2 == 0 { "even" } else { "odd" };
            let d = doc(&format!(
                r#"{{"topic":"{topic}","n":{i},"meta":{{"ok":true}}}}"#
            ));
            s.put("c", &format!("k{i}"), &d).unwrap();
        }
        let f = vec![("topic".to_string(), "even".to_string())];
        let p = s.list("c", &f, None, 2).unwrap();
        let ids: Vec<_> = p.docs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["k0", "k2"]);
        assert_eq!(p.next.as_deref(), Some("k2"));
        let p = s.list("c", &f, p.next.as_deref(), 2).unwrap();
        let ids: Vec<_> = p.docs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["k4", "k6"]);
        assert_eq!(p.next, None);
        let f = vec![
            ("n".to_string(), "3".to_string()),
            ("meta.ok".to_string(), "true".to_string()),
        ];
        let p = s.list("c", &f, None, 10).unwrap();
        assert_eq!(p.docs.len(), 1);
        assert_eq!(p.docs[0].0, "k3");
        assert!(s.list("c", &[], None, 0).is_err());
        assert!(s.list("c", &[], None, MAX_LIMIT + 1).is_err());
        assert_eq!(s.list("absent", &[], None, 5).unwrap().docs.len(), 0);
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn names_and_documents_are_checked() {
        let dir = tmp("names");
        let mut s = Store::open(&dir).unwrap();
        for bad in ["", "../x", "a/b", "a.log", &"x".repeat(MAX_COLLECTION + 1)] {
            assert!(s.put(bad, "id", &doc("{}")).is_err(), "{bad:?}");
        }
        assert!(s.put("c", "", &doc("{}")).is_err());
        assert!(s.put("c", "a\nb", &doc("{}")).is_err());
        assert!(s.put("c", &"i".repeat(MAX_ID + 1), &doc("{}")).is_err());
        assert!(
            s.put("c", "id", &doc("[1]")).is_err(),
            "a document is an object"
        );
        assert!(s.put("c", "id with spaces/and slash", &doc("{}")).is_ok());
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }
}
