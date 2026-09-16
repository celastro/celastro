//! The server's log: one line per event on stderr, with a timestamp and a
//! level, as text a person reads or as JSON a collector parses
//! (`CELASTRO_LOG=json`). What `serve` and the wire say about connections,
//! compactions, seals that failed and the like goes through here; the
//! command line's own messages to its user do not.
//!
//! ```text
//! 2026-09-16T04:12:09Z WARN compaction_failed what="items shard 0 level 0" error="..."
//! {"ts":"2026-09-16T04:12:09Z","level":"warn","event":"compaction_failed","what":"...","error":"..."}
//! ```

use std::sync::atomic::{AtomicBool, Ordering};

static JSON: AtomicBool = AtomicBool::new(false);

/// Emit JSON lines rather than text. Read from `CELASTRO_LOG` by the
/// command line at start; `false` until then.
pub fn set_json(on: bool) {
    JSON.store(on, Ordering::Relaxed);
}

/// Configure from the environment: `CELASTRO_LOG=json` for JSON lines,
/// anything else (or unset) for text.
pub fn from_env() {
    let json =
        std::env::var("CELASTRO_LOG").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false);
    set_json(json);
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

pub fn info(event: &str, fields: &[(&str, String)]) {
    line(Level::Info, event, fields);
}

pub fn warn(event: &str, fields: &[(&str, String)]) {
    line(Level::Warn, event, fields);
}

pub fn error(event: &str, fields: &[(&str, String)]) {
    line(Level::Error, event, fields);
}

/// One line, in whichever form is set.
pub fn line(level: Level, event: &str, fields: &[(&str, String)]) {
    eprintln!("{}", render(level, event, fields, now_iso8601(), JSON.load(Ordering::Relaxed)));
}

/// The line itself, for the tests: `ts` is the timestamp to print.
pub fn render(
    level: Level,
    event: &str,
    fields: &[(&str, String)],
    ts: String,
    json: bool,
) -> String {
    if json {
        let mut out = format!(
            r#"{{"ts":{},"level":{},"event":{}"#,
            crate::json::to_string(&crate::value::Value::Str(ts)),
            crate::json::to_string(&crate::value::Value::Str(level.name().into())),
            crate::json::to_string(&crate::value::Value::Str(event.into()))
        );
        for (k, v) in fields {
            out.push_str(&format!(
                ",{}:{}",
                crate::json::to_string(&crate::value::Value::Str((*k).into())),
                crate::json::to_string(&crate::value::Value::Str(v.clone()))
            ));
        }
        out.push('}');
        out
    } else {
        let mut out = format!("{ts} {} {event}", level.name().to_ascii_uppercase());
        for (k, v) in fields {
            out.push(' ');
            out.push_str(k);
            out.push('=');
            out.push_str(&crate::json::to_string(&crate::value::Value::Str(v.clone())));
        }
        out
    }
}

/// Now, as `YYYY-MM-DDTHH:MM:SSZ`.
pub fn now_iso8601() -> String {
    iso8601(crate::time::now_micros() / 1_000_000)
}

/// Seconds since the Unix epoch as `YYYY-MM-DDTHH:MM:SSZ` (the civil date
/// from Howard Hinnant's algorithm, so no table of month lengths).
pub fn iso8601(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, (rem % 3600) / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_timestamp_is_civil_utc() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(iso8601(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(iso8601(-1), "1969-12-31T23:59:59Z");
    }

    #[test]
    fn a_line_renders_as_text_or_json_with_its_fields_quoted() {
        let fields =
            [("what", "items shard 0".to_string()), ("error", "disk \"full\"".to_string())];
        let ts = "2026-09-16T04:12:09Z".to_string();
        assert_eq!(
            render(Level::Warn, "compaction_failed", &fields, ts.clone(), false),
            r#"2026-09-16T04:12:09Z WARN compaction_failed what="items shard 0" error="disk \"full\"""#
        );
        let json = render(Level::Warn, "compaction_failed", &fields, ts, true);
        let v = crate::json::parse(&json).unwrap();
        assert_eq!(v.get("level").and_then(|l| l.as_str()), Some("warn"));
        assert_eq!(v.get("event").and_then(|l| l.as_str()), Some("compaction_failed"));
        assert_eq!(v.get("error").and_then(|l| l.as_str()), Some("disk \"full\""));
    }
}
