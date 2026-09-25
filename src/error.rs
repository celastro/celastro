//! Error type for the whole engine.

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// Malformed SQL, bad DDL, unsupported syntax.
    Sql(String),
    /// The statement parsed but does not make sense against the catalog.
    Plan(String),
    /// Type mismatch, bad document shape, constraint violation.
    Schema(String),
    /// Storage-level corruption or version mismatch.
    Storage(String),
    /// Something went wrong talking to the filesystem.
    Io(std::io::Error),
    /// A snapshot or manifest a reader pinned is no longer available.
    SnapshotGone(String),
    /// The query exceeded its deadline.
    Deadline(String),
    /// Not implemented in this phase; carries the design section that owns it.
    NotYet(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Sql(m) => write!(f, "syntax error: {m}"),
            Error::Plan(m) => write!(f, "planner error: {m}"),
            Error::Schema(m) => write!(f, "schema error: {m}"),
            Error::Storage(m) => write!(f, "storage error: {m}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::SnapshotGone(m) => write!(f, "snapshot no longer available: {m}"),
            Error::Deadline(m) => write!(f, "deadline exceeded: {m}"),
            Error::NotYet(s) => write!(f, "not implemented in this phase (see design {s})"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Shorthand for constructing errors with format strings.
#[macro_export]
macro_rules! err {
    ($kind:ident, $($arg:tt)*) => {
        $crate::error::Error::$kind(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! bail {
    ($kind:ident, $($arg:tt)*) => {
        return Err($crate::error::Error::$kind(format!($($arg)*)))
    };
}

impl Error {
    /// The same error with `prefix` before its message -- which row of a
    /// batch, which key -- its kind kept, so a deadline stays a deadline.
    pub(crate) fn prefixed(self, prefix: &str) -> Error {
        match self {
            Error::Sql(m) => Error::Sql(format!("{prefix}{m}")),
            Error::Plan(m) => Error::Plan(format!("{prefix}{m}")),
            Error::Schema(m) => Error::Schema(format!("{prefix}{m}")),
            Error::Storage(m) => Error::Storage(format!("{prefix}{m}")),
            Error::SnapshotGone(m) => Error::SnapshotGone(format!("{prefix}{m}")),
            Error::Deadline(m) => Error::Deadline(format!("{prefix}{m}")),
            Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), format!("{prefix}{e}"))),
            Error::NotYet(m) => Error::NotYet(m),
        }
    }
}
