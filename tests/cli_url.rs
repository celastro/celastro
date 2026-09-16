//! `celastro-cli --url`: `exec`, `run`, `repl` and `catalog` as clients of a
//! console another process serves, rendered as they would be locally, the
//! token taken from the URL `serve` printed.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("celastro-cli-url-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A console on a free port; its URL with the token, as `serve` prints it.
fn serve(d: &Path) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_celastro-cli"))
        .args(["--json", "--dir", d.to_str().unwrap(), "serve", "--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn celastro-cli");
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut first).unwrap();
    let hello = celastro::json::parse(&first).expect("the first line is the JSON url object");
    let url = hello.get("url").and_then(|v| v.as_str()).unwrap().to_string();
    (child, url)
}

fn cli(args: &[&str], stdin: Option<&str>) -> (bool, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_celastro-cli"));
    cmd.args(args).env_remove("CELASTRO_TOKEN");
    cmd.stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() });
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    if let Some(text) = stdin {
        child.stdin.take().unwrap().write_all(text.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[test]
fn the_cli_is_a_client_of_a_served_console_through_url() {
    let d = dir("db");
    let (mut child, url) = serve(&d);
    assert!(url.contains("?t="), "{url}");

    let (ok, out, err) = cli(
        &["--url", &url, "exec", "CREATE COLLECTION notes (id TEXT PRIMARY KEY, topic TEXT)"],
        None,
    );
    assert!(ok, "{out} {err}");
    assert!(out.contains("collection `notes` created"), "{out}");

    let script = d.with_extension("sql");
    std::fs::write(
        &script,
        "INSERT INTO notes VALUES ('{\"id\":\"n1\",\"topic\":\"a\",\"body\":\"one\"}');\n\
         INSERT INTO notes VALUES ('{\"id\":\"n2\",\"topic\":\"b\",\"body\":\"two\"}');\n\
         SELECT id, topic FROM notes WHERE topic = 'b';\n",
    )
    .unwrap();
    let (ok, out, err) = cli(&["--url", &url, "run", script.to_str().unwrap()], None);
    assert!(ok, "{out} {err}");
    assert!(out.contains("1 document(s) written"), "{out}");
    assert!(out.contains("| n2 ") && out.contains("1 row(s)"), "rows come back as a table: {out}");
    assert!(out.contains("ms at the console"), "{out}");

    // The REPL over stdin: a table, an acknowledgement, a refusal that does
    // not end the session but decides the exit code.
    let (ok, out, err) = cli(
        &["--url", &url, "repl"],
        Some("SELECT count(*) FROM notes;\nSELECT nothing FROM nowhere;\nexit\n"),
    );
    assert!(!ok, "{out} {err}");
    assert!(out.contains("count(*)") && out.contains("| 2"), "{out}");
    assert!(err.contains("error:") && err.contains("nowhere"), "{err}");

    let (ok, out, _) = cli(&["--url", &url, "catalog"], None);
    assert!(ok && out.contains("collection notes"), "{out}");

    // `--json` passes the console's document through unchanged.
    let (ok, out, _) =
        cli(&["--json", "--url", &url, "exec", "SELECT id FROM notes LIMIT 5"], None);
    assert!(ok, "{out}");
    let doc = celastro::json::parse(out.trim()).unwrap();
    assert_eq!(doc.get("kind").and_then(|v| v.as_str()), Some("rows"));
    assert_eq!(doc.get("count").and_then(|v| v.as_i64()), Some(2));

    // Without the token in the URL or the environment: refused before a
    // request; with a wrong one, refused by the console.
    let bare = url.split('?').next().unwrap().to_string();
    let (ok, _, err) = cli(&["--url", &bare, "exec", "SELECT id FROM notes LIMIT 1"], None);
    assert!(!ok && err.contains("no token"), "{err}");
    let (ok, _, err) =
        cli(&["--url", &format!("{bare}?t=wrong"), "exec", "SELECT id FROM notes LIMIT 1"], None);
    assert!(!ok && err.contains("error:"), "{err}");

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&d);
    let _ = std::fs::remove_file(&script);
}

#[test]
fn url_refuses_what_it_cannot_mean() {
    for (args, want) in [
        (vec!["--url", "http://h", "--dir", "d", "exec", "SELECT 1"], "one or the other"),
        (vec!["--url", "http://h", "serve"], "`exec`, `run`, `repl` and `catalog`"),
        (vec!["--url", "http://h", "demo"], "`exec`, `run`, `repl` and `catalog`"),
    ] {
        let (ok, _, err) = cli(&args, None);
        assert!(!ok && err.contains(want), "{args:?}: {err}");
    }
    let (ok, _, err) = cli(&["--url", "ftp://h", "exec", "SELECT 1"], None);
    assert!(!ok && err.contains("http://"), "{err}");
    let (ok, _, err) = cli(&["--url", "http://127.0.0.1:1?t=x", "exec", "SELECT 1"], None);
    assert!(!ok && err.contains("reaching http://127.0.0.1:1"), "{err}");
}
