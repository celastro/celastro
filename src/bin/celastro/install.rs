//! `celastro install`: this binary as a systemd service on the host it runs
//! on. One command writes what a node on a virtual machine needs -- the
//! binary under `/usr/local/bin`, a system user, the data directory, an
//! environment file with the tokens and the addresses, a unit -- and
//! starts it. Run again with new settings, or from a newer binary, it
//! rewrites those files and restarts the service; the data directory is
//! never touched.
//!
//! The secrets come from the environment (`CELASTRO_TOKEN`,
//! `CELASTRO_WIRE_TOKEN`) rather than from flags, so they are not in a shell
//! history or a process list. `--root DIR` writes the same files under DIR
//! and starts nothing, which is what a package build or a test wants.

use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The wire's port when an address names none, as `serve --shard-bind`
/// reads it.
const WIRE_PORT: u16 = 2352;
/// How long the start is given to answer the health probe.
const START_WAIT_SECS: u64 = 30;

/// What was asked for, parsed and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opts {
    /// This node's wire address, `tcp://host:port`, for a node of a cluster;
    /// none for a single node with no wire.
    pub node: Option<String>,
    /// Every node of the cluster, the same list on every node.
    pub attach: Vec<String>,
    /// `coordinator`, or nothing.
    pub role: Option<String>,
    /// A directory holding `tls.crt`, `tls.key` and `ca.crt`.
    pub tls_dir: Option<PathBuf>,
    /// `CELASTRO_TLS_CLIENT_AUTH=required`.
    pub client_auth: bool,
    /// Encryption at rest: the master key file and the wrapped data key.
    pub master_key: Option<PathBuf>,
    pub data_key: Option<PathBuf>,
    /// Further `CELASTRO_*` settings, sorted by name.
    pub env: Vec<(String, String)>,
    /// Write under this directory and start nothing.
    pub root: Option<PathBuf>,
    /// Write the files and enable the unit, but do not start it.
    pub start: bool,
    pub user: String,
    pub port: u16,
    pub bind: IpAddr,
    /// `--shard-bind`, `0.0.0.0:2352` unless given.
    pub shard_bind: String,
    pub dir: PathBuf,
}

/// The secrets, read from the environment by the caller so that the
/// installer itself can be tested without touching the process's
/// environment.
#[derive(Debug, Clone, Default)]
pub struct Secrets {
    pub token: Option<String>,
    pub wire_token: Option<String>,
}

/// The flags after `install`, with the global ones that apply. `rest` holds
/// the flags the global parser did not know, in the order written, values
/// as separate items or joined by `=`.
pub fn parse(
    rest: &[String],
    port: Option<u16>,
    bind: Option<IpAddr>,
    shard_bind: Option<String>,
    dir: Option<PathBuf>,
) -> Result<Opts, String> {
    let mut opts = Opts {
        node: None,
        attach: Vec::new(),
        role: None,
        tls_dir: None,
        client_auth: false,
        master_key: None,
        data_key: None,
        env: Vec::new(),
        root: None,
        start: true,
        user: "celastro".to_string(),
        port: port.unwrap_or(super::DEFAULT_PORT),
        bind: bind.unwrap_or(IpAddr::from([0, 0, 0, 0])),
        shard_bind: String::new(),
        dir: dir.unwrap_or_else(|| PathBuf::from("/var/lib/celastro")),
    };
    let mut i = 0;
    let value = |i: &mut usize, name: &str, inline: Option<String>| -> Result<String, String> {
        if let Some(v) = inline {
            return Ok(v);
        }
        match rest.get(*i) {
            Some(v) if !v.starts_with('-') => {
                *i += 1;
                Ok(v.clone())
            }
            _ => Err(super::missing_value(name)),
        }
    };
    while i < rest.len() {
        let arg = rest[i].clone();
        i += 1;
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        match name.as_str() {
            "--node" => opts.node = Some(wire_address(&value(&mut i, &name, inline)?)?),
            "--attach" => {
                for a in value(&mut i, &name, inline)?.split(',') {
                    let a = a.trim();
                    if !a.is_empty() {
                        opts.attach.push(wire_address(a)?);
                    }
                }
            }
            "--role" => {
                let r = value(&mut i, &name, inline)?;
                if r != "coordinator" {
                    return Err(format!("`--role` is `coordinator` or nothing, not `{r}`"));
                }
                opts.role = Some(r);
            }
            "--tls" => opts.tls_dir = Some(PathBuf::from(value(&mut i, &name, inline)?)),
            "--client-auth" => {
                if inline.is_some() {
                    return Err("`--client-auth` takes no value".to_string());
                }
                opts.client_auth = true;
            }
            "--master-key" => opts.master_key = Some(PathBuf::from(value(&mut i, &name, inline)?)),
            "--data-key" => opts.data_key = Some(PathBuf::from(value(&mut i, &name, inline)?)),
            "--env" => {
                let kv = value(&mut i, &name, inline)?;
                let Some((k, v)) = kv.split_once('=') else {
                    return Err(format!("`--env` wants NAME=VALUE, not `{kv}`"));
                };
                let ok_name = !k.is_empty()
                    && k.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                    && !k.as_bytes()[0].is_ascii_digit();
                if !ok_name {
                    return Err(format!("`--env`: `{k}` is not a variable name (A-Z, 0-9 and _)"));
                }
                if OWN_VARIABLES.contains(&k) {
                    return Err(format!(
                        "`--env`: {k} is set by the install's own flags, not by `--env`"
                    ));
                }
                if v.bytes().any(|b| b == b'\n' || b == b'\r') {
                    return Err(format!("`--env`: the value of {k} must be one line"));
                }
                opts.env.retain(|(name, _)| name != k);
                opts.env.push((k.to_string(), v.to_string()));
            }
            "--root" => opts.root = Some(PathBuf::from(value(&mut i, &name, inline)?)),
            "--no-start" => {
                if inline.is_some() {
                    return Err("`--no-start` takes no value".to_string());
                }
                opts.start = false;
            }
            "--user" => opts.user = value(&mut i, &name, inline)?,
            other if other.starts_with('-') => {
                return Err(format!("unknown flag `{other}` for `install`"))
            }
            other => return Err(format!("`install` takes flags only, not `{other}`")),
        }
    }
    opts.env.sort();
    if opts.node.is_none() && !opts.attach.is_empty() {
        return Err("`--attach` names the other nodes; `--node` must name this one".to_string());
    }
    if opts.node.is_none() && opts.role.is_some() {
        return Err(
            "`--role coordinator` is for a node of a cluster; give `--node` too".to_string()
        );
    }
    if opts.client_auth && opts.tls_dir.is_none() {
        return Err("`--client-auth` needs the certificates: give `--tls DIR`".to_string());
    }
    if opts.master_key.is_some() != opts.data_key.is_some() {
        return Err(
            "encryption at rest takes both `--master-key FILE` and `--data-key FILE`".to_string()
        );
    }
    if opts.node.is_some() && shard_bind.is_none() && opts.bind.is_loopback() {
        return Err("a node of a cluster with `--bind 127.0.0.1` cannot be reached by the others; bind 0.0.0.0 or the host's address".to_string());
    }
    opts.shard_bind = match shard_bind {
        Some(s) => s,
        None => format!("0.0.0.0:{WIRE_PORT}"),
    };
    if opts.user.is_empty()
        || !opts.user.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(format!("`--user`: `{}` is not a user name", opts.user));
    }
    Ok(opts)
}

/// The variables the flags set; `--env` may not set them behind the flags'
/// back.
const OWN_VARIABLES: [&str; 10] = [
    "CELASTRO_TOKEN",
    "CELASTRO_WIRE_TOKEN",
    "CELASTRO_NODE",
    "CELASTRO_ATTACH",
    "CELASTRO_ROLE",
    "CELASTRO_TLS_CERT",
    "CELASTRO_TLS_KEY",
    "CELASTRO_TLS_CA",
    "CELASTRO_MASTER_KEY_FILE",
    "CELASTRO_KEY_FILE",
];

/// `tcp://host:port`, from that, `host:port` or `host`.
fn wire_address(s: &str) -> Result<String, String> {
    let rest = s.strip_prefix("tcp://").unwrap_or(s);
    if rest.is_empty() || rest.contains('/') || rest.contains(char::is_whitespace) {
        return Err(format!("`{s}` is not a node address (tcp://host:port)"));
    }
    // An IPv6 literal carries colons of its own and is written [::1]:2352.
    let has_port = if rest.starts_with('[') {
        rest.rsplit_once(']').map(|(_, p)| p.starts_with(':')).unwrap_or(false)
    } else {
        rest.matches(':').count() == 1
    };
    if has_port {
        let (_, p) = rest.rsplit_once(':').unwrap_or((rest, ""));
        if p.parse::<u16>().is_err() {
            return Err(format!("`{s}` is not a node address: `{p}` is not a port"));
        }
        Ok(format!("tcp://{rest}"))
    } else if rest.matches(':').count() > 1 && !rest.starts_with('[') {
        Err(format!("`{s}` is not a node address: write an IPv6 address as [addr]:port"))
    } else {
        Ok(format!("tcp://{rest}:{WIRE_PORT}"))
    }
}

/// Where everything goes, under the root when there is one.
#[derive(Debug)]
struct Layout {
    binary: PathBuf,
    etc: PathBuf,
    env_file: PathBuf,
    unit: PathBuf,
    data: PathBuf,
    tls: PathBuf,
    master_key: PathBuf,
    data_key: PathBuf,
}

impl Layout {
    fn new(opts: &Opts) -> Layout {
        let under = |p: &str| -> PathBuf {
            match &opts.root {
                Some(r) => r.join(p.trim_start_matches('/')),
                None => PathBuf::from(p),
            }
        };
        let etc = under("/etc/celastro");
        Layout {
            binary: under("/usr/local/bin/celastro"),
            env_file: etc.join("celastro.env"),
            unit: under("/etc/systemd/system/celastro.service"),
            data: match &opts.root {
                Some(r) => r.join(opts.dir.to_string_lossy().trim_start_matches('/')),
                None => opts.dir.clone(),
            },
            tls: etc.join("tls"),
            master_key: etc.join("master.key"),
            data_key: etc.join("data.key"),
            etc,
        }
    }
}

/// The install, end to end. Exit code as `main` wants it.
pub fn run(opts: &Opts, secrets: &Secrets, json: bool) -> i32 {
    match install(opts, secrets) {
        Ok(report) => {
            if json {
                println!("{}", report.json());
            } else {
                println!("{}", report.text());
            }
            super::EXIT_OK
        }
        Err(e) => super::fail(json, &e),
    }
}

/// What the install did, for the report.
#[derive(Debug)]
struct Report {
    layout: Layout,
    opts: Opts,
    started: Option<bool>,
    unit_name: &'static str,
}

impl Report {
    fn console(&self) -> String {
        let scheme = if self.opts.tls_dir.is_some() { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.opts.bind, self.opts.port)
    }
    fn text(&self) -> String {
        let mut s = format!(
            "installed celastro {} as the service `{}`: binary {}, settings {}, data {}; the console on {}",
            env!("CARGO_PKG_VERSION"),
            self.unit_name,
            self.layout.binary.display(),
            self.layout.env_file.display(),
            self.layout.data.display(),
            self.console()
        );
        if let Some(node) = &self.opts.node {
            s.push_str(&format!(", the wire on {} as {node}", self.opts.shard_bind));
            if !self.opts.attach.is_empty() {
                s.push_str(&format!(", attaching {}", self.opts.attach.join(", ")));
            }
        }
        match self.started {
            Some(true) => s.push_str(&format!(
                "\nserving; `celastro health --port {}` asks it, `journalctl -u {}` reads its log",
                self.opts.port, self.unit_name
            )),
            Some(false) => s.push_str(&format!(
                "\nstarted, but not serving after {START_WAIT_SECS} s: `journalctl -u {}` says why",
                self.unit_name
            )),
            None if self.opts.root.is_some() => {
                s.push_str("\nwritten under the root; nothing started")
            }
            None => s.push_str(&format!(
                "\nenabled, not started: `systemctl start {}` when ready",
                self.unit_name
            )),
        }
        s
    }
    fn json(&self) -> String {
        use celastro::value::Value;
        let p = |p: &Path| Value::Str(p.display().to_string());
        let mut fields = vec![
            ("ok".to_string(), Value::Bool(true)),
            ("kind".to_string(), Value::Str("install".into())),
            ("version".to_string(), Value::Str(env!("CARGO_PKG_VERSION").into())),
            ("service".to_string(), Value::Str(self.unit_name.into())),
            ("binary".to_string(), p(&self.layout.binary)),
            ("settings".to_string(), p(&self.layout.env_file)),
            ("unit".to_string(), p(&self.layout.unit)),
            ("data".to_string(), p(&self.layout.data)),
            ("console".to_string(), Value::Str(self.console())),
        ];
        if let Some(node) = &self.opts.node {
            fields.push(("node".to_string(), Value::Str(node.clone())));
            fields.push((
                "attach".to_string(),
                Value::Array(self.opts.attach.iter().map(|a| Value::Str(a.clone())).collect()),
            ));
        }
        fields.push((
            "serving".to_string(),
            match self.started {
                Some(b) => Value::Bool(b),
                None => Value::Null,
            },
        ));
        celastro::json::to_string(&Value::obj(fields))
    }
}

fn install(opts: &Opts, secrets: &Secrets) -> Result<Report, String> {
    // Everything is checked before anything is written, so a refusal leaves
    // the host as it was.
    let token = match &secrets.token {
        Some(t) => t.clone(),
        None => {
            return Err(format!(
                "{} must be in the environment: the token every client presents, at least {} printable ASCII bytes, the same on every node",
                celastro::serve::TOKEN_ENV,
                celastro::serve::MIN_NETWORK_TOKEN
            ))
        }
    };
    if token.len() < celastro::serve::MIN_NETWORK_TOKEN
        || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(format!(
            "{} must be at least {} printable ASCII bytes with no whitespace; this one is {} byte(s)",
            celastro::serve::TOKEN_ENV,
            celastro::serve::MIN_NETWORK_TOKEN,
            token.len()
        ));
    }
    let wire_token = match (&opts.node, &secrets.wire_token) {
        (Some(_), Some(t)) if !t.is_empty() && !t.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) => Some(t.clone()),
        (Some(_), Some(_)) => return Err("CELASTRO_WIRE_TOKEN must be one word of printable bytes".to_string()),
        (Some(_), None) => {
            return Err("a node of a cluster needs CELASTRO_WIRE_TOKEN in the environment: the secret every node's wire shares".to_string())
        }
        (None, _) => None,
    };
    if let Some(dir) = &opts.tls_dir {
        for f in ["tls.crt", "tls.key", "ca.crt"] {
            let p = dir.join(f);
            if !p.is_file() {
                return Err(format!(
                    "`--tls {}`: no {f} there; `celastro tls init` writes the three files",
                    dir.display()
                ));
            }
        }
    }
    for (flag, file) in [("--master-key", &opts.master_key), ("--data-key", &opts.data_key)] {
        if let Some(f) = file {
            if !f.is_file() {
                return Err(format!("`{flag} {}`: not a file", f.display()));
            }
        }
    }
    let live = opts.root.is_none();
    let layout = Layout::new(opts);
    let ids = if live {
        if !is_root() {
            return Err("`install` writes under /etc, /usr/local and /var/lib and talks to systemd: run it as root (or with `--root DIR` to write the files under DIR and start nothing)".to_string());
        }
        if !Path::new("/run/systemd/system").is_dir() {
            return Err("this host is not running systemd; `install` writes a systemd unit (`--root DIR` writes the files for something else to use)".to_string());
        }
        Some(ensure_user(&opts.user, &opts.dir)?)
    } else {
        None
    };
    let owner = ids;

    // The binary, unless it already is the installed one.
    let me = std::env::current_exe().map_err(|e| format!("could not find this binary: {e}"))?;
    if me.canonicalize().ok() != layout.binary.canonicalize().ok() {
        copy_file(&me, &layout.binary, 0o755, None)?;
    }
    make_dir(&layout.etc, 0o750, owner.map(|(_, g)| (0, g)))?;
    make_dir(&layout.data, 0o700, owner)?;

    // The material a node reads at start: readable by the service user,
    // owned by root so that a compromised node cannot rewrite its own keys.
    let mut env = vec![("CELASTRO_TOKEN".to_string(), token)];
    if let Some(node) = &opts.node {
        env.push(("CELASTRO_NODE".to_string(), node.clone()));
        if !opts.attach.is_empty() {
            env.push(("CELASTRO_ATTACH".to_string(), opts.attach.join(",")));
        }
        env.push(("CELASTRO_WIRE_TOKEN".to_string(), wire_token.unwrap_or_default()));
    }
    if let Some(role) = &opts.role {
        env.push(("CELASTRO_ROLE".to_string(), role.clone()));
    }
    if let Some(dir) = &opts.tls_dir {
        make_dir(&layout.tls, 0o750, owner.map(|(_, g)| (0, g)))?;
        for (f, var) in [
            ("tls.crt", "CELASTRO_TLS_CERT"),
            ("tls.key", "CELASTRO_TLS_KEY"),
            ("ca.crt", "CELASTRO_TLS_CA"),
        ] {
            let to = layout.tls.join(f);
            copy_file(&dir.join(f), &to, 0o640, owner.map(|(_, g)| (0, g)))?;
            env.push((var.to_string(), to.display().to_string()));
        }
        if opts.client_auth {
            env.push(("CELASTRO_TLS_CLIENT_AUTH".to_string(), "required".to_string()));
        }
    }
    if let (Some(master), Some(data)) = (&opts.master_key, &opts.data_key) {
        copy_file(master, &layout.master_key, 0o640, owner.map(|(_, g)| (0, g)))?;
        copy_file(data, &layout.data_key, 0o640, owner.map(|(_, g)| (0, g)))?;
        env.push(("CELASTRO_MASTER_KEY_FILE".to_string(), layout.master_key.display().to_string()));
        env.push(("CELASTRO_KEY_FILE".to_string(), layout.data_key.display().to_string()));
    }
    for (k, v) in &opts.env {
        env.push((k.clone(), v.clone()));
    }
    write_file(&layout.env_file, &env_file(&env), 0o640, owner.map(|(_, g)| (0, g)))?;
    let unit = unit_file(opts, &layout);
    if let Some(parent) = layout.unit.parent() {
        make_dir(parent, 0o755, None)?;
    }
    write_file(&layout.unit, &unit, 0o644, None)?;

    let mut report = Report { layout, opts: opts.clone(), started: None, unit_name: "celastro" };
    if live {
        systemctl(&["daemon-reload"])?;
        if opts.start {
            systemctl(&["enable", "--now", "celastro.service"])?;
            // A second install is an upgrade or a change of settings: the
            // running service has the old binary and the old file.
            systemctl(&["restart", "celastro.service"])?;
            report.started = Some(wait_serving(opts, &report.layout));
        } else {
            systemctl(&["enable", "celastro.service"])?;
        }
    }
    Ok(report)
}

/// The environment file systemd reads: one `NAME="value"` per line, the
/// value's backslashes and quotes escaped as `EnvironmentFile=` reads them.
fn env_file(env: &[(String, String)]) -> String {
    let mut s = String::from(
        "# Written by `celastro install`. Edit and `systemctl restart celastro`, or run\n\
         # the install again with the new flags. Readable by root and the service user.\n",
    );
    for (k, v) in env {
        let escaped: String = v
            .chars()
            .flat_map(|c| match c {
                '\\' => vec!['\\', '\\'],
                '"' => vec!['\\', '"'],
                c => vec![c],
            })
            .collect();
        s.push_str(&format!("{k}=\"{escaped}\"\n"));
    }
    s
}

/// The unit. The service runs as its own user with the file system read-only
/// but for the data directory, no new privileges, and stdout discarded: the
/// one line `serve` prints there is its URL with the token in it, which does
/// not belong in the journal; the log is on stderr and goes there.
fn unit_file(opts: &Opts, layout: &Layout) -> String {
    let bin = match &opts.root {
        Some(_) => "/usr/local/bin/celastro".to_string(),
        None => layout.binary.display().to_string(),
    };
    let env_file = match &opts.root {
        Some(_) => "/etc/celastro/celastro.env".to_string(),
        None => layout.env_file.display().to_string(),
    };
    let mut exec = format!(
        "{bin} --dir {} --port {} serve --bind {}",
        opts.dir.display(),
        opts.port,
        opts.bind
    );
    if opts.node.is_some() {
        exec.push_str(&format!(" --shard-bind {}", opts.shard_bind));
    }
    format!(
        "# Written by `celastro install`; a second install rewrites it.\n\
         [Unit]\n\
         Description=celastro, a hybrid document database\n\
         Documentation=https://github.com/celastro/celastro\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         User={user}\n\
         Group={user}\n\
         EnvironmentFile={env_file}\n\
         ExecStart={exec}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         KillSignal=SIGTERM\n\
         TimeoutStopSec=90\n\
         LimitNOFILE=65536\n\
         StandardOutput=null\n\
         StandardError=journal\n\
         NoNewPrivileges=yes\n\
         ProtectSystem=strict\n\
         ProtectHome=yes\n\
         PrivateTmp=yes\n\
         ReadWritePaths={data}\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        user = opts.user,
        data = opts.dir.display(),
    )
}

fn is_root() -> bool {
    // No libc: the process's own status line carries its uids.
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("Uid:"))
                .and_then(|l| l.split_whitespace().nth(2).map(|u| u == "0"))
        })
        .unwrap_or(false)
}

/// The uid and gid of a user from the passwd file, without libc.
fn user_ids(name: &str) -> Option<(u32, u32)> {
    let passwd = std::fs::read_to_string("/etc/passwd").ok()?;
    passwd.lines().find_map(|l| {
        let f: Vec<&str> = l.split(':').collect();
        if f.len() >= 4 && f[0] == name {
            Some((f[2].parse().ok()?, f[3].parse().ok()?))
        } else {
            None
        }
    })
}

/// The system user the service runs as, made if it does not exist.
fn ensure_user(name: &str, home: &Path) -> Result<(u32, u32), String> {
    if let Some(ids) = user_ids(name) {
        return Ok(ids);
    }
    let out = Command::new("useradd")
        .args([
            "--system",
            "--user-group",
            "--no-create-home",
            "--shell",
            "/usr/sbin/nologin",
            "--home-dir",
        ])
        .arg(home)
        .arg(name)
        .output()
        .map_err(|e| format!("could not run useradd to make the user `{name}`: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "useradd could not make the user `{name}`: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    user_ids(name)
        .ok_or_else(|| format!("useradd made `{name}`, but the passwd file does not show it"))
}

fn systemctl(args: &[&str]) -> Result<(), String> {
    let out = Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| format!("could not run systemctl {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(())
}

/// Whether the started service answers the health probe on loopback within
/// the wait. A bind on one routable address is not on loopback, and is not
/// probed.
fn wait_serving(opts: &Opts, layout: &Layout) -> bool {
    if !(opts.bind.is_loopback() || opts.bind.is_unspecified()) {
        return true;
    }
    let tls = if opts.tls_dir.is_some() {
        // The probe speaks what the console speaks; the installed copies are
        // root's to read.
        std::env::set_var("CELASTRO_TLS_CERT", layout.tls.join("tls.crt"));
        std::env::set_var("CELASTRO_TLS_KEY", layout.tls.join("tls.key"));
        std::env::set_var("CELASTRO_TLS_CA", layout.tls.join("ca.crt"));
        match celastro::tls::Tls::from_env() {
            Ok(t) => t.map(std::sync::Arc::new),
            Err(_) => None,
        }
    } else {
        None
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(START_WAIT_SECS);
    while std::time::Instant::now() < deadline {
        if celastro::serve::probe_health(opts.port, tls.as_ref()).unwrap_or(false) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    false
}

fn make_dir(path: &Path, mode: u32, owner: Option<(u32, u32)>) -> Result<(), String> {
    std::fs::create_dir_all(path)
        .map_err(|e| format!("could not create {}: {e}", path.display()))?;
    set_mode(path, mode)?;
    chown(path, owner)
}

fn copy_file(from: &Path, to: &Path, mode: u32, owner: Option<(u32, u32)>) -> Result<(), String> {
    let bytes =
        std::fs::read(from).map_err(|e| format!("could not read {}: {e}", from.display()))?;
    write_bytes(to, &bytes, mode, owner)
}

fn write_file(path: &Path, text: &str, mode: u32, owner: Option<(u32, u32)>) -> Result<(), String> {
    write_bytes(path, text.as_bytes(), mode, owner)
}

/// Written whole to a sibling and renamed over the target, so a reader --
/// systemd, a service restarting -- sees the old file or the new one and
/// never a half. A running binary is replaced the same way, which is the
/// one way to replace one on Linux.
fn write_bytes(
    path: &Path,
    bytes: &[u8],
    mode: u32,
    owner: Option<(u32, u32)>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("could not create {}: {e}", parent.display()))?;
    }
    let tmp = path.with_extension("celastro-install");
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(mode);
    }
    let write = || -> std::io::Result<()> {
        let mut f = o.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()
    };
    write().map_err(|e| format!("could not write {}: {e}", tmp.display()))?;
    set_mode(&tmp, mode)?;
    chown(&tmp, owner)?;
    std::fs::rename(&tmp, path)
        .map_err(|e| format!("could not move {} into place: {e}", path.display()))
}

fn set_mode(path: &Path, mode: u32) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(|e| format!("could not set the mode of {}: {e}", path.display()))?;
    }
    let _ = (path, mode);
    Ok(())
}

fn chown(path: &Path, owner: Option<(u32, u32)>) -> Result<(), String> {
    #[cfg(unix)]
    {
        if let Some((uid, gid)) = owner {
            std::os::unix::fs::chown(path, Some(uid), Some(gid))
                .map_err(|e| format!("could not set the owner of {}: {e}", path.display()))?;
        }
    }
    let _ = (path, owner);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(args: &[&str]) -> Opts {
        let rest: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        parse(&rest, None, None, None, None).unwrap_or_else(|e| panic!("{args:?} must parse: {e}"))
    }

    fn parse_err(args: &[&str]) -> String {
        let rest: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        match parse(&rest, None, None, None, None) {
            Ok(o) => panic!("{args:?} must be refused, got {o:?}"),
            Err(e) => e,
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("celastro-install-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn secrets() -> Secrets {
        Secrets { token: Some("sixteen-bytes-ok!".into()), wire_token: Some("wire-secret".into()) }
    }

    #[test]
    fn a_bare_host_becomes_a_wire_address_on_the_default_port() {
        let o = parse_ok(&[
            "--node",
            "10.0.0.2",
            "--attach",
            "10.0.0.2,tcp://10.0.0.3:2352, 10.0.0.4:2353",
        ]);
        assert_eq!(o.node.as_deref(), Some("tcp://10.0.0.2:2352"));
        assert_eq!(
            o.attach,
            vec!["tcp://10.0.0.2:2352", "tcp://10.0.0.3:2352", "tcp://10.0.0.4:2353"]
        );
        assert_eq!(o.shard_bind, "0.0.0.0:2352");
    }

    #[test]
    fn a_bad_address_is_refused_rather_than_written_into_the_settings() {
        assert!(parse_err(&["--node", "10.0.0.2:port"]).contains("not a port"));
        assert!(parse_err(&["--node", "fe80::1"]).contains("[addr]:port"));
        assert!(parse_err(&["--node", "tcp://a b"]).contains("not a node address"));
        assert_eq!(
            parse_ok(&["--node", "[fe80::1]:2352"]).node.as_deref(),
            Some("tcp://[fe80::1]:2352")
        );
    }

    #[test]
    fn attach_or_a_role_without_a_node_is_refused() {
        assert!(parse_err(&["--attach", "10.0.0.3"]).contains("--node"));
        assert!(parse_err(&["--role", "coordinator"]).contains("--node"));
        assert!(parse_err(&["--node", "10.0.0.2", "--role", "witness"]).contains("coordinator"));
    }

    #[test]
    fn env_takes_variable_names_only_and_never_the_flags_own() {
        let o = parse_ok(&[
            "--env",
            "CELASTRO_INSERT_BATCH=5000",
            "--env=CELASTRO_AUTO_FAILOVER=on",
            "--env",
            "CELASTRO_INSERT_BATCH=100",
        ]);
        assert_eq!(
            o.env,
            vec![
                ("CELASTRO_AUTO_FAILOVER".to_string(), "on".to_string()),
                ("CELASTRO_INSERT_BATCH".to_string(), "100".to_string())
            ]
        );
        assert!(parse_err(&["--env", "lower=1"]).contains("not a variable name"));
        assert!(parse_err(&["--env", "CELASTRO_TOKEN=x"]).contains("install's own flags"));
        assert!(parse_err(&["--env", "NOVALUE"]).contains("NAME=VALUE"));
    }

    #[test]
    fn half_an_encryption_or_a_client_auth_without_certificates_is_refused() {
        assert!(parse_err(&["--master-key", "m"]).contains("both"));
        assert!(parse_err(&["--client-auth"]).contains("--tls"));
        assert!(parse_err(&["--frobnicate"]).contains("unknown flag"));
        assert!(parse_err(&["extra"]).contains("flags only"));
    }

    #[test]
    fn a_cluster_node_bound_to_loopback_is_refused() {
        let rest = vec!["--node".to_string(), "10.0.0.2".to_string()];
        let e = parse(&rest, None, Some(IpAddr::from([127, 0, 0, 1])), None, None).unwrap_err();
        assert!(e.contains("cannot be reached"));
    }

    #[test]
    fn a_missing_or_short_token_is_refused_before_anything_is_written() {
        let root = scratch("token");
        let mut o = parse_ok(&[]);
        o.root = Some(root.clone());
        let e = install(&o, &Secrets::default()).unwrap_err();
        assert!(e.contains("CELASTRO_TOKEN"));
        let e =
            install(&o, &Secrets { token: Some("short".into()), wire_token: None }).unwrap_err();
        assert!(e.contains("16 printable"));
        assert!(!root.join("etc").exists(), "a refusal must write nothing");
        let mut cluster = parse_ok(&["--node", "10.0.0.2"]);
        cluster.root = Some(root.clone());
        let e = install(
            &cluster,
            &Secrets { token: Some("sixteen-bytes-ok!".into()), wire_token: None },
        )
        .unwrap_err();
        assert!(e.contains("CELASTRO_WIRE_TOKEN"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_install_under_a_root_writes_the_unit_the_settings_and_the_binary() {
        let root = scratch("root");
        let tls = root.join("tls-in");
        std::fs::create_dir_all(&tls).unwrap();
        for f in ["tls.crt", "tls.key", "ca.crt"] {
            std::fs::write(tls.join(f), f).unwrap();
        }
        let mut o = parse_ok(&[
            "--node",
            "10.0.0.2",
            "--attach",
            "10.0.0.2,10.0.0.3",
            "--tls",
            tls.to_str().unwrap(),
            "--client-auth",
            "--env",
            "CELASTRO_AUTO_FAILOVER=on",
        ]);
        o.root = Some(root.clone());
        let report = install(&o, &secrets()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(report.started, None);
        let env = std::fs::read_to_string(root.join("etc/celastro/celastro.env")).unwrap();
        assert!(env.contains("CELASTRO_TOKEN=\"sixteen-bytes-ok!\"\n"), "{env}");
        assert!(env.contains("CELASTRO_NODE=\"tcp://10.0.0.2:2352\"\n"), "{env}");
        assert!(
            env.contains("CELASTRO_ATTACH=\"tcp://10.0.0.2:2352,tcp://10.0.0.3:2352\"\n"),
            "{env}"
        );
        assert!(env.contains("CELASTRO_WIRE_TOKEN=\"wire-secret\"\n"), "{env}");
        assert!(env.contains("CELASTRO_TLS_CLIENT_AUTH=\"required\"\n"), "{env}");
        assert!(env.contains("CELASTRO_AUTO_FAILOVER=\"on\"\n"), "{env}");
        assert!(
            env.contains(&format!(
                "CELASTRO_TLS_CA=\"{}\"\n",
                root.join("etc/celastro/tls/ca.crt").display()
            )),
            "{env}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("etc/celastro/tls/tls.key")).unwrap(),
            "tls.key"
        );
        let unit =
            std::fs::read_to_string(root.join("etc/systemd/system/celastro.service")).unwrap();
        assert!(unit.contains("ExecStart=/usr/local/bin/celastro --dir /var/lib/celastro --port 8787 serve --bind 0.0.0.0 --shard-bind 0.0.0.0:2352\n"), "{unit}");
        assert!(unit.contains("EnvironmentFile=/etc/celastro/celastro.env\n"), "{unit}");
        assert!(unit.contains("ReadWritePaths=/var/lib/celastro\n"), "{unit}");
        assert!(
            unit.contains("StandardOutput=null\n"),
            "the token's URL must not reach the journal"
        );
        assert!(root.join("var/lib/celastro").is_dir());
        assert!(root.join("usr/local/bin/celastro").is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |p: &str| std::fs::metadata(root.join(p)).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode("etc/celastro/celastro.env"), 0o640);
            assert_eq!(mode("etc/celastro/tls/tls.key"), 0o640);
            assert_eq!(mode("usr/local/bin/celastro"), 0o755);
            assert_eq!(mode("var/lib/celastro"), 0o700);
        }
        // Text output names the essentials.
        let text = report.text();
        assert!(
            text.contains("tcp://10.0.0.2:2352") && text.contains("https://0.0.0.0:8787"),
            "{text}"
        );
        assert!(report.json().contains("\"kind\":\"install\""));

        // A second install with other settings rewrites the files.
        let mut again = parse_ok(&["--env", "CELASTRO_INSERT_BATCH=100"]);
        again.root = Some(root.clone());
        install(&again, &secrets()).unwrap_or_else(|e| panic!("{e}"));
        let env = std::fs::read_to_string(root.join("etc/celastro/celastro.env")).unwrap();
        assert!(
            !env.contains("CELASTRO_NODE") && env.contains("CELASTRO_INSERT_BATCH=\"100\"\n"),
            "{env}"
        );
        let unit =
            std::fs::read_to_string(root.join("etc/systemd/system/celastro.service")).unwrap();
        assert!(!unit.contains("--shard-bind"), "{unit}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_settings_file_escapes_what_systemd_would_misread() {
        let s = env_file(&[("A".into(), "say \"hi\" \\ there".into())]);
        assert!(s.contains("A=\"say \\\"hi\\\" \\\\ there\"\n"), "{s}");
    }

    #[test]
    fn a_live_install_without_root_is_refused_with_the_way_out() {
        if is_root() {
            return;
        }
        let o = parse_ok(&[]);
        let e = install(&o, &secrets()).unwrap_err();
        assert!(e.contains("--root"), "{e}");
    }
}
