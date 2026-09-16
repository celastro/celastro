//! `celastro-cli`: the name the command had until 0.40.0. Says so once on
//! stderr, then becomes the `celastro` installed beside it -- `exec`, so the
//! process, its pid and its signals are the real tool's, which matters when
//! this is a container's entrypoint. Goes away in a later release.

fn main() {
    eprintln!("celastro-cli is now called celastro; this name goes away in a later release");
    let me = std::env::current_exe().unwrap_or_default();
    let real = me.with_file_name("celastro");
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let e = std::process::Command::new(&real).args(&args).exec();
        eprintln!("celastro-cli: could not run {}: {e}", real.display());
        std::process::exit(2);
    }
    #[cfg(not(unix))]
    {
        match std::process::Command::new(&real).args(&args).status() {
            Ok(s) => std::process::exit(s.code().unwrap_or(1)),
            Err(e) => {
                eprintln!("celastro-cli: could not run {}: {e}", real.display());
                std::process::exit(2);
            }
        }
    }
}
