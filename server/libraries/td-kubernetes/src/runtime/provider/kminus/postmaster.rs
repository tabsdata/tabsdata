//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as SystemCommand;
use std::time::Duration;

use td_common::os::EXE_SUFFIX;
use tracing::warn;

use crate::core::system::subprocess;

const POSTMASTER_PID_FILE: &str = "postmaster.pid";
const POSTMASTER_OPTS_FILE: &str = "postmaster.opts";
const PG_CTL_BINARY: &str = "pg_ctl";
const PG_CTL_LABEL: &str = "pg_ctl";
const PG_CTL_STOP_SECONDS: u64 = 60;
const PG_CTL_TIMEOUT: Duration = Duration::from_secs(PG_CTL_STOP_SECONDS + 10);
const FORCE_KILL_WAIT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Postmaster {
    pub pid: u32,
    binary: PathBuf,
}

pub fn running_on(data_dir: &Path) -> Option<Postmaster> {
    let pid = recorded_pid(data_dir)?;
    let binary = recorded_binary(data_dir)?;
    is_postmaster(pid, &binary, data_dir).then_some(Postmaster { pid, binary })
}

pub fn stop(data_dir: &Path, postmaster: &Postmaster, verbose: bool) -> bool {
    stop_with_pg_ctl(data_dir, &postmaster.binary, verbose);
    if !is_postmaster(postmaster.pid, &postmaster.binary, data_dir) {
        return true;
    }
    warn!(
        pid = postmaster.pid,
        "The database server outlived 'pg_ctl stop'; killing its process tree"
    );
    subprocess::force_terminate_process_tree(postmaster.pid, FORCE_KILL_WAIT);
    !is_postmaster(postmaster.pid, &postmaster.binary, data_dir)
}

fn is_postmaster(pid: u32, binary: &Path, data_dir: &Path) -> bool {
    let Some((Some(exe), cwd)) = subprocess::process_paths(pid) else {
        return false;
    };
    same_path(&exe, binary) && cwd.is_none_or(|cwd| same_path(&cwd, data_dir))
}

fn stop_with_pg_ctl(data_dir: &Path, binary: &Path, verbose: bool) {
    let pg_ctl = binary.with_file_name(format!("{PG_CTL_BINARY}{EXE_SUFFIX}"));
    let mut command = SystemCommand::new(&pg_ctl);
    command
        .arg("stop")
        .arg("-D")
        .arg(data_dir)
        .args(["-m", "fast", "-w", "-t"])
        .arg(PG_CTL_STOP_SECONDS.to_string());
    subprocess::apply_detached_flags(&mut command);
    match subprocess::run_with_timeout(&mut command, PG_CTL_LABEL, verbose, PG_CTL_TIMEOUT) {
        Ok(Some(output)) if output.status.success() => {}
        Ok(Some(output)) => warn!(
            status = %output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "'pg_ctl stop' did not stop the database server"
        ),
        Ok(None) => warn!("'pg_ctl stop' did not finish in time"),
        Err(error) => warn!(
            pg_ctl = %pg_ctl.display(),
            %error,
            "Could not run 'pg_ctl stop'"
        ),
    }
}

fn recorded_pid(data_dir: &Path) -> Option<u32> {
    let content = fs::read_to_string(data_dir.join(POSTMASTER_PID_FILE)).ok()?;
    content.lines().next()?.trim().parse::<u32>().ok()
}

fn recorded_binary(data_dir: &Path) -> Option<PathBuf> {
    let content = fs::read_to_string(data_dir.join(POSTMASTER_OPTS_FILE)).ok()?;
    let line = content.lines().next()?;
    let binary = line.split(" \"").next()?.trim();
    (!binary.is_empty()).then(|| PathBuf::from(binary))
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}
