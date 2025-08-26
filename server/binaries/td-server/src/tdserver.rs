//
// Copyright 2024 Tabs Data Inc.
//

use std::io::Write;
use std::process::{Command, Stdio, exit};
use std::{env, fs};
use td_build::version::TABSDATA_VERSION;
use td_common::attach::attach;
use td_common::env::{TABSDATA_HOME_DIR, get_home_dir};
use td_common::logging;
use td_common::logging::LogOutput;
use td_common::server::TD_DETACHED_SUBPROCESSES;
use td_common::settings::TRUE;
use td_supervisor::services::tdserver;
use td_supervisor::services::tdserver::show_banner;
use tm_workspace::workspace_root;
use tracing::Level;

const ACK: &str = ".ack";

const COMPATIBILITY_PY: &str = include_str!(concat!(
    workspace_root!(),
    "/client/td-sdk/tabsdata/_utils/compatibility.py"
));

fn check_banner() -> std::io::Result<()> {
    let ack = get_home_dir().join(TABSDATA_HOME_DIR).join(ACK);
    let ack_version = fs::read_to_string(&ack).unwrap_or_else(|_| "".to_string());
    if ack_version.trim() == TABSDATA_VERSION.trim() {
        return Ok(());
    }
    show_banner();
    if let Some(parent) = ack.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&ack)?;
    file.write_all(TABSDATA_VERSION.trim().as_bytes())?;
    Ok(())
}

fn check_polars() {
    let mut py = Command::new("python")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start python process");

    {
        let stdin = py.stdin.as_mut().expect("Failed to open python stdin");
        stdin
            .write_all(COMPATIBILITY_PY.as_bytes())
            .expect("Failed to write python script");
    }
    let status = py.wait().expect("Failed to execute python script");
    if !status.success() {
        eprintln!("!!! Polars compatibility check failed: {status}");
        exit(1);
    }
}

#[tokio::main]
#[attach(signal = "tdserver")]
async fn main() {
    logging::start(Level::INFO, Some(LogOutput::StdOut), false);
    let _ = check_banner();
    check_polars();
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        env::set_var(TD_DETACHED_SUBPROCESSES, TRUE);
    }
    tdserver::start().await;
}
