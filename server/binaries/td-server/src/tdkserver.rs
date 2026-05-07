//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Write;
use std::process::{Command, Stdio, exit};
use std::{env, fs};
use td_build::version::TABSDATA_VERSION;
use td_common::attach::attach;
use td_common::env::{TABSDATA_HOME_DIR, get_home_dir};
use td_common::logging;
use td_common::logging::{CONSOLE_PROFILE, LogFormat};
use td_common::server::TD_DETACHED_SUBPROCESSES;
use td_common::settings::TRUE;
use td_kubernetes::client::tdkserver;
use td_kubernetes::client::tdkserver::show_banner;
use te_system::license::{
    ACCEPT_TERMS_ARGUMENT, ACCEPT_TERMS_N, ACCEPT_TERMS_NO, ACCEPT_TERMS_Y, ACCEPT_TERMS_YES,
    LicenseAcceptance, TabsdataLicense,
};
use tm_workspace::workspace_root;
use tracing::Level;

const ACK: &str = ".ack";

const COMPATIBILITY_PY: &str = include_str!(concat!(
    workspace_root!(),
    "/client/platform/src/main/python/tabsdata/_utils/compatibility.py"
));

fn parse_accept_terms_value(value: &str) -> Option<bool> {
    match value.to_lowercase().as_str() {
        accept if accept == ACCEPT_TERMS_Y || accept == ACCEPT_TERMS_YES => Some(true),
        accept if accept == ACCEPT_TERMS_N || accept == ACCEPT_TERMS_NO => Some(false),
        invalid => {
            eprintln!(
                " Invalid value '{}' for {ACCEPT_TERMS_ARGUMENT}. Expected: {ACCEPT_TERMS_Y}, {ACCEPT_TERMS_YES}, {ACCEPT_TERMS_N}, or {ACCEPT_TERMS_NO}.",
                invalid
            );
            exit(1);
        }
    }
}

fn extract_accept_terms() -> Option<bool> {
    let arguments: Vec<String> = env::args().collect();
    let prefix = format!("{}=", ACCEPT_TERMS_ARGUMENT);
    for index in 0..arguments.len() {
        if arguments[index] == ACCEPT_TERMS_ARGUMENT {
            return match arguments.get(index + 1) {
                Some(value) => parse_accept_terms_value(value),
                None => {
                    eprintln!(
                        " Missing value for {ACCEPT_TERMS_ARGUMENT}. Expected: {ACCEPT_TERMS_Y}, {ACCEPT_TERMS_YES}, {ACCEPT_TERMS_N}, or {ACCEPT_TERMS_NO}."
                    );
                    exit(1);
                }
            };
        } else if let Some(value) = arguments[index].strip_prefix(prefix.as_str()) {
            return parse_accept_terms_value(value);
        }
    }
    None
}

fn check_banner(accept_terms: Option<bool>) -> std::io::Result<bool> {
    let ack = get_home_dir().join(TABSDATA_HOME_DIR).join(ACK);
    let ack_version = fs::read_to_string(&ack).unwrap_or_else(|_| "".to_string());
    if ack_version.trim() == TABSDATA_VERSION.trim() {
        return Ok(true);
    }
    show_banner();
    let license = TabsdataLicense;
    if !license.check_license_acceptance(accept_terms)? {
        exit(1);
    }
    if let Some(parent) = ack.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&ack)?;
    file.write_all(TABSDATA_VERSION.trim().as_bytes())?;
    Ok(false)
}

fn check_polars() {
    let mut python = Command::new("python")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start python process");

    {
        let stdin = python.stdin.as_mut().expect("Failed to open python stdin");
        stdin
            .write_all(COMPATIBILITY_PY.as_bytes())
            .expect("Failed to write python script");
    }
    let status = python.wait().expect("Failed to execute python script");
    if !status.success() {
        eprintln!("!!! Polars compatibility check failed: {status}");
        exit(1);
    }
}

#[tokio::main]
#[attach(signal = "tdkserver")]
async fn main() {
    logging::start(
        Some(Level::INFO),
        Some(CONSOLE_PROFILE.to_string()),
        false,
        false,
        Some(LogFormat::Compact),
    );
    let accept_terms = extract_accept_terms();
    let previously_acknowledged = check_banner(accept_terms).unwrap_or(false);
    if !previously_acknowledged {
        check_polars();
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        env::set_var(TD_DETACHED_SUBPROCESSES, TRUE);
    }
    tdkserver::start().await;
}
