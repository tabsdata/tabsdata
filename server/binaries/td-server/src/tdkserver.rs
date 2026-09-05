//
// Copyright 2026 Tabsdata Inc.
//

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio, exit};
use std::{env, fs};
use td_build::version::TABSDATA_VERSION;
use td_common::attach::attach;
use td_common::env::{TABSDATA_HOME_DIR, get_home_dir};
use td_common::logging;
use td_common::logging::{
    LogFormat, LogProfile, LogProfileOutput, LogSinkConfig, LogSinkOutput, LoggingOptions,
};
use td_common::os::TD_DETACHED_SUBPROCESSES;
use td_common::settings::TRUE;
use td_kubernetes::base::constants::{
    ENV_TD_KUBE_UV_MODE, ENV_TD_KUBE_UV_MODE_NOWARN, LOG_SIZE_WARNING, UV_MODE_LINKED,
};
use td_kubernetes::base::privacy::mask_command_line;
use td_kubernetes::base::trace;
use td_kubernetes::client::registry::tdkserver_log_file;
use td_kubernetes::client::tdkserver;
use td_kubernetes::client::tdkserver::widgets::{show_banner, show_caution};
use te_system::license::{
    ACCEPT_TERMS_ARGUMENT, ACCEPT_TERMS_N, ACCEPT_TERMS_NO, ACCEPT_TERMS_Y, ACCEPT_TERMS_YES,
    LicenseAcceptance, TabsdataLicense,
};
use tm_workspace::workspace_root;
use tracing::Level;
use tracing::debug;

const ACK: &str = ".ack";

const BANNER_WIDTH: usize = 100;
const PROGRAM_TDKSERVER: &str = "tdkserver";

const HELP_COMMAND: &str = "help";
const HELP_FLAGS: [&str; 2] = ["--help", "-h"];
const FULL_FLAG: &str = "--full";

const RUST_LOG: &str = "RUST_LOG";
const RUST_LOG_DIRECTIVES: &str = "info,kube_client::client::tls=error,td_common::os=error";

const FILE_LOG_DIRECTIVES: &[&str] = &[
    "debug",
    "kube_client::client::builder=info",
    "hyper_util=info",
    "hyper_rustls=info",
    "tower=info",
    "rustls=info",
];

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

fn full_help_requested() -> bool {
    let mut arguments = env::args().skip(1).skip_while(|argument| {
        argument != HELP_COMMAND && !HELP_FLAGS.contains(&argument.as_str())
    });
    arguments.next().is_some() && arguments.any(|argument| argument == FULL_FLAG)
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

fn tdkserver_log_profile() -> LogProfile {
    LogProfile {
        level: Level::INFO.to_string(),
        directives: Vec::new(),
        output: LogProfileOutput::StdOut,
        format: Some(LogFormat::CompactColoredSpinner),
        sinks: vec![LogSinkConfig {
            file: log_file_path(),
            directives: FILE_LOG_DIRECTIVES
                .iter()
                .map(|directive| (*directive).to_owned())
                .collect(),
            output: LogSinkOutput::File,
            format: Some(LogFormat::Full),
            without_time: Some(false),
        }],
    }
}

fn log_file_path() -> Option<String> {
    tdkserver_log_file()
        .ok()
        .map(|path| path.display().to_string())
}

fn log_command_banner() {
    let mut arguments = env::args();
    let program = arguments
        .next()
        .map(PathBuf::from)
        .and_then(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| PROGRAM_TDKSERVER.to_owned());
    let line = mask_command_line(std::iter::once(program).chain(arguments));
    debug!("{}", "=".repeat(BANNER_WIDTH));
    debug!("{line}");
    debug!("{}", "=".repeat(BANNER_WIDTH));
}

fn cautions_muted() -> bool {
    matches!(
        env::var(ENV_TD_KUBE_UV_MODE_NOWARN)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "true" | "1" | "yes" | "on"
    )
}

fn warn_on_uv_mode() {
    if cautions_muted() {
        return;
    }
    let value = env::var(ENV_TD_KUBE_UV_MODE).unwrap_or_default();
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    let mut caution = format!(
        "Caution: the environment variable {ENV_TD_KUBE_UV_MODE} is set to '{value}'. \
         Depending on its value, tdkserver will build its Python environments from your \
         current (hydrated) virtual environment or from a development package index, \
         instead of resolving them from PyPI. This instance therefore reflects your \
         development setup rather than what an end user installing tabsdata from PyPI \
         gets, and it may behave differently. Unset the variable to restore the standard \
         behaviour."
    );
    if value.eq_ignore_ascii_case(UV_MODE_LINKED) {
        caution.push_str(&format!(
            "\n\nWith '{UV_MODE_LINKED}' the instance does not get its own Python environments \
             at all: every one of them is a symbolic link to the environment tdkserver runs \
             from. Nothing isolates the environment running user functions from the one running \
             the server, and any change to your environment reaches the instance immediately, \
             including one that breaks it. Use it for development only."
        ));
    }
    show_caution(
        &caution,
        80,
        true,
        &[ENV_TD_KUBE_UV_MODE, &format!("'{value}'")],
    );
}

fn warn_on_log_size() {
    if cautions_muted() {
        return;
    }
    let Ok(path) = tdkserver_log_file() else {
        return;
    };
    let Ok(metadata) = fs::metadata(&path) else {
        return;
    };
    if metadata.len() <= LOG_SIZE_WARNING {
        return;
    }
    let megabytes = metadata.len() / (1024 * 1024);
    let limit = LOG_SIZE_WARNING / (1024 * 1024);
    show_caution(
        &format!(
            "The tdkserver log has grown to {megabytes} MB, above the \
             notice threshold of {limit} MB.\n\n{}\n\nYou may want to delete \
             it, or rename it, to start a fresh log.",
            path.display()
        ),
        80,
        true,
        &[],
    );
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
    let full_help = full_help_requested();
    let previous_rust_log = env::var_os(RUST_LOG);
    unsafe {
        env::set_var(RUST_LOG, RUST_LOG_DIRECTIVES);
    }
    logging::start_with_profile(
        LoggingOptions {
            max_level: Some(Level::DEBUG),
            profile: None,
            enable_bootstrap_notice: false,
            format: LogFormat::CompactColoredSpinner,
            ..Default::default()
        },
        tdkserver_log_profile(),
    );
    trace::open();
    if let Some(operation) = trace::operation_of(env::args()) {
        trace::set_operation(&operation);
    }
    log_command_banner();
    unsafe {
        match previous_rust_log {
            Some(value) => env::set_var(RUST_LOG, value),
            None => env::remove_var(RUST_LOG),
        }
    }
    if !full_help {
        let accept_terms = extract_accept_terms();
        let previously_acknowledged = check_banner(accept_terms).unwrap_or(false);
        if !previously_acknowledged {
            check_polars();
        }
        warn_on_uv_mode();
        warn_on_log_size();
    }
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        env::set_var(TD_DETACHED_SUBPROCESSES, TRUE);
    }
    tdkserver::start().await;
}
