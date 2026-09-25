//
// Copyright 2024 Tabsdata Inc.
//

#![allow(linker_messages)]

use std::{env, process};
use td_common::attach::attach;
use td_common::crypto::install_crypto_provider;
use td_common::{about, logging};
use td_process::launcher::hooks;
use td_process::pathenv::extend_path;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const LOG_CONFIG_LONG: &str = "--log-config";
const LOG_CONFIG_SHORT: &str = "-c";

// Keep the schema-component registrations in this binary's final link (see the
// macro docs); without the anchor the linker drops the inventory submissions and
// the registry comes up empty.
te_database::force_link!();

#[attach(signal = "td_apiserver")]
fn main() {
    extend_path();
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    match log_config_arg() {
        Some(path) => logging::start_with_config_file(&path),
        None => logging::start(),
    }

    install_crypto_provider();

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apiserver with command: \n{}", command);

    td_apiserver::run();
}

fn log_config_arg() -> Option<String> {
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == LOG_CONFIG_LONG || arg == LOG_CONFIG_SHORT {
            return iter.next().cloned();
        }
        if let Some(value) = arg.strip_prefix(&format!("{LOG_CONFIG_LONG}=")) {
            return Some(value.to_owned());
        }
        if let Some(value) = arg.strip_prefix(&format!("{LOG_CONFIG_SHORT}=")) {
            return Some(value.to_owned());
        }
    }
    None
}
