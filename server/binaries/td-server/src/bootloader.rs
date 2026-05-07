//
// Copyright 2024 Tabs Data Inc.
//

use std::{env, process};
use td_common::attach::attach;
use td_common::{about, logging};
use td_process::launcher::hooks;
use td_supervisor::services::bootloader;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "td_bootloader")]
pub fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start(None, None, false, true, None);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running bootloader with command: \n{}", command);

    bootloader::start();
}
