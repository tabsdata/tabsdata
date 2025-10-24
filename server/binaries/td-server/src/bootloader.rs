//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::process;
use td_common::about;
use td_common::attach::attach;
use td_common::logging;
use td_process::launcher::hooks;
use td_supervisor::services::bootloader;
use tracing::{Level, info};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "bootloader")]
pub fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start(Level::INFO, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running bootloader with command: \n{}", command);

    bootloader::start();
}
