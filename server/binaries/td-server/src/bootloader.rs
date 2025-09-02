//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use td_common::attach::attach;
use td_common::logging;
use td_process::launcher::hooks;
use td_supervisor::services::bootloader;
use tracing::{Level, info};

#[attach(signal = "bootloader")]
pub fn main() {
    hooks::panic();

    logging::start(Level::INFO, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running bootloader with command: \n{}", command);

    bootloader::start();
}
