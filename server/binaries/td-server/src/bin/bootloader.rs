//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use tabsdatalib::bin::platform::bootloader;
use td_common::attach::attach;
use td_common::logging;
use tracing::{info, Level};

#[attach(signal = "bootloader")]
pub fn main() {
    logging::start(Level::DEBUG, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running bootloader with command: \n{}", command);

    bootloader::start();
}
