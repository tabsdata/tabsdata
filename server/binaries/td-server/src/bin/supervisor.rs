//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use tabsdatalib::bin::supervisor;
use td_attach::attach;
use td_common::logging;
use tracing::{info, Level};

#[attach(signal = "supervisor")]
pub fn main() {
    logging::start(Level::DEBUG, None, true);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running supervisor with command: \n{}", command);

    supervisor::start();
}
