//
// Copyright 2024 Tabs Data Inc.
//

use std::time::Duration;
use std::{process, thread};
use td_attach::attach;
use td_common::logging;
use tracing::{info, Level};

#[attach(signal = "hal")]
pub fn main() {
    logging::start(Level::DEBUG, None, false);

    let pid = process::id();
    info!("One hundred percent");
    for i in 0..24 {
        info!(
            "( {} - {} ) - Before you get teary, try to remember that as a robot I have to do anything you say, anyway",
            pid, i
        );
        thread::sleep(Duration::from_secs(5));
    }
    info!("I’m not joking.");
}
