//
// Copyright 2024 Tabs Data Inc.
//

use std::time::Duration;
use std::{process, thread};
use td_common::attach::attach;
use td_common::logging;
use tracing::{Level, info};

#[attach(signal = "hal")]
pub fn main() {
    logging::start(Level::DEBUG, None, false);

    let pid = process::id();
    info!("Hey, Dave, what are you doing?");
    for i in 0..36 {
        info!(
            "( {} - {} ) - Hello, Dave. Shall we continue the game?",
            pid, i
        );
        thread::sleep(Duration::from_secs(5));
    }
    info!("Dave, I don't understand why are you doing this to me...");
}
