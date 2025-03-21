//
// Copyright 2024 Tabs Data Inc.
//

use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn, Level};

use td_common::logging;
use td_common::logging::{LogOutput, CURRENT_DIR};

pub fn main() {
    logging::start(
        Level::TRACE,
        Some(LogOutput::File(PathBuf::from(CURRENT_DIR))),
        false,
    );

    trace!("Simple trace message");
    debug!("Simple debug message");
    info!("Simple info message");
    warn!("Simple warn message");
    error!("Simple error message");

    info!(message = "Qualified message");

    let name = "Stanley";
    let surname = "Kubrick";
    info!("Hey, {} {}!", name, surname);

    sleep(Duration::from_secs(5));
}
