//
// Copyright 2024 Tabs Data Inc.
//

use tracing::{Level, debug, error, info, trace, warn};

use td_common::logging;
use td_common::logging::LogOutput;

pub fn main() {
    logging::start(Level::TRACE, Some(LogOutput::StdOut), false);

    trace!("Simple trace message");
    debug!("Simple debug message");
    info!("Simple info message");
    warn!("Simple warn message");
    error!("Simple error message");

    info!(message = "Qualified message");

    let name = "Stanley";
    let surname = "Kubrick";
    info!("Hey, {} {}!", name, surname);
}
