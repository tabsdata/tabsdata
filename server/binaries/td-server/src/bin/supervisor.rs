//
// Copyright 2024 Tabs Data Inc.
//

use clap::Parser;
use std::env;
use std::env::set_var;
use tabsdatalib::bin::platform::supervisor;
use tabsdatalib::bin::platform::supervisor::{prepend_slash, Arguments};
use td_attach::attach;
use td_common::logging;
use td_common::server::INSTANCE_ENV;
use tracing::{info, Level};

#[attach(signal = "supervisor")]
pub fn main() {
    set_var(
        INSTANCE_ENV,
        prepend_slash(Arguments::parse().instance_path()),
    );

    logging::start(Level::DEBUG, None, true);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running supervisor with command: \n{}", command);

    supervisor::start();
}
