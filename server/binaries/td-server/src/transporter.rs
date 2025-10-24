//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::process;
use td_common::about;
use td_common::attach::attach;
use td_process::launcher::hooks;
use td_shuttle::transporter::cli;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "transporter")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    cli::run()
}
