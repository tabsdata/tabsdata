//
// Copyright 2025 Tabs Data Inc.
//

use std::{env, process};
use td_common::about;
use td_common::attach::attach;
use td_process::launcher::hooks;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "td_crontrigger")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    td_crontrigger::run();
}
