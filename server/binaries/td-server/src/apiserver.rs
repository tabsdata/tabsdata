//
// Copyright 2024 Tabs Data Inc.
//

use std::{env, process};
use td_common::attach::attach;
use td_common::{about, logging};
use td_process::launcher::hooks;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "apiserver")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start(None, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apiserver with command: \n{}", command);

    td_apiserver::run();
}
