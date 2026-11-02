//
// Copyright 2026 Tabsdata Inc.
//

#![allow(linker_messages)]

use std::{env, process};
use td_common::attach::attach;
use td_common::crypto::install_crypto_provider;
use td_common::{about, logging};
use td_process::launcher::hooks;
use td_process::pathenv::extend_path;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "td_tdmanager")]
fn main() {
    extend_path();
    hooks::panic();

    if env::args().any(|argument| argument == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start();
    install_crypto_provider();
    td_apiserver::tdmanager::run(te_edition::edition());
}
