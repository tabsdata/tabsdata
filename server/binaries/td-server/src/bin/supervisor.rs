//
// Copyright 2024 Tabs Data Inc.
//

use clap::Parser;
use path_slash::PathBufExt;
use std::env;
use std::env::set_var;
use std::ffi::OsString;
use tabsdatalib::bin::platform::supervisor;
use tabsdatalib::bin::platform::supervisor::{prepend_slash, Arguments};
use td_common::attach::attach;
use td_common::logging;
use td_common::server::{INSTANCE_PATH_ENV, INSTANCE_URI_ENV};
use tracing::{info, Level};

#[attach(signal = "supervisor")]
pub fn main() {
    let instance_path = Arguments::parse().instance_path();
    set_var(INSTANCE_URI_ENV, prepend_slash(instance_path.clone()));
    set_var(
        INSTANCE_PATH_ENV,
        OsString::from(
            instance_path
                .clone()
                .to_slash()
                .unwrap_or_else(|| {
                    panic!("Invalid characters in instance path: {:?}", instance_path)
                })
                .into_owned(),
        ),
    );

    logging::start(Level::DEBUG, None, true);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running supervisor with command: \n{}", command);

    supervisor::start();
}
