//
// Copyright 2024 Tabs Data Inc.
//

use clap::Parser;
use path_slash::PathBufExt;
use std::env;
use std::env::set_var;
use std::ffi::OsString;
use td_common::attach::attach;
use td_common::logging;
use td_common::server::{INSTANCE_PATH_ENV, INSTANCE_URI_ENV};
use td_supervisor::services::supervisor;
use td_supervisor::services::supervisor::{prepend_slash, Arguments};
use tracing::{info, Level};

#[attach(signal = "supervisor")]
pub fn main() {
    let instance_path = Arguments::parse().instance_path();
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(INSTANCE_URI_ENV, prepend_slash(instance_path.clone()));
    }
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
