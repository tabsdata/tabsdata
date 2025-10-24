//
// Copyright 2024 Tabs Data Inc.
//

use clap::Parser;
use path_slash::PathBufExt;
use std::env;
use std::env::set_var;
use std::ffi::OsString;
use std::process;
use td_common::about;
use td_common::attach::attach;
use td_common::env::check_flag_env;
use td_common::logging;
use td_common::server::{INSTANCE_PATH_ENV, INSTANCE_URI_ENV, TD_DETACHED_SUBPROCESSES};
use td_process::launcher::hooks;
use td_supervisor::services::supervisor;
use td_supervisor::services::supervisor::{Arguments, prepend_slash};
use tracing::{Level, info};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "supervisor")]
pub fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    let instance_path = Arguments::parse().instance_path();
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        set_var(INSTANCE_URI_ENV, prepend_slash(instance_path.clone()));
    }
    unsafe {
        set_var(
            INSTANCE_PATH_ENV,
            OsString::from(
                instance_path
                    .clone()
                    .to_slash()
                    .unwrap_or_else(|| {
                        panic!("Invalid characters in instance path: {instance_path:?}")
                    })
                    .into_owned(),
            ),
        );
    }

    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        unsafe {
            use windows_sys::Win32::System::Console::FreeConsole;

            let _ = FreeConsole();
        }
    }

    logging::start(Level::INFO, None, true);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running supervisor with command: \n{}", command);

    supervisor::start();
}
