//
// Copyright 2024 Tabs Data Inc.
//

use clap::Parser;
use path_slash::PathBufExt;
use std::env;
use std::env::set_var;
use std::ffi::OsString;
use td_common::attach::attach;
use td_common::env::check_flag_env;
use td_common::logging;
use td_common::server::{INSTANCE_PATH_ENV, INSTANCE_URI_ENV, TD_DETACHED_SUBPROCESSES};
use td_process::launcher::hooks;
use td_supervisor::services::supervisor;
use td_supervisor::services::supervisor::{Arguments, prepend_slash};
use tracing::{Level, info};

#[attach(signal = "supervisor")]
pub fn main() {
    hooks::panic();

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
