//
// Copyright 2025 Tabs Data Inc.
//

use std::process::Output;
use tracing::info;

pub fn log_std_out_and_err(output: &Output) {
    match String::from_utf8(output.clone().stdout) {
        Ok(output) => {
            print!("{}", output);
            info!("{}", output)
        }
        Err(e) => {
            print!("Error processing system standard output: {}", e);
            info!("Error processing system standard output: {}", e)
        }
    };
    match String::from_utf8(output.clone().stderr) {
        Ok(output) => {
            eprint!("{}", output);
            info!("{}", output)
        }
        Err(e) => {
            eprint!("Error processing system standard error: {}", e);
            info!("Error processing system standard error: {}", e)
        }
    };
}
