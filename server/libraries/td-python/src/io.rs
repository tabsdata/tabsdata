//
// Copyright 2025 Tabs Data Inc.
//

use std::process::Output;
use tracing::info;

pub fn log_std_out_and_err(output: &Output) {
    match String::from_utf8(output.clone().stdout) {
        Ok(output) => {
            print!("Standard Output:\n{}", output);
            info!("Standard Output:\n{}", output)
        }
        Err(e) => {
            print!(
                "Standard Output: Error processing system standard output: {}",
                e
            );
            info!(
                "Standard Output: Error processing system standard output: {}",
                e
            )
        }
    };
    match String::from_utf8(output.clone().stderr) {
        Ok(output) => {
            eprint!("Standard Error:\n{}", output);
            info!("Standard Error:\n{}", output)
        }
        Err(e) => {
            eprint!(
                "Standard Error: Error processing system standard error: {}",
                e
            );
            info!(
                "Standard Error: Error processing system standard error: {}",
                e
            )
        }
    };
}
