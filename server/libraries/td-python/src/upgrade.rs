//
// Copyright 2025 Tabs Data Inc.
//

use crate::error::PythonError::{InstanceUpgradeError, InstanceUpgradePanic};
use crate::io::log_std_out_and_err;
use std::path::PathBuf;
use std::process::{exit, Command, Output};
use td_common::error::TdError;
use td_common::os::name_program;
use td_common::status::ExitStatus::GeneralError;
use tracing::error;

pub const TDUPGRADE_PROGRAM: &str = "tdupgrade";
pub const TDUPGRADE_ARGUMENT_INSTANCE: &str = "--instance";

pub fn perform(instance: &PathBuf) -> Result<(), TdError> {
    let tdupgrade = name_program(&PathBuf::from(TDUPGRADE_PROGRAM));
    let output = Command::new(tdupgrade)
        .arg(TDUPGRADE_ARGUMENT_INSTANCE)
        .arg(instance)
        .output()
        .map_err(InstanceUpgradePanic)?;
    dump(&output);
    if !output.status.success() {
        error!("Bad exit code upgrading instance");
        return Err(TdError::new(InstanceUpgradeError(output.status)));
    };
    Ok(())
}

pub fn upgrade(instance: &PathBuf) {
    if let Err(e) = perform(instance) {
        error!("Failed to upgrade instance: {}", e);
        exit(GeneralError.code());
    }
}

fn dump(output: &Output) {
    log_std_out_and_err(output);
}
