//
// Copyright 2025 Tabs Data Inc.
//

use crate::error::PythonError::{
    EmptyVersionFile, InstanceUpgradeError, InstanceUpgradePanic, InvalidVersionFile,
    InvalidVersionFormat,
};
use crate::io::log_std_out_and_err;
use semver::Version;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use td_build::version::TABSDATA_VERSION;
use td_common::os::name_program;
use td_error::TdError;
use tracing::error;

const VERSION_FILE: &str = ".version";
const SEED_VERSION: &str = "0.9.0";

pub const TDUPGRADER_PROGRAM: &str = "_tdupgrader";
pub const TDUPGRADER_ARGUMENT_INSTANCE: &str = "--instance";
pub const TDUPGRADER_ARGUMENT_EXECUTE: &str = "--execute";

pub fn perform(instance: &PathBuf, execute: bool) -> Result<(), TdError> {
    let tdupgrader = name_program(&PathBuf::from(TDUPGRADER_PROGRAM));
    let output = Command::new(tdupgrader)
        .arg(TDUPGRADER_ARGUMENT_INSTANCE)
        .arg(instance)
        .args(execute.then_some(TDUPGRADER_ARGUMENT_EXECUTE).iter())
        .output()
        .map_err(InstanceUpgradePanic)?;
    dump(&output);
    if !output.status.success() {
        error!("Bad exit code upgrading instance");
        return Err(TdError::new(InstanceUpgradeError(output.status)));
    };
    Ok(())
}

pub fn upgrade(instance: &PathBuf, execute: bool) -> Result<(), TdError> {
    perform(instance, execute)
}

fn dump(output: &Output) {
    log_std_out_and_err(output);
}

pub fn get_source_version(instance: &Path) -> Result<Version, TdError> {
    let version_file = instance.join(VERSION_FILE);
    if version_file.exists() {
        match fs::read_to_string(&version_file) {
            Ok(version) => {
                let version = version.trim();
                if version.is_empty() {
                    Err(EmptyVersionFile)?;
                }
                Version::parse(version)
                    .map_err(|_| InvalidVersionFormat(version.to_string()).into())
            }
            Err(err) => {
                eprintln!("Failed to read .version file: {}", err);
                Err(TdError::new(InvalidVersionFile(err)))
            }
        }
    } else {
        Version::parse(SEED_VERSION)
            .map_err(|_| InvalidVersionFormat(SEED_VERSION.to_string()).into())
    }
}

pub fn get_target_version() -> Result<Version, TdError> {
    Version::parse(TABSDATA_VERSION)
        .map_err(|_| InvalidVersionFormat(TABSDATA_VERSION.to_string()).into())
}
