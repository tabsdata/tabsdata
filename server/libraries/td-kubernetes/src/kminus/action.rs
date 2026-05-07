//
// Copyright 2026 Tabs Data Inc.
//

use std::process::{Command as SystemCommand, Stdio};

use td_common::status::ExitStatus::GeneralError;
use td_python::python::resolve_python_binary;
use tracing::error;

use crate::exit::{Outcome, abort};
use crate::kminus::model::KminusInstance;

const KM_BINARY: &str = "km";
const KM_CREATE_SUBCOMMAND: &str = "create";
const KM_WORKDIR_ARGUMENT: &str = "--workdir";

pub fn create_cluster(instance: &KminusInstance) -> Outcome {
    let km = match resolve_python_binary(KM_BINARY) {
        Ok(path) => path,
        Err(error) => {
            error!("Failed to locate the '{KM_BINARY}' binary: {error}");
            return abort(GeneralError);
        }
    };
    let mut command = SystemCommand::new(&km);
    command
        .arg(KM_CREATE_SUBCOMMAND)
        .arg(KM_WORKDIR_ARGUMENT)
        .arg(instance.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = match command.status() {
        Ok(status) => status,
        Err(error) => {
            error!(
                "Failed to run '{} {KM_CREATE_SUBCOMMAND}': {error}",
                km.display()
            );
            return abort(GeneralError);
        }
    };
    if !status.success() {
        error!(
            "'{} {KM_CREATE_SUBCOMMAND}' failed with status {status}.",
            km.display()
        );
        return abort(GeneralError);
    }
    Ok(())
}
