//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use std::process::Command;

use td_common::os::name_program;
use td_error::TdError;

use crate::error::PythonError::{
    OutputEncodingError, TabsdataVersionResolutionError, TabsdataVersionResolutionFailure,
};
use crate::venv::{PYTHON_ARGUMENT_C, PYTHON_PROGRAM};

const TABSDATA_VERSION_QUERY: &str =
    "from importlib.metadata import version; print(version('tabsdata'))";

pub fn tabsdata_version() -> Result<String, TdError> {
    let python = name_program(&PathBuf::from(PYTHON_PROGRAM));
    let output = Command::new(python)
        .arg(PYTHON_ARGUMENT_C)
        .arg(TABSDATA_VERSION_QUERY)
        .output()
        .map_err(TabsdataVersionResolutionFailure)?;
    if !output.status.success() {
        return Err(TdError::new(TabsdataVersionResolutionError(output.status)));
    }
    let stdout = String::from_utf8(output.stdout).map_err(OutputEncodingError)?;
    Ok(stdout.trim().to_owned())
}
