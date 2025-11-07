//
// Copyright 2025 Tabs Data Inc.
//

use crate::error::PythonError::{
    OutputEncodingError, PythonBinaryNotFound, ScriptsFolderNotFound, ScriptsFolderResolutionError,
    ScriptsFolderResolutionFailure,
};
use std::path::PathBuf;
use std::process::Command;
use td_common::os::name_program;
use td_error::TdError;

const PYTHON: &str = "python";
const PYTHON_SCRIPTS_QUERY: &str = "import sysconfig; print(sysconfig.get_path('scripts'))";

pub fn get_python_scripts_folder() -> Result<PathBuf, TdError> {
    let python = name_program(&PathBuf::from(PYTHON));
    let python_output = Command::new(python)
        .arg("-c")
        .arg(PYTHON_SCRIPTS_QUERY)
        .output()
        .map_err(ScriptsFolderResolutionFailure)?;
    if !python_output.status.success() {
        return Err(TdError::new(ScriptsFolderResolutionError(
            python_output.status,
        )));
    }
    let python_stdout = String::from_utf8(python_output.stdout).map_err(OutputEncodingError)?;
    let scripts_folder = PathBuf::from(python_stdout.trim());
    if !scripts_folder.exists() {
        return Err(TdError::new(ScriptsFolderNotFound(scripts_folder)));
    }
    Ok(scripts_folder)
}

pub fn resolve_python_binary(program_name: &str) -> Result<PathBuf, TdError> {
    let scripts_folder = get_python_scripts_folder()?;
    let program = name_program(&PathBuf::from(program_name));
    let program_path = scripts_folder.join(&program);
    if program_path.exists() {
        Ok(program_path)
    } else {
        Err(TdError::new(PythonBinaryNotFound(program_path)))
    }
}
