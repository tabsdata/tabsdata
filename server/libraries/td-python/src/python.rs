//
// Copyright 2025 Tabs Data Inc.
//

use crate::error::PythonError::{
    OutputEncodingError, PyenvPythonNotFound, PyenvPythonResolutionError,
    PyenvPythonResolutionFailure, PyenvPythonScriptsNotFound, PyenvPythonScriptsResolutionError,
    PyenvPythonScriptsResolutionFailure, PythonBinaryNotFound, ScriptsFolderNotFound,
    ScriptsFolderResolutionError, ScriptsFolderResolutionFailure,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use td_common::os::name_program;
use td_error::TdError;
use tracing::debug;

const PYTHON: &str = "python";
const PYTHON_SCRIPTS_QUERY: &str = "import sysconfig; print(sysconfig.get_path('scripts'))";

const LIBEXEC_DIR: &str = "libexec";
const TABSDATA_DIR: &str = "tabsdata";

const PYVENV_COMMAND_PYTHON: &str = "python";
const PYVENV_COMMAND_SCRIPTS: &str = "scripts";
const PYVENV_ARGUMENT_TARGET: &str = "--target";

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
        debug!(
            program = %program_name,
            path = %program_path.display(),
            "python.resolve: {} -> {}",
            program_name,
            program_path.display()
        );
        return Ok(program_path);
    }
    if let Some(prefix) = scripts_folder.parent() {
        let libexec_path = prefix.join(LIBEXEC_DIR).join(TABSDATA_DIR).join(&program);
        if libexec_path.exists() {
            debug!(
                program = %program_name,
                path = %libexec_path.display(),
                "python.resolve: {} -> {}",
                program_name,
                libexec_path.display()
            );
            return Ok(libexec_path);
        }
    }
    Err(TdError::new(PythonBinaryNotFound(program_path)))
}

pub fn resolve_pyenv_python(pyvenv: &Path, target: &Path) -> Result<PathBuf, TdError> {
    let output = Command::new(pyvenv)
        .arg(PYVENV_COMMAND_PYTHON)
        .arg(PYVENV_ARGUMENT_TARGET)
        .arg(target)
        .output()
        .map_err(PyenvPythonResolutionFailure)?;
    if !output.status.success() {
        return Err(TdError::new(PyenvPythonResolutionError(output.status)));
    }
    let stdout = String::from_utf8(output.stdout).map_err(OutputEncodingError)?;
    let python_path = PathBuf::from(stdout.trim());
    if !python_path.exists() {
        return Err(TdError::new(PyenvPythonNotFound(python_path)));
    }
    Ok(python_path)
}

pub fn resolve_pyenv_python_scripts(pyvenv: &Path, target: &Path) -> Result<PathBuf, TdError> {
    let output = Command::new(pyvenv)
        .arg(PYVENV_COMMAND_SCRIPTS)
        .arg(PYVENV_ARGUMENT_TARGET)
        .arg(target)
        .output()
        .map_err(PyenvPythonScriptsResolutionFailure)?;
    if !output.status.success() {
        return Err(TdError::new(PyenvPythonScriptsResolutionError(
            output.status,
        )));
    }
    let stdout = String::from_utf8(output.stdout).map_err(OutputEncodingError)?;
    let scripts_path = PathBuf::from(stdout.trim());
    if !scripts_path.exists() {
        return Err(TdError::new(PyenvPythonScriptsNotFound(scripts_path)));
    }
    Ok(scripts_path)
}
