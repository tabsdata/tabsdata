//
// Copyright 2025 Tabs Data Inc.
//

use crate::env::{prepend_in_path, remove_from_path};
use crate::error::PythonError::{
    InstanceExtractionError, InterpreterResolutionError, InterpreterResolutionPanic,
    InterpreterResolutionParseError, OutputEncodingError, VenvCreationError, VenvCreationPanic,
    VenvCreationParseError,
};
use crate::io::log_std_out_and_err;
use std::env;
use std::path::{Path, PathBuf};
use std::process::{exit, Command, Output};
use td_common::os::name_program;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{debug, error};

pub const PYTHON_PROGRAM: &str = "python";
pub const PYTHON_ARGUMENT_C: &str = "-c";
pub const PYTHON_INTERPRETER_SCRIPT: &str =
    "import sys; print(f\"<interpreter>{sys.executable}</interpreter>\")";

pub const TDVENV_PROGRAM: &str = "_tdvenv";
pub const TDVENV_ARGUMENT_INSTANCE: &str = "--instance";

pub const BIN: &str = "bin";

pub const ENV_CONDA_PREFIX: &str = "CONDA_PREFIX";
pub const ENV_PYENV_VERSION: &str = "PYENV_VERSION";
pub const ENV_PYTHONHOME: &str = "PYTHONHOME";
pub const ENV_PYTHONPATH: &str = "PYTHONPATH";
pub const ENV_UV_VENV: &str = "UV_VENV";
pub const ENV_VIRTUAL_ENV: &str = "VIRTUAL_ENV";
pub const ENV_VIRTUAL_ENV_PROMPT: &str = "VIRTUAL_ENV_PROMPT";

const ENVIRONMENT_START: &str = "<environment>";
const ENVIRONMENT_END: &str = "</environment>";
const INTERPRETER_START: &str = "<interpreter>";
const INTERPRETER_END: &str = "</interpreter>";

pub fn set(instance: &PathBuf) -> Result<PathBuf, TdError> {
    let environment = create(instance)?;
    activate(&environment)?;
    Ok(environment)
}

pub fn get() -> Result<PathBuf, TdError> {
    let python = name_program(&PathBuf::from(PYTHON_PROGRAM));
    let output = Command::new(python)
        .arg(PYTHON_ARGUMENT_C)
        .arg(PYTHON_INTERPRETER_SCRIPT)
        .output()
        .map_err(InterpreterResolutionPanic)?;
    dump(&output);
    if !output.status.success() {
        error!("Bad exit code checking python virtual environment");
        return Err(TdError::new(InterpreterResolutionError(output.status)));
    }
    match String::from_utf8(output.stdout) {
        Ok(output) => {
            let interpreter = extract(&output, INTERPRETER_START, INTERPRETER_END)
                .ok_or(InterpreterResolutionParseError(output))?;
            Ok(PathBuf::from(interpreter))
        }
        Err(e) => Err(OutputEncodingError(e))?,
    }
}

pub fn create(instance: &PathBuf) -> Result<PathBuf, TdError> {
    let tdvenv = name_program(&PathBuf::from(TDVENV_PROGRAM));
    let output = Command::new(tdvenv)
        .arg(TDVENV_ARGUMENT_INSTANCE)
        .arg(instance)
        .output()
        .map_err(VenvCreationPanic)?;
    dump(&output);
    if !output.status.success() {
        error!("Bad exit code creating python virtual environment");
        return Err(TdError::new(VenvCreationError(output.status)));
    }
    match String::from_utf8(output.stdout) {
        Ok(output) => {
            let environment = extract(&output, ENVIRONMENT_START, ENVIRONMENT_END)
                .ok_or(VenvCreationParseError(output))?;
            Ok(PathBuf::from(environment))
        }
        Err(e) => Err(OutputEncodingError(e))?,
    }
}

pub fn activate(venv: &PathBuf) -> Result<(), TdError> {
    let instance = venv
        .file_name()
        .ok_or_else(|| InstanceExtractionError(venv.clone()))?
        .to_string_lossy()
        .to_string();

    prepend_in_path(Path::new(venv.as_path()).join(BIN).to_str().unwrap(), None)?;
    env::remove_var(ENV_CONDA_PREFIX);
    env::remove_var(ENV_PYENV_VERSION);
    env::remove_var(ENV_PYTHONHOME);
    // ToDo...
    // env::remove_var(ENV_PYTHONPATH);
    // filter_pythonpath();
    env::remove_var(ENV_UV_VENV);
    env::set_var(ENV_VIRTUAL_ENV, venv);
    env::set_var(ENV_VIRTUAL_ENV_PROMPT, format!("({})", instance));
    Ok(())
}

pub fn deactivate(venv: &PathBuf) -> Result<(), TdError> {
    remove_from_path(Path::new(venv).join(BIN).to_str().unwrap(), None)?;
    env::remove_var(ENV_CONDA_PREFIX);
    env::remove_var(ENV_PYENV_VERSION);
    env::remove_var(ENV_PYTHONHOME);
    // ToDo...
    // env::remove_var(ENV_PYTHONPATH);
    // filter_pythonpath();
    env::remove_var(ENV_UV_VENV);
    env::remove_var(ENV_VIRTUAL_ENV);
    env::remove_var(ENV_VIRTUAL_ENV_PROMPT);
    Ok(())
}

// ToDo ...
/*
fn filter_pythonpath() {
    if let Ok(sys_path) = env::var(ENV_PYTHONPATH) {
        let path_sep = if cfg!(windows) { ';' } else { ':' };
        let filtered_pythonpath: Vec<String> = sys_path
            .split(path_sep)
            .filter(|p| Path::new(p).file_name().is_some_and(|f| f == "egg"))
            .map(String::from)
            .collect();
        if filtered_pythonpath.is_empty() {
            env::remove_var(ENV_PYTHONPATH);
        } else {
            env::set_var(
                ENV_PYTHONPATH,
                filtered_pythonpath.join(&path_sep.to_string()),
            );
        }
        info!(
            "Filtered {}: {}",
            ENV_PYTHONPATH,
            filtered_pythonpath.join(&path_sep.to_string())
        );
    }
}
*/

fn extract(string: &str, start: &str, end: &str) -> Option<String> {
    if let Some(start_i) = string.find(start) {
        if let Some(end_i) = string.find(end) {
            if start_i + start.len() < end_i {
                return Some(string[start_i + start.len()..end_i].trim().to_string());
            }
        }
    }
    None
}

pub fn prepare(instance: &PathBuf) {
    match set(instance) {
        Ok(environment) => {
            debug!(
                "Using Python base virtual environment: {}",
                environment.display()
            )
        }
        Err(e) => {
            error!("Failed to create the Python base environment: {}", e);
            exit(GeneralError.code());
        }
    }
    match get() {
        Ok(interpreter) => {
            debug!(
                "Using Python base virtual interpreter: {}",
                interpreter.display()
            )
        }
        Err(e) => {
            error!(
                "Failed to check the interpreter of the Python base environment: {}",
                e
            );
            exit(GeneralError.code());
        }
    }
}

fn dump(output: &Output) {
    log_std_out_and_err(output);
}
