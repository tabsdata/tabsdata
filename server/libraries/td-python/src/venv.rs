//
// Copyright 2025 Tabs Data Inc.
//

use crate::env::{ENV_PATH, prepend_in_path, remove_from_path};
use crate::error::PythonError::{
    EnvironmentVariablesPropagationError, InstanceExtractionError, InterpreterResolutionError,
    InterpreterResolutionPanic, InterpreterResolutionParseError, OutputEncodingError,
    VenvCreationError, VenvCreationPanic, VenvCreationParseError,
};
use crate::io::log_std_out_and_err;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, exit};
use td_common::env::check_flag_env;
use td_common::os::name_program;
use td_common::server::TD_DETACHED_SUBPROCESSES;
use td_common::status::ExitStatus::GeneralError;
use td_error::TdError;
use tracing::{debug, error};

pub const PYTHON_PROGRAM: &str = "python";
pub const PYTHON_ARGUMENT_C: &str = "-c";
pub const PYTHON_INTERPRETER_SCRIPT: &str =
    "import sys; print(f\"<interpreter>{sys.executable}</interpreter>\")";

pub const TDVENV_PROGRAM: &str = "tdvenv";
pub const TDVENV_ARGUMENT_INSTANCE: &str = "--instance";
pub const TDVENV_ARGUMENT_REQUIREMENTS: &str = "--requirements";

#[cfg(not(target_os = "windows"))]
pub const PYTHON_BIN_FOLDER: &str = "bin";

#[cfg(target_os = "windows")]
pub const PYTHON_BIN_FOLDER: &str = "Scripts";

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

pub fn set(
    instance: &PathBuf,
    requirements: Option<&PathBuf>,
    dump_std: bool,
) -> Result<PathBuf, TdError> {
    let environment = create(instance, requirements, dump_std)?;
    activate(&environment)?;
    Ok(environment)
}

pub fn get(
    instance: &PathBuf,
    requirements: Option<&PathBuf>,
    dump_std: bool,
) -> Result<(PathBuf, HashMap<String, Option<String>>), TdError> {
    let environment = create(instance, requirements, dump_std)?;
    let variables = match propagate(&environment) {
        Ok(variables) => variables,
        Err(err) => return Err(TdError::from(EnvironmentVariablesPropagationError(err))),
    };
    Ok((environment, variables))
}

//noinspection DuplicatedCode
pub fn check(dump_std: bool) -> Result<PathBuf, TdError> {
    let python = name_program(&PathBuf::from(PYTHON_PROGRAM));
    let mut command = Command::new(python);
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            command.creation_flags(CREATE_NO_WINDOW);
        }
    }
    let output = command
        .arg(PYTHON_ARGUMENT_C)
        .arg(PYTHON_INTERPRETER_SCRIPT)
        .output()
        .map_err(InterpreterResolutionPanic)?;
    let mut dumped = false;
    if dump_std {
        dump(&output);
        dumped = true;
    }
    if !output.status.success() {
        if !dumped {
            dump(&output);
        }
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

//noinspection DuplicatedCode
pub fn create(
    instance: &PathBuf,
    requirements: Option<&PathBuf>,
    dump_std: bool,
) -> Result<PathBuf, TdError> {
    let tdvenv = name_program(&PathBuf::from(TDVENV_PROGRAM));
    let mut command = Command::new(tdvenv);
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            command.creation_flags(CREATE_NO_WINDOW);
        }
    }
    command.arg(TDVENV_ARGUMENT_INSTANCE).arg(instance);
    if let Some(requirements) = requirements {
        command.arg(TDVENV_ARGUMENT_REQUIREMENTS).arg(requirements);
    }
    let output = command.output().map_err(VenvCreationPanic)?;
    let mut dumped = false;
    if dump_std {
        dump(&output);
        dumped = true;
    }
    if !output.status.success() {
        if !dumped {
            dump(&output);
        }
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

pub fn propagate(venv: &Path) -> Result<HashMap<String, Option<String>>, TdError> {
    let instance = venv
        .file_name()
        .ok_or_else(|| InstanceExtractionError(venv.to_path_buf()))?
        .to_string_lossy()
        .to_string();

    let path = prepend_in_path(
        Path::new(venv).join(PYTHON_BIN_FOLDER).to_str().unwrap(),
        None,
    )?;
    Ok(HashMap::from([
        (
            ENV_PATH.to_string(),
            Some(path.to_string_lossy().to_string()),
        ),
        (ENV_CONDA_PREFIX.to_string(), None),
        (ENV_PYENV_VERSION.to_string(), None),
        (ENV_PYTHONHOME.to_string(), None),
        (ENV_PYTHONPATH.to_string(), None),
        (ENV_UV_VENV.to_string(), None),
        (
            ENV_VIRTUAL_ENV.to_string(),
            Some(venv.to_string_lossy().to_string()),
        ),
        (
            ENV_VIRTUAL_ENV_PROMPT.to_string(),
            Some(format!("({instance})")),
        ),
    ]))
}

pub fn activate(venv: &Path) -> Result<(), TdError> {
    let envs = propagate(venv)?;
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        for (key, value) in envs {
            match value {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
            }
        }
    }
    Ok(())
}

pub fn supress(venv: &PathBuf) -> Result<HashMap<String, Option<String>>, TdError> {
    let path = remove_from_path(
        Path::new(venv).join(PYTHON_BIN_FOLDER).to_str().unwrap(),
        None,
    )?;
    Ok(HashMap::from([
        (
            ENV_PATH.to_string(),
            Some(path.to_string_lossy().to_string()),
        ),
        (ENV_CONDA_PREFIX.to_string(), None),
        (ENV_PYENV_VERSION.to_string(), None),
        (ENV_PYTHONHOME.to_string(), None),
        (ENV_PYTHONPATH.to_string(), None),
        (ENV_UV_VENV.to_string(), None),
        (ENV_VIRTUAL_ENV.to_string(), None),
        (ENV_VIRTUAL_ENV_PROMPT.to_string(), None),
    ]))
}

pub fn deactivate(venv: &PathBuf) -> Result<(), TdError> {
    let envs = supress(venv)?;
    // Setting env vars is not thread-safe; use with care.
    unsafe {
        for (key, value) in envs {
            match value {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
            }
        }
    }
    Ok(())
}

fn extract(string: &str, start: &str, end: &str) -> Option<String> {
    if let Some(start_i) = string.find(start)
        && let Some(end_i) = string.find(end)
        && start_i + start.len() < end_i
    {
        return Some(string[start_i + start.len()..end_i].trim().to_string());
    }
    None
}

pub fn prepare(instance: &PathBuf, dump_std: bool) {
    match set(instance, None, dump_std) {
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
    match check(dump_std) {
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
