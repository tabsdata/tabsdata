//
// Copyright 2024 Tabs Data Inc.
//

use crate::env::EnvironmentError::{
    MissingExeDir, MissingExeFile, UndeterminedExeDir, UndeterminedExeFile, UndeterminedExePath,
};
use std::env::{current_dir, current_exe, var_os};
use std::fs::canonicalize;
use std::path::PathBuf;
use std::{fs, io};
use thiserror::Error;

pub const TABSDATA_HOME_DIR: &str = ".tabsdata";

#[derive(Debug, Error)]
pub enum EnvironmentError {
    #[error("Path of the executing program cannot be determined: {cause}")]
    UndeterminedExePath {
        #[source]
        cause: io::Error,
    },
    #[error("Directory of the executing program cannot be determined: {cause}")]
    UndeterminedExeDir {
        #[source]
        cause: io::Error,
    },
    #[error("Directory of the executing program cannot be identified")]
    MissingExeDir,
    #[error("File of the executing program cannot be determined: {cause}")]
    UndeterminedExeFile {
        #[source]
        cause: io::Error,
    },
    #[error("File of the executing program cannot be identified")]
    MissingExeFile,
    #[error("An error occurred running file system operations: {0}")]
    IOError(#[from] io::Error),
}

/// Retrieves the path of the currently executing program.
pub fn get_current_exe_path() -> Result<PathBuf, EnvironmentError> {
    match current_exe() {
        Ok(path) => match path.is_absolute() {
            true => Ok(path),
            false => match canonicalize(&path) {
                Ok(canonical) => Ok(canonical),
                Err(e) => Err(UndeterminedExePath { cause: e }),
            },
        },
        Err(e) => Err(UndeterminedExePath { cause: e }),
    }
}

/// Retrieves the directory of the currently executing program.
pub fn get_current_exe_dir() -> Result<PathBuf, EnvironmentError> {
    match current_exe() {
        Ok(path) => match path.parent() {
            Some(dir) => match dir.is_absolute() {
                true => Ok(dir.to_path_buf()),
                false => match canonicalize(dir) {
                    Ok(canonical) => Ok(canonical),
                    Err(e) => Err(UndeterminedExeDir { cause: e }),
                },
            },
            None => Err(MissingExeDir),
        },
        Err(e) => Err(UndeterminedExeDir { cause: e }),
    }
}

/// Retrieves the file of the currently executing program.
pub fn get_current_exe_name() -> Result<String, EnvironmentError> {
    match current_exe() {
        Ok(path) => match path.file_name() {
            Some(file) => Ok(file.to_string_lossy().to_string()),
            None => Err(MissingExeFile),
        },
        Err(e) => Err(UndeterminedExeFile { cause: e }),
    }
}

/// Retrieves the name (no extension) of the currently executing program.
pub fn get_current_exe_stem() -> Result<String, EnvironmentError> {
    match current_exe() {
        Ok(path) => path
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_string())
            .ok_or(MissingExeFile),
        Err(e) => Err(UndeterminedExeFile { cause: e }),
    }
}

/// Converts a path from relative to absolute, without making it canonical.
pub fn to_absolute(path: &PathBuf) -> Result<PathBuf, EnvironmentError> {
    Ok(match path.is_absolute() {
        true => path.clone(),
        false => current_dir()?.join(path),
    })
}

/// Retrieves the current working directory.
///
/// # Panics
/// Panics if the current working directory cannot be retrieved.
///
/// # Test Configuration
/// In test mode, it creates and returns a temporary test directory for isolation.
#[cfg(not(any(test, feature = "mock-env")))]
pub fn get_current_dir() -> PathBuf {
    current_dir().expect("Failed to get current directory")
}

#[cfg(any(test, feature = "mock-env"))]
pub fn get_current_dir() -> PathBuf {
    let dir = testdir::testdir!().join("current_dir");
    fs::create_dir_all(&dir).expect("Failed to create config test current dir");
    dir
}

/// Retrieves the home directory of the current user.
///
/// # Panics
/// Panics if the home directory cannot be retrieved.
///
/// # Test Configuration
/// In test mode, it creates and returns a temporary test home directory for isolation.
#[cfg(not(any(test, feature = "mock-env")))]
pub fn get_home_dir() -> PathBuf {
    homedir::my_home()
        .expect("Failed to get user home dir")
        .expect("Failed to get user home dir")
}

#[cfg(any(test, feature = "mock-env"))]
pub fn get_home_dir() -> PathBuf {
    let dir = testdir::testdir!().join("home");
    fs::create_dir_all(&dir).expect("Failed to create config test home dir");
    dir
}

/// Retrieves the username of the current user, replacing spaces with hyphens.
///
/// # Test Configuration
/// In test mode, it returns a fixed test username.
#[cfg(not(any(test, feature = "mock-env")))]
pub fn get_user_name() -> String {
    whoami::username()
}

#[cfg(any(test, feature = "mock-env"))]
pub fn get_user_name() -> String {
    String::from("test user")
}

/// Retrieves the .tabsdata directory path inside the user's home directory.
///
/// This function combines the home directory path with the `.tabsdata` subdirectory.
pub fn get_tabsdata_home_dir() -> PathBuf {
    get_home_dir().join(TABSDATA_HOME_DIR)
}

/// Creates the .tabsdata directory inside the user's home directory if it doesn't exist.
///
/// This function ensures the existence of the `.tabsdata` directory, creating it if necessary.
///
/// # Panics
/// Panics if the directory cannot be created.
pub fn create_tabsdata_home_dir() -> PathBuf {
    let path = get_home_dir().join(TABSDATA_HOME_DIR);
    if !&path.exists() {
        fs::create_dir(&path).expect("Failed to create tabsdata home dir");
    }
    path
}

/// Checks for an environment variable acting as a flag.
#[allow(dead_code)]
pub fn check_flag_env(env: &str) -> bool {
    const TRUE: &str = "true";
    const YES: &str = "yes";
    const ONE: &str = "1";

    var_os(env).is_some_and(|val| {
        val.to_str()
            .map(|s| {
                let lower = s.to_lowercase();
                lower == TRUE || lower == YES || lower == ONE
            })
            .unwrap_or(false)
    })
}
