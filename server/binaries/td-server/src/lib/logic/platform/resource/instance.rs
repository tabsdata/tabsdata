//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::platform::resource::instance::InstanceError::{
    NonExecutableProgramPath, NonExistingProgramPath, NonFileProgramPath,
    OverlappingSourceAndTargetFolders, SourceCopyFolderDoesNotExist, UnresolvedProgramFolder,
    UnspecifiedInstance, UnspecifiedProgram,
};
use crate::logic::platform::resource::settings::extract_profile;
use std::fs::{canonicalize, copy, create_dir_all};
use std::io;
use std::path::{PathBuf, StripPrefixError};
use std::string::ToString;
use td_common::env::{get_current_exe_dir, EnvironmentError, TABSDATA_HOME_DIR};
use td_common::files::ROOT;
use td_common::logging::LOG_LOCATION;
use td_common::os::{is_executable, name_program};
use thiserror::Error;
use tracing::{error, info};
use walkdir::WalkDir;

pub const DEFAULT_INSTANCE: &str = "tabsdata";

pub const CURRENT_FOLDER: &str = ".";
pub const PARENT_FOLDER: &str = "..";

pub const INSTANCES_FOLDER: &str = "instances";

pub const WORKSPACE_FOLDER: &str = "workspace";
pub const REPOSITORY_FOLDER: &str = "repository";

pub const MOLD_FOLDER: &str = "mold";
pub const LOG_FOLDER: &str = LOG_LOCATION;
pub const LOCK_FOLDER: &str = "lock";
pub const MSG_FOLDER: &str = td_common::server::MSG_FOLDER;
pub const PROC_FOLDER: &str = "proc";
pub const CAST_FOLDER: &str = "cast";
pub const BIN_FOLDER: &str = "bin";
pub const REQUEST_FOLDER: &str = "request";
pub const RESPONSE_FOLDER: &str = "response";
pub const INPUT_FOLDER: &str = "input";
pub const OUTPUT_FOLDER: &str = "output";

pub const CONFIG_NAMESPACE: &str = "td";

pub const INSTANCE_FOLDER: &str = "instance";

pub const CONFIG_FOLDER: &str = "config";
pub const WORK_FOLDER: &str = "work";

pub const CONFIG_FILE_STEM: &str = "config";
pub const CONFIG_FILE: &str = "config.yaml";

pub const REQUEST_FILE: &str = "request.yaml";
pub const RESPONSE_FILE: &str = "response.yaml";
pub const EXCEPTION_FILE: &str = "exception.yaml";

pub const WORKER_PID_FILE: &str = "pid";
pub const WORKER_OUT_FILE: &str = "out.log";
pub const WORKER_ERR_FILE: &str = "err.log";

pub const MESSAGE_PATTERN: &str = "_*";
pub const LOG_PATTERN: &str = "*.log";

/// Get the instance path of a given instance.
pub fn get_instance_path_for_instance(instance: &Option<PathBuf>) -> PathBuf {
    let root = dirs::home_dir().unwrap_or_else(|| PathBuf::from(ROOT));
    match instance {
        None => root
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER)
            .join(DEFAULT_INSTANCE),
        Some(path) if path.is_relative() => root
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER)
            .join(path),
        Some(path) => path.clone(),
    }
}

/// Get the repository path of a given instance.
pub fn get_repository_path_for_instance(
    repository: &Option<PathBuf>,
    instance: &Option<PathBuf>,
) -> PathBuf {
    get_folder_path_for_instance(repository, REPOSITORY_FOLDER, instance)
}

/// Get the workspace path of a given instance.
pub fn get_workspace_path_for_instance(
    workspace: &Option<PathBuf>,
    instance: &Option<PathBuf>,
) -> PathBuf {
    get_folder_path_for_instance(workspace, WORKSPACE_FOLDER, instance)
}

/// Get a folder path of a given instance.
fn get_folder_path_for_instance(
    folder: &Option<PathBuf>,
    default: &str,
    instance: &Option<PathBuf>,
) -> PathBuf {
    match folder {
        None => get_instance_path_for_instance(instance).join(default),
        Some(path) if path.is_relative() => instance.clone().unwrap().join(path),
        Some(path) => path.clone(),
    }
}

/// Get the workspace path of a given program.
pub fn get_workspace_path_for_program(
    parent_workspace: &Option<PathBuf>,
    program: &str,
) -> PathBuf {
    parent_workspace
        .as_ref()
        .unwrap_or(&PathBuf::from(WORKSPACE_FOLDER))
        .join(program)
}

/// Get the program path of a given program.
pub fn get_program_path(program: &PathBuf) -> Result<PathBuf, InstanceError> {
    if program.as_os_str().is_empty() {
        return Err(UnspecifiedProgram);
    }
    let program = match program.is_absolute() {
        true => program,
        false => &{
            let mut path = match get_current_exe_dir() {
                Ok(folder) => folder,
                Err(e) => return Err(UnresolvedProgramFolder { cause: e }),
            };
            path.push(program.as_path());
            path
        },
    };
    let program = name_program(program);
    if !program.exists() {
        Err(NonExistingProgramPath {
            path: program.clone(),
        })
    } else if !program.is_file() {
        Err(NonFileProgramPath {
            path: program.clone(),
        })
    } else if !is_executable(&program) {
        Err(NonExecutableProgramPath {
            path: program.clone(),
        })
    } else {
        Ok(program)
    }
}

#[cfg(not(test))]
pub fn config_folder() -> String {
    CONFIG_FOLDER.to_string()
}

#[cfg(test)]
#[cfg(not(target_os = "windows"))]
pub const RSC_FOLDER: &str = "./resources/profiles/base/workspace/config/bootloader";
#[cfg(target_os = "windows")]
pub const RSC_FOLDER: &str = ".\\resources\\profiles\\base\\workspace\\config\\bootloader";

#[cfg(test)]
pub fn config_folder() -> String {
    PathBuf::from(RSC_FOLDER)
        .join(CONFIG_FOLDER)
        .as_os_str()
        .to_string_lossy()
        .to_string()
}

/// Create an instance using base and custom profile. Any file existing in the instance is
/// preserved and never overwritten. If some file exists in both base and custom profile, the file
/// in custom profile takes precedence.
/// Note: The custom profile can be, therefore, a partial replica of the base profile, holding only
/// files that require custom contents. This way, avoiding duplicating unnecessarily unmodified
/// files allows a smarter customization governance.
/// Omitting profiles is supported, and then it is assumed the instance is already set up.
pub fn create_instance_tree(
    profile: Option<PathBuf>,
    instance: Option<PathBuf>,
) -> Result<(), InstanceError> {
    info!("Creating instance tree: {:?} - {:?}", profile, instance);
    if instance.is_none() {
        return Err(UnspecifiedInstance);
    }
    let instance = instance.unwrap();
    if !instance.exists() {
        create_dir_all(&instance)?;
    };
    if let Some(custom_profile) = profile {
        copy_profile_tree(&custom_profile, &instance)?;
    }
    extract_profile(&instance, false)?;
    Ok(())
}

/// Copies a profile folder to a target workspace. Only non-existing files are copied, giving
/// precedence to already existing custom files in target workspace.
fn copy_profile_tree(source: &PathBuf, target: &PathBuf) -> Result<(), InstanceError> {
    if !source.exists() {
        return Err(SourceCopyFolderDoesNotExist {
            folder: source.clone(),
        });
    }
    if !target.exists() {
        create_dir_all(target)?;
    };
    if !are_paths_independent(source, target)? {
        return Err(OverlappingSourceAndTargetFolders {
            source_folder: source.clone(),
            target_folder: target.clone(),
        });
    };
    for entry in WalkDir::new(source) {
        let entry = entry?;
        let source_path = entry.path();
        let canonical_source_path = canonicalize(source_path)?;
        let source_file = source_path.strip_prefix(source)?;
        let target_path = target.join(source_file);
        match canonical_source_path {
            f if f.is_dir() => {
                if !target_path.exists() {
                    create_dir_all(&target_path)?;
                    info!(
                        "Source folder copied: '{}' '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                } else {
                    info!(
                        "Target folder kept: '{}' - '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                }
            }
            f if f.is_file() => {
                if !target_path.exists() {
                    copy(source_path, &target_path)?;
                    info!(
                        "Source file copied: '{}' '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                } else {
                    info!(
                        "Target file kept: '{}' - '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                }
            }
            // Canonical files are ensured to point only to a regular file or a regular folder.
            _ => {
                error!(
                    "Entry is not a regular folder or file. Discarding it: {:?}",
                    entry
                );
            }
        }
    }
    Ok(())
}

/// Copies a mold folder to a target workspace.
pub fn copy_mold_tree(source: &PathBuf, target: &PathBuf) -> Result<(), InstanceError> {
    if !source.exists() {
        return Err(SourceCopyFolderDoesNotExist {
            folder: source.clone(),
        });
    }
    if !target.exists() {
        create_dir_all(target)?;
    };
    if !are_paths_independent(source, target)? {
        return Err(OverlappingSourceAndTargetFolders {
            source_folder: source.clone(),
            target_folder: target.clone(),
        });
    };
    for entry in WalkDir::new(source) {
        let entry = entry?;
        let source_path = entry.path();
        let canonical_source_path = canonicalize(source_path)?;
        let source_file = source_path.strip_prefix(source)?;
        let target_path = target.join(source_file);
        match canonical_source_path {
            f if f.is_dir() => {
                if !target_path.exists() {
                    create_dir_all(&target_path)?;
                    info!(
                        "Source folder copied: '{}' '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                } else {
                    info!(
                        "Target folder kept: '{}' - '{}'",
                        source_file.display(),
                        target_path.display()
                    );
                }
            }
            f if f.is_file() => {
                copy(source_path, &target_path)?;
                info!(
                    "Source file copied: '{}' '{}'",
                    source_file.display(),
                    target_path.display()
                );
            }
            // Canonical files are ensured to point only to a regular file or a regular folder.
            _ => {
                error!(
                    "Entry is not a regular folder or file. Discarding it: {:?}",
                    entry
                );
            }
        }
    }
    Ok(())
}

/// checks if two folders overlap.
pub fn are_paths_independent(path1: &PathBuf, path2: &PathBuf) -> Result<bool, InstanceError> {
    let canonical_path1 = canonicalize(path1)?;
    let canonical_path2 = canonicalize(path2)?;
    Ok(!canonical_path1.starts_with(&canonical_path2)
        && !canonical_path2.starts_with(&canonical_path1))
}

#[derive(Debug, Error)]
pub enum InstanceError {
    #[error("Instance has not been specified")]
    UnspecifiedInstance,
    #[error("Program path not specified")]
    UnspecifiedProgram,
    #[error("Error resolving program folder: {cause}")]
    UnresolvedProgramFolder {
        #[source]
        cause: EnvironmentError,
    },
    #[error("Program path does no exist: {path}")]
    NonExistingProgramPath { path: PathBuf },
    #[error("Program path is not a regular file: {path}")]
    NonFileProgramPath { path: PathBuf },
    #[error("Program path is not executable: {path}")]
    NonExecutableProgramPath { path: PathBuf },
    #[error("Error getting current path: {cause}")]
    CurrentPathError { cause: io::Error },
    #[error("Error converting source configuration folder to absolute path: '{folder}' - {cause}")]
    SourceAbsolutePathError {
        folder: PathBuf,
        #[source]
        cause: io::Error,
    },
    #[error("Source folder to copy does not exist: {folder}")]
    SourceCopyFolderDoesNotExist { folder: PathBuf },
    #[error("Overlapping folders to copy: '{source_folder}' - '{target_folder}'")]
    OverlappingSourceAndTargetFolders {
        source_folder: PathBuf,
        target_folder: PathBuf,
    },
    #[error("An error occurred running file system operations: {0}")]
    IOError(#[from] io::Error),
    #[error("An error occurred traversing a directory: {0}")]
    WalkdirError(#[from] walkdir::Error),
    #[error("An error occurred traversing a directory: {0}")]
    StripError(#[from] StripPrefixError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::create_dir;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_get_workspace_path_for_instance_with_workspace() {
        #[cfg(not(target_os = "windows"))]
        let workspace = Some(PathBuf::from("/path/to/workspace"));
        #[cfg(target_os = "windows")]
        let workspace = Some(PathBuf::from("c:\\path\\to\\workspace"));

        #[cfg(not(target_os = "windows"))]
        let instance = Some(PathBuf::from("/path/to/instance"));
        #[cfg(target_os = "windows")]
        let instance = Some(PathBuf::from("c:\\path\\to\\instance"));

        let result = get_workspace_path_for_instance(&workspace, &instance);

        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, PathBuf::from("/path/to/workspace"));
        #[cfg(target_os = "windows")]
        assert_eq!(result, PathBuf::from("c:\\path\\to\\workspace"));
    }

    #[test]
    fn test_get_workspace_path_for_instance_with_instance() {
        let workspace = None;

        #[cfg(not(target_os = "windows"))]
        let instance = Some(PathBuf::from("/path/to/instance"));
        #[cfg(target_os = "windows")]
        let instance = Some(PathBuf::from("c:\\path\\to\\instance"));

        let result = get_workspace_path_for_instance(&workspace, &instance);

        #[cfg(not(target_os = "windows"))]
        assert_eq!(result, PathBuf::from("/path/to/instance/workspace"));
        #[cfg(target_os = "windows")]
        assert_eq!(result, PathBuf::from("c:\\path\\to\\instance\\workspace"));
    }

    #[test]
    fn test_get_workspace_path_for_instance_with_nothing() {
        let workspace = None;
        let instance = None;
        let result = get_workspace_path_for_instance(&workspace, &instance);
        assert_eq!(
            result,
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from(ROOT))
                .join(".tabsdata")
                .join("instances")
                .join("tabsdata")
                .join("workspace")
        );
    }

    #[test]
    fn get_program_workspace_custom() {
        #[cfg(not(target_os = "windows"))]
        let parent_workspace = Some(PathBuf::from("/custom/workspace"));
        #[cfg(target_os = "windows")]
        let parent_workspace = Some(PathBuf::from("\\custom\\workspace"));
        let program = "my_program";
        #[cfg(not(target_os = "windows"))]
        let expected = PathBuf::from("/custom/workspace/my_program");
        #[cfg(target_os = "windows")]
        let expected = PathBuf::from("\\custom/workspace\\my_program");
        let result = get_workspace_path_for_program(&parent_workspace, program);
        assert_eq!(result, expected);
    }

    #[test]
    fn get_program_workspace_default() {
        let parent_workspace = None;
        let program = "my_program";
        #[cfg(not(target_os = "windows"))]
        let expected = PathBuf::from("workspace/my_program");
        #[cfg(target_os = "windows")]
        let expected = PathBuf::from("workspace\\my_program");
        let result = get_workspace_path_for_program(&parent_workspace, program);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_paths_are_independent() {
        let temp_dir1 = tempdir().unwrap();
        let temp_dir2 = tempdir().unwrap();
        let path1 = temp_dir1.path().to_path_buf();
        let path2 = temp_dir2.path().to_path_buf();
        let result = are_paths_independent(&path1, &path2).unwrap();
        assert!(result, "Expected paths to be independent");
    }

    #[test]
    fn test_one_path_is_subfolder_of_the_other() {
        let temp_dir = tempdir().unwrap();
        let subfolder_path = temp_dir.path().join("subfolder");
        create_dir(&subfolder_path).unwrap();
        let path1 = temp_dir.path().to_path_buf();
        let path2 = subfolder_path.to_path_buf();
        let result = are_paths_independent(&path1, &path2).unwrap();
        assert!(!result, "Expected one path to be a subfolder of the other");
    }

    #[test]
    fn test_same_paths() {
        let temp_dir = tempdir().unwrap();
        let path1 = temp_dir.path().to_path_buf();
        let path2 = temp_dir.path().to_path_buf();
        let result = are_paths_independent(&path1, &path2).unwrap();
        assert!(
            !result,
            "Expected paths not to be independent (they are the same)"
        );
    }

    #[test]
    fn test_paths_with_same_prefix_but_different() {
        let temp_dir = tempdir().unwrap();
        let path1 = temp_dir.path().join("folder1");
        let path2 = temp_dir.path().join("folder2");
        create_dir(&path1).unwrap();
        create_dir(&path2).unwrap();
        let result = are_paths_independent(&path1, &path2).unwrap();
        assert!(
            result,
            "Expected paths to be independent even if they have the same prefix"
        );
    }
}
