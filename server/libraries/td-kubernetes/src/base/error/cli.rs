//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum CliError {
    #[error("Argument '--{argument}' is required: {reason}.")]
    ArgumentRequired { argument: String, reason: String } = 0,
    #[error("Invalid command line.")]
    InvalidCommandLine = 1,
    #[error("Invalid shell command.")]
    InvalidShellCommand = 2,
    #[error("Folder '{0}' does not exist.")]
    FolderNotFound(String) = 1000,
    #[error("Folder '{0}' is not empty.")]
    FolderNotEmpty(String) = 1001,
    #[error("Folder '{0}' is empty.")]
    FolderEmpty(String) = 1002,
    #[error("Shell line editor failed: {0}.")]
    ShellLineEditorFailed(String) = 5000,
    #[error("Shell command '{command}' failed: {cause}.")]
    ShellCommandFailed { command: String, cause: String } = 5001,
    #[error("Could not change directory: {0}.")]
    ChangeDirectoryFailed(String) = 5002,
    #[error("Could not read folder '{folder}': {cause}.")]
    FolderReadFailed { folder: String, cause: String } = 5003,
    #[error("Could not write file '{file}': {cause}.")]
    FileWriteFailed { file: String, cause: String } = 5004,
    #[error("Could not create folder '{folder}': {cause}.")]
    FolderCreateFailed { folder: String, cause: String } = 5005,
}

#[cfg(test)]
mod tests;
