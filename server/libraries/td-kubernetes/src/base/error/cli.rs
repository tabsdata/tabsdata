//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum CliError {
    #[error("Argument '--{argument}' is required: {reason}")]
    ArgumentRequired { argument: String, reason: String } = 0,
    #[error("Invalid command line")]
    InvalidCommandLine = 1,
    #[error("Invalid shell command")]
    InvalidShellCommand = 2,
    #[error("Shell line editor failed: {0}")]
    ShellLineEditorFailed(String) = 5000,
    #[error("Shell command '{command}' failed: {cause}")]
    ShellCommandFailed { command: String, cause: String } = 5001,
    #[error("Could not change directory: {0}")]
    ChangeDirectoryFailed(String) = 5002,
}

#[cfg(test)]
mod tests;
