//
// Copyright 2026 Tabsdata Inc.
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
    #[error(
        "Environment variable '{variable}' has unsupported value '{value}'; \
         supported values are: {supported}. Leave it unset to resolve packages from the \
         uv index environment variables, or from the uv defaults if those are unset too."
    )]
    UvModeUnsupported {
        variable: String,
        value: String,
        supported: String,
    } = 3,
    #[error(
        "Argument '--uv-index' applies only to a 'kminus' instance with '{variable}' unset, \
         because only then are the packages resolved from this machine's environment. \
         This instance uses provider '{provider}' and '{variable}' is '{mode}'; set the \
         index in the 'pods.global.envs' section of its configuration instead."
    )]
    UvIndexNotGoverned {
        variable: String,
        provider: String,
        mode: String,
    } = 15,
    #[error(
        "Argument '--{argument}' is no longer supported by 'tdkserver quickstart'. To declare \
         additional packages for any of the tabsdata base virtual environments, either run \
         'tdkserver venv update', which works both with 'tdkserver quickstart' and on an \
         already created instance, or edit the requirements-*.txt init assets after \
         'tdkserver init' and before 'tdkserver create'."
    )]
    ArgumentNoLongerSupported { argument: String } = 4,
    #[error(
        "Argument '--fill' opens the settings file in an editor, which needs a terminal. When \
         running unattended, fill a stub from 'tdkserver stubs' and pass it as '--config @FILE'."
    )]
    FillNeedsTerminal = 16,
    #[error("File '{file}' is not a valid pip requirements file; line {line}: {cause}.")]
    RequirementsInvalid {
        file: String,
        line: usize,
        cause: String,
    } = 5,
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
    #[error("Could not read file '{file}': {cause}.")]
    FileReadFailed { file: String, cause: String } = 5006,
    #[error("File '{file}' is not valid UTF-8: {cause}.")]
    FileNotUtf8 { file: String, cause: String } = 5007,
    #[error("Could not parse file '{file}': {cause}.")]
    FileParseFailed { file: String, cause: String } = 5008,
    #[error("Could not serialize file '{file}': {cause}.")]
    FileSerializeFailed { file: String, cause: String } = 5009,
    #[error("File '{file}' has no '{entry}' entry to set.")]
    FileEntryMissing { file: String, entry: String } = 5010,
    #[error("Could not remove folder '{folder}': {cause}.")]
    FolderRemoveFailed { folder: String, cause: String } = 5011,
    #[error("Could not run the settings editor on file '{file}': {cause}.")]
    EditorFailed { file: String, cause: String } = 5012,
}

#[cfg(test)]
mod tests;
