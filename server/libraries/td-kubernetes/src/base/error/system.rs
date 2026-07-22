//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum SystemError {
    #[error("Could not spawn subprocess '{0}': {1}.")]
    SubprocessSpawnFailed(String, String) = 5000,
    #[error("Subprocess '{program}' exited with status {status}: {stderr}.")]
    SubprocessFailed {
        program: String,
        status: String,
        stderr: String,
    } = 5001,
    #[error("Output of '{0}' is not valid UTF-8.")]
    SubprocessOutputNonUtf8(String) = 5002,
    #[error("Could not write to '{0}': {1}.")]
    WriteFailed(String, String) = 5003,
    #[error("Could not read '{0}': {1}.")]
    ReadFailed(String, String) = 5004,
    #[error("Could not delete '{0}': {1}.")]
    DeleteFailed(String, String) = 5005,
    #[error("Could not create directory '{0}': {1}.")]
    CreateDirectoryFailed(String, String) = 5006,
}

#[cfg(test)]
mod tests;
