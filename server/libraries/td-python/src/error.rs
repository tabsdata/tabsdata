//
// Copyright 2025 Tabs Data Inc.
//

use std::env::JoinPathsError;
use std::io::Error;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::string::FromUtf8Error;
use td_error::td_error;

#[td_error]
pub enum PythonError {
    #[error("Error converting stdout utf-8 stream to string: {0}")]
    OutputEncodingError(#[source] FromUtf8Error) = 5001,
    #[error("Environment variable PATH contains invalid characters: {0}")]
    WrongEnvPath(#[source] JoinPathsError) = 5002,
    #[error("Creation of the environment panicked: {0}")]
    VenvCreationPanic(#[source] Error) = 5003,
    #[error("Creation of environment finished with errors: {0}")]
    VenvCreationError(ExitStatus) = 5004,
    #[error("Failed to extract environment from output: {0}")]
    VenvCreationParseError(String) = 5005,
    #[error("Deciding the current Python environment panicked: {0}")]
    InterpreterResolutionPanic(#[source] Error) = 5006,
    #[error("Deciding the current Python environment finished with errors: {0}")]
    InterpreterResolutionError(ExitStatus) = 5007,
    #[error("Failed to extract interpreter from output: {0}")]
    InterpreterResolutionParseError(String) = 5008,
    #[error("Error instance name from path '{0}'.")]
    InstanceExtractionError(PathBuf) = 5009,
    #[error("Upgrade of instance panicked: {0}")]
    InstanceUpgradePanic(#[source] Error) = 5010,
    #[error("Upgrade of instance finished with errors: {0}")]
    InstanceUpgradeError(ExitStatus) = 5011,
    #[error("The file .version for this instance is empty.")]
    EmptyVersionFile = 5012,
    #[error("Invalid version format in .version file: {0}")]
    InvalidVersionFormat(String) = 5013,
    #[error("Failed to read .version file: {0}")]
    InvalidVersionFile(#[source] Error) = 5014,
}
