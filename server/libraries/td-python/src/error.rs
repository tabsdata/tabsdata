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
    #[error("Error converting stdout utf-8 stream to string.")]
    OutputEncodingError(#[source] FromUtf8Error) = 5001,
    #[error("Environment variable PATH contains invalid characters")]
    WrongEnvPath(#[source] JoinPathsError) = 5002,
    #[error("Creation of the environment panicked.")]
    VenvCreationPanic(#[source] Error) = 5003,
    #[error("Creation of environment finished with errors.")]
    VenvCreationError(ExitStatus) = 5004,
    #[error("Failed to extract environment from output.")]
    VenvCreationParseError(String) = 5005,
    #[error("Deciding the current Python environment panicked.")]
    InterpreterResolutionPanic(#[source] Error) = 5006,
    #[error("Deciding the current Python environment finished with errors.")]
    InterpreterResolutionError(ExitStatus) = 5007,
    #[error("Failed to extract interpreter from output.")]
    InterpreterResolutionParseError(String) = 5008,
    #[error("Error instance name from path '{0}'.")]
    InstanceExtractionError(PathBuf) = 5009,
}
