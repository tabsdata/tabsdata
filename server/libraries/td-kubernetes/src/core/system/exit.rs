//
// Copyright 2026 Tabs Data Inc.
//

use td_common::status::ExitStatus;
use td_error::{TdDomainError, TdError};

use crate::base::error::kubernetes::KubernetesError;

#[derive(Debug)]
pub struct CommandError {
    status: ExitStatus,
    source: Option<TdError>,
}

impl CommandError {
    pub fn new(status: ExitStatus) -> Self {
        Self {
            status,
            source: None,
        }
    }

    pub fn with(status: ExitStatus, error: TdError) -> Self {
        Self {
            status,
            source: Some(error),
        }
    }

    pub fn status(&self) -> &ExitStatus {
        &self.status
    }

    pub fn source(&self) -> Option<&TdError> {
        self.source.as_ref()
    }
}

impl From<TdError> for CommandError {
    fn from(error: TdError) -> Self {
        CommandError::with(ExitStatus::GeneralError, error)
    }
}

impl From<KubernetesError> for CommandError {
    fn from(error: KubernetesError) -> Self {
        CommandError::with(ExitStatus::GeneralError, TdError::from(error))
    }
}

pub type Result<T = ()> = std::result::Result<T, CommandError>;

pub fn abort<T>(status: ExitStatus) -> Result<T> {
    Err(CommandError::new(status))
}

pub fn abort_with<T, E>(status: ExitStatus, error: E) -> Result<T>
where
    E: TdDomainError + 'static,
{
    Err(CommandError::with(status, TdError::new(error)))
}

#[cfg(test)]
mod tests;
