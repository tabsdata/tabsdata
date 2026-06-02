//
// Copyright 2026 Tabs Data Inc.
//

use td_common::status::ExitStatus;
use td_error::{ApiError, TdDomainError, TdError};

use crate::base::error::kubernetes::{KubeconfigError, KubernetesError};
use crate::base::error::launcher::LauncherError;
use crate::base::error::manifest::ManifestError;
use crate::base::error::naming::NamingError;
use crate::base::error::profile::ProfileError;
use crate::base::error::provider::ProviderError;

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

fn exit_status_for(error: &TdError) -> ExitStatus {
    match error.api_error() {
        ApiError::InputError
        | ApiError::NotFound
        | ApiError::NotAllowed
        | ApiError::Forbidden
        | ApiError::NotAuthorized => ExitStatus::TabsDataError,
        ApiError::InternalError | ApiError::NotImplemented | ApiError::Unexpected => {
            ExitStatus::GeneralError
        }
    }
}

impl From<TdError> for CommandError {
    fn from(error: TdError) -> Self {
        let status = exit_status_for(&error);
        CommandError::with(status, error)
    }
}

impl From<KubernetesError> for CommandError {
    fn from(error: KubernetesError) -> Self {
        TdError::from(error).into()
    }
}

crate::command_error_from!(
    KubeconfigError,
    LauncherError,
    ManifestError,
    NamingError,
    ProfileError,
    ProviderError
);

pub type CommandResult<T = ()> = Result<T, CommandError>;

pub fn abort<T>(status: ExitStatus) -> CommandResult<T> {
    Err(CommandError::new(status))
}

pub fn abort_with<T, E>(status: ExitStatus, error: E) -> CommandResult<T>
where
    E: TdDomainError + 'static,
{
    Err(CommandError::with(status, TdError::new(error)))
}

#[cfg(test)]
mod tests;
