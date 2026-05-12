//
// Copyright 2026 Tabs Data Inc.
//

use td_common::status::ExitStatus;
use td_error::{TdDomainError, TdError};
use tracing::{error, info};

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

pub type Result<T = ()> = std::result::Result<T, CommandError>;

pub fn abort<T>(status: ExitStatus) -> Result<T> {
    match status {
        ExitStatus::Cancelled => info!("Operation cancelled."),
        ExitStatus::NoAction => {}
        _ => {
            error!("Command aborted.");
            error!("");
        }
    }
    Err(CommandError::new(status))
}

pub fn abort_with<T, E>(status: ExitStatus, error: E) -> Result<T>
where
    E: TdDomainError + 'static,
{
    let td_error = TdError::new(error);
    match status {
        ExitStatus::Cancelled => info!("Operation cancelled: {}", td_error),
        ExitStatus::NoAction => info!("{}", td_error),
        _ => {
            error!("Command aborted: {}", td_error);
            error!("");
        }
    }
    Err(CommandError::with(status, td_error))
}
