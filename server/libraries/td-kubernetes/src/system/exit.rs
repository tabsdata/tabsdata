//
// Copyright 2026 Tabs Data Inc.
//

use td_common::status::ExitStatus;
use tracing::error;

#[derive(Debug)]
pub struct CommandError {
    status: ExitStatus,
}

impl CommandError {
    pub fn new(status: ExitStatus) -> Self {
        Self { status }
    }

    pub fn status(&self) -> &ExitStatus {
        &self.status
    }
}

pub type Result<T = ()> = std::result::Result<T, CommandError>;

pub fn abort<T>(status: ExitStatus) -> Result<T> {
    error!("Command aborted.");
    error!("");
    Err(CommandError::new(status))
}
