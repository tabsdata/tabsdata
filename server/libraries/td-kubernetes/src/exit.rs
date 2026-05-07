//
// Copyright 2026 Tabs Data Inc.
//

use td_common::status::ExitStatus;

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

pub type Outcome<T = ()> = Result<T, CommandError>;

pub fn abort<T>(status: ExitStatus) -> Outcome<T> {
    Err(CommandError::new(status))
}
