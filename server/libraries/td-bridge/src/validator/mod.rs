//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;
use td_objects::types::basic::{WorkerConfig, WorkerTypeDto};

#[td_error]
pub enum ValidatorError {
    #[error("The {worker} worker would not run: {cause}")]
    Unrunnable { worker: String, cause: String } = 0,
}

pub struct WorkloadDefinition<'a> {
    pub worker_type: &'a WorkerTypeDto,
    pub worker_config: &'a WorkerConfig,
}

pub trait WorkloadValidator: Send + Sync {
    fn validate(&self, definition: &WorkloadDefinition<'_>) -> Result<(), ValidatorError>;
}
