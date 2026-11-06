//
// Copyright 2026 Tabsdata Inc.
//

use td_bridge::validator::{ValidatorError, WorkloadDefinition, WorkloadValidator};
use td_common::cron::validate_schedule;
use td_objects::dxo::worker::CronTriggerConfig;
use td_objects::types::basic::WorkerTypeDto;

const CRON_TRIGGER: &str = "cron trigger";

pub struct KubernetesWorkloadValidator;

impl WorkloadValidator for KubernetesWorkloadValidator {
    fn validate(&self, definition: &WorkloadDefinition<'_>) -> Result<(), ValidatorError> {
        match definition.worker_type {
            WorkerTypeDto::CronTrigger => validate_cron_trigger(definition),
            WorkerTypeDto::StageTrigger => Ok(()),
        }
    }
}

fn validate_cron_trigger(definition: &WorkloadDefinition<'_>) -> Result<(), ValidatorError> {
    let unrunnable = |cause: String| ValidatorError::Unrunnable {
        worker: CRON_TRIGGER.to_owned(),
        cause,
    };
    let config: CronTriggerConfig = serde_json::from_str(definition.worker_config.as_str())
        .map_err(|error| unrunnable(format!("its configuration does not parse: {error}")))?;
    validate_schedule(&config.mask.to_string()).map_err(|error| unrunnable(error.to_string()))
}

#[cfg(test)]
mod tests;
