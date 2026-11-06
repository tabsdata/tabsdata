//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use td_objects::types::basic::WorkerConfig;

fn validate(worker_type: WorkerTypeDto, config: &str) -> Result<(), ValidatorError> {
    let worker_config = WorkerConfig::try_from(config).unwrap();
    KubernetesWorkloadValidator.validate(&WorkloadDefinition {
        worker_type: &worker_type,
        worker_config: &worker_config,
    })
}

fn cron_trigger(mask: &str) -> Result<(), ValidatorError> {
    validate(
        WorkerTypeDto::CronTrigger,
        &serde_json::json!({ "start": null, "end": null, "mask": mask }).to_string(),
    )
}

#[test]
fn a_cron_trigger_kubernetes_schedules_is_valid() {
    for mask in ["*/5 * * * *", "@daily", "@every 1h30m", "0 0 * * fri"] {
        assert!(cron_trigger(mask).is_ok(), "{mask}");
    }
}

#[test]
fn a_cron_trigger_kubernetes_refuses_is_unrunnable() {
    let error = cron_trigger("0 0 * * 7").unwrap_err();
    assert_eq!(
        error.to_string(),
        "The cron trigger worker would not run: Invalid cron schedule \"0 0 * * 7\": \
         end of range (7) above maximum (6): 7"
    );
}

#[test]
fn a_cron_trigger_without_a_mask_is_unrunnable() {
    let error = validate(WorkerTypeDto::CronTrigger, "{}").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("its configuration does not parse"),
        "{error}"
    );
}

#[test]
fn a_stage_trigger_is_valid() {
    assert!(validate(WorkerTypeDto::StageTrigger, "{}").is_ok());
}
