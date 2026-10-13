//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::worker_input::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, WorkerConfig, WorkerId,
};

/// Lean k8s cron `request.yaml`: identity + the cron `config` (schedule),
/// no `task` callback. Unlike the supervisor's
/// `cron_trigger::CronTriggerInput` (which embeds the `task` HTTP
/// callback), the k8s cron-publisher receives its trigger via env from the
/// pod Secret, so this yaml is callback-free. The resolve endpoint builds
/// it from the worker row alone (no jwt / server addresses).
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CronInput {
    pub info: CronInfo,
}

impl Locations for CronInput {
    fn locations(&self) -> Vec<&Location> {
        vec![]
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CronInfo {
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub worker_id: WorkerId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub scheduled_on: AtTime,
    pub config: WorkerConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronTriggerConfig;
    use crate::dxo::worker_input::WorkerInput;
    use crate::types::basic::CronMask;

    #[test]
    fn test_cron_input_ser_deser() {
        let cfg = CronTriggerConfig {
            start: None,
            end: None,
            mask: CronMask::try_from("* * * * *").unwrap(),
        };
        let worker_id = WorkerId::new();
        let function_version_id = FunctionVersionId::new();
        let input = WorkerInput::Cron(Box::new(CronInput {
            info: CronInfo {
                worker_id: worker_id.clone(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: function_version_id.clone(),
                function: FunctionName::try_from("function").unwrap(),
                scheduled_on: AtTime::default(),
                config: WorkerConfig::serialize(cfg).unwrap(),
            },
        }));
        let serialized = serde_json::to_value(&input).unwrap();
        let info = &serialized["Cron"]["info"];
        assert!(info.get("task").is_none());
        assert_eq!(info["worker_id"], worker_id.padded_str());
        assert_eq!(
            info["function_version_id"],
            function_version_id.padded_str()
        );

        let deserialized: WorkerInput = serde_json::from_value(serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
