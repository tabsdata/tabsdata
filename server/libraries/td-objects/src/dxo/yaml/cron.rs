//
// Copyright 2026 Tabs Data Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
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
    pub worker_id: WorkerId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub scheduled_on: AtTime,
    pub config: WorkerConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronTriggerConfig;
    use crate::dxo::yaml::WorkerInput;
    use crate::types::basic::CronMask;

    #[test]
    fn test_cron_input_ser_deser() {
        let cfg = CronTriggerConfig {
            start: None,
            end: None,
            mask: CronMask::try_from("* * * * *").unwrap(),
        };
        let input = WorkerInput::Cron(Box::new(CronInput {
            info: CronInfo {
                worker_id: WorkerId::new(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: FunctionVersionId::new(),
                function: FunctionName::try_from("function").unwrap(),
                scheduled_on: AtTime::default(),
                config: WorkerConfig::serialize(cfg).unwrap(),
            },
        }));
        let serialized = serde_yaml::to_string(&input).unwrap();

        // Lean: no `task` callback.
        assert!(serialized.starts_with("!Cron\ninfo:"));
        assert!(!serialized.contains("task"));

        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
