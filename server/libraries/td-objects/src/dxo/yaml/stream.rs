//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, ProjectId, WorkerId,
};

/// Lean k8s streamer `request.yaml`: identity + bundle/stage locations only.
///
/// Unlike the supervisor's `stage_trigger::StageTriggerInput` (which embeds
/// the `fn_trigger`/`fn_state`/`fn_status` callbacks), the k8s streamer
/// receives its callbacks via env from the pod Secret, so this yaml is
/// callback-free -- exactly like `function::FunctionInput`. The resolve
/// endpoint builds it from `Storage` alone (no jwt / server addresses).
/// Mirrors the Python `_lib/request.py:StreamerInfo`; change both together.
#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct StreamerInput {
    pub info: StreamerInfo,
}

impl Locations for StreamerInput {
    fn locations(&self) -> Vec<&Location> {
        vec![&self.info.function_bundle]
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct StreamerInfo {
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function_defined_on: AtTime,
    pub function: FunctionName,
    pub function_bundle: Location,
    pub function_stage: Location,
    pub scheduled_on: AtTime,
    pub worker_id: WorkerId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::yaml::WorkerInput;
    use td_common::datetime::Timestamp;
    use url::Url;

    #[test]
    fn test_streamer_input_ser_deser() {
        let input = WorkerInput::Streamer(Box::new(StreamerInput {
            info: StreamerInfo {
                project_id: ProjectId::default(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: FunctionVersionId::new(),
                function_defined_on: AtTime::from(Timestamp::default()),
                function: FunctionName::try_from("function").unwrap(),
                function_bundle: Location {
                    uri: Url::try_from("file:///bundle").unwrap(),
                    env_prefix: None,
                },
                function_stage: Location {
                    uri: Url::try_from("file:///stage").unwrap(),
                    env_prefix: None,
                },
                scheduled_on: AtTime::default(),
                worker_id: WorkerId::new(),
            },
        }));
        let serialized = serde_yaml::to_string(&input).unwrap();

        // Lean: callback-free and no checkpoint.
        assert!(serialized.starts_with("!Streamer\ninfo:"));
        assert!(!serialized.contains("fn_trigger"));
        assert!(!serialized.contains("function_checkpoint"));
        assert!(!serialized.contains("config"));

        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
