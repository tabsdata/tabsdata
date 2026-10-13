//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::worker_input::location::{Location, Locations};
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
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_version_id: FunctionVersionId,
    pub function_defined_on: AtTime,
    pub function: FunctionName,
    pub function_bundle: Location,
    pub function_stage: Location,
    pub scheduled_on: AtTime,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub worker_id: WorkerId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker_input::WorkerInput;
    use td_common::datetime::Timestamp;
    use url::Url;

    #[test]
    fn test_streamer_input_ser_deser() {
        let function_version_id = FunctionVersionId::new();
        let worker_id = WorkerId::new();
        let input = WorkerInput::Streamer(Box::new(StreamerInput {
            info: StreamerInfo {
                project_id: ProjectId::default(),
                collection_id: CollectionId::default(),
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: function_version_id.clone(),
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
                worker_id: worker_id.clone(),
            },
        }));
        let serialized = serde_json::to_value(&input).unwrap();
        let info = &serialized["Streamer"]["info"];
        for absent in ["fn_trigger", "function_checkpoint", "config"] {
            assert!(info.get(absent).is_none(), "{absent} is serialized");
        }
        assert_eq!(
            info["function_version_id"],
            function_version_id.padded_str()
        );
        assert_eq!(info["worker_id"], worker_id.padded_str());

        let deserialized: WorkerInput = serde_json::from_value(serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
