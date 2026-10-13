//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::worker_input::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, ProjectId, WorkerConfig,
    WorkerId,
};
use td_common::worker::HttpRequest;

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct StageTriggerInput {
    pub info: Info,
}

impl Locations for StageTriggerInput {
    fn locations(&self) -> Vec<&Location> {
        vec![&self.info.function_bundle]
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct Info {
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub worker_id: WorkerId,
    /// The trigger's project — workers resolve project-scoped resources
    /// (e.g. the connection artifacts under `connections/<project_id>/`)
    /// with it.
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_version_id: FunctionVersionId,
    pub function_defined_on: AtTime,
    pub function: FunctionName,
    pub function_bundle: Location,
    pub function_stage: Location,
    pub function_checkpoint: Location,
    pub scheduled_on: AtTime, // when the request yaml was created
    pub fn_trigger: HttpRequest,
    pub fn_state: HttpRequest,
    pub fn_status: HttpRequest, // status of the execution plan (uses ep_id and f_version_id)
    pub config: WorkerConfig,   // StageTriggerConfig serialized
}

pub type StageTriggerOutput = ();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::StageTriggerConfig;
    use crate::dxo::worker_input::WorkerInput;
    use crate::types::basic::{CollectionName, FunctionName, WorkerConfig};
    use chrono::DateTime;
    use http::Method;
    use serde_json::json;
    use std::collections::HashMap;
    use td_common::datetime::Timestamp;
    use url::Url;

    #[test]
    fn test_stage_trigger_input_ser_deser() {
        let worker_id = WorkerId::new();
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::new();
        let scheduled_on = AtTime::default();
        let start = DateTime::default();
        let end = DateTime::default();
        let defined_on = AtTime::from(Timestamp::default());

        let cfg = StageTriggerConfig {
            timeout_threshold_min: Default::default(),
            size_threshold: Default::default(),
            start: Some(start),
            end: Some(end),
        };
        let project_id = ProjectId::default();
        let input = WorkerInput::StageTrigger(Box::new(StageTriggerInput {
            info: Info {
                worker_id: worker_id.clone(),
                project_id,
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: function_version_id.clone(),
                function: FunctionName::try_from("function").unwrap(),
                function_defined_on: defined_on,
                function_bundle: Location {
                    uri: Url::try_from("file:///bundle").unwrap(),
                    env_prefix: None,
                },
                function_stage: Location {
                    uri: Url::try_from("file:///stage").unwrap(),
                    env_prefix: None,
                },
                function_checkpoint: Location {
                    uri: Url::try_from("file:///checkpoint").unwrap(),
                    env_prefix: None,
                },
                scheduled_on,
                fn_trigger: HttpRequest {
                    url: Url::try_from("http://localhost:2457/internal/fn_trigger").unwrap(),
                    method: Method::POST,
                    headers: Default::default(),
                    body: false,
                },
                fn_state: HttpRequest {
                    url: Url::try_from("http://localhost:2457/internal/fn_state").unwrap(),
                    method: Method::GET,
                    headers: HashMap::from([(
                        "content-type".to_string(),
                        "application/parquet".to_string(),
                    )]),
                    body: false,
                },
                fn_status: HttpRequest {
                    url: Url::try_from("http://localhost:2457/internal/fn_status").unwrap(),
                    method: Method::GET,
                    headers: Default::default(),
                    body: false,
                },
                config: WorkerConfig::serialize(cfg).unwrap(),
            },
        }));
        let serialized = serde_json::to_value(&input).unwrap();

        let start = serde_json::to_string(&start).unwrap();
        let end = serde_json::to_string(&end).unwrap();
        assert_eq!(
            serialized,
            json!({
                "StageTrigger": {
                    "info": {
                        "worker_id": worker_id.padded_str(),
                        "project_id": project_id,
                        "collection_id": collection_id,
                        "collection": "collection",
                        "function_version_id": function_version_id.padded_str(),
                        "function_defined_on": defined_on,
                        "function": "function",
                        "function_bundle": { "uri": "file:///bundle", "env_prefix": null },
                        "function_stage": { "uri": "file:///stage", "env_prefix": null },
                        "function_checkpoint": { "uri": "file:///checkpoint", "env_prefix": null },
                        "scheduled_on": scheduled_on,
                        "fn_trigger": {
                            "url": "http://localhost:2457/internal/fn_trigger",
                            "method": "POST",
                            "headers": {},
                            "body": false
                        },
                        "fn_state": {
                            "url": "http://localhost:2457/internal/fn_state",
                            "method": "GET",
                            "headers": { "content-type": "application/parquet" },
                            "body": false
                        },
                        "fn_status": {
                            "url": "http://localhost:2457/internal/fn_status",
                            "method": "GET",
                            "headers": {},
                            "body": false
                        },
                        "config": format!(
                            r#"{{"timeout_threshold_min":60,"size_threshold":10000,"start":{start},"end":{end}}}"#
                        )
                    }
                }
            })
        );
        let deserialized: WorkerInput = serde_json::from_value(serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
