//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::worker_input::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, WorkerConfig, WorkerId,
};
use td_common::worker::HttpRequest;

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct CronTriggerInput {
    pub info: Info,
}

impl Locations for CronTriggerInput {
    fn locations(&self) -> Vec<&Location> {
        vec![]
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct Info {
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub worker_id: WorkerId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    #[serde(serialize_with = "td_common::logic_id::padded")]
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub scheduled_on: AtTime, // when the request yaml was created
    pub task: HttpRequest,
    pub config: WorkerConfig, // cron trigger config serialized
}

pub type CronTriggerOutput = ();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronTriggerConfig;
    use crate::dxo::worker_input::WorkerInput;
    use crate::types::basic::{AtTime, CollectionName, CronMask, FunctionName, WorkerConfig};
    use chrono::DateTime;
    use http::Method;
    use serde_json::json;
    use url::Url;

    #[test]
    fn test_cron_input_ser_deser() {
        let worker_id = WorkerId::new();
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::new();
        let scheduled_on = AtTime::default();
        let start = DateTime::default();
        let end = DateTime::default();

        let cfg = CronTriggerConfig {
            start: Some(start),
            end: Some(end),
            mask: CronMask::try_from("* * * * *").unwrap(),
        };
        let input = WorkerInput::CronTrigger(Box::new(CronTriggerInput {
            info: Info {
                worker_id: worker_id.clone(),
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id: function_version_id.clone(),
                function: FunctionName::try_from("function").unwrap(),
                scheduled_on,
                task: HttpRequest {
                    url: Url::try_from("http://localhost:2457/internal/fn_trigger").unwrap(),
                    method: Method::POST,
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
                "CronTrigger": {
                    "info": {
                        "worker_id": worker_id.padded_str(),
                        "collection_id": collection_id,
                        "collection": "collection",
                        "function_version_id": function_version_id.padded_str(),
                        "function": "function",
                        "scheduled_on": scheduled_on,
                        "task": {
                            "url": "http://localhost:2457/internal/fn_trigger",
                            "method": "POST",
                            "headers": {},
                            "body": false
                        },
                        "config": format!(r#"{{"start":{start},"end":{end},"mask":"* * * * *"}}"#)
                    }
                }
            })
        );
        let deserialized: WorkerInput = serde_json::from_value(serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
