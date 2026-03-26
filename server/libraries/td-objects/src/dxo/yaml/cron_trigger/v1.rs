//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, WorkerConfig, WorkerId,
};
use td_common::server::HttpRequest;

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
    pub worker_id: WorkerId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub scheduled_on: AtTime, // when the request yaml was created
    pub task: HttpRequest,    // http task of the cron trigger
    pub config: WorkerConfig, // cron trigger config serialized
}

pub type CronTriggerOutput = ();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronTriggerConfig;
    use crate::dxo::yaml::WorkerInput;
    use crate::types::basic::{AtTime, CollectionName, CronMask, FunctionName, WorkerConfig};
    use chrono::DateTime;
    use http::Method;
    use url::Url;

    #[test]
    fn test_cron_input_ser_deser() {
        let worker_id = WorkerId::default();
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
        let input = WorkerInput::CronTriggerV1(Box::new(CronTriggerInput {
            info: Info {
                worker_id,
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
        let serialized = serde_yaml::to_string(&input).unwrap();
        // println!("{}", serialized);

        let scheduled_on = scheduled_on.to_string();
        let start = serde_json::to_string(&start).unwrap();
        let end = serde_json::to_string(&end).unwrap();
        let function_version_id = function_version_id.unpadded_str();
        assert_eq!(
            serialized.as_str(),
            format!(
                r#"!CronTriggerV1
info:
  worker_id: {worker_id}
  collection_id: {collection_id}
  collection: collection
  function_version_id: {function_version_id}
  function: function
  scheduled_on: {scheduled_on}
  task:
    url: http://localhost:2457/internal/fn_trigger
    method: POST
    headers: {{}}
    body: false
  config: '{{"start":{start},"end":{end},"mask":"* * * * *"}}'
"#
            )
        );
        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
