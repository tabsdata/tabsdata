//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, WorkerConfig, WorkerId,
};
use td_common::server::HttpRequest;

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct CronInput {
    pub info: Info,
}

impl Locations for CronInput {
    fn locations(&self) -> Vec<&Location> {
        vec![]
    }
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct Info {
    pub worker_id: WorkerId,
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub scheduled_on: AtTime, // when the request yaml was created
    pub task: HttpRequest,    // http task of the cron
    pub config: WorkerConfig, // CronConfig serialized
}

pub type CronOutput = ();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronConfig;
    use crate::dxo::yaml::WorkerInput;
    use crate::types::basic::{AtTime, CollectionName, CronMask, FunctionName, WorkerConfig};
    use chrono::DateTime;
    use http::Method;
    use url::Url;

    #[test]
    fn test_cron_input_ser_deser() {
        let worker_id = WorkerId::default();
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::default();
        let scheduled_on = AtTime::default();
        let start = DateTime::default();
        let end = DateTime::default();

        let cfg = CronConfig {
            start: Some(start),
            end: Some(end),
            mask: CronMask::try_from("* * * * *").unwrap(),
        };
        let input = WorkerInput::CronV1(Box::new(CronInput {
            info: Info {
                worker_id,
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id,
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

        let scheduled_on = scheduled_on.as_str_millis();
        let start = serde_json::to_string(&start).unwrap();
        let end = serde_json::to_string(&end).unwrap();
        assert_eq!(
            serialized.as_str(),
            format!(
                r#"!CronV1
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
