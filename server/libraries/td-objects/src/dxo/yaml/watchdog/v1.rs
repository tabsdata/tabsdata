//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::yaml::http::HttpRequest;
use crate::dxo::yaml::location::{Location, Locations};
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionVersionId, WorkerConfig,
};

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct WatchdogInput {
    pub info: Info,
}

impl Locations for WatchdogInput {
    fn locations(&self) -> Vec<&Location> {
        vec![&self.info.function_bundle]
    }
}

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct Info {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub function: FunctionName,
    pub function_bundle: Location,
    pub scheduled_on: AtTime,    // when the request yaml was created
    pub fn_trigger: HttpRequest, // trigger of the function
    pub fn_state: HttpRequest,   // state of the function
    pub fn_status: HttpRequest,  // status of the execution plan (uses ep_id and f_version_id)
    pub config: WorkerConfig,    // WatchdogConfig serialized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::WatchdogConfig;
    use crate::dxo::yaml::WorkerInput;
    use crate::dxo::yaml::http::HttpRequest;
    use crate::types::basic::{CollectionName, FunctionName, WorkerConfig, WorkerVersion};
    use http::Method;
    use url::Url;

    #[test]
    fn test_watchdog_input_ser_deser() {
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::default();
        let scheduled_on = AtTime::default();
        let start = AtTime::default();
        let end = AtTime::default();

        let cfg = WatchdogConfig {
            version: WorkerVersion::default(),
            timeout_threshold_min: Default::default(),
            size_threshold: Default::default(),
            start: Some(start),
            end: Some(end),
        };
        let input = WorkerInput::WatchdogV1(Box::new(WatchdogInput {
            info: Info {
                collection_id,
                collection: CollectionName::try_from("collection").unwrap(),
                function_version_id,
                function: FunctionName::try_from("function").unwrap(),
                function_bundle: Location {
                    uri: Url::try_from("file:///foo").unwrap(),
                    env_prefix: None,
                },
                scheduled_on,
                fn_trigger: HttpRequest {
                    method: Method::POST.to_string(),
                    url: Url::try_from("http://localhost:2457/internal/fn_trigger").unwrap(),
                    headers: vec![],
                    body: None,
                },
                fn_state: HttpRequest {
                    method: Method::GET.to_string(),
                    url: Url::try_from("http://localhost:2457/internal/fn_state").unwrap(),
                    headers: vec![(
                        "content-type".to_string(),
                        "application/parquet".to_string(),
                    )],
                    body: None,
                },
                fn_status: HttpRequest {
                    method: Method::GET.to_string(),
                    url: Url::try_from("http://localhost:2457/internal/fn_status").unwrap(),
                    headers: vec![],
                    body: None,
                },
                config: WorkerConfig::serialize(cfg).unwrap(),
            },
        }));
        let serialized = serde_yaml::to_string(&input).unwrap();
        // println!("{}", serialized);

        let scheduled_on = scheduled_on.as_str_millis();
        let start = start.as_str_millis();
        let end = end.as_str_millis();
        assert_eq!(
            serialized.as_str(),
            format!(
                r#"!WatchdogV1
info:
  collection_id: {collection_id}
  collection: collection
  function_version_id: {function_version_id}
  function: function
  function_bundle:
    uri: file:///foo
    env_prefix: null
  scheduled_on: {scheduled_on}
  fn_trigger:
    method: POST
    url: http://localhost:2457/internal/fn_trigger
    headers: []
    body: null
  fn_state:
    method: GET
    url: http://localhost:2457/internal/fn_state
    headers:
    - - content-type
      - application/parquet
    body: null
  fn_status:
    method: GET
    url: http://localhost:2457/internal/fn_status
    headers: []
    body: null
  config: '{{"version":"V1","timeout_threshold_min":60,"size_threshold":10000,"start":{start},"end":{end}}}'
"#
            )
        );
        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
