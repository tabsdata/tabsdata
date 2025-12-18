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
pub struct CronInput {
    pub info: Info,
}

impl Locations for CronInput {
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
    pub scheduled_on: AtTime, // when the request yaml was created
    pub task: HttpRequest,    // http task of the cron
    pub config: WorkerConfig, // CronConfig serialized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::worker::CronConfig;
    use crate::dxo::yaml::WorkerInput;
    use crate::dxo::yaml::http::HttpRequest;
    use crate::types::basic::{
        AtTime, CollectionName, CronMask, FunctionName, WorkerConfig, WorkerVersion,
    };
    use http::Method;
    use url::Url;

    #[test]
    fn test_cron_input_ser_deser() {
        let collection_id = CollectionId::default();
        let function_version_id = FunctionVersionId::default();
        let scheduled_on = AtTime::default();
        let start = AtTime::default();
        let end = AtTime::default();

        let cfg = CronConfig {
            version: WorkerVersion::default(),
            start: Some(start),
            end: Some(end),
            mask: CronMask::try_from("* * * * *").unwrap(),
        };
        let input = WorkerInput::CronV1(Box::new(CronInput {
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
                task: HttpRequest {
                    method: Method::POST.to_string(),
                    url: Url::try_from("http://localhost:2457/internal/fn_trigger").unwrap(),
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
                r#"!CronV1
info:
  collection_id: {collection_id}
  collection: collection
  function_version_id: {function_version_id}
  function: function
  function_bundle:
    uri: file:///foo
    env_prefix: null
  scheduled_on: {scheduled_on}
  task:
    method: POST
    url: http://localhost:2457/internal/fn_trigger
    headers: []
    body: null
  config: '{{"version":"V1","start":{start},"end":{end},"mask":"* * * * *"}}'
"#
            )
        );
        let deserialized: WorkerInput = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(input, deserialized);
    }
}
