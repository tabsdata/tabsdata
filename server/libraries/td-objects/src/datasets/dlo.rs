//
// Copyright 2025 Tabs Data Inc.
//

//! Datasets Data Logic Objects

use crate as td_objects;
use crate::datasets::dao::{DatasetWithNames, DependencyUris, TriggerUris};
use crate::datasets::dto::TableUriParams;
use crate::dlo::{CollectionName, Creator, DatasetName, TableName};
use crate::tower_service::extractor::{
    CollectionNameProvider, DatasetIdProvider, DatasetNameProvider, TableProvider, VersionProvider,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures_util::Stream;
use getset::Getters;
use itertools::Itertools;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::pin::Pin;
use td_common::error::TdError;
use td_common::execution_status::DataVersionStatus;
use td_common::uri::TdUriNameId;
use td_error::td_error;
use td_typing::service_type;

#[service_type]
pub struct FunctionTablesMap(pub HashMap<String, Vec<String>>);

#[service_type]
pub struct FunctionDependenciesMap(pub HashMap<String, Vec<DependencyUris>>);

#[service_type]
pub struct FunctionTriggersMap(pub HashMap<String, Vec<TriggerUris>>);

#[service_type]
pub struct WorkerLogPaths(pub Vec<PathBuf>);

pub struct BoxedSyncStream(
    pub Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>>,
);

impl BoxedSyncStream {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static,
    {
        Self(Box::pin(stream))
    }

    pub fn into_inner(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, TdError>> + Send + Sync + 'static>> {
        self.0
    }
}

impl DatasetIdProvider for DatasetWithNames {
    fn dataset_id(&self) -> String {
        self.id().to_string()
    }
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct FunctionTriggers {
    triggers: Vec<TdUriNameId>,
}

impl FunctionTriggers {
    pub fn new(uri_name_id: Vec<TdUriNameId>) -> Self {
        Self {
            triggers: uri_name_id,
        }
    }

    #[cfg(test)]
    pub fn is_none(&self) -> bool {
        self.triggers.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionDependencies {
    to_self: BTreeMap<i64, TdUriNameId>,
    external: BTreeMap<i64, TdUriNameId>,
}

impl FunctionDependencies {
    pub fn new(to_self: BTreeMap<i64, TdUriNameId>, external: BTreeMap<i64, TdUriNameId>) -> Self {
        Self { to_self, external }
    }

    pub fn to_self(&self) -> Vec<&TdUriNameId> {
        self.to_self.values().collect()
    }

    pub fn external(&self) -> Vec<&TdUriNameId> {
        self.external.values().collect()
    }

    pub fn all(&self) -> Vec<&TdUriNameId> {
        self.to_self
            .iter()
            .merge_by(&self.external, |(k1, _), (k2, _)| k1 < k2)
            .map(|(_, v)| v)
            .collect()
    }
}

#[derive(Debug, Clone, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DataVersionState<T = Value> {
    status: DataVersionStatus,
    #[builder(default)]
    start: Option<DateTime<Utc>>,
    #[builder(default)]
    end: Option<DateTime<Utc>>,
    #[builder(default)]
    execution: Option<u16>,
    #[builder(default)]
    limit: Option<u16>,
    #[builder(default)]
    error: Option<String>,
    #[builder(default)]
    context: Option<T>,
}

pub trait IntoDateTimeUtc {
    fn datetime_utc(self) -> Result<DateTime<Utc>, FromTimestampMillisError>;
}

impl IntoDateTimeUtc for i64 {
    fn datetime_utc(self) -> Result<DateTime<Utc>, FromTimestampMillisError> {
        match DateTime::from_timestamp_millis(self) {
            Some(dt) => Ok(dt),
            None => Err(FromTimestampMillisError::InvalidTimestamp(self)),
        }
    }
}

#[td_error]
pub enum FromTimestampMillisError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64) = 0,
}

impl CollectionNameProvider for TableUriParams {
    fn collection_name(&self) -> String {
        self.collection().clone()
    }
}

impl DatasetNameProvider for TableUriParams {
    fn dataset_name(&self) -> String {
        self.dataset().clone()
    }
}

impl VersionProvider for TableUriParams {
    fn version(&self) -> String {
        self.version().clone()
    }
}

impl TableProvider for TableUriParams {
    fn table(&self) -> String {
        self.table().clone()
    }
}

impl Creator<TableUriParams> for CollectionName {
    fn create(value: impl Into<TableUriParams>) -> Self {
        CollectionName::new(value.into().collection().clone())
    }
}

impl Creator<TableUriParams> for DatasetName {
    fn create(value: impl Into<TableUriParams>) -> Self {
        DatasetName::new(value.into().dataset().clone())
    }
}

impl Creator<TableUriParams> for TableName {
    fn create(value: impl Into<TableUriParams>) -> Self {
        TableName::new(value.into().table().clone())
    }
}

#[cfg(test)]
pub mod tests {
    use td_common::uri::TdUri;

    #[test]
    fn test_function_trigger() {
        assert!(super::FunctionTriggers::new(vec![]).is_none());

        let uri_name = TdUri::parse("ds", "td://d").unwrap();
        let uri_id = uri_name.replace("0", "1");
        let uri_name_id = super::TdUriNameId::new(uri_name, uri_id);
        let trigger = super::FunctionTriggers::new(vec![uri_name_id]);

        assert!(!trigger.is_none());
    }
}
