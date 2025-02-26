//
// Copyright 2025 Tabs Data Inc.
//

use crate as td_objects;
use chrono::{DateTime, Utc};
use td_common::uri::Version;
use td_type::service_type;

pub trait Creator<T> {
    fn create(value: impl Into<T>) -> Self;
}

pub trait Value<T> {
    fn value(&self) -> &T;
}

#[service_type]
#[derive(Debug, Clone)]
pub struct UserId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct UserName(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct CollectionId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct CollectionName(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct DatasetId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct DatasetName(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct FunctionId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct RequestUser(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct RequestTime(pub DateTime<Utc>);

#[service_type]
#[derive(Debug, Clone)]
pub struct RequestUserId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct RequestIsAdmin(pub bool);

#[service_type]
#[derive(Debug, Clone)]
pub struct ExecutionPlanId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct TransactionId(pub String);

#[service_type]
#[derive(Debug, Clone)]
pub struct DataVersionId(String);

#[service_type]
#[derive(Debug, Clone)]
pub struct Limit(i32);

#[service_type]
#[derive(Debug, Clone)]
pub struct TableName(String);

#[service_type]
#[derive(Debug, Clone)]
pub struct VersionId(Version);

#[service_type]
#[derive(Debug, Clone)]
pub struct WorkerMessageId(pub String);
