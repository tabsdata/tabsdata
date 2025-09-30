//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BundleHash, BundleId, CollectionId, CollectionName, Connector, DataLocation, Decorator,
    Description, FunctionId, FunctionName, FunctionRuntimeValues, FunctionStatus,
    FunctionVersionId, ReuseFrozen, Snippet, StorageVersion, TableDependency, TableDependencyDto,
    TableName, TableNameDto, TableTrigger, TableTriggerDto, UserId, UserName,
};
use axum::body::BodyDataStream;
use axum::extract::Request;
use std::sync::Arc;
use tokio::sync::Mutex;

#[td_type::Dto]
pub struct FunctionRegister {
    #[td_type(extractor)]
    name: FunctionName,
    description: Description,
    bundle_id: BundleId,
    snippet: Snippet,
    decorator: Decorator,
    #[builder(default)]
    connector: Option<Connector>,
    #[td_type(extractor)]
    dependencies: Option<Vec<TableDependencyDto>>,
    #[td_type(extractor)]
    triggers: Option<Vec<TableTriggerDto>>,
    #[td_type(extractor)]
    tables: Option<Vec<TableNameDto>>,
    #[serde(default)]
    runtime_values: FunctionRuntimeValues,
    #[td_type(extractor)]
    reuse_frozen_tables: ReuseFrozen,
}

pub type FunctionUpdate = FunctionRegister;

// This behaves like a dto, for request the whole body.
#[derive(Debug, Clone)]
pub struct FunctionUpload {
    request: Arc<Mutex<Option<Request>>>,
}

impl FunctionUpload {
    pub fn new(request: Request) -> Self {
        Self {
            request: Arc::new(Mutex::new(Some(request))),
        }
    }

    pub async fn stream(&self) -> Option<BodyDataStream> {
        self.request
            .lock()
            .await
            .take()
            .map(|request| request.into_body().into_data_stream())
    }
}

#[td_type::Dao]
#[dao(sql_table = "bundles")]
#[td_type(builder(try_from = RequestContext, skip_all))]
pub struct BundleDB {
    #[td_type(setter)]
    id: BundleId,
    #[td_type(setter)]
    collection_id: CollectionId,
    #[td_type(setter)]
    hash: BundleHash,
    #[td_type(builder(include, field = "time"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "user_id"))]
    created_by_id: UserId,
}

#[td_type::Dto]
#[td_type(builder(try_from = BundleDB))]
pub struct Bundle {
    id: BundleId,
}

#[td_type::Dao]
#[dao(
    sql_table = "functions",
    partition_by = "function_id",
    versioned_at(order_by = "defined_on", condition_by = "status")
)]
#[td_type(
    builder(try_from = FunctionDB),
    builder(try_from = FunctionRegister, skip_all),
    updater(try_from = RequestContext, skip_all)
)]
pub struct FunctionDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: FunctionVersionId,
    #[td_type(setter)]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: FunctionName,
    #[td_type(builder(include))]
    description: Description,
    #[td_type(builder(include))]
    decorator: Decorator,
    #[td_type(builder(include))]
    connector: Option<Connector>,
    #[td_type(builder(include))]
    runtime_values: FunctionRuntimeValues,
    #[builder(default)]
    #[td_type(setter, extractor)]
    function_id: FunctionId,
    #[td_type(setter)]
    data_location: DataLocation,
    #[td_type(setter)]
    storage_version: StorageVersion,
    #[td_type(builder(include), extractor)]
    bundle_id: BundleId,
    #[td_type(builder(include))]
    snippet: Snippet,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    #[builder(default = FunctionStatus::Active)]
    status: FunctionStatus,
}

#[td_type::Dao]
#[dao(
    sql_table = "functions__with_names",
    partition_by = "function_id",
    versioned_at(order_by = "defined_on", condition_by = "status")
)]
pub struct FunctionDBWithNames {
    #[td_type(extractor)]
    id: FunctionVersionId,
    #[td_type(extractor)]
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
    decorator: Decorator,
    connector: Option<Connector>,
    #[td_type(extractor)]
    function_id: FunctionId,
    data_location: DataLocation,
    storage_version: StorageVersion,
    bundle_id: BundleId,
    snippet: Snippet,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[dto(list(on = FunctionDBWithNames))]
#[td_type(builder(try_from = FunctionDBWithNames))]
pub struct Function {
    #[dto(list(pagination_by = "+"))]
    id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    description: Description,
    #[dto(list(filter, filter_like, order_by))]
    decorator: Decorator,
    #[dto(list(filter, filter_like, order_by))]
    connector: Option<Connector>,
    #[dto(list(filter, filter_like, order_by))]
    function_id: FunctionId,
    #[dto(list(filter, filter_like, order_by))]
    data_location: DataLocation,
    #[dto(list(filter, filter_like, order_by))]
    storage_version: StorageVersion,
    bundle_id: BundleId,
    #[dto(list(filter, filter_like, order_by))]
    snippet: Snippet,
    #[dto(list(filter, filter_like, order_by))]
    defined_on: AtTime,
    #[dto(list(filter, filter_like, order_by))]
    defined_by_id: UserId,
    #[dto(list(filter, filter_like, order_by))]
    status: FunctionStatus,

    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    defined_by: UserName,
}

#[td_type::Dto]
pub struct FunctionWithTables {
    #[serde(flatten)]
    #[td_type(setter)]
    function_version: Function,

    #[td_type(setter)]
    dependencies: Vec<TableDependency>,
    #[td_type(setter)]
    triggers: Vec<TableTrigger>,
    #[td_type(setter)]
    tables: Vec<TableName>,
}

#[td_type::Dto]
pub struct FunctionWithAllVersions {
    #[serde(flatten)]
    #[td_type(setter)]
    current: FunctionWithTables,
    #[td_type(setter)]
    all: Vec<Function>,
}
