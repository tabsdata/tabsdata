//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BundleHash, BundleId, CollectionId, CollectionName, DataLocation, Description, Frozen,
    FunctionId, FunctionName, FunctionRuntimeValues, FunctionStatus, FunctionVersionId,
    ReuseFrozen, Snippet, StorageVersion, TableDependency, TableName, TableTrigger, UserId,
    UserName,
};
use axum::body::BodyDataStream;
use axum::extract::Request;
use std::sync::Arc;
use tokio::sync::Mutex;

#[td_type::Dao(sql_table = "functions")]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
pub struct FunctionDB {
    #[td_type(extractor, builder(include, field = "function_id"))]
    id: FunctionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: FunctionName,
    #[td_type(extractor, builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    #[builder(default)]
    frozen: Frozen,
    #[td_type(builder(include, field = "defined_on"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "defined_by_id"))]
    created_by_id: UserId,
}

#[td_type::Dao(sql_table = "functions__with_names")]
pub struct FunctionDBWithNames {
    #[td_type(extractor)]
    id: FunctionId,
    collection_id: CollectionId,
    name: FunctionName,
    #[td_type(extractor)]
    function_version_id: FunctionVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionDBWithNames))]
pub struct Function {
    id: FunctionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_version_id: FunctionVersionId,
    frozen: Frozen,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dto]
pub struct FunctionRegister {
    #[td_type(extractor)]
    name: FunctionName,
    description: Description,
    bundle_id: BundleId,
    snippet: Snippet,
    #[td_type(extractor)]
    dependencies: Option<Vec<TableDependency>>,
    #[td_type(extractor)]
    triggers: Option<Vec<TableTrigger>>,
    #[td_type(extractor)]
    tables: Option<Vec<TableName>>,
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

#[td_type::Dao(sql_table = "bundles")]
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

#[td_type::Dao(
    sql_table = "function_versions",
    partition_by = "function_id",
    natural_order_by = "defined_on"
)]
#[td_type(builder(try_from = FunctionRegister, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct FunctionVersionDB {
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
    runtime_values: FunctionRuntimeValues,
    #[builder(default)]
    #[td_type(setter)]
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

#[td_type::Dao(
    sql_table = "function_versions__with_names",
    partition_by = "function_id",
    natural_order_by = "defined_on"
)]
pub struct FunctionVersionDBWithNames {
    #[td_type(extractor)]
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
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
#[td_type(builder(try_from = FunctionVersionDBWithNames))]
pub struct FunctionVersion {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    description: Description,
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
pub struct FunctionVersionWithTables {
    #[serde(flatten)]
    #[td_type(setter)]
    function_version: FunctionVersion,

    #[td_type(setter)]
    dependencies: Vec<TableDependency>,
    #[td_type(setter)]
    triggers: Vec<TableTrigger>,
    #[td_type(setter)]
    tables: Vec<TableName>,
}

#[td_type::Dto]
pub struct FunctionVersionWithAllVersions {
    #[serde(flatten)]
    #[td_type(setter)]
    current: FunctionVersionWithTables,
    #[td_type(setter)]
    all: Vec<FunctionVersion>,
}

#[td_type::Dao]
pub struct FunctionVersionDBWithNamesList {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_id: FunctionId,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionVersionDBWithNamesList))]
pub struct FunctionVersionList {
    id: FunctionVersionId,
    collection_id: CollectionId,
    name: FunctionName,
    function_id: FunctionId,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: FunctionStatus,

    collection: CollectionName,
    defined_by: UserName,
}
