//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, FunctionName, FunctionVersionId, TableId, TableName,
    TableVersionId, TableVersions, UserId, UserName,
};
use crate::types::function::FunctionVersionDB;

#[td_type::Dao]
#[dao(sql_table = "dependencies")]
#[td_type(builder(try_from = DependencyVersionDB))]
pub struct DependencyDB {
    #[td_type(builder(field = "dependency_id"))]
    id: DependencyId,
    collection_id: CollectionId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    #[td_type(builder(field = "id"))]
    dependency_version_id: DependencyVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
}

#[td_type::Dao]
#[dao(sql_table = "dependencies__with_names")]
pub struct DependencyDBWithNames {
    id: DependencyId,
    collection_id: CollectionId,
    function_id: FunctionId,
    dependency_version_id: DependencyVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,

    collection: CollectionName,
    table_collection: CollectionName,
}

#[td_type::Dao]
#[dao(
    sql_table = "dependency_versions",
    partition_by = "dependency_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "function_version_id", down = "table_function_version_id")
)]
#[td_type(
    builder(try_from = FunctionVersionDB, skip_all),
    updater(try_from = RequestContext, skip_all)
)]
pub struct DependencyVersionDB {
    #[builder(default)]
    id: DependencyVersionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[builder(default)]
    dependency_id: DependencyId,
    #[td_type(builder(include, field = "function_id"))]
    function_id: FunctionId,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_function_version_id: FunctionVersionId,
    table_id: TableId,
    table_version_id: TableVersionId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    status: DependencyStatus,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
}

#[td_type::Dao]
#[dao(
    sql_table = "dependency_versions__with_names",
    order_by = "dep_pos",
    partition_by = "dependency_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "function_version_id", down = "table_function_version_id")
)]
pub struct DependencyVersionDBWithNames {
    id: DependencyVersionId,
    collection_id: CollectionId,
    dependency_id: DependencyId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_function_version_id: FunctionVersionId,
    table_id: TableId,
    table_version_id: TableVersionId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    status: DependencyStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    trigger_by_collection: CollectionName,
    table_collection: CollectionName,
    table_function: FunctionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = DependencyVersionDBWithNames))]
pub struct DependencyVersionRead {
    id: DependencyVersionId,
    collection_id: CollectionId,
    dependency_id: DependencyId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    table_collection_id: CollectionId,
    table_id: TableId,
    table_name: TableName,
    table_versions: TableVersions,
    dep_pos: DependencyPos,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    defined_by: UserName,
}

pub type DependencyVersionDBWithNamesList = DependencyVersionDBWithNames;

pub type DependencyVersionList = DependencyVersionRead;
