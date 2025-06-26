//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus,
    DependencyVersionId, FunctionId, FunctionName, FunctionVersionId, TableId, TableName,
    TableVersionId, TableVersions, UserId, UserName,
};
use crate::types::function::FunctionDB;

#[td_type::Dao]
#[dao(
    sql_table = "dependencies",
    partition_by = "table_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "function_version_id", down = "table_function_version_id")
)]
#[td_type(
    builder(try_from = FunctionDB, skip_all),
    updater(try_from = RequestContext, skip_all)
)]
pub struct DependencyDB {
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
    sql_table = "dependencies__with_names",
    order_by = "dep_pos",
    partition_by = "dependency_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "function_version_id", down = "table_function_version_id")
)]
pub struct DependencyDBWithNames {
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

#[td_type::Dao]
#[dao(
    sql_table = "dependencies__read",
    order_by = "dep_pos",
    partition_by = "dependency_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "function_version_id", down = "table_function_version_id")
)]
pub struct DependencyDBRead {
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
