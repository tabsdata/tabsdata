//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId, TableId,
    TableName, TableVersionId, TriggerId, TriggerStatus, TriggerVersionId, UserId, UserName,
};
use crate::types::function::FunctionDB;

#[td_type::Dao]
#[dao(
    sql_table = "triggers",
    partition_by = "function_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_version_id", down = "function_version_id")
)]
#[td_type(
    builder(try_from = FunctionDB, skip_all),
    updater(try_from = RequestContext, skip_all)
)]
pub struct TriggerDB {
    #[builder(default)]
    id: TriggerVersionId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[builder(default)]
    trigger_id: TriggerId,
    #[td_type(builder(include, field = "function_id"))]
    function_id: FunctionId,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    trigger_by_table_version_id: TableVersionId,
    status: TriggerStatus,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
}

#[td_type::Dao]
#[dao(
    sql_table = "triggers__with_names",
    partition_by = "trigger_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_version_id", down = "function_version_id")
)]
pub struct TriggerDBWithNames {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    trigger_by_table_version_id: TableVersionId,
    status: TriggerStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    trigger_by_collection: CollectionName,
    trigger_by_table_name: TableName,
    trigger_by_function: FunctionName,
    defined_by: UserName,
}

#[td_type::Dao]
#[dao(
    sql_table = "triggers__read",
    partition_by = "trigger_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_version_id", down = "function_version_id")
)]
pub struct TriggerDBRead {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    function_version_id: FunctionVersionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_function_version_id: FunctionVersionId,
    trigger_by_table_id: TableId,
    trigger_by_table_version_id: TableVersionId,
    status: TriggerStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    trigger_by_collection: CollectionName,
    trigger_by_table_name: TableName,
    trigger_by_function: FunctionName,
    defined_by: UserName,
}
