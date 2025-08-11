//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, System, TableId, TriggerId, TriggerStatus,
    TriggerVersionId, UserId,
};
use crate::types::function::FunctionDB;

#[td_type::Dao]
#[dao(
    sql_table = "triggers",
    partition_by = "trigger_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_id", down = "function_id")
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
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_table_id: TableId,
    status: TriggerStatus,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    system: System,
}

#[td_type::Dao]
#[dao(
    sql_table = "triggers__with_names",
    partition_by = "trigger_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_id", down = "function_id")
)]
pub struct TriggerDBWithNames {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    trigger_by_table_id: TableId,
    status: TriggerStatus,
    defined_on: AtTime,
    defined_by_id: UserId,
    system: System,

    collection: CollectionName,
    trigger_by_collection: CollectionName,
}

#[td_type::Dao]
#[dao(
    sql_table = "triggers__read",
    partition_by = "trigger_id",
    versioned_at(order_by = "defined_on", condition_by = "status"),
    recursive(up = "trigger_by_function_id", down = "function_id")
)]
pub struct TriggerDBRead {
    id: TriggerVersionId,
    collection_id: CollectionId,
    trigger_id: TriggerId,
    function_id: FunctionId,
    trigger_by_collection_id: CollectionId,
    trigger_by_function_id: FunctionId,
    #[td_type(extractor)]
    trigger_by_table_id: TableId,
    status: TriggerStatus,
    defined_on: AtTime,
    defined_by_id: UserId,

    collection: CollectionName,
    trigger_by_collection: CollectionName,
}
