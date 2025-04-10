//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, Frozen, FunctionId, FunctionName, FunctionVersionId,
    Private, TableFunctionParamPos, TableId, TableName, TableStatus, TableVersionId, UserId,
    UserName,
};
use crate::types::function::{FunctionDB, FunctionVersionDB};

#[td_type::Dao(sql_table = "tables")]
#[td_type(builder(try_from = TableVersionDB, skip_all))]
#[td_type(updater(try_from = FunctionDB, skip_all))]
pub struct TableDB {
    #[td_type(builder(include, field = "table_id"))]
    id: TableId,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(include))]
    name: TableName,
    #[td_type(updater(include, field = "id"))]
    function_id: FunctionId,
    #[td_type(builder(include))]
    function_version_id: FunctionVersionId,
    #[td_type(builder(include, field = "id"))]
    table_version_id: TableVersionId,
    #[builder(default = "Frozen::from(false)")]
    frozen: Frozen,
    #[td_type(builder(include))]
    private: Private,
    #[td_type(builder(include, field = "defined_on"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "defined_by_id"))]
    created_by_id: UserId,
}

#[td_type::Dao(sql_table = "tables__with_names")]
pub struct TableDBWithNames {
    #[td_type(extractor)]
    id: TableId,
    collection_id: CollectionId,
    name: TableName,
    #[td_type(extractor)]
    function_id: FunctionId,
    #[td_type(extractor)]
    function_version_id: FunctionVersionId,
    #[td_type(extractor)]
    table_version_id: TableVersionId,
    frozen: Frozen,
    private: Private,
    created_on: AtTime,
    created_by_id: UserId,

    collection: CollectionName,
    created_by: UserName,
}

#[td_type::Dao(
    sql_table = "table_versions",
    partition_by = "table_id",
    natural_order_by = "defined_on"
)]
#[td_type(builder(try_from = FunctionVersionDB, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct TableVersionDB {
    #[builder(default)]
    id: TableVersionId,
    #[td_type(extractor, builder(include))]
    collection_id: CollectionId,
    table_id: TableId,
    #[td_type(extractor)]
    name: TableName,
    #[td_type(builder(include, field = "id"))]
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    #[builder(default = "Private::from(false)")]
    private: Private,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    status: TableStatus,
}

#[td_type::Dao(
    sql_table = "table_versions__with_names",
    order_by = "function_param_pos",
    partition_by = "table_id",
    natural_order_by = "defined_on"
)]
pub struct TableVersionDBWithNames {
    id: TableVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,

    collection: CollectionName,
    function: FunctionName,
    defined_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = TableVersionDBWithNames))]
pub struct TableVersion {
    id: TableVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    function_version_id: FunctionVersionId,
    function_param_pos: Option<TableFunctionParamPos>,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,

    collection: CollectionName,
    defined_by: UserName,
}
