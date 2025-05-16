//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::rest_urls::{AtMultiParam, CollectionParam, SampleOffsetLenParam, TableParam};
use crate::types::basic::{
    AtMulti, AtTime, CollectionId, CollectionIdName, CollectionName, DataChanged, DataVersionId,
    ExecutionId, Frozen, FunctionId, FunctionName, FunctionVersionId, Partitioned, Private,
    SampleLen, SampleOffset, SchemaFieldName, SchemaFieldType, TableFunctionParamPos, TableId,
    TableIdName, TableName, TableStatus, TableVersionId, TransactionId, UserId, UserName,
};
use crate::types::function::{FunctionDB, FunctionVersionDB};

#[td_type::Dao]
#[dao(sql_table = "tables")]
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
    #[td_type(builder(include))]
    partitioned: Partitioned,
    #[td_type(builder(include, field = "defined_on"))]
    created_on: AtTime,
    #[td_type(builder(include, field = "defined_by_id"))]
    created_by_id: UserId,
}

#[td_type::Dao]
#[dao(sql_table = "tables__with_names")]
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
    partitioned: Partitioned,
    created_on: AtTime,
    created_by_id: UserId,

    created_by: UserName,
    collection: CollectionName,
}

#[td_type::Dao]
#[dao(
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
    #[builder(default = "Partitioned::from(false)")]
    partitioned: Partitioned,
    #[td_type(updater(include, field = "time"))]
    defined_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    defined_by_id: UserId,
    status: TableStatus,
}

#[td_type::Dao]
#[dao(
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
    private: Private,
    partitioned: Partitioned,
    defined_on: AtTime,
    defined_by_id: UserId,
    status: TableStatus,

    defined_by: UserName,
    collection: CollectionName,
    function: FunctionName,
}

#[td_type::Dlo]
pub struct CollectionAtName {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    at: AtMulti,
}

impl CollectionAtName {
    pub fn new(collection: CollectionParam, at: AtMultiParam) -> Self {
        Self {
            collection: collection.collection().clone(),
            at: at.at().clone(),
        }
    }
}

#[td_type::Dlo]
pub struct TableAtName {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    table: TableIdName,
    #[td_type(extractor)]
    at: AtMulti,
}

impl TableAtName {
    pub fn new(table: TableParam, at: AtMultiParam) -> Self {
        Self {
            collection: table.collection().clone(),
            table: table.table().clone(),
            at: at.at().clone(),
        }
    }
}

#[td_type::Dlo]
pub struct TableSampleAtName {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    table: TableIdName,
    #[td_type(extractor)]
    at: AtMulti,
    #[td_type(extractor)]
    offset: SampleOffset,
    #[td_type(extractor)]
    len: SampleLen,
}

impl TableSampleAtName {
    pub fn new(table: TableParam, at: AtMultiParam, offset_len: SampleOffsetLenParam) -> Self {
        Self {
            collection: table.collection().clone(),
            table: table.table().clone(),
            at: at.at().clone(),
            offset: offset_len.offset().clone(),
            len: offset_len.len().clone(),
        }
    }
}

#[td_type::Dto(
    //TODO
)]
pub struct Table {
    id: TableVersionId,
    name: TableName,
    collection_id: CollectionId,
    collection_name: CollectionName,
    function_id: FunctionId,
    function_name: FunctionName,
    last_data_version: DataVersionId,
    last_data_changed_version: DataVersionId,
}

#[td_type::Dto(
    //TODO
)]
pub struct TableDataVersion {
    id: TableVersionId,
    collection_id: CollectionId,
    collection_name: CollectionName,
    table_id: TableId,
    table_name: TableName,
    function_id: FunctionId,
    function_name: FunctionName,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    data_changed: DataChanged,
    created_at: AtTime,
}

#[td_type::Dto(
    //TODO
)]
pub struct SchemaField {
    name: SchemaFieldName,
    #[serde(rename = "type")]
    type_: SchemaFieldType,
}

#[td_type::Dto(
    //TODO
)]
pub struct TableSchema {
    fields: Vec<SchemaField>,
}
