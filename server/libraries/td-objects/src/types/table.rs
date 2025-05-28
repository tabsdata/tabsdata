//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::rest_urls::{AtTimeParam, CollectionParam, SampleOffsetLenParam, TableParam};
use crate::types::basic::{
    AtTime, CollectionId, CollectionIdName, CollectionName, FunctionId, FunctionName,
    FunctionVersionId, Partitioned, Private, SampleLen, SampleOffset, SchemaFieldName,
    SchemaFieldType, TableDataVersionId, TableFunctionParamPos, TableId, TableIdName, TableName,
    TableStatus, TableVersionId, UserId, UserName,
};
use crate::types::function::FunctionDB;
use polars::prelude::Field;
use td_error::TdError;

#[td_type::Dao]
#[dao(
    sql_table = "tables",
    partition_by = "table_id",
    versioned_at(order_by = "defined_on", condition_by = "status")
)]
#[td_type(
    builder(try_from = TableDB),
    builder(try_from = FunctionDB, skip_all),
    updater(try_from = RequestContext, skip_all)
)]
pub struct TableDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: TableVersionId,
    #[td_type(extractor, builder(include))]
    collection_id: CollectionId,
    #[td_type(extractor)]
    table_id: TableId,
    #[td_type(extractor)]
    name: TableName,
    #[td_type(builder(include, try_from = FunctionDB, field = "function_id"))]
    function_id: FunctionId,
    #[td_type(builder(include, try_from = FunctionDB, field = "id"))]
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
    sql_table = "tables__with_names",
    order_by = "function_param_pos",
    partition_by = "table_id",
    versioned_at(order_by = "defined_on", condition_by = "status")
)]
pub struct TableDBWithNames {
    #[td_type(extractor)]
    id: TableVersionId,
    collection_id: CollectionId,
    #[td_type(extractor)]
    table_id: TableId,
    name: TableName,
    function_id: FunctionId,
    #[td_type(extractor)]
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
    at: AtTime,
}

impl CollectionAtName {
    pub fn new(collection: CollectionParam, at: AtTimeParam) -> Self {
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
    at: AtTime,
}

impl TableAtName {
    pub fn new(table: TableParam, at: AtTimeParam) -> Self {
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
    at: AtTime,
    #[td_type(extractor)]
    offset: SampleOffset,
    #[td_type(extractor)]
    len: SampleLen,
}

impl TableSampleAtName {
    pub fn new(table: TableParam, at: AtTimeParam, offset_len: SampleOffsetLenParam) -> Self {
        Self {
            collection: table.collection().clone(),
            table: table.table().clone(),
            at: at.at().clone(),
            offset: offset_len.offset().clone(),
            len: offset_len.len().clone(),
        }
    }
}

#[td_type::Dao]
#[dao(
    sql_table = "tables__read",
    partition_by = "table_id",
    versioned_at(order_by = "defined_on", condition_by = "status")
)]
pub struct TableDBRead {
    id: TableVersionId,
    name: TableName,
    table_id: TableId,
    collection_id: CollectionId,
    collection_name: CollectionName,
    function_version_id: FunctionVersionId,
    function_name: FunctionName,
    last_data_version: Option<TableDataVersionId>,
    // last_data_changed_version: Option<TableDataVersionId>,
    status: TableStatus,
    defined_on: AtTime,
}

#[td_type::Dto]
#[dto(list(on = TableDBRead))]
#[td_type(builder(try_from = TableDBRead))]
pub struct Table {
    #[dto(list(pagination_by = "+"))]
    id: TableVersionId,
    #[dto(list(order_by))]
    name: TableName,
    collection_id: CollectionId,
    collection_name: CollectionName,
    table_id: TableId,
    function_version_id: FunctionVersionId,
    function_name: FunctionName,
    last_data_version: Option<TableDataVersionId>,
    // last_data_changed_version: Option<TableDataVersionId>, // TODO this is hard to get in a view
    defined_on: AtTime,
}

#[td_type::Dto()]
pub struct SchemaField {
    name: SchemaFieldName,
    #[serde(rename = "type")]
    type_: SchemaFieldType,
}

impl TryFrom<Field> for SchemaField {
    type Error = TdError;
    fn try_from(field: Field) -> Result<Self, TdError> {
        let schema_field = SchemaField::builder()
            .try_name(field.name().to_string())?
            .try_type_(field.dtype().to_string())?
            .build()?;
        Ok(schema_field)
    }
}

#[td_type::Dto]
pub struct TableSchema {
    fields: Vec<SchemaField>,
}
