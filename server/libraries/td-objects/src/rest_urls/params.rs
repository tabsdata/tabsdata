//
// Copyright 2025 Tabs Data Inc.
//

use crate::rest_urls::{
    AtTimeParam, CollectionParam, FileFormat, FileFormatParam, FunctionParam, SampleOffsetLenParam,
    SqlParam, TableParam, UncommittedParam,
};
use crate::types::basic::{
    AtTime, CollectionIdName, FunctionIdName, SampleLen, SampleOffset, SchemaFieldName,
    SchemaFieldType, Sql, TableIdName, UncommittedStatus,
};
use polars::prelude::Field;
use ta_objects::Buildable;
use td_error::TdError;

#[ta_objects::Dlo]
pub struct CollectionAtName {
    #[dxo(extractor)]
    collection: CollectionIdName,
    #[dxo(extractor)]
    at: AtTime,
}

impl CollectionAtName {
    pub fn new(collection: CollectionParam, at: AtTimeParam) -> Self {
        Self {
            collection: collection.collection,
            at: at.at,
        }
    }
}

#[ta_objects::Dlo]
pub struct FunctionAtIdName {
    #[dxo(extractor)]
    collection: CollectionIdName,
    #[dxo(extractor)]
    function: FunctionIdName,
    #[dxo(extractor)]
    at: AtTime,
}

impl FunctionAtIdName {
    pub fn new(function: FunctionParam, at: AtTimeParam) -> Self {
        Self {
            collection: function.collection,
            function: function.function,
            at: at.at,
        }
    }
}

#[ta_objects::Dlo]
pub struct TableAtIdName {
    #[dxo(extractor)]
    pub collection: CollectionIdName,
    #[dxo(extractor)]
    pub table: TableIdName,
    #[dxo(extractor)]
    pub at: AtTime,
    #[builder(default)]
    #[dxo(extractor)]
    pub uncommitted: UncommittedStatus,
}

impl TableAtIdName {
    pub fn new(table: TableParam, at: AtTimeParam, uncommitted: UncommittedParam) -> Self {
        Self {
            collection: table.collection,
            table: table.table,
            at: at.at,
            uncommitted: uncommitted.uncommitted,
        }
    }
}

#[ta_objects::Dlo]
pub struct PhysicalTableDataVersionAtIdName {
    #[dxo(extractor)]
    collection: CollectionIdName,
    #[dxo(extractor)]
    table: TableIdName,
    #[dxo(extractor)]
    at: AtTime,
    #[builder(default)]
    #[dxo(extractor)]
    uncommitted: UncommittedStatus,
}

impl PhysicalTableDataVersionAtIdName {
    pub fn new(table: TableAtIdName) -> Self {
        Self {
            collection: table.collection,
            table: table.table,
            at: table.at,
            uncommitted: table.uncommitted,
        }
    }
}

#[ta_objects::Dlo]
pub struct TableSampleAtName {
    #[dxo(extractor)]
    collection: CollectionIdName,
    #[dxo(extractor)]
    table: TableIdName,
    #[dxo(extractor)]
    at: AtTime,
    #[builder(default)]
    #[dxo(extractor)]
    uncommitted: UncommittedStatus,
    #[dxo(extractor)]
    offset: SampleOffset,
    #[dxo(extractor)]
    len: SampleLen,
    #[dxo(extractor)]
    format: FileFormat,
    #[dxo(extractor)]
    sql: Option<Sql>,
}

impl TableSampleAtName {
    pub fn new(
        table: TableParam,
        at: AtTimeParam,
        uncommitted: UncommittedParam,
        offset_len: SampleOffsetLenParam,
        format: FileFormatParam,
        sql: SqlParam,
    ) -> Self {
        Self {
            collection: table.collection,
            table: table.table,
            at: at.at,
            uncommitted: uncommitted.uncommitted,
            offset: offset_len.offset,
            len: offset_len.len,
            format: format.format,
            sql: sql.sql,
        }
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct SchemaField {
    name: SchemaFieldName,
    #[serde(rename = "type")]
    type_: SchemaFieldType,
}

impl TryFrom<Field> for SchemaField {
    type Error = TdError;
    fn try_from(field: Field) -> Result<Self, TdError> {
        let schema_field = SchemaField::builder()
            .try_name(field.name.to_string())?
            .try_type_(field.dtype.to_string())?
            .build()?;
        Ok(schema_field)
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct TableSchema {
    pub fields: Vec<SchemaField>,
}
