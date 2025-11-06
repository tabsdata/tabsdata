//
// Copyright 2025 Tabs Data Inc.
//

use crate::rest_urls::{
    AtTimeParam, CollectionParam, FileFormat, FileFormatParam, FunctionParam, SampleOffsetLenParam,
    SqlParam, TableParam,
};
use crate::types::basic::{
    AtTime, CollectionIdName, FunctionIdName, SampleLen, SampleOffset, SchemaFieldName,
    SchemaFieldType, Sql, TableIdName,
};
use polars::prelude::Field;
use td_error::TdError;

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
            collection: collection.collection.clone(),
            at: at.at.clone(),
        }
    }
}

#[td_type::Dlo]
pub struct FunctionAtIdName {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    function: FunctionIdName,
    #[td_type(extractor)]
    at: AtTime,
}

impl FunctionAtIdName {
    pub fn new(function: FunctionParam, at: AtTimeParam) -> Self {
        Self {
            collection: function.collection.clone(),
            function: function.function.clone(),
            at: at.at.clone(),
        }
    }
}

#[td_type::Dlo]
pub struct TableAtIdName {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    table: TableIdName,
    #[td_type(extractor)]
    at: AtTime,
}

impl TableAtIdName {
    pub fn new(table: TableParam, at: AtTimeParam) -> Self {
        Self {
            collection: table.collection.clone(),
            table: table.table.clone(),
            at: at.at.clone(),
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
    #[td_type(extractor)]
    format: FileFormat,
    #[td_type(extractor)]
    sql: Option<Sql>,
}

impl TableSampleAtName {
    pub fn new(
        table: TableParam,
        at: AtTimeParam,
        offset_len: SampleOffsetLenParam,
        format: FileFormatParam,
        sql: SqlParam,
    ) -> Self {
        Self {
            collection: table.collection.clone(),
            table: table.table.clone(),
            at: at.at.clone(),
            offset: offset_len.offset.clone(),
            len: offset_len.len.clone(),
            format: format.format.clone(),
            sql: sql.sql.clone(),
        }
    }
}

#[td_type::Dto]
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

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct TableSchema {
    pub fields: Vec<SchemaField>,
}
