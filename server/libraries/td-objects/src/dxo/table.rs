//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::dxo::execution::{FunctionNodeResponse, TableNodeResponse};
use crate::dxo::graph::GraphEdge;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionId, FunctionName, FunctionVersionId, Partitioned,
    Private, SampleLen, SampleOffset, SchemaFieldName, SchemaFieldType, Sql, System,
    TableFunctionParamPos, TableId, TableIdName, TableName, TableStatus, TableType, TableVersionId,
    UncommittedStatus, UserId, UserName,
};
use crate::types::composed::TableVersions;
use polars::prelude::Field;
use std::collections::{HashMap, HashSet};
use ta_objects::{FromDao, StateFilter, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx, and, or};
use td_error::TdError;

#[ta_objects::Dao]
#[dao(
    sql_table = "tables",
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "table_id",
        current = "tables_current",
    )
)]
pub struct TableDB {
    pub id: TableVersionId,
    pub collection_id: CollectionId,
    pub table_id: TableId,
    pub name: TableName,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    pub function_param_pos: Option<TableFunctionParamPos>,
    pub private: Private,
    pub partitioned: Partitioned,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub status: TableStatus,
    pub table_type: TableType,
}

impl TableDB {
    /// Tables that can be generated and consumed.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(TableStatus::Active)
    }

    /// A new Deleted version of this table.
    pub fn deleted(&self, ctx: &RequestContext) -> Self {
        Self {
            id: TableVersionId::default(),
            status: TableStatus::Deleted,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }

    /// A new version of this table bound to a re-registered function version.
    pub fn re_registered(
        &self,
        ctx: &RequestContext,
        function_version_id: FunctionVersionId,
    ) -> Self {
        Self {
            id: TableVersionId::default(),
            function_version_id,
            defined_on: ctx.time,
            defined_by_id: ctx.user_id,
            ..self.clone()
        }
    }

    /// Every non-deleted table (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(TableStatus::Active, TableStatus::Frozen))
    }
}

#[ta_objects::Dao]
#[dao(
    sql_table = "tables__with_names",
    order_by(col = "function_param_pos", dir = "desc"),
    versioned(
        pk = "id",
        version_by = "defined_on",
        partition_by = "table_id",
        current = "tables_current__with_names"
    )
)]
pub struct TableDBWithNames {
    #[dao(flatten)]
    pub base: TableDB,
    pub system: System,
    pub defined_by: UserName,
    pub collection: CollectionName,
    pub function: FunctionName,
}

impl TableDBWithNames {
    /// Tables that can be generated and consumed.
    pub fn active() -> StateFilter<Self> {
        StateFilter::of(TableStatus::Active)
    }

    /// Tables that cannot be generated but can still be consumed.
    pub fn frozen() -> StateFilter<Self> {
        StateFilter::of(TableStatus::Frozen)
    }

    /// Every non-deleted table (active or frozen).
    pub fn available() -> StateFilter<Self> {
        StateFilter::of(or!(TableStatus::Active, TableStatus::Frozen))
    }

    /// Non-deleted user tables.
    pub fn user() -> StateFilter<Self> {
        StateFilter::of(and!(
            or!(TableStatus::Active, TableStatus::Frozen),
            System::FALSE
        ))
    }

    /// Non-deleted system tables.
    pub fn system() -> StateFilter<Self> {
        StateFilter::of(and!(
            or!(TableStatus::Active, TableStatus::Frozen),
            System::TRUE
        ))
    }
}

#[ta_objects::Dto]
#[dto(list(on = TableDBWithNames))]
pub struct Table {
    #[dto(list(pagination_by = "+"))]
    pub id: TableVersionId,
    pub collection_id: CollectionId,
    pub table_id: TableId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: TableName,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    pub function_param_pos: Option<TableFunctionParamPos>,
    pub private: Private,
    pub partitioned: Partitioned,
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    pub status: TableStatus,
    pub table_type: TableType,
    pub system: System,
    pub defined_by: UserName,
    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub function: FunctionName,
}

impl FromDao for Table {
    type Dao = TableDBWithNames;

    fn from_dao(dao: TableDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            collection_id: dao.base.collection_id,
            table_id: dao.base.table_id,
            name: dao.base.name,
            function_id: dao.base.function_id,
            function_version_id: dao.base.function_version_id,
            function_param_pos: dao.base.function_param_pos,
            private: dao.base.private,
            partitioned: dao.base.partitioned,
            defined_on: dao.base.defined_on,
            defined_by_id: dao.base.defined_by_id,
            status: dao.base.status,
            table_type: dao.base.table_type,
            system: dao.system,
            defined_by: dao.defined_by,
            collection: dao.collection,
            function: dao.function,
        })
    }
}

#[ta_objects::Dto]
pub struct TableFlowGraphResponse {
    pub table: TableVersionId,
    pub all_functions: HashMap<FunctionVersionId, FunctionNodeResponse>,
    pub all_tables: HashMap<TableVersionId, TableNodeResponse>,
    pub system_tables: HashSet<TableVersionId>,
    pub user_tables: HashSet<TableVersionId>,
    pub relations: Vec<(FunctionVersionId, TableVersionId, GraphEdge<TableVersions>)>,
}

#[ta_objects::UrlParam]
pub struct TableParam {
    pub table: TableIdName,
}

/// `?uncommitted=` — whether a read may resolve to a data version whose
/// producing run has not committed yet.
#[ta_objects::QueryParam]
pub struct UncommittedParam {
    #[serde(default)]
    pub uncommitted: UncommittedStatus,
}

#[ta_objects::QueryParam]
pub struct SampleOffsetParam {
    #[serde(default)]
    pub offset: SampleOffset,
}

#[ta_objects::QueryParam]
pub struct SampleLenParam {
    #[serde(default)]
    pub len: SampleLen,
}

#[ta_objects::QueryParam]
pub struct FileFormatParam {
    #[serde(default)]
    pub format: FileFormat,
}

#[ta_objects::QueryParam]
pub struct SqlParam {
    #[serde(default)]
    pub sql: Option<Sql>,
}

/// `?extra-cols=` — which non-user column families a read returns on top of
/// the user columns: the `@td.` connector metadata and the `$td.` system ones.
#[ta_objects::QueryParam]
pub struct ExtraColsParam {
    #[serde(default, rename = "extra-cols")]
    pub extra_cols: ExtraCols,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default)]
pub enum ExtraCols {
    #[default]
    #[typed_enum(rename = "none")]
    None,
    #[typed_enum(rename = "meta")]
    Meta,
    #[typed_enum(rename = "sys")]
    Sys,
    #[typed_enum(rename = "all")]
    All,
}

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Default)]
pub enum FileFormat {
    #[default]
    Parquet,
    Csv,
    Json,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct SchemaField {
    pub name: SchemaFieldName,
    #[serde(rename = "type")]
    pub type_: SchemaFieldType,
}

impl TryFrom<Field> for SchemaField {
    type Error = TdError;
    fn try_from(field: Field) -> Result<Self, TdError> {
        let schema_field = SchemaField {
            name: field.name.to_string().try_into()?,
            type_: field.dtype.to_string().try_into()?,
        };
        Ok(schema_field)
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct TableSchema {
    pub fields: Vec<SchemaField>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extra_cols_param_wire_form() {
        let param: ExtraColsParam = serde_json::from_str(r#"{"extra-cols":"meta"}"#).unwrap();
        assert!(matches!(param.extra_cols, ExtraCols::Meta));
        let param: ExtraColsParam = serde_json::from_str(r#"{}"#).unwrap();
        assert!(matches!(param.extra_cols, ExtraCols::None));
        assert_eq!(ExtraCols::Sys.to_string(), "sys");
        assert!(serde_json::from_str::<ExtraColsParam>(r#"{"extra-cols":"nope"}"#).is_err());
    }
}
