//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::types::basic::{TableName, TableNameDto};
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};
use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_error::TdError;

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(try_from = TableDependencyDto)]
pub struct TableDependency(VersionedTableRef<TableName>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct TableDependencyDto(VersionedTableRef<TableNameDto>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
#[typed(try_from = TableTriggerDto)]
pub struct TableTrigger(TableRef<TableName>);

impl TryFrom<&TableDependencyDto> for TableTrigger {
    type Error = TdError;

    fn try_from(v: &TableDependencyDto) -> Result<Self, Self::Error> {
        let table = TableTrigger::new(TableRef::new(
            v.collection.clone(),
            v.table.clone().try_into()?,
        ));
        Ok(table)
    }
}

impl TryFrom<&TableDBWithNames> for TableTrigger {
    type Error = TdError;

    fn try_from(v: &TableDBWithNames) -> Result<Self, Self::Error> {
        let table = TableTrigger::new(TableRef::new(
            Some(v.collection.clone()),
            v.base.name.clone(),
        ));
        Ok(table)
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct TableTriggerDto(TableRef<TableNameDto>);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableVersions(Versions);
