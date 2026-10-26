//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::types::basic::{CollectionLabel, TableName, TableNameDto};
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

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct TableTriggerDto(TableRef<TableNameDto>);

/// A dependency as a function read shows it: its collection labelled, so a
/// deleted collection reads `[<id>]/table@versions`.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct LabeledTableDependency(VersionedTableRef<TableName, CollectionLabel>);

/// A trigger as a function read shows it: its collection labelled, so a
/// deleted collection reads `[<id>]/table`.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Hash)]
pub struct LabeledTableTrigger(TableRef<TableName, CollectionLabel>);

impl From<&TableDBWithNames> for LabeledTableTrigger {
    fn from(v: &TableDBWithNames) -> Self {
        LabeledTableTrigger::new(TableRef::new(
            Some(v.collection.clone()),
            v.base.base.name.clone(),
        ))
    }
}

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableVersions(Versions);
