//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::types::basic::{TableName, TableNameDto};
use crate::types::table_ref::{TableRef, VersionedTableRef, Versions};
use ta_objects::ComposedString;
use td_error::TdError;

#[ta_objects::typed(composed(inner = "VersionedTableRef::<TableName>"), try_from = TableDependencyDto)]
#[derive(Eq, PartialEq)]
pub struct TableDependency;

#[ta_objects::typed(composed(inner = "VersionedTableRef::<TableNameDto>"))]
#[derive(Eq, PartialEq, Hash)]
pub struct TableDependencyDto;

#[ta_objects::typed(composed(inner = "TableRef::<TableName>"), try_from = TableTriggerDto)]
#[derive(Eq, PartialEq, Hash)]
pub struct TableTrigger;

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
        let table = TableTrigger::new(TableRef::new(Some(v.collection.clone()), v.name.clone()));
        Ok(table)
    }
}

#[ta_objects::typed(composed(inner = "TableRef::<TableNameDto>"))]
#[derive(Eq, PartialEq)]
pub struct TableTriggerDto;

#[ta_objects::typed(composed(inner = "Versions"))]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TableVersions;
