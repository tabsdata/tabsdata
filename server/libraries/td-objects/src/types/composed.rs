//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::table::TableDBWithNames;
use crate::table_ref::{TableRef, VersionedTableRef, Versions};
use crate::types::ComposedString;
use crate::types::basic::{TableName, TableNameDto};
use td_error::TdError;

#[td_type::typed(composed(inner = "VersionedTableRef::<TableName>"), try_from = TableDependencyDto)]
pub struct TableDependency;

#[td_type::typed(composed(inner = "VersionedTableRef::<TableNameDto>"))]
pub struct TableDependencyDto;

#[td_type::typed(composed(inner = "TableRef::<TableName>"), try_from = TableTriggerDto)]
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

#[td_type::typed(composed(inner = "TableRef::<TableNameDto>"))]
pub struct TableTriggerDto;

#[td_type::typed(composed(inner = "Versions"))]
pub struct TableVersions;
