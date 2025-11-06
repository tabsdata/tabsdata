//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBRead;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::table_ref::VersionedTableRef;
use td_objects::types::composed::TableDependency;
use td_tower::extractors::Input;

pub async fn vec_create_table_dependency(
    Input(dependencies): Input<Vec<DependencyDBRead>>,
    Input(tables): Input<Vec<TableDBWithNames>>,
) -> Result<Vec<TableDependency>, TdError> {
    let tables = tables
        .iter()
        .map(|t| (t.table_id, t))
        .collect::<HashMap<_, _>>();

    let table_deps = dependencies
        .iter()
        .filter_map(|d| {
            let table = tables.get(&d.table_id)?;
            let versions = d.table_versions.deref().clone();
            Some(TableDependency::new(VersionedTableRef::new(
                Some(table.collection.clone()),
                table.name.clone(),
                versions,
            )))
        })
        .collect::<Vec<_>>();
    Ok(table_deps)
}
