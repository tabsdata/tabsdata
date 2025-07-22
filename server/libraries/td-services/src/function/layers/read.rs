//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use td_error::TdError;
use td_objects::types::basic::TableDependency;
use td_objects::types::dependency::DependencyDBRead;
use td_objects::types::table::TableDBWithNames;
use td_objects::types::table_ref::VersionedTableRef;
use td_tower::extractors::Input;

pub async fn vec_create_table_dependency(
    Input(dependencies): Input<Vec<DependencyDBRead>>,
    Input(tables): Input<Vec<TableDBWithNames>>,
) -> Result<Vec<TableDependency>, TdError> {
    let tables = tables
        .iter()
        .map(|t| (t.table_id(), t))
        .collect::<HashMap<_, _>>();

    let table_deps = dependencies
        .iter()
        .map(|d| {
            let table = tables[d.table_id()];
            let versions = &**d.table_versions();
            TableDependency::new(VersionedTableRef::new(
                Some(table.collection().clone()),
                table.name().clone(),
                versions.clone(),
            ))
        })
        .collect::<Vec<_>>();
    Ok(table_deps)
}
