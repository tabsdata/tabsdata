//
// Copyright 2025 Tabsdata Inc.
//

use std::collections::HashMap;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::{WorkerId, WorkerType, WorkerTypeDto};
use td_objects::types::composed::TableDependency;
use td_objects::types::table_ref::VersionedTableRef;

pub async fn vec_create_table_dependency(
    dependencies: &Vec<DependencyDBWithNames>,
    tables: &Vec<TableDBWithNames>,
) -> Result<Vec<TableDependency>, TdError> {
    let tables = tables
        .iter()
        .map(|t| (t.base.table_id, t))
        .collect::<HashMap<_, _>>();

    let table_deps = dependencies
        .iter()
        .filter_map(|d| {
            let table = tables.get(&d.base.table_id)?;
            let versions = d.base.table_versions.deref().clone();
            Some(TableDependency::new(VersionedTableRef::new(
                Some(table.collection.clone()),
                table.base.name.clone(),
                versions,
            )))
        })
        .collect::<Vec<_>>();
    Ok(table_deps)
}

pub async fn map_create_worker(
    workers: &Vec<WorkerDBWithNames>,
) -> Result<HashMap<WorkerTypeDto, WorkerId>, TdError> {
    let workers = workers
        .iter()
        .filter_map(|w| match w.base.worker_type {
            WorkerType::CronTrigger => Some((WorkerTypeDto::CronTrigger, w.base.id.clone())),
            WorkerType::StageTrigger => Some((WorkerTypeDto::StageTrigger, w.base.id.clone())),
            _ => None,
        })
        .collect();
    Ok(workers)
}
