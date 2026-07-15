//
// Copyright 2025 Tabs Data Inc.
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
use td_tower::extractors::Ref;

pub async fn vec_create_table_dependency(
    dependencies: Ref<Vec<DependencyDBWithNames>>,
    tables: Ref<Vec<TableDBWithNames>>,
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

pub async fn map_create_worker(
    workers: Ref<Vec<WorkerDBWithNames>>,
) -> Result<HashMap<WorkerTypeDto, WorkerId>, TdError> {
    let workers = workers
        .iter()
        .filter_map(|w| match w.worker_type {
            WorkerType::CronTrigger => Some((WorkerTypeDto::CronTrigger, w.id.clone())),
            WorkerType::StageTrigger => Some((WorkerTypeDto::StageTrigger, w.id.clone())),
            _ => None,
        })
        .collect();
    Ok(workers)
}
