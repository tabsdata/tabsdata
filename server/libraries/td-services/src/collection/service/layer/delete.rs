//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::worker::{UpdateWorkerDB, WorkerDB};
use td_objects::types::basic::{
    CollectionId, FunctionStatus, FunctionVersionId, TableStatus, TableVersionId, WorkerType,
};
use td_objects::{Buildable, TryUpdater, and, or};
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};
use tracing::debug;

pub async fn build_deleted_functions(
    request_context: Ref<RequestContext>,
    functions: Ref<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let deleted = functions
        .iter()
        .map(|v| {
            v.clone()
                .into_builder()
                .try_update_from(request_context.deref())?
                .id(FunctionVersionId::new())
                .status(FunctionStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}

pub async fn build_deleted_tables(
    request_context: Ref<RequestContext>,
    tables: Ref<Vec<TableDB>>,
) -> Result<Vec<TableDB>, TdError> {
    let deleted = tables
        .iter()
        .map(|v| {
            v.clone()
                .into_builder()
                .try_update_from(request_context.deref())?
                .id(TableVersionId::default())
                .status(TableStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}

pub async fn update_deleted_workers(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    collection_id: Ref<CollectionId>,
) -> Result<(), TdError> {
    let update = UpdateWorkerDB::stop_scheduled().await?;

    let res = conn
        .live()
        .update::<_, WorkerDB>(
            &update,
            and!(
                *collection_id,
                or!(WorkerType::CronTrigger, WorkerType::StageTrigger)
            ),
        )
        .await?;
    debug!(
        "Set stopped status in {} workers for collection {}.",
        res.rows_affected(),
        collection_id
    );

    Ok(())
}
