//
// Copyright 2025 Tabsdata Inc.
//

use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::worker::{UpdateWorkerDB, WorkerDB};
use td_objects::types::basic::{CollectionId, WorkerType};
use td_objects::{and, or};
use td_queries::LiveEntry;
use tracing::debug;

pub async fn update_deleted_workers(
    conn: &mut DomainConnection<Apiserver>,
    collection_id: &CollectionId,
) -> Result<(), TdError> {
    let update = UpdateWorkerDB::stop_scheduled();

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
