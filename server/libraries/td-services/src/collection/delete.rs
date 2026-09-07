//
// Copyright 2025 Tabsdata Inc.
//

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::worker::WorkerUpdateDB;
use td_objects::types::basic::{CollectionId, WorkerType};
use td_objects::{and, or};
use td_queries::Queries;
use tracing::debug;

pub async fn update_deleted_workers<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection_id: &CollectionId,
) -> Result<(), TdError> {
    let update = WorkerUpdateDB::stop_scheduled();

    let res = conn
        .update(
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
