//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{DbPool, Global, Grant, retrying};
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_queries::Queries;

pub async fn active_projects(database: &DbPool<Global>) -> Result<Vec<ActiveProjectDB>, TdError> {
    retrying(|attempt| async {
        let mut gated_transaction = database.begin(attempt).await?;
        let connection = gated_transaction.get(Grant::asserted("tdmanager active projects"));
        let projects: Vec<ActiveProjectDB> = connection.fetch_all(()).await?;
        gated_transaction.commit().await?;
        Ok(projects)
    })
    .await
}
