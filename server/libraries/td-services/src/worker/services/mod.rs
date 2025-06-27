//
// Copyright 2025 Tabs Data Inc.
//

mod list;
mod logs;

use crate::worker::services::list::WorkerListService;
use crate::worker::services::logs::WorkerLogService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, ReadRequest};
use td_objects::rest_urls::WorkerLogsParams;
use td_objects::sql::DaoQueries;
use td_objects::types::execution::Worker;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::service_provider::TdBoxService;

pub struct WorkerServices {
    list: WorkerListService,
    logs: WorkerLogService,
}

impl WorkerServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            list: WorkerListService::new(db.clone(), queries.clone()),
            logs: WorkerLogService::new(db.clone(), queries.clone(), authz_context.clone()),
        }
    }

    pub async fn list(&self) -> TdBoxService<ListRequest<()>, ListResponse<Worker>, TdError> {
        self.list.service().await
    }

    pub async fn logs(
        &self,
    ) -> TdBoxService<ReadRequest<WorkerLogsParams>, BoxedSyncStream, TdError> {
        self.logs.service().await
    }
}
