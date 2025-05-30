//
// Copyright 2025 Tabs Data Inc.
//

mod list;
mod logs;

use crate::worker_message::services::list::WorkerMessageListService;
use crate::worker_message::services::logs::WorkerMessageLogService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, ReadRequest};
use td_objects::rest_urls::WorkerMessageParam;
use td_objects::sql::DaoQueries;
use td_objects::types::execution::WorkerMessage;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::service_provider::TdBoxService;

pub struct WorkerMessageServices {
    list: WorkerMessageListService,
    logs: WorkerMessageLogService,
}

impl WorkerMessageServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            list: WorkerMessageListService::new(db.clone(), queries.clone()),
            logs: WorkerMessageLogService::new(db.clone(), queries.clone(), authz_context.clone()),
        }
    }

    pub async fn list(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<WorkerMessage>, TdError> {
        self.list.service().await
    }

    pub async fn logs(
        &self,
    ) -> TdBoxService<ReadRequest<WorkerMessageParam>, BoxedSyncStream, TdError> {
        self.logs.service().await
    }
}
