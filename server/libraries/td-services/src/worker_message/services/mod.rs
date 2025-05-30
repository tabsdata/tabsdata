//
// Copyright 2025 Tabs Data Inc.
//

pub mod list;

use crate::worker_message::services::list::WorkerMessageListService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::rest_urls::TransactionParam;
use td_objects::sql::DaoQueries;
use td_objects::types::execution::WorkerMessage;
use td_tower::service_provider::TdBoxService;

pub struct WorkerMessageServices {
    list: WorkerMessageListService,
}

impl WorkerMessageServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            list: WorkerMessageListService::new(db.clone(), queries.clone(), authz_context.clone()),
        }
    }

    pub async fn list(
        &self,
    ) -> TdBoxService<ListRequest<TransactionParam>, ListResponse<WorkerMessage>, TdError> {
        self.list.service().await
    }
}
