//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::list::FunctionRunListService;
use crate::function_run::services::read::FunctionRunReadService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, ReadRequest};
use td_objects::rest_urls::FunctionRunParam;
use td_objects::sql::DaoQueries;
use td_objects::types::execution::FunctionRun;
use td_tower::service_provider::TdBoxService;

mod list;
mod read;

pub struct FunctionRunServices {
    list: FunctionRunListService,
    read: FunctionRunReadService,
}

impl FunctionRunServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            list: FunctionRunListService::new(db.clone(), queries.clone()),
            read: FunctionRunReadService::new(db.clone(), queries.clone(), authz_context.clone()),
        }
    }

    pub async fn list(&self) -> TdBoxService<ListRequest<()>, ListResponse<FunctionRun>, TdError> {
        self.list.service().await
    }

    pub async fn read(&self) -> TdBoxService<ReadRequest<FunctionRunParam>, FunctionRun, TdError> {
        self.read.service().await
    }
}
