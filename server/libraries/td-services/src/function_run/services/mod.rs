//
// Copyright 2025 Tabs Data Inc.
//

use crate::function_run::services::read::FunctionRunReadService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::FunctionRunParam;
use td_objects::types::execution::FunctionRun;
use td_tower::service_provider::TdBoxService;

mod read;

pub struct FunctionRunServices {
    read: FunctionRunReadService,
}

impl FunctionRunServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        Self {
            read: FunctionRunReadService::new(db.clone(), authz_context.clone()),
        }
    }

    pub async fn read(&self) -> TdBoxService<ReadRequest<FunctionRunParam>, FunctionRun, TdError> {
        self.read.service().await
    }
}
