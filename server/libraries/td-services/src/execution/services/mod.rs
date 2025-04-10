//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::execute::ExecuteFunctionService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::rest_urls::FunctionParam;
use td_objects::types::execution::{ExecutionRequest, ExecutionResponse};
use td_tower::service_provider::TdBoxService;

mod execute;

pub struct ExecutionServices {
    execute: ExecuteFunctionService,
}

impl ExecutionServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            execute: ExecuteFunctionService::new(db.clone()),
        }
    }

    pub async fn execute(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>
    {
        self.execute.service().await
    }
}
