//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::recover::ExecutionRecoverService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, UpdateRequest};
use td_objects::rest_urls::{ExecutionParam, FunctionParam, FunctionRunIdParam};
use td_objects::sql::DaoQueries;
use td_objects::types::execution::{CallbackRequest, ExecutionRequest, ExecutionResponse};
use td_tower::service_provider::TdBoxService;

pub(crate) mod callback;
mod cancel;
pub(crate) mod execute;
mod recover;

pub struct ExecutionServices {
    callback: ExecutionCallbackService,
    cancel_execution: ExecutionCancelService,
    execute: ExecuteFunctionService,
    recover_execution: ExecutionRecoverService,
}

impl ExecutionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            callback: ExecutionCallbackService::new(db.clone()),
            cancel_execution: ExecutionCancelService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            execute: ExecuteFunctionService::new(db.clone(), authz_context.clone()),
            recover_execution: ExecutionRecoverService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
        }
    }

    pub async fn callback(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionRunIdParam, CallbackRequest>, (), TdError> {
        self.callback.service().await
    }

    pub async fn cancel(&self) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.cancel_execution.service().await
    }

    pub async fn execute(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>
    {
        self.execute.service().await
    }

    pub async fn recover(&self) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.recover_execution.service().await
    }
}
