//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::list::ExecutionListService;
use crate::execution::services::recover::ExecutionRecoverService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ListRequest, ListResponse, UpdateRequest};
use td_objects::rest_urls::{ExecutionParam, FunctionParam, FunctionRunIdParam};
use td_objects::sql::DaoQueries;
use td_objects::types::execution::{
    CallbackRequest, Execution, ExecutionRequest, ExecutionResponse,
};
use td_tower::service_provider::TdBoxService;
use te_execution::transaction::TransactionBy;

pub(crate) mod callback;
mod cancel;
pub(crate) mod execute;
mod list;
mod recover;

pub struct ExecutionServices {
    callback: ExecutionCallbackService,
    cancel: ExecutionCancelService,
    execute: ExecuteFunctionService,
    list: ExecutionListService,
    recover: ExecutionRecoverService,
}

impl ExecutionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        let transaction_by = Arc::new(TransactionBy::default());
        Self {
            callback: ExecutionCallbackService::new(db.clone()),
            cancel: ExecutionCancelService::new(db.clone(), queries.clone(), authz_context.clone()),
            execute: ExecuteFunctionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
                transaction_by.clone(),
            ),
            list: ExecutionListService::new(db.clone(), queries.clone()),
            recover: ExecutionRecoverService::new(
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
        self.cancel.service().await
    }

    pub async fn execute(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>
    {
        self.execute.service().await
    }

    pub async fn list(&self) -> TdBoxService<ListRequest<()>, ListResponse<Execution>, TdError> {
        self.list.service().await
    }

    pub async fn recover(&self) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.recover.service().await
    }
}
