//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel::ExecutionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::list::ExecutionListService;
use crate::execution::services::read::ExecutionReadService;
use crate::execution::services::recover::ExecutionRecoverService;
use crate::execution::services::runtime_info::RuntimeInfoService;
use crate::execution::RuntimeContext;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest};
use td_objects::rest_urls::{ExecutionParam, FunctionParam, FunctionRunIdParam};
use td_objects::sql::DaoQueries;
use td_objects::types::execution::{
    CallbackRequest, Execution, ExecutionRequest, ExecutionResponse,
};
use td_objects::types::runtime_info::RuntimeInfo;
use td_tower::service_provider::TdBoxService;
use te_execution::transaction::TransactionBy;

pub(crate) mod callback;
mod cancel;
pub(crate) mod execute;
mod list;
mod read;
mod recover;
pub mod runtime_info;

pub struct ExecutionServices {
    callback: ExecutionCallbackService,
    cancel: ExecutionCancelService,
    execute: ExecuteFunctionService,
    list: ExecutionListService,
    read: ExecutionReadService,
    recover: ExecutionRecoverService,
    info: RuntimeInfoService,
}

impl ExecutionServices {
    pub fn new(
        db: DbPool,
        authz_context: Arc<AuthzContext>,
        runtime_context: Arc<RuntimeContext>,
    ) -> Self {
        let queries = Arc::new(DaoQueries::default());
        let transaction_by = Arc::new(TransactionBy::default());
        Self {
            callback: ExecutionCallbackService::new(db.clone(), queries.clone()),
            cancel: ExecutionCancelService::new(db.clone(), queries.clone(), authz_context.clone()),
            execute: ExecuteFunctionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
                transaction_by.clone(),
            ),
            list: ExecutionListService::new(db.clone(), queries.clone()),
            read: ExecutionReadService::new(db.clone(), queries.clone(), transaction_by.clone()),
            recover: ExecutionRecoverService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            info: RuntimeInfoService::new(runtime_context),
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

    pub async fn read(
        &self,
    ) -> TdBoxService<ReadRequest<ExecutionParam>, ExecutionResponse, TdError> {
        self.read.service().await
    }

    pub async fn recover(&self) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.recover.service().await
    }

    pub async fn info(&self) -> TdBoxService<ReadRequest<()>, RuntimeInfo, TdError> {
        self.info.service().await
    }
}
