//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel_execution::ExecutionCancelService;
use crate::execution::services::cancel_transaction::TransactionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::read_function_run::FunctionRunReadService;
use crate::execution::services::recover_execution::ExecutionRecoverService;
use crate::execution::services::recover_transaction::TransactionRecoverService;
use crate::execution::services::schedule_commit::ScheduleCommitService;
use crate::execution::services::schedule_request::ScheduleRequestService;
use crate::execution::services::synchrotron::SynchrotronService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest};
use td_objects::rest_urls::{
    ExecutionParam, FunctionParam, FunctionRunIdParam, FunctionRunParam, TransactionParam,
};
use td_objects::sql::DaoQueries;
use td_objects::types::execution::{
    CallbackRequest, ExecutionRequest, ExecutionResponse, FunctionRun, SynchrotronResponse,
};
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

mod callback;
mod cancel_execution;
mod cancel_transaction;
mod execute;
mod read_function_run;
mod recover_execution;
mod recover_transaction;
mod schedule_commit;
mod schedule_request;
mod synchrotron;

pub struct ExecutionServices {
    execute: ExecuteFunctionService,
    callback: ExecutionCallbackService,
    cancel_transaction: TransactionCancelService,
    cancel_execution: ExecutionCancelService,
    read_function_run: FunctionRunReadService,
    recover_execution: ExecutionRecoverService,
    recover_transaction: TransactionRecoverService,
    synchrotron: SynchrotronService,
}

impl ExecutionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            execute: ExecuteFunctionService::new(db.clone(), authz_context.clone()),
            callback: ExecutionCallbackService::new(db.clone()),
            cancel_transaction: TransactionCancelService::new(db.clone(), authz_context.clone()),
            cancel_execution: ExecutionCancelService::new(db.clone(), authz_context.clone()),
            read_function_run: FunctionRunReadService::new(db.clone(), authz_context.clone()),
            recover_execution: ExecutionRecoverService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            recover_transaction: TransactionRecoverService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            synchrotron: SynchrotronService::new(db.clone(), queries.clone()),
        }
    }

    pub async fn execute(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse, TdError>
    {
        self.execute.service().await
    }

    pub async fn callback(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionRunIdParam, CallbackRequest>, (), TdError> {
        self.callback.service().await
    }

    pub async fn cancel_transaction(
        &self,
    ) -> TdBoxService<UpdateRequest<TransactionParam, ()>, (), TdError> {
        self.cancel_transaction.service().await
    }

    pub async fn cancel_execution(
        &self,
    ) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.cancel_execution.service().await
    }

    pub async fn read_function_run(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionRunParam>, FunctionRun, TdError> {
        self.read_function_run.service().await
    }

    pub async fn recover_execution(
        &self,
    ) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.recover_execution.service().await
    }

    pub async fn recover_transaction(
        &self,
    ) -> TdBoxService<UpdateRequest<TransactionParam, ()>, (), TdError> {
        self.recover_transaction.service().await
    }
    pub async fn synchrotron(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<SynchrotronResponse>, TdError> {
        self.synchrotron.service().await
    }
}

pub struct SchedulerServices<T> {
    schedule_request: ScheduleRequestService<T>,
    schedule_commit: ScheduleCommitService<T>,
}

impl<T: WorkerMessageQueue> SchedulerServices<T> {
    pub fn new(
        db: DbPool,
        storage: Arc<Storage>,
        message_queue: Arc<T>,
        server_url: Arc<SocketAddr>,
    ) -> Self {
        Self {
            schedule_request: ScheduleRequestService::new(
                db.clone(),
                storage.clone(),
                message_queue.clone(),
                server_url.clone(),
            ),
            schedule_commit: ScheduleCommitService::new(db.clone(), message_queue.clone()),
        }
    }

    pub async fn request(&self) -> TdBoxService<(), (), TdError> {
        self.schedule_request.service().await
    }

    pub async fn commit(&self) -> TdBoxService<(), (), TdError> {
        self.schedule_commit.service().await
    }
}
