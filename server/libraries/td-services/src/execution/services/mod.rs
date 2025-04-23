//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::cancel_execution::ExecutionCancelService;
use crate::execution::services::cancel_transaction::TransactionCancelService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::schedule_commit::ScheduleCommitService;
use crate::execution::services::schedule_request::ScheduleRequestService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, UpdateRequest};
use td_objects::rest_urls::{ExecutionParam, FunctionParam, FunctionRunParam, TransactionParam};
use td_objects::types::execution::{CallbackRequest, ExecutionRequest, ExecutionResponse};
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

mod callback;
mod cancel_execution;
mod cancel_transaction;
mod execute;
mod schedule_commit;
mod schedule_request;

pub struct ExecutionServices {
    execute: ExecuteFunctionService,
    callback: ExecutionCallbackService,
    cancel_transaction: TransactionCancelService,
    cancel_execution: ExecutionCancelService,
}

#[allow(dead_code)] // TODO remove
impl ExecutionServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            execute: ExecuteFunctionService::new(db.clone()),
            callback: ExecutionCallbackService::new(db.clone()),
            cancel_transaction: TransactionCancelService::new(db.clone()),
            cancel_execution: ExecutionCancelService::new(db.clone()),
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
    ) -> TdBoxService<UpdateRequest<FunctionRunParam, CallbackRequest>, (), TdError> {
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
