//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::services::callback::ExecutionCallbackService;
use crate::execution::services::execute::ExecuteFunctionService;
use crate::execution::services::schedule_commit::ScheduleCommitService;
use crate::execution::services::schedule_request::ScheduleRequestService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, UpdateRequest};
use td_objects::rest_urls::{FunctionParam, FunctionRunParam};
use td_objects::types::execution::{CallbackRequest, ExecutionRequest, ExecutionResponse};
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

mod callback;
mod execute;
mod schedule_commit;
mod schedule_request;
mod cancel_transaction;
mod cancel_execution;

pub struct ExecutionServices {
    execute: ExecuteFunctionService,
    callback: ExecutionCallbackService,
}

impl ExecutionServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            execute: ExecuteFunctionService::new(db.clone()),
            callback: ExecutionCallbackService::new(db.clone()),
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
