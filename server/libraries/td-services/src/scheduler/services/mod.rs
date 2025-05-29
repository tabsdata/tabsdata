//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::commit::ScheduleCommitService;
use crate::scheduler::services::request::ScheduleRequestService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::WorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

mod commit;
mod request;

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
