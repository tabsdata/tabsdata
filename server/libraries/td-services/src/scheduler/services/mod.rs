//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::services::commit::ScheduleCommitService;
use crate::scheduler::services::request::ScheduleRequestService;
use std::net::SocketAddr;
use std::sync::Arc;
use td_common::server::FileWorkerMessageQueue;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

mod commit;
mod request;

pub struct SchedulerServices {
    schedule_request: ScheduleRequestService,
    schedule_commit: ScheduleCommitService,
}

impl SchedulerServices {
    pub fn new(
        db: DbPool,
        storage: Arc<Storage>,
        message_queue: Arc<FileWorkerMessageQueue>,
        server_url: Arc<SocketAddr>,
    ) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            schedule_request: ScheduleRequestService::new(
                db.clone(),
                queries.clone(),
                storage.clone(),
                message_queue.clone(),
                server_url.clone(),
            ),
            schedule_commit: ScheduleCommitService::new(
                db.clone(),
                queries.clone(),
                message_queue.clone(),
            ),
        }
    }

    pub async fn request(&self) -> TdBoxService<(), (), TdError> {
        self.schedule_request.service().await
    }

    pub async fn commit(&self) -> TdBoxService<(), (), TdError> {
        self.schedule_commit.service().await
    }
}
