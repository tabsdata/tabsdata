//
// Copyright 2025 Tabs Data Inc.
//

use crate::system::services::status::StatusService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::types::system::ApiStatus;
use td_tower::service_provider::TdBoxService;

mod status;

pub struct SystemServices {
    status: StatusService,
}

impl SystemServices {
    pub fn new(db: DbPool) -> Self {
        Self {
            status: StatusService::new(db.clone()),
        }
    }

    pub async fn status(&self) -> TdBoxService<(), ApiStatus, TdError> {
        self.status.service().await
    }
}
