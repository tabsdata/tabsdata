//
// Copyright 2025 Tabs Data Inc.
//

pub mod cancel;
pub mod recover;
pub mod synchrotron;

use crate::transaction::services::cancel::TransactionCancelService;
use crate::transaction::services::recover::TransactionRecoverService;
use crate::transaction::services::synchrotron::SynchrotronService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, UpdateRequest};
use td_objects::rest_urls::TransactionParam;
use td_objects::sql::DaoQueries;
use td_objects::types::execution::SynchrotronResponse;
use td_tower::service_provider::TdBoxService;

pub struct TransactionServices {
    cancel: TransactionCancelService,
    recover: TransactionRecoverService,
    synchrotron: SynchrotronService,
}

impl TransactionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            cancel: TransactionCancelService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            recover: TransactionRecoverService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            synchrotron: SynchrotronService::new(db.clone(), queries.clone()),
        }
    }

    pub async fn cancel(&self) -> TdBoxService<UpdateRequest<TransactionParam, ()>, (), TdError> {
        self.cancel.service().await
    }

    pub async fn recover(&self) -> TdBoxService<UpdateRequest<TransactionParam, ()>, (), TdError> {
        self.recover.service().await
    }

    pub async fn synchrotron(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<SynchrotronResponse>, TdError> {
        self.synchrotron.service().await
    }
}
