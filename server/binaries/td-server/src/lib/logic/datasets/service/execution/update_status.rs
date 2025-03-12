//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::data_version_update_request_to_state::data_version_update_request_to_state;
use crate::logic::datasets::layer::event_time::event_time;
use crate::logic::datasets::layer::select_data_version::select_data_version;
use crate::logic::datasets::layer::select_transaction::select_transaction;
use crate::logic::datasets::layer::update_data_version_status::update_data_version_status;
use crate::logic::datasets::layer::update_dependants_status::update_dependants_status;
use crate::logic::datasets::layer::update_publish_status::update_publish_status;
use crate::logic::datasets::layer::update_transaction_status::update_transaction_status;
use crate::logic::datasets::layer::validate_execution_state_action::validate_execution_state_action;
use td_common::execution_status::DataVersionUpdateRequest;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::datasets::dao::DsDataVersion;
use td_objects::dlo::DataVersionId;
use td_objects::tower_service::extractor::{
    extract_req_dto, extract_req_name, extract_transaction_id, to_vec,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use td_tower::{layers, p, service_provider};

pub struct UpdateExecutionStatusService {
    provider: ServiceProvider<UpdateRequest<DataVersionId, DataVersionUpdateRequest>, (), TdError>,
}

impl UpdateExecutionStatusService {
    /// Creates a new instance of [`UpdateExecutionStatusService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db.clone()),
        }
    }

    p! {
        provider(db: DbPool) -> TdError {
            service_provider!(layers!(
                from_fn(event_time),
                from_fn(extract_req_name::<UpdateRequest<DataVersionId, DataVersionUpdateRequest>, DataVersionId>),
                from_fn(extract_req_dto::<UpdateRequest<DataVersionId, DataVersionUpdateRequest>, DataVersionId, DataVersionUpdateRequest>),
                from_fn(validate_execution_state_action),
                from_fn(data_version_update_request_to_state),
                TransactionProvider::new(db),
                from_fn(select_data_version),
                from_fn(extract_transaction_id::<DsDataVersion>),
                from_fn(select_transaction),
                from_fn(to_vec::<DsDataVersion>),
                from_fn(update_data_version_status),
                from_fn(update_transaction_status),
                from_fn(update_dependants_status),
                from_fn(update_publish_status),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<DataVersionId, DataVersionUpdateRequest>, (), TdError> {
        self.provider.make().await
    }
}
