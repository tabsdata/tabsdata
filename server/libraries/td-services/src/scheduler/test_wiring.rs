//
// Copyright 2025 Tabsdata Inc.
//

use crate::Services;
use crate::internal_api::{InternalApi, InternalRoutes};
use std::sync::Arc;
use ta_services::service::Ctx;
use td_database::pool::TdPool;
use td_objects::Route;

/// `Services` for the scheduler tests, carrying the apiserver's real internal
/// route templates so the callbacks and token scopes minted are the ones
/// production mints.
pub(crate) fn dispatch_services(db: &TdPool) -> Services {
    let mut services = Services::with_defaults(db.clone());
    // Field-by-field from the apiserver's served templates; see the
    // export's docs for why the strings, not `InternalRoutes`, cross.
    let routes = InternalRoutes {
        base: td_apiserver::INTERNAL_BASE_URL,
        worker_callback: Route::new(td_apiserver::WORKER_CALLBACK_PATH),
        worker_message: Route::new(td_apiserver::WORKER_MESSAGE_PATH),
        function_run_callback: Route::new(td_apiserver::FUNCTION_RUN_CALLBACK_PATH),
        function_version_execute: Route::new(td_apiserver::FUNCTION_VERSION_EXECUTE_PATH),
        function_version_execution_status: Route::new(
            td_apiserver::FUNCTION_VERSION_EXECUTION_STATUS_PATH,
        ),
        download_function_state: Route::new(td_apiserver::DOWNLOAD_STATE_PATH),
    };
    services.internal_api = Ctx::new(InternalApi::new(
        routes,
        services.jwt_config.clone(),
        Arc::default(),
    ));
    services
}
