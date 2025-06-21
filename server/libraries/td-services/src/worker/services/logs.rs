//
// Copyright 2025 Tabs Data Inc.
//

use crate::worker::layers::logs::{get_worker_logs, resolve_worker_log_path};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::WorkerMessageParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{CollectionId, WorkerIdName};
use td_objects::types::execution::WorkerDB;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = WorkerLogService,
    request = ReadRequest<WorkerMessageParam>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<WorkerMessageParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<WorkerMessageParam>>::extract_name::<WorkerMessageParam>),
        // find collection ID
        from_fn(With::<WorkerMessageParam>::extract::<WorkerIdName>),
        from_fn(By::<WorkerIdName>::select::<DaoQueries, WorkerDB>),
        from_fn(With::<WorkerDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Resolve worker message path.
        from_fn(resolve_worker_log_path),
        // Get worker message logs.
        from_fn(get_worker_logs),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_workers_logs(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let provider = WorkerLogService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<WorkerMessageParam>, BoxedSyncStream>(&[
            // Extract parameters
            type_of_val(&With::<ReadRequest<WorkerMessageParam>>::extract::<RequestContext>),
            type_of_val(
                &With::<ReadRequest<WorkerMessageParam>>::extract_name::<WorkerMessageParam>,
            ),
            // find collection ID
            type_of_val(&With::<WorkerMessageParam>::extract::<WorkerIdName>),
            type_of_val(&By::<WorkerIdName>::select::<DaoQueries, WorkerDB>),
            type_of_val(&With::<WorkerDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
            // Resolve worker message path.
            type_of_val(&resolve_worker_log_path),
            // Get worker message logs.
            type_of_val(&get_worker_logs),
        ]);
    }

    // TODO: tests using WORKSPACE_ENV won't work.
    // This test just asserts that the service can be created and called, as it has to compile.
    #[ignore]
    #[td_test::test(sqlx)]
    async fn test_read_workers_logs(db: DbPool) -> Result<(), TdError> {
        let service = WorkerLogService::new(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(WorkerMessageParam::builder().try_worker("")?.build()?);

        let _ = service.raw_oneshot(request).await?;
        Ok(())
    }
}
