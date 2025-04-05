//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_execution_plans_sql::list_execution_plans_sql;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DsExecutionPlanWithNames;
use td_objects::datasets::dto::*;
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListExecutionPlansService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<ExecutionPlanList>, TdError>,
}

impl ListExecutionPlansService {
    /// Creates a new instance of [`ListExecutionPlansService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(read_dataset_authorize))
            .layer(from_fn(list_execution_plans_sql))
            .layer(from_fn(
                map_list::<(), DsExecutionPlanWithNames, ExecutionPlanList>,
            ))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<ExecutionPlanList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::service::list_execution_plans::ListExecutionPlansService;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_execution_plans_versions_service() {
        use crate::logic::datasets::layer::list_execution_plans_sql::list_execution_plans_sql;
        use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
        use crate::logic::datasets::service::list_execution_plans::ListExecutionPlansService;
        use td_objects::crudl::ListRequest;
        use td_objects::crudl::ListResponse;
        use td_objects::datasets::dao::DsExecutionPlanWithNames;
        use td_objects::datasets::dto::ExecutionPlanList;
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListExecutionPlansService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<ExecutionPlanList>>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(&list_execution_plans_sql),
            type_of_val(&map_list::<(), DsExecutionPlanWithNames, ExecutionPlanList>),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        seed_collection(&db, None, "ds0").await;

        let service = ListExecutionPlansService::new(db.clone()).service().await;

        let request =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
    }
}
