//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_commits_sql::list_commits_sql;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DsTransaction;
use td_objects::datasets::dto::CommitList;
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListCommitsService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<CommitList>, TdError>,
}

impl ListCommitsService {
    /// Creates a new instance of [`ListCommitsService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(list_commits_sql))
            .layer(from_fn(map_list::<(), DsTransaction, CommitList>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<CommitList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, ListResponse, RequestContext};
    use td_objects::datasets::dto::CommitList;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_commit::seed_commit;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_commits() {
        use super::*;
        use crate::logic::datasets::layer::list_commits_sql::list_commits_sql;
        use td_objects::crudl::ListRequest;
        use td_objects::crudl::ListResponse;
        use td_objects::datasets::dao::DsTransaction;
        use td_objects::datasets::dto::CommitList;
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListCommitsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<CommitList>>(&[
            type_of_val(&list_commits_sql),
            type_of_val(&map_list::<(), DsTransaction, CommitList>),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let e0 = seed_execution_plan(&db, "exec_plan_0", &collection_id, &d0, &f0, None).await;
        let (c0, _) = seed_commit(&db, &e0, None).await;
        let e1 = seed_execution_plan(&db, "exec_plan_1", &collection_id, &d0, &f0, None).await;
        let (c1, _) = seed_commit(&db, &e1, None).await;

        let service = ListCommitsService::new(db).service().await;

        let request =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .list((), ListParams::default());
        let response: ListResponse<CommitList> = service.raw_oneshot(request).await.unwrap();
        assert_eq!(
            response
                .data()
                .iter()
                .map(|t| t.id().to_string())
                .collect::<Vec<_>>(),
            vec![c1.to_string(), c0.to_string()]
        );
    }
}
