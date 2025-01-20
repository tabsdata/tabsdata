//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_tables_sql::list_tables_sql;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::Collection;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DsTableList;
use td_objects::datasets::dto::TableList;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::extractor::{extract_collection_id, extract_name};
use td_objects::tower_service::finder::find_by_name;
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::util::BoxService;
use tower::ServiceBuilder;

pub struct ListTablesService {
    provider: ServiceProvider<ListRequest<CollectionParam>, ListResponse<TableList>, TdError>,
}

impl ListTablesService {
    /// Creates a new instance of [`ListTablesService`].
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
            .layer(from_fn(
                extract_name::<ListRequest<CollectionParam>, CollectionParam, CollectionName>,
            ))
            .layer(from_fn(find_by_name::<CollectionName, Collection>))
            .layer(from_fn(extract_collection_id::<Collection>))
            .layer(from_fn(list_tables_sql))
            .layer(from_fn(map_list::<CollectionParam, DsTableList, TableList>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> BoxService<ListRequest<CollectionParam>, ListResponse<TableList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::service::list_tables::ListTablesService;
    use std::collections::HashSet;
    use td_objects::crudl::ListParams;
    use td_objects::crudl::ListResponse;
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dto::TableList;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_tables() {
        use crate::logic::datasets::layer::list_tables_sql::list_tables_sql;
        use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
        use crate::logic::datasets::service::list_tables::ListTablesService;
        use td_objects::collections::dao::Collection;
        use td_objects::crudl::ListRequest;
        use td_objects::crudl::ListResponse;
        use td_objects::datasets::dao::DsTableList;
        use td_objects::datasets::dto::TableList;
        use td_objects::dlo::CollectionName;
        use td_objects::rest_urls::CollectionParam;
        use td_objects::tower_service::extractor::extract_collection_id;
        use td_objects::tower_service::extractor::extract_name;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::{type_of_val, Metadata};
        use tower::ServiceExt;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListTablesService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<CollectionParam>, ListResponse<TableList>>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(
                &extract_name::<ListRequest<CollectionParam>, CollectionParam, CollectionName>,
            ),
            type_of_val(&find_by_name::<CollectionName, Collection>),
            type_of_val(&extract_collection_id::<Collection>),
            type_of_val(&list_tables_sql),
            type_of_val(&map_list::<CollectionParam, DsTableList, TableList>),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id0 = seed_collection(&db, None, "ds0").await;
        let collection_id1 = seed_collection(&db, None, "ds1").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id0,
            "d0",
            &["t0", "t1"],
            &[],
            &[],
            "hash",
        )
        .await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id1,
            "d1",
            &["t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        let service = ListTablesService::new(db).service().await;

        let request = RequestContext::with(&creator_id.to_string(), "r", false)
            .await
            .list(CollectionParam::new("ds0"), ListParams::default());
        let response: ListResponse<TableList> = service.oneshot(request).await.unwrap();
        assert_eq!(
            response
                .data()
                .iter()
                .map(|t| t.name().to_string())
                .collect::<HashSet<_>>(),
            vec!["t0".to_string(), "t1".to_string()]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }
}
