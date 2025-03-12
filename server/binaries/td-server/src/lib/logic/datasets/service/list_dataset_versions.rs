//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_dataset_data_versions_sql::list_dataset_data_versions_sql;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::collections::dao::Collection;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::{DatasetWithNames, DsDataVersion};
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, CollectionName, DatasetName};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_dataset_id, extract_name,
};
use td_objects::tower_service::finder::{find_by_name, find_scoped_by_name};
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListDatasetVersionsService {
    provider: ServiceProvider<ListRequest<FunctionParam>, ListResponse<DataVersionList>, TdError>,
}

impl ListDatasetVersionsService {
    /// Creates a new instance of [`ListDatasetVersionsService`].
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
                extract_name::<ListRequest<FunctionParam>, FunctionParam, CollectionName>,
            ))
            .layer(from_fn(
                extract_name::<ListRequest<FunctionParam>, FunctionParam, DatasetName>,
            ))
            .layer(from_fn(find_by_name::<CollectionName, Collection>))
            .layer(from_fn(extract_collection_id::<Collection>))
            .layer(from_fn(
                find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>,
            ))
            .layer(from_fn(extract_dataset_id::<DatasetWithNames>))
            .layer(from_fn(list_dataset_data_versions_sql))
            .layer(from_fn(
                map_list::<FunctionParam, DsDataVersion, DataVersionList>,
            ))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<FunctionParam>, ListResponse<DataVersionList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::service::list_dataset_versions::ListDatasetVersionsService;
    use std::collections::HashSet;
    use td_common::id;
    use td_objects::crudl::{ListParams, ListResponse, RequestContext};
    use td_objects::datasets::dto::DataVersionList;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use crate::logic::datasets::layer::list_dataset_data_versions_sql::list_dataset_data_versions_sql;
        use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
        use crate::logic::datasets::service::list_dataset_versions::ListDatasetVersionsService;
        use td_objects::collections::dao::Collection;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::datasets::dao::{DatasetWithNames, DsDataVersion};
        use td_objects::datasets::dto::DataVersionList;
        use td_objects::dlo::CollectionName;
        use td_objects::dlo::{CollectionId, DatasetName};
        use td_objects::rest_urls::FunctionParam;
        use td_objects::tower_service::extractor::{
            extract_collection_id, extract_dataset_id, extract_name,
        };
        use td_objects::tower_service::finder::{find_by_name, find_scoped_by_name};
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListDatasetVersionsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<FunctionParam>, ListResponse<DataVersionList>>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(&extract_name::<ListRequest<FunctionParam>, FunctionParam, CollectionName>),
            type_of_val(&extract_name::<ListRequest<FunctionParam>, FunctionParam, DatasetName>),
            type_of_val(&find_by_name::<CollectionName, Collection>),
            type_of_val(&extract_collection_id::<Collection>),
            type_of_val(&find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>),
            type_of_val(&extract_dataset_id::<DatasetWithNames>),
            type_of_val(&list_dataset_data_versions_sql),
            type_of_val(&map_list::<FunctionParam, DsDataVersion, DataVersionList>),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id0, function_id0) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let (dataset_id1, function_id1) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;
        let execution_plan_id0 = id::id();
        let dv0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id0,
            &function_id0,
            &execution_plan_id0,
            &execution_plan_id0,
            "M",
            "D",
        )
        .await;
        let execution_plan_id1 = id::id();
        let _dv1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &execution_plan_id1,
            &execution_plan_id1,
            "D",
            "S",
        )
        .await;
        let execution_plan_id2 = id::id();
        let dv2 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id0,
            &function_id0,
            &execution_plan_id2,
            &execution_plan_id2,
            "M",
            "S",
        )
        .await;

        let service = ListDatasetVersionsService::new(db.clone()).service().await;

        let request = RequestContext::with(&creator_id.to_string(), "r", false)
            .await
            .list(FunctionParam::new("ds0", "d0"), ListParams::default());
        let response: ListResponse<DataVersionList> = service.raw_oneshot(request).await.unwrap();
        assert_eq!(*response.len(), 2);
        let versions = response
            .data()
            .iter()
            .map(|v| v.id().clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            versions,
            [dv0.to_string(), dv2.to_string()].iter().cloned().collect()
        );
    }
}
