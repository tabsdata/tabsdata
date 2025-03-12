//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::list_dataset_functions_sql::{
    list_dataset_functions_sql, read_dataset_function_to_list_result,
    read_dataset_functions_dependencies_sql, read_dataset_functions_tables_sql,
    read_dataset_functions_triggers_sql,
};
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::collections::dao::Collection;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dao::DatasetWithNames;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, CollectionName, DatasetName};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_dataset_id, extract_name,
};
use td_objects::tower_service::finder::{find_by_name, find_scoped_by_name};
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct ListDatasetFunctionsService {
    provider: ServiceProvider<ListRequest<FunctionParam>, ListResponse<FunctionList>, TdError>,
}

impl ListDatasetFunctionsService {
    /// Creates a new instance of [`ListDatasetFunctionsService`].
    pub fn new(db: DbPool) -> Self {
        ListDatasetFunctionsService {
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
            .layer(from_fn(list_dataset_functions_sql))
            .layer(from_fn(read_dataset_functions_tables_sql))
            .layer(from_fn(read_dataset_functions_dependencies_sql))
            .layer(from_fn(read_dataset_functions_triggers_sql))
            .layer(from_fn(read_dataset_function_to_list_result))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<FunctionParam>, ListResponse<FunctionList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use td_common::uri::{TdUri, Version, Versions};
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_user::seed_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use td_objects::dlo::CollectionName;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListDatasetFunctionsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<FunctionParam>, ListResponse<FunctionList>>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(&extract_name::<ListRequest<FunctionParam>, FunctionParam, CollectionName>),
            type_of_val(&extract_name::<ListRequest<FunctionParam>, FunctionParam, DatasetName>),
            type_of_val(&find_by_name::<CollectionName, Collection>),
            type_of_val(&extract_collection_id::<Collection>),
            type_of_val(&find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>),
            type_of_val(&extract_dataset_id::<DatasetWithNames>),
            type_of_val(&list_dataset_functions_sql),
            type_of_val(&read_dataset_functions_tables_sql),
            type_of_val(&read_dataset_functions_dependencies_sql),
            type_of_val(&read_dataset_functions_triggers_sql),
            type_of_val(&read_dataset_function_to_list_result),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id0, _function_id) = seed_dataset(
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
        let (dataset_id1, function_id0) = seed_dataset(
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
        let function_id1 = seed_function(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            &dataset_id1,
            "d11",
            &["t11"],
            &[TdUri::new_with_ids(
                collection_id,
                dataset_id0,
                Some("t0".to_string()),
                Some(Versions::Single(Version::Head(0))),
            )],
            &[TdUri::new_with_ids(collection_id, dataset_id0, None, None)],
            "hash",
        )
        .await;

        let service = ListDatasetFunctionsService::new(db).service().await;

        let request = RequestContext::with(&creator_id.to_string(), "r", false)
            .await
            .list(FunctionParam::new("ds0", "d11"), ListParams::default());
        let response: ListResponse<FunctionList> = service.raw_oneshot(request).await.unwrap();
        assert_eq!(*response.len(), 2);
        let functions: HashMap<_, _> = response
            .data()
            .iter()
            .map(|f| (f.id().to_string(), f))
            .collect();
        assert_eq!(
            functions
                .keys()
                .map(ToString::to_string)
                .collect::<HashSet<_>>(),
            [function_id0, function_id1]
                .iter()
                .map(ToString::to_string)
                .collect::<HashSet<_>>()
        );
        let function = functions.get(&function_id1.to_string()).unwrap();
        assert_eq!(function.name(), "d11");
        assert_eq!(function.tables(), &vec!["t11".to_string()]);
        assert_eq!(
            function.dependencies_with_names(),
            &vec!["ds0/t0@HEAD".to_string()]
        );
        assert_eq!(function.trigger_with_names(), &vec!["ds0/d0"]);
    }
}
