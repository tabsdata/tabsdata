//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::find_data_version_info::find_data_version_info;
use crate::logic::datasets::layer::find_table_dataset_id::find_table_dataset_id;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use crate::logic::datasets::layer::resolve_table_location::resolve_table_location;
use crate::logic::datasets::layer::verify_table_exists::verify_table_exists;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::dlo::{CollectionName, TableName};
use td_objects::rest_urls::{At, TableCommitParam};
use td_objects::tower_service::extractor::extract_name;
use td_storage::SPath;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers};
use tower::ServiceBuilder;

pub struct DataService {
    provider: ServiceProvider<ReadRequest<TableCommitParam>, SPath, TdError>,
}

impl DataService {
    /// Creates a new instance of [`DataService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(Self::table_data())
            .map_err(TdError::from) // TODO make this disappear, type conversion should be implicit
            .service(ServiceReturn)
            .into_service_provider()
    }

    l! {
        table_data() -> TdError {
            layers!(
                from_fn(read_dataset_authorize),
                from_fn(extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, CollectionName>),
                from_fn(extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, TableName>),
                from_fn(extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, At>),
                from_fn(find_table_dataset_id),
                from_fn(find_data_version_info),
                from_fn(verify_table_exists),
                from_fn(resolve_table_location),
            )
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<TableCommitParam>, SPath, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::error::DatasetError;
    use crate::logic::datasets::service::data::DataService;
    use chrono::{DateTime, Utc};
    use td_common::id;
    use td_common::time::UniqueUtc;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::DATE_TIME_FORMAT;
    use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_storage::location::StorageLocation;
    use td_storage::SPath;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_data_service() {
        use crate::logic::datasets::layer::find_data_version_info::find_data_version_info;
        use crate::logic::datasets::layer::find_table_dataset_id::find_table_dataset_id;
        use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
        use crate::logic::datasets::layer::resolve_table_location::resolve_table_location;
        use crate::logic::datasets::layer::verify_table_exists::verify_table_exists;
        use crate::logic::datasets::service::data::DataService;
        use td_objects::crudl::ReadRequest;
        use td_objects::dlo::{CollectionName, TableName};
        use td_objects::rest_urls::At;
        use td_objects::tower_service::extractor::extract_name;
        use td_storage::SPath;
        use td_tower::metadata::type_of_val;
        use td_tower::metadata::Metadata;
        let db = td_database::test_utils::db().await.unwrap();
        let provider = DataService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<TableCommitParam>, SPath>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(
                &extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, CollectionName>,
            ),
            type_of_val(
                &extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, TableName>,
            ),
            type_of_val(&extract_name::<ReadRequest<TableCommitParam>, TableCommitParam, At>),
            type_of_val(&find_table_dataset_id),
            type_of_val(&find_data_version_info),
            type_of_val(&verify_table_exists),
            type_of_val(&resolve_table_location),
        ]);
    }

    async fn test_data_version(use_fixed: bool) {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, Some(creator_id.to_string()), "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
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
        let _data_version0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let data_version1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let _data_version2 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;

        let version = if use_fixed {
            data_version1.to_string()
        } else {
            "HEAD^".to_string()
        };

        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id.to_string())
            .function(function_id.to_string())
            .version(data_version1.to_string())
            .table("t0".to_string())
            .build();

        let service = DataService::new(db.clone()).service().await;

        let request =
            RequestContext::with(AccessTokenId::default(), creator_id, RoleId::user(), false).read(
                TableCommitParam::new(
                    &TableParam::new("ds0".to_string(), "t0".to_string()),
                    &AtParam::version(Some(version)),
                )
                .unwrap(),
            );
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        assert_eq!(response.unwrap(), path);
    }

    #[tokio::test]
    async fn test_data_fixed_version() {
        test_data_version(true).await;
    }

    #[tokio::test]
    async fn test_data_relative_version() {
        test_data_version(false).await;
    }

    #[tokio::test]
    async fn test_data_at_commit() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, Some(creator_id.to_string()), "ds0").await;

        let (dataset_idx, function_idx) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "dx",
            &["tx"],
            &[],
            &[],
            "hash",
        )
        .await;

        // dataset we are testing
        let (dataset_id1, function_id1) = seed_dataset(
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

        // trx prior to the 1st version of the dataset we are testing
        let trx0 = id::id();
        let _data_version0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &trx0,
            &id::id(),
            "M",
            "P",
        )
        .await;
        // 1st version of the dataset we are testing
        let trx1 = id::id();
        let data_version1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &trx1,
            &id::id(),
            "M",
            "P",
        )
        .await;
        // another dataset part of the same trx
        let _data_version2 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &trx1,
            &id::id(),
            "M",
            "P",
        )
        .await;
        // another dataset part of a different trx
        let trx2 = id::id();
        let _data_version3 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &trx2,
            &id::id(),
            "M",
            "P",
        )
        .await;
        // 3rd version of the dataset we are testing
        let trx3 = id::id();
        let data_version4 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &trx3,
            &id::id(),
            "M",
            "P",
        )
        .await;
        // 4th version of the dataset we are testing, not published yet
        let trx4 = id::id();
        let _data_version5 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &trx4,
            &id::id(),
            "M",
            "D",
        )
        .await;

        // at trx0 dataset version should none
        // at trx1 dataset version should be data_version1
        // at trx2 dataset version should be data_version1
        // at trx3 dataset version should be data_version4
        // at trx4 there is no commit_id

        let response = get_data_path_for_commit(&db, &trx0.to_string()).await;
        assert!(response.is_err());
        assert!(matches!(
            response.unwrap_err().domain_err(),
            DatasetError::TableHasNoDataAtCommit(_)
        ));

        let response = get_data_path_for_commit(&db, &trx1.to_string()).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version1.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);

        let response = get_data_path_for_commit(&db, &trx2.to_string()).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version1.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);

        let response = get_data_path_for_commit(&db, &trx3.to_string()).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version4.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);
    }

    async fn get_data_path_for_commit(db: &DbPool, trx: &str) -> Result<SPath, TdError> {
        const COMMIT_ID_FOR_TRX: &str = r#"
            SELECT commit_id
            FROM ds_data_versions
            WHERE transaction_id = ?1
        "#;

        let commit_id: String = sqlx::query_scalar(COMMIT_ID_FOR_TRX)
            .bind(trx.to_string())
            .fetch_one(&mut *db.acquire().await.unwrap())
            .await
            .unwrap();

        let service = DataService::new(db.clone()).service().await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            false,
        )
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t0".to_string()),
                &AtParam::commit(commit_id),
            )
            .unwrap(),
        );
        service.raw_oneshot(request).await
    }

    #[tokio::test]
    async fn test_data_at_time() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, Some(creator_id.to_string()), "ds0").await;

        let (dataset_idx, function_idx) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "dx",
            &["tx"],
            &[],
            &[],
            "hash",
        )
        .await;

        // dataset we are testing
        let (dataset_id1, function_id1) = seed_dataset(
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

        // trx prior to the 1st version of the dataset we are testing
        let _data_version0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let trx0_time = UniqueUtc::now_millis();

        // 1st version of the dataset we are testing
        let data_version1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let trx1_time = UniqueUtc::now_millis();
        // another dataset part of the same trx
        let _data_version2 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        // another dataset part of a different trx
        let _data_version3 = seed_data_version(
            &db,
            &collection_id,
            &dataset_idx,
            &function_idx,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let trx2_time = UniqueUtc::now_millis();
        // 3rd version of the dataset we are testing
        let data_version4 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &id::id(),
            &id::id(),
            "M",
            "P",
        )
        .await;
        let trx3_time = UniqueUtc::now_millis();
        // 4th version of the dataset we are testing, not published yet
        let _data_version5 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id1,
            &function_id1,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;
        let trx4_time = UniqueUtc::now_millis();

        // Note that these times are after the data version is seeded:
        // at trx0_time dataset version should be none
        // at trx1_time dataset version should be data_version1
        // at trx2_time dataset version should be data_version1
        // at trx3_time dataset version should be data_version4
        // at trx4_time dataset version should be data_version4 (5 is not yet published)

        let response = get_data_path_for_time(&db, &trx0_time).await;
        assert!(response.is_err());
        assert!(matches!(
            response.unwrap_err().domain_err(),
            DatasetError::TableHasNoDataAtTime(_)
        ));

        let response = get_data_path_for_time(&db, &trx1_time).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version1.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);

        let response = get_data_path_for_time(&db, &trx2_time).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version1.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);

        let response = get_data_path_for_time(&db, &trx3_time).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version4.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);

        let response = get_data_path_for_time(&db, &trx4_time).await;
        assert!(response.is_ok());
        let (path, _) = StorageLocation::V1
            .builder(SPath::default())
            .collection(collection_id.to_string())
            .dataset(dataset_id1.to_string())
            .function(function_id1.to_string())
            .version(data_version4.to_string())
            .table("t0".to_string())
            .build();
        assert_eq!(response.unwrap(), path);
    }

    async fn get_data_path_for_time(db: &DbPool, time: &DateTime<Utc>) -> Result<SPath, TdError> {
        let service = DataService::new(db.clone()).service().await;

        let time = time.format(DATE_TIME_FORMAT).to_string();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            false,
        )
        .read(
            TableCommitParam::new(
                &TableParam::new("ds0".to_string(), "t0".to_string()),
                &AtParam::time(time),
            )
            .unwrap(),
        );
        service.raw_oneshot(request).await
    }
}
