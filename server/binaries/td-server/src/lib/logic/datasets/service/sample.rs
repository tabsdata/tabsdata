//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::find_data_version_info::find_data_version_info;
use crate::logic::datasets::layer::find_table_dataset_id::find_table_dataset_id;
use crate::logic::datasets::layer::get_table_sample::get_table_sample;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use crate::logic::datasets::layer::resolve_table_location::resolve_table_location;
use crate::logic::datasets::layer::verify_table_exists::verify_table_exists;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ListRequest;
use td_objects::datasets::dlo::BoxedSyncStream;
use td_objects::dlo::{CollectionName, TableName};
use td_objects::rest_urls::{At, TableCommitParam};
use td_objects::tower_service::extractor::{extract_list_params, extract_name};
use td_storage::Storage;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{
    ConnectionProvider, ServiceEntry, ServiceReturn, Share, SrvCtxProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers};
use tower::ServiceBuilder;

pub struct SampleService {
    provider: ServiceProvider<ListRequest<TableCommitParam>, BoxedSyncStream, TdError>,
}

impl SampleService {
    /// Creates a new instance of [`SampleService`].
    pub fn new(db: DbPool, storage: Arc<Storage>) -> Self {
        Self {
            provider: Self::provider(db, storage),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        storage: Arc<Storage>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(SrvCtxProvider::new(storage))
            .layer(Self::table_sample())
            .map_err(TdError::from) // TODO make this disappear, type conversion should be implicit
            .service(ServiceReturn)
            .into_service_provider()
    }

    l! {
        table_sample() -> TdError {
            layers!(
                from_fn(read_dataset_authorize),
                from_fn(extract_name::<ListRequest<TableCommitParam>, TableCommitParam, CollectionName>),
                from_fn(extract_name::<ListRequest<TableCommitParam>, TableCommitParam, TableName>),
                from_fn(extract_name::<ListRequest<TableCommitParam>, TableCommitParam, At>),
                from_fn(find_table_dataset_id),
                from_fn(find_data_version_info),
                from_fn(verify_table_exists),
                from_fn(resolve_table_location),
                from_fn(extract_list_params::<ListRequest<TableCommitParam>>),
                from_fn(get_table_sample),
            )
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<TableCommitParam>, BoxedSyncStream, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::datasets::service::sample::SampleService;
    use futures_util::StreamExt;
    use polars::datatypes::{Int64Chunked, StringChunked};
    use polars::prelude::SerReader;
    use polars::prelude::{
        DataFrame, IntoColumn, IntoLazy, NamedFrom, ParquetReader, ParquetWriteOptions,
    };
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::Arc;
    use td_common::absolute_path::AbsolutePath;
    use td_common::id;
    use td_objects::crudl::{ListParamsBuilder, RequestContext};
    use td_objects::rest_urls::{AtParam, TableCommitParam, TableParam};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, SPath, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_sample_service() {
        use crate::logic::datasets::layer::find_data_version_info::find_data_version_info;
        use crate::logic::datasets::layer::find_table_dataset_id::find_table_dataset_id;
        use crate::logic::datasets::layer::get_table_sample::get_table_sample;
        use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
        use crate::logic::datasets::layer::resolve_table_location::resolve_table_location;
        use crate::logic::datasets::layer::verify_table_exists::verify_table_exists;
        use td_objects::crudl::ListRequest;
        use td_objects::datasets::dlo::BoxedSyncStream;
        use td_objects::dlo::{CollectionName, TableName};
        use td_objects::rest_urls::At;
        use td_objects::tower_service::extractor::extract_list_params;
        use td_objects::tower_service::extractor::extract_name;
        use td_tower::metadata::type_of_val;
        use td_tower::metadata::Metadata;

        fn dummy_file() -> String {
            if cfg!(target_os = "windows") {
                "file:///c:\\dummy".to_string()
            } else {
                "file:///dummy".to_string()
            }
        }

        let db = td_database::test_utils::db().await.unwrap();
        let mound_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(dummy_file())
            .build()
            .unwrap();
        let storage = Storage::from(vec![mound_def]).await.unwrap();
        let provider = SampleService::provider(db, Arc::new(storage));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<TableCommitParam>, BoxedSyncStream>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(
                &extract_name::<ListRequest<TableCommitParam>, TableCommitParam, CollectionName>,
            ),
            type_of_val(
                &extract_name::<ListRequest<TableCommitParam>, TableCommitParam, TableName>,
            ),
            type_of_val(&extract_name::<ListRequest<TableCommitParam>, TableCommitParam, At>),
            type_of_val(&find_table_dataset_id),
            type_of_val(&find_data_version_info),
            type_of_val(&verify_table_exists),
            type_of_val(&resolve_table_location),
            type_of_val(&extract_list_params::<ListRequest<TableCommitParam>>),
            type_of_val(&get_table_sample),
        ]);
    }

    async fn test_sample(use_fixed: bool) {
        let db = td_database::test_utils::db().await.unwrap();
        let test_dir = testdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![MountDef::builder()
            .id("id")
            .uri(url)
            .mount_path("/")
            .build()
            .unwrap()])
        .await
        .unwrap();
        let storage = Arc::new(storage);

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
        let url = storage.to_external_uri(&path).unwrap().0;
        let path = url.abs_path();
        std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        tokio::task::block_in_place(move || {
            let a = Int64Chunked::new("i".into(), &[1, 2, 3, 4, 5]).into_column();
            let b = StringChunked::new("s".into(), &["a", "b", "c", "d", "e"]).into_column();
            let lf = DataFrame::new(vec![a, b]).unwrap().lazy();
            lf.sink_parquet(path, ParquetWriteOptions::default())
                .unwrap();
        });

        let service = SampleService::new(db.clone(), storage).service().await;

        let request = RequestContext::with(&creator_id.to_string(), "r", false)
            .await
            .list(
                TableCommitParam::new(
                    &TableParam::new("ds0".to_string(), "t0".to_string()),
                    &AtParam::version(Some(version)),
                )
                .unwrap(),
                ListParamsBuilder::default()
                    .offset(1usize)
                    .len(2usize)
                    .build()
                    .unwrap(),
            );
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let mut data = response.unwrap().into_inner();
        let bytes = data.next().await.unwrap().unwrap();
        let df = ParquetReader::new(Cursor::new(&bytes)).finish().unwrap();
        println!("{:?}", df);
        assert_eq!(df.height(), 2);
        assert_eq!(df.get_columns().len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_fixed_version() {
        test_sample(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sample_relative_version() {
        test_sample(false).await;
    }
}
