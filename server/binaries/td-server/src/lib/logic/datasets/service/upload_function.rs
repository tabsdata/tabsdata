//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::find_collection_id::find_collection_id;
use crate::logic::datasets::layer::select_dataset_by_id::select_function_by_id;
use crate::logic::datasets::layer::upload_function_authorize::upload_function_authorize;
use crate::logic::datasets::layer::upload_function_to_collection_name::upload_function_to_collection_name;
use crate::logic::datasets::layer::upload_function_update_sql::upload_function_update_sql;
use crate::logic::datasets::layer::upload_function_validate_hash_write_to_storage::upload_function_validate_hash_write_to_storage;
use crate::logic::datasets::layer::upload_function_validate_no_bundle_yet::upload_function_validate_no_bundle_yet;
use std::sync::Arc;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::datasets::dto::*;
use td_storage::Storage;
use td_tower::default_services::{
    ServiceEntry, ServiceReturn, Share, SrvCtxProvider, TransactionProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

/// Service for uploading a function bundle.
///
/// This service must be called, to upload the new function bundle,
/// after the dataset has been created or updated.
pub struct UploadFunctionService {
    provider: ServiceProvider<UploadFunction, (), TdError>,
}

impl UploadFunctionService {
    /// Creates a new instance of [`UploadFunctionService`].
    pub fn new(db: DbPool, storage: Arc<Storage>) -> Self {
        UploadFunctionService {
            provider: Self::provider(db, storage),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        storage: Arc<Storage>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(SrvCtxProvider::new(storage))
            .layer(from_fn(upload_function_to_collection_name))
            .layer(from_fn(find_collection_id))
            .layer(from_fn(select_function_by_id))
            .layer(from_fn(upload_function_authorize))
            .layer(from_fn(upload_function_validate_no_bundle_yet))
            .layer(from_fn(upload_function_validate_hash_write_to_storage))
            .layer(from_fn(upload_function_update_sql))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<UploadFunction, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::datasets::service::upload_function::UploadFunctionService;
    use axum::body::Body;
    use http::Request;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use td_objects::crudl::select_by;
    use td_objects::datasets::dao::DsFunction;
    use td_objects::datasets::dto::UploadFunction;
    use td_objects::rest_urls::FunctionIdParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_storage::{MountDef, SPath, Storage};
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;
    use url::Url;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_upload_function_provider() {
        use crate::logic::datasets::layer::find_collection_id::find_collection_id;
        use crate::logic::datasets::layer::select_dataset_by_id::select_function_by_id;
        use crate::logic::datasets::layer::upload_function_authorize::upload_function_authorize;
        use crate::logic::datasets::layer::upload_function_to_collection_name::upload_function_to_collection_name;
        use crate::logic::datasets::layer::upload_function_update_sql::upload_function_update_sql;
        use crate::logic::datasets::layer::upload_function_validate_hash_write_to_storage::upload_function_validate_hash_write_to_storage;
        use crate::logic::datasets::layer::upload_function_validate_no_bundle_yet::upload_function_validate_no_bundle_yet;
        use crate::logic::datasets::service::upload_function::UploadFunctionService;
        use std::sync::Arc;
        use td_objects::datasets::dto::UploadFunction;
        use td_storage::{MountDef, Storage};
        use td_tower::metadata::{type_of_val, Metadata};

        fn dummy_file() -> String {
            if cfg!(target_os = "windows") {
                "file:///c:/dummy".to_string()
            } else {
                "file:///dummy".to_string()
            }
        }

        let db = td_database::test_utils::db().await.unwrap();
        let mound_def = MountDef::builder()
            .mount_path("/")
            .uri(dummy_file())
            .build()
            .unwrap();
        let storage = Storage::from(vec![mound_def]).await.unwrap();
        let provider = UploadFunctionService::provider(db, Arc::new(storage));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<UploadFunction, ()>(&[
            type_of_val(&upload_function_to_collection_name),
            type_of_val(&find_collection_id),
            type_of_val(&select_function_by_id),
            type_of_val(&upload_function_authorize),
            type_of_val(&upload_function_validate_no_bundle_yet),
            type_of_val(&upload_function_validate_hash_write_to_storage),
            type_of_val(&upload_function_update_sql),
        ]);
    }

    #[tokio::test]
    async fn test_upload_function_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let payload = "TEST";
        let hash = hex::encode(&Sha256::digest(payload.as_bytes())[..]);

        let (_dataset_id, function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            &hash,
        )
        .await;

        let test_dir = testdir!();
        let url = Url::from_directory_path(test_dir).unwrap();
        let storage = Storage::from(vec![MountDef::builder()
            .uri(url)
            .mount_path("/")
            .build()
            .unwrap()])
        .await
        .unwrap();

        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();

        let upload_function =
            UploadFunction::new(FunctionIdParam::new("ds0", "d0", function_id), request);

        let storage = Arc::new(storage);
        let upload_function_service = UploadFunctionService::new(db.clone(), storage.clone());
        let res = upload_function_service
            .service()
            .await
            .raw_oneshot(upload_function)
            .await;
        assert!(res.is_ok());

        const DS_FUNCTION_SELECT_SQL: &str = r#"
            SELECT * FROM ds_functions WHERE id = ?1
        "#;
        let function: DsFunction = select_by(
            &mut db.acquire().await.unwrap(),
            DS_FUNCTION_SELECT_SQL,
            &function_id.to_string(),
        )
        .await
        .unwrap();

        assert!(function.bundle_avail());

        let path = StorageLocation::current()
            .builder(SPath::default())
            .collection(collection_id)
            .dataset(_dataset_id)
            .function(function_id)
            .build()
            .0;
        assert!(storage.exists(&path).await.unwrap());
        let content = String::from_utf8(storage.read(&path).await.unwrap()).unwrap();
        assert_eq!(content, "TEST");
    }
}
