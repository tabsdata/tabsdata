//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::function::layers::register::data_location;
use crate::function::layers::upload::upload_function_write_to_storage;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::FunctionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectIdOrNameService};
use td_objects::types::basic::{
    BundleHash, BundleId, CollectionId, CollectionIdName, StorageVersion,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::function::{
    Bundle, BundleBuilder, BundleDB, BundleDBBuilder, FunctionUpload,
};
use td_storage::Storage;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UploadFunctionService {
    provider: ServiceProvider<CreateRequest<FunctionParam, FunctionUpload>, Bundle, TdError>,
}

impl UploadFunctionService {
    pub fn new(db: DbPool, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, storage),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, storage: Arc<Storage>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(storage),
                from_fn(extract_req_context::<CreateRequest<FunctionParam, FunctionUpload >>),
                from_fn(extract_req_dto::<CreateRequest<FunctionParam, FunctionUpload>, _>),
                from_fn(extract_req_name::<CreateRequest<FunctionParam, FunctionUpload>, _>),

                // Extract function (TODO also use FunctionId to generate data_location)
                from_fn(With::<FunctionParam>::extract::<CollectionIdName>),

                TransactionProvider::new(db),

                // Extract collection
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // Get location and storage version.
                from_fn(With::<StorageVersion>::default),
                from_fn(data_location),

                // Write to storage with new bundle id.
                from_fn(With::<BundleId>::default),
                from_fn(upload_function_write_to_storage),

                // Build BundleDB
                from_fn(With::<RequestContext>::convert_to::<BundleDBBuilder, _>),
                from_fn(With::<BundleId>::set::<BundleDBBuilder>),
                from_fn(With::<CollectionId>::set::<BundleDBBuilder>),
                from_fn(With::<BundleHash>::set::<BundleDBBuilder>),
                from_fn(With::<BundleDBBuilder>::build::<BundleDB, _>),
                from_fn(insert::<DaoQueries, BundleDB>),

                // Build response
                from_fn(With::<BundleDB>::convert_to::<BundleBuilder, _>),
                from_fn(With::<BundleBuilder>::build::<Bundle, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, FunctionUpload>, Bundle, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::FunctionUpdate;
    use axum::body::Body;
    use axum::extract::Request;
    use sha2::{Digest, Sha256};
    use td_common::id::Id;
    use td_objects::crudl::handle_sql_err;
    use td_objects::location2::StorageLocation;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, DataLocation, Decorator, FunctionRuntimeValues,
        RoleId, UserId,
    };
    use td_storage::MountDef;
    use td_test::file::mount_uri;
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_upload_function(db: DbPool) -> Result<(), TdError> {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);
        let provider = UploadFunctionService::provider(db, queries, storage);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<FunctionParam, FunctionUpload>, Bundle>(&[
            type_of_val(&extract_req_context::<CreateRequest<FunctionParam, FunctionUpload>>),
            type_of_val(&extract_req_dto::<CreateRequest<FunctionParam, FunctionUpload>, _>),
            type_of_val(&extract_req_name::<CreateRequest<FunctionParam, FunctionUpload>, _>),
            // Extract function (TODO also use FunctionId to generate data_location)
            type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
            // Extract collection
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // Get location and storage version.
            type_of_val(&With::<StorageVersion>::default),
            type_of_val(&data_location),
            // Write to storage with new bundle id.
            type_of_val(&With::<BundleId>::default),
            type_of_val(&upload_function_write_to_storage),
            // Build BundleDB
            type_of_val(&With::<RequestContext>::convert_to::<BundleDBBuilder, _>),
            type_of_val(&With::<BundleId>::set::<BundleDBBuilder>),
            type_of_val(&With::<CollectionId>::set::<BundleDBBuilder>),
            type_of_val(&With::<BundleHash>::set::<BundleDBBuilder>),
            type_of_val(&With::<BundleDBBuilder>::build::<BundleDB, _>),
            type_of_val(&insert::<DaoQueries, BundleDB>),
            // Build response
            type_of_val(&With::<BundleDB>::convert_to::<BundleBuilder, _>),
            type_of_val(&With::<BundleBuilder>::build::<Bundle, _>),
        ]);
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_upload(db: DbPool) -> Result<(), TdError> {
        let admin_id = UserId::from(Id::try_from(&admin_user(&db).await)?);
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &admin_id).await;

        let create = FunctionUpdate::builder()
            .try_name("function_foo")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);

        let payload = "TEXT";
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("function_foo")?
                .build()?,
            function_upload,
        );

        let service = UploadFunctionService::new(db.clone(), storage.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        // Assert db
        let queries = DaoQueries::default();
        let bundle_db: Vec<BundleDB> = queries
            .select_by::<BundleDB>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;

        assert_eq!(bundle_db.len(), 1);
        let bundle = &bundle_db[0];
        assert_eq!(bundle.id(), response.id());
        assert_eq!(bundle.collection_id(), collection.id());
        let hash = hex::encode(&Sha256::digest(payload)[..]);
        assert_eq!(bundle.hash().to_string(), hash);
        assert_eq!(*bundle.created_by_id(), admin_id);

        // Assert storage
        let data_location = DataLocation::default();
        let (bundle_location, _) = StorageLocation::current()
            .builder(&data_location)
            .collection(collection.id())
            .function(bundle.id())
            .build();
        let content = storage.read(&bundle_location).await?;
        let content = String::from_utf8(content).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }
}
