//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::data_location;
use crate::function::layers::upload::upload_function_write_to_storage;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::CreateRequest;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractDataService, ExtractNameService, ExtractService,
    SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::{
    BundleHash, BundleId, CollectionId, CollectionIdName, StorageVersion,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::function::{
    Bundle, BundleBuilder, BundleDB, BundleDBBuilder, FunctionUpload,
};
use td_storage::Storage;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UploadFunctionService {
    provider: ServiceProvider<CreateRequest<CollectionParam, FunctionUpload>, Bundle, TdError>,
}

impl UploadFunctionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context, storage),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(storage),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<CreateRequest<CollectionParam, FunctionUpload>>::extract::<RequestContext>),
                from_fn(With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_name::<CollectionParam>),
                from_fn(With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_data::<FunctionUpload>),

                // Extract function (TODO also use FunctionId to generate data_location)
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),


                // Extract collection
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),

                // check requester is coll_admin or coll_dev for the function's collection
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<CollAdmin, CollDev>::check),

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
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionUpload>, Bundle, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::Request;
    use sha2::{Digest, Sha256};
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, CollectionName, DataLocation, RoleId, UserId};
    use td_storage::location::StorageLocation;
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
            .path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);
        let provider = UploadFunctionService::provider(
            db,
            queries,
            Arc::new(AuthzContext::default()),
            storage,
        );
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<CollectionParam, FunctionUpload>, Bundle>(&[
            type_of_val(
                &With::<CreateRequest<CollectionParam, FunctionUpload>>::extract::<RequestContext>,
            ),
            type_of_val(
                &With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_name::<
                    CollectionParam,
                >,
            ),
            type_of_val(
                &With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_data::<
                    FunctionUpload,
                >,
            ),
            // Extract function (TODO also use FunctionId to generate data_location)
            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            // Extract collection
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester is coll_admin or coll_dev for the function's collection
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev>::check),
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
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .path("/")
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
            CollectionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .build()?,
            function_upload,
        );

        let service = UploadFunctionService::new(
            db.clone(),
            Arc::new(AuthzContext::default()),
            storage.clone(),
        )
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
        assert_eq!(*bundle.created_by_id(), UserId::admin());

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
