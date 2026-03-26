//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::register::data_location;
use crate::function::layers::upload::upload_function_write_to_storage;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::bundle::{Bundle, BundleBuilder, BundleDB, BundleDBBuilder};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::function_upload::FunctionUpload;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    BundleHash, BundleId, CollectionId, CollectionIdName, StorageVersion,
};
use td_queries::By;
use td_queries::insert;
use td_storage::Storage;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UploadFunctionService,
    request = CreateRequest<CollectionParam, FunctionUpload>,
    response = Bundle,
    connection = TransactionProvider,
    context = AuthzContext,
    context = Storage,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<CollectionParam, FunctionUpload>>::extract::<RequestContext>),
        from_fn(
            With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_name::<CollectionParam>
        ),
        from_fn(
            With::<CreateRequest<CollectionParam, FunctionUpload>>::extract_data::<FunctionUpload>
        ),
        // Extract function (TODO also use FunctionId to generate data_location)
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        // Extract collection
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
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
        from_fn(With::<RequestContext>::try_convert_to::<BundleDBBuilder, _>),
        from_fn(With::<BundleId>::set::<BundleDBBuilder>),
        from_fn(With::<CollectionId>::set::<BundleDBBuilder>),
        from_fn(With::<BundleHash>::set::<BundleDBBuilder>),
        from_fn(With::<BundleDBBuilder>::build::<BundleDB>),
        from_fn(insert::<BundleDB>),
        // Build response
        from_fn(With::<BundleDB>::try_convert_to::<BundleBuilder, _>),
        from_fn(With::<BundleBuilder>::build::<Bundle>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use axum::body::Body;
    use axum::extract::Request;
    use sha2::{Digest, Sha256};
    use std::str::FromStr;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::types::basic::{AccessTokenId, CollectionName, DataLocation, RoleId, UserId};
    use td_queries::BasicQueries;
    use td_queries::test_utils::SeedQueries;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_upload_function(db: TdPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        UploadFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<CollectionParam, FunctionUpload>, Bundle>(&[
                type_of_val(
                    &With::<CreateRequest<CollectionParam, FunctionUpload>>::extract::<
                        RequestContext,
                    >,
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
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
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
                type_of_val(&With::<RequestContext>::try_convert_to::<BundleDBBuilder, _>),
                type_of_val(&With::<BundleId>::set::<BundleDBBuilder>),
                type_of_val(&With::<CollectionId>::set::<BundleDBBuilder>),
                type_of_val(&With::<BundleHash>::set::<BundleDBBuilder>),
                type_of_val(&With::<BundleDBBuilder>::build::<BundleDB>),
                type_of_val(&insert::<BundleDB>),
                // Build response
                type_of_val(&With::<BundleDB>::try_convert_to::<BundleBuilder, _>),
                type_of_val(&With::<BundleBuilder>::build::<Bundle>),
            ]);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_upload(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed_collection(&collection_name, &UserId::admin())
            .await?;

        let payload = "TEXT";
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .build()?,
                function_upload,
            );

        let context = Context::with_defaults(db.clone());
        let service = UploadFunctionService::build(&context).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        // Assert db
        let bundle_db: Vec<BundleDB> = db.fetch_all::<BundleDB>(()).await?;

        assert_eq!(bundle_db.len(), 1);
        let bundle = &bundle_db[0];
        assert_eq!(bundle.id, response.id);
        assert_eq!(bundle.collection_id, collection.id);
        let hash = hex::encode(&Sha256::digest(payload)[..]);
        assert_eq!(bundle.hash.to_string(), hash);
        assert_eq!(bundle.created_by_id, UserId::admin());

        // Assert storage
        let data_location = DataLocation::default();
        let (bundle_location, _) = StorageLocation::current()
            .builder(&data_location)
            .collection(&collection.id)
            .function(&bundle.id)
            .build();
        let content = context.storage.read(&bundle_location).await?;
        let content = String::from_utf8(content).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }
}
