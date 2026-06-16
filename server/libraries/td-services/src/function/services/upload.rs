//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::layers::upload::upload_function_write_to_storage;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_objects::dxo::bundle::{Bundle, BundleBuilder, BundleDB, BundleDBBuilder};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::function_upload::FunctionUpload;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    BasePath, BundleHash, BundleId, CollectionId, CollectionIdName, StorageVersion,
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
    scope = "Project",
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
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        // Extract collection
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // The bundle is stored under the collection's base_path, stamped onto the
        // bundle row so it self-locates regardless of later collection changes.
        from_fn(With::<CollectionDB>::extract::<BasePath>),
        // check requester can manage or develop the current project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        // Get storage version.
        from_fn(With::<StorageVersion>::default),
        // Write to storage with new bundle id.
        from_fn(With::<BundleId>::default),
        from_fn(upload_function_write_to_storage),
        // Build BundleDB (status defaults to Pending until a function claims it).
        from_fn(With::<RequestContext>::try_convert_to::<BundleDBBuilder, _>),
        from_fn(With::<BundleId>::set::<BundleDBBuilder>),
        from_fn(With::<CollectionId>::set::<BundleDBBuilder>),
        from_fn(With::<BasePath>::set::<BundleDBBuilder>),
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
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, BundleStatus, CollectionName, ProjectId, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_upload_function(db: TdPool) -> Result<(), TdError> {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        UploadFunctionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<CollectionParam, FunctionUpload>, ProjectId>, Bundle>(&[
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
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                // Extract collection
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionDB>::extract::<BasePath>),
                // check requester can manage or develop the current project
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                // Get storage version.
                type_of_val(&With::<StorageVersion>::default),
                // Write to storage with new bundle id.
                type_of_val(&With::<BundleId>::default),
                type_of_val(&upload_function_write_to_storage),
                // Build BundleDB
                type_of_val(&With::<RequestContext>::try_convert_to::<BundleDBBuilder, _>),
                type_of_val(&With::<BundleId>::set::<BundleDBBuilder>),
                type_of_val(&With::<CollectionId>::set::<BundleDBBuilder>),
                type_of_val(&With::<BasePath>::set::<BundleDBBuilder>),
                type_of_val(&With::<BundleHash>::set::<BundleDBBuilder>),
                type_of_val(&With::<BundleDBBuilder>::build::<BundleDB>),
                type_of_val(&insert::<BundleDB>),
                // Build response
                type_of_val(&With::<BundleDB>::try_convert_to::<BundleBuilder, _>),
                type_of_val(&With::<BundleBuilder>::build::<Bundle>),
            ]);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_upload(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let payload = "TEXT";
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .build()?,
                function_upload,
            )
            .in_project(ProjectId::sandbox());

        let context = Context::with_defaults(db.clone());
        let service = UploadFunctionService::build(&context).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        // Assert db
        let bundle_db: Vec<BundleDB> = db.live().fetch_all::<BundleDB>(()).await?;

        assert_eq!(bundle_db.len(), 1);
        let bundle = &bundle_db[0];
        assert_eq!(bundle.id, response.id);
        assert_eq!(bundle.collection_id, collection.id);
        let hash = hex::encode(&Sha256::digest(payload)[..]);
        assert_eq!(bundle.hash.to_string(), hash);
        assert_eq!(bundle.created_by_id, UserId::admin());

        // Bundle is stamped with the collection base_path and starts Pending.
        assert_eq!(bundle.base_path, collection.base_path);
        assert_eq!(bundle.status, BundleStatus::Pending);

        // Assert storage
        let base_path = BasePath::default();
        let (bundle_location, _) = StorageLocation::current()
            .builder(&base_path)
            .collection(&collection.id)
            .function(&bundle.id)
            .build()?;
        let content = context.storage.read(&bundle_location).await?;
        let content = String::from_utf8(content).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }

    /// The upload service runs a best-effort GC of abandoned (stale `Pending`)
    /// bundles. Recent pending bundles must be spared: two uploads to the same
    /// collection leave two pending rows (neither is older than the cutoff).
    /// Drives the upload service once against `collection`, returning the response.
    async fn upload_once(
        context: &Context,
        collection_name: &CollectionName,
        payload: &str,
    ) -> Result<Bundle, TdError> {
        let request = Request::builder()
            .body(Body::new(payload.to_string()))
            .unwrap();
        let function_upload = FunctionUpload::new(request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{collection_name}"))?)
                    .build()?,
                function_upload,
            )
            .in_project(ProjectId::sandbox());
        let service = UploadFunctionService::build(context).service();
        service.raw_oneshot(request).await
    }

    /// A bundle is stamped with the collection's base_path at upload time. Here the
    /// collection carries a non-default base_path ("/prod"), so the bundle must too.
    #[td_test::test(db)]
    async fn test_upload_bundle_base_path_non_default(db: TdPool) -> Result<(), TdError> {
        use td_objects::dxo::collection::{CollectionUpdateDB, CollectionUpdateDBBuilder};

        let collection_name = CollectionName::try_from("prod_coll")?;
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Context::with_defaults(db.clone());

        // Move the collection onto a non-default base_path (the upload service's
        // public surface offers no base_path knob; the live update is the seam used
        // elsewhere to mutate seeded rows directly).
        let prod = BasePath::try_from("/prod")?;
        let update: CollectionUpdateDB = CollectionUpdateDBBuilder::try_from(&collection)?
            .modified_on(AtTime::now())
            .modified_by_id(UserId::admin())
            .base_path(prod.clone())
            .build()?;
        db.live()
            .update::<CollectionUpdateDB, CollectionDB>(&update, &collection.id)
            .await?;
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, prod);

        // Upload: the bundle must inherit the collection's non-default base_path.
        let payload = "TEXT";
        upload_once(&context, &collection_name, payload).await?;

        let bundle_db: Vec<BundleDB> = db.live().fetch_all::<BundleDB>(()).await?;
        assert_eq!(bundle_db.len(), 1);
        assert_eq!(bundle_db[0].base_path, prod);

        // And the object lands under that base_path in storage.
        let (location, _) = StorageLocation::current()
            .builder(&prod)
            .collection(&bundle_db[0].collection_id)
            .function(&bundle_db[0].id)
            .build()?;
        let content = String::from_utf8(context.storage.read(&location).await?).unwrap();
        assert_eq!(content, payload);

        Ok(())
    }
}
