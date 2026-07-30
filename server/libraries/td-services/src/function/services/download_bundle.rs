//
// Copyright 2026 Tabsdata Inc.
//

pub use crate::function::layers::download_bundle::BundleDownload;
use crate::function::layers::download_bundle::{build_bundle_download, resolve_function_version};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope, enter_project_scope};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::function::FunctionVersionDBWithNames;
use td_objects::rest_urls::FunctionVersionIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, FunctionVersionId, ProjectId};
use td_queries::By;
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DownloadBundleService,
    request = ReadRequest<FunctionVersionIdParam>,
    response = BundleDownload,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
    context = Storage,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<FunctionVersionIdParam>>::extract::<RequestContext>),
        from_fn(
            With::<ReadRequest<FunctionVersionIdParam>>::extract_name::<FunctionVersionIdParam>
        ),
        from_fn(With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
        // The version id is globally unique; resolve it, then pivot into its
        // project's scope via the collection before the authz check.
        from_fn(resolve_function_version),
        from_fn(With::<FunctionVersionDBWithNames>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<ProjectId>),
        from_fn(enter_project_scope),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(build_bundle_download),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::function::services::upload::UploadFunctionService;
    use axum::body::Body;
    use axum::extract::Request;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::str::FromStr;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::{ApiError, TdError};
    use td_objects::Buildable;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDBBuilder, FunctionPatchDB, FunctionRegister};
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleHash, BundleId, CollectionIdName, CollectionName, Decorator,
        FunctionStatus, FunctionVersionId, ProjectId, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_download_bundle(db: TdPool) {
        use td_tower::metadata::type_of_val;

        DownloadBundleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<FunctionVersionIdParam>, BundleDownload>(&[
                type_of_val(
                    &With::<ReadRequest<FunctionVersionIdParam>>::extract::<RequestContext>,
                ),
                type_of_val(
                    &With::<ReadRequest<FunctionVersionIdParam>>::extract_name::<
                        FunctionVersionIdParam,
                    >,
                ),
                type_of_val(&With::<FunctionVersionIdParam>::extract::<FunctionVersionId>),
                type_of_val(&resolve_function_version),
                type_of_val(&With::<FunctionVersionDBWithNames>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<ProjectId>),
                type_of_val(&enter_project_scope),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(&build_bundle_download),
            ]);
    }

    #[td_test::test(db)]
    async fn test_download_bundle(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Context::with_defaults(db.clone());

        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Upload bundle A and register v1 of the function bound to it.
        let payload_a = b"BUNDLE-PAYLOAD-A".to_vec();
        let bundle_a_id =
            upload_bundle(&context, p.id, &collection.name, payload_a.clone()).await?;

        let create_v1 = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("desc")?
            .bundle_id(bundle_a_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("rv")?
            .reuse_frozen_tables(false)
            .build()?;
        let v1 = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create_v1)
            .await?;

        // Upload bundle B and stamp v2 directly: same function_id, fresh
        // version id, later defined_on, bundle B. Going through
        // UpdateFunctionService would drag in trigger/dependency rebuilds
        // that are irrelevant to what we're asserting here.
        let payload_b = b"BUNDLE-PAYLOAD-B".to_vec();
        let bundle_b_id =
            upload_bundle(&context, p.id, &collection.name, payload_b.clone()).await?;

        let v2 = FunctionDBBuilder::try_from(&v1)?
            .id(FunctionVersionId::new())
            .bundle_id(bundle_b_id)
            .defined_on(AtTime::now())
            .build()?;
        db.scoped(p.id).live().insert(&v2).await?;

        let service = DownloadBundleService::build(&context).service();

        // Each version id resolves to its own bundle, regardless of which one
        // is current — v1 → bundle A, v2 → bundle B. The ETag is the bundle's
        // content hash.
        let cases = [
            (&v1, &bundle_a_id, &payload_a),
            (&v2, &bundle_b_id, &payload_b),
        ];
        for (expected_fn, expected_bundle, expected_payload) in cases {
            let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(FunctionVersionIdParam {
                    function_version_id: expected_fn.id.clone(),
                });
            let response = service.clone().raw_oneshot(download_request).await?;

            assert_eq!(response.function_version_id, expected_fn.id);
            assert_eq!(response.function_name, expected_fn.name);
            assert_eq!(response.etag, bundle_hash(&db, expected_bundle).await?);

            let mut stream = response.stream.into_inner();
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                buf.extend_from_slice(&chunk?);
            }
            assert_eq!(Bytes::from(buf).as_ref(), expected_payload.as_slice());
        }

        // A patch is addressed by its own version id and serves its own
        // bundle, under its parent's collection and name.
        let payload_patch = b"BUNDLE-PAYLOAD-PATCH".to_vec();
        let bundle_patch_id =
            upload_bundle(&context, p.id, &collection.name, payload_patch.clone()).await?;
        let patch = FunctionPatchDB::builder()
            .id(FunctionVersionId::new())
            .parent_function_version_id(v2.id.clone())
            .function_id(v2.function_id)
            .collection_id(v2.collection_id)
            .try_snippet("patched snippet")?
            .bundle_id(bundle_patch_id)
            .try_runtime_values("rv")?
            .try_description("patched")?
            .defined_on(AtTime::now())
            .defined_by_id(UserId::admin())
            .build()?;
        db.scoped(p.id).live().insert(&patch).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionIdParam {
                function_version_id: patch.id.clone(),
            },
        );
        let response = service.clone().raw_oneshot(request).await?;

        assert_eq!(response.function_version_id, patch.id);
        assert_eq!(response.etag, bundle_hash(&db, &bundle_patch_id).await?);
        assert_eq!(response.function_name, v2.name);
        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(Bytes::from(buf).as_ref(), payload_patch.as_slice());

        // A version whose function has been deleted (status Deleted) is a
        // tombstone, not a downloadable artifact: not found.
        let deleted = FunctionDBBuilder::try_from(&v2)?
            .id(FunctionVersionId::new())
            .defined_on(AtTime::now())
            .status(FunctionStatus::Deleted)
            .build()?;
        db.scoped(p.id).live().insert(&deleted).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionIdParam {
                function_version_id: deleted.id.clone(),
            },
        );
        let err = service
            .clone()
            .raw_oneshot(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        // A patch of a deleted function is hidden with it: deletion does not
        // cascade to the patch row, so the parent's status is what decides.
        let orphan = FunctionPatchDB::builder()
            .id(FunctionVersionId::new())
            .parent_function_version_id(deleted.id.clone())
            .function_id(deleted.function_id)
            .collection_id(deleted.collection_id)
            .try_snippet("orphan snippet")?
            .bundle_id(bundle_patch_id)
            .try_runtime_values("rv")?
            .try_description("orphan")?
            .defined_on(AtTime::now())
            .defined_by_id(UserId::admin())
            .build()?;
        db.scoped(p.id).live().insert(&orphan).await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionIdParam {
                function_version_id: orphan.id.clone(),
            },
        );
        let err = service
            .clone()
            .raw_oneshot(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        // An unknown version id is likewise not found.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
            FunctionVersionIdParam {
                function_version_id: FunctionVersionId::new(),
            },
        );
        let err = service
            .clone()
            .raw_oneshot(request)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        Ok(())
    }

    async fn bundle_hash(db: &TdPool, bundle_id: &BundleId) -> Result<BundleHash, TdError> {
        let bundle: BundleDB = db.live().fetch_one::<BundleDB>(bundle_id).await?;
        Ok(bundle.hash)
    }

    async fn upload_bundle(
        context: &Context,
        project_id: ProjectId,
        collection: &CollectionName,
        payload: Vec<u8>,
    ) -> Result<BundleId, TdError> {
        let request = Request::builder().body(Body::from(payload)).unwrap();
        let upload = FunctionUpload::new(request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection))?)
                    .build()?,
                upload,
            )
            .in_project(project_id);
        let bundle = UploadFunctionService::build(context)
            .service()
            .raw_oneshot(request)
            .await?;
        Ok(bundle.id)
    }
}
