//
// Copyright 2026 Tabsdata Inc.
//

pub use crate::function::layers::download_bundle::BundleDownload;
use crate::function::layers::download_bundle::{
    build_bundle_download, resolve_function_with_names,
};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::params::FunctionMaybeAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, CollectionIdName, FunctionIdName};
use td_storage::Storage;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DownloadBundleService,
    request = ReadRequest<FunctionMaybeAtIdName>,
    response = BundleDownload,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
    context = Storage,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<FunctionMaybeAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<FunctionMaybeAtIdName>>::extract_name::<FunctionMaybeAtIdName>),
        from_fn(With::<FunctionMaybeAtIdName>::extract::<CollectionIdName>),
        from_fn(With::<FunctionMaybeAtIdName>::extract::<FunctionIdName>),
        from_fn(With::<FunctionMaybeAtIdName>::extract::<Option<AtTime>>),
        from_fn(resolve_function_with_names),
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
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDBBuilder, FunctionRegister};
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::rest_urls::{CollectionParam, FunctionParam, OptionalAtTimeParam};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionVersionId, ProjectId, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_download_bundle(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        DownloadBundleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<FunctionMaybeAtIdName>, ProjectId>, BundleDownload>(&[
                type_of_val(&With::<ReadRequest<FunctionMaybeAtIdName>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ReadRequest<FunctionMaybeAtIdName>>::extract_name::<
                        FunctionMaybeAtIdName,
                    >,
                ),
                type_of_val(&With::<FunctionMaybeAtIdName>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionMaybeAtIdName>::extract::<FunctionIdName>),
                type_of_val(&With::<FunctionMaybeAtIdName>::extract::<Option<AtTime>>),
                type_of_val(&resolve_function_with_names),
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

        // Snapshot time after v1 but before v2 — historic ranked-CTE at this
        // time must see v1 only.
        let at_v1 = AtTime::now();

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

        let function_param = FunctionParam::builder()
            .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
            .function(FunctionIdName::from_str("joaquin")?)
            .build()?;
        let service = DownloadBundleService::build(&context).service();

        // `at = None` → current() branch → must return v2 (bundle B).
        // `at = at_v1` → historic() branch → must return v1 (bundle A) since
        // v2 didn't exist yet at that snapshot.
        let cases = [
            (None, &v2, &bundle_b_id, &payload_b),
            (Some(at_v1), &v1, &bundle_a_id, &payload_a),
        ];
        for (at, expected_fn, expected_bundle, expected_payload) in cases {
            let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(FunctionMaybeAtIdName::new(
                    function_param.clone(),
                    OptionalAtTimeParam { at },
                ))
                .in_project(p.id);
            let response = service.clone().raw_oneshot(download_request).await?;

            assert_eq!(response.function_version_id, expected_fn.id);
            assert_eq!(response.function_name, expected_fn.name);
            assert_eq!(response.bundle_id, *expected_bundle);

            let mut stream = response.stream.into_inner();
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                buf.extend_from_slice(&chunk?);
            }
            assert_eq!(Bytes::from(buf).as_ref(), expected_payload.as_slice());
        }

        Ok(())
    }

    /// When the (durable) bundle row has been reaped, the download layer falls
    /// back to the collection's live base_path and still streams the bytes.
    #[td_test::test(db)]
    async fn test_download_bundle_falls_back_to_collection_base_path_when_bundle_row_missing(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Context::with_defaults(db.clone());

        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let payload = b"BUNDLE-PAYLOAD".to_vec();
        let bundle_id = upload_bundle(&context, p.id, &collection.name, payload.clone()).await?;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("desc")?
            .bundle_id(bundle_id)
            .try_snippet("snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("rv")?
            .reuse_frozen_tables(false)
            .build()?;
        db.scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        // Reap the bundle row, leaving only the storage object and the live
        // collection base_path to locate it.
        db.scoped(p.id)
            .live()
            .delete::<BundleDB>(&bundle_id)
            .await?;
        assert!(
            db.live()
                .fetch_optional::<BundleDB>(&bundle_id)
                .await?
                .is_none(),
            "bundle row must be gone before downloading"
        );

        let function_param = FunctionParam::builder()
            .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
            .function(FunctionIdName::from_str("joaquin")?)
            .build()?;
        let service = DownloadBundleService::build(&context).service();
        let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(FunctionMaybeAtIdName::new(
                function_param,
                OptionalAtTimeParam { at: None },
            ))
            .in_project(p.id);
        let response = service.raw_oneshot(download_request).await?;
        assert_eq!(response.bundle_id, bundle_id);

        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(Bytes::from(buf).as_ref(), payload.as_slice());

        Ok(())
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
