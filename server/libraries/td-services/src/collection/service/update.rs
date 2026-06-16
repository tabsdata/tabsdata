//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::layer::update::{
    UpdateCollectionDBBuilderUpdate, update_collection_validate,
};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_objects::dxo::collection::{
    CollectionDB, CollectionDBWithNames, CollectionRead, CollectionReadBuilder, CollectionUpdate,
    CollectionUpdateDB, CollectionUpdateDBBuilder,
};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateCollectionService,
    request = UpdateRequest<CollectionParam, CollectionUpdate>,
    response = CollectionRead,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<RequestContext>
        ),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<CollectionParam>
        ),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                CollectionUpdate,
            >
        ),
        from_fn(update_collection_validate),
        from_fn(With::<CollectionDB>::try_convert_to::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<CollectionUpdate>::update_collection_update_db_builder),
        from_fn(With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB>),
        from_fn(By::<CollectionId>::update::<CollectionUpdateDB, CollectionDB>),
        from_fn(By::<CollectionId>::fetch_one::<CollectionDBWithNames>),
        from_fn(With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>),
        from_fn(With::<CollectionReadBuilder>::build::<CollectionRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::collection::CollectionError;
    use crate::function::services::register::RegisterFunctionService;
    use crate::function::services::upload::UploadFunctionService;
    use axum::body::Body;
    use axum::extract::Request;
    use std::str::FromStr;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::rest_urls::params::FunctionMaybeAtIdName;
    use td_objects::rest_urls::{CollectionParam, FunctionParam, OptionalAtTimeParam};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, CollectionName, Decorator, Description, FunctionIdName,
        FunctionName, FunctionRuntimeValues, FunctionStatus, ProjectId, RoleId, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_update_collection(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_tower::metadata::type_of_val;

        UpdateCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<CollectionParam, CollectionUpdate>, ProjectId>, CollectionRead>(&[
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<
                        RequestContext,
                    >,
                ),
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, DevelopProject>::check),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<
                        CollectionParam,
                    >,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                        CollectionUpdate,
                    >,
                ),
                type_of_val(&update_collection_validate),
                type_of_val(&With::<CollectionDB>::try_convert_to::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<CollectionUpdate>::update_collection_update_db_builder),
                type_of_val(&With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB>),
                type_of_val(&By::<CollectionId>::update::<CollectionUpdateDB, CollectionDB>),
                type_of_val(&By::<CollectionId>::fetch_one::<CollectionDBWithNames>),
                type_of_val(
                    &With::<CollectionDBWithNames>::try_convert_to::<CollectionReadBuilder, _>,
                ),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_update_collection(db: TdPool) {
        let create_name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&create_name, &UserId::admin())
            .await;

        let before_update = AtTime::now();

        let name = CollectionName::try_from("ds1").unwrap();
        let description = Description::try_from("DS1").unwrap();

        let update = CollectionUpdate::builder()
            .name(Some(name.clone()))
            .description(Some(description.clone()))
            .build()
            .unwrap();

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&create_name.to_string()).unwrap())
                .build()
                .unwrap(),
            update,
        )
        .in_project(ProjectId::sandbox());

        let service = UpdateCollectionService::with_defaults(db).service();
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert_eq!(updated.name, name);
        assert_eq!(updated.description, description);
        assert!(updated.created_on < before_update);
        assert_eq!(updated.created_by_id, UserId::admin());
        assert_eq!(updated.created_by, UserName::admin());
        assert!(updated.modified_on > before_update);
        assert_eq!(updated.modified_by_id, UserId::admin());
        assert_eq!(updated.modified_by, UserName::admin());
    }

    /// Drives UpdateCollectionService against `collection_name`, optionally
    /// changing name/description/base_path. Returns the service result.
    async fn update_collection(
        db: &TdPool,
        collection_name: &CollectionName,
        name: Option<CollectionName>,
        description: Option<Description>,
        base_path: Option<BasePath>,
    ) -> Result<CollectionRead, TdError> {
        let update = CollectionUpdate::builder()
            .name(name)
            .description(description)
            .base_path(base_path)
            .build()?;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(collection_name.as_str())?)
                .build()?,
            update,
        )
        .in_project(ProjectId::sandbox());
        UpdateCollectionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await
    }

    /// The current Active version of the function named `name`.
    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.name == *name)
            .expect("matching active function exists");
        Ok(function)
    }

    #[td_test::test(db)]
    async fn test_update_collection_base_path(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("bp_coll")?;
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Seeded collections start on the default base_path "/".
        assert_eq!(collection.base_path, BasePath::default());

        let data = BasePath::try_from("/data")?;
        update_collection(&db, &collection_name, None, None, Some(data.clone())).await?;

        // Read back the persisted CollectionDB and confirm the new base_path.
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, data);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_collection_nothing_to_update_errors(db: TdPool) {
        let collection_name = CollectionName::try_from("noop_coll").unwrap();
        let _ = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await;

        // name=None, description=None, base_path=None -> nothing to update.
        let update = CollectionUpdate::builder()
            .name(None)
            .description(None)
            .base_path(None)
            .build()
            .unwrap();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(collection_name.as_str()).unwrap())
                .build()
                .unwrap(),
            update,
        )
        .in_project(ProjectId::sandbox());

        let service = UpdateCollectionService::with_defaults(db).service();
        assert_service_error(service, request, |err| {
            #[allow(unreachable_patterns)]
            match err {
                CollectionError::UpdateRequestHasNothingToUpdate => {}
                other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
            }
        })
        .await;
    }

    /// A collection base_path change must not relocate artifacts that were
    /// generated under the old base_path: those rows carry their own stamped
    /// prefix so they stay findable. The collection update bumps no function
    /// version, so the function row is untouched and the bundle stays put.
    #[td_test::test(db)]
    async fn test_collection_base_path_update_does_not_relocate_existing_function_artifacts(
        db: TdPool,
    ) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("move_coll")?;
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Context::with_defaults(db.clone());

        // Put the collection on base_path "/a" before any artifacts exist.
        let a = BasePath::try_from("/a")?;
        update_collection(&db, &collection_name, None, None, Some(a.clone())).await?;

        // Upload a real bundle so a Pending BundleDB row + storage object exist.
        let payload = b"BUNDLE-PAYLOAD".to_vec();
        let upload_request = Request::builder()
            .body(Body::from(payload.clone()))
            .unwrap();
        let upload = FunctionUpload::new(upload_request);
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::default_role())
                .create(
                    CollectionParam::builder()
                        .collection(CollectionIdName::from_str(collection_name.as_str())?)
                        .build()?,
                    upload,
                )
                .in_project(ProjectId::sandbox());
        let bundle = UploadFunctionService::build(&context)
            .service()
            .raw_oneshot(request)
            .await?;

        // Register the function bound to that bundle.
        let create = FunctionRegister::builder()
            .try_name("artifact_fn")?
            .try_description("description")?
            .bundle_id(bundle.id)
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::default_role())
                .create(
                    CollectionParam::builder()
                        .collection(CollectionIdName::from_str(collection_name.as_str())?)
                        .build()?,
                    create,
                )
                .in_project(ProjectId::sandbox());
        RegisterFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Both artifacts are stamped with the old "/a" prefix at create time.
        let function_name = FunctionName::try_from("artifact_fn")?;
        let function = active_function(&db, &function_name).await?;
        assert_eq!(function.stage_base_path, a);
        let bundle_before: Vec<BundleDB> =
            db.live().fetch_all::<BundleDB>(&function.bundle_id).await?;
        assert_eq!(bundle_before.len(), 1);
        assert_eq!(bundle_before[0].base_path, a);

        // Move the collection to "/b".
        let b = BasePath::try_from("/b")?;
        update_collection(&db, &collection_name, None, None, Some(b.clone())).await?;
        let reloaded: CollectionDB = db.live().fetch_one::<CollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, b);

        // The previously-generated artifacts still carry "/a": the move did not
        // relocate them, and no new function version was created.
        let function_after = active_function(&db, &function_name).await?;
        assert_eq!(
            function_after.id, function.id,
            "collection update must not bump the function version"
        );
        assert_eq!(
            function_after.stage_base_path, a,
            "function stage_base_path stays on the prefix it was stamped with"
        );
        let bundle_after: Vec<BundleDB> = db
            .live()
            .fetch_all::<BundleDB>(&function_after.bundle_id)
            .await?;
        assert_eq!(bundle_after.len(), 1);
        assert_eq!(
            bundle_after[0].base_path, a,
            "bundle base_path stays on the prefix it was stamped with"
        );

        // And the bundle is still downloadable: the download layer reads the
        // stamped bundle.base_path ("/a"), unaffected by the collection move.
        use crate::function::services::download_bundle::DownloadBundleService;
        use bytes::Bytes;
        use futures::StreamExt;
        let function_param = FunctionParam::builder()
            .collection(CollectionIdName::from_str(collection_name.as_str())?)
            .function(FunctionIdName::from_str("artifact_fn")?)
            .build()?;
        let download_request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::default_role())
                .read(FunctionMaybeAtIdName::new(
                    function_param,
                    OptionalAtTimeParam { at: None },
                ))
                .in_project(ProjectId::sandbox());
        let response = DownloadBundleService::build(&context)
            .service()
            .raw_oneshot(download_request)
            .await?;
        assert_eq!(response.bundle_id, function_after.bundle_id);
        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(
            Bytes::from(buf).as_ref(),
            payload.as_slice(),
            "bundle still streams after the collection moved"
        );

        Ok(())
    }
}
