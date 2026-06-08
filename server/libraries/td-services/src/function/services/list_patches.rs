//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionPatchRecord};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName, FunctionIdName};
use td_objects::types::visible_collections::{VisibleCollections, VisibleFunctionsCollections};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /collections/{coll}/functions/{name}/patches` — every patch
/// row for the function, ordered latest first.
#[service_factory(
    name = ListFunctionPatchesService,
    request = ListRequest<FunctionParam>,
    response = ListResponse<FunctionPatchRecord>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<FunctionParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<FunctionParam>>::extract_name::<FunctionParam>),
        from_fn(With::<FunctionParam>::extract::<CollectionIdName>),
        from_fn(With::<FunctionParam>::extract::<FunctionIdName>),
        // Resolve collection + authz.
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Resolve function id from the URL.
        from_fn(By::<And![CollectionId, FunctionIdName]>::assert_exists::<FunctionDBWithNames>),
        // get allowed collections
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
        // convert them to allowed function collections
        from_fn(With::<VisibleCollections>::try_convert_to::<VisibleFunctionsCollections, _>),
        // list function patches
        from_fn(With::<ListRequest<FunctionParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<FunctionPatchRecord>, _>),
        from_fn(By::<()>::list::<VisibleFunctionsCollections, FunctionPatchRecord>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::patch::CreateFunctionPatchService;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::function::{FunctionPatch, FunctionRegister};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        ProjectId, RoleId, TableNameDto, UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::CurrentEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_patches(db: TdPool) {
        use td_tower::metadata::type_of_val;
        use td_tower::project_scope::Scoped;

        ListFunctionPatchesService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<FunctionParam>, ProjectId>, ListResponse<FunctionPatchRecord>>(&[
                type_of_val(&With::<ListRequest<FunctionParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<FunctionParam>>::extract_name::<FunctionParam>),
                type_of_val(&With::<FunctionParam>::extract::<CollectionIdName>),
                type_of_val(&With::<FunctionParam>::extract::<FunctionIdName>),
                // Resolve collection + authz.
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // Resolve function id from the URL.
                type_of_val(
                    &By::<And![CollectionId, FunctionIdName]>::assert_exists::<FunctionDBWithNames>,
                ),
                // get allowed collections
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
                // convert them to allowed function collections
                type_of_val(
                    &With::<VisibleCollections>::try_convert_to::<VisibleFunctionsCollections, _>,
                ),
                // list function patches
                type_of_val(&With::<ListRequest<FunctionParam>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<FunctionPatchRecord>, _>,
                ),
                type_of_val(&By::<()>::list::<VisibleFunctionsCollections, FunctionPatchRecord>),
            ]);
    }

    async fn seed_patches_fixture(db: &TdPool) -> CollectionDB {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let register = FunctionRegister::builder()
            .try_name("f0")
            .unwrap()
            .try_description("d")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("s")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0").unwrap()]))
            .runtime_values(FunctionRuntimeValues::try_from("rv").unwrap())
            .reuse_frozen_tables(false)
            .build()
            .unwrap();
        let _ = db.seed().function(&collection, &register).await.unwrap();
        collection
    }

    #[td_test::test(db)]
    async fn test_list_patches_returns_empty_for_no_patches(db: TdPool) -> Result<(), TdError> {
        let _coll = seed_patches_fixture(&db).await;
        let param = FunctionParam::builder()
            .collection(CollectionIdName::from_str("c0")?)
            .function(FunctionIdName::from_str("f0")?)
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list(param, ListParams::default())
                .in_project(ProjectId::sandbox());
        let response = ListFunctionPatchesService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;
        assert_eq!(response.data.len(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_patches_returns_all_patches_latest_first(db: TdPool) -> Result<(), TdError> {
        let _coll = seed_patches_fixture(&db).await;
        let parent: FunctionDBWithNames = {
            db.current()
                .fetch_one::<FunctionDBWithNames>(and!(
                    CollectionName::try_from("c0")?,
                    FunctionName::try_from("f0")?
                ))
                .await?
        };

        // Two patches of the same parent fvid.
        for i in 0..2 {
            let body = FunctionPatch::builder()
                .patch_of(parent.id.clone())
                .bundle_id(BundleId::default())
                .try_snippet(&format!("v{i}"))?
                .build()?;
            let req =
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                    .create(
                        FunctionParam::builder()
                            .collection(CollectionIdName::from_str("c0")?)
                            .function(FunctionIdName::from_str("f0")?)
                            .build()?,
                        body,
                    )
                    .in_project(ProjectId::sandbox());
            let _ = CreateFunctionPatchService::with_defaults(db.clone())
                .service()
                .raw_oneshot(req)
                .await?;
        }

        let read_request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str("c0")?)
                        .function(FunctionIdName::from_str("f0")?)
                        .build()?,
                    ListParams::default(),
                )
                .in_project(ProjectId::sandbox());
        let response = ListFunctionPatchesService::with_defaults(db.clone())
            .service()
            .raw_oneshot(read_request)
            .await?;

        assert_eq!(response.data.len(), 2);
        assert!(response.data[0].defined_on >= response.data[1].defined_on);
        for v in &response.data {
            assert_eq!(v.parent_function_version_id, parent.id);
        }
        Ok(())
    }
}
