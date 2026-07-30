//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::And;
use td_objects::NoFilter;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionVersion, FunctionVersionDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::params::FunctionAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, FunctionId, FunctionIdName,
};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionHistoryService,
    request = ListRequest<FunctionAtIdName>,
    response = ListResponse<FunctionVersion>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract_name::<FunctionAtIdName>),
        from_fn(With::<FunctionAtIdName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
        from_fn(With::<ActiveCollectionDB>::extract::<CollectionId>),
        // check requester can view the current project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        // extract attime (natural order)
        from_fn(With::<FunctionAtIdName>::extract::<AtTime>),
        // find function ID at time (as name could change in time)
        from_fn(With::<FunctionAtIdName>::extract::<FunctionIdName>),
        from_fn(
            By::<And![CollectionId, FunctionIdName]>::historic_fetch_one::<
                { FunctionDBWithNames::Available },
                FunctionDBWithNames,
            >
        ),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        // List (all active versions at the time). Here we want the history, so we do not want
        // to query the versioned view. Head versions and patches interleave: the
        // union DAO is the one patch-aware surface.
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<FunctionVersion>, _>),
        from_fn(
            By::<FunctionId>::historic_list_full::<
                NoFilter,
                { FunctionVersionDBWithNames::Available },
                FunctionVersion,
            >
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::delete::DeleteFunctionService;
    use crate::function::services::update::UpdateFunctionService;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, ListParamsBuilder};
    use td_objects::dxo::function::{FunctionPatchDB, FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionName,
        FunctionRuntimeValues, FunctionStatus, Snippet, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_function_history(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        FunctionHistoryService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ListRequest<FunctionAtIdName>, ProjectId>, ListResponse<FunctionVersion>>(&[
                type_of_val(&With::<ListRequest<FunctionAtIdName>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ListRequest<FunctionAtIdName>>::extract_name::<FunctionAtIdName>,
                ),
                type_of_val(&With::<FunctionAtIdName>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::fetch_one::<ActiveCollectionDB>),
                type_of_val(&With::<ActiveCollectionDB>::extract::<CollectionId>),
                // check requester can view the current project
                type_of_val(&Scope::<Project>::set),
                type_of_val(&Rbac::<Project, ViewProject>::check),
                // extract attime (natural order)
                type_of_val(&With::<FunctionAtIdName>::extract::<AtTime>),
                // find function ID at time (as name could change in time)
                type_of_val(&With::<FunctionAtIdName>::extract::<FunctionIdName>),
                type_of_val(
                    &By::<And![CollectionId, FunctionIdName]>::historic_fetch_one::<
                        { FunctionDBWithNames::Available },
                        FunctionDBWithNames,
                    >,
                ),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                // List (all active versions at the time). Here we want the history, so we do not want
                // to query the versioned view. Head versions and patches interleave: the
                // union DAO is the one patch-aware surface.
                type_of_val(&With::<ListRequest<FunctionAtIdName>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<FunctionVersion>, _>,
                ),
                type_of_val(
                    &By::<FunctionId>::historic_list_full::<
                        NoFilter,
                        { FunctionVersionDBWithNames::Available },
                        FunctionVersion,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_function_history(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let t0 = AtTime::now();

        // Create function_1
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let t1 = AtTime::now();

        // Update function_1 to function_2
        let update = FunctionUpdate::builder()
            .name(Some("function_2".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Transformer)
            .build()?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
                update.clone(),
            )
            .in_project(p.id);

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let updated = response?;

        let t2 = AtTime::now();

        // Patch function_2's head version
        let patch = FunctionPatchDB::builder()
            .parent_function_version_id(updated.id.clone())
            .function_id(updated.function_id)
            .collection_id(collection.id)
            .snippet(Snippet::try_from("patched snippet")?)
            .bundle_id(BundleId::default())
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .description(Description::try_from("patched description")?)
            .defined_on(AtTime::now())
            .defined_by_id(UserId::admin())
            .status(FunctionStatus::Active)
            .build()?;
        db.scoped(p.id).live().insert(&patch).await?;

        let t2p = AtTime::now();

        // Delete function_2
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .build()?,
            )
            .in_project(p.id);
        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let t3 = AtTime::now();

        // Create function_1 again (DIFFERENT THAN THE FIRST ONE)
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Transformer)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let t4 = AtTime::now();

        // Actual test
        // t0 -> no functions yet
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t0)
                    .build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t1)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()?,
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2 in history
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .at(t2)
                    .build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);
        assert!(data.iter().all(|v| v.patch_of.is_none()));

        // t2p -> the patch interleaves with the head versions, borrowing
        // the parent's name and carrying patch_of
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .at(t2p)
                    .build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 3);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[2].id, patch.id);
        assert_eq!(data[2].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[2].patch_of, Some(updated.id.clone()));
        assert_eq!(data[2].snippet, Snippet::try_from("patched snippet")?);
        assert_eq!(data[2].decorator, Decorator::Transformer);

        // t3 -> no function to retrieve the history
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .at(t3)
                    .build()?,
                ListParams::default(),
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t4)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            )
            .in_project(p.id);

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        Ok(())
    }
}
