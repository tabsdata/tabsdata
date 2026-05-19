//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::function::{Function, FunctionDBWithNames};
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
    response = ListResponse<Function>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract_name::<FunctionAtIdName>),
        from_fn(With::<FunctionAtIdName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
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
        // to query the versioned view.
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Function>, _>),
        from_fn(
            By::<FunctionId>::historic_list_full::<
                NoFilter,
                { FunctionDBWithNames::Available },
                Function,
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
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, ListParamsBuilder};
    use td_objects::dxo::function::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, RoleId, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_function_history(db: TdPool) {
        use td_tower::metadata::type_of_val;

        FunctionHistoryService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<FunctionAtIdName>, ListResponse<Function>>(&[
                type_of_val(&With::<ListRequest<FunctionAtIdName>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ListRequest<FunctionAtIdName>>::extract_name::<FunctionAtIdName>,
                ),
                type_of_val(&With::<FunctionAtIdName>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
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
                // to query the versioned view.
                type_of_val(&With::<ListRequest<FunctionAtIdName>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Function>, _>),
                type_of_val(
                    &By::<FunctionId>::historic_list_full::<
                        NoFilter,
                        { FunctionDBWithNames::Available },
                        Function,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_function_history(db: TdPool) -> Result<(), TdError> {
        let collection = db
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
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.seed().function(&collection, &create).await?;

        let t1 = AtTime::now();

        // Update function_1 to function_2
        let update = FunctionUpdate::builder()
            .name(Some("function_2".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .build()?;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        let t2 = AtTime::now();

        // Delete function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .build()?,
            );
        let service = DeleteFunctionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let t3 = AtTime::now();

        // Create function_1 again (DIFFERENT THAN THE FIRST ONE)
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.seed().function(&collection, &create).await?;

        let t4 = AtTime::now();

        // Actual test
        // t0 -> no functions yet
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t0)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t1)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()?,
            );

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2 in history
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .at(t2)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);

        // t3 -> no function to retrieve the history
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_2")?)
                    .at(t3)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .at(t4)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        Ok(())
    }
}
