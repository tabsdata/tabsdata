//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::function::{Function, FunctionDBWithNames};
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::AtTimeParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::AtTime;
use td_objects::types::visible_collections::{VisibleCollections, VisibleFunctionsCollections};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionListHistoryService,
    request = ListRequest<AtTimeParam>,
    response = ListResponse<Function>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<AtTimeParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<AtTimeParam>>::extract_name::<AtTimeParam>),
        from_fn(With::<AtTimeParam>::extract::<AtTime>),
        // get allowed collections
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
        // convert them to allowed function collections
        from_fn(With::<VisibleCollections>::try_convert_to::<VisibleFunctionsCollections, _>),
        // list
        from_fn(With::<ListRequest<AtTimeParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Function>, _>),
        from_fn(
            By::<()>::historic_list_versions::<
                VisibleFunctionsCollections,
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
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        FunctionName, RoleId, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_function_versions_history(db: TdPool) {
        use td_tower::metadata::type_of_val;

        FunctionListHistoryService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<AtTimeParam>, ListResponse<Function>>(&[
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract_name::<AtTimeParam>),
                type_of_val(&With::<AtTimeParam>::extract::<AtTime>),
                // get allowed collections
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
                // convert them to allowed function collections
                type_of_val(
                    &With::<VisibleCollections>::try_convert_to::<VisibleFunctionsCollections, _>,
                ),
                // list
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Function>, _>),
                type_of_val(
                    &By::<()>::historic_list_versions::<
                        VisibleFunctionsCollections,
                        { FunctionDBWithNames::Available },
                        Function,
                    >,
                ),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_function_versions_history(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let t0 = AtTime::now();

        // Create function_1 and function_2
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

        // Create function_2
        let create = FunctionRegister::builder()
            .try_name("function_2")?
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

        let t2 = AtTime::now();

        // Update function_1 to function_3
        let update = FunctionUpdate::builder()
            .name(Some("function_3".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .bundle_id(BundleId::default())
            .snippet(Some("function_foo snippet".try_into()?))
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

        let t3 = AtTime::now();

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

        let t4 = AtTime::now();

        // Create function_5
        let create = FunctionRegister::builder()
            .try_name("function_5")?
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

        let t5 = AtTime::now();

        // Actual test
        // t0 -> no functions
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t0).build()?,
                ListParams::default(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> function_1
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t1).build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t2).build()?,
                ListParams::default(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);

        // t3 -> function_2 and function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t3).build()?,
                ListParams::default(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_3")?);

        // t4 -> function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t4).build()?,
                ListParams::default(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_3")?);

        // t5 -> function_3 and function_5
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t5).build()?,
                ListParams::default(),
            );

        let service = FunctionListHistoryService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_3")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_5")?);
        Ok(())
    }
}
