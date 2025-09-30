//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With, combine};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, FunctionId, FunctionIdName, FunctionStatus,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::function::{Function, FunctionDBWithNames};
use td_objects::types::table::FunctionAtIdName;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = FunctionHistoryService,
    request = ListRequest<FunctionAtIdName>,
    response = ListResponse<Function>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<FunctionAtIdName>>::extract_name::<FunctionAtIdName>),
        from_fn(With::<FunctionAtIdName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // extract attime (natural order)
        from_fn(With::<FunctionAtIdName>::extract::<AtTime>),
        // find function ID at time (as name could change in time)
        from_fn(FunctionStatus::active_or_frozen),
        from_fn(With::<FunctionAtIdName>::extract::<FunctionIdName>),
        from_fn(combine::<CollectionId, FunctionIdName>),
        from_fn(By::<(CollectionId, FunctionIdName)>::select_version::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        // List (all active versions at the time). Here we want the history, so we do not want
        // to query the versioned view.
        from_fn(By::<FunctionId>::list_at::<FunctionAtIdName, NoListFilter, Function>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::delete::DeleteFunctionService;
    use crate::function::services::update::UpdateFunctionService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{ListParams, ListParamsBuilder};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, FunctionName, RoleId, UserId,
    };
    use td_objects::types::function::{FunctionRegister, FunctionUpdate};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_function_history(db: DbPool) {
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
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // extract attime (natural order)
                type_of_val(&With::<FunctionAtIdName>::extract::<AtTime>),
                // find function ID at time (as name could change in time)
                type_of_val(&FunctionStatus::active_or_frozen),
                type_of_val(&With::<FunctionAtIdName>::extract::<FunctionIdName>),
                type_of_val(&combine::<CollectionId, FunctionIdName>),
                type_of_val(
                    &By::<(CollectionId, FunctionIdName)>::select_version::<FunctionDBWithNames>,
                ),
                type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
                // List (all active versions at the time). Here we want the history, so we do not want
                // to query the versioned view.
                type_of_val(&By::<FunctionId>::list_at::<FunctionAtIdName, NoListFilter, Function>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_function_history(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

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
        let _ = seed_function(&db, &collection, &create).await;

        let t1 = AtTime::now();

        // Update function_1 to function_2
        let update = FunctionUpdate::builder()
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
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("function_1")?
                    .build()?,
                update.clone(),
            );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        let t2 = AtTime::now();

        // Delete function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).delete(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("function_2")?
                    .build()?,
            );
        let service = DeleteFunctionService::with_defaults(db.clone())
            .service()
            .await;
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
        let _ = seed_function(&db, &collection, &create).await;

        let t4 = AtTime::now();

        // Actual test
        // t0 -> no functions yet
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_1")?
                    .at(t0)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_1")?
                    .at(t1)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2 in history
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_2")?
                    .at(t2)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name(), &FunctionName::try_from("function_2")?);

        // t3 -> no function to retrieve the history
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_2")?
                    .at(t3)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response;
        assert!(response.is_err());

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                FunctionAtIdName::builder()
                    .try_collection(format!("~{}", collection.id()))?
                    .try_function("function_1")?
                    .at(t4)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            );

        let service = FunctionHistoryService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);

        Ok(())
    }
}
