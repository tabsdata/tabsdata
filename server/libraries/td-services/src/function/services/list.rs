//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::rest_urls::AtTimeParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, TryIntoService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::basic::{
    AtTime, FunctionStatus, VisibleCollections, VisibleFunctionsCollections,
};
use td_objects::types::function::Function;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = FunctionListService,
    request = ListRequest<AtTimeParam>,
    response = ListResponse<Function>,
    connection = ConnectionProvider,
    context = DaoQueries,
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
        from_fn(With::<VisibleCollections>::convert_to::<VisibleFunctionsCollections, _>),
        // list
        from_fn(FunctionStatus::active_or_frozen),
        from_fn(By::<()>::list_versions_at::<AtTimeParam, VisibleFunctionsCollections, Function>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::delete::DeleteFunctionService;
    use crate::function::services::update::UpdateFunctionService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{ListParams, ListParamsBuilder, RequestContext};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::seed_user_role;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, Description, FunctionName,
        RoleId, RoleName, UserEnabled, UserId, UserName,
    };
    use td_objects::types::function::{FunctionRegister, FunctionUpdate};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_function_versions(db: DbPool) {
        use td_tower::metadata::type_of_val;

        FunctionListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<AtTimeParam>, ListResponse<Function>>(&[
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<AtTimeParam>>::extract_name::<AtTimeParam>),
                type_of_val(&With::<AtTimeParam>::extract::<AtTime>),
                // get allowed collections
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::visible_collections),
                // convert them to allowed function collections
                type_of_val(&With::<VisibleCollections>::convert_to::<VisibleFunctionsCollections, _>),
                // list
                type_of_val(&FunctionStatus::active_or_frozen),
                type_of_val(
                    &By::<()>::list_versions_at::<
                        AtTimeParam,
                        VisibleFunctionsCollections,
                        Function,
                    >,
                ),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_function_versions(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

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
        let _ = seed_function(&db, &collection, &create).await;

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
        let _ = seed_function(&db, &collection, &create).await;

        let t2 = AtTime::now();

        // Update function_1 to function_3
        let update = FunctionUpdate::builder()
            .try_name("function_3")?
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

        let t3 = AtTime::now();

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
        let _ = seed_function(&db, &collection, &create).await;

        let t5 = AtTime::now();

        // Actual test
        // t0 -> no functions
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t0).build()?,
                ListParams::default(),
            );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

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

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t2).build()?,
                ListParams::default(),
            );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name(), &FunctionName::try_from("function_2")?);

        // t3 -> function_2 and function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t3).build()?,
                ListParams::default(),
            );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_2")?);
        assert_eq!(data[1].name(), &FunctionName::try_from("function_3")?);

        // t4 -> function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t4).build()?,
                ListParams::default(),
            );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_3")?);

        // t5 -> function_3 and function_5
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                AtTimeParam::builder().at(t5).build()?,
                ListParams::default(),
            );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_3")?);
        assert_eq!(data[1].name(), &FunctionName::try_from("function_5")?);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_function_versions_without_permissions(db: DbPool) -> Result<(), TdError> {
        // Create new role without permissions
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin")?,
            &UserEnabled::from(true),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("unauthorized_role")?,
            Description::try_from("any user")?,
        )
        .await;
        let _user_role = seed_user_role(&db, user.id(), role.id()).await;

        // Create collection
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

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
        let _ = seed_function(&db, &collection, &create).await;

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
        let _ = seed_function(&db, &collection, &create).await;

        // All functions should be listed for authorized user
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .list(
            AtTimeParam::builder().at(AtTime::default()).build()?,
            ListParams::default(),
        );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name(), &FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name(), &FunctionName::try_from("function_2")?);

        // No functions should be listed for unauthorized user
        let request = RequestContext::with(AccessTokenId::default(), user.id(), role.id()).list(
            AtTimeParam::builder().at(AtTime::default()).build()?,
            ListParams::default(),
        );

        let service = FunctionListService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 0);
        Ok(())
    }
}
