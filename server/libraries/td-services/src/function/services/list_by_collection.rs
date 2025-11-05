//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::defs::CollectionDB;
use td_objects::dxo::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::dxo::function::defs::{Function, FunctionDBWithNames};
use td_objects::rest_urls::params::CollectionAtName;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::id::CollectionId;
use td_objects::types::id_name::CollectionIdName;
use td_objects::types::timestamp::AtTime;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionListByCollectionService,
    request = ListRequest<CollectionAtName>,
    response = ListResponse<Function>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>),
        from_fn(With::<CollectionAtName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // extract attime (natural order)
        from_fn(With::<CollectionAtName>::extract::<AtTime>),
        // list
        from_fn(
            By::<CollectionId>::list_versions_at::<
                CollectionAtName,
                NoListFilter,
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
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, ListParamsBuilder};
    use td_objects::dxo::function::defs::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::id::{AccessTokenId, BundleId, RoleId, UserId};
    use td_objects::types::string::{CollectionName, FunctionName};
    use td_objects::types::typed_enum::Decorator;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_function_versions(db: DbPool) {
        use td_tower::metadata::type_of_val;

        FunctionListByCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<CollectionAtName>, ListResponse<Function>>(&[
                type_of_val(&With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>,
                ),
                type_of_val(&With::<CollectionAtName>::extract::<CollectionIdName>),
                // find collection ID
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // extract attime (natural order)
                type_of_val(&With::<CollectionAtName>::extract::<AtTime>),
                // list
                type_of_val(
                    &By::<CollectionId>::list_versions_at::<
                        CollectionAtName,
                        NoListFilter,
                        { FunctionDBWithNames::Available },
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
                    .try_collection(format!("{}", collection.name))?
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
                    .try_collection(format!("{}", collection.name))?
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
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t0)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 0);

        // t1 -> function_1
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t1)
                    .build()?,
                ListParamsBuilder::default()
                    .order_by("name".to_string())
                    .build()
                    .unwrap(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);

        // t2 -> function_1 and function_2
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t2)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_1")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_2")?);

        // t3 -> function_2 and function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t3)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_2")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_3")?);

        // t4 -> function_3
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t4)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, FunctionName::try_from("function_3")?);

        // t5 -> function_3 and function_5
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionAtName::builder()
                    .try_collection(format!("~{}", collection.id))?
                    .at(t5)
                    .build()?,
                ListParams::default(),
            );

        let service = FunctionListByCollectionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data;

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name, FunctionName::try_from("function_3")?);
        assert_eq!(data[1].name, FunctionName::try_from("function_5")?);
        Ok(())
    }
}
