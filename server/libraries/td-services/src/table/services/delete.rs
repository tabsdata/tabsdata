//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::delete::{
    build_deleted_table, build_deleted_triggers, build_frozen_functions,
};
use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::collection::defs::CollectionDB;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::dependency::defs::DependencyDB;
use td_objects::dxo::function::defs::FunctionDB;
use td_objects::dxo::table::defs::{TableDB, TableDBBuilder, TableDBWithNames};
use td_objects::dxo::trigger::defs::TriggerDB;
use td_objects::rest_urls::TableParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev};
use td_objects::tower_service::from::{
    ExtractNameService, ExtractService, ExtractVecService, TryIntoService, UpdateService, With,
    combine,
};
use td_objects::tower_service::sql::{
    By, SqlFindService, SqlSelectAllService, SqlSelectService, insert, insert_vec,
};
use td_objects::types::id::{CollectionId, FunctionId, TableId, TableVersionId};
use td_objects::types::id_name::{CollectionIdName, TableIdName};
use td_objects::types::string::CollectionName;
use td_objects::types::timestamp::AtTime;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableDeleteService,
    request = DeleteRequest<TableParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<TableParam>>::extract::<RequestContext>),
        from_fn(With::<DeleteRequest<TableParam>>::extract_name::<TableParam>),
        // Extract collection and table from request.
        from_fn(With::<TableParam>::extract::<CollectionIdName>),
        from_fn(With::<TableParam>::extract::<TableIdName>),
        // Get collection. Extract collection id and name.
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        // Get table. Extract table id, table version id
        from_fn(combine::<CollectionIdName, TableIdName>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<(CollectionIdName, TableIdName)>::select_version::<
                { TableDBWithNames::Frozen },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        from_fn(With::<TableDBWithNames>::extract::<TableVersionId>),
        // Insert into function_versions(sql) entries with status=Frozen,
        // for all functions with status=Active that have the table as dependency
        // at the current time.
        from_fn(By::<TableId>::select_all_versions::<{ DependencyDB::Active }, DependencyDB>),
        from_fn(With::<DependencyDB>::extract_vec::<FunctionId>),
        from_fn(By::<FunctionId>::find_versions::<{ FunctionDB::Active }, FunctionDB>),
        from_fn(build_frozen_functions),
        from_fn(insert_vec::<FunctionDB>),
        // Insert into trigger_versions(sql) entries with status=Deleted,
        // for all triggers that have the table as dependency
        // at the current time.
        from_fn(By::<TableId>::select_all_versions::<{ TriggerDB::Available }, TriggerDB>),
        from_fn(build_deleted_triggers),
        from_fn(insert_vec::<TriggerDB>),
        // Insert into table_versions(sql) status=Deleted.
        from_fn(By::<TableVersionId>::select::<TableDB>),
        from_fn(With::<TableDB>::convert_to::<TableDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TableDBBuilder, _>),
        from_fn(build_deleted_table),
        from_fn(insert::<TableDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use crate::table::services::tests::{assert_delete, assert_not_deleted};
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::defs::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::composed::TableDependencyDto;
    use td_objects::types::id::{AccessTokenId, BundleId, RoleId, UserId};
    use td_objects::types::string::{FunctionRuntimeValues, TableName, TableNameDto};
    use td_objects::types::typed_enum::Decorator;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_delete_table(db: DbPool) {
        use td_tower::metadata::type_of_val;

        TableDeleteService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<TableParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<TableParam>>::extract::<RequestContext>),
                type_of_val(&With::<DeleteRequest<TableParam>>::extract_name::<TableParam>),
                // Extract collection and table from request.
                type_of_val(&With::<TableParam>::extract::<CollectionIdName>),
                type_of_val(&With::<TableParam>::extract::<TableIdName>),
                // Get collection. Extract collection id and name.
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),
                // Get table. Extract table id, table version id
                type_of_val(&combine::<CollectionIdName, TableIdName>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &By::<(CollectionIdName, TableIdName)>::select_version::<
                        { TableDBWithNames::Frozen },
                        TableDBWithNames,
                    >,
                ),
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                type_of_val(&With::<TableDBWithNames>::extract::<TableVersionId>),
                // Insert into function_versions(sql) entries with status=Frozen,
                // for all functions with status=Active that have the table as dependency
                // at the current time.
                type_of_val(
                    &By::<TableId>::select_all_versions::<{ DependencyDB::Active }, DependencyDB>,
                ),
                type_of_val(&With::<DependencyDB>::extract_vec::<FunctionId>),
                type_of_val(&By::<FunctionId>::find_versions::<{ FunctionDB::Active }, FunctionDB>),
                type_of_val(&build_frozen_functions),
                type_of_val(&insert_vec::<FunctionDB>),
                // Insert into trigger_versions(sql) entries with status=Deleted,
                // for all triggers that have the table as dependency
                // at the current time.
                type_of_val(
                    &By::<TableId>::select_all_versions::<{ TriggerDB::Available }, TriggerDB>,
                ),
                type_of_val(&build_deleted_triggers),
                type_of_val(&insert_vec::<TriggerDB>),
                // Insert into table_versions(sql) status=Deleted.
                type_of_val(&By::<TableVersionId>::select::<TableDB>),
                type_of_val(&With::<TableDB>::convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_deleted_table),
                type_of_val(&insert::<TableDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_table(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Test remove tables.
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id))?
                .try_table("super_table")?
                .build()?,
        );

        let service = TableDeleteService::with_defaults(db.clone())
            .service()
            .await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_function_with_dependency(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Create a function with some tables.
        let create = FunctionRegister::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![
                TableNameDto::try_from("super_table")?,
                TableNameDto::try_from("keep_this_one")?,
            ]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Create a function depending on it.
        let create = FunctionRegister::builder()
            .try_name("joaquin_dependant_function")?
            .try_description("joaquin_dependant_function description")?
            .bundle_id(BundleId::default())
            .try_snippet("joaquin_dependant_function snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("super_table")?]))
            .triggers(None)
            .tables(None)
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .try_name("joaquin_workout")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("~{}", collection.id))?
                .try_function("joaquin_workout")?
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Test remove tables.
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("~{}", collection.id))?
                .try_table("super_table")?
                .build()?,
        );

        let service = TableDeleteService::with_defaults(db.clone())
            .service()
            .await;
        service.raw_oneshot(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("super_table")?,
        )
        .await?;

        assert_not_deleted(
            &db,
            &UserId::admin(),
            &collection,
            &TableName::try_from("keep_this_one")?,
        )
        .await?;

        Ok(())
    }
}
