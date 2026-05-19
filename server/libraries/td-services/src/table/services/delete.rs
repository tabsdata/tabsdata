//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::delete::{
    build_deleted_table, build_deleted_triggers, build_frozen_functions,
};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::rest_urls::TableParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, CollectionName, FunctionId, TableId, TableIdName,
    TableVersionId,
};
use td_queries::By;
use td_queries::{insert, insert_vec};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableDeleteService,
    request = DeleteRequest<TableParam>,
    response = (),
    connection = TransactionProvider,
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
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionName>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev>::check),
        // Get table. Extract table id, table version id
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
                { TableDBWithNames::Frozen },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        from_fn(With::<TableDBWithNames>::extract::<TableVersionId>),
        // Insert into function_versions(sql) entries with status=Frozen,
        // for all functions with status=Active that have the table as dependency
        // at the current time.
        from_fn(By::<TableId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>),
        from_fn(With::<Vec<DependencyDB>>::extract::<Vec<FunctionId>>),
        from_fn(
            By::<Vec<FunctionId>>::historic_fetch_all_chunked::<{ FunctionDB::Active }, FunctionDB>
        ),
        from_fn(build_frozen_functions),
        from_fn(insert_vec::<FunctionDB>),
        // Insert into trigger_versions(sql) entries with status=Deleted,
        // for all triggers that have the table as dependency
        // at the current time.
        from_fn(By::<TableId>::historic_fetch_all::<{ TriggerDB::Available }, TriggerDB>),
        from_fn(build_deleted_triggers),
        from_fn(insert_vec::<TriggerDB>),
        // Insert into table_versions(sql) status=Deleted.
        from_fn(By::<TableVersionId>::fetch_one::<TableDB>),
        from_fn(With::<TableDB>::try_convert_to::<TableDBBuilder, _>),
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionRegister, FunctionUpdate};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionIdName, FunctionRuntimeValues, RoleId,
        TableName, TableNameDto, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_delete_table(db: TdPool) {
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
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionName>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev>::check),
                // Get table. Extract table id, table version id
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(
                    &By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
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
                    &By::<TableId>::historic_fetch_all::<{ DependencyDB::Active }, DependencyDB>,
                ),
                type_of_val(&With::<Vec<DependencyDB>>::extract::<Vec<FunctionId>>),
                type_of_val(
                    &By::<Vec<FunctionId>>::historic_fetch_all_chunked::<
                        { FunctionDB::Active },
                        FunctionDB,
                    >,
                ),
                type_of_val(&build_frozen_functions),
                type_of_val(&insert_vec::<FunctionDB>),
                // Insert into trigger_versions(sql) entries with status=Deleted,
                // for all triggers that have the table as dependency
                // at the current time.
                type_of_val(
                    &By::<TableId>::historic_fetch_all::<{ TriggerDB::Available }, TriggerDB>,
                ),
                type_of_val(&build_deleted_triggers),
                type_of_val(&insert_vec::<TriggerDB>),
                // Insert into table_versions(sql) status=Deleted.
                type_of_val(&By::<TableVersionId>::fetch_one::<TableDB>),
                type_of_val(&With::<TableDB>::try_convert_to::<TableDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<TableDBBuilder, _>),
                type_of_val(&build_deleted_table),
                type_of_val(&insert::<TableDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_table(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let _ = db.seed().function(&collection, &create).await?;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                .function(FunctionIdName::from_str("joaquin_workout")?)
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
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
                .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                .table(TableIdName::from_str("super_table")?)
                .build()?,
        );

        let service = TableDeleteService::with_defaults(db.clone()).service();
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
    async fn test_delete_function_with_dependency(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

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

        let _ = db.seed().function(&collection, &create).await?;

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

        let _ = db.seed().function(&collection, &create).await?;

        // Update the function to remove the tables (set it to frozen).
        let update = FunctionUpdate::builder()
            .name(Some("joaquin_workout".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(Some(vec![TableNameDto::try_from("keep_this_one")?]))
            .runtime_values(FunctionRuntimeValues::try_from("mock runtime values")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            FunctionParam::builder()
                .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                .function(FunctionIdName::from_str("joaquin_workout")?)
                .build()?,
            update.clone(),
        );

        let service = UpdateFunctionService::with_defaults(db.clone()).service();
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
                .collection(CollectionIdName::from_str(&format!("~{}", collection.id))?)
                .table(TableIdName::from_str("super_table")?)
                .build()?,
        );

        let service = TableDeleteService::with_defaults(db.clone()).service();
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
