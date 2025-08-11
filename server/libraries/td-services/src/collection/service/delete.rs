//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::layer::delete::{build_deleted_functions, build_deleted_tables};
use crate::table::layers::delete::{
    build_deleted_dependencies, build_deleted_triggers, build_frozen_functions,
};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, ExtractVecService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{
    insert_vec, By, SqlFindService, SqlSelectAllService, SqlSelectService, SqlUpdateService,
};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, DependencyStatus, FunctionId, FunctionStatus, TableId,
    TableStatus, TriggerStatus,
};
use td_objects::types::collection::{CollectionDB, CollectionDeleteDB, CollectionDeleteDBBuilder};
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::FunctionDB;
use td_objects::types::table::TableDB;
use td_objects::types::trigger::TriggerDB;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = DeleteCollectionService,
    request = DeleteRequest<CollectionParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        // Get collection
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        // Build deleted collection
        from_fn(With::<CollectionDB>::convert_to::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
        // Update collection
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
        // Find tables in the collection
        from_fn(TableStatus::active_or_frozen),
        from_fn(By::<CollectionId>::select_all_versions::<TableDB>),
        from_fn(With::<TableDB>::extract_vec::<TableId>),
        // Find functions that use the tables and freeze them
        from_fn(DependencyStatus::active),
        from_fn(By::<TableId>::find_versions::<DependencyDB>),
        from_fn(With::<DependencyDB>::extract_vec::<FunctionId>),
        from_fn(FunctionStatus::active),
        from_fn(By::<FunctionId>::find_versions::<FunctionDB>),
        from_fn(build_frozen_functions),
        from_fn(insert_vec::<FunctionDB>),
        // Delete tables in the collection
        from_fn(build_deleted_tables),
        from_fn(insert_vec::<TableDB>),
        // Find triggers for the tables in the collection and delete them
        from_fn(TriggerStatus::active_or_frozen),
        from_fn(By::<TableId>::find_versions::<TriggerDB>),
        from_fn(build_deleted_triggers),
        from_fn(insert_vec::<TriggerDB>),
        // Find dependencies for the tables in the collection and delete them
        from_fn(DependencyStatus::active),
        from_fn(By::<TableId>::find_versions::<DependencyDB>),
        from_fn(build_deleted_dependencies),
        from_fn(insert_vec::<DependencyDB>),
        // Find functions in the collection
        from_fn(FunctionStatus::active_or_frozen),
        from_fn(By::<CollectionId>::select_all_versions::<FunctionDB>),
        // Delete functions in the collection (note this will delete possible frozen
        // functions created in the previous step, this is, functions using tables
        // in the same collection)
        from_fn(build_deleted_functions),
        from_fn(insert_vec::<FunctionDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::cte::CteQueries;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        RoleId, TableDependencyDto, TableName, TableNameDto, TableTriggerDto, UserId,
    };
    use td_objects::types::collection::{CollectionCreateDB, CollectionDB};
    use td_objects::types::dependency::DependencyDBWithNames;
    use td_objects::types::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::types::table::TableDBWithNames;
    use td_objects::types::trigger::TriggerDBWithNames;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_service(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeleteCollectionService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<DeleteRequest<CollectionParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>,
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                // Get collection
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                // Build deleted collection
                type_of_val(&With::<CollectionDB>::convert_to::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
                // Update collection
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
                // Find tables in the collection
                type_of_val(&TableStatus::active_or_frozen),
                type_of_val(&By::<CollectionId>::select_all_versions::<TableDB>),
                type_of_val(&With::<TableDB>::extract_vec::<TableId>),
                // Find functions that use the tables and freeze them
                type_of_val(&DependencyStatus::active),
                type_of_val(&By::<TableId>::find_versions::<DependencyDB>),
                type_of_val(&With::<DependencyDB>::extract_vec::<FunctionId>),
                type_of_val(&FunctionStatus::active),
                type_of_val(&By::<FunctionId>::find_versions::<FunctionDB>),
                type_of_val(&build_frozen_functions),
                type_of_val(&insert_vec::<FunctionDB>),
                // Delete tables in the collection
                type_of_val(&build_deleted_tables),
                type_of_val(&insert_vec::<TableDB>),
                // Find triggers for the tables in the collection and delete them
                type_of_val(&TriggerStatus::active_or_frozen),
                type_of_val(&By::<TableId>::find_versions::<TriggerDB>),
                type_of_val(&build_deleted_triggers),
                type_of_val(&insert_vec::<TriggerDB>),
                // Find dependencies for the tables in the collection and delete them
                type_of_val(&DependencyStatus::active),
                type_of_val(&By::<TableId>::find_versions::<DependencyDB>),
                type_of_val(&build_deleted_dependencies),
                type_of_val(&insert_vec::<DependencyDB>),
                // Find functions in the collection
                type_of_val(&FunctionStatus::active_or_frozen),
                type_of_val(&By::<CollectionId>::select_all_versions::<FunctionDB>),
                // Delete functions in the collection (note this will delete possible frozen
                // functions created in the previous step, this is, functions using tables
                // in the same collection)
                type_of_val(&build_deleted_functions),
                type_of_val(&insert_vec::<FunctionDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_collection(db: DbPool) -> Result<(), TdError> {
        // Create 3 functions and 3 tables in the same collection
        let name = CollectionName::try_from("c")?;
        let collection = seed_collection(&db, &name, &UserId::admin()).await;

        for i in 0..3 {
            let create = FunctionRegister::builder()
                .try_name(format!("function_{i}"))?
                .try_description("description")?
                .bundle_id(BundleId::default())
                .try_snippet("snippet")?
                .decorator(Decorator::Publisher)
                .dependencies(None)
                .triggers(None)
                .tables(Some(vec![TableNameDto::try_from(format!("table_{i}"))?]))
                .runtime_values(FunctionRuntimeValues::default())
                .reuse_frozen_tables(false)
                .build()?;
            let _ = seed_function(&db, &collection, &create).await;
        }

        let create = FunctionRegister::builder()
            .try_name("function_3")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(Some(vec![
                TableTriggerDto::try_from("table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ]))
            .tables(Some(vec![TableNameDto::try_from("table_3")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let _ = seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            CollectionParam::builder()
                .try_collection(name.to_string())?
                .build()?,
        );

        DeleteCollectionService::with_defaults(db.clone())
            .await
            .service()
            .await
            .raw_oneshot(request)
            .await?;

        // Assert collection is deleted
        let found: Vec<CollectionDB> = DaoQueries::default()
            .select_by::<CollectionDB>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 0);

        let res: CollectionCreateDB = DaoQueries::default()
            .select_by::<CollectionCreateDB>(&(collection.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(res.name_when_deleted().as_ref().unwrap(), collection.name());

        // Assert functions are deleted
        let found: Vec<FunctionDBWithNames> = DaoQueries::default()
            .select_versions_at::<FunctionDBWithNames>(None, None, &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 4);
        for function in found {
            assert!(matches!(function.status(), FunctionStatus::Deleted));
        }

        // Assert tables are deleted
        let found: Vec<TableDBWithNames> = DaoQueries::default()
            .select_versions_at::<TableDBWithNames>(None, None, &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 4);
        for table in found {
            assert!(matches!(table.status(), TableStatus::Deleted));
        }

        // Assert triggers are deleted
        let found: Vec<TriggerDBWithNames> = DaoQueries::default()
            .select_versions_at::<TriggerDBWithNames>(None, None, &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 2);
        for trigger in found {
            assert!(matches!(trigger.status(), TriggerStatus::Deleted));
        }

        // Assert dependencies are deleted
        let found: Vec<DependencyDBWithNames> = DaoQueries::default()
            .select_versions_at::<DependencyDBWithNames>(None, None, &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        for dep in found {
            assert!(matches!(dep.status(), DependencyStatus::Deleted));
        }

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_delete_collection_downstream_frozen(db: DbPool) -> Result<(), TdError> {
        let name_c_0 = CollectionName::try_from("c_0")?;
        let collection_0 = seed_collection(&db, &name_c_0, &UserId::admin()).await;
        let create = FunctionRegister::builder()
            .try_name("function_0")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("table_0")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let _ = seed_function(&db, &collection_0, &create).await;

        let name_c_1 = CollectionName::try_from("c_1")?;
        let collection_1 = seed_collection(&db, &name_c_1, &UserId::admin()).await;
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("c_0/table_0")?])
            .triggers(vec![
                TableTriggerDto::try_from("c_0/table_0")?,
                TableTriggerDto::try_from("table_1")?,
            ])
            .tables(Some(vec![TableNameDto::try_from("table_1")?]))
            .runtime_values(FunctionRuntimeValues::default())
            .reuse_frozen_tables(false)
            .build()?;
        let _ = seed_function(&db, &collection_1, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            CollectionParam::builder()
                .try_collection(name_c_0.to_string())?
                .build()?,
        );

        DeleteCollectionService::with_defaults(db.clone())
            .await
            .service()
            .await
            .raw_oneshot(request)
            .await?;

        // Assert only c_1 is active
        let found: Vec<CollectionDB> = DaoQueries::default()
            .select_by::<CollectionDB>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name(), &name_c_1);

        // Assert function_1 is frozen
        let found: Vec<FunctionDBWithNames> = DaoQueries::default()
            .select_versions_at::<FunctionDBWithNames>(None, Some(&[&FunctionStatus::Frozen]), &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(*found[0].name(), FunctionName::try_from("function_1")?);

        // Assert table_1 is active still
        let found: Vec<TableDBWithNames> = DaoQueries::default()
            .select_versions_at::<TableDBWithNames>(None, Some(&[&TableStatus::Active]), &())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(*found[0].name(), TableName::try_from("table_1")?);

        Ok(())
    }
}
