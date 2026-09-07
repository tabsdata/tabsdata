//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::register::{ensure_connection_kind_matches, validate_tables_do_not_exist};
use crate::function::{
    check_private_tables, register_dependencies, register_tables, register_triggers,
    register_workers,
};
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::bundle::{BundleDB, BundleRegisteredDB};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreateDB};
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::function::{Function, FunctionDBWithNames, FunctionRegister};
use td_objects::types::basic::ResetState;
use td_objects::{FromDao, and};
use td_queries::{HistoricEntry, LiveEntry};

#[service_factory]
async fn register_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    bridge: Ctx<BridgeProviderHandle>,
    msgs: &mut ReqCtx,
    db: DbPool<InProject>,
    request: WithinProject<CreateRequest<FunctionRegister, CollectionParam>>,
) -> Result<Function, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, register) = request.into_parts();
    let mut gated_trx = db.begin().await?;
    let grant = authz
        .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    // Bare-table row lock (views with outer joins aren't lockable); the
    // active-view fetch is the liveness check, decided under the lock.
    let locked: CollectionCreateDB = conn.live().fetch_one_for_update(&param.collection).await?;
    let collection: ActiveCollectionDB = conn.live().fetch_one(&locked.id).await?;

    let at = ctx.time;
    // Stage/checkpoint storage prefix: the collection's effective base_path (its
    // own, else the project's — already resolved by the collections_active view).
    let base_path = collection.effective_base_path.clone();

    conn.historic(&at)
        .state(FunctionDBWithNames::available())
        .assert_not_exists(and!(&collection.id, &register.name))
        .await?;

    let function = register.to_dao(&ctx, collection.id, base_path.clone())?;
    // A publisher/subscriber's connector must match the collection's connection kind.
    ensure_connection_kind_matches(conn, &bridge, &function, &collection).await?;
    conn.live().insert(&function).await?;

    // Mark the uploaded bundle as Registered (kept as a durable record, not deleted).
    let bundle_registered = BundleRegisteredDB::registered();
    conn.live()
        .update::<BundleRegisteredDB, BundleDB>(&bundle_registered, function.bundle_id)
        .await?;

    let previous_version_id = None;
    let existing_tables = Vec::new();
    let existing_dependencies = Vec::new();
    let existing_triggers = Vec::new();
    validate_tables_do_not_exist(
        conn,
        &collection.id,
        &collection.name,
        &existing_tables,
        &register.tables,
        &at,
    )
    .await?;
    check_private_tables(&collection.name, &register.dependencies)?;
    check_private_tables(&collection.name, &register.triggers)?;
    // Register never resets state (fresh tables get new ids anyway).
    let reset_state = ResetState::default();
    register_tables(
        conn,
        msgs,
        &ctx,
        &function,
        &collection.id,
        &collection.name,
        &at,
        &existing_tables,
        &register.tables,
        &register.table_types,
        &register.reuse_frozen_tables,
        &reset_state,
    )
    .await?;
    register_dependencies(
        conn,
        &ctx,
        &function,
        &collection.name,
        &at,
        &existing_dependencies,
        &register.dependencies,
    )
    .await?;
    register_triggers(
        conn,
        &ctx,
        &function,
        &collection.name,
        &at,
        &existing_triggers,
        &register.tables,
        &register.triggers,
        &register.dependencies,
    )
    .await?;
    register_workers(
        conn,
        &function,
        &project,
        &previous_version_id,
        &register.workers,
    )
    .await?;

    let with_names: FunctionDBWithNames = conn.live().fetch_one(&function.id).await?;
    let read = Function::from_dao(with_names)?;
    gated_trx.commit().await?;

    // Wake the scheduler so newly registered cron/stage workers start
    // immediately instead of at the next polling tick.
    scheduler_notify.notify_scheduler();

    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::create_collection;
    use crate::function::register::RegisterFunctionError;
    use crate::function::services::tests::assert_register;
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_execution::VersionResolverError;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionUpdate;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, BundleStatus, ConnectionKind, Decorator, FunctionName,
        FunctionRuntimeValues, FunctionStatus, GroupName, ProjectId, SNIPPET_MAX_LEN, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{
        BundleId, CollectionIdName, CollectionName, ReuseFrozen, TableNameDto, WorkerConfig,
        WorkerTypeDto,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.name == *name)
            .expect("matching active function exists");
        Ok(function)
    }

    async fn set_collection_base_path(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &CollectionName,
        base_path: &str,
    ) -> Result<BasePath, TdError> {
        let base_path = BasePath::try_from(base_path)?;
        let update = CollectionUpdate {
            name: None,
            description: None,
            base_path: Some(Some(base_path.clone())),
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                update,
            )
            .in_project(project_id);
        Services::with_defaults(db.clone())
            .update_collection(request)
            .await?;
        Ok(base_path)
    }

    #[td_test::test(db)]
    async fn test_register_empty(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_snippet_larger_than_the_legacy_limit(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let module = format!(
            "import tabsdata as td\n{}def f():\n    return 1\n",
            "# padding\n".repeat(2000)
        );
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: module.as_str().try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await?.data;

        assert_eq!(*response.snippet, module);

        let stored: FunctionDB = db.live().fetch_one(&response.id).await?;
        assert_eq!(*stored.snippet, module);

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_snippet_larger_than_storage_is_trimmed(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let mut module = String::from("import tabsdata as td\n\n\n@td.transformer()\ndef f(df):\n");
        for line in 0..2000 {
            module.push_str(&format!("    value_{line} = df.select('col_{line}')\n"));
        }
        module.push_str("    return value_0\n");
        assert!(module.len() > SNIPPET_MAX_LEN);

        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: module.as_str().try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await?.data;

        let snippet = &*response.snippet;
        assert!(snippet.len() <= SNIPPET_MAX_LEN);
        assert!(snippet.starts_with("import tabsdata as td\n"));
        assert!(snippet.ends_with("    return value_0\n"));
        assert!(snippet.contains(" bytes elided ..."));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_empty_vec(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = Some(vec![]);
        let triggers = Some(vec![]);
        let tables = Some(vec![]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![TableNameDto::try_from("table_foo")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_table_output_duplicate_output_tables_err(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let _collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableNameDto::try_from("table_foo")?,
            TableNameDto::try_from("table_foo")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let err = response.unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        if let RegisterFunctionError::DuplicateTableDefinition(duplicate) = err {
            assert_eq!(duplicate.len(), 1);
            assert_eq!(
                duplicate[0],
                TableNameDto::try_from("table_foo")?.try_into()?
            );
        } else {
            panic!("Unexpected error: {:?}", err);
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_table_dependency(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = Some(vec![TableDependencyDto::try_from("table_foo")?]);
        let triggers = None;
        let tables = Some(vec![TableNameDto::try_from("table_foo")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_none(db: TdPool) -> Result<(), TdError> {
        test_register_trigger(db, None).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_some_empty(db: TdPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![]);
        test_register_trigger(db, test_triggers).await
    }

    #[td_test::test(db)]
    async fn test_register_implicit_trigger_some(db: TdPool) -> Result<(), TdError> {
        let test_triggers = Some(vec![
            TableTriggerDto::try_from("table_1")?,
            TableTriggerDto::try_from("table_2")?,
        ]);
        test_register_trigger(db, test_triggers).await
    }

    async fn test_register_trigger(
        db: TdPool,
        test_triggers: Option<Vec<TableTriggerDto>>,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "function_1 description".try_into()?,
            bundle_id,
            snippet: "function_1 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let _response = service.register_function(request).await?.data;

        let dependencies = Some(vec![
            TableDependencyDto::try_from("table_1")?,
            TableDependencyDto::try_from("table_2")?,
        ]);
        let tables = Some(vec![
            TableNameDto::try_from("output_1")?,
            TableNameDto::try_from("output_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_2".try_into()?,
            description: "function_2 description".try_into()?,
            bundle_id,
            snippet: "function_2 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: test_triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_triggers(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "function_1 description".try_into()?,
            bundle_id,
            snippet: "function_1 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let _response = service.register_function(request).await?.data;

        let dependencies = Some(vec![
            TableDependencyDto::try_from("table_1")?,
            TableDependencyDto::try_from("table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("table_1")?,
            TableTriggerDto::try_from("table_2")?,
        ]);
        let tables = Some(vec![
            TableNameDto::try_from("output_1")?,
            TableNameDto::try_from("output_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_2".try_into()?,
            description: "function_2 description".try_into()?,
            bundle_id,
            snippet: "function_2 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response?.data;

        assert_register(&db, &UserId::admin(), &collection, &create, &response).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_dependencies_different_collections(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_register_tables_triggers_different_collections(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_register_tables_dependencies_triggers_different_collections(db, false, true).await
    }

    // Within a project there is no cross-collection permission: every
    // collection's public (non-private) tables are usable from any function in
    // the project. Referencing another collection's public tables as
    // dependencies/triggers always succeeds.
    async fn test_register_tables_dependencies_triggers_different_collections(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let _collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_2, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = Some(vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "function_1 description".try_into()?,
            bundle_id,
            snippet: "function_1 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_1.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let _response = service.register_function(request).await?.data;

        let dependencies = Some(vec![]);
        let triggers = Some(vec![]);
        let tables = Some(vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_3".try_into()?,
            description: "function_3 description".try_into()?,
            bundle_id,
            snippet: "function_3 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_2.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let _ = service.register_function(request).await?.data;

        let deps_collection = if deps_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };
        let triggers_collection = if triggers_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };

        let dependencies = Some(vec![
            TableDependencyDto::try_from(format!("{deps_collection}/table_1"))?,
            TableDependencyDto::try_from(format!("{deps_collection}/table_2"))?,
            TableDependencyDto::try_from("collection_2/output_1")?,
            TableDependencyDto::try_from("output_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("collection_1/table_1")?,
            TableTriggerDto::try_from(format!("{triggers_collection}/table_2"))?,
        ]);
        let tables = Some(vec![
            TableNameDto::try_from("output_1")?,
            TableNameDto::try_from("output_2")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_2".try_into()?,
            description: "function_2 description".try_into()?,
            bundle_id,
            snippet: "function_2 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_2.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;

        assert_register(&db, &UserId::admin(), &collection_2, &create, &res?.data).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_same_name(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request.clone()).await;
        let _ = response?;

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let response = response;
        let err = response.err().unwrap();
        assert!(matches!(
            err.domain_err(),
            SqlError::EntityAlreadyExists(_, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_all_same_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_deps_diff_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_register_private_tables_triggers_diff_collection(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_private_tables(db, false, true).await
    }

    async fn test_private_tables(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_2, &UserId::admin())
            .await?;

        for collection in [&collection_1, &collection_2] {
            let dependencies = None;
            let triggers = None;
            let tables = Some(vec![
                TableNameDto::try_from("_table_1")?,
                TableNameDto::try_from("_table_2")?,
            ]);

            let bundle_id = BundleId::default();
            let create = FunctionRegister {
                name: "function_1".try_into()?,
                description: "function_1 description".try_into()?,
                bundle_id,
                snippet: "function_1 snippet".try_into()?,
                decorator: Decorator::Transformer,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: dependencies.clone(),
                triggers: triggers.clone(),
                tables: tables.clone(),
                table_types: None,
                runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            db.scoped(p.id).seed().function(collection, &create).await?;
        }

        let deps_collection = if deps_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };
        let triggers_collection = if triggers_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };

        let dependencies = Some(vec![
            TableDependencyDto::try_from(format!("{deps_collection}/_table_1"))?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from(format!("{triggers_collection}/_table_1"))?,
            TableTriggerDto::try_from("_table_2")?,
        ]);
        let tables = Some(vec![TableNameDto::try_from("output_1")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function_2".try_into()?,
            description: "function_2 description".try_into()?,
            bundle_id,
            snippet: "function_2 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_2.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;

        if deps_diff_collection || triggers_diff_collection {
            assert!(res.is_err());
            assert!(res.err().unwrap().code().contains("PrivateTableError"))
        } else {
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_private_table_flag(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let tables = Some(vec![
            TableNameDto::try_from("table0")?,
            TableNameDto::try_from("_table0")?,
        ]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![]),
            triggers: Some(vec![]),
            tables,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_1.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = db.live().fetch_all::<TableDB>(&(res.unwrap().id)).await?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.name.to_string(), table))
            .collect();
        assert!(!*map.get("table0").unwrap().private);
        assert!(*map.get("_table0").unwrap().private);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_dependency_fixed_validation(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004",
            )?]),
            triggers: Some(vec![]),
            tables: Some(vec![TableNameDto::try_from("table_0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_1.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(
            err,
            VersionResolverError::FixedTableDataVersionsNotFound(_)
        ));

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from(
                "table_0@00000000000000000000000004..00000000000000000000000004",
            )?]),
            triggers: Some(vec![]),
            tables: Some(vec![TableNameDto::try_from("table_0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_1.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(
            err,
            VersionResolverError::FixedTableDataVersionsNotFound(_)
        ));

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_dependency_range_validation(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0@HEAD..HEAD~2")?]),
            triggers: Some(vec![]),
            tables: Some(vec![TableNameDto::try_from("table_0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name_1.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<VersionResolverError>();
        assert!(matches!(err, VersionResolverError::InvalidRange(_)));

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_workers_created(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])),
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await?.data;

        let workers: Vec<WorkerDB> = db.live().fetch_all::<WorkerDB>(&res.id).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, res.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::Scheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_multiple_workers_error(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: Some(HashMap::from([
                (WorkerTypeDto::CronTrigger, WorkerConfig::default()),
                (WorkerTypeDto::StageTrigger, WorkerConfig::default()),
            ])),
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.register_function(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(format!("{err}").contains("Multiple workers are not supported."));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_stamps_stage_base_path_from_collection(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("prod_coll")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let prod = set_collection_base_path(&db, p.id, &collection_name, "/prod").await?;

        let create = FunctionRegister {
            name: "stamped_fn".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        service.register_function(request).await?;

        let function = active_function(&db, &FunctionName::try_from("stamped_fn")?).await?;
        assert_eq!(
            function.stage_base_path, prod,
            "stage_base_path is stamped from the collection base_path at register"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_flips_bundle_to_registered(db: TdPool) -> Result<(), TdError> {
        use crate::Services;
        use axum::body::Body;
        use axum::extract::Request;
        use td_objects::dxo::function_upload::FunctionUpload;

        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Services::with_defaults(db.clone());

        // Upload a bundle so a Pending BundleDB row exists for the function to claim.
        let upload_request = Request::builder()
            .body(Body::from(b"PAYLOAD".to_vec()))
            .unwrap();
        let upload = FunctionUpload::new(upload_request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                upload,
            )
            .in_project(p.id);
        let bundle = context.clone().upload_function(request).await?.data;
        assert_eq!(
            db.live().fetch_all::<BundleDB>(&bundle.id).await?[0].status,
            BundleStatus::Pending,
            "bundle starts Pending right after upload"
        );

        let create = FunctionRegister {
            name: "flip_fn".try_into()?,
            description: "description".try_into()?,
            bundle_id: bundle.id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        service.register_function(request).await?;

        // The bundle the function claimed is flipped Pending -> Registered.
        let function = active_function(&db, &FunctionName::try_from("flip_fn")?).await?;
        let bundles: Vec<BundleDB> = db.live().fetch_all::<BundleDB>(&function.bundle_id).await?;
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].status, BundleStatus::Registered);
        assert_eq!(bundles[0].collection_id, collection.id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_base_path_override(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("prod_coll")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Non-default collection base_path so we can tell override from stamping.
        let prod = set_collection_base_path(&db, p.id, &collection_name, "/prod").await?;

        let create = FunctionRegister {
            name: "override_fn".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: Some(BasePath::try_from("/fn")?),
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        service.register_function(request).await?;

        let function = active_function(&db, &FunctionName::try_from("override_fn")?).await?;
        assert_eq!(
            function.base_path,
            Some(BasePath::try_from("/fn")?),
            "base_path override is persisted on the function version"
        );
        assert_eq!(
            function.stage_base_path, prod,
            "stage_base_path still comes from the collection, not the override"
        );
        Ok(())
    }

    fn pubsub_register(
        name: &str,
        decorator: Decorator,
        connection: Option<&str>,
    ) -> Result<FunctionRegister, TdError> {
        let mut create = FunctionRegister {
            name: name.try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        if let Some(connection) = connection {
            create.connection = Some(ConnectionKind::try_from(connection)?);
        }
        Ok(create)
    }

    async fn register_in(
        db: &TdPool,
        project_id: ProjectId,
        collection: &str,
        create: FunctionRegister,
    ) -> Result<Function, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                create,
            )
            .in_project(project_id);
        Services::with_defaults(db.clone())
            .register_function(request)
            .await
            .map(|response| response.data)
    }

    /// Like [`register_in`] but runs the service under the Kubernetes
    /// orchestrator (where a publisher/subscriber connection kind is
    /// mandatory) rather than the default local Supervisor.
    async fn register_in_kubernetes(
        db: &TdPool,
        project_id: ProjectId,
        collection: &str,
        create: FunctionRegister,
    ) -> Result<Function, TdError> {
        use std::sync::Arc;
        use td_bridge::test::TestBridgeProvider;
        use td_bridge::{BridgeProviderHandle, Orchestrator};

        let mut context = Services::with_defaults(db.clone());
        context.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
        )));
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                create,
            )
            .in_project(project_id);
        context
            .clone()
            .register_function(request)
            .await
            .map(|response| response.data)
    }

    #[td_test::test(db)]
    async fn test_register_publisher_matching_connector_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_pub", Some(GroupName::source())).await;
        let create = pubsub_register("pub_fn", Decorator::Publisher, Some("mysqlSrcConn"))?;
        register_in_kubernetes(&db, p.id, "src_pub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("pub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Publisher);
        assert_eq!(
            function.connection,
            Some(ConnectionKind::try_from("mysqlSrcConn")?)
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_subscriber_matching_connector_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "dst_sub", Some(GroupName::destination())).await;
        let create = pubsub_register("sub_fn", Decorator::Subscriber, Some("mysqlDestConn"))?;
        register_in_kubernetes(&db, p.id, "dst_sub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("sub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Subscriber);
        assert_eq!(
            function.connection,
            Some(ConnectionKind::try_from("mysqlDestConn")?)
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_connector_mismatch_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_mm", Some(GroupName::source())).await;
        let create = pubsub_register("mm_fn", Decorator::Publisher, Some("postgresSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "src_mm", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindMismatch(..)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_requires_connector_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // Under Kubernetes a publisher must declare its connection kind.
        create_collection(&db, p.id, "src_req", Some(GroupName::source())).await;
        let create = pubsub_register("req_fn", Decorator::Publisher, None)?;
        let err = register_in_kubernetes(&db, p.id, "src_req", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindRequired(_)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_publisher_without_connection_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // A plain (DataFactory) collection carries no connection.
        let collection_name = CollectionName::try_from("fac_pub")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = pubsub_register("facpub_fn", Decorator::Publisher, Some("mysqlSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "fac_pub", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::CollectionHasNoConnection(_)
        ));
        Ok(())
    }

    // TODO FOR 2.0: remove together with the Supervisor exemption in
    // `ensure_connection_kind_matches`. Pins the 1.x behaviour: under the
    // local Supervisor a publisher registers with no connection kind (the
    // legacy `tabsdata` client never sends one); under Kubernetes the same
    // registration is rejected -- see
    // `test_register_publisher_requires_connector_err`.
    #[td_test::test(db)]
    async fn test_register_publisher_legacy_no_connection_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        // A plain (DataFactory) collection carries no connection.
        let collection_name = CollectionName::try_from("fac_legacy_pub")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = pubsub_register("legacy_pub_fn", Decorator::Publisher, None)?;
        register_in(&db, p.id, "fac_legacy_pub", create).await?;

        let function = active_function(&db, &FunctionName::try_from("legacy_pub_fn")?).await?;
        assert_eq!(function.decorator, Decorator::Publisher);
        assert_eq!(function.connection, None);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_transformer_with_connector_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_tf", Some(GroupName::source())).await;
        let create = pubsub_register("tf_fn", Decorator::Transformer, Some("mysqlSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "src_tf", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindNotAllowed(_)
        ));
        Ok(())
    }
}
