//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::lifecycle::{reconcile_dq_summary_trend_children, register_one};
use crate::function::register::ensure_no_dq_summary_trends_table_types;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::FromDao;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDB};
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::function::{Function, FunctionDBWithNames, FunctionRegister};
use td_queries::Queries;

#[service_factory]
async fn register_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    bridge: Ctx<BridgeProviderHandle>,
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
    let (locked, at) = conn
        .for_update()
        .serialize_on::<CollectionDB>(&param.collection)
        .await?;
    let collection: ActiveCollectionDB = conn.fetch_one(&locked.base.id).await?;

    ensure_no_dq_summary_trends_table_types(&register.table_types)?;
    let function = register_one(conn, &ctx, &bridge, &collection, &register, None, at).await?;
    reconcile_dq_summary_trend_children(
        conn,
        &ctx,
        &bridge,
        &collection,
        &function,
        &register.child_functions,
        register.reuse_frozen_tables,
        at,
    )
    .await?;

    let with_names: FunctionDBWithNames = conn.timeline().fetch_one(&function.id).await?;
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
    use td_objects::and;
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::CollectionUpdate;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::DQ_SUMMARY_TRENDS_FUNCTION_SUFFIX;
    use td_objects::dxo::function::{ChildFunction, FunctionDB, FunctionParam, FunctionUpdate};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, BundleStatus, CollectionType, ConnectionKind, Decorator, Enabled,
        FunctionIdName, FunctionName, FunctionRuntimeValues, FunctionStatus, FunctionType,
        GroupName, Parent, ProjectId, SNIPPET_MAX_LEN, TableName, TableStatus, TableType,
        TriggerStatus, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{
        BundleId, CollectionIdName, CollectionName, ReuseFrozen, TableNameDto, WorkerConfig,
        WorkerTypeDto,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.base.name == *name)
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
            child_functions: None,
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
            child_functions: None,
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

        let stored: FunctionDB = db.timeline().fetch_one(&response.id).await?;
        assert_eq!(*stored.base.snippet, module);

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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
    async fn test_register_adopts_frozen_table_of_deleted_function(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let producer = FunctionRegister {
            child_functions: None,
            name: "producer".try_into()?,
            description: "producer description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "producer snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("table_foo")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(p.id)
            .seed()
            .function(&collection, &producer)
            .await?;

        let consumer = FunctionRegister {
            child_functions: None,
            name: "consumer".try_into()?,
            description: "consumer description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "consumer snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_foo")?]),
            triggers: Some(vec![TableTriggerDto::try_from("table_foo")?]),
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let consumer_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &consumer)
            .await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("producer")?,
                },
            ))
            .in_project(p.id);
        service.delete_function(request).await?;

        let frozen: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(TableName::try_from("table_foo")?, TableStatus::Frozen))
            .await?;
        assert_eq!(frozen.len(), 1);
        let table_id = frozen[0].base.table_id;

        let triggers: Vec<TriggerDB> = db
            .scd2()
            .fetch_all::<TriggerDB>(&consumer_function.base.function_id)
            .await?;
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].base.status, TriggerStatus::Frozen);

        let successor = FunctionRegister {
            child_functions: None,
            name: "successor".try_into()?,
            ..producer.clone()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                successor.clone(),
            )
            .in_project(p.id);
        let error = service.register_function(request).await.unwrap_err();
        assert!(matches!(
            error.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::FrozenTableAlreadyExists(..)
        ));

        let successor = FunctionRegister {
            child_functions: None,
            reuse_frozen_tables: ReuseFrozen::TRUE,
            ..successor
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                successor.clone(),
            )
            .in_project(p.id);
        let registered = service.register_function(request).await?.data;

        let tables: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(TableName::try_from("table_foo")?, TableStatus::Active))
            .await?;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].base.table_id, table_id);
        assert_eq!(tables[0].base.function_id, registered.function_id);

        let triggers: Vec<TriggerDB> = db
            .scd2()
            .fetch_all::<TriggerDB>(&consumer_function.base.function_id)
            .await?;
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].base.status, TriggerStatus::Active);
        assert_eq!(
            *triggers[0].base.trigger_by_function_id,
            registered.function_id
        );
        Ok(())
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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

    // Within a project there is no cross-collection permission: any function
    // may reference another collection's public tables as dependencies/triggers.
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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
                child_functions: None,
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
            child_functions: None,
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
            child_functions: None,
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

        let tables: Vec<TableDB> = db
            .timeline()
            .fetch_all::<TableDB>(&(res.unwrap().id))
            .await?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.base.name.to_string(), table))
            .collect();
        assert!(!*map.get("table0").unwrap().base.private);
        assert!(*map.get("_table0").unwrap().base.private);
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
            child_functions: None,
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from(
                "table_0@dv20260101-9999999999",
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
            child_functions: None,
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from(
                "table_0@dv20260101-9999999999..dv20260101-9999999999",
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
            child_functions: None,
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
            child_functions: None,
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

        let workers: Vec<WorkerDB> = db.fetch_all::<WorkerDB>(&res.id).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, res.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::Scheduled);
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));
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
            child_functions: None,
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
            child_functions: None,
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
            function.base.stage_base_path, prod,
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
            db.fetch_all::<BundleDB>(&bundle.id).await?[0].base.status,
            BundleStatus::Pending,
            "bundle starts Pending right after upload"
        );

        let create = FunctionRegister {
            child_functions: None,
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

        let function = active_function(&db, &FunctionName::try_from("flip_fn")?).await?;
        let bundles: Vec<BundleDB> = db.fetch_all::<BundleDB>(&function.base.bundle_id).await?;
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].base.status, BundleStatus::Registered);
        assert_eq!(bundles[0].base.collection_id, collection.id);
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
            child_functions: None,
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
            function.base.base_path,
            Some(BasePath::try_from("/fn")?),
            "base_path override is persisted on the function version"
        );
        assert_eq!(
            function.base.stage_base_path, prod,
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
            child_functions: None,
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
        assert_eq!(function.base.decorator, Decorator::Publisher);
        assert_eq!(
            function.base.connection,
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
        assert_eq!(function.base.decorator, Decorator::Subscriber);
        assert_eq!(
            function.base.connection,
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
        assert_eq!(function.base.decorator, Decorator::Publisher);
        assert_eq!(function.base.connection, None);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_transformer_with_connector_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("fac_tf")?;
        db.scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = pubsub_register("tf_fn", Decorator::Transformer, Some("mysqlSrcConn"))?;
        let err = register_in_kubernetes(&db, p.id, "fac_tf", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindNotAllowed(_)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_transformer_in_source_collection_err(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_tf", Some(GroupName::source())).await;
        let create = pubsub_register("src_tf_fn", Decorator::Transformer, None)?;
        let err = register_in_kubernetes(&db, p.id, "src_tf", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::TransformerInConnectedCollection(_, CollectionType::Source)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_transformer_in_destination_collection_err(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "dst_tf", Some(GroupName::destination())).await;
        let create = pubsub_register("dst_tf_fn", Decorator::Transformer, None)?;
        let err = register_in_kubernetes(&db, p.id, "dst_tf", create)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::TransformerInConnectedCollection(_, CollectionType::Destination)
        ));
        Ok(())
    }

    const OWNER: &str = "trend_owner_fn";

    /// An owner declaring one trend child, registered under Kubernetes where
    /// the admission contract actually runs. A request cannot name a class or
    /// a parent, so the reconcile is the only way a trend function is born.
    async fn register_owner_with_trend(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
        group: Option<GroupName>,
        decorator: Decorator,
        connection: Option<&str>,
    ) -> Result<Function, TdError> {
        create_collection(db, project, collection, group).await;
        let mut create = pubsub_register(OWNER, decorator, connection)?;
        create.tables = Some(vec![TableNameDto::try_from("owned")?]);
        create.child_functions = Some(vec![child("trend")?]);
        register_in_kubernetes(db, project, collection, create).await
    }

    #[td_test::test(db)]
    async fn test_dq_trend_transformer_in_data_factory_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let owner =
            register_owner_with_trend(&db, p.id, "fct_trend", None, Decorator::Transformer, None)
                .await?;

        let trend = live_by_name(&db, &child_name("trend"))
            .await?
            .expect("registered");
        assert_eq!(trend.base.function_type, FunctionType::DQSummaryTrends);
        assert_eq!(
            trend.base.parent_function_id,
            Some(Parent::from(owner.function_id))
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_dq_trend_transformer_in_source_collection_ok(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        register_owner_with_trend(
            &db,
            p.id,
            "src_trend",
            Some(GroupName::source()),
            Decorator::Publisher,
            Some("mysqlSrcConn"),
        )
        .await?;

        let trend = live_by_name(&db, &child_name("trend"))
            .await?
            .expect("registered");
        assert_eq!(trend.base.function_type, FunctionType::DQSummaryTrends);
        Ok(())
    }

    /// The exemption is for sources only: a trend transformer follows the
    /// publisher whose summaries it reads, and no publisher lives here.
    #[td_test::test(db)]
    async fn test_dq_trend_transformer_in_destination_collection_err(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let err = register_owner_with_trend(
            &db,
            p.id,
            "dst_trend",
            Some(GroupName::destination()),
            Decorator::Subscriber,
            Some("mysqlDestConn"),
        )
        .await
        .unwrap_err();

        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::TrendChildInUnsupportedCollection(
                _,
                CollectionType::Destination
            )
        ));
        assert!(live_by_name(&db, &child_name("trend")).await?.is_none());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_regular_transformer_in_a_source_collection_is_still_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_regular", Some(GroupName::source())).await;
        let mut create = pubsub_register("plain_tfr", Decorator::Transformer, None)?;
        create.tables = Some(vec![TableNameDto::try_from("plain")?]);

        let err = register_in_kubernetes(&db, p.id, "src_regular", create)
            .await
            .unwrap_err();

        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::TransformerInConnectedCollection(_, CollectionType::Source)
        ));
        Ok(())
    }

    async fn register_err(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
        create: FunctionRegister,
    ) -> Result<TdError, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                create,
            )
            .in_project(project);
        let service = Services::with_defaults(db.clone());
        Ok(service.register_function(request).await.unwrap_err())
    }

    async fn register_ok(
        db: &TdPool,
        project: ProjectId,
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
            .in_project(project);
        let service = Services::with_defaults(db.clone());
        Ok(service.register_function(request).await?.data)
    }

    /// The link is stored as the parent's *logical* id, so renaming the owner
    /// does not break it and the child's next version carries it forward.
    #[td_test::test(db)]
    async fn test_parent_function_id_is_stable_across_writes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "parent_stable", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?]))?;
        let owner = register_ok(&db, p.id, "parent_stable", create).await?;
        let expected = Some(Parent::from(owner.function_id));
        let before = live_by_name(&db, &child_name("a"))
            .await?
            .expect("child registered");
        assert_eq!(before.base.parent_function_id, expected);

        let mut update = owner_update(Some(vec![child("a")?]));
        update.name = Some("owner_renamed".try_into()?);
        update_ok(&db, p.id, "parent_stable", CHILD_OWNER, update).await?;

        let after = live_by_name(&db, &child_name("a"))
            .await?
            .expect("child still registered");
        assert_ne!(after.base.id, before.base.id);
        assert_eq!(after.base.parent_function_id, expected);
        Ok(())
    }

    const CHILD_OWNER: &str = "child_owner";

    fn child(table: &str) -> Result<ChildFunction, TdError> {
        Ok(ChildFunction {
            function_type: FunctionType::DQSummaryTrends,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            dependencies: None,
            tables: Some(vec![TableNameDto::try_from(table)?]),
        })
    }

    fn child_name(table: &str) -> String {
        format!("{table}{DQ_SUMMARY_TRENDS_FUNCTION_SUFFIX}")
    }

    fn owner_register(
        tables: &[&str],
        child_functions: Option<Vec<ChildFunction>>,
    ) -> Result<FunctionRegister, TdError> {
        let mut create = pubsub_register(CHILD_OWNER, Decorator::Transformer, None)?;
        create.tables = Some(
            tables
                .iter()
                .map(|t| TableNameDto::try_from(*t))
                .collect::<Result<Vec<_>, _>>()?,
        );
        create.child_functions = child_functions;
        Ok(create)
    }

    async fn update_ok(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
        function: &str,
        update: FunctionUpdate,
    ) -> Result<Function, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection)?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
                    },
                ),
                update,
            )
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .update_function(request)
            .await?
            .data)
    }

    async fn update_err(
        db: &TdPool,
        project: ProjectId,
        collection: &str,
        function: &str,
        update: FunctionUpdate,
    ) -> Result<TdError, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection)?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
                    },
                ),
                update,
            )
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .update_function(request)
            .await
            .expect_err("update should have been refused"))
    }

    async fn live_by_name(db: &TdPool, name: &str) -> Result<Option<FunctionDB>, TdError> {
        let rows: Vec<FunctionDB> = db
            .scd2()
            .fetch_all::<FunctionDB>(&FunctionName::try_from(name)?)
            .await?;
        Ok(rows.into_iter().next())
    }

    fn owner_update(child_functions: Option<Vec<ChildFunction>>) -> FunctionUpdate {
        owner_update_reusing(child_functions, ReuseFrozen::TRUE)
    }

    fn owner_update_reusing(
        child_functions: Option<Vec<ChildFunction>>,
        reuse_frozen_tables: ReuseFrozen,
    ) -> FunctionUpdate {
        FunctionUpdate {
            child_functions,
            reuse_frozen_tables,
            ..Default::default()
        }
    }

    #[td_test::test(db)]
    async fn test_register_with_a_child_function_registers_it(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd", None).await;
        let create = owner_register(&["owned"], Some(vec![child("m")?]))?;
        let owner = register_ok(&db, p.id, "mgd", create).await?;

        let child = live_by_name(&db, &child_name("m"))
            .await?
            .expect("child registered");
        assert_eq!(child.base.function_type, FunctionType::DQSummaryTrends);
        assert_eq!(
            child.base.parent_function_id,
            Some(Parent::from(owner.function_id))
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_register_with_two_child_functions(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd2", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?, child("b")?]))?;
        register_ok(&db, p.id, "mgd2", create).await?;

        assert!(live_by_name(&db, &child_name("a")).await?.is_some());
        assert!(live_by_name(&db, &child_name("b")).await?.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_table_too_long_to_name_a_trend_function_is_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "tt_long", None).await;
        let table = "t".repeat(100 - DQ_SUMMARY_TRENDS_FUNCTION_SUFFIX.len() + 1);
        let create = owner_register(&["owned"], Some(vec![child(&table)?]))?;

        let err = register_err(&db, p.id, "tt_long", create).await?;
        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::ChildFunctionNameTooLong(_, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_trend_function_name_already_taken_is_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "tt_taken", None).await;
        let mut squatter = pubsub_register(&child_name("a"), Decorator::Transformer, None)?;
        squatter.tables = Some(vec![TableNameDto::try_from("squatted")?]);
        register_ok(&db, p.id, "tt_taken", squatter).await?;

        let create = owner_register(&["owned"], Some(vec![child("a")?]))?;
        let err = register_err(&db, p.id, "tt_taken", create).await?;
        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::ChildFunctionNameTaken(_, _, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_function_cannot_type_its_own_table_as_a_trend(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "tt_dqst", None).await;
        let mut create = owner_register(&["owned"], None)?;
        create.table_types = Some(HashMap::from([(
            TableNameDto::try_from("owned")?,
            TableType::DQSummaryTrends,
        )]));

        let err = register_err(&db, p.id, "tt_dqst", create).await?;
        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::DQSummaryTrendsTableTypeNotAllowed(_, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_function_may_type_its_own_data_quality_tables(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "tt_dqs", None).await;
        let mut create = owner_register(&["owned"], None)?;
        create.table_types = Some(HashMap::from([(
            TableNameDto::try_from("owned")?,
            TableType::DQSummary,
        )]));

        register_ok(&db, p.id, "tt_dqs", create).await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_trend_childs_tables_are_typed_as_trends(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "tt_child", None).await;
        let declared = child("t")?;
        let create = owner_register(&["owned"], Some(vec![declared]))?;
        register_ok(&db, p.id, "tt_child", create).await?;

        let child = live_by_name(&db, &child_name("t"))
            .await?
            .expect("child registered");
        let tables: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(
                &child.base.function_id,
                TableName::try_from("t")?,
                TableDB::active()
            ))
            .await?;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].base.table_type, TableType::DQSummaryTrends);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_child_of_a_disabled_owner_is_registered_disabled(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_off", None).await;
        let mut create = owner_register(&["owned"], Some(vec![child("d")?]))?;
        create.enabled = Some(Enabled::FALSE);
        register_ok(&db, p.id, "mgd_off", create).await?;

        let child = live_by_name(&db, &child_name("d"))
            .await?
            .expect("child registered");
        assert_eq!(child.base.enabled, Enabled::FALSE);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_updating_the_owner_re_enables_its_child(db: TdPool) -> Result<(), TdError> {
        // The child cannot be toggled directly, so an owner that came back on
        // without carrying its child would strand it disabled for good.
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_back", None).await;
        let mut create = owner_register(&["owned"], Some(vec![child("r")?]))?;
        create.enabled = Some(Enabled::FALSE);
        register_ok(&db, p.id, "mgd_back", create).await?;
        assert_eq!(
            live_by_name(&db, &child_name("r"))
                .await?
                .expect("child")
                .base
                .enabled,
            Enabled::FALSE
        );

        let update = FunctionUpdate {
            enabled: Some(Enabled::TRUE),
            ..owner_update(Some(vec![child("r")?]))
        };
        update_ok(&db, p.id, "mgd_back", CHILD_OWNER, update).await?;

        assert_eq!(
            live_by_name(&db, &child_name("r"))
                .await?
                .expect("child")
                .base
                .enabled,
            Enabled::TRUE
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_disabling_the_owner_through_an_update_disables_its_child(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_down", None).await;
        let create = owner_register(&["owned"], Some(vec![child("x")?]))?;
        register_ok(&db, p.id, "mgd_down", create).await?;
        assert_eq!(
            live_by_name(&db, &child_name("x"))
                .await?
                .expect("child")
                .base
                .enabled,
            Enabled::TRUE
        );

        let update = FunctionUpdate {
            enabled: Some(Enabled::FALSE),
            ..owner_update(Some(vec![child("x")?]))
        };
        update_ok(&db, p.id, "mgd_down", CHILD_OWNER, update).await?;

        assert_eq!(
            live_by_name(&db, &child_name("x"))
                .await?
                .expect("child")
                .base
                .enabled,
            Enabled::FALSE
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_dropping_a_child_function_deletes_it(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_drop", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?, child("b")?]))?;
        register_ok(&db, p.id, "mgd_drop", create).await?;

        let update = owner_update(Some(vec![child("a")?]));
        update_ok(&db, p.id, "mgd_drop", CHILD_OWNER, update).await?;

        assert!(live_by_name(&db, &child_name("a")).await?.is_some());
        assert!(live_by_name(&db, &child_name("b")).await?.is_none());
        Ok(())
    }

    /// The trend table survives the trend being dropped, so re-declaring it
    /// lands on a frozen table the caller never named.
    #[td_test::test(db)]
    async fn test_re_adding_a_trend_without_reuse_tables_names_the_trend_table(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_refrozen", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?]))?;
        register_ok(&db, p.id, "mgd_refrozen", create).await?;
        update_ok(
            &db,
            p.id,
            "mgd_refrozen",
            CHILD_OWNER,
            owner_update(Some(vec![])),
        )
        .await?;

        let update = owner_update_reusing(Some(vec![child("a")?]), ReuseFrozen::FALSE);
        let err = update_err(&db, p.id, "mgd_refrozen", CHILD_OWNER, update).await?;

        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::TrendChildTableFrozen(_, _)
        ));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_re_adding_a_trend_with_reuse_tables_adopts_its_table(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_readopt", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?]))?;
        register_ok(&db, p.id, "mgd_readopt", create).await?;
        update_ok(
            &db,
            p.id,
            "mgd_readopt",
            CHILD_OWNER,
            owner_update(Some(vec![])),
        )
        .await?;
        assert!(live_by_name(&db, &child_name("a")).await?.is_none());

        let update = owner_update_reusing(Some(vec![child("a")?]), ReuseFrozen::TRUE);
        update_ok(&db, p.id, "mgd_readopt", CHILD_OWNER, update).await?;

        assert!(live_by_name(&db, &child_name("a")).await?.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_without_child_functions_deletes_them(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_none", None).await;
        let create = owner_register(&["owned"], Some(vec![child("a")?]))?;
        register_ok(&db, p.id, "mgd_none", create).await?;

        let mut update = owner_update(None);
        update.description = Some("touched".try_into()?);
        update_ok(&db, p.id, "mgd_none", CHILD_OWNER, update).await?;

        assert!(live_by_name(&db, &child_name("a")).await?.is_none());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_a_child_function_declaring_a_regular_class_is_refused(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "mgd_class", None).await;
        let mut child = child("plain_table")?;
        child.function_type = FunctionType::Regular;
        let create = owner_register(&["owned"], Some(vec![child]))?;

        let err = register_err(&db, p.id, "mgd_class", create).await?;
        assert!(matches!(
            err.domain_err::<RegisterFunctionError>(),
            RegisterFunctionError::ChildFunctionTypeNotAllowed(FunctionType::Regular)
        ));
        Ok(())
    }
}
