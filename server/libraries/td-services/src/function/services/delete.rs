//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::children::ensure_user_managed;
use crate::function::lifecycle::delete_one;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject, retrying};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_queries::Queries;

#[service_factory]
async fn delete_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    db: DbPool<InProject>,
    request: WithinProject<DeleteRequest<(CollectionParam, FunctionParam)>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param)) = request.into_parts();
    retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
            .await?;
        let at = gated_trx.locked_at();
        let conn = gated_trx.get(grant);

        // Bare-table row lock (views with outer joins aren't lockable); the
        // active-view fetch is the liveness check, decided under the lock.
        let (locked, at) = conn
            .for_update()
            .serialize_on::<CollectionDB>(&collection_param.collection)
            .await?;
        let _: ActiveCollectionDB = conn.fetch_one(&locked.base.id).await?;

        let current: FunctionDBWithNames = conn
            .scd2()
            .fetch_one(and!(collection_id_name, &function_param.function))
            .await?;

        ensure_user_managed(&current.base.base)?;

        let children: Vec<FunctionDB> = conn
            .scd2()
            .fetch_all(FunctionDB::children_of(&current.base.base.function_id))
            .await?;
        for child in &children {
            delete_one(conn, &ctx, &child.base, at).await?;
        }
        delete_one(conn, &ctx, &current.base.base, at).await?;
        gated_trx.commit().await
    })
    .await?;

    // Wake the scheduler so cron/stage workers stop immediately instead of at
    // the next polling tick.
    scheduler_notify.notify_scheduler();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::children::ChildFunctionError;
    use crate::function::services::tests::{assert_delete, assert_register};
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::FromDao;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{ChildFunction, Function, FunctionDB, FunctionRegister};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::TableType;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, UserId, WorkerConfig,
        WorkerMessageStatus, WorkerStatus, WorkerType, WorkerTypeDto,
    };
    use td_objects::types::basic::{
        CollectionId, CollectionName, FunctionType, ProjectId, TableName, TableNameDto, TableStatus,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_delete_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_function_with_tables(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("table1")?,
                TableNameDto::try_from("table2")?,
            ]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_function_with_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_function_with_triggers(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_function_unique(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create_1 = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: Some(vec![TableNameDto::try_from("workout_1")?]),
            table_types: None,
            parent_tables: None,
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
                create_1.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let created_function_version_1 = response?;

        let create_2 = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout_2".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: Some(vec![TableNameDto::try_from("workout_2")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function_2 = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create_2)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout_2")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        let function_version: FunctionDBWithNames = db
            .timeline()
            .fetch_one::<FunctionDBWithNames>(&(&created_function_version_1.id))
            .await?;
        let function_version = Function::from_dao(function_version)?;
        assert_register(
            &db,
            &UserId::admin(),
            &collection,
            &create_1,
            &function_version,
        )
        .await?;

        assert_delete(
            &db,
            &UserId::admin(),
            &collection,
            &create_2,
            &created_function_2,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_delete_workers(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            parent_tables: None,
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
        let function = service.register_function(request).await?.data;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("function_1")?,
                },
            ))
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        service.delete_function(request).await?;

        let workers: Vec<WorkerDB> = db.fetch_all::<WorkerDB>(()).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, function.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));
        Ok(())
    }

    const OWNER: &str = "parent_fn";
    const CHILD: &str = "child_fn";

    fn register_dto(
        name: &str,
        children: Option<Vec<ChildFunction>>,
        tables: &[&str],
    ) -> Result<FunctionRegister, TdError> {
        let parent_tables = match &children {
            Some(children) => {
                let parent = TableNameDto::try_from(tables[0])?;
                Some(
                    children
                        .iter()
                        .flat_map(|child| child.tables.iter().flatten())
                        .map(|table| (table.clone(), parent.clone()))
                        .collect(),
                )
            }
            None => None,
        };
        Ok(FunctionRegister {
            child_functions: children,
            name: name.try_into()?,
            description: "a function".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "a snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(
                tables
                    .iter()
                    .map(|t| TableNameDto::try_from(*t))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            table_types: None,
            parent_tables,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        })
    }

    async fn register(
        db: &TdPool,
        project: ProjectId,
        collection: &CollectionName,
        create: FunctionRegister,
    ) -> Result<Function, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection.as_str())?,
                },
                create,
            )
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .register_function(request)
            .await?
            .data)
    }

    async fn delete(
        db: &TdPool,
        project: ProjectId,
        collection: &CollectionName,
        function: &str,
    ) -> Result<(), TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete((
                CollectionParam {
                    collection: CollectionIdName::from_str(collection.as_str())?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str(function)?,
                },
            ))
            .in_project(project);
        Services::with_defaults(db.clone())
            .delete_function(request)
            .await?;
        Ok(())
    }

    async fn live_names(db: &TdPool, collection: &CollectionId) -> Result<Vec<String>, TdError> {
        let functions: Vec<FunctionDB> = db.scd2().fetch_all::<FunctionDB>(collection).await?;
        let mut names: Vec<String> = functions.iter().map(|f| f.base.name.to_string()).collect();
        names.sort();
        Ok(names)
    }

    fn child(name: &str, table: &str) -> Result<ChildFunction, TdError> {
        Ok(ChildFunction {
            name: name.try_into()?,
            function_type: FunctionType::default(),
            bundle_id: BundleId::default(),
            snippet: "a snippet".try_into()?,
            dependencies: None,
            tables: Some(vec![TableNameDto::try_from(table)?]),
            table_types: Some(HashMap::from([(
                TableNameDto::try_from(table)?,
                TableType::DQSummaryTrends,
            )])),
        })
    }

    async fn seed_parent_and_child(
        db: &TdPool,
        project: ProjectId,
        collection: &CollectionName,
    ) -> Result<Function, TdError> {
        register(
            db,
            project,
            collection,
            register_dto(
                OWNER,
                Some(vec![child(CHILD, "child_table")?]),
                &["owned_table"],
            )?,
        )
        .await
    }

    async fn a_collection(
        db: &TdPool,
        project: ProjectId,
        name: &str,
    ) -> Result<ActiveCollectionDB, TdError> {
        let name = CollectionName::try_from(name)?;
        db.scoped(project)
            .seed()
            .collection(&name, &UserId::admin())
            .await
    }

    #[td_test::test(db)]
    async fn test_delete_cascades_to_the_child_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.id, "cascade").await?;
        seed_parent_and_child(&db, p.id, &collection.name).await?;
        assert_eq!(live_names(&db, &collection.id).await?, [CHILD, OWNER]);

        delete(&db, p.id, &collection.name, OWNER).await?;

        assert!(live_names(&db, &collection.id).await?.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_cascades_to_every_child_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.id, "cascade_many").await?;
        register(
            &db,
            p.id,
            &collection.name,
            register_dto(
                OWNER,
                Some(vec![
                    child(CHILD, "child_table")?,
                    child("second_child_fn", "second_child_table")?,
                ]),
                &["owned_table"],
            )?,
        )
        .await?;

        delete(&db, p.id, &collection.name, OWNER).await?;

        assert!(live_names(&db, &collection.id).await?.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_deleting_a_child_function_directly_is_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.id, "cascade_child").await?;
        seed_parent_and_child(&db, p.id, &collection.name).await?;

        let err = delete(&db, p.id, &collection.name, CHILD)
            .await
            .expect_err("a child function must not be deletable directly");

        assert!(matches!(
            err.domain_err::<ChildFunctionError>(),
            ChildFunctionError::NotUserManaged(_)
        ));
        assert_eq!(live_names(&db, &collection.id).await?, [CHILD, OWNER]);
        Ok(())
    }

    /// The child's tables freeze rather than vanish, which is what lets a
    /// re-registration with `reuse_tables` adopt them back.
    #[td_test::test(db)]
    async fn test_the_cascade_freezes_the_child_functions_tables(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.id, "cascade_tables").await?;
        seed_parent_and_child(&db, p.id, &collection.name).await?;

        delete(&db, p.id, &collection.name, OWNER).await?;

        let frozen: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(&TableName::try_from("child_table")?)
            .await?;
        assert_eq!(frozen.len(), 1);
        assert_eq!(frozen[0].base.status, TableStatus::Frozen);
        Ok(())
    }
}
