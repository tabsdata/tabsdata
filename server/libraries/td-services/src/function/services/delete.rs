//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::engine::{Changes, FunctionState, apply};
use crate::function::workers::register_workers;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::types::basic::{WorkerConfig, WorkerTypeDto};
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
    let mut gated_trx = db.begin().await?;
    let grant = authz
        .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
        .await?;
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
        .fetch_one(and!(&collection_param.collection, &function_param.function))
        .await?;

    let previous_version_id = Some(current.base.base.id.clone());
    let state = FunctionState::load(conn, &current.base.base.function_id).await?;
    let changes = Changes::for_delete(&state, &ctx);
    apply(conn, &changes, at).await?;
    // An empty workers map so all the previous workers get deleted.
    let empty_workers = Some(HashMap::<WorkerTypeDto, WorkerConfig>::default());
    register_workers(
        conn,
        &current.base.base,
        &previous_version_id,
        &empty_workers,
    )
    .await?;
    conn.scd2()
        .close::<FunctionDB>(&current.base.base.function_id, at)
        .await?;
    gated_trx.commit().await?;

    // Wake the scheduler so cron/stage workers stop immediately instead of at
    // the next polling tick.
    scheduler_notify.notify_scheduler();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::services::tests::{assert_delete, assert_register};
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::FromDao;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{Function, FunctionRegister};
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, UserId, WorkerMessageStatus,
        WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::basic::{CollectionName, TableNameDto};
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
            parent_function: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
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
            parent_function: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
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
            parent_function: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
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
            parent_function: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: None,
            tables: None,
            table_types: None,
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
            parent_function: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
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
            parent_function: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: None,
            table_types: None,
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
            parent_function: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
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
            parent_function: None,
            name: "joaquin_workout_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: Some(vec![TableNameDto::try_from("workout_1")?]),
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
                create_1.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let created_function_version_1 = response?;

        let create_2 = FunctionRegister {
            parent_function: None,
            name: "joaquin_workout_2".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: Some(vec![TableNameDto::try_from("workout_2")?]),
            table_types: None,
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
            parent_function: None,
            name: "function_1".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
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
}
