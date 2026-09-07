//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::{
    register_dependencies, register_tables, register_triggers, register_workers,
};
use std::collections::HashMap;
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreateDB, CollectionParam};
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{ResetState, ReuseFrozen, WorkerConfig, WorkerTypeDto};
use td_queries::{HistoricEntry, LiveEntry};

#[service_factory]
async fn delete_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    msgs: &mut ReqCtx,
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
    let locked: CollectionCreateDB = conn
        .live()
        .fetch_one_for_update(&collection_param.collection)
        .await?;
    let collection: ActiveCollectionDB = conn.live().fetch_one(&locked.id).await?;

    let at = ctx.time;
    let current: FunctionDBWithNames = conn
        .historic(&at)
        .state(FunctionDBWithNames::available())
        .fetch_one(and!(&collection_param.collection, &function_param.function))
        .await?;

    let previous: FunctionDB = conn.live().fetch_one(&current.base.id).await?;
    let deleted = previous.deleted(&ctx);
    conn.live().insert(&deleted).await?;

    let previous_version_id = Some(current.base.id);
    let existing_tables: Vec<TableDB> = conn
        .historic(&at)
        .state(TableDB::active())
        .fetch_all(&current.base.function_id)
        .await?;
    let existing_dependencies: Vec<DependencyDB> = conn
        .historic(&at)
        .state(DependencyDB::active())
        .fetch_all(&current.base.function_id)
        .await?;
    let existing_triggers: Vec<TriggerDBWithNames> = conn
        .historic(&at)
        .state(TriggerDBWithNames::available())
        .fetch_all(&current.base.function_id)
        .await?;
    let no_tables = None;
    let no_table_types = None;
    let no_dependencies = None;
    let no_triggers = None;
    // An empty workers map so all the previous workers get deleted.
    let empty_workers = Some(HashMap::<WorkerTypeDto, WorkerConfig>::default());
    // Deletes reuse nothing and never reset state.
    let reuse_frozen = ReuseFrozen::default();
    let reset_state = ResetState::default();
    register_tables(
        conn,
        msgs,
        &ctx,
        &deleted,
        &collection.id,
        &collection.name,
        &at,
        &existing_tables,
        &no_tables,
        &no_table_types,
        &reuse_frozen,
        &reset_state,
    )
    .await?;
    register_dependencies(
        conn,
        &ctx,
        &deleted,
        &collection.name,
        &at,
        &existing_dependencies,
        &no_dependencies,
    )
    .await?;
    register_triggers(
        conn,
        &ctx,
        &deleted,
        &collection.name,
        &at,
        &existing_triggers,
        &no_tables,
        &no_triggers,
        &no_dependencies,
    )
    .await?;
    register_workers(
        conn,
        &deleted,
        &project,
        &previous_version_id,
        &empty_workers,
    )
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
    use td_objects::types::basic::{
        AccessTokenId, BundleId, Decorator, FunctionRuntimeValues, UserId, WorkerMessageStatus,
        WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::basic::{CollectionName, TableNameDto};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::LiveEntry;
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
        service.delete_function(request).await?.data;

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
        service.delete_function(request).await?.data;

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
        service.delete_function(request).await?.data;

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
        service.delete_function(request).await?.data;

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
        service.delete_function(request).await?.data;

        let function_version: FunctionDBWithNames = db
            .live()
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
        service.delete_function(request).await?.data;

        let workers: Vec<WorkerDB> = db.live().fetch_all::<WorkerDB>(()).await.unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.collection_id, collection.id);
        assert_eq!(worker.execution_id, None);
        assert_eq!(worker.transaction_id, None);
        assert_eq!(worker.physical_function_run_id, None);
        assert_eq!(worker.function_version_id, function.id);
        assert_eq!(worker.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.started_on, None);
        assert_eq!(worker.ended_on, None);
        assert_eq!(worker.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.worker_config, Some(WorkerConfig::default()));
        Ok(())
    }
}
