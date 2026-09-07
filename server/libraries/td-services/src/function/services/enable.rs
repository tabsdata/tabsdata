//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::workers::register_workers;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::{CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{Function, FunctionDB, FunctionDBWithNames, FunctionEnable};
use td_objects::types::basic::{WorkerConfig, WorkerTypeDto};
use td_objects::{FromDao, and};
use td_queries::Queries;

#[service_factory]
async fn enable_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    db: DbPool<InProject>,
    request: WithinProject<UpdateRequest<FunctionEnable, (CollectionParam, FunctionParam)>>,
) -> Result<Function, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), enable) = request.into_parts();
    let mut gated_trx = db.begin().await?;
    let grant = authz
        .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    // Bare-table row lock: the re-registration must not resurrect a function
    // inside a collection whose delete cascade is in flight.
    let _: CollectionDB = conn
        .for_update()
        .fetch_one(&collection_param.collection)
        .await?;

    // Before-update ids: the function id does not change, the function
    // version id does.
    let current: FunctionDBWithNames = conn
        .latest()
        .fetch_one(and!(&collection_param.collection, &function_param.function))
        .await?;

    let previous: FunctionDB = conn.fetch_one(&current.base.base.id).await?;
    let function = previous.re_registered(&ctx, enable.enabled);
    conn.insert(&function).await?;

    let previous_version_id = Some(current.base.base.id);
    let no_workers: Option<HashMap<WorkerTypeDto, WorkerConfig>> = None;
    register_workers(conn, &function, &previous_version_id, &no_workers).await?;

    let with_names: FunctionDBWithNames = conn.fetch_one(&function.id).await?;
    let read = Function::from_dao(with_names)?;
    gated_trx.commit().await?;

    // Wake the scheduler so worker state changes (cron Stop/Start) are picked
    // up immediately.
    scheduler_notify.notify_scheduler();

    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::{
        AccessTokenId, Decorator, Enabled, FunctionRuntimeValues, TableNameDto, UserId,
    };
    use td_objects::types::basic::{BundleId, CollectionName, ReuseFrozen};
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_enable_enabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::TRUE, Enabled::TRUE).await
    }

    #[td_test::test(db)]
    async fn test_enable_disabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::FALSE, Enabled::TRUE).await
    }

    #[td_test::test(db)]
    async fn test_disable_enabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::TRUE, Enabled::FALSE).await
    }

    #[td_test::test(db)]
    async fn test_disable_disabled_empty_function(db: TdPool) -> Result<(), TdError> {
        test_enable_function(db, None, None, None, None, Enabled::FALSE, Enabled::FALSE).await
    }

    #[td_test::test(db)]
    async fn test_enable_disabled(db: TdPool) -> Result<(), TdError> {
        test_enable_function(
            db,
            Some(vec![
                TableNameDto::try_from("foo_1")?,
                TableNameDto::try_from("foo_2")?,
            ]),
            Some(vec![
                TableDependencyDto::try_from("dummy_1")?,
                TableDependencyDto::try_from("dummy_2")?,
            ]),
            Some(vec![
                TableTriggerDto::try_from("dummy_1")?,
                TableTriggerDto::try_from("dummy_2")?,
            ]),
            Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])),
            Enabled::FALSE,
            Enabled::TRUE,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_disable_enabled(db: TdPool) -> Result<(), TdError> {
        test_enable_function(
            db,
            Some(vec![
                TableNameDto::try_from("foo_1")?,
                TableNameDto::try_from("foo_2")?,
            ]),
            None,
            None,
            None,
            Enabled::FALSE,
            Enabled::TRUE,
        )
        .await
    }

    async fn test_enable_function(
        db: TdPool,
        tables: Option<Vec<TableNameDto>>,
        dependencies: Option<Vec<TableDependencyDto>>,
        triggers: Option<Vec<TableTriggerDto>>,
        workers: Option<HashMap<WorkerTypeDto, WorkerConfig>>,
        enabled: Enabled,
        enabled_update: Enabled,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // dummy to allow for triggers and dependencies to exist
        let dummy = FunctionRegister {
            name: "dummy_foo".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("dummy_1")?,
                TableNameDto::try_from("dummy_2")?,
            ]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _dummy_function = db.scoped(p.id).seed().function(&collection, &dummy).await?;

        let create = FunctionRegister {
            name: "foo".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: Some(enabled),
            workers: workers.clone(),
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("foo")?,
                    },
                ),
                FunctionEnable {
                    enabled: enabled_update,
                },
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.enable_function(request).await;
        let response = response?;

        assert_ne!(response.id, created_function.base.id);
        assert_ne!(response.defined_on, created_function.base.defined_on);

        assert_eq!(response.collection_id, created_function.base.collection_id);
        assert_eq!(response.name, created_function.base.name);
        assert_eq!(response.description, created_function.base.description);
        assert_eq!(response.decorator, created_function.base.decorator);
        assert_eq!(response.connector, created_function.base.connector);
        assert_eq!(
            response.runtime_values,
            created_function.base.runtime_values
        );
        assert_eq!(response.function_id, created_function.base.function_id);
        assert_eq!(
            response.storage_version,
            created_function.base.storage_version
        );
        assert_eq!(response.bundle_id, created_function.base.bundle_id);
        assert_eq!(response.snippet, created_function.base.snippet);
        assert_eq!(response.status, created_function.base.status);

        assert_eq!(created_function.base.enabled, enabled);
        assert_eq!(response.enabled, enabled_update);

        let current_function: Vec<FunctionDB> = db
            .latest()
            .fetch_all::<FunctionDB>(&response.function_id)
            .await?;
        assert_eq!(current_function.len(), 1);
        let current_function = current_function.first().unwrap();
        assert_eq!(current_function.base.id, response.id);
        assert_eq!(current_function.base.enabled, enabled_update);

        // Flipping the flag versions the function alone: its tables keep the
        // version that defined them.
        let current_tables: Vec<TableDB> = db
            .latest()
            .fetch_all::<TableDB>(and!(&current_function.base.function_id, TableDB::active()))
            .await?;
        assert_eq!(current_tables.len(), tables.unwrap_or_default().len());
        assert!(
            current_tables
                .iter()
                .all(|t| t.base.function_version_id == created_function.base.id)
        );

        let current_dependencies: Vec<DependencyDB> = db
            .latest()
            .fetch_all::<DependencyDB>(&current_function.base.function_id)
            .await?;
        assert_eq!(
            current_dependencies.len(),
            dependencies.unwrap_or_default().len()
        );

        let current_triggers: Vec<TriggerDB> = db
            .latest()
            .fetch_all::<TriggerDB>(&current_function.base.function_id)
            .await?;
        assert_eq!(current_triggers.len(), triggers.unwrap_or_default().len());

        let current_workers: Vec<WorkerDB> =
            db.fetch_all::<WorkerDB>(&current_function.base.id).await?;
        assert_eq!(current_workers.len(), workers.unwrap_or_default().len());

        Ok(())
    }
}
