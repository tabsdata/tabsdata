//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::resolve_collection_id;
use crate::function::children::{cascade_enabled, ensure_user_managed_now};
use crate::function::workers::register_workers;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{DbPool, InProject, retrying};
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
    let read = retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
            .await?;
        let at = gated_trx.locked_at();
        let conn = gated_trx.get(grant);
        let topology_at = conn.topology().bump(&project, at).await?;

        let _: CollectionDB = conn.scd2().fetch_one(&collection_param.collection).await?;

        // Before-update ids: the function id does not change, the function
        // version id does.
        let collection_id = resolve_collection_id(conn, &collection_param.collection).await?;
        let current: FunctionDBWithNames = conn
            .scd2()
            .fetch_one(and!(&collection_id, &function_param.function))
            .await?;

        ensure_user_managed_now(conn, &current).await?;

        let previous: FunctionDB = conn.timeline().fetch_one(&current.id).await?;
        let function = previous.re_registered(&ctx, enable.enabled);
        conn.scd2().open(&function, topology_at).await?;

        let previous_version_id = Some(current.id.clone());
        let no_workers: Option<HashMap<WorkerTypeDto, WorkerConfig>> = None;
        register_workers(conn, &function, &previous_version_id, &no_workers, at).await?;

        cascade_enabled(
            conn,
            &ctx,
            &current.function_id,
            enable.enabled,
            topology_at,
        )
        .await?;

        let with_names: FunctionDBWithNames = conn.timeline().fetch_one(&function.id).await?;
        let read = Function::from_dao(with_names)?;
        gated_trx.commit().await?;
        Ok(read)
    })
    .await?;

    // Wake the scheduler so worker state changes (cron Stop/Start) are picked
    // up immediately.
    scheduler_notify.notify_scheduler();

    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::children::ChildFunctionError;
    use crate::tag::parent::test_support::function_children;
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::{ChildFunction, FunctionRegister};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::TableType;
    use td_objects::types::basic::{
        AccessTokenId, Decorator, Enabled, FunctionRuntimeValues, TableNameDto, UserId,
    };
    use td_objects::types::basic::{AtTime, CollectionIdName, FunctionIdName};
    use td_objects::types::basic::{BundleId, CollectionName, ReuseFrozen};
    use td_objects::types::basic::{FunctionId, FunctionName, FunctionType, ProjectId};
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
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // dummy to allow for triggers and dependencies to exist
        let dummy = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _dummy_function = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &dummy)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
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
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: Some(enabled),
            workers: workers.clone(),
        };

        let created_function = db
            .scoped(p.project_id)
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
            .in_project(p.project_id);

        let service = Services::with_defaults(db.clone());
        let response = service.enable_function(request).await;
        let response = response?;

        assert_ne!(response.id, created_function.id);
        assert_ne!(
            response.defined_on,
            AtTime::from(created_function.interval.valid_from)
        );

        assert_eq!(response.collection_id, created_function.collection_id);
        assert_eq!(response.name, created_function.name);
        assert_eq!(response.description, created_function.description);
        assert_eq!(response.decorator, created_function.decorator);
        assert_eq!(response.connector, created_function.connector);
        assert_eq!(response.runtime_values, created_function.runtime_values);
        assert_eq!(response.function_id, created_function.function_id);
        assert_eq!(response.storage_version, created_function.storage_version);
        assert_eq!(response.bundle_id, created_function.bundle_id);
        assert_eq!(response.snippet, created_function.snippet);
        assert_eq!(response.status, created_function.status);

        assert_eq!(created_function.enabled, enabled);
        assert_eq!(response.enabled, enabled_update);

        let current_function: Vec<FunctionDB> = db
            .scd2()
            .fetch_all::<FunctionDB>(&response.function_id)
            .await?;
        assert_eq!(current_function.len(), 1);
        let current_function = current_function.first().unwrap();
        assert_eq!(current_function.id, response.id);
        assert_eq!(current_function.enabled, enabled_update);

        // Flipping the flag versions the function alone: its tables keep the
        // version that defined them.
        let current_tables: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(&current_function.function_id, TableDB::active()))
            .await?;
        assert_eq!(current_tables.len(), tables.unwrap_or_default().len());
        assert!(
            current_tables
                .iter()
                .all(|t| t.function_version_id == created_function.id)
        );

        let current_dependencies: Vec<DependencyDB> = db
            .scd2()
            .fetch_all::<DependencyDB>(&current_function.function_id)
            .await?;
        assert_eq!(
            current_dependencies.len(),
            dependencies.unwrap_or_default().len()
        );

        let current_triggers: Vec<TriggerDB> = db
            .scd2()
            .fetch_all::<TriggerDB>(&current_function.function_id)
            .await?;
        assert_eq!(current_triggers.len(), triggers.unwrap_or_default().len());

        let current_workers: Vec<WorkerDB> = db.fetch_all::<WorkerDB>(&current_function.id).await?;
        assert_eq!(current_workers.len(), workers.unwrap_or_default().len());

        Ok(())
    }

    const OWNER: &str = "cascade_owner";

    fn child(name: &str) -> Result<ChildFunction, TdError> {
        let table = format!("{name}_table");
        Ok(ChildFunction {
            name: name.try_into()?,
            function_type: FunctionType::default(),
            bundle_id: BundleId::default(),
            snippet: "a snippet".try_into()?,
            dependencies: None,
            tables: Some(vec![TableNameDto::try_from(table.as_str())?]),
            table_types: Some(HashMap::from([(
                TableNameDto::try_from(table.as_str())?,
                TableType::DQSummaryTrends,
            )])),
        })
    }

    fn cascade_dto(
        name: &str,
        children: Option<Vec<ChildFunction>>,
        enabled: Enabled,
    ) -> Result<FunctionRegister, TdError> {
        let table = format!("{name}_table");
        // Every child's tables derive from the owner's table.
        let parent_tables = match &children {
            Some(children) => {
                let parent = TableNameDto::try_from(table.as_str())?;
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
            tables: Some(vec![TableNameDto::try_from(table.as_str())?]),
            table_types: None,
            parent_tables,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: Some(enabled),
            workers: None,
        })
    }

    async fn register_fn(
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

    async fn set_enabled(
        db: &TdPool,
        project: ProjectId,
        collection: &CollectionName,
        function: &str,
        enabled: Enabled,
    ) -> Result<Function, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection.as_str())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str(function)?,
                    },
                ),
                FunctionEnable { enabled },
            )
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .enable_function(request)
            .await?
            .data)
    }

    async fn current_version(db: &TdPool, function_id: &FunctionId) -> Result<FunctionDB, TdError> {
        let rows: Vec<FunctionDB> = db.scd2().fetch_all::<FunctionDB>(function_id).await?;
        assert_eq!(rows.len(), 1);
        Ok(rows.into_iter().next().unwrap())
    }

    async fn a_collection(
        db: &TdPool,
        project: ProjectId,
        name: &str,
    ) -> Result<CollectionName, TdError> {
        let name = CollectionName::try_from(name)?;
        db.scoped(project)
            .seed()
            .collection(&name, &UserId::admin())
            .await?;
        Ok(name)
    }

    #[td_test::test(db)]
    async fn test_disable_cascades_to_a_child(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.project_id, "endis").await?;
        let owner = register_fn(
            &db,
            p.project_id,
            &collection,
            cascade_dto(OWNER, Some(vec![child("child")?]), Enabled::TRUE)?,
        )
        .await?;
        let child = live_child(&db, &owner.function_id).await?;

        let response = set_enabled(&db, p.project_id, &collection, OWNER, Enabled::FALSE).await?;

        assert_eq!(response.name, FunctionName::try_from(OWNER)?);
        assert_eq!(response.enabled, Enabled::FALSE);
        let child = current_version(&db, &child.function_id).await?;
        assert_eq!(child.enabled, Enabled::FALSE);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_enable_cascades_back_to_the_subtree(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.project_id, "endis_back").await?;
        let owner = register_fn(
            &db,
            p.project_id,
            &collection,
            cascade_dto(OWNER, Some(vec![child("child")?]), Enabled::TRUE)?,
        )
        .await?;
        let child = live_child(&db, &owner.function_id).await?;

        set_enabled(&db, p.project_id, &collection, OWNER, Enabled::FALSE).await?;
        set_enabled(&db, p.project_id, &collection, OWNER, Enabled::TRUE).await?;

        let child = current_version(&db, &child.function_id).await?;
        assert_eq!(child.enabled, Enabled::TRUE);
        Ok(())
    }

    async fn live_child(db: &TdPool, owner: &FunctionId) -> Result<FunctionDB, TdError> {
        let children = function_children(db, owner).await?;
        let rows: Vec<FunctionDB> = db.scd2().chunked().fetch_all(&children, ()).await?;
        assert_eq!(rows.len(), 1);
        Ok(rows.into_iter().next().unwrap())
    }

    #[td_test::test(db)]
    async fn test_a_child_already_at_the_target_is_not_versioned(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.project_id, "endis_skip").await?;
        let owner = register_fn(
            &db,
            p.project_id,
            &collection,
            cascade_dto(OWNER, Some(vec![child("child")?]), Enabled::TRUE)?,
        )
        .await?;
        set_enabled(&db, p.project_id, &collection, OWNER, Enabled::FALSE).await?;
        let disabled = live_child(&db, &owner.function_id).await?;

        set_enabled(&db, p.project_id, &collection, OWNER, Enabled::FALSE).await?;

        let current = current_version(&db, &disabled.function_id).await?;
        assert_eq!(current.id, disabled.id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_toggling_a_child_function_directly_is_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.project_id, "endis_guard").await?;
        let owner = register_fn(
            &db,
            p.project_id,
            &collection,
            cascade_dto(OWNER, Some(vec![child("child_fn")?]), Enabled::TRUE)?,
        )
        .await?;
        let child = live_child(&db, &owner.function_id).await?;

        let err = set_enabled(&db, p.project_id, &collection, "child_fn", Enabled::FALSE)
            .await
            .expect_err("a child function must not be toggled directly");

        assert!(matches!(
            err.domain_err::<ChildFunctionError>(),
            ChildFunctionError::NotUserManaged(_)
        ));
        let current = current_version(&db, &child.function_id).await?;
        assert_eq!(current.enabled, Enabled::TRUE);
        assert_eq!(current.id, child.id);
        let owner_now = current_version(&db, &owner.function_id).await?;
        assert_eq!(owner_now.enabled, Enabled::TRUE);
        Ok(())
    }

    /// The owner's toggle still reaches it: the cascade writes the child's
    /// version itself rather than re-entering this service.
    #[td_test::test(db)]
    async fn test_the_cascade_still_reaches_a_child_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = a_collection(&db, p.project_id, "endis_cascade").await?;
        let owner = register_fn(
            &db,
            p.project_id,
            &collection,
            cascade_dto(OWNER, Some(vec![child("child_fn")?]), Enabled::TRUE)?,
        )
        .await?;
        let child = live_child(&db, &owner.function_id).await?;

        set_enabled(&db, p.project_id, &collection, OWNER, Enabled::FALSE).await?;

        let current = current_version(&db, &child.function_id).await?;
        assert_eq!(current.enabled, Enabled::FALSE);
        Ok(())
    }
}
