//
// Copyright 2025 Tabsdata Inc.
//

use std::collections::HashMap;
use std::ops::Deref;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{AtTimeParam, ReadRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{Function, FunctionDBWithNames, FunctionWithTables};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::WorkerType;
use td_objects::types::basic::{TableId, TableName, WorkerId, WorkerTypeDto};
use td_objects::types::composed::TableDependency;
use td_objects::types::composed::TableTrigger;
use td_objects::types::table_ref::VersionedTableRef;
use td_objects::{FromDao, and};
use td_queries::Queries;

#[service_factory]
async fn read_function(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, FunctionParam, AtTimeParam)>>,
) -> Result<FunctionWithTables, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param, at_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection = collection_param.collection;
    let function_id_name = function_param.function;
    let at = at_param.at;

    let function: FunctionDBWithNames = conn
        .scd2()
        .as_of(&at)
        .fetch_one(and!(&collection, &function_id_name))
        .await?;

    let function_id = function.base.base.function_id;
    let function_version_id = function.base.base.id.clone();
    let read: Function = Function::from_dao(function)?;

    let tables: Vec<TableDBWithNames> = conn
        .scd2()
        .as_of(&at)
        .fetch_all(and!(&function_id, TableDBWithNames::user()))
        .await?;
    let table_names = tables
        .iter()
        .map(TableName::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let triggers: Vec<TriggerDBWithNames> = conn
        .scd2()
        .as_of(&at)
        .fetch_all(and!(&function_id, TriggerDBWithNames::active()))
        .await?;
    let trigger_table_ids: Vec<TableId> = triggers
        .iter()
        .map(|x| x.base.base.trigger_by_table_id)
        .collect();
    let trigger_tables: Vec<TableDBWithNames> = conn
        .scd2()
        .as_of(&at)
        .chunked()
        .fetch_all(&trigger_table_ids, ())
        .await?;
    let table_triggers = trigger_tables
        .iter()
        .map(TableTrigger::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let dependencies: Vec<DependencyDBWithNames> = conn
        .scd2()
        .as_of(&at)
        .fetch_all(and!(&function_id, DependencyDBWithNames::user()))
        .await?;
    let dependency_table_ids: Vec<TableId> =
        dependencies.iter().map(|x| x.base.base.table_id).collect();
    let dependency_tables: Vec<TableDBWithNames> = conn
        .scd2()
        .as_of(&at)
        .chunked()
        .fetch_all(&dependency_table_ids, ())
        .await?;
    let table_dependencies = vec_create_table_dependency(&dependencies, &dependency_tables)?;

    let workers: Vec<WorkerDBWithNames> = conn
        .fetch_all::<WorkerDBWithNames>(and!(
            &function_version_id,
            WorkerDBWithNames::enabled_triggers()
        ))
        .await?;
    let worker_map: HashMap<WorkerTypeDto, WorkerId> = map_create_worker(&workers)?;

    Ok(FunctionWithTables {
        function: read,
        dependencies: table_dependencies,
        triggers: table_triggers,
        workers: worker_map,
        tables: table_names,
    })
}

pub fn vec_create_table_dependency(
    dependencies: &[DependencyDBWithNames],
    tables: &[TableDBWithNames],
) -> Result<Vec<TableDependency>, TdError> {
    let tables = tables
        .iter()
        .map(|t| (t.base.base.table_id, t))
        .collect::<HashMap<_, _>>();

    let table_deps = dependencies
        .iter()
        .filter_map(|d| {
            let table = tables.get(&d.base.base.table_id)?;
            let versions = d.base.base.table_versions.deref().clone();
            Some(TableDependency::new(VersionedTableRef::new(
                Some(table.collection.clone()),
                table.base.base.name.clone(),
                versions,
            )))
        })
        .collect::<Vec<_>>();
    Ok(table_deps)
}

pub fn map_create_worker(
    workers: &[WorkerDBWithNames],
) -> Result<HashMap<WorkerTypeDto, WorkerId>, TdError> {
    let workers = workers
        .iter()
        .filter_map(|w| match w.base.base.worker_type {
            WorkerType::CronTrigger => Some((WorkerTypeDto::CronTrigger, w.base.base.id.clone())),
            WorkerType::StageTrigger => Some((WorkerTypeDto::StageTrigger, w.base.base.id.clone())),
            _ => None,
        })
        .collect();
    Ok(workers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues, TableNameDto,
        UserId, UserName, WorkerConfig,
    };
    use td_objects::types::basic::{AtTime, CollectionIdName, FunctionIdName};
    use td_objects::types::composed::{TableDependency, TableDependencyDto, TableTriggerDto};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("cofnig")?, &UserId::admin())
            .await?;

        let dependencies = Some(vec![TableDependencyDto::try_from("cofnig/table@HEAD~2")?]);
        let triggers = Some(vec![TableTriggerDto::try_from("cofnig/table")?]);
        let tables = Some(vec![TableNameDto::try_from("table")?]);

        let create = FunctionRegister {
            parent_function: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            function_type: None,
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
            workers: Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])),
        };

        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!(
                        "~{}",
                        function.base.collection_id
                    ))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
                AtTimeParam { at: AtTime::now() },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.read_function(request).await;
        let response = response?;

        assert_eq!(function.base.id, response.function.id);
        assert_eq!(function.base.collection_id, response.function.collection_id);
        assert_eq!(function.base.name, response.function.name);
        assert_eq!(function.base.description, response.function.description);
        assert_eq!(function.base.function_id, response.function.function_id);
        assert_eq!(
            function.base.storage_version,
            response.function.storage_version
        );
        assert_eq!(function.base.bundle_id, response.function.bundle_id);
        assert_eq!(function.base.snippet, response.function.snippet);
        assert_eq!(
            AtTime::from(function.interval.valid_from),
            response.function.defined_on
        );
        assert_eq!(function.base.defined_by_id, response.function.defined_by_id);
        assert_eq!(function.base.status, response.function.status);
        assert_eq!(
            response.function.collection,
            CollectionName::try_from("cofnig")?
        );
        assert_eq!(response.function.defined_by, UserName::try_from("admin")?);

        assert_eq!(
            response.dependencies,
            dependencies
                .unwrap_or(vec![])
                .into_iter()
                .map(TableDependency::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.triggers,
            triggers
                .unwrap_or(vec![])
                .into_iter()
                .map(TableTrigger::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.tables,
            tables
                .unwrap_or(vec![])
                .into_iter()
                .map(TableName::try_from)
                .collect::<Result<Vec<_>, _>>()?
        );
        assert_eq!(
            response.workers.keys().cloned().collect::<Vec<_>>(),
            vec![WorkerTypeDto::CronTrigger]
        );
        Ok(())
    }
    /// Deleting a collection closes every version interval it owns, so there
    /// is nothing live left to read; `?at=` is how a plan reads the version
    /// that ran.
    #[td_test::test(db)]
    async fn test_read_at_past_instant_after_collection_delete(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("doomed")?;
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            parent_function: None,
            name: "publisher".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            function_type: None,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let registered_at = AtTime::now();

        let service = Services::with_defaults(db.clone());
        service
            .delete_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str(&collection_name.to_string())?,
                    })
                    .in_project(p.id),
            )
            .await?;

        let request = |at: AtTime| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read((
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection_name.to_string())
                            .unwrap(),
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("publisher").unwrap(),
                    },
                    AtTimeParam { at },
                ))
                .in_project(p.id)
        };

        let read = service.read_function(request(registered_at)).await?.data;
        assert_eq!(read.function.id, function.base.id);
        assert_eq!(read.function.collection_id, collection.id);
        assert_eq!(read.tables, vec![TableName::try_from("t0")?]);

        assert!(service.read_function(request(AtTime::now())).await.is_err());

        let recreated = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let refunction = db.scoped(p.id).seed().function(&recreated, &create).await?;

        let read = service.read_function(request(registered_at)).await?.data;
        assert_eq!(read.function.id, function.base.id);
        let read = service.read_function(request(AtTime::now())).await?.data;
        assert_eq!(read.function.id, refunction.base.id);
        assert_eq!(read.function.collection_id, recreated.id);
        Ok(())
    }
}
