//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::read::{map_create_worker, vec_create_table_dependency};
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{Function, FunctionDBWithNames, FunctionWithTables};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::{TableId, TableName, WorkerId, WorkerTypeDto};
use td_objects::types::composed::TableTrigger;
use td_objects::{FromDao, and};
use td_queries::{HistoricEntry, LiveEntry};

#[service_factory]
async fn read_function(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, FunctionParam)>>,
) -> Result<FunctionWithTables, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection = collection_param.collection;
    let function_id_name = function_param.function;
    let at = ctx.time;

    let function: FunctionDBWithNames = conn
        .historic(&at)
        .state(FunctionDBWithNames::available())
        .fetch_one(and!(&collection, &function_id_name))
        .await?;

    let function_id = function.base.function_id;
    let function_version_id = function.base.id.clone();
    let read: Function = Function::from_dao(function)?;

    let tables: Vec<TableDBWithNames> = conn
        .historic(&at)
        .state(TableDBWithNames::user())
        .fetch_all(&function_id)
        .await?;
    let table_names = tables
        .iter()
        .map(TableName::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let triggers: Vec<TriggerDBWithNames> = conn
        .historic(&at)
        .state(TriggerDBWithNames::active())
        .fetch_all(&function_id)
        .await?;
    let trigger_table_ids: Vec<TableId> = triggers
        .iter()
        .map(|x| x.base.trigger_by_table_id)
        .collect();
    let trigger_tables: Vec<TableDBWithNames> = conn
        .historic(&at)
        .state(TableDBWithNames::available())
        .fetch_all_chunked::<TableId>(&trigger_table_ids)
        .await?;
    let table_triggers = trigger_tables
        .iter()
        .map(TableTrigger::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let dependencies: Vec<DependencyDBWithNames> = conn
        .historic(&at)
        .state(DependencyDBWithNames::user())
        .fetch_all(&function_id)
        .await?;
    let dependency_table_ids: Vec<TableId> = dependencies.iter().map(|x| x.base.table_id).collect();
    let dependency_tables: Vec<TableDBWithNames> = conn
        .historic(&at)
        .state(TableDBWithNames::available())
        .fetch_all_chunked::<TableId>(&dependency_table_ids)
        .await?;
    let table_dependencies = vec_create_table_dependency(&dependencies, &dependency_tables).await?;

    let workers: Vec<WorkerDBWithNames> = conn
        .live()
        .fetch_all_in::<WorkerDBWithNames>(
            WorkerDBWithNames::enabled_triggers(),
            &function_version_id,
        )
        .await?;
    let worker_map: HashMap<WorkerTypeDto, WorkerId> = map_create_worker(&workers).await?;

    Ok(FunctionWithTables {
        function: read,
        dependencies: table_dependencies,
        triggers: table_triggers,
        workers: worker_map,
        tables: table_names,
    })
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
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
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
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
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
                        function.collection_id
                    ))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("joaquin_workout")?,
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.read_function(request).await;
        let response = response?;

        assert_eq!(function.id, response.function.id);
        assert_eq!(function.collection_id, response.function.collection_id);
        assert_eq!(function.name, response.function.name);
        assert_eq!(function.description, response.function.description);
        assert_eq!(function.function_id, response.function.function_id);
        assert_eq!(function.storage_version, response.function.storage_version);
        assert_eq!(function.bundle_id, response.function.bundle_id);
        assert_eq!(function.snippet, response.function.snippet);
        assert_eq!(function.defined_on, response.function.defined_on);
        assert_eq!(function.defined_by_id, response.function.defined_by_id);
        assert_eq!(function.status, response.function.status);
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
}
