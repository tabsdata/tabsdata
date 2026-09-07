//
// Copyright 2025 Tabsdata Inc.
//

use crate::function_run::resolve_bound_function_run;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam};
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
use td_objects::{FromDao, and};
use td_queries::{CurrentEntry, LiveEntry};

#[service_factory]
async fn function_run_read(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, FunctionParam, ExecutionParam)>>,
) -> Result<PhysicalFunctionRun, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param, execution_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let _collection: ActiveCollectionDB = conn.live().fetch_one(&collection_id_name).await?;

    let function_id_name = function_param.function;
    let function: FunctionDBWithNames = conn
        .current()
        .state(FunctionDBWithNames::executable())
        .fetch_one(and!(&collection_id_name, &function_id_name))
        .await?;
    let execution_id = execution_param.execution;
    conn.live()
        .assert_exists::<ExecutionDB>(&execution_id)
        .await?;

    // find function run via LFR pointer: replays mint multiple FRs
    // under one LFR identity; the canonical one is whatever the
    // pointer currently binds to.
    let run = resolve_bound_function_run(conn, &execution_id, &function.base.id).await?;
    PhysicalFunctionRun::from_dao(run)
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
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, TableNameDto, TransactionKey, UserId, UserName,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_function_run(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = vec![TableNameDto::try_from("table_version")?];
        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables: Some(tables.clone()),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;

        let execution = db.scoped(p.id).seed().execution(&function_version).await?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str(&format!("{}", function_version.name))?,
                },
                ExecutionParam {
                    execution: execution.id.clone(),
                },
            ))
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.function_run_read(request).await;
        let response = response?;
        assert_eq!(response.id, physical_function_run.id);
        assert_eq!(response.collection_id, physical_function_run.collection_id);
        assert_eq!(
            response.function_version_id,
            physical_function_run.function_version_id
        );
        assert_eq!(response.execution_id, physical_function_run.execution_id);
        assert_eq!(
            response.transaction_id,
            physical_function_run.transaction_id
        );
        assert_eq!(response.triggered_on, physical_function_run.triggered_on);
        assert_eq!(response.trigger, physical_function_run.trigger);
        assert_eq!(response.started_on, physical_function_run.started_on);
        assert_eq!(response.ended_on, physical_function_run.ended_on);
        assert_eq!(response.status, PhysicalFunctionRunStatus::Done);
        assert_eq!(response.name, FunctionName::try_from("joaquin")?);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.execution, execution.name);
        assert_eq!(response.triggered_by, UserName::admin());
        Ok(())
    }
}
