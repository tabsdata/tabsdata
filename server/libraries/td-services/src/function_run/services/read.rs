//
// Copyright 2025 Tabsdata Inc.
//

use crate::function_run::resolve_current_function_run;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
use td_objects::types::basic::AtTime;
use td_objects::{FromDao, and};
use td_queries::Queries;

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
    let execution_id = execution_param.execution;
    let execution: ExecutionDB = conn.fetch_one(&execution_id).await?;
    // No state filter, and at the plan's instant: a run that already happened
    // stays readable once its function is frozen, disabled or its collection gone.
    let at = AtTime::from(execution.base.triggered_on);

    let function_id_name = function_param.function;
    let function: FunctionDBWithNames = conn
        .scd2()
        .as_of(&at)
        .fetch_one(and!(&collection_id_name, &function_id_name))
        .await?;

    // Replays mint several attempts under one LFR; the current one is canonical.
    let run =
        resolve_current_function_run(conn, &execution_id, &function.base.base.function_id).await?;
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
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Enabled, FunctionName,
        LogicalFunctionRunStatus, PhysicalFunctionRunStatus, ProjectId, TableNameDto,
        TransactionKey, UserId, UserLabel, UserName,
    };
    use td_objects::types::basic::{AtTime, ReuseFrozen};
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_queries::test_utils::SeedEntry;

    const COLLECTION: &str = "collection";
    const FUNCTION: &str = "joaquin";

    struct SeededRun {
        collection: ActiveCollectionDB,
        function: FunctionDB,
        execution: ExecutionDB,
        run: PhysicalFunctionRunDB,
    }

    async fn seed_done_run(
        db: &TdPool,
        project: ProjectId,
        enabled: Option<Enabled>,
    ) -> Result<SeededRun, TdError> {
        let collection = db
            .scoped(project)
            .seed()
            .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION.try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("table_version")?]),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled,
            workers: None,
        };
        let function = db
            .scoped(project)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(project).seed().execution(&function).await?;
        let transaction = db
            .scoped(project)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let run = db
            .scoped(project)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;
        Ok(SeededRun {
            collection,
            function,
            execution,
            run,
        })
    }

    fn read_request(
        project: ProjectId,
        execution: &ExecutionDB,
    ) -> WithinProject<ReadRequest<(CollectionParam, FunctionParam, ExecutionParam)>> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(COLLECTION).unwrap(),
                },
                FunctionParam {
                    function: FunctionIdName::from_str(FUNCTION).unwrap(),
                },
                ExecutionParam {
                    execution: execution.base.id.clone(),
                },
            ))
            .in_project(project)
    }

    #[td_test::test(db)]
    async fn test_read_function_run(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let seeded = seed_done_run(&db, p.id, None).await?;
        let SeededRun {
            collection,
            function,
            execution,
            run,
        } = &seeded;

        let service = Services::with_defaults(db.clone());
        let response = service
            .function_run_read(read_request(p.id, execution))
            .await?
            .data;

        assert_eq!(response.id, run.base.id);
        assert_eq!(response.collection_id, run.base.collection_id);
        assert_eq!(response.function_version_id, run.base.function_version_id);
        assert_eq!(response.execution_id, run.base.execution_id);
        assert_eq!(response.transaction_id, run.base.transaction_id);
        assert_eq!(response.triggered_on, AtTime::from(run.interval.valid_from));
        assert_eq!(response.trigger, run.base.trigger);
        assert_eq!(response.started_on, run.base.started_on);
        assert_eq!(response.ended_on, run.base.ended_on);
        assert_eq!(response.status, PhysicalFunctionRunStatus::Done);
        assert_eq!(response.name, FunctionName::try_from(FUNCTION)?);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.execution, execution.base.name);
        assert_eq!(response.triggered_by, UserLabel::from(UserName::admin()));
        assert_eq!(function.base.name, response.name);
        Ok(())
    }

    /// Deleting the collection closes every version interval it owns; the run
    /// still reads because it resolves the function at its plan's instant.
    #[td_test::test(db)]
    async fn test_read_function_run_of_deleted_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let seeded = seed_done_run(&db, p.id, None).await?;

        let service = Services::with_defaults(db.clone());
        service
            .delete_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str(COLLECTION)?,
                    })
                    .in_project(p.id),
            )
            .await?;

        let response = service
            .function_run_read(read_request(p.id, &seeded.execution))
            .await?
            .data;
        assert_eq!(response.id, seeded.run.base.id);
        assert_eq!(response.collection, seeded.collection.name);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_read_function_run_of_disabled_function(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let seeded = seed_done_run(&db, p.id, Some(Enabled::FALSE)).await?;

        let response = Services::with_defaults(db.clone())
            .function_run_read(read_request(p.id, &seeded.execution))
            .await?
            .data;
        assert_eq!(response.id, seeded.run.base.id);
        Ok(())
    }
}
