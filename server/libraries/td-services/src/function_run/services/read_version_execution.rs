//
// Copyright 2025 Tabsdata Inc.
//

use crate::function_run::resolve_current_function_run;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam};
use td_objects::dxo::function::{FunctionDB, FunctionVersionParam};
use td_objects::dxo::physical_function_run::PhysicalFunctionRun;
use td_queries::Queries;

#[service_factory]
async fn function_version_execution_read(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<(FunctionVersionParam, ExecutionParam)>,
) -> Result<PhysicalFunctionRun, TdError> {
    let (_, (version, execution)) = request.into_parts();
    let function_version_id = version.function_version;
    let execution_id = execution.execution;

    let mut trx = db.read().await?;
    let conn = trx.get(authz.none());
    let execution: ExecutionDB = conn.fetch_one(&execution_id).await?;
    let collection: ResolvedCollectionDB = conn.fetch_one(&execution.base.collection_id).await?;
    let mut trx = trx.enter_project(&collection.base.project_id).await?;
    let conn = trx.get(authz.none());

    // Replays mint several attempts under one LFR; the current one is canonical.
    let function: FunctionDB = conn.timeline().fetch_one(&function_version_id).await?;
    let run = resolve_current_function_run(conn, &execution_id, &function.base.function_id).await?;
    PhysicalFunctionRun::from_dao(run)
}
#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionParam;
    use td_objects::dxo::function::{FunctionRegister, FunctionVersionParam};
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, TableNameDto, TransactionKey, UserId, UserName,
    };
    use td_objects::types::basic::{AtTime, ReuseFrozen};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_function_version_execution_run(db: TdPool) -> Result<(), TdError> {
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

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).read((
            FunctionVersionParam {
                function_version: function_version.base.id,
            },
            ExecutionParam {
                execution: execution.base.id.clone(),
            },
        ));

        let service = Services::with_defaults(db.clone());
        let response = service.function_version_execution_read(request).await;
        let response = response?;
        assert_eq!(response.id, physical_function_run.base.id);
        assert_eq!(
            response.collection_id,
            physical_function_run.base.collection_id
        );
        assert_eq!(
            response.function_version_id,
            physical_function_run.base.function_version_id
        );
        assert_eq!(
            response.execution_id,
            physical_function_run.base.execution_id
        );
        assert_eq!(
            response.transaction_id,
            physical_function_run.base.transaction_id
        );
        assert_eq!(
            response.triggered_on,
            AtTime::from(physical_function_run.interval.valid_from)
        );
        assert_eq!(response.trigger, physical_function_run.base.trigger);
        assert_eq!(response.started_on, physical_function_run.base.started_on);
        assert_eq!(response.ended_on, physical_function_run.base.ended_on);
        // Status lives on the LFR; the read endpoint joins it in.
        assert_eq!(response.status, PhysicalFunctionRunStatus::Done);
        assert_eq!(response.name, FunctionName::try_from("joaquin")?);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.execution, execution.base.name);
        assert_eq!(response.triggered_by, UserName::admin());
        Ok(())
    }
}
