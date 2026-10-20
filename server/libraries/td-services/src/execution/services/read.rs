//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::plan::recursion_options_of;
use crate::execution::plan::response::{build_response, fetch_relations_info};
use crate::execution::plan::rows::build_existing_transaction_map;
use crate::execution::plan::versions::pinned_execution_plan_versions;
use std::collections::HashSet;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_execution::TransactionBy;
use td_execution::topology::FunctionState;
use td_execution::topology::ProjectTopology;
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::execution::{ExecutionDB, ExecutionResponse};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunInputInsertDB,
    LogicalFunctionRunInsertDB,
};
use td_objects::dxo::topology::ProjectTopologyDB;
use td_objects::dxo::transaction::{TransactionDB, TransactionInsertDB};
use td_objects::types::basic::FunctionId;
use td_queries::Queries;

#[service_factory]
async fn execution_read(
    authz: Ctx<AuthzContext>,
    transaction_by: Ctx<TransactionBy>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<ExecutionParam>,
) -> Result<ExecutionResponse, TdError> {
    let (ctx, param) = request.into_parts();
    let execution_id = param.execution;

    let mut trx = db.read().await?;
    let conn = trx.get(authz.none());
    let execution: ExecutionDB = conn.fetch_one(&execution_id).await?;
    let collection: ResolvedCollectionDB = conn.fetch_one(&execution.base.collection_id).await?;
    let project = collection.base.project_id;
    let mut trx = trx.enter_project(&project).await?;
    let grant = authz
        .require::<Project, ViewProject>(&mut trx, &ctx, &project)
        .await?;
    let conn = trx.get(grant);
    let topology: ProjectTopologyDB = conn
        .timeline()
        .fetch_one(&execution.base.topology_version_id)
        .await?;

    let recursion_options = recursion_options_of(&execution)?;
    let lfrs: Vec<LogicalFunctionRunInsertDB> = conn
        .fetch_all::<LogicalFunctionRunDB>(&execution_id)
        .await?
        .into_iter()
        .map(|lfr| lfr.base)
        .collect();
    let inputs: Vec<LogicalFunctionRunInputInsertDB> = conn
        .fetch_all::<LogicalFunctionRunInputDB>(&execution_id)
        .await?
        .into_iter()
        .map(|input| input.base)
        .collect();
    let ran: HashSet<FunctionId> = lfrs.iter().map(|lfr| lfr.function_id).collect();
    let execution_plan = ProjectTopology::of(conn, topology).await?.plan(
        FunctionState::Ran(&ran),
        &execution.base.function_id,
        &recursion_options,
    )?;
    let transactions: Vec<TransactionInsertDB> = conn
        .fetch_all::<TransactionDB>(&execution_id)
        .await?
        .into_iter()
        .map(|transaction| transaction.base)
        .collect();
    let transaction_map = build_existing_transaction_map(&transaction_by, &transactions)?;
    let pinned_plan = pinned_execution_plan_versions(conn, &execution_plan, &lfrs, &inputs).await?;
    let relations_info = fetch_relations_info(conn, &pinned_plan).await?;

    build_response(
        &transaction_by,
        &transaction_map,
        &execution.base,
        &pinned_plan,
        &lfrs,
        &inputs,
        relations_info,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::execution::services::execute::tests::test_execute;
    use td_database::pool::TdPool;
    use td_error::ApiError;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::project::ProjectDeleteDB;
    use td_objects::types::basic::{AccessTokenId, ProjectName, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_execution_in_deleted_project_is_not_found(
        db: TdPool,
    ) -> Result<(), TdError> {
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let execution = test_execute(db.clone(), alpha.id, false, false, false).await?;
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let request = |ctx: &RequestContext| {
            ctx.read(ExecutionParam {
                execution: execution.id.clone(),
            })
        };

        let service = Services::with_defaults(db.clone());
        service.execution_read(request(&ctx)).await?;

        db.update::<ProjectDeleteDB>(&alpha.to_delete_dao(&ctx), alpha.id)
            .await?;
        let err = service.execution_read(request(&ctx)).await.unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_read_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let execution = test_execute(db.clone(), p.id, false, false, false).await?;

        let service = Services::with_defaults(db.clone());
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(ExecutionParam {
                execution: execution.id.clone(),
            });

        let response = service.execution_read(request).await?.data;
        assert_eq!(execution.id, response.id);
        assert_eq!(execution.name, response.name);
        assert_eq!(execution.triggered_on, response.triggered_on);
        assert_eq!(execution.trigger_type, response.trigger_type);
        assert_eq!(execution.manual_trigger, response.manual_trigger);
        assert_eq!(execution.all_functions, response.all_functions);
        assert_eq!(execution.triggered_functions, response.triggered_functions);
        assert_eq!(execution.transactions, response.transactions);
        assert_eq!(execution.all_tables, response.all_tables);
        assert_eq!(execution.created_tables, response.created_tables);
        assert_eq!(execution.system_tables, response.system_tables);
        assert_eq!(execution.user_tables, response.user_tables);
        assert_eq!(execution.relations_info, response.relations_info);

        assert_eq!(execution.relations, response.relations);
        assert_eq!(execution.dot, response.dot);

        Ok(())
    }
}
