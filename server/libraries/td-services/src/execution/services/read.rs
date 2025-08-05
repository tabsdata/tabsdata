//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::plan::{build_execution_plan, build_response};
use crate::execution::layers::read::build_existing_transaction_map;
use crate::execution::layers::template::{
    build_execution_template, find_all_input_tables, find_trigger_graph,
};
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::ExecutionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{ExtractNameService, ExtractService, TryIntoService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{
    AtTime, ExecutionId, ExecutionIdName, FunctionId, FunctionVersionId, TriggeredOn,
};
use td_objects::types::execution::{ExecutionDB, ExecutionResponse, TransactionDB};
use td_objects::types::function::FunctionDBWithNames;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};
use te_execution::transaction::TransactionBy;

// Very similar to trigger, but without triggering. It selects entities needed to build an execution plan.
#[provider(
    name = ExecutionReadService,
    request = ReadRequest<ExecutionParam>,
    response = ExecutionResponse,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = TransactionBy,
)]
fn provider() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
        // Find Plan and its TriggeredOn (and convert to AtTime).
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::select::<DaoQueries, ExecutionDB>),
        from_fn(With::<ExecutionDB>::extract::<TriggeredOn>),
        from_fn(With::<TriggeredOn>::convert_to::<AtTime, _>),
        // Find function that triggered the plan.
        from_fn(With::<ExecutionDB>::extract::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        // Find trigger graph
        from_fn(find_trigger_graph::<DaoQueries>),
        // Find all input tables
        from_fn(find_all_input_tables::<DaoQueries>),
        // Create execution template
        from_fn(build_execution_template::<DaoQueries>),
        // Get transactions
        from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
        from_fn(By::<ExecutionId>::select_all::<DaoQueries, TransactionDB>),
        from_fn(build_existing_transaction_map),
        // Create execution plan
        from_fn(build_execution_plan::<DaoQueries>),
        // Execution plan response
        from_fn(build_response),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::tests::test_execute;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_execution(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let provider = ExecutionReadService::provider(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(TransactionBy::default()),
        );
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<ExecutionParam>, ExecutionResponse>(&[
            // Extract from request.
            type_of_val(&With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
            // Find Plan and its TriggeredOn (and convert to AtTime).
            type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
            type_of_val(&By::<ExecutionIdName>::select::<DaoQueries, ExecutionDB>),
            type_of_val(&With::<ExecutionDB>::extract::<TriggeredOn>),
            type_of_val(&With::<TriggeredOn>::convert_to::<AtTime, _>),
            // Find function that triggered the plan.
            type_of_val(&With::<ExecutionDB>::extract::<FunctionVersionId>),
            type_of_val(&By::<FunctionVersionId>::select::<DaoQueries, FunctionDBWithNames>),
            type_of_val(&With::<FunctionDBWithNames>::extract::<FunctionId>),
            // Create execution template.
            // Find trigger graph
            type_of_val(&find_trigger_graph::<DaoQueries>),
            // Find all input tables
            type_of_val(&find_all_input_tables::<DaoQueries>),
            // Create execution template
            type_of_val(&build_execution_template::<DaoQueries>),
            // Get transactions
            type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
            type_of_val(&By::<ExecutionId>::select_all::<DaoQueries, TransactionDB>),
            type_of_val(&build_existing_transaction_map),
            // Create execution plan
            type_of_val(&build_execution_plan::<DaoQueries>),
            // Execution plan response
            type_of_val(&build_response),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_read_execution(db: DbPool) -> Result<(), TdError> {
        let execution = test_execute(db.clone(), false, false, true).await?;

        let service = ExecutionReadService::new(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(TransactionBy::default()),
        )
        .service()
        .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                ExecutionParam::builder()
                    .try_execution(execution.id().to_string())?
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;
        assert_eq!(execution, response);
        Ok(())
    }
}
