//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::ExecutionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, ConvertIntoMapService, ExtractNameService, ExtractService, ExtractVecService,
    SetService, TryIntoService, VecBuildService, With, builder,
};
use td_objects::tower_service::sql::{By, SqlFindService, SqlSelectAllService, SqlSelectService};
use td_objects::types::basic::{ExecutionId, ExecutionIdName, FunctionVersionId};
use td_objects::types::execution::{
    Execution, ExecutionBuilder, ExecutionDBWithStatus, ExecutionDetails, ExecutionDetailsBuilder,
    FunctionRunDB,
};
use td_objects::types::function::{Function, FunctionBuilder, FunctionDBWithNames};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionDetailsService,
    request = ReadRequest<ExecutionParam>,
    response = ExecutionDetails,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
        // Find Plan and its ID.
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::select::<ExecutionDBWithStatus>),
        from_fn(With::<ExecutionDBWithStatus>::extract::<ExecutionId>),
        // Initialize builder.
        from_fn(builder::<ExecutionDetailsBuilder>),
        from_fn(With::<ExecutionDBWithStatus>::convert_to::<ExecutionBuilder, _>),
        from_fn(With::<ExecutionBuilder>::build::<Execution, _>),
        from_fn(With::<Execution>::set::<ExecutionDetailsBuilder>),
        // Find all function runs.
        from_fn(By::<ExecutionId>::select_all::<FunctionRunDB>),
        // Find all functions.
        from_fn(With::<FunctionRunDB>::extract_vec::<FunctionVersionId>),
        from_fn(By::<FunctionVersionId>::find::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::vec_convert_to::<FunctionBuilder, _>),
        from_fn(With::<FunctionBuilder>::vec_build::<Function, _>),
        from_fn(With::<Vec<Function>>::set::<ExecutionDetailsBuilder>),
        // We could add as much information as we need here.
        // Then, build response.
        from_fn(With::<ExecutionDetailsBuilder>::build::<ExecutionDetails, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::tests::test_execute;
    use std::collections::HashSet;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_execution_details(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ExecutionDetailsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<ExecutionParam>, ExecutionDetails>(&[
                // Extract from request.
                type_of_val(&With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
                // Find Plan and its ID.
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
                type_of_val(&By::<ExecutionIdName>::select::<ExecutionDBWithStatus>),
                type_of_val(&With::<ExecutionDBWithStatus>::extract::<ExecutionId>),
                // Initialize builder
                type_of_val(&builder::<ExecutionDetailsBuilder>),
                type_of_val(&With::<ExecutionDBWithStatus>::convert_to::<ExecutionBuilder, _>),
                type_of_val(&With::<ExecutionBuilder>::build::<Execution, _>),
                type_of_val(&With::<Execution>::set::<ExecutionDetailsBuilder>),
                // Find all function runs.
                type_of_val(&By::<ExecutionId>::select_all::<FunctionRunDBWithNames>),
                type_of_val(
                    &With::<FunctionRunDBWithNames>::vec_convert_to::<FunctionRunBuilder, _>,
                ),
                type_of_val(&With::<FunctionRunBuilder>::vec_build::<FunctionRun, _>),
                type_of_val(&With::<Vec<FunctionRun>>::set::<ExecutionDetailsBuilder>),
                // Find all functions.
                type_of_val(&With::<FunctionRunDBWithNames>::extract_vec::<FunctionVersionId>),
                type_of_val(&By::<FunctionVersionId>::find::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::vec_convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<FunctionBuilder>::vec_build::<Function, _>),
                type_of_val(&With::<Vec<Function>>::set::<ExecutionDetailsBuilder>),
                // We could add as much information as we need here.
                // Then, build response.
                type_of_val(&With::<ExecutionDetailsBuilder>::build::<ExecutionDetails, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execution_details(db: DbPool) -> Result<(), TdError> {
        let execution = test_execute(db.clone(), false, false, true).await?;

        let service = ExecutionDetailsService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                ExecutionParam::builder()
                    .try_execution(execution.id().to_string())?
                    .build()?,
            );
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.execution().id(), execution.id());

        // Ensure no missing nor duplicate functions in execution details
        let ids: HashSet<_> = response.functions().iter().map(|fr| *fr.id()).collect();
        assert_eq!(
            ids.len(),
            response.functions().len(),
            "Duplicate or missing functions in execution details"
        );

        // Ensure all expected functions appear in the functions list
        for expected_id in execution
            .triggered_functions()
            .iter()
            .chain(std::iter::once(execution.manual_trigger()))
        {
            assert!(ids.contains(expected_id));
        }

        Ok(())
    }
}
