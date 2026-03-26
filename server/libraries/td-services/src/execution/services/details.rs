//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::{
    Execution, ExecutionBuilder, ExecutionDBWithStatus, ExecutionDetails, ExecutionDetailsBuilder,
};
use td_objects::dxo::function::{Function, FunctionBuilder, FunctionDBWithNames};
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::rest_urls::ExecutionParam;
use td_objects::tower_service::with::{With, builder};
use td_objects::types::basic::{ExecutionId, ExecutionIdName, FunctionVersionId};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ExecutionDetailsService,
    request = ReadRequest<ExecutionParam>,
    response = ExecutionDetails,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
        // Find Plan and its ID.
        from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),
        from_fn(By::<ExecutionIdName>::fetch_one::<ExecutionDBWithStatus>),
        from_fn(With::<ExecutionDBWithStatus>::extract::<ExecutionId>),
        // Initialize builder.
        from_fn(builder::<ExecutionDetails>),
        from_fn(With::<ExecutionDBWithStatus>::try_convert_to::<ExecutionBuilder, _>),
        from_fn(With::<ExecutionBuilder>::build::<Execution>),
        from_fn(With::<Execution>::set::<ExecutionDetailsBuilder>),
        // Find all function runs.
        from_fn(By::<ExecutionId>::fetch_all::<FunctionRunDB>),
        // Find all functions.
        from_fn(With::<Vec<FunctionRunDB>>::extract::<Vec<FunctionVersionId>>),
        from_fn(By::<Vec<FunctionVersionId>>::fetch_all::<FunctionDBWithNames>),
        from_fn(With::<FunctionDBWithNames>::vec_convert_to::<FunctionBuilder, _>),
        from_fn(With::<Vec<FunctionBuilder>>::build::<Vec<Function>>),
        from_fn(With::<Vec<Function>>::set::<ExecutionDetailsBuilder>),
        // We could add as much information as we need here.
        // Then, build response.
        from_fn(With::<ExecutionDetailsBuilder>::build::<ExecutionDetails>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::tests::test_execute;
    use std::collections::HashSet;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_execution_details(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ExecutionDetailsService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<ExecutionParam>, ExecutionDetails>(&[
                // Extract from request.
                type_of_val(&With::<ReadRequest<ExecutionParam>>::extract_name::<ExecutionParam>),
                // Find Plan and its ID.
                type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
                type_of_val(&By::<ExecutionIdName>::fetch_one::<ExecutionDBWithStatus>),
                type_of_val(&With::<ExecutionDBWithStatus>::extract::<ExecutionId>),
                // Initialize builder
                type_of_val(&builder::<ExecutionDetails>),
                type_of_val(&With::<ExecutionDBWithStatus>::try_convert_to::<ExecutionBuilder, _>),
                type_of_val(&With::<ExecutionBuilder>::build::<Execution>),
                type_of_val(&With::<Execution>::set::<ExecutionDetailsBuilder>),
                // Find all function runs.
                type_of_val(&By::<ExecutionId>::fetch_all::<FunctionRunDB>),
                // Find all functions.
                type_of_val(&With::<Vec<FunctionRunDB>>::extract::<Vec<FunctionVersionId>>),
                type_of_val(&By::<Vec<FunctionVersionId>>::fetch_all::<FunctionDBWithNames>),
                type_of_val(&With::<FunctionDBWithNames>::vec_convert_to::<FunctionBuilder, _>),
                type_of_val(&With::<Vec<FunctionBuilder>>::build::<Vec<Function>>),
                type_of_val(&With::<Vec<Function>>::set::<ExecutionDetailsBuilder>),
                // We could add as much information as we need here.
                // Then, build response.
                type_of_val(&With::<ExecutionDetailsBuilder>::build::<ExecutionDetails>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_execution_details(db: TdPool) -> Result<(), TdError> {
        let execution = test_execute(db.clone(), false, false, true, false).await?;

        let service = ExecutionDetailsService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                ExecutionParam::builder()
                    .execution(ExecutionIdName::from_str(&execution.id.to_string())?)
                    .build()?,
            );
        let response = service.raw_oneshot(request).await?;

        assert_eq!(response.execution.id, execution.id);

        // Ensure no missing nor duplicate functions in execution details
        let ids: HashSet<_> = response.functions.iter().map(|fr| fr.id.clone()).collect();
        assert_eq!(
            ids.len(),
            response.functions.len(),
            "Duplicate or missing functions in execution details"
        );

        // Ensure all expected functions appear in the functions list
        for expected_id in execution
            .triggered_functions
            .iter()
            .chain(std::iter::once(&execution.manual_trigger))
        {
            assert!(ids.contains(expected_id));
        }

        Ok(())
    }
}
