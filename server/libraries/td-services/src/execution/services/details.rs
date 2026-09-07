//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::ViewProject;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::execution::{Execution, ExecutionDBWithStatus, ExecutionDetails};
use td_objects::dxo::function::{Function, FunctionDBWithNames};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::types::basic::FunctionVersionId;
use td_queries::{ChunkedQueries, LiveEntry};

#[service_factory]
async fn execution_details(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<ExecutionParam>,
) -> Result<ExecutionDetails, TdError> {
    let (ctx, param) = request.into_parts();
    let execution_id = param.execution;

    let conn = gated_conn.get(authz.none());
    let execution: ExecutionDBWithStatus = conn.live().fetch_one(&execution_id).await?;
    let _grant = authz
        .require_in_project::<ViewProject>(conn, &ctx, &execution.project_id)
        .await?;

    let read: Execution = Execution::from_dao(execution)?;

    // Find all LFRs (concrete FRs are minted at dispatch; LFRs are the
    // plan-build identity rows), then all functions via the LFR's
    // function_version_id.
    let lfrs: Vec<LogicalFunctionRunDB> = conn.live().fetch_all(&execution_id).await?;
    let function_version_ids: Vec<FunctionVersionId> =
        lfrs.iter().map(|x| x.function_version_id.clone()).collect();
    let function_rows: Vec<FunctionDBWithNames> = conn
        .fetch_all_chunked::<_, FunctionVersionId>(&function_version_ids)
        .await?;
    let functions = function_rows
        .into_iter()
        .map(Function::from_dao)
        .collect::<Result<Vec<Function>, _>>()?;
    Ok(ExecutionDetails {
        execution: read,
        functions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::execution::services::execute::tests::test_execute;
    use std::collections::HashSet;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_execution_details(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let execution = test_execute(db.clone(), p.id, false, false, false).await?;

        let service = Services::with_defaults(db.clone());
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin()).read(ExecutionParam {
                execution: execution.id.clone(),
            });
        let response = service.execution_details(request).await?.data;

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
