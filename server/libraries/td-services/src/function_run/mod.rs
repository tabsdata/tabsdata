//
// Copyright 2025 Tabsdata Inc.
//

pub mod raw_data;
pub mod services;

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunDBWithNames,
};
use td_objects::types::basic::{ExecutionId, FunctionId};
use td_queries::Queries;
use tracing::instrument;

/// Resolve the current `PhysicalFunctionRunDBWithNames` for an
/// `(execution_id, function_version_id)`: find the LFR, then its current
/// attempt.
#[instrument(
    name = "function_run.resolve_current",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn resolve_current_function_run<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    execution_id: &ExecutionId,
    function_id: &FunctionId,
) -> Result<PhysicalFunctionRunDBWithNames, TdError> {
    let lfr: LogicalFunctionRunDB = conn.fetch_one(and!(execution_id, function_id)).await?;
    let current: PhysicalFunctionRunDB = conn
        .scd2()
        .fetch_optional::<PhysicalFunctionRunDB>(&lfr.base.id)
        .await?
        .ok_or_else(|| {
            api_error!(
                ApiError::NotFound,
                "LFR {} has no current physical function run",
                lfr.base.id
            )
        })?;
    let fr: PhysicalFunctionRunDBWithNames = conn.timeline().fetch_one(&current.base.id).await?;
    Ok(fr)
}
