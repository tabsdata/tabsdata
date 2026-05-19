//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
use td_objects::types::basic::{AtTime, ExecutionId, FunctionVersionId};
use td_queries::{LfrEntry, LiveEntry};
use td_tower::extractors::{MutRef, Ref};
use tracing::instrument;

/// Resolve the canonically-bound `PhysicalFunctionRunDBWithNames` for an
/// `(execution_id, function_version_id)`: find the LFR, read its pointer
/// at NOW, fetch the bound physical FR by id. Replaces the historical
/// direct `By::<And![ExecutionId, FunctionVersionId]>::fetch_one` lookup
/// which broke once replays minted multiple FRs under one LFR.
#[instrument(
    name = "function_run.resolve_bound",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn resolve_bound_function_run(
    mut conn: MutRef<DynamicConnection<'static>>,
    execution_id: Ref<ExecutionId>,
    function_version_id: Ref<FunctionVersionId>,
) -> Result<PhysicalFunctionRunDBWithNames, TdError> {
    let at = AtTime::now();
    let lfr: LogicalFunctionRunDB = conn
        .live()
        .fetch_one(and!(execution_id.deref(), function_version_id.deref()))
        .await?;
    let bound_fr_id = conn
        .lfr()
        .pointers()
        .concrete_at(&lfr.id, &at)
        .await?
        .ok_or_else(|| {
            api_error!(
                ApiError::NotFound,
                "LFR {} has no currently-bound physical function run",
                lfr.id
            )
        })?;
    let fr: PhysicalFunctionRunDBWithNames = conn.live().fetch_one(&bound_fr_id).await?;
    Ok(fr)
}
