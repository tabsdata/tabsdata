//
// Copyright 2026 Tabsdata Inc.
//

//! Logical function run (LFR) routes — operator-driven. Top-level:
//! LFR ids are globally unique, so the services resolve the owning project
//! from the row.

use axum::Extension;
use axum::extract::{Path, State};
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::CreateStatus;
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::logical::{LogicalFunctionRunParam, LogicalFunctionRunReplayResponse};
use td_services::Services;

const LOGICAL_VERSIONS_TAG: &str = "Logical Versions";

#[apiserver_path(mount = global, method = post, path = "/logical-function-runs/{logical_function_run}/replay", tag = LOGICAL_VERSIONS_TAG, doc = "Re-execute a finished LFR — mint a new concrete PhysicalFunctionRun against the latest patch")]
pub async fn replay(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(logical_function_run): Path<LogicalFunctionRunParam>,
) -> Result<CreateStatus<LogicalFunctionRunReplayResponse>, ErrorStatus> {
    let request = context.create(logical_function_run, ());
    let response = state.replay_logical_function_run(request).await?;
    Ok(CreateStatus::CREATED(response))
}
