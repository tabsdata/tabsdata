//
// Copyright 2025. Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::ListStatus;
use axum::extract::State;
use axum::Extension;
use axum_extra::extract::Query;
use td_apiforge::apiserver_path;
use td_objects::crudl::{ListParams, RequestContext};
use td_objects::rest_urls::{AtTimeParam, LIST_TABLES};
use td_objects::types::table::Table;
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { list_table }
}

#[apiserver_path(method = get, path = LIST_TABLES, tag = TABLES_TAG)]
#[doc = "List tables"]
pub async fn list_table(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Query(query_params): Query<ListParams>,
    Query(at_param): Query<AtTimeParam>,
) -> Result<ListStatus<Table>, ErrorStatus> {
    let request = context.list(at_param, query_params);
    let response = state.list_table_service().await.oneshot(request).await?;
    Ok(ListStatus::OK(response))
}
