//
// Copyright 2025 Tabs Data Inc.
//

use crate::router;
use crate::router::state::Tables;
use crate::router::tables::TABLES_TAG;
use crate::status::error_status::ErrorStatus;
use crate::status::ok_status::{DeleteStatus, NoContent};
use axum::Extension;
use axum::extract::{Path, State};
use td_apiforge::apiserver_path;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::{TABLE_DELETE, TableParam};
use tower::ServiceExt;

router! {
    state => { Tables },
    routes => { delete_table }
}

#[apiserver_path(method = delete, path = TABLE_DELETE, tag = TABLES_TAG)]
#[doc = "Delete a table"]
pub async fn delete_table(
    State(state): State<Tables>,
    Extension(context): Extension<RequestContext>,
    Path(table_path): Path<TableParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(table_path);
    let response = state.table_delete_service().await.oneshot(request).await?;
    Ok(DeleteStatus::OK(response))
}
