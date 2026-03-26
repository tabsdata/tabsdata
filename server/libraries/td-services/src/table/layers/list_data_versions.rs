//
// Copyright 2025 Tabs Data Inc.
//

use sea_query::{Expr, ExprTrait};
use std::any::Any;
use std::ops::Deref;
use td_error::{ApiError, TdError, api_error};
use td_objects::SeaValue;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::table_data_version::TableDataVersion;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{FunctionRunStatus, UncommittedStatus};
use td_objects::{ListDto, TypedColumns};
use td_tower::extractors::Ref;

pub async fn build_query_params(
    uncommitted_status: Ref<UncommittedStatus>,
    params: Ref<ListParams>,
) -> Result<ListQueryParams<TableDataVersion>, TdError> {
    let params = ListQueryParams::<TableDataVersion>::try_from(params.deref())?;

    let params = if **uncommitted_status {
        params
    } else {
        let committed_status = FunctionRunStatus::Committed;
        let status_column = <<TableDataVersion as ListDto>::Dao>::col_for_type(
            committed_status.type_id(),
        )
        .ok_or(api_error!(
            ApiError::InternalError,
            "Function run status column not found in column mapping" // should never happen
        ))?;
        let filter = params
            .filter
            .add(Expr::col(status_column).eq(committed_status.as_sea_value()));

        ListQueryParams { filter, ..params }
    };

    Ok(params)
}
