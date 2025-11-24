//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::table_data_version::TableDataVersion;
use td_objects::dxo::{DataAccessObject, ListQuery};
use td_objects::sql::list::{AndConditions, Condition, ListQueryParams, OrConditions};
use td_objects::types::SqlEntity;
use td_objects::types::basic::{FunctionRunStatus, UncommittedStatus};
use td_tower::extractors::Input;

pub async fn build_query_params(
    Input(uncommitted_status): Input<UncommittedStatus>,
    Input(params): Input<ListParams>,
) -> Result<ListQueryParams<TableDataVersion>, TdError> {
    let params = ListQueryParams::<TableDataVersion>::try_from(params.deref())?;

    let params = if **uncommitted_status {
        params
    } else {
        let AndConditions(mut or_conditions) = params.conditions;
        let committed_status = FunctionRunStatus::Committed;
        let status_column =
            <<TableDataVersion as ListQuery>::Dao>::sql_field_for_type(committed_status.type_id())?;
        let only_committed_condition =
            Condition::Eq(status_column.to_string(), Box::new(committed_status));
        let only_committed_condition = OrConditions(vec![only_committed_condition]);
        or_conditions.push(only_committed_condition);

        ListQueryParams {
            conditions: AndConditions(or_conditions),
            ..params
        }
    };

    Ok(params)
}
