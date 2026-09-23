//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, DbScope};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_objects::types::basic::{FunctionVersionId, LogicalTableDataVersionId};
use td_queries::Queries;

pub async fn list_executions_by_tdv<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    tdv_id: &LogicalTableDataVersionId,
    list_params: &ListParams,
    query_params: &ListQueryParams<Execution>,
) -> Result<ListResponse<Execution>, TdError> {
    let result = conn
        .executions()
        .list_by_table_data_version(tdv_id, query_params)
        .await?;
    build_list_response(list_params, query_params, result)
}

pub async fn list_executions_by_fv<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    fv_id: &FunctionVersionId,
    list_params: &ListParams,
    query_params: &ListQueryParams<Execution>,
) -> Result<ListResponse<Execution>, TdError> {
    let result = conn
        .executions()
        .list_by_function_version(fv_id, query_params)
        .await?;
    build_list_response(list_params, query_params, result)
}
