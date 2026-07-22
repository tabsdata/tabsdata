//
// Copyright 2026 Tabsdata Inc.
//

use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_objects::types::basic::{FunctionVersionId, PhysicalTableDataVersionId};
use td_queries::ExecutionsEntry;
use td_tower::extractors::{MutRef, Ref};

pub async fn list_executions_by_tdv(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    tdv_id: Ref<Option<PhysicalTableDataVersionId>>,
    list_params: Ref<ListParams>,
    query_params: Ref<ListQueryParams<Execution>>,
) -> Result<ListResponse<Execution>, TdError> {
    let result = conn
        .executions()
        .list_by_table_data_version(tdv_id.deref(), &query_params)
        .await?;
    build_list_response(&list_params, &query_params, result)
}

pub async fn list_executions_by_fv(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    fv_id: Ref<FunctionVersionId>,
    list_params: Ref<ListParams>,
    query_params: Ref<ListQueryParams<Execution>>,
) -> Result<ListResponse<Execution>, TdError> {
    let result = conn
        .executions()
        .list_by_function_version(fv_id.deref(), &query_params)
        .await?;
    build_list_response(&list_params, &query_params, result)
}
