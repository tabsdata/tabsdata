//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::AtTimeParam;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::logical::TableDataVersion;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::table::TableParam;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_objects::types::basic::TriggeredOn;
use td_queries::Queries;

#[service_factory]
async fn table_list_data_versions(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, TableParam, AtTimeParam)>>,
) -> Result<ListResponse<TableDataVersion>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, table_param, at_param), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let _collection: ActiveCollectionDB = conn.fetch_one(&collection_id_name).await?;
    let at = at_param.at;
    let triggered_on = TriggeredOn::try_from(&at)?;
    let table_id_name = table_param.table;
    let table: TableDBWithNames = conn
        .scd2()
        .fetch_one(and!(&collection_id_name, &table_id_name))
        .await?;

    let query_params = ListQueryParams::<TableDataVersion>::try_from(&list_params)?;
    let rows = conn
        .timeline()
        .list_until(&triggered_on, &table.base.base.table_id, &query_params)
        .await?;
    build_list_response(&list_params, &query_params, rows)
}
