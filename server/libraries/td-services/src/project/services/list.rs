//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{CreateProjects, System, SystemEntity};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::project::ProjectRead;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_projects(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<ProjectRead>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    // List is open to any authenticated role: every user holds the
    // `default` system role, whose `create_projects` grant gates this.
    let _grant = authz
        .require::<System, CreateProjects>(gated_conn, &ctx, &SystemEntity)
        .await?;
    // Resolve the requester's viewable projects; the list is filtered to them.
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<ProjectRead>::try_from(&list_params)?;
    let rows = conn.list(&visible, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}
