//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{MaintainCatalog, System, SystemEntity};
use td_database::access::{AcrossProjects, GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::connection_catalog::ConnectionSummary;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::types::visible_projects::VisibleProjects;
use td_queries::Queries;

/// Every connection in the projects the caller can view (in every project for
/// `maintain_catalog`): name, kind, collection type and probe status, never the
/// definition.
#[service_factory]
async fn list_connections(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ListRequest,
) -> Result<ListResponse<ConnectionSummary>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible_projects) = authz.visible_projects(gated_conn, &ctx).await?;
    let visible_projects = if authz
        .satisfies::<System, MaintainCatalog>(gated_conn, &ctx, &SystemEntity)
        .await?
    {
        VisibleProjects::All
    } else {
        visible_projects
    };
    gated_conn
        .get(grant)
        .list(&visible_projects, list_params)
        .await
}
