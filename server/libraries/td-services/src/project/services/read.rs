//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::project::{ActiveProjectDBWithNames, ProjectParam, ProjectRead};
use td_queries::Queries;

#[service_factory]
async fn read_project(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<ProjectParam>,
) -> Result<ProjectRead, TdError> {
    let (ctx, param) = request.into_parts();
    let id_name = param.project;
    let conn = gated_conn.get(authz.none());
    let row: ActiveProjectDBWithNames = conn.fetch_one(&id_name).await?;
    // Reading a project's metadata requires `view_project` on that project.
    let _grant = authz
        .require::<Project, ViewProject>(conn, &ctx, &row.base.id)
        .await?;
    ProjectRead::from_dao(row)
}
