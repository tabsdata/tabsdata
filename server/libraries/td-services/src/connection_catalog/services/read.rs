//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::resolve_catalog;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{MaintainCatalog, Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection_catalog::ConnectionCatalogRead;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};

#[service_factory]
async fn read_connection_catalog(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<CollectionParam>>,
) -> Result<ConnectionCatalogRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, (ViewProject, MaintainCatalog)>(gated_conn, &ctx, &project)
        .await?;
    let catalog = resolve_catalog(gated_conn.get(grant), &param).await?;
    ConnectionCatalogRead::from_dao(catalog)
}
