//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::resolve_catalog;
use std::collections::HashMap;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, MaintainCatalog, Project};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection_catalog::{
    ConnectionEntityDB, EntitiesIn, EntityListParams, EntityRead,
};
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_queries::Queries;

/// The explored entities: they reveal the external system's shape, so
/// `DevelopProject` (or `maintain_catalog`). Documents are read only for
/// `fields=true`, and only the page's.
#[service_factory]
async fn list_connection_entities(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, EntityListParams)>>,
) -> Result<ListResponse<EntityRead>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (param, entity_params), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, (DevelopProject, MaintainCatalog)>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let catalog = resolve_catalog(conn, &param).await?;
    let mut page: ListResponse<EntityRead> = conn.list(&catalog.connection_id, list_params).await?;
    let rows = &mut page.data;
    if *entity_params.fields && !rows.is_empty() {
        let mut docs: HashMap<_, _> = conn
            .fetch_all::<ConnectionEntityDB>(EntitiesIn(rows.iter().map(|row| row.id).collect()))
            .await?
            .into_iter()
            .map(|entity| (entity.base.id, entity.base.doc.0.0))
            .collect();
        rows.iter_mut()
            .for_each(|row| row.doc = docs.remove(&row.id));
    }
    Ok(page)
}
