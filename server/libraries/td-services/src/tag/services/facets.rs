//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::tag::{TagFacetParams, TagKeyParam};
use td_objects::types::basic::{TagKey, TagValue};
use td_queries::Queries;

#[service_factory]
async fn tag_keys(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<TagFacetParams>>,
) -> Result<Vec<TagKey>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, facet) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    Ok(conn.tags().keys(&facet).await?)
}

#[service_factory]
async fn tag_values(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(TagKeyParam, TagFacetParams)>>,
) -> Result<Vec<TagValue>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (key, facet)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    Ok(conn.tags().values(&key.key, &facet).await?)
}
