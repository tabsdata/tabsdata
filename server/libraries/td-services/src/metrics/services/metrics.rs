//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::metrics_response;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::metrics::{MetricsRequest, MetricsResponse};
use td_objects::types::visible_projects::VisibleProjects;

/// `GET /projects/{project}/metrics`; RLS scopes the read, the query carries
/// no project predicate.
#[service_factory]
async fn metrics(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<MetricsRequest>>,
) -> Result<MetricsResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    metrics_response(&param, &VisibleProjects::All, gated_conn.get(grant)).await
}

/// `GET /metrics`, across every project the requester may view.
#[service_factory]
async fn metrics_global(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ReadRequest<MetricsRequest>,
) -> Result<MetricsResponse, TdError> {
    let (ctx, param) = request.into_parts();
    let (grant, visible_projects) = authz.visible_projects(gated_conn, &ctx).await?;
    metrics_response(&param, &visible_projects, gated_conn.get(grant)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::metrics::Dimension;
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow};
    use td_objects::types::basic::{
        AccessTokenId, EpochMs, IncludePrivate, IncludeReplaced, UserId,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_metrics_empty_window(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = MetricsRequest {
            dim: Dimension::Collection,
            window: TimeWindow::new(
                EpochMs::try_from(0).unwrap(),
                EpochMs::try_from(86_400_000).unwrap(),
            ),
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
            include_replaced: IncludeReplaced::FALSE,
            include_private: IncludePrivate::FALSE,
        };
        let service = Services::with_defaults(db);
        let response = service
            .metrics(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?
            .data;
        assert_eq!(response.dim, Dimension::Collection);
        assert_eq!(*response.start_ms, 0);
        assert_eq!(*response.end_ms, 86_400_000);
        assert!(response.nodes.is_empty());
        assert!(response.edges.is_empty());
        Ok(())
    }
}
