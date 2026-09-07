//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::group::Group;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn list_group(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest>,
) -> Result<ListResponse<Group>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, list_params) = request.into_parts();
    // List + read open to any role with access to the project.
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Group>::try_from(&list_params)?;
    let rows = conn.list(&(), &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, Description, GroupName, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_groups_includes_seeds(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default())
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.list_group(request).await?.data;
        // 3 seeded system groups + 1 user-created.
        assert_eq!(response.len, 4);
        let names: Vec<_> = response.data.iter().map(|g| g.name.clone()).collect();
        assert!(names.contains(&GroupName::default_group()));
        assert!(names.contains(&GroupName::source()));
        assert!(names.contains(&GroupName::destination()));
        assert!(names.contains(&GroupName::try_from("analytics")?));
        Ok(())
    }
}
