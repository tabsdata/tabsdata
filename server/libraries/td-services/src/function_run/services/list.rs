//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::logical::FunctionRun;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::LiveEntry;

#[service_factory]
async fn function_run_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<FunctionRun>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<FunctionRun>::try_from(&list_params)?;
    let rows = conn.live().list(&visible, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}
#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, ProjectName, UserId};
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};

    #[td_test::test(db)]
    async fn test_list_function_run_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        seed_run(&db, alpha.id).await?;

        let service = Services::with_defaults(db.clone());
        let wildcard = service
            .function_run_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        // The wildcard spans both projects and each row resolves its own name.
        let projects: Vec<_> = wildcard
            .data
            .iter()
            .filter_map(|r| r.project.clone())
            .collect();
        assert!(projects.contains(&base.name.clone()));
        assert!(projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_function_run_wildcard_filters_to_viewable(
        db: TdPool,
    ) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        seed_run(&db, base.id).await?;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        seed_run(&db, alpha.id).await?;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .function_run_list(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        let projects: Vec<_> = response
            .data
            .iter()
            .filter_map(|r| r.project.clone())
            .collect();
        assert!(projects.contains(&base.name.clone()));
        assert!(!projects.contains(&ProjectName::try_from("alpha")?));
        Ok(())
    }
}
