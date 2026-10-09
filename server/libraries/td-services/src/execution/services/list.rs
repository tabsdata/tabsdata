//
// Copyright 2025 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_queries::Queries;

#[service_factory]
async fn execution_list(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ListRequest,
) -> Result<ListResponse<Execution>, TdError> {
    let (ctx, _, list_params) = request.into_parts();
    let (grant, visible_projects) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let query_params = ListQueryParams::<Execution>::try_from(&list_params)?;
    let rows = conn.list(&visible_projects, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::project::ProjectDeleteDB;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ProjectName, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::{SeedEntry, seed_project_viewer, seed_run};

    #[td_test::test(db)]
    async fn test_list_execution(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            parent_tables: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let executions = [
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
            db.scoped(p.id).seed().execution(&function_version).await?,
        ];

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());

        let response = service.execution_list(request).await?.data;
        assert_eq!(response.len, executions.len());
        assert_eq!(response.data[0].id, executions[0].base.id);
        assert_eq!(
            response.data[0].triggered_on,
            executions[0].base.triggered_on
        );
        assert_eq!(response.data[1].id, executions[1].base.id);
        assert_eq!(
            response.data[1].triggered_on,
            executions[1].base.triggered_on
        );
        assert_eq!(response.data[2].id, executions[2].base.id);
        assert_eq!(
            response.data[2].triggered_on,
            executions[2].base.triggered_on
        );
        assert_eq!(response.data[3].id, executions[3].base.id);
        assert_eq!(
            response.data[3].triggered_on,
            executions[3].base.triggered_on
        );
        assert_eq!(response.data[4].id, executions[4].base.id);
        assert_eq!(
            response.data[4].triggered_on,
            executions[4].base.triggered_on
        );
        let base = Some(p.name.clone());
        assert!(response.data.iter().all(|e| e.project == base));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_wildcard_spans_projects(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_exec = seed_run(&db, alpha.id).await?.execution;

        let service = Services::with_defaults(db.clone());
        let wildcard = service
            .execution_list(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        // The wildcard spans both projects and each row resolves to its own.
        let base_row = wildcard.data.iter().find(|e| e.id == base_exec).unwrap();
        assert_eq!(base_row.project, Some(base.name.clone()));
        let alpha_row = wildcard.data.iter().find(|e| e.id == alpha_exec).unwrap();
        assert_eq!(alpha_row.project, Some(ProjectName::try_from("alpha")?));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_wildcard_filters_to_viewable(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_exec = seed_run(&db, alpha.id).await?.execution;

        let user = seed_project_viewer(&db, base.id).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .execution_list(
                RequestContext::with(AccessTokenId::default(), user.id)
                    .list((), ListParams::default()),
            )
            .await?
            .data;

        assert!(response.data.iter().any(|e| e.id == base_exec));
        assert!(!response.data.iter().any(|e| e.id == alpha_exec));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_execution_wildcard_skips_deleted_project(db: TdPool) -> Result<(), TdError> {
        let base = db.seed().test_project().await?;
        let base_exec = seed_run(&db, base.id).await?.execution;
        let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
        let alpha_exec = seed_run(&db, alpha.id).await?.execution;

        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        db.update::<ProjectDeleteDB>(&alpha.to_delete_dao(&ctx), alpha.id)
            .await?;

        let service = Services::with_defaults(db.clone());
        let wildcard = service
            .execution_list(ctx.list((), ListParams::default()))
            .await?
            .data;

        assert!(wildcard.data.iter().any(|e| e.id == base_exec));
        assert!(!wildcard.data.iter().any(|e| e.id == alpha_exec));
        Ok(())
    }
}
