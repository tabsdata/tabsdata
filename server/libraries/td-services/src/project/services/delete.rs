//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::ProjectError;
use crate::project::delete_project_roles::delete_project_roles;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, RbacSource};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::project::{ActiveProjectDB, ProjectDeleteDB, ProjectParam};
use td_objects::types::basic::ProjectId;
use td_queries::Queries;

#[service_factory]
async fn delete_project(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: DeleteRequest<ProjectParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let id_name = &param.project;
        let conn = trx.get(authz.none());
        let current = conn.fetch_one::<ActiveProjectDB>(&id_name).await?;
        let _grant = authz
            .require::<Project, ManageProject>(conn, &ctx, &current.id)
            .await?;

        // Guard before the soft-delete: never strand a non-empty project's data.
        let collections: Vec<ActiveCollectionDB> = conn.fetch_all(current.id).await?;
        reject_delete_non_empty_project(&current.id, &collections)?;

        let delete_db = current.to_delete_dao(&ctx);
        conn.update::<ProjectDeleteDB>(&delete_db, current.id)
            .await?;
        // System cascade: drop the project's three per-project roles together
        // with their permission + membership rows (bypasses the `fixed` guard;
        // see `delete_project_roles`).
        delete_project_roles(conn, &current.id).await?;
        trx.commit().await?;
        Ok(())
    })
    .await
}

/// Reject deleting a project that still owns collections. Collections are the
/// top of the project-scoped data tree (functions, tables, runs, … all hang
/// off a collection), so "no collections" means "no project-scoped data".
/// Deleting a non-empty project would leave those rows in place but
/// unreachable — the resolver reads `projects_active`, so once the project is
/// soft-deleted no request can ever set the GUC back to it.
///
/// The delete-project service spans projects (bypass scope), so the
/// collections fetch sees the target project's collections regardless of
/// the request's own scope.
pub fn reject_delete_non_empty_project(
    project_id: &ProjectId,
    collections: &[ActiveCollectionDB],
) -> Result<(), TdError> {
    if !collections.is_empty() {
        Err(ProjectError::ProjectNotEmpty(project_id.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, CollectionIdName, ProjectIdName, UserId};
    use td_queries::test_utils::{SeedEntry, seed_run};

    #[td_test::test(db)]
    async fn test_delete_project_hides_it_and_its_plans(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let execution = seed_run(&db, p.id).await?.execution;
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let service = Services::with_defaults(db.clone());
        let delete_project = || {
            ctx.delete(ProjectParam {
                project: ProjectIdName::from_str(&format!("~{}", p.id)).unwrap(),
            })
        };

        let err = service.delete_project(delete_project()).await.unwrap_err();
        assert!(matches!(
            err.domain_err(),
            ProjectError::ProjectNotEmpty(id) if *id == p.id.to_string()
        ));

        service
            .delete_collection(
                ctx.delete(CollectionParam {
                    collection: CollectionIdName::from_str("collection")?,
                })
                .in_project(p.id),
            )
            .await?;
        service.delete_project(delete_project()).await?;

        let projects = service
            .list_projects(ctx.list((), ListParams::default()))
            .await?
            .data;
        assert!(projects.data.iter().all(|row| row.id != p.id));
        let executions = service
            .execution_list(ctx.list((), ListParams::default()))
            .await?
            .data;
        assert!(executions.data.iter().all(|row| row.id != execution));
        let live: Option<ActiveProjectDB> = db.fetch_optional(&p.id).await?;
        assert!(live.is_none());
        Ok(())
    }
}
