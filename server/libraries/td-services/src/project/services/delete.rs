//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::ProjectError;
use crate::project::delete_project_roles::delete_project_roles;
use crate::tag::user::close_user_tags_of;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{AcrossProjects, DbPool, retrying};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::project::{ProjectDB, ProjectParam};
use td_objects::types::basic::{GroupId, ProjectId};
use td_queries::Queries;

#[service_factory]
async fn delete_project(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: DeleteRequest<ProjectParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    let project = {
        let mut conn = db.read().await?;
        let current: ProjectDB = conn
            .get(authz.none())
            .scd2()
            .fetch_one(&param.project)
            .await?;
        current.project_id
    };
    let in_project = db.in_project(&project);
    retrying(|attempt| async {
        let mut trx = in_project.lock_project().begin(attempt).await?;
        let id_name = &param.project;
        let conn = trx.get(authz.none());
        let current = conn.scd2().fetch_one::<ProjectDB>(&id_name).await?;
        authz
            .project_visible_or_not_found(conn, &ctx, &current.project_id)
            .await?;
        let _grant = authz
            .require::<Project, ManageProject>(conn, &ctx, &current.project_id)
            .await?;

        // Guard before the delete: never strand a non-empty project's data.
        let collections: Vec<CollectionDB> = conn.scd2().fetch_all(current.project_id).await?;
        reject_delete_non_empty_project(&current.project_id, &collections)?;

        // The project row closes before the role cascade, as a rename writes it
        // before its roles: both serialize on the project row, never on each
        // other's roles.
        let at = trx.locked_at();
        let conn = trx.get(authz.none());
        let groups: Vec<GroupDB> = conn.scd2().fetch_all(&current.project_id).await?;
        let group_ids: Vec<GroupId> = groups.iter().map(|g| g.group_id).collect();
        conn.scd2()
            .close::<ProjectDB>(&current.project_id, at)
            .await?;
        conn.scd2()
            .close::<GroupDB>(&current.project_id, at)
            .await?;
        close_user_tags_of(conn, &[current.project_id], at).await?;
        close_user_tags_of(conn, &group_ids, at).await?;
        // System cascade: drop the project's three per-project roles together
        // with their permission + membership rows (bypasses the `fixed` guard;
        // see `delete_project_roles`).
        delete_project_roles(conn, &current.project_id).await?;
        trx.commit().await?;
        Ok(())
    })
    .await
}

/// Reject deleting a project that still owns collections. Collections are the
/// top of the project-scoped data tree (functions, tables, runs, … all hang
/// off a collection), so "no collections" means "no project-scoped data".
/// Deleting a non-empty project would leave those rows in place but
/// unreachable — the resolver reads the open project versions, so once the
/// project is deleted no request can ever set the GUC back to it.
///
/// The delete-project service spans projects (bypass scope), so the
/// collections fetch sees the target project's collections regardless of
/// the request's own scope.
pub fn reject_delete_non_empty_project(
    project_id: &ProjectId,
    collections: &[CollectionDB],
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
    use td_objects::dxo::group::GroupDB;
    use td_objects::types::basic::{AccessTokenId, CollectionIdName, ProjectIdName, UserId};
    use td_queries::test_utils::{SeedEntry, seed_run};

    #[td_test::test(db)]
    async fn test_delete_project_hides_it_and_its_plans(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let execution = seed_run(&db, p.project_id).await?.execution;
        let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let service = Services::with_defaults(db.clone());
        let delete_project = || {
            ctx.delete(ProjectParam {
                project: ProjectIdName::from_str(&format!("~{}", p.project_id)).unwrap(),
            })
        };

        let err = service.delete_project(delete_project()).await.unwrap_err();
        assert!(matches!(
            err.domain_err(),
            ProjectError::ProjectNotEmpty(id) if *id == p.project_id.to_string()
        ));

        service
            .delete_collection(
                ctx.delete(CollectionParam {
                    collection: CollectionIdName::from_str("collection")?,
                })
                .in_project(p.project_id),
            )
            .await?;
        service.delete_project(delete_project()).await?;

        let projects = service
            .list_projects(ctx.list((), ListParams::default()))
            .await?
            .data;
        assert!(
            projects
                .data
                .iter()
                .all(|row| row.project_id != p.project_id)
        );
        let executions = service
            .execution_list(ctx.list((), ListParams::default()))
            .await?
            .data;
        assert!(executions.data.iter().all(|row| row.id != execution));
        let live: Option<ProjectDB> = db.scd2().fetch_optional(&p.project_id).await?;
        assert!(live.is_none());

        // The project's groups close with it.
        let groups: Vec<GroupDB> = db.scoped(p.project_id).scd2().fetch_all(()).await?;
        assert!(groups.is_empty());
        let versions: Vec<GroupDB> = db.scoped(p.project_id).timeline().fetch_all(()).await?;
        assert_eq!(versions.len(), 3);
        assert!(versions.iter().all(|g| !g.interval.valid_to.is_infinity()));
        Ok(())
    }
}
