//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::seed_groups::seed_default_groups;
use crate::project::seed_project_roles::seed_project_roles;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{CreateProjects, RbacSource, System, SystemEntity};
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::project::{
    ActiveProjectDB, ActiveProjectDBWithNames, ProjectCreate, ProjectRead,
};
use td_queries::Queries;

#[service_factory]
async fn create_project(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<ProjectCreate>,
) -> Result<ProjectRead, TdError> {
    let (ctx, _, create) = request.into_parts();
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<System, CreateProjects>(&mut trx, &ctx, &SystemEntity)
            .await?;
        let conn = trx.get(grant);

        let project = create.clone().to_dao(&ctx);
        // A duplicate project name reports a clean "already exists" error
        // instead of the raw unique-constraint violation from the insert.
        conn.assert_not_exists::<ActiveProjectDB>(&project.name)
            .await?;
        conn.insert(&project)
            .await
            .map_err(SqlError::already_exists)?;

        // Enter the new project for the rest of the unit of work: the seeds below
        // default `project_id` from the GUC instead of stamping it by hand, and
        // the connection is typed for it.
        let mut trx = trx.enter_project(&project.id).await?;
        let conn = trx.get(grant);
        // A collection resolves its group within its own project, so seed the
        // system groups (default/source/destination) into the new project.
        seed_default_groups(conn, &ctx).await?;
        seed_project_roles(conn, &ctx, &project.id, &project).await?;

        let with_names: ActiveProjectDBWithNames = conn.fetch_one(&project.id).await?;
        let read = ProjectRead::from_dao(with_names)?;
        trx.commit().await?;
        Ok(read)
    })
    .await
}
