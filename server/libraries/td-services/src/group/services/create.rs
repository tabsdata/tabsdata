//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::dxo::group::{Group, GroupCreate, GroupDB, GroupDBWithNames};
use td_queries::Queries;

#[service_factory]
async fn create_group(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<CreateRequest<GroupCreate>>,
) -> Result<Group, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _, create) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    let group = create.to_dao(&ctx);
    // A duplicate name within the project (the connection is project-scoped,
    // matching the `groups___project_id__name___uidx` UNIQUE index) reports a
    // clean "already exists" error instead of the raw unique-constraint
    // violation from the insert.
    conn.assert_not_exists::<GroupDB>(&group.name).await?;
    conn.insert(&group).await?;

    let with_names: GroupDBWithNames = conn.fetch_one(&group.id).await?;
    let read = Group::from_dao(with_names)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_error::assert_service_error;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::project::ProjectCreate;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::ProjectName;
    use td_objects::types::basic::{AccessTokenId, BasePath, Description, GroupName, UserId};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_create_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let create = GroupCreate {
            name: "analytics".try_into()?,
            description: "Analytics tier group".try_into()?,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.create_group(request).await?.data;

        let found = db
            .fetch_one::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert_eq!(response.id, found.base.id);
        assert_eq!(response.name, found.base.name);
        assert_eq!(response.description, found.base.description);
        assert_eq!(response.created_by_id, found.base.created_by_id);
        assert_eq!(response.modified_by_id, found.base.modified_by_id);
        // User-created groups are never system.
        assert!(!*found.base.system);
        Ok(())
    }

    /// Creating a group with a name that already exists in the project is
    /// rejected up front with a clean `SqlError::EntityAlreadyExists` (via the
    /// `assert_not_exists` pre-check) rather than the raw
    /// `UniqueViolation` from the `groups___project_id__name___uidx` constraint.
    /// (Case-insensitive uniqueness is a follow-up; for now the check is
    /// exact-match, matching the collections convention.)
    #[td_test::test(db)]
    async fn test_create_duplicate_name_rejected(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let name = GroupName::try_from("analytics").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .group(name.clone(), Description::try_from("first one").unwrap())
            .await;

        let service = Services::with_defaults(db.clone());
        let create = GroupCreate {
            name,
            description: Description::try_from("collision").unwrap(),
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create((), create)
            .in_project(p.id);

        assert_service_error(service.create_group(request).await, |err| match err {
            SqlError::EntityAlreadyExists(_, _) => {}
            other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
        });
    }

    /// Group names are unique *within* a project, not globally — the
    /// `groups___project_id__name___uidx` is on `(project_id, name)`. Two projects
    /// can each hold a group of the same name. (The within-project rejection is
    /// covered by `test_create_duplicate_name_rejected`.) PostgreSQL only
    /// (multi-project needs RLS).
    #[td_test::test(db)]
    async fn test_group_name_unique_per_project(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let ctx = || RequestContext::with(AccessTokenId::default(), UserId::admin());
        let dup = || GroupCreate {
            name: "dup".try_into().unwrap(),
            description: "d".try_into().unwrap(),
        };

        // `dup` in the first project.
        Services::with_defaults(db.clone())
            .create_group(ctx().create((), dup()).in_project(p.id))
            .await?;

        // A second project, then the same group name within it — succeeds
        // because uniqueness is scoped by `project_id`.
        let p2 = Services::with_defaults(db.clone())
            .create_project(ctx().create(
                (),
                ProjectCreate {
                    name: ProjectName::try_from("p2")?,
                    description: Description::try_from("p2")?,
                    base_path: BasePath::default(),
                },
            ))
            .await?
            .data;

        Services::with_defaults(db.clone())
            .create_group(ctx().create((), dup()).in_project(p2.id))
            .await?;
        Ok(())
    }
}
