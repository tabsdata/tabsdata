//
// Copyright 2026 Tabsdata Inc.
//

//! A role membership belongs to a live user and to a role of the project it
//! was granted on, whatever runs concurrently with the grant.

use crate::Services;
use crate::project::rename_project_roles::rename_project_roles;
use crate::project::seed_groups::seed_default_groups;
use crate::project::seed_project_roles::seed_project_roles;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, Global, Grant, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::project::{
    ActiveProjectDB, ProjectCreate, ProjectDeleteDB, ProjectUpdate, ProjectUpdateDB,
};
use td_objects::dxo::role::{RoleDB, RoleParam};
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user::{UserDB, UserParam};
use td_objects::dxo::user_role::{UserRoleCreate, UserRoleDB};
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, ProjectIdName, ProjectName, RoleIdName, RoleKind,
    RoleName, SimplePermissionType, UserEnabled, UserId, UserIdName, UserName,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::lock_table_exclusive;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

async fn gate(db: &TdPool) -> Result<GatedConnection<RW>, TdError> {
    DbPool::<Global>::new(DomainPools::of_apiserver(db.clone()), Global)
        .begin(Attempt::unretried())
        .await
}

fn create(name: &str) -> Result<ProjectCreate, TdError> {
    Ok(ProjectCreate {
        name: ProjectName::try_from(name)?,
        description: "d".try_into()?,
        base_path: BasePath::default(),
    })
}

async fn memberships_of(db: &TdPool, user: &UserId) -> Result<Vec<UserRoleDB>, TdError> {
    let all: Vec<UserRoleDB> = db.fetch_all(()).await?;
    Ok(all.into_iter().filter(|m| &m.user_id == user).collect())
}

/// A role is granted to U while U's delete is in flight (held here
/// after it removed U's memberships and U). Once both commit, U must have no
/// membership left: either the grant fails or the delete removes it.
#[td_test::test(db)]
async fn test_a_grant_racing_a_user_delete_leaves_no_membership(db: TdPool) -> Result<(), TdError> {
    let user = db
        .seed()
        .user(&UserName::try_from("u_leaving")?, &UserEnabled::TRUE)
        .await?;
    db.seed()
        .role(RoleName::try_from("r_member")?, Description::try_from("d")?)
        .await?;

    let services = Services::with_defaults(db.clone());
    let mut gate = gate(&db).await?;
    let conn = gate.get(Grant::asserted("user delete held open"));
    conn.delete::<UserRoleDB>(user.id).await?;
    conn.delete::<UserDB>(user.id).await?;

    let mut granting = tokio::spawn(async move {
        let request = admin().create(
            RoleParam {
                role: RoleIdName::from_str("r_member")?,
            },
            UserRoleCreate {
                user: UserName::try_from("u_leaving")?,
            },
        );
        services.create_user_role(request).await.map(|_| ())
    });
    let finished = tokio::select! {
        granted = &mut granting => Some(granted),
        _ = gate.get(Grant::asserted("blocking probe")).wait_until_blocking(BLOCKED_WITHIN) => None,
    };
    gate.commit().await?;
    let _ = match finished {
        Some(granted) => granted,
        None => granting.await,
    }
    .expect("grant task");

    let left = memberships_of(&db, &user.id).await?;
    assert!(
        left.is_empty(),
        "the deleted user kept {} membership(s)",
        left.len()
    );
    Ok(())
}

/// U holds the admin, dev and view roles of the empty project P
/// (granted in that order). P is deleted while U is deleted: P's delete is
/// held here after it removed the memberships of its first role, U's delete
/// waits on it, then P's delete moves on to its next role. Neither delete may
/// fail with a deadlock; U's waits for P's and then goes through. The gate
/// replays the project delete's statements in its order (roles by id
/// descending, one membership delete per role).
#[td_test::test(db)]
async fn test_a_project_delete_and_a_user_delete_do_not_deadlock(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let p = services
        .clone()
        .create_project(admin().create((), create("p_gone")?))
        .await?
        .data;
    let user = db
        .seed()
        .user(&UserName::try_from("u_gone")?, &UserEnabled::TRUE)
        .await?;
    let roles: Vec<RoleDB> = db.fetch_all(Some(p.id)).await?;
    for kind in [
        RoleKind::ProjectAdmin,
        RoleKind::ProjectDev,
        RoleKind::ProjectView,
    ] {
        let role = roles
            .iter()
            .find(|r| r.kind == kind)
            .expect("per-project role");
        db.seed().user_role(&user.id, &role.id).await?;
    }

    let ctx = admin();
    let mut gate =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
            .begin(Attempt::unretried())
            .await?;
    let conn = gate.get(Grant::asserted("project delete held open"));
    let current: ActiveProjectDB = conn
        .for_update()
        .fetch_one(&ProjectName::try_from("p_gone")?)
        .await?;
    conn.update::<ProjectDeleteDB>(&current.to_delete_dao(&ctx), current.id)
        .await?;
    let roles: Vec<RoleDB> = conn.fetch_all(Some(current.id)).await?;
    conn.delete::<PermissionDB>(roles[0].id).await?;
    conn.delete::<UserRoleDB>(roles[0].id).await?;

    let deleting_user = tokio::spawn({
        let services = services.clone();
        async move {
            services
                .delete_user(admin().delete(UserParam {
                    user: UserIdName::from_str("u_gone")?,
                }))
                .await
        }
    });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the user delete never waited on the project delete"
    );

    let conn = gate.get(Grant::asserted("project delete held open"));
    let cascade: Result<(), TdError> = async {
        for role in &roles[1..] {
            conn.delete::<PermissionDB>(role.id).await?;
            conn.delete::<UserRoleDB>(role.id).await?;
        }
        conn.delete::<RoleDB>(Some(current.id)).await?;
        Ok(())
    }
    .await;
    if cascade.is_ok() {
        gate.commit().await?;
    } else {
        drop(gate);
    }
    let user_deleted = deleting_user.await.expect("user delete task");

    assert!(
        cascade.is_ok(),
        "the project delete failed: {:?}",
        cascade.err()
    );
    if let Err(error) = user_deleted {
        assert_ne!(
            error.api_error(),
            ApiError::InternalError,
            "the user delete failed: {error:?}"
        );
    }
    Ok(())
}

/// `manage_project` on project `a` is granted to V while `a` is renamed
/// to `b` and another project is created as `a` (both held here while the
/// grant waits between resolving the project and resolving its role). The
/// membership must land in a role of the project the grant resolved and
/// authorized, or the grant must fail; never in the other project's role.
#[td_test::test(db)]
async fn test_a_simple_grant_lands_in_the_resolved_projects_role(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let granted_on = services
        .clone()
        .create_project(admin().create((), create("a")?))
        .await?
        .data;
    let user = db
        .seed()
        .user(&UserName::try_from("u_grantee")?, &UserEnabled::TRUE)
        .await?;

    let mut gate = gate(&db).await?;
    lock_table_exclusive(gate.get(Grant::asserted("role resolution held")), "roles")
        .await
        .expect("lock roles");

    let granting = tokio::spawn({
        let services = services.clone();
        async move {
            let grant = SimplePermissionGrant {
                user: UserName::try_from("u_grantee")?,
                permission: SimplePermissionType::ManageProject,
                project: Some(ProjectIdName::from_str("a")?),
            };
            services
                .grant_simple_permission(admin().create((), grant))
                .await
        }
    });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the grant never waited on the gate"
    );

    let ctx = admin();
    let conn = gate.get(Grant::asserted("rename held open"));
    let current: ActiveProjectDB = conn.fetch_one(&ProjectName::try_from("a")?).await?;
    let rename = ProjectUpdate {
        name: Some(ProjectName::try_from("b")?),
        description: None,
    };
    rename_project_roles(conn, &ctx, &current.id, &rename, &current).await?;
    let current_id = current.id;
    let renamed = ProjectUpdateDB {
        name: Some(ProjectName::try_from("b")?),
        description: None,
        modified_on: admin().time.into(),
        modified_by_id: admin().user_id.into(),
    };
    conn.update::<ProjectUpdateDB>(&renamed, current_id).await?;

    let other = create("a")?.to_dao(&ctx);
    gate.get(Grant::asserted("create held open"))
        .insert(&other)
        .await?;
    let mut gate = gate.enter_project(&other.id).await?;
    let conn = gate.get(Grant::asserted("create held open"));
    seed_default_groups(conn, &ctx).await?;
    seed_project_roles(conn, &ctx, &other.id, &other).await?;
    gate.commit().await?;
    let _ = granting.await.expect("grant task");

    for membership in memberships_of(&db, &user.id).await? {
        let role: RoleDB = db.fetch_one(&membership.role_id).await?;
        assert_eq!(
            role.project_id,
            Some(granted_on.id),
            "the grant on `a` put the user in {}",
            role.name
        );
    }
    Ok(())
}
