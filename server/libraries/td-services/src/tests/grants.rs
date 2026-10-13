//
// Copyright 2026 Tabsdata Inc.
//

//! Authorization follows the grants as committed, whatever the cache holds.

use crate::Services;
use std::str::FromStr;
use std::time::Duration;
use td_authz::rbac::{RbacSource, Requires, SystemPermission};
use td_database::access::Attempt;
use td_database::access::{DbPool, Global, Grant, RO};
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainConnection, DomainPools};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::ProjectCreate;
use td_objects::dxo::role::RoleParam;
use td_objects::dxo::user::{UserCreate, UserParam};
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{
    AccessTokenId, BasePath, Password, ProjectName, RoleId, RoleIdName, RoleName, UserEnabled,
    UserId, UserIdName, UserName,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn project(name: &str) -> Result<ProjectCreate, TdError> {
    Ok(ProjectCreate {
        name: ProjectName::try_from(name)?,
        description: "d".try_into()?,
        base_path: BasePath::default(),
    })
}

/// Every user may create projects. A user created while the grants
/// cache is warm must be able to, without waiting for an unrelated grant
/// change to refresh it.
#[td_test::test(db)]
async fn test_a_new_user_can_create_a_project_with_a_warm_grants_cache(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let admin = || RequestContext::with(AccessTokenId::default(), UserId::admin());

    services
        .clone()
        .create_project(admin().create((), project("p_warm")?))
        .await?;

    let create = UserCreate {
        name: UserName::try_from("u_new")?,
        full_name: "u_new".to_string().try_into()?,
        email: None,
        password: Password::try_from("password")?,
        enabled: UserEnabled::TRUE,
    };
    let user = services
        .clone()
        .create_user(admin().create((), create))
        .await?
        .data;

    let as_new_user = RequestContext::with(AccessTokenId::default(), user.id);
    services
        .clone()
        .create_project(as_new_user.create((), project("p_new")?))
        .await?;
    Ok(())
}

async fn manages_access(services: &Services, db: &TdPool, user: &UserId) -> Result<bool, TdError> {
    let mut slot = DomainConnection::<Apiserver, Global, RO>::new(db.begin_ro().await.unwrap());
    let grants = services.auth_context.user_grants(&mut slot, user).await?;
    Ok(grants.allows(&Requires::system(SystemPermission::ManageAccess)))
}

/// An admin revokes a user's `sys_admin` membership while the grants
/// cache is warm, and the request is dropped after its commit, while it waits
/// for the connection it only takes to purge the cache (the request timeout or
/// a client disconnect under pool pressure). The revoke is committed, so the
/// user must no longer be authorized.
#[td_test::test(db)]
async fn test_a_committed_revoke_is_enforced_when_its_request_is_dropped(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let user = db
        .seed()
        .user(&UserName::try_from("u_revoked")?, &UserEnabled::TRUE)
        .await?;
    let membership = db.seed().user_role(&user.id, &RoleId::sys_admin()).await?;
    assert!(manages_access(&services, &db, &user.id).await?);

    let pools = DbPool::<Global>::new(DomainPools::of_apiserver(db.clone()), Global);
    let mut gate = pools.begin(Attempt::unretried()).await?;
    gate.get(Grant::asserted("membership held"))
        .for_update()
        .fetch_one::<UserRoleDB>(&membership.id)
        .await?;

    let revoke = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
        RoleParam {
            role: RoleIdName::from_str(&RoleName::sys_admin().to_string())?,
        },
        UserParam {
            user: UserIdName::from_str("u_revoked")?,
        },
    ));
    let revoking = tokio::spawn({
        let services = services.clone();
        async move { services.delete_user_role(revoke).await }
    });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(Duration::from_secs(10))
            .await
            .expect("blocking probe"),
        "the revoke never waited on the gate"
    );

    let first = tokio::spawn({
        let db = db.clone();
        async move { db.begin().await }
    });
    let second = tokio::spawn({
        let db = db.clone();
        async move { db.begin().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.commit().await?;
    let first = first
        .await
        .expect("first waiter")
        .expect("first connection");
    let second = second
        .await
        .expect("second waiter")
        .expect("second connection");

    let mut first = DomainConnection::<Apiserver, Global, RO>::new(first);
    let left: Option<UserRoleDB> = first.fetch_optional(&membership.id).await?;
    assert!(left.is_none(), "the revoke did not commit");
    revoking.abort();
    let _ = revoking.await;
    drop(first);
    drop(second);

    assert!(
        !manages_access(&services, &db, &user.id).await?,
        "the revoked membership still authorizes"
    );
    Ok(())
}
