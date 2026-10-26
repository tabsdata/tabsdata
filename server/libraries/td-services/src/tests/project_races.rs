//
// Copyright 2026 Tabsdata Inc.
//

//! Project create, rename and delete against each other and against
//! unauthorized callers: the loser gets a clean error, never a deadlock or a
//! raw constraint violation, and no caller without authority holds the
//! project's row.

use crate::Services;
use crate::project::delete_project_roles::delete_project_roles;
use crate::project::rename_project_roles::rename_project_roles;
use crate::project::seed_groups::seed_default_groups;
use crate::project::seed_project_roles::seed_project_roles;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, DbScope, GatedConnection, Global, Grant, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainPools, ScopedBy};
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::{ProjectCreate, ProjectDB, ProjectParam, ProjectUpdate};
use td_objects::types::basic::{
    AccessTokenId, BasePath, ProjectIdName, ProjectName, UserEnabled, UserId, UserName,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::{lock_row_nowait, lock_table_exclusive};

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

fn pools(db: &TdPool) -> DbPool<Global> {
    DbPool::<Global>::new(DomainPools::of_apiserver(db.clone()), Global)
}

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn create(name: &str) -> Result<ProjectCreate, TdError> {
    Ok(ProjectCreate {
        name: ProjectName::try_from(name)?,
        description: "d".try_into()?,
        base_path: BasePath::default(),
        secrets: Vec::new(),
    })
}

fn rename(to: &str) -> Result<ProjectUpdate, TdError> {
    Ok(ProjectUpdate {
        name: Some(ProjectName::try_from(to)?),
        description: None,
        secrets: None,
    })
}

fn param(name: &str) -> Result<ProjectParam, TdError> {
    Ok(ProjectParam {
        project: ProjectIdName::from_str(name)?,
    })
}

async fn create_project(services: &Services, name: &str) -> Result<(), TdError> {
    services
        .clone()
        .create_project(admin().create((), create(name)?))
        .await?;
    Ok(())
}

async fn wait_until_blocking<S: DbScope>(gate: &mut GatedConnection<RW, S>, what: &str)
where
    Apiserver: ScopedBy<S>,
{
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "{what} never waited on the gate"
    );
}

/// A user without `manage_project` on P tries to delete it. The
/// attempt must not lock P's row: while it is still deciding (held here on
/// the grants it loads), another unit can take P's row lock without waiting.
#[td_test::test(db)]
async fn test_an_unauthorized_delete_does_not_lock_the_project(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let stranger = db
        .seed()
        .user(&UserName::try_from("u_stranger")?, &UserEnabled::TRUE)
        .await?;

    let mut gate = pools(&db).begin(Attempt::unretried()).await?;
    lock_table_exclusive(gate.get(Grant::asserted("grants load held")), "users_roles")
        .await
        .expect("lock permissions");

    let services = Services::with_defaults(db.clone());
    let deleting = tokio::spawn(async move {
        let request =
            RequestContext::with(AccessTokenId::default(), stranger.id).delete(ProjectParam {
                project: ProjectIdName::from_str("test")?,
            });
        services.delete_project(request).await
    });
    wait_until_blocking(&mut gate, "the unauthorized delete").await;

    let probe = lock_row_nowait(
        gate.get(Grant::asserted("row lock probe")),
        "projects",
        &p.base.project_id.to_string(),
    )
    .await;
    drop(gate);
    let deleted = deleting.await.expect("delete task");

    assert!(deleted.is_err(), "the unauthorized delete went through");
    assert!(
        probe.is_ok(),
        "an unauthorized delete holds the project's row lock: {probe:?}"
    );
    Ok(())
}

/// P is renamed to `p_new` while a create of `p_new` is in flight
/// (held here after all its writes). The rename must lose with the same clean
/// error a rename to an already existing name gets, not a raw unique violation
/// or a deadlock.
#[td_test::test(db)]
async fn test_a_rename_racing_a_create_of_its_name_fails_cleanly(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    create_project(&services, "p_old").await?;

    let ctx = admin();
    let gate = pools(&db).begin(Attempt::unretried()).await?;
    let project_create = create("p_new")?;
    let project = project_create.to_dao(&ctx);
    let mut gate = gate.enter_project(&project.project_id).await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("create held open"));
    conn.topology().bump(&project.project_id, at).await?;
    conn.scd2().open(&project, at).await?;
    seed_default_groups(conn, &ctx, at).await?;
    seed_project_roles(conn, &ctx, &project.project_id, &project_create.name).await?;

    let renaming = tokio::spawn({
        let services = services.clone();
        async move {
            services
                .update_project(admin().update(param("p_old")?, rename("p_new")?))
                .await
        }
    });
    wait_until_blocking(&mut gate, "the rename").await;
    gate.commit().await?;
    let raced = renaming
        .await
        .expect("rename task")
        .expect_err("the name was taken");

    let sequential = services
        .clone()
        .update_project(admin().update(param("p_old")?, rename("p_new")?))
        .await
        .expect_err("the name is taken");
    assert_eq!(
        raced.code(),
        sequential.code(),
        "the racing rename failed with {raced}"
    );
    Ok(())
}

/// A create of `p_new` while P's rename to `p_new` is in flight
/// (held here after all its writes). The create must lose with the same clean
/// error a create of an already existing name gets, not a raw unique
/// violation or a deadlock.
#[td_test::test(db)]
async fn test_a_create_racing_a_rename_to_its_name_fails_cleanly(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    create_project(&services, "p_old").await?;

    let ctx = admin();
    let mut gate = pools(&db).begin(Attempt::unretried()).await?;
    let current: ProjectDB = gate
        .get(Grant::asserted("rename held open"))
        .scd2()
        .fetch_one(&ProjectName::try_from("p_old")?)
        .await?;
    let update = rename("p_new")?;
    let mut gate = gate.enter_project(&current.base.project_id).await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("rename held open"));
    conn.scd2()
        .open(&current.updated(&update, &ctx), at)
        .await?;
    rename_project_roles(conn, &ctx, &current.base.project_id, &update, &current).await?;

    let creating = tokio::spawn({
        let services = services.clone();
        async move { create_project(&services, "p_new").await }
    });
    wait_until_blocking(&mut gate, "the create").await;
    gate.commit().await?;
    let raced = creating
        .await
        .expect("create task")
        .expect_err("the name was taken");

    let sequential = create_project(&services, "p_new")
        .await
        .expect_err("the name is taken");
    assert_eq!(
        raced.code(),
        sequential.code(),
        "the racing create failed with {raced}"
    );
    Ok(())
}

/// P is renamed while its delete is in flight (held here after it took P's
/// topology and closed P). The rename waits for P; then the delete drops
/// P's roles. Neither may fail with a deadlock: the delete goes through and the
/// rename either finds P gone or fails with a domain error.
#[td_test::test(db)]
async fn test_a_rename_racing_a_delete_does_not_deadlock(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    create_project(&services, "p_old").await?;

    let mut gate =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
            .begin(Attempt::unretried())
            .await?;
    let current: ProjectDB = gate
        .get(Grant::asserted("delete held open"))
        .scd2()
        .fetch_one(&ProjectName::try_from("p_old")?)
        .await?;
    let mut gate = gate.enter_project(&current.base.project_id).await?;
    let at = gate.locked_at();
    let conn = gate.get(Grant::asserted("delete held open"));
    conn.scd2()
        .close::<ProjectDB>(&current.base.project_id, at)
        .await?;

    let renaming = tokio::spawn({
        let services = services.clone();
        async move {
            services
                .update_project(admin().update(param("p_old")?, rename("p_new")?))
                .await
        }
    });
    wait_until_blocking(&mut gate, "the rename").await;
    let cascade = delete_project_roles(
        gate.get(Grant::asserted("delete held open")),
        &current.base.project_id,
    )
    .await;
    if cascade.is_ok() {
        gate.commit().await?;
    } else {
        drop(gate);
    }
    let renamed = renaming.await.expect("rename task");

    assert!(
        cascade.is_ok(),
        "the project delete failed: {:?}",
        cascade.err()
    );
    if let Err(error) = renamed {
        assert_ne!(
            error.api_error(),
            ApiError::InternalError,
            "the rename failed: {error:?}"
        );
    }
    Ok(())
}
