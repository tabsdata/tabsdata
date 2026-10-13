//
// Copyright 2026 Tabsdata Inc.
//

//! An update writes what its request changed, never a stale copy of the
//! rest of the row. Each test holds the competing write uncommitted in a
//! gate transaction, runs the real service until it waits on the gate's
//! lock, then commits the gate.

use crate::Services;
use crate::project::delete_project_roles::delete_project_roles;
use crate::project::rename_project_roles::rename_project_roles;
use std::str::FromStr;
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::{
    ActiveCollectionDB, CollectionParam, CollectionUpdate, CollectionUpdateDB,
};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::{
    ActiveProjectDB, ProjectCreate, ProjectDeleteDB, ProjectParam, ProjectUpdate, ProjectUpdateDB,
};
use td_objects::dxo::user::{UserDB, UserParam, UserUpdate, UserUpdateDB};
use td_objects::types::basic::{
    AccessTokenId, BasePath, CollectionIdName, CollectionName, Description, FullName,
    ProjectIdName, ProjectName, UserEnabled, UserId, UserIdName, UserName,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_security::config::PasswordHashingConfig;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

/// A description-only update of P reads P, then waits on the row
/// a concurrent delete of P holds. Once the delete commits, the update must
/// not write P's old name back over the deleted row: the name must be free
/// for a new project.
#[td_test::test(db)]
async fn test_description_update_racing_delete_keeps_the_project_deleted(
    db: TdPool,
) -> Result<(), TdError> {
    let project = |description: &str| -> Result<ProjectCreate, TdError> {
        Ok(ProjectCreate {
            name: ProjectName::try_from("p_race")?,
            description: Description::try_from(description)?,
            base_path: BasePath::default(),
        })
    };
    let p = Services::with_defaults(db.clone())
        .create_project(admin().create((), project("created")?))
        .await?
        .data;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = pools.begin(Attempt::unretried()).await?;
    let conn = gate.get(Grant::asserted("project delete held open"));
    let current: ActiveProjectDB = conn.for_update().fetch_one(&p.id).await?;
    conn.update::<ProjectDeleteDB>(&current.to_delete_dao(&admin()), p.id)
        .await?;
    delete_project_roles(conn, &p.id).await?;

    let update = admin().update(
        ProjectParam {
            project: ProjectIdName::from_str(&format!("~{}", p.id))?,
        },
        ProjectUpdate {
            name: None,
            description: Some(Description::try_from("described while deleted")?),
        },
    );
    let services = Services::with_defaults(db.clone());
    let racing = tokio::spawn(async move { services.update_project(update).await });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the update never waited on the delete"
    );
    gate.commit().await?;
    let _ = racing.await.expect("update task");

    let recreate = admin().create((), project("recreated")?);
    Services::with_defaults(db.clone())
        .create_project(recreate)
        .await
        .map(|_| ())
}

/// A description-only update of P reads P, then waits on the row a
/// concurrent rename of P holds. Once the rename commits, the update must
/// not revert P's name.
#[td_test::test(db)]
async fn test_description_update_racing_rename_keeps_the_new_name(
    db: TdPool,
) -> Result<(), TdError> {
    let p = Services::with_defaults(db.clone())
        .create_project(admin().create(
            (),
            ProjectCreate {
                name: ProjectName::try_from("p_before")?,
                description: Description::try_from("created")?,
                base_path: BasePath::default(),
            },
        ))
        .await?
        .data;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = pools.begin(Attempt::unretried()).await?;
    let conn = gate.get(Grant::asserted("project rename held open"));
    let current: ActiveProjectDB = conn.fetch_one(&p.id).await?;
    let rename = ProjectUpdate {
        name: Some(ProjectName::try_from("p_after")?),
        description: None,
    };
    rename_project_roles(conn, &admin(), &p.id, &rename, &current).await?;
    let renamed = ProjectUpdateDB {
        name: Some(ProjectName::try_from("p_after")?),
        description: None,
        modified_on: admin().time.into(),
        modified_by_id: admin().user_id.into(),
    };
    conn.update::<ProjectUpdateDB>(&renamed, p.id).await?;

    let update = admin().update(
        ProjectParam {
            project: ProjectIdName::from_str(&format!("~{}", p.id))?,
        },
        ProjectUpdate {
            name: None,
            description: Some(Description::try_from("described while renamed")?),
        },
    );
    let services = Services::with_defaults(db.clone());
    let racing = tokio::spawn(async move { services.update_project(update).await });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the update never waited on the rename"
    );
    gate.commit().await?;
    let _ = racing.await.expect("update task");

    let after: ActiveProjectDB = db.fetch_one(&p.id).await?;
    assert_eq!(after.name, ProjectName::try_from("p_after")?);
    Ok(())
}

/// A user edits their own full name while an admin disables them.
/// The edit reads the user row, then waits on the disable's lock; once the
/// disable commits, the edit must not write the user back enabled.
#[td_test::test(db)]
async fn test_self_edit_racing_disable_keeps_the_user_disabled(db: TdPool) -> Result<(), TdError> {
    let user = db
        .seed()
        .user(&UserName::try_from("u_race")?, &UserEnabled::TRUE)
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = pools.begin(Attempt::unretried()).await?;
    let conn = gate.get(Grant::asserted("user disable held open"));
    let disable = UserUpdate {
        full_name: None,
        email: None,
        password: None,
        enabled: Some(UserEnabled::FALSE),
    }
    .to_dao(&admin(), &PasswordHashingConfig::default())?;
    conn.update::<UserUpdateDB>(&disable, user.id).await?;

    let edit = RequestContext::with(AccessTokenId::default(), user.id).update(
        UserParam {
            user: UserIdName::from_str("u_race")?,
        },
        UserUpdate {
            full_name: Some(FullName::try_from("Renamed By Self")?),
            email: None,
            password: None,
            enabled: None,
        },
    );
    let services = Services::with_defaults(db.clone());
    let racing = tokio::spawn(async move { services.update_user(edit).await });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the edit never waited on the disable"
    );
    gate.commit().await?;
    let _ = racing.await.expect("edit task");

    let after: UserDB = db.fetch_one(&user.id).await?;
    assert_eq!(after.enabled, UserEnabled::FALSE, "the disable must stick");
    Ok(())
}

/// A description update of collection C resolves C, then waits on the
/// row a concurrent rename of C holds. Once the rename commits, the update
/// must not revert C's name.
#[td_test::test(db)]
async fn test_collection_description_update_racing_rename_keeps_the_new_name(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_before")?, &UserId::admin())
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut gate = DbPool::in_project(&pools, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let conn = gate.get(Grant::asserted("collection rename held open"));
    let renamed = CollectionUpdateDB {
        name: Some(CollectionName::try_from("c_after")?),
        description: None,
        modified_on: admin().time.into(),
        modified_by_id: admin().user_id.into(),
        base_path: None,
    };
    conn.update::<CollectionUpdateDB>(&renamed, c.id).await?;

    let update = admin()
        .update(
            CollectionParam {
                collection: CollectionIdName::from_str(&format!("~{}", c.id))?,
            },
            CollectionUpdate {
                name: None,
                description: Some(Description::try_from("described while renamed")?),
                base_path: None,
            },
        )
        .in_project(p.id);
    let services = Services::with_defaults(db.clone());
    let racing = tokio::spawn(async move { services.update_collection(update).await });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(BLOCKED_WITHIN)
            .await
            .expect("blocking probe"),
        "the update never waited on the rename"
    );
    gate.commit().await?;
    let _ = racing.await.expect("update task");

    let after: ActiveCollectionDB = db.fetch_one(&c.id).await?;
    assert_eq!(after.name, CollectionName::try_from("c_after")?);
    Ok(())
}
