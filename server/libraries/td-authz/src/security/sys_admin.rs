//
// Copyright 2026 Tabsdata Inc.
//

//! Startup reconciliation of the built-in `sys_admin` role with
//! extension-contributed permissions.
//!
//! The OS seeds `sys_admin`'s built-in permissions at schema creation
//! (`td-schema`), which cannot know about extension permissions. Extensions
//! declare permissions that ride on `sys_admin` via
//! [`AuthzRegistry::grant_to_sys_admin`](crate::rbac::AuthzRegistry::grant_to_sys_admin);
//! this reconciler — run at apiserver startup with the wired registry — adds
//! any such rows that are not already present. It is idempotent, so it is safe
//! to run on every boot, and both this and the RBAC→Simple representability
//! check read the same `sys_admin_permissions()` list, so they stay consistent.

use std::collections::HashSet;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::TryUpdater;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::permission::{PermissionDB, PermissionDBBuilder};
use td_objects::types::basic::{
    AccessTokenId, EntityId, Fixed, PermissionEntityType, PermissionType, RoleId, UserId,
};
use td_queries::LiveEntry;

/// Ensure the built-in `sys_admin` role holds every extension-contributed
/// permission in `sys_admin_extras` (`(entity_type, permission)` pairs). Rows
/// already present are skipped; the extension rows are stored like the built-in
/// system rows — on `__all__`, system-granted, `fixed = true`.
pub async fn ensure_sys_admin_permissions(
    conn: &mut DomainConnection<Apiserver>,
    sys_admin_extras: &[(&str, &str)],
) -> Result<(), TdError> {
    if sys_admin_extras.is_empty() {
        return Ok(());
    }

    let sys_admin = RoleId::sys_admin();
    let existing = conn.live().fetch_all::<PermissionDB>(sys_admin).await?;
    let present: HashSet<(String, String)> = existing
        .iter()
        .map(|p| (p.entity_type.to_string(), p.permission_type.to_string()))
        .collect();

    // The reconciler-granted rows are attributed to the system user, like the
    // seeded built-ins.
    let context = RequestContext::with(AccessTokenId::default(), UserId::system());

    for &(entity_type, permission) in sys_admin_extras {
        if present.contains(&(entity_type.to_string(), permission.to_string())) {
            continue;
        }
        let mut builder = PermissionDBBuilder::default();
        builder.try_update_from(&context)?;
        builder
            .role_id(sys_admin)
            .permission_type(PermissionType::try_from(permission)?)
            .entity_type(PermissionEntityType::try_from(entity_type)?)
            .entity_id(EntityId::all_entities())
            .fixed(Fixed::from(true));
        let permission_db = builder.build()?;
        conn.live().insert(&permission_db).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;

    #[td_test::test(db)]
    async fn test_ensure_sys_admin_permissions_is_idempotent(db: TdPool) -> Result<(), TdError> {
        let extras: &[(&str, &str)] = &[("ai", "manage_ai")];

        // Run the reconciler in a committed read-write transaction (the
        // production startup path) so the row persists between calls.
        async fn reconcile(db: &TdPool, extras: &[(&str, &str)]) -> Result<(), TdError> {
            let tx = db.begin().await.unwrap();
            let mut conn = DomainConnection::from_transaction(tx);
            ensure_sys_admin_permissions(&mut conn, extras).await?;
            conn.into_transaction().unwrap().commit().await.unwrap();
            Ok(())
        }

        async fn count_ai(db: &TdPool) -> Result<usize, TdError> {
            let mut conn = db.acquire().await.unwrap();
            let perms = conn.live().fetch_all::<PermissionDB>(()).await?;
            Ok(perms
                .iter()
                .filter(|p| {
                    p.role_id == RoleId::sys_admin()
                        && p.entity_type.as_str() == "ai"
                        && p.permission_type.as_str() == "manage_ai"
                })
                .count())
        }

        // not present initially
        assert_eq!(count_ai(&db).await?, 0);
        // first run inserts the row
        reconcile(&db, extras).await?;
        assert_eq!(count_ai(&db).await?, 1);
        // second run is a no-op (no duplicate, no unique-index violation)
        reconcile(&db, extras).await?;
        assert_eq!(count_ai(&db).await?, 1);

        Ok(())
    }
}
