//
// Copyright 2026 Tabsdata Inc.
//

//! The bootstrap-seeded reference data: the default admin user, the built-in
//! roles, their user-role bindings, and the seeded permissions.

use super::{count_rows, fetch_text};
use crate::test_utils::TestDb;
use td_security::{
    ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS, ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
    ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
    ENCODED_ID_ROLE_DEFAULT, ENCODED_ID_ROLE_PROJECT_CREATOR, ENCODED_ID_ROLE_SYS_ADMIN,
    ENCODED_ID_USER_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_DEFAULT,
    ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
};

#[tokio::test]
async fn test_tabsdata_db_default_users_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    let id = fetch_text(
        &db.serving,
        "SELECT id FROM users WHERE name = $1",
        &["admin"],
    )
    .await;
    assert_eq!(id, ENCODED_ID_USER_ADMIN);
}

#[tokio::test]
async fn test_tabsdata_db_default_roles_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    test_default_roles_body(&db).await;
}

async fn test_default_roles_body(db: &TestDb) {
    const BY_NAME: &str = "SELECT id FROM roles WHERE name = $1";
    for (name, expected) in [
        ("sys_admin", ENCODED_ID_ROLE_SYS_ADMIN),
        ("default", ENCODED_ID_ROLE_DEFAULT),
        ("project_creator", ENCODED_ID_ROLE_PROJECT_CREATOR),
    ] {
        assert_eq!(
            fetch_text(&db.serving, BY_NAME, &[name]).await,
            expected,
            "role {name}"
        );
    }
    assert_eq!(count_rows(&db.serving, "roles").await, 3);
}

#[tokio::test]
async fn test_tabsdata_db_default_user_roles_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    test_default_user_roles_body(&db).await;
}

async fn test_default_user_roles_body(db: &TestDb) {
    const BY_PAIR: &str = "SELECT id FROM users_roles WHERE user_id = $1 AND role_id = $2";
    for (role, expected) in [
        (
            ENCODED_ID_ROLE_SYS_ADMIN,
            ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
        ),
        (ENCODED_ID_ROLE_DEFAULT, ENCODED_ID_USER_ROLE_ADMIN_DEFAULT),
    ] {
        assert_eq!(
            fetch_text(&db.serving, BY_PAIR, &[ENCODED_ID_USER_ADMIN, role]).await,
            expected,
            "admin binding to role {role}"
        );
    }
    assert_eq!(count_rows(&db.serving, "users_roles").await, 2);
}

#[tokio::test]
async fn test_tabsdata_db_default_permissions_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    test_default_permissions_body(&db).await;
}

async fn test_default_permissions_body(db: &TestDb) {
    const ROOT: &str = "00000000000000000000000204";
    // (role_id, permission_type, entity_type, entity_id, expected permission id)
    let cases: [(&str, &str, &str, &str, &str); 6] = [
        (
            ENCODED_ID_ROLE_SYS_ADMIN,
            "manage_access",
            "system",
            ROOT,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
        ),
        (
            ENCODED_ID_ROLE_SYS_ADMIN,
            "create_projects",
            "system",
            ROOT,
            ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
        ),
        (
            ENCODED_ID_ROLE_SYS_ADMIN,
            "manage_roles",
            "system",
            ROOT,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
        ),
        (
            ENCODED_ID_ROLE_SYS_ADMIN,
            "manage_project",
            "project",
            ROOT,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
        ),
        (
            ENCODED_ID_ROLE_DEFAULT,
            "create_projects",
            "system",
            ROOT,
            ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS,
        ),
        (
            ENCODED_ID_ROLE_PROJECT_CREATOR,
            "create_projects",
            "system",
            ROOT,
            ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
        ),
    ];
    const BY_GRANT: &str = "SELECT id FROM permissions WHERE role_id = $1 \
         AND permission_type = $2 AND entity_type = $3 AND entity_id = $4";
    for (role_id, permission_type, entity_type, entity_id, expected) in cases {
        assert_eq!(
            fetch_text(
                &db.serving,
                BY_GRANT,
                &[role_id, permission_type, entity_type, entity_id]
            )
            .await,
            expected,
            "permission {permission_type} for {role_id}"
        );
    }
    assert_eq!(count_rows(&db.serving, "permissions").await, 6);
}
