//
// Copyright 2026 Tabs Data Inc.
//

//! The bootstrap-seeded reference data: the default admin user, the built-in
//! roles, their user-role bindings, and the seeded permissions.

use super::{Value, fetch_id};
use crate::raw_query;
use crate::sea_query::builder::SeaQueryExt;
use crate::test_utils::TestDb;
use td_security::{
    ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS, ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
    ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
    ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
    ENCODED_ID_ROLE_DEFAULT, ENCODED_ID_ROLE_PROJECT_CREATOR, ENCODED_ID_ROLE_SYS_ADMIN,
    ENCODED_ID_USER_ADMIN, ENCODED_ID_USER_ROLE_ADMIN_DEFAULT,
    ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN,
};

#[cfg(sqlite)]
#[tokio::test]
async fn test_tabsdata_db_default_users_sqlite() {
    let db = crate::test_utils::sqlite::pool().await;
    db.upgrade().await.unwrap();
    let row: Value = raw_query!(db, "SELECT id FROM users WHERE name = 'admin'")
        .build_query_as()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
}

#[cfg(postgres)]
#[tokio::test]
async fn test_tabsdata_db_default_users_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    let row: Value = raw_query!(db, "SELECT id FROM users WHERE name = 'admin'")
        .build_query_as()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(row.id, ENCODED_ID_USER_ADMIN);
}

#[cfg(sqlite)]
#[tokio::test]
async fn test_tabsdata_db_default_roles_sqlite() {
    let db = crate::test_utils::sqlite::pool().await;
    db.upgrade().await.unwrap();
    test_default_roles_body(&db).await;
}

#[cfg(postgres)]
#[tokio::test]
async fn test_tabsdata_db_default_roles_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    test_default_roles_body(&db).await;
}

async fn test_default_roles_body(db: &TestDb) {
    // `raw_query!` interpolates only consts/paths (macro hygiene can't see
    // locals), so each role name is inlined as a literal.
    let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'sys_admin'")
        .build_query_as()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(row.id, ENCODED_ID_ROLE_SYS_ADMIN);

    let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'default'")
        .build_query_as()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(row.id, ENCODED_ID_ROLE_DEFAULT);

    let row: Value = raw_query!(db, "SELECT id FROM roles WHERE name = 'project_creator'")
        .build_query_as()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(row.id, ENCODED_ID_ROLE_PROJECT_CREATOR);

    let count: i64 = raw_query!(db, "SELECT count(*) FROM roles")
        .build_query_scalar()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(count, 3);
}

#[cfg(sqlite)]
#[tokio::test]
async fn test_tabsdata_db_default_user_roles_sqlite() {
    let db = crate::test_utils::sqlite::pool().await;
    db.upgrade().await.unwrap();
    test_default_user_roles_body(&db).await;
}

#[cfg(postgres)]
#[tokio::test]
async fn test_tabsdata_db_default_user_roles_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    test_default_user_roles_body(&db).await;
}

async fn test_default_user_roles_body(db: &TestDb) {
    let row: Value = raw_query!(
        db,
        r#"SELECT id FROM users_roles
           WHERE user_id = {ENCODED_ID_USER_ADMIN} AND role_id = {ENCODED_ID_ROLE_SYS_ADMIN}"#
    )
    .build_query_as()
    .fetch_one(&db.serving)
    .await
    .unwrap();
    assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_SYS_ADMIN);

    let row: Value = raw_query!(
        db,
        r#"SELECT id FROM users_roles
           WHERE user_id = {ENCODED_ID_USER_ADMIN} AND role_id = {ENCODED_ID_ROLE_DEFAULT}"#
    )
    .build_query_as()
    .fetch_one(&db.serving)
    .await
    .unwrap();
    assert_eq!(row.id, ENCODED_ID_USER_ROLE_ADMIN_DEFAULT);

    let count: i64 = raw_query!(db, "SELECT count(*) FROM users_roles")
        .build_query_scalar()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[cfg(sqlite)]
#[tokio::test]
async fn test_tabsdata_db_default_permissions_sqlite() {
    let db = crate::test_utils::sqlite::pool().await;
    db.upgrade().await.unwrap();
    test_default_permissions_body(&db).await;
}

#[cfg(postgres)]
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
        (ENCODED_ID_ROLE_SYS_ADMIN, "manage_access", "system", ROOT, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS),
        (ENCODED_ID_ROLE_SYS_ADMIN, "create_projects", "system", ROOT, ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS),
        (ENCODED_ID_ROLE_SYS_ADMIN, "manage_roles", "system", ROOT, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES),
        (ENCODED_ID_ROLE_SYS_ADMIN, "manage_project", "project", ROOT, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT),
        (ENCODED_ID_ROLE_DEFAULT, "create_projects", "system", ROOT, ENCODED_ID_PERM_DEFAULT_CREATE_PROJECTS),
        (ENCODED_ID_ROLE_PROJECT_CREATOR, "create_projects", "system", ROOT, ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS),
    ];
    for (role_id, permission_type, entity_type, entity_id, expected) in cases {
        // Built at runtime (the case values are locals), so query directly via
        // sqlx rather than `raw_query!`, which only interpolates consts/paths.
        let sql = format!(
            "SELECT id FROM permissions WHERE role_id = '{role_id}' \
             AND permission_type = '{permission_type}' AND entity_type = '{entity_type}' \
             AND entity_id = '{entity_id}'"
        );
        assert_eq!(
            fetch_id(&db.serving, &sql).await,
            expected,
            "permission {permission_type} for {role_id}"
        );
    }

    let count: i64 = raw_query!(db, "SELECT count(*) FROM permissions")
        .build_query_scalar()
        .fetch_one(&db.serving)
        .await
        .unwrap();
    assert_eq!(count, 6);
}
