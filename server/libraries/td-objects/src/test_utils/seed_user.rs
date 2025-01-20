//
// Copyright 2025 Tabs Data Inc.
//

use crate::users::dao::UserBuilder;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;

pub async fn admin_user(conn: &DbPool) -> String {
    sqlx::query_scalar("SELECT id FROM users WHERE name = ?")
        .bind(td_security::ADMIN_USER)
        .fetch_one(conn)
        .await
        .unwrap()
}

pub async fn seed_user(db: &DbPool, creator_id: Option<String>, name: &str, enabled: bool) -> Id {
    let creator_id = if let Some(creator_id) = creator_id {
        creator_id
    } else {
        td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER)
            .await
            .0
    };

    let now = UniqueUtc::now_millis().await;

    let user = UserBuilder::default()
        .id(id::id())
        .name(name)
        .full_name(format!("FullName: {}", name))
        .email(format!("{}@foo.com", name))
        .created_on(now)
        .created_by_id(&creator_id)
        .modified_on(now)
        .modified_by_id(&creator_id)
        .password_hash(create_password_hash(
            &PasswordHashingConfig::default(),
            "password",
        ))
        .password_set_on(now)
        .password_must_change(false)
        .enabled(enabled)
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
              INSERT INTO users (
                    id,
                    name,
                    full_name,
                    email,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id,
                    password_hash,
                    password_set_on,
                    password_must_change,
                    enabled
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(user.id())
        .bind(user.name())
        .bind(user.full_name())
        .bind(user.email())
        .bind(user.created_on())
        .bind(user.created_by_id())
        .bind(user.modified_on())
        .bind(user.modified_by_id())
        .bind(user.password_hash())
        .bind(user.password_set_on())
        .bind(user.password_must_change())
        .bind(user.enabled())
        .execute(db)
        .await
        .unwrap();

    Id::try_from(user.id()).unwrap()
}

#[cfg(test)]
pub mod tests {
    use crate::crudl::select_by;
    use crate::test_utils::seed_user::seed_user;
    use crate::users::dao::User;
    use td_common::time::UniqueUtc;
    use td_security::password::verify_password;

    #[tokio::test]
    async fn test_seed_user() {
        let before = UniqueUtc::now_millis().await;
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "user", true).await;

        let user: User = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM users WHERE id = ?",
            &user_id.to_string(),
        )
        .await
        .unwrap();

        let creator_id = td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
            .await
            .0;

        assert_eq!(user.id(), &user_id.to_string());
        assert_eq!(user.name(), "user");
        assert_eq!(user.full_name(), "FullName: user");
        assert_eq!(user.email().as_ref().unwrap(), "user@foo.com");
        assert!(user.created_on() >= &before);
        assert_eq!(user.created_by_id(), &creator_id);
        assert!(user.modified_on() >= &before);
        assert_eq!(user.modified_by_id(), &creator_id);
        assert!(verify_password(user.password_hash(), "password"));
        assert!(user.password_set_on() >= &before);
        assert!(!user.password_must_change());
        assert!(user.enabled());
    }
}
