//
// Copyright 2025 Tabs Data Inc.
//

use crate::users::dao::UserWithNames;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_utoipa::api_server_schema;

/// API: Payload for user create.
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Deserialize, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct UserCreate {
    pub name: String,
    pub full_name: String,
    pub email: Option<String>,
    /// Password must be at least 8 characters long
    pub password: String,
    pub enabled: Option<bool>,
}

impl UserCreate {
    pub fn builder() -> UserCreateBuilder {
        UserCreateBuilder::default()
    }
}
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum PasswordUpdate {
    ForceChange {
        temporary_password: Option<String>,
    },
    Change {
        old_password: String,
        new_password: String,
    },
}

/// API: Payload for user update.
#[api_server_schema]
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Getters, Builder)]
#[builder(setter(into, strip_option), default)]
#[getset(get = "pub")]
pub struct UserUpdate {
    pub full_name: Option<String>,
    pub email: Option<String>,
    /// If set by an admin, for another user, the old password is not provided and
    /// the user must change their password after login.
    ///
    /// The password must be at least 8 characters long.
    pub password: Option<PasswordUpdate>,
    pub enabled: Option<bool>,
}

impl UserUpdate {
    pub fn builder() -> UserUpdateBuilder {
        UserUpdateBuilder::default()
    }
}
/// API: Payload for user get.
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct UserRead {
    id: String,
    name: String,
    full_name: String,
    email: Option<String>,
    created_on: i64,
    created_by_id: String,
    created_by: String,
    modified_on: i64,
    modified_by_id: String,
    modified_by: String,
    password_set_on: i64,
    password_must_change: bool,
    enabled: bool,
}

/// API: Payload for user list.
pub type UserList = UserRead;

impl From<&UserWithNames> for UserRead {
    fn from(db: &UserWithNames) -> Self {
        UserRead {
            id: db.id().clone(),
            name: db.name().clone(),
            full_name: db.full_name().clone(),
            email: db.email().clone(),
            created_on: db.created_on().timestamp_millis(),
            created_by_id: db.created_by_id().clone(),
            created_by: db.created_by().clone(),
            modified_on: db.modified_on().timestamp_millis(),
            modified_by_id: db.modified_by_id().clone(),
            modified_by: db.modified_by().clone(),
            password_set_on: db.password_set_on().clone().timestamp_millis(),
            password_must_change: *db.password_must_change(),
            enabled: *db.enabled(),
        }
    }
}

#[derive(Debug, Clone, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct AuthenticateRequest {
    name: String,
    password: String,
}

impl AuthenticateRequest {
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        AuthenticateRequest {
            name: name.into(),
            password: password.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::users::dao::UserWithNamesBuilder;
    use crate::users::dto::UserRead;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_user_read_from_user_with_names() {
        let user_with_names = UserWithNamesBuilder::default()
            .id(String::from("id"))
            .name(String::from("name"))
            .full_name(String::from("full_name"))
            .email(String::from("email"))
            .created_on(UniqueUtc::now_millis().await)
            .created_by_id(String::from("created_by_id"))
            .created_by(String::from("created_by"))
            .modified_on(UniqueUtc::now_millis().await)
            .modified_by_id(String::from("modified_by_id"))
            .modified_by(String::from("modified_by"))
            .password_set_on(UniqueUtc::now_millis().await)
            .password_must_change(true)
            .enabled(true)
            .build()
            .unwrap();
        let user_read = UserRead::from(&user_with_names);
        assert_eq!(user_read.id(), user_with_names.id());
        assert_eq!(user_read.name(), user_with_names.name());
        assert_eq!(user_read.full_name(), user_with_names.full_name());
        assert_eq!(user_read.email(), user_with_names.email());
        assert_eq!(
            user_read.created_on(),
            &user_with_names.created_on().timestamp_millis()
        );
        assert_eq!(user_read.created_by_id(), user_with_names.created_by_id());
        assert_eq!(user_read.created_by(), user_with_names.created_by());
        assert_eq!(
            user_read.modified_on(),
            &user_with_names.modified_on().timestamp_millis()
        );
        assert_eq!(user_read.modified_by_id(), user_with_names.modified_by_id());
        assert_eq!(user_read.modified_by(), user_with_names.modified_by());
        assert_eq!(
            user_read.password_set_on(),
            &user_with_names.password_set_on().timestamp_millis()
        );
        assert_eq!(
            user_read.password_must_change(),
            user_with_names.password_must_change()
        );
        assert_eq!(user_read.enabled(), user_with_names.enabled());
    }
}
