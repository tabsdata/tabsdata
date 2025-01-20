//
// Copyright 2025 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use getset::Getters;
use sqlx::FromRow;
use td_database::sql::DbData;

/// Users table
#[derive(Debug, Clone, PartialEq, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct User {
    id: String,   //PK
    name: String, //Unique
    full_name: String,
    email: Option<String>, //Unique
    created_on: DateTime<Utc>,
    created_by_id: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,
    password_hash: String,
    password_set_on: DateTime<Utc>,
    password_must_change: bool,
    enabled: bool,
}

impl User {
    /// Returns a new [`UserBuilder`] with the same values as the current [`User`].
    pub fn builder(&self) -> UserBuilder {
        UserBuilder::default()
            .id(self.id())
            .name(self.name())
            .full_name(self.full_name())
            .email(self.email().clone())
            .created_on(*self.created_on())
            .created_by_id(self.created_by_id())
            .modified_on(*self.modified_on())
            .modified_by_id(self.modified_by_id())
            .password_hash(self.password_hash())
            .password_set_on(*self.password_set_on())
            .password_must_change(*self.password_must_change())
            .enabled(*self.enabled())
            .clone()
    }
}

impl DbData for User {}

/// Users table
#[derive(Debug, Clone, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct UserWithNames {
    id: String,   //PK
    name: String, //Unique
    full_name: String,
    email: Option<String>, //Unique
    created_on: DateTime<Utc>,
    created_by_id: String,
    created_by: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,
    modified_by: String,
    password_set_on: DateTime<Utc>,
    password_must_change: bool,
    enabled: bool,
}

impl DbData for UserWithNames {}

impl UserWithNames {
    pub fn builder_from_user(user: &User) -> UserWithNamesBuilder {
        UserWithNamesBuilder::default()
            .id(user.id())
            .name(user.name())
            .full_name(user.full_name())
            .email(user.email().clone())
            .created_on(*user.created_on())
            .created_by_id(user.created_by_id())
            .created_by(format!("[{}]", user.created_by_id()))
            .modified_on(*user.modified_on())
            .modified_by_id(user.modified_by_id())
            .modified_by(format!("[{}]", user.modified_by_id()))
            .password_set_on(*user.password_set_on())
            .password_must_change(*user.password_must_change())
            .enabled(*user.enabled())
            .clone()
    }

    /// Returns a new [`UserWithNameBuilder`] with the same values as the current [`User`].
    pub fn builder(&self) -> UserWithNamesBuilder {
        UserWithNamesBuilder::default()
            .id(self.id())
            .name(self.name())
            .full_name(self.full_name())
            .email(self.email().clone())
            .created_on(*self.created_on())
            .created_by_id(self.created_by_id())
            .created_by(self.created_by())
            .modified_on(*self.modified_on())
            .modified_by_id(self.modified_by_id())
            .modified_by(self.modified_by())
            .password_set_on(*self.password_set_on())
            .password_must_change(*self.password_must_change())
            .enabled(*self.enabled())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::UserBuilder;
    use chrono::TimeDelta;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_user_builder() {
        let created_on = UniqueUtc::now_millis()
            .await
            .checked_sub_signed(TimeDelta::minutes(1))
            .unwrap();
        let modified_on = UniqueUtc::now_millis()
            .await
            .checked_sub_signed(TimeDelta::minutes(2))
            .unwrap();
        let password_set_on = UniqueUtc::now_millis()
            .await
            .checked_sub_signed(TimeDelta::minutes(3))
            .unwrap();
        let user = UserBuilder::default()
            .id(String::from("id"))
            .name(String::from("name"))
            .full_name(String::from("full_name"))
            .email(String::from("email"))
            .created_on(created_on)
            .created_by_id(String::from("created_by"))
            .modified_on(modified_on)
            .modified_by_id(String::from("modified_by"))
            .password_hash(String::from("password_hash"))
            .password_set_on(password_set_on)
            .password_must_change(true)
            .enabled(true)
            .build()
            .unwrap();
        let user_rebuilt = user.builder().build().unwrap();
        assert_eq!(user, user_rebuilt);
    }
}
