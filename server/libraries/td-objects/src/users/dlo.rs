//
// Copyright 2025 Tabs Data Inc.
//

use crate as td_objects;
use crate::crudl::{Name, ReadRequest};
use crate::dlo::UserName;
use crate::tower_service::extractor::{RequestNameProvider, UserIdProvider, UserNameProvider};
use crate::users::dao::{User, UserWithNames};
use crate::users::dto::PasswordUpdate::{Change, ForceChange};
use crate::users::dto::{AuthenticateRequest, UserCreate, UserUpdate};
use td_type::service_type;

#[service_type]
#[derive(Debug, Clone)]
pub struct Password(Option<String>);

pub trait PasswordProvider {
    fn password(&self) -> Option<String>;
}

impl PasswordProvider for UserCreate {
    fn password(&self) -> Option<String> {
        Some(self.password().trim().to_string())
    }
}

impl PasswordProvider for UserUpdate {
    fn password(&self) -> Option<String> {
        self.password()
            .as_ref()
            .and_then(|password_update| match password_update {
                ForceChange { temporary_password } => temporary_password
                    .as_ref()
                    .map(|password| password.trim().to_string()),
                Change {
                    old_password: _,
                    new_password,
                } => Some(new_password.trim().to_string()),
            })
    }
}

impl RequestNameProvider<UserName> for ReadRequest<String> {
    fn name(&self) -> Name<UserName> {
        Name::new(UserName::new(self.name().value()))
    }
}

impl UserIdProvider for UserWithNames {
    fn user_id(&self) -> String {
        self.id().to_string()
    }
}

impl UserIdProvider for User {
    fn user_id(&self) -> String {
        self.id().to_string()
    }
}

impl UserNameProvider for AuthenticateRequest {
    fn user_name(&self) -> &str {
        self.name()
    }
}

#[service_type]
#[derive(Debug, Clone)]
pub struct UserPassword(String);

#[service_type]
#[derive(Debug, Clone)]
pub struct UserPasswordHash(String);
