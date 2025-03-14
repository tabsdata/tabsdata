//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::rest_urls::UserParam;
use crate::types::basic::{
    AtTime, Description, Fixed, RoleId, RoleName, UserId, UserName, UserRoleId,
};

#[td_type::Dao(sql_table = "roles")]
#[td_type(builder(try_from = RoleCreate, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct RoleDB {
    #[td_type(extractor)]
    #[builder(default)]
    id: RoleId,
    #[td_type(builder(include))]
    name: RoleName,
    #[td_type(builder(include))]
    description: Description,
    #[td_type(updater(include, field = "time"))]
    created_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    created_by_id: UserId,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
    #[builder(default)]
    fixed: Fixed,
}

#[td_type::Dto]
pub struct RoleCreate {
    name: RoleName,
    description: Description,
}

#[td_type::Dao]
#[td_type(builder(try_from = RoleUpdate, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct RoleDBUpdate {
    #[td_type(builder(include))]
    name: RoleName,
    #[td_type(builder(include))]
    description: Description,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
}

pub type RoleUpdate = RoleCreate;

#[td_type::Dao(sql_table = "roles__with_names")]
pub struct RoleDBWithNames {
    #[td_type(extractor)]
    id: RoleId,
    name: RoleName,
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    modified_on: AtTime,
    modified_by_id: UserId,
    fixed: Fixed,

    created_by: UserName,
    modified_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = RoleDBWithNames))]
pub struct Role {
    id: RoleId,
    name: RoleName,
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    modified_on: AtTime,
    modified_by_id: UserId,
    fixed: Fixed,

    created_by: UserName,
    modified_by: UserName,
}

#[td_type::Dao(sql_table = "users_roles")]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct UserRoleDB {
    #[td_type(extractor)]
    #[builder(default)]
    id: UserRoleId,
    #[td_type(setter)]
    user_id: UserId,
    #[td_type(setter)]
    role_id: RoleId,
    #[td_type(updater(include, field = "time"))]
    added_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    added_by_id: UserId,
    #[builder(default)]
    fixed: Fixed,
}

#[td_type::Dto]
pub struct UserRoleCreate {
    #[td_type(extractor)]
    #[schema(value_type = String)] // openapi flattening
    user: UserParam,
}

#[td_type::Dao(sql_table = "users_roles__with_names")]
pub struct UserRoleDBWithNames {
    id: UserRoleId,
    user_id: UserId,
    role_id: RoleId,
    added_on: AtTime,
    added_by_id: UserId,
    fixed: Fixed,

    user: UserName,
    role: RoleName,
    added_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = UserRoleDBWithNames))]
pub struct UserRole {
    id: UserRoleId,
    user_id: UserId,
    role_id: RoleId,
    added_on: AtTime,
    added_by_id: UserId,
    fixed: Fixed,

    user: UserName,
    role: RoleName,
    added_by: UserName,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_role_create_to_role_db() {
        let role_create = RoleCreate {
            name: RoleName::try_from("Admin".to_string()).unwrap(),
            description: Description::try_from("Administrator role".to_string()).unwrap(),
        };
        let role_db = RoleDBBuilder::try_from(&role_create).unwrap();
        assert_eq!(
            role_db.name,
            Some(RoleName::try_from("Admin".to_string()).unwrap())
        );
        assert_eq!(
            role_db.description,
            Some(Description::try_from("Administrator role".to_string()).unwrap())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_role_db_with_names_to_role() {
        let role_db_with_names = RoleDBWithNames {
            id: RoleId::default(),
            name: RoleName::try_from("Admin".to_string()).unwrap(),
            description: Description::try_from("Administrator role".to_string()).unwrap(),
            created_on: AtTime::default(),
            created_by_id: UserId::default(),
            modified_on: AtTime::default(),
            modified_by_id: UserId::default(),
            fixed: Fixed::try_from(false).unwrap(),
            created_by: UserName::try_from("creator".to_string()).unwrap(),
            modified_by: UserName::try_from("modifier".to_string()).unwrap(),
        };
        let role_read = RoleBuilder::try_from(&role_db_with_names).unwrap();
        assert_eq!(role_read.id, Some(role_db_with_names.id));
        assert_eq!(role_read.name, Some(role_db_with_names.name));
        assert_eq!(role_read.description, Some(role_db_with_names.description));
        assert_eq!(role_read.created_on, Some(role_db_with_names.created_on));
        assert_eq!(
            role_read.created_by_id,
            Some(role_db_with_names.created_by_id)
        );
        assert_eq!(role_read.modified_on, Some(role_db_with_names.modified_on));
        assert_eq!(
            role_read.modified_by_id,
            Some(role_db_with_names.modified_by_id)
        );
        assert_eq!(role_read.fixed, Some(role_db_with_names.fixed));
        assert_eq!(role_read.created_by, Some(role_db_with_names.created_by));
        assert_eq!(role_read.modified_by, Some(role_db_with_names.modified_by));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_users_roles_db_with_names_to_users_roles_read() {
        let users_roles_db_with_names = UserRoleDBWithNames {
            id: UserRoleId::default(),
            user_id: UserId::default(),
            role_id: RoleId::default(),
            added_on: AtTime::default(),
            added_by_id: UserId::default(),
            fixed: Fixed::try_from(false).unwrap(),
            user: UserName::try_from("user".to_string()).unwrap(),
            role: RoleName::try_from("Admin".to_string()).unwrap(),
            added_by: UserName::try_from("adder".to_string()).unwrap(),
        };
        let users_roles_read = UserRoleBuilder::try_from(&users_roles_db_with_names).unwrap();
        assert_eq!(users_roles_read.id, Some(users_roles_db_with_names.id));
        assert_eq!(
            users_roles_read.user_id,
            Some(users_roles_db_with_names.user_id)
        );
        assert_eq!(
            users_roles_read.role_id,
            Some(users_roles_db_with_names.role_id)
        );
        assert_eq!(
            users_roles_read.added_on,
            Some(users_roles_db_with_names.added_on)
        );
        assert_eq!(
            users_roles_read.added_by_id,
            Some(users_roles_db_with_names.added_by_id)
        );
        assert_eq!(
            users_roles_read.fixed,
            Some(users_roles_db_with_names.fixed)
        );
        assert_eq!(users_roles_read.user, Some(users_roles_db_with_names.user));
        assert_eq!(users_roles_read.role, Some(users_roles_db_with_names.role));
        assert_eq!(
            users_roles_read.added_by,
            Some(users_roles_db_with_names.added_by)
        );
    }
}
