//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{Added, AtTime, Fixed, RoleId, RoleName, UserId, UserName, UserRoleId};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(
    table = "users_roles",
    ops(select, insert, delete),
    unique(UserId, RoleId)
)]
pub struct UserRoleDB {
    #[dao(pk)]
    pub id: UserRoleId,
    pub user_id: UserId,
    pub role_id: RoleId,
    pub added_on: AtTime,
    pub added_by_id: Added<UserId>,
    pub fixed: Fixed,
}

impl UserRoleDB {
    pub fn new(user_id: UserId, role_id: RoleId, ctx: &RequestContext, fixed: Fixed) -> Self {
        Self {
            id: UserRoleId::default(),
            user_id,
            role_id,
            added_on: ctx.time,
            added_by_id: ctx.user_id.into(),
            fixed,
        }
    }
}

#[ta_objects::Dao]
#[dao(view = "users_roles__with_names", unique(UserId, RoleId))]
pub struct UserRoleDBWithNames {
    #[dao(flatten)]
    pub base: UserRoleDB,
    pub user: UserName,
    pub role: RoleName,
    pub added_by: Added<UserName>,
}

#[ta_objects::Dlo]
pub struct FixedUserRole {
    pub role_id: RoleId,
    pub fixed: Fixed,
}

#[ta_objects::Dto]
pub struct UserRoleCreate {
    pub user: UserName,
}

#[ta_objects::Dto]
#[dto(list(on = UserRoleDBWithNames))]
pub struct UserRole {
    #[dto(list(pagination_by = "+"))]
    pub id: UserRoleId,
    pub user_id: UserId,
    pub role_id: RoleId,
    pub added_on: AtTime,
    pub added_by_id: Added<UserId>,
    pub fixed: Fixed,

    #[dto(list(filter, filter_like, order_by))]
    pub user: UserName,
    #[dto(list(filter, filter_like, order_by))]
    pub role: RoleName,
    #[dto(list(filter, filter_like, order_by))]
    pub added_by: Added<UserName>,
}

impl FromDao for UserRole {
    type Dao = UserRoleDBWithNames;

    fn from_dao(dao: UserRoleDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            user_id: dao.base.user_id,
            role_id: dao.base.role_id,
            added_on: dao.base.added_on,
            added_by_id: dao.base.added_by_id,
            fixed: dao.base.fixed,
            user: dao.user,
            role: dao.role,
            added_by: dao.added_by,
        })
    }
}
