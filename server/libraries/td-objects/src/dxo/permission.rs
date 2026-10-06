//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{
    AtTime, EntityId, EntityName, Fixed, PermissionEntityType, PermissionId, PermissionType,
    RoleId, RoleName, UserId, UserLabel,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(
    table = "permissions",
    ops(select, insert, delete),
    unique(RoleId, PermissionType, PermissionEntityType, EntityId)
)]
pub struct PermissionDB {
    #[dao(pk)]
    pub id: PermissionId,
    pub role_id: RoleId,
    pub permission_type: PermissionType,
    pub entity_type: PermissionEntityType,
    pub entity_id: EntityId,
    pub granted_by_id: UserId,
    pub granted_on: AtTime,
    pub fixed: Fixed,
}

// Flat, not nested on `PermissionDB`: the view relaxes `entity_id` to
// nullable (NULL = ALL), so the table row's shape doesn't embed.
#[ta_objects::Dao]
#[dao(
    view = "permissions__with_names",
    unique(RoleId, PermissionType, PermissionEntityType, EntityId)
)]
pub struct PermissionDBWithNames {
    #[dao(pk)]
    pub id: PermissionId,
    pub role_id: RoleId,
    pub permission_type: PermissionType,
    pub entity_type: PermissionEntityType,
    pub entity_id: Option<EntityId>,
    pub granted_by_id: UserId,
    pub granted_on: AtTime,
    pub fixed: Fixed,
    pub granted_by: UserLabel,
    pub role: RoleName,
    pub entity: Option<EntityName>,
}

#[ta_objects::Dto]
pub struct PermissionCreate {
    pub permission_type: PermissionType,
    pub entity_name: Option<EntityName>, // None means ALL
}

#[ta_objects::Dto]
#[dto(list(on = PermissionDBWithNames))]
pub struct Permission {
    #[dto(list(pagination_by = "+", filter))]
    pub id: PermissionId,
    pub role_id: RoleId,
    #[dto(list(filter, order_by))]
    pub permission_type: PermissionType,
    pub entity_type: PermissionEntityType,
    pub entity_id: Option<EntityId>,
    pub granted_by_id: UserId,
    pub granted_on: AtTime,
    pub fixed: Fixed,
    pub granted_by: UserLabel,

    #[dto(list(filter, filter_like, order_by))]
    pub role: RoleName,
    #[dto(list(filter, filter_like))]
    pub entity: Option<EntityName>,
}

impl FromDao for Permission {
    type Dao = PermissionDBWithNames;

    fn from_dao(dao: PermissionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.id,
            role_id: dao.role_id,
            permission_type: dao.permission_type,
            entity_type: dao.entity_type,
            entity_id: dao.entity_id,
            granted_by_id: dao.granted_by_id,
            granted_on: dao.granted_on,
            fixed: dao.fixed,
            granted_by: dao.granted_by,
            role: dao.role,
            entity: dao.entity,
        })
    }
}

#[ta_objects::UrlParam]
pub struct PermissionParam {
    pub permission: PermissionId,
}
