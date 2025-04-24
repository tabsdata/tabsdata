//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, EntityId, EntityName, FixedRole, PermissionEntityType, PermissionId, PermissionType,
    RoleId, RoleName, UserId, UserName,
};
use crate::types::role::RoleDB;

#[td_type::Dto]
pub struct PermissionCreate {
    permission_type: PermissionType,
    entity_name: Option<EntityName>, // None means ALL
}

#[td_type::Dao(sql_table = "permissions")]
#[td_type(builder(try_from = PermissionCreate, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
#[td_type(updater(try_from = RoleDB, skip_all))]
pub struct PermissionDB {
    #[td_type(extractor)]
    #[builder(default)]
    id: PermissionId,
    #[td_type(updater(try_from = RoleDB, field = "id"))]
    role_id: RoleId,
    #[td_type(builder(include))]
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    #[td_type(extractor)]
    entity_id: Option<EntityId>,
    #[td_type(updater(try_from = RequestContext, field = "user_id"))]
    granted_by_id: UserId,
    #[td_type(updater(try_from = RequestContext, field = "time"))]
    granted_on: AtTime,
    #[builder(default)]
    fixed: FixedRole,
}

#[td_type::Dao(sql_table = "permissions__with_names")]
pub struct PermissionDBWithNames {
    id: PermissionId,
    role_id: RoleId,
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: FixedRole,

    granted_by: UserName,
    role: RoleName,
    entity: Option<EntityName>,
}

#[td_type::Dto]
#[td_type(builder(try_from = PermissionDBWithNames))]
pub struct Permission {
    id: PermissionId,
    role_id: RoleId,
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: FixedRole,

    granted_by: UserName,
    role: RoleName,
    entity: Option<EntityName>,
}
