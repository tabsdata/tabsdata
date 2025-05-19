//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, EntityId, EntityName, Fixed, InterCollectionPermissionId,
    PermissionEntityType, PermissionId, PermissionType, RoleId, RoleName, ToCollectionId,
    ToCollectionName, UserId, UserName,
};
use crate::types::role::RoleDB;

#[td_type::Dto]
pub struct PermissionCreate {
    permission_type: PermissionType,
    entity_name: Option<EntityName>, // None means ALL
}

#[td_type::Dao]
#[dao(sql_table = "permissions")]
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
    #[builder(default = "Fixed::from(false)")]
    fixed: Fixed,
}

#[td_type::Dao]
#[dao(sql_table = "permissions__with_names")]
pub struct PermissionDBWithNames {
    #[td_type(extractor)]
    id: PermissionId,
    role_id: RoleId,
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    #[td_type(extractor)]
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: Fixed,

    granted_by: UserName,
    role: RoleName,
    entity: Option<EntityName>,
}

#[td_type::Dto]
#[dto(list(on = PermissionDBWithNames))]
#[td_type(builder(try_from = PermissionDBWithNames))]
pub struct Permission {
    id: PermissionId,
    role_id: RoleId,
    #[dto(list(filter, order_by))]
    permission_type: PermissionType,
    entity_type: PermissionEntityType,
    entity_id: Option<EntityId>,
    granted_by_id: UserId,
    granted_on: AtTime,
    fixed: Fixed,

    granted_by: UserName,
    #[dto(list(filter, filter_like, order_by))]
    role: RoleName,
    #[dto(list(filter, filter_like, order_by))]
    entity: Option<EntityName>,
}

#[td_type::Dto]
pub struct InterCollectionPermissionCreate {
    #[td_type(extractor)]
    to_collection: ToCollectionName,
}

#[td_type::Dao]
#[dao(sql_table = "inter_collection_permissions")]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct InterCollectionPermissionDB {
    #[td_type(extractor)]
    #[builder(default)]
    id: InterCollectionPermissionId,
    #[td_type(setter)]
    from_collection_id: CollectionId,
    #[td_type(setter)]
    to_collection_id: ToCollectionId,
    #[td_type(updater(try_from = RequestContext, field = "user_id"))]
    granted_by_id: UserId,
    #[td_type(updater(try_from = RequestContext, field = "time"))]
    granted_on: AtTime,
}

#[td_type::Dao]
#[dao(sql_table = "inter_collection_permissions__with_names")]
pub struct InterCollectionPermissionDBWithNames {
    #[td_type(extractor)]
    id: InterCollectionPermissionId,
    #[td_type(extractor)]
    from_collection_id: CollectionId,
    from_collection: CollectionName,
    to_collection_id: ToCollectionId,
    to_collection: CollectionName,
    granted_by_id: UserId,
    granted_by: UserName,
    granted_on: AtTime,
}

#[td_type::Dto]
#[dto(list(on = InterCollectionPermissionDBWithNames))]
#[td_type(builder(try_from = InterCollectionPermissionDBWithNames))]
pub struct InterCollectionPermission {
    id: InterCollectionPermissionId,
    to_collection_id: ToCollectionId,
    #[dto(list(filter, filter_like, order_by))]
    to_collection: CollectionName,
    granted_by_id: UserId,
    granted_by: UserName,
    granted_on: AtTime,
}
