//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::role::RoleDB;
    use crate::types::basic::{
        AtTime, EntityId, EntityName, Fixed, PermissionEntityType, PermissionId, PermissionType,
        RoleId, RoleName, UserId, UserName,
    };

    #[ta_objects::Dao]
    #[dao(sql_table = "permissions")]
    #[dxo(
        builder(try_from = PermissionCreate, skip_all),
        updater(try_from = RequestContext, skip_all),
        updater(try_from = RoleDB, skip_all)
    )]
    pub struct PermissionDB {
        #[dxo(extractor)]
        #[builder(default)]
        pub id: PermissionId,
        #[dxo(updater(try_from = RoleDB, field = "id"))]
        pub role_id: RoleId,
        #[dxo(builder(include))]
        pub permission_type: PermissionType,
        pub entity_type: PermissionEntityType,
        #[dxo(extractor)]
        pub entity_id: EntityId,
        #[dxo(updater(try_from = RequestContext, field = "user_id"))]
        pub granted_by_id: UserId,
        #[dxo(updater(try_from = RequestContext, field = "time"))]
        pub granted_on: AtTime,
        #[builder(default = "Fixed::from(false)")]
        pub fixed: Fixed,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "permissions__with_names")]
    #[inherits(PermissionDB)]
    pub struct PermissionDBWithNames {
        #[dxo(extractor)]
        pub id: PermissionId,
        #[dxo(extractor)]
        pub entity_id: Option<EntityId>,

        pub granted_by: UserName,
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
    #[inherits(PermissionDBWithNames)]
    pub struct Permission {
        #[dto(list(pagination_by = "+", filter))]
        pub id: PermissionId,
        #[dto(list(filter, order_by))]
        pub permission_type: PermissionType,

        #[dto(list(filter, filter_like, order_by))]
        pub role: RoleName,
        #[dto(list(filter, filter_like))]
        pub entity: Option<EntityName>,
    }
}
