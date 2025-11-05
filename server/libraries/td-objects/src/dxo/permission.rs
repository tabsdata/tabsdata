//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::role::defs::RoleDB;
    use crate::types::bool::Fixed;
    use crate::types::id::{EntityId, PermissionId, RoleId, UserId};
    use crate::types::string::{EntityName, RoleName, UserName};
    use crate::types::timestamp::AtTime;
    use crate::types::typed_enum::{PermissionEntityType, PermissionType};

    #[td_type::Dao]
    #[dao(sql_table = "permissions")]
    #[td_type(
        builder(try_from = PermissionCreate, skip_all),
        updater(try_from = RequestContext, skip_all),
        updater(try_from = RoleDB, skip_all)
    )]
    pub struct PermissionDB {
        #[td_type(extractor)]
        #[builder(default)]
        pub id: PermissionId,
        #[td_type(updater(try_from = RoleDB, field = "id"))]
        pub role_id: RoleId,
        #[td_type(builder(include))]
        pub permission_type: PermissionType,
        pub entity_type: PermissionEntityType,
        #[td_type(extractor)]
        pub entity_id: EntityId,
        #[td_type(updater(try_from = RequestContext, field = "user_id"))]
        pub granted_by_id: UserId,
        #[td_type(updater(try_from = RequestContext, field = "time"))]
        pub granted_on: AtTime,
        #[builder(default = "Fixed::from(false)")]
        pub fixed: Fixed,
    }

    #[td_type::Dao]
    #[dao(sql_table = "permissions__with_names")]
    #[inherits(PermissionDB)]
    pub struct PermissionDBWithNames {
        #[td_type(extractor)]
        pub id: PermissionId,
        #[td_type(extractor)]
        pub entity_id: Option<EntityId>,

        pub granted_by: UserName,
        pub role: RoleName,
        pub entity: Option<EntityName>,
    }

    #[td_type::Dto]
    pub struct PermissionCreate {
        pub permission_type: PermissionType,
        pub entity_name: Option<EntityName>, // None means ALL
    }

    #[td_type::Dto]
    #[dto(list(on = PermissionDBWithNames))]
    #[td_type(builder(try_from = PermissionDBWithNames))]
    #[inherits(PermissionDBWithNames)]
    pub struct Permission {
        #[dto(list(pagination_by = "+", filter))]
        pub id: PermissionId,
        #[dto(list(filter, order_by))]
        pub permission_type: PermissionType,

        #[dto(list(filter, filter_like, order_by))]
        pub role: RoleName,
        #[dto(list(filter, filter_like))] // TODO should we allow order by on nullable??
        pub entity: Option<EntityName>,
    }
}
