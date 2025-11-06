//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, InterCollectionPermissionId, ToCollectionId,
        ToCollectionName, UserId, UserName,
    };

    #[td_type::Dao]
    #[dao(sql_table = "inter_collection_permissions")]
    #[td_type(updater(try_from = RequestContext, skip_all))]
    pub struct InterCollectionPermissionDB {
        #[td_type(extractor)]
        #[builder(default)]
        pub id: InterCollectionPermissionId,
        #[td_type(setter)]
        pub from_collection_id: CollectionId, // the collection that grants access
        #[td_type(setter)]
        pub to_collection_id: ToCollectionId, // the collection that is granted read access
        #[td_type(updater(try_from = RequestContext, field = "user_id"))]
        pub granted_by_id: UserId,
        #[td_type(updater(try_from = RequestContext, field = "time"))]
        pub granted_on: AtTime,
    }

    #[td_type::Dao]
    #[dao(sql_table = "inter_collection_permissions__with_names")]
    #[inherits(InterCollectionPermissionDB)]
    pub struct InterCollectionPermissionDBWithNames {
        #[td_type(extractor)]
        pub id: InterCollectionPermissionId,
        #[td_type(extractor)]
        pub from_collection_id: CollectionId,

        pub from_collection: CollectionName,
        pub to_collection: CollectionName,
        pub granted_by: UserName,
    }

    #[td_type::Dto]
    pub struct InterCollectionPermissionCreate {
        #[td_type(extractor)]
        pub to_collection: ToCollectionName,
    }

    #[td_type::Dto]
    #[dto(list(on = InterCollectionPermissionDBWithNames))]
    #[td_type(builder(try_from = InterCollectionPermissionDBWithNames))]
    pub struct InterCollectionPermission {
        #[dto(list(pagination_by = "+", filter))]
        pub id: InterCollectionPermissionId,
        pub to_collection_id: ToCollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub to_collection: CollectionName,
        pub granted_by_id: UserId,
        pub granted_by: UserName,
        pub granted_on: AtTime,
    }
}
