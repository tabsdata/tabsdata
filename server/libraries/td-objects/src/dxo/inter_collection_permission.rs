//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, InterCollectionPermissionId, ToCollectionId,
        ToCollectionName, UserId, UserName,
    };

    #[ta_objects::Dao]
    #[dao(sql_table = "inter_collection_permissions")]
    #[dxo(updater(try_from = RequestContext, skip_all))]
    pub struct InterCollectionPermissionDB {
        #[dxo(extractor)]
        #[builder(default)]
        pub id: InterCollectionPermissionId,
        #[dxo(setter)]
        pub from_collection_id: CollectionId, // the collection that grants access
        #[dxo(setter)]
        pub to_collection_id: ToCollectionId, // the collection that is granted read access
        #[dxo(updater(try_from = RequestContext, field = "user_id"))]
        pub granted_by_id: UserId,
        #[dxo(updater(try_from = RequestContext, field = "time"))]
        pub granted_on: AtTime,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "inter_collection_permissions__with_names")]
    #[inherits(InterCollectionPermissionDB)]
    pub struct InterCollectionPermissionDBWithNames {
        #[dxo(extractor)]
        pub id: InterCollectionPermissionId,
        #[dxo(extractor)]
        pub from_collection_id: CollectionId,

        pub from_collection: CollectionName,
        pub to_collection: CollectionName,
        pub granted_by: UserName,
    }

    #[ta_objects::Dto]
    pub struct InterCollectionPermissionCreate {
        #[dxo(extractor)]
        pub to_collection: ToCollectionName,
    }

    #[ta_objects::Dto]
    #[dto(list(on = InterCollectionPermissionDBWithNames))]
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
