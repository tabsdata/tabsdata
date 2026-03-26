//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, BundleHash, BundleId, CollectionId, UserId};

    #[ta_objects::Dao]
    #[dao(sql_table = "bundles")]
    #[dxo(builder(try_from = RequestContext, skip_all))]
    pub struct BundleDB {
        #[dxo(setter)]
        pub id: BundleId,
        #[dxo(setter)]
        pub collection_id: CollectionId,
        #[dxo(setter)]
        pub hash: BundleHash,
        #[dxo(builder(include, field = "time"))]
        pub created_on: AtTime,
        #[dxo(builder(include, field = "user_id"))]
        pub created_by_id: UserId,
    }

    #[ta_objects::Dto]
    #[dxo(builder(try_from = BundleDB))]
    pub struct Bundle {
        pub id: BundleId,
    }
}
