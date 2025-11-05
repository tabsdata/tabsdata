//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::types::id::{BundleId, CollectionId, UserId};
    use crate::types::string::BundleHash;
    use crate::types::timestamp::AtTime;

    #[td_type::Dao]
    #[dao(sql_table = "bundles")]
    #[td_type(builder(try_from = RequestContext, skip_all))]
    pub struct BundleDB {
        #[td_type(setter)]
        pub id: BundleId,
        #[td_type(setter)]
        pub collection_id: CollectionId,
        #[td_type(setter)]
        pub hash: BundleHash,
        #[td_type(builder(include, field = "time"))]
        pub created_on: AtTime,
        #[td_type(builder(include, field = "user_id"))]
        pub created_by_id: UserId,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = BundleDB))]
    pub struct Bundle {
        pub id: BundleId,
    }
}
