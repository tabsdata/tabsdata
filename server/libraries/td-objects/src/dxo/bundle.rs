//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, BasePath, BundleHash, BundleId, BundleStatus, CollectionId, UserId,
    };

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
        // Storage prefix frozen at upload = collection.base_path. The bundle row is
        // durable, so it self-locates its .tgz regardless of later collection changes.
        #[dxo(setter)]
        #[builder(default)]
        pub base_path: BasePath,
        // Pending at upload; flipped to Registered when a function claims it.
        #[dxo(setter)]
        #[builder(default = BundleStatus::Pending)]
        pub status: BundleStatus,
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

    /// Update applied when a function register/update/patch claims an
    /// uploaded bundle: flips its status to `Registered` (the row is kept,
    /// not deleted, so the bundle artifact stays a durable record).
    /// Constructed by struct literal at the call site.
    #[ta_objects::Dao]
    #[dao(update, sql_table = "bundles")]
    pub struct BundleRegisteredDB {
        pub status: BundleStatus,
    }
}
