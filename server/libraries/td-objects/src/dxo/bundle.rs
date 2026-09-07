//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{
    AtTime, BasePath, BundleHash, BundleId, BundleStatus, CollectionId, UserId,
};

#[ta_objects::Dao]
#[dao(sql_table = "bundles")]
pub struct BundleDB {
    pub id: BundleId,
    pub collection_id: CollectionId,
    pub hash: BundleHash,
    // Storage prefix frozen at upload = collection.base_path. The bundle row is
    // durable, so it self-locates its .tar.zst regardless of later collection changes.
    pub base_path: BasePath,
    // Pending at upload; flipped to Registered when a function claims it.
    pub status: BundleStatus,
    pub created_on: AtTime,
    pub created_by_id: UserId,
}

#[ta_objects::Dto]
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
impl BundleRegisteredDB {
    /// The bundle-claim update: a function register/update/patch flips the
    /// uploaded bundle's status to `Registered` (kept, not deleted).
    pub fn registered() -> Self {
        Self {
            status: BundleStatus::Registered,
        }
    }
}

impl From<&BundleDB> for Bundle {
    fn from(src: &BundleDB) -> Self {
        Bundle { id: src.id }
    }
}
