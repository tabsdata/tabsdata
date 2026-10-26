//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_queries::Queries;

/// The collection a request names: an id as given, deleted collections
/// included, or a name looked up among the live collections.
pub async fn resolve_collection_id<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    collection: &CollectionIdName,
) -> Result<CollectionId, TdError> {
    Ok(match collection {
        CollectionIdName::Id(id) => *id,
        CollectionIdName::Name(_) => {
            let named: CollectionDB = conn.scd2().fetch_one(collection).await?;
            named.base.collection_id
        }
    })
}
