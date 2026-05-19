//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::resolve_table_location;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionIdName, TableId, TableIdName, TriggeredOn, UncommittedStatus,
};
use td_objects::{And, Extractor};
use td_queries::By;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod delete;
pub mod download;
pub mod download_state;
pub mod flow;
pub mod list_data_versions;
pub mod sample;
pub mod schema;
pub mod storage;

// TableAtIdName -> PhysicalTableDataVersionDBRead, SPath
// Only looks for existing tables at the given time of committed transactions
#[layer]
pub fn find_data_version_location_at<E>()
where
    for<'a> E: Extractor<CollectionIdName>
        + Extractor<TableIdName>
        + Extractor<AtTime>
        + Extractor<UncommittedStatus>
        + Send
        + Sync
        + 'a,
{
    layers!(
        // Extract parameters
        from_fn(With::<E>::extract::<CollectionIdName>),
        from_fn(With::<E>::extract::<TableIdName>),
        from_fn(With::<E>::extract::<AtTime>),
        from_fn(With::<E>::extract::<UncommittedStatus>),
        // Find Table ID, looking at the version at the time
        from_fn(
            By::<And![CollectionIdName, TableIdName]>::historic_fetch_one::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        // At the triggered on time
        from_fn(With::<AtTime>::try_convert_to::<TriggeredOn, _>),
        // Find the latest data version of the table ID, at that time
        // Resolve the location of the data version. This takes into account versions without
        // data changes (in which the previous version is resolved)
        from_fn(resolve_table_location),
    )
}
