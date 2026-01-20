//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::resolve_table_location;
use td_objects::dxo::Extractor;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::tower_service::from::{ExtractService, TryIntoService, With, combine};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{AtTime, CollectionIdName, TableId, TableIdName, TriggeredOn};
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

// TableAtIdName -> TableDataVersionDBRead, SPath
// Only looks for existing tables at the given time of committed transactions
#[layer]
pub fn find_data_version_location_at<E>()
where
    for<'a> E:
        Extractor<CollectionIdName> + Extractor<TableIdName> + Extractor<AtTime> + Send + Sync + 'a,
{
    layers!(
        // Extract parameters
        from_fn(With::<E>::extract::<CollectionIdName>),
        from_fn(With::<E>::extract::<TableIdName>),
        from_fn(With::<E>::extract::<AtTime>),
        // Find Table ID, looking at the version at the time
        from_fn(combine::<CollectionIdName, TableIdName>),
        from_fn(
            By::<(CollectionIdName, TableIdName)>::select_version::<
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
