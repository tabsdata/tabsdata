//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::storage::resolve_table_location;
use td_objects::dxo::table::defs::TableDBWithNames;
use td_objects::dxo::table_data_version::defs::TableDataVersionDBWithNames;
use td_objects::tower_service::from::{ExtractService, TryIntoService, With, combine};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::Extractor;
use td_objects::types::id::TableId;
use td_objects::types::id_name::{CollectionIdName, TableIdName};
use td_objects::types::timestamp::{AtTime, TriggeredOn};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod delete;
pub mod download;
pub mod sample;
pub mod schema;
pub mod storage;

// TableAtIdName -> TableDataVersionDBRead, SPath
// Only looks for existing tables at the given time of committed transactions
#[layer]
pub fn find_data_version_location_at<E>()
where
    E: Extractor<CollectionIdName>
        + Extractor<TableIdName>
        + Extractor<AtTime>
        + Send
        + Sync
        + 'static,
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
        // Only committed transactions, at the triggered on time
        from_fn(With::<AtTime>::convert_to::<TriggeredOn, _>),
        // Find the latest data version of the table ID, at that time
        from_fn(
            By::<TableId>::select_version_optional::<
                { TableDataVersionDBWithNames::Committed },
                TableDataVersionDBWithNames,
            >
        ),
        // Resolve the location of the data version. This takes into account versions without
        // data changes (in which the previous version is resolved)
        from_fn(resolve_table_location),
    )
}
