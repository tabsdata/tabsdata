//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::And;
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::logical::{TableDataVersion, TableDataVersionDBWithNames};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::params::TableDataVersionAtIdName;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionIdName, TableId, TableIdName, TriggeredOn,
};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableListDataVersionsService,
    request = ListRequest<TableDataVersionAtIdName>,
    response = ListResponse<TableDataVersion>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<TableDataVersionAtIdName>>::extract::<RequestContext>),
        from_fn(
            With::<ListRequest<TableDataVersionAtIdName>>::extract_name::<TableDataVersionAtIdName>
        ),
        // find collection ID
        from_fn(With::<TableDataVersionAtIdName>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // extract at time (triggered on)
        from_fn(With::<TableDataVersionAtIdName>::extract::<AtTime>),
        from_fn(With::<AtTime>::try_convert_to::<TriggeredOn, _>),
        // find table ID
        from_fn(With::<TableDataVersionAtIdName>::extract::<TableIdName>),
        from_fn(
            By::<And![CollectionIdName, TableIdName]>::fetch_one_current::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        // list
        from_fn(With::<ListRequest<TableDataVersionAtIdName>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<TableDataVersion>, _>),
        from_fn(
            By::<TableId>::historic_list_full::<
                NoFilter,
                { TableDataVersionDBWithNames::All },
                TableDataVersion,
            >
        ),
    )
}
