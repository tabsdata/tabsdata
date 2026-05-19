//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersion;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::UncommittedStatus;
use td_queries::ptdv_is_canonical_committed;
use td_tower::extractors::Ref;

pub async fn build_query_params(
    uncommitted_status: Ref<UncommittedStatus>,
    params: Ref<ListParams>,
) -> Result<ListQueryParams<PhysicalTableDataVersion>, TdError> {
    let params = ListQueryParams::<PhysicalTableDataVersion>::try_from(params.deref())?;

    let params = if **uncommitted_status {
        params
    } else {
        // td-queries owns the predicate; the layer just plumbs it.
        // The canonical "committed" projection walks LTDV → latest
        // Bound LTDV-pointer → PTDV, on a separate entity from the
        // PFR-side per-attempt audit.
        let filter = params.filter.add(ptdv_is_canonical_committed());
        ListQueryParams { filter, ..params }
    };

    Ok(params)
}
