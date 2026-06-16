//
// Copyright 2026 Tabs Data Inc.
//

//! List function runs.
//!
//! Returns one row per `(execution_id, function_id)` pair — the
//! plan-time logical identity (LFR) merged with the currently-bound
//! PFR's per-attempt fields (started_on, ended_on, the PFR id itself).
//! `status` is the canonical LFR-level (sticky-Committed) flow-control
//! status. PFR-derived fields are NULL when no PFR is currently bound
//! (Pending/Voided).

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::logical::FunctionRun;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = FunctionRunListService,
    request = ListRequest<()>,
    response = ListResponse<FunctionRun>,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        // Authorize: the requester must be able to view the project in scope.
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<FunctionRun>, _>),
        from_fn(By::<()>::list::<NoFilter, FunctionRun>),
    )
}
