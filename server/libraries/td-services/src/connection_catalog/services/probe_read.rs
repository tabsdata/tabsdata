//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::{ConnectionCatalog, MAX_WAIT_SECS};
use std::time::Duration;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, MaintainCatalog, Project};
use td_database::access::{DbPool, InProject};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection_catalog::{
    ConnectionCatalogDBWithNames, ConnectionProbeDB, ProbeParam, ProbeRead, ProbeWaitParams,
};
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::types::basic::AtTime;
use td_queries::Queries;
use tokio::time::Instant;

/// Re-read cadence while waiting, for outcomes landing on another replica.
const POLL: Duration = Duration::from_secs(1);

/// `?wait=N` (at most 55 s, under the request timeout) holds the request until
/// the probe ends or the wait runs out. A sample's rows are shown to its
/// requester alone; `maintain_catalog` reads only the probes it submitted.
#[service_factory]
async fn read_connection_probe(
    authz: Ctx<AuthzContext>,
    catalog: Ctx<ConnectionCatalog>,
    db: DbPool<InProject>,
    request: WithinProject<ReadRequest<(ProbeParam, ProbeWaitParams)>>,
) -> Result<ProbeRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (param, wait)) = request.into_parts();
    let wait = *wait.wait.unwrap_or_default() as u64;
    let deadline = Instant::now() + Duration::from_secs(wait.min(MAX_WAIT_SECS));
    loop {
        let ended = catalog.ended.notified();
        tokio::pin!(ended);
        ended.as_mut().enable();
        let read = {
            let mut conn = db.read().await?;
            let grant = authz
                .require::<Project, (DevelopProject, MaintainCatalog)>(&mut conn, &ctx, &project)
                .await?;
            let develop = authz
                .satisfies::<Project, DevelopProject>(&mut conn, &ctx, &project)
                .await?;
            let conn = conn.get(grant);
            let probe: ConnectionProbeDB = conn.fetch_one(&param.probe).await?;
            if !develop && probe.base.requested_by_id != ctx.user_id {
                return Err(api_error!(ApiError::NotFound, "connection probe not found"));
            }
            let names: ConnectionCatalogDBWithNames =
                conn.fetch_one(&probe.base.connection_id).await?;
            ProbeRead::of(probe, names.collection, &ctx.user_id, &AtTime::now())
        };
        let now = Instant::now();
        if read.status.is_terminal() || now >= deadline {
            return Ok(read);
        }
        let _ = tokio::time::timeout(POLL.min(deadline - now), ended).await;
    }
}
