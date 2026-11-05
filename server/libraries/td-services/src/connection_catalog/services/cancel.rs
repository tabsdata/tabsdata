//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::ConnectionCatalog;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, MaintainCatalog, ManageProject, Project};
use td_database::access::{DbPool, InProject, retrying};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection_catalog::{
    ConnectionCatalogDBWithNames, ConnectionProbeDB, ProbeEndUpdateDB, ProbeParam, ProbeRead,
};
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::types::basic::{AtTime, Ended, ProbeStatus};
use td_queries::Queries;

/// The submitter cancels its own probe; anyone else needs `ManageProject`.
/// A job already started runs on, and its outcome is dropped.
#[service_factory]
async fn cancel_connection_probe(
    authz: Ctx<AuthzContext>,
    catalog: Ctx<ConnectionCatalog>,
    db: DbPool<InProject>,
    request: WithinProject<UpdateRequest<(), ProbeParam>>,
) -> Result<ProbeRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, ()) = request.into_parts();
    let (probe, names) = retrying(|attempt| async {
        let mut trx = db.lock_project().begin(attempt).await?;
        let grant = authz
            .require::<Project, (DevelopProject, MaintainCatalog)>(&mut trx, &ctx, &project)
            .await?;
        let probe: ConnectionProbeDB = trx.get(grant).for_update().fetch_one(&param.probe).await?;
        let grant = if probe.base.requested_by_id == ctx.user_id {
            grant
        } else {
            authz
                .require::<Project, ManageProject>(&mut trx, &ctx, &project)
                .await?
        };
        let conn = trx.get(grant);
        if probe.base.status.is_terminal() {
            return Err(api_error!(
                ApiError::Any(409),
                "the probe already ended as {}",
                probe.base.status
            ));
        }
        conn.update(
            &ProbeEndUpdateDB {
                status: ProbeStatus::Cancelled,
                error: None,
                result: None,
                result_expires: None,
                ended_on: Ended::from(AtTime::now()),
            },
            ConnectionProbeDB::active_of(param.probe),
        )
        .await?;
        let probe: ConnectionProbeDB = conn.fetch_one(&param.probe).await?;
        let names: ConnectionCatalogDBWithNames = conn.fetch_one(&probe.base.connection_id).await?;
        trx.commit().await?;
        Ok((probe, names))
    })
    .await?;
    catalog.ended.notify_waiters();
    Ok(ProbeRead::of(
        probe,
        names.collection,
        &ctx.user_id,
        &AtTime::now(),
    ))
}
