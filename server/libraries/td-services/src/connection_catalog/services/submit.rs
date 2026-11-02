//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::{
    ACTIVE_PROBE_INDEX, ConnectionCatalog, MAX_QUEUED_PER_PROJECT, MAX_SAMPLE_ENTITIES, dedupe_key,
    entity_not_in_catalog, rate_limited, resolve_catalog,
};
use std::sync::atomic::{AtomicBool, Ordering};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, MaintainCatalog, Project};
use td_database::access::{DbPool, InProject, retrying, unique_violation_on};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection::ConnectionDB;
use td_objects::dxo::connection_catalog::{
    ConnectionEntitySummaryDB, ConnectionProbeDB, ConnectionProbeInsertDB, ProbeRead, ProbeSubmit,
};
use td_objects::dxo::crudl::{CreateRequest, WithinProject};
use td_objects::types::basic::{
    AtTime, EntityFqn, ProbeId, ProbeKind, ProbeParams, ProbeRequestedOn, ProbeStatus, SampleRows,
};
use td_objects::types::catalog::CatalogConfig;
use td_objects::types::json::Json;
use td_queries::Queries;
use tracing::info;

/// Every kind is `DevelopProject`: a sample shows what a developer could
/// extract by running a publisher, is capped and audit-logged, joined or
/// not. `maintain_catalog` submits only a sample of at most
/// [`MAINTENANCE_SAMPLE_ROWS`] rows, and only while
/// `catalog.maintenance_samples` is on. A submission matching an active
/// probe joins it.
#[service_factory]
async fn submit_connection_probe(
    authz: Ctx<AuthzContext>,
    catalog: Ctx<ConnectionCatalog>,
    catalog_config: Ctx<CatalogConfig>,
    db: DbPool<InProject>,
    request: WithinProject<CreateRequest<ProbeSubmit, CollectionParam>>,
) -> Result<ProbeRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, submit) = request.into_parts();
    let params = probe_params(&submit)?;
    let admitted = AtomicBool::new(false);

    let unit = |attempt| async {
        let mut trx = db.begin(attempt).await?;
        let grant = authz
            .require::<Project, (DevelopProject, MaintainCatalog)>(&mut trx, &ctx, &project)
            .await?;
        if !authz
            .satisfies::<Project, DevelopProject>(&mut trx, &ctx, &project)
            .await?
        {
            within_maintenance(&catalog_config, &submit, &params)?;
        }
        let conn = trx.get(grant);
        let names = resolve_catalog(conn, &param).await?;
        let connection_id = names.connection_id;
        conn.for_update()
            .fetch_one::<ConnectionDB>(&connection_id)
            .await?;

        for fqn in params.entity.iter().chain(params.entities.iter().flatten()) {
            let fqn = EntityFqn::try_from(fqn.as_str())?;
            let entity: Option<ConnectionEntitySummaryDB> =
                conn.fetch_first(and!(connection_id, fqn.clone())).await?;
            if entity.is_none() {
                return Err(entity_not_in_catalog(&fqn));
            }
        }

        let key = dedupe_key(&connection_id, submit.kind, &params, &ctx.user_id)?;
        let active = conn
            .fetch_first::<ConnectionProbeDB>(ConnectionProbeDB::active(key.clone()))
            .await?;
        let joined = active.is_some();
        let probe = match active {
            Some(active) => active,
            None => {
                let full = conn
                    .fetch_nth_last::<ConnectionProbeDB>(
                        ConnectionProbeDB::queued(),
                        (MAX_QUEUED_PER_PROJECT - 1) as u64,
                    )
                    .await?
                    .is_some();
                if full {
                    return Err(api_error!(
                        ApiError::Any(429),
                        "rate_limited: the project already has {MAX_QUEUED_PER_PROJECT} probes queued"
                    ));
                }
                if !admitted.swap(true, Ordering::Relaxed) {
                    catalog.limiter.admit(&ctx.user_id).map_err(rate_limited)?;
                }

                let id = ProbeId::default();
                conn.insert(&ConnectionProbeInsertDB {
                    id,
                    connection_id,
                    probe_kind: submit.kind,
                    params: Json(params.clone()),
                    dedupe_key: key,
                    status: ProbeStatus::Queued,
                    requested_by_id: ctx.user_id,
                    requested_on: ProbeRequestedOn::try_from(ctx.time)?,
                })
                .await?;
                conn.fetch_one(&id).await?
            }
        };
        trx.commit().await?;
        Ok((names, connection_id, joined, probe))
    };
    // An identical probe committed after this unit's snapshot (a scheduled
    // refresh, another replica's submission) is joined by a fresh one.
    let (names, connection_id, joined, probe) = match retrying(unit).await {
        Err(e) if unique_violation_on(&e, ACTIVE_PROBE_INDEX) => retrying(unit).await?,
        submitted => submitted?,
    };

    if submit.kind == ProbeKind::Sample {
        info!(
            target: "tabsdata::audit",
            event = "connection_sample_requested",
            user_id = %ctx.user_id,
            project_id = %project,
            connection_id = %connection_id,
            probe_id = %probe.base.id,
            entity = params.entity.as_deref().unwrap_or_default(),
            entities = params.entities.as_ref().map(|e| e.join(",")).unwrap_or_default(),
            limit = params.limit.unwrap_or_default(),
            joined,
        );
    }
    if !joined {
        catalog.submitted.notify_one();
    }
    Ok(ProbeRead::of(
        probe,
        names.collection,
        &ctx.user_id,
        &AtTime::now(),
    ))
}

/// The rows a catalog maintainer may sample per entity.
pub const MAINTENANCE_SAMPLE_ROWS: i32 = 5;

fn within_maintenance(
    config: &CatalogConfig,
    submit: &ProbeSubmit,
    params: &ProbeParams,
) -> Result<(), TdError> {
    if submit.kind != ProbeKind::Sample {
        return Err(api_error!(
            ApiError::Forbidden,
            "maintain_catalog submits only sample probes, not '{}'",
            submit.kind
        ));
    }
    if !config.maintenance_samples {
        return Err(api_error!(
            ApiError::Forbidden,
            "maintain_catalog samples are off: catalog.maintenance_samples is false"
        ));
    }
    let limit = params.limit.unwrap_or(*SampleRows::default());
    if limit > MAINTENANCE_SAMPLE_ROWS {
        return Err(api_error!(
            ApiError::Forbidden,
            "maintain_catalog samples at most {MAINTENANCE_SAMPLE_ROWS} rows, got {limit}"
        ));
    }
    Ok(())
}

fn probe_params(submit: &ProbeSubmit) -> Result<ProbeParams, TdError> {
    match submit.kind {
        ProbeKind::Sample => {
            let limit = *submit.limit.unwrap_or_default();
            match (&submit.entity, &submit.entities) {
                (Some(entity), None) => Ok(ProbeParams {
                    entity: Some(entity.to_string()),
                    entities: None,
                    limit: Some(limit),
                }),
                (None, Some(entities)) if (1..=MAX_SAMPLE_ENTITIES).contains(&entities.len()) => {
                    let mut names: Vec<String> = entities.iter().map(|e| e.to_string()).collect();
                    names.sort();
                    names.dedup();
                    Ok(ProbeParams {
                        entity: None,
                        entities: Some(names),
                        limit: Some(limit),
                    })
                }
                (None, Some(entities)) => Err(api_error!(
                    ApiError::InputError,
                    "a sample takes 1 to {MAX_SAMPLE_ENTITIES} entities, got {}",
                    entities.len()
                )),
                _ => Err(api_error!(
                    ApiError::InputError,
                    "a sample probe names its entity, or its entities"
                )),
            }
        }
        ProbeKind::Check | ProbeKind::Metadata => {
            if submit.entity.is_some() || submit.entities.is_some() || submit.limit.is_some() {
                return Err(api_error!(
                    ApiError::InputError,
                    "only a sample probe takes entities and a limit"
                ));
            }
            Ok(ProbeParams::default())
        }
    }
}
