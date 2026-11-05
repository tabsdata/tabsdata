//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::{
    ACTIVE_PROBE_INDEX, ConnectionCatalog, DELIVERY_GRACE, MAX_CONCURRENT,
    MAX_CONCURRENT_PER_PROJECT, MAX_ENTITIES, MAX_ENTITIES_BYTES, MAX_QUEUED,
    MAX_QUEUED_PER_PROJECT, RETENTION, dedupe_key, entity_not_in_catalog, running_deadline,
    shifted,
};
use crate::internal_api::InternalApi;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_bridge::messaging::{OrchestratorMessagePayload, ProbeRequest, ProbeStartRequest};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::access::{
    AccessMode, AcrossProjects, DbPool, Grant, Holds, InProject, ProjectLock, SeesProjectRows,
    retrying, unique_violation_on,
};
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::{ServingPool, SystemPool};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::connection_catalog::{
    ConnectionCatalogDBWithNames, ConnectionEntityDB, ConnectionProbeDB, ConnectionProbeInsertDB,
    EndedBefore, MetadataDue, MetadataFailedSince, MetadataRequestedSince, ProbeDeliveredUpdateDB,
    ProbeEndUpdateDB, ProbeRequeueUpdateDB, ProbeResultPurgeDB, ProbeStartUpdateDB, QueuedOverdue,
    RunningOverdue, SampleResultsExpired, UndeliveredOverdue,
};
use td_objects::types::basic::{
    AtTime, ConnectionId, Dispatched, Ended, EntityFqn, Expires, MetadataOn, MetadataStatus,
    ProbeError, ProbeId, ProbeKind, ProbeParams, ProbeRequestedOn, ProbeStatus, ProjectId, Started,
    UserId,
};
use td_objects::types::catalog::CatalogConfig;
use td_objects::types::json::Json;
use td_queries::Queries;
use tracing::{debug, warn};

type ProbeMessage = OrchestratorMessagePayload<serde_yaml::Value>;

/// One dispatcher tick, each project under its own lock.
#[service_factory]
async fn dispatch_connection_probes(
    catalog: Ctx<ConnectionCatalog>,
    catalog_config: Ctx<CatalogConfig>,
    bridge: Ctx<BridgeProviderHandle>,
    internal_api: Ctx<InternalApi>,
    db: DbPool<AcrossProjects, SystemPool>,
    _request: (),
) -> Result<(), TdError> {
    let grant = Grant::asserted("connection probe dispatch tick");
    let now = AtTime::now();

    let projects: Vec<ProjectId> = {
        let mut conn = db.read().await?;
        let probes: Vec<ConnectionProbeDB> = conn.get(grant).fetch_all(()).await?;
        probes
            .into_iter()
            .map(|probe| probe.project_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    };

    let mut timed_out = 0;
    for project in &projects {
        let in_project = db.in_project(project);
        timed_out += retrying(|attempt| async {
            let mut trx = in_project.lock_project().begin(attempt).await?;
            let conn = trx.get(grant);
            conn.update(
                &ProbeResultPurgeDB::default(),
                SampleResultsExpired {
                    at: Expires::from(now),
                },
            )
            .await?;
            conn.delete::<ConnectionProbeDB>(EndedBefore {
                at: Ended::from(shifted(&now, RETENTION, false)),
            })
            .await?;
            let mut timed_out = 0;
            for kind in [ProbeKind::Check, ProbeKind::Metadata, ProbeKind::Sample] {
                timed_out += conn
                    .update(
                        &ended(ProbeStatus::Timeout, "no outcome reported in time", &now),
                        RunningOverdue {
                            kind,
                            started_before: Started::from(shifted(
                                &now,
                                running_deadline(kind),
                                false,
                            )),
                        },
                    )
                    .await?
                    .rows_affected();
            }
            timed_out += conn
                .update(
                    &ended(ProbeStatus::Timeout, "never started within the caps", &now),
                    QueuedOverdue {
                        requested_before: ProbeRequestedOn::try_from(shifted(
                            &now, MAX_QUEUED, false,
                        ))?,
                    },
                )
                .await?
                .rows_affected();
            conn.update(
                &ProbeRequeueUpdateDB::default(),
                UndeliveredOverdue {
                    started_before: Started::from(shifted(&now, DELIVERY_GRACE, false)),
                },
            )
            .await?;
            trx.commit().await?;
            Ok(timed_out)
        })
        .await?;
    }

    let kubernetes = matches!(bridge.orchestrator(), Orchestrator::Kubernetes);
    let refresh_interval = catalog_config.refresh_interval.filter(|_| kubernetes);

    let refreshes = {
        let mut conn = db.read().await?;
        match refresh_interval {
            Some(interval) => due_refreshes(conn.get(grant), interval, &now).await?,
            None => Vec::new(),
        }
    };
    let queued = queue_refreshes(&db, grant, refreshes, &now).await?;
    if queued > 0 {
        debug!(queued, "scheduled connection metadata refreshes");
    }

    let chosen = {
        let mut conn = db.read().await?;
        let conn = conn.get(grant);
        let running: Vec<ConnectionProbeDB> = conn.fetch_all(ConnectionProbeDB::running()).await?;
        let queued: Vec<ConnectionProbeDB> = conn.fetch_all(ConnectionProbeDB::queued()).await?;
        within_caps(MAX_CONCURRENT, MAX_CONCURRENT_PER_PROJECT, &running, queued)
    };
    let mut by_project: HashMap<ProjectId, HashSet<ProbeId>> = HashMap::new();
    for probe in chosen {
        by_project
            .entry(probe.project_id)
            .or_default()
            .insert(probe.base.id);
    }
    let mut messages = Vec::new();
    let mut refused = 0;
    for (project, chosen) in by_project {
        let in_project = db.in_project(&project);
        let (claimed, refused_here) = retrying(|attempt| async {
            let mut trx = in_project.lock_project().begin(attempt).await?;
            let claimed = claim(trx.get(grant), &bridge, &internal_api, &chosen, &now).await?;
            trx.commit().await?;
            Ok(claimed)
        })
        .await?;
        messages.extend(claimed);
        refused += refused_here;
    }

    let mut requeued = 0;
    for (project, probe, message) in messages {
        let delivered = bridge.message_queue().put(probe.to_string(), message).await;
        if let Err(e) = &delivered {
            warn!(probe_id = %probe, "connection probe dispatch failed: {e:#?}");
            requeued += 1;
        }
        settle_delivery(&db, grant, &project, probe, delivered.is_ok()).await?;
    }
    if requeued > 0 {
        debug!(requeued, "connection probes back in the queue");
    }

    if timed_out > 0 || refused > 0 {
        debug!(timed_out, refused, "connection probe dispatch tick");
        catalog.ended.notify_waiters();
    }
    Ok(())
}

/// Claims the project's queued probes the caps chose and marks them
/// running; a probe whose request cannot be built fails instead.
async fn claim<M: Holds<ProjectLock>>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    bridge: &BridgeProviderHandle,
    internal_api: &InternalApi,
    chosen: &HashSet<ProbeId>,
    now: &AtTime,
) -> Result<(Vec<(ProjectId, ProbeId, ProbeMessage)>, usize), TdError> {
    let queued: Vec<ConnectionProbeDB> = conn
        .for_update()
        .claim_all(ConnectionProbeDB::queued())
        .await?;
    let mut messages = Vec::new();
    let mut refused = 0;
    for probe in queued
        .into_iter()
        .filter(|probe| chosen.contains(&probe.base.id))
    {
        let message = match bridge.orchestrator() {
            Orchestrator::Kubernetes => {
                probe_start(conn, internal_api, &probe).await.map(|start| {
                    OrchestratorMessagePayload::OrchestratorProbeRequestMessagePayload(
                        ProbeRequest::Start(start),
                    )
                })
            }
            Orchestrator::Supervisor => Err(ProbeError::new(
                "unsupported",
                "connection probes run on kubernetes only",
            )),
        };
        match message {
            Ok(message) => {
                conn.update(
                    &ProbeStartUpdateDB {
                        status: ProbeStatus::Running,
                        started_on: Started::from(*now),
                    },
                    ConnectionProbeDB::queued_of(probe.base.id),
                )
                .await?;
                messages.push((probe.project_id, probe.base.id, message));
            }
            Err(error) => {
                conn.update(
                    &end_with(ProbeStatus::Failed, error, now),
                    ConnectionProbeDB::queued_of(probe.base.id),
                )
                .await?;
                refused += 1;
            }
        }
    }
    Ok((messages, refused))
}

/// Interactive probes before scheduled refreshes, each oldest first, within
/// the instance and per-project caps.
pub(super) fn within_caps(
    max_concurrent: usize,
    max_concurrent_per_project: usize,
    running: &[ConnectionProbeDB],
    mut queued: Vec<ConnectionProbeDB>,
) -> Vec<ConnectionProbeDB> {
    queued.sort_by_key(|probe| probe.base.requested_by_id == UserId::system());
    let mut per_project: HashMap<ProjectId, usize> = HashMap::new();
    for probe in running {
        *per_project.entry(probe.project_id).or_default() += 1;
    }
    let mut total = running.len();
    let mut chosen = Vec::new();
    for probe in queued {
        if total >= max_concurrent {
            break;
        }
        let project = per_project.entry(probe.project_id).or_default();
        if *project >= max_concurrent_per_project {
            continue;
        }
        *project += 1;
        total += 1;
        chosen.push(probe);
    }
    chosen
}

async fn probe_start<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    internal_api: &InternalApi,
    probe: &ConnectionProbeDB,
) -> Result<ProbeStartRequest, ProbeError> {
    let failed = |message: String| ProbeError::new("dispatch_failed", message);
    let names: ConnectionCatalogDBWithNames = conn
        .fetch_one(&probe.base.connection_id)
        .await
        .map_err(|e| failed(e.to_string()))?;
    let mut request = serde_json::json!({
        "probe_id": probe.base.id.to_string(),
        "kind": probe.base.probe_kind.to_string(),
        "params": &probe.base.params.0,
        "max_entities": MAX_ENTITIES,
        "max_entities_bytes": MAX_ENTITIES_BYTES,
    });
    let not_found = |fqn: &str| {
        ProbeError::new(
            "entity_not_in_catalog",
            entity_not_in_catalog(fqn).to_string(),
        )
    };
    let mut docs = Vec::new();
    for fqn in probe
        .base
        .params
        .entity
        .iter()
        .chain(probe.base.params.entities.iter().flatten())
    {
        let fqn = EntityFqn::try_from(fqn.as_str()).map_err(|_| not_found(fqn))?;
        let entity: ConnectionEntityDB = conn
            .fetch_first(and!(probe.base.connection_id, fqn.clone()))
            .await
            .map_err(|e| failed(e.to_string()))?
            .ok_or_else(|| not_found(&fqn))?;
        docs.push(entity.base.doc.0.0);
    }
    if probe.base.params.entities.is_some() {
        request["entity_docs"] = serde_json::Value::Array(docs);
    } else if let Some(doc) = docs.pop() {
        request["entity_doc"] = doc;
    }
    let responder = internal_api
        .probe_responder(&probe.base.id, running_deadline(probe.base.probe_kind))
        .map_err(|e| failed(e.to_string()))?;
    Ok(ProbeStartRequest {
        probe_id: probe.base.id,
        responder,
        project_id: probe.project_id,
        project: Some(names.project),
        collection_id: names.collection_id,
        collection: names.collection,
        connection_id: names.connection_id,
        connection: names.kind,
        request,
    })
}

/// The connections due a metadata probe, requested by the system: never
/// explored, or its metadata older than `interval`; not unsupported; no
/// metadata probe active, or requested within the interval; and backed off
/// from the probes that failed since (see [`refresh_due`]); most overdue
/// first, at most [`MAX_CONCURRENT`] a tick and never past a project's queue
/// cap.
async fn due_refreshes<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    interval: Duration,
    now: &AtTime,
) -> Result<Vec<ConnectionCatalogDBWithNames>, TdError> {
    let cutoff = shifted(now, interval, false);
    let recent: Vec<ConnectionProbeDB> = conn
        .fetch_all(MetadataRequestedSince {
            since: ProbeRequestedOn::try_from(cutoff)?,
        })
        .await?;
    let recent: HashSet<ConnectionId> = recent.into_iter().map(|p| p.base.connection_id).collect();
    let failed: Vec<ConnectionProbeDB> = conn
        .fetch_all(MetadataFailedSince {
            since: ProbeRequestedOn::try_from(shifted(now, RETENTION, false))?,
        })
        .await?;
    let mut failures: HashMap<ConnectionId, Vec<i64>> = HashMap::new();
    for probe in failed {
        failures
            .entry(probe.base.connection_id)
            .or_default()
            .push(probe.base.requested_on.timestamp_millis());
    }
    let queued: Vec<ConnectionProbeDB> = conn.fetch_all(ConnectionProbeDB::queued()).await?;
    let mut per_project: HashMap<ProjectId, usize> = HashMap::new();
    for probe in &queued {
        *per_project.entry(probe.project_id).or_default() += 1;
    }
    let running: Vec<ConnectionProbeDB> = conn.fetch_all(ConnectionProbeDB::running()).await?;
    let active: HashSet<ConnectionId> = queued
        .iter()
        .chain(&running)
        .filter(|probe| probe.base.probe_kind == ProbeKind::Metadata)
        .map(|probe| probe.base.connection_id)
        .collect();
    let mut due: Vec<ConnectionCatalogDBWithNames> = conn
        .fetch_all(MetadataDue {
            before: MetadataOn::try_from(cutoff)?,
        })
        .await?;
    due.retain(|catalog| {
        let explored = catalog
            .metadata_on
            .as_ref()
            .map(|metadata_on| metadata_on.timestamp_millis());
        let since: Vec<i64> = failures
            .get(&catalog.connection_id)
            .into_iter()
            .flatten()
            .copied()
            .filter(|requested| explored.is_none_or(|explored| *requested > explored))
            .collect();
        catalog.metadata_status != MetadataStatus::Unsupported
            && !recent.contains(&catalog.connection_id)
            && !active.contains(&catalog.connection_id)
            && refresh_due(
                explored,
                &since,
                interval,
                RETENTION,
                now.timestamp_millis(),
            )
    });
    due.sort_by_key(|catalog| catalog.metadata_on);

    let mut chosen = Vec::new();
    for catalog in due {
        if chosen.len() >= MAX_CONCURRENT {
            break;
        }
        let project = per_project.entry(catalog.project_id).or_default();
        if *project >= MAX_QUEUED_PER_PROJECT {
            continue;
        }
        *project += 1;
        chosen.push(catalog);
    }
    Ok(chosen)
}

/// Whether a connection last explored at `explored` (never, when `None`) is
/// due a refresh at `now` (epoch milliseconds), given when the metadata
/// probes that failed since were requested: `interval` after the
/// exploration (at once when never explored), then, after the last failure,
/// `interval` doubled per failure, at most `cap` (the probe retention: the
/// failures are forgotten past it).
pub(super) fn refresh_due(
    explored: Option<i64>,
    failures: &[i64],
    interval: Duration,
    cap: Duration,
    now: i64,
) -> bool {
    let interval = interval.as_millis().min(i64::MAX as u128) as i64;
    let Some(last) = failures.iter().max() else {
        return explored.is_none_or(|explored| now - explored >= interval);
    };
    let cap = (cap.as_millis().min(i64::MAX as u128) as i64).max(interval);
    let doublings = failures.len().min(32) as u32;
    let wait = interval.saturating_mul(1_i64 << doublings).min(cap);
    now - last >= wait
}

/// Queues the chosen metadata refreshes, each in its project. One that a
/// probe submitted since already covers is skipped.
async fn queue_refreshes<P: ServingPool>(
    db: &DbPool<AcrossProjects, P>,
    grant: Grant,
    due: Vec<ConnectionCatalogDBWithNames>,
    now: &AtTime,
) -> Result<usize, TdError> {
    let system = UserId::system();
    let params = ProbeParams::default();
    let mut queued = 0;
    for catalog in &due {
        let in_project = db.in_project(&catalog.project_id);
        let inserted = retrying(|attempt| async {
            let mut trx = in_project.lock_project().begin(attempt).await?;
            trx.get(grant)
                .insert(&ConnectionProbeInsertDB {
                    id: ProbeId::default(),
                    connection_id: catalog.connection_id,
                    probe_kind: ProbeKind::Metadata,
                    params: Json(params.clone()),
                    dedupe_key: dedupe_key(
                        &catalog.connection_id,
                        ProbeKind::Metadata,
                        &params,
                        &system,
                    )?,
                    status: ProbeStatus::Queued,
                    requested_by_id: system,
                    requested_on: ProbeRequestedOn::try_from(*now)?,
                })
                .await?;
            trx.commit().await?;
            Ok(())
        })
        .await;
        match inserted {
            Ok(()) => queued += 1,
            Err(e) if unique_violation_on(&e, ACTIVE_PROBE_INDEX) => {
                debug!(connection_id = %catalog.connection_id, "metadata refresh already active");
            }
            Err(e) => return Err(e),
        }
    }
    Ok(queued)
}

/// A delivered request is stamped; an undelivered one goes back to the
/// queue for the next tick, within the queued deadline.
async fn settle_delivery<P: ServingPool>(
    db: &DbPool<AcrossProjects, P>,
    grant: Grant,
    project: &ProjectId,
    probe: ProbeId,
    delivered: bool,
) -> Result<(), TdError> {
    let db = db.in_project(project);
    retrying(|attempt| async {
        let mut trx = db.lock_project().begin(attempt).await?;
        let conn = trx.get(grant);
        if delivered {
            conn.update(
                &ProbeDeliveredUpdateDB {
                    dispatched_on: Dispatched::from(AtTime::now()),
                },
                ConnectionProbeDB::running_of(probe),
            )
            .await?;
        } else {
            conn.update(
                &ProbeRequeueUpdateDB::default(),
                ConnectionProbeDB::running_of(probe),
            )
            .await?;
        }
        trx.commit().await?;
        Ok(())
    })
    .await
}

fn ended(status: ProbeStatus, message: &str, now: &AtTime) -> ProbeEndUpdateDB {
    end_with(status, ProbeError::new(status.to_string(), message), now)
}

fn end_with(status: ProbeStatus, error: ProbeError, now: &AtTime) -> ProbeEndUpdateDB {
    ProbeEndUpdateDB {
        status,
        error: Some(Json(error)),
        result: None,
        result_expires: None,
        ended_on: Ended::from(*now),
    }
}
