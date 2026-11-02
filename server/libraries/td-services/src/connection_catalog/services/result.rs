//
// Copyright 2026 Tabsdata Inc.
//

use crate::connection_catalog::{
    ConnectionCatalog, MAX_ENTITIES, MAX_SAMPLE_BYTES, SAMPLE_TTL, shifted,
};
use serde::Deserialize;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::{AcrossProjects, DbPool, Grant, InProject, WriteMode, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection_catalog::{
    CatalogCheckWriteDB, CatalogMetadataFailureWriteDB, CatalogMetadataWriteDB,
    ConnectionCatalogDB, ConnectionCatalogDBWithNames, ConnectionEntityDB,
    ConnectionEntityInsertDB, ConnectionProbeDB, ProbeCallback, ProbeEndUpdateDB, ProbeParam,
};
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::types::basic::{
    AtTime, CatalogEntityKind, CatalogSchemaHash, CatalogTruncated, CatalogUpdatedOn, CheckError,
    CheckStatus, CheckedOn, ConnectionEntityId, Ended, EntityCount, EntityDoc, EntityFqn, Expires,
    FieldCount, FieldNames, MetadataError, MetadataOn, MetadataStatus, ProbeError, ProbeKind,
    ProbeResult, ProbeStatus,
};
use td_objects::types::json::Json;
use td_queries::Queries;
use tracing::{info, warn};

/// What the probe runner posts as the `Done` callback's output.
#[derive(Debug, Deserialize)]
struct Outcome {
    status: String,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<ProbeError>,
}

#[derive(Debug, Deserialize)]
struct MetadataResult {
    schema_hash: String,
    #[serde(default)]
    truncated: bool,
    #[serde(default)]
    entities: Vec<EntityRow>,
}

#[derive(Debug, Deserialize)]
struct EntityRow {
    fqn: String,
    entity_kind: String,
    #[serde(default)]
    field_names: String,
    #[serde(default)]
    field_count: i32,
    doc: serde_json::Value,
}

#[derive(Clone)]
enum Ending {
    Succeeded(serde_json::Value),
    Failed(ProbeStatus, ProbeError),
}

/// Persists a probe's outcome: the kind's catalog columns and entities, the
/// probe's own status and result. An outcome for a probe that already ended
/// (cancelled, timed out) is dropped.
#[service_factory]
async fn connection_probe_result(
    catalog: Ctx<ConnectionCatalog>,
    db: DbPool<AcrossProjects>,
    request: UpdateRequest<ProbeCallback, ProbeParam>,
) -> Result<(), TdError> {
    let (_, param, callback) = request.into_parts();
    let grant = Grant::asserted("connection probe outcome callback");
    let Some(ending) = ending(&callback)? else {
        return Ok(());
    };
    let probe: ConnectionProbeDB = {
        let mut conn = db.read().await?;
        conn.get(grant).fetch_one(&param.probe).await?
    };
    if probe.base.status.is_terminal() {
        return Ok(());
    }

    let in_project = db.in_project(&probe.project_id);
    let Some((probe, end)) = retrying(|attempt| async {
        let mut trx = in_project.begin(attempt).await?;
        let conn = trx.get(grant);
        let probe: ConnectionProbeDB = conn.for_update().fetch_one(&param.probe).await?;
        if probe.base.status.is_terminal() {
            trx.commit().await?;
            return Ok(None);
        }
        let names: ConnectionCatalogDBWithNames = conn.fetch_one(&probe.base.connection_id).await?;
        let now = AtTime::now();
        let (end, entities_changed) = persist(conn, &probe, &names, ending.clone(), &now).await?;
        conn.update(&end, &param.probe).await?;
        if entities_changed {
            conn.connection_catalogs()
                .text_changed(&probe.base.connection_id)
                .await?;
        }
        trx.commit().await?;
        Ok(Some((probe, end)))
    })
    .await?
    else {
        return Ok(());
    };

    if probe.base.probe_kind == ProbeKind::Sample {
        let rows = end
            .result
            .as_ref()
            .map_or(0, |result| sample_rows(&result.0.0));
        info!(
            target: "tabsdata::audit",
            event = "connection_sample_served",
            user_id = %probe.base.requested_by_id,
            project_id = %probe.project_id,
            connection_id = %probe.base.connection_id,
            probe_id = %probe.base.id,
            entity = probe.base.params.entity.as_deref().unwrap_or_default(),
            entities = probe.base.params.entities.as_ref().map(|e| e.join(",")).unwrap_or_default(),
            status = %end.status,
            rows,
        );
    }
    catalog.ended.notify_waiters();
    Ok(())
}

fn ending(callback: &ProbeCallback) -> Result<Option<Ending>, TdError> {
    match callback.status {
        WorkerCallbackStatus::Running => Ok(None),
        WorkerCallbackStatus::Failed | WorkerCallbackStatus::Stopped => Ok(Some(Ending::Failed(
            ProbeStatus::Failed,
            ProbeError::new(
                "probe_failed",
                callback
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "the probe pod failed".to_owned()),
            ),
        ))),
        WorkerCallbackStatus::Done => {
            let output = callback.output.as_ref().ok_or_else(|| {
                api_error!(ApiError::InputError, "a probe outcome carries its output")
            })?;
            let outcome: Outcome = serde_json::from_value(output.clone())
                .map_err(|e| api_error!(ApiError::InputError, "not a probe outcome: {e}"))?;
            let error = || {
                outcome
                    .error
                    .clone()
                    .unwrap_or_else(|| ProbeError::new(outcome.status.clone(), ""))
                    .bounded()
            };
            Ok(Some(match outcome.status.as_str() {
                "succeeded" => Ending::Succeeded(outcome.result.unwrap_or_default()),
                "timeout" => Ending::Failed(ProbeStatus::Timeout, error()),
                _ => Ending::Failed(ProbeStatus::Failed, error()),
            }))
        }
    }
}

fn unsupported(result: &serde_json::Value) -> bool {
    result.get("supported") == Some(&serde_json::Value::Bool(false))
}

/// The probe's ending, and whether the catalog's entities changed.
async fn persist<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    probe: &ConnectionProbeDB,
    names: &ConnectionCatalogDBWithNames,
    ending: Ending,
    now: &AtTime,
) -> Result<(ProbeEndUpdateDB, bool), TdError> {
    let ended_on = Ended::from(*now);
    let mut entities_changed = false;
    let (status, error, result) = match ending {
        Ending::Failed(status, error) => {
            match probe.base.probe_kind {
                ProbeKind::Check => {
                    write_check(
                        conn,
                        probe,
                        names,
                        check_status(status),
                        Some(&error.message),
                        now,
                    )
                    .await?
                }
                ProbeKind::Metadata => {
                    write_metadata_failure(
                        conn,
                        probe,
                        names,
                        metadata_status(status),
                        &error.message,
                        now,
                    )
                    .await?
                }
                ProbeKind::Sample => {}
            }
            (status, Some(Json(error)), None)
        }
        Ending::Succeeded(result) => {
            let result = match probe.base.probe_kind {
                ProbeKind::Check => {
                    let (status, message) = if unsupported(&result) {
                        (CheckStatus::Unsupported, None)
                    } else if result.get("ok") == Some(&serde_json::Value::Bool(true)) {
                        (CheckStatus::Ok, None)
                    } else {
                        (
                            CheckStatus::Error,
                            result
                                .get("error")
                                .and_then(|e| e.as_str())
                                .map(|e| ProbeError::bound(e.to_owned())),
                        )
                    };
                    write_check(conn, probe, names, status, message.as_deref(), now).await?;
                    result
                }
                ProbeKind::Metadata if unsupported(&result) => {
                    write_metadata_failure(
                        conn,
                        probe,
                        names,
                        MetadataStatus::Unsupported,
                        "",
                        now,
                    )
                    .await?;
                    result
                }
                ProbeKind::Metadata => {
                    let (result, changed) = write_metadata(conn, probe, names, result, now).await?;
                    entities_changed = changed;
                    result
                }
                ProbeKind::Sample => cap_sample(result),
            };
            (
                ProbeStatus::Succeeded,
                None,
                Some(Json(ProbeResult(result))),
            )
        }
    };
    let result_expires = match (probe.base.probe_kind, &result) {
        (ProbeKind::Sample, Some(_)) => Some(Expires::from(shifted(now, SAMPLE_TTL, true))),
        _ => None,
    };
    Ok((
        ProbeEndUpdateDB {
            status,
            error,
            result,
            result_expires,
            ended_on,
        },
        entities_changed,
    ))
}

fn check_status(status: ProbeStatus) -> CheckStatus {
    match status {
        ProbeStatus::Timeout => CheckStatus::Timeout,
        _ => CheckStatus::Error,
    }
}

fn metadata_status(status: ProbeStatus) -> MetadataStatus {
    match status {
        ProbeStatus::Timeout => MetadataStatus::Timeout,
        _ => MetadataStatus::Error,
    }
}

async fn write_check<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    probe: &ConnectionProbeDB,
    names: &ConnectionCatalogDBWithNames,
    status: CheckStatus,
    message: Option<&str>,
    now: &AtTime,
) -> Result<(), TdError> {
    conn.upsert(&CatalogCheckWriteDB {
        connection_id: names.connection_id,
        collection_id: names.collection_id,
        kind: names.kind.clone(),
        check_status: status,
        check_error: message
            .filter(|message| !message.is_empty())
            .map(CheckError::try_from)
            .transpose()?,
        checked_on: CheckedOn::try_from(*now)?,
        last_probe_id: probe.base.id,
        updated_on: CatalogUpdatedOn::try_from(*now)?,
    })
    .await?;
    Ok(())
}

async fn write_metadata_failure<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    probe: &ConnectionProbeDB,
    names: &ConnectionCatalogDBWithNames,
    status: MetadataStatus,
    message: &str,
    now: &AtTime,
) -> Result<(), TdError> {
    conn.upsert(&CatalogMetadataFailureWriteDB {
        connection_id: names.connection_id,
        collection_id: names.collection_id,
        kind: names.kind.clone(),
        metadata_status: status,
        metadata_error: (!message.is_empty())
            .then(|| MetadataError::try_from(message))
            .transpose()?,
        last_probe_id: probe.base.id,
        updated_on: CatalogUpdatedOn::try_from(*now)?,
    })
    .await?;
    Ok(())
}

/// The entities by fqn, without repeats, at most `max`; and whether any were
/// dropped, here or by the runner.
fn kept_entities(
    mut entities: Vec<EntityRow>,
    truncated: bool,
    max: usize,
) -> (Vec<EntityRow>, bool) {
    entities.sort_by(|a, b| a.fqn.cmp(&b.fqn));
    entities.dedup_by(|a, b| a.fqn == b.fqn);
    let truncated = truncated || entities.len() > max;
    entities.truncate(max);
    (entities, truncated)
}

/// An unchanged fingerprint only stamps the catalog; a changed one replaces
/// the entities in the same transaction. The probe keeps the summary; the
/// flag says whether the entities were replaced.
async fn write_metadata<M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    probe: &ConnectionProbeDB,
    names: &ConnectionCatalogDBWithNames,
    result: serde_json::Value,
    now: &AtTime,
) -> Result<(serde_json::Value, bool), TdError> {
    let metadata: MetadataResult = serde_json::from_value(result)
        .map_err(|e| api_error!(ApiError::InputError, "not a metadata result: {e}"))?;
    let schema_hash = CatalogSchemaHash::try_from(metadata.schema_hash.as_str())?;
    let previous: Option<ConnectionCatalogDB> = conn.fetch_optional(&names.connection_id).await?;
    let changed = previous
        .as_ref()
        .and_then(|previous| previous.schema_hash.as_ref())
        != Some(&schema_hash);

    let (entities, truncated) = kept_entities(metadata.entities, metadata.truncated, MAX_ENTITIES);
    let (entity_count, truncated) = match previous.as_ref().filter(|_| !changed) {
        Some(previous) => (*previous.entity_count as usize, *previous.truncated),
        None => (entities.len(), truncated),
    };

    conn.upsert(&CatalogMetadataWriteDB {
        connection_id: names.connection_id,
        collection_id: names.collection_id,
        kind: names.kind.clone(),
        check_status: CheckStatus::Ok,
        check_error: None,
        checked_on: CheckedOn::try_from(*now)?,
        schema_hash: schema_hash.clone(),
        entity_count: EntityCount::try_from(entity_count as i32)?,
        truncated: CatalogTruncated::from(truncated),
        metadata_on: MetadataOn::try_from(*now)?,
        metadata_status: MetadataStatus::Ok,
        metadata_error: None,
        last_probe_id: probe.base.id,
        updated_on: CatalogUpdatedOn::try_from(*now)?,
    })
    .await?;

    if changed {
        conn.delete::<ConnectionEntityDB>(&names.connection_id)
            .await?;
        let rows = entities
            .into_iter()
            .map(|entity| {
                Ok(ConnectionEntityInsertDB {
                    id: ConnectionEntityId::default(),
                    connection_id: names.connection_id,
                    fqn: EntityFqn::try_from(entity.fqn)?,
                    entity_kind: CatalogEntityKind::try_from(entity.entity_kind)?,
                    field_names: FieldNames::try_from(entity.field_names)?,
                    field_count: FieldCount::try_from(entity.field_count)?,
                    doc: Json(EntityDoc(entity.doc)),
                })
            })
            .collect::<Result<Vec<_>, TdError>>()?;
        conn.chunked().insert(&rows).await?;
    }
    let summary = serde_json::json!({
        "schema_hash": schema_hash.to_string(),
        "entity_count": entity_count,
        "changed": changed,
        "truncated": truncated,
    });
    Ok((summary, changed))
}

fn byte_size(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |v| v.len())
}

/// The rows a sample outcome holds, one sample or several.
fn sample_rows(result: &serde_json::Value) -> usize {
    let rows = |sample: &serde_json::Value| {
        sample
            .get("rows")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len)
    };
    match result.get("samples").and_then(serde_json::Value::as_array) {
        Some(samples) => samples.iter().map(rows).sum(),
        None => rows(result),
    }
}

/// The runner caps rows and bytes already; the server holds the same line.
/// Several samples lose rows from the one holding the most, one at a time.
fn cap_sample(mut result: serde_json::Value) -> serde_json::Value {
    if byte_size(&result) <= MAX_SAMPLE_BYTES {
        return result;
    }
    if result
        .get("samples")
        .is_some_and(serde_json::Value::is_array)
    {
        while byte_size(&result) > MAX_SAMPLE_BYTES {
            let samples = result["samples"].as_array_mut().expect("checked above");
            let largest = samples
                .iter_mut()
                .filter_map(|sample| {
                    let len = sample.get("rows")?.as_array()?.len();
                    (len > 0).then_some((len, sample))
                })
                .max_by_key(|(len, _)| *len);
            let Some((_, sample)) = largest else {
                warn!("a sample outcome over the byte cap was dropped");
                return serde_json::json!({"samples": []});
            };
            sample["rows"].as_array_mut().expect("filtered above").pop();
            sample["truncated"] = serde_json::Value::Bool(true);
        }
        return result;
    }
    let Some(object) = result.as_object_mut() else {
        warn!("a sample outcome over the byte cap was dropped");
        return serde_json::json!({"columns": [], "rows": [], "truncated": true});
    };
    object.insert("truncated".to_owned(), serde_json::Value::Bool(true));
    let rows = match object.remove("rows") {
        Some(serde_json::Value::Array(rows)) => rows,
        _ => Vec::new(),
    };
    object.insert("rows".to_owned(), serde_json::Value::Array(Vec::new()));
    let mut used = byte_size(&result);
    let mut kept = Vec::new();
    for row in rows {
        let row_size = byte_size(&row) + 1;
        if used + row_size > MAX_SAMPLE_BYTES {
            break;
        }
        used += row_size;
        kept.push(row);
    }
    result["rows"] = serde_json::Value::Array(kept);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entities_past_the_cap_keep_the_first_by_fqn() {
        let row = |fqn: &str| EntityRow {
            fqn: fqn.to_owned(),
            entity_kind: "table".to_owned(),
            field_names: String::new(),
            field_count: 0,
            doc: serde_json::Value::Null,
        };
        let fqns =
            |entities: &[EntityRow]| entities.iter().map(|e| e.fqn.clone()).collect::<Vec<_>>();
        let (kept, truncated) =
            kept_entities(vec![row("c"), row("a"), row("b"), row("a")], false, 2);
        assert_eq!(fqns(&kept), ["a", "b"]);
        assert!(truncated);
        let (kept, truncated) = kept_entities(vec![row("b"), row("a")], false, 2);
        assert_eq!(fqns(&kept), ["a", "b"]);
        assert!(!truncated);
        assert!(
            kept_entities(vec![row("a")], true, 2).1,
            "the runner's own truncation holds"
        );
    }

    #[test]
    fn a_sample_over_the_byte_cap_loses_rows_and_says_so() {
        let row = serde_json::json!(["x".repeat(1024)]);
        let rows = vec![row; 2048];
        let capped = cap_sample(serde_json::json!({"columns": ["c"], "rows": rows}));
        assert!(serde_json::to_vec(&capped).unwrap().len() <= MAX_SAMPLE_BYTES);
        assert_eq!(capped["truncated"], true);
        assert!(!capped["rows"].as_array().unwrap().is_empty());
    }

    #[test]
    fn several_samples_over_the_byte_cap_lose_rows_from_the_largest() {
        let row = serde_json::json!(["x".repeat(1024)]);
        let big =
            serde_json::json!({"entity": "a", "columns": ["c"], "rows": vec![row.clone(); 1500]});
        let small = serde_json::json!({"entity": "b", "columns": ["c"], "rows": vec![row; 5]});
        let failed = serde_json::json!({"entity": "c", "error": "boom"});
        let capped = cap_sample(serde_json::json!({"samples": [big, small, failed]}));
        assert!(serde_json::to_vec(&capped).unwrap().len() <= MAX_SAMPLE_BYTES);
        let samples = capped["samples"].as_array().unwrap();
        assert_eq!(samples[0]["truncated"], true);
        assert_eq!(samples[1]["rows"].as_array().unwrap().len(), 5);
        assert_eq!(samples[2]["error"], "boom");
        assert_eq!(
            sample_rows(&capped),
            samples[0]["rows"].as_array().unwrap().len() + 5
        );
    }

    #[test]
    fn error_texts_are_bounded_whatever_the_runner_sends() {
        let long = "x".repeat(10 * 1024);
        let failed = ProbeCallback {
            status: WorkerCallbackStatus::Failed,
            error_message: Some(long.clone()),
            output: None,
        };
        let Some(Ending::Failed(_, error)) = ending(&failed).unwrap() else {
            panic!("a failed callback ends the probe");
        };
        assert_eq!(error.message.len(), ProbeError::MAX_BYTES);

        let done = ProbeCallback {
            status: WorkerCallbackStatus::Done,
            output: Some(serde_json::json!({
                "status": "failed", "error": {"code": long, "message": long}
            })),
            error_message: None,
        };
        let Some(Ending::Failed(_, error)) = ending(&done).unwrap() else {
            panic!("a failed outcome ends the probe");
        };
        assert_eq!(error.code.len(), ProbeError::MAX_BYTES);
        assert_eq!(error.message.len(), ProbeError::MAX_BYTES);
    }

    #[test]
    fn the_callback_envelope_and_the_output_both_end_a_probe() {
        let failed = ProbeCallback {
            status: WorkerCallbackStatus::Failed,
            error_message: Some("boom".to_owned()),
            output: None,
        };
        assert!(matches!(
            ending(&failed).unwrap(),
            Some(Ending::Failed(ProbeStatus::Failed, ProbeError { message, .. })) if message == "boom"
        ));

        let running = ProbeCallback {
            status: WorkerCallbackStatus::Running,
            error_message: None,
            output: None,
        };
        assert!(ending(&running).unwrap().is_none());

        let timeout = ProbeCallback {
            status: WorkerCallbackStatus::Done,
            output: Some(serde_json::json!({
                "probe_id": "p", "kind": "metadata", "status": "timeout",
                "error": {"code": "timeout", "message": "exceeded 90s"}
            })),
            error_message: None,
        };
        assert!(matches!(
            ending(&timeout).unwrap(),
            Some(Ending::Failed(ProbeStatus::Timeout, _))
        ));

        let succeeded = ProbeCallback {
            status: WorkerCallbackStatus::Done,
            output: Some(serde_json::json!({
                "probe_id": "p", "kind": "check", "status": "succeeded", "result": {"ok": true}
            })),
            error_message: None,
        };
        assert!(matches!(
            ending(&succeeded).unwrap(),
            Some(Ending::Succeeded(result)) if result["ok"] == true
        ));
    }
}
