//
// Copyright 2026 Tabsdata Inc.
//

use super::dispatch::{refresh_due, within_caps};
use crate::Services;
use crate::collection::services::connection::test_support::{
    collection_param, create_collection, seed_dev_user,
};
use crate::connection_catalog::{
    ConnectionCatalog, MAX_ENTITIES, MAX_ENTITIES_BYTES, MAX_QUEUED_PER_PROJECT, SubmitLimiter,
    dedupe_key,
};
use crate::scheduler::test_wiring::dispatch_services;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::messaging::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue, ProbeRequest,
    QueueError,
};
use td_bridge::test::TestBridgeProvider;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::execution_status::WorkerCallbackStatus;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::ApiError;
use td_objects::dxo::connection_catalog::{
    ConnectionProbeDB, EntityListParams, ProbeCallback, ProbeParam, ProbeRead, ProbeSubmit,
    ProbeWaitParams,
};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::user::UserParam;
use td_objects::types::basic::{
    AccessTokenId, CheckStatus, ConnectionId, EntityFqn, GroupName, IncludeFields, MetadataStatus,
    ProbeError, ProbeId, ProbeKind, ProbeParams, ProbeStatus, ProbeWaitSecs, ProjectId, SampleRows,
    TokenScope, UserId, UserIdName,
};
use td_objects::types::catalog::CatalogConfig;
use td_queries::test_utils::SeedEntry;
use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, span};

const HASH_A: &str = "aaaa";
const HASH_B: &str = "bbbb";

/// Services whose bridge records what it sends, in `sent`.
struct Probing {
    services: Services,
    sent: InMemoryOrchestratorMessageQueue,
}

impl std::ops::Deref for Probing {
    type Target = Services;

    fn deref(&self) -> &Services {
        &self.services
    }
}

fn services(db: &TdPool, orchestrator: Orchestrator) -> Probing {
    let sent = InMemoryOrchestratorMessageQueue::default();
    Probing {
        services: Services {
            bridge: Ctx::new(BridgeProviderHandle::new(Arc::new(
                TestBridgeProvider::default()
                    .with_orchestrator(orchestrator)
                    .with_message_queue(sent.clone()),
            ))),
            ..dispatch_services(db)
        },
        sent,
    }
}

async fn project_with_source(db: &TdPool) -> ProjectId {
    let project = db.seed().test_project().await.unwrap();
    create_collection(db, project.project_id, "crm", Some(GroupName::source())).await;
    project.project_id
}

fn as_user(user: UserId) -> RequestContext {
    RequestContext::with(AccessTokenId::default(), user)
}

fn admin() -> RequestContext {
    as_user(UserId::admin())
}

fn rows(rows: i32) -> SampleRows {
    SampleRows::try_from(rows).unwrap()
}

fn submit_of(kind: ProbeKind) -> ProbeSubmit {
    ProbeSubmit {
        kind,
        entity: None,
        entities: None,
        limit: None,
    }
}

async fn submit(
    services: &Services,
    ctx: RequestContext,
    project: ProjectId,
    probe: ProbeSubmit,
) -> Result<ProbeRead, td_error::TdError> {
    services
        .submit_connection_probe(
            ctx.create(collection_param("crm"), probe)
                .in_project(project),
        )
        .await
        .map(|response| response.data)
}

async fn read_probe(services: &Services, project: ProjectId, probe: ProbeId) -> ProbeRead {
    services
        .read_connection_probe(
            admin()
                .read((ProbeParam { probe }, ProbeWaitParams { wait: None }))
                .in_project(project),
        )
        .await
        .unwrap()
        .data
}

async fn report(services: &Services, probe: ProbeId, context: serde_json::Value) {
    services
        .connection_probe_result(RequestContext::internal().update(
            ProbeParam { probe },
            ProbeCallback {
                status: WorkerCallbackStatus::Done,
                error_message: None,
                output: Some(context),
            },
        ))
        .await
        .unwrap();
}

async fn dispatch(services: &Services) {
    services.dispatch_connection_probes(()).await.unwrap();
}

fn entity(fqn: &str, fields: &str) -> serde_json::Value {
    serde_json::json!({
        "fqn": fqn,
        "entity_kind": "table",
        "field_names": fields,
        "field_count": fields.split(' ').count(),
        "doc": {"name": fqn, "columns": fields.split(' ').collect::<Vec<_>>()},
    })
}

async fn refresh(
    services: &Services,
    project: ProjectId,
    hash: &str,
    entities: Vec<serde_json::Value>,
) {
    let probe = submit(services, admin(), project, submit_of(ProbeKind::Metadata))
        .await
        .unwrap();
    dispatch(services).await;
    report(
        services,
        probe.probe_id,
        serde_json::json!({
            "probe_id": probe.probe_id.to_string(), "kind": "metadata", "status": "succeeded",
            "result": {"schema_hash": hash, "entity_count": entities.len(), "truncated": false,
                       "entities": entities},
        }),
    )
    .await;
}

async fn sent_requests(services: &Probing) -> Vec<serde_json::Value> {
    services
        .sent
        .messages()
        .into_iter()
        .filter_map(|message| match message.payload {
            OrchestratorMessagePayload::OrchestratorProbeRequestMessagePayload(
                ProbeRequest::Start(start),
            ) => Some(start.request),
            _ => None,
        })
        .collect()
}

async fn text_revision(db: &TdPool) -> i64 {
    let mut conn = db.begin().await.unwrap();
    sqlx::query_scalar::<_, i64>("SELECT text_revision FROM connection_catalogs")
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

#[td_test::test(db)]
async fn identical_submissions_coalesce_into_one_dispatched_job(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);

    let first = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    let again = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    assert_eq!(first.probe_id, again.probe_id);
    assert_eq!(first.status, ProbeStatus::Queued);
    let other = submit(&services, admin(), project, submit_of(ProbeKind::Metadata))
        .await
        .unwrap();
    assert_ne!(first.probe_id, other.probe_id);

    dispatch(&services).await;
    let requests = sent_requests(&services).await;
    assert_eq!(requests.len(), 2);
    let check = requests.iter().find(|r| r["kind"] == "check").unwrap();
    assert_eq!(check["probe_id"], first.probe_id.to_string());
    assert_eq!(
        read_probe(&services, project, first.probe_id).await.status,
        ProbeStatus::Running
    );

    dispatch(&services).await;
    assert_eq!(
        sent_requests(&services).await.len(),
        2,
        "a running probe is not sent twice"
    );
}

/// A submission whose snapshot predates the commit of an identical probe (a
/// scheduled refresh, another replica's submission) joins that probe instead
/// of failing on the active-probe unique index.
#[td_test::test(db)]
async fn a_submission_racing_an_identical_insert_joins_it(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let mut refresh = db.begin().await.unwrap();
    let connection_id: String = sqlx::query_scalar("SELECT connection_id FROM connection_catalogs")
        .fetch_one(&mut *refresh)
        .await
        .unwrap();
    let connection_id = ConnectionId::try_from(connection_id.as_str()).unwrap();
    let params = ProbeParams::default();
    let key = dedupe_key(
        &connection_id,
        ProbeKind::Metadata,
        &params,
        &UserId::system(),
    )
    .unwrap();
    let racing = ProbeId::default();
    sqlx::query(
        "INSERT INTO connection_probes \
         (id, connection_id, project_id, probe_kind, params, dedupe_key, status, requested_by_id) \
         VALUES ($1, $2, $3, 'metadata', $4::jsonb, $5, 'queued', $6)",
    )
    .bind(racing.to_string())
    .bind(connection_id.to_string())
    .bind(project.to_string())
    .bind(serde_json::to_string(&params).unwrap())
    .bind(key.to_string())
    .bind(UserId::system().to_string())
    .execute(&mut *refresh)
    .await
    .unwrap();

    let submitting = tokio::spawn(async move {
        submit(&services, admin(), project, submit_of(ProbeKind::Metadata)).await
    });
    let blocked = "SELECT EXISTS (SELECT 1 FROM pg_locks \
                   WHERE NOT granted AND pg_backend_pid() = ANY (pg_blocking_pids(pid)))";
    while !sqlx::query_scalar::<_, bool>(blocked)
        .fetch_one(&mut *refresh)
        .await
        .unwrap()
    {
        assert!(
            !submitting.is_finished(),
            "the submission waits on the uncommitted probe"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    refresh.commit().await.unwrap();

    let joined = submitting.await.unwrap().unwrap();
    assert_eq!(joined.probe_id, racing);
    assert_eq!(joined.status, ProbeStatus::Queued);
}

#[td_test::test(db)]
async fn a_check_outcome_lands_in_the_catalog(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let catalog = || {
        services.read_connection_catalog(admin().read(collection_param("crm")).in_project(project))
    };
    let before = catalog().await.unwrap().data;
    assert_eq!(before.check_status, CheckStatus::Unknown);
    assert_eq!(before.metadata_status, MetadataStatus::None);
    let revision = text_revision(&db).await;

    let probe = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&services).await;
    report(
        &services,
        probe.probe_id,
        serde_json::json!({"probe_id": probe.probe_id.to_string(), "kind": "check",
                           "status": "succeeded", "result": {"ok": false, "error": "refused"}}),
    )
    .await;

    let read = read_probe(&services, project, probe.probe_id).await;
    assert_eq!(read.status, ProbeStatus::Succeeded);
    assert_eq!(read.result.unwrap()["ok"], false);
    let after = catalog().await.unwrap().data;
    assert_eq!(after.check_status, CheckStatus::Error);
    assert_eq!(after.check_error.unwrap().to_string(), "refused");
    assert_eq!(after.last_probe_id, Some(probe.probe_id));
    assert_eq!(
        text_revision(&db).await,
        revision,
        "a check leaves the texts alone"
    );

    report(
        &services,
        probe.probe_id,
        serde_json::json!({"probe_id": probe.probe_id.to_string(), "kind": "check",
                           "status": "succeeded", "result": {"ok": true}}),
    )
    .await;
    assert_eq!(
        catalog().await.unwrap().data.check_status,
        CheckStatus::Error,
        "a late outcome is dropped"
    );
}

#[td_test::test(db)]
async fn a_metadata_probe_replaces_entities_unless_the_fingerprint_holds(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let entities = |fields: bool| {
        services.list_connection_entities(
            admin()
                .list(
                    (
                        collection_param("crm"),
                        EntityListParams {
                            fields: fields.into(),
                        },
                    ),
                    ListParams::default(),
                )
                .in_project(project),
        )
    };

    let created = text_revision(&db).await;
    refresh(
        &services,
        project,
        HASH_A,
        vec![
            entity("crm.public.invoices", "id amount"),
            entity("crm.public.accounts", "id name"),
        ],
    )
    .await;
    let explored = text_revision(&db).await;
    assert!(explored > created, "the first entities change the texts");
    let listed = entities(false).await.unwrap().data;
    assert_eq!(listed.len, 2);
    assert!(listed.data.iter().all(|e| e.doc.is_none()));
    assert!(
        entities(true)
            .await
            .unwrap()
            .data
            .data
            .iter()
            .all(|e| e.doc.is_some())
    );

    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.other", "x")],
    )
    .await;
    let catalog = services
        .read_connection_catalog(admin().read(collection_param("crm")).in_project(project))
        .await
        .unwrap()
        .data;
    assert_eq!(*catalog.entity_count, 2);
    assert_eq!(
        entities(false).await.unwrap().data.len,
        2,
        "an unchanged fingerprint keeps the rows"
    );
    assert_eq!(
        text_revision(&db).await,
        explored,
        "an unchanged fingerprint keeps the texts"
    );

    refresh(
        &services,
        project,
        HASH_B,
        vec![entity("crm.public.other", "x")],
    )
    .await;
    let listed = entities(false).await.unwrap().data;
    assert_eq!(listed.len, 1);
    assert_eq!(listed.data[0].fqn.to_string(), "crm.public.other");
    assert!(text_revision(&db).await > explored);
}

#[td_test::test(db)]
async fn a_sample_resolves_its_entity_from_the_catalog(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let sample = || ProbeSubmit {
        kind: ProbeKind::Sample,
        entity: Some(EntityFqn::try_from("crm.public.invoices").unwrap()),
        entities: None,
        limit: Some(rows(5)),
    };

    let err = submit(&services, admin(), project, sample())
        .await
        .unwrap_err();
    assert_eq!(err.api_error(), ApiError::NotFound);
    assert!(err.to_string().contains("entity_not_in_catalog"), "{err}");
    assert!(
        serde_json::from_value::<ProbeSubmit>(serde_json::json!({
            "kind": "sample",
            "entity": "crm.public.invoices",
            "limit": 101,
        }))
        .is_err()
    );

    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.invoices", "id amount")],
    )
    .await;
    let probe = submit(&services, admin(), project, sample()).await.unwrap();
    dispatch(&services).await;
    let request = sent_requests(&services)
        .await
        .into_iter()
        .find(|r| r["kind"] == "sample")
        .unwrap();
    assert_eq!(request["params"]["entity"], "crm.public.invoices");
    assert_eq!(request["params"]["limit"], 5);
    assert_eq!(request["entity_doc"]["name"], "crm.public.invoices");

    report(
        &services,
        probe.probe_id,
        serde_json::json!({"probe_id": probe.probe_id.to_string(), "kind": "sample", "status": "succeeded",
                           "result": {"entity": "crm.public.invoices", "columns": ["id"], "rows": [[1], [2]],
                                      "truncated": false}}),
    )
    .await;
    let read = read_probe(&services, project, probe.probe_id).await;
    assert_eq!(read.result.unwrap()["rows"].as_array().unwrap().len(), 2);
    assert!(read.expires_on.is_some());
}

#[td_test::test(db)]
async fn a_sample_of_several_entities_sends_each_doc_and_keeps_each_sample(db: TdPool) {
    let project = project_with_source(&db).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let fqns = |names: &[&str]| {
        names
            .iter()
            .map(|n| EntityFqn::try_from(*n).unwrap())
            .collect::<Vec<_>>()
    };
    let batch = |entities: Vec<EntityFqn>| ProbeSubmit {
        kind: ProbeKind::Sample,
        entity: None,
        entities: Some(entities),
        limit: Some(rows(5)),
    };
    refresh(
        &services,
        project,
        HASH_A,
        vec![
            entity("crm.public.invoices", "id amount"),
            entity("crm.public.orders", "id total"),
        ],
    )
    .await;

    for refused in [
        batch(Vec::new()),
        batch(fqns(&["crm.public.invoices"; 11])),
        ProbeSubmit {
            entity: Some(EntityFqn::try_from("crm.public.orders").unwrap()),
            ..batch(fqns(&["crm.public.invoices"]))
        },
    ] {
        assert_eq!(
            submit(&services, admin(), project, refused)
                .await
                .unwrap_err()
                .api_error(),
            ApiError::InputError
        );
    }
    let missing = submit(
        &services,
        admin(),
        project,
        batch(fqns(&["crm.public.invoices", "crm.public.gone"])),
    )
    .await
    .unwrap_err();
    assert!(
        missing.to_string().contains("entity_not_in_catalog"),
        "{missing}"
    );

    let probe = submit(
        &services,
        delegated(dev),
        project,
        batch(fqns(&[
            "crm.public.orders",
            "crm.public.invoices",
            "crm.public.orders",
        ])),
    )
    .await
    .unwrap();
    dispatch(&services).await;
    let request = sent_requests(&services)
        .await
        .into_iter()
        .find(|r| r["kind"] == "sample")
        .unwrap();
    assert_eq!(
        request["params"]["entities"],
        serde_json::json!(["crm.public.invoices", "crm.public.orders"])
    );
    let docs: Vec<_> = request["entity_docs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(docs, ["crm.public.invoices", "crm.public.orders"]);
    assert!(request.get("entity_doc").is_none());

    report(
        &services,
        probe.probe_id,
        serde_json::json!({"probe_id": probe.probe_id.to_string(), "kind": "sample", "status": "succeeded",
                           "result": {"samples": [
                               {"entity": "crm.public.invoices", "columns": ["id"], "rows": [[1], [2]], "truncated": false},
                               {"entity": "crm.public.orders", "error": "boom"}]}}),
    )
    .await;
    let read = services
        .read_connection_probe(
            delegated(dev)
                .read((
                    ProbeParam {
                        probe: probe.probe_id,
                    },
                    ProbeWaitParams { wait: None },
                ))
                .in_project(project),
        )
        .await
        .unwrap()
        .data;
    let samples = read.result.unwrap()["samples"].as_array().unwrap().clone();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(samples[1]["error"], "boom");
    assert!(read.expires_on.is_some());
}

#[td_test::test(db)]
async fn only_the_submitter_or_a_manager_cancels(db: TdPool) {
    let project = project_with_source(&db).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let probe = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    let cancel = |ctx: RequestContext| {
        services.cancel_connection_probe(
            ctx.update(
                ProbeParam {
                    probe: probe.probe_id,
                },
                (),
            )
            .in_project(project),
        )
    };
    assert_eq!(
        cancel(as_user(dev)).await.unwrap_err().api_error(),
        ApiError::Forbidden
    );
    assert_eq!(
        cancel(admin()).await.unwrap().data.status,
        ProbeStatus::Cancelled
    );
    assert_eq!(
        cancel(admin()).await.unwrap_err().api_error(),
        ApiError::Any(409)
    );

    let own = submit(
        &services,
        as_user(dev),
        project,
        submit_of(ProbeKind::Check),
    )
    .await
    .unwrap();
    assert_ne!(
        own.probe_id, probe.probe_id,
        "a cancelled probe is not joined"
    );
    let cancelled = services
        .cancel_connection_probe(
            as_user(dev)
                .update(
                    ProbeParam {
                        probe: own.probe_id,
                    },
                    (),
                )
                .in_project(project),
        )
        .await
        .unwrap()
        .data;
    assert_eq!(cancelled.status, ProbeStatus::Cancelled);
}

#[td_test::test(db)]
async fn the_supervisor_refuses_probes_and_overdue_ones_time_out(db: TdPool) {
    let project = project_with_source(&db).await;
    let supervisor = services(&db, Orchestrator::Supervisor);
    let refused = submit(&supervisor, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&supervisor).await;
    let read = read_probe(&supervisor, project, refused.probe_id).await;
    assert_eq!(read.status, ProbeStatus::Failed);
    assert_eq!(read.error.unwrap()["code"], "unsupported");

    let services = services(&db, Orchestrator::Kubernetes);
    let stuck = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&services).await;
    db.execute_batch(&[format!(
        "UPDATE connection_probes SET started_on = now() - interval '1 hour' WHERE id = '{}'",
        stuck.probe_id
    )])
    .await
    .unwrap();
    dispatch(&services).await;
    assert_eq!(
        read_probe(&services, project, stuck.probe_id).await.status,
        ProbeStatus::Timeout
    );
}

#[td_test::test(db)]
async fn a_wait_returns_once_the_probe_ends(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let probe = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&services).await;
    let waiter = services.read_connection_probe(
        admin()
            .read((
                ProbeParam {
                    probe: probe.probe_id,
                },
                ProbeWaitParams {
                    wait: Some(ProbeWaitSecs::try_from(30).unwrap()),
                },
            ))
            .in_project(project),
    );
    let reporter = async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        report(
            &services,
            probe.probe_id,
            serde_json::json!({"probe_id": probe.probe_id.to_string(), "kind": "check",
                               "status": "succeeded", "result": {"ok": true}}),
        )
        .await;
    };
    let started = std::time::Instant::now();
    let (read, ()) = tokio::join!(waiter, reporter);
    assert_eq!(read.unwrap().data.status, ProbeStatus::Succeeded);
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}

fn probe_in(project: ProjectId) -> ConnectionProbeDB {
    use td_objects::dxo::connection_catalog::ConnectionProbeInsertDB;
    use td_objects::types::basic::{ConnectionId, ProbeDedupeKey, ProbeParams, ProbeRequestedOn};
    use td_objects::types::json::Json;
    ConnectionProbeDB {
        base: ConnectionProbeInsertDB {
            id: ProbeId::default(),
            connection_id: ConnectionId::default(),
            probe_kind: ProbeKind::Check,
            params: Json(ProbeParams::default()),
            dedupe_key: ProbeDedupeKey::try_from("0".repeat(64)).unwrap(),
            status: ProbeStatus::Queued,
            requested_by_id: UserId::admin(),
            requested_on: ProbeRequestedOn::try_from(td_objects::types::basic::AtTime::now())
                .unwrap(),
        },
        project_id: project,
        error: None,
        result: None,
        result_expires: None,
        started_on: None,
        dispatched_on: None,
        ended_on: None,
    }
}

#[test]
fn claims_respect_the_instance_and_project_caps() {
    let (a, b) = (ProjectId::default(), ProjectId::default());
    let running = vec![probe_in(a)];
    let queued = vec![probe_in(a), probe_in(a), probe_in(b), probe_in(b)];
    let expected = [queued[0].base.id, queued[2].base.id];
    let chosen = within_caps(3, 2, &running, queued);
    assert_eq!(
        chosen.iter().map(|p| p.base.id).collect::<Vec<_>>(),
        expected,
        "a's second slot is taken by its running probe; the instance cap stops at three"
    );
}

fn services_with(db: &TdPool, catalog: ConnectionCatalog) -> Probing {
    let base = services(db, Orchestrator::Kubernetes);
    Probing {
        services: Services {
            connection_catalog: Ctx::new(catalog),
            ..base.services
        },
        sent: base.sent,
    }
}

async fn submit_to(
    services: &Services,
    ctx: RequestContext,
    project: ProjectId,
    collection: &str,
    probe: ProbeSubmit,
) -> Result<ProbeRead, td_error::TdError> {
    services
        .submit_connection_probe(
            ctx.create(collection_param(collection), probe)
                .in_project(project),
        )
        .await
        .map(|response| response.data)
}

async fn read_as(
    services: &Services,
    ctx: RequestContext,
    project: ProjectId,
    probe: ProbeId,
) -> ProbeRead {
    services
        .read_connection_probe(
            ctx.read((ProbeParam { probe }, ProbeWaitParams { wait: None }))
                .in_project(project),
        )
        .await
        .unwrap()
        .data
}

fn sample_of(fqn: &str) -> ProbeSubmit {
    ProbeSubmit {
        kind: ProbeKind::Sample,
        entity: Some(EntityFqn::try_from(fqn).unwrap()),
        entities: None,
        limit: Some(rows(5)),
    }
}

async fn report_sample(services: &Services, probe: ProbeId) {
    report(
        services,
        probe,
        serde_json::json!({"probe_id": probe.to_string(), "kind": "sample", "status": "succeeded",
                           "result": {"entity": "crm.public.invoices", "columns": ["id"],
                                      "rows": [[1], [2]], "truncated": false}}),
    )
    .await;
}

async fn probe_status(db: &TdPool, probe: ProbeId) -> Option<String> {
    let mut conn = db.begin().await.unwrap();
    sqlx::query_scalar::<_, String>("SELECT status FROM connection_probes WHERE id = $1")
        .bind(probe.to_string())
        .fetch_optional(&mut *conn)
        .await
        .unwrap()
}

fn delegated(user: UserId) -> RequestContext {
    RequestContext::service_token(AccessTokenId::default(), user, TokenScope::Delegated)
}

/// Collects the `tabsdata::audit` events emitted on this thread.
#[derive(Clone, Default)]
struct AuditLog(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

thread_local! {
    static AUDIT_SINK: std::cell::RefCell<Option<AuditLog>> = const { std::cell::RefCell::new(None) };
}

/// The process default subscriber, handing each audit event to the log of
/// the thread that emitted it. A scoped default would race other tests'
/// threads over each callsite's cached interest and lose events.
struct AuditSink;

impl AuditLog {
    fn capture() -> Self {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            tracing::subscriber::set_global_default(AuditSink)
                .expect("no other global subscriber in these tests");
        });
        let log = Self::default();
        AUDIT_SINK.with(|sink| *sink.borrow_mut() = Some(log.clone()));
        log
    }

    fn events(&self, event: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|fields| fields.get("event").map(String::as_str) == Some(event))
            .cloned()
            .collect()
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for AuditSink {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "tabsdata::audit"
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        AUDIT_SINK.with(|sink| {
            if let Some(log) = sink.borrow().as_ref() {
                log.0.lock().unwrap().push(fields);
            }
        });
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[td_test::test(db)]
async fn a_user_with_probes_can_still_be_deleted(db: TdPool) {
    let project = project_with_source(&db).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let probe = submit(
        &services,
        as_user(dev),
        project,
        submit_of(ProbeKind::Check),
    )
    .await
    .unwrap();

    services
        .delete_user(admin().delete(UserParam {
            user: UserIdName::from_str("test_dev_t").unwrap(),
        }))
        .await
        .unwrap();
    assert_eq!(
        read_probe(&services, project, probe.probe_id)
            .await
            .requested_by_id,
        dev,
        "the probe keeps its requester's id"
    );
}

#[td_test::test(db)]
async fn sample_rows_reach_only_their_requester_and_every_request_is_audited(db: TdPool) {
    let project = project_with_source(&db).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services(&db, Orchestrator::Kubernetes);
    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.invoices", "id amount")],
    )
    .await;
    let audit = AuditLog::capture();

    let mine = submit(
        &services,
        admin(),
        project,
        sample_of("crm.public.invoices"),
    )
    .await
    .unwrap();
    let again = submit(
        &services,
        admin(),
        project,
        sample_of("crm.public.invoices"),
    )
    .await
    .unwrap();
    assert_eq!(
        mine.probe_id, again.probe_id,
        "the requester's own repeat joins"
    );
    let theirs = submit(
        &services,
        as_user(dev),
        project,
        sample_of("crm.public.invoices"),
    )
    .await
    .unwrap();
    assert_ne!(
        mine.probe_id, theirs.probe_id,
        "another requester's sample never joins, so its rows are its own"
    );
    let requested = audit.events("connection_sample_requested");
    assert_eq!(requested.len(), 3, "a joined request is audited too");
    assert_eq!(requested[1]["joined"], "true");
    assert_eq!(requested[1]["user_id"], UserId::admin().to_string());
    assert_eq!(requested[2]["user_id"], dev.to_string());

    dispatch(&services).await;
    report_sample(&services, mine.probe_id).await;
    let served = audit.events("connection_sample_served");
    assert_eq!(served.len(), 1);
    assert_eq!(served[0]["user_id"], UserId::admin().to_string());
    assert_eq!(served[0]["rows"], "2");

    let own = read_as(&services, admin(), project, mine.probe_id).await;
    assert_eq!(own.result.unwrap()["rows"].as_array().unwrap().len(), 2);
    let other = read_as(&services, as_user(dev), project, mine.probe_id).await;
    assert_eq!(other.status, ProbeStatus::Succeeded);
    assert!(other.result.is_none() && other.expires_on.is_none());
}

#[td_test::test(db)]
async fn an_expired_sample_is_hidden_then_purged(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.invoices", "id amount")],
    )
    .await;
    let probe = submit(
        &services,
        admin(),
        project,
        sample_of("crm.public.invoices"),
    )
    .await
    .unwrap();
    dispatch(&services).await;
    report_sample(&services, probe.probe_id).await;
    db.execute_batch(&[format!(
        "UPDATE connection_probes SET result_expires = now() - interval '1 second' WHERE id = '{}'",
        probe.probe_id
    )])
    .await
    .unwrap();
    assert!(
        read_probe(&services, project, probe.probe_id)
            .await
            .result
            .is_none(),
        "past its expiry a sample shows no rows, purged or not"
    );

    dispatch(&services).await;
    let mut conn = db.begin().await.unwrap();
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT result FROM connection_probes WHERE id = $1")
            .bind(probe.probe_id.to_string())
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(stored.is_none(), "the tick purges the rows");
}

#[td_test::test(db)]
async fn concurrent_ticks_send_each_probe_once(db: TdPool) {
    let project = project_with_source(&db).await;
    create_collection(&db, project, "erp", Some(GroupName::source())).await;
    let services = services(&db, Orchestrator::Kubernetes);
    for collection in ["crm", "erp"] {
        submit_to(
            &services,
            admin(),
            project,
            collection,
            submit_of(ProbeKind::Check),
        )
        .await
        .unwrap();
    }
    let (a, b) = tokio::join!(dispatch(&services), dispatch(&services));
    let ((), ()) = (a, b);
    let sent = sent_requests(&services).await;
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert_ne!(sent[0]["probe_id"], sent[1]["probe_id"]);
}

#[td_test::test(db)]
async fn the_queue_cap_answers_429(db: TdPool) {
    let project = project_with_source(&db).await;
    create_collection(&db, project, "erp", Some(GroupName::source())).await;
    let services = services_with(
        &db,
        ConnectionCatalog {
            limiter: SubmitLimiter::new(u32::MAX),
            ..ConnectionCatalog::default()
        },
    );
    refresh(
        &services,
        project,
        HASH_A,
        vec![entity("crm.public.a", "x")],
    )
    .await;
    for limit in 1..=MAX_QUEUED_PER_PROJECT as i32 {
        let sample = ProbeSubmit {
            kind: ProbeKind::Sample,
            entity: Some(EntityFqn::try_from("crm.public.a").unwrap()),
            entities: None,
            limit: Some(rows(limit)),
        };
        submit_to(&services, admin(), project, "crm", sample)
            .await
            .unwrap();
    }
    let full = submit_to(
        &services,
        admin(),
        project,
        "erp",
        submit_of(ProbeKind::Check),
    )
    .await
    .unwrap_err();
    assert_eq!(full.api_error(), ApiError::Any(429));
    assert!(full.to_string().contains("queued"), "{full}");
    assert_eq!(full.retry_after(), None);

    dispatch(&services).await;
    submit_to(
        &services,
        admin(),
        project,
        "erp",
        submit_of(ProbeKind::Check),
    )
    .await
    .unwrap();
}

#[td_test::test(db)]
async fn the_submit_rate_answers_429(db: TdPool) {
    let project = project_with_source(&db).await;
    create_collection(&db, project, "erp", Some(GroupName::source())).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services_with(
        &db,
        ConnectionCatalog {
            limiter: SubmitLimiter::new(2),
            ..ConnectionCatalog::default()
        },
    );
    for collection in ["crm", "erp"] {
        submit_to(
            &services,
            admin(),
            project,
            collection,
            submit_of(ProbeKind::Check),
        )
        .await
        .unwrap();
    }
    dispatch(&services).await;
    let limited = submit_to(
        &services,
        admin(),
        project,
        "crm",
        submit_of(ProbeKind::Metadata),
    )
    .await
    .unwrap_err();
    assert_eq!(limited.api_error(), ApiError::Any(429));
    assert!(limited.to_string().contains("retry after"), "{limited}");
    let retry_after = limited.retry_after().expect("the limiter knows the wait");
    assert!(
        retry_after > Duration::ZERO && retry_after <= Duration::from_secs(60),
        "{retry_after:?}"
    );
    submit_to(
        &services,
        as_user(dev),
        project,
        "crm",
        submit_of(ProbeKind::Metadata),
    )
    .await
    .expect("the rate is per user");
}

#[td_test::test(db)]
async fn an_over_long_error_is_cut_and_still_ends_the_probe(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let long = "e".repeat(10 * 1024);
    for (kind, context) in [
        (
            ProbeKind::Check,
            serde_json::json!({"status": "succeeded", "result": {"ok": false, "error": long}}),
        ),
        (
            ProbeKind::Metadata,
            serde_json::json!({"status": "failed", "error": {"code": "metadata_failed", "message": long}}),
        ),
    ] {
        let probe = submit(&services, admin(), project, submit_of(kind))
            .await
            .unwrap();
        dispatch(&services).await;
        report(&services, probe.probe_id, context).await;
        assert!(
            read_probe(&services, project, probe.probe_id)
                .await
                .status
                .is_terminal()
        );
    }
    let catalog = services
        .read_connection_catalog(admin().read(collection_param("crm")).in_project(project))
        .await
        .unwrap()
        .data;
    assert_eq!(catalog.check_error.unwrap().len(), ProbeError::MAX_BYTES);
    assert_eq!(catalog.metadata_error.unwrap().len(), ProbeError::MAX_BYTES);
}

#[td_test::test(db)]
async fn ended_probes_past_the_retention_are_deleted(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let old = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&services).await;
    report(
        &services,
        old.probe_id,
        serde_json::json!({"status": "succeeded", "result": {"ok": true}}),
    )
    .await;
    let recent = submit(&services, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&services).await;
    report(
        &services,
        recent.probe_id,
        serde_json::json!({"status": "succeeded", "result": {"ok": true}}),
    )
    .await;
    db.execute_batch(&[format!(
        "UPDATE connection_probes SET ended_on = now() - interval '8 days' WHERE id = '{}'",
        old.probe_id
    )])
    .await
    .unwrap();

    dispatch(&services).await;
    assert_eq!(probe_status(&db, old.probe_id).await, None);
    assert_eq!(
        probe_status(&db, recent.probe_id).await.as_deref(),
        Some("succeeded")
    );
}

struct UnreachableQueue;

#[async_trait::async_trait]
impl OrchestratorMessageQueue for UnreachableQueue {
    async fn put(
        &self,
        _id: String,
        _payload: OrchestratorMessagePayload<serde_yaml::Value>,
    ) -> Result<OrchestratorMessage<serde_yaml::Value>, QueueError> {
        Err(QueueError::BackendError {
            cause: "unreachable".to_owned(),
        })
    }
}

#[td_test::test(db)]
async fn an_undelivered_claim_goes_back_to_the_queue(db: TdPool) {
    let project = project_with_source(&db).await;
    let unreachable = Services {
        bridge: Ctx::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default()
                .with_orchestrator(Orchestrator::Kubernetes)
                .with_message_queue(UnreachableQueue),
        ))),
        ..dispatch_services(&db)
    };
    let probe = submit(&unreachable, admin(), project, submit_of(ProbeKind::Check))
        .await
        .unwrap();
    dispatch(&unreachable).await;
    assert_eq!(
        read_probe(&unreachable, project, probe.probe_id)
            .await
            .status,
        ProbeStatus::Queued,
        "a failed send is retried, not failed"
    );

    let services = services(&db, Orchestrator::Kubernetes);
    dispatch(&services).await;
    assert_eq!(sent_requests(&services).await.len(), 1);
    let delivered = |db: TdPool| async move {
        let mut conn = db.begin().await.unwrap();
        sqlx::query_scalar::<_, bool>(
            "SELECT dispatched_on IS NOT NULL FROM connection_probes WHERE id = $1",
        )
        .bind(probe.probe_id.to_string())
        .fetch_one(&mut *conn)
        .await
        .unwrap()
    };
    assert!(delivered(db.clone()).await);

    db.execute_batch(&[format!(
        "UPDATE connection_probes SET dispatched_on = NULL, started_on = now() - interval '2 minutes' \
         WHERE id = '{}'",
        probe.probe_id
    )])
    .await
    .unwrap();
    dispatch(&services).await;
    assert_eq!(
        sent_requests(&services).await.len(),
        2,
        "a claim left undelivered past the grace is claimed and sent again"
    );
    assert!(delivered(db.clone()).await);
    assert_eq!(
        read_probe(&services, project, probe.probe_id).await.status,
        ProbeStatus::Running
    );
}

#[td_test::test(db)]
async fn metadata_is_sent_the_entity_caps_and_keeps_its_entities_by_fqn(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let probe = submit(&services, admin(), project, submit_of(ProbeKind::Metadata))
        .await
        .unwrap();
    dispatch(&services).await;
    let request = &sent_requests(&services).await[0];
    assert_eq!(request["max_entities"], MAX_ENTITIES);
    assert_eq!(request["max_entities_bytes"], MAX_ENTITIES_BYTES);
    report(
        &services,
        probe.probe_id,
        serde_json::json!({"status": "succeeded", "result": {
            "schema_hash": HASH_A, "truncated": true,
            "entities": [entity("crm.public.b", "x"), entity("crm.public.a", "x")]}}),
    )
    .await;
    let catalog = services
        .read_connection_catalog(admin().read(collection_param("crm")).in_project(project))
        .await
        .unwrap()
        .data;
    assert_eq!(*catalog.entity_count, 2);
    assert!(*catalog.truncated);
    let listed = services
        .list_connection_entities(
            admin()
                .list(
                    (
                        collection_param("crm"),
                        EntityListParams {
                            fields: IncludeFields::from(false),
                        },
                    ),
                    ListParams::default(),
                )
                .in_project(project),
        )
        .await
        .unwrap()
        .data;
    let fqns: Vec<_> = listed.data.iter().map(|e| e.fqn.to_string()).collect();
    assert_eq!(fqns, ["crm.public.a", "crm.public.b"]);
}

#[td_test::test(db)]
async fn a_delegated_token_develops_but_does_not_manage(db: TdPool) {
    let project = project_with_source(&db).await;
    let dev = seed_dev_user(&db, project).await;
    let services = services(&db, Orchestrator::Kubernetes);
    let own = submit(
        &services,
        delegated(dev),
        project,
        submit_of(ProbeKind::Check),
    )
    .await
    .unwrap();
    let theirs = submit(&services, admin(), project, submit_of(ProbeKind::Metadata))
        .await
        .unwrap();
    assert_ne!(own.probe_id, theirs.probe_id);
    assert_eq!(
        services
            .cancel_connection_probe(
                delegated(dev)
                    .update(
                        ProbeParam {
                            probe: theirs.probe_id,
                        },
                        (),
                    )
                    .in_project(project),
            )
            .await
            .unwrap_err()
            .api_error(),
        ApiError::Forbidden
    );
}

fn configured(db: &TdPool, config: CatalogConfig) -> Probing {
    let base = services(db, Orchestrator::Kubernetes);
    Probing {
        services: Services {
            catalog_config: Ctx::new(config),
            ..base.services
        },
        sent: base.sent,
    }
}

fn refreshing(db: &TdPool, interval: std::time::Duration) -> Probing {
    configured(
        db,
        CatalogConfig {
            refresh_interval: Some(interval),
            ..CatalogConfig::default()
        },
    )
}

async fn requesters(db: &TdPool) -> Vec<(String, String, String)> {
    let mut conn = db.begin().await.unwrap();
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT probe_kind, status, requested_by_id FROM connection_probes ORDER BY requested_on",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

/// Explores the connection once, as a user would: its metadata is then
/// what a scheduled refresh keeps fresh.
async fn explored(services: &Probing, project: ProjectId) {
    refresh(services, project, HASH_A, vec![]).await;
}

async fn age(db: &TdPool, by: &str) {
    db.execute_batch(&[
        format!("UPDATE connection_catalogs SET metadata_on = metadata_on - interval '{by}'"),
        format!("UPDATE connection_probes SET requested_on = requested_on - interval '{by}'"),
    ])
    .await
    .unwrap();
}

fn system_requests(requests: &[(String, String, String)]) -> usize {
    requests
        .iter()
        .filter(|(_, _, by)| *by == UserId::system().to_string())
        .count()
}

#[td_test::test(db)]
async fn a_stale_connection_is_refreshed_once_per_interval(db: TdPool) {
    project_with_source(&db).await;
    let services = refreshing(&db, std::time::Duration::from_secs(3600));

    dispatch(&services).await;
    let queued = requesters(&db).await;
    assert_eq!(queued.len(), 1, "a never explored connection is due");
    assert_eq!(queued[0].0, "metadata");
    assert_eq!(system_requests(&queued), 1);
    let sent = sent_requests(&services).await;
    let probe = ProbeId::try_from(sent[0]["probe_id"].as_str().unwrap()).unwrap();

    dispatch(&services).await;
    assert_eq!(
        requesters(&db).await.len(),
        1,
        "a refresh in flight is not queued twice"
    );

    report(
        &services,
        probe,
        serde_json::json!({"probe_id": probe.to_string(), "kind": "metadata", "status": "succeeded",
                           "result": {"schema_hash": HASH_A, "entity_count": 0, "truncated": false,
                                      "entities": []}}),
    )
    .await;
    dispatch(&services).await;
    assert_eq!(requesters(&db).await.len(), 1, "a fresh catalog is not due");

    age(&db, "2 hours").await;
    dispatch(&services).await;
    let queued = requesters(&db).await;
    assert_eq!(queued.len(), 2, "past the interval it is refreshed");
    assert_eq!(system_requests(&queued), 2);

    dispatch(&services).await;
    assert_eq!(
        requesters(&db).await.len(),
        2,
        "a refresh in flight is not queued twice"
    );
}

#[td_test::test(db)]
async fn a_failing_connection_backs_off(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = refreshing(&db, std::time::Duration::from_secs(3600));
    explored(&services, project).await;
    age(&db, "2 hours").await;
    dispatch(&services).await;
    let sent = sent_requests(&services).await;
    let probe = ProbeId::try_from(sent.last().unwrap()["probe_id"].as_str().unwrap()).unwrap();
    report(
        &services,
        probe,
        serde_json::json!({"probe_id": probe.to_string(), "kind": "metadata", "status": "failed",
                           "error": {"code": "metadata_failed", "message": "refused"}}),
    )
    .await;
    assert_eq!(system_requests(&requesters(&db).await), 1);

    age(&db, "90 minutes").await;
    dispatch(&services).await;
    assert_eq!(
        system_requests(&requesters(&db).await),
        1,
        "one failure doubles the wait to two intervals"
    );

    age(&db, "1 hour").await;
    dispatch(&services).await;
    assert_eq!(
        system_requests(&requesters(&db).await),
        2,
        "past the doubled wait it is retried"
    );
}

#[td_test::test(db)]
async fn a_connection_without_metadata_support_is_not_refreshed(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = refreshing(&db, std::time::Duration::from_secs(3600));
    dispatch(&services).await;
    let sent = sent_requests(&services).await;
    let probe = ProbeId::try_from(sent[0]["probe_id"].as_str().unwrap()).unwrap();
    report(
        &services,
        probe,
        serde_json::json!({"probe_id": probe.to_string(), "kind": "metadata", "status": "succeeded",
                           "result": {"supported": false}}),
    )
    .await;

    age(&db, "2 hours").await;
    dispatch(&services).await;
    assert_eq!(system_requests(&requesters(&db).await), 1);

    explored(&services, project).await;
    let catalog = services
        .read_connection_catalog(admin().read(collection_param("crm")).in_project(project))
        .await
        .unwrap()
        .data;
    assert_eq!(
        catalog.metadata_status,
        MetadataStatus::Ok,
        "a user still refreshes it"
    );
}

#[td_test::test(db)]
async fn a_connection_with_an_active_metadata_probe_is_not_refreshed(db: TdPool) {
    let project = project_with_source(&db).await;
    let services = refreshing(&db, std::time::Duration::from_secs(60));
    explored(&services, project).await;
    age(&db, "5 minutes").await;
    submit(&services, admin(), project, submit_of(ProbeKind::Metadata))
        .await
        .unwrap();
    age(&db, "2 minutes").await;

    dispatch(&services).await;
    assert_eq!(
        system_requests(&requesters(&db).await),
        0,
        "the user's probe still covers it"
    );
}

#[test]
fn a_refresh_waits_twice_as_long_after_each_failure_up_to_the_cap() {
    let hour = std::time::Duration::from_secs(3600);
    let cap = hour * 24 * 7;
    let h = 3_600_000_i64;
    assert!(!refresh_due(Some(0), &[], hour, cap, h - 1));
    assert!(refresh_due(Some(0), &[], hour, cap, h));
    assert!(
        !refresh_due(Some(0), &[h], hour, cap, 3 * h - 1),
        "one failure: 2h"
    );
    assert!(refresh_due(Some(0), &[h], hour, cap, 3 * h));
    assert!(
        !refresh_due(Some(0), &[h, 3 * h], hour, cap, 7 * h - 1),
        "two: 4h"
    );
    assert!(refresh_due(Some(0), &[h, 3 * h], hour, cap, 7 * h));
    let many: Vec<i64> = (1..=40).map(|n| n * h).collect();
    assert!(!refresh_due(
        Some(0),
        &many,
        hour,
        cap,
        40 * h + 168 * h - 1
    ));
    assert!(
        refresh_due(Some(0), &many, hour, cap, 40 * h + 168 * h),
        "at most the retention"
    );
    assert!(
        refresh_due(Some(0), &[h], 4 * hour, hour, 5 * h),
        "never less than the interval"
    );
    assert!(
        refresh_due(None, &[], hour, cap, 0),
        "never explored: at once"
    );
    assert!(
        !refresh_due(None, &[0], hour, cap, 2 * h - 1),
        "a never explored connection backs off its failures too"
    );
}

#[td_test::test(db)]
async fn without_an_interval_or_on_the_supervisor_nothing_is_scheduled(db: TdPool) {
    project_with_source(&db).await;
    dispatch(&services(&db, Orchestrator::Kubernetes)).await;
    let supervisor = Services {
        catalog_config: Ctx::new(CatalogConfig {
            refresh_interval: Some(std::time::Duration::from_secs(60)),
            ..CatalogConfig::default()
        }),
        ..services(&db, Orchestrator::Supervisor).services
    };
    dispatch(&supervisor).await;
    assert!(requesters(&db).await.is_empty());
}

#[test]
fn interactive_probes_are_claimed_before_scheduled_refreshes() {
    let project = ProjectId::default();
    let mut scheduled = probe_in(project);
    scheduled.base.requested_by_id = UserId::system();
    let interactive = probe_in(project);
    let expected = interactive.base.id;
    let chosen = within_caps(1, 1, &[], vec![scheduled, interactive]);
    assert_eq!(
        chosen.iter().map(|p| p.base.id).collect::<Vec<_>>(),
        [expected]
    );
}

#[test]
fn maintenance_samples_are_off_unless_configured() {
    let read =
        |yaml: &str| serde_yaml::from_str::<CatalogConfig>(yaml).map(|c| c.maintenance_samples);
    assert!(!read("{}").unwrap());
    assert!(read("maintenance_samples: true").unwrap());
    assert!(read("samples: true").is_err());
}

#[test]
fn the_refresh_interval_reads_durations_and_off() {
    let read = |yaml: &str| serde_yaml::from_str::<CatalogConfig>(yaml).map(|c| c.refresh_interval);
    assert_eq!(
        read("refresh_interval: 24h").unwrap(),
        Some(std::time::Duration::from_secs(86_400))
    );
    assert_eq!(read("refresh_interval: 'off'").unwrap(), None);
    assert_eq!(read("{}").unwrap(), None);
    assert!(read("refresh_interval: 0s").is_err());
    assert!(read("refresh_interval: soon").is_err());
}

mod maintain_catalog;
