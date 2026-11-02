//
// Copyright 2026 Tabsdata Inc.
//

//! The connection catalog: per-connection probe status and fingerprint
//! (`connection_catalogs`), the explored entities (`connection_entities`) and
//! the asynchronous probe jobs (`connection_probes`). The catalog row is
//! written by the first probe result; `connection_catalogs__with_names` shows
//! every connected collection, probed or not.

use crate::types::basic::{
    AtTime, CatalogEntityKind, CatalogSchemaHash, CatalogTruncated, CatalogUpdatedOn, CheckError,
    CheckStatus, CheckedOn, CollectionId, CollectionName, CollectionType, ConnectionEntityId,
    ConnectionId, ConnectionKind, Description, Dispatched, Ended, EntityCount, EntityDoc,
    EntityFqn, Expires, FieldCount, FieldNames, IncludeFields, MetadataError, MetadataOn,
    MetadataStatus, ProbeDedupeKey, ProbeError, ProbeId, ProbeKind, ProbeParams, ProbeRequestedOn,
    ProbeResult, ProbeStatus, ProbeWaitSecs, ProjectId, ProjectName, SampleRows, Started, TagValue,
    TextRevision, UserId, UserTagKey,
};
use crate::types::json::Json;
use sea_query::{Expr, ExprTrait};
use std::collections::BTreeMap;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{FromDao, HasColumn, SeaValue, and, or};
use td_common::execution_status::WorkerCallbackStatus;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "connection_catalogs", ops(select), project_scoped)]
pub struct ConnectionCatalogDB {
    #[dao(pk)]
    pub connection_id: ConnectionId,
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub kind: ConnectionKind,
    pub check_status: CheckStatus,
    pub check_error: Option<CheckError>,
    pub checked_on: Option<CheckedOn>,
    pub schema_hash: Option<CatalogSchemaHash>,
    pub entity_count: EntityCount,
    pub truncated: CatalogTruncated,
    pub metadata_on: Option<MetadataOn>,
    pub metadata_status: MetadataStatus,
    pub metadata_error: Option<MetadataError>,
    pub last_probe_id: Option<ProbeId>,
    pub updated_on: CatalogUpdatedOn,
    pub text_revision: TextRevision,
}

/// A check outcome, creating the catalog row on the first probe.
#[ta_objects::Dao]
#[dao(
    table = "connection_catalogs",
    ops(insert, upsert(ConnectionId)),
    project_scoped
)]
pub struct CatalogCheckWriteDB {
    #[dao(pk)]
    pub connection_id: ConnectionId,
    pub collection_id: CollectionId,
    pub kind: ConnectionKind,
    pub check_status: CheckStatus,
    pub check_error: Option<CheckError>,
    pub checked_on: CheckedOn,
    pub last_probe_id: ProbeId,
    pub updated_on: CatalogUpdatedOn,
}

/// A successful metadata probe: it connected, so it also stamps the check.
#[ta_objects::Dao]
#[dao(
    table = "connection_catalogs",
    ops(insert, upsert(ConnectionId)),
    project_scoped
)]
pub struct CatalogMetadataWriteDB {
    #[dao(pk)]
    pub connection_id: ConnectionId,
    pub collection_id: CollectionId,
    pub kind: ConnectionKind,
    pub check_status: CheckStatus,
    pub check_error: Option<CheckError>,
    pub checked_on: CheckedOn,
    pub schema_hash: CatalogSchemaHash,
    pub entity_count: EntityCount,
    pub truncated: CatalogTruncated,
    pub metadata_on: MetadataOn,
    pub metadata_status: MetadataStatus,
    pub metadata_error: Option<MetadataError>,
    pub last_probe_id: ProbeId,
    pub updated_on: CatalogUpdatedOn,
}

/// A failed metadata probe keeps the last good entities and fingerprint.
#[ta_objects::Dao]
#[dao(
    table = "connection_catalogs",
    ops(insert, upsert(ConnectionId)),
    project_scoped
)]
pub struct CatalogMetadataFailureWriteDB {
    #[dao(pk)]
    pub connection_id: ConnectionId,
    pub collection_id: CollectionId,
    pub kind: ConnectionKind,
    pub metadata_status: MetadataStatus,
    pub metadata_error: Option<MetadataError>,
    pub last_probe_id: ProbeId,
    pub updated_on: CatalogUpdatedOn,
}

#[ta_objects::Dao]
#[dao(view = "connection_catalogs__with_names", project_scoped)]
pub struct ConnectionCatalogDBWithNames {
    #[dao(pk)]
    pub connection_id: ConnectionId,
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub kind: ConnectionKind,
    pub check_status: CheckStatus,
    pub check_error: Option<CheckError>,
    pub checked_on: Option<CheckedOn>,
    pub schema_hash: Option<CatalogSchemaHash>,
    pub entity_count: EntityCount,
    pub truncated: CatalogTruncated,
    pub metadata_on: Option<MetadataOn>,
    pub metadata_status: MetadataStatus,
    pub metadata_error: Option<MetadataError>,
    pub last_probe_id: Option<ProbeId>,
    pub text_revision: TextRevision,
    pub collection: CollectionName,
    pub collection_type: CollectionType,
    pub project: ProjectName,
}

#[ta_objects::Dao]
#[dao(table = "connection_entities", ops(insert), project_scoped)]
pub struct ConnectionEntityInsertDB {
    #[dao(pk)]
    pub id: ConnectionEntityId,
    pub connection_id: ConnectionId,
    #[dao(order_by)]
    pub fqn: EntityFqn,
    pub entity_kind: CatalogEntityKind,
    pub field_names: FieldNames,
    pub field_count: FieldCount,
    pub doc: Json<EntityDoc>,
}

#[ta_objects::Dao]
#[dao(
    table = "connection_entities",
    ops(select, delete),
    project_scoped(unique(ConnectionId, EntityFqn))
)]
pub struct ConnectionEntityDB {
    #[dao(flatten)]
    pub base: ConnectionEntityInsertDB,
    pub project_id: ProjectId,
}

/// An entity without its document.
#[ta_objects::Dao]
#[dao(table = "connection_entities", ops(select), project_scoped)]
pub struct ConnectionEntitySummaryDB {
    #[dao(pk)]
    pub id: ConnectionEntityId,
    pub connection_id: ConnectionId,
    pub project_id: ProjectId,
    #[dao(order_by)]
    pub fqn: EntityFqn,
    pub entity_kind: CatalogEntityKind,
    pub field_count: FieldCount,
}

/// The entities of a listed page, to fill in their documents.
pub struct EntitiesIn(pub Vec<ConnectionEntityId>);

impl SqlCondition<ConnectionEntityDB> for EntitiesIn {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all().add(
            Expr::col(<ConnectionEntityDB as HasColumn<ConnectionEntityId>>::COL)
                .is_in(self.0.iter().map(|id| id.as_sea_expr())),
        )
    }
}

#[ta_objects::Dao]
#[dao(table = "connection_probes", ops(insert), project_scoped)]
pub struct ConnectionProbeInsertDB {
    #[dao(pk)]
    pub id: ProbeId,
    pub connection_id: ConnectionId,
    pub probe_kind: ProbeKind,
    pub params: Json<ProbeParams>,
    pub dedupe_key: ProbeDedupeKey,
    pub status: ProbeStatus,
    pub requested_by_id: UserId,
    #[dao(order_by)]
    pub requested_on: ProbeRequestedOn,
}

#[ta_objects::Dao]
#[dao(table = "connection_probes", ops(select, delete), project_scoped)]
pub struct ConnectionProbeDB {
    #[dao(flatten)]
    pub base: ConnectionProbeInsertDB,
    pub project_id: ProjectId,
    pub error: Option<Json<ProbeError>>,
    pub result: Option<Json<ProbeResult>>,
    pub result_expires: Option<Expires<AtTime>>,
    pub started_on: Option<Started<AtTime>>,
    pub dispatched_on: Option<Dispatched<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
}

impl ConnectionProbeDB {
    /// The active probe a submission with this key joins.
    pub fn active(dedupe_key: ProbeDedupeKey) -> impl SqlCondition<Self> {
        and!(dedupe_key, or!(ProbeStatus::Queued, ProbeStatus::Running))
    }

    pub fn queued() -> impl SqlCondition<Self> {
        and!(ProbeStatus::Queued)
    }

    pub fn running() -> impl SqlCondition<Self> {
        and!(ProbeStatus::Running)
    }

    pub fn queued_of(probe_id: ProbeId) -> impl SqlCondition<Self> {
        and!(probe_id, ProbeStatus::Queued)
    }

    pub fn running_of(probe_id: ProbeId) -> impl SqlCondition<Self> {
        and!(probe_id, ProbeStatus::Running)
    }

    pub fn active_of(probe_id: ProbeId) -> impl SqlCondition<Self> {
        and!(probe_id, or!(ProbeStatus::Queued, ProbeStatus::Running))
    }
}

/// Running probes of a kind started before the cutoff, with no outcome.
pub struct RunningOverdue {
    pub kind: ProbeKind,
    pub started_before: Started<AtTime>,
}

impl SqlCondition<ConnectionProbeDB> for RunningOverdue {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeStatus>>::COL)
                    .eq(ProbeStatus::Running.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeKind>>::COL)
                    .eq(self.kind.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<Option<Started<AtTime>>>>::COL)
                    .lt(self.started_before.as_sea_expr()),
            )
    }
}

/// Probes claimed before the cutoff whose request was never delivered.
pub struct UndeliveredOverdue {
    pub started_before: Started<AtTime>,
}

impl SqlCondition<ConnectionProbeDB> for UndeliveredOverdue {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeStatus>>::COL)
                    .eq(ProbeStatus::Running.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<Option<Dispatched<AtTime>>>>::COL)
                    .is_null(),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<Option<Started<AtTime>>>>::COL)
                    .lt(self.started_before.as_sea_expr()),
            )
    }
}

/// Ended probes past the retention window.
pub struct EndedBefore {
    pub at: Ended<AtTime>,
}

impl SqlCondition<ConnectionProbeDB> for EndedBefore {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all().add(
            Expr::col(<ConnectionProbeDB as HasColumn<Option<Ended<AtTime>>>>::COL)
                .lt(self.at.as_sea_expr()),
        )
    }
}

/// Probes the caps kept queued since before the cutoff.
pub struct QueuedOverdue {
    pub requested_before: ProbeRequestedOn,
}

impl SqlCondition<ConnectionProbeDB> for QueuedOverdue {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeStatus>>::COL)
                    .eq(ProbeStatus::Queued.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeRequestedOn>>::COL)
                    .lt(self.requested_before.as_sea_expr()),
            )
    }
}

/// Metadata probes requested since the cutoff, whatever became of them: a
/// connection that has one is not due for a scheduled refresh.
pub struct MetadataRequestedSince {
    pub since: ProbeRequestedOn,
}

impl SqlCondition<ConnectionProbeDB> for MetadataRequestedSince {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeKind>>::COL)
                    .eq(ProbeKind::Metadata.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeRequestedOn>>::COL)
                    .gte(self.since.as_sea_expr()),
            )
    }
}

/// Metadata probes requested since the cutoff that failed or timed out: a
/// scheduled refresh backs off from them.
pub struct MetadataFailedSince {
    pub since: ProbeRequestedOn,
}

impl SqlCondition<ConnectionProbeDB> for MetadataFailedSince {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all()
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeKind>>::COL)
                    .eq(ProbeKind::Metadata.as_sea_expr()),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeStatus>>::COL).is_in([
                    ProbeStatus::Failed.as_sea_expr(),
                    ProbeStatus::Timeout.as_sea_expr(),
                ]),
            )
            .add(
                Expr::col(<ConnectionProbeDB as HasColumn<ProbeRequestedOn>>::COL)
                    .gte(self.since.as_sea_expr()),
            )
    }
}

/// Connections never explored, or last explored successfully before the
/// cutoff.
pub struct MetadataDue {
    pub before: MetadataOn,
}

impl SqlCondition<ConnectionCatalogDBWithNames> for MetadataDue {
    fn resolve(&self) -> sea_query::Condition {
        let metadata_on = <ConnectionCatalogDBWithNames as HasColumn<Option<MetadataOn>>>::COL;
        sea_query::Condition::any()
            .add(Expr::col(metadata_on).is_null())
            .add(Expr::col(metadata_on).lt(self.before.as_sea_expr()))
    }
}

/// Sample rows past their time to live.
pub struct SampleResultsExpired {
    pub at: Expires<AtTime>,
}

impl SqlCondition<ConnectionProbeDB> for SampleResultsExpired {
    fn resolve(&self) -> sea_query::Condition {
        sea_query::Condition::all().add(
            Expr::col(<ConnectionProbeDB as HasColumn<Option<Expires<AtTime>>>>::COL)
                .lt(self.at.as_sea_expr()),
        )
    }
}

#[ta_objects::Dao]
#[dao(
    table = "connection_probes",
    ops(update(ConnectionProbeDB)),
    project_scoped
)]
pub struct ProbeStartUpdateDB {
    pub status: ProbeStatus,
    pub started_on: Started<AtTime>,
}

#[ta_objects::Dao]
#[dao(
    table = "connection_probes",
    ops(update(ConnectionProbeDB)),
    project_scoped
)]
pub struct ProbeDeliveredUpdateDB {
    pub dispatched_on: Dispatched<AtTime>,
}

/// A claim whose request never left: back to the queue.
#[ta_objects::Dao]
#[dao(
    table = "connection_probes",
    ops(update(ConnectionProbeDB)),
    project_scoped
)]
pub struct ProbeRequeueUpdateDB {
    pub status: ProbeStatus,
    pub started_on: Option<Option<Started<AtTime>>>,
}

impl Default for ProbeRequeueUpdateDB {
    fn default() -> Self {
        Self {
            status: ProbeStatus::Queued,
            started_on: Some(None),
        }
    }
}

#[ta_objects::Dao]
#[dao(
    table = "connection_probes",
    ops(update(ConnectionProbeDB)),
    project_scoped
)]
pub struct ProbeEndUpdateDB {
    pub status: ProbeStatus,
    pub error: Option<Json<ProbeError>>,
    pub result: Option<Json<ProbeResult>>,
    pub result_expires: Option<Expires<AtTime>>,
    pub ended_on: Ended<AtTime>,
}

#[ta_objects::Dao]
#[dao(
    table = "connection_probes",
    ops(update(ConnectionProbeDB)),
    project_scoped
)]
pub struct ProbeResultPurgeDB {
    pub result: Option<Option<Json<ProbeResult>>>,
    pub result_expires: Option<Option<Expires<AtTime>>>,
}

impl Default for ProbeResultPurgeDB {
    fn default() -> Self {
        Self {
            result: Some(None),
            result_expires: Some(None),
        }
    }
}

#[ta_objects::Dto]
#[dto(list(on = ConnectionCatalogDBWithNames))]
pub struct ConnectionSummary {
    #[dto(list(pagination_by = "+"))]
    pub connection_id: ConnectionId,
    #[dto(list(filter))]
    pub project_id: ProjectId,
    #[dto(list(filter, filter_like, order_by))]
    pub project: ProjectName,
    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub kind: ConnectionKind,
    #[dto(list(filter))]
    pub collection_type: CollectionType,
    #[dto(list(filter, order_by))]
    pub check_status: CheckStatus,
    pub checked_on: Option<CheckedOn>,
    #[dto(list(filter, order_by))]
    pub metadata_status: MetadataStatus,
    pub metadata_on: Option<MetadataOn>,
    pub entity_count: EntityCount,
    pub schema_hash: Option<CatalogSchemaHash>,
    pub truncated: CatalogTruncated,
}

impl FromDao for ConnectionSummary {
    type Dao = ConnectionCatalogDBWithNames;

    fn from_dao(dao: ConnectionCatalogDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            connection_id: dao.connection_id,
            project_id: dao.project_id,
            project: dao.project,
            collection: dao.collection,
            kind: dao.kind,
            collection_type: dao.collection_type,
            check_status: dao.check_status,
            checked_on: dao.checked_on,
            metadata_status: dao.metadata_status,
            metadata_on: dao.metadata_on,
            entity_count: dao.entity_count,
            schema_hash: dao.schema_hash,
            truncated: dao.truncated,
        })
    }
}

#[ta_objects::Dto]
pub struct ConnectionCatalogRead {
    pub connection_id: ConnectionId,
    pub project_id: ProjectId,
    pub project: ProjectName,
    pub collection: CollectionName,
    pub kind: ConnectionKind,
    pub collection_type: CollectionType,
    pub check_status: CheckStatus,
    pub check_error: Option<CheckError>,
    pub checked_on: Option<CheckedOn>,
    pub metadata_status: MetadataStatus,
    pub metadata_error: Option<MetadataError>,
    pub metadata_on: Option<MetadataOn>,
    pub entity_count: EntityCount,
    pub schema_hash: Option<CatalogSchemaHash>,
    pub truncated: CatalogTruncated,
    pub last_probe_id: Option<ProbeId>,
}

impl FromDao for ConnectionCatalogRead {
    type Dao = ConnectionCatalogDBWithNames;

    fn from_dao(dao: ConnectionCatalogDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            connection_id: dao.connection_id,
            project_id: dao.project_id,
            project: dao.project,
            collection: dao.collection,
            kind: dao.kind,
            collection_type: dao.collection_type,
            check_status: dao.check_status,
            check_error: dao.check_error,
            checked_on: dao.checked_on,
            metadata_status: dao.metadata_status,
            metadata_error: dao.metadata_error,
            metadata_on: dao.metadata_on,
            entity_count: dao.entity_count,
            schema_hash: dao.schema_hash,
            truncated: dao.truncated,
            last_probe_id: dao.last_probe_id,
        })
    }
}

/// `doc` is the explorer's entity verbatim, returned only when asked for.
#[ta_objects::Dto]
#[dto(list(on = ConnectionEntitySummaryDB))]
pub struct EntityRead {
    #[dto(list(pagination_by = "+"))]
    pub id: ConnectionEntityId,
    #[dto(list(filter, filter_like, order_by))]
    pub fqn: EntityFqn,
    #[dto(list(filter, order_by))]
    pub entity_kind: CatalogEntityKind,
    pub field_count: FieldCount,
    #[schema(value_type = Option<Object>)]
    pub doc: Option<serde_json::Value>,
}

impl FromDao for EntityRead {
    type Dao = ConnectionEntitySummaryDB;

    fn from_dao(dao: ConnectionEntitySummaryDB) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.id,
            fqn: dao.fqn,
            entity_kind: dao.entity_kind,
            field_count: dao.field_count,
            doc: None,
        })
    }
}

#[ta_objects::QueryParam]
pub struct EntityListParams {
    #[serde(default)]
    pub fields: IncludeFields,
}

#[ta_objects::Dto]
pub struct ProbeSubmit {
    pub kind: ProbeKind,
    #[serde(default)]
    pub entity: Option<EntityFqn>,
    /// A sample of several entities at once, in place of `entity`.
    #[serde(default)]
    pub entities: Option<Vec<EntityFqn>>,
    #[serde(default)]
    pub limit: Option<SampleRows>,
}

#[ta_objects::Dto]
pub struct ProbeRead {
    pub probe_id: ProbeId,
    pub connection_id: ConnectionId,
    pub collection: CollectionName,
    pub kind: ProbeKind,
    pub status: ProbeStatus,
    pub requested_by_id: UserId,
    pub requested_on: ProbeRequestedOn,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    #[schema(value_type = Option<Object>)]
    pub error: Option<serde_json::Value>,
    #[schema(value_type = Option<Object>)]
    pub result: Option<serde_json::Value>,
    pub expires_on: Option<Expires<AtTime>>,
}

impl ProbeRead {
    /// Sample rows reach only the submitter, and only until they expire;
    /// anyone else sees the probe without its result.
    pub fn of(
        probe: ConnectionProbeDB,
        collection: CollectionName,
        viewer: &UserId,
        now: &AtTime,
    ) -> Self {
        let shown = probe.base.probe_kind != ProbeKind::Sample
            || (probe.base.requested_by_id == *viewer
                && probe
                    .result_expires
                    .is_some_and(|expires| **expires > **now));
        Self {
            probe_id: probe.base.id,
            connection_id: probe.base.connection_id,
            collection,
            kind: probe.base.probe_kind,
            status: probe.base.status,
            requested_by_id: probe.base.requested_by_id,
            requested_on: probe.base.requested_on,
            started_on: probe.started_on,
            ended_on: probe.ended_on,
            error: probe
                .error
                .map(|error| serde_json::json!({"code": error.0.code, "message": error.0.message})),
            result: probe.result.filter(|_| shown).map(|result| result.0.0),
            expires_on: probe.result_expires.filter(|_| shown),
        }
    }
}

#[ta_objects::QueryParam]
pub struct ProbeWaitParams {
    #[serde(default)]
    pub wait: Option<ProbeWaitSecs>,
}

#[ta_objects::UrlParam]
pub struct ProbeParam {
    pub probe: ProbeId,
}

#[derive(Debug, Clone, Default)]
pub struct CatalogFilter {
    pub kind: Option<ConnectionKind>,
    pub collection_type: Option<CollectionType>,
    pub project: Option<ProjectName>,
    pub metadata_since: Option<AtTime>,
}

/// One catalog a caller sees, with its collection's name: `develop` when the
/// caller develops in its project and so may read its entities.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct CatalogVisibleRow {
    pub connection_id: ConnectionId,
    pub collection: CollectionName,
    pub develop: bool,
}

/// A catalog's texts at `text_revision`: its collection's name,
/// description and open user tags (key to value) and its kind. Its entities
/// are [`CatalogEntityTextRow`]s.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct CatalogTextRow {
    pub connection_id: ConnectionId,
    pub text_revision: TextRevision,
    pub collection: CollectionName,
    pub collection_description: Description,
    pub kind: ConnectionKind,
    pub collection_tags: Json<BTreeMap<UserTagKey, TagValue>>,
}

#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct CatalogEntityTextRow {
    pub id: ConnectionEntityId,
    pub connection_id: ConnectionId,
    pub fqn: EntityFqn,
    pub entity_kind: CatalogEntityKind,
    pub field_names: FieldNames,
}

/// A live catalog's text revision and schema fingerprint.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct CatalogRevisionRow {
    pub connection_id: ConnectionId,
    pub text_revision: TextRevision,
    pub schema_hash: Option<CatalogSchemaHash>,
}

/// The probe pod's status report, as the runner it shares with functions
/// posts it; a `Done` report carries the probe outcome in `output`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProbeCallback {
    pub status: WorkerCallbackStatus,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub output: Option<serde_json::Value>,
}
