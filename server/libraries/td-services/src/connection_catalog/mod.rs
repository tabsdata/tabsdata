//
// Copyright 2026 Tabsdata Inc.
//

//! The connection catalog: probes (one Explorer call against one connection,
//! run as a worker job that receives the connection secret like a function
//! does) and the catalog they fill. Every change to a catalog's texts gives it
//! a new `text_revision`, which a derived index follows.
//!
//! A probe is submitted `queued`; the dispatcher claims it within the
//! concurrency caps, marks it `running` and sends its `ProbeRequest`; the
//! runner posts the outcome to the internal result route, which persists it.
//! Submissions with the same `(connection, kind, params)` coalesce while one
//! is active; a sample also keys on its requester, whose rows they are. Sample
//! rows are kept on the probe only until their expiry, reach only the
//! requester and every sample is audit-logged.

mod limits;
pub mod services;

use crate::collection::services::connection::{ensure_connectable, require_connection_id};
use sha2::{Digest, Sha256};
use std::time::Duration;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::{CollectionDB, CollectionParam};
use td_objects::dxo::connection_catalog::ConnectionCatalogDBWithNames;
use td_objects::types::basic::{
    AtTime, ConnectionId, ProbeDedupeKey, ProbeKind, ProbeParams, UserId,
};
use td_queries::Queries;
use tokio::sync::Notify;

pub use limits::SubmitLimiter;

/// Probes running at once in the instance, and in one project.
pub const MAX_CONCURRENT: usize = 16;
pub const MAX_CONCURRENT_PER_PROJECT: usize = 4;
/// Probe submissions per user per minute.
pub const SUBMIT_PER_MINUTE: u32 = 30;
pub const MAX_QUEUED_PER_PROJECT: usize = 64;
/// Ended probes are deleted this long after they end.
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// The entities a catalog keeps; a metadata outcome past it keeps the first
/// by fqn and says `truncated`.
pub const MAX_ENTITIES: usize = 20_000;
/// What a metadata outcome may spend per entity on average; the runner
/// drops the last entities by fqn (and says `truncated`) past the budget.
pub const ENTITY_BYTES: usize = 4 * 1024;
/// The entities' byte budget the runner is sent: about 78 MiB.
pub const MAX_ENTITIES_BYTES: usize = MAX_ENTITIES * ENTITY_BYTES;
/// The callback envelope and a sample's rows, on top of the entities.
const OUTCOME_ENVELOPE_BYTES: usize = MAX_SAMPLE_BYTES + 64 * 1024;
/// The probe result callback's body limit, in place of the 2 MiB default:
/// the entities' budget plus the envelope.
pub const MAX_OUTCOME_BYTES: usize = MAX_ENTITIES_BYTES + OUTCOME_ENVELOPE_BYTES;

/// The entities one sample probe may read.
pub const MAX_SAMPLE_ENTITIES: usize = 10;
pub const MAX_SAMPLE_BYTES: usize = 1024 * 1024;
pub const SAMPLE_TTL: Duration = Duration::from_secs(600);
pub const MAX_WAIT_SECS: u64 = 55;

/// Slack on top of the runner's own timeout: up to 180 s of venv loading
/// before the runner starts, the 32 s the Done post may retry, and a margin.
/// The Job's `activeDeadlineSeconds` (330) is 180 + 90 (the metadata
/// timeout) + 32 + 28, so every kind's deadline here outlasts its pod.
const DISPATCH_GRACE: Duration = Duration::from_secs(250);
/// A probe the caps kept queued this long is given up on.
pub const MAX_QUEUED: Duration = Duration::from_secs(600);
/// A claimed probe whose request was not delivered within this goes back to
/// the queue; longer than one tick takes to deliver its claims.
pub const DELIVERY_GRACE: Duration = Duration::from_secs(60);

pub(crate) const ACTIVE_PROBE_INDEX: &str = "connection_probes___dedupe_key___active_uidx";

/// The runner's per-kind timeouts, plus the dispatch grace: past this a
/// running probe with no outcome is marked `timeout`.
pub fn running_deadline(kind: ProbeKind) -> Duration {
    let runner = match kind {
        ProbeKind::Check => 20,
        ProbeKind::Metadata => 90,
        ProbeKind::Sample => 30,
    };
    Duration::from_secs(runner) + DISPATCH_GRACE
}

/// The service context shared by the catalog services and the dispatcher.
pub struct ConnectionCatalog {
    pub limiter: SubmitLimiter,
    /// Fired on every submission, so the dispatcher claims without waiting
    /// for its timer.
    pub submitted: Notify,
    /// Fired on every probe outcome; waiters re-read the probe, so a result
    /// landing on another replica is still seen on its next read.
    pub ended: Notify,
}

impl Default for ConnectionCatalog {
    fn default() -> Self {
        Self {
            limiter: SubmitLimiter::new(SUBMIT_PER_MINUTE),
            submitted: Notify::new(),
            ended: Notify::new(),
        }
    }
}

/// `sha256(connection_id, kind, params)`, and the requester for a sample:
/// its rows are only for the one who asked.
pub fn dedupe_key(
    connection_id: &ConnectionId,
    kind: ProbeKind,
    params: &ProbeParams,
    requester: &UserId,
) -> Result<ProbeDedupeKey, TdError> {
    let params = serde_json::to_string(params)
        .map_err(|e| api_error!(ApiError::InternalError, "cannot serialize the params: {e}"))?;
    let mut hasher = Sha256::new();
    hasher.update(connection_id.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(kind.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(params.as_bytes());
    if kind == ProbeKind::Sample {
        hasher.update(b"\0");
        hasher.update(requester.to_string().as_bytes());
    }
    ProbeDedupeKey::try_from(hex::encode(hasher.finalize()))
}

/// The connection of a source/destination collection, with its catalog view
/// row (defaults until the first probe).
pub async fn resolve_catalog<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    param: &CollectionParam,
) -> Result<ConnectionCatalogDBWithNames, TdError> {
    let collection: CollectionDB = conn.scd2().fetch_one(&param.collection).await?;
    ensure_connectable(&collection)?;
    let connection_id = require_connection_id(&collection)?;
    Ok(conn.fetch_one(&connection_id).await?)
}

pub(crate) fn shifted(at: &AtTime, by: Duration, forward: bool) -> AtTime {
    let by = chrono::Duration::from_std(by).unwrap_or(chrono::Duration::MAX);
    AtTime::from(if forward { ***at + by } else { ***at - by })
}

pub(crate) fn rate_limited(retry_after: Duration) -> TdError {
    let seconds = (retry_after.as_secs() + u64::from(retry_after.subsec_nanos() > 0)).max(1);
    api_error!(
        ApiError::Any(429),
        "rate_limited: too many probe submissions, retry after {seconds}s"
    )
    .with_retry_after(Duration::from_secs(seconds))
}

pub(crate) fn entity_not_in_catalog(fqn: &str) -> TdError {
    api_error!(
        ApiError::NotFound,
        "entity_not_in_catalog: '{fqn}' is not in the catalog; refresh the connection first"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_dedupe_key_distinguishes_kind_and_params() {
        let connection = ConnectionId::default();
        let (user, other) = (UserId::default(), UserId::default());
        let none = ProbeParams::default();
        let sample = ProbeParams {
            entity: Some("db.public.t".to_owned()),
            entities: None,
            limit: Some(20),
        };
        let key = |connection: &ConnectionId, kind, params, user| {
            dedupe_key(connection, kind, params, user).unwrap()
        };
        let check = key(&connection, ProbeKind::Check, &none, &user);
        assert_eq!(check, key(&connection, ProbeKind::Check, &none, &user));
        assert_eq!(
            check,
            key(&connection, ProbeKind::Check, &none, &other),
            "a check is shared by every requester"
        );
        assert_ne!(check, key(&connection, ProbeKind::Metadata, &none, &user));
        assert_ne!(
            key(&connection, ProbeKind::Sample, &sample, &user),
            key(&connection, ProbeKind::Sample, &none, &user)
        );
        assert_ne!(
            key(&connection, ProbeKind::Sample, &sample, &user),
            key(&connection, ProbeKind::Sample, &sample, &other),
            "a sample is its requester's own"
        );
        assert_ne!(
            check,
            key(&ConnectionId::default(), ProbeKind::Check, &none, &user)
        );
    }

    #[test]
    fn a_running_probe_gets_its_kind_timeout_plus_the_grace() {
        assert_eq!(running_deadline(ProbeKind::Check), Duration::from_secs(270));
        assert_eq!(
            running_deadline(ProbeKind::Metadata),
            Duration::from_secs(340)
        );
        assert_eq!(
            running_deadline(ProbeKind::Sample),
            Duration::from_secs(280)
        );
    }

    /// The server gives up on a probe only after its pod is gone, so an
    /// outcome the pod still manages to post is never dropped as late.
    #[test]
    fn every_kind_outlasts_the_probe_job_deadline() {
        let pods = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../variant/resources/kprofile/pods");
        let mut templates = 0;
        for entry in std::fs::read_dir(&pods).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if !name.starts_with("connection-probe_") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            let deadline: u64 = text
                .lines()
                .find_map(|line| line.trim().strip_prefix("activeDeadlineSeconds:"))
                .unwrap_or_else(|| panic!("{name}: no activeDeadlineSeconds"))
                .trim()
                .parse()
                .unwrap();
            assert!(
                running_deadline(ProbeKind::Metadata) > Duration::from_secs(deadline),
                "{name}: the metadata deadline must outlast the job's {deadline}s"
            );
            assert!(
                deadline >= 180 + 90 + 32,
                "{name}: {deadline}s cannot fit venv loading, the metadata timeout and the post retries"
            );
            templates += 1;
        }
        assert_eq!(templates, 6, "one probe job per platform");
    }
}
