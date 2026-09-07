//
// Copyright 2026 Tabsdata Inc.
//

//! Operational metrics. `/metrics` is the consolidated nodes+edges endpoint,
//! parameterised by `dim=collection|group`. `/inflight` is the point-in-time
//! complement (no window); `/snapshot` returns both in one response over a
//! single DB connection.
//!
//! Each has two forms. The project-scoped `/projects/{project}/...` form drives
//! the connection GUC from the `{project}` segment and lets RLS filter the
//! aggregation to that project (the queries carry no project predicate). The
//! top-level form runs `scope = Global`, which bypasses RLS so the aggregation
//! spans every project; its rows carry `project_id`/`project_name` so
//! same-named collections/groups across projects stay distinguishable.

use crate::extractors::Project;
use axum::Extension;
use axum::extract::State;
use axum_extra::extract::Query;
use itertools::Itertools;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::GetStatus;
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::metrics::{
    Dim, InflightQuery, InflightRequest, InflightResponse, InflightStatusQuery, MetricsQuery,
    MetricsRequest, MetricsResponse, SnapshotRequest, SnapshotResponse, StatusFilterError,
};
use td_objects::dxo::query_filters::{
    DimensionFilter, TimeWindow, WindowAlign, WindowSpec, WindowUnit,
};
use td_objects::types::basic::{EpochMs, LogicalFunctionRunStatus};
use td_services::Services;

const METRICS_TAG: &str = "Metrics";

/// Resolve the time-window grammar from URL params. Span /
/// emptiness / per-unit span-cap enforced by [`WindowSpec::resolve`].
fn resolve_window(
    unit: Option<WindowUnit>,
    align: Option<WindowAlign>,
    from: Option<i64>,
    span: Option<u32>,
) -> Result<(TimeWindow, WindowUnit), TdError> {
    let unit = unit.unwrap_or(WindowUnit::Day);
    let from = from.map(EpochMs::try_from).transpose()?;
    let spec = WindowSpec {
        align: align.unwrap_or(WindowAlign::Calendar),
        from,
        unit,
        span: span.unwrap_or(1),
    };
    let window = spec.resolve()?;
    Ok((window, unit))
}

pub(crate) fn metrics_query_into_request(q: MetricsQuery) -> Result<MetricsRequest, TdError> {
    let (window, unit) = resolve_window(q.unit, q.align, q.from, q.span)?;
    // Dedup to bound the SQL `IN (...)` clause against repeated params.
    let statuses = if q.status.is_empty() {
        vec![LogicalFunctionRunStatus::Committed]
    } else {
        q.status.into_iter().unique().collect()
    };
    if let Some(status) = statuses.iter().find(|s| !s.is_ended()) {
        return Err(StatusFilterError::NotEnded {
            status: status.to_string(),
            allowed: format!("{:?}", LogicalFunctionRunStatus::ENDED),
        }
        .into());
    }
    Ok(MetricsRequest {
        dim: q.dim.unwrap_or(Dim::Collection),
        window,
        unit,
        collections: DimensionFilter::new(q.collection, q.collection_exclude),
        groups: DimensionFilter::new(q.group, q.group_exclude),
        statuses,
    })
}

/// Dedup and reject any status that isn't IN_FLIGHT.
fn checked_inflight_statuses(
    status: Vec<LogicalFunctionRunStatus>,
) -> Result<Vec<LogicalFunctionRunStatus>, TdError> {
    let statuses: Vec<_> = status.into_iter().unique().collect();
    if let Some(status) = statuses
        .iter()
        .find(|s| !LogicalFunctionRunStatus::IN_FLIGHT.contains(s))
    {
        return Err(StatusFilterError::NotInFlight {
            status: status.to_string(),
            allowed: format!("{:?}", LogicalFunctionRunStatus::IN_FLIGHT),
        }
        .into());
    }
    Ok(statuses)
}

/// Combine the `/metrics` query with the snapshot-only `inflight_status`
/// filter. The inflight half shares the metrics dim filters.
pub(crate) fn snapshot_query_into_request(
    metrics: MetricsQuery,
    inflight_status: Vec<LogicalFunctionRunStatus>,
) -> Result<SnapshotRequest, TdError> {
    let inflight_statuses = checked_inflight_statuses(inflight_status)?;
    let metrics = metrics_query_into_request(metrics)?;
    let inflight = InflightRequest {
        collections: metrics.collections.clone(),
        groups: metrics.groups.clone(),
        statuses: inflight_statuses,
    };
    Ok(SnapshotRequest { metrics, inflight })
}

pub(crate) fn inflight_query_into_request(q: InflightQuery) -> Result<InflightRequest, TdError> {
    let statuses = checked_inflight_statuses(q.status)?;
    Ok(InflightRequest {
        collections: DimensionFilter::new(q.collection, q.collection_exclude),
        groups: DimensionFilter::new(q.group, q.group_exclude),
        statuses,
    })
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/metrics", tag = METRICS_TAG, doc = "Consolidated operational metrics: nodes + edges over a window")]
pub async fn get_metrics(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(metrics): Query<MetricsQuery>,
) -> Result<GetStatus<MetricsResponse>, ErrorStatus> {
    let request = context
        .read(metrics_query_into_request(metrics)?)
        .in_project(project_id);
    let response = state.metrics(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/inflight", tag = METRICS_TAG, doc = "Point-in-time count of in-flight runs")]
pub async fn get_inflight(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(inflight): Query<InflightQuery>,
) -> Result<GetStatus<InflightResponse>, ErrorStatus> {
    let request = context
        .read(inflight_query_into_request(inflight)?)
        .in_project(project_id);
    let response = state.inflight(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = project, method = get, path = "/projects/{project}/snapshot", tag = METRICS_TAG, doc = "Combined `/metrics` + `/inflight` on one DB connection")]
pub async fn get_snapshot(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(metrics): Query<MetricsQuery>,
    Query(inflight_status): Query<InflightStatusQuery>,
) -> Result<GetStatus<SnapshotResponse>, ErrorStatus> {
    let request = context
        .read(snapshot_query_into_request(
            metrics,
            inflight_status.inflight_status,
        )?)
        .in_project(project_id);
    let response = state.snapshot(request).await?;
    Ok(GetStatus::OK(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_resolves_to_day_unit() {
        let (_, unit) = resolve_window(None, None, None, None).unwrap();
        assert_eq!(unit, WindowUnit::Day);
    }

    #[test]
    fn over_span_cap_is_rejected() {
        let err = resolve_window(Some(WindowUnit::Hour), None, None, Some(200)).unwrap_err();
        assert!(err.to_string().contains("cap"), "got: {err}");
    }

    /// `from` propagates from the URL param into the resolved
    /// window when `align=rolling`.
    #[test]
    fn from_param_pins_window_end() {
        // Wed 2026-05-20 00:00 UTC.
        let wed_midnight: i64 = 1_779_321_600_000;
        let (window, _) = resolve_window(
            Some(WindowUnit::Week),
            Some(WindowAlign::Rolling),
            Some(wed_midnight),
            None,
        )
        .unwrap();
        assert_eq!(window.end_ms, wed_midnight);
        assert_eq!(window.start_ms, wed_midnight - 7 * 86_400_000);
    }

    fn metrics_query(status: Vec<LogicalFunctionRunStatus>) -> MetricsQuery {
        MetricsQuery {
            dim: None,
            unit: None,
            align: None,
            from: None,
            span: None,
            collection: vec![],
            collection_exclude: vec![],
            group: vec![],
            group_exclude: vec![],
            status,
        }
    }

    fn inflight_query(statuses: Vec<LogicalFunctionRunStatus>) -> InflightQuery {
        InflightQuery {
            collection: vec![],
            collection_exclude: vec![],
            group: vec![],
            group_exclude: vec![],
            status: statuses,
        }
    }

    #[test]
    fn metrics_default_status_is_committed() {
        let req = metrics_query_into_request(metrics_query(vec![])).unwrap();
        assert_eq!(req.statuses, vec![LogicalFunctionRunStatus::Committed]);
    }

    #[test]
    fn metrics_accepts_ended_status() {
        for s in LogicalFunctionRunStatus::ENDED {
            let req = metrics_query_into_request(metrics_query(vec![*s])).unwrap();
            assert_eq!(req.statuses, vec![*s]);
        }
    }

    #[test]
    fn metrics_accepts_multiple_ended_statuses() {
        let qs: Vec<_> = LogicalFunctionRunStatus::ENDED.to_vec();
        let req = metrics_query_into_request(metrics_query(qs.clone())).unwrap();
        assert_eq!(req.statuses, qs);
    }

    #[test]
    fn metrics_dedups_repeated_statuses() {
        let dupes = vec![
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::Done,
        ];
        let req = metrics_query_into_request(metrics_query(dupes)).unwrap();
        assert_eq!(
            req.statuses,
            vec![
                LogicalFunctionRunStatus::Done,
                LogicalFunctionRunStatus::Failed
            ]
        );
    }

    #[test]
    fn metrics_rejects_non_ended_status() {
        for s in LogicalFunctionRunStatus::IN_FLIGHT {
            let Err(err) = metrics_query_into_request(metrics_query(vec![*s])) else {
                panic!("expected NotEnded rejection for {s}");
            };
            assert!(err.to_string().contains("not ENDED"), "got: {err}");
        }
    }

    #[test]
    fn metrics_rejects_mix_of_ended_and_non_ended() {
        let mix = vec![
            LogicalFunctionRunStatus::Committed,
            LogicalFunctionRunStatus::IN_FLIGHT[0],
        ];
        let Err(err) = metrics_query_into_request(metrics_query(mix)) else {
            panic!("expected NotEnded rejection for mixed statuses");
        };
        assert!(err.to_string().contains("not ENDED"), "got: {err}");
    }

    #[test]
    fn inflight_accepts_empty_status() {
        let req = inflight_query_into_request(inflight_query(vec![])).unwrap();
        assert!(req.statuses.is_empty());
    }

    #[test]
    fn inflight_accepts_in_flight_statuses() {
        let qs: Vec<_> = LogicalFunctionRunStatus::IN_FLIGHT.to_vec();
        let req = inflight_query_into_request(inflight_query(qs.clone())).unwrap();
        assert_eq!(req.statuses, qs);
    }

    #[test]
    fn inflight_dedups_repeated_statuses() {
        let s = LogicalFunctionRunStatus::IN_FLIGHT[0];
        let req = inflight_query_into_request(inflight_query(vec![s, s, s])).unwrap();
        assert_eq!(req.statuses, vec![s]);
    }

    #[test]
    fn inflight_rejects_ended_status() {
        for s in LogicalFunctionRunStatus::ENDED {
            let Err(err) = inflight_query_into_request(inflight_query(vec![*s])) else {
                panic!("expected NotInFlight rejection for {s}");
            };
            assert!(err.to_string().contains("not IN_FLIGHT"), "got: {err}");
        }
    }

    #[test]
    fn snapshot_metrics_honors_multiple_statuses() {
        let qs = vec![
            LogicalFunctionRunStatus::Done,
            LogicalFunctionRunStatus::Failed,
        ];
        let req = snapshot_query_into_request(metrics_query(qs.clone()), vec![]).unwrap();
        assert_eq!(req.metrics.statuses, qs);
        // Empty inflight_status → all in-flight variants.
        assert!(req.inflight.statuses.is_empty());
    }

    #[test]
    fn snapshot_metrics_defaults_to_committed() {
        let req = snapshot_query_into_request(metrics_query(vec![]), vec![]).unwrap();
        assert_eq!(
            req.metrics.statuses,
            vec![LogicalFunctionRunStatus::Committed]
        );
        assert!(req.inflight.statuses.is_empty());
    }

    #[test]
    fn snapshot_inflight_honors_statuses() {
        let inflight = LogicalFunctionRunStatus::IN_FLIGHT.to_vec();
        let req = snapshot_query_into_request(metrics_query(vec![]), inflight.clone()).unwrap();
        assert_eq!(req.inflight.statuses, inflight);
        // Metrics half still defaults independently.
        assert_eq!(
            req.metrics.statuses,
            vec![LogicalFunctionRunStatus::Committed]
        );
    }

    #[test]
    fn snapshot_inflight_dedups_repeated_statuses() {
        let s = LogicalFunctionRunStatus::IN_FLIGHT[0];
        let req = snapshot_query_into_request(metrics_query(vec![]), vec![s, s, s]).unwrap();
        assert_eq!(req.inflight.statuses, vec![s]);
    }

    #[test]
    fn snapshot_rejects_non_ended_metrics_status() {
        let q = metrics_query(vec![LogicalFunctionRunStatus::IN_FLIGHT[0]]);
        let Err(err) = snapshot_query_into_request(q, vec![]) else {
            panic!("expected NotEnded rejection");
        };
        assert!(err.to_string().contains("not ENDED"), "got: {err}");
    }

    #[test]
    fn snapshot_rejects_ended_inflight_status() {
        let q = metrics_query(vec![]);
        let Err(err) = snapshot_query_into_request(q, vec![LogicalFunctionRunStatus::Committed])
        else {
            panic!("expected NotInFlight rejection");
        };
        assert!(err.to_string().contains("not IN_FLIGHT"), "got: {err}");
    }

    /// Snapshot's two `Query` extractors split one query string: each
    /// reads its own params and ignores the other's.
    #[test]
    fn snapshot_two_extractors_split_one_query_string() {
        use axum::http::Uri;
        // Enums use the lowercased-ident aliases (`group`, `done`, ...).
        let uri: Uri = "/snapshot?dim=group&unit=hour\
            &collection=00000000000000000000000c00&collection=00000000000000000000000c00\
            &status=done&status=failed&inflight_status=running"
            .parse()
            .unwrap();
        let Query(m) = Query::<MetricsQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(m.dim, Some(Dim::Group));
        assert_eq!(m.unit, Some(WindowUnit::Hour));
        assert_eq!(m.collection.len(), 2);
        assert_eq!(
            m.status,
            vec![
                LogicalFunctionRunStatus::Done,
                LogicalFunctionRunStatus::Failed
            ]
        );
        let Query(i) = Query::<InflightStatusQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(i.inflight_status, vec![LogicalFunctionRunStatus::Running]);
    }
}

/// Global (cross-project) metrics endpoints. Identical query grammar and
/// responses to the project-scoped router, but mounted top-level (no `{project}`
/// segment) and backed by the `*Global` services, which bypass RLS to aggregate
/// across every project. The query→request conversion is shared with the scoped
/// router; the only differences are the absence of a `{project}` (so no
/// `Project` extractor / `.in_project`) and the `*_global` service handle.

#[apiserver_path(mount = global, method = get, path = "/metrics", tag = METRICS_TAG, doc = "Cross-project operational metrics: nodes + edges over a window")]
pub async fn get_metrics_global(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(metrics): Query<MetricsQuery>,
) -> Result<GetStatus<MetricsResponse>, ErrorStatus> {
    let request = context.read(metrics_query_into_request(metrics)?);
    let response = state.metrics_global(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = global, method = get, path = "/inflight", tag = METRICS_TAG, doc = "Cross-project point-in-time count of in-flight runs")]
pub async fn get_inflight_global(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(inflight): Query<InflightQuery>,
) -> Result<GetStatus<InflightResponse>, ErrorStatus> {
    let request = context.read(inflight_query_into_request(inflight)?);
    let response = state.inflight_global(request).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(mount = global, method = get, path = "/snapshot", tag = METRICS_TAG, doc = "Cross-project combined `/metrics` + `/inflight` on one DB connection")]
pub async fn get_snapshot_global(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(metrics): Query<MetricsQuery>,
    Query(inflight_status): Query<InflightStatusQuery>,
) -> Result<GetStatus<SnapshotResponse>, ErrorStatus> {
    let request = context.read(snapshot_query_into_request(
        metrics,
        inflight_status.inflight_status,
    )?);
    let response = state.snapshot_global(request).await?;
    Ok(GetStatus::OK(response))
}
