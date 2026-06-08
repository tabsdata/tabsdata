//
// Copyright 2026 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(MetricsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::State;
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::GetStatus;
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_error::{ApiError, TdError, api_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::metrics::{
        Dim, InflightQuery, InflightRequest, InflightResponse, MetricsQuery, MetricsRequest,
        MetricsResponse, StatusFilterError,
    };
    use td_objects::dxo::query_filters::{
        DimensionFilter, TimeWindow, WindowAlign, WindowSpec, WindowUnit,
    };
    use td_objects::rest_urls::{INFLIGHT, METRICS};
    use td_objects::types::basic::{EpochMs, LogicalFunctionRunStatus, ProjectId};
    use td_services::metrics::services::OperationalMetricsServices;
    use tower::ServiceExt;

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

    fn metrics_query_into_request(
        project_id: ProjectId,
        q: MetricsQuery,
    ) -> Result<MetricsRequest, TdError> {
        let (window, unit) = resolve_window(q.unit, q.align, q.from, q.span)?;
        let status = q.status.unwrap_or(LogicalFunctionRunStatus::Committed);
        if !status.is_ended() {
            return Err(StatusFilterError::NotEnded {
                status: status.to_string(),
                allowed: format!("{:?}", LogicalFunctionRunStatus::ENDED),
            }
            .into());
        }
        Ok(MetricsRequest {
            project_id,
            dim: q.dim.unwrap_or(Dim::Collection),
            window,
            unit,
            collections: DimensionFilter::new(q.collection, q.collection_exclude),
            groups: DimensionFilter::new(q.group, q.group_exclude),
            statuses: vec![status],
        })
    }

    fn inflight_query_into_request(
        project_id: ProjectId,
        q: InflightQuery,
    ) -> Result<InflightRequest, TdError> {
        for status in &q.status {
            if !LogicalFunctionRunStatus::IN_FLIGHT.contains(status) {
                return Err(StatusFilterError::NotInFlight {
                    status: status.to_string(),
                    allowed: format!("{:?}", LogicalFunctionRunStatus::IN_FLIGHT),
                }
                .into());
            }
        }
        Ok(InflightRequest {
            project_id,
            collections: DimensionFilter::new(q.collection, q.collection_exclude),
            groups: DimensionFilter::new(q.group, q.group_exclude),
            statuses: q.status,
        })
    }

    /// The request's resolved project, set by `project_resolver_layer` from the
    /// `{project}` URL segment. Tenant routes always carry it; absence is an
    /// internal error.
    fn require_project(context: &RequestContext) -> Result<ProjectId, TdError> {
        context.project_id.clone().ok_or_else(|| {
            api_error!(ApiError::InternalError, "metrics endpoint reached without a resolved project")
        })
    }

    #[apiserver_path(method = get, path = METRICS, tag = METRICS_TAG)]
    #[doc = "Consolidated operational metrics: nodes + edges over a window"]
    pub async fn get_metrics(
        State(state): State<Arc<OperationalMetricsServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query): Query<MetricsQuery>,
    ) -> Result<GetStatus<MetricsResponse>, ErrorStatus> {
        let request = metrics_query_into_request(require_project(&context)?, query)?;
        let response = state.metrics.service().oneshot(request).await?;
        Ok(GetStatus::OK(response))
    }

    #[apiserver_path(method = get, path = INFLIGHT, tag = METRICS_TAG)]
    #[doc = "Point-in-time count of in-flight runs"]
    pub async fn get_inflight(
        State(state): State<Arc<OperationalMetricsServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query): Query<InflightQuery>,
    ) -> Result<GetStatus<InflightResponse>, ErrorStatus> {
        let request = inflight_query_into_request(require_project(&context)?, query)?;
        let response = state.inflight.service().oneshot(request).await?;
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

        // -------- Status validation -------------------------------------

        fn metrics_query(status: Option<LogicalFunctionRunStatus>) -> MetricsQuery {
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
            let req = metrics_query_into_request(ProjectId::default(), metrics_query(None)).unwrap();
            assert_eq!(req.statuses, vec![LogicalFunctionRunStatus::Committed]);
        }

        #[test]
        fn metrics_accepts_ended_status() {
            for s in LogicalFunctionRunStatus::ENDED {
                let req = metrics_query_into_request(ProjectId::default(), metrics_query(Some(*s))).unwrap();
                assert_eq!(req.statuses, vec![*s]);
            }
        }

        #[test]
        fn metrics_rejects_non_ended_status() {
            for s in LogicalFunctionRunStatus::IN_FLIGHT {
                let Err(err) = metrics_query_into_request(ProjectId::default(), metrics_query(Some(*s))) else {
                    panic!("expected NotEnded rejection for {s}");
                };
                assert!(err.to_string().contains("not ENDED"), "got: {err}");
            }
        }

        #[test]
        fn inflight_accepts_empty_status() {
            let req = inflight_query_into_request(ProjectId::default(), inflight_query(vec![])).unwrap();
            assert!(req.statuses.is_empty());
        }

        #[test]
        fn inflight_accepts_in_flight_statuses() {
            let qs: Vec<_> = LogicalFunctionRunStatus::IN_FLIGHT.to_vec();
            let req = inflight_query_into_request(ProjectId::default(), inflight_query(qs.clone())).unwrap();
            assert_eq!(req.statuses, qs);
        }

        #[test]
        fn inflight_rejects_ended_status() {
            for s in LogicalFunctionRunStatus::ENDED {
                let Err(err) = inflight_query_into_request(ProjectId::default(), inflight_query(vec![*s])) else {
                    panic!("expected NotInFlight rejection for {s}");
                };
                assert!(err.to_string().contains("not IN_FLIGHT"), "got: {err}");
            }
        }
    }
}
