//
//  Copyright 2024 Tabs Data Inc.
//

use tower_http::trace::{
    DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, HttpMakeClassifier, TraceLayer,
};
use tower_http::LatencyUnit;
use tracing::Level;

#[derive(Default)]
pub struct TraceService;

impl TraceService {
    pub fn layer() -> TraceLayer<HttpMakeClassifier> {
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().include_headers(true))
            .on_request(DefaultOnRequest::new().level(Level::INFO))
            .on_response(
                DefaultOnResponse::new()
                    .level(Level::INFO)
                    .latency_unit(LatencyUnit::Micros),
            )
    }
}
