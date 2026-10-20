//
// Copyright 2026 Tabsdata Inc.
//

use crate::config::Config;
use http::StatusCode;
use std::time::Duration;
use tower_http::timeout::TimeoutLayer;

/// A request still running at the deadline answers 504: not 408, which blames
/// the client, nor 503, which tells clients nothing was applied, while a write
/// may have committed before the deadline cut its response.
pub fn layer(config: &Config) -> TimeoutLayer {
    TimeoutLayer::with_status_code(
        StatusCode::GATEWAY_TIMEOUT,
        Duration::from_secs(config.request_timeout as u64),
    )
}
