//
//  Copyright 2024 Tabs Data Inc.
//

use chrono::Duration;
use tower_http::timeout::TimeoutLayer;

pub struct TimeoutService {
    timeout: Duration,
}

impl TimeoutService {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl TimeoutService {
    /// Creates a `TimeoutLayer` with predefined settings.
    pub fn layer(self) -> TimeoutLayer {
        TimeoutLayer::new(self.timeout.to_std().unwrap())
    }
}
