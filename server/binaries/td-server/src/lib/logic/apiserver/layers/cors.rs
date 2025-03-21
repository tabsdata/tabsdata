//
//  Copyright 2024 Tabs Data Inc.
//

use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use http::Method;
use tower_http::cors::CorsLayer;

#[derive(Default)]
pub struct CorsService;

impl CorsService {
    /// Creates a `CorsLayer` with predefined settings.
    ///
    /// - Allows credentials.
    /// - Allows methods: GET, POST, PUT, DELETE.
    /// - Allows headers: AUTHORIZATION, ACCEPT, CONTENT_TYPE.
    pub fn layer() -> CorsLayer {
        CorsLayer::new()
            .allow_credentials(false)
            .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE])
    }
}
