//
//  Copyright 2024 Tabsdata Inc.
//

use http::Method;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use tower_http::cors::CorsLayer;

#[derive(Default)]
pub struct CorsService;

impl CorsService {
    pub fn layer() -> CorsLayer {
        CorsLayer::new()
            .allow_credentials(false)
            .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE])
    }
}
