//
// Copyright 2025 Tabs Data Inc.
//

pub use ta_apiserver::RouterExtension;

use axum::Router;

pub struct ExtendedRouter;

impl RouterExtension for ExtendedRouter {
    fn router() -> Router {
        Router::new()
    }
}
