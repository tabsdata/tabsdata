//
// Copyright 2025 Tabs Data Inc.
//

use ta_apiserver::router::RouterExtension;
use utoipa_axum::router::OpenApiRouter;

pub struct AuthenticatedExtendedRouter;

impl<S> RouterExtension<S> for AuthenticatedExtendedRouter {
    fn router(_state: S) -> OpenApiRouter {
        OpenApiRouter::default()
    }
}

pub struct UnauthenticatedExtendedRouter;

impl<S> RouterExtension<S> for UnauthenticatedExtendedRouter {
    fn router(_state: S) -> OpenApiRouter {
        OpenApiRouter::default()
    }
}
