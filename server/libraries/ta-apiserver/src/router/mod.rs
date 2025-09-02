//
// Copyright 2025 Tabs Data Inc.
//

use utoipa_axum::router::OpenApiRouter;

pub trait RouterExtension<S> {
    fn router(state: S) -> OpenApiRouter;
}
