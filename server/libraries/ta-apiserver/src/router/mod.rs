//
// Copyright 2025 Tabsdata Inc.
//

use utoipa_axum::router::OpenApiRouter;

pub trait RouterExtension<S> {
    fn router(state: S) -> OpenApiRouter;
}
