//
// Copyright 2025 Tabs Data Inc.
//

use axum::Router;

pub trait RouterExtension {
    fn router() -> Router;
}
