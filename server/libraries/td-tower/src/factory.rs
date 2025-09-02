//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;

pub trait ServiceFactory<C> {
    type Service;

    fn build(ctx: &C) -> Self::Service;
}

impl<C, T: ServiceFactory<C>> ServiceFactory<C> for Arc<T> {
    type Service = Arc<T::Service>;

    fn build(ctx: &C) -> Self::Service {
        Arc::new(T::build(ctx))
    }
}
