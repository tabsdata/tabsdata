//
// Copyright 2025 Tabs Data Inc.
//

pub use tm_services::{FieldAccessors, ServiceFactory, service_factory};

use std::sync::Arc;

pub trait FieldAccessor<T> {
    fn get_field(ctx: &T) -> Self;
}

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
