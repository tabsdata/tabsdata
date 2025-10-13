//
// Copyright 2025 Tabs Data Inc.
//

use axum::extract::FromRef;
use ta_services::extension::ContextExt;
use ta_services::factory::{FieldAccessors, ServiceFactory};

#[derive(ServiceFactory, Clone, Debug)]
pub struct ExtendedServices {}

#[derive(FieldAccessors, FromRef, Default, Clone, Debug)]
pub struct ExtendedContext {}

impl<Base, Extended> ContextExt<Base, Extended> for ExtendedContext {
    fn build(_base: &Base, _extended: &Extended) -> Self {
        Self::default()
    }
}
