//
// Copyright 2025 Tabs Data Inc.
//

use axum::extract::FromRef;
use ta_services::extension::ContextExt;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_error::TdError;

#[derive(ServiceFactory, Clone, Debug)]
pub struct ExtendedServices {}

#[derive(FieldAccessors, FromRef, Default, Clone, Debug)]
pub struct ExtendedContext {}

impl<Base, Extended> ContextExt<Base, Extended> for ExtendedContext {
    async fn build(_base: &Base, _extended: &Extended) -> Result<Self, TdError> {
        Ok(Self::default())
    }
}
