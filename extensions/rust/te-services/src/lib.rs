//
// Copyright 2025 Tabs Data Inc.
//

use axum::extract::FromRef;
use std::future::Future;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_database::system::ServingPools;
use td_error::TdError;

#[derive(ServiceFactory, Clone, Debug)]
pub struct ExtendedServices {}

#[derive(FieldAccessors, FromRef, Default, Clone, Debug)]
pub struct ExtendedContext {}

/// Edition seam: extend a base context into this edition's [`ExtendedContext`],
/// given the serving pools and the edition's resolved config. Implemented for any
/// base — an extension grabs what it needs off the base (via `FieldAccessor`) and
/// the pools. The open-source edition carries no extension state.
pub trait ContextExt<Extended> {
    fn extend(
        &self,
        serving_pools: &ServingPools,
        extended: &Extended,
    ) -> impl Future<Output = Result<ExtendedContext, TdError>>;
}

impl<Base, Extended> ContextExt<Extended> for Base {
    async fn extend(
        &self,
        _serving_pools: &ServingPools,
        _extended: &Extended,
    ) -> Result<ExtendedContext, TdError> {
        Ok(ExtendedContext::default())
    }
}
