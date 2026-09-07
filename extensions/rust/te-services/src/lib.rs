//
// Copyright 2025 Tabsdata Inc.
//

use std::future::Future;
use td_database::components::ServingPools;
use td_error::TdError;

/// The edition's service state; the open-source edition carries none.
#[derive(Default, Clone, Debug)]
pub struct ExtendedServices {}

/// Edition seam: extend the base services into this edition's
/// [`ExtendedServices`], given the serving pools and the edition's resolved
/// config. Implemented for any base — an extension grabs what it needs off
/// the base and the pools.
pub trait ContextExt<Extended> {
    fn extend(
        &self,
        serving_pools: &ServingPools,
        extended: &Extended,
    ) -> impl Future<Output = Result<ExtendedServices, TdError>>;
}

impl<Base, Extended> ContextExt<Extended> for Base {
    async fn extend(
        &self,
        _serving_pools: &ServingPools,
        _extended: &Extended,
    ) -> Result<ExtendedServices, TdError> {
        Ok(ExtendedServices::default())
    }
}
