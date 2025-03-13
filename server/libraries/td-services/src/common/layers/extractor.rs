//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::{CreateRequest, UpdateRequest};
use td_tower::extractors::Input;

// TODO use this one in favor of the one in td-objects
pub trait RequestDtoProvider<D> {
    fn dto(&self) -> D;
}

impl<C: Clone, N> RequestDtoProvider<C> for CreateRequest<N, C> {
    fn dto(&self) -> C {
        self.data().clone()
    }
}

impl<C: Clone, N> RequestDtoProvider<C> for UpdateRequest<N, C> {
    fn dto(&self) -> C {
        self.data().clone()
    }
}

pub async fn extract_req_dto<P: RequestDtoProvider<D>, D>(
    Input(provider): Input<P>,
) -> Result<D, TdError> {
    Ok(provider.dto())
}
