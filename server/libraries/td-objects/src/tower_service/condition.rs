//
// Copyright 2025 Tabs Data Inc.
//

use crate::dlo::{RequestIsAdmin, Value};
use td_common::error::TdError;
use td_tower::default_services::Condition;
use td_tower::extractors::Input;

pub async fn is_req_by_admin(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<Condition, TdError> {
    Ok(Condition(*req_is_admin.value()))
}
