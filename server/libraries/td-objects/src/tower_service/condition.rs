//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::dlo::UserId;
use td_error::TdError;
use td_tower::default_services::Condition;
use td_tower::extractors::Input;

pub async fn is_req_by_user(
    Input(request_context): Input<RequestContext>,
    Input(user_id): Input<UserId>,
) -> Result<Condition, TdError> {
    let request_by_user = request_context.user_id().to_string() == user_id.to_string();
    Ok(Condition(request_by_user))
}
