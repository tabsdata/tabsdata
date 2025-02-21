//
//  Copyright 2024 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use td_apiforge::{api_server_schema, status};
use td_concrete::concrete;
use td_tower::ctx_service::{CtxMap, CtxResponse, CtxResponseBuilder};

pub mod error_status;
pub mod extractors;
pub mod td_error_status;

#[api_server_schema]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyResponse;

#[concrete]
#[api_server_schema]
type DeleteResponse = CtxResponse<EmptyResponse>;

impl From<CtxResponse<()>> for DeleteResponse {
    fn from(value: CtxResponse<()>) -> Self {
        value.transform(|_| EmptyResponse).into()
    }
}

status!(
    DeleteStatus,
    OK => DeleteResponse,
);

#[concrete]
#[api_server_schema]
type EmptyUpdateResponse = CtxResponse<EmptyResponse>;

impl From<CtxResponse<()>> for EmptyUpdateResponse {
    fn from(value: CtxResponse<()>) -> Self {
        value.transform(|_| EmptyResponse).into()
    }
}

status!(
    EmptyUpdateStatus,
    OK => EmptyUpdateResponse,
);
