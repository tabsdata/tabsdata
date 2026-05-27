//
//  Copyright 2024 Tabs Data Inc.
//

use axum::extract::{Request, connect_info::ConnectInfo};
use axum::middleware::Next;
use axum::response::Response;
use std::net::SocketAddr;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use tracing::{Instrument, Level, span};

#[derive(Default)]
pub struct LoopbackIpFilterService;

impl LoopbackIpFilterService {
    pub async fn layer(
        ConnectInfo(addr): ConnectInfo<SocketAddr>,
        mut request: Request,
        next: Next,
    ) -> Result<Response, ErrorStatus> {
        if addr.ip().is_loopback() {
            let request_context = RequestContext::with(
                AccessTokenId::default(),
                UserId::system(),
                RoleId::sys_admin(),
            );
            request.extensions_mut().insert(request_context);

            let log_span = span!(Level::INFO, "loopback_request");
            let future = next.run(request).instrument(log_span);
            Ok(future.await)
        } else {
            Err(TdError::from(UriFilterError::Unauthorized))?
        }
    }
}

#[td_error]
enum UriFilterError {
    #[error("Unauthorized")]
    Unauthorized = 4000,
}
