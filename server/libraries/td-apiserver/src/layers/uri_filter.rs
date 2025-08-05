//
//  Copyright 2024 Tabs Data Inc.
//

use crate::status::error_status::ErrorStatus;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use axum_extra::extract::Host;
use std::net::ToSocketAddrs;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use tracing::{span, Instrument, Level};

#[derive(Default)]
pub struct LoopbackIpFilterService;

impl LoopbackIpFilterService {
    pub async fn layer(
        Host(addr): Host,
        request: Request,
        next: Next,
    ) -> Result<Response, ErrorStatus> {
        let is_loopback = match addr.to_socket_addrs() {
            Ok(mut addrs) => {
                // Check if all resolved IP addresses are loopback addresses
                addrs.all(|socket_addr| socket_addr.ip().is_loopback())
            }
            Err(_) => false,
        };

        if is_loopback {
            // TODO create internal user and role
            // Insert the context into the request extensions.
            let request_context = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
            );
            let mut request = request;
            request.extensions_mut().insert(request_context);

            let log_span = span!(Level::INFO, "authorized_internal");
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
