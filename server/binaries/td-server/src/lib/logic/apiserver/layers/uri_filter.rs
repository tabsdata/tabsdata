//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::apiserver::status::error_status::AuthorizeErrorStatus;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use axum_extra::extract::Host;
use std::net::ToSocketAddrs;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::RequestContext;

#[derive(Default)]
pub struct LoopbackIpFilterService;

impl LoopbackIpFilterService {
    pub async fn layer(
        Host(addr): Host,
        request: Request,
        next: Next,
    ) -> Result<Response, AuthorizeErrorStatus> {
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
            let request_context =
                RequestContext::with(td_security::ADMIN_USER, td_security::SYS_ADMIN_ROLE, true)
                    .await;
            let mut request = request;
            request.extensions_mut().insert(request_context);

            Ok(next.run(request).await)
        } else {
            Err(TdError::from(UriFilterError::Unauthorized).into())
        }
    }
}

#[td_error]
pub enum UriFilterError {
    #[error("Unauthorized")]
    Unauthorized = 4000,
}
