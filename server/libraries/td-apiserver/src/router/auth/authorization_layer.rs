//
// Copyright 2025. Tabs Data Inc.
//

use crate::status::error_status::AuthorizeErrorStatus;
use axum::extract::{Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use std::string::ToString;
use std::sync::Arc;
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::AccessTokenId;
use td_services::auth::services::AuthServices;
use td_services::auth::session::SessionProvider;
use td_services::auth::{decode_token, AuthError};

pub async fn authorization_layer(
    State(auth_services): State<Arc<AuthServices>>,
    request: Request,
    next: Next,
) -> Result<Response, AuthorizeErrorStatus> {
    // Check if the Authorization header is present
    let auth_header = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .ok_or(TdError::from(AuthError::MissingAuthorizationHeader))?;
    // .log_debug(|res| "Request does not have authorization header".to_string())
    // .log_ok_trace(|ok| "Request does not have authorization header".to_string())
    // .log_err_warn(|err| "Request does not have authorization header".to_string())?;

    // Check if the Authorization header is valid
    let auth_header = auth_header.to_str().map_err(|_| {
        TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It must be a string".to_string(),
        ))
    })?;
    //        .log_warn_err(|_| format!("Invalid authorization header: {:?}", auth_header))?;
    // Check if the Authorization header is a Bearer token
    let auth_header: Vec<_> = auth_header.split_whitespace().collect();
    if auth_header.len() != 2 {
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It should be 2 words: Bearer <ACCESS_TOKEN>".to_string(),
        )))?;
        //        .log_err_warn(|e| e.to_string())?;
    }
    if auth_header[0] != "Bearer" {
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "Not a Bearer token".to_string(),
        )))?;
        //        .log_err_warn(|e| e.to_string())?;
    }
    let token = auth_header[1];

    // Check if the token is valid
    let token = decode_token(&auth_services.jwt_settings(), token)
        //        .log_err_warn(|e| e.to_string())
        .map_err(|_| TdError::from(AuthError::AuthenticationFailed))?;
    let access_token_id: AccessTokenId = token.jti().into();

    // Get user_id/role_id from session
    let session = auth_services
        .sessions()
        .get_session(None, &access_token_id)
        .await
        //        .log_err_warn(ToString::to_string)
        .map_err(|_| TdError::from(AuthError::AuthenticationFailed))?;

    // Insert the context into the request extensions
    let request_context = RequestContext::with(
        session.access_token_id(),
        session.user_id(),
        session.role_id(),
        true, // TODO TD-273
    );
    let mut request = request;
    request.extensions_mut().insert(request_context);

    // Let the request continue
    Ok(next.run(request).await)
}
