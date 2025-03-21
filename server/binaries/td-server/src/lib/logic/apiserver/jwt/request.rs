//
//  Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;

use crate::logic::apiserver::jwt::jwt_logic::JwtLogic;
use crate::logic::apiserver::jwt::token::EncodedToken;
use crate::logic::apiserver::status::error_status::AuthorizeErrorStatus;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::RequestContext;

/// Decodes JWT tokens from the Authorization header of incoming requests and sets claims in the
/// request extensions.
#[derive(Default)]
pub struct JwtDecoderService;

/// JWT Authorization state.
pub type JwtState = Arc<JwtLogic>;

impl JwtDecoderService {
    /// This method decodes the JWT token from the Authorization header of the request.
    pub async fn layer(
        State(jwt_state): State<JwtState>,
        request: Request,
        next: Next,
    ) -> Result<Response, AuthorizeErrorStatus> {
        // Check if the Authorization header is present
        let auth_header = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .ok_or(TdError::from(AuthorizeError::MissingAuthorizationHeader))?;

        // Check if the Authorization header is valid
        let auth_str = auth_header.to_str().map_err(|error| {
            TdError::from(AuthorizeError::InvalidAuthorizationHeader(
                error.to_string(),
            ))
        })?;

        // Check if the Authorization header is a Bearer token
        let mut header = auth_str.split_whitespace();
        let (_, token) = (
            header.next(),
            header
                .next()
                .ok_or(TdError::from(AuthorizeError::MissingAuthorizationToken))?,
        );

        // Check if the token is valid
        let token = EncodedToken::new(token);
        let token_claims = jwt_state
            .authenticate_access(token)
            .map_err(|error| TdError::from(AuthorizeError::InvalidGrant(error.to_string())))?;

        // Insert the context into the request extensions
        let request_context = RequestContext::with(
            token_claims.sub(),
            token_claims.role(),
            true, // TODO TD-273
        )
        .await;
        let mut request = request;
        request.extensions_mut().insert(request_context);

        // Let the request continue
        Ok(next.run(request).await)
    }
}

#[td_error]
pub enum AuthorizeError {
    #[error("Missing Authorization Header")]
    MissingAuthorizationHeader = 0,
    #[error("Invalid Authorization Header: {0}")]
    InvalidAuthorizationHeader(String) = 1,
    #[error("Missing Authorization Token")]
    MissingAuthorizationToken = 2,

    #[error("Invalid Grant: {0}")]
    InvalidGrant(String) = 4000,
}

#[cfg(test)]
mod tests {
    use axum::middleware::from_fn_with_state;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use http::header::AUTHORIZATION;
    use tower::ServiceExt;

    use crate::logic::apiserver::jwt::jwt_logic::tests::test_jwt_logic;

    use super::*;

    fn jwt_test_state() -> JwtState {
        JwtState::new(test_jwt_logic())
    }

    async fn handler() -> &'static str {
        "Hello, World!"
    }

    fn app() -> Router {
        Router::new()
            .route("/", get(handler))
            .layer(from_fn_with_state(
                jwt_test_state(),
                JwtDecoderService::layer,
            ))
    }

    #[tokio::test]
    async fn test_missing_authorization_header() {
        let app = app();

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_authorization_header_format() {
        let app = app();

        let req = Request::builder()
            .uri("/")
            .header(AUTHORIZATION, "InvalidFormat")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_invalid_bearer_token() {
        let app = app();

        let req = Request::builder()
            .uri("/")
            .header(AUTHORIZATION, "Bearer invalidtoken")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
