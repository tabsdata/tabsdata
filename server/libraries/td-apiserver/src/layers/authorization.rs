//
// Copyright 2025. Tabs Data Inc.
//

use axum::extract::{Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use std::string::ToString;
use std::sync::Arc;
use ta_apiserver::status::error_status::ErrorStatus;
use td_common::jwt::{JwtConfig, decode_token};
use td_authz::session_provider::{Session, SessionError, Sessions};
use td_common::provider::Provider;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessToken, AccessTokenId};
use td_services::auth::AuthError;
use tracing::{Instrument, Level, Span, error, span, warn};

pub async fn authorization_layer(
    State(db): State<TdPool>,
    State(jwt_config): State<Arc<JwtConfig>>,
    State(sessions): State<Arc<Sessions>>,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    // Check if the Authorization header is present
    let auth_header = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .ok_or(TdError::from(AuthError::MissingAuthorizationHeader))?;

    // Check if the Authorization header is valid
    let auth_header = auth_header.to_str().map_err(|e| {
        warn!("Invalid authorization header: {e:#?}");
        TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It must be a string".to_string(),
        ))
    })?;
    // Check if the Authorization header is a Bearer token
    let auth_header: Vec<_> = auth_header.split_whitespace().collect();
    if auth_header.len() != 2 {
        warn!(
            "Invalid authorization header, not 2 words: {:?}",
            auth_header
        );
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It should be 2 words: Bearer <ACCESS_TOKEN>".to_string(),
        )))?;
    }
    if auth_header[0] != "Bearer" {
        warn!(
            "Invalid authorization header, not a Bearer token: {:?}",
            auth_header
        );
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "Not a Bearer token".to_string(),
        )))?;
    }
    let access_token = AccessToken::try_from(auth_header[1])?;

    // Check if the token is valid
    let token = decode_token(&jwt_config, access_token.as_str()).map_err(|e| {
        error!("Could not decode token: {e:#?}");
        TdError::from(AuthError::AuthenticationFailed)
    })?;
    let access_token_id: AccessTokenId = token.jti.into();

    // Get user_id from session
    let session = sessions
        .get(&db)
        .await
        .map_err(|e| {
            error!("Could not get session: {e:#?}");
            TdError::from(AuthError::AuthenticationFailed)
        })?
        .get(&access_token_id)
        .cloned()
        .ok_or_else(|| TdError::from(SessionError::NotFound(access_token_id)))
        .map_err(|e| {
            error!("Could not get session: {e:#?}");
            TdError::from(AuthError::AuthenticationFailed)
        })?;

    // The target project (tenant) is resolved later by `project_resolver_layer`
    // — but only on the tenant subtree (`/projects/{project}/...`). For global
    // routes (auth, users, roles, projects CRUDL, server status, …) the project
    // stays `None` here, and the connection runs with RLS bypassed, which is
    // fine because those routes don't touch project-scoped tables.
    let request_context = RequestContext::with(&session.access_token_id, &session.user_id);
    let mut request = request;
    request.extensions_mut().insert(request_context);
    request.extensions_mut().insert(access_token);

    // Let the request continue
    // TODO this could be a separate layer
    let log_span = log_span(&session);
    let future = next.run(request).instrument(log_span);
    Ok(future.await)
}

fn log_span(session: &Session) -> Span {
    span!(
        Level::INFO,
        "authorized",
        user_id = %session.user_id,
        user_name = %session.user_name,
        access_token_id = %session.access_token_id.log(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use td_objects::Buildable;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, RefreshTokenId, SessionStatus, UserId, UserName,
    };
    use tracing::info;
    use tracing::subscriber::set_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{fmt, registry};

    struct WriterGuard {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for WriterGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut lock = self.buffer.lock().unwrap();
            lock.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_auth_span() {
        // Collect logs in a buffer
        let logs = Arc::new(Mutex::new(Vec::new()));

        // Custom layer to capture span data
        let logs_clone = logs.clone();
        let layer = fmt::layer()
            .with_writer(move || WriterGuard {
                buffer: logs_clone.clone(),
            })
            .with_ansi(false)
            .with_level(true);

        let subscriber = registry().with(layer);
        let _guard = set_default(subscriber);

        // Create some logs
        let user_name = UserName::admin();
        let access_token_id = AccessTokenId::default();
        let user_id = UserId::admin();
        let session = Session::builder()
            .access_token_id(access_token_id)
            .refresh_token_id(RefreshTokenId::default())
            .user_id(user_id)
            .created_on(AtTime::default())
            .expires_on(AtTime::default())
            .status_change_on(AtTime::default())
            .status(SessionStatus::Active)
            .user_name(&user_name)
            .build()
            .unwrap();
        let log_span = log_span(&session);

        async {
            info!("This is a test log message");
        }
        .instrument(log_span)
        .await;

        // Inspect the logs
        let logs = logs.lock().unwrap().to_vec();
        let log_output = String::from_utf8_lossy(&logs);
        let trace_auth_id = access_token_id.log();
        assert!(log_output.contains(&format!(
            "authorized{{user_id={user_id} user_name={user_name} access_token_id={trace_auth_id}}}"
        )));
    }
}
