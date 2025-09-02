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
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::{AccessToken, AccessTokenId};
use td_services::auth::AuthError;
use td_services::auth::jwt::{JwtConfig, decode_token};
use td_services::auth::session::{Session, SessionError, SessionProvider, Sessions};
use tracing::{Instrument, Level, Span, error, span};

pub async fn authorization_layer(
    State(db): State<DbPool>,
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
    // .log_debug(|res| "Request does not have authorization header".to_string())
    // .log_ok_trace(|ok| "Request does not have authorization header".to_string())
    // .log_err_warn(|err| "Request does not have authorization header".to_string())?;

    // Check if the Authorization header is valid
    let auth_header = auth_header.to_str().map_err(|e| {
        error!("Invalid authorization header: {}", e);
        TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It must be a string".to_string(),
        ))
    })?;
    //        .log_warn_err(|_| format!("Invalid authorization header: {:?}", auth_header))?;
    // Check if the Authorization header is a Bearer token
    let auth_header: Vec<_> = auth_header.split_whitespace().collect();
    if auth_header.len() != 2 {
        error!(
            "Invalid authorization header, not 2 words: {:?}",
            auth_header
        );
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It should be 2 words: Bearer <ACCESS_TOKEN>".to_string(),
        )))?;
        //        .log_err_warn(|e| e.to_string())?;
    }
    if auth_header[0] != "Bearer" {
        error!(
            "Invalid authorization header, not a Bearer token: {:?}",
            auth_header
        );
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "Not a Bearer token".to_string(),
        )))?;
        //        .log_err_warn(|e| e.to_string())?;
    }
    let access_token = AccessToken::try_from(auth_header[1])?;

    // Check if the token is valid
    let token = decode_token(&jwt_config, access_token.as_str())
        //        .log_err_warn(|e| e.to_string())
        .map_err(|e| {
            error!("Could not decode token: {}", e);
            TdError::from(AuthError::AuthenticationFailed)
        })?;
    let access_token_id: AccessTokenId = token.jti().into();

    // Get user_id/role_id from session
    let mut conn = db
        .acquire()
        .await
        .map_err(|e| TdError::from(SessionError::CouldNotGetDbConn(e)))?;
    let session = sessions
        .get_session(&mut conn, &access_token_id)
        .await
        //        .log_err_warn(ToString::to_string)
        .map_err(|e| {
            error!("Could not get session: {}", e);
            TdError::from(AuthError::AuthenticationFailed)
        })?;

    // Insert the context into the request extensions
    let request_context = RequestContext::with(
        session.access_token_id(),
        session.user_id(),
        session.role_id(),
    );
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
        user_name = %session.user_name(),
        role_name = %session.role_name(),
        access_token_id = %session.access_token_id().log(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use td_objects::types::basic::{
        AtTime, RefreshTokenId, RoleId, RoleName, SessionStatus, UserId, UserName,
    };
    use td_services::auth::session::Session;
    use tracing::info;
    use tracing::subscriber::set_default;
    use tracing_subscriber::{fmt, layer::SubscriberExt, registry};

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
        let role_name = RoleName::sys_admin();
        let user_name = UserName::admin();
        let access_token_id = AccessTokenId::default();
        let session = Session::builder()
            .access_token_id(access_token_id)
            .refresh_token_id(RefreshTokenId::default())
            .user_id(UserId::admin())
            .role_id(RoleId::sys_admin())
            .created_on(AtTime::default())
            .expires_on(AtTime::default())
            .status_change_on(AtTime::default())
            .status(SessionStatus::Active)
            .user_name(&user_name)
            .role_name(&role_name)
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
            "authorized{{user_name={user_name} role_name={role_name} access_token_id={trace_auth_id}}}"
        )));
    }
}
