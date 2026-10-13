//
// Copyright 2025 Tabsdata Inc.
//

use crate::apiserver::BASE_URL;
use axum::extract::{MatchedPath, Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use std::string::ToString;
use ta_apiserver::router::{Auth, accepts};
use ta_apiserver::status::error_status::ErrorStatus;
use ta_services::service::Ctx;
use td_authz::rbac::enforce::RbacError;
use td_authz::session_provider::{Session, SessionError, active_session};
use td_authz::token_provider::ServiceTokens;
use td_common::jwt::{JwtConfig, decode_token};
use td_database::access::{DbPool, GatedConnection, Global, Grant, RO, retrying};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::service_token::ServiceTokenDBWithNames;
use td_objects::types::basic::{AccessToken, AccessTokenId, TokenDigest};
use td_security::token;
use td_services::auth::AuthError;
use tracing::{Instrument, Level, Span, debug, error, span, warn};

/// Reads the bearer and hands it to the credential's own path, told apart by
/// prefix so neither falls through to the other. Both paths run the same
/// steps: route gate, credential lookup, acceptance, request context.
pub async fn authorization_layer(
    State(db): State<DbPool<Global>>,
    State(jwt_config): State<Ctx<JwtConfig>>,
    State(tokens): State<Ctx<ServiceTokens>>,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    let bearer = bearer(&request)?;
    if bearer.starts_with(token::PREFIX) {
        service_token(db, tokens, bearer, request, next).await
    } else {
        session(db, jwt_config, bearer, request, next).await
    }
}

fn bearer(request: &Request) -> Result<String, TdError> {
    let auth_header = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .ok_or(TdError::from(AuthError::MissingAuthorizationHeader))?;
    let auth_header = auth_header.to_str().map_err(|e| {
        warn!("Invalid authorization header: {e:#?}");
        TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It must be a string".to_string(),
        ))
    })?;
    let words: Vec<_> = auth_header.split_whitespace().collect();
    if words.len() != 2 {
        warn!("Invalid authorization header, not 2 words: {words:?}");
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "It should be 2 words: Bearer <ACCESS_TOKEN>".to_string(),
        )))?;
    }
    if words[0] != "Bearer" {
        warn!("Invalid authorization header, not a Bearer token: {words:?}");
        Err(TdError::from(AuthError::InvalidAuthorizationHeaderValue(
            "Not a Bearer token".to_string(),
        )))?;
    }
    Ok(words[1].to_string())
}

async fn session(
    db: DbPool<Global>,
    jwt_config: Ctx<JwtConfig>,
    bearer: String,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    if !route_accepts(&request, Auth::Session) {
        Err(TdError::from(RbacError::SessionUnsupported))?
    }

    let access_token = AccessToken::try_from(bearer.as_str())?;
    let token = decode_token(&jwt_config, access_token.as_str()).map_err(|e| {
        error!("Could not decode token: {e:#?}");
        TdError::from(AuthError::AuthenticationFailed)
    })?;
    let access_token_id: AccessTokenId = token.jti.into();

    // The database failing is the server's error, not the token's: a 401
    // would tell the client to discard a valid token.
    let session = {
        let mut conn = db.read().await?;
        active_session(
            conn.get(Grant::asserted("session authentication")),
            &access_token_id,
        )
        .await?
    };
    let session = session.ok_or_else(|| {
        warn!("{}", SessionError::NotFound(access_token_id));
        TdError::from(AuthError::AuthenticationFailed)
    })?;

    // The project, if any, is resolved later by `project_resolver_layer`.
    let context = RequestContext::with(session.base.access_token_id, session.base.user_id);
    let mut request = request;
    request.extensions_mut().insert(context);
    request.extensions_mut().insert(access_token);

    let response = next.run(request).instrument(session_span(&session)).await;
    Ok(response)
}

/// Every authentication failure is `AuthenticationFailed`, as for a session.
async fn service_token(
    db: DbPool<Global>,
    tokens: Ctx<ServiceTokens>,
    bearer: String,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    if !route_accepts(&request, Auth::ServiceToken) {
        Err(TdError::from(RbacError::ServiceTokenUnsupported))?
    }

    let presented = token::parse(&bearer).ok_or_else(|| {
        warn!("Malformed service token");
        TdError::from(AuthError::AuthenticationFailed)
    })?;
    let digest = TokenDigest::try_from(token::digest(presented)).map_err(|e| {
        error!("Could not build a token digest: {e:#?}");
        TdError::from(AuthError::AuthenticationFailed)
    })?;

    let token = {
        let mut conn = read_connection(&db).await?;
        tokens
            .find(
                conn.get(Grant::asserted("service token authentication")),
                &digest,
            )
            .await
            .map_err(|e| {
                error!("Could not look up a service token: {e:#?}");
                TdError::from(AuthError::AuthenticationFailed)
            })?
    };
    let token = token.ok_or_else(|| {
        warn!("Service token not recognised");
        TdError::from(AuthError::AuthenticationFailed)
    })?;

    let context = RequestContext::service_token(
        AccessTokenId::from(*token.base.id),
        token.base.user_id,
        token.base.scope,
    );
    // The request's instant decides expiry and stamps last use.
    let at = context.time;
    tokens.accept(&token, &at).map_err(|e| {
        warn!("Refusing a service token: {e:#?}");
        TdError::from(AuthError::AuthenticationFailed)
    })?;

    let mut request = request;
    request.extensions_mut().insert(context);

    let response = next
        .run(request)
        .instrument(service_token_span(&token))
        .await;

    // The interval is checked before a write connection is acquired.
    if tokens.due_for_touch(&token.base.id, **at) {
        let stamped = retrying(|attempt| async {
            let mut conn = db.begin(attempt).await?;
            let grant = Grant::asserted("service token last-used stamp");
            tokens.touch(conn.get(grant), &token.base.id, &at).await?;
            conn.commit().await?;
            Ok(())
        })
        .await;
        if let Err(e) = stamped {
            warn!("Could not stamp a service token's last use: {e:#?}");
        }
    }
    Ok(response)
}

async fn read_connection(db: &DbPool<Global>) -> Result<GatedConnection<RO>, TdError> {
    db.read().await.map_err(|e| {
        error!("Could not get a read connection: {e:#?}");
        TdError::from(AuthError::AuthenticationFailed)
    })
}

/// Both credentials log under one span name; `credential` tells them apart.
fn session_span(session: &Session) -> Span {
    span!(
        Level::INFO,
        "authorized",
        credential = "session",
        user_id = %session.base.user_id,
        user_name = %session.user_name,
        access_token_id = %session.base.access_token_id.log(),
    )
}

fn service_token_span(token: &ServiceTokenDBWithNames) -> Span {
    span!(
        Level::INFO,
        "authorized",
        credential = "service",
        user_id = %token.base.user_id,
        user_name = %token.user_name,
        slot = %token.base.slot,
        token_id = %token.base.id,
    )
}

/// Fails closed without a matched path, as the internal-token layer does.
fn route_accepts(request: &Request, auth: Auth) -> bool {
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string());
    let route = matched
        .as_deref()
        .and_then(|matched| matched.strip_prefix(BASE_URL));
    let method = request.method().as_str();
    let accepted = route.is_some_and(|route| accepts(method, route, auth));
    if !accepted {
        debug!(method = %method, route = ?route, ?auth, "the route does not accept this credential");
    }
    accepted
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::middleware::from_fn_with_state;
    use http::{Method, StatusCode};
    use std::io::{self, Write};
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
    use ta_apiserver::router::{RouterStates, named_router};
    use td_database::pool::TdPool;
    use td_objects::dxo::auth::{Login, SessionDB};
    use td_objects::dxo::service_token::{TokenCreate, TokenCreated, TokenSlotParam};
    use td_objects::dxo::user::{UserParam, UserUpdate};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, ExpiresInDays, Password, RefreshTokenId, SessionStatus, TokenSlot,
        UserEnabled, UserId, UserIdName, UserName,
    };
    use td_queries::test_utils::SeedEntry;
    use td_services::Services;
    use tower::ServiceExt;
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

    fn build(db: TdPool) -> (Router, Services) {
        let services = Services::with_defaults(db);
        let states = RouterStates::default().with(services.clone());
        let inner: Router = named_router(
            &[
                "list_collections",
                "create_user",
                "list_service_tokens",
                "read_own_service_token",
                "logout",
            ],
            &states,
        )
        .into();
        let inner = inner.layer(from_fn_with_state(services.clone(), authorization_layer));
        (Router::new().nest(BASE_URL, inner), services)
    }

    async fn call(router: &Router, method: Method, path: &str, bearer: &str) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("{BASE_URL}{path}"))
            .header(http::header::AUTHORIZATION, format!("Bearer {bearer}"));
        if path == "/users" {
            request = request.header("content-type", "application/json");
        }
        let body = if path == "/users" {
            Body::from(r#"{"name":"u1","full_name":"U1","password":"a real password"}"#)
        } else {
            Body::empty()
        };
        router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap()
    }

    async fn status(router: &Router, method: Method, path: &str, bearer: &str) -> StatusCode {
        call(router, method, path, bearer).await.status()
    }

    async fn mint(services: &Services, user: UserId, slot: i32) -> TokenCreated {
        let request = RequestContext::with(AccessTokenId::default(), user).create(
            TokenSlotParam {
                slot: TokenSlot::try_from(slot).unwrap(),
            },
            TokenCreate {
                expires_in_days: ExpiresInDays::NEVER,
                comment: None,
            },
        );
        services.create_service_token(request).await.unwrap().data
    }

    #[td_test::test(db)]
    async fn test_service_token_through_the_layer(db: TdPool) {
        db.seed().test_project().await.unwrap();
        let (router, services) = build(db.clone());
        let token = mint(&services, UserId::admin(), 1).await;
        let bearer = token.token.to_string();

        assert_eq!(
            status(&router, Method::GET, "/collections", &bearer).await,
            StatusCode::OK
        );

        let response = call(&router, Method::GET, "/auth/service-token", &bearer).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"]["user_name"], "admin");
        assert_eq!(body["data"]["slot"], 1);

        for (method, path) in [
            (Method::GET, "/auth/service-tokens"),
            (Method::POST, "/users"),
        ] {
            let response = call(&router, method, path, &bearer).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["code"], "RbacError::3001");
        }

        let unknown = format!("{}{}", token::PREFIX, "0".repeat(64));
        for bad in [
            format!("{}short", token::PREFIX),
            format!("{}{}", token::PREFIX, "x".repeat(64)),
            unknown,
        ] {
            assert_eq!(
                status(&router, Method::GET, "/collections", &bad).await,
                StatusCode::UNAUTHORIZED,
                "{bad}"
            );
        }

        let session = RequestContext::with(AccessTokenId::default(), UserId::admin());
        services
            .revoke_service_token(session.delete(TokenSlotParam { slot: token.slot }))
            .await
            .unwrap();
        assert_eq!(
            status(&router, Method::GET, "/collections", &bearer).await,
            StatusCode::UNAUTHORIZED
        );
    }

    /// A session on a route declared `auth(service_token)` is refused by the
    /// gate, before its credential is looked at.
    #[td_test::test(db)]
    async fn test_session_is_refused_on_a_service_token_only_route(db: TdPool) {
        db.seed().test_project().await.unwrap();
        let (router, services) = build(db.clone());
        let login = Login {
            name: UserName::admin(),
            password: Password::try_from("tabsdata").unwrap(),
        };
        let bearer = services
            .login(login)
            .await
            .unwrap()
            .data
            .access_token
            .to_string();

        assert_eq!(
            status(&router, Method::GET, "/collections", &bearer).await,
            StatusCode::OK
        );

        let response = call(&router, Method::GET, "/auth/service-token", &bearer).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "RbacError::3002");
    }

    /// The session is read on every request, so a logout takes effect on
    /// the very next one.
    #[td_test::test(db)]
    async fn test_a_logged_out_session_is_refused(db: TdPool) {
        db.seed().test_project().await.unwrap();
        let (router, services) = build(db.clone());
        let login = Login {
            name: UserName::admin(),
            password: Password::try_from("tabsdata").unwrap(),
        };
        let bearer = services
            .login(login)
            .await
            .unwrap()
            .data
            .access_token
            .to_string();
        assert_eq!(
            status(&router, Method::GET, "/collections", &bearer).await,
            StatusCode::OK
        );

        assert_eq!(
            status(&router, Method::POST, "/auth/logout", &bearer).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&router, Method::GET, "/collections", &bearer).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[td_test::test(db)]
    async fn test_disabling_the_owner_kills_its_tokens(db: TdPool) {
        let owner = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let (router, services) = build(db.clone());
        let bearer = mint(&services, owner.id, 1).await.token.to_string();
        assert_eq!(
            status(&router, Method::GET, "/auth/service-token", &bearer).await,
            StatusCode::OK
        );

        let disable = UserUpdate {
            full_name: None,
            email: None,
            password: None,
            enabled: Some(UserEnabled::FALSE),
        };
        services
            .update_user(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                    UserParam {
                        user: UserIdName::from_str("u0").unwrap(),
                    },
                    disable,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            status(&router, Method::GET, "/auth/service-token", &bearer).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn test_auth_span() {
        let logs = Arc::new(Mutex::new(Vec::new()));

        let logs_clone = logs.clone();
        let layer = fmt::layer()
            .with_writer(move || WriterGuard {
                buffer: logs_clone.clone(),
            })
            .with_ansi(false)
            .with_level(true);

        let subscriber = registry().with(layer);
        let _guard = set_default(subscriber);

        let user_name = UserName::admin();
        let access_token_id = AccessTokenId::default();
        let user_id = UserId::admin();
        let session = Session {
            base: SessionDB {
                access_token_id,
                refresh_token_id: RefreshTokenId::default(),
                user_id,
                created_on: AtTime::default().into(),
                expires_on: AtTime::default().into(),
                status_change_on: AtTime::default().into(),
                status: SessionStatus::Active,
            },
            user_name: user_name.clone(),
        };
        let log_span = session_span(&session);

        async {
            info!("This is a test log message");
        }
        .instrument(log_span)
        .await;

        let logs = logs.lock().unwrap().to_vec();
        let log_output = String::from_utf8_lossy(&logs);
        let trace_auth_id = access_token_id.log();
        assert!(log_output.contains(&format!(
            "authorized{{credential=\"session\" user_id={user_id} user_name={user_name} access_token_id={trace_auth_id}}}"
        )));
    }

    /// A valid token arrives while the database cannot be reached. That is the server's failure, not the
    /// token's: the request must fail with a server error the client can retry,
    /// not a 401 that tells it to discard the token and log in again.
    #[td_test::test(db)]
    async fn test_an_unreachable_database_is_not_reported_as_bad_credentials(db: TdPool) {
        let (router, services) = build(db.clone());
        db.seed()
            .user(&UserName::try_from("u_valid").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let token = services
            .clone()
            .login(Login {
                name: UserName::try_from("u_valid").unwrap(),
                password: Password::try_from("password").unwrap(),
            })
            .await
            .unwrap();
        db.close().await;

        let answered = status(
            &router,
            Method::GET,
            "/collections",
            token.access_token.as_str(),
        )
        .await;
        assert!(
            answered.is_server_error(),
            "a database outage answered {answered}"
        );
    }
}
