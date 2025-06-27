//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::services::cert_download::CertDownloadService;
use crate::auth::services::login::LoginService;
use crate::auth::services::logout::LogoutService;
use crate::auth::services::password_change::PasswordChangeService;
use crate::auth::services::refresh::RefreshService;
use crate::auth::services::role_change::RoleChangeService;
use crate::auth::services::user_info::UserInfoService;
use crate::auth::session::Sessions;
use argon2::{Argon2, Params, PasswordHasher, Version};
use getset::Getters;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use td_common::id;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, UpdateRequest};
use td_objects::sql::DaoQueries;
use td_objects::types::auth::UserInfo;
use td_objects::types::auth::{Login, PasswordChange, RoleChange, TokenResponseX};
use td_objects::types::basic::RefreshToken;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::service_provider::TdBoxService;

mod cert_download;
mod login;
mod logout;
mod password_change;
mod refresh;
mod role_change;
mod user_info;

#[derive(Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct PasswordHashConfig {
    algorithm: String,
    version: usize,
    memory_cost_mib: usize,
    time_cost: usize,
    parallelism_cost: usize,
}

impl PasswordHashConfig {
    pub fn new(
        algorithm: &str,
        version: usize,
        memory_cost_mib: usize,
        time_cost: usize,
        parallelism_cost: usize,
    ) -> Self {
        Self {
            algorithm: algorithm.into(),
            version,
            memory_cost_mib,
            time_cost,
            parallelism_cost,
        }
    }

    pub fn hasher(&self) -> impl PasswordHasher {
        Argon2::new(
            argon2::Algorithm::from_str(&self.algorithm)
                .unwrap_or_else(|_| panic!("Invalid configuration: unknown password hashing algorithm {}. Valid values: argon2d, argon2i, argon2id (default)", self.algorithm)),
            Version::try_from(self.version as u32)
                .unwrap_or_else(|_| panic!("Invalid configuration: unknown password hashing version {}. Valid values: 16, 19 (default)", self.version)),
            Params::new(
                self.memory_cost_mib as u32,
                self.time_cost as u32,
                self.parallelism_cost as u32,
                None,
            )
                .unwrap(),
        )
    }
}

impl Default for PasswordHashConfig {
    fn default() -> Self {
        PasswordHashConfig::new("argon2d", 19, 65536, 4, 1)
    }
}

#[derive(Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct JwtConfig {
    secret: Option<String>,
    access_token_expiration: i64,
    #[serde(skip)]
    #[getset(skip)]
    encoding_key: Option<EncodingKey>,
    #[serde(skip)]
    #[getset(skip)]
    decoding_key: Option<DecodingKey>,
    #[serde(skip)]
    validation: Validation,
}

impl JwtConfig {
    pub fn new(secret: String, access_token_expiration: i64) -> Self {
        let encoding_key = Some(EncodingKey::from_secret(secret.as_bytes()));
        let decoding_key = Some(DecodingKey::from_secret(secret.as_bytes()));
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 5;
        validation.set_required_spec_claims(&["jti", "exp"]);

        Self {
            secret: Some(secret),
            access_token_expiration,
            encoding_key,
            decoding_key,
            validation,
        }
    }

    pub fn encoding_key(&self) -> &EncodingKey {
        self.encoding_key.as_ref().unwrap()
    }

    pub fn decoding_key(&self) -> &DecodingKey {
        self.decoding_key.as_ref().unwrap()
    }
}

impl Default for JwtConfig {
    fn default() -> Self {
        const EXPIRATION: i64 = 3600;
        JwtConfig::new(id::id().to_string(), EXPIRATION)
    }
}

pub struct AuthServices {
    jwt_settings: Arc<JwtConfig>,
    sessions: Arc<Sessions<'static>>,
    login: LoginService,
    refresh: RefreshService,
    logout: LogoutService,
    user_info: UserInfoService,
    role_change: RoleChangeService,
    password_change: PasswordChangeService,
    cert_download: CertDownloadService,
}

impl AuthServices {
    pub fn new(
        db: &DbPool,
        sessions: impl Into<Arc<Sessions<'static>>>,
        password_settings: impl Into<PasswordHashConfig>,
        jwt_settings: impl Into<JwtConfig>,
        ssl_folder: impl Into<PathBuf>,
    ) -> Self {
        let queries = Arc::new(DaoQueries::default());
        let sessions = sessions.into();
        let password_settings = Arc::new(password_settings.into());
        let jwt_settings = Arc::new(jwt_settings.into());
        let ssl_folder = Arc::new(ssl_folder.into());
        Self {
            jwt_settings: jwt_settings.clone(),
            sessions: sessions.clone(),
            login: LoginService::new(
                db.clone(),
                queries.clone(),
                jwt_settings.clone(),
                sessions.clone(),
            ),
            refresh: RefreshService::new(
                db.clone(),
                queries.clone(),
                jwt_settings.clone(),
                sessions.clone(),
            ),
            logout: LogoutService::new(db.clone(), queries.clone(), sessions.clone()),
            user_info: UserInfoService::new(db.clone(), queries.clone()),
            role_change: RoleChangeService::new(
                db.clone(),
                queries.clone(),
                jwt_settings.clone(),
                sessions.clone(),
            ),
            password_change: PasswordChangeService::new(
                db.clone(),
                queries.clone(),
                password_settings.clone(),
                sessions.clone(),
            ),
            cert_download: CertDownloadService::new(ssl_folder),
        }
    }

    pub async fn login_service(&self) -> TdBoxService<Login, TokenResponseX, TdError> {
        self.login.service().await
    }

    pub async fn refresh_service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), RefreshToken>, TokenResponseX, TdError> {
        self.refresh.service().await
    }

    pub async fn logout_service(&self) -> TdBoxService<UpdateRequest<(), ()>, (), TdError> {
        self.logout.service().await
    }

    pub async fn user_info_service(&self) -> TdBoxService<ReadRequest<()>, UserInfo, TdError> {
        self.user_info.service().await
    }

    pub async fn role_change_service(
        &self,
    ) -> TdBoxService<UpdateRequest<(), RoleChange>, TokenResponseX, TdError> {
        self.role_change.service().await
    }

    pub async fn password_change_service(&self) -> TdBoxService<PasswordChange, (), TdError> {
        self.password_change.service().await
    }

    pub async fn cert_download(&self) -> TdBoxService<(), BoxedSyncStream, TdError> {
        self.cert_download.service().await
    }

    pub fn jwt_settings(&self) -> &JwtConfig {
        &self.jwt_settings
    }

    pub fn sessions(&self) -> &Sessions {
        &self.sessions
    }
}

#[cfg(test)]
pub mod tests {
    use crate::auth::services::{AuthServices, JwtConfig, PasswordHashConfig};
    use crate::auth::session;
    use crate::auth::session::Sessions;
    use sqlx::FromRow;
    use std::path::PathBuf;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::types::auth::SessionDB;
    use td_objects::types::basic::AccessTokenId;

    pub async fn auth_services(db: &DbPool) -> AuthServices {
        let sessions: Arc<Sessions> = Arc::new(session::new(db.clone()));
        let password_hash_config: PasswordHashConfig = PasswordHashConfig::default();
        let jwt_config: JwtConfig = JwtConfig::default();
        let ssl_folder = PathBuf::default();
        AuthServices::new(db, sessions, password_hash_config, jwt_config, ssl_folder)
    }

    pub async fn assert_session(db: &DbPool, access_token_id: &Option<AccessTokenId>) {
        #[derive(Debug, FromRow)]
        struct Count {
            count: i64,
        }

        let mut conn = db.acquire().await.unwrap();

        match access_token_id {
            Some(access_token_id) => {
                let count: Count = sqlx::query_as(
                    "SELECT count(*) as count FROM sessions WHERE access_token_id = ?",
                )
                .bind(access_token_id)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
                assert_eq!(count.count, 1);
            }
            None => {
                let count: Count = sqlx::query_as("SELECT count(*) as count FROM sessions")
                    .fetch_one(&mut *conn)
                    .await
                    .unwrap();
                assert_eq!(count.count, 0);
            }
        }
    }

    pub async fn get_session(db: &DbPool, access_token_id: &AccessTokenId) -> Option<SessionDB> {
        let mut conn = db.acquire().await.unwrap();
        let session: Option<SessionDB> =
            sqlx::query_as("SELECT * FROM sessions WHERE access_token_id = ?")
                .bind(access_token_id)
                .fetch_optional(&mut *conn)
                .await
                .unwrap();
        session
    }
}
