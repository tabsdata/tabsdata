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
use getset::Getters;
use td_tower::ServiceFactory;

mod cert_download;
mod login;
mod logout;
mod password_change;
mod refresh;
mod role_change;
mod user_info;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct AuthServices {
    login: LoginService,
    refresh: RefreshService,
    logout: LogoutService,
    user_info: UserInfoService,
    role_change: RoleChangeService,
    password_change: PasswordChangeService,
    cert_download: CertDownloadService,
}

#[cfg(test)]
mod tests {
    use sqlx::FromRow;
    use td_database::sql::DbPool;
    use td_objects::types::auth::SessionDB;
    use td_objects::types::basic::AccessTokenId;

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
