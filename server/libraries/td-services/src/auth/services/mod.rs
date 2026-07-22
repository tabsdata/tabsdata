//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::services::cert_download::CertDownloadService;
use crate::auth::services::login::LoginService;
use crate::auth::services::logout::LogoutService;
use crate::auth::services::password_change::PasswordChangeService;
use crate::auth::services::refresh::RefreshService;
use crate::auth::services::user_info::UserInfoService;
use ta_services::factory::ServiceFactory;

mod cert_download;
mod login;
mod logout;
mod password_change;
mod refresh;
mod user_info;

#[derive(ServiceFactory)]
pub struct AuthServices {
    pub login: LoginService,
    pub refresh: RefreshService,
    pub logout: LogoutService,
    pub user_info: UserInfoService,
    pub password_change: PasswordChangeService,
    pub cert_download: CertDownloadService,
}

#[cfg(test)]
mod tests {
    use sea_query::{Asterisk, Expr, ExprTrait, Func, Query};
    use td_database::pool::TdPool;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_objects::DaoCore;
    use td_objects::SeaValue;
    use td_objects::dxo::auth::{SessionDB, SessionDBIden};
    use td_objects::types::basic::AccessTokenId;

    pub async fn assert_session(db: &TdPool, access_token_id: &Option<AccessTokenId>) {
        match access_token_id {
            Some(access_token_id) => {
                let count: i64 = Query::select()
                    .expr(Func::count(Expr::col(Asterisk)))
                    .from(SessionDB::table())
                    .cond_where(
                        Expr::col(SessionDBIden::AccessTokenId).eq(access_token_id.as_sea_value()),
                    )
                    .build_query_scalar()
                    .fetch_one(db)
                    .await
                    .unwrap();
                assert_eq!(count, 1);
            }
            None => {
                let count: i64 = Query::select()
                    .expr(Func::count(Expr::col(Asterisk)))
                    .from(SessionDB::table())
                    .build_query_scalar()
                    .fetch_one(db)
                    .await
                    .unwrap();
                assert_eq!(count, 0);
            }
        }
    }
}
