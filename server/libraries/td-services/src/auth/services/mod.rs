//
// Copyright 2025 Tabsdata Inc.
//

mod cert_download;
mod login;
mod logout;
mod password_change;
mod refresh;
mod user_info;

#[cfg(test)]
mod tests {
    use td_database::pool::TdPool;
    use td_objects::dxo::auth::SessionDB;
    use td_objects::types::basic::AccessTokenId;
    use td_queries::Queries;

    pub async fn assert_session(db: &TdPool, access_token_id: &Option<AccessTokenId>) {
        match access_token_id {
            Some(access_token_id) => {
                let session = db
                    .fetch_optional::<SessionDB>(access_token_id)
                    .await
                    .unwrap();
                assert!(session.is_some());
            }
            None => {
                let sessions = db.fetch_all::<SessionDB>(()).await.unwrap();
                assert!(sessions.is_empty());
            }
        }
    }
}
