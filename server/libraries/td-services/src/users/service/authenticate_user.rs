//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::layers::{
    auth_user_authenticate, auth_user_create_jwt, auth_user_extract_password_hash,
    auth_user_extract_req_password, auth_user_validate_enabled,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::dlo::UserName;
use td_objects::jwt::jwt_logic::{JwtLogic, TokenResponse};
use td_objects::tower_service::extractor::{extract_user_id, extract_user_name};
use td_objects::tower_service::finder::find_by_name;
use td_objects::users::dao::User;
use td_objects::users::dto::AuthenticateRequest;
use td_tower::default_services::{
    ConnectionProvider, ServiceEntry, ServiceReturn, Share, SrvCtxProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct AuthenticateUserService {
    provider: ServiceProvider<AuthenticateRequest, TokenResponse, TdError>,
}

impl AuthenticateUserService {
    pub fn new(db: DbPool, jwt_logic: Arc<JwtLogic>) -> Self {
        AuthenticateUserService {
            provider: Self::provider(db, jwt_logic),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        jwt_logic: Arc<JwtLogic>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(SrvCtxProvider::new(jwt_logic))
            .layer(from_fn(extract_user_name::<AuthenticateRequest>))
            .layer(from_fn(find_by_name::<UserName, User>))
            .layer(from_fn(auth_user_validate_enabled))
            .layer(from_fn(auth_user_extract_password_hash))
            .layer(from_fn(auth_user_extract_req_password))
            .layer(from_fn(auth_user_authenticate))
            .layer(from_fn(extract_user_id::<User>))
            .layer(from_fn(auth_user_create_jwt))
            //TODO persist token(s) to db
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<AuthenticateRequest, TokenResponse, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_authenticate_provider() {
        use crate::users::layers::{
            auth_user_authenticate, auth_user_create_jwt, auth_user_extract_password_hash,
            auth_user_extract_req_password, auth_user_validate_enabled,
        };
        use crate::users::service::authenticate_user::AuthenticateUserService;
        use chrono::Duration;
        use std::sync::Arc;
        use td_objects::dlo::UserName;
        use td_objects::jwt::jwt_logic::{JwtLogic, TokenResponse};
        use td_objects::tower_service::extractor::extract_user_id;
        use td_objects::tower_service::extractor::extract_user_name;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::users::dao::User;
        use td_objects::users::dto::AuthenticateRequest;
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let jwt_logic = Arc::new(JwtLogic::new(
            "SECRET",
            Duration::seconds(3600),
            Duration::seconds(7200),
        ));

        let provider = AuthenticateUserService::provider(db, jwt_logic);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<AuthenticateRequest, TokenResponse>(&[
            type_of_val(&extract_user_name::<AuthenticateRequest>),
            type_of_val(&find_by_name::<UserName, User>),
            type_of_val(&auth_user_validate_enabled),
            type_of_val(&auth_user_extract_password_hash),
            type_of_val(&auth_user_extract_req_password),
            type_of_val(&auth_user_authenticate),
            type_of_val(&extract_user_id::<User>),
            type_of_val(&auth_user_create_jwt),
        ]);
    }
}
