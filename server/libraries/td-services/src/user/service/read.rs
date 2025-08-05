//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::UserParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrUserId};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{UserId, UserIdName};
use td_objects::types::user::{UserDBWithNames, UserRead, UserReadBuilder};
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadUserService {
    provider: ServiceProvider<ReadRequest<UserParam>, UserRead, TdError>,
}

impl ReadUserService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        ReadUserService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ReadRequest<UserParam>>::extract::<RequestContext>),
                from_fn(With::<ReadRequest<UserParam>>::extract_name::<UserParam>),
                from_fn(With::<UserParam>::extract::<UserIdName>),
                from_fn(By::<UserIdName>::select::<DaoQueries, UserDBWithNames>),
                from_fn(With::<UserDBWithNames>::extract::<UserId>),

                from_fn(AuthzOn::<SystemOrUserId>::set),
                from_fn(Authz::<SecAdmin, Requester>::check),

                from_fn(With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
                from_fn(With::<UserReadBuilder>::build::<UserRead, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<UserParam>, UserRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::UserParam;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ReadUserService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<UserParam>, UserRead>(&[
            type_of_val(&With::<ReadRequest<UserParam>>::extract::<RequestContext>),
            type_of_val(&With::<ReadRequest<UserParam>>::extract_name::<UserParam>),
            type_of_val(&With::<UserParam>::extract::<UserIdName>),
            type_of_val(&By::<UserIdName>::select::<DaoQueries, UserDBWithNames>),
            type_of_val(&With::<UserDBWithNames>::extract::<UserId>),
            type_of_val(&AuthzOn::<SystemOrUserId>::set),
            type_of_val(&Authz::<SecAdmin, Requester>::check),
            type_of_val(&With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
            type_of_val(&With::<UserReadBuilder>::build::<UserRead, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_read_user(db: DbPool) {
        let user_name = UserName::try_from("u0").unwrap();
        let user = seed_user(&db, &user_name, &UserEnabled::from(true)).await;

        let service = ReadUserService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(AccessTokenId::default(), user.id(), RoleId::user())
            .read(
                UserParam::builder()
                    .try_user(user_name.to_string())
                    .unwrap()
                    .build()
                    .unwrap(),
            );
        let response = service.raw_oneshot(request).await;
        // assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.id(), user.id());
        assert_eq!(*created.name(), user_name);
    }
}
