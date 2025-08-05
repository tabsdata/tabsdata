//
// Copyright 2024 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::NoPermissions;
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::basic::VisibleCollections;
use td_objects::types::collection::CollectionRead;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = ListCollectionsService,
    request = ListRequest<()>,
    response = ListResponse<CollectionRead>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Authz::<NoPermissions>::visible_collections),
        from_fn(By::<()>::list::<(), VisibleCollections, DaoQueries, CollectionRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::seed_user_role;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, RoleId, RoleName, UserEnabled, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            ListCollectionsService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<CollectionRead>>(&[
            type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
            type_of_val(&Authz::<NoPermissions>::visible_collections),
            type_of_val(&By::<()>::list::<(), VisibleCollections, DaoQueries, CollectionRead>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());

        let service = ListCollectionsService::new(
            db,
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len(), &1);
        assert_eq!(*list.data()[0].name(), name);
    }

    #[td_test::test(sqlx)]
    async fn test_list_collection_unauthorized(db: DbPool) -> Result<(), TdError> {
        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());

        // Create new role without permissions
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin")?,
            &UserEnabled::from(true),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("unauthorized_role")?,
            Description::try_from("any user")?,
        )
        .await;
        let _user_role = seed_user_role(&db, user.id(), role.id()).await;

        // Create a collection
        let name = CollectionName::try_from("ds0")?;
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        // All collections are visible to authorized users
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response?;
        assert_eq!(*list.len(), 1);
        assert_eq!(*list.data()[0].name(), name);

        // No collections are visible to unauthorized users
        let request = RequestContext::with(AccessTokenId::default(), user.id(), role.id())
            .list((), ListParams::default());

        let service =
            ListCollectionsService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response?;
        assert_eq!(*list.len(), 0);
        Ok(())
    }
}
