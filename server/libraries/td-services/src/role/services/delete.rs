//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::permission::PermissionDB;
use td_objects::types::role::{RoleDB, UserRoleDB};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteRoleService {
    provider: ServiceProvider<DeleteRequest<RoleParam>, (), TdError>,
}

impl DeleteRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_name::<DeleteRequest<RoleParam>, _>),

                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                TransactionProvider::new(db),
                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),

                // Delete all permissions with that role
                from_fn(By::<RoleId>::delete::<DaoQueries, PermissionDB>),
                // Delete all user roles with that role
                from_fn(By::<RoleId>::delete::<DaoQueries, UserRoleDB>),

                // Delete the role
                from_fn(By::<RoleId>::delete::<DaoQueries, RoleDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<RoleParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_permission::seed_permission;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::seed_user_role;
    use td_objects::types::basic::{AccessTokenId, Description, PermissionType, RoleName, UserId};
    use td_objects::types::permission::PermissionDBWithNames;
    use td_objects::types::role::UserRoleDBWithNames;
    use td_objects::types::{IdOrName, SqlEntity};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = DeleteRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<RoleParam>, ()>(&[
            type_of_val(&extract_req_name::<DeleteRequest<RoleParam>, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::delete::<DaoQueries, PermissionDB>),
            type_of_val(&By::<RoleId>::delete::<DaoQueries, UserRoleDB>),
            type_of_val(&By::<RoleId>::delete::<DaoQueries, RoleDB>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_role_by_id(db: DbPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::try_from(format!("~{}", hero.id()))?).await
    }

    #[td_test::test(sqlx)]
    async fn test_delete_role_by_name(db: DbPool) -> Result<(), TdError> {
        let (hero, _villain) = setup_roles(&db).await;
        test_delete_role(&db, RoleIdName::try_from(format!("{}", hero.name()))?).await
    }

    async fn setup_roles(db: &DbPool) -> (RoleDB, RoleDB) {
        // Users
        let user_id = seed_user(db, None, "joaquin", true).await;

        // Roles
        let hero_role = seed_role(
            db,
            RoleName::try_from("hero").unwrap(),
            Description::try_from("Hero Role").unwrap(),
        )
        .await;
        let villain_role = seed_role(
            db,
            RoleName::try_from("villain").unwrap(),
            Description::try_from("Villain Role").unwrap(),
        )
        .await;

        // User Roles
        let _user_hero_role = seed_user_role(db, &UserId::from(user_id), hero_role.id()).await;
        let _user_hero_role = seed_user_role(db, &UserId::from(user_id), villain_role.id()).await;

        // Permissions
        let _hero_permissions = seed_permission(
            db,
            PermissionType::try_from("cR").unwrap(),
            None,
            None,
            &hero_role,
        )
        .await;
        let _villain_permissions = seed_permission(
            db,
            PermissionType::try_from("cR").unwrap(),
            None,
            None,
            &villain_role,
        )
        .await;

        (hero_role, villain_role)
    }

    async fn test_delete_role(db: &DbPool, role_id_name: RoleIdName) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .delete(RoleParam::builder().role(role_id_name.clone()).build()?);
        let service = DeleteRoleService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        if let Some(role_id) = role_id_name.id() {
            assert_deleted_role(db, role_id).await
        } else if let Some(role_name) = role_id_name.name() {
            assert_deleted_role(db, role_name).await
        } else {
            panic!("RoleIdName must have either id or name")
        }
    }

    async fn assert_deleted_role<R: SqlEntity>(db: &DbPool, role_ref: &R) -> Result<(), TdError> {
        // Assert just one of the roles in the db got deleted
        let found: Vec<RoleDB> = DaoQueries::default()
            .select_by::<RoleDB>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<RoleDB> = DaoQueries::default()
            .select_by::<RoleDB>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());

        // Assert that associated user roles got deleted, but not others
        let found: Vec<UserRoleDBWithNames> = DaoQueries::default()
            .select_by::<UserRoleDBWithNames>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<UserRoleDBWithNames> = DaoQueries::default()
            .select_by::<UserRoleDBWithNames>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());

        // Assert that associated role permissions got deleted, but not others
        let found: Vec<PermissionDBWithNames> = DaoQueries::default()
            .select_by::<PermissionDBWithNames>(&role_ref)?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(found.is_empty());

        let found: Vec<PermissionDBWithNames> = DaoQueries::default()
            .select_by::<PermissionDBWithNames>(&())?
            .build_query_as()
            .fetch_all(db)
            .await
            .unwrap();
        assert!(!found.is_empty());
        Ok(())
    }
}
