//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRole;
use td_objects::rest_urls::RoleParam;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListUserRoleService,
    request = ListRequest<RoleParam>,
    response = ListResponse<UserRole>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(By::<RoleId>::list::<RoleParam, NoListFilter, UserRole>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::{get_user_role, seed_user_role};
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleName, UserEnabled, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_user_role(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ListUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<RoleParam>, ListResponse<UserRole>>(&[
                type_of_val(&With::<ListRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::select::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&By::<RoleId>::list::<RoleParam, NoListFilter, UserRole>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_user_role(db: DbPool) -> Result<(), TdError> {
        let user = seed_user(
            &db,
            &UserName::try_from("joaquin")?,
            &UserEnabled::from(false),
        )
        .await;
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let user_role = seed_user_role(&db, &user.id, &role.id).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .list(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListUserRoleService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(response.len, 1);
        let found = get_user_role(&db, &user_role.id).await?;
        let response = response.data.first().unwrap();
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
