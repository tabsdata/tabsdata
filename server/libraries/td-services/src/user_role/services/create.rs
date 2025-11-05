//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::role::defs::RoleDB;
use td_objects::dxo::user::defs::UserDB;
use td_objects::dxo::user_role::defs::{
    UserRole, UserRoleBuilder, UserRoleCreate, UserRoleDB, UserRoleDBBuilder, UserRoleDBWithNames,
};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, SetService,
    TryIntoService, UpdateService, With, builder,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::id::{RoleId, UserId, UserRoleId};
use td_objects::types::id_name::RoleIdName;
use td_objects::types::string::UserName;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateUserRoleService,
    request = CreateRequest<RoleParam, UserRoleCreate>,
    response = UserRole,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>),
        from_fn(With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>),
        from_fn(builder::<UserRoleDBBuilder>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(With::<RoleId>::set::<UserRoleDBBuilder>),
        from_fn(With::<UserRoleCreate>::extract::<UserName>),
        from_fn(By::<UserName>::select::<UserDB>),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(With::<UserId>::set::<UserRoleDBBuilder>),
        from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
        from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
        from_fn(insert::<UserRoleDB>),
        from_fn(With::<UserRoleDB>::extract::<UserRoleId>),
        from_fn(By::<UserRoleId>::select::<UserRoleDBWithNames>),
        from_fn(With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
        from_fn(With::<UserRoleBuilder>::build::<UserRole, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_user_role::get_user_role;
    use td_objects::types::bool::UserEnabled;
    use td_objects::types::id::AccessTokenId;
    use td_objects::types::string::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_create_user_role(db: DbPool) {
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_tower::metadata::type_of_val;

        CreateUserRoleService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<RoleParam, UserRoleCreate>, UserRole>(&[
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract::<RequestContext>,
            ),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_name::<RoleParam>,
            ),
            type_of_val(
                &With::<CreateRequest<RoleParam, UserRoleCreate>>::extract_data::<UserRoleCreate>,
            ),
            type_of_val(&builder::<UserRoleDBBuilder>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<RoleId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<UserRoleCreate>::extract::<UserName>),
            type_of_val(&By::<UserName>::select::<UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
            type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
            type_of_val(&insert::<UserRoleDB>),
            type_of_val(&With::<UserRoleDB>::extract::<UserRoleId>),
            type_of_val(&By::<UserRoleId>::select::<UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
            type_of_val(&With::<UserRoleBuilder>::build::<UserRole, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_user_role(db: DbPool) -> Result<(), TdError> {
        let _user = seed_user(
            &db,
            &UserName::try_from("joaquin")?,
            &UserEnabled::from(false),
        )
        .await;
        let _role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;

        let create = UserRoleCreate::builder()
            .user(UserName::try_from("joaquin")?)
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            create,
        );

        let service = CreateUserRoleService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_user_role(&db, &response.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.user_id, found.user_id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.added_on, found.added_on);
        assert_eq!(response.added_by_id, found.added_by_id);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }
}
