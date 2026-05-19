//
// Copyright 2025 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, Requester, SecAdmin, SystemOrRoleId};
use td_authz::{Authz, AuthzContext};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::permission::Permission;
use td_objects::dxo::role::RoleDB;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, RoleIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListPermissionService,
    request = ListRequest<RoleParam>,
    response = ListResponse<Permission>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<RoleParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::extract::<RoleId>),
        from_fn(AuthzOn::<SystemOrRoleId>::set),
        from_fn(Authz::<SecAdmin, Requester>::check),
        from_fn(With::<ListRequest<RoleParam>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Permission>, _>),
        from_fn(By::<RoleId>::list::<NoFilter, Permission>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, PermissionType, RoleName, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_permission(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ListPermissionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<RoleParam>, ListResponse<Permission>>(&[
                type_of_val(&With::<ListRequest<RoleParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
                type_of_val(&With::<RoleDB>::extract::<RoleId>),
                type_of_val(&AuthzOn::<SystemOrRoleId>::set),
                type_of_val(&Authz::<SecAdmin, Requester>::check),
                type_of_val(&With::<ListRequest<RoleParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<Permission>, _>),
                type_of_val(&By::<RoleId>::list::<NoFilter, Permission>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_permissions(db: TdPool) -> Result<(), TdError> {
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let seeded = db
            .seed()
            .permission(PermissionType::SysAdmin, None, None, &role)
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .list(
            RoleParam::builder()
                .role(RoleIdName::from_str("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListPermissionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(response.len, 1);
        let permission = response.data.first().unwrap();
        assert_eq!(permission.id, seeded.id);
        assert_eq!(permission.role_id, seeded.role_id);
        assert_eq!(permission.permission_type, seeded.permission_type);
        assert_eq!(permission.entity_type, seeded.entity_type);
        assert_eq!(
            permission.entity_id.unwrap_or(EntityId::all_entities()),
            seeded.entity_id
        );
        assert_eq!(permission.granted_by_id, seeded.granted_by_id);
        assert_eq!(permission.granted_on, seeded.granted_on);
        assert_eq!(permission.fixed, seeded.fixed);
        Ok(())
    }
}
