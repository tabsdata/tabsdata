//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_error::TdError;
use td_objects::{NoFilter, Not};
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::user::UserRead;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_objects::types::basic::UserId;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListUsersService,
    request = ListRequest<()>,
    response = ListResponse<UserRead>,
    connection = ConnectionProvider,
    scope = "Global",
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<UserRead>, _>),
        from_fn(set_system_user),
        from_fn(By::<Not![UserId]>::list::<NoFilter, UserRead>),
    )
}

// Ignoring system user.
pub async fn set_system_user() -> Result<UserId, TdError> {
    Ok(UserId::system())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserName};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_users(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ListUsersService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<UserRead>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<UserRead>, _>),
                type_of_val(&set_system_user),
                type_of_val(&By::<Not![UserId]>::list::<NoFilter, UserRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_users(db: TdPool) {
        let _ = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await;
        let _ = db
            .seed()
            .user(&UserName::try_from("u1").unwrap(), &UserEnabled::from(true))
            .await
            .unwrap();

        let service = ListUsersService::with_defaults(db.clone()).service();

        // `admin` is the only seeded user in `sys_admin` (holding manage_access),
        // so it is the requester able to list users.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        // The system user is excluded from the listing.
        assert_eq!(list.len, 3);
        assert!(
            list.data.iter().all(|u| u.name != UserName::system()),
            "system user should be excluded from the listing"
        );
        let users = list
            .data
            .into_iter()
            .map(|u| u.name)
            .collect::<HashSet<_>>();
        let expected = HashSet::from([
            UserName::admin(),
            UserName::try_from("u0").unwrap(),
            UserName::try_from("u1").unwrap(),
        ]);
        assert_eq!(expected, users);
    }
}
