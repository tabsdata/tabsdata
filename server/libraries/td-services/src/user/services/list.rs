//
// Copyright 2024 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ListRequest, ListResponse};
use td_objects::dxo::user::UserRead;
use td_objects::list::params::ListQueryParams;
use td_objects::list::response::build_list_response;
use td_objects::types::basic::UserSystem;
use td_queries::LiveEntry;

#[service_factory]
async fn list_users(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListRequest,
) -> Result<ListResponse<UserRead>, TdError> {
    let (_, _, list_params) = request.into_parts();
    let conn = gated_conn.get(authz.none());
    // Excludes system users (the built-in `system` user and other service
    // users) from listings by filtering on `system = false`.
    let query_params = ListQueryParams::<UserRead>::try_from(&list_params)?;
    let rows = conn.live().list(&UserSystem::FALSE, &query_params).await?;
    build_list_response(&list_params, &query_params, rows)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use std::collections::HashSet;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserName};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_users(db: TdPool) {
        let _ = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await;
        let _ = db
            .seed()
            .user(&UserName::try_from("u1").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        // An enabled, system-flagged user must still be excluded — proving the
        // filter is on `system`, not `enabled`.
        let _ = db
            .seed()
            .system_user(&UserName::try_from("svc").unwrap())
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());

        // `admin` is the only seeded user in `sys_admin` (holding manage_access),
        // so it is the requester able to list users.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());
        let response = service.list_users(request).await;
        assert!(response.is_ok());
        let list = response.unwrap().data;
        // System users (built-in `system` and the seeded `svc`) are excluded.
        assert_eq!(list.len, 3);
        assert!(
            list.data
                .iter()
                .all(|u| u.name != UserName::system()
                    && u.name != UserName::try_from("svc").unwrap()),
            "system users should be excluded from the listing"
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
