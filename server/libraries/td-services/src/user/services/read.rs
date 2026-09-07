//
// Copyright 2024 Tabsdata Inc.
//

use crate::user::assert_user_readable;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, SelfUser, User};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::user::{UserDBWithNames, UserParam, UserRead};
use td_queries::LiveEntry;

#[service_factory]
async fn read_user(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<UserParam>,
) -> Result<UserRead, TdError> {
    let (ctx, param) = request.into_parts();
    let id_name = param.user;
    let conn = gated_conn.get(authz.none());
    let row: UserDBWithNames = conn.live().fetch_one(&id_name).await?;
    assert_user_readable(&row)?;
    let _grant = authz
        .require::<User, (SelfUser, ManageAccess)>(conn, &ctx, &row.base.id)
        .await?;
    UserRead::from_dao(row)
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::UserParam;
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserIdName, UserName};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_read_user(db: TdPool) {
        let user_name = UserName::try_from("u0").unwrap();
        let user = db
            .seed()
            .user(&user_name, &UserEnabled::TRUE)
            .await
            .unwrap();

        let service = Services::with_defaults(db.clone());

        let request = RequestContext::with(AccessTokenId::default(), user.id).read(UserParam {
            user: UserIdName::from_str(&user_name.to_string()).unwrap(),
        });
        let response = service.read_user(request).await;
        let created = response.unwrap();

        assert_eq!(created.id, user.id);
        assert_eq!(created.name, user_name);
    }
}
