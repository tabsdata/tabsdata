//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use crate::user::UserError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserParam, UserUpdate};
use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserIdName, UserName};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_request_has_nothing_to_update(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .user(&UserName::try_from("u0").unwrap(), &UserEnabled::FALSE)
        .await;

    let service = Services::with_defaults(db.clone());

    let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let update = UserUpdate {
        full_name: None,
        email: Option::<Option<_>>::None,
        password: None,
        enabled: None,
    };
    let request = ctx.update(
        UserParam {
            user: UserIdName::from_str("admin").unwrap(),
        },
        update,
    );

    assert_service_error(service.update_user(request).await, |err| match err {
        UserError::UpdateRequestHasNothingToUpdate => {}
        other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
    });
}
