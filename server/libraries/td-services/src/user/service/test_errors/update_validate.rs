//
// Copyright 2024 Tabsdata Inc.
//

use crate::user::UserError;
use crate::user::service::update::UpdateUserService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::UserUpdate;
use td_objects::rest_urls::UserParam;
use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserIdName, UserName};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_request_has_nothing_to_update(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .user(
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(false),
        )
        .await;

    let service = UpdateUserService::with_defaults(db.clone()).service();

    let ctx = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let update = UserUpdate::builder()
        .full_name(None)
        .email(Option::<Option<_>>::None)
        .password(None)
        .enabled(None)
        .build()
        .unwrap();
    let request = ctx.update(
        UserParam::builder()
            .user(UserIdName::from_str("admin").unwrap())
            .build()
            .unwrap(),
        update,
    );

    assert_service_error(service, request, |err| match err {
        UserError::UpdateRequestHasNothingToUpdate => {}
        other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
    })
    .await;
}
