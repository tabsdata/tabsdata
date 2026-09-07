//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use std::str::FromStr;
use td_authz::rbac::RbacError;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, Description, RoleName, UserEnabled, UserId,
    UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_not_allowed_to_delete_collection(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name, &UserId::admin())
        .await;

    // A user whose role carries no develop_project on the project cannot
    // delete collections in it.
    let user = db
        .seed()
        .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::TRUE)
        .await
        .unwrap();
    let role = db
        .seed()
        .role(
            RoleName::try_from("unauthorized_role").unwrap(),
            Description::try_from("any user").unwrap(),
        )
        .await
        .unwrap();
    let _user_role = db.seed().user_role(&user.id, &role.id).await.unwrap();

    let service = Services::with_defaults(db.clone());

    let request = RequestContext::with(AccessTokenId::default(), user.id)
        .delete(CollectionParam {
            collection: CollectionIdName::from_str(&name.to_string()).unwrap(),
        })
        .in_project(p.id);

    assert_service_error(service.delete_collection(request).await, |err| match err {
        RbacError::Forbidden(_, _) => {}
    });
}
