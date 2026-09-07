//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use td_authz::rbac::RbacError;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, Description, GroupName, RoleName, UserEnabled, UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_not_allowed_to_create_collection(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    // A user whose role carries no develop_project on the project cannot
    // create collections in it.
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

    let name = CollectionName::try_from("ds0").unwrap();
    let description = Description::try_from("DS0").unwrap();

    let create = CollectionCreate {
        name,
        description,
        group: GroupName::default_group(),
        base_path: None,
        connection: None,
        secrets: None,
        vars: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), user.id)
        .create((), create)
        .in_project(p.id);

    let service = Services::with_defaults(db.clone());

    assert_service_error(service.create_collection(request).await, |err| match err {
        RbacError::Forbidden(_, _) => {}
    });
}
