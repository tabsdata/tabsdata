//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use ta_services::service::TdService;
use td_authz::rbac::RbacError;
use td_database::pool::TdPool;
use td_database::pool::scoped::InProject;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, Description, RoleName, UserEnabled, UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_not_allowed_to_create_collection(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    // A user whose role carries no develop_project on the project cannot
    // create collections in it.
    let user = db
        .seed()
        .user(
            &UserName::try_from("joaquin").unwrap(),
            &UserEnabled::from(true),
        )
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

    let create = CollectionCreate::builder()
        .name(&name)
        .description(&description)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), user.id)
        .create((), create)
        .in_project(p.id);

    let service = CreateCollectionService::with_defaults(db.clone()).service();

    assert_service_error(service, request, |err| match err {
        RbacError::Forbidden(_, _) => {}
    })
    .await;
}
