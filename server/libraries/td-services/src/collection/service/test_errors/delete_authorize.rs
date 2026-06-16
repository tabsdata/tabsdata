//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::delete::DeleteCollectionService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_authz::rbac::RbacError;
use td_database::pool::TdPool;
use td_database::scoped::InProject;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, Description, ProjectId, RoleName, UserEnabled,
    UserId, UserName,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_not_allowed_to_delete_collection(db: TdPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db
        .scoped(ProjectId::sandbox())
        .seed()
        .collection(&name, &UserId::admin())
        .await;

    // A user whose role carries no develop_project on the project cannot
    // delete collections in it.
    let user = db
        .seed()
        .user(&UserName::try_from("joaquin").unwrap(), &UserEnabled::from(true))
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

    let service = DeleteCollectionService::with_defaults(db.clone()).service();

    let request = RequestContext::with(AccessTokenId::default(), user.id, role.id)
        .delete(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name.to_string()).unwrap())
                .build()
                .unwrap(),
        )
        .in_project(ProjectId::sandbox());

    assert_service_error(service, request, |err| match err {
        RbacError::Forbidden(_, _) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
