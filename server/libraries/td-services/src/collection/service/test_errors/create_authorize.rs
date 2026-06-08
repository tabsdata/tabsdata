//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, Description, ProjectId, RoleId, UserId,
};
use td_database::scoped::InProject;

#[td_test::test(db)]
async fn test_not_allowed_to_create_collection(db: TdPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let description = Description::try_from("DS0").unwrap();

    let create = CollectionCreate::builder()
        .name(&name)
        .description(&description)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .create((), create)
        .in_project(ProjectId::sandbox());

    let service = CreateCollectionService::with_defaults(db.clone()).service();

    assert_service_error(service, request, |err| match err {
        td_authz::tower::AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
