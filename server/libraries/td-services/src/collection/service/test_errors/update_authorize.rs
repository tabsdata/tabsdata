//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::update::UpdateCollectionService;
use std::str::FromStr;
use ta_services::service::TdService;
use td_authz::tower::AuthzError;
use td_database::pool::TdPool;
use td_error::assert_service_error;
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, ProjectId, RoleId, UserId,
};
use td_queries::test_utils::SeedEntry;
use td_tower::project_scope::InProject;

#[td_test::test(db)]
async fn test_not_allowed_to_update_collection(db: TdPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db.seed().collection(&name, &UserId::admin()).await;

    let service = UpdateCollectionService::with_defaults(db).service();

    let name = CollectionName::try_from("ds00").unwrap();

    let update = CollectionUpdate::builder()
        .name(Some(name.clone()))
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .update(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str(&name.to_string()).unwrap())
                .build()
                .unwrap(),
            update,
        )
        .in_project(ProjectId::sandbox());

    assert_service_error(service, request, |err| match err {
        AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
