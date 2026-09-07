//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionCreate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::errors::SqlError;
use td_objects::types::basic::{AccessTokenId, CollectionName, Description, GroupName, UserId};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_create_already_existing(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name, &UserId::admin())
        .await;

    let service = Services::with_defaults(db.clone());

    let create = CollectionCreate {
        name,
        description: Description::default(),
        group: GroupName::default_group(),
        base_path: None,
        connection: None,
        secrets: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create((), create)
        .in_project(p.id);

    // The `ensure_collection_name_free` pre-check rejects the duplicate up front
    // with a clean `EntityAlreadyExists` rather than the raw `UniqueViolation`
    // from the insert.
    assert_service_error(service.create_collection(request).await, |err| match err {
        SqlError::EntityAlreadyExists(_, _) => {}
        other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
    });
}
