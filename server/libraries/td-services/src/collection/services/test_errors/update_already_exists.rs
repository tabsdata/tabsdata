//
// Copyright 2025 Tabsdata Inc.
//

use crate::Services;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::errors::SqlError;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, UserId};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_already_existing(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let name0 = CollectionName::try_from("ds0").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name0, &UserId::admin())
        .await;
    let name1 = CollectionName::try_from("ds1").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name1, &UserId::admin())
        .await;

    let service = Services::with_defaults(db);

    let update = CollectionUpdate {
        name: Some(name0.clone()),
        description: None,
        base_path: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .update(
            CollectionParam {
                collection: CollectionIdName::from_str(&name1.to_string()).unwrap(),
            },
            update,
        )
        .in_project(p.id);

    // The `ensure_collection_name_free` pre-check rejects the rename collision
    // up front with a clean `EntityAlreadyExists` rather than the raw
    // `UniqueViolation` from the update write.
    assert_service_error(service.update_collection(request).await, |err| match err {
        SqlError::EntityAlreadyExists(_, _) => {}
        other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
    });
}
