//
// Copyright 2024 Tabsdata Inc.
//

use crate::Services;
use crate::collection::CollectionError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::assert_service_error;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::CollectionUpdate;
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, UserId};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_update_collection_validate(db: TdPool) {
    let p = db.seed().test_project().await.unwrap();
    let name = CollectionName::try_from("c0").unwrap();
    let _ = db
        .scoped(p.id)
        .seed()
        .collection(&name, &UserId::admin())
        .await;

    let update = CollectionUpdate {
        name: None,
        description: None,
        base_path: None,
    };

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .update(
            CollectionParam {
                collection: CollectionIdName::from_str(&name.to_string()).unwrap(),
            },
            update,
        )
        .in_project(p.id);

    let service = Services::with_defaults(db);
    assert_service_error(service.update_collection(request).await, |err| {
        #[allow(unreachable_patterns)]
        match err {
            CollectionError::UpdateRequestHasNothingToUpdate => {}
            other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
        }
    });
}
