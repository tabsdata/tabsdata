//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, key, set};
use crate::Services;
use crate::collection::services::connection::test_support::{create_collection, linked_connection};
use std::collections::BTreeMap;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection_catalog::CatalogTextRow;
use td_objects::types::basic::{
    CollectionIdName, ConnectionId, GroupName, ProjectId, TagValue, UserTagKey,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

async fn catalog_texts(
    db: &TdPool,
    project: ProjectId,
    connection: &ConnectionId,
) -> Result<CatalogTextRow, TdError> {
    let mut texts = (&db.scoped(project))
        .connection_catalogs()
        .texts_of(std::slice::from_ref(connection))
        .await?;
    assert_eq!(texts.len(), 1);
    Ok(texts.remove(0))
}

fn tags(pairs: &[(&str, &str)]) -> BTreeMap<UserTagKey, TagValue> {
    pairs
        .iter()
        .map(|(key, value)| {
            (
                UserTagKey::try_from(*key).unwrap(),
                TagValue::try_from(*value).unwrap(),
            )
        })
        .collect()
}

#[td_test::test(db)]
async fn test_collection_user_tags_are_its_connection_catalog_texts(
    db: TdPool,
) -> Result<(), TdError> {
    let project = db.seed().test_project().await?.project_id;
    create_collection(&db, project, "src0", Some(GroupName::source())).await;
    let connection = linked_connection(&db, project, "src0").await.unwrap().id;
    let service = Services::with_defaults(db.clone());
    let collection = || CollectionParam {
        collection: CollectionIdName::from_str("src0").unwrap(),
    };

    let untagged = catalog_texts(&db, project, &connection).await?;
    assert_eq!(*untagged.collection_tags, tags(&[]));

    service
        .set_collection_tag(
            ctx()
                .create(collection(), set("owner", "finance team")?)
                .in_project(project),
        )
        .await?;
    let owned = catalog_texts(&db, project, &connection).await?;
    assert!(owned.text_revision > untagged.text_revision);
    assert_eq!(*owned.collection_tags, tags(&[("owner", "finance team")]));

    service
        .set_collection_tag(
            ctx()
                .create(collection(), set("pii", "")?)
                .in_project(project),
        )
        .await?;
    let tagged = catalog_texts(&db, project, &connection).await?;
    assert!(tagged.text_revision > owned.text_revision);
    assert_eq!(
        *tagged.collection_tags,
        tags(&[("owner", "finance team"), ("pii", "")])
    );

    service
        .delete_collection_tag(
            ctx()
                .delete((collection(), key("owner")?))
                .in_project(project),
        )
        .await?;
    let deleted = catalog_texts(&db, project, &connection).await?;
    assert!(deleted.text_revision > tagged.text_revision);
    assert_eq!(*deleted.collection_tags, tags(&[("pii", "")]));
    Ok(())
}
