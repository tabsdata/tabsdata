//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, fixture, function_params, key, project_tags, set, sorted, tag};
use crate::Services;
use crate::tag::user::TagError;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::{TdError, assert_service_error};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::group::GroupParam;
use td_objects::types::basic::{
    CollectionIdName, Description, GroupIdName, GroupName, TagKind, TagValue, TopologyTagKey,
};
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_function_user_tags_set_list_update_and_delete(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let service = Services::with_defaults(db.clone());
    let params = || function_params(&f, &f.owner);

    let created = service
        .set_function_tag(
            ctx()
                .create(params(), set("team", "a")?)
                .in_project(f.project),
        )
        .await?
        .data;
    assert_eq!(sorted(vec![created]), [tag("team", "a", TagKind::User)]);
    service
        .set_function_tag(
            ctx()
                .create(params(), set("team", "b")?)
                .in_project(f.project),
        )
        .await?;
    service
        .set_function_tag(
            ctx()
                .create(params(), set("flag", "")?)
                .in_project(f.project),
        )
        .await?;

    let listed = service
        .list_function_tags(ctx().read(params()).in_project(f.project))
        .await?
        .data;
    assert_eq!(
        sorted(listed),
        [
            tag("flag", "", TagKind::User),
            tag("team", "b", TagKind::User)
        ]
    );

    let (collection, function) = params();
    service
        .delete_function_tag(
            ctx()
                .delete((collection, function, key("team")?))
                .in_project(f.project),
        )
        .await?;
    let listed = service
        .list_function_tags(ctx().read(params()).in_project(f.project))
        .await?
        .data;
    assert_eq!(sorted(listed), [tag("flag", "", TagKind::User)]);

    let (collection, function) = params();
    let missing = service
        .delete_function_tag(
            ctx()
                .delete((collection, function, key("team")?))
                .in_project(f.project),
        )
        .await;
    assert_service_error(missing, |e: &TagError| {
        assert!(matches!(e, TagError::NotFound(_)))
    });
    Ok(())
}

#[td_test::test(db)]
async fn test_listing_shows_system_tags_beside_user_tags(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let service = Services::with_defaults(db.clone());
    let params = || function_params(&f, &f.child);

    service
        .set_function_tag(
            ctx()
                .create(params(), set("team", "a")?)
                .in_project(f.project),
        )
        .await?;
    let listed = service
        .list_function_tags(ctx().read(params()).in_project(f.project))
        .await?
        .data;
    assert_eq!(
        sorted(listed),
        [
            tag(
                TopologyTagKey::Parent.to_string().as_str(),
                &TagValue::of_id(f.owner.function_id),
                TagKind::Topology
            ),
            tag("team", "a", TagKind::User),
        ]
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_collection_group_and_project_tags(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let service = Services::with_defaults(db.clone());
    let collection = || CollectionParam {
        collection: CollectionIdName::from_str("c").unwrap(),
    };
    service
        .set_collection_tag(
            ctx()
                .create(collection(), set("env", "prod")?)
                .in_project(f.project),
        )
        .await?;
    let listed = service
        .list_collection_tags(ctx().read(collection()).in_project(f.project))
        .await?
        .data;
    assert_eq!(sorted(listed), [tag("env", "prod", TagKind::User)]);

    db.scoped(f.project)
        .seed()
        .group(GroupName::try_from("g")?, Description::try_from("")?)
        .await?;
    let group = || GroupParam {
        group: GroupIdName::from_str("g").unwrap(),
    };
    service
        .set_group_tag(
            ctx()
                .create(group(), set("owner", "data")?)
                .in_project(f.project),
        )
        .await?;
    let listed = service
        .list_group_tags(ctx().read(group()).in_project(f.project))
        .await?
        .data;
    assert_eq!(sorted(listed), [tag("owner", "data", TagKind::User)]);

    service
        .set_project_tag(ctx().create((), set("tier", "gold")?).in_project(f.project))
        .await?;
    let listed = project_tags(&service, f.project).await?;
    assert_eq!(sorted(listed), [tag("tier", "gold", TagKind::User)]);
    service
        .delete_project_tag(ctx().delete(key("tier")?).in_project(f.project))
        .await?;
    assert!(project_tags(&service, f.project).await?.is_empty());
    Ok(())
}
