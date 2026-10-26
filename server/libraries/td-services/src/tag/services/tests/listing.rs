//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, fixture, set};
use crate::Services;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::group::GroupParam;
use td_objects::types::basic::{
    CollectionIdName, CollectionName, Description, GroupIdName, GroupName, UserId,
};
use td_queries::test_utils::SeedEntry;

fn list(filter: &[&str]) -> ListParams {
    ListParams {
        filter: filter.iter().map(|f| f.to_string()).collect(),
        ..ListParams::default()
    }
}

#[td_test::test(db)]
async fn test_listing_collections_groups_and_projects_by_tag(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    db.scoped(f.project)
        .seed()
        .collection(&CollectionName::try_from("d")?, &UserId::admin())
        .await?;
    db.scoped(f.project)
        .seed()
        .group(GroupName::try_from("g")?, Description::try_from("")?)
        .await?;
    let service = Services::with_defaults(db.clone());
    service
        .set_collection_tag(
            ctx()
                .create(
                    CollectionParam {
                        collection: CollectionIdName::from_str("c")?,
                    },
                    set("env", "prod")?,
                )
                .in_project(f.project),
        )
        .await?;
    service
        .set_group_tag(
            ctx()
                .create(
                    GroupParam {
                        group: GroupIdName::from_str("g")?,
                    },
                    set("owner", "data")?,
                )
                .in_project(f.project),
        )
        .await?;
    service
        .set_project_tag(ctx().create((), set("tier", "gold")?).in_project(f.project))
        .await?;

    let collections = |filter: &[&str]| {
        let request = ctx().list((), list(filter));
        let service = &service;
        async move {
            Ok::<_, TdError>(
                service
                    .list_collections(request)
                    .await?
                    .data
                    .data
                    .into_iter()
                    .map(|c| c.name.to_string())
                    .collect::<Vec<_>>(),
            )
        }
    };
    assert_eq!(collections(&["tag.env:eq:prod"]).await?, ["c"]);
    assert_eq!(collections(&["tag.env:is:null"]).await?, ["d"]);
    assert_eq!(collections(&["tag.env:lk:pr*"]).await?, ["c"]);
    assert!(collections(&["tag.env:ne:prod"]).await?.is_empty());

    let groups = service
        .list_group(
            ctx()
                .list((), list(&["tag.owner:eq:data"]))
                .in_project(f.project),
        )
        .await?
        .data
        .data;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].name, GroupName::try_from("g")?);

    let projects = |filter: &[&str]| {
        let request = ctx().list((), list(filter));
        let service = &service;
        async move {
            Ok::<_, TdError>(
                service
                    .list_projects(request)
                    .await?
                    .data
                    .data
                    .into_iter()
                    .map(|p| p.project_id)
                    .collect::<Vec<_>>(),
            )
        }
    };
    assert_eq!(projects(&["tag.tier:eq:gold"]).await?, [f.project]);
    assert!(!projects(&["tag.tier:is:null"]).await?.contains(&f.project));
    Ok(())
}
