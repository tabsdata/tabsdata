//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, fixture, function_params, set};
use crate::Services;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::Typed;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::function::Function;
use td_objects::dxo::group::GroupParam;
use td_objects::dxo::tag::TagDB;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{
    CollectionIdName, CollectionName, Description, GroupIdName, GroupName, TagEntityId,
    TagEntityType, TagKey, UserId,
};
use td_queries::Queries;
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
    assert_eq!(collections(&["collection.env:eq:prod"]).await?, ["c"]);
    assert_eq!(collections(&["collection.env:is:null"]).await?, ["d"]);
    assert_eq!(collections(&["collection.env:lk:pr*"]).await?, ["c"]);
    assert!(collections(&["collection.env:ne:prod"]).await?.is_empty());

    let groups = service
        .list_group(
            ctx()
                .list((), list(&["group.owner:eq:data"]))
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
    assert_eq!(projects(&["project.tier:eq:gold"]).await?, [f.project]);
    assert!(
        !projects(&["project.tier:is:null"])
            .await?
            .contains(&f.project)
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_listing_by_the_tags_of_a_referenced_entity(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
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
    let functions = async |filter: &[&str]| -> Result<Vec<String>, TdError> {
        let mut names: Vec<String> = service
            .function_list(ctx().list((), list(filter)).in_project(f.project))
            .await?
            .data
            .data
            .into_iter()
            .map(|f| f.name.to_string())
            .collect();
        names.sort();
        Ok(names)
    };
    assert_eq!(
        functions(&["collection.env:eq:prod"]).await?,
        ["child", "owner"]
    );
    assert!(functions(&["collection.env:eq:dev"]).await?.is_empty());

    let err = functions(&["table.team:eq:a"])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("can filter by the tags of"), "{err}");
    for entity in ["collection", "function", "group"] {
        assert!(err.contains(entity), "{err}");
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_an_as_of_listing_reads_the_tags_of_its_instant(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let service = Services::with_defaults(db.clone());
    let tag_team = async |team: &str| {
        service
            .set_function_tag(
                ctx()
                    .create(function_params(&f, &f.owner), set("team", team)?)
                    .in_project(f.project),
            )
            .await
    };
    tag_team("a").await?;
    let then: TagDB = db
        .scoped(f.project)
        .scd2()
        .fetch_one(and!(
            &TagEntityType::Function,
            &TagEntityId::of(f.owner.function_id),
            &TagKey::try_new("team")?
        ))
        .await?;
    let then = then.interval.valid_from;
    tag_team("b").await?;

    let as_of = async |filter: &str| -> Result<Vec<String>, TdError> {
        let params = ListQueryParams::<Function>::try_from(&list(&[filter]))?;
        let rows = db
            .scoped(f.project)
            .scd2()
            .as_of(&then)
            .list::<Function>((), &params)
            .await?;
        Ok(rows.into_iter().map(|f| f.name.to_string()).collect())
    };
    assert_eq!(as_of("function.team:eq:a").await?, ["owner"]);
    assert!(as_of("function.team:eq:b").await?.is_empty());
    Ok(())
}
