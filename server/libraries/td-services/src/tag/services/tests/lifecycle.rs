//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, fixture, publisher, set, sorted, tag};
use crate::Services;
use crate::tag::parent::test_support::table_parent;
use std::collections::HashMap;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::function::{ChildFunction, FunctionParam, FunctionRegister};
use td_objects::dxo::group::{GroupDB, GroupParam};
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::table::{TableDBWithNames, TableParam};
use td_objects::dxo::tag::{Tag, TagDB};
use td_objects::types::basic::{
    BundleId, CollectionIdName, CollectionName, Description, FunctionIdName, FunctionType,
    GroupIdName, GroupName, ProjectIdName, TableIdName, TableName, TableNameDto, TableStatus,
    TableType, TagEntity, TagKind, TagValue, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_deleting_a_collection_closes_its_tags(db: TdPool) -> Result<(), TdError> {
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
    service
        .delete_collection(ctx().delete(collection()).in_project(f.project))
        .await?;
    let open: Vec<TagDB> = db
        .scd2()
        .fetch_all(TagEntity::from(f.collection.collection_id).rows())
        .await?;
    assert!(open.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_deleting_a_group_closes_its_tags(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let group = db
        .scoped(f.project)
        .seed()
        .group(GroupName::try_from("g")?, Description::try_from("")?)
        .await?;
    let service = Services::with_defaults(db.clone());
    let param = || GroupParam {
        group: GroupIdName::from_str("g").unwrap(),
    };
    service
        .set_group_tag(
            ctx()
                .create(param(), set("owner", "data")?)
                .in_project(f.project),
        )
        .await?;
    service
        .delete_group(ctx().delete(param()).in_project(f.project))
        .await?;
    let open: Vec<TagDB> = db
        .scd2()
        .fetch_all(TagEntity::from(group.group_id).rows())
        .await?;
    assert!(open.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_deleting_a_project_closes_its_and_its_groups_tags(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let project = p.project_id;
    let service = Services::with_defaults(db.clone());
    service
        .set_project_tag(ctx().create((), set("tier", "gold")?).in_project(project))
        .await?;
    let groups: Vec<GroupDB> = db.scoped(project).scd2().fetch_all(()).await?;
    assert!(!groups.is_empty());
    for group in &groups {
        service
            .set_group_tag(
                ctx()
                    .create(
                        GroupParam {
                            group: GroupIdName::from_str(&format!("~{}", group.group_id))?,
                        },
                        set("owner", "data")?,
                    )
                    .in_project(project),
            )
            .await?;
    }
    service
        .delete_project(ctx().delete(ProjectParam {
            project: ProjectIdName::from_str(&format!("~{project}"))?,
        }))
        .await?;
    let open: Vec<TagDB> = db.scoped(project).scd2().fetch_all(()).await?;
    assert!(open.is_empty());
    let versions: Vec<TagDB> = db.scoped(project).timeline().fetch_all(()).await?;
    assert_eq!(versions.len(), groups.len() + 1);
    Ok(())
}

#[td_test::test(db)]
async fn test_a_frozen_table_keeps_its_parent_and_user_tags(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let service = Services::with_defaults(db.clone());
    let collection = || CollectionParam {
        collection: CollectionIdName::from_str("c").unwrap(),
    };
    let trend = TableNameDto::try_from("owner_trends")?;
    let owner = FunctionRegister {
        tables: Some(vec![TableNameDto::try_from("owner_t0")?]),
        parent_tables: Some(HashMap::from([(
            trend.clone(),
            TableNameDto::try_from("owner_t0")?,
        )])),
        child_functions: Some(vec![ChildFunction {
            name: "owner_trend".try_into()?,
            function_type: FunctionType::DQSummaryTrends,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            dependencies: Some(vec![TableDependencyDto::try_from("owner_t0")?]),
            tables: Some(vec![trend.clone()]),
            table_types: Some(HashMap::from([(trend, TableType::DQSummaryTrends)])),
        }]),
        ..publisher("owner")?
    };
    let owner = service
        .register_function(ctx().create(collection(), owner).in_project(p.project_id))
        .await?
        .data;
    let trend_table = |db: &TdPool| {
        let db = db.clone();
        async move {
            let open: Vec<TableDBWithNames> = db
                .scd2()
                .fetch_all(&TableName::try_from("owner_trends")?)
                .await?;
            Ok::<_, TdError>(open.into_iter().next().expect("the trend table"))
        }
    };
    let trend_id = trend_table(&db).await?.table_id;
    assert!(table_parent(&db, &trend_id).await?.is_some());
    service
        .set_table_tag(
            ctx()
                .create(
                    (
                        collection(),
                        TableParam {
                            table: TableIdName::from_str("owner_trends")?,
                        },
                    ),
                    set("team", "a")?,
                )
                .in_project(p.project_id),
        )
        .await?;

    service
        .delete_function(
            ctx()
                .delete((
                    collection(),
                    FunctionParam {
                        function: FunctionIdName::from_str(&owner.name.to_string())?,
                    },
                ))
                .in_project(p.project_id),
        )
        .await?;

    let frozen = trend_table(&db).await?;
    assert_eq!(frozen.status, TableStatus::Frozen);
    let parent = table_parent(&db, &trend_id)
        .await?
        .expect("frozen child keeps its parent");
    let kept: Vec<TagDB> = db
        .scd2()
        .fetch_all(TagEntity::from(trend_id).rows())
        .await?;
    let kept = kept
        .into_iter()
        .map(Tag::from_dao)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        sorted(kept),
        [
            tag("_parent", &TagValue::of_id(parent), TagKind::Topology),
            tag("team", "a", TagKind::User),
        ]
    );
    Ok(())
}
