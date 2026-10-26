//
// Copyright 2026 Tabsdata Inc.
//

use crate::Services;
use crate::tag::parent::test_support::table_parent;
use crate::tag::user::TagError;
use std::collections::HashMap;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::{TdError, assert_service_error};
use td_objects::FromDao;
use td_objects::Typed;
use td_objects::dxo::collection::{CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::function::{ChildFunction, FunctionDB, FunctionParam, FunctionRegister};
use td_objects::dxo::group::{GroupDB, GroupParam};
use td_objects::dxo::table::{TableDBWithNames, TableParam};
use td_objects::dxo::tag::{Tag, TagDB, TagParam, TagSet};
use td_objects::types::basic::{
    AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, Description, FunctionIdName, FunctionType, GroupIdName, GroupName, ProjectId, ProjectIdName, ReuseFrozen, TableIdName, TableNameDto, TableStatus, TableType, TagEntity, TagKind, TagValue, TopologyTagKey, UserId, UserTagKey,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

async fn project_tags(service: &Services, project: ProjectId) -> Result<Vec<Tag>, TdError> {
    let listed = service
        .tag_list(
            ctx()
                .list(
                    (),
                    ListParams {
                        filter: vec!["entity_type:eq:P".to_string()],
                        ..ListParams::default()
                    },
                )
                .in_project(project),
        )
        .await?;
    Ok(listed.data.data)
}

fn ctx() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn set(key: &str, value: &str) -> Result<TagSet, TdError> {
    Ok(TagSet {
        key: UserTagKey::try_new(key)?,
        value: TagValue::try_new(value)?,
    })
}

fn key(key: &str) -> Result<TagParam, TdError> {
    Ok(TagParam {
        key: UserTagKey::try_new(key)?,
    })
}

fn tag(key: &str, value: &str, kind: TagKind) -> (String, String, TagKind) {
    (key.to_string(), value.to_string(), kind)
}

fn sorted(mut tags: Vec<Tag>) -> Vec<(String, String, TagKind)> {
    tags.sort_by_key(|t| t.key.to_string());
    tags.into_iter()
        .map(|t| (t.key.to_string(), t.value.to_string(), t.kind))
        .collect()
}

fn publisher(name: &str) -> Result<FunctionRegister, TdError> {
    Ok(FunctionRegister {
        child_functions: None,
        name: name.try_into()?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: None,
        table_types: None,
        parent_tables: None,
        runtime_values: "mock runtime values".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    })
}

struct Fixture {
    project: ProjectId,
    collection: CollectionDB,
    owner: FunctionDB,
    child: FunctionDB,
}

async fn fixture(db: &TdPool) -> Result<Fixture, TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.base.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let owner = db
        .scoped(p.base.project_id)
        .seed()
        .function(&collection, &publisher("owner")?)
        .await?;
    let child = db
        .scoped(p.base.project_id)
        .seed()
        .child_function(&collection, &publisher("child")?, &owner.base.function_id)
        .await?;
    Ok(Fixture {
        project: p.base.project_id,
        collection,
        owner,
        child,
    })
}

fn function_params(f: &Fixture, function: &FunctionDB) -> (CollectionParam, FunctionParam) {
    (
        CollectionParam {
            collection: CollectionIdName::from_str(&f.collection.base.name.to_string()).unwrap(),
        },
        FunctionParam {
            function: FunctionIdName::from_str(&function.base.name.to_string()).unwrap(),
        },
    )
}

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
                &TagValue::of_id(f.owner.base.function_id),
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
        .fetch_all(TagEntity::from(f.collection.base.collection_id).rows())
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
        .fetch_all(TagEntity::from(group.base.group_id).rows())
        .await?;
    assert!(open.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_deleting_a_project_closes_its_and_its_groups_tags(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let project = p.base.project_id;
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
                            group: GroupIdName::from_str(&format!("~{}", group.base.group_id))?,
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

#[td_test::test(db)]
async fn test_a_frozen_table_keeps_its_parent_and_user_tags(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.base.project_id)
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
        .register_function(
            ctx()
                .create(collection(), owner)
                .in_project(p.base.project_id),
        )
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
    let trend_id = trend_table(&db).await?.base.base.table_id;
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
                .in_project(p.base.project_id),
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
                .in_project(p.base.project_id),
        )
        .await?;

    let frozen = trend_table(&db).await?;
    assert_eq!(frozen.base.base.status, TableStatus::Frozen);
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
            tag("td.parent", &TagValue::of_id(parent), TagKind::Topology),
            tag("team", "a", TagKind::User),
        ]
    );
    Ok(())
}
