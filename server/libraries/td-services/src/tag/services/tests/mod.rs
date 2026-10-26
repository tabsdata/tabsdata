//
// Copyright 2026 Tabsdata Inc.
//

mod facets;
mod lifecycle;
mod listing;
mod user_tags;

use crate::Services;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::Typed;
use td_objects::dxo::collection::{CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister};
use td_objects::dxo::tag::{Tag, TagParam, TagSet};
use td_objects::types::basic::{
    AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
    ProjectId, ReuseFrozen, TagKind, TagValue, UserId, UserTagKey,
};
use td_queries::test_utils::SeedEntry;

pub(super) async fn project_tags(
    service: &Services,
    project: ProjectId,
) -> Result<Vec<Tag>, TdError> {
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

pub(super) fn ctx() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

pub(super) fn set(key: &str, value: &str) -> Result<TagSet, TdError> {
    Ok(TagSet {
        key: UserTagKey::try_new(key)?,
        value: TagValue::try_new(value)?,
    })
}

pub(super) fn key(key: &str) -> Result<TagParam, TdError> {
    Ok(TagParam {
        key: UserTagKey::try_new(key)?,
    })
}

pub(super) fn tag(key: &str, value: &str, kind: TagKind) -> (String, String, TagKind) {
    (key.to_string(), value.to_string(), kind)
}

pub(super) fn sorted(mut tags: Vec<Tag>) -> Vec<(String, String, TagKind)> {
    tags.sort_by_key(|t| t.key.to_string());
    tags.into_iter()
        .map(|t| (t.key.to_string(), t.value.to_string(), t.kind))
        .collect()
}

pub(super) fn publisher(name: &str) -> Result<FunctionRegister, TdError> {
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

pub(super) struct Fixture {
    pub(super) project: ProjectId,
    pub(super) collection: CollectionDB,
    pub(super) owner: FunctionDB,
    pub(super) child: FunctionDB,
}

pub(super) async fn fixture(db: &TdPool) -> Result<Fixture, TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let owner = db
        .scoped(p.project_id)
        .seed()
        .function(&collection, &publisher("owner")?)
        .await?;
    let child = db
        .scoped(p.project_id)
        .seed()
        .child_function(&collection, &publisher("child")?, &owner.function_id)
        .await?;
    Ok(Fixture {
        project: p.project_id,
        collection,
        owner,
        child,
    })
}

pub(super) fn function_params(
    f: &Fixture,
    function: &FunctionDB,
) -> (CollectionParam, FunctionParam) {
    (
        CollectionParam {
            collection: CollectionIdName::from_str(&f.collection.name.to_string()).unwrap(),
        },
        FunctionParam {
            function: FunctionIdName::from_str(&function.name.to_string()).unwrap(),
        },
    )
}
