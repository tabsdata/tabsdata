//
// Copyright 2026 Tabsdata Inc.
//

use super::{ctx, fixture, function_params, set};
use crate::Services;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::Typed;
use td_objects::dxo::tag::{TagFacetParams, TagKeyParam};
use td_objects::types::basic::{TagEntityType, TagFacetLen, TagKey, TagKind, TagPrefix};

#[td_test::test(db)]
async fn test_tag_keys_and_values_in_use(db: TdPool) -> Result<(), TdError> {
    let f = fixture(&db).await?;
    let service = Services::with_defaults(db.clone());
    for (function, team) in [(&f.owner, "a"), (&f.child, "b")] {
        service
            .set_function_tag(
                ctx()
                    .create(function_params(&f, function), set("team", team)?)
                    .in_project(f.project),
            )
            .await?;
    }
    service
        .set_project_tag(ctx().create((), set("tier", "gold")?).in_project(f.project))
        .await?;

    let keys = |facet: TagFacetParams| {
        let request = ctx().read(facet).in_project(f.project);
        let service = &service;
        async move {
            Ok::<_, TdError>(
                service
                    .tag_keys(request)
                    .await?
                    .data
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>(),
            )
        }
    };
    let on = |entity_type| TagFacetParams {
        entity_type: Some(entity_type),
        ..TagFacetParams::default()
    };
    assert_eq!(
        keys(TagFacetParams::default()).await?,
        ["_parent", "team", "tier"]
    );
    assert_eq!(
        keys(on(TagEntityType::Function)).await?,
        ["_parent", "team"]
    );
    assert!(keys(on(TagEntityType::Table)).await?.is_empty());
    let user = TagFacetParams {
        kind: Some(TagKind::User),
        ..TagFacetParams::default()
    };
    assert_eq!(keys(user).await?, ["team", "tier"]);
    let starting_te = TagFacetParams {
        prefix: Some(TagPrefix::try_new("te")?),
        ..TagFacetParams::default()
    };
    assert_eq!(keys(starting_te).await?, ["team"]);
    let first = TagFacetParams {
        len: TagFacetLen::try_new(1)?,
        ..TagFacetParams::default()
    };
    assert_eq!(keys(first).await?, ["_parent"]);
    let literal = TagFacetParams {
        prefix: Some(TagPrefix::try_new("t_")?),
        ..TagFacetParams::default()
    };
    assert!(keys(literal).await?.is_empty());

    let values = service
        .tag_values(
            ctx()
                .read((
                    TagKeyParam {
                        key: TagKey::try_new("team")?,
                    },
                    TagFacetParams::default(),
                ))
                .in_project(f.project),
        )
        .await?
        .data
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>();
    assert_eq!(values, ["a", "b"]);
    Ok(())
}
