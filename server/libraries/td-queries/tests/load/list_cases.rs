//
// Copyright 2026 Tabsdata Inc.
//

//! `list` surfaces — filtered/paginated reads mapped through a `ListDto`,
//! with no filter and default pagination, over the chain dataset.

use std::time::Instant;

use ta_objects::{FromDao, ListDto, TaggedDao, Typed};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionRead;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::function::Function;
use td_objects::dxo::table::Table;
use td_objects::dxo::tag::TagFacetParams;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::TagKey;
use td_objects::types::tag_selector::TagSelector;
use td_queries::Queries;

use crate::harness::{record, seed};
use td_objects::dxo::topology::ProjectTopologyDB;

#[td_test::test(db)]
async fn load_default_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<CollectionRead>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().list((), &params).await?;
    record("default::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().list::<Table>((), &params).await?;
    record("latest::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_list_full(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let version = db
        .scd2()
        .fetch_one::<ProjectTopologyDB>(&h.project_id)
        .await?;
    let params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db
        .scd2()
        .as_of(&version.interval.valid_from)
        .list::<Function>((), &params)
        .await?;
    record("as_of::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_last_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.scd2().last().list::<Table>((), &params).await?;
    record("last::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tagged_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team_a: ListQueryParams<Table, TagSelector> = team_a()?;
    let t = Instant::now();
    let rows = db.scd2().list::<Table>((), &team_a).await?;
    record("tagged::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tagged_collections_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team_a: ListQueryParams<CollectionRead, TagSelector> = team_a()?;
    let t = Instant::now();
    let rows = db.scd2().list::<CollectionRead>((), &team_a).await?;
    record(
        "latest::tagged::collections",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tag_keys(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = db.tags().keys(&TagFacetParams::default()).await?;
    record("tags::keys", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_tag_values(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let team = TagKey::try_new("team")?;
    let t = Instant::now();
    let rows = db.tags().values(&team, &TagFacetParams::default()).await?;
    record("tags::values", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

fn team_a<T: ListDto>() -> Result<ListQueryParams<T, TagSelector>, TdError>
where
    <T as FromDao>::Dao: TaggedDao,
{
    ListQueryParams::try_from(&ListParams {
        filter: vec!["tag.team:eq:a".to_string()],
        ..ListParams::default()
    })
}
