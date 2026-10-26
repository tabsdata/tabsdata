//
// Copyright 2026 Tabsdata Inc.
//

//! An update plans on a read of the collection and writes in a later unit;
//! a delete committing in between leaves nothing to update.

use crate::Services;
use crate::collection::services::update::{apply_collection_update, plan_collection_update};
use std::str::FromStr;
use td_database::access::{AcrossProjects, Attempt, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::{ApiError, TdError};
use td_objects::dxo::collection::{CollectionDB, CollectionParam, CollectionUpdate};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, CollectionIdName, CollectionName, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn grant() -> Grant {
    Grant::asserted("collection race test")
}

#[td_test::test(db)]
async fn test_a_rename_planned_before_a_delete_does_not_rename_the_deleted_collection(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let name = CollectionName::try_from("c_doomed")?;
    let seeded = db
        .scoped(p.base.project_id)
        .seed()
        .collection(&name, &UserId::admin())
        .await?;
    let across =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let pool = DbPool::in_project(&across, &p.base.project_id);
    let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
    let update = CollectionUpdate {
        name: Some(CollectionName::try_from("c_renamed")?),
        description: None,
        base_path: None,
    };

    let plan = {
        let mut conn = pool.read().await?;
        let conn = conn.get(grant());
        let collection: CollectionDB = conn.scd2().fetch_one(&seeded.base.collection_id).await?;
        plan_collection_update(conn, &collection, &update).await?
    };
    Services::with_defaults(db.clone())
        .delete_collection(
            admin
                .delete(CollectionParam {
                    collection: CollectionIdName::from_str(name.as_str())?,
                })
                .in_project(p.base.project_id),
        )
        .await?;

    let mut trx = pool.begin(Attempt::unretried()).await?;
    let at = trx.locked_at();
    let outcome = apply_collection_update(trx.get(grant()), &admin, at, &plan).await;
    drop(trx);

    let error = outcome.expect_err("the collection is gone");
    assert_eq!(error.api_error(), ApiError::NotFound);
    let versions: Vec<CollectionDB> = db
        .scoped(p.base.project_id)
        .timeline()
        .fetch_all(&seeded.base.collection_id)
        .await?;
    assert!(versions.iter().all(|v| !v.interval.valid_to.is_infinity()));
    assert!(versions.iter().all(|v| v.base.name.as_str() != "c_renamed"));
    Ok(())
}
