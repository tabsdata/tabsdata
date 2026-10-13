//
// Copyright 2026 Tabsdata Inc.
//

//! Write units are SERIALIZABLE, so two of them claiming the same name or
//! slot cannot both commit: one wins and the other gets a clean refusal,
//! never a server error or a duplicate.

use crate::Services;
use crate::collection::services::connection::test_support::create_request;
use futures_util::future::join_all;
use td_database::access::{AcrossProjects, Attempt, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::service_token::{ServiceTokenDB, TokenCreate, TokenSlotParam};
use td_objects::types::basic::{AccessTokenId, CollectionName, ExpiresInDays, TokenSlot, UserId};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::show_setting;

const CONTENDERS: usize = 4;

/// Exactly one winner; every other contender refused by the client's fault.
fn one_winner<T>(outcomes: Vec<Result<T, TdError>>) {
    let refusals: Vec<ApiError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .map(|error| error.api_error())
        .collect();
    assert_eq!(refusals.len(), CONTENDERS - 1, "{refusals:?}");
    assert!(
        refusals
            .iter()
            .all(|refusal| !refusal.is_internal() && *refusal != ApiError::Unavailable),
        "{refusals:?}"
    );
}

#[td_test::test(db)]
async fn test_write_units_are_serializable(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let across =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = DbPool::in_project(&across, &p.id)
        .begin(Attempt::unretried())
        .await?;
    let isolation = show_setting(
        trx.get(Grant::asserted("isolation probe")),
        "transaction_isolation",
    )
    .await
    .expect("transaction_isolation");
    trx.commit().await?;
    assert_eq!(isolation, "serializable");
    Ok(())
}

#[td_test::test(db)]
async fn test_concurrent_creates_of_one_collection_name_leave_one(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let services = Services::with_defaults(db.clone());
    let outcomes = join_all(
        (0..CONTENDERS).map(|_| services.create_collection(create_request(p.id, "c_twin", None))),
    )
    .await;
    one_winner(outcomes);
    let live: Vec<ActiveCollectionDB> = db
        .scoped(p.id)
        .fetch_all(&CollectionName::try_from("c_twin")?)
        .await?;
    assert_eq!(live.len(), 1);
    Ok(())
}

#[td_test::test(db)]
async fn test_concurrent_creates_of_one_token_slot_leave_one(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let slot = TokenSlot::try_from(1)?;
    let outcomes = join_all((0..CONTENDERS).map(|_| {
        services.create_service_token(
            RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
                TokenSlotParam { slot },
                TokenCreate {
                    expires_in_days: ExpiresInDays::NEVER,
                    comment: None,
                },
            ),
        )
    }))
    .await;
    for refusal in outcomes.iter().filter_map(|outcome| outcome.as_ref().err()) {
        assert_eq!(refusal.api_error(), ApiError::Any(409), "{refusal:?}");
    }
    one_winner(outcomes);
    let tokens: Vec<ServiceTokenDB> = db.fetch_all(&UserId::admin()).await?;
    assert_eq!(
        tokens.iter().filter(|token| token.slot == slot).count(),
        1,
        "{tokens:?}"
    );
    Ok(())
}
