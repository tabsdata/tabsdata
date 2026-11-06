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
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::role::{RoleCreate, RoleDB};
use td_objects::dxo::service_token::{ServiceTokenDB, TokenCreate, TokenSlotParam};
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user::{UserCreate, UserDB};
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::errors::SqlError;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, Description, ExpiresInDays, FullName, Password, RoleId,
    RoleName, SimplePermissionType, TokenSlot, UserEnabled, UserId, UserName,
};
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

/// Exactly one winner; every other contender told the value already exists.
fn already_exists_but_one<T>(outcomes: Vec<Result<T, TdError>>) {
    for refusal in outcomes.iter().filter_map(|outcome| outcome.as_ref().err()) {
        assert!(
            matches!(
                refusal.domain_err::<SqlError>(),
                SqlError::EntityAlreadyExists(_, _)
            ),
            "{refusal:?}"
        );
    }
    one_winner(outcomes);
}

fn as_admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

#[td_test::test(db)]
async fn test_write_units_are_serializable(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let across =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = DbPool::in_project(&across, &p.project_id)
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
        (0..CONTENDERS)
            .map(|_| services.create_collection(create_request(p.project_id, "c_twin", None))),
    )
    .await;
    one_winner(outcomes);
    let live: Vec<CollectionDB> = db
        .scoped(p.project_id)
        .scd2()
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
        services.create_service_token(as_admin().create(
            TokenSlotParam { slot },
            TokenCreate {
                expires_in_days: ExpiresInDays::NEVER,
                comment: None,
            },
        ))
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

#[td_test::test(db)]
async fn test_concurrent_creates_of_one_user_name_leave_one(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let create = UserCreate {
        name: UserName::try_from("u_twin")?,
        full_name: FullName::try_from("Twin")?,
        email: None,
        password: Password::try_from("password")?,
        enabled: UserEnabled::TRUE,
    };
    let outcomes = join_all(
        (0..CONTENDERS).map(|_| services.create_user(as_admin().create((), create.clone()))),
    )
    .await;
    already_exists_but_one(outcomes);
    let users: Vec<UserDB> = db.fetch_all(&UserName::try_from("u_twin")?).await?;
    assert_eq!(users.len(), 1, "{users:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_concurrent_creates_of_one_role_name_leave_one(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let create = RoleCreate {
        name: RoleName::try_from("r_twin")?,
        description: Description::default(),
    };
    let outcomes = join_all(
        (0..CONTENDERS).map(|_| services.create_role(as_admin().create((), create.clone()))),
    )
    .await;
    already_exists_but_one(outcomes);
    let roles: Vec<RoleDB> = db.fetch_all(&RoleName::try_from("r_twin")?).await?;
    assert_eq!(roles.len(), 1, "{roles:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_concurrent_grants_of_one_permission_leave_one(db: TdPool) -> Result<(), TdError> {
    let user = db
        .seed()
        .user(&UserName::try_from("u_grantee")?, &UserEnabled::TRUE)
        .await?;
    let services = Services::with_defaults(db.clone());
    let grant = SimplePermissionGrant {
        user: UserName::try_from("u_grantee")?,
        permission: SimplePermissionType::CreateProjects,
        project: None,
    };
    let outcomes = join_all(
        (0..CONTENDERS)
            .map(|_| services.grant_simple_permission(as_admin().create((), grant.clone()))),
    )
    .await;
    already_exists_but_one(outcomes);
    let memberships: Vec<UserRoleDB> = db.fetch_all(()).await?;
    assert_eq!(
        memberships
            .iter()
            .filter(|membership| membership.user_id == user.id
                && membership.role_id == RoleId::project_creator())
            .count(),
        1,
        "{memberships:?}"
    );
    Ok(())
}
