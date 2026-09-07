//
// Copyright 2026 Tabsdata Inc.
//

//! The wrapper's end of a connection, against a database. Every create service
//! test covers the commit incidentally, by reading its row back through the
//! pool; the rollback has no other coverage.

use crate::Services;
use ta_services::factory::service_factory;
use td_database::access::{GatedConnection, Grant, RW};
use td_database::pool::TdPool;
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::role::{RoleCreate, RoleDB};
use td_objects::types::basic::{AccessTokenId, RoleName, UserId};
use td_queries::Queries;

#[td_error]
enum ProbeError {
    #[error("the body failed after writing")]
    Failed = 0,
}

#[service_factory]
async fn probe_commits(
    gated_trx: &mut GatedConnection<RW>,
    request: CreateRequest<RoleCreate>,
) -> Result<(), TdError> {
    let (ctx, _, create) = request.into_parts();
    let conn = gated_trx.get(Grant::asserted("test: connection lifecycle probe"));
    conn.insert(&create.to_dao(&ctx)).await?;
    Ok(())
}

#[service_factory]
async fn probe_rolls_back(
    gated_trx: &mut GatedConnection<RW>,
    request: CreateRequest<RoleCreate>,
) -> Result<(), TdError> {
    let (ctx, _, create) = request.into_parts();
    let conn = gated_trx.get(Grant::asserted("test: connection lifecycle probe"));
    conn.insert(&create.to_dao(&ctx)).await?;
    Err(ProbeError::Failed.into())
}

fn request(name: &str) -> Result<CreateRequest<RoleCreate>, TdError> {
    let create = RoleCreate {
        name: name.try_into()?,
        description: "lifecycle probe".try_into()?,
    };
    Ok(RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create))
}

async fn stored(db: &TdPool, name: &str) -> Result<Option<RoleDB>, TdError> {
    Ok(db.fetch_optional(&RoleName::try_from(name)?).await?)
}

#[td_test::test(db)]
async fn test_body_ok_commits(db: TdPool) -> Result<(), TdError> {
    let service = Services::with_defaults(db.clone());
    service.probe_commits(request("probe_ok")?).await?;

    assert!(stored(&db, "probe_ok").await?.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn test_body_error_rolls_back(db: TdPool) -> Result<(), TdError> {
    let service = Services::with_defaults(db.clone());
    let err = service
        .probe_rolls_back(request("probe_err")?)
        .await
        .expect_err("the body returns an error");
    assert_eq!(err.code(), "ProbeError::0000");

    assert!(stored(&db, "probe_err").await?.is_none());
    Ok(())
}
