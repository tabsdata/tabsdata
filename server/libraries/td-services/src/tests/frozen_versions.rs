//
// Copyright 2026 Tabsdata Inc.
//

//! A deletion freezes each dependant function once.

use crate::Services;
use crate::tests::fixtures::register;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionName, FunctionStatus, TableNameDto,
    UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// F depends on 101 tables of c_0, so its dependency rows span two
/// chunks of the dependants read. Deleting c_0 must freeze F once, not once
/// per chunk.
#[td_test::test(db)]
async fn test_collection_delete_freezes_a_wide_dependant_once(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let tables: Vec<TableNameDto> = (0..101)
        .map(|i| TableNameDto::try_from(format!("t_{i}")))
        .collect::<Result<_, _>>()?;

    let producer_collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(
            &producer_collection,
            &FunctionRegister {
                tables: Some(tables.clone()),
                ..register("producer", None)
            },
        )
        .await?;

    let dependant_collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_1")?, &UserId::admin())
        .await?;
    let dependencies = tables
        .iter()
        .map(|t| TableDependencyDto::try_from(format!("c_0/{t}")))
        .collect::<Result<Vec<_>, _>>()?;
    db.scoped(p.id)
        .seed()
        .function(
            &dependant_collection,
            &FunctionRegister {
                dependencies: Some(dependencies),
                ..register("wide", None)
            },
        )
        .await?;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .delete(CollectionParam {
            collection: CollectionIdName::from_str("c_0")?,
        })
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .delete_collection(request)
        .await?;

    let versions: Vec<FunctionDBWithNames> = db
        .timeline()
        .fetch_all(&FunctionName::try_from("wide")?)
        .await?;
    let frozen = versions
        .iter()
        .filter(|v| v.base.base.status == FunctionStatus::Frozen)
        .count();
    assert_eq!(frozen, 1, "one deletion, one frozen version");
    Ok(())
}
