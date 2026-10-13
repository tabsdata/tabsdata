//
// Copyright 2026 Tabsdata Inc.
//

//! What a table delete leaves behind stays readable.

use crate::Services;
use crate::tests::fixtures::register;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{OptionalAtTimeParam, RequestContext};
use td_objects::dxo::function::{FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::dxo::table::TableParam;
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, TableIdName, TableNameDto,
    UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::test_utils::SeedEntry;

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn collection() -> Result<CollectionParam, TdError> {
    Ok(CollectionParam {
        collection: CollectionIdName::from_str("c_flow")?,
    })
}

/// `producer` stops writing t_gone and the user deletes it; `reader`
/// still depends on t_gone and writes t_out. The flow of t_out must still
/// render.
#[td_test::test(db)]
async fn test_flow_renders_after_a_dependency_table_is_deleted(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_flow")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(
            &c,
            &FunctionRegister {
                tables: Some(vec![
                    TableNameDto::try_from("t_gone")?,
                    TableNameDto::try_from("t_kept")?,
                ]),
                ..register("producer", None)
            },
        )
        .await?;
    db.scoped(p.id)
        .seed()
        .function(
            &c,
            &FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("t_gone")?]),
                tables: Some(vec![TableNameDto::try_from("t_out")?]),
                ..register("reader", None)
            },
        )
        .await?;

    let update = FunctionUpdate {
        tables: Some(vec![TableNameDto::try_from("t_kept")?]),
        ..Default::default()
    };
    let request = admin()
        .update(
            (
                collection()?,
                FunctionParam {
                    function: FunctionIdName::from_str("producer")?,
                },
            ),
            update,
        )
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .update_function(request)
        .await?;

    let request = admin()
        .delete((
            collection()?,
            TableParam {
                table: TableIdName::from_str("t_gone")?,
            },
        ))
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .table_delete(request)
        .await?;

    let request = admin()
        .read((
            collection()?,
            TableParam {
                table: TableIdName::from_str("t_out")?,
            },
            OptionalAtTimeParam { at: None },
        ))
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .table_flow_graph(request)
        .await?;
    Ok(())
}
