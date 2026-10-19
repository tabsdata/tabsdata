//
// Copyright 2026 Tabsdata Inc.
//

//! An execution reads its plan back as of its instant: a topology write
//! committed after the plan never shows in it.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::catalog_races::admin;
use crate::tests::fixtures::register;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::execution::{ExecutionParam, ExecutionRequest};
use td_objects::dxo::function::{FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::types::basic::{
    AllowDisabled, CollectionIdName, CollectionName, FunctionIdName, TableNameDto, UserId,
};
use td_queries::test_utils::SeedEntry;

fn keys<K: Clone + Eq + Hash, V>(map: &HashMap<K, V>) -> HashSet<K> {
    map.keys().cloned().collect()
}

#[td_test::test(db)]
async fn test_an_execution_reads_back_the_topology_it_planned_on(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let collection = || -> Result<CollectionParam, TdError> {
        Ok(CollectionParam {
            collection: CollectionIdName::from_str("c")?,
        })
    };
    let producer = || -> Result<FunctionParam, TdError> {
        Ok(FunctionParam {
            function: FunctionIdName::from_str("producer")?,
        })
    };
    let services = Services::with_defaults(db.clone());
    services
        .register_function(
            admin()
                .create(
                    collection()?,
                    FunctionRegister {
                        tables: Some(vec![TableNameDto::try_from("t_a")?]),
                        ..register("producer", None)
                    },
                )
                .in_project(p.id),
        )
        .await?;

    let planned = dispatch_services(&db)
        .execute_function(
            admin()
                .create(
                    (collection()?, producer()?),
                    ExecutionRequest {
                        name: None,
                        function_only: Default::default(),
                        override_disabled_trigger: Default::default(),
                        skip_if_running: Default::default(),
                        external_uid: None,
                    },
                )
                .in_project(p.id),
        )
        .await?
        .data;

    services
        .update_function(
            admin()
                .update(
                    (collection()?, producer()?),
                    FunctionUpdate {
                        tables: Some(vec![
                            TableNameDto::try_from("t_a")?,
                            TableNameDto::try_from("t_b")?,
                        ]),
                        ..Default::default()
                    },
                )
                .in_project(p.id),
        )
        .await?;

    let read = services
        .execution_read(admin().read((
            ExecutionParam {
                execution: planned.id.clone(),
            },
            AllowDisabled::FALSE,
        )))
        .await?
        .data;
    assert_eq!(keys(&read.all_functions), keys(&planned.all_functions));
    assert_eq!(keys(&read.all_tables), keys(&planned.all_tables));
    Ok(())
}
