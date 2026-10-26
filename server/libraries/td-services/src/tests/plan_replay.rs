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
    CollectionIdName, CollectionName, FunctionIdName, TableNameDto, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::test_utils::SeedEntry;

fn keys<K: Clone + Eq + Hash, V>(map: &HashMap<K, V>) -> HashSet<K> {
    map.keys().cloned().collect()
}

#[td_test::test(db)]
async fn test_an_execution_reads_back_the_topology_it_planned_on(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
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
                .in_project(p.project_id),
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
                .in_project(p.project_id),
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
                .in_project(p.project_id),
        )
        .await?;

    let read = services
        .execution_read(admin().read(ExecutionParam {
            execution: planned.id.clone(),
        }))
        .await?
        .data;
    assert_eq!(keys(&read.all_functions), keys(&planned.all_functions));
    assert_eq!(keys(&read.all_tables), keys(&planned.all_tables));
    Ok(())
}

/// A read shows the versions the plan pinned, even once their producer is
/// cancelled and they no longer resolve.
#[td_test::test(db)]
async fn test_an_execution_reads_back_the_versions_it_pinned(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let collection = || -> Result<CollectionParam, TdError> {
        Ok(CollectionParam {
            collection: CollectionIdName::from_str("c")?,
        })
    };
    let function = |name: &str| -> Result<FunctionParam, TdError> {
        Ok(FunctionParam {
            function: FunctionIdName::from_str(name)?,
        })
    };
    let services = Services::with_defaults(db.clone());
    for register in [
        FunctionRegister {
            tables: Some(vec![TableNameDto::try_from("t_a")?]),
            ..register("producer", None)
        },
        FunctionRegister {
            dependencies: Some(vec![TableDependencyDto::try_from("t_a")?]),
            tables: Some(vec![TableNameDto::try_from("t_c")?]),
            ..register("consumer", None)
        },
    ] {
        services
            .register_function(
                admin()
                    .create(collection()?, register)
                    .in_project(p.project_id),
            )
            .await?;
    }
    let project_id = p.project_id;
    let execute = |name: &'static str| {
        let services = dispatch_services(&db);
        async move {
            Ok::<_, TdError>(
                services
                    .execute_function(
                        admin()
                            .create(
                                (collection()?, function(name)?),
                                ExecutionRequest {
                                    name: None,
                                    function_only: Default::default(),
                                    override_disabled_trigger: Default::default(),
                                    skip_if_running: Default::default(),
                                    external_uid: None,
                                },
                            )
                            .in_project(project_id),
                    )
                    .await?
                    .data,
            )
        }
    };
    let produced = execute("producer").await?;
    let planned = execute("consumer").await?;
    let pinned: Vec<_> = planned
        .relations_info
        .keys()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    assert!(
        !pinned.is_empty(),
        "the consumer pinned the producer's version"
    );

    services
        .execution_cancel(admin().update(
            ExecutionParam {
                execution: produced.id.clone(),
            },
            (),
        ))
        .await?;

    let read = services
        .execution_read(admin().read(ExecutionParam {
            execution: planned.id.clone(),
        }))
        .await?
        .data;
    assert_eq!(read.relations, planned.relations);
    assert_eq!(keys(&read.relations_info), keys(&planned.relations_info));
    Ok(())
}
