//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use td_database::access::{InProject, RW};
use td_database::pool::TdPool;
use td_objects::and;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::types::basic::{AtTime, TriggeredOn};

use td_objects::dxo::logical::LogicalFunctionRunInsertDB;

use td_objects::dxo::logical::LogicalTableDataVersionInsertDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::basic::ReuseFrozen;
use td_objects::types::basic::Trigger;
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, LogicalFunctionRunId, Private, ProjectId, TableName,
    TableNameDto, TransactionKey, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// Insert an LTDV identity row. The resolver works over the LTDV
/// timeline alone, so these tests seed no materializations.
async fn insert_ftdv(db: &TdPool, project: &ProjectId, ltdv: &LogicalTableDataVersionInsertDB) {
    db.scoped(*project)
        .insert::<LogicalTableDataVersionInsertDB>(ltdv)
        .await
        .unwrap();
}

/// Seed a collection / function / single table with `n` LTDVs,
/// returned in chronological order.
async fn seed_ftdv_timeline(
    db: &TdPool,
    n: usize,
) -> (
    CollectionDB,
    FunctionDB,
    TableId,
    Vec<LogicalTableDataVersionInsertDB>,
) {
    let p = db.seed().test_project().await.unwrap();
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
        .await
        .unwrap();

    let table_name = TableNameDto::try_from("t0").unwrap();
    let create = FunctionRegister {
        child_functions: None,
        name: "f0".try_into().unwrap(),
        description: "d".try_into().unwrap(),
        bundle_id: BundleId::default(),
        snippet: "s".try_into().unwrap(),
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: vec![table_name.clone()].into(),
        table_types: None,
        parent_tables: None,
        runtime_values: "rt".try_into().unwrap(),
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    let function = db
        .scoped(p.project_id)
        .seed()
        .function(&collection, &create)
        .await
        .unwrap();

    let table_name_typed = TableName::try_from(&table_name).unwrap();
    let table = db
        .scd2()
        .fetch_first::<TableDB>(and!(&collection.collection_id, &table_name_typed))
        .await
        .unwrap()
        .expect("row must exist");

    let mut ltdvs = Vec::with_capacity(n);
    for _ in 0..n {
        let execution = db
            .scoped(p.project_id)
            .seed()
            .execution(&function)
            .await
            .unwrap();
        let transaction = db
            .scoped(p.project_id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
            .await
            .unwrap();

        let ltdv = LogicalTableDataVersionInsertDB {
            id: LogicalTableDataVersionId::new(),
            collection_id: collection.collection_id,
            execution_id: execution.id.clone(),
            transaction_id: transaction.id.clone(),
            table_id: table.table_id,
            producer_logical_function_run_id: None,
            function_param_pos: None,
            private: Private::FALSE,
            triggered_on: execution.triggered_on,
            triggered_by_id: execution.triggered_by_id,
            group_id: collection.group_id,
        };

        insert_ftdv(db, &collection.project_id, &ltdv).await;
        ltdvs.push(ltdv);
    }
    (collection, function, table.table_id, ltdvs)
}

#[td_test::test(db)]
async fn test_resolve_logical_none(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
    let triggered_on = AtTime::now();

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::None)
        .triggered_on(&triggered_on)
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_single_head(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let cases = [
        (Version::Head(0), Some(&ltdvs[2])),
        (Version::Head(-1), Some(&ltdvs[1])),
        (Version::Head(-2), Some(&ltdvs[0])),
        (Version::Head(-3), None),
    ];
    for (version, expected) in cases {
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(version))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        match expected {
            Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().id, ltdv.id),
            None => assert!(found[0].is_none()),
        }
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_single_initial(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let cases = [
        (Version::Initial(0), Some(&ltdvs[0])),
        (Version::Initial(1), Some(&ltdvs[1])),
        (Version::Initial(2), Some(&ltdvs[2])),
        (Version::Initial(3), None),
    ];
    for (version, expected) in cases {
        let found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&Versions::Single(version))
            .triggered_on(&AtTime::now())
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(found.len(), 1);
        match expected {
            Some(ltdv) => assert_eq!(found[0].as_ref().unwrap().id, ltdv.id),
            None => assert!(found[0].is_none()),
        }
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_list_relative(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::List(vec![
        Version::Head(0),
        Version::Head(-1),
        Version::Head(-2),
        Version::Initial(0),
        Version::Initial(1),
        Version::Initial(2),
    ]);
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 6);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[1].id);
    assert_eq!(found[1].as_ref().unwrap().id, ltdvs[0].id);
    assert!(found[2].is_none());
    assert_eq!(found[3].as_ref().unwrap().id, ltdvs[0].id);
    assert_eq!(found[4].as_ref().unwrap().id, ltdvs[1].id);
    assert!(found[5].is_none());
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_list_without_head_or_initial(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::List(vec![
        Version::Head(-1),
        Version::Head(-2),
        Version::Head(-3),
        Version::Initial(2),
        Version::Initial(1),
        Version::Initial(3),
    ]);
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 6);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[1].id);
    assert_eq!(found[1].as_ref().unwrap().id, ltdvs[0].id);
    assert!(found[2].is_none());
    assert_eq!(found[3].as_ref().unwrap().id, ltdvs[2].id);
    assert_eq!(found[4].as_ref().unwrap().id, ltdvs[1].id);
    assert!(found[5].is_none());
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_range_head(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::Range(Version::Head(-2), Version::Head(0));
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 3);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
    assert_eq!(found[1].as_ref().unwrap().id, ltdvs[1].id);
    assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_range_descending(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, _ftdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::Range(Version::Head(-1), Version::Head(-3));
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert!(found.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_logical_range_initial(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::Range(Version::Initial(0), Version::Initial(2));
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 3);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
    assert_eq!(found[1].as_ref().unwrap().id, ltdvs[1].id);
    assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
    Ok(())
}

/// Mixed-side bounds don't define a deterministic range size, so the
/// resolver returns the contiguous slice as-is without padding.
#[td_test::test(db)]
async fn test_resolve_logical_range_mixed_bounds(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let versions = Versions::Range(Version::Initial(0), Version::Head(0));
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&versions)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 3);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
    assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
    Ok(())
}

/// With no prior execution, `Versions::New` returns the whole timeline.
#[td_test::test(db)]
async fn test_resolve_logical_new_no_prior_execution(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::New)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 3);
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[0].id);
    assert_eq!(found[2].as_ref().unwrap().id, ltdvs[2].id);
    Ok(())
}

/// `Versions::New` for a downstream consumer (never the plan's trigger)
/// returns just the delta since its prior LFR — not the whole timeline.
#[td_test::test(db)]
async fn test_resolve_logical_new_downstream_consumer(db: TdPool) -> Result<(), TdError> {
    let (collection, _publisher, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

    let consumer_create = FunctionRegister {
        child_functions: None,
        name: "c0".try_into()?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: vec![TableNameDto::try_from("c0_out")?].into(),
        table_types: None,
        parent_tables: None,
        runtime_values: "rt".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    let consumer = db
        .scoped(collection.project_id)
        .seed()
        .function(&collection, &consumer_create)
        .await?;

    // Consumer LFR aligned with run 1 is the resolution cutoff.
    let lfr_run1 = LogicalFunctionRunInsertDB {
        id: LogicalFunctionRunId::new(),
        collection_id: collection.collection_id,
        execution_id: ltdvs[0].execution_id.clone(),
        transaction_id: ltdvs[0].transaction_id.clone(),
        function_id: consumer.function_id,
        function_version_id: consumer.base.id,
        trigger: Trigger::Dependency,
        triggered_on: ltdvs[0].triggered_on,
        triggered_by_id: UserId::admin(),
        status: LogicalFunctionRunStatus::Committed,
        group_id: collection.group_id,
    };
    db.scoped(collection.project_id)
        .insert::<LogicalFunctionRunInsertDB>(&lfr_run1)
        .await?;

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    let found = VersionResolver::builder()
        .function_id(&consumer.base.function_id)
        .table_id(&table_id)
        .versions(&Versions::New)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;

    let found_ids: Vec<_> = found
        .iter()
        .map(|f| f.as_ref().map(|f| f.id.clone()))
        .collect();
    assert_eq!(
        found.len(),
        1,
        "downstream consumer should see only the LTDV minted after its prior run, got {found_ids:?}",
    );
    assert_eq!(found[0].as_ref().unwrap().id, ltdvs[1].id);
    Ok(())
}

/// LTDVs partition by `table_id`: a resolver call against table A must
/// not surface LTDVs minted for table B.
#[td_test::test(db)]
async fn test_resolve_logical_partition_isolation(db: TdPool) -> Result<(), TdError> {
    let (collection, _function, table_a_id, ftdvs_a) = seed_ftdv_timeline(&db, 2).await;

    let table_b_name = TableNameDto::try_from("table_b")?;
    let table_b_create = FunctionRegister {
        child_functions: None,
        name: "f_b".try_into()?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: vec![table_b_name.clone()].into(),
        table_types: None,
        parent_tables: None,
        runtime_values: "rt".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    let function_b = db
        .scoped(collection.project_id)
        .seed()
        .function(&collection, &table_b_create)
        .await?;
    let table_b_typed = TableName::try_from(&table_b_name)?;
    let table_b = db
        .scd2()
        .fetch_first::<TableDB>(and!(&collection.collection_id, &table_b_typed))
        .await?
        .expect("row must exist");
    for _ in 0..2 {
        let execution = db
            .scoped(collection.project_id)
            .seed()
            .execution(&function_b)
            .await?;
        let transaction = db
            .scoped(collection.project_id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let ltdv = LogicalTableDataVersionInsertDB {
            id: LogicalTableDataVersionId::new(),
            collection_id: collection.collection_id,
            execution_id: execution.id.clone(),
            transaction_id: transaction.id.clone(),
            table_id: table_b.table_id,
            producer_logical_function_run_id: None,
            function_param_pos: None,
            private: Private::FALSE,
            triggered_on: execution.triggered_on,
            triggered_by_id: execution.triggered_by_id,
            group_id: collection.group_id,
        };
        insert_ftdv(&db, &collection.project_id, &ltdv).await;
    }

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_a_id)
        .versions(&Versions::Range(Version::Initial(0), Version::Head(0)))
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 2);
    let ids: Vec<_> = found
        .iter()
        .map(|f| f.as_ref().unwrap().id.clone())
        .collect();
    assert!(ids.contains(&ftdvs_a[0].id));
    assert!(ids.contains(&ftdvs_a[1].id));
    for ltdv in found.iter().flatten() {
        assert_eq!(ltdv.table_id, table_a_id);
    }
    Ok(())
}

/// LTDVs with no materialization remain visible to the resolver. The
/// planner pins whatever is the latest at `triggered_on`; the gate
/// against running on unmaterialized inputs lives at dispatch.
#[td_test::test(db)]
async fn test_resolve_ignores_materialization_state(db: TdPool) -> Result<(), TdError> {
    let (_collection, _function, table_id, ltdvs) = seed_ftdv_timeline(&db, 2).await;

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::Single(Version::Head(0)))
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 1);
    let ltdv = found[0].as_ref().unwrap();
    assert_eq!(ltdv.id, ltdvs[1].id);
    // The resolver pins the latest logical version whether or not a
    // materialization exists for it; translating to one is the
    // caller's step.
    let current: Option<PhysicalTableDataVersionDB> = conn
        .scd2()
        .fetch_optional::<PhysicalTableDataVersionDB>(&ltdv.id)
        .await?;
    assert!(current.is_none(), "no materialization was seeded");
    Ok(())
}

/// Seed a Canceled-producer LTDV for `table_id` — the Canceled producer
/// is what makes the active view hide the returned LTDV.
async fn seed_canceled_ltdv(
    db: &TdPool,
    collection: &CollectionDB,
    function: &FunctionDB,
    table_id: TableId,
) -> LogicalTableDataVersionInsertDB {
    let execution = db
        .scoped(collection.project_id)
        .seed()
        .execution(function)
        .await
        .unwrap();
    let transaction = db
        .scoped(collection.project_id)
        .seed()
        .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
        .await
        .unwrap();
    let canceled_lfr = LogicalFunctionRunInsertDB {
        id: LogicalFunctionRunId::new(),
        collection_id: collection.collection_id,
        execution_id: execution.id.clone(),
        transaction_id: transaction.id.clone(),
        function_id: function.function_id,
        function_version_id: function.id.clone(),
        trigger: Trigger::Manual,
        triggered_on: TriggeredOn::asserted(*AtTime::now()),
        triggered_by_id: UserId::admin(),
        status: LogicalFunctionRunStatus::Canceled,
        group_id: collection.group_id,
    };
    db.scoped(collection.project_id)
        .insert::<LogicalFunctionRunInsertDB>(&canceled_lfr)
        .await
        .unwrap();
    let canceled_ltdv = LogicalTableDataVersionInsertDB {
        id: LogicalTableDataVersionId::new(),
        collection_id: collection.collection_id,
        execution_id: execution.base.id,
        transaction_id: transaction.base.id,
        table_id,
        producer_logical_function_run_id: Some(canceled_lfr.id.clone()),
        function_param_pos: None,
        private: Private::FALSE,
        triggered_on: canceled_lfr.triggered_on,
        triggered_by_id: canceled_lfr.triggered_by_id,
        group_id: collection.group_id,
    };
    db.scoped(collection.project_id)
        .insert::<LogicalTableDataVersionInsertDB>(&canceled_ltdv)
        .await
        .unwrap();
    canceled_ltdv
}

#[td_test::test(db)]
async fn test_resolve_logical_skips_canceled_producer(db: TdPool) -> Result<(), TdError> {
    let (collection, function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
    let good = &ltdvs[0];

    seed_canceled_ltdv(&db, &collection, &function, table_id).await;

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::None)
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].as_ref().unwrap().id,
        good.id,
        "canceled-producer version must disappear; HEAD falls back to the previous live version",
    );
    Ok(())
}

/// A `Fixed` pin is filtered too (not exempt from the active view).
#[td_test::test(db)]
async fn test_resolve_logical_fixed_skips_canceled_producer(db: TdPool) -> Result<(), TdError> {
    let (collection, function, table_id, ltdvs) = seed_ftdv_timeline(&db, 1).await;
    let good = &ltdvs[0];

    let canceled_ltdv = seed_canceled_ltdv(&db, &collection, &function, table_id).await;

    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());

    let found = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::Single(Version::Fixed(good.id.clone())))
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].as_ref().unwrap().id, good.id);

    let err = VersionResolver::builder()
        .function_id(&FunctionId::default())
        .table_id(&table_id)
        .versions(&Versions::Single(Version::Fixed(canceled_ltdv.id.clone())))
        .triggered_on(&AtTime::now())
        .build()?
        .resolve(&mut conn)
        .await
        .unwrap_err();
    assert!(matches!(
        err.domain_err::<VersionResolverError>(),
        VersionResolverError::FixedTableDataVersionsNotFound(_)
    ));
    Ok(())
}

/// The batched heads are what each table's `Versions::None` resolves to, at
/// any instant; a table without versions has none.
#[td_test::test(db)]
async fn test_resolve_heads_matches_each_tables_head(db: TdPool) -> Result<(), TdError> {
    let (_collection, function, table_id, ltdvs) = seed_ftdv_timeline(&db, 3).await;
    let empty = TableId::default();
    let mut conn =
        DomainConnection::<Apiserver, InProject, RW>::asserted(db.begin().await.unwrap());
    for at in [AtTime::from(ltdvs[1].triggered_on), AtTime::now()] {
        let heads = VersionResolver::resolve_heads(&mut conn, &[table_id, empty], &at).await?;
        let head = VersionResolver::builder()
            .function_id(&function.function_id)
            .table_id(&table_id)
            .versions(&Versions::None)
            .triggered_on(&at)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(
            heads.get(&table_id).map(|l| l.id.clone()),
            head[0].as_ref().map(|l| l.id.clone()),
        );
        assert!(!heads.contains_key(&empty));
    }
    Ok(())
}
