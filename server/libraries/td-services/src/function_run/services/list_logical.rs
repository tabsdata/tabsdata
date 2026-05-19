//
// Copyright 2026 Tabs Data Inc.
//

//! List logical function runs.
//!
//! Returns one row per `(execution_id, function_id)` pair — the
//! plan-time identity of a run. `status` is the canonical
//! (sticky-Committed) flow-control status; for per-attempt audit use
//! the physical function run listing instead.

use ta_services::factory::service_factory;
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse};
use td_objects::dxo::logical::LogicalFunctionRun;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = LogicalFunctionRunListService,
    request = ListRequest<()>,
    response = ListResponse<LogicalFunctionRun>,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<LogicalFunctionRun>, _>),
        from_fn(By::<()>::list::<NoFilter, LogicalFunctionRun>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::logical::LogicalFunctionRunDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_logical_function_run(db: TdPool) {
        use td_tower::metadata::type_of_val;

        LogicalFunctionRunListService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<LogicalFunctionRun>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(
                    &With::<ListParams>::try_convert_to::<ListQueryParams<LogicalFunctionRun>, _>,
                ),
                type_of_val(&By::<()>::list::<NoFilter, LogicalFunctionRun>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_logical_function_run(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("c")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f0")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .try_runtime_values("rv")?
            .reuse_frozen_tables(false)
            .build()?;
        let func = db.seed().function(&collection, &create).await?;
        let exec = db.seed().execution(&func).await?;
        let txn = db
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .seed()
            .physical_function_run(
                &collection,
                &func,
                &exec,
                &txn,
                &FunctionRunStatus::Scheduled,
            )
            .await?;
        // Pull the LFR that was auto-minted alongside the PFR seed.
        let lfr: LogicalFunctionRunDB = (&db)
            .live()
            .fetch_one(and!(&pfr.execution_id, &pfr.function_id))
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .list((), ListParams::default());
        let response = LogicalFunctionRunListService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        assert_eq!(response.len, 1);
        assert_eq!(response.data[0].id, lfr.id);
        assert_eq!(response.data[0].execution_id, lfr.execution_id);
        assert_eq!(response.data[0].function_id, lfr.function_id);
        assert_eq!(response.data[0].status, lfr.status);
        assert_eq!(*response.data[0].name, "f0");
        assert_eq!(*response.data[0].collection, "c");
        Ok(())
    }
}
