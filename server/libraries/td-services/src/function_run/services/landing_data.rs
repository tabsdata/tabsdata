//
// Copyright 2026 Tabsdata Inc.
//

use crate::function_run::landing::{landing_file, read_landing_data};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::physical_function_run::{
    FunctionRunParam, InputIdxParam, PhysicalFunctionRunDBWithNames,
};
use td_objects::types::basic::{CollectionIdName, Decorator};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use td_storage::Storage;

#[service_factory]
async fn function_run_landing_data(
    authz: Ctx<AuthzContext>,
    storage: Ctx<Storage>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, FunctionRunParam, InputIdxParam)>>,
) -> Result<BoxedSyncStream, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection, function_run, input_idx)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let run_id = function_run.function_run;
    let run: PhysicalFunctionRunDBWithNames = conn.timeline().fetch_one(&run_id).await?;
    let collection = collection.collection;
    let addresses_run = match &collection {
        CollectionIdName::Id(id) => *id == run.collection_id,
        CollectionIdName::Name(name) => run.collection.name().is_ok_and(|n| n == *name),
    };
    if !addresses_run {
        return Err(api_error!(
            ApiError::NotFound,
            "function run {} does not belong to collection {}",
            run_id,
            collection
        ));
    }
    // Only a publisher ingests, so only a publisher run has landing data at all.
    let function: FunctionDB = conn.timeline().fetch_one(&run.function_version_id).await?;
    if function.decorator != Decorator::Publisher {
        return Err(api_error!(
            ApiError::NotFound,
            "function run {} is a {:?}, which ingests no landing data",
            run_id,
            function.decorator
        ));
    }

    let path = landing_file(&run, &input_idx.input_idx)?;
    // The index set lives in the runner, not the database: the only honest
    // existence check is against storage.
    if !storage.exists(&path).await? {
        return Err(api_error!(
            ApiError::NotFound,
            "function run {} has no landing data at input index {}",
            run_id,
            input_idx.input_idx
        ));
    }
    read_landing_data(&storage, &path).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::project::ProjectDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, InputIdx,
        LogicalFunctionRunStatus, ReuseFrozen, StorageVersion, TransactionKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_storage::location::StorageLocation;

    const LANDING: &[u8] = b"stamped-parquet-bytes";

    async fn seed_run(
        db: &TdPool,
        p: &ProjectDB,
        collection_name: &str,
        decorator: Decorator,
    ) -> Result<(CollectionDB, PhysicalFunctionRunDB), TdError> {
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(
                &CollectionName::try_from(collection_name)?,
                &UserId::admin(),
            )
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "f0".try_into()?,
            description: "f0".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "f0".try_into()?,
            decorator,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec!["t0".try_into()?]),
            table_types: None,
            parent_tables: None,
            runtime_values: "rv".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function: FunctionDB = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.project_id).seed().execution(&function).await?;
        let transaction = db
            .scoped(p.project_id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let run = db
            .scoped(p.project_id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        Ok((collection, run))
    }

    /// Writes `LANDING` where the runner would have uploaded it: the run's stamped
    /// prefix, under the physical run that ingested it, named by the index.
    async fn write_landing(
        context: &Services,
        collection: &CollectionDB,
        run: &PhysicalFunctionRunDB,
        input_idx: &InputIdx,
    ) -> Result<(), TdError> {
        let (path, _) = StorageLocation::from(&StorageVersion::default())
            .builder(&run.base_path)
            .collection(&collection.collection_id)
            .function_run(&run.id)
            .landing_input(input_idx)
            .build()?;
        context.storage.write(&path, LANDING.to_vec()).await?;
        Ok(())
    }

    async fn get_landing_data(
        context: &Services,
        p: &ProjectDB,
        collection: &str,
        run: &PhysicalFunctionRunDB,
        input_idx: &InputIdx,
    ) -> Result<Bytes, TdError> {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                FunctionRunParam {
                    function_run: run.id.clone(),
                },
                InputIdxParam {
                    input_idx: *input_idx,
                },
            ))
            .in_project(p.project_id);
        let response = context
            .clone()
            .function_run_landing_data(request)
            .await?
            .data;
        let mut stream = response.into_inner();
        match stream.next().await {
            Some(bytes) => Ok(bytes?),
            None => Ok(Bytes::new()),
        }
    }

    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_landing_data(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());
        let (collection, run) = seed_run(&db, &p, "c0", Decorator::Publisher).await?;
        let idx = InputIdx::try_from(2)?;
        write_landing(&context, &collection, &run, &idx).await?;

        // Addressable by collection name and by id, like every other
        // collection-scoped endpoint.
        for collection_ref in [
            collection.name.to_string(),
            format!("~{}", collection.collection_id),
        ] {
            let bytes = get_landing_data(&context, &p, &collection_ref, &run, &idx).await?;
            assert_eq!(bytes, Bytes::from_static(LANDING));
        }
        Ok(())
    }

    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_landing_data_unwritten_index_not_found(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());
        let (collection, run) = seed_run(&db, &p, "c0", Decorator::Publisher).await?;
        write_landing(&context, &collection, &run, &InputIdx::try_from(0)?).await?;

        let err = get_landing_data(&context, &p, "c0", &run, &InputIdx::try_from(1)?)
            .await
            .expect_err("index 1 was never uploaded");
        assert_eq!(err.api_error(), ApiError::NotFound);
        Ok(())
    }

    /// Only publishers ingest, so a transformer run has no landing data to read
    /// even at an index that happens to exist in storage.
    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_landing_data_non_publisher_not_found(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());
        let (collection, run) = seed_run(&db, &p, "c0", Decorator::Transformer).await?;
        let idx = InputIdx::try_from(0)?;
        write_landing(&context, &collection, &run, &idx).await?;

        let err = get_landing_data(&context, &p, "c0", &run, &idx)
            .await
            .expect_err("a transformer run ingests nothing");
        assert_eq!(err.api_error(), ApiError::NotFound);
        // `Decorator`'s `Display` is the one-letter storage code, so the
        // message has to name the variant, not render the value.
        assert!(err.to_string().contains("is a Transformer"), "{err}");
        Ok(())
    }

    /// The run id is globally unique, so the collection segment must be checked
    /// against it — otherwise any collection in the project addresses any run.
    #[td_test::test(db, flavor = "multi_thread", worker_threads = 1)]
    async fn test_landing_data_foreign_collection_not_found(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let context = Services::with_defaults(db.clone());
        let (collection, run) = seed_run(&db, &p, "c0", Decorator::Publisher).await?;
        let (_, other_run) = seed_run(&db, &p, "c1", Decorator::Publisher).await?;
        let idx = InputIdx::try_from(0)?;
        write_landing(&context, &collection, &run, &idx).await?;

        let err = get_landing_data(&context, &p, "c0", &other_run, &idx)
            .await
            .expect_err("the run belongs to c1");
        assert_eq!(err.api_error(), ApiError::NotFound);
        Ok(())
    }
}
