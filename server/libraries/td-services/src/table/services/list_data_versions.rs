//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::resolve_collection_id;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::AtTimeParam;
use td_objects::dxo::crudl::{ListRequest, ListResponse, WithinProject};
use td_objects::dxo::logical::TableDataVersion;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::table::TableParam;
use td_queries::Queries;

#[service_factory]
async fn table_list_data_versions(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ListRequest<(CollectionParam, TableParam, AtTimeParam)>>,
) -> Result<ListResponse<TableDataVersion>, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, table_param, at_param), list_params) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let table_id_name = table_param.table;
    let collection_id = resolve_collection_id(conn, &collection_id_name).await?;
    let condition = and!(&collection_id, &table_id_name);
    match &at_param.at {
        Some(at) => {
            let table: TableDBWithNames = conn.scd2().as_of(at).fetch_one(condition).await?;
            conn.timeline()
                .until(at)
                .list(&table.table_id, list_params)
                .await
        }
        None => {
            let table: TableDBWithNames = conn.scd2().fetch_one(condition).await?;
            conn.timeline().list(&table.table_id, list_params).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionIdName, CollectionName, Decorator,
        FunctionRuntimeValues, LogicalFunctionRunStatus, ReuseFrozen, TableIdName, TableNameDto,
        TransactionKey, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_list_data_versions_after_collection_delete(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("doomed")?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "publisher".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
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
        let physical_function_run = db
            .scoped(p.project_id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let table: TableDB = db
            .scd2()
            .fetch_first(&function.id)
            .await?
            .expect("row must exist");
        let data_version = db
            .scoped(p.project_id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        let written_at = AtTime::now();

        let service = Services::with_defaults(db.clone());
        service
            .delete_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str(&collection_name.to_string())?,
                    })
                    .in_project(p.project_id),
            )
            .await?;

        let request = |collection: &str, at: AtTime| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .list(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(collection).unwrap(),
                        },
                        TableParam {
                            table: TableIdName::from_str("t0").unwrap(),
                        },
                        AtTimeParam { at: Some(at) },
                    ),
                    ListParams::default(),
                )
                .in_project(p.project_id)
        };

        let by_name = collection_name.to_string();
        let by_id = format!("~{}", collection.collection_id);
        let err = service
            .table_list_data_versions(request(&by_name, written_at))
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        let data = service
            .table_list_data_versions(request(&by_id, written_at))
            .await?
            .data
            .data;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].table_id, table.table_id);
        assert_eq!(data[0].execution_id, data_version.execution_id);

        assert!(
            service
                .table_list_data_versions(request(&by_id, AtTime::now()))
                .await
                .is_err()
        );
        Ok(())
    }
}
