//
// Copyright 2025 Tabs Data Inc.
//

use crate::table::layers::graph::{build_response, find_dependency_graph};
use ta_services::factory::service_factory;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::table::{TableDBWithNames, TableGraphResponse};
use td_objects::rest_urls::TableParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With, combine};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{AtTime, CollectionIdName, TableId, TableIdName};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = TableDependencyGraphService,
    request = ReadRequest<TableParam>,
    response = TableGraphResponse,
    connection = ConnectionProvider,
    context = DaoQueries,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<TableParam>>::extract_name::<TableParam>),
        from_fn(With::<ReadRequest<TableParam>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<TableParam>::extract::<CollectionIdName>),
        from_fn(With::<TableParam>::extract::<TableIdName>),
        // Find table
        from_fn(combine::<CollectionIdName, TableIdName>),
        from_fn(
            By::<(CollectionIdName, TableIdName)>::select_version::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        // Find dependency graph
        from_fn(find_dependency_graph),
        // // Build response
        from_fn(build_response),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::collections::HashMap;
    use std::sync::LazyLock;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{RequestContext, handle_sql_err};
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::sql::cte::CteQueries;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        RoleId, TableName, TableNameDto, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_execution(db: DbPool) {
        use crate::graph::layers::build::{
            build_td_graph, find_all_functions_map, find_all_tables_map,
            find_functions_output_tables, find_recursive_dependency_graph,
            find_recursive_trigger_graph, main_function_id, unique_function_ids,
        };
        use td_tower::metadata::type_of_val;

        TableDependencyGraphService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<TableParam>, TableGraphResponse>(&[
                // Extract from request.
                type_of_val(&With::<ReadRequest<TableParam>>::extract_name::<TableParam>),
                type_of_val(&With::<ReadRequest<TableParam>>::extract::<RequestContext>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&With::<TableParam>::extract::<CollectionIdName>),
                type_of_val(&With::<TableParam>::extract::<TableIdName>),
                // Find main table
                type_of_val(&combine::<CollectionIdName, TableIdName>),
                type_of_val(
                    &By::<(CollectionIdName, TableIdName)>::select_version::<
                        { TableDBWithNames::Available },
                        TableDBWithNames,
                    >,
                ),
                // Generate td graph
                type_of_val(&With::<TableDBWithNames>::extract::<TableId>),
                type_of_val(&With::<TableId>::convert_to::<FunctionOrTable>),
                type_of_val(&with_recursion_options),
                // Find td graph
                // Find triggers
                type_of_val(&find_recursive_trigger_graph),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&main_function_id),
                type_of_val(&find_recursive_dependency_graph),
                type_of_val(&unique_function_ids),
                // Find output tables
                type_of_val(&find_functions_output_tables),
                // Find all tables and functions
                type_of_val(&find_all_tables_map),
                type_of_val(&find_all_functions_map),
                // Create td graph and validate it
                type_of_val(&build_td_graph),
                // Build response
                type_of_val(&build_response),
            ]);
    }

    static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
        vec![
            FunctionName::try_from("function_0").unwrap(),
            FunctionName::try_from("function_1").unwrap(),
            FunctionName::try_from("function_2").unwrap(),
            FunctionName::try_from("function_3").unwrap(),
        ]
    });

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_table_graph_no_deps(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![
                TableNameDto::try_from("table_0")?,
                TableNameDto::try_from("table_1")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_0 = seed_function(&db, &collection_0, &create).await;

        // Actual test
        let service = TableDependencyGraphService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .try_collection(collection_name.to_string())?
                    .try_table("table_0")?
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table: TableDBWithNames = DaoQueries::default()
            .select_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
                None,
                &(function_0.id, TableName::try_from("table_0")?),
            )?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0]
            .iter()
            .map(|f| (f.id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (*f_id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        assert_eq!(functions, expected_functions);

        let expected_tables = [
            TableName::try_from("table_0")?,
            TableName::try_from("table_1")?,
        ];
        let tables = response
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        // All tables are user in this test
        assert!(response.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.user_tables.len());

        // All relations created are dependencies
        assert!(response.relations.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_table_graph_downstream(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_0 = seed_function(&db, &collection_0, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_1 = seed_function(&db, &collection_0, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_3")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_2 = seed_function(&db, &collection_0, &create).await;

        // Actual test
        let service = TableDependencyGraphService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .try_collection(collection_name.to_string())?
                    .try_table("table_0")?
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table: TableDBWithNames = DaoQueries::default()
            .select_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
                None,
                &(function_0.id, TableName::try_from("table_0")?),
            )?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_1, &function_2]
            .iter()
            .map(|f| (f.id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (*f_id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        assert_eq!(functions, expected_functions);

        let expected_tables = [
            TableName::try_from("table_0")?,
            TableName::try_from("table_1")?,
            TableName::try_from("table_2")?,
            TableName::try_from("table_3")?,
        ];
        let tables = response
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        // All tables are user in this test
        assert!(response.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.user_tables.len());

        // All relations created are dependencies
        let expected_relations = [
            (function_0.id, vec![TableName::try_from("table_0")?]),
            (function_1.id, vec![TableName::try_from("table_0")?]),
            (function_2.id, vec![TableName::try_from("table_0")?]),
        ];
        let relations = response
            .relations
            .iter()
            .map(|(f_id, t_id, _)| {
                let table_name = response
                    .all_tables
                    .get(t_id)
                    .map(|t| t.name.clone())
                    .unwrap();
                (*f_id, table_name)
            })
            .into_group_map();
        let relations = relations.into_iter().sorted().collect::<Vec<_>>();
        assert_eq!(relations, expected_relations);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_table_graph_upstream(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[0].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_0")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_0 = seed_function(&db, &collection_0, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _function_1 = seed_function(&db, &collection_0, &create).await;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[2].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_3")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_2 = seed_function(&db, &collection_0, &create).await;

        // Actual test
        let service = TableDependencyGraphService::with_defaults(db.clone())
            .service()
            .await;
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .try_collection(collection_name.to_string())?
                    .try_table("table_3")?
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table: TableDBWithNames = DaoQueries::default()
            .select_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
                None,
                &(function_2.id, TableName::try_from("table_3")?),
            )?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_2]
            .iter()
            .map(|f| (f.id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (*f_id, f.name.clone()))
            .collect::<HashMap<_, _>>();
        assert_eq!(functions, expected_functions);

        let expected_tables = [
            TableName::try_from("table_0")?,
            TableName::try_from("table_3")?,
        ];
        let tables = response
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        // All tables are user in this test
        assert!(response.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.user_tables.len());

        // All relations created are dependencies
        let expected_relations = [
            (function_0.id, vec![TableName::try_from("table_0")?]),
            (function_2.id, vec![TableName::try_from("table_0")?]),
        ];
        let relations = response
            .relations
            .iter()
            .map(|(f_id, t_id, _)| {
                let table_name = response
                    .all_tables
                    .get(t_id)
                    .map(|t| t.name.clone())
                    .unwrap();
                (*f_id, table_name)
            })
            .into_group_map();
        let relations = relations.into_iter().sorted().collect::<Vec<_>>();
        assert_eq!(relations, expected_relations);
        Ok(())
    }
}
