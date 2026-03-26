//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::SKIP_AUTHZ;
use crate::graph::layers::{FunctionOrTable, GraphConfig, generate_td_graph};
use crate::table::layers::flow::{build_response, with_recursion_options};
use ta_services::factory::service_factory;
use td_objects::And;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBWithNames, TableFlowGraphResponse};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::rest_urls::TableParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, CollectionIdName, TableId, TableIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

pub struct GraphOpts;
impl
    GraphConfig<
        { TriggerDBWithNames::Active },
        { DependencyDBWithNames::Active },
        { FunctionDB::Available },
        { TableDB::Available },
    > for GraphOpts
{
    type Authz = SKIP_AUTHZ;

    fn include_dependency_functions(&self) -> bool {
        true
    }
}

#[service_factory(
    name = TableFlowGraphService,
    request = ReadRequest<TableParam>,
    response = TableFlowGraphResponse,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(With::<ReadRequest<TableParam>>::extract_name::<TableParam>),
        from_fn(With::<ReadRequest<TableParam>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(With::<TableParam>::extract::<CollectionIdName>),
        from_fn(With::<TableParam>::extract::<TableIdName>),
        // Find main table
        from_fn(
            By::<And![CollectionIdName, TableIdName]>::fetch_one_version::<
                { TableDBWithNames::Available },
                TableDBWithNames,
            >
        ),
        // Generate td graph
        from_fn(With::<TableDBWithNames>::extract::<TableId>),
        from_fn(With::<TableId>::convert_to::<FunctionOrTable>),
        from_fn(with_recursion_options),
        generate_td_graph(GraphOpts),
        // Build response
        from_fn(build_response),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::graph::GraphEdge;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        RoleId, TableName, TableNameDto, UserId,
    };
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_objects::{Buildable, and};
    use td_queries::CurrentQueries;
    use td_queries::test_utils::SeedQueries;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_table_flow_graph(db: TdPool) {
        use crate::graph::layers::build::GraphBuilder;
        use td_tower::metadata::type_of_val;

        TableFlowGraphService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<TableParam>, TableFlowGraphResponse>(&[
                // Extract from request.
                type_of_val(&With::<ReadRequest<TableParam>>::extract_name::<TableParam>),
                type_of_val(&With::<ReadRequest<TableParam>>::extract::<RequestContext>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&With::<TableParam>::extract::<CollectionIdName>),
                type_of_val(&With::<TableParam>::extract::<TableIdName>),
                // Find main table
                type_of_val(
                    &By::<And![CollectionIdName, TableIdName]>::fetch_one_version::<
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
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_recursive_trigger_graph),
                // Find dependencies. For clarity, we always show the function, even if
                // generating a table centered graph, the same way we show the tables of
                // a function centered graph.
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::main_function_id),
                type_of_val(
                    &GraphBuilder::<GraphOpts, _, _, _, _>::find_recursive_dependency_graph,
                ),
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::unique_function_ids),
                // Find output tables
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_functions_output_tables),
                // Find all tables and functions
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_tables_map),
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::find_all_functions_map),
                // Create td graph and validate it
                type_of_val(&GraphBuilder::<GraphOpts, _, _, _, _>::build_td_graph),
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
    async fn test_table_graph_no_deps(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .seed_collection(&collection_name, &UserId::admin())
            .await?;

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

        let function_0 = db.seed_function(&collection_0, &create).await?;

        // Actual test
        let service = TableFlowGraphService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .collection(CollectionIdName::from_str(&collection_name.to_string())?)
                    .table(TableIdName::from_str("table_0")?)
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table = db
            .fetch_one_current::<{ TableDBWithNames::All }, TableDBWithNames>(and!(
                &function_0.id,
                TableName::try_from("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (f_id.clone(), f.name.clone()))
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

        // Assert relations
        let relations = response.relations;
        assert_eq!(relations.len(), 2);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            // function_0 depends on table_0 and creates table_0
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_table_graph_downstream(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .seed_collection(&collection_name, &UserId::admin())
            .await?;

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

        let function_0 = db.seed_function(&collection_0, &create).await?;

        let create = FunctionRegister::builder()
            .name(FUNCTION_NAMES[1].clone())
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(Some(vec![TableDependencyDto::try_from("table_0")?]))
            .triggers(vec![TableTriggerDto::try_from("table_0")?])
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let function_1 = db.seed_function(&collection_0, &create).await?;

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

        let function_2 = db.seed_function(&collection_0, &create).await?;

        // Actual test
        let service = TableFlowGraphService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .collection(CollectionIdName::from_str(&collection_name.to_string())?)
                    .table(TableIdName::from_str("table_0")?)
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table: TableDBWithNames = db
            .fetch_one_current::<{ TableDBWithNames::All }, TableDBWithNames>(and!(
                &function_0.id,
                TableName::try_from("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_1, &function_2]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (f_id.clone(), f.name.clone()))
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

        // Assert relations
        let relations = response.relations;
        assert_eq!(relations.len(), 8);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            // function_0 depends on table_0 and creates table_0
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
            // function_1 depends on and gets triggered by table_0 and creates table_1 and table_2
            if f == function_1.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                ));
            }
            if f == function_1.id && t == expected_tables[1] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
            if f == function_1.id && t == expected_tables[2] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }

            // function_2 depends on table_0 and creates table_3
            if f == function_2.id && t == expected_tables[0] {
                assert!(matches!(edge, GraphEdge::Dependency { .. }));
            }
            if f == function_2.id && t == expected_tables[3] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_table_graph_upstream(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .seed_collection(&collection_name, &UserId::admin())
            .await?;

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

        let function_0 = db.seed_function(&collection_0, &create).await?;

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

        let _function_1 = db.seed_function(&collection_0, &create).await?;

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

        let function_2 = db.seed_function(&collection_0, &create).await?;

        // Actual test
        let service = TableFlowGraphService::with_defaults(db.clone()).service();
        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                TableParam::builder()
                    .collection(CollectionIdName::from_str(&collection_name.to_string())?)
                    .table(TableIdName::from_str("table_3")?)
                    .build()?,
            );

        let response = service.raw_oneshot(request).await?;

        // Assertions
        let central_table = db
            .fetch_one_current::<{ TableDBWithNames::All }, TableDBWithNames>(and!(
                &function_2.id,
                TableName::try_from("table_3")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_2]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .all_functions
            .iter()
            .map(|(f_id, f)| (f_id.clone(), f.name.clone()))
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

        // Assert relations
        let relations = response.relations;
        assert_eq!(relations.len(), 4);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            // function_0 depends on table_0 and creates table_0
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
            // function_2 depends on table_0 and creates table_3
            if f == function_2.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                ));
            }
            if f == function_2.id && t == expected_tables[1] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
        }
        Ok(())
    }
}
