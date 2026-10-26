//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::resolve_collection_id;
use crate::table::flow::build_response;
use crate::topology::read_version;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_execution::topology::ProjectTopology;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::graph::TableFlowGraphResponse;
use td_objects::dxo::table::{TableDBWithNames, TableParam};
use td_objects::dxo::topology::TopologyReadParam;
use td_queries::Queries;

#[service_factory]
async fn table_flow_graph(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, TableParam, TopologyReadParam)>>,
) -> Result<TableFlowGraphResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, table_param, topology_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let table_id_name = table_param.table;
    let topology = read_version(conn, &project, &topology_param).await?;
    let collection_id = resolve_collection_id(conn, &collection_id_name).await?;
    let table: TableDBWithNames = conn
        .scd2()
        .as_of(&topology.interval.valid_from)
        .fetch_one(and!(&collection_id, &table_id_name))
        .await?;

    let topology = ProjectTopology::of(conn, topology).await?;
    let graph = topology.flow_graph(&table.table_id)?;

    build_response(&table, &graph, topology.labels(&graph)?).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use itertools::Itertools;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::graph::GraphEdge;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        TableName, TableNameDto, UserId,
    };
    use td_objects::types::basic::{AtTime, CollectionIdName, TableIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    static FUNCTION_NAMES: LazyLock<Vec<FunctionName>> = LazyLock::new(|| {
        vec![
            FunctionName::try_from("function_0").unwrap(),
            FunctionName::try_from("function_1").unwrap(),
            FunctionName::try_from("function_2").unwrap(),
            FunctionName::try_from("function_3").unwrap(),
        ]
    });

    #[td_test::test(db)]
    async fn test_table_graph_no_deps(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![
                TableNameDto::try_from("table_0")?,
                TableNameDto::try_from("table_1")?,
            ]
            .into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(&collection_name.to_string())?,
                },
                TableParam {
                    table: TableIdName::from_str("table_0")?,
                },
                TopologyReadParam {
                    topology_version: None,
                    at: None,
                },
            ))
            .in_project(p.project_id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &function_0.collection_id,
                &TableIdName::from_str("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .graph
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
            .graph
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        assert!(response.graph.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.graph.user_tables.len());

        let relations = response.graph.relations;
        assert_eq!(relations.len(), 2);
        for relation in relations {
            let f = relation.function_version_id;
            let edge = relation.edge;
            let t = response
                .graph
                .all_tables
                .get(&relation.table_version_id)
                .map(|t| t.name.clone())
                .unwrap();
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_table_graph_downstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[1].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: vec![TableTriggerDto::try_from("table_0")?].into(),
            tables: vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]
            .into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_1 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[2].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_3")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_2 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(&collection_name.to_string())?,
                },
                TableParam {
                    table: TableIdName::from_str("table_0")?,
                },
                TopologyReadParam {
                    topology_version: None,
                    at: None,
                },
            ))
            .in_project(p.project_id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table: TableDBWithNames = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &function_0.collection_id,
                &TableIdName::from_str("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_1, &function_2]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .graph
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
            .graph
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        assert!(response.graph.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.graph.user_tables.len());

        let relations = response.graph.relations;
        assert_eq!(relations.len(), 8);
        for relation in relations {
            let f = relation.function_version_id;
            let edge = relation.edge;
            let t = response
                .graph
                .all_tables
                .get(&relation.table_version_id)
                .map(|t| t.name.clone())
                .unwrap();
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
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

            if f == function_2.id && t == expected_tables[0] {
                assert!(matches!(edge, GraphEdge::Dependency { .. }));
            }
            if f == function_2.id && t == expected_tables[3] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_table_graph_upstream(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("collection_0")?;
        let collection_0 = db
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[1].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: None,
            tables: vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]
            .into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _function_1 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[2].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("table_0")?]),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_3")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_2 = db
            .scoped(p.project_id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let service = Services::with_defaults(db.clone());
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read((
                CollectionParam {
                    collection: CollectionIdName::from_str(&collection_name.to_string())?,
                },
                TableParam {
                    table: TableIdName::from_str("table_3")?,
                },
                TopologyReadParam {
                    topology_version: None,
                    at: None,
                },
            ))
            .in_project(p.project_id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table = db
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &function_2.collection_id,
                &TableIdName::from_str("table_3")?
            ))
            .await?;
        assert_eq!(response.table, central_table.id);

        let expected_functions = [&function_0, &function_2]
            .iter()
            .map(|f| (f.id.clone(), f.name.clone()))
            .collect::<HashMap<_, _>>();
        let functions = response
            .graph
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
            .graph
            .all_tables
            .values()
            .map(|t| t.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(tables, expected_tables);

        let relations = response.graph.relations;
        assert_eq!(relations.len(), 4);
        for relation in relations {
            let f = relation.function_version_id;
            let edge = relation.edge;
            let t = response
                .graph
                .all_tables
                .get(&relation.table_version_id)
                .map(|t| t.name.clone())
                .unwrap();
            if f == function_0.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
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

    #[td_test::test(db)]
    async fn test_table_graph_after_collection_delete(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("doomed")?;
        let collection = db
            .scoped(p.project_id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: FUNCTION_NAMES[0].clone(),
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_0")?].into(),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.project_id)
            .seed()
            .function(&collection, &create)
            .await?;
        let table: TableDBWithNames = db
            .scd2()
            .fetch_one(and!(
                &collection.collection_id,
                &TableIdName::from_str("table_0")?
            ))
            .await?;
        let registered_at = AtTime::now();

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

        let request = |collection: &str, at: Option<AtTime>| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read((
                    CollectionParam {
                        collection: CollectionIdName::from_str(collection).unwrap(),
                    },
                    TableParam {
                        table: TableIdName::from_str("table_0").unwrap(),
                    },
                    TopologyReadParam {
                        topology_version: None,
                        at,
                    },
                ))
                .in_project(p.project_id)
        };

        let by_name = collection_name.to_string();
        let by_id = format!("~{}", collection.collection_id);
        let err = service
            .table_flow_graph(request(&by_name, Some(registered_at)))
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.api_error(), ApiError::NotFound);

        let response = service
            .table_flow_graph(request(&by_id, Some(registered_at)))
            .await?
            .data;
        assert_eq!(response.table, table.id);
        assert!(response.graph.all_functions.contains_key(&function.id));

        assert!(
            service
                .table_flow_graph(request(&by_id, None))
                .await
                .is_err()
        );
        Ok(())
    }
}
