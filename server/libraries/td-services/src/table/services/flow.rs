//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::{FunctionOrTable, GraphConfig, generate_td_graph};
use crate::table::flow::build_response;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::table::{TableDBWithNames, TableFlowGraphResponse, TableParam};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;

#[service_factory]
async fn table_flow_graph(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<(CollectionParam, TableParam)>>,
) -> Result<TableFlowGraphResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, table_param)) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let collection_id_name = collection_param.collection;
    let table_id_name = table_param.table;
    let at = ctx.time;

    let table: TableDBWithNames = conn
        .as_of(&at)
        .fetch_one(and!(&collection_id_name, &table_id_name))
        .await?;

    let table_id = table.base.base.table_id;
    let entrypoint = FunctionOrTable::from(&table_id);
    let recursion = RecursionOptions::new(None, None);
    let config = GraphConfig {
        include_dependency_functions: true,
        ..GraphConfig::defaults(())
    };
    let graph = generate_td_graph(conn, config, &at, &entrypoint, &recursion).await?;

    build_response(&table, &graph).await
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
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::graph::GraphEdge;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionName, FunctionRuntimeValues,
        TableName, TableNameDto, UserId,
    };
    use td_objects::types::basic::{CollectionIdName, TableIdName};
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
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.id)
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
            ))
            .in_project(p.id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table = db
            .latest()
            .fetch_one::<TableDBWithNames>(and!(
                &function_0.base.collection_id,
                &TableIdName::from_str("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.base.base.id);

        let expected_functions = [&function_0]
            .iter()
            .map(|f| (f.base.id.clone(), f.base.name.clone()))
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

        assert!(response.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.user_tables.len());

        let relations = response.relations;
        assert_eq!(relations.len(), 2);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            if f == function_0.base.id && t == expected_tables[0] {
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
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_1 = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_2 = db
            .scoped(p.id)
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
            ))
            .in_project(p.id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table: TableDBWithNames = db
            .latest()
            .fetch_one::<TableDBWithNames>(and!(
                &function_0.base.collection_id,
                &TableIdName::from_str("table_0")?
            ))
            .await?;
        assert_eq!(response.table, central_table.base.base.id);

        let expected_functions = [&function_0, &function_1, &function_2]
            .iter()
            .map(|f| (f.base.id.clone(), f.base.name.clone()))
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

        assert!(response.system_tables.is_empty());
        assert_eq!(expected_tables.len(), response.user_tables.len());

        let relations = response.relations;
        assert_eq!(relations.len(), 8);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            if f == function_0.base.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
            if f == function_1.base.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                ));
            }
            if f == function_1.base.id && t == expected_tables[1] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
            if f == function_1.base.id && t == expected_tables[2] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }

            if f == function_2.base.id && t == expected_tables[0] {
                assert!(matches!(edge, GraphEdge::Dependency { .. }));
            }
            if f == function_2.base.id && t == expected_tables[3] {
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
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_0 = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _function_1 = db
            .scoped(p.id)
            .seed()
            .function(&collection_0, &create)
            .await?;

        let create = FunctionRegister {
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
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function_2 = db
            .scoped(p.id)
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
            ))
            .in_project(p.id);

        let response = service.table_flow_graph(request).await?.data;

        let central_table = db
            .latest()
            .fetch_one::<TableDBWithNames>(and!(
                &function_2.base.collection_id,
                &TableIdName::from_str("table_3")?
            ))
            .await?;
        assert_eq!(response.table, central_table.base.base.id);

        let expected_functions = [&function_0, &function_2]
            .iter()
            .map(|f| (f.base.id.clone(), f.base.name.clone()))
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

        let relations = response.relations;
        assert_eq!(relations.len(), 4);
        for (f, t, edge) in relations {
            let t = response.all_tables.get(&t).map(|t| t.name.clone()).unwrap();
            if f == function_0.base.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
            if f == function_2.base.id && t == expected_tables[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                ));
            }
            if f == function_2.base.id && t == expected_tables[1] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
        }
        Ok(())
    }
}
