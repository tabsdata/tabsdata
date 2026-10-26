//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::graph_response;
use crate::topology::read_version;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{GatedConnection, InProject, RO};
use td_error::TdError;
use td_execution::topology::ProjectTopology;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::graph::ProjectGraphResponse;
use td_objects::dxo::topology::TopologyReadParam;
use td_objects::types::basic::AtTime;

#[service_factory]
async fn project_graph(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<TopologyReadParam>>,
) -> Result<ProjectGraphResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, topology_param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);

    let version = read_version(conn, &project, &topology_param).await?;
    let topology = ProjectTopology::of(conn, version).await?;
    let graph = topology.graph()?;
    Ok(ProjectGraphResponse {
        version: topology.version.id,
        at: AtTime::from(topology.version.interval.valid_from),
        graph: graph_response(&graph, topology.labels(&graph)?).await?,
    })
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use crate::tests::catalog_races::admin;
    use crate::tests::fixtures::register;
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_common::id::Id;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_error::TdError;
    use td_objects::Typed;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::dxo::function::{
        ChildFunction, FunctionParam, FunctionRegister, FunctionUpdate,
    };
    use td_objects::dxo::graph::{GraphEdge, ProjectGraphResponse};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::topology::TopologyReadParam;
    use td_objects::types::basic::{
        BundleId, CollectionIdName, CollectionName, FunctionIdName, FunctionType, GroupName,
        ProjectId, TableNameDto, TableType, TagValue, TopologyTagKey, TopologyTags,
        TopologyVersionId, UserId,
    };
    use td_objects::types::basic::{CollectionLabel, GroupLabel};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_objects::types::label::Label;
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn graph(
        db: &TdPool,
        project: ProjectId,
        version: Option<TopologyVersionId>,
    ) -> Result<ProjectGraphResponse, TdError> {
        let request = admin()
            .read(TopologyReadParam {
                topology_version: version,
                at: None,
            })
            .in_project(project);
        Ok(Services::with_defaults(db.clone())
            .project_graph(request)
            .await?
            .data)
    }

    /// What two reads of the same topology agree on, order aside.
    fn shape(graph: &ProjectGraphResponse) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut functions: Vec<String> = graph
            .graph
            .all_functions
            .iter()
            .map(|(id, f)| format!("{}/{id}", f.name))
            .collect();
        let mut tables: Vec<String> = graph
            .graph
            .all_tables
            .iter()
            .map(|(id, t)| format!("{}/{id}", t.name))
            .collect();
        let mut relations: Vec<String> = graph
            .graph
            .relations
            .iter()
            .map(|r| format!("{r:?}"))
            .collect();
        functions.sort();
        tables.sort();
        relations.sort();
        (functions, tables, relations)
    }

    async fn setup(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
        db.scoped(project)
            .seed()
            .collection(&CollectionName::try_from("c")?, &UserId::admin())
            .await?;
        let services = Services::with_defaults(db.clone());
        for register in [
            FunctionRegister {
                tables: Some(vec![
                    TableNameDto::try_from("t_a")?,
                    TableNameDto::try_from("t_b")?,
                ]),
                ..register("producer", None)
            },
            FunctionRegister {
                dependencies: Some(vec![TableDependencyDto::try_from("t_a")?]),
                triggers: Some(vec![TableTriggerDto::try_from("t_a")?]),
                ..register("reader", None)
            },
        ] {
            let request = admin()
                .create(
                    CollectionParam {
                        collection: CollectionIdName::from_str("c")?,
                    },
                    register,
                )
                .in_project(project);
            services.register_function(request).await?;
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_project_graph_holds_the_whole_topology(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        setup(&db, p.project_id).await?;

        let graph = graph(&db, p.project_id, None).await?;
        assert_eq!(graph.graph.all_functions.len(), 2);
        assert_eq!(graph.graph.user_tables.len(), 3);
        let count = |user: bool, kind: fn(&GraphEdge<_>) -> bool| {
            graph
                .graph
                .relations
                .iter()
                .filter(|r| {
                    !*graph.graph.all_tables[&r.table_version_id].system == user && kind(&r.edge)
                })
                .count()
        };
        assert_eq!(count(true, |e| matches!(e, GraphEdge::Output { .. })), 3);
        assert_eq!(
            count(true, |e| matches!(e, GraphEdge::Dependency { .. })),
            1
        );
        assert_eq!(count(true, |e| matches!(e, GraphEdge::Trigger { .. })), 1);

        let [(collection_id, label)] =
            graph.graph.labels.collections.iter().collect::<Vec<_>>()[..]
        else {
            panic!("one collection: {:?}", graph.graph.labels.collections);
        };
        assert_eq!(
            label.name,
            CollectionLabel::new(CollectionName::try_from("c")?)
        );
        let group_id = label.group_id;
        let group = &graph.graph.labels.groups[&group_id];
        assert_eq!(group.name, GroupLabel::new(GroupName::default_group()));
        assert!(
            graph
                .graph
                .all_functions
                .values()
                .all(|f| f.collection_id == *collection_id && f.group_id == group_id)
        );
        assert!(
            graph
                .graph
                .all_tables
                .values()
                .all(|t| t.group_id == group_id)
        );
        Ok(())
    }

    /// A past version keeps its functions and tables, but labels its
    /// collection as it is now: deleted, by its id.
    #[td_test::test(db)]
    async fn test_project_graph_labels_its_collections_as_they_are_now(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        setup(&db, p.project_id).await?;
        let before = graph(&db, p.project_id, None).await?;

        let delete = admin()
            .delete(CollectionParam {
                collection: CollectionIdName::from_str("c")?,
            })
            .in_project(p.project_id);
        Services::with_defaults(db.clone())
            .delete_collection(delete)
            .await?;

        let replayed = graph(&db, p.project_id, Some(before.version)).await?;
        assert_eq!(shape(&replayed), shape(&before));
        let labels: Vec<_> = replayed.graph.labels.collections.iter().collect();
        assert_eq!(labels.len(), 1);
        let (collection_id, label) = labels[0];
        assert_eq!(
            label.name,
            CollectionLabel::new(Label::Deleted(*collection_id))
        );
        let group = &replayed.graph.labels.groups[&label.group_id];
        assert_eq!(group.name, GroupLabel::new(GroupName::default_group()));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_project_graph_of_its_version_replays_it(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        setup(&db, p.project_id).await?;
        let before = graph(&db, p.project_id, None).await?;

        let drop_t_a = admin()
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("c")?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("producer")?,
                    },
                ),
                FunctionUpdate {
                    tables: Some(vec![TableNameDto::try_from("t_b")?]),
                    ..Default::default()
                },
            )
            .in_project(p.project_id);
        Services::with_defaults(db.clone())
            .update_function(drop_t_a)
            .await?;

        let after = graph(&db, p.project_id, None).await?;
        assert!(after.at > before.at);
        assert_ne!(after.version, before.version);
        assert_ne!(shape(&after), shape(&before));

        let replayed = graph(&db, p.project_id, Some(before.version)).await?;
        assert_eq!(replayed.version, before.version);
        assert_eq!(replayed.at, before.at);
        assert_eq!(shape(&replayed), shape(&before));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_topology_versions_list_the_project_timeline(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        setup(&db, p.project_id).await?;
        let latest = graph(&db, p.project_id, None).await?;

        let request = admin()
            .list((), ListParams::default())
            .in_project(p.project_id);
        let versions = Services::with_defaults(db.clone())
            .list_topology_versions(request)
            .await?
            .data;
        let mut versions = versions.data;
        versions.sort_by_key(|a| a.valid_from);
        assert!(versions.len() > 1, "creation and setup each open a version");
        for pair in versions.windows(2) {
            assert!(!pair[0].valid_to.is_infinity());
        }
        let open = versions.last().expect("an open version");
        assert!(open.valid_to.is_infinity());
        assert_eq!(open.id, latest.version);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_project_graph_of_an_empty_project(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let graph = graph(&db, p.project_id, None).await?;
        assert!(graph.graph.all_functions.is_empty() && graph.graph.relations.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_project_graph_nodes_carry_their_system_tags(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        db.scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("c")?, &UserId::admin())
            .await?;
        let trend = TableNameDto::try_from("owner_trends")?;
        let owner = FunctionRegister {
            parent_tables: Some(HashMap::from([(
                trend.clone(),
                TableNameDto::try_from("owner_t0")?,
            )])),
            child_functions: Some(vec![ChildFunction {
                name: "owner_trend".try_into()?,
                function_type: FunctionType::DQSummaryTrends,
                bundle_id: BundleId::default(),
                snippet: "s".try_into()?,
                dependencies: Some(vec![TableDependencyDto::try_from("owner_t0")?]),
                tables: Some(vec![trend.clone()]),
                table_types: Some(HashMap::from([(trend, TableType::DQSummaryTrends)])),
            }]),
            ..register("owner", None)
        };
        let request = admin()
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str("c")?,
                },
                owner,
            )
            .in_project(p.project_id);
        let owner = Services::with_defaults(db.clone())
            .register_function(request)
            .await?
            .data;

        let graph = graph(&db, p.project_id, None).await?;
        let function = |name: &str| {
            graph
                .graph
                .all_functions
                .values()
                .find(|f| f.name.to_string() == name)
                .unwrap()
        };
        let table = |name: &str| {
            graph
                .graph
                .all_tables
                .values()
                .find(|t| t.name.to_string() == name)
                .unwrap()
        };
        fn parent<T: Typed<Inner = Id>>(id: T) -> TopologyTags {
            TopologyTags::from([(TopologyTagKey::Parent, TagValue::of_id(id))])
        }
        assert_eq!(function("owner").tags, TopologyTags::new());
        assert_eq!(function("owner_trend").tags, parent(owner.function_id));
        let owner_tables: Vec<TableDB> = db.scd2().fetch_all(&owner.id).await?;
        let owner_table = owner_tables
            .iter()
            .find(|t| t.name.to_string() == "owner_t0")
            .expect("the owner's table");
        assert_eq!(table("owner_t0").tags, TopologyTags::new());
        assert_eq!(table("owner_trends").tags, parent(owner_table.table_id));
        Ok(())
    }
}
