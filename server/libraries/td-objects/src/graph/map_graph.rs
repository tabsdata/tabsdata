//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::graph::{GraphEdge, GraphNode};
use crate::graph::TdGraph;
use petgraph::prelude::EdgeRef;
use petgraph::visit::IntoNodeReferences;
use std::collections::HashMap;
use td_error::TdError;

pub trait MapGraph<F, T, V> {
    type Output<FF, TT, VV>: MapGraph<FF, TT, VV>;

    /// Transforms the graph edges by applying transformation functions
    fn map<'a, CN, RN, CE, RE, FF, TT, VV>(
        &'a self,
        transform_nodes: CN,
        transform_edges: CE,
    ) -> impl Future<Output = Result<Self::Output<FF, TT, VV>, TdError>>
    where
        CN: FnMut(&'a GraphNode<F, T>) -> RN,
        RN: Future<Output = Result<GraphNode<FF, TT>, TdError>>,
        CE: FnMut(&'a GraphEdge<V>, &'a GraphNode<F, T>, &'a GraphNode<F, T>) -> RE,
        RE: Future<Output = Result<VV, TdError>>,
        F: 'a,
        T: 'a,
        V: 'a;
}

impl<F, T, V> MapGraph<F, T, V> for TdGraph<F, T, V> {
    type Output<FF, TT, VV> = TdGraph<FF, TT, VV>;

    async fn map<'a, CN, RN, CE, RE, FF, TT, VV>(
        &'a self,
        mut transform_nodes: CN,
        mut transform_edges: CE,
    ) -> Result<Self::Output<FF, TT, VV>, TdError>
    where
        CN: FnMut(&'a GraphNode<F, T>) -> RN,
        RN: Future<Output = Result<GraphNode<FF, TT>, TdError>>,
        CE: FnMut(&'a GraphEdge<V>, &'a GraphNode<F, T>, &'a GraphNode<F, T>) -> RE,
        RE: Future<Output = Result<VV, TdError>>,
    {
        let mut node_futures = Vec::new();
        for (idx, node) in self.inner.node_references() {
            let fut = transform_nodes(node);
            node_futures.push(async move {
                let new_node = fut.await?;
                Ok::<_, TdError>((idx, new_node))
            });
        }
        let mut updated_nodes = futures::future::try_join_all(node_futures)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        // TODO this does a query per edge, we can optimize if table + version is the same (adding a transform cache)
        // TODO we could probably also skip non triggered nodes, setting just None
        let mut edge_futures = Vec::new();
        for edge in self.inner.edge_references() {
            let source = &self.inner[edge.source()];
            let target = &self.inner[edge.target()];
            let fut = transform_edges(edge.weight(), source, target);
            edge_futures.push(async move {
                let new_version = fut.await?;
                Ok::<_, TdError>((edge.id(), new_version))
            });
        }
        let mut updated_versions = futures::future::try_join_all(edge_futures)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let new_graph = self.inner.map(
            |node_idx, _| updated_nodes.remove(&node_idx).unwrap(),
            |idx, edge_weight| {
                let new_version = updated_versions.remove(&idx).unwrap();
                edge_weight.versioned(new_version)
            },
        );

        Ok(TdGraph { inner: new_graph })
    }
}
