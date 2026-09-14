//
// Copyright 2026 Tabsdata Inc.
//

//! Folds the per-table rows of a `/metrics` read into nodes and edges; a
//! table with versions in two statuses counts once.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::metrics::names::Names;
use td_error::TdError;
use td_objects::dxo::metrics::{
    Dimension, EdgeRow, EdgeStatusMeasures, NodeKind, NodeRow, NodeStatusMeasures,
};
use td_objects::dxo::metrics_facts::MetricsTableRow;
use td_objects::types::basic::{
    ByteCount, CollectionId, EdgeKind, FactSide, FunctionRunCount, GroupId,
    LogicalFunctionRunStatus, ProjectId, RowCount, TableCount, TableId, TableVersionCount,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Endpoint {
    Collection(CollectionId),
    Group(GroupId),
}

impl Endpoint {
    fn of(dim: Dimension, collection: CollectionId, group: GroupId) -> Self {
        match dim {
            Dimension::Collection => Endpoint::Collection(collection),
            Dimension::Group => Endpoint::Group(group),
        }
    }

    fn id(&self) -> String {
        match self {
            Endpoint::Collection(id) => id.to_string(),
            Endpoint::Group(id) => id.to_string(),
        }
    }

    fn name(&self, names: &Names) -> Option<String> {
        match self {
            Endpoint::Collection(id) => names.collection(id).map(|n| n.to_string()),
            Endpoint::Group(id) => names.group(id).map(|n| n.to_string()),
        }
    }
}

#[derive(Default)]
struct Tables {
    all: HashSet<TableId>,
    by_status: HashMap<LogicalFunctionRunStatus, HashSet<TableId>>,
}

impl Tables {
    fn add(&mut self, status: LogicalFunctionRunStatus, table: Option<TableId>) {
        if let Some(table) = table {
            self.all.insert(table);
            self.by_status.entry(status).or_default().insert(table);
        }
    }

    fn count(&self) -> Result<TableCount, TdError> {
        TableCount::try_from(self.all.len() as i64)
    }

    fn within(&self, status: LogicalFunctionRunStatus) -> Result<TableCount, TdError> {
        TableCount::try_from(self.by_status.get(&status).map_or(0, |t| t.len() as i64))
    }
}

/// A table is unread only when none of its versions in the window was.
#[derive(Default)]
struct Unread {
    per_table: HashMap<TableId, (i64, i64)>,
}

impl Unread {
    fn add(&mut self, row: &MetricsTableRow) {
        if let Some(table) = row.table_id {
            let (versions, unconsumed) = self.per_table.entry(table).or_default();
            *versions += row.versions;
            *unconsumed += row.versions_unconsumed;
        }
    }

    fn count(&self) -> Result<TableCount, TdError> {
        let unread = self
            .per_table
            .values()
            .filter(|(versions, unconsumed)| *unconsumed > 0 && unconsumed == versions)
            .count();
        TableCount::try_from(unread as i64)
    }
}

#[derive(Default)]
struct NodeSums {
    fn_runs: i64,
    versions_out: i64,
    unconsumed_out: i64,
    rows_out: i64,
    bytes_out: i64,
    versions_in: i64,
    rows_in: i64,
    bytes_in: i64,
}

impl NodeSums {
    fn measures(
        &self,
        tables_out: TableCount,
        tables_in: TableCount,
    ) -> Result<NodeStatusMeasures, TdError> {
        Ok(NodeStatusMeasures {
            fn_runs: FunctionRunCount::try_from(self.fn_runs)?,
            table_versions_out: TableVersionCount::try_from(self.versions_out)?,
            logical_tables_out: tables_out,
            versions_unconsumed: TableVersionCount::try_from(self.unconsumed_out)?,
            row_count_out: RowCount::try_from(self.rows_out)?,
            data_bytes_out: ByteCount::try_from(self.bytes_out)?,
            table_versions_in: TableVersionCount::try_from(self.versions_in)?,
            logical_tables_in: tables_in,
            row_count_in: RowCount::try_from(self.rows_in)?,
            data_bytes_in: ByteCount::try_from(self.bytes_in)?,
        })
    }
}

#[derive(Default)]
struct NodeAcc {
    by_status: BTreeMap<LogicalFunctionRunStatus, NodeSums>,
    out: Tables,
    inbound: Tables,
    unread: Unread,
}

impl NodeAcc {
    fn add(&mut self, row: &MetricsTableRow) {
        let m = self.by_status.entry(row.run_status).or_default();
        match row.side {
            FactSide::Attempt => m.fn_runs += row.fn_runs,
            FactSide::Written => {
                m.versions_out += row.versions_with_data;
                m.unconsumed_out += row.versions_unconsumed;
                m.rows_out += row.row_count;
                m.bytes_out += row.data_bytes;
                self.out.add(row.run_status, row.table_id);
                self.unread.add(row);
            }
            FactSide::Read => {
                m.versions_in += row.versions;
                m.rows_in += row.row_count;
                m.bytes_in += row.data_bytes;
                self.inbound.add(row.run_status, row.table_id);
            }
            FactSide::Egress => {}
        }
    }

    fn by_status(&self) -> Result<BTreeMap<LogicalFunctionRunStatus, NodeStatusMeasures>, TdError> {
        self.by_status
            .iter()
            .map(|(status, sums)| {
                Ok((
                    *status,
                    sums.measures(self.out.within(*status)?, self.inbound.within(*status)?)?,
                ))
            })
            .collect()
    }
}

pub(crate) fn fold_nodes(
    dim: Dimension,
    rows: &[MetricsTableRow],
    names: &Names,
) -> Result<Vec<NodeRow>, TdError> {
    let mut acc: HashMap<(ProjectId, Endpoint), NodeAcc> = HashMap::new();
    for row in rows {
        let key = (
            row.project_id,
            Endpoint::of(dim, row.collection_id, row.group_id),
        );
        acc.entry(key).or_default().add(row);
    }
    let group_of: HashMap<CollectionId, GroupId> =
        rows.iter().map(|r| (r.collection_id, r.group_id)).collect();

    let mut nodes = Vec::with_capacity(acc.len());
    for ((project, endpoint), acc) in acc {
        let (collection_id, group_id) = match endpoint {
            Endpoint::Collection(c) => (Some(c), group_of.get(&c).copied()),
            Endpoint::Group(g) => (None, Some(g)),
        };
        nodes.push(NodeRow {
            project_id: Some(project),
            project_name: names.project(&project),
            kind: NodeKind::Dim,
            collection_id,
            collection_name: collection_id.and_then(|c| names.collection(&c)),
            group_id,
            group_name: group_id.and_then(|g| names.group(&g)),
            logical_tables_out: acc.out.count()?,
            tables_unread: acc.unread.count()?,
            logical_tables_in: acc.inbound.count()?,
            by_status: acc.by_status()?,
        });
    }
    nodes.sort_by(|a, b| {
        (a.project_id, a.collection_id, a.group_id).cmp(&(
            b.project_id,
            b.collection_id,
            b.group_id,
        ))
    });
    Ok(nodes)
}

#[derive(Default)]
struct EdgeSums {
    fn_runs: i64,
    versions: i64,
    rows: i64,
    bytes: i64,
}

impl EdgeSums {
    fn measures(&self, tables: TableCount) -> Result<EdgeStatusMeasures, TdError> {
        Ok(EdgeStatusMeasures {
            fn_runs: FunctionRunCount::try_from(self.fn_runs)?,
            tables: TableVersionCount::try_from(self.versions)?,
            logical_tables: tables,
            row_count: RowCount::try_from(self.rows)?,
            data_bytes: ByteCount::try_from(self.bytes)?,
        })
    }
}

#[derive(Default)]
struct EdgeAcc {
    by_status: BTreeMap<LogicalFunctionRunStatus, EdgeSums>,
    tables: Tables,
}

impl EdgeAcc {
    fn add(&mut self, row: &MetricsTableRow) {
        let m = self.by_status.entry(row.run_status).or_default();
        m.fn_runs += row.fn_runs;
        m.versions += row.versions;
        m.rows += row.row_count;
        m.bytes += row.data_bytes;
        self.tables.add(row.run_status, row.table_id);
    }

    fn by_status(&self) -> Result<BTreeMap<LogicalFunctionRunStatus, EdgeStatusMeasures>, TdError> {
        self.by_status
            .iter()
            .map(|(status, sums)| Ok((*status, sums.measures(self.tables.within(*status)?)?)))
            .collect()
    }
}

pub(crate) fn fold_edges(
    dim: Dimension,
    rows: &[MetricsTableRow],
    names: &Names,
) -> Result<Vec<EdgeRow>, TdError> {
    let mut acc: HashMap<(ProjectId, EdgeKind, Option<Endpoint>, Option<Endpoint>), EdgeAcc> =
        HashMap::new();
    for row in rows {
        let Some(kind) = row.edge_kind else {
            continue;
        };
        let dim_end = Some(Endpoint::of(dim, row.collection_id, row.group_id));
        let source_end = row
            .from_collection_id
            .zip(row.from_group_id)
            .map(|(c, g)| Endpoint::of(dim, c, g));
        let (source, target) = match kind {
            EdgeKind::ExternalSource => (None, dim_end),
            EdgeKind::Real => (source_end, dim_end),
            EdgeKind::Egress => (dim_end, None),
        };
        acc.entry((row.project_id, kind, source, target))
            .or_default()
            .add(row);
    }

    let mut edges = Vec::with_capacity(acc.len());
    for ((project, kind, source, target), acc) in acc {
        edges.push(EdgeRow {
            project_id: Some(project),
            project_name: names.project(&project),
            source_kind: match kind {
                EdgeKind::ExternalSource => NodeKind::ExternalSource,
                _ => NodeKind::Dim,
            },
            source_name: source.and_then(|e| e.name(names)),
            source: source.map(|e| e.id()),
            target_kind: match kind {
                EdgeKind::Egress => NodeKind::ExternalDestination,
                _ => NodeKind::Dim,
            },
            target_name: target.and_then(|e| e.name(names)),
            target: target.map(|e| e.id()),
            logical_tables: acc.tables.count()?,
            by_status: acc.by_status()?,
        });
    }
    edges.sort_by(|a, b| {
        (&a.project_id, &a.source, &a.target).cmp(&(&b.project_id, &b.source, &b.target))
    });
    Ok(edges)
}

/// An edge's tables belong to one collection, so summing them over the edges
/// keeps them distinct.
pub(crate) fn external_nodes(edges: &[EdgeRow]) -> Result<Vec<NodeRow>, TdError> {
    #[derive(Default)]
    struct Sums {
        by_status: BTreeMap<LogicalFunctionRunStatus, (NodeSums, i64)>,
        tables: i64,
    }
    let mut acc: HashMap<NodeKind, Sums> = HashMap::new();
    for edge in edges {
        let kind = match (edge.source_kind, edge.target_kind) {
            (NodeKind::ExternalSource, _) => NodeKind::ExternalSource,
            (_, NodeKind::ExternalDestination) => NodeKind::ExternalDestination,
            _ => continue,
        };
        let node = acc.entry(kind).or_default();
        node.tables += *edge.logical_tables;
        for (status, e) in &edge.by_status {
            let (m, tables) = node.by_status.entry(*status).or_default();
            m.fn_runs += *e.fn_runs;
            *tables += *e.logical_tables;
            if kind == NodeKind::ExternalSource {
                m.versions_out += *e.tables;
                m.rows_out += *e.row_count;
                m.bytes_out += *e.data_bytes;
            } else {
                m.versions_in += *e.tables;
                m.rows_in += *e.row_count;
                m.bytes_in += *e.data_bytes;
            }
        }
    }

    let none = TableCount::try_from(0)?;
    let split = |kind: NodeKind, tables: TableCount| match kind {
        NodeKind::ExternalSource => (tables, none),
        _ => (none, tables),
    };
    let mut nodes = Vec::with_capacity(acc.len());
    for (kind, node) in acc {
        let (out, inbound) = split(kind, TableCount::try_from(node.tables)?);
        let mut by_status = BTreeMap::new();
        for (status, (sums, tables)) in &node.by_status {
            let (tables_out, tables_in) = split(kind, TableCount::try_from(*tables)?);
            by_status.insert(*status, sums.measures(tables_out, tables_in)?);
        }
        nodes.push(NodeRow {
            project_id: None,
            project_name: None,
            kind,
            collection_id: None,
            collection_name: None,
            group_id: None,
            group_name: None,
            logical_tables_out: out,
            tables_unread: none,
            logical_tables_in: inbound,
            by_status,
        });
    }
    nodes.sort_by_key(|n| n.kind);
    Ok(nodes)
}
