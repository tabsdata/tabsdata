#
# Copyright 2024 Tabs Data Inc.
#

# Disclaimer: This is an experimental script not for use in production environments.
# There might be some hard-code values that you might need to adjust to your current
# setup. You might also need to install some additional Python packages to run it.

import os
import sqlite3

import graphviz
import igraph as ig
import networkx
import pydot
from asciinet import graph_to_ascii
from networkx.drawing.nx_pydot import from_pydot
from nicegui import ui

script_folder = os.path.dirname(os.path.abspath(__file__))
database_path = os.path.abspath(os.path.join(script_folder, "tabsdata.db"))


def create_dag(nodes, edges, kind):
    g = ig.Graph.Erdos_Renyi(n=nodes, m=edges, directed=False, loops=False)
    g.to_directed(mode="acyclic")

    dag = networkx.DiGraph()
    dag.add_nodes_from(range(g.vcount()))
    for edge in g.es:
        source, target = edge.tuple
        dag.add_edge(source, target, label=kind)
        print(source, " - ", target, " - ", kind)
    return dag


def merge_graphs(dag_t, dag_d):
    dag = networkx.DiGraph()
    dag.add_edges_from(dag_t.edges(data=True))
    dag.add_edges_from(dag_d.edges(data=True))
    return dag


def create_database(graph):
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    cursor.execute(
        """create table if not exists dataset
                      (source int,
                       target int,
                       kind text)"""
    )

    for edge in graph.edges(data=True):
        cursor.execute(
            "insert into dataset values (?, ?, ?)", (edge[0], edge[1], edge[2]["label"])
        )

    connection.commit()
    connection.close()


def query_reachable_nodes(center_node):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    recursive_query = """
with recursive outgoing_t_edges(source,
                                target,
                                kind) as (      select source,
                                                       target,
                                                       kind
                                                  from dataset
                                                 where source = :center_node
                                                   and kind = 't'
                                          union
                                                select d.source,
                                                       d.target,
                                                       d.kind
                                                  from dataset d
                                                  join outgoing_t_edges o
                                                    on d.source = o.target
                                                 where d.kind = 't'),
               incoming_t_edges(source,
                                target,
                                kind) as (      select source,
                                                       target,
                                                       kind
                                                  from dataset
                                                 where target = :center_node
                                                   and kind = 't'
                                          union
                                                select d.source,
                                                       d.target,
                                                       d.kind
                                                  from dataset d
                                                  join incoming_t_edges i
                                                    on d.target = i.source
                                                 where d.kind = 't'),
               outgoing_d_edges(source,
                                target,
                                kind) as (      select source,
                                                       target,
                                                       kind
                                                  from dataset
                                                 where source = :center_node
                                                   and kind = 'd'
                                          union
                                                select d.source,
                                                       d.target,
                                                       d.kind
                                                  from dataset d
                                                  join outgoing_d_edges o
                                                    on d.source = o.target
                                                 where d.kind = 'd'),
               incoming_d_edges(source,
                                target,
                                kind) as (      select source,
                                                       target,
                                                       kind
                                                  from dataset
                                                 where target = :center_node
                                                   and kind = 'd'
                                          union
                                                select d.source,
                                                       d.target,
                                                       d.kind
                                                  from dataset d
                                                  join incoming_d_edges i
                                                    on d.target = i.source
                                                 where d.kind = 'd')
       select source,
              target,
              kind,
              'outgoing' as direction
         from outgoing_t_edges
union
       select source,
              target,
              kind,
              'incoming' as direction
         from incoming_t_edges
union
       select source,
              target,
              kind,
              'outgoing' as direction
         from outgoing_d_edges
union
       select source,
              target,
              kind,
              'incoming' as direction
         from incoming_d_edges;

    """

    global_query = """
    select source,
           target,
           kind,
           'both'
      from dataset
    """

    if center_node == 0:
        cursor.execute(global_query)
    else:
        cursor.execute(recursive_query, {"center_node": center_node})

    rows = cursor.fetchall()

    conn.close()
    return rows


def visualize_dags(center_node):
    edges = query_reachable_nodes(center_node)

    mermaid_text = """
    %%{init: {'theme': 'neutral'}}%%
    graph TD\n
    """

    link_counter = 0
    link_styles = []
    for edge in edges:
        source, target, kind, direction = edge
        mermaid_text += f"    {source} --> {target}\n"
        if kind == "t":
            link_styles.append(f"linkStyle {link_counter} stroke:red,stroke-width:2px;")
        elif kind == "d":
            link_styles.append(
                f"linkStyle {link_counter} stroke:blue,stroke-width:2px;"
            )
        link_counter += 1
    mermaid_text += "\n" + "\n".join(link_styles)
    print(mermaid_text)

    dot = graphviz.Digraph(comment=f"DataStore Graph for DataSet {center_node}")
    dot.attr(splines="ortho")
    dot.attr(rankdir="LR")
    dot.attr(ranksep="4")

    nodes = set()
    for edge in edges:
        source, target, kind, direction = edge

        nodes.add(source)
        nodes.add(target)

        if kind == "t":
            dot.edge(str(source), str(target), color="red")
        elif kind == "d":
            dot.edge(str(source), str(target), color="blue")

    for node in nodes:
        dot.node(str(node), str(node))

    base_file = f"dag_at_{center_node}"
    base_file = os.path.abspath(os.path.join(script_folder, base_file))

    dot.save(f"{base_file}.dot")

    dot.render(base_file, format="png", cleanup=True, engine="dot")

    # ASCII
    with open(f"{base_file}.dot", "r") as file:
        dot_file = file.read()
    pydot_graph = pydot.graph_from_dot_data(dot_file)[0]
    networkx_graph = from_pydot(pydot_graph)
    ascii_graph = graph_to_ascii(networkx_graph)
    print(ascii_graph)

    # NiceGUI
    ui.mermaid(mermaid_text)
    ui.run(native=True)

    print(f"DataStore Graph for DataSet {center_node} saved as {base_file}")


def main(center_node):
    generate = True

    if generate:
        nodes = 10

        dag_t = create_dag(nodes, 4, "t")
        dag_d = create_dag(nodes, 4, "d")

        dag = merge_graphs(dag_t, dag_d)

        create_database(dag)

    visualize_dags(center_node)

    a = """
with recursive whole_deps_graph(node) as (
           select 'd0' as node
    union
           select deps.tgt as node
             from deps
       inner join whole_deps_graph
               on deps.src = whole_deps_graph.node
    union
           select deps.src as node
             from deps
       inner join whole_deps_graph
               on deps.tgt = whole_deps_graph.node
)
  select src,
         tgt
    from deps
   where src in (select node
                   from whole_deps_graph)
      or tgt in (select node
                   from whole_deps_graph)
order by src,
         tgt; (edited)
    """

    b = """
with recursive whole_deps_graph(node) as (
           select column1 as node
             from (values ('d0'),
                          ('d4'))
    union
           select deps.tgt as node
             from deps
       inner join whole_deps_graph
               on deps.src = whole_deps_graph.node
    union
           select deps.src as node
             from deps
       inner join whole_deps_graph
               on deps.tgt = whole_deps_graph.node
)
  select src,
         tgt
    from deps
   where src in (select node
                   from whole_deps_graph)
      or tgt in (select node
                   from whole_deps_graph)
order by src,
         tgt; (edited)
    """

    print(f"{a}")
    print(f"{b}")


main("1")


# if __name__ == '__main__':
#    input_center_node = int(input('Enter the root node: '))
#    main(input_center_node)
