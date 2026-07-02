import neo4j
import os
import pandas as pd

database = neo4j.GraphDatabase.driver("bolt://localhost:7687")
PWD = os.getcwd()
PATH = "../data-gen/sf3/"


def run_query(query):
    _, summary, _ = database.execute_query(query)
    print(summary.result_available_after)
    print(summary.counters)


def del_prop(node, prop):
    """Remove property `prop` from every node labeled `node`."""
    print(f"Delete {node}.{prop}")
    run_query(f"""
    MATCH (n:{node})
    REMOVE n.{prop}
    """)


def create_node(node, source):
    """Create a `node`-labeled node for each `source`-labeled node, copying only node_id."""
    print(f"Create node {node}")
    run_query(f"""
    MATCH (n:{source})
    CREATE (m:{node} {{ node_id: n.node_id }})
    """)


def create_node_index(node, source):
    """
    Unused
    """
    print(f"Create node {node}")
    run_query(f"""
    MATCH (n:{source})
    CREATE (m:{node} {{ node_id: n.node_id }})
    """)
    run_query(f"""
    CREATE INDEX {node}_index FOR (n:{node}) ON (n.node_id)
    """)


def add_prop(node, prop, snode, sprop):
    """Set `node`.`prop` from `snode`.`sprop` by matching on node_id."""
    print(f"Add {node}.{prop}")
    run_query(f"""
    MATCH (n:{node})
    MATCH (m:{snode} {{ node_id: n.node_id }})
    SET n.{prop} = m.{sprop}
    """)


def import_prop(node, prop, file, fileindex):
    """Load a pipe-delimited CSV and merge column `fileindex` into `node`.`prop`, matching/creating nodes by node_id (CSV column 0)."""
    print(f"Create {node}.{prop}")
    run_query(f"""
LOAD CSV FROM 'file:///{PWD}/{PATH}/{file}.csv' AS row FIELDTERMINATOR '|'
MERGE (n:{node} {{ node_id: toString(row[0]) }})
SET n.{prop} = row[{fileindex}]
                           """)


def del_node(node):
    """Delete every `node`-labeled node along with its relationships."""
    print(f"Delete {node}")
    run_query(f"""
    MATCH (n:{node})
    DETACH DELETE n
    """)


# def create_edge(name, from_node, to):
#     print(f"Create {from_node}-[{name}]-{to}")
#     run_query(f"""
#     MATCH (f:{from_node})
#     MATCH (t:{to} {{ node_id: f.node_id}})
#     CREATE (f)-[r:{name}]->(t)
#     """)


def create_node_from_prop(node, source, sprop):
    """Create one `node`-labeled node (node_id = value) per distinct non-null `source`.`sprop` value."""
    print(f"Create node {node} from {source}.{sprop}")
    run_query(f"""
    MATCH (n:{source})
    WHERE n.{sprop} IS NOT NULL
    MERGE (m:{node} {{ node_id: n.{sprop} }})
    """)


def add_prop_from(node, prop, source, sprop, skey):
    """Set `node`.`prop` to `source`.`sprop`, matching `source`.`skey` to the target's node_id."""
    print(f"Add {node}.{prop} from {source}.{sprop}")
    run_query(f"""
    MATCH (n:{source})
    WHERE n.{skey} IS NOT NULL
    MATCH (m:{node} {{ node_id: n.{skey} }})
    SET m.{prop} = n.{sprop}
    """)


def create_edge(name, from_node, to, from_key="node_id", to_key="node_id"):
    """Create a `name` relationship from each `from_node` to the `to` node whose `to_key` matches its `from_key`."""
    print(f"Create {from_node}-[{name}]->{to}")
    run_query(f"""
    MATCH (f:{from_node})
    WHERE f.{from_key} IS NOT NULL
    MATCH (t:{to} {{ {to_key}: f.{from_key} }})
    CREATE (f)-[r:{name}]->(t)
    """)


def create_edge_from_edge(name, from_node, to, orig_name, orig_from, orig_to):
    """Recreate every `orig_name` edge between `orig_from`/`orig_to` as a `name` edge between the corresponding `from_node`/`to` nodes (matched by node_id)."""
    print(f"Create {from_node}-[{name}]->{to}")
    run_query(f"""
    MATCH (of:{orig_from})-[:{orig_name}]->(ot:{orig_to})
    MATCH (f:{from_node} {{ node_id: of.node_id }})
    MATCH (t:{to} {{ node_id: ot.node_id }})
    CREATE (f)-[r:{name}]->(t)
    """)
