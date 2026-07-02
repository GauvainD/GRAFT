import neo4j
import os
import pandas as pd
from lark import Lark, Transformer
import time

database = neo4j.GraphDatabase.driver("bolt://localhost:7687")
PWD = os.getcwd()
PATH = "../data-gen/sf1/"

def run_query(query):
    _, summary, _ = database.execute_query(query)
    print(summary.result_available_after)
    print(summary.counters)

def clear():
    run_query("MATCH (n) DETACH DELETE n")

def import_node(nodename, properties):
    print("import",nodename)
    props = ",".join([f"{p}:row[{i}]" for (i,p) in enumerate(["id"]+properties)])
    run_query(f"""
LOAD CSV FROM 'file:///{PWD}/{PATH}/{nodename}.csv' AS row FIELDTERMINATOR '|'
CREATE (n:{nodename} {{ {props} }})
                           """)
    run_query(f"""
    CREATE INDEX {nodename}_index FOR (n:{nodename}) ON (n.id)
    """)

def import_edge(edgename, node1, node2, mergeindex1, mergeindex2):
    print("import",edgename)
    data1 = pd.read_csv(f"{PATH}/{node1}.csv", sep="|", header=None)
    data2 = pd.read_csv(f"{PATH}/{node2}.csv", sep="|", header=None)
    data = pd.merge(data1, data2, how='inner', left_on=mergeindex1, right_on=mergeindex2)
    pairs = str(data[["0_x", "0_y"]].to_numpy().tolist())
    run_query(f"""
    unwind {pairs} as pair
    MATCH (n1:{node1} {{ id: toString(pair[0]) }})
    MATCH (n2:{node2} {{ id: toString(pair[1]) }})
    CREATE (n1)-[r:{edgename}]->(n2)
    """)

parser = Lark("""

    graph: "create"i "graph"i "type"i "{" _elems? "}"

    _elems: elem ("," elem)* ","?

    ?elem: node | edge

    node: "(" SYMBOL ":" _opt_props ")"

    edge: "(" ":" SYMBOL ")" "-" "[" SYMBOL ":" _opt_props "]" "-" ">" "(" ":" SYMBOL ")"

    _opt_props: (props | "{" "}")

    props: "{" prop ("," prop)* "}"

    prop: SYMBOL "TEXT"

    SYMBOL: /[a-zA-Z][a-zA-Z0-9_]*/

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
              """, start="graph")

class MyTransformer(Transformer):
    def graph(self, items):
        res = dict()
        for item in items:
            res.update(item)
        return res

    def SYMBOL(self, item):
        return item.value

    def prop(self, item):
        return item[0]

    def props(self, items):
        return list(items)

    def node(self, items):
        if len(items) == 1:
            return {("node", items[0]): []}
        else:
            return {("node", items[0]): items[1]}

    def edge(self, items):
        if len(items) == 3:
            return {("edge", items[1]): (items[0], items[2], [])}
        else:
            return {("edge", items[1]): (items[0], items[3], items[2])}


def parse_schema(filename):
    with open(filename) as f:
        text = f.read()
        res = parser.parse(text)
        res = MyTransformer().transform(res)
        return res

def import_schema(schema):
    clear()
    for i, item in enumerate(schema):
        if item[0] == "node":
            name = item[1]
            props = schema[item]
            print(f"{i}: import {name}")
            import_node(name, props)
        else:
            name = item[1]
            (node1, node2, props) = schema[item]
            np1 = len(schema[("node", node1)])
            np2 = len(schema[("node", node2)])
            index = max(np1, np2)
            print(f"{i}: import edge {name}")
            import_edge(name, node1, node2, index, index)
    # time.sleep(2)

schema = parse_schema("sources.pgschema")
print(schema)
import_schema(schema)

