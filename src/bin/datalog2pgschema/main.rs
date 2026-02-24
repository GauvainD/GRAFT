use std::collections::HashMap;

use docopt::Docopt;
use pest::Parser;
use pest_derive::Parser;
use serde::Deserialize;
use transproof::property_graph::{Properties, PropertyGraph};

#[derive(Parser)]
#[grammar = "src/bin/datalog2pgschema/datalog.pest"]
struct DatalogParser;

const USAGE: &str = "
Converts from datalog to pgschema

Usage:
    datalog2pgschema <schema>
";

#[derive(Debug, Deserialize, Clone)]
struct Args {
    arg_schema: String,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    let content = std::fs::read_to_string(&args.arg_schema).unwrap();
    let res = DatalogParser::parse(Rule::MAIN, &content)
        .unwrap()
        .next()
        .unwrap();
    let mut pg = PropertyGraph::default();
    let mut nodeNames = HashMap::new();
    for rule in res.into_inner() {
        match rule.as_rule() {
            Rule::NodeType => {
                let mut inner = rule.into_inner();
                let name = inner.next().unwrap().as_str().to_string();
                nodeNames.insert(
                    name.clone(),
                    pg.graph.add_node(Properties {
                        name: name,
                        map: HashMap::new(),
                    }),
                );
            }
            Rule::Property => {
                let mut inner = rule.into_inner();
                let nodename = inner.next().unwrap().as_str().to_string();
                let propname = inner.next().unwrap().as_str().to_string();
                let proptype = inner.next().unwrap().as_str().to_string();
                let node = nodeNames.get(&nodename).unwrap();
                let weight = pg.graph.node_weight_mut(*node).unwrap();
                weight.map.insert(propname, proptype);
            }
            Rule::EdgeType => {
                let mut inner = rule.into_inner();
                let edgename = inner.next().unwrap().as_str().to_string();
                let nodename1 = inner.next().unwrap().as_str().to_string();
                let nodename2 = inner.next().unwrap().as_str().to_string();
                let node1 = nodeNames.get(&nodename1).unwrap();
                let node2 = nodeNames.get(&nodename2).unwrap();
                pg.graph.add_edge(
                    *node1,
                    *node2,
                    Properties {
                        name: edgename,
                        map: HashMap::new(),
                    },
                );
            }
            _ => {}
        }
    }
    println!("{}", pg);
}
